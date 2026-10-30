// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package logging

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.uber.org/mock/gomock"

	"github.com/apache/skywalking-banyandb/api/common"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// fakeClient is a queue.Client that records what was published, and can be told
// to panic so that the consumer's recovery can be exercised.
type fakeClient struct {
	// markedModules is the module the write path logs under for the last batch.
	markedModules atomic.Value
	// nodeIDs records the node_id tag of every published element, so a test can
	// assert on what actually reached the write path rather than on what the
	// sink held.
	nodeIDs []string
	// timeouts records the timeout each publisher was created with.
	timeouts []time.Duration
	// batchSizes records the message count of every Publish.
	batchSizes []int
	// messageNodes records the node every published message is addressed to.
	messageNodes []string
	// unreadyFor makes the next N Close calls answer as a local bus with no
	// subscriber on the topic: a nil map and bus.ErrTopicNotExist.
	unreadyFor atomic.Int64
	// publishDelay slows every Publish, in nanoseconds, to hold a batch in flight.
	publishDelay atomic.Int64
	// closes counts Close calls, which is one per publish attempt.
	closes atomic.Int64
	// started counts the publishes that have begun, delayed or not.
	started        atomic.Int64
	published      atomic.Int64
	mu             sync.Mutex
	panicOnPublish atomic.Bool
	// rejectOnClose makes Close answer the way localBatchPublisher answers a
	// refused batch: a populated per-node map alongside a NIL error.
	rejectOnClose atomic.Bool
}

func (f *fakeClient) seenMessageNodes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.messageNodes...)
}

func (f *fakeClient) seenBatchSizes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.batchSizes...)
}

func (f *fakeClient) seenTimeouts() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Duration(nil), f.timeouts...)
}

func (f *fakeClient) seenNodeIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.nodeIDs...)
}

func (f *fakeClient) NewBatchPublisher(timeout time.Duration) queue.BatchPublisher {
	f.mu.Lock()
	f.timeouts = append(f.timeouts, timeout)
	f.mu.Unlock()
	return &fakePublisher{client: f}
}

func (f *fakeClient) Publish(context.Context, bus.Topic, ...bus.Message) (bus.Future, error) {
	return nil, nil
}

func (f *fakeClient) Broadcast(time.Duration, bus.Topic, bus.Message) ([]bus.Future, error) {
	return nil, nil
}

func (f *fakeClient) Scatter(context.Context, bus.Topic, bus.Message, queue.ScatterOptions) ([]queue.NodeStream, error) {
	return nil, nil
}

func (f *fakeClient) NewChunkedSyncClient(string, uint32) (queue.ChunkedSyncClient, error) {
	return nil, nil
}

func (f *fakeClient) NewNodeSchemaStatusClient(string) (clusterv1.NodeSchemaStatusServiceClient, error) {
	return nil, nil
}

func (f *fakeClient) NewExportClient(string) (transferv1.ExportServiceClient, error) {
	return nil, errors.New("not supported")
}
func (f *fakeClient) SetSelfNode(_, _, _ string)              {}
func (f *fakeClient) Register(bus.Topic, schema.EventHandler) {}
func (f *fakeClient) OnAddOrUpdate(schema.Metadata)           {}
func (f *fakeClient) GracefulStop()                           {}
func (f *fakeClient) HealthyNodes() []string                  { return nil }
func (f *fakeClient) Name() string                            { return "fake-queue" }
func (f *fakeClient) Serve() run.StopNotify                   { return nil }
func (f *fakeClient) GetRouteTable() *databasev1.RouteTable   { return nil }

type fakePublisher struct{ client *fakeClient }

func (p *fakePublisher) Publish(ctx context.Context, _ bus.Topic, messages ...bus.Message) (bus.Future, error) {
	// What the write path would log about this batch, under the logger it
	// would take from ctx.
	if l := logger.FetchOrDefault(ctx, "stream", nil); l != nil {
		p.client.markedModules.Store(l.Module())
	}
	p.client.started.Add(1)
	if p.client.panicOnPublish.Load() {
		panic("induced publish panic")
	}
	if d := p.client.publishDelay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}
	p.client.published.Add(int64(len(messages)))
	p.client.mu.Lock()
	defer p.client.mu.Unlock()
	p.client.batchSizes = append(p.client.batchSizes, len(messages))
	for _, m := range messages {
		p.client.messageNodes = append(p.client.messageNodes, m.Node())
		iwr, ok := m.Data().(*streamv1.InternalWriteRequest)
		if !ok {
			continue
		}
		tags := iwr.GetRequest().GetElement().GetTagFamilies()[0].GetTags()
		p.client.nodeIDs = append(p.client.nodeIDs, tags[0].GetStr().GetValue())
	}
	return nil, nil
}

func (p *fakePublisher) Close() (map[string]*common.Error, error) {
	p.client.closes.Add(1)
	if p.client.unreadyFor.Load() > 0 {
		p.client.unreadyFor.Add(-1)
		return nil, bus.ErrTopicNotExist
	}
	if p.client.rejectOnClose.Load() {
		// Exactly banyand/queue/local.go:163-167 -- the map carries the
		// rejection and the error return is nil.
		return map[string]*common.Error{
			"local": common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL,
				"disk usage is too high, stop writing"),
		}, nil
	}
	return nil, nil
}

// testService starts delivery against client. adjust sets the buffer's bounds
// over the test defaults, and each opt adjusts the service before delivery
// starts, so a test states only the part it is about.
func testService(t *testing.T, client queue.Client, adjust func(*queue.BufferedConfig), opts ...func(*Service)) (*Service, *Sink) {
	t.Helper()
	cfg := &logger.NativeLogging{Enabled: true, Level: "debug", TTLDays: 7}
	sink := newSink(func(c *queue.BufferedConfig) {
		c.FlushInterval = 20 * time.Millisecond
		c.BatchCount = 5
		if adjust != nil {
			adjust(c)
		}
	})
	sink.SetNode(NodeInfo{NodeID: "data-hot-0", NodeType: "data"})
	svc := NewService(sink, cfg, nil, client, nil, nil)
	svc.l = logger.GetLogger("native-log-test")
	// The schema is created in Serve against a metadata repo; these tests drive
	// delivery directly, so stand in for what one successful pass would have
	// established.
	svc.schema.Store(schemaReady)
	for _, opt := range opts {
		opt(svc)
	}
	sink.pub.Start(svc.delivery())
	t.Cleanup(sink.pub.Close)
	return svc, sink
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitSettled waits until nothing is charged. flush counts an outcome in its
// body and settles the bytes in its defer, so a test that reads the byte
// counters the moment an outcome is counted can still see them charged. A real
// leak never settles, so it still fails, by timeout.
func waitSettled(t *testing.T, sink *Sink) {
	t.Helper()
	waitFor(t, "every charged byte to be settled", func() bool {
		return sink.QueuedBytes() == 0 && sink.InFlightBytes() == 0
	})
}

// TestEveryAdmittedEventIsWrittenOrCounted is the accounting invariant: on the
// healthy path nothing is admitted and then silently lost.
func TestEveryAdmittedEventIsWrittenOrCounted(t *testing.T) {
	client := &fakeClient{}
	_, sink := testService(t, client, nil)

	const admitted = 40
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	waitFor(t, "all events to be published", func() bool {
		return client.published.Load() == int64(admitted)
	})

	// Publish is counted before the outcome is, so wait for the outcome.
	waitFor(t, "every admitted event to be written or counted as dropped", func() bool {
		var dropped uint64
		for _, r := range allReasons {
			dropped += sink.Dropped(r)
		}
		return sink.Written()+dropped == admitted
	})
	waitSettled(t, sink)
}

// TestDeliverySurvivesAPanicInPublish is the falsifying assertion for the
// recovery boundary: run.Go recovers a panic but does not restart the
// delivery goroutine, so recovering only there would end native logging for
// the process. The assertion is that events admitted AFTER a panic are still
// written, and that the panicking batch is counted as publish_failed.
func TestDeliverySurvivesAPanicInPublish(t *testing.T) {
	client := &fakeClient{}
	_, sink := testService(t, client, nil)

	client.panicOnPublish.Store(true)
	for i := 0; i < 5; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	waitFor(t, "the panicking batch to be counted", func() bool {
		return sink.Dropped(reasonPublishFailed) >= 5
	})

	client.panicOnPublish.Store(false)
	for i := 0; i < 5; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	waitFor(t, "the consumer to keep working after the panic", func() bool {
		return client.published.Load() >= 5
	})

	waitSettled(t, sink)
}

// TestShutdownDrainsWhatWasAdmitted covers the drain path: admission stops
// first, so everything admitted before the cutoff is published rather than
// counted as lost.
func TestShutdownDrainsWhatWasAdmitted(t *testing.T) {
	client := &fakeClient{}
	_, sink := testService(t, client, nil)

	const admitted = 12
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	sink.pub.Close()

	if got := client.published.Load(); got != admitted {
		t.Fatalf("published %d of %d admitted events at shutdown; the rest were stranded",
			got, admitted)
	}
	if got := sink.Dropped(reasonShutdown); got != 0 {
		t.Fatalf("%d events counted as shutdown_deadline while the deadline had room", got)
	}
}

// TestFlushStampsIdentityOnWhatItPublishes is the falsifying assertion for the
// wiring, not the helper: stamp existing is not enough, flush has to call it.
// Without it every published element carries an empty node_id -- half the
// series key, and an input to the shard.
func TestFlushStampsIdentityOnWhatItPublishes(t *testing.T) {
	client := &fakeClient{}
	_, sink := testService(t, client, nil)

	for i := 0; i < 6; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	waitFor(t, "the batch to be published", func() bool {
		return client.published.Load() >= 6
	})

	for i, got := range client.seenNodeIDs() {
		if got != "data-hot-0" {
			t.Fatalf("published element %d carries node_id %q, want the node's own id", i, got)
		}
	}
}

// TestRejectedBatchIsCountedAsLostNotWritten is the falsifying assertion for
// the sink's central promise -- that loss is always counted. A local publisher
// reports a refusal in Close's per-node map and returns a nil error beside it,
// so a flush that reads only the error sees success. The assertion is on
// written_total staying at zero: counting a discarded batch as written is worse
// than losing it, because the counter an operator is told to watch says the
// feature is healthy while every line is being thrown away.
func TestRejectedBatchIsCountedAsLostNotWritten(t *testing.T) {
	client := &fakeClient{}
	_, sink := testService(t, client, nil)
	client.rejectOnClose.Store(true)

	const admitted = 10
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	waitFor(t, "the refused batches to be counted", func() bool {
		return sink.Dropped(reasonPublishFailed) >= admitted
	})

	if got := sink.Written(); got != 0 {
		t.Fatalf("written_total = %d after every batch was refused; "+
			"Close reported the rejection in its map, not its error", got)
	}
	waitSettled(t, sink)
}

// TestAcceptedBatchIsStillCountedAsWritten guards the other direction: reading
// Close's map must not turn an empty map into a failure.
func TestAcceptedBatchIsStillCountedAsWritten(t *testing.T) {
	client := &fakeClient{}
	_, sink := testService(t, client, nil)

	const admitted = 10
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	waitFor(t, "the batches to be written", func() bool {
		return sink.Written() >= admitted
	})
	if got := sink.Dropped(reasonPublishFailed); got != 0 {
		t.Fatalf("publish_failed = %d on the healthy path", got)
	}
}

// TestSchemaStateSurvivesATransientMetadataFailure pins the retry semantics.
// The schema pass now runs on every tick rather than latching on first success,
// so a momentary metadata failure reaches applySchema while the node is healthy.
// Treating that as "schema gone" would drop batches that would have been
// written -- the retry must not be able to make things worse than the latch did.
func TestSchemaStateSurvivesATransientMetadataFailure(t *testing.T) {
	svc := &Service{l: logger.GetLogger("native-log-test")}

	svc.applySchema(schemaState{}, nil)
	if svc.schema.Load() != schemaReady {
		t.Fatal("a successful pass did not set ready")
	}

	svc.applySchema(schemaState{}, errors.New("etcd: context deadline exceeded"))
	if svc.schema.Load() != schemaReady {
		t.Fatal("a transient metadata error cleared ready; batches that would " +
			"have been written are now dropped as schema_unavailable")
	}

	svc.applySchema(schemaState{}, fmt.Errorf("%w: tag 2 is wrong", errSchemaIncompatible))
	if got := svc.schema.Load(); got != schemaIncompatible {
		t.Fatalf("an incompatible schema left state %d", got)
	}

	svc.applySchema(schemaState{}, nil)
	if got := svc.schema.Load(); got != schemaReady {
		t.Fatalf("recovery left state %d", got)
	}
}

// TestNeverReadyStaysNotReady covers the startup case: a node that has never
// established the schema must not be nudged into publishing by a failure.
func TestNeverReadyStaysNotReady(t *testing.T) {
	svc := &Service{l: logger.GetLogger("native-log-test")}
	svc.applySchema(schemaState{}, errors.New("metadata not up yet"))
	if svc.schema.Load() == schemaReady {
		t.Fatal("ready was set by a failed schema pass")
	}
}

// TestUnestablishedSchemaDropsAsUnavailable covers delivery before the schema
// exists: the batch is counted under its own reason, not as written and not
// as a publish failure.
func TestUnestablishedSchemaDropsAsUnavailable(t *testing.T) {
	client := &fakeClient{}
	_, sink := testService(t, client, nil, func(s *Service) { s.schema.Store(schemaUnready) })
	for i := 0; i < 5; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	waitFor(t, "the batch to be counted", func() bool { return sink.Dropped(reasonSchemaMissing) == 5 })
	if got := client.published.Load(); got != 0 {
		t.Fatalf("%d events published without a schema", got)
	}
	waitSettled(t, sink)
}

// TestIncompatibleSchemaDropsAsIncompatible keeps the two schema reasons
// apart at delivery: an incompatible stream needs an operator, so its loss is
// not counted as a wait for the schema.
func TestIncompatibleSchemaDropsAsIncompatible(t *testing.T) {
	client := &fakeClient{}
	_, sink := testService(t, client, nil, func(s *Service) { s.schema.Store(schemaIncompatible) })
	for i := 0; i < 5; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	waitFor(t, "the batch to be counted", func() bool { return sink.Dropped(reasonSchemaIncompatible) == 5 })
	if got := sink.Dropped(reasonSchemaMissing) + uint64(client.published.Load()); got != 0 {
		t.Fatalf("%d events counted as schema_unavailable or published to an incompatible stream", got)
	}
	waitSettled(t, sink)
}

// TestWriteTimeoutReachesThePublisher checks that the service's write timeout,
// not some other value, is handed to every batch publisher.
func TestWriteTimeoutReachesThePublisher(t *testing.T) {
	client := &fakeClient{}
	_, sink := testService(t, client, nil, func(s *Service) { s.timing.writeTimeout = 750 * time.Millisecond })

	for i := 0; i < 5; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	waitFor(t, "a batch to be published", func() bool { return client.published.Load() >= 5 })

	seen := client.seenTimeouts()
	if len(seen) == 0 {
		t.Fatal("no publisher was created, so the timeout was never checked")
	}
	for _, got := range seen {
		if got != 750*time.Millisecond {
			t.Fatalf("publisher created with timeout %s, want 750ms", got)
		}
	}
}

// TestDrainTimeoutBoundsShutdown is the falsifying assertion for the drain
// limit. Each publish takes 50ms and the limit is 10ms, so the drain can
// publish at most one batch before its deadline and must count the rest as
// lost. Without the limit, everything drains.
func TestDrainTimeoutBoundsShutdown(t *testing.T) {
	client := &fakeClient{}
	_, sink := testService(t, client, func(c *queue.BufferedConfig) { c.DrainTimeout = 10 * time.Millisecond })
	client.publishDelay.Store(int64(50 * time.Millisecond))

	const admitted = 100
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	sink.pub.Close()

	lost := sink.Dropped(reasonShutdown)
	if lost == 0 {
		t.Fatal("the drain published everything; the 10ms drain limit was not applied")
	}
	var dropped uint64
	for _, r := range allReasons {
		dropped += sink.Dropped(r)
	}
	if got := sink.Written() + dropped; got != admitted {
		t.Fatalf("admitted %d, but written+dropped = %d", admitted, got)
	}
}

// fakeMemory reports a memory pressure state. Only State is implemented:
// bindPressure calls nothing else, and in particular nothing that blocks.
type fakeMemory struct {
	protector.Memory
	high *atomic.Bool
}

func (m fakeMemory) State() protector.State {
	if m.high.Load() {
		return protector.StateHigh
	}
	return protector.StateLow
}

// TestHighMemoryPressureDropsAndCounts is the falsifying assertion for reusing
// the memory protector: while it reports high pressure the sink drops and
// counts instead of buffering, and it buffers again once pressure falls.
func TestHighMemoryPressureDropsAndCounts(t *testing.T) {
	high := &atomic.Bool{}
	sink := NewSink()
	svc := NewService(sink, &logger.NativeLogging{}, nil, nil, fakeMemory{high: high}, nil)
	svc.bindPressure()

	high.Store(true)
	sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	if got := sink.Dropped(reasonMemoryPressure); got != 1 {
		t.Fatalf("memory_pressure drops = %d under high pressure, want 1", got)
	}
	if sink.QueuedBytes() != 0 {
		t.Fatal("an event was buffered under high memory pressure")
	}

	high.Store(false)
	sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	if sink.QueuedBytes() == 0 {
		t.Fatal("the sink did not buffer again after pressure fell")
	}
}

// TestGracefulStopReturnsWhileAWriteIsStuck is the falsifying assertion for
// the shutdown bound. A local write runs on the consumer and does not honor
// its context, so a stuck one cannot be interrupted; shutdown must not wait
// for it.
func TestGracefulStopReturnsWhileAWriteIsStuck(t *testing.T) {
	client := &fakeClient{}
	client.publishDelay.Store(int64(time.Hour))
	svc, sink := testService(t, client, func(c *queue.BufferedConfig) {
		c.BatchCount = 1
		c.DrainTimeout = 50 * time.Millisecond
	})
	//panicdiag:allow-rawgo test stand-in for Serve's loop
	go svc.maintain(context.Background())
	sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	waitFor(t, "the write to start and get stuck", func() bool { return client.started.Load() == 1 })

	returned := make(chan struct{})
	//panicdiag:allow-rawgo test watchdog
	go func() {
		svc.GracefulStop()
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("GracefulStop is still waiting for a stuck write")
	}
	if got := sink.Dropped(reasonShutdown); got != 1 {
		t.Fatalf("the stuck event was counted %d times as shutdown_deadline, want 1", got)
	}
}

// TestGracefulStopDoesNotWaitForASchemaPass blocks the schema pass on its
// metadata call: GracefulStop cancels it instead of waiting out its timeout.
func TestGracefulStopDoesNotWaitForASchemaPass(t *testing.T) {
	ctrl := gomock.NewController(t)
	groups := schema.NewMockGroup(ctrl)
	entered := make(chan struct{}, 1)
	groups.EXPECT().GetGroup(gomock.Any(), GroupName).DoAndReturn(func(ctx context.Context, _ string) (*commonv1.Group, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}).AnyTimes()
	svc, _ := testService(t, &fakeClient{}, nil, func(s *Service) {
		s.metadata = repoWith(ctrl, groups, schema.NewMockStream(ctrl))
		s.timing.schemaRetry = 10 * time.Millisecond
	})
	ctx, cancel := context.WithCancel(context.Background())
	svc.stopMaintain = cancel
	//panicdiag:allow-rawgo test stand-in for Serve's loop
	go svc.maintain(ctx)
	<-entered

	start := time.Now()
	svc.GracefulStop()
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("GracefulStop waited %s for a schema pass", waited)
	}
}

// TestWriterThatLoadedTheSinkBeforeStopIsCounted is the review's shutdown
// race at the sink: a writer that took the sink before GracefulStop and
// admits after it has drained. The event is counted as lost, not left in a
// buffer nothing delivers.
func TestWriterThatLoadedTheSinkBeforeStopIsCounted(t *testing.T) {
	client := &fakeClient{}
	svc, sink := testService(t, client, nil)
	//panicdiag:allow-rawgo test stand-in for Serve's loop
	go svc.maintain(context.Background())
	svc.GracefulStop()

	sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	if got := sink.Dropped(reasonShutdown); got != 1 {
		t.Fatalf("an event admitted after shutdown was counted %d times as shutdown_deadline, want 1", got)
	}
	if got := sink.QueuedBytes(); got != 0 {
		t.Fatalf("%d bytes left charged after shutdown", got)
	}
}

// TestIncompatibleSchemaIsLoggedOnce pins the design's "one error through
// normal logging". The schema pass repeats every ten seconds, so logging on
// every pass would repeat the same error forever. A recurrence after a
// recovery is a new event and is logged again.
func TestIncompatibleSchemaIsLoggedOnce(t *testing.T) {
	var buf bytes.Buffer
	zl := zerolog.New(&buf)
	svc := &Service{l: &logger.Logger{Logger: &zl}}
	incompatible := fmt.Errorf("%w: tag 2 is wrong", errSchemaIncompatible)

	for i := 0; i < 3; i++ {
		svc.applySchema(schemaState{}, incompatible)
	}
	if n := strings.Count(buf.String(), "cannot store"); n != 1 {
		t.Fatalf("three passes on the same incompatible stream logged %d errors, want 1", n)
	}

	svc.applySchema(schemaState{}, nil)
	svc.applySchema(schemaState{}, incompatible)
	if n := strings.Count(buf.String(), "cannot store"); n != 2 {
		t.Fatalf("an incompatibility after a recovery logged %d errors in total, want 2", n)
	}
}

// TestBatchIsCappedAtAQuarterOfTheBudget is the falsifying assertion for the
// byte cap. The count trigger (1000) and the interval (1h) can never fire, so
// only the cap -- a quarter of a 12-line budget, which is 3 lines -- can make
// anything reach the destination.
func TestBatchIsCappedAtAQuarterOfTheBudget(t *testing.T) {
	// An event charges its line plus what its built request holds, so the
	// budget is expressed in whole events rather than in line lengths.
	built, err := testSink(t).build(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	event := entryCost(int64(len(sampleLine)), built)
	client := &fakeClient{}
	_, sink := testService(t, client, func(c *queue.BufferedConfig) {
		c.BatchCount = 1000
		c.FlushInterval = time.Hour
		c.MaxBytes = 12 * event
	})

	const admitted = 9
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	waitFor(t, "the byte cap to flush every batch", func() bool {
		return client.published.Load() == admitted
	})
	for _, n := range client.seenBatchSizes() {
		if n > 3 {
			t.Fatalf("published a batch of %d lines, over the 3-line quarter of the budget", n)
		}
	}
}

// fakeGauge records the last value set for each label combination.
type fakeGauge struct {
	vals map[string]float64
	mu   sync.Mutex
}

func (g *fakeGauge) Set(v float64, labels ...string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.vals == nil {
		g.vals = map[string]float64{}
	}
	g.vals[strings.Join(labels, ",")] = v
}
func (g *fakeGauge) Add(float64, ...string) {}
func (g *fakeGauge) Delete(...string) bool  { return false }

// TestBufferGaugeSplitsQueuedFromInFlight is the falsifying assertion for the
// state label. QueuedBytes includes the batch being published, so the queued
// state must subtract it; a single unlabelled value cannot show a stalled
// publish.
func TestBufferGaugeSplitsQueuedFromInFlight(t *testing.T) {
	sink := testSink(t)
	p := stubbed(sink, queue.Accepted)
	p.charged, p.inFlight = 300, 100
	buffer := &fakeGauge{}
	m := &metrics{dropped: &fakeGauge{}, written: &fakeGauge{}, bufferBytes: buffer}

	m.observe(sink)

	if got := buffer.vals["queued"]; got != 200 {
		t.Fatalf(`buffer_bytes{state="queued"} = %v, want 200 (held minus in flight)`, got)
	}
	if got := buffer.vals["in_flight"]; got != 100 {
		t.Fatalf(`buffer_bytes{state="in_flight"} = %v, want 100`, got)
	}
}

// TestUnreadyDestinationIsRetriedThenWritten is the falsifying assertion for
// the retain-and-retry. The first three attempts find no subscriber on the
// local topic, as at startup before the stream service registers it; the
// fourth succeeds. Every event must be written and none dropped.
func TestUnreadyDestinationIsRetriedThenWritten(t *testing.T) {
	client := &fakeClient{}
	client.unreadyFor.Store(3)
	_, sink := testService(t, client, nil)

	const admitted = 5
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	waitFor(t, "the held-back batch to be written", func() bool { return sink.Written() == admitted })

	if got := sink.Dropped(reasonDestinationUnready) + sink.Dropped(reasonPublishFailed); got != 0 {
		t.Fatalf("%d events dropped although the destination became ready within the retries", got)
	}
	waitSettled(t, sink)
}

// TestUnreadyDestinationIsDroppedAfterBoundedRetries pins the bound: a topic
// that never gets a subscriber costs destinationRetries attempts and then the
// batch, counted under its own reason.
func TestUnreadyDestinationIsDroppedAfterBoundedRetries(t *testing.T) {
	client := &fakeClient{}
	client.unreadyFor.Store(1 << 30)
	_, sink := testService(t, client, nil)

	const admitted = 5
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	waitFor(t, "the batch to be dropped after the retries", func() bool {
		return sink.Dropped(reasonDestinationUnready) == admitted
	})

	if got := client.closes.Load(); got != destinationRetries+1 {
		t.Fatalf("%d publish attempts, want %d: the retries are not bounded", got, destinationRetries+1)
	}
	waitSettled(t, sink)
}

// TestDrainDoesNotHoldBackForAnUnreadyDestination covers shutdown: there is
// no next tick to retry on, so a batch held back there would stay charged and
// never be released.
func TestDrainDoesNotHoldBackForAnUnreadyDestination(t *testing.T) {
	client := &fakeClient{}
	client.unreadyFor.Store(1 << 30)
	_, sink := testService(t, client, func(c *queue.BufferedConfig) {
		c.BatchCount = 1000
		c.FlushInterval = time.Hour
	})

	const admitted = 5
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	sink.pub.Close()

	if got := sink.Dropped(reasonDestinationUnready); got != admitted {
		t.Fatalf("drain dropped %d as destination_unready, want %d", got, admitted)
	}
	if got := sink.QueuedBytes(); got != 0 {
		t.Fatalf("%d bytes still charged after shutdown", got)
	}
}

// TestCollectionResumesInTheSameProcess covers what the restart-based
// integration test cannot: a running sink recovers by itself once the
// destination stops refusing, because nothing latches on a refused batch.
func TestCollectionResumesInTheSameProcess(t *testing.T) {
	client := &fakeClient{}
	client.rejectOnClose.Store(true)
	_, sink := testService(t, client, nil)

	for i := 0; i < 5; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	waitFor(t, "the refused batch to be counted", func() bool {
		return sink.Dropped(reasonPublishFailed) >= 5
	})

	client.rejectOnClose.Store(false)
	for i := 0; i < 5; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	waitFor(t, "collection to resume without a restart", func() bool { return sink.Written() >= 5 })
}

// TestEveryEventIsWrittenToShardZero keeps each node's logs on the node that
// produced them, as native metrics do. A shard taken from the entity hash would
// spread a node's events over several shards, and in a cluster over several
// nodes.
func TestEveryEventIsWrittenToShardZero(t *testing.T) {
	sink := NewSink()
	for i := 0; i < 64; i++ {
		sink.SetNode(NodeInfo{NodeID: fmt.Sprintf("data-%d:17912", i), NodeType: "data"})
		for _, level := range []zerolog.Level{zerolog.DebugLevel, zerolog.InfoLevel, zerolog.WarnLevel, zerolog.ErrorLevel} {
			req, err := sink.build(level, "MEASURE", []byte(`{"message":"m"}`))
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			req.seq = uint64(i)
			sink.stamp(req)
			iwr, err := internalRequest(req.WriteRequest)
			if err != nil {
				t.Fatalf("internalRequest: %v", err)
			}
			if iwr.GetShardId() != 0 {
				t.Fatalf("node %s level %s was written to shard %d, want 0",
					sink.node.Load().NodeID, level, iwr.GetShardId())
			}
		}
	}
}

// TestEveryPublishCarriesTheNativeWriteMark ties the consumer to the write
// path's protection: the write path logs about a batch under the logger it
// finds in the publish context, and only a marked context keeps those lines
// out of native storage.
func TestEveryPublishCarriesTheNativeWriteMark(t *testing.T) {
	client := &fakeClient{}
	_, sink := testService(t, client, nil)
	sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(`{"message":"m"}`))
	waitFor(t, "the batch to be published", func() bool { return client.published.Load() == 1 })
	if got, _ := client.markedModules.Load().(string); got != "NATIVE-LOG.STREAM" {
		t.Fatalf("the write path would log this batch under %q, want NATIVE-LOG.STREAM", got)
	}
}

// fakeLocator answers Locate the way a liaison's node registry does.
type fakeLocator struct {
	err   error
	node  string
	calls atomic.Int64
}

func (l *fakeLocator) Locate(group, name string, shardID, replicaID uint32) (string, error) {
	l.calls.Add(1)
	if group != GroupName || name != StreamName || shardID != 0 || replicaID != 0 {
		return "", fmt.Errorf("located %s/%s(%d,%d), want shard 0 of the log stream", group, name, shardID, replicaID)
	}
	return l.node, l.err
}

// TestLiaisonSendsToTheDataNodeOfShardZero covers the liaison route: every
// message is addressed to the data node that owns shard 0 of the log group,
// carries the liaison's own identity, and is counted as written.
func TestLiaisonSendsToTheDataNodeOfShardZero(t *testing.T) {
	client := &fakeClient{}
	locator := &fakeLocator{node: "data-2"}
	_, sink := testService(t, client, nil, func(s *Service) { s.nodes = locator })
	sink.SetNode(NodeInfo{NodeID: "liaison-0", NodeType: "liaison"})

	const admitted = 7
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "LIAISON-GRPC", []byte(sampleLine))
	}
	waitFor(t, "every event to be written", func() bool { return sink.Written() == admitted })

	for i, node := range client.seenMessageNodes() {
		if node != "data-2" {
			t.Fatalf("message %d is addressed to %q, want the owner of shard 0", i, node)
		}
	}
	for i, id := range client.seenNodeIDs() {
		if id != "liaison-0" {
			t.Fatalf("element %d carries node_id %q, want the liaison's own", i, id)
		}
	}
}

// TestLiaisonCountsWhatNoDataNodeTook covers a liaison that locates no data
// node: the batch is held for the bounded retries, publishes nothing, and is
// then counted as locate_failed, never as written.
func TestLiaisonCountsWhatNoDataNodeTook(t *testing.T) {
	client := &fakeClient{}
	locator := &fakeLocator{err: errors.New("no nodes available")}
	_, sink := testService(t, client, nil, func(s *Service) { s.nodes = locator })

	const admitted = 5
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "LIAISON-GRPC", []byte(sampleLine))
	}
	waitFor(t, "the batch to be counted as locate_failed", func() bool {
		return sink.Dropped(reasonLocateFailed) == admitted
	})
	// The interval may split the events into more than one batch, and each
	// batch is located once per attempt.
	if got := locator.calls.Load(); got < destinationRetries+1 {
		t.Fatalf("located %d times, want at least the first attempt and %d retries", got, destinationRetries)
	}
	if got := client.published.Load(); got != 0 {
		t.Fatalf("published %d events with no node located", got)
	}
	if got := sink.Written(); got != 0 {
		t.Fatalf("%d events counted as written with no node located", got)
	}
	waitSettled(t, sink)
}
