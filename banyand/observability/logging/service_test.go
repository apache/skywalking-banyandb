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
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/apache/skywalking-banyandb/api/common"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
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
	// nodeIDs records the node_id tag of every published element, so a test can
	// assert on what actually reached the write path rather than on what the
	// sink held.
	nodeIDs []string
	// timeouts records the timeout each publisher was created with.
	timeouts  []time.Duration
	published atomic.Int64
	// publishDelay slows every Publish, in nanoseconds, to hold a batch in flight.
	publishDelay   atomic.Int64
	mu             sync.Mutex
	panicOnPublish atomic.Bool
	// rejectOnClose makes Close answer the way localBatchPublisher answers a
	// refused batch: a populated per-node map alongside a NIL error.
	rejectOnClose atomic.Bool
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

func (f *fakeClient) NewChunkedSyncClient(string, uint32) (queue.ChunkedSyncClient, error) {
	return nil, nil
}

func (f *fakeClient) NewNodeSchemaStatusClient(string) (clusterv1.NodeSchemaStatusServiceClient, error) {
	return nil, nil
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

func (p *fakePublisher) Publish(_ context.Context, _ bus.Topic, messages ...bus.Message) (bus.Future, error) {
	if p.client.panicOnPublish.Load() {
		panic("induced publish panic")
	}
	if d := p.client.publishDelay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}
	p.client.published.Add(int64(len(messages)))
	p.client.mu.Lock()
	defer p.client.mu.Unlock()
	for _, m := range messages {
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

// testService starts a consumer against client. Each opt adjusts the
// configuration before anything reads it, so a test states only the part it
// is about.
func testService(t *testing.T, client queue.Client, opts ...func(*logger.NativeLogging)) (*Service, *Sink) {
	t.Helper()
	cfg := &logger.NativeLogging{
		Enabled: true, Level: "debug", FlushInterval: 20 * time.Millisecond,
		FlushSize: 5, MaxBytes: 1 << 20, MaxEventBytes: 64 << 10, ShardNum: 2, TTLDays: 7,
		WriteTimeout: 5 * time.Second, DrainTimeout: 5 * time.Second,
		MemoryFraction: 0.02, MemoryReserve: 64 << 20,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	sink := NewSink(cfg)
	sink.SetNode(NodeInfo{NodeID: "data-hot-0", NodeType: "data"})
	svc := NewService(sink, cfg, nil, client, nil, nil)
	svc.l = logger.GetLogger("native-log-test")
	// The schema is created in Serve against a metadata repo; these tests drive
	// the consumer directly, so stand in for what one successful pass would
	// have established -- including the shard count, which routing divides by.
	svc.ready = true
	svc.shardNum = cfg.ShardNum
	go svc.consume(context.Background()) //panicdiag:allow-rawgo test consumer, no recovery wrapper needed
	t.Cleanup(func() { svc.closer.CloseThenWait() })
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

// TestEveryAdmittedEventIsWrittenOrCounted is the accounting invariant: on the
// healthy path nothing is admitted and then silently lost.
func TestEveryAdmittedEventIsWrittenOrCounted(t *testing.T) {
	client := &fakeClient{}
	_, sink := testService(t, client)

	const admitted = 40
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	waitFor(t, "all events to be published", func() bool {
		return client.published.Load() == int64(admitted)
	})

	var dropped uint64
	for _, r := range allReasons {
		dropped += sink.Dropped(r)
	}
	if got := sink.Written() + dropped; got != admitted {
		t.Fatalf("admitted %d, but written+dropped = %d; events were lost silently", admitted, got)
	}
	if sink.QueuedBytes() != 0 {
		t.Fatalf("%d bytes still charged after every event was written", sink.QueuedBytes())
	}
}

// TestConsumerSurvivesAPanicInFlush is the falsifying assertion for the
// recovery boundary: run.Go recovers a panic but does not restart the consumer,
// so recovering at the goroutine would end native logging for the process. The
// assertion is that events admitted AFTER a panic are still written.
func TestConsumerSurvivesAPanicInFlush(t *testing.T) {
	client := &fakeClient{}
	_, sink := testService(t, client)

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

	if sink.QueuedBytes() != 0 {
		t.Fatalf("%d bytes still charged after a panicking flush; the accounting leaked",
			sink.QueuedBytes())
	}
}

// TestShutdownDrainsWhatWasAdmitted covers the drain path: admission stops
// first, so everything admitted before the cutoff is published rather than
// counted as lost.
func TestShutdownDrainsWhatWasAdmitted(t *testing.T) {
	client := &fakeClient{}
	svc, sink := testService(t, client)

	const admitted = 12
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	svc.closer.CloseThenWait()

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
	_, sink := testService(t, client)

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
	_, sink := testService(t, client)
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
	if got := sink.QueuedBytes(); got != 0 {
		t.Fatalf("%d bytes still charged after the refused batches settled", got)
	}
}

// TestAcceptedBatchIsStillCountedAsWritten guards the other direction: reading
// Close's map must not turn an empty map into a failure.
func TestAcceptedBatchIsStillCountedAsWritten(t *testing.T) {
	client := &fakeClient{}
	_, sink := testService(t, client)

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

	svc.applySchema(schemaState{shardNum: 4}, nil)
	if !svc.ready || svc.shardNum != 4 {
		t.Fatalf("after a successful pass: ready=%v shardNum=%d, want true/4", svc.ready, svc.shardNum)
	}

	svc.applySchema(schemaState{}, errors.New("etcd: context deadline exceeded"))
	if !svc.ready {
		t.Fatal("a transient metadata error cleared ready; batches that would " +
			"have been written are now dropped as schema_unavailable")
	}
	if svc.shardNum != 4 {
		t.Fatalf("shardNum moved to %d on a failed pass; routing must not follow a reading that failed", svc.shardNum)
	}

	svc.applySchema(schemaState{}, fmt.Errorf("%w: tag 2 is wrong", errSchemaIncompatible))
	if svc.ready || !svc.incompatible {
		t.Fatalf("an incompatible schema left ready=%v incompatible=%v", svc.ready, svc.incompatible)
	}

	svc.applySchema(schemaState{shardNum: 2}, nil)
	if !svc.ready || svc.incompatible || svc.shardNum != 2 {
		t.Fatalf("recovery left ready=%v incompatible=%v shardNum=%d",
			svc.ready, svc.incompatible, svc.shardNum)
	}
}

// TestNeverReadyStaysNotReady covers the startup case: a node that has never
// established the schema must not be nudged into publishing by a failure.
func TestNeverReadyStaysNotReady(t *testing.T) {
	svc := &Service{l: logger.GetLogger("native-log-test")}
	svc.applySchema(schemaState{}, errors.New("metadata not up yet"))
	if svc.ready {
		t.Fatal("ready was set by a failed schema pass")
	}
}

// TestWriteTimeoutFlagReachesThePublisher is the falsifying assertion for
// --logging-native-write-timeout: the configured value, not a constant, bounds
// each batch publish.
func TestWriteTimeoutFlagReachesThePublisher(t *testing.T) {
	client := &fakeClient{}
	_, sink := testService(t, client, func(c *logger.NativeLogging) { c.WriteTimeout = 750 * time.Millisecond })

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
			t.Fatalf("publisher created with timeout %s, want the configured 750ms", got)
		}
	}
}

// TestDrainTimeoutFlagBoundsShutdown is the falsifying assertion for
// --logging-native-drain-timeout. Each publish takes 50ms and the drain limit
// is 10ms, so the drain can publish at most one batch before its deadline and
// must count the rest as lost. With the old fixed 5s limit, everything drains.
func TestDrainTimeoutFlagBoundsShutdown(t *testing.T) {
	client := &fakeClient{}
	svc, sink := testService(t, client, func(c *logger.NativeLogging) { c.DrainTimeout = 10 * time.Millisecond })
	client.publishDelay.Store(int64(50 * time.Millisecond))

	const admitted = 100
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	}
	svc.closer.CloseThenWait()

	lost := sink.Dropped(reasonShutdown)
	if lost == 0 {
		t.Fatal("the drain published everything; the configured 10ms drain limit was not applied")
	}
	var dropped uint64
	for _, r := range allReasons {
		dropped += sink.Dropped(r)
	}
	if got := sink.Written() + dropped; got != admitted {
		t.Fatalf("admitted %d, but written+dropped = %d", admitted, got)
	}
}

// fakeMemory reports a fixed amount of available memory. Only AvailableBytes
// is implemented: bindBudget calls nothing else.
type fakeMemory struct {
	protector.Memory
	available int64
}

func (m fakeMemory) AvailableBytes() int64 { return m.available }

// TestMemoryFlagsShapeTheAdaptiveBudget is the falsifying assertion for
// --logging-native-memory-fraction and --logging-native-memory-reserve.
// 1100 bytes available, a 100-byte reserve and a fraction of 0.5 give a budget
// of 500. The old constants (0.02 and 64 MiB) give 0.
func TestMemoryFlagsShapeTheAdaptiveBudget(t *testing.T) {
	cfg := &logger.NativeLogging{MaxBytes: 1 << 30, MemoryFraction: 0.5, MemoryReserve: 100}
	sink := NewSink(cfg)
	svc := NewService(sink, cfg, nil, nil, fakeMemory{available: 1100}, nil)

	svc.bindBudget()

	if got := sink.budgetBytes(); got != 500 {
		t.Fatalf("budget = %d, want 500 from the configured fraction and reserve", got)
	}
}
