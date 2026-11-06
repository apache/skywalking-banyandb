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
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcmetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// producerIdentity is the identity every producer test stamps.
var producerIdentity = NodeInfo{NodeID: "backup-0", NodeType: "backup"}

// testProducer starts the delivery of a producer that writes to dest, with
// the intervals of testService. adjust sets the buffer's bounds over those.
func testProducer(t *testing.T, dest *destination, adjust ...func(*queue.BufferedConfig)) *Sink {
	t.Helper()
	sink := newSink(func(c *queue.BufferedConfig) {
		c.FlushInterval = 20 * time.Millisecond
		c.BatchCount = 5
		for _, a := range adjust {
			a(c)
		}
	})
	svc := &Service{
		sink:   sink,
		cfg:    &logger.NativeLogging{Enabled: true, Level: "debug"},
		closer: run.NewCloser(1),
		timing: defaultTiming,
		node:   producerIdentity,
		dest:   dest,
		l:      logger.GetLogger("native-log-test"),
	}
	svc.schema.Store(schemaReady)
	sink.SetNode(producerIdentity)
	sink.pub.Start(svc.delivery())
	t.Cleanup(sink.pub.Close)
	return sink
}

// clusterDestination is the cluster server route over client, as
// newClusterDestination builds it around its own pub client.
func clusterDestination(client queue.Client) *destination {
	return &destination{
		delivery: queue.NewNodeDelivery(client, data.TopicStreamWrite, fixedNode(client), time.Second),
		start:    func() {},
		stop:     func() {},
	}
}

// TestProducerWritesToItsDataNode covers the cluster server route: every
// message goes to the one node the producer's client holds, carries the
// producer's own identity, is marked as a native write, and is counted as
// written.
func TestProducerWritesToItsDataNode(t *testing.T) {
	client := &fakeClient{}
	client.healthy.Store(true)
	sink := testProducer(t, clusterDestination(client))

	const admitted = 7
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "BACKUP", []byte(sampleLine))
	}
	waitFor(t, "every event to be written", func() bool { return sink.Written() == admitted })

	for i, node := range client.seenMessageNodes() {
		if node != destinationNodeName {
			t.Fatalf("message %d is addressed to %q, want the producer's data node", i, node)
		}
	}
	ids := client.seenNodeIDs()
	if len(ids) != admitted {
		t.Fatalf("published %d elements, want %d", len(ids), admitted)
	}
	for i, id := range ids {
		if id != producerIdentity.NodeID {
			t.Fatalf("element %d carries node_id %q, want the producer's own", i, id)
		}
	}
	if got, _ := client.markedModules.Load().(string); got != "NATIVE-LOG.STREAM" {
		t.Fatalf("the data node would log this batch under %q, want NATIVE-LOG.STREAM", got)
	}
}

// TestProducerHoldsItsLogsUntilItsDataNodeIsBack covers a producer whose data
// node is not connected yet: nothing is published while there is no
// connection, and the held events are written once there is one, without a
// restart.
func TestProducerHoldsItsLogsUntilItsDataNodeIsBack(t *testing.T) {
	client := &fakeClient{}
	sink := testProducer(t, clusterDestination(client))

	const admitted = 5
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "BACKUP", []byte(sampleLine))
	}
	time.Sleep(100 * time.Millisecond)
	if got := client.published.Load(); got != 0 {
		t.Fatalf("published %d events with no connection", got)
	}

	client.healthy.Store(true)
	waitFor(t, "the held events to be written", func() bool { return sink.Written() == admitted })
	if got := sink.Dropped(reasonDestinationUnready); got != 0 {
		t.Fatalf("%d events counted as destination_unready though the node came back in time", got)
	}
}

// TestProducerCountsWhatNoConnectionTook covers a data node that never comes
// back: after the bounded retries the events are counted as
// destination_unready, never as written.
func TestProducerCountsWhatNoConnectionTook(t *testing.T) {
	client := &fakeClient{}
	sink := testProducer(t, clusterDestination(client))

	const admitted = 5
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "BACKUP", []byte(sampleLine))
	}
	waitFor(t, "the events to be counted as destination_unready", func() bool {
		return sink.Dropped(reasonDestinationUnready) == admitted
	})
	if got := client.published.Load(); got != 0 {
		t.Fatalf("published %d events with no connection", got)
	}
	if got := sink.Written(); got != 0 {
		t.Fatalf("%d events counted as written with no connection", got)
	}
	waitSettled(t, sink)
}

// fakeLiaison is a liaison's public StreamService: it records each write and
// the credentials it came with, and answers each event with the status set
// for its message.
type fakeLiaison struct {
	streamv1.UnimplementedStreamServiceServer
	refuse map[string]modelv1.Status
	deny   error
	got    []*streamv1.WriteRequest
	creds  []string
	mu     sync.Mutex
}

func (f *fakeLiaison) Write(stream streamv1.StreamService_WriteServer) error {
	md, _ := grpcmetadata.FromIncomingContext(stream.Context())
	f.mu.Lock()
	f.creds = append(f.creds, strings.Join(md.Get("username"), ",")+":"+strings.Join(md.Get("password"), ","))
	f.mu.Unlock()
	if f.deny != nil {
		return f.deny
	}
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		f.mu.Lock()
		f.got = append(f.got, req)
		f.mu.Unlock()
		st := modelv1.Status_STATUS_SUCCEED
		if refused, ok := f.refuse[req.GetElement().GetTagFamilies()[0].GetTags()[6].GetStr().GetValue()]; ok {
			st = refused
		}
		if err = stream.Send(&streamv1.WriteResponse{MessageId: req.GetMessageId(), StatusCode: st, Status: st.String()}); err != nil {
			return err
		}
	}
}

func (f *fakeLiaison) writes() []*streamv1.WriteRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*streamv1.WriteRequest(nil), f.got...)
}

// serveLiaison serves f on lis until the test ends.
func serveLiaison(t *testing.T, f *fakeLiaison, lis net.Listener) {
	t.Helper()
	srv := grpc.NewServer()
	streamv1.RegisterStreamServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
}

// publicDestinationTo is the public route to addr, with credentials.
func publicDestinationTo(t *testing.T, addr string) *destination {
	t.Helper()
	dest, err := newPublicDestination(Destination{Addr: addr, Public: true, Username: "fodc", Password: "secret"}, time.Second)
	if err != nil {
		t.Fatalf("newPublicDestination: %v", err)
	}
	t.Cleanup(dest.stop)
	return dest
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return lis
}

func messageOf(m string) []byte { return []byte(`{"level":"info","message":"` + m + `"}`) }

// TestProducerWritesThroughTheLiaisonAPI covers the public route: every event
// is a write to the log stream with the producer's identity and a message ID
// of its own, sent with the producer's credentials, and counted as written
// once the liaison accepted it.
func TestProducerWritesThroughTheLiaisonAPI(t *testing.T) {
	liaison := &fakeLiaison{}
	lis := listen(t)
	serveLiaison(t, liaison, lis)
	sink := testProducer(t, publicDestinationTo(t, lis.Addr().String()))

	const admitted = 7
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "FODC", messageOf("m"))
	}
	waitFor(t, "every event to be written", func() bool { return sink.Written() == admitted })

	writes := liaison.writes()
	if len(writes) != admitted {
		t.Fatalf("the liaison received %d writes, want %d", len(writes), admitted)
	}
	ids := map[uint64]bool{}
	for i, w := range writes {
		if w.GetMetadata().GetGroup() != GroupName || w.GetMetadata().GetName() != StreamName {
			t.Fatalf("write %d is to %s/%s, want the log stream", i, w.GetMetadata().GetGroup(), w.GetMetadata().GetName())
		}
		if got := w.GetElement().GetTagFamilies()[0].GetTags()[0].GetStr().GetValue(); got != producerIdentity.NodeID {
			t.Fatalf("write %d carries node_id %q, want the producer's own", i, got)
		}
		if w.GetMessageId() == 0 || ids[w.GetMessageId()] {
			t.Fatalf("write %d has message ID %d, which is 0 or already used", i, w.GetMessageId())
		}
		ids[w.GetMessageId()] = true
	}
	for _, c := range liaison.creds {
		if c != "fodc:secret" {
			t.Fatalf("a write came with credentials %q, want the producer's", c)
		}
	}
}

// TestLiaisonRefusalIsCountedPerEvent covers a liaison that answers some
// events with a refusal: each refused event is counted under its own reason,
// a missing stream as schema_unavailable, and the rest of the batch as written.
func TestLiaisonRefusalIsCountedPerEvent(t *testing.T) {
	liaison := &fakeLiaison{refuse: map[string]modelv1.Status{
		"no-stream": modelv1.Status_STATUS_NOT_FOUND,
		"broken":    modelv1.Status_STATUS_INTERNAL_ERROR,
	}}
	lis := listen(t)
	serveLiaison(t, liaison, lis)
	sink := testProducer(t, publicDestinationTo(t, lis.Addr().String()))

	for _, m := range []string{"ok", "no-stream", "ok", "broken", "no-stream"} {
		sink.Admit(zerolog.InfoLevel, "FODC", messageOf(m))
	}
	waitFor(t, "every event to be settled", func() bool {
		return sink.Written()+sink.Dropped(reasonSchemaMissing)+sink.Dropped(reasonPublishFailed) == 5
	})
	if got := sink.Written(); got != 2 {
		t.Errorf("written = %d, want the 2 accepted events", got)
	}
	if got := sink.Dropped(reasonSchemaMissing); got != 2 {
		t.Errorf("schema_unavailable = %d, want the 2 events refused for a missing stream", got)
	}
	if got := sink.Dropped(reasonPublishFailed); got != 1 {
		t.Errorf("publish_failed = %d, want the 1 event refused otherwise", got)
	}
}

// TestRefusedCredentialsArePublishFailed covers a liaison whose RBAC refuses
// the producer: nothing is written, the batch is counted as publish_failed,
// and it is not retried, because waiting does not change the credentials.
func TestRefusedCredentialsArePublishFailed(t *testing.T) {
	liaison := &fakeLiaison{deny: status.Error(codes.PermissionDenied, "permission denied")}
	lis := listen(t)
	serveLiaison(t, liaison, lis)
	sink := testProducer(t, publicDestinationTo(t, lis.Addr().String()))

	const admitted = 3
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "FODC", messageOf("m"))
	}
	waitFor(t, "the batch to be counted as publish_failed", func() bool {
		return sink.Dropped(reasonPublishFailed) == admitted
	})
	if got := sink.Written(); got != 0 {
		t.Fatalf("%d events counted as written though the liaison refused the credentials", got)
	}
}

// TestProducerRetriesALiaisonNotUpYet covers a liaison that is not listening
// yet: the batch is held, nothing is lost, and it is written once the liaison
// is up.
func TestProducerRetriesALiaisonNotUpYet(t *testing.T) {
	// Take a free port, and leave it closed until the liaison starts.
	reserved := listen(t)
	addr := reserved.Addr().String()
	_ = reserved.Close()
	// gRPC waits at least a second before it dials a refused address again,
	// so the retries have to outlast that; in production they span ten
	// seconds.
	sink := testProducer(t, publicDestinationTo(t, addr), func(c *queue.BufferedConfig) { c.MaxRetries = 250 })

	const admitted = 3
	for i := 0; i < admitted; i++ {
		sink.Admit(zerolog.InfoLevel, "FODC", messageOf("m"))
	}
	time.Sleep(60 * time.Millisecond)
	if got := sink.Written(); got != 0 {
		t.Fatalf("%d events counted as written with no liaison", got)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("the reserved port was taken meanwhile: %v", err)
	}
	liaison := &fakeLiaison{}
	serveLiaison(t, liaison, lis)
	waitFor(t, "the held events to be written", func() bool { return sink.Written() == admitted })
	if got := sink.Dropped(reasonDestinationUnready); got != 0 {
		t.Fatalf("%d events counted as destination_unready though the liaison came up in time", got)
	}
}

// TestNewProducerIsOffUntilEnabled covers the default: a producer that does
// not enable native logging gets no service, and one that enables it with no
// address to write to is refused at startup rather than silently dropping.
func TestNewProducerIsOffUntilEnabled(t *testing.T) {
	svc, err := NewProducer(ProducerConfig{Native: &logger.NativeLogging{}, Destination: Destination{Addr: "127.0.0.1:17912"}})
	if err != nil || svc != nil {
		t.Fatalf("NewProducer with native logging off = (%v, %v), want (nil, nil)", svc, err)
	}
	if _, err = NewProducer(ProducerConfig{Native: &logger.NativeLogging{Enabled: true}}); err == nil {
		t.Fatal("NewProducer accepted native logging with no address to write to")
	}
}

// producerWithGauges serves a producer whose destination never takes a batch,
// so every event ends as a drop, and stops it.
func producerWithGauges(t *testing.T, gauges Gauges) {
	t.Helper()
	sink := newSink(func(c *queue.BufferedConfig) {
		c.FlushInterval = 20 * time.Millisecond
		c.MaxRetries = 1
	})
	svc := &Service{
		sink:   sink,
		cfg:    &logger.NativeLogging{Enabled: true, Level: "debug"},
		closer: run.NewCloser(1),
		timing: timing{reportInterval: 10 * time.Millisecond, schemaRetry: time.Hour, writeTimeout: time.Second},
		node:   producerIdentity,
		dest:   clusterDestination(&fakeClient{}),
		gauges: gauges,
	}
	svc.schema.Store(schemaReady)
	if err := svc.PreRun(context.Background()); err != nil {
		t.Fatalf("PreRun: %v", err)
	}
	svc.Serve()
	sink.Admit(zerolog.InfoLevel, "BACKUP", []byte(sampleLine))
	waitFor(t, "the event to be dropped", func() bool { return sink.Dropped(reasonDestinationUnready) == 1 })
	time.Sleep(50 * time.Millisecond)
	svc.GracefulStop()
}

// TestProducerPublishesItsCountersThroughItsGauges covers a producer with a
// metrics endpoint, such as an FODC agent: its counters reach the gauges it
// was given.
func TestProducerPublishesItsCountersThroughItsGauges(t *testing.T) {
	gauges := map[string]*fakeGauge{}
	var mu sync.Mutex
	factory := func(name string, _ ...string) meter.Gauge {
		mu.Lock()
		defer mu.Unlock()
		g := &fakeGauge{}
		gauges[name] = g
		return g
	}
	producerWithGauges(t, factory)

	mu.Lock()
	dropped := gauges["native_log_dropped_total"]
	mu.Unlock()
	if dropped == nil {
		t.Fatal("no native_log_dropped_total gauge was created")
	}
	dropped.mu.Lock()
	defer dropped.mu.Unlock()
	if got := dropped.vals[reasonDestinationUnready]; got != 1 {
		t.Fatalf("native_log_dropped_total{reason=%q} = %v, want 1", reasonDestinationUnready, got)
	}
}

// TestProducerWithoutGaugesReportsItsDropsAtExit covers a producer with no
// metrics endpoint, such as backup: its loss is written to stderr once, at
// exit, and only when there was one.
func TestProducerWithoutGaugesReportsItsDropsAtExit(t *testing.T) {
	var out bytes.Buffer
	prev := reportTo
	reportTo = &out
	t.Cleanup(func() { reportTo = prev })

	producerWithGauges(t, nil)

	if got := out.String(); !strings.Contains(got, "native log events dropped: "+reasonDestinationUnready+"=1") {
		t.Fatalf("stderr at exit = %q, want the dropped count", got)
	}
}

// TestReceiverOnlyNodeOwnsTheSchemaAndCollectsNothing covers a node with only
// --logging-native-receiver-enabled: it keeps the schema for the producers
// that write to it, and collects nothing of its own.
func TestReceiverOnlyNodeOwnsTheSchemaAndCollectsNothing(t *testing.T) {
	ctrl := gomock.NewController(t)
	groups := schema.NewMockGroup(ctrl)
	streams := schema.NewMockStream(ctrl)
	groups.EXPECT().GetGroup(gomock.Any(), GroupName).Return(&commonv1.Group{
		Metadata:     &commonv1.Metadata{Name: GroupName},
		ResourceOpts: &commonv1.ResourceOpts{ShardNum: 1, Ttl: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7}},
	}, nil).MinTimes(1)
	streams.EXPECT().GetStream(gomock.Any(), gomock.Any()).Return(streamSpec(), nil).MinTimes(1)

	client := &fakeClient{}
	sink := NewSink()
	svc := NewService(sink, &logger.NativeLogging{ReceiverEnabled: true, TTLDays: 7}, repoWith(ctrl, groups, streams), client, nil, nil)
	if err := svc.PreRun(context.Background()); err != nil {
		t.Fatalf("PreRun: %v", err)
	}
	svc.Serve()
	t.Cleanup(svc.GracefulStop)

	if got := svc.schema.Load(); got != schemaReady {
		t.Fatalf("schema state = %d, want ready: a receiver owns the schema", got)
	}
	sink.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))
	time.Sleep(50 * time.Millisecond)
	if got := client.published.Load(); got != 0 {
		t.Fatalf("a receiver-only node published %d events of its own", got)
	}
}

// TestNodeWithNeitherFlagTouchesNoSchema is the falsifier of the receiver
// test: with neither flag the node makes no metadata call at all. The mocks
// have no expectations, so any call fails the test.
func TestNodeWithNeitherFlagTouchesNoSchema(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := repoWith(ctrl, schema.NewMockGroup(ctrl), schema.NewMockStream(ctrl))
	svc := NewService(NewSink(), &logger.NativeLogging{TTLDays: 7}, repo, &fakeClient{}, nil, nil)
	if err := svc.PreRun(context.Background()); err != nil {
		t.Fatalf("PreRun: %v", err)
	}
	svc.Serve()
	svc.GracefulStop()
	if got := svc.schema.Load(); got == schemaReady {
		t.Fatal("a node with neither flag established the schema")
	}
}
