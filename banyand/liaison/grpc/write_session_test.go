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

package grpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// fakeBatch is one publisher attempt as seen by the fake queue.
type fakeBatch struct {
	node     string
	payloads [][]byte
}

// fakePublishers hands out publishers that record what was published and answer Close as the test scripts.
type fakePublishers struct {
	onClose func(b *fakeBatch) (map[string]*common.Error, error)
	batches []*fakeBatch
	mu      sync.Mutex
}

func (f *fakePublishers) newPublisher() queue.BatchPublisher {
	return &fakePublisher{owner: f, batch: &fakeBatch{}}
}

func (f *fakePublishers) attempts() []*fakeBatch {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*fakeBatch(nil), f.batches...)
}

type fakePublisher struct {
	owner *fakePublishers
	batch *fakeBatch
}

func (p *fakePublisher) Publish(_ context.Context, _ bus.Topic, messages ...bus.Message) (bus.Future, error) {
	for _, m := range messages {
		p.batch.node = m.Node()
		p.batch.payloads = append(p.batch.payloads, m.Data().([]byte))
	}
	return nil, nil
}

func (p *fakePublisher) Close() (map[string]*common.Error, error) {
	p.owner.mu.Lock()
	p.owner.batches = append(p.owner.batches, p.batch)
	hook := p.owner.onClose
	p.owner.mu.Unlock()
	if hook == nil {
		return nil, nil
	}
	return hook(p.batch)
}

type replyCollector struct {
	replies []writeReply
	mu      sync.Mutex
}

func (c *replyCollector) send(reply writeReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.replies = append(c.replies, reply)
	return nil
}

func (c *replyCollector) snapshot() []writeReply {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]writeReply(nil), c.replies...)
}

func newTestSession(cfg writeSessionConfig, f *fakePublishers, c *replyCollector) *writeSession {
	return newWriteSession(context.Background(), cfg, writeSessionDeps{
		newPublisher: f.newPublisher,
		send:         c.send,
		log:          logger.GetLogger("test"),
		metrics:      newBypassMetrics(),
		service:      "trace",
		topic:        data.TopicTraceWrite,
	})
}

var testWriteMetadata = &commonv1.Metadata{Group: "g", Name: "t"}

// appendRow appends a padded row identified by version.
func appendRow(s *writeSession, node string, version uint64, size int) error {
	return s.Append(node, "g", 1, writeAck{metadata: testWriteMetadata, id: version}, func(bool) ([]byte, error) {
		payload := make([]byte, size)
		copy(payload, fmt.Sprintf("v%d", version))
		return payload, nil
	})
}

func requireExactlyOnce(t *testing.T, replies []writeReply, total int, want modelv1.Status) {
	t.Helper()
	require.Len(t, replies, total)
	seen := make(map[uint64]int, total)
	for _, r := range replies {
		seen[r.id]++
		assert.Equal(t, want, r.status)
	}
	for v := 1; v <= total; v++ {
		require.Equal(t, 1, seen[uint64(v)], "version %d", v)
	}
}

func TestWriteSessionSealByRows(t *testing.T) {
	f, c := &fakePublishers{}, &replyCollector{}
	s := newTestSession(writeSessionConfig{sealRows: 3, sealInterval: time.Hour}, f, c)
	for v := 1; v <= 7; v++ {
		require.NoError(t, appendRow(s, "n1", uint64(v), 10))
	}
	require.NoError(t, s.Finish())
	var sizes []int
	for _, b := range f.attempts() {
		sizes = append(sizes, len(b.payloads))
	}
	assert.ElementsMatch(t, []int{3, 3, 1}, sizes)
	requireExactlyOnce(t, c.snapshot(), 7, modelv1.Status_STATUS_SUCCEED)
}

func TestWriteSessionSealByBytes(t *testing.T) {
	f, c := &fakePublishers{}, &replyCollector{}
	s := newTestSession(writeSessionConfig{sealRows: 1000, sealBytes: 250, sealInterval: time.Hour}, f, c)
	for v := 1; v <= 7; v++ {
		require.NoError(t, appendRow(s, "n1", uint64(v), 100))
	}
	// Rows are 100 bytes: the third row crosses 250 bytes, so batches hold 3, 3 and 1 rows.
	require.Eventually(t, func() bool { return len(f.attempts()) == 2 }, 2*time.Second, 5*time.Millisecond)
	require.NoError(t, s.Finish())
	var sizes []int
	for _, b := range f.attempts() {
		sizes = append(sizes, len(b.payloads))
	}
	assert.ElementsMatch(t, []int{3, 3, 1}, sizes)
	requireExactlyOnce(t, c.snapshot(), 7, modelv1.Status_STATUS_SUCCEED)
}

func TestWriteSessionSealByInterval(t *testing.T) {
	f, c := &fakePublishers{}, &replyCollector{}
	s := newTestSession(writeSessionConfig{sealRows: 1000, sealInterval: 30 * time.Millisecond}, f, c)
	require.NoError(t, appendRow(s, "n1", 1, 10))
	require.NoError(t, appendRow(s, "n1", 2, 10))
	// No further rows and no half-close: the interval alone must seal the batch and acknowledge it.
	require.Eventually(t, func() bool { return len(c.snapshot()) == 2 }, 2*time.Second, 5*time.Millisecond)
	require.Len(t, f.attempts(), 1)
	assert.Len(t, f.attempts()[0].payloads, 2)
	require.NoError(t, s.Finish())
	requireExactlyOnce(t, c.snapshot(), 2, modelv1.Status_STATUS_SUCCEED)
}

func TestWriteSessionAcknowledgementOrderAndExactlyOnce(t *testing.T) {
	f, c := &fakePublishers{}, &replyCollector{}
	s := newTestSession(writeSessionConfig{sealRows: 5, sealInterval: time.Hour}, f, c)
	const total = 200
	for v := 1; v <= total; v++ {
		require.NoError(t, appendRow(s, fmt.Sprintf("n%d", v%3), uint64(v), 10))
	}
	require.NoError(t, s.Finish())
	replies := c.snapshot()
	requireExactlyOnce(t, replies, total, modelv1.Status_STATUS_SUCCEED)
	// The rows of a batch are published and acknowledged in the order they were appended.
	position := make(map[uint64]int, total)
	for i, r := range replies {
		position[r.id] = i
	}
	for _, b := range f.attempts() {
		prevVersion, prevPos := uint64(0), -1
		for _, payload := range b.payloads {
			var version uint64
			_, err := fmt.Sscanf(string(bytes.TrimRight(payload, "\x00")), "v%d", &version)
			require.NoError(t, err)
			assert.Greater(t, version, prevVersion)
			assert.Greater(t, position[version], prevPos)
			prevVersion, prevPos = version, position[version]
		}
	}
}

func TestWriteSessionEpochSwitchSealsOpenBatch(t *testing.T) {
	f, c := &fakePublishers{}, &replyCollector{}
	s := newTestSession(writeSessionConfig{sealRows: 100, sealInterval: time.Hour}, f, c)
	var firstFlags []bool
	for v, epoch := range []uint64{1, 1, 2, 2} {
		err := s.Append("n1", "g", epoch, writeAck{metadata: testWriteMetadata, id: uint64(v + 1)}, func(first bool) ([]byte, error) {
			firstFlags = append(firstFlags, first)
			return []byte("row"), nil
		})
		require.NoError(t, err)
	}
	require.NoError(t, s.Finish())
	assert.Equal(t, []bool{true, false, true, false}, firstFlags, "a switch opens a new batch whose first row carries the metadata")
	require.Len(t, f.attempts(), 2)
	requireExactlyOnce(t, c.snapshot(), 4, modelv1.Status_STATUS_SUCCEED)
}

func TestWriteSessionInflightLimitBlocksAppendUntilAck(t *testing.T) {
	f, c := &fakePublishers{}, &replyCollector{}
	release := make(chan struct{})
	f.onClose = func(b *fakeBatch) (map[string]*common.Error, error) {
		// Only n1's batches are held, so a single release frees exactly one n1 slot.
		if b.node == "n1" {
			<-release
		}
		return nil, nil
	}
	s := newTestSession(writeSessionConfig{sealRows: 1, maxInflight: 2, sealInterval: time.Hour}, f, c)
	require.NoError(t, appendRow(s, "n1", 1, 10))
	require.NoError(t, appendRow(s, "n1", 2, 10))
	// Another node is unaffected by n1's limit.
	require.NoError(t, appendRow(s, "n2", 100, 10))
	third := make(chan error, 1)
	go func() { third <- appendRow(s, "n1", 3, 10) }()
	select {
	case err := <-third:
		t.Fatalf("append returned %v while the node had its maximum of unacknowledged batches", err)
	case <-time.After(150 * time.Millisecond):
	}
	for _, r := range c.snapshot() {
		assert.GreaterOrEqual(t, r.id, uint64(100), "n1 acknowledges nothing while it holds the batches")
	}
	release <- struct{}{}
	select {
	case err := <-third:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("append stayed blocked after a batch was acknowledged")
	}
	close(release)
	require.NoError(t, s.Finish())
	replies := c.snapshot()
	require.Len(t, replies, 4)
}

func TestWriteSessionBusyRetryThenSuccess(t *testing.T) {
	f, c := &fakePublishers{}, &replyCollector{}
	busy := map[string]*common.Error{"n1": common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, "busy")}
	var closes int
	f.onClose = func(*fakeBatch) (map[string]*common.Error, error) {
		closes++
		if closes <= 2 {
			return busy, nil
		}
		return nil, nil
	}
	s := newTestSession(writeSessionConfig{
		sealRows: 2, sealInterval: time.Hour, pressureTimeout: 10 * time.Second,
		retryBackoff: 200 * time.Millisecond, retryBackoffMax: 200 * time.Millisecond,
	}, f, c)
	require.NoError(t, appendRow(s, "n1", 1, 10))
	require.NoError(t, appendRow(s, "n1", 2, 10))
	// While the batch backs off, an acknowledgement sent meanwhile asks the client to slow down.
	require.Eventually(t, func() bool {
		s.Reply(writeReply{metadata: testWriteMetadata, id: 1000, status: modelv1.Status_STATUS_NOT_FOUND})
		for _, r := range c.snapshot() {
			if r.id == 1000 && r.retryAfterMs > 0 {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, s.Finish())
	attempts := f.attempts()
	require.Len(t, attempts, 3)
	for _, a := range attempts[1:] {
		assert.Equal(t, attempts[0].payloads, a.payloads, "a retry replays the session's own copy of the rows")
	}
	var batch []writeReply
	for _, r := range c.snapshot() {
		if r.id == 1 || r.id == 2 {
			batch = append(batch, r)
		}
	}
	require.Len(t, batch, 2)
	for _, r := range batch {
		assert.Equal(t, modelv1.Status_STATUS_SUCCEED, r.status)
	}
}

func TestWriteSessionPressureTimeoutEndsStream(t *testing.T) {
	f, c := &fakePublishers{}, &replyCollector{}
	f.onClose = func(*fakeBatch) (map[string]*common.Error, error) {
		return map[string]*common.Error{"n1": common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "full")}, nil
	}
	s := newTestSession(writeSessionConfig{
		sealRows: 2, sealInterval: time.Hour, pressureTimeout: 150 * time.Millisecond,
		retryBackoff: 20 * time.Millisecond, retryBackoffMax: 20 * time.Millisecond,
	}, f, c)
	// Two rows seal and keep failing; the third sits in the open batch.
	require.NoError(t, appendRow(s, "n1", 1, 10))
	require.NoError(t, appendRow(s, "n1", 2, 10))
	require.NoError(t, appendRow(s, "n1", 3, 10))
	select {
	case <-s.Fatal():
	case <-time.After(5 * time.Second):
		t.Fatal("the session did not fail after the pressure timeout")
	}
	err := appendRow(s, "n1", 4, 10)
	require.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	finishErr := s.Finish()
	require.Error(t, finishErr)
	assert.Equal(t, codes.ResourceExhausted, status.Code(finishErr))
	requireExactlyOnce(t, c.snapshot(), 3, modelv1.Status_STATUS_SERVER_BUSY)
	assert.Greater(t, len(f.attempts()), 1, "the batch was retried before giving up")
	for _, a := range f.attempts() {
		assert.Len(t, a.payloads, 2, "the open batch was never sent")
	}
}

func TestWriteSessionUnknownOutcomeIsNotReplayed(t *testing.T) {
	for name, hook := range map[string]func(*fakeBatch) (map[string]*common.Error, error){
		"close error": func(*fakeBatch) (map[string]*common.Error, error) { return nil, errors.New("stream broke") },
		"internal error": func(*fakeBatch) (map[string]*common.Error, error) {
			return map[string]*common.Error{"n1": common.NewErrorWithStatus(modelv1.Status_STATUS_INTERNAL_ERROR, "recv")}, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			f, c := &fakePublishers{onClose: hook}, &replyCollector{}
			s := newTestSession(writeSessionConfig{sealRows: 2, sealInterval: time.Hour, retryBackoff: time.Millisecond}, f, c)
			require.NoError(t, appendRow(s, "n1", 1, 10))
			require.NoError(t, appendRow(s, "n1", 2, 10))
			require.NoError(t, s.Finish())
			assert.Len(t, f.attempts(), 1, "an unknown outcome is never replayed")
			requireExactlyOnce(t, c.snapshot(), 2, modelv1.Status_STATUS_INTERNAL_ERROR)
		})
	}
}

func TestReplyQueueNeverBlocksPush(t *testing.T) {
	block := make(chan struct{})
	q := newReplyQueue(context.Background(), func(writeReply) error { <-block; return nil }, logger.GetLogger("test"))
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100000; i++ {
			q.push(writeReply{id: uint64(i)})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("push blocked on a sender that is stuck")
	}
	close(block)
	q.closeAndWait()
}

// traceWriteHarness is an in-process gRPC server running the real trace Write handler against fake data nodes.
type traceWriteHarness struct {
	client  tracev1.TraceServiceClient
	fake    *fakePublishers
	service *traceService
	conn    *grpclib.ClientConn
}

type fakeNodeRegistry struct{}

func (fakeNodeRegistry) Locate(_, _ string, shardID, _ uint32) (string, error) {
	return fmt.Sprintf("node-%d", shardID%2), nil
}

func (fakeNodeRegistry) LocateAll(string, uint32, int) ([]string, error) { return nil, nil }
func (fakeNodeRegistry) String() string                                  { return "fake" }

type fakePipeline struct {
	queue.Client
	fake *fakePublishers
}

func (p fakePipeline) NewBatchPublisher(time.Duration) queue.BatchPublisher {
	return p.fake.newPublisher()
}

func newTraceWriteHarness(t *testing.T, cfg writeSessionConfig) *traceWriteHarness {
	t.Helper()
	id := identity{group: "g", name: "t"}
	er := newEmptyEntityRepo()
	er.traceMap[id] = &databasev1.Trace{
		Metadata:         &commonv1.Metadata{Group: "g", Name: "t"},
		TraceIdTagName:   "trace_id",
		TimestampTagName: "ts",
	}
	gr := &groupRepo{
		resourceOpts: map[string]*commonv1.ResourceOpts{"g": {ShardNum: 4}},
		inflight:     make(map[string]*groupInflight),
	}
	fake := &fakePublishers{}
	svc := &traceService{
		discoveryService: &discoveryService{entityRepo: er, groupRepo: gr, nodeRegistry: fakeNodeRegistry{}},
		pipeline:         fakePipeline{fake: fake},
		l:                logger.GetLogger("test"),
		metrics:          newBypassMetrics(),
		writeConfig:      &cfg,
	}
	const window = 65535
	srv := grpclib.NewServer(grpclib.InitialWindowSize(window), grpclib.InitialConnWindowSize(window))
	tracev1.RegisterTraceServiceServer(srv, svc)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	conn, err := grpclib.NewClient(lis.Addr().String(), grpclib.WithTransportCredentials(insecure.NewCredentials()),
		grpclib.WithInitialWindowSize(window), grpclib.WithInitialConnWindowSize(window))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})
	return &traceWriteHarness{client: tracev1.NewTraceServiceClient(conn), fake: fake, service: svc, conn: conn}
}

func traceRow(version uint64, withMetadata bool, spec *tracev1.TagSpec) *tracev1.WriteRequest {
	req := &tracev1.WriteRequest{
		Version: version,
		Tags: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("trace-%d", version)}}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(time.Now().Truncate(time.Millisecond))}},
		},
		Span:    []byte("span"),
		TagSpec: spec,
	}
	if withMetadata {
		req.Metadata = &commonv1.Metadata{Group: "g", Name: "t"}
	}
	return req
}

func TestTraceWriteSendThenReadClientDoesNotDeadlock(t *testing.T) {
	const total = 200000
	h := newTraceWriteHarness(t, writeSessionConfig{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	spec := &tracev1.TagSpec{TagNames: []string{"trace_id", "ts"}}
	start := time.Now()
	for v := uint64(1); v <= total; v++ {
		var rowSpec *tracev1.TagSpec
		if v == 1 {
			rowSpec = spec
		}
		require.NoError(t, stream.Send(traceRow(v, v == 1, rowSpec)))
	}
	require.NoError(t, stream.CloseSend())
	t.Logf("sent %d rows before reading in %s", total, time.Since(start))
	seen := make(map[uint64]struct{}, total)
	for {
		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		require.NoError(t, recvErr)
		assert.Equal(t, modelv1.Status_STATUS_SUCCEED, resp.GetStatusCode())
		assert.Equal(t, modelv1.Status_STATUS_SUCCEED.String(), resp.GetStatus())
		seen[resp.GetVersion()] = struct{}{}
	}
	assert.Len(t, seen, total)
	rows := 0
	for _, b := range h.fake.attempts() {
		rows += len(b.payloads)
	}
	assert.Equal(t, total, rows)
}

func TestTraceWriteMetadataSwitchLandsOnNextBatch(t *testing.T) {
	h := newTraceWriteHarness(t, writeSessionConfig{sealInterval: time.Hour})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	specA := &tracev1.TagSpec{TagNames: []string{"trace_id", "ts"}}
	specB := &tracev1.TagSpec{TagNames: []string{"ts", "trace_id"}}
	// Rows 1-2 use specA; row 3 switches to specB, whose column order differs, so it cannot share a batch with them.
	// Every row has the same trace ID so that all rows land on the same node.
	rowWith := func(version uint64, spec *tracev1.TagSpec, withMetadata bool, swap bool) *tracev1.WriteRequest {
		req := traceRow(version, withMetadata, spec)
		req.Tags[0] = &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "same-trace"}}}
		if swap {
			req.Tags[0], req.Tags[1] = req.Tags[1], req.Tags[0]
		}
		return req
	}
	require.NoError(t, stream.Send(rowWith(1, specA, true, false)))
	require.NoError(t, stream.Send(rowWith(2, nil, false, false)))
	require.NoError(t, stream.Send(rowWith(3, specB, false, true)))
	require.NoError(t, stream.Send(rowWith(4, nil, false, true)))
	require.NoError(t, stream.CloseSend())
	for i := 0; i < 4; i++ {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		assert.Equal(t, modelv1.Status_STATUS_SUCCEED, resp.GetStatusCode())
	}
	_, recvErr := stream.Recv()
	require.ErrorIs(t, recvErr, io.EOF)

	batches := h.fake.attempts()
	require.Len(t, batches, 2)
	first := func(b *fakeBatch) *tracev1.WriteRequest {
		iwr := &tracev1.InternalWriteRequest{}
		require.NoError(t, proto.Unmarshal(b.payloads[0], iwr))
		return iwr.GetRequest()
	}
	for _, b := range batches {
		require.Len(t, b.payloads, 2)
		req := first(b)
		assert.Equal(t, "g", req.GetMetadata().GetGroup(), "every batch carries the metadata on its first message")
		later := &tracev1.InternalWriteRequest{}
		require.NoError(t, proto.Unmarshal(b.payloads[1], later))
		assert.Nil(t, later.GetRequest().GetMetadata())
		assert.Nil(t, later.GetRequest().GetTagSpec())
	}
	specs := map[string]uint64{}
	for _, b := range batches {
		req := first(b)
		specs[fmt.Sprint(req.GetTagSpec().GetTagNames())] = req.GetVersion()
	}
	assert.Equal(t, uint64(1), specs["[trace_id ts]"])
	assert.Equal(t, uint64(3), specs["[ts trace_id]"])
}

func TestTraceWriteRepeatedMetadataAndSpecShareOneBatch(t *testing.T) {
	h := newTraceWriteHarness(t, writeSessionConfig{sealInterval: time.Hour})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	// Like many clients, send the same metadata and tag spec on every message; all
	// rows go to one node, so they must stay in one batch.
	spec := &tracev1.TagSpec{TagNames: []string{"trace_id", "ts"}}
	const rows = 10
	for v := uint64(1); v <= rows; v++ {
		req := traceRow(v, true, &tracev1.TagSpec{TagNames: spec.GetTagNames()})
		req.Tags[0] = &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "same-trace"}}}
		require.NoError(t, stream.Send(req))
	}
	require.NoError(t, stream.CloseSend())
	for i := 0; i < rows; i++ {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		assert.Equal(t, modelv1.Status_STATUS_SUCCEED, resp.GetStatusCode())
	}
	_, recvErr := stream.Recv()
	require.ErrorIs(t, recvErr, io.EOF)
	batches := h.fake.attempts()
	require.Len(t, batches, 1, "repeating identical metadata and spec must not seal the batch")
	require.Len(t, batches[0].payloads, rows)
}

func TestTraceWritePressureTimeoutEndsStreamWithResourceExhausted(t *testing.T) {
	h := newTraceWriteHarness(t, writeSessionConfig{
		sealRows: 2, pressureTimeout: 200 * time.Millisecond, retryBackoff: 20 * time.Millisecond, retryBackoffMax: 20 * time.Millisecond,
	})
	h.fake.onClose = func(b *fakeBatch) (map[string]*common.Error, error) {
		return map[string]*common.Error{b.node: common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, "busy")}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	spec := &tracev1.TagSpec{TagNames: []string{"trace_id", "ts"}}
	for v := uint64(1); v <= 2; v++ {
		row := traceRow(v, v == 1, nil)
		row.Tags[0] = &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "same-trace"}}}
		if v == 1 {
			row.TagSpec = spec
		}
		require.NoError(t, stream.Send(row))
	}
	// The client keeps the stream open and only reads: the server must end it by itself.
	var got []*tracev1.WriteResponse
	for {
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			assert.Equal(t, codes.ResourceExhausted, status.Code(recvErr))
			break
		}
		got = append(got, resp)
	}
	require.Len(t, got, 2)
	for _, resp := range got {
		assert.Equal(t, modelv1.Status_STATUS_SERVER_BUSY, resp.GetStatusCode())
	}
}

// togglePressureProtector reports high memory pressure while high is set.
type togglePressureProtector struct {
	protector.Nop
	high atomic.Bool
}

func (p *togglePressureProtector) State() protector.State {
	if p.high.Load() {
		return protector.StateHigh
	}
	return protector.StateLow
}

func TestTraceWriteLiaisonMemoryPressureSetsRetryAfterWhileStreamStaysOpen(t *testing.T) {
	h := newTraceWriteHarness(t, writeSessionConfig{sealRows: 2, sealInterval: time.Hour})
	pm := &togglePressureProtector{}
	h.service.protector = pm
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	spec := &tracev1.TagSpec{TagNames: []string{"trace_id", "ts"}}
	// Every row has the same trace ID so that both rows of a pair land in one batch on one node.
	// The first row of a pair carries the metadata and spec; metadata is sticky, so the second continues the batch.
	sendRow := func(version uint64, first bool) {
		var rowSpec *tracev1.TagSpec
		if first {
			rowSpec = spec
		}
		row := traceRow(version, first, rowSpec)
		row.Tags[0] = &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "same-trace"}}}
		require.NoError(t, stream.Send(row))
	}
	// A row for an unknown trace is rejected on its own, so it exercises the per-row reply path.
	sendMissing := func(version uint64) {
		row := traceRow(version, false, nil)
		row.Metadata = &commonv1.Metadata{Group: "g", Name: "missing"}
		require.NoError(t, stream.Send(row))
	}
	recv := func() *tracev1.WriteResponse {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		return resp
	}
	recvByVersion := func(versions ...uint64) map[uint64]*tracev1.WriteResponse {
		got := make(map[uint64]*tracev1.WriteResponse, len(versions))
		for range versions {
			resp := recv()
			got[resp.GetVersion()] = resp
		}
		for _, v := range versions {
			require.Contains(t, got, v)
		}
		return got
	}
	wantRetryAfter := uint32(writeMemoryHighRetryAfter.Milliseconds())

	pm.high.Store(true)
	sendMissing(1)
	row := recv()
	assert.Equal(t, modelv1.Status_STATUS_NOT_FOUND, row.GetStatusCode())
	assert.Equal(t, wantRetryAfter, row.GetRetryAfterMs(), "a per-row reply asks the client to slow down under pressure")
	sendRow(2, true)
	sendRow(3, false)
	for _, resp := range recvByVersion(2, 3) {
		assert.Equal(t, modelv1.Status_STATUS_SUCCEED, resp.GetStatusCode(), "pressure does not change the status of an acknowledgement")
		assert.Equal(t, wantRetryAfter, resp.GetRetryAfterMs(), "a batch acknowledgement asks the client to slow down under pressure")
	}

	pm.high.Store(false)
	sendMissing(4)
	row = recv()
	assert.Equal(t, modelv1.Status_STATUS_NOT_FOUND, row.GetStatusCode())
	assert.Zero(t, row.GetRetryAfterMs(), "a per-row reply stops asking once pressure clears")
	sendRow(5, true)
	sendRow(6, false)
	for _, resp := range recvByVersion(5, 6) {
		assert.Equal(t, modelv1.Status_STATUS_SUCCEED, resp.GetStatusCode())
		assert.Zero(t, resp.GetRetryAfterMs(), "a batch acknowledgement stops asking once pressure clears")
	}

	// The stream was never ended by the pressure: it still ends cleanly on half-close.
	require.NoError(t, stream.CloseSend())
	_, recvErr := stream.Recv()
	require.ErrorIs(t, recvErr, io.EOF)
}
