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

package lifecycle

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// senderTestNode and senderTestGroup are the routing labels every test message
// carries; the metadata test asserts they round-trip onto the bus.Message.
const (
	senderTestNode    = "node-1"
	senderTestGroup   = "g1"
	senderTestMeasure = "m"
)

// capturingBatchPublisher records every published body. Because the sender
// reuses the underlying marshal buffers right after Publish returns (returnAll
// then borrow on the next batch), Publish must deep-decode each []byte body NOW
// and store a proto.Clone — reading the raw buffer later would observe a body
// the next batch has already overwritten. Metadata is captured per message too.
type capturingBatchPublisher struct {
	decoded   []*measurev1.InternalWriteRequest
	messages  []capturingMessageMeta
	mu        sync.Mutex
	publishes int
}

// capturingMessageMeta is the per-message routing/shape snapshot taken inside
// Publish, decoupled from the reusable buffer behind Data().
type capturingMessageMeta struct {
	node      string
	group     string
	isBytes   bool
	batchMode bool
}

// Publish decodes and clones every []byte body before returning so a captured
// body is immune to the sender's subsequent buffer reuse.
func (p *capturingBatchPublisher) Publish(_ context.Context, _ bus.Topic, messages ...bus.Message) (bus.Future, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.publishes++
	for i := range messages {
		meta := capturingMessageMeta{
			node:      messages[i].Node(),
			group:     messages[i].Group(),
			batchMode: messages[i].BatchModeEnabled(),
		}
		body, ok := messages[i].Data().([]byte)
		meta.isBytes = ok
		if !ok {
			return nil, fmt.Errorf("unexpected message payload %T", messages[i].Data())
		}
		iwr := &measurev1.InternalWriteRequest{}
		if err := proto.Unmarshal(body, iwr); err != nil {
			return nil, fmt.Errorf("unmarshal captured body: %w", err)
		}
		p.decoded = append(p.decoded, proto.Clone(iwr).(*measurev1.InternalWriteRequest))
		p.messages = append(p.messages, meta)
	}
	return nil, nil
}

// Close satisfies queue.BatchPublisher; the sender confirms each batch by
// closing its rotated-out publisher, which has nothing to report here.
func (p *capturingBatchPublisher) Close() (map[string]*common.Error, error) { return nil, nil }

// senderNewIWR builds a valid measure InternalWriteRequest whose Str tag body is
// strLen bytes, so a sequence of these spans several marshal-buffer size classes.
func senderNewIWR(ts time.Time, strLen int, fieldVal int64) *measurev1.InternalWriteRequest {
	wr := &measurev1.WriteRequest{
		Metadata: &commonv1.Metadata{Name: senderTestMeasure, Group: senderTestGroup},
		DataPoint: &measurev1.DataPointValue{
			Timestamp: timestamppb.New(ts),
			TagFamilies: []*modelv1.TagFamilyForWrite{{
				Tags: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: strings.Repeat("x", strLen)}}},
				},
			}},
			Fields: []*modelv1.FieldValue{
				{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: fieldVal}}},
			},
		},
	}
	return &measurev1.InternalWriteRequest{
		EntityValues: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: strings.Repeat("x", strLen)}}},
		},
		Request: wr,
	}
}

// senderMockClient wires a queue.MockClient that hands out the given publisher
// on every NewBatchPublisher call (the sender rotates a fresh publisher per
// flush, so it must be returnable repeatedly).
func senderMockClient(t *testing.T, pub queue.BatchPublisher) queue.Client {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	client := queue.NewMockClient(ctrl)
	client.EXPECT().NewBatchPublisher(gomock.Any()).
		DoAndReturn(func(time.Duration) queue.BatchPublisher { return pub }).AnyTimes()
	return client
}

// senderCursor is a synthetic rowCursor: it advances total times so replay's
// emit closure runs once per row, with no real underlying data.
type senderCursor struct {
	total int
	cur   int
}

func (c *senderCursor) Next() bool {
	if c.cur >= c.total {
		return false
	}
	c.cur++
	return true
}

func (c *senderCursor) Err() error { return nil }

func (c *senderCursor) Position() dump.Position {
	return dump.Position{BlockIdx: 0, RowIdx: c.cur - 1}
}

// TestBatchSender_BufferPoolReuseSafety drives many enqueues across different marshal
// size classes through a small row cap (so several flushes and thus buffer
// reuse rounds happen), then asserts every captured/cloned body still proto.Equals
// the source IWR it was built from. This proves returnAll-then-reuse never
// corrupts an already-published body.
func TestBatchSender_BufferPoolReuseSafety(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	pub := &capturingBatchPublisher{}
	client := senderMockClient(t, pub)
	sender := newBatchSender(client, bus.Topic{}, 2, 1<<30, time.Second)

	base := time.Now().UTC().Truncate(time.Millisecond)
	strLens := []int{1, 16, 200, 1024, 4, 512, 2, 900, 64, 1, 1500, 8}
	sources := make([]*measurev1.InternalWriteRequest, 0, len(strLens))
	for i, strLen := range strLens {
		iwr := senderNewIWR(base.Add(time.Duration(i)*time.Second), strLen, int64(i))
		sources = append(sources, proto.Clone(iwr).(*measurev1.InternalWriteRequest))
		require.NoError(t, sender.enqueueMarshaled(context.Background(), senderTestNode, senderTestGroup, iwr, proto.Size(iwr)))
	}
	require.NoError(t, sender.flush(context.Background()))
	require.NoError(t, sender.drain())

	require.Len(t, pub.decoded, len(sources), "every enqueued row must be published exactly once")
	for i := range sources {
		require.Truef(t, proto.Equal(sources[i], pub.decoded[i]),
			"captured body %d corrupted by buffer reuse:\n want=%v\n got =%v", i, sources[i], pub.decoded[i])
	}
}

// TestBatchSender_EnqueueMarshaledMetadata enqueues a single IWR with a known node and
// group, forces an immediate flush (maxRows=1) and asserts the captured
// bus.Message shape and routing labels.
func TestBatchSender_EnqueueMarshaledMetadata(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	pub := &capturingBatchPublisher{}
	client := senderMockClient(t, pub)
	sender := newBatchSender(client, bus.Topic{}, 1, 1<<30, time.Second)

	src := senderNewIWR(time.Now().UTC().Truncate(time.Millisecond), 32, 7)
	want := proto.Clone(src).(*measurev1.InternalWriteRequest)
	require.NoError(t, sender.enqueueMarshaled(context.Background(), senderTestNode, senderTestGroup, src, proto.Size(src)))
	require.NoError(t, sender.drain())

	require.Len(t, pub.messages, 1, "the row cap of 1 must flush a single-message batch")
	meta := pub.messages[0]
	require.True(t, meta.isBytes, "enqueueMarshaled payload must be []byte")
	require.True(t, proto.Equal(want, pub.decoded[0]), "decoded body must equal the source IWR")
	require.Equal(t, senderTestNode, meta.node, "Node() must carry the routing node")
	require.Equal(t, senderTestGroup, meta.group, "Group() must carry the routing group")
	require.True(t, meta.batchMode, "BatchModeEnabled() must be true")
}

// TestBatchSender_DualCapBatching exercises the row-count and byte-budget caps
// independently and asserts each attributes its flush to the right tally and
// resets inflightBytes.
func TestBatchSender_DualCapBatching(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	base := time.Now().UTC().Truncate(time.Millisecond)

	t.Run("row cap", func(t *testing.T) {
		pub := &capturingBatchPublisher{}
		client := senderMockClient(t, pub)
		sender := newBatchSender(client, bus.Topic{}, 3, 1<<30, time.Second)
		for i := 0; i < 7; i++ {
			iwr := senderNewIWR(base.Add(time.Duration(i)*time.Second), 8, int64(i))
			require.NoError(t, sender.enqueueMarshaled(context.Background(), senderTestNode, senderTestGroup, iwr, proto.Size(iwr)))
		}
		require.Equal(t, 2, sender.rowLimitFlushes, "7 rows at a cap of 3 flush twice on the row cap")
		require.Zero(t, sender.byteLimitFlushes, "the huge byte budget must never trigger a flush")
		require.Equal(t, 6, len(pub.decoded), "two row-cap flushes publish 6 rows")
		require.Len(t, sender.batch, 1, "the 7th row stays buffered until drain/flush")
		require.Positive(t, sender.inflightBytes, "the buffered 7th row's bytes are still in flight before the tail flush")
		require.NoError(t, sender.flush(context.Background()))
		require.Zero(t, sender.inflightBytes, "the trailing flush resets inflightBytes to 0")
		require.NoError(t, sender.drain())
		require.Equal(t, 7, len(pub.decoded), "the trailing flush delivers the 7th row")
	})

	t.Run("byte cap", func(t *testing.T) {
		pub := &capturingBatchPublisher{}
		client := senderMockClient(t, pub)
		// A single ~2KB body exceeds maxBytes, so every enqueue flushes on the byte
		// cap before the (huge) row cap is ever reached.
		sender := newBatchSender(client, bus.Topic{}, 1<<20, 1024, time.Second)
		for i := 0; i < 3; i++ {
			iwr := senderNewIWR(base.Add(time.Duration(i)*time.Second), 2048, int64(i))
			require.NoError(t, sender.enqueueMarshaled(context.Background(), senderTestNode, senderTestGroup, iwr, proto.Size(iwr)))
			require.Zero(t, sender.inflightBytes, "inflightBytes resets to 0 after every byte-cap flush")
		}
		require.Equal(t, 3, sender.byteLimitFlushes, "each large body crosses the byte budget and flushes")
		require.Zero(t, sender.rowLimitFlushes, "the huge row cap must never trigger a flush")
		require.NoError(t, sender.drain())
		require.Equal(t, 3, len(pub.decoded), "all three large rows are published")
	})
}

// TestBatchSender_ErrSkipSeriesSkip drives replay with an emit that returns an
// errSkipSeries-wrapped error for two rows and enqueues a real message for the
// rest. replay must not abort, must report no error, must tally exactly the two
// skipped rows, and must publish only the non-skipped rows.
func TestBatchSender_ErrSkipSeriesSkip(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	pub := &capturingBatchPublisher{}
	client := senderMockClient(t, pub)
	sender := newBatchSender(client, bus.Topic{}, 2, 1<<30, time.Second)

	base := time.Now().UTC().Truncate(time.Millisecond)
	const totalRows = 6
	cur := &senderCursor{total: totalRows}
	skip := map[int]bool{2: true, 4: true}
	row := 0
	emit := func() error {
		idx := row
		row++
		if skip[idx] {
			return fmt.Errorf("row %d cannot resolve entity: %w", idx, errSkipSeries)
		}
		iwr := senderNewIWR(base.Add(time.Duration(idx)*time.Second), 16, int64(idx))
		return sender.enqueueMarshaled(context.Background(), senderTestNode, senderTestGroup, iwr, proto.Size(iwr))
	}

	l := logger.GetLogger("sender-skip-test")
	rowCount, err := sender.replay(context.Background(), l, senderTestGroup, "part-0", nil, cur, emit)
	require.NoError(t, err, "errSkipSeries must not abort the part")
	require.Equal(t, totalRows-len(skip), rowCount, "replay counts only non-skipped rows")
	require.Equal(t, len(skip), sender.skippedRows, "each unresolvable series increments skippedRows")
	require.Equal(t, totalRows-len(skip), len(pub.decoded), "only the non-skipped rows are published")
}

// countingBatchPublisher is a queue.BatchPublisher fake that records how many
// times it was published to and closed, and how many messages each Publish
// carried. Close runs in the replayer's background confirmation goroutines, so
// the counters are atomic. closeCee injects a per-node delivery failure,
// publishErr injects a synchronous send failure, and panicOnClose makes Close
// panic to exercise the confirm goroutine's panic-safety.
type countingBatchPublisher struct {
	closeCee      map[string]*common.Error
	publishErr    error
	panicOnClose  bool
	publishCount  atomic.Int32
	publishedRows atomic.Int32
	closeCount    atomic.Int32
}

func (f *countingBatchPublisher) Publish(_ context.Context, _ bus.Topic, messages ...bus.Message) (bus.Future, error) {
	f.publishCount.Add(1)
	f.publishedRows.Add(int32(len(messages)))
	return nil, f.publishErr
}

func (f *countingBatchPublisher) Close() (map[string]*common.Error, error) {
	f.closeCount.Add(1)
	if f.panicOnClose {
		panic("simulated publisher close panic")
	}
	return f.closeCee, nil
}

// senderReturning wires a batchSender whose successive publishers are the given
// ones in order, then fresh clean publishers once the list is exhausted. It
// centralizes the mock-controller + NewBatchPublisher boilerplate and the
// "first publisher carries the first batch" rotation order for the sender tests.
// The returned accessor reports every publisher actually vended (provided and
// fresh) — including appends that happen after this returns — so a test can
// assert the exact number vended and inspect each, catching extra
// rotations/publishes that a pre-sized expectation would miss.
func senderReturning(t *testing.T, pubs ...*countingBatchPublisher) (*batchSender, func() []*countingBatchPublisher) {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	mockClient := queue.NewMockClient(ctrl)
	var vended []*countingBatchPublisher
	i := 0
	mockClient.EXPECT().NewBatchPublisher(measureReplayBatchTimeout).
		DoAndReturn(func(time.Duration) queue.BatchPublisher {
			var p *countingBatchPublisher
			if i < len(pubs) {
				p, i = pubs[i], i+1
			} else {
				p = &countingBatchPublisher{}
			}
			vended = append(vended, p)
			return p
		}).AnyTimes()
	return newBatchSender(mockClient, data.TopicMeasureWrite, measureReplayBatchSize, int(rowReplayMaxBatchBytes), measureReplayBatchTimeout),
		func() []*countingBatchPublisher { return vended }
}

// totalPublished sums the publish counts across the given publishers.
func totalPublished(pubs []*countingBatchPublisher) int32 {
	var total int32
	for _, p := range pubs {
		total += p.publishCount.Load()
	}
	return total
}

// totalPublishedRows sums the message counts actually handed to Publish across
// the given publishers, i.e. how many source rows were really sent.
func totalPublishedRows(pubs []*countingBatchPublisher) int32 {
	var total int32
	for _, p := range pubs {
		total += p.publishedRows.Load()
	}
	return total
}

// cleanEmit returns a replay emit callback that enqueues one dummy row per call.
func cleanEmit(s *batchSender) func() error {
	return func() error {
		return s.enqueue(context.TODO(), 0,
			bus.NewBatchMessageWithNode(bus.MessageID(1), "node-1", &measurev1.InternalWriteRequest{}))
	}
}

// failingEmit is cleanEmit that instead returns a build error on the failAt-th
// call, aborting the part with whatever was buffered/sent so far.
func failingEmit(s *batchSender, failAt int) func() error {
	clean := cleanEmit(s)
	calls := 0
	return func() error {
		calls++
		if calls == failAt {
			return fmt.Errorf("build failed")
		}
		return clean()
	}
}

// nodeErrCee extracts the per-node delivery errors carried by a failed replay
// error, failing the test if err is nil or not a *nodeReplayError.
func nodeErrCee(t *testing.T, err error) map[string]*common.Error {
	t.Helper()
	var nre *nodeReplayError
	require.ErrorAs(t, err, &nre)
	return nre.cee
}

// TestConfirmPipeline_BoundsInflightDepth proves the pipeline never keeps more
// than depth confirmations outstanding: a (depth+1)th add blocks until the
// oldest in-flight confirmation resolves, and drain surfaces the first failure.
func TestConfirmPipeline_BoundsInflightDepth(t *testing.T) {
	// rowReplayMaxInflight is 2, so a 3rd unconfirmed batch must wait.
	p := newConfirmPipeline()
	ch1 := make(chan error, 1)
	ch2 := make(chan error, 1)
	ch3 := make(chan error, 1)

	// Within depth: add must not block and has nothing to surface yet.
	require.NoError(t, p.add(ch1))
	require.NoError(t, p.add(ch2))

	// Full (2 in flight): the 3rd add must block until the oldest (ch1) resolves.
	added := make(chan error, 1)
	go func() { added <- p.add(ch3) }()
	select {
	case <-added:
		t.Fatal("add exceeded the in-flight depth without waiting for the oldest confirmation")
	case <-time.After(100 * time.Millisecond):
	}
	ch1 <- nil // oldest confirms cleanly
	select {
	case out := <-added:
		require.NoError(t, out, "a clean oldest confirmation must not report failure")
	case <-time.After(2 * time.Second):
		t.Fatal("add did not unblock after the oldest confirmation resolved")
	}

	// drain waits for the rest and returns the first failing error.
	ch2 <- nil
	ch3 <- newNodeReplayError(map[string]*common.Error{"node-1": common.NewError("boom")}, nil)
	require.Contains(t, nodeErrCee(t, p.drain()), "node-1")
}

// enqueueN feeds n messages through the sender, failing the test on any error
// surfaced early by the in-flight bound.
func enqueueN(t *testing.T, s *batchSender, n int) {
	t.Helper()
	emit := cleanEmit(s)
	for i := 0; i < n; i++ {
		require.NoError(t, emit())
	}
}

// TestBatchSender_SendsRotatesAndConfirmsEachBatch verifies the sender flow:
// every full batch is published once on its own publisher, the publisher is
// rotated for the next batch, and each sent batch is confirmed (closed) exactly
// once, with all confirmations drained before the part is considered durable.
func TestBatchSender_SendsRotatesAndConfirmsEachBatch(t *testing.T) {
	const batches = 3
	s, vended := senderReturning(t)
	enqueueN(t, s, batches*measureReplayBatchSize)
	require.NoError(t, s.drain())

	require.Len(t, vended(), batches+1, "exactly the initial publisher plus one rotation per sent batch")
	for i := 0; i < batches; i++ {
		require.Equalf(t, int32(1), vended()[i].publishCount.Load(), "publisher %d must send its batch once", i)
		require.Equalf(t, int32(1), vended()[i].closeCount.Load(), "publisher %d must be confirmed (closed) once", i)
	}
	require.Equal(t, int32(0), vended()[batches].publishCount.Load(), "the staged publisher holds no batch yet")
}

// TestBatchSender_SurfacesBatchNodeError verifies a per-node delivery failure on
// a confirmed batch propagates out of the sender (via the final drain) so the
// caller can mark the part errored and retry it on resume.
func TestBatchSender_SurfacesBatchNodeError(t *testing.T) {
	failing := &countingBatchPublisher{closeCee: map[string]*common.Error{"node-1": common.NewError("flush timeout")}}
	s, _ := senderReturning(t, failing) // the first publisher carries the batch and fails
	enqueueN(t, s, measureReplayBatchSize)
	require.Contains(t, nodeErrCee(t, s.drain()), "node-1",
		"a batch's per-node delivery error must surface when drained")
}

// fakeCursor yields n rows for the replay driver, with a zero Position. err, when
// set, is returned by Err() after the rows are exhausted to exercise the
// iterator-error path.
type fakeCursor struct {
	err error
	n   int
	i   int
}

func (c *fakeCursor) Next() bool              { c.i++; return c.i <= c.n }
func (c *fakeCursor) Err() error              { return c.err }
func (c *fakeCursor) Position() dump.Position { return dump.Position{} }

// TestBatchSender_SurfacesClosePanic pins the panic-safety of the confirm
// goroutine: if a publisher's Close panics, the goroutine must still deliver a
// (failed) error so the pipeline never deadlocks. Without the deferred
// recover+send this would hang (or crash), so the bounded wait turns a
// regression into a failed assertion rather than a hung test.
func TestBatchSender_SurfacesClosePanic(t *testing.T) {
	s, _ := senderReturning(t, &countingBatchPublisher{panicOnClose: true})
	enqueueN(t, s, measureReplayBatchSize) // one full batch on the panicking publisher

	done := make(chan error, 1)
	go func() { done <- s.drain() }()
	select {
	case out := <-done:
		require.ErrorContains(t, out, "panic", "a panicking Close must surface as an error")
	case <-time.After(2 * time.Second):
		t.Fatal("drain hung: the confirm goroutine did not deliver an error after Close panicked")
	}
}

// TestConfirmPipeline_DrainKeepsFirstFailure pins that drain returns the EARLIEST
// failing error, not a later one, when several in-flight batches fail.
func TestConfirmPipeline_DrainKeepsFirstFailure(t *testing.T) {
	p := newConfirmPipeline()
	ch1 := make(chan error, 1)
	ch2 := make(chan error, 1)
	require.NoError(t, p.add(ch1))
	require.NoError(t, p.add(ch2))

	ch1 <- fmt.Errorf("first failure")
	ch2 <- fmt.Errorf("second failure")
	require.ErrorContains(t, p.drain(), "first failure", "drain must keep the earliest failure")
}

// TestBatchSender_SurfacesFailureViaInflightBound pins the realistic mid-stream
// abort path: a batch's failure is surfaced through the in-flight bound's
// add-await (i.e. out of enqueue), not only at the final drain.
func TestBatchSender_SurfacesFailureViaInflightBound(t *testing.T) {
	failing := &countingBatchPublisher{closeCee: map[string]*common.Error{"node-1": common.NewError("boom")}}
	s, _ := senderReturning(t, failing) // batch 1 fails; later batches are clean

	emit := cleanEmit(s)
	var surfaced error
	for i := 0; i < 3*measureReplayBatchSize; i++ {
		if out := emit(); out != nil {
			surfaced = out
		}
	}
	require.Contains(t, nodeErrCee(t, surfaced), "node-1",
		"batch 1's failure must surface via the in-flight bound during enqueue, not only at drain")
	s.drain()
}

// TestReplay_RowCountBoundaries pins the driver's batching across small and large
// parts: for every part size the reported row count is exact, a successful part
// (even an empty one) counts once, the number of published batches matches the
// 2000-row boundary, and a subsequent close never publishes.
func TestReplay_RowCountBoundaries(t *testing.T) {
	cases := []struct {
		name        string
		rows        int
		wantBatches int
	}{
		{"empty", 0, 0},
		{"single_row", 1, 1},
		{"just_under_one_batch", measureReplayBatchSize - 1, 1},
		{"exactly_one_batch", measureReplayBatchSize, 1},
		{"one_over_one_batch", measureReplayBatchSize + 1, 2},
		{"exactly_two_batches", 2 * measureReplayBatchSize, 2},
		{"two_batches_and_a_tail", 2*measureReplayBatchSize + 5, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, vended := senderReturning(t)
			var counter uint64
			rows, err := s.replay(context.TODO(), logger.GetLogger("test-replayer"), "g", "p", &counter, &fakeCursor{n: tc.rows}, cleanEmit(s))
			require.NoError(t, err)
			require.Equal(t, tc.rows, rows, "rowCount must equal the source row count")
			require.Equal(t, uint64(1), counter, "every successful part counts once, even when empty")
			// Sum over EVERY vended publisher so an extra batch on a rotated-beyond
			// publisher is still counted, and pin the exact rotation count.
			require.Equal(t, int32(tc.wantBatches), totalPublished(vended()), "published batch count")
			require.Len(t, vended(), tc.wantBatches+1, "exactly one publisher per batch plus the staged one")
			// Every source row must reach Publish exactly once: no row dropped at a
			// batch boundary and none lost in the trailing tail.
			require.Equal(t, int32(tc.rows), totalPublishedRows(vended()), "every source row must be published exactly once")

			_, cerr := s.close()
			require.NoError(t, cerr)
			require.Equal(t, int32(tc.wantBatches), totalPublished(vended()), "close must not publish after a successful part")
		})
	}
}

// TestReplay_BatchPlusTailSendsEveryRow pins the "a few rows past a flush
// boundary" case: with two full batches plus a short tail, each full batch
// carries exactly batchSize messages, the tail batch carries the remainder, the
// staged publisher carries nothing, and the grand total equals the row count —
// so no row is dropped at a boundary nor lost in the trailing tail.
func TestReplay_BatchPlusTailSendsEveryRow(t *testing.T) {
	const tail = 5
	const rows = 2*measureReplayBatchSize + tail
	s, vended := senderReturning(t)
	var counter uint64
	got, err := s.replay(context.TODO(), logger.GetLogger("test-replayer"), "g", "p", &counter, &fakeCursor{n: rows}, cleanEmit(s))
	require.NoError(t, err)
	require.Equal(t, rows, got, "rowCount must equal the source row count")

	pubs := vended()
	require.Len(t, pubs, 4, "two full batches + the tail batch + the staged publisher")
	require.Equal(t, int32(measureReplayBatchSize), pubs[0].publishedRows.Load(), "first full batch carries batchSize rows")
	require.Equal(t, int32(measureReplayBatchSize), pubs[1].publishedRows.Load(), "second full batch carries batchSize rows")
	require.Equal(t, int32(tail), pubs[2].publishedRows.Load(), "the trailing batch carries exactly the tail")
	require.Equal(t, int32(0), pubs[3].publishedRows.Load(), "the staged publisher holds nothing")
	require.Equal(t, int32(rows), totalPublishedRows(pubs), "every row sent exactly once")

	_, cerr := s.close()
	require.NoError(t, cerr)
}

// TestReplay_AbortDrainsInflightAndDiscards pins the abort path across small and
// large parts and at the batch boundary: whatever full batches were already sent
// are still confirmed (drained), the un-flushed residual is discarded (never
// resurrected by a later close), the failed part is not counted, and no row of an
// aborted part is published beyond the batches that left before the failure.
func TestReplay_AbortDrainsInflightAndDiscards(t *testing.T) {
	cases := []struct {
		name            string
		failAt          int // the 1-based emit call that returns a build error
		wantRows        int // rows counted before the failure
		wantSentBatches int // full batches flushed before the failure
	}{
		{"first_row_nothing_buffered", 1, 0, 0},
		{"small_residual_no_batch", 2, 1, 0},
		{"empty_residual_at_batch_boundary", measureReplayBatchSize + 1, measureReplayBatchSize, 1},
		{"residual_after_two_batches", 2*measureReplayBatchSize + 501, 2*measureReplayBatchSize + 500, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, vended := senderReturning(t)
			var counter uint64
			rows, err := s.replay(context.TODO(), logger.GetLogger("test-replayer"), "g", "p", &counter, &fakeCursor{n: tc.failAt}, failingEmit(s, tc.failAt))
			require.Error(t, err, "the part must report failure")
			require.Equal(t, tc.wantRows, rows, "rowCount counts the rows enqueued before the failure")
			require.Zero(t, counter, "a failed part must not be counted")

			require.Len(t, vended(), tc.wantSentBatches+1, "one publisher per sent batch plus the staged one")
			for i := 0; i < tc.wantSentBatches; i++ {
				require.Equalf(t, int32(1), vended()[i].closeCount.Load(), "sent batch %d must be confirmed (drained)", i)
			}
			require.Equal(t, int32(tc.wantSentBatches), totalPublished(vended()), "only the batches sent before the failure are published")
			require.Nil(t, s.take(), "the residual must be discarded")
			_, cerr := s.close()
			require.NoError(t, cerr)
		})
	}
}

// TestReplay_IteratorErrorDrainsAndDiscards covers the distinct abort branch where
// the source iterator fails after yielding rows (cur.Err() != nil). The already-
// sent batches must still be drained, the residual discarded, and the part failed
// — so a truncated part is never committed as durable.
func TestReplay_IteratorErrorDrainsAndDiscards(t *testing.T) {
	s, vended := senderReturning(t)
	const n = 2*measureReplayBatchSize + 300 // two full batches sent, 300 buffered, then the iterator errors
	var counter uint64
	rows, err := s.replay(context.TODO(), logger.GetLogger("test-replayer"), "g", "p", &counter,
		&fakeCursor{n: n, err: fmt.Errorf("iterator boom")}, cleanEmit(s))
	require.ErrorContains(t, err, "iterator boom", "an iterator error must fail the part")
	require.Equal(t, n, rows, "all rows read before the iterator error are counted")
	require.Zero(t, counter, "a failed part must not be counted")

	require.Len(t, vended(), 3, "two sent batches plus the staged one")
	require.Equal(t, int32(1), vended()[0].closeCount.Load(), "sent batch 0 must be drained")
	require.Equal(t, int32(1), vended()[1].closeCount.Load(), "sent batch 1 must be drained")
	require.Equal(t, int32(0), vended()[2].publishCount.Load(), "the residual must not be published")
	require.Nil(t, s.take(), "the residual must be discarded")
}

// TestReplay_NodeErrorViaInflightBoundDrivesAbort covers the composition of a
// per-node confirm failure surfacing through the in-flight bound's add-await
// while driven by replay (not raw enqueue): the part aborts mid-scan, in-flight
// batches drain, and the residual is discarded.
func TestReplay_NodeErrorViaInflightBoundDrivesAbort(t *testing.T) {
	failing := &countingBatchPublisher{closeCee: map[string]*common.Error{"node-1": common.NewError("boom")}}
	s, _ := senderReturning(t, failing) // batch 1's confirm fails
	var counter uint64
	rows, err := s.replay(context.TODO(), logger.GetLogger("test-replayer"), "g", "p", &counter,
		&fakeCursor{n: 3 * measureReplayBatchSize}, cleanEmit(s))
	require.Contains(t, nodeErrCee(t, err), "node-1", "the node error must abort the part")
	require.Zero(t, counter, "a failed part must not be counted")
	// Batch 1's failure surfaces only once a later batch's flush awaits it at the
	// in-flight bound, so the part aborts mid-scan: after more than one batch's
	// worth of rows but before consuming all of them.
	require.Greater(t, rows, measureReplayBatchSize, "aborted after more than one batch")
	require.Less(t, rows, 3*measureReplayBatchSize, "aborted mid-scan, not after consuming every row")
	require.Nil(t, s.take(), "the residual must be discarded")
	_, cerr := s.close()
	require.NoError(t, cerr)
}

// TestRecordReplayNodeErrors_ReportsPartialFailure pins the partial-failure
// report: the per-node delivery errors a failed batch carries are recorded into
// the progress report keyed by node (even through fmt wrapping), while a
// global-only failure and a plain non-nodeReplayError add no node entries — so
// the resume report reflects exactly which nodes rejected rows and nothing else.
func TestRecordReplayNodeErrors_ReportsPartialFailure(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	l := logger.GetLogger("test")

	t.Run("per_node_errors_recorded", func(t *testing.T) {
		p := NewProgress(filepath.Join(t.TempDir(), "progress.json"), l)
		err := newNodeReplayError(map[string]*common.Error{
			"node-2": common.NewError("flush timeout"),
			"node-5": common.NewError("shard rejected"),
		}, nil)
		recordReplayNodeErrors(p, "g", "hot", "warm", catalogMeasure, err)
		// One node-scoped MigrationError per failing node; non-failing nodes absent.
		require.Len(t, p.MigrationErrors, 2, "exactly the failing nodes are reported")
		nodes := map[string]string{}
		for i := range p.MigrationErrors {
			e := p.MigrationErrors[i]
			require.Equal(t, scopeNode, e.Scope)
			require.Equal(t, catalogMeasure, e.Catalog)
			require.Equal(t, "g", e.Group)
			require.Equal(t, "hot", e.SourceStage)
			require.Equal(t, "warm", e.TargetStage)
			require.Nil(t, e.Shard)
			require.Nil(t, e.Part)
			nodes[e.Node] = e.Error
		}
		require.Contains(t, nodes["node-2"], "flush timeout")
		require.Contains(t, nodes["node-5"], "shard rejected")
		require.NotContains(t, nodes, "node-1", "a node that did not fail must not be reported")
	})

	t.Run("wrapped_error_still_unwrapped", func(t *testing.T) {
		p := NewProgress(filepath.Join(t.TempDir(), "progress.json"), l)
		inner := newNodeReplayError(map[string]*common.Error{"node-7": common.NewError("boom")}, nil)
		recordReplayNodeErrors(p, "g", "hot", "warm", catalogMeasure, fmt.Errorf("row-replay measure part p: %w", inner))
		require.Len(t, p.MigrationErrors, 1, "errors.As must find node errors through fmt wrapping")
		require.Equal(t, "node-7", p.MigrationErrors[0].Node)
	})

	t.Run("global_only_error_records_nothing", func(t *testing.T) {
		p := NewProgress(filepath.Join(t.TempDir(), "progress.json"), l)
		recordReplayNodeErrors(p, "g", "hot", "warm", catalogMeasure, newNodeReplayError(nil, fmt.Errorf("stream broke")))
		require.Empty(t, p.MigrationErrors, "a global-only failure must not add node entries")
	})

	t.Run("plain_error_records_nothing", func(t *testing.T) {
		p := NewProgress(filepath.Join(t.TempDir(), "progress.json"), l)
		recordReplayNodeErrors(p, "g", "hot", "warm", catalogMeasure, fmt.Errorf("open part failed"))
		require.Empty(t, p.MigrationErrors, "a non-nodeReplayError must not be reported as node failures")
	})
}

// TestBatchSender_EachBatchGetsItsOwnTimeoutWindow pins the fix for the original
// migration timeout bug: every batch is published on its OWN publisher opened
// with a fresh copy of the configured timeout, so the window is per-batch. A slow
// gap between two sends can never eat into the next batch's window the way a
// single long-lived stream's lifetime once did. A 5s-timeout sender sends two
// full batches; we assert a fresh NewBatchPublisher(5s) backs each batch (initial
// publisher + one rotation per batch), every call uses the full 5s (not a shrunk
// or shared window), and the two batches ride distinct publishers.
func TestBatchSender_EachBatchGetsItsOwnTimeoutWindow(t *testing.T) {
	const (
		timeout   = 5 * time.Second
		batchSize = 2
	)
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	mockClient := queue.NewMockClient(ctrl)
	var timeouts []time.Duration
	var pubs []*countingBatchPublisher
	mockClient.EXPECT().NewBatchPublisher(gomock.Any()).
		DoAndReturn(func(d time.Duration) queue.BatchPublisher {
			timeouts = append(timeouts, d)
			p := &countingBatchPublisher{}
			pubs = append(pubs, p)
			return p
		}).AnyTimes()

	s := newBatchSender(mockClient, data.TopicMeasureWrite, batchSize, int(rowReplayMaxBatchBytes), timeout)
	for i := 0; i < 2*batchSize; i++ { // two full batches
		require.NoError(t, s.enqueue(context.TODO(), 0,
			bus.NewBatchMessageWithNode(bus.MessageID(1), "node-1", &measurev1.InternalWriteRequest{})))
	}
	require.NoError(t, s.drain())

	// Initial publisher + one rotation per sent batch = 3 NewBatchPublisher calls,
	// and EVERY one must request the full configured timeout (a fresh 5s window),
	// proving the two sends do not share one stream's single 5s lifetime.
	require.Len(t, timeouts, 3, "each batch must open its own publisher, not share one")
	for i, d := range timeouts {
		require.Equalf(t, timeout, d, "publisher %d must open with the full configured timeout, not a shared/shrunk one", i)
	}
	require.Len(t, pubs, 3)
	require.Equal(t, int32(1), pubs[0].publishCount.Load(), "batch 1 rides its own publisher")
	require.Equal(t, int32(1), pubs[1].publishCount.Load(), "batch 2 rides a different publisher")
	require.Equal(t, int32(0), pubs[2].publishCount.Load(), "the staged publisher holds nothing")
	require.NotSame(t, pubs[0], pubs[1], "the two batches must not share a publisher (nor its timeout window)")

	_, cerr := s.close()
	require.NoError(t, cerr)
}

// TestNodeReplayError_ErrorAndUnwrap pins the error type directly: its two
// message forms (global cause vs per-node count) and that Unwrap exposes the
// global cause so errors.Is/As chains reach it even through fmt wrapping.
func TestNodeReplayError_ErrorAndUnwrap(t *testing.T) {
	// Global-error form: Error() is the wrapped cause, and Unwrap exposes it.
	cause := fmt.Errorf("publish failed")
	globalErr := newNodeReplayError(nil, cause)
	require.EqualError(t, globalErr, "publish failed")
	require.ErrorIs(t, globalErr, cause, "Unwrap must expose the global cause to errors.Is")
	require.ErrorIs(t, fmt.Errorf("row-replay measure part p: %w", globalErr), cause,
		"the global cause must stay reachable through fmt wrapping")

	// Node-error form (cee only, no global err): Error() reports the node count
	// and there is no global cause to unwrap.
	nodeErr := newNodeReplayError(map[string]*common.Error{
		"node-1": common.NewError("boom"),
		"node-2": common.NewError("boom"),
	}, nil)
	require.EqualError(t, nodeErr, "2 node error(s)")
	require.NotErrorIs(t, nodeErr, cause, "a node-only error has no global cause to unwrap")
}

// TestBatchSender_CloseSurfacesDrainedFailure covers close()'s failure branch:
// when an in-flight batch failed, close() drains it and returns that batch's
// per-node errors instead of the idle publisher's clean result.
func TestBatchSender_CloseSurfacesDrainedFailure(t *testing.T) {
	failing := &countingBatchPublisher{closeCee: map[string]*common.Error{"node-9": common.NewError("boom")}}
	s, _ := senderReturning(t, failing)    // the first publisher carries the batch and fails
	enqueueN(t, s, measureReplayBatchSize) // one full batch sent, left in-flight (undrained)
	cee, err := s.close()                  // close() must drain it and surface the node error
	require.NoError(t, err, "a node-only failure carries no global error")
	require.Contains(t, cee, "node-9", "close must return the drained batch's per-node errors")
}

// TestReplay_SurfacesSynchronousPublishErrorPromptly pins that a synchronous
// Publish failure aborts the part on the flush that triggered it — not deferred
// until the in-flight bound or the final drain. With the failing publisher
// carrying the first batch, replay must stop right after that batch instead of
// scanning ~depth more batches: the old code returned the error only via the
// bound, so it would have consumed ~3 batches before aborting.
func TestReplay_SurfacesSynchronousPublishErrorPromptly(t *testing.T) {
	pub := &countingBatchPublisher{publishErr: fmt.Errorf("stream send broke")}
	s, _ := senderReturning(t, pub) // the first batch's Publish fails synchronously
	var counter uint64
	rows, err := s.replay(context.TODO(), logger.GetLogger("test-replayer"), "g", "p", &counter,
		&fakeCursor{n: 3 * measureReplayBatchSize}, cleanEmit(s))
	require.ErrorContains(t, err, "stream send broke", "a synchronous Publish error must abort the part")
	require.Zero(t, counter, "a failed part must not be counted")
	// Row 2000's emit triggers the failing flush and returns its error, so the loop
	// breaks before counting it: exactly one batch worth minus the failing row, far
	// short of the 3 batches the deferred (bound-only) behavior would have scanned.
	require.Equal(t, measureReplayBatchSize-1, rows, "replay must abort on the failing flush, not keep scanning")
	require.Nil(t, s.take(), "the residual must be discarded")
	_, cerr := s.close()
	require.NoError(t, cerr)
}
