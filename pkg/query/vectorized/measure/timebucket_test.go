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

package measure

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// TestBucketStart_BoundaryTimestamp_StaysPut pins design §11: a timestamp
// exactly on a bucket boundary (ts % width == 0) floors to itself.
func TestBucketStart_BoundaryTimestamp_StaysPut(t *testing.T) {
	if got := bucketStart(5000, 1000); got != 5000 {
		t.Fatalf("bucketStart(5000, 1000) = %d, want 5000", got)
	}
}

// TestBucketStart_AgreesWithGetWindowStart_ForNonNegativeTimestamps pins
// agreement with pkg/flow/streaming/sliding_window.go:302 getWindowStart
// ("ts - ts%width") across the entire reachable domain (timestamps are Unix
// milliseconds; the write path rejects non-positive ones) — keeping the
// query path and the TopN pre-aggregation path from drifting into two
// definitions of "the 5-minute bucket".
func TestBucketStart_AgreesWithGetWindowStart_ForNonNegativeTimestamps(t *testing.T) {
	cases := []struct{ ts, width int64 }{
		{0, 1000},
		{1, 1000},
		{999, 1000},
		{1000, 1000},
		{1001, 1000},
		{5999, 1000},
		{6000, 1000},
		{123456789, 60000},
	}
	for _, c := range cases {
		got := bucketStart(c.ts, c.width)
		want := c.ts - c.ts%c.width // getWindowStart's own formula
		if got != want {
			t.Errorf("bucketStart(%d, %d) = %d, want %d (getWindowStart)", c.ts, c.width, got, want)
		}
	}
}

// TestBucketStart_NegativeTimestamp_FloorsRatherThanTruncates pins design
// §7.2's explicit correction: Go's % truncates toward zero, which would
// skew getWindowStart's plain formula by one bucket for ts < 0 — negative
// timestamps are unreachable in practice, but bucketStart floors correctly
// regardless of a caller's invariant. -1 belongs to the bucket [-1000, 0),
// so its floor is -1000, not 0 (what a naive "ts - ts%width" port yields).
func TestBucketStart_NegativeTimestamp_FloorsRatherThanTruncates(t *testing.T) {
	if got := bucketStart(-1, 1000); got != -1000 {
		t.Fatalf("bucketStart(-1, 1000) = %d, want -1000 (floor, not truncate-toward-zero)", got)
	}
	if got := bucketStart(-1000, 1000); got != -1000 {
		t.Fatalf("bucketStart(-1000, 1000) = %d, want -1000 (boundary stays put)", got)
	}
}

// bucketTestRow is one input row for bucketTestSchema / feedBucketBatch.
type bucketTestRow struct {
	g  string
	ts int64
	v  int64
}

// bucketTestSchema is "timestamp (int64), tag.default.g (string), field v
// (int64)" — a minimal scan-shaped schema for BatchTimeBucket tests.
func bucketTestSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
}

func buildBucketBatch(schema *vectorized.BatchSchema, rows ...bucketTestRow) *vectorized.RecordBatch {
	b := vectorized.NewRecordBatch(schema, len(rows))
	tsCol := b.Columns[0].(*vectorized.TypedColumn[int64])
	gCol := b.Columns[1].(*vectorized.TypedColumn[string])
	vCol := b.Columns[2].(*vectorized.TypedColumn[int64])
	for _, r := range rows {
		tsCol.Append(r.ts)
		gCol.Append(r.g)
		vCol.Append(r.v)
	}
	b.Len = len(rows)
	return b
}

// fakeBucketUpstream is a minimal vectorized.PullOperator serving a fixed
// batch sequence, standing in for the Scan chain BatchTimeBucket wraps in
// production.
type fakeBucketUpstream struct {
	schema  *vectorized.BatchSchema
	batches []*vectorized.RecordBatch
	idx     int
}

func (f *fakeBucketUpstream) Init(_ context.Context) error          { return nil }
func (f *fakeBucketUpstream) OutputSchema() *vectorized.BatchSchema { return f.schema }
func (f *fakeBucketUpstream) Close() error                          { return nil }
func (f *fakeBucketUpstream) NextBatch(_ context.Context) (*vectorized.RecordBatch, error) {
	if f.idx >= len(f.batches) {
		return nil, nil
	}
	b := f.batches[f.idx]
	f.idx++
	return b, nil
}

// sumByBucketAndTag reads a drained batch shaped [timestamp, g, v(sum)] into
// a map keyed "bucket|tag" for order-independent assertions.
func sumByBucketAndTag(t *testing.T, batches []*vectorized.RecordBatch) map[string]int64 {
	t.Helper()
	out := make(map[string]int64)
	for _, b := range batches {
		tsCol := b.Columns[0].(*vectorized.TypedColumn[int64])
		gCol := b.Columns[1].(*vectorized.TypedColumn[string])
		vCol := b.Columns[2].(*vectorized.TypedColumn[int64])
		for i := 0; i < b.Len; i++ {
			key := formatBucketKey(tsCol.Data()[i], gCol.Data()[i])
			out[key] = vCol.Data()[i]
		}
	}
	return out
}

func formatBucketKey(ts int64, g string) string {
	return g + "|" + strconv.FormatInt(ts, 10)
}

func drainBucket(t *testing.T, bt *BatchTimeBucket) []*vectorized.RecordBatch {
	t.Helper()
	var out []*vectorized.RecordBatch
	for {
		nb, err := bt.NextBatch(context.Background())
		if err != nil {
			t.Fatalf("NextBatch: %v", err)
		}
		if nb == nil {
			break
		}
		out = append(out, nb)
	}
	return out
}

// TestBatchTimeBucket_Streaming_GroupsPerBucketPerTag pins the core
// streaming shape end to end, through the public PullOperator interface:
// rows across two buckets and two tag groups are summed per (bucket, tag),
// and the output timestamp column carries the bucket start (design §7.2's
// conditional D2 reversal).
func TestBatchTimeBucket_Streaming_GroupsPerBucketPerTag(t *testing.T) {
	s := bucketTestSchema()
	keyIndices := []int{1} // tag "g"
	spec := []AggSpec{{Func: AggSum, InputCol: 2, Output: "v"}}
	batch := buildBucketBatch(s,
		bucketTestRow{"a", 100, 1},  // bucket 0
		bucketTestRow{"b", 200, 4},  // bucket 0
		bucketTestRow{"a", 300, 2},  // bucket 0
		bucketTestRow{"a", 1100, 3}, // bucket 1000
		bucketTestRow{"b", 1200, 5}, // bucket 1000
	)
	upstream := &fakeBucketUpstream{schema: s, batches: []*vectorized.RecordBatch{batch}}
	bt := NewBatchTimeBucket(upstream, s, keyIndices, 0, 1000, spec, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0, true)
	if initErr := bt.Init(context.Background()); initErr != nil {
		t.Fatal(initErr)
	}
	defer bt.Close()

	got := sumByBucketAndTag(t, drainBucket(t, bt))
	want := map[string]int64{"a|0": 3, "b|0": 4, "a|1000": 3, "b|1000": 5}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("sum[%s] = %d, want %d (full result: %v)", k, got[k], v, got)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("result set size = %d, want %d: %v", len(got), len(want), got)
	}
}

// TestBatchTimeBucket_Streaming_NextBatch_ReturnsClosedBucketBeforeUpstreamEOF
// pins the pipelining property (design §7.2): the first NextBatch call
// returns bucket 0's output as soon as a later row in the SAME upstream
// batch advances past it — without ever asking upstream for more input, let
// alone reaching its EOF. Only the second NextBatch call, which does drive
// upstream to EOF, flushes the still-open final bucket. The operator never
// buffers more than what closed while processing one upstream batch.
func TestBatchTimeBucket_Streaming_NextBatch_ReturnsClosedBucketBeforeUpstreamEOF(t *testing.T) {
	s := bucketTestSchema()
	spec := []AggSpec{{Func: AggSum, InputCol: 2, Output: "v"}}
	batch := buildBucketBatch(s,
		bucketTestRow{"a", 100, 1},
		bucketTestRow{"a", 1100, 2}, // advances past bucket 0
	)
	upstream := &fakeBucketUpstream{schema: s, batches: []*vectorized.RecordBatch{batch}}
	bt := NewBatchTimeBucket(upstream, s, nil, 0, 1000, spec, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0, true)
	_ = bt.Init(context.Background())
	defer bt.Close()

	first, err := bt.NextBatch(context.Background())
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if first == nil {
		t.Fatal("bucket 0 must flush and be returned by the first NextBatch call")
	}
	if upstream.idx != 1 {
		t.Fatalf("the first NextBatch call must pull upstream exactly once, got %d pulls", upstream.idx)
	}
	tsCol := first.Columns[0].(*vectorized.TypedColumn[int64])
	if tsCol.Data()[0] != 0 {
		t.Fatalf("first flushed bucket timestamp = %d, want 0", tsCol.Data()[0])
	}

	second, err := bt.NextBatch(context.Background())
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if second == nil {
		t.Fatal("bucket 1000 must flush on the second NextBatch call (upstream EOF)")
	}
	if second.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0] != 1000 {
		t.Fatalf("second flushed bucket timestamp = %d, want 1000", second.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0])
	}

	third, err := bt.NextBatch(context.Background())
	if err != nil || third != nil {
		t.Fatalf("want EOF (nil, nil) after both buckets drained, got (%v, %v)", third, err)
	}
}

// TestBatchTimeBucket_Streaming_MonotonicityViolation_Errors pins the
// operator-side guard (design §7.2): a row whose bucket regresses is an
// invariant violation and must fail loudly, not silently reopen a
// already-flushed bucket. Exercises consumeBatch directly (package-internal
// access) since the violation is a property of one Consume-shaped call, not
// of NextBatch's surrounding pull loop.
func TestBatchTimeBucket_Streaming_MonotonicityViolation_Errors(t *testing.T) {
	s := bucketTestSchema()
	spec := []AggSpec{{Func: AggSum, InputCol: 2, Output: "v"}}
	bt := NewBatchTimeBucket(nil, s, nil, 0, 1000, spec, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0, true)
	defer bt.Close()

	batch := buildBucketBatch(s,
		bucketTestRow{"a", 1100, 1}, // bucket 1000
		bucketTestRow{"a", 100, 2},  // bucket 0 — regresses
	)
	if consumeErr := bt.consumeBatch(context.Background(), batch); consumeErr == nil {
		t.Fatal("a regressing bucket must error, not silently reopen bucket 0")
	}
}

// TestBatchTimeBucket_MapMode_HandlesOutOfOrderInput pins design §7.2's
// index-mode fallback: input arriving as buckets B, A, B (which would
// violate streaming's monotonicity guard) must still produce one correct
// result per bucket when streaming=false — proving it took the map path.
func TestBatchTimeBucket_MapMode_HandlesOutOfOrderInput(t *testing.T) {
	s := bucketTestSchema()
	spec := []AggSpec{{Func: AggSum, InputCol: 2, Output: "v"}}
	batch := buildBucketBatch(s,
		bucketTestRow{"a", 1100, 10}, // bucket 1000 (B)
		bucketTestRow{"a", 100, 1},   // bucket 0 (A) — out of order, must not error
		bucketTestRow{"a", 1200, 20}, // bucket 1000 (B) again
	)
	upstream := &fakeBucketUpstream{schema: s, batches: []*vectorized.RecordBatch{batch}}
	bt := NewBatchTimeBucket(upstream, s, nil, 0, 1000, spec, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0, false)
	if initErr := bt.Init(context.Background()); initErr != nil {
		t.Fatal(initErr)
	}
	defer bt.Close()

	got := sumByBucketAndTag(t, drainBucket(t, bt))
	want := map[string]int64{"a|0": 1, "a|1000": 30}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("sum[%s] = %d, want %d (full result: %v)", k, got[k], v, got)
		}
	}
}

// TestBatchTimeBucket_MapMode_EmitsBucketsInAscendingOrder pins the review
// finding that motivated BatchAggregation.SortInsertionByBucket: map mode's
// single persistent aggregator accumulates groups in first-seen order over
// input that may not be time-ordered (the whole reason map mode exists),
// so without an explicit sort, out-of-order input like [2100, 1100, 2200]
// would emit buckets [2000, 1000] instead of [1000, 2000] — silently
// violating the bucket-ascending output contract (design §7.5) that a
// downstream Limit relies on.
func TestBatchTimeBucket_MapMode_EmitsBucketsInAscendingOrder(t *testing.T) {
	s := bucketTestSchema()
	spec := []AggSpec{{Func: AggSum, InputCol: 2, Output: "v"}}
	batch := buildBucketBatch(s,
		bucketTestRow{"a", 2100, 2}, // bucket 2000
		bucketTestRow{"a", 1100, 1}, // bucket 1000 — out of order
		bucketTestRow{"a", 2200, 20},
	)
	upstream := &fakeBucketUpstream{schema: s, batches: []*vectorized.RecordBatch{batch}}
	bt := NewBatchTimeBucket(upstream, s, nil, 0, 1000, spec, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0, false)
	if initErr := bt.Init(context.Background()); initErr != nil {
		t.Fatal(initErr)
	}
	defer bt.Close()

	drained := drainBucket(t, bt)
	var gotTS []int64
	for _, b := range drained {
		tsCol := b.Columns[0].(*vectorized.TypedColumn[int64])
		for i := 0; i < b.Len; i++ {
			gotTS = append(gotTS, tsCol.Data()[i])
		}
	}
	wantTS := []int64{1000, 2000}
	if len(gotTS) != len(wantTS) {
		t.Fatalf("bucket timestamps = %v, want %v", gotTS, wantTS)
	}
	for i, want := range wantTS {
		if gotTS[i] != want {
			t.Fatalf("bucket timestamps = %v, want ascending %v", gotTS, wantTS)
		}
	}
}

// TestBatchTimeBucket_MapMode_DrainsTerminalAggregatorIncrementally pins the
// PR review finding that motivated drainMapModeAggregator: map mode's
// terminal aggregator represents the whole scan (unlike a streaming
// instance, which only ever holds one bucket), so materializing every one
// of its pages into pending before the first NextBatch call returns would
// re-introduce the same unbounded-queue shape the PullOperator rewrite
// exists to avoid. With one group per bucket, batchSize=8, and the
// per-bucket aggregator pool capped at bucketAggPoolCapacity=64 internally,
// draining must proceed in small increments — not all 100 groups at once —
// so no single NextBatch call should return a batch of more than batchSize
// rows, and pending must never hold more than a couple of batches' worth at
// a time while the drain is in progress.
func TestBatchTimeBucket_MapMode_DrainsTerminalAggregatorIncrementally(t *testing.T) {
	s := bucketTestSchema()
	spec := []AggSpec{{Func: AggSum, InputCol: 2, Output: "v"}}
	const numGroups = 100
	const batchSize = 8
	rows := make([]bucketTestRow, 0, numGroups)
	for i := 0; i < numGroups; i++ {
		// Distinct tag per row forces one group per row; map mode never
		// flushes mid-scan regardless of bucket, so all numGroups groups
		// stay live in the single terminal aggregator until upstream EOF.
		rows = append(rows, bucketTestRow{fmt.Sprintf("tag-%d", i), 0, int64(i)})
	}
	batch := buildBucketBatch(s, rows...)
	upstream := &fakeBucketUpstream{schema: s, batches: []*vectorized.RecordBatch{batch}}
	bt := NewBatchTimeBucket(upstream, s, []int{1}, 0, 1000, spec, AggModeAll, batchSize, vectorized.NewMemoryTracker(1<<30), 0, false)
	if initErr := bt.Init(context.Background()); initErr != nil {
		t.Fatal(initErr)
	}
	defer bt.Close()

	total := 0
	batches := 0
	for {
		if len(bt.pending) > 2 {
			t.Fatalf("pending holds %d batches mid-drain, want the terminal aggregator drained incrementally, not materialized all at once", len(bt.pending))
		}
		nb, err := bt.NextBatch(context.Background())
		if err != nil {
			t.Fatalf("NextBatch: %v", err)
		}
		if nb == nil {
			break
		}
		batches++
		if nb.Len > batchSize {
			t.Fatalf("output batch Len = %d, exceeds batchSize %d", nb.Len, batchSize)
		}
		total += nb.Len
	}
	if total != numGroups {
		t.Fatalf("total rows drained = %d, want %d", total, numGroups)
	}
	if wantBatches := (numGroups + batchSize - 1) / batchSize; batches != wantBatches {
		t.Fatalf("got %d output batches, want %d", batches, wantBatches)
	}
}

// TestBatchTimeBucket_Close_ReleasesLiveState pins that closing the
// operator mid-stream (error path, or caller giving up early) releases the
// open bucket's memory reservation rather than leaking it. Exercises
// consumeBatch directly so the reservation can be inspected before any
// flush would otherwise release it.
func TestBatchTimeBucket_Close_ReleasesLiveState(t *testing.T) {
	s := bucketTestSchema()
	spec := []AggSpec{{Func: AggSum, InputCol: 2, Output: "v"}}
	tracker := vectorized.NewMemoryTracker(1 << 30)
	bt := NewBatchTimeBucket(nil, s, []int{1}, 0, 1000, spec, AggModeAll, 8, tracker, 512, true)

	batch := buildBucketBatch(s, bucketTestRow{"a", 100, 1}, bucketTestRow{"b", 200, 2})
	if consumeErr := bt.consumeBatch(context.Background(), batch); consumeErr != nil {
		t.Fatalf("consumeBatch: %v", consumeErr)
	}
	if tracker.Used() == 0 {
		t.Fatal("open bucket must have reserved memory for its live groups")
	}
	if closeErr := bt.Close(); closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	if tracker.Used() != 0 {
		t.Fatalf("Close must release the open bucket's reservation, tracker.Used() = %d", tracker.Used())
	}
}

// TestBatchTimeBucket_NextBatch_PullsUpstreamLazily is the regression pin
// for the P1 review finding this design was rewritten to fix: a
// BreakerOperator-shaped implementation is driven by breakerStage, which
// pulls the ENTIRE upstream — closing every bucket the scan will ever
// produce — before the breaker's own NextBatch is ever called, so the
// flushed-but-unemitted queue would hold every bucket, not just the one
// that just closed. As a genuine PullOperator that owns upstream directly,
// BatchTimeBucket must only pull as much upstream as it needs to produce
// ONE output batch, leaving later buckets' upstream batches unpulled.
func TestBatchTimeBucket_NextBatch_PullsUpstreamLazily(t *testing.T) {
	s := bucketTestSchema()
	spec := []AggSpec{{Func: AggSum, InputCol: 2, Output: "v"}}
	// Three widely-spaced buckets, one upstream batch each — a scan that,
	// under the old Breaker shape, would be fully drained (all 3 pulls) by
	// the very first NextBatch call.
	batches := []*vectorized.RecordBatch{
		buildBucketBatch(s, bucketTestRow{"a", 0, 1}),
		buildBucketBatch(s, bucketTestRow{"a", 1000, 2}),
		buildBucketBatch(s, bucketTestRow{"a", 2000, 3}),
	}
	upstream := &fakeBucketUpstream{schema: s, batches: batches}
	bt := NewBatchTimeBucket(upstream, s, nil, 0, 1000, spec, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0, true)
	_ = bt.Init(context.Background())
	defer bt.Close()

	first, err := bt.NextBatch(context.Background())
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if first == nil {
		t.Fatal("want the first flushed bucket back immediately")
	}
	// Only enough upstream batches to detect ONE bucket transition (bucket
	// 0's opening row, then bucket 1000's opening row triggers the flush) —
	// not all three, which the old Breaker shape would have pulled.
	if upstream.idx != 2 {
		t.Fatalf("first NextBatch call pulled upstream %d times, want 2 (must not race ahead into unopened buckets)", upstream.idx)
	}
	if len(bt.pending) != 0 {
		t.Fatalf("pending queue must be drained by the return, not accumulate: len=%d", len(bt.pending))
	}
}

// TestBatchTimeBucket_Streaming_SparseBuckets_CompactsOutputBatches pins the
// review finding that motivated appendOutput/commitBuilder: one upstream
// batch spanning many distinct, single-row buckets must not queue one
// full-batchSize-capacity RecordBatch per bucket (O(buckets) allocations for
// O(buckets) rows). With batchSize=8 and 20 buckets closed while processing
// a single upstream batch, output must be packed into ceil(20/8)=3 batches,
// not 20 — and no output batch may exceed batchSize rows.
func TestBatchTimeBucket_Streaming_SparseBuckets_CompactsOutputBatches(t *testing.T) {
	s := bucketTestSchema()
	spec := []AggSpec{{Func: AggSum, InputCol: 2, Output: "v"}}
	const numBuckets = 20
	const batchSize = 8
	rows := make([]bucketTestRow, 0, numBuckets)
	for i := 0; i < numBuckets; i++ {
		rows = append(rows, bucketTestRow{"a", int64(i) * 1000, int64(i)})
	}
	batch := buildBucketBatch(s, rows...)
	upstream := &fakeBucketUpstream{schema: s, batches: []*vectorized.RecordBatch{batch}}
	bt := NewBatchTimeBucket(upstream, s, nil, 0, 1000, spec, AggModeAll, batchSize, vectorized.NewMemoryTracker(1<<30), 0, true)
	if initErr := bt.Init(context.Background()); initErr != nil {
		t.Fatal(initErr)
	}
	defer bt.Close()

	drained := drainBucket(t, bt)
	// The last bucket (opened by the batch's final row) never sees an
	// advancing row within this batch, so only numBuckets-1 buckets flush
	// here; the final one flushes at upstream EOF as its own commit.
	wantBatches := (numBuckets-1+batchSize-1)/batchSize + 1
	if len(drained) != wantBatches {
		t.Fatalf("got %d output batches, want %d (compaction must pack multiple closed buckets per batch)", len(drained), wantBatches)
	}
	total := 0
	for _, b := range drained {
		if b.Len > batchSize {
			t.Errorf("output batch Len = %d, exceeds batchSize %d", b.Len, batchSize)
		}
		total += b.Len
	}
	if total != numBuckets {
		t.Fatalf("total rows across output batches = %d, want %d", total, numBuckets)
	}
}

// TestBatchTimeBucket_Streaming_BucketAdvanceReleasesPriorBucketMemory pins
// the memory-bound property this whole design exists for (§7.2): flushing a
// closed bucket must release ITS reservation immediately — not just at
// final Close — so live memory never grows past one bucket's groups
// regardless of how many buckets the scan crosses. Exercises consumeBatch
// directly to inspect the tracker right after the internal flush, before
// NextBatch's own draining would otherwise complicate the picture.
func TestBatchTimeBucket_Streaming_BucketAdvanceReleasesPriorBucketMemory(t *testing.T) {
	s := bucketTestSchema()
	spec := []AggSpec{{Func: AggSum, InputCol: 2, Output: "v"}}
	tracker := vectorized.NewMemoryTracker(1 << 30)
	bt := NewBatchTimeBucket(nil, s, []int{1}, 0, 1000, spec, AggModeAll, 8, tracker, 512, true)
	defer bt.Close()

	batch := buildBucketBatch(s,
		bucketTestRow{"a", 100, 1},  // opens bucket 0, reserves memory
		bucketTestRow{"a", 1100, 2}, // advances past bucket 0: must flush + release it
	)
	if consumeErr := bt.consumeBatch(context.Background(), batch); consumeErr != nil {
		t.Fatalf("consumeBatch: %v", consumeErr)
	}
	// Only bucket 1000 should still be live; bucket 0's reservation must
	// already be released, not deferred until upstream EOF or Close.
	if got := tracker.Used(); got != 512 {
		t.Fatalf("tracker.Used() = %d, want 512 (only the current bucket's one group live)", got)
	}
}
