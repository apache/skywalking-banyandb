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
	"maps"
	"strconv"
	"strings"
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// sidTestSchema is bucketTestSchema plus a trailing series-ID column, the
// shape the run-folding fast path needs to split a batch at series
// boundaries.
func sidTestSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
	})
}

type sidTestRow struct {
	g   string
	sid int64
	ts  int64
	v   int64
}

func buildSidBatch(schema *vectorized.BatchSchema, rows ...sidTestRow) *vectorized.RecordBatch {
	b := vectorized.NewRecordBatch(schema, len(rows))
	for _, r := range rows {
		b.Columns[0].(*vectorized.TypedColumn[int64]).Append(r.ts)
		b.Columns[1].(*vectorized.TypedColumn[string]).Append(r.g)
		b.Columns[2].(*vectorized.TypedColumn[int64]).Append(r.v)
		b.Columns[3].(*vectorized.TypedColumn[int64]).Append(r.sid)
	}
	b.Len = len(rows)
	return b
}

func drainAgg(t *testing.T, a *BatchAggregation) []*vectorized.RecordBatch {
	t.Helper()
	var out []*vectorized.RecordBatch
	for {
		nb, err := a.NextBatch(context.Background())
		if err != nil {
			t.Fatalf("NextBatch: %v", err)
		}
		if nb == nil {
			return out
		}
		out = append(out, nb)
	}
}

// TestBatchAggregation_RunFolding_SplitsMixedSeriesBatch pins that a single
// RecordBatch carrying several series (BatchSourceFromBatchResult refills
// across storage batches) is folded per series run, not under row 0's key.
func TestBatchAggregation_RunFolding_SplitsMixedSeriesBatch(t *testing.T) {
	build := func() *vectorized.RecordBatch {
		return buildSidBatch(sidTestSchema(),
			sidTestRow{sid: 1, g: "A", ts: 1, v: 1},
			sidTestRow{sid: 2, g: "B", ts: 2, v: 1},
			sidTestRow{sid: 2, g: "B", ts: 3, v: 1},
			sidTestRow{sid: 1, g: "A", ts: 4, v: 1},
		)
	}
	specs := []AggSpec{{Func: AggCount, InputCol: 1, Output: "cnt", TargetIndexResolved: true}}
	run := func(keyIndices []int, resolved bool) (map[string]int64, int64) {
		a := NewBatchAggregation(sidTestSchema(), keyIndices, specs, AggModeAll, 16, vectorized.NewMemoryTracker(1<<30), 0)
		a.SetAllKeysIndexResolved(resolved)
		if err := a.Init(context.Background()); err != nil {
			t.Fatalf("Init: %v", err)
		}
		if err := a.Consume(context.Background(), build()); err != nil {
			t.Fatalf("Consume: %v", err)
		}
		got := make(map[string]int64)
		for _, b := range drainAgg(t, a) {
			for i := 0; i < b.Len; i++ {
				key := ""
				if len(keyIndices) > 0 {
					key = b.Columns[0].(*vectorized.TypedColumn[string]).Data()[i]
				}
				got[key] = b.Columns[len(b.Columns)-1].(*vectorized.TypedColumn[int64]).Data()[i]
			}
		}
		folds := a.spanFolds
		if err := a.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return got, folds
	}

	got, folds := run([]int{1}, true)
	if want := map[string]int64{"A": 2, "B": 2}; !maps.Equal(got, want) {
		t.Fatalf("grouped COUNT = %v, want %v", got, want)
	}
	if folds != 3 {
		t.Fatalf("spanFolds = %d, want 3 (A, B, A runs)", folds)
	}
	ref, _ := run([]int{1}, false)
	if !maps.Equal(got, ref) {
		t.Fatalf("fast path %v != per-row oracle %v", got, ref)
	}

	// Scalar COUNT over an index-resolved target must also fold per series:
	// the target value is only constant within one series.
	scalar, _ := run(nil, true)
	scalarRef, _ := run(nil, false)
	if !maps.Equal(scalar, scalarRef) || scalar[""] != 4 {
		t.Fatalf("scalar COUNT fast=%v ref=%v, want 4", scalar, scalarRef)
	}
}

// TestBatchAggregation_RunFolding_NoSeriesIDColumn_FallsBackToPerRow pins
// that without a series-ID column constancy cannot be proven, so the fast
// path is not taken.
func TestBatchAggregation_RunFolding_NoSeriesIDColumn_FallsBackToPerRow(t *testing.T) {
	s := bucketTestSchema()
	specs := []AggSpec{{Func: AggCount, InputCol: 1, Output: "cnt", TargetIndexResolved: true}}
	a := NewBatchAggregation(s, []int{1}, specs, AggModeAll, 16, vectorized.NewMemoryTracker(1<<30), 0)
	a.SetAllKeysIndexResolved(true)
	if err := a.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	b := buildBucketBatch(s, bucketTestRow{g: "A", ts: 1, v: 1}, bucketTestRow{g: "B", ts: 2, v: 1})
	if err := a.Consume(context.Background(), b); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if a.spanFolds != 0 {
		t.Fatalf("spanFolds = %d, want 0 without a series-ID column", a.spanFolds)
	}
	if got := len(a.insertion); got != 2 {
		t.Fatalf("groups = %d, want 2", got)
	}
	_ = a.Close()
}

// TestBatchAggregation_RunFolding_BudgetRejectionAfterReservation pins the
// reservation error path of the span fast path: the second series' group
// exceeds the budget after the first one reserved successfully.
func TestBatchAggregation_RunFolding_BudgetRejectionAfterReservation(t *testing.T) {
	const entrySize = int64(100)
	tracker := vectorized.NewMemoryTracker(entrySize) // room for exactly one group
	specs := []AggSpec{{Func: AggCount, InputCol: 1, Output: "cnt", TargetIndexResolved: true}}
	a := NewBatchAggregation(sidTestSchema(), []int{1}, specs, AggModeAll, 16, tracker, entrySize)
	a.SetAllKeysIndexResolved(true)
	if err := a.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	b := buildSidBatch(sidTestSchema(),
		sidTestRow{sid: 1, g: "A", ts: 1, v: 1},
		sidTestRow{sid: 2, g: "B", ts: 2, v: 1},
	)
	err := a.Consume(context.Background(), b)
	if err == nil {
		t.Fatal("Consume must fail once the budget is exhausted")
	}
	if !strings.Contains(err.Error(), "aggregation memory budget exceeded") || !strings.Contains(err.Error(), "memory budget exceeded: used") {
		t.Fatalf("error %q must wrap the tracker's budget error", err)
	}
	if got := tracker.Used(); got != entrySize {
		t.Fatalf("tracker.Used() = %d after rejection, want %d (only group A reserved)", got, entrySize)
	}
	if a.reserved != entrySize || len(a.insertion) != 1 || len(a.groups) != 1 {
		t.Fatalf("state inconsistent: reserved=%d insertion=%d groups=%d, want %d/1/1", a.reserved, len(a.insertion), len(a.groups), entrySize)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := tracker.Used(); got != 0 {
		t.Fatalf("tracker.Used() = %d after Close, want 0", got)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := tracker.Used(); got != 0 {
		t.Fatalf("tracker.Used() = %d after double Close, want 0 (no double release)", got)
	}
}

// TestBatchAggregation_RunFolding_EarlyAndDoubleClose pins that closing a
// fast-path aggregator after a partial drain refunds the full reservation
// exactly once, and that further Close calls neither re-release nor error.
func TestBatchAggregation_RunFolding_EarlyAndDoubleClose(t *testing.T) {
	const entrySize = int64(100)
	tracker := vectorized.NewMemoryTracker(1 << 20)
	specs := []AggSpec{{Func: AggCount, InputCol: 1, Output: "cnt", TargetIndexResolved: true}}
	// batchSize 1 forces the three groups out over three NextBatch calls.
	a := NewBatchAggregation(sidTestSchema(), []int{1}, specs, AggModeAll, 1, tracker, entrySize)
	a.SetAllKeysIndexResolved(true)
	if err := a.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	b := buildSidBatch(sidTestSchema(),
		sidTestRow{sid: 1, g: "A", ts: 1, v: 1},
		sidTestRow{sid: 2, g: "B", ts: 2, v: 1},
		sidTestRow{sid: 3, g: "C", ts: 3, v: 1},
	)
	if err := a.Consume(context.Background(), b); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if got, want := tracker.Used(), 3*entrySize; got != want {
		t.Fatalf("tracker.Used() = %d after Consume, want %d", got, want)
	}
	first, err := a.NextBatch(context.Background())
	if err != nil || first == nil {
		t.Fatalf("first NextBatch = %v, %v; want a batch", first, err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("early Close: %v", err)
	}
	if got := tracker.Used(); got != 0 {
		t.Fatalf("tracker.Used() = %d after early Close, want 0", got)
	}
	// Another reservation holder shares the tracker: a second Close must not
	// release on its behalf.
	if reserveErr := tracker.Reserve(entrySize); reserveErr != nil {
		t.Fatalf("Reserve: %v", reserveErr)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := tracker.Used(); got != entrySize {
		t.Fatalf("tracker.Used() = %d after double Close, want %d (no double release)", got, entrySize)
	}
}

// TestBatchTimeBucket_RunFolding_EarlyAndDoubleClose pins that closing a
// streaming bucket operator mid-drain, and closing it again, releases the
// open bucket's reservation exactly once.
func TestBatchTimeBucket_RunFolding_EarlyAndDoubleClose(t *testing.T) {
	const entrySize = int64(100)
	s := sidTestSchema()
	input := buildSidBatch(s,
		sidTestRow{sid: 1, g: "A", ts: 1, v: 1},
		sidTestRow{sid: 2, g: "B", ts: 2, v: 1},
		sidTestRow{sid: 1, g: "A", ts: 7_200_000, v: 1},
	)
	upstream := &fakeBucketUpstream{schema: s, batches: []*vectorized.RecordBatch{input}}
	specs := []AggSpec{{Func: AggCount, InputCol: 1, Output: "cnt", TargetIndexResolved: true}}
	tracker := vectorized.NewMemoryTracker(1 << 20)
	bt := NewBatchTimeBucket(upstream, s, []int{1}, 0, 3_600_000, specs, AggModeAll, 1, tracker, entrySize, true)
	bt.SetAllKeysIndexResolved(true)
	if err := bt.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	first, err := bt.NextBatch(context.Background())
	if err != nil || first == nil {
		t.Fatalf("first NextBatch = %v, %v; want a batch", first, err)
	}
	if tracker.Used() == 0 {
		t.Fatal("expected an outstanding reservation mid-drain")
	}
	if err := bt.Close(); err != nil {
		t.Fatalf("early Close: %v", err)
	}
	if got := tracker.Used(); got != 0 {
		t.Fatalf("tracker.Used() = %d after early Close, want 0", got)
	}
	if err := bt.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := tracker.Used(); got != 0 {
		t.Fatalf("tracker.Used() = %d after double Close, want 0", got)
	}
}

func eligibilityOpts(resolved bool, bucketed bool) model.MeasureQueryOptions {
	gb := &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"g"}, AllTagsIndexResolved: resolved}
	if bucketed {
		gb.TimeBucket = &model.MeasureTimeBucket{WidthNanos: 3_600_000}
	}
	return model.MeasureQueryOptions{
		GroupBy: gb,
		Agg:     &model.MeasureAgg{FieldName: "v", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT},
	}
}

// TestBuilders_PropagateRunFoldEligibility pins that the planner-derived
// MeasureGroupBy.AllTagsIndexResolved reaches the operators built by both
// BuildOperators and BuildTimeBucketOperator, in both polarities.
func TestBuilders_PropagateRunFoldEligibility(t *testing.T) {
	s := sidTestSchema()
	tracker := vectorized.NewMemoryTracker(1 << 20)
	for _, resolved := range []bool{true, false} {
		ops, err := BuildOperators(eligibilityOpts(resolved, false), s, tracker, 16, AggModeAll)
		if err != nil {
			t.Fatalf("BuildOperators: %v", err)
		}
		agg := ops[0].(*BatchAggregation)
		if agg.allKeysIndexResolved != resolved {
			t.Fatalf("BuildOperators: allKeysIndexResolved = %v, want %v", agg.allKeysIndexResolved, resolved)
		}
		bt, err := BuildTimeBucketOperator(&fakeBucketUpstream{schema: s}, eligibilityOpts(resolved, true), s, tracker, 16, AggModeAll)
		if err != nil {
			t.Fatalf("BuildTimeBucketOperator: %v", err)
		}
		if bt.allKeysIndexResolved != resolved {
			t.Fatalf("BuildTimeBucketOperator: allKeysIndexResolved = %v, want %v", bt.allKeysIndexResolved, resolved)
		}
		if err := bt.Init(context.Background()); err != nil {
			t.Fatalf("Init: %v", err)
		}
		if err := bt.openBucket(context.Background()); err != nil {
			t.Fatalf("openBucket: %v", err)
		}
		if bt.currentAgg.allKeysIndexResolved != resolved {
			t.Fatalf("per-bucket aggregator: allKeysIndexResolved = %v, want %v", bt.currentAgg.allKeysIndexResolved, resolved)
		}
		_ = bt.Close()
	}
	// A scalar reduce has no key columns: vacuously eligible.
	ops, err := BuildOperators(model.MeasureQueryOptions{
		Agg: &model.MeasureAgg{FieldName: "v", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT},
	}, s, tracker, 16, AggModeAll)
	if err != nil {
		t.Fatalf("BuildOperators scalar: %v", err)
	}
	if !ops[0].(*BatchAggregation).allKeysIndexResolved {
		t.Fatal("scalar reduce must be vacuously eligible")
	}
}

// TestBatchAggregation_RunFolding_MatchesPerRowFold_AtBlockScale pins design
// §7.7's stage-8a run-folding: an 8192-row single-series block — the
// storage layer's maxBlockLength (banyand/measure/measure.go) — grouped by
// an index-resolved tag at hour-wide time buckets must fold each bucket's
// rows as one span (COUNT += spanLength; COUNT_DISTINCT = one set insert)
// and produce results identical to the pre-existing per-row fold path.
//
// The oracle is that same unoptimized path, run in this test over the
// identical input with SetAllKeysIndexResolved(false) — not a hand-computed
// number. The magnitude assertion (~137 spans, not 8192 key computations)
// is independent of the fix: it falls out of the fixture's own 1-minute
// cadence against a 60-minute bucket (ceil(8192/60) = 137), computed here
// from the same arithmetic the fixture is built from, not from the
// implementation under test.
func TestBatchAggregation_RunFolding_MatchesPerRowFold_AtBlockScale(t *testing.T) {
	const rowCount = 8192
	const stepMillis = int64(60_000)    // 1-minute cadence
	const widthNanos = int64(3_600_000) // 1-hour bucket, same unit as the fixture's timestamps
	const entity = "svc-1"              // single series: the fixture's one and only tag value

	wantSpans := int64((rowCount + 59) / 60) // ceil(rowCount / 60), independent of the fix

	buildInput := func() *vectorized.RecordBatch {
		s := sidTestSchema()
		rows := make([]sidTestRow, rowCount)
		for i := range rows {
			rows[i] = sidTestRow{sid: 1, g: entity, ts: int64(i) * stepMillis, v: 1}
		}
		return buildSidBatch(s, rows...)
	}

	run := func(allKeysIndexResolved bool) (counts, distincts map[string]int64, spanFolds int64) {
		s := sidTestSchema()
		upstream := &fakeBucketUpstream{schema: s, batches: []*vectorized.RecordBatch{buildInput()}}
		specs := []AggSpec{
			{Func: AggCount, InputCol: 1, Output: "cnt", TargetIndexResolved: true},
			{Func: AggCountDistinct, InputCol: 1, Output: "cnt_distinct", TargetIndexResolved: true},
		}
		tracker := vectorized.NewMemoryTracker(1 << 30)
		bt := NewBatchTimeBucket(upstream, s, []int{1}, 0, widthNanos, specs, AggModeAll, 1024, tracker, 0, true)
		bt.SetAllKeysIndexResolved(allKeysIndexResolved)
		if err := bt.Init(context.Background()); err != nil {
			t.Fatalf("Init: %v", err)
		}
		out := drainBucket(t, bt)
		spanFolds = bt.spanFolds
		if err := bt.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		counts = make(map[string]int64)
		distincts = make(map[string]int64)
		for _, b := range out {
			tsCol := b.Columns[0].(*vectorized.TypedColumn[int64])
			cntCol := b.Columns[2].(*vectorized.TypedColumn[int64])
			distCol := b.Columns[3].(*vectorized.TypedColumn[int64])
			for i := 0; i < b.Len; i++ {
				key := strconv.FormatInt(tsCol.Data()[i], 10)
				counts[key] = cntCol.Data()[i]
				distincts[key] = distCol.Data()[i]
			}
		}
		return counts, distincts, spanFolds
	}

	fastCounts, fastDistincts, spanFolds := run(true)
	refCounts, refDistincts, refSpanFolds := run(false)

	if refSpanFolds != 0 {
		t.Fatalf("reference (unoptimized) run unexpectedly took the span-fold path: spanFolds=%d", refSpanFolds)
	}
	if spanFolds != wantSpans {
		t.Fatalf("spanFolds = %d, want %d (%d rows in 60-row buckets, independent of the fix)", spanFolds, wantSpans, rowCount)
	}
	if int64(len(fastCounts)) != wantSpans {
		t.Fatalf("got %d output buckets, want %d", len(fastCounts), wantSpans)
	}
	if !maps.Equal(fastCounts, refCounts) {
		t.Fatalf("COUNT mismatch between run-folded and per-row paths:\n fast=%v\n ref =%v", fastCounts, refCounts)
	}
	if !maps.Equal(fastDistincts, refDistincts) {
		t.Fatalf("COUNT_DISTINCT mismatch between run-folded and per-row paths:\n fast=%v\n ref =%v", fastDistincts, refDistincts)
	}
}

// TestBatchAggregation_RunFolding_SpanFallback_HandlesVaryingNullableTargets
// pins foldSpan's per-row fallback loop (design §7.7): SUM/MIN/MAX/MEAN
// always take it (even over an index-resolved target), and a
// TargetIndexResolved: false COUNT/COUNT_DISTINCT target takes it too. The
// loop is reachable through consumeSpan whenever a key-equal, single-series
// span covers more than one row, and must fold a target that genuinely
// varies row to row — including a null row — identically to the per-row
// oracle. Every pre-existing executor test for the fast path
// (TestBatchAggregation_RunFolding_MatchesPerRowFold_AtBlockScale,
// TestBatchAggregation_RunFolding_SplitsMixedSeriesBatch, and the Close/
// budget tests below) uses only COUNT/COUNT_DISTINCT with
// TargetIndexResolved: true, both of which return from foldSpan before
// ever reaching this loop — deleting it would not fail any of them.
func TestBatchAggregation_RunFolding_SpanFallback_HandlesVaryingNullableTargets(t *testing.T) {
	// One series, 5 rows, "v" = [10, null, 30, 5, 20]. Independently
	// computed (not from the implementation under test): SUM ignores the
	// null (10+30+5+20=65); MIN=5; MAX=30; MEAN over an int FIELD truncates
	// toward zero via aggregation.meanFunc[int64].Val() (65/4=16, not
	// clamped since 16 >= 1); COUNT and COUNT_DISTINCT both count the 4
	// non-null rows (10, 30, 5, 20 are all distinct).
	values := []int64{10, 0, 30, 5, 20}
	const nullRow = 1

	build := func() *vectorized.RecordBatch {
		s := sidTestSchema()
		b := vectorized.NewRecordBatch(s, len(values))
		tsCol := b.Columns[0].(*vectorized.TypedColumn[int64])
		gCol := b.Columns[1].(*vectorized.TypedColumn[string])
		vCol := b.Columns[2].(*vectorized.TypedColumn[int64])
		sidCol := b.Columns[3].(*vectorized.TypedColumn[int64])
		for i, v := range values {
			tsCol.Append(int64(i))
			gCol.Append("A")
			sidCol.Append(1)
			if i == nullRow {
				vCol.AppendNull()
			} else {
				vCol.Append(v)
			}
		}
		b.Len = len(values)
		return b
	}

	specs := []AggSpec{
		{Func: AggSum, InputCol: 2, Output: "sum"},
		{Func: AggMin, InputCol: 2, Output: "min"},
		{Func: AggMax, InputCol: 2, Output: "max"},
		{Func: AggMean, InputCol: 2, Output: "mean"},
		{Func: AggCount, InputCol: 2, Output: "cnt", TargetIndexResolved: false},
		{Func: AggCountDistinct, InputCol: 2, Output: "cnt_distinct", TargetIndexResolved: false},
	}

	run := func(allKeysIndexResolved bool) (map[string]int64, int64) {
		s := sidTestSchema()
		a := NewBatchAggregation(s, []int{1}, specs, AggModeAll, 16, vectorized.NewMemoryTracker(1<<30), 0)
		a.SetAllKeysIndexResolved(allKeysIndexResolved)
		if err := a.Init(context.Background()); err != nil {
			t.Fatalf("Init: %v", err)
		}
		if err := a.Consume(context.Background(), build()); err != nil {
			t.Fatalf("Consume: %v", err)
		}
		out := drainAgg(t, a)
		if len(out) != 1 || out[0].Len != 1 {
			t.Fatalf("expected exactly one output row for the one group, got %d batches", len(out))
		}
		b := out[0]
		got := map[string]int64{
			"sum":          b.Columns[1].(*vectorized.TypedColumn[int64]).Data()[0],
			"min":          b.Columns[2].(*vectorized.TypedColumn[int64]).Data()[0],
			"max":          b.Columns[3].(*vectorized.TypedColumn[int64]).Data()[0],
			"mean":         b.Columns[4].(*vectorized.TypedColumn[int64]).Data()[0],
			"cnt":          b.Columns[5].(*vectorized.TypedColumn[int64]).Data()[0],
			"cnt_distinct": b.Columns[6].(*vectorized.TypedColumn[int64]).Data()[0],
		}
		folds := a.spanFolds
		if err := a.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return got, folds
	}

	fast, fastFolds := run(true)
	ref, refFolds := run(false)

	if fastFolds != 1 {
		t.Fatalf("spanFolds = %d, want 1 (one series, one span)", fastFolds)
	}
	if refFolds != 0 {
		t.Fatalf("reference (unoptimized) run unexpectedly took the span-fold path: spanFolds=%d", refFolds)
	}
	want := map[string]int64{"sum": 65, "min": 5, "max": 30, "mean": 16, "cnt": 4, "cnt_distinct": 4}
	for k, w := range want {
		if fast[k] != w {
			t.Fatalf("fast-path %s = %d, want %d (independently computed from the fixture)", k, fast[k], w)
		}
		if ref[k] != w {
			t.Fatalf("per-row oracle %s = %d, want %d", k, ref[k], w)
		}
	}
}

// sidShardTestSchema is sidTestSchema plus a trailing shard-ID column, the
// shape AggModeMap COUNT_DISTINCT uses once BuildOperators/
// BuildTimeBucketOperator append the shard-ID column to keyIndices (design
// §7.4's per-(shard, group) partial requirement).
// TestBatchAggregation_RunFolding_AggModeMap_SplitsSeriesSpanningShards pins
// design §7.4's per-(shard, group) partial requirement against design
// §7.7's run-folding fast path: BuildOperators/BuildTimeBucketOperator
// append the shard-ID column to keyIndices for AggModeMap COUNT_DISTINCT
// specifically so replica dedup can key on the exact (shard, group) that
// produced each partial. A series ID (hashing only entity values) is
// independent of shard placement (pkg/partition/route.go's sharding-key
// Locate), so one series can legitimately span more than one shard when a
// sharding key narrower than or different from the entity is configured.
// consumeSeriesRuns must therefore split a run at a shard-ID change even
// when the series ID stays constant — one span per (series, shard), not
// one span per series — or two distinct partials silently merge under the
// first row's shard identity, which the liaison's replica dedup would then
// misattribute.
//
// The schema, rows, and batch are built inline with vectorized.NewBatchSchema/
// vectorized.NewRecordBatch/TypedColumn.Append — the same pre-existing
// constructors sidTestSchema/buildSidBatch above already use — rather than
// through a new named helper, so this test's only dependency on anything
// new is the specific shard-boundary behavior under test itself.
func TestBatchAggregation_RunFolding_AggModeMap_SplitsSeriesSpanningShards(t *testing.T) {
	s := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Type: vectorized.ColumnTypeInt64},
	})
	// One series (sid=1) with two rows on shard 10 (value 100) followed by
	// two rows on shard 20 (value 200) — same series throughout, shard
	// changes mid-run. TargetIndexResolved: true only claims the target is
	// constant within one series' own rows on a given shard, exactly the
	// shape this fixture pins: each shard's own rows share one value.
	type row struct {
		g     string
		sid   int64
		shard int64
		ts    int64
		v     int64
	}
	rows := []row{
		{sid: 1, shard: 10, g: "A", ts: 1, v: 100},
		{sid: 1, shard: 10, g: "A", ts: 2, v: 100},
		{sid: 1, shard: 20, g: "A", ts: 3, v: 200},
		{sid: 1, shard: 20, g: "A", ts: 4, v: 200},
	}
	build := func() *vectorized.RecordBatch {
		b := vectorized.NewRecordBatch(s, len(rows))
		for _, r := range rows {
			b.Columns[0].(*vectorized.TypedColumn[int64]).Append(r.ts)
			b.Columns[1].(*vectorized.TypedColumn[string]).Append(r.g)
			b.Columns[2].(*vectorized.TypedColumn[int64]).Append(r.v)
			b.Columns[3].(*vectorized.TypedColumn[int64]).Append(r.sid)
			b.Columns[4].(*vectorized.TypedColumn[int64]).Append(r.shard)
		}
		b.Len = len(rows)
		return b
	}
	specs := []AggSpec{{Func: AggCountDistinct, InputCol: 2, Output: "cnt_distinct", TargetIndexResolved: true}}
	shardIdx := 4

	type partial struct {
		g     string
		shard int64
		cnt   int64
	}
	run := func(allKeysIndexResolved bool) ([]partial, int64) {
		a := NewBatchAggregation(s, []int{1, shardIdx}, specs, AggModeMap, 16, vectorized.NewMemoryTracker(1<<30), 0)
		a.SetAllKeysIndexResolved(allKeysIndexResolved)
		if err := a.Init(context.Background()); err != nil {
			t.Fatalf("Init: %v", err)
		}
		if err := a.Consume(context.Background(), build()); err != nil {
			t.Fatalf("Consume: %v", err)
		}
		var got []partial
		for _, b := range drainAgg(t, a) {
			shardCol := b.Columns[0].(*vectorized.TypedColumn[int64])
			gCol := b.Columns[1].(*vectorized.TypedColumn[string])
			cntCol := b.Columns[2].(*vectorized.TypedColumn[int64])
			for i := 0; i < b.Len; i++ {
				got = append(got, partial{shard: shardCol.Data()[i], g: gCol.Data()[i], cnt: cntCol.Data()[i]})
			}
		}
		folds := a.spanFolds
		if err := a.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return got, folds
	}

	fast, folds := run(true)
	if folds != 2 {
		t.Fatalf("spanFolds = %d, want 2 (one series but two shards, so two spans)", folds)
	}
	want := map[int64]partial{10: {shard: 10, g: "A", cnt: 1}, 20: {shard: 20, g: "A", cnt: 1}}
	if len(fast) != 2 {
		t.Fatalf("got %d partials, want 2 (one per shard): %+v", len(fast), fast)
	}
	for _, p := range fast {
		w, ok := want[p.shard]
		if !ok || p != w {
			t.Fatalf("partial %+v does not match an expected (shard, group, count) in %+v", p, want)
		}
	}

	ref, refFolds := run(false)
	if refFolds != 0 {
		t.Fatalf("reference (unoptimized) run unexpectedly took the span-fold path: spanFolds=%d", refFolds)
	}
	refSet := map[partial]struct{}{}
	for _, p := range ref {
		refSet[p] = struct{}{}
	}
	for _, p := range fast {
		if _, ok := refSet[p]; !ok {
			t.Fatalf("fast-path partial %+v has no match in the per-row oracle %+v", p, ref)
		}
	}
	if len(ref) != len(fast) {
		t.Fatalf("fast path produced %d partials, oracle produced %d: fast=%+v ref=%+v", len(fast), len(ref), fast, ref)
	}
}

// TestBatchAggregation_RunFolding_AllNullIndexResolvedCount_CountsZero pins
// foldSpan's bulk-COUNT branch (design §7.7) for an index-resolved target
// that is null for the whole span: the branch checks only the first row's
// nullness before bulk-adding len(active), bypassing fold's own per-row
// IsNull check, so an all-null span must still contribute zero — not
// len(active) — and a following non-null series must accumulate correctly,
// unaffected by the preceding null span's state. The only pre-existing
// null-target coverage (TestBatchAggregation_RunFolding_SpanFallback_
// HandlesVaryingNullableTargets) sets TargetIndexResolved: false on its
// COUNT spec, which never reaches this branch at all.
func TestBatchAggregation_RunFolding_AllNullIndexResolvedCount_CountsZero(t *testing.T) {
	s := sidTestSchema()
	build := func() *vectorized.RecordBatch {
		b := vectorized.NewRecordBatch(s, 5)
		tsCol := b.Columns[0].(*vectorized.TypedColumn[int64])
		gCol := b.Columns[1].(*vectorized.TypedColumn[string])
		vCol := b.Columns[2].(*vectorized.TypedColumn[int64])
		sidCol := b.Columns[3].(*vectorized.TypedColumn[int64])
		// Series 1 (sid=1, g="A"): three rows, target "v" null throughout.
		for i := 0; i < 3; i++ {
			tsCol.Append(int64(i))
			gCol.Append("A")
			vCol.AppendNull()
			sidCol.Append(1)
		}
		// Series 2 (sid=2, g="B"): two rows, target "v" non-null.
		for i := 3; i < 5; i++ {
			tsCol.Append(int64(i))
			gCol.Append("B")
			vCol.Append(7)
			sidCol.Append(2)
		}
		b.Len = 5
		return b
	}
	specs := []AggSpec{{Func: AggCount, InputCol: 2, Output: "cnt", TargetIndexResolved: true}}

	run := func(allKeysIndexResolved bool) (map[string]int64, int64) {
		a := NewBatchAggregation(s, []int{1}, specs, AggModeAll, 16, vectorized.NewMemoryTracker(1<<30), 0)
		a.SetAllKeysIndexResolved(allKeysIndexResolved)
		if err := a.Init(context.Background()); err != nil {
			t.Fatalf("Init: %v", err)
		}
		if err := a.Consume(context.Background(), build()); err != nil {
			t.Fatalf("Consume: %v", err)
		}
		got := make(map[string]int64)
		for _, b := range drainAgg(t, a) {
			gCol := b.Columns[0].(*vectorized.TypedColumn[string])
			cntCol := b.Columns[1].(*vectorized.TypedColumn[int64])
			for i := 0; i < b.Len; i++ {
				got[gCol.Data()[i]] = cntCol.Data()[i]
			}
		}
		folds := a.spanFolds
		if err := a.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return got, folds
	}

	fast, folds := run(true)
	if folds != 2 {
		t.Fatalf("spanFolds = %d, want 2 (one span per series)", folds)
	}
	want := map[string]int64{"A": 0, "B": 2}
	if !maps.Equal(fast, want) {
		t.Fatalf("fast-path COUNT = %v, want %v (an all-null span must count 0, not len(active))", fast, want)
	}
	ref, refFolds := run(false)
	if refFolds != 0 {
		t.Fatalf("reference (unoptimized) run unexpectedly took the span-fold path: spanFolds=%d", refFolds)
	}
	if !maps.Equal(ref, want) {
		t.Fatalf("per-row oracle COUNT = %v, want %v", ref, want)
	}
}
