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
	"fmt"
	"math"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// scalarPartialSchema is "shard_id, tag.default.g, field out (sum partial)"
// — the AggModeMap partial shape for a scalar (non-bucketed) tag GroupBy +
// Agg.
func scalarPartialSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "out", Type: vectorized.ColumnTypeInt64},
	})
}

// scalarPartialRow is one input row for scalarPartialBatch. shard is unique
// per row so AggModeReduce's replica dedup (keyed on shard_id + group_key,
// see markDedupSeen) never collapses two of these rows into one.
type scalarPartialRow struct {
	g     string
	shard int64
	v     int64
}

func scalarPartialBatch(rows []scalarPartialRow) *vectorized.RecordBatch {
	s := scalarPartialSchema()
	b := vectorized.NewRecordBatch(s, len(rows))
	shardCol := b.Columns[0].(*vectorized.TypedColumn[int64])
	gCol := b.Columns[1].(*vectorized.TypedColumn[string])
	vCol := b.Columns[2].(*vectorized.TypedColumn[int64])
	for _, r := range rows {
		shardCol.Append(r.shard)
		gCol.Append(r.g)
		vCol.Append(r.v)
	}
	b.Len = len(rows)
	return b
}

// sumReducedByGroup sums the "out" field of every reduced batch, keyed by
// the "g" tag — the scalar-reduce output layout is [tag g, field out] (no
// shard column; AggModeReduce never emits one).
func sumReducedByGroup(t *testing.T, batches []*vectorized.RecordBatch) map[string]int64 {
	t.Helper()
	got := map[string]int64{}
	for _, b := range batches {
		gCol := b.Columns[0].(*vectorized.TypedColumn[string])
		vCol := b.Columns[1].(*vectorized.TypedColumn[int64])
		for i := 0; i < b.Len; i++ {
			got[gCol.Data()[i]] += vCol.Data()[i]
		}
	}
	return got
}

// TestReducePartialBatches_OverUint16Boundary_SumsCorrectly is design §5's
// gate (§9 case (f)): a single partial spanning more than math.MaxUint16
// rows must reduce to the same per-group sums as the same data split across
// several smaller partials.
//
// Before the chunking fix, BatchAggregation.Consume drives its per-row loop
// from activeIndices, which materializes a nil Selection as []uint16 across
// [0, b.Len). A uint16 fully covers indices [0, maxSafeLen) — 65,536 values,
// 0..65535 — so a batch of exactly maxSafeLen rows is still safe; totalRows
// here is one past that (maxSafeLen+1 = 65,537 rows), which is the smallest
// batch that actually wraps: index 65536 (the last row) overflows to 0, so
// activeIndices visits physical row 0 twice and physical row 65536 not at
// all. Row values are distinct per row (v = i+1) specifically so this
// cannot cancel out even where row 0 and row 65536 land in the same group:
// the duplicated contribution (1) and the dropped one (65537) are far
// apart, so the affected group's sum comes out wrong by a large amount
// rather than merely "off by one". The reduce does not error on this — it
// must be checked on value.
func TestReducePartialBatches_OverUint16Boundary_SumsCorrectly(t *testing.T) {
	const maxSafeLen = math.MaxUint16 + 1 // 65536: largest nil-Selection batch length that does not wrap.
	const totalRows = maxSafeLen + 1      // one past the boundary.
	const numGroups = 3

	rows := make([]scalarPartialRow, totalRows)
	want := map[string]int64{}
	for i := range rows {
		g := fmt.Sprintf("g%d", i%numGroups)
		v := int64(i) + 1
		rows[i] = scalarPartialRow{shard: int64(i), g: g, v: v}
		want[g] += v
	}

	t.Run("single_oversized_partial", func(t *testing.T) {
		batches, _, err := ReducePartialBatches(
			[]*vectorized.RecordBatch{scalarPartialBatch(rows)},
			[]string{"g"},
			[]AggReduceSpec{{OutputName: "out", Func: AggSum}},
			1024, vectorized.NewMemoryTracker(1<<30),
		)
		if err != nil {
			t.Fatalf("ReducePartialBatches: %v", err)
		}
		got := sumReducedByGroup(t, batches)
		for g, wantSum := range want {
			if got[g] != wantSum {
				t.Errorf("sum[%s] = %d, want %d (full result: %v)", g, got[g], wantSum, got)
			}
		}
		if len(got) != len(want) {
			t.Fatalf("result set size = %d, want %d: %v", len(got), len(want), got)
		}
	})

	t.Run("same_data_split_across_smaller_partials", func(t *testing.T) {
		const chunk = 8000 // several partials, each well under batchSize's own bound.
		var partials []*vectorized.RecordBatch
		for start := 0; start < len(rows); start += chunk {
			end := min(start+chunk, len(rows))
			partials = append(partials, scalarPartialBatch(rows[start:end]))
		}
		batches, _, err := ReducePartialBatches(
			partials,
			[]string{"g"},
			[]AggReduceSpec{{OutputName: "out", Func: AggSum}},
			1024, vectorized.NewMemoryTracker(1<<30),
		)
		if err != nil {
			t.Fatalf("ReducePartialBatches: %v", err)
		}
		got := sumReducedByGroup(t, batches)
		for g, wantSum := range want {
			if got[g] != wantSum {
				t.Errorf("sum[%s] = %d, want %d (full result: %v)", g, got[g], wantSum, got)
			}
		}
		if len(got) != len(want) {
			t.Fatalf("result set size = %d, want %d: %v", len(got), len(want), got)
		}
	})
}

// reducedTestSchema is the AggModeReduce output shape ApplyTopToReduce
// consumes: one tag key column plus a field value column, no shard column
// (AggModeReduce never emits one).
func reducedTestSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "out", Type: vectorized.ColumnTypeInt64},
	})
}

func reducedTestBatch(groups []string, values []int64) *vectorized.RecordBatch {
	s := reducedTestSchema()
	b := vectorized.NewRecordBatch(s, len(groups))
	gCol := b.Columns[0].(*vectorized.TypedColumn[string])
	vCol := b.Columns[1].(*vectorized.TypedColumn[int64])
	for i := range groups {
		gCol.Append(groups[i])
		vCol.Append(values[i])
	}
	b.Len = len(groups)
	return b
}

func collectReducedValues(batches []*vectorized.RecordBatch) []int64 {
	var out []int64
	for _, b := range batches {
		vCol := b.Columns[1].(*vectorized.TypedColumn[int64])
		for i := 0; i < b.Len; i++ {
			out = append(out, vCol.Data()[i])
		}
	}
	return out
}

// TestApplyTopToReduce_OverUint16Boundary_SelectsCorrectTopN is this
// follow-up's §9(f)-equivalent gate for ApplyTopToReduce. reduced here is
// normally ReduceRawFrames/ReducePartialBatches's own NextBatch-paginated
// output, so this builds the oversized batch directly rather than through
// a reduce, to prove ApplyTopToReduce is safe on its own terms rather than
// only because its current callers happen to always pre-page their input.
//
// Before this function chunked its own input, BatchTop.Consume drove its
// per-row loop from activeIndices directly on the whole oversized batch.
// Design §5's guard in activeIndices turns that into a hard error rather
// than a silently wrong top-N (verified: reverting this function's
// chunking reproduces exactly that error on this test). After chunking,
// the correct top-3 by value is selected regardless.
func TestApplyTopToReduce_OverUint16Boundary_SelectsCorrectTopN(t *testing.T) {
	const totalRows = math.MaxUint16 + 2 // one past the []uint16 selection boundary.
	groups := make([]string, totalRows)
	values := make([]int64, totalRows)
	for i := range groups {
		groups[i] = fmt.Sprintf("g%d", i)
		values[i] = int64(i)
	}
	batch := reducedTestBatch(groups, values)

	topped, topErr := ApplyTopToReduce(
		[]*vectorized.RecordBatch{batch},
		ReduceTopSpec{FieldName: "out", N: 3, Asc: false},
		1024,
	)
	if topErr != nil {
		t.Fatalf("ApplyTopToReduce: %v", topErr)
	}
	got := collectReducedValues(topped)
	want := []int64{totalRows - 1, totalRows - 2, totalRows - 3}
	if len(got) != len(want) {
		t.Fatalf("top-3: got %d rows, want %d: %v", len(got), len(want), got)
	}
	for i, wantVal := range want {
		if got[i] != wantVal {
			t.Fatalf("top-3 row %d: got value=%d, want %d (all: %v)", i, got[i], wantVal, got)
		}
	}
}
