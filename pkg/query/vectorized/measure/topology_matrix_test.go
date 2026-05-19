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
	"sort"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// TestTopologyMatrix is the load-bearing G9f.3 proof: Map+Reduce produces
// the same final values as a single-node AggModeAll for every (shards,
// replicas) topology in a 6×6 = 36 cell grid. It is the partial-oracle
// harness specified in the G9f spec (Principle 4): the row path / vec
// AggModeAll defines the truth, and the distributed vec pipeline must
// agree byte-for-byte (well, value-for-value) under any topology a
// production cluster can realise.
//
// Per cell:
//   - Split the source rows across S shards (rows i → shard i%S).
//   - For each shard, run AggModeMap → encode via frame.Encode.
//   - Duplicate each shard's frame R times (replica duplicates — same
//     shard_id, same group keys, same partial values).
//   - ReduceRawFrames over the (S*R) frames.
//   - Assert the reduced output equals the AggModeAll oracle.
//
// The replica-duplication step is what stresses the (shard, group) dedup:
// without dedup, R replicas would multiply each shard's contribution R
// times. With dedup, the answer must be identical regardless of R.
//
// Aggs covered: SUM, COUNT, MIN, MAX, MEAN. Each runs as its own subtest;
// MEAN exercises the count sidecar + Reduce.Val finalisation.
func TestTopologyMatrix(t *testing.T) {
	rows := []topologyRow{
		{group: "a", value: 10},
		{group: "b", value: 20},
		{group: "a", value: 30},
		{group: "c", value: 40},
		{group: "b", value: 50},
		{group: "a", value: 60},
		{group: "c", value: 70},
		{group: "b", value: 80},
		{group: "a", value: 90},
		{group: "c", value: 100},
		{group: "b", value: 110},
		{group: "a", value: 120},
	}
	cases := []struct {
		name string
		fn   AggFunc
	}{
		{name: "SUM", fn: AggSum},
		{name: "COUNT", fn: AggCount},
		{name: "MIN", fn: AggMin},
		{name: "MAX", fn: AggMax},
		{name: "MEAN", fn: AggMean},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			oracle := computeOracle(t, rows, tc.fn)
			for shards := 1; shards <= 6; shards++ {
				for replicas := 1; replicas <= 6; replicas++ {
					name := fmt.Sprintf("S%d_R%d", shards, replicas)
					t.Run(name, func(t *testing.T) {
						frames := buildPartialFrames(t, rows, shards, replicas, tc.fn)
						got, reduceErr := ReduceRawFrames(frames,
							[]string{"g"},
							[]AggReduceSpec{{OutputName: "out", Func: tc.fn}},
							1024, vectorized.NewMemoryTracker(1<<30))
						if reduceErr != nil {
							t.Fatalf("ReduceRawFrames: %v", reduceErr)
						}
						gotMap := flattenReduce(got)
						if !mapsEqual(gotMap, oracle) {
							t.Fatalf("Reduce mismatch:\n  got    %v\n  oracle %v", gotMap, oracle)
						}
					})
				}
			}
		})
	}
}

// topologyRow is a row in the synthetic source dataset — one tag column
// "g" and one int64 value column "v".
type topologyRow struct {
	group string
	value int64
}

// topologyRawSchema is the (g, v) input layout that the Map operator
// consumes. Built fresh per call so each operator owns its schema.
func topologyRawSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
}

// computeOracle runs an AggModeAll BatchAggregation on the FULL row set
// and returns the canonical group→final-value map the distributed path
// must reproduce. MEAN returns float64; the other aggs return int64; both
// are normalised to a stringly-typed value for cross-comparison.
func computeOracle(t *testing.T, rows []topologyRow, fn AggFunc) map[string]string {
	t.Helper()
	s := topologyRawSchema()
	op := NewBatchAggregation(s, []int{1},
		[]AggSpec{{Func: fn, InputCol: 2, Output: "out"}},
		AggModeAll, 1024, vectorized.NewMemoryTracker(1<<30), 0)
	defer op.Close()
	if initErr := op.Init(context.Background()); initErr != nil {
		t.Fatalf("oracle init: %v", initErr)
	}
	b := vectorized.NewRecordBatch(s, len(rows))
	shardCol := b.Columns[0].(*vectorized.TypedColumn[int64])
	gCol := b.Columns[1].(*vectorized.TypedColumn[string])
	vCol := b.Columns[2].(*vectorized.TypedColumn[int64])
	for _, r := range rows {
		shardCol.Append(0)
		gCol.Append(r.group)
		vCol.Append(r.value)
	}
	b.Len = len(rows)
	if consumeErr := op.Consume(context.Background(), b); consumeErr != nil {
		t.Fatalf("oracle consume: %v", consumeErr)
	}
	if finalErr := op.Finalize(context.Background()); finalErr != nil {
		t.Fatalf("oracle finalize: %v", finalErr)
	}
	out := make(map[string]string)
	for {
		nb, nextErr := op.NextBatch(context.Background())
		if nextErr != nil {
			t.Fatalf("oracle next: %v", nextErr)
		}
		if nb == nil {
			break
		}
		// AggModeAll output layout: tags first (tagOutOffset=0), then agg
		// value at aggOutOffsets[0]. The schema mirrors topologyRawSchema:
		// shard_id is RoleShardID so it counts as a tag-adjacent column.
		// However collectTagIndices specifically picks RoleTag columns
		// only, so col 0 of output is "g" (RoleTag) and col 1 is "out".
		groupVals := nb.Columns[0].(*vectorized.TypedColumn[string]).Data()
		for r := 0; r < nb.Len; r++ {
			key := groupVals[r]
			out[key] = columnStringAt(nb.Columns[1], r)
		}
	}
	return out
}

// buildPartialFrames slices rows across S shards, runs AggModeMap per
// shard, encodes the result, then duplicates each shard's frame R times
// to simulate replica responses. Empty shards (no rows assigned) produce
// no frame — matches the codec-empty carve-out.
func buildPartialFrames(t *testing.T, rows []topologyRow, shards, replicas int, fn AggFunc) [][]byte {
	t.Helper()
	perShard := make([][]topologyRow, shards)
	for i, r := range rows {
		idx := i % shards
		perShard[idx] = append(perShard[idx], r)
	}
	var out [][]byte
	for shardID := range shards {
		srows := perShard[shardID]
		if len(srows) == 0 {
			continue
		}
		body := encodeShardPartial(t, int64(shardID+1), srows, fn)
		for range replicas {
			out = append(out, body)
		}
	}
	return out
}

// encodeShardPartial builds one shard's input batch (with shard_id stamped
// to the shard's id), runs AggModeMap, takes the first emitted batch (the
// only one for these dataset sizes), and frame.Encode's it.
func encodeShardPartial(t *testing.T, shardID int64, rows []topologyRow, fn AggFunc) []byte {
	t.Helper()
	s := topologyRawSchema()
	op := NewBatchAggregation(s, []int{1},
		[]AggSpec{{Func: fn, InputCol: 2, Output: "out"}},
		AggModeMap, 1024, vectorized.NewMemoryTracker(1<<30), 0)
	defer op.Close()
	if initErr := op.Init(context.Background()); initErr != nil {
		t.Fatalf("map init: %v", initErr)
	}
	b := vectorized.NewRecordBatch(s, len(rows))
	shardCol := b.Columns[0].(*vectorized.TypedColumn[int64])
	gCol := b.Columns[1].(*vectorized.TypedColumn[string])
	vCol := b.Columns[2].(*vectorized.TypedColumn[int64])
	for _, r := range rows {
		shardCol.Append(shardID)
		gCol.Append(r.group)
		vCol.Append(r.value)
	}
	b.Len = len(rows)
	if consumeErr := op.Consume(context.Background(), b); consumeErr != nil {
		t.Fatalf("map consume: %v", consumeErr)
	}
	if finalErr := op.Finalize(context.Background()); finalErr != nil {
		t.Fatalf("map finalize: %v", finalErr)
	}
	out, nextErr := op.NextBatch(context.Background())
	if nextErr != nil {
		t.Fatalf("map next: %v", nextErr)
	}
	if out == nil {
		t.Fatal("map next returned nil for non-empty input")
	}
	body, encErr := frame.Encode(out)
	if encErr != nil {
		t.Fatalf("frame.Encode: %v", encErr)
	}
	return body
}

// flattenReduce converts the reduce output (group, out) into a string→string
// map for comparison against the oracle. The reduce output schema after
// Reduce is [tags..., agg values...] — no shard_id, no count sidecar.
func flattenReduce(batches []*vectorized.RecordBatch) map[string]string {
	out := make(map[string]string)
	for _, b := range batches {
		// First RoleTag column is "g"; first RoleField column is "out".
		var gIdx, outIdx = -1, -1
		for i, def := range b.Schema.Columns {
			if def.Role == vectorized.RoleTag && gIdx < 0 {
				gIdx = i
			}
			if def.Role == vectorized.RoleField && outIdx < 0 {
				outIdx = i
			}
		}
		groupVals := b.Columns[gIdx].(*vectorized.TypedColumn[string]).Data()
		for r := 0; r < b.Len; r++ {
			out[groupVals[r]] = columnStringAt(b.Columns[outIdx], r)
		}
	}
	return out
}

// mapsEqual compares two group→value maps for equality, with stable
// failure output (sorted by key).
func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	keys := make([]string, 0, len(a))
	for k := range a {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		va, okA := a[k]
		vb, okB := b[k]
		if !okA || !okB {
			return false
		}
		if !numericallyEqual(va, vb) {
			return false
		}
	}
	return true
}

// numericallyEqual compares two string-formatted numeric values for
// equality, tolerating int vs float rendering differences (e.g. "10" vs
// "10" — AggModeAll renders int as "%d" and float as "%g", so the oracle
// and reduce paths must agree on the type, which they do by construction).
func numericallyEqual(a, b string) bool {
	return strings.EqualFold(a, b)
}

// TestTopologyMatrix_WithTop is the G9f.4 proof: distributed Top-over-Agg
// (the case the previous :281 narrowing fell back to row) now passes
// through the vec stack end-to-end. Per cell, the data nodes emit Map
// partials (one row per (shard, group)), the liaison Reduces them and
// applies a final BatchTop. The oracle is AggModeAll over the full
// dataset followed by the same BatchTop — both must converge to the
// same top-N rows, irrespective of (shards, replicas) topology.
//
// Covered: SUM ASC/DESC with N ∈ {1, 3, 6}. Replica duplication tests the
// (shard, group) dedup; cross-shard combination tests Reduce's
// aggregation.Reduce[N].Combine semantics; the final BatchTop tests
// ApplyTopToReduce's bind-by-name + heap consistency.
func TestTopologyMatrix_WithTop(t *testing.T) {
	rows := []topologyRow{
		{group: "a", value: 10}, {group: "b", value: 20},
		{group: "c", value: 30}, {group: "d", value: 40},
		{group: "e", value: 50}, {group: "f", value: 60},
		{group: "a", value: 100}, {group: "b", value: 100},
		{group: "c", value: 100}, {group: "d", value: 100},
		{group: "e", value: 100}, {group: "f", value: 100},
	}
	cases := []struct {
		name string
		topN int
		asc  bool
	}{
		{name: "Top1_DESC", topN: 1, asc: false},
		{name: "Top3_DESC", topN: 3, asc: false},
		{name: "Top6_DESC", topN: 6, asc: false},
		{name: "Top1_ASC", topN: 1, asc: true},
		{name: "Top3_ASC", topN: 3, asc: true},
		{name: "Top6_ASC", topN: 6, asc: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			oracle := computeOracleWithTop(t, rows, AggSum, tc.topN, tc.asc)
			for shards := 1; shards <= 6; shards++ {
				for replicas := 1; replicas <= 6; replicas++ {
					name := fmt.Sprintf("S%d_R%d", shards, replicas)
					t.Run(name, func(t *testing.T) {
						frames := buildPartialFrames(t, rows, shards, replicas, AggSum)
						reduced, reduceErr := ReduceRawFrames(frames,
							[]string{"g"},
							[]AggReduceSpec{{OutputName: "out", Func: AggSum}},
							1024, vectorized.NewMemoryTracker(1<<30))
						if reduceErr != nil {
							t.Fatalf("ReduceRawFrames: %v", reduceErr)
						}
						topped, topErr := ApplyTopToReduce(reduced,
							ReduceTopSpec{FieldName: "out", N: tc.topN, Asc: tc.asc}, 1024)
						if topErr != nil {
							t.Fatalf("ApplyTopToReduce: %v", topErr)
						}
						got := flattenReduce(topped)
						if !mapsEqual(got, oracle) {
							t.Fatalf("Top-Reduce mismatch:\n  got    %v\n  oracle %v", got, oracle)
						}
					})
				}
			}
		})
	}
}

// computeOracleWithTop runs an AggModeAll + BatchTop over the full row
// set and returns the canonical top-N group→value map.
func computeOracleWithTop(t *testing.T, rows []topologyRow, fn AggFunc, topN int, asc bool) map[string]string {
	t.Helper()
	s := topologyRawSchema()
	op := NewBatchAggregation(s, []int{1},
		[]AggSpec{{Func: fn, InputCol: 2, Output: "out"}},
		AggModeAll, 1024, vectorized.NewMemoryTracker(1<<30), 0)
	defer op.Close()
	if initErr := op.Init(context.Background()); initErr != nil {
		t.Fatalf("oracle init: %v", initErr)
	}
	b := vectorized.NewRecordBatch(s, len(rows))
	shardCol := b.Columns[0].(*vectorized.TypedColumn[int64])
	gCol := b.Columns[1].(*vectorized.TypedColumn[string])
	vCol := b.Columns[2].(*vectorized.TypedColumn[int64])
	for _, r := range rows {
		shardCol.Append(0)
		gCol.Append(r.group)
		vCol.Append(r.value)
	}
	b.Len = len(rows)
	if consumeErr := op.Consume(context.Background(), b); consumeErr != nil {
		t.Fatalf("oracle consume: %v", consumeErr)
	}
	if finalErr := op.Finalize(context.Background()); finalErr != nil {
		t.Fatalf("oracle finalize: %v", finalErr)
	}
	var batches []*vectorized.RecordBatch
	for {
		nb, nextErr := op.NextBatch(context.Background())
		if nextErr != nil {
			t.Fatalf("oracle next: %v", nextErr)
		}
		if nb == nil {
			break
		}
		batches = append(batches, nb)
	}
	topped, topErr := ApplyTopToReduce(batches,
		ReduceTopSpec{FieldName: "out", N: topN, Asc: asc}, 1024)
	if topErr != nil {
		t.Fatalf("oracle top: %v", topErr)
	}
	return flattenReduce(topped)
}
