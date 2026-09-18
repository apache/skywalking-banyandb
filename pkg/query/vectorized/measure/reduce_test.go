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
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// bucketPartialSchema is "shard_id, timestamp (bucket), tag.default.g,
// field out (sum partial)" — the AggModeMap partial shape for a bucketed
// tag GroupBy + Agg (design §7.2's leading-timestamp-key layout).
func bucketPartialSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "out", Type: vectorized.ColumnTypeInt64},
	})
}

// bucketPartialRow is one input row for bucketPartialBatch.
type bucketPartialRow struct {
	g     string
	shard int64
	ts    int64
	v     int64
}

func bucketPartialBatch(rows ...bucketPartialRow) *vectorized.RecordBatch {
	s := bucketPartialSchema()
	b := vectorized.NewRecordBatch(s, len(rows))
	shardCol := b.Columns[0].(*vectorized.TypedColumn[int64])
	tsCol := b.Columns[1].(*vectorized.TypedColumn[int64])
	gCol := b.Columns[2].(*vectorized.TypedColumn[string])
	vCol := b.Columns[3].(*vectorized.TypedColumn[int64])
	for _, r := range rows {
		shardCol.Append(r.shard)
		tsCol.Append(r.ts)
		gCol.Append(r.g)
		vCol.Append(r.v)
	}
	b.Len = len(rows)
	return b
}

// TestResolveKeyIndices_Bucketed_ResolvesTimestampAsLeadingKey pins design
// §7.2's distributed seam: bucketed=true resolves the RoleTimestamp column
// index ahead of any tag key indices.
func TestResolveKeyIndices_Bucketed_ResolvesTimestampAsLeadingKey(t *testing.T) {
	s := bucketPartialSchema()
	idx, err := resolveKeyIndices(s, []string{"g"}, true)
	if err != nil {
		t.Fatalf("resolveKeyIndices: %v", err)
	}
	if len(idx) != 2 || idx[0] != 1 || idx[1] != 2 {
		t.Fatalf("keyIndices = %v, want [1 2] (timestamp then tag g)", idx)
	}
}

// TestResolveKeyIndices_BucketOnly_NoTagKeys pins the bucket-only GroupBy
// case: bucketed=true with no keyTagNames still resolves a non-nil
// keyIndices (grouping by bucket alone), not the scalar-reduce nil.
func TestResolveKeyIndices_BucketOnly_NoTagKeys(t *testing.T) {
	s := bucketPartialSchema()
	idx, err := resolveKeyIndices(s, nil, true)
	if err != nil {
		t.Fatalf("resolveKeyIndices: %v", err)
	}
	if len(idx) != 1 || idx[0] != 1 {
		t.Fatalf("keyIndices = %v, want [1] (timestamp only)", idx)
	}
}

// TestResolveKeyIndices_Bucketed_MissingTimestampColumn_Errors pins design
// §10's mixed-version guard: a bucketed reduce whose schema carries no
// RoleTimestamp column — an older data node ignored time_bucket — hard-errors
// rather than silently collapsing the series to one row per tag group.
func TestResolveKeyIndices_Bucketed_MissingTimestampColumn_Errors(t *testing.T) {
	s := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "out", Type: vectorized.ColumnTypeInt64},
	})
	if _, err := resolveKeyIndices(s, []string{"g"}, true); err == nil {
		t.Fatal("a bucketed reduce over a schema with no RoleTimestamp column must error")
	}
}

// TestResolveKeyIndices_NotBucketed_Unaffected pins that bucketed=false
// preserves the pre-existing behavior exactly (no timestamp resolution).
func TestResolveKeyIndices_NotBucketed_Unaffected(t *testing.T) {
	idx, err := resolveKeyIndices(bucketPartialSchema(), nil, false)
	if err != nil {
		t.Fatalf("resolveKeyIndices: %v", err)
	}
	if idx != nil {
		t.Fatalf("keyIndices = %v, want nil (scalar reduce, unaffected by an unrelated timestamp column)", idx)
	}
}

// TestReducePartialBatches_Bucketed_CombinesAcrossShards pins the
// distributed reduce end to end: two shards' partials for the same
// (bucket, tag) combine into one summed row, and a different bucket stays
// separate.
func TestReducePartialBatches_Bucketed_CombinesAcrossShards(t *testing.T) {
	p1 := bucketPartialBatch(
		bucketPartialRow{shard: 1, ts: 0, g: "a", v: 3},
		bucketPartialRow{shard: 1, ts: 1000, g: "a", v: 7},
	)
	p2 := bucketPartialBatch(
		bucketPartialRow{shard: 2, ts: 0, g: "a", v: 4},
	)
	batches, _, err := ReducePartialBatches(
		[]*vectorized.RecordBatch{p1, p2},
		[]string{"g"}, true,
		[]AggReduceSpec{{OutputName: "out", Func: AggSum}},
		64, vectorized.NewMemoryTracker(1<<30),
	)
	if err != nil {
		t.Fatalf("ReducePartialBatches: %v", err)
	}
	got := map[string]int64{}
	for _, b := range batches {
		tsCol := b.Columns[0].(*vectorized.TypedColumn[int64])
		gCol := b.Columns[1].(*vectorized.TypedColumn[string])
		vCol := b.Columns[2].(*vectorized.TypedColumn[int64])
		for i := 0; i < b.Len; i++ {
			got[formatBucketKey(tsCol.Data()[i], gCol.Data()[i])] = vCol.Data()[i]
		}
	}
	want := map[string]int64{"a|0": 7, "a|1000": 7}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("sum[%s] = %d, want %d (full result: %v)", k, got[k], v, got)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("result set size = %d, want %d: %v", len(got), len(want), got)
	}
}

// TestReducePartialBatches_Bucketed_GloballySortsAcrossNodePartials pins the
// review finding that motivated BatchAggregation.SortInsertionByBucket:
// each node partial arrives already bucket-ascending (design §7.2 streams
// per node), but consuming them frame by frame only yields piecewise
// ascending insertion order — node A's [2000, 3000] followed by node B's
// [1000, 2000] would insert as [2000, 3000, 1000] without a final sort.
// iteratorFromBatches's offset/limit pagination (design §7.5) requires the
// reduced result to be globally bucket-ascending, not just per-partial.
func TestReducePartialBatches_Bucketed_GloballySortsAcrossNodePartials(t *testing.T) {
	nodeA := bucketPartialBatch(
		bucketPartialRow{shard: 1, ts: 2000, g: "a", v: 2},
		bucketPartialRow{shard: 1, ts: 3000, g: "a", v: 3},
	)
	nodeB := bucketPartialBatch(
		bucketPartialRow{shard: 2, ts: 1000, g: "a", v: 1},
		bucketPartialRow{shard: 2, ts: 2000, g: "a", v: 20},
	)
	batches, _, err := ReducePartialBatches(
		[]*vectorized.RecordBatch{nodeA, nodeB},
		[]string{"g"}, true,
		[]AggReduceSpec{{OutputName: "out", Func: AggSum}},
		64, vectorized.NewMemoryTracker(1<<30),
	)
	if err != nil {
		t.Fatalf("ReducePartialBatches: %v", err)
	}
	var gotTS []int64
	for _, b := range batches {
		tsCol := b.Columns[0].(*vectorized.TypedColumn[int64])
		for i := 0; i < b.Len; i++ {
			gotTS = append(gotTS, tsCol.Data()[i])
		}
	}
	wantTS := []int64{1000, 2000, 3000}
	if len(gotTS) != len(wantTS) {
		t.Fatalf("bucket timestamps = %v, want %v", gotTS, wantTS)
	}
	for i, want := range wantTS {
		if gotTS[i] != want {
			t.Fatalf("bucket timestamps = %v, want globally ascending %v", gotTS, wantTS)
		}
	}
}

// TestReducePartialBatches_Bucketed_MixedVersionOldNodeFirst_Errors pins
// design §10: when the first non-empty partial lacks a RoleTimestamp
// column (an old node that ignored time_bucket), the reduce hard-errors
// immediately rather than silently reducing on tags alone.
func TestReducePartialBatches_Bucketed_MixedVersionOldNodeFirst_Errors(t *testing.T) {
	oldNodeSchema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "out", Type: vectorized.ColumnTypeInt64},
	})
	oldPartial := vectorized.NewRecordBatch(oldNodeSchema, 1)
	oldPartial.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	oldPartial.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	oldPartial.Columns[2].(*vectorized.TypedColumn[int64]).Append(5)
	oldPartial.Len = 1

	_, _, err := ReducePartialBatches(
		[]*vectorized.RecordBatch{oldPartial},
		[]string{"g"}, true,
		[]AggReduceSpec{{OutputName: "out", Func: AggSum}},
		64, vectorized.NewMemoryTracker(1<<30),
	)
	if err == nil {
		t.Fatal("a bucketed reduce whose first partial has no RoleTimestamp column must error")
	}
}

// TestReducePartialBatches_Bucketed_MixedVersionOldNodeSecond_Errors is the
// other ordering of the same mixed-version scenario: a new node's partial
// (with RoleTimestamp) defines the schema, and an old node's partial (without
// it) arrives later — caught by the existing schemaCompatible structural
// check rather than resolveKeyIndices.
func TestReducePartialBatches_Bucketed_MixedVersionOldNodeSecond_Errors(t *testing.T) {
	newPartial := bucketPartialBatch(bucketPartialRow{shard: 1, ts: 0, g: "a", v: 1})

	oldNodeSchema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "out", Type: vectorized.ColumnTypeInt64},
	})
	oldPartial := vectorized.NewRecordBatch(oldNodeSchema, 1)
	oldPartial.Columns[0].(*vectorized.TypedColumn[int64]).Append(2)
	oldPartial.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	oldPartial.Columns[2].(*vectorized.TypedColumn[int64]).Append(5)
	oldPartial.Len = 1

	_, _, err := ReducePartialBatches(
		[]*vectorized.RecordBatch{newPartial, oldPartial},
		[]string{"g"}, true,
		[]AggReduceSpec{{OutputName: "out", Func: AggSum}},
		64, vectorized.NewMemoryTracker(1<<30),
	)
	if err == nil {
		t.Fatal("a schema-mismatched later partial must error")
	}
}
