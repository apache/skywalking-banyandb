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
	"strings"
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// planSchema is the fixture schema for plan_test: one groupby-eligible tag,
// one agg-eligible field.
func planSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "value", Type: vectorized.ColumnTypeInt64},
	})
}

func TestBuildOperators_NoGroupByNoAgg_ReturnsEmpty(t *testing.T) {
	ops, err := BuildOperators(model.MeasureQueryOptions{}, planSchema(),
		vectorized.NewMemoryTracker(1<<20), 1024, AggModeAll)
	if err != nil {
		t.Fatalf("empty opts should not error: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("empty opts should produce no operators, got %d", len(ops))
	}
}

func TestBuildOperators_GroupByPlusAgg_EmitsBatchAggregation(t *testing.T) {
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"svc"}},
		Agg:     &model.MeasureAgg{FieldName: "value", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
	}
	ops, err := BuildOperators(opts, planSchema(), vectorized.NewMemoryTracker(1<<20), 1024, AggModeAll)
	if err != nil {
		t.Fatalf("BuildOperators error: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("GroupBy+Agg should emit 1 operator (BatchAggregation), got %d", len(ops))
	}
	if _, ok := ops[0].(*BatchAggregation); !ok {
		t.Fatalf("operator must be *BatchAggregation, got %T", ops[0])
	}
}

// TestBuildOperators_AggOutputName_InheritsInputFieldName pins the
// G8d.2 row-path-parity name: the agg result column reuses the input
// field name (e.g. "value") for every AggFunc, matching the row-path
// aggGroupIterator.Current() that emits a single DataPoint_Field named
// after the original input field. Any auto-derived "<field>_<func>"
// suffix would break proto.Equal parity in the integration suite.
func TestBuildOperators_AggOutputName_InheritsInputFieldName(t *testing.T) {
	fns := []modelv1.AggregationFunction{
		modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
		modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT,
		modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN,
		modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX,
		modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN,
	}
	const wantName = "value"
	for _, fn := range fns {
		opts := model.MeasureQueryOptions{
			GroupBy: &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"svc"}},
			Agg:     &model.MeasureAgg{FieldName: "value", Func: fn},
		}
		ops, err := BuildOperators(opts, planSchema(), vectorized.NewMemoryTracker(1<<20), 1024, AggModeAll)
		if err != nil {
			t.Fatalf("%v: BuildOperators error: %v", fn, err)
		}
		agg := ops[0].(*BatchAggregation)
		// Output schema layout: key columns then the agg result column.
		got := agg.OutputSchema().Columns[len(agg.OutputSchema().Columns)-1].Name
		if got != wantName {
			t.Fatalf("%v: want output column name %q (row-path parity), got %q", fn, wantName, got)
		}
	}
}

// TestBuildOperators_GroupByWithoutAgg_EmitsFirstOnlyGroupBy pins the
// raw-GroupBy shape: a first-seen-row-per-group BatchGroupBy whose output
// preserves the input schema. It matches the row path's groupIterator
// combined with processor.go's current[0] read.
func TestBuildOperators_GroupByWithoutAgg_EmitsFirstOnlyGroupBy(t *testing.T) {
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"svc"}},
	}
	ops, err := BuildOperators(opts, planSchema(), vectorized.NewMemoryTracker(1<<20), 1024, AggModeAll)
	if err != nil {
		t.Fatalf("GroupBy without Agg (raw groupby) must not error: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("raw GroupBy should emit 1 operator, got %d", len(ops))
	}
	gb, ok := ops[0].(*BatchGroupBy)
	if !ok {
		t.Fatalf("operator must be *BatchGroupBy, got %T", ops[0])
	}
	if !gb.firstOnly {
		t.Fatal("raw GroupBy must be first-only (one row per group)")
	}
}

// TestBuildTimeBucketOperator_BucketOnly_ReturnsBatchTimeBucket pins the
// bucket-only GroupBy shape (design §7.2): no tag key at all, grouping
// purely by the resolved bucket width. This exercises BuildTimeBucketOperator
// directly (opts.Agg nil) to pin the mechanical routing; plan/analyzer.go's
// Analyze rejects this combination for real requests today (a bucketed raw
// GroupBy has no execution support — BatchAggregation's empty-AggSpec output
// layout drops every projected field, unlike BatchGroupByFirst's
// full-schema passthrough for the non-bucketed case), so this shape isn't
// reachable outside a direct BuildTimeBucketOperator call like this one.
// upstream is nil: this test never calls Init/NextBatch on the result.
func TestBuildTimeBucketOperator_BucketOnly_ReturnsBatchTimeBucket(t *testing.T) {
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{TimeBucket: &model.MeasureTimeBucket{WidthNanos: 1000}},
	}
	bucket, err := BuildTimeBucketOperator(nil, opts, bucketTestSchema(), vectorized.NewMemoryTracker(1<<20), 1024, AggModeAll)
	if err != nil {
		t.Fatalf("BuildTimeBucketOperator: %v", err)
	}
	if bucket == nil {
		t.Fatal("want a non-nil *BatchTimeBucket")
	}
}

// TestBuildTimeBucketOperator_BucketPlusTagGroupByPlusAgg pins the combined
// shape: bucket + tag GroupBy + Agg all route to the same BatchTimeBucket
// operator (design §7.2), which internally binds the agg spec exactly like
// the non-bucketed path.
func TestBuildTimeBucketOperator_BucketPlusTagGroupByPlusAgg(t *testing.T) {
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{
			TagFamily:  "default",
			TagNames:   []string{"g"},
			TimeBucket: &model.MeasureTimeBucket{WidthNanos: 1000},
		},
		Agg: &model.MeasureAgg{FieldName: "v", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
	}
	bucket, err := BuildTimeBucketOperator(nil, opts, bucketTestSchema(), vectorized.NewMemoryTracker(1<<20), 1024, AggModeAll)
	if err != nil {
		t.Fatalf("BuildTimeBucketOperator: %v", err)
	}
	if bucket == nil {
		t.Fatal("want a non-nil *BatchTimeBucket")
	}
}

// TestBuildTimeBucketOperator_MissingTimestampColumn_Errors pins the
// defensive guard: a schema with no RoleTimestamp column (should never
// happen in production — BuildBatchSchema always emits one) is rejected
// rather than panicking downstream.
func TestBuildTimeBucketOperator_MissingTimestampColumn_Errors(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
	})
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{TimeBucket: &model.MeasureTimeBucket{WidthNanos: 1000}},
	}
	if _, err := BuildTimeBucketOperator(nil, opts, schema, vectorized.NewMemoryTracker(1<<20), 1024, AggModeAll); err == nil {
		t.Fatal("a schema with no RoleTimestamp column must error, not panic")
	}
}

// TestBuildTimeBucketOperator_CountDistinctTagTarget_HideTag pins the
// composition of two independently-shipped features: time-bucket grouping
// (#14089) and a tag-targeted COUNT_DISTINCT with HideTag (#14090). Two
// rows share the same target tag value ("entity_id") but differ on another
// carried-forward tag ("id") and land in the same bucket — the target's
// hidden tag column must not leak into the carried-forward tag set (design
// §5.2), and the distinct count must still collapse to 1.
func TestBuildTimeBucketOperator_CountDistinctTagTarget_HideTag(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "id", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "entity_id", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "value", Type: vectorized.ColumnTypeInt64},
	})
	b := vectorized.NewRecordBatch(schema, 2)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(13)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(47)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("off1")
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("off2")
	b.Columns[2].(*vectorized.TypedColumn[string]).Append("off_cadence_entity")
	b.Columns[2].(*vectorized.TypedColumn[string]).Append("off_cadence_entity")
	b.Columns[3].(*vectorized.TypedColumn[int64]).Append(10)
	b.Columns[3].(*vectorized.TypedColumn[int64]).Append(20)
	b.Len = 2

	upstream := &fakeBucketUpstream{schema: schema, batches: []*vectorized.RecordBatch{b}}
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{TimeBucket: &model.MeasureTimeBucket{WidthNanos: 1000}},
		Agg:     &model.MeasureAgg{TagFamily: "default", TagName: "entity_id", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT, HideTag: true},
	}
	tracker := vectorized.NewMemoryTracker(1 << 20)
	bucket, buildErr := BuildTimeBucketOperator(upstream, opts, schema, tracker, 8, AggModeAll)
	if buildErr != nil {
		t.Fatalf("BuildTimeBucketOperator: %v", buildErr)
	}
	if initErr := bucket.Init(context.Background()); initErr != nil {
		t.Fatal(initErr)
	}
	defer bucket.Close()
	out, nextErr := bucket.NextBatch(context.Background())
	if nextErr != nil {
		t.Fatal(nextErr)
	}
	if out == nil {
		t.Fatal("want a non-nil output batch")
	}
	// Output layout: [timestamp(bucket start), id(carried-forward tag),
	// entity_id(agg result field)] — entity_id must NOT also appear as a
	// separate tag column (HideTag).
	if len(out.Schema.Columns) != 3 {
		t.Fatalf("output column count = %d, want 3 (timestamp, id tag, entity_id field): %+v", len(out.Schema.Columns), out.Schema.Columns)
	}
	for _, col := range out.Schema.Columns {
		if col.Role == vectorized.RoleTag && col.Name == "entity_id" {
			t.Fatalf("HideTag must exclude entity_id from the carried-forward tag set, found %+v", col)
		}
	}
	valCol := out.Columns[2].(*vectorized.TypedColumn[int64])
	if got := valCol.Data()[0]; got != 1 {
		t.Fatalf("distinct entity_id = %d, want 1 (both rows share the same entity_id)", got)
	}
}

// countDistinctBucketedShardRow is one input row for
// TestBuildTimeBucketOperator_CountDistinctMapMode_SeparatesShards.
type countDistinctBucketedShardRow struct {
	g, target string
	ts, shard int64
}

// TestBuildTimeBucketOperator_CountDistinctMapMode_SeparatesShards is the
// bucketed sibling of TestCountDistinct_MapThenReduce_SumsDisjointShardsDedupsReplicas
// (aggregation_test.go), pinning the P1 review finding on the first
// version of this fix: BuildOperators' shard-id-in-keyIndices fix did not
// extend to BuildTimeBucketOperator, so a distributed bucketed
// COUNT_DISTINCT still merged a data node's shards into one
// incidentally-labeled partial per bucket — exactly the shape the fix
// exists to prevent for the unbucketed case. Two shards contribute
// disjoint target values inside the same bucket; the map phase must emit
// one partial per (shard, bucket, group), not one merged partial per
// (bucket, group).
func TestBuildTimeBucketOperator_CountDistinctMapMode_SeparatesShards(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "target", Type: vectorized.ColumnTypeString},
	})
	b := vectorized.NewRecordBatch(schema, 4)
	tsCol := b.Columns[0].(*vectorized.TypedColumn[int64])
	shardCol := b.Columns[1].(*vectorized.TypedColumn[int64])
	gCol := b.Columns[2].(*vectorized.TypedColumn[string])
	targetCol := b.Columns[3].(*vectorized.TypedColumn[string])
	for _, row := range []countDistinctBucketedShardRow{
		{ts: 13, shard: 1, g: "a", target: "v1"},
		{ts: 20, shard: 1, g: "a", target: "v2"},
		{ts: 30, shard: 2, g: "a", target: "v3"},
		{ts: 40, shard: 2, g: "a", target: "v4"},
	} {
		tsCol.Append(row.ts)
		shardCol.Append(row.shard)
		gCol.Append(row.g)
		targetCol.Append(row.target)
	}
	b.Len = 4

	upstream := &fakeBucketUpstream{schema: schema, batches: []*vectorized.RecordBatch{b}}
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"g"}, TimeBucket: &model.MeasureTimeBucket{WidthNanos: 1000}},
		Agg:     &model.MeasureAgg{TagFamily: "default", TagName: "target", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT, HideTag: true},
	}
	tracker := vectorized.NewMemoryTracker(1 << 20)
	bucket, buildErr := BuildTimeBucketOperator(upstream, opts, schema, tracker, 8, AggModeMap)
	if buildErr != nil {
		t.Fatalf("BuildTimeBucketOperator: %v", buildErr)
	}
	if initErr := bucket.Init(context.Background()); initErr != nil {
		t.Fatal(initErr)
	}
	defer bucket.Close()

	// Output layout for AggModeMap + bucketed: [shard_id(0), timestamp(1,
	// bucket start), g(2, tag), target(3, agg result field)].
	gotByShard := map[int64]int64{}
	for {
		out, nextErr := bucket.NextBatch(context.Background())
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		if out == nil {
			break
		}
		shardOut := out.Columns[0].(*vectorized.TypedColumn[int64])
		valOut := out.Columns[3].(*vectorized.TypedColumn[int64])
		for i := 0; i < out.Len; i++ {
			gotByShard[shardOut.Data()[i]] = valOut.Data()[i]
		}
	}
	want := map[int64]int64{1: 2, 2: 2}
	if len(gotByShard) != len(want) {
		t.Fatalf("got %d distinct-per-shard partials, want %d (one per shard): %v", len(gotByShard), len(want), gotByShard)
	}
	for shard, wantCount := range want {
		if got := gotByShard[shard]; got != wantCount {
			t.Errorf("shard %d distinct count = %d, want %d", shard, got, wantCount)
		}
	}
}

// TestBuildOperators_AggWithoutGroupBy_EmitsBatchAggregation pins the
// scalar-reduce shape: a BatchAggregation with no key columns, so every
// row collapses into a single output row carrying the first-seen tags
// plus the agg result, matching the row path's aggAllIterator.
func TestBuildOperators_AggWithoutGroupBy_EmitsBatchAggregation(t *testing.T) {
	opts := model.MeasureQueryOptions{
		Agg: &model.MeasureAgg{FieldName: "value", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
	}
	ops, err := BuildOperators(opts, planSchema(), vectorized.NewMemoryTracker(1<<20), 1024, AggModeAll)
	if err != nil {
		t.Fatalf("Agg without GroupBy (scalar reduce) must not error: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("scalar reduce should emit 1 operator, got %d", len(ops))
	}
	agg, ok := ops[0].(*BatchAggregation)
	if !ok {
		t.Fatalf("operator must be *BatchAggregation, got %T", ops[0])
	}
	if len(agg.keyIndices) != 0 {
		t.Fatalf("scalar reduce must have no key columns, got %d", len(agg.keyIndices))
	}
}

func TestBuildOperators_UnknownGroupByTag_Errors(t *testing.T) {
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"missing"}},
		Agg:     &model.MeasureAgg{FieldName: "value", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
	}
	_, err := BuildOperators(opts, planSchema(), vectorized.NewMemoryTracker(1<<20), 1024, AggModeAll)
	if err == nil {
		t.Fatal("unknown groupby tag must error")
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Fatalf("error should name the missing tag, got %v", err)
	}
}

func TestBuildOperators_UnknownAggField_Errors(t *testing.T) {
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"svc"}},
		Agg:     &model.MeasureAgg{FieldName: "ghost", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
	}
	_, err := BuildOperators(opts, planSchema(), vectorized.NewMemoryTracker(1<<20), 1024, AggModeAll)
	if err == nil {
		t.Fatal("unknown agg field must error")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("error should name the missing field, got %v", err)
	}
}

func TestBuildOperators_AggUnspecified_Errors(t *testing.T) {
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"svc"}},
		Agg:     &model.MeasureAgg{FieldName: "value", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED},
	}
	_, err := BuildOperators(opts, planSchema(), vectorized.NewMemoryTracker(1<<20), 1024, AggModeAll)
	if err == nil {
		t.Fatal("UNSPECIFIED Agg.Func must error")
	}
}

func TestBuildOperators_NilTracker_Errors(t *testing.T) {
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"svc"}},
		Agg:     &model.MeasureAgg{FieldName: "value", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
	}
	_, err := BuildOperators(opts, planSchema(), nil, 1024, AggModeAll)
	if err == nil {
		t.Fatal("nil tracker must error when operators are emitted")
	}
}

func TestBuildOperators_MultiKeyGroupBy_PreservesKeyOrder(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "region", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "value", Type: vectorized.ColumnTypeInt64},
	})
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"region", "svc"}},
		Agg:     &model.MeasureAgg{FieldName: "value", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
	}
	ops, err := BuildOperators(opts, schema, vectorized.NewMemoryTracker(1<<20), 1024, AggModeAll)
	if err != nil {
		t.Fatalf("BuildOperators error: %v", err)
	}
	agg := ops[0].(*BatchAggregation)
	// First two output columns are the keys, in TagNames order.
	out := agg.OutputSchema().Columns
	if out[0].Name != "region" || out[1].Name != "svc" {
		t.Fatalf("output columns 0/1 should be region/svc (TagNames order), got %s/%s", out[0].Name, out[1].Name)
	}
}
