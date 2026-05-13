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

package plan

import (
	"context"
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	measure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// fakePullSource emits a fixed sequence of RecordBatches then EOF.
type fakePullSource struct {
	schema  *vectorized.BatchSchema
	batches []*vectorized.RecordBatch
	idx     int
}

func (f *fakePullSource) Init(_ context.Context) error          { return nil }
func (f *fakePullSource) OutputSchema() *vectorized.BatchSchema { return f.schema }
func (f *fakePullSource) Close() error                          { return nil }
func (f *fakePullSource) NextBatch(_ context.Context) (*vectorized.RecordBatch, error) {
	if f.idx >= len(f.batches) {
		return nil, nil
	}
	b := f.batches[f.idx]
	f.idx++
	return b, nil
}

// buildScanInput constructs a BatchSchema + RecordBatch shaped like a Scan
// output: (timestamp, version, sid, shardID) + (svc tag) + (value field).
// The aggregation pipeline projects svc + value out of this layout.
func buildScanInput(t *testing.T) (*vectorized.BatchSchema, *vectorized.RecordBatch) {
	t.Helper()
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "value", Type: vectorized.ColumnTypeInt64},
	})
	b := vectorized.NewRecordBatch(schema, 5)
	pushRow := func(ts int64, svc string, v int64) {
		b.Columns[0].(*vectorized.TypedColumn[int64]).Append(ts)
		b.Columns[1].(*vectorized.TypedColumn[int64]).Append(1)
		b.Columns[2].(*vectorized.TypedColumn[int64]).Append(1)
		b.Columns[3].(*vectorized.TypedColumn[int64]).Append(0)
		b.Columns[4].(*vectorized.TypedColumn[string]).Append(svc)
		b.Columns[5].(*vectorized.TypedColumn[int64]).Append(v)
	}
	pushRow(1, "a", 1)
	pushRow(2, "b", 4)
	pushRow(3, "a", 2)
	pushRow(4, "a", 3)
	pushRow(5, "b", 5)
	b.Len = 5
	return schema, b
}

// drainPipeline pulls every batch from the pipeline and returns the slice
// of returned batches (caller must not modify them after this call).
func drainPipeline(t *testing.T, p *vectorized.Pipeline) []*vectorized.RecordBatch {
	t.Helper()
	var out []*vectorized.RecordBatch
	for {
		b, err := p.Next(context.Background())
		if err != nil {
			t.Fatalf("pipeline Next: %v", err)
		}
		if b == nil {
			break
		}
		out = append(out, b)
	}
	return out
}

func TestScanLimit_Build_EmitsSourceBatchesUnchanged(t *testing.T) {
	schema, batch := buildScanInput(t)
	src := &fakePullSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}

	scan := NewScan(schema, ScanParams{})
	scan.Source = src
	root := NewLimit(scan, 0, 10) // limit > total → no rows dropped

	tracker := vectorized.NewMemoryTracker(1 << 20)
	bc := &BuildContext{
		Builder: vectorized.NewPipelineBuilder().WithMemoryTracker(tracker),
		Tracker: tracker,
		Config:  measure.VectorizedConfig{BatchSize: 1024, QueryMemoryMiB: 1},
	}
	if buildErr := root.Build(context.Background(), bc); buildErr != nil {
		t.Fatalf("Build: %v", buildErr)
	}
	pipeline, err := bc.Builder.Build()
	if err != nil {
		t.Fatalf("PipelineBuilder.Build: %v", err)
	}
	if initErr := pipeline.Init(context.Background()); initErr != nil {
		t.Fatal(initErr)
	}
	batches := drainPipeline(t, pipeline)
	if len(batches) != 1 {
		t.Fatalf("want 1 batch, got %d", len(batches))
	}
	if batches[0].Len != 5 {
		t.Fatalf("want 5 rows, got %d", batches[0].Len)
	}
}

func TestScanGroupByAggLimit_Build_AggregatesByKey(t *testing.T) {
	schema, batch := buildScanInput(t)
	src := &fakePullSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}

	scan := NewScan(schema, ScanParams{})
	scan.Source = src
	gba, err := NewGroupByAgg(scan,
		&model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"svc"}},
		&model.MeasureAgg{FieldName: "value", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
	)
	if err != nil {
		t.Fatalf("NewGroupByAgg: %v", err)
	}
	root := NewLimit(gba, 0, 10)

	tracker := vectorized.NewMemoryTracker(1 << 20)
	bc := &BuildContext{
		Builder: vectorized.NewPipelineBuilder().WithMemoryTracker(tracker),
		Tracker: tracker,
		Config:  measure.VectorizedConfig{BatchSize: 1024, QueryMemoryMiB: 1},
	}
	if buildErr := root.Build(context.Background(), bc); buildErr != nil {
		t.Fatalf("Build: %v", buildErr)
	}
	pipeline, buildErr := bc.Builder.Build()
	if buildErr != nil {
		t.Fatalf("PipelineBuilder.Build: %v", buildErr)
	}
	if initErr := pipeline.Init(context.Background()); initErr != nil {
		t.Fatal(initErr)
	}
	batches := drainPipeline(t, pipeline)
	totalRows := 0
	for _, b := range batches {
		totalRows += b.Len
	}
	if totalRows != 2 {
		t.Fatalf("want 2 aggregated rows (2 groups), got %d", totalRows)
	}

	// Verify by-key sums: a=6, b=9.
	bySvc := map[string]int64{}
	for _, b := range batches {
		svcCol := b.Columns[0].(*vectorized.TypedColumn[string])
		sumCol := b.Columns[1].(*vectorized.TypedColumn[int64])
		for i := 0; i < b.Len; i++ {
			bySvc[svcCol.Data()[i]] = sumCol.Data()[i]
		}
	}
	if bySvc["a"] != 6 {
		t.Fatalf("sum(a): want 6, got %d", bySvc["a"])
	}
	if bySvc["b"] != 9 {
		t.Fatalf("sum(b): want 9, got %d", bySvc["b"])
	}
}

func TestScan_Build_MissingSource_Errors(t *testing.T) {
	schema, _ := buildScanInput(t)
	scan := NewScan(schema, ScanParams{}) // Source intentionally unset
	tracker := vectorized.NewMemoryTracker(1 << 20)
	bc := &BuildContext{
		Builder: vectorized.NewPipelineBuilder().WithMemoryTracker(tracker),
		Tracker: tracker,
		Config:  measure.VectorizedConfig{BatchSize: 1024, QueryMemoryMiB: 1},
	}
	if err := scan.Build(context.Background(), bc); err == nil {
		t.Fatal("Build with nil Source must error")
	}
}

func TestGroupByAgg_Schema_DropsTimestampAddsAggField(t *testing.T) {
	schema, _ := buildScanInput(t)
	src := &fakePullSource{schema: schema}
	scan := NewScan(schema, ScanParams{})
	scan.Source = src
	gba, err := NewGroupByAgg(scan,
		&model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"svc"}},
		&model.MeasureAgg{FieldName: "value", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
	)
	if err != nil {
		t.Fatalf("NewGroupByAgg: %v", err)
	}
	out := gba.Schema()
	if out == nil {
		t.Fatal("GroupByAgg.Schema must not be nil after BuildOperators resolves it")
	}
	if out.TimestampIndex() >= 0 {
		t.Fatalf("aggregation output must drop timestamp (D2); got index %d", out.TimestampIndex())
	}
	// 2 columns: svc key + value_sum agg result.
	if len(out.Columns) != 2 {
		t.Fatalf("want 2 output columns (key + agg result), got %d", len(out.Columns))
	}
	if out.Columns[0].Name != "svc" {
		t.Fatalf("col 0 should be the svc key, got %s", out.Columns[0].Name)
	}
	if out.Columns[1].Name != "value_sum" {
		t.Fatalf("col 1 should be value_sum, got %s", out.Columns[1].Name)
	}
}
