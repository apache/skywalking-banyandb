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
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// TestAggregation_EndToEnd_BatchAggregationToProto exercises the full
// planner → operator → serialize path against a known dataset. Verifies the
// final wire shape (InternalDataPoint) matches D2 (nil timestamp) and that
// group keys reappear as tags while aggregation results appear as fields.
func TestAggregation_EndToEnd_BatchAggregationToProto(t *testing.T) {
	inputSchema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "value", Type: vectorized.ColumnTypeInt64},
	})
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"svc"}},
		Agg:     &model.MeasureAgg{FieldName: "value", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
	}

	tracker := vectorized.NewMemoryTracker(1 << 20)
	ops, err := BuildOperators(opts, inputSchema, tracker, 1024)
	if err != nil {
		t.Fatalf("BuildOperators: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("want 1 operator, got %d", len(ops))
	}
	agg := ops[0].(*BatchAggregation)
	if initErr := agg.Init(context.Background()); initErr != nil {
		t.Fatalf("agg init: %v", initErr)
	}
	defer agg.Close()

	// Feed 5 rows: 3×"a" (sum=6), 2×"b" (sum=9).
	in := vectorized.NewRecordBatch(inputSchema, 5)
	pushRow := func(ts int64, svc string, v int64) {
		in.Columns[0].(*vectorized.TypedColumn[int64]).Append(ts)
		in.Columns[1].(*vectorized.TypedColumn[string]).Append(svc)
		in.Columns[2].(*vectorized.TypedColumn[int64]).Append(v)
	}
	pushRow(1, "a", 1)
	pushRow(2, "b", 4)
	pushRow(3, "a", 2)
	pushRow(4, "a", 3)
	pushRow(5, "b", 5)
	in.Len = 5

	if consumeErr := agg.Consume(context.Background(), in); consumeErr != nil {
		t.Fatalf("agg consume: %v", consumeErr)
	}
	if finalizeErr := agg.Finalize(context.Background()); finalizeErr != nil {
		t.Fatalf("agg finalize: %v", finalizeErr)
	}

	out, pullErr := agg.NextBatch(context.Background())
	if pullErr != nil {
		t.Fatalf("agg nextbatch: %v", pullErr)
	}
	if out == nil || out.Len != 2 {
		t.Fatalf("want 2 output rows (2 groups), got %v", out)
	}

	// Egress: convert to protobuf and verify the wire shape.
	dps := serializeBatchToProto(out, nil)
	if len(dps) != 2 {
		t.Fatalf("want 2 InternalDataPoints, got %d", len(dps))
	}

	// Index by svc tag for deterministic assertions.
	bySvc := map[string]int64{}
	for _, idp := range dps {
		if idp.DataPoint.Timestamp != nil {
			t.Fatalf("aggregation row must have nil Timestamp (D2); got %v", idp.DataPoint.Timestamp)
		}
		if len(idp.DataPoint.TagFamilies) != 1 || idp.DataPoint.TagFamilies[0].Name != "default" {
			t.Fatalf("want one TagFamily 'default', got %+v", idp.DataPoint.TagFamilies)
		}
		tags := idp.DataPoint.TagFamilies[0].Tags
		if len(tags) != 1 || tags[0].Key != "svc" {
			t.Fatalf("want one Tag 'svc', got %+v", tags)
		}
		svc := tags[0].Value.GetStr().GetValue()
		if len(idp.DataPoint.Fields) != 1 || idp.DataPoint.Fields[0].Name != "value_sum" {
			t.Fatalf("want one Field 'value_sum', got %+v", idp.DataPoint.Fields)
		}
		bySvc[svc] = idp.DataPoint.Fields[0].Value.GetInt().GetValue()
	}
	if bySvc["a"] != 6 {
		t.Fatalf("sum(a): want 6, got %d", bySvc["a"])
	}
	if bySvc["b"] != 9 {
		t.Fatalf("sum(b): want 9, got %d", bySvc["b"])
	}
}
