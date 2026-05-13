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
		vectorized.NewMemoryTracker(1<<20), 1024)
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
	ops, err := BuildOperators(opts, planSchema(), vectorized.NewMemoryTracker(1<<20), 1024)
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

func TestBuildOperators_AggOutputName_IsFieldUnderscoreFunc(t *testing.T) {
	cases := []struct {
		want string
		fn   modelv1.AggregationFunction
	}{
		{"value_sum", modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
		{"value_count", modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT},
		{"value_min", modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN},
		{"value_max", modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX},
		{"value_mean", modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN},
	}
	for _, c := range cases {
		opts := model.MeasureQueryOptions{
			GroupBy: &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"svc"}},
			Agg:     &model.MeasureAgg{FieldName: "value", Func: c.fn},
		}
		ops, err := BuildOperators(opts, planSchema(), vectorized.NewMemoryTracker(1<<20), 1024)
		if err != nil {
			t.Fatalf("%v: BuildOperators error: %v", c.fn, err)
		}
		agg := ops[0].(*BatchAggregation)
		// Output schema's last column is the agg result; its Name is the auto-derived output name.
		got := agg.OutputSchema().Columns[len(agg.OutputSchema().Columns)-1].Name
		if got != c.want {
			t.Fatalf("%v: want output column name %q, got %q", c.fn, c.want, got)
		}
	}
}

func TestBuildOperators_GroupByWithoutAgg_Errors(t *testing.T) {
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"svc"}},
	}
	_, err := BuildOperators(opts, planSchema(), vectorized.NewMemoryTracker(1<<20), 1024)
	if err == nil {
		t.Fatal("GroupBy without Agg must error in v1")
	}
}

func TestBuildOperators_AggWithoutGroupBy_Errors(t *testing.T) {
	opts := model.MeasureQueryOptions{
		Agg: &model.MeasureAgg{FieldName: "value", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
	}
	_, err := BuildOperators(opts, planSchema(), vectorized.NewMemoryTracker(1<<20), 1024)
	if err == nil {
		t.Fatal("Agg without GroupBy (scalar reduce) must error in v1")
	}
}

func TestBuildOperators_UnknownGroupByTag_Errors(t *testing.T) {
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"missing"}},
		Agg:     &model.MeasureAgg{FieldName: "value", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
	}
	_, err := BuildOperators(opts, planSchema(), vectorized.NewMemoryTracker(1<<20), 1024)
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
	_, err := BuildOperators(opts, planSchema(), vectorized.NewMemoryTracker(1<<20), 1024)
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
	_, err := BuildOperators(opts, planSchema(), vectorized.NewMemoryTracker(1<<20), 1024)
	if err == nil {
		t.Fatal("UNSPECIFIED Agg.Func must error")
	}
}

func TestBuildOperators_NilTracker_Errors(t *testing.T) {
	opts := model.MeasureQueryOptions{
		GroupBy: &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"svc"}},
		Agg:     &model.MeasureAgg{FieldName: "value", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
	}
	_, err := BuildOperators(opts, planSchema(), nil, 1024)
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
	ops, err := BuildOperators(opts, schema, vectorized.NewMemoryTracker(1<<20), 1024)
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
