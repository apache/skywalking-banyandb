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
	"strings"
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	measure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

func execCfg() measure.VectorizedConfig {
	return measure.VectorizedConfig{Enabled: true, BatchSize: 1024, QueryMemoryMiB: 16}
}

func TestExecute_NilPlan_Errors(t *testing.T) {
	if _, err := Execute(context.Background(), nil, execCfg()); err == nil {
		t.Fatal("nil plan must error")
	}
}

func TestExecute_InvalidConfig_Errors(t *testing.T) {
	schema, _ := buildScanInput(t)
	scan := NewScan(schema, ScanParams{})
	scan.Source = &fakePullSource{schema: schema}
	root := NewLimit(scan, 0, 10)

	bad := measure.VectorizedConfig{BatchSize: 0, QueryMemoryMiB: 16}
	if _, err := Execute(context.Background(), root, bad); err == nil {
		t.Fatal("invalid config (BatchSize=0) must error")
	}
}

func TestExecute_ScanLimit_DrainsThroughIterator(t *testing.T) {
	schema, batch := buildScanInput(t)
	src := &fakePullSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
	scan := NewScan(schema, ScanParams{})
	scan.Source = src
	root := NewLimit(scan, 0, 10)

	iter, err := Execute(context.Background(), root, execCfg())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	defer iter.Close()

	rows := 0
	for iter.Next() {
		rows += len(iter.Current())
	}
	if rows != 5 {
		t.Fatalf("Scan+Limit should emit 5 rows, got %d", rows)
	}
}

func TestExecute_GroupByAgg_EmitsAggregatedRowsWithNilTimestamp(t *testing.T) {
	schema, batch := buildScanInput(t)
	src := &fakePullSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
	scan := NewScan(schema, ScanParams{})
	scan.Source = src
	gba, err := NewGroupByAgg(scan,
		&model.MeasureGroupBy{TagFamily: "default", TagNames: []string{tagSvc}},
		&model.MeasureAgg{FieldName: fieldValue, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
		measure.AggModeAll,
	)
	if err != nil {
		t.Fatalf("NewGroupByAgg: %v", err)
	}
	root := NewLimit(gba, 0, 10)

	iter, err := Execute(context.Background(), root, execCfg())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	defer iter.Close()

	bySvc := map[string]int64{}
	for iter.Next() {
		dps := iter.Current()
		if len(dps) != 1 {
			t.Fatalf("Current must yield 1 InternalDataPoint per Next, got %d", len(dps))
		}
		idp := dps[0]
		if idp.DataPoint.Timestamp != nil {
			t.Fatalf("aggregation row must have nil Timestamp (D2); got %v", idp.DataPoint.Timestamp)
		}
		if len(idp.DataPoint.TagFamilies) != 1 || idp.DataPoint.TagFamilies[0].Name != "default" {
			t.Fatalf("want one TagFamily 'default', got %+v", idp.DataPoint.TagFamilies)
		}
		tags := idp.DataPoint.TagFamilies[0].Tags
		if len(tags) != 1 || tags[0].Key != tagSvc {
			t.Fatalf("want one Tag 'svc', got %+v", tags)
		}
		svc := tags[0].Value.GetStr().GetValue()
		if len(idp.DataPoint.Fields) != 1 || idp.DataPoint.Fields[0].Name != fieldValue {
			t.Fatalf("want one Field 'value' (row-path parity), got %+v", idp.DataPoint.Fields)
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

// TestExecute_ScalarReduce_EmitsSingleRow drives Agg without GroupBy.
// The whole result collapses into one row carrying the first-seen
// projected tag plus the agg result, with nil Timestamp — matching the
// row path's aggAllIterator. Fixture: value column {1,4,2,3,5} → SUM 15.
func TestExecute_ScalarReduce_EmitsSingleRow(t *testing.T) {
	schema, batch := buildScanInput(t)
	src := &fakePullSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
	scan := NewScan(schema, ScanParams{})
	scan.Source = src
	gba, err := NewGroupByAgg(scan, nil,
		&model.MeasureAgg{FieldName: fieldValue, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
		measure.AggModeAll,
	)
	if err != nil {
		t.Fatalf("NewGroupByAgg: %v", err)
	}
	root := NewLimit(gba, 0, 10)

	iter, err := Execute(context.Background(), root, execCfg())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	defer iter.Close()

	rows := 0
	var sum int64
	var svc string
	for iter.Next() {
		dps := iter.Current()
		if len(dps) != 1 {
			t.Fatalf("Current must yield 1 InternalDataPoint per Next, got %d", len(dps))
		}
		idp := dps[0]
		if idp.DataPoint.Timestamp != nil {
			t.Fatalf("scalar reduce row must have nil Timestamp; got %v", idp.DataPoint.Timestamp)
		}
		tags := idp.DataPoint.TagFamilies[0].Tags
		svc = tags[0].Value.GetStr().GetValue()
		if len(idp.DataPoint.Fields) != 1 || idp.DataPoint.Fields[0].Name != fieldValue {
			t.Fatalf("want one Field 'value', got %+v", idp.DataPoint.Fields)
		}
		sum = idp.DataPoint.Fields[0].Value.GetInt().GetValue()
		rows++
	}
	if rows != 1 {
		t.Fatalf("scalar reduce must emit exactly 1 row, got %d", rows)
	}
	if sum != 15 {
		t.Fatalf("scalar SUM(value): want 15, got %d", sum)
	}
	if svc != "a" {
		t.Fatalf("scalar reduce carries first-seen tag: want svc=a, got %q", svc)
	}
}

// TestExecute_RawGroupBy_EmitsFirstRowPerGroup drives GroupBy without
// Agg. One row per group is emitted in group-insertion order with the
// input schema preserved (timestamp + field present), matching the row
// path's groupIterator + processor.go's current[0] read. Fixture groups:
// a → first row (ts=1,value=1); b → first row (ts=2,value=4).
func TestExecute_RawGroupBy_EmitsFirstRowPerGroup(t *testing.T) {
	schema, batch := buildScanInput(t)
	src := &fakePullSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
	scan := NewScan(schema, ScanParams{})
	scan.Source = src
	gba, err := NewGroupByAgg(scan,
		&model.MeasureGroupBy{TagFamily: "default", TagNames: []string{tagSvc}}, nil,
		measure.AggModeAll,
	)
	if err != nil {
		t.Fatalf("NewGroupByAgg: %v", err)
	}
	root := NewLimit(gba, 0, 10)

	iter, err := Execute(context.Background(), root, execCfg())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	defer iter.Close()

	type row struct {
		value int64
		first bool
	}
	got := map[string]row{}
	order := make([]string, 0, 2)
	for iter.Next() {
		dps := iter.Current()
		if len(dps) != 1 {
			t.Fatalf("Current must yield 1 InternalDataPoint per Next, got %d", len(dps))
		}
		idp := dps[0]
		// Raw GroupBy preserves the input schema, so the field column
		// survives (unlike the aggregation shapes).
		if len(idp.DataPoint.Fields) != 1 || idp.DataPoint.Fields[0].Name != fieldValue {
			t.Fatalf("raw GroupBy must preserve the field column, got %+v", idp.DataPoint.Fields)
		}
		svc := idp.DataPoint.TagFamilies[0].Tags[0].Value.GetStr().GetValue()
		if _, seen := got[svc]; !seen {
			order = append(order, svc)
		}
		got[svc] = row{value: idp.DataPoint.Fields[0].Value.GetInt().GetValue(), first: true}
	}
	if len(got) != 2 {
		t.Fatalf("raw GroupBy must emit one row per group (2), got %d", len(got))
	}
	if got["a"].value != 1 {
		t.Fatalf("group a first-seen value: want 1, got %d", got["a"].value)
	}
	if got["b"].value != 4 {
		t.Fatalf("group b first-seen value: want 4, got %d", got["b"].value)
	}
	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("groups must emit in insertion order [a b], got %v", order)
	}
}

func TestExecute_BuildError_SurfacesAsExecuteError(t *testing.T) {
	schema, _ := buildScanInput(t)
	scan := NewScan(schema, ScanParams{}) // Source intentionally unset
	root := NewLimit(scan, 0, 10)
	_, err := Execute(context.Background(), root, execCfg())
	if err == nil {
		t.Fatal("Execute must propagate Build error from unset Scan.Source")
	}
	if !strings.Contains(err.Error(), "Source") {
		t.Fatalf("error should mention Source, got %v", err)
	}
}
