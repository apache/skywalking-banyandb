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
	"strings"
	"testing"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// testMeasureSchema builds a minimal Measure schema with one "default" tag
// family containing "svc" + "region" tag specs and one "value" field.
func testMeasureSchema() *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: "default"},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
		Fields: []*databasev1.FieldSpec{
			{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		},
	}
}

func projTagProj() *modelv1.TagProjection {
	return &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
		{Name: "default", Tags: []string{"svc"}},
	}}
}

func TestAnalyze_BareRequest_BuildsScanWrappedInLimit(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
	}
	p, err := Analyze(req, testMeasureSchema())
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if _, ok := p.(*Limit); !ok {
		t.Fatalf("root should be *Limit, got %T", p)
	}
	if _, ok := p.Children()[0].(*Scan); !ok {
		t.Fatalf("Limit child should be *Scan, got %T", p.Children()[0])
	}
}

func TestAnalyze_GroupByAgg_BuildsGroupByAggBelowLimit(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: projTagProj(),
			FieldName:     "value",
		},
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
			FieldName: "value",
		},
	}
	p, err := Analyze(req, testMeasureSchema())
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	limit, ok := p.(*Limit)
	if !ok {
		t.Fatalf("root should be *Limit, got %T", p)
	}
	gba, ok := limit.Child.(*GroupByAgg)
	if !ok {
		t.Fatalf("Limit child should be *GroupByAgg, got %T", limit.Child)
	}
	if _, ok := gba.Children()[0].(*Scan); !ok {
		t.Fatalf("GroupByAgg child should be *Scan, got %T", gba.Children()[0])
	}
	if gba.GroupBy.TagNames[0] != "svc" {
		t.Fatalf("GroupBy.TagNames: want [svc], got %v", gba.GroupBy.TagNames)
	}
	if gba.Agg.FieldName != "value" {
		t.Fatalf("Agg.FieldName: want value, got %s", gba.Agg.FieldName)
	}
}

func TestAnalyze_TopBetweenGroupByAggAndLimit(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: projTagProj(),
			FieldName:     "value",
		},
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
			FieldName: "value",
		},
		Top: &measurev1.QueryRequest_Top{
			Number:         5,
			FieldName:      "value_sum",
			FieldValueSort: modelv1.Sort_SORT_DESC,
		},
	}
	p, err := Analyze(req, testMeasureSchema())
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	limit, ok := p.(*Limit)
	if !ok {
		t.Fatalf("root: want *Limit, got %T", p)
	}
	top, ok := limit.Child.(*Top)
	if !ok {
		t.Fatalf("Limit child: want *Top, got %T", limit.Child)
	}
	if _, ok := top.Child.(*GroupByAgg); !ok {
		t.Fatalf("Top child: want *GroupByAgg, got %T", top.Child)
	}
}

func TestAnalyze_GroupByWithoutAgg_Errors(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: projTagProj(),
			FieldName:     "value",
		},
	}
	_, err := Analyze(req, testMeasureSchema())
	if err == nil {
		t.Fatal("GroupBy without Agg must error")
	}
	if !strings.Contains(err.Error(), "Agg") {
		t.Fatalf("error should mention Agg, got %v", err)
	}
}

func TestAnalyze_AggWithoutGroupBy_Errors(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
			FieldName: "value",
		},
	}
	_, err := Analyze(req, testMeasureSchema())
	if err == nil {
		t.Fatal("Agg without GroupBy must error (scalar reduce not supported)")
	}
}

func TestAnalyze_UnknownGroupByTag_Errors(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "default", Tags: []string{"missing"}},
			}},
			FieldName: "value",
		},
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
			FieldName: "value",
		},
	}
	_, err := Analyze(req, testMeasureSchema())
	if err == nil {
		t.Fatal("unknown groupby tag must error")
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Fatalf("error should mention the missing tag, got %v", err)
	}
}

func TestAnalyze_UnknownAggField_Errors(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: projTagProj(),
			FieldName:     "value",
		},
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
			FieldName: "ghost",
		},
	}
	_, err := Analyze(req, testMeasureSchema())
	if err == nil {
		t.Fatal("unknown agg field must error")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("error should mention the missing field, got %v", err)
	}
}

func TestAnalyze_NilRequest_Errors(t *testing.T) {
	_, err := Analyze(nil, testMeasureSchema())
	if err == nil {
		t.Fatal("nil request must error")
	}
}

func TestAnalyze_NilSchema_Errors(t *testing.T) {
	req := &measurev1.QueryRequest{Name: "demo"}
	_, err := Analyze(req, nil)
	if err == nil {
		t.Fatal("nil schema must error")
	}
}

func TestAnalyze_DefaultLimit_AppliedWhenZero(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
		// Limit unset (0) → default 100 per defaultLimit constant
	}
	p, err := Analyze(req, testMeasureSchema())
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	limit := p.(*Limit)
	if limit.N != defaultLimit {
		t.Fatalf("default limit: want %d, got %d", defaultLimit, limit.N)
	}
}

func TestPrintTree_RendersHierarchy(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: projTagProj(),
			FieldName:     "value",
		},
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
			FieldName: "value",
		},
	}
	p, err := Analyze(req, testMeasureSchema())
	if err != nil {
		t.Fatal(err)
	}
	out := PrintTree(p)
	// Three lines: Limit / GroupByAgg / Scan, each at increasing indent.
	if !strings.Contains(out, "Limit(") {
		t.Fatalf("PrintTree must include Limit, got: %s", out)
	}
	if !strings.Contains(out, "  GroupByAgg(") {
		t.Fatalf("PrintTree must include indented GroupByAgg, got: %s", out)
	}
	if !strings.Contains(out, "    Scan(") {
		t.Fatalf("PrintTree must include double-indented Scan, got: %s", out)
	}
}
