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
	"math"
	"strings"
	"testing"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

func TestAnalyzeDistributed_RejectsUnsupportedNonAgg(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: projTagProj(),
		},
		Top: &measurev1.QueryRequest_Top{
			Number:         5,
			FieldName:      fieldValue,
			FieldValueSort: modelv1.Sort_SORT_DESC,
		},
		Limit:  7,
		Offset: 3,
	}
	_, analyzeErr := AnalyzeDistributed(req, testMeasureSchema(), vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
	if analyzeErr == nil {
		t.Fatal("AnalyzeDistributed should reject unsupported non-agg scans")
	}
	if !strings.Contains(analyzeErr.Error(), "unsupported non-aggregation scan") {
		t.Fatalf("AnalyzeDistributed error should point at the row merge fallback, got %v", analyzeErr)
	}
}

func TestAnalyzeDistributed_AllowsSupportedNonAggRows(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{"default"},
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		Limit:           7,
		Offset:          3,
	}
	p, analyzeErr := AnalyzeDistributed(req, testMeasureSchema(), vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
	if analyzeErr != nil {
		t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
	}
	if p.nodeTemplate.GetLimit() != 10 {
		t.Fatalf("node limit got %d, want limit+offset 10", p.nodeTemplate.GetLimit())
	}
	if p.nodeTemplate.GetOffset() != 0 {
		t.Fatalf("node offset got %d, want 0", p.nodeTemplate.GetOffset())
	}
	if p.nodeTemplate.GetTop() != nil || p.nodeTemplate.GetGroupBy() != nil || p.nodeTemplate.GetAgg() != nil {
		t.Fatalf("supported row node template must stay plain scan: %+v", p.nodeTemplate)
	}
}

func TestSupportsDistributedRows(t *testing.T) {
	base := func() *measurev1.QueryRequest {
		return &measurev1.QueryRequest{Name: "demo", Groups: []string{"default"}}
	}
	cases := []struct {
		name string
		req  *measurev1.QueryRequest
		want bool
	}{
		{name: "plain", req: base(), want: true},
		{name: "multi group", req: &measurev1.QueryRequest{Name: "demo", Groups: []string{"a", "b"}}, want: false},
		{
			name: "group by",
			req: func() *measurev1.QueryRequest {
				req := base()
				req.GroupBy = &measurev1.QueryRequest_GroupBy{TagProjection: projTagProj()}
				return req
			}(),
			want: false,
		},
		{
			name: "top",
			req: func() *measurev1.QueryRequest {
				req := base()
				req.Top = &measurev1.QueryRequest_Top{Number: 2, FieldName: fieldValue}
				return req
			}(),
			want: false,
		},
		{
			name: "index order",
			req: func() *measurev1.QueryRequest {
				req := base()
				req.OrderBy = &modelv1.QueryOrder{IndexRuleName: "idx"}
				return req
			}(),
			want: false,
		},
		{
			name: "time order",
			req: func() *measurev1.QueryRequest {
				req := base()
				req.OrderBy = &modelv1.QueryOrder{Sort: modelv1.Sort_SORT_DESC}
				return req
			}(),
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SupportsDistributedRows(tc.req); got != tc.want {
				t.Fatalf("SupportsDistributedRows() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAnalyzeDistributed_NodeTemplatePushesAggPartials(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: projTagProj(),
		},
		Agg: &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, FieldName: fieldValue},
		Top: &measurev1.QueryRequest_Top{
			Number:         2,
			FieldName:      fieldValue,
			FieldValueSort: modelv1.Sort_SORT_ASC,
		},
	}
	p, analyzeErr := AnalyzeDistributed(req, testMeasureSchema(), vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
	if analyzeErr != nil {
		t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
	}
	if p.nodeTemplate.GetGroupBy() == nil || p.nodeTemplate.GetAgg() == nil {
		t.Fatal("node query template should push GroupBy+Agg for partial aggregation")
	}
	if p.nodeTemplate.GetTop() != nil {
		t.Fatal("node query template should not push local Top before liaison reduce/global Top")
	}
	// Top+Agg queries must let every aggregated group cross the wire so
	// the liaison's global Top can pick the true top-N. A finite node
	// limit would silently truncate the per-node group set before the
	// liaison ever sees the global winners.
	if got, want := p.nodeTemplate.GetLimit(), uint32(math.MaxUint32); got != want {
		t.Fatalf("Top+Agg node limit: got %d, want unbounded (%d)", got, want)
	}
}

// TestAnalyzeDistributed_TopAggUnboundsNodeLimit_Matrix is the regression
// gate for the per-node Limit truncation bug in distributed Top-over-Agg:
// each (Top.N, request Limit) combination must produce a node template with
// an unbounded Limit so every aggregated group reaches the liaison's global
// Top. The bug shape was nodeTemplate.Limit = req.Limit (with default 100
// when zero); a request like Top.N=2 with explicit Limit=2 then capped each
// data node to 2 groups, and the global Top picked from a per-node-truncated
// subset.
//
// Cardinality reasoning: with two data nodes and G aggregated groups
// distributed by sid hash, each node holds ~G/2 groups. The bug surfaces
// whenever G/2 > nodeLimit; i.e. roughly groups_per_node > Limit. The
// existing fixture suite (top.yaml, top_with_filter.yaml, etc.) only seeds
// 3 distinct services, so groups_per_node <= 2 <= Limit and the bug stayed
// hidden. This table-driven unit test stands in for the missing
// high-cardinality fixture.
func TestAnalyzeDistributed_TopAggUnboundsNodeLimit_Matrix(t *testing.T) {
	cases := []struct {
		name   string
		topN   int32
		limit  uint32
		offset uint32
	}{
		{name: "Top2_Limit2", topN: 2, limit: 2, offset: 0},
		{name: "Top2_Limit0_defaults_to_100", topN: 2, limit: 0, offset: 0},
		{name: "Top10_LimitSmaller", topN: 10, limit: 5, offset: 0},
		{name: "Top2_LimitLarger", topN: 2, limit: 1000, offset: 0},
		{name: "Top5_WithOffset", topN: 5, limit: 5, offset: 7},
		{name: "Top1_Limit1_smallest", topN: 1, limit: 1, offset: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := &measurev1.QueryRequest{
				Name:            "demo",
				TagProjection:   projTagProj(),
				FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
				GroupBy:         &measurev1.QueryRequest_GroupBy{TagProjection: projTagProj()},
				Agg:             &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, FieldName: fieldValue},
				Top: &measurev1.QueryRequest_Top{
					Number:         tc.topN,
					FieldName:      fieldValue,
					FieldValueSort: modelv1.Sort_SORT_DESC,
				},
				Limit:  tc.limit,
				Offset: tc.offset,
			}
			p, analyzeErr := AnalyzeDistributed(req, testMeasureSchema(), vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
			if analyzeErr != nil {
				t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
			}
			if got, want := p.nodeTemplate.GetLimit(), uint32(math.MaxUint32); got != want {
				t.Fatalf("Top+Agg node limit must be unbounded (got %d, want %d) for topN=%d limit=%d offset=%d; finite caps drop aggregated groups before the liaison",
					got, want, tc.topN, tc.limit, tc.offset)
			}
			if p.nodeTemplate.GetOffset() != 0 {
				t.Fatalf("node offset must be zero (got %d); offset is applied at the liaison after global Top", p.nodeTemplate.GetOffset())
			}
			if p.nodeTemplate.GetTop() != nil {
				t.Fatalf("node template must not carry Top; global Top is the liaison's job")
			}
		})
	}
}
