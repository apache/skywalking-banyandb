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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
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
	_, analyzeErr := AnalyzeDistributed(req, testMeasureSchema(), nil, vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
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
	p, analyzeErr := AnalyzeDistributed(req, testMeasureSchema(), nil, vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
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
			want: true,
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
	p, analyzeErr := AnalyzeDistributed(req, testMeasureSchema(), nil, vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
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
			p, analyzeErr := AnalyzeDistributed(req, testMeasureSchema(), nil, vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
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

// testIndexRuleOnTag returns an index rule named ruleName that indexes a
// single tag identified by tagName, ready to thread through
// AnalyzeDistributed as the indexRules parameter.
func testIndexRuleOnTag(ruleName, tagName string) *databasev1.IndexRule {
	return &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: ruleName, Group: "default"},
		Tags:     []string{tagName},
	}
}

// TestAnalyzeDistributed_OrderByByIndexRule_AcceptedNatively is the Phase 2
// gate proving the analyzer no longer rejects an OrderBy.IndexRuleName,
// resolves it to a (family, tag) on the supplied indexRules slice, and the
// resulting plan executes via the native row path (SupportsDistributedRows
// must return true for the same request shape).
func TestAnalyzeDistributed_OrderByByIndexRule_AcceptedNatively(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{"default"},
		TagProjection:   projTagProj(), // already projects "svc"
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		OrderBy:         &modelv1.QueryOrder{IndexRuleName: "svc_idx", Sort: modelv1.Sort_SORT_ASC},
		Limit:           5,
	}
	indexRules := []*databasev1.IndexRule{testIndexRuleOnTag("svc_idx", tagSvc)}
	if !SupportsDistributedRows(req) {
		t.Fatal("Phase 2: SupportsDistributedRows must accept OrderBy by index rule")
	}
	p, analyzeErr := AnalyzeDistributed(req, testMeasureSchema(), indexRules, vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
	if analyzeErr != nil {
		t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
	}
	if p.orderByTag == nil {
		t.Fatal("DistributedPlan.orderByTag must be populated when an index rule resolves")
	}
	if p.orderByTag.family != "default" || p.orderByTag.tag != tagSvc {
		t.Fatalf("orderByTag got %+v want default/%s", *p.orderByTag, tagSvc)
	}
	if !p.hiddenOrderBy.IsEmpty() {
		t.Fatalf("hiddenOrderBy must be empty when the OrderBy tag is already in the visible projection; got %+v", p.hiddenOrderBy)
	}
	// The node template must keep the OrderBy projected (it was visible to
	// begin with) and must not carry GroupBy/Top/Agg.
	if p.nodeTemplate.GetTop() != nil || p.nodeTemplate.GetGroupBy() != nil || p.nodeTemplate.GetAgg() != nil {
		t.Fatalf("OrderBy-by-index-rule node template must stay a plain scan, got %+v", p.nodeTemplate)
	}
}

// TestAnalyzeDistributed_OrderByByIndexRule_HiddenProjectionAdded covers
// the augmentation path: when the OrderBy tag is NOT in the request's
// TagProjection, AnalyzeDistributed must append it to the nodeTemplate's
// projection (so data nodes materialize the column on the wire) and record
// it on hiddenOrderBy so the egress strip removes it before the response.
func TestAnalyzeDistributed_OrderByByIndexRule_HiddenProjectionAdded(t *testing.T) {
	// Projection visible to the user: only "region" — the OrderBy tag
	// "svc" is NOT projected, so the analyzer must hide-project it.
	visibleProjection := &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
		{Name: "default", Tags: []string{"region"}},
	}}
	req := &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{"default"},
		TagProjection:   visibleProjection,
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		OrderBy:         &modelv1.QueryOrder{IndexRuleName: "svc_idx", Sort: modelv1.Sort_SORT_DESC},
		Limit:           5,
	}
	indexRules := []*databasev1.IndexRule{testIndexRuleOnTag("svc_idx", tagSvc)}
	p, analyzeErr := AnalyzeDistributed(req, testMeasureSchema(), indexRules, vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
	if analyzeErr != nil {
		t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
	}
	if p.orderByTag == nil || p.orderByTag.family != "default" || p.orderByTag.tag != tagSvc {
		t.Fatalf("orderByTag got %+v want default/%s", p.orderByTag, tagSvc)
	}
	if p.hiddenOrderBy.IsEmpty() {
		t.Fatal("hiddenOrderBy must be populated when OrderBy is not in visible projection")
	}
	if !p.hiddenOrderBy.Contains(tagSvc) {
		t.Fatalf("hiddenOrderBy must contain %q; got %+v", tagSvc, p.hiddenOrderBy)
	}
	// The user's request TagProjection must be untouched (the analyzer
	// clones before mutating).
	if reqFams := req.GetTagProjection().GetTagFamilies(); len(reqFams) != 1 ||
		len(reqFams[0].GetTags()) != 1 || reqFams[0].GetTags()[0] != "region" {
		t.Fatalf("user request must be untouched, got %+v", reqFams)
	}
	// The node template's TagProjection must include the OrderBy tag.
	nodeFams := p.nodeTemplate.GetTagProjection().GetTagFamilies()
	if len(nodeFams) != 1 || nodeFams[0].GetName() != "default" {
		t.Fatalf("node template projection wrong shape, got %+v", nodeFams)
	}
	saw := map[string]bool{}
	for _, tag := range nodeFams[0].GetTags() {
		saw[tag] = true
	}
	if !saw["region"] || !saw[tagSvc] {
		t.Fatalf("node template must project both 'region' and %q, got %+v", tagSvc, nodeFams[0].GetTags())
	}
}

// TestAnalyzeDistributed_OrderByByIndexRule_UnknownRuleErrors proves the
// resolver's parity error: an unknown indexRuleName surfaces the row-path's
// canonical "index rule %s not found" text byte-for-byte. Test fixtures
// asserting WantErr across paths rely on this byte-equivalence.
func TestAnalyzeDistributed_OrderByByIndexRule_UnknownRuleErrors(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{"default"},
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		OrderBy:         &modelv1.QueryOrder{IndexRuleName: "nope"},
	}
	_, analyzeErr := AnalyzeDistributed(req, testMeasureSchema(), nil, vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
	if analyzeErr == nil {
		t.Fatal("AnalyzeDistributed must reject an unknown index rule")
	}
	if got := analyzeErr.Error(); got != "index rule nope not found" {
		t.Fatalf("error text got %q want %q (row-path parity)", got, "index rule nope not found")
	}
}
