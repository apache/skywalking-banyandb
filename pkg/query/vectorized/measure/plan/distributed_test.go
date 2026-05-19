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

// TestAnalyzeDistributed_AllowsGroupByTopWithoutAgg verifies that Phase 5
// accepts GroupBy + Top without Agg natively (no longer falls through to the
// row path). This was the Phase 4 carve-out that Phase 5 drops.
func TestAnalyzeDistributed_AllowsGroupByTopWithoutAgg(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{"default"},
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
	_, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, nil, vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
	if analyzeErr != nil {
		t.Fatalf("AnalyzeDistributed must accept GroupBy+Top without Agg natively (Phase 5): %v", analyzeErr)
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
	p, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, nil, vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
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
		{name: "multi group", req: &measurev1.QueryRequest{Name: "demo", Groups: []string{"a", "b"}}, want: true},
		{
			name: "group by without agg (Phase 5: native raw GroupBy)",
			req: func() *measurev1.QueryRequest {
				req := base()
				req.GroupBy = &measurev1.QueryRequest_GroupBy{TagProjection: projTagProj()}
				return req
			}(),
			want: true,
		},
		{
			name: "group by with agg (always routes to executeAgg)",
			req: func() *measurev1.QueryRequest {
				req := base()
				req.GroupBy = &measurev1.QueryRequest_GroupBy{TagProjection: projTagProj()}
				req.Agg = &measurev1.QueryRequest_Aggregation{
					Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
					FieldName: fieldValue,
				}
				return req
			}(),
			want: false,
		},
		{
			name: "top without agg (Phase 4: native raw top scan)",
			req: func() *measurev1.QueryRequest {
				req := base()
				req.Top = &measurev1.QueryRequest_Top{Number: 2, FieldName: fieldValue}
				return req
			}(),
			want: true,
		},
		{
			name: "multi group + top (Phase 5: native with calibrated per-group limit)",
			req: func() *measurev1.QueryRequest {
				req := &measurev1.QueryRequest{Name: "demo", Groups: []string{"a", "b"}}
				req.Top = &measurev1.QueryRequest_Top{Number: 2, FieldName: fieldValue}
				return req
			}(),
			want: true,
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
	p, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, nil, vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
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
			p, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, nil, vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
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

// TestAnalyzeDistributed_TopWithoutAgg_NodeLimitIsUnbounded verifies that
// Top-without-Agg requests without GroupBy use MaxUint32 as the per-node Limit.
// The calibrated limit (2*N + N*(G-1)/G) only applies when GroupBy is present,
// because GroupByFirst collapses per-node rows to at most one per group. Without
// GroupBy the per-node row count is unbounded and any finite cap risks dropping
// global winners before the liaison's BatchTop can select them.
func TestAnalyzeDistributed_TopWithoutAgg_NodeLimitIsUnbounded(t *testing.T) {
	cases := []struct {
		name  string
		topN  int32
		limit uint32
		asc   bool
	}{
		{name: "Top3_Desc_DefaultLimit", topN: 3, limit: 0, asc: false},
		{name: "Top3_Asc_ExplicitLimit", topN: 3, limit: 10, asc: true},
		{name: "Top1_Desc_Limit1", topN: 1, limit: 1, asc: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sort := modelv1.Sort_SORT_DESC
			if tc.asc {
				sort = modelv1.Sort_SORT_ASC
			}
			req := &measurev1.QueryRequest{
				Name:            "demo",
				Groups:          []string{"default"},
				TagProjection:   projTagProj(),
				FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
				Top: &measurev1.QueryRequest_Top{
					Number:         tc.topN,
					FieldName:      fieldValue,
					FieldValueSort: sort,
				},
				Limit: tc.limit,
			}
			p, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, nil,
				vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
			if analyzeErr != nil {
				t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
			}
			// Top-without-Agg without GroupBy: per-node limit must be MaxUint32.
			// The calibrated limit only applies when GroupBy is present (Phase 5).
			if got, want := p.nodeTemplate.GetLimit(), uint32(math.MaxUint32); got != want {
				t.Fatalf("Top-without-Agg (no GroupBy) node limit: got %d, want unbounded (%d)", got, want)
			}
			if p.nodeTemplate.GetTop() != nil {
				t.Fatal("node template must not carry Top; global BatchTop is the liaison's job")
			}
			if p.nodeTemplate.GetAgg() != nil {
				t.Fatal("node template must not carry Agg for a Top-without-Agg request")
			}
		})
	}
}

// TestAnalyzeDistributed_TopWithoutAgg_HiddenFieldProjectionAdded covers
// the Phase 4 augmentation path: when Top.FieldName is NOT in the request's
// FieldProjection, AnalyzeDistributed must append it to the nodeTemplate so
// data nodes materialise the column, and record hiddenTopField so the egress
// strip removes it before the response.
func TestAnalyzeDistributed_TopWithoutAgg_HiddenFieldProjectionAdded(t *testing.T) {
	// Visible field projection: only "total" — "value" (Top.FieldName) is absent.
	req := &measurev1.QueryRequest{
		Name:   "demo",
		Groups: []string{"default"},
		TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: "default", Tags: []string{tagSvc}},
		}},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"total"}},
		Top: &measurev1.QueryRequest_Top{
			Number:         3,
			FieldName:      fieldValue, // "value" — NOT in FieldProjection
			FieldValueSort: modelv1.Sort_SORT_DESC,
		},
		Limit: 10,
	}
	// Schema must have both "total" and "value" fields.
	ms := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: "default"},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "default", Tags: []*databasev1.TagSpec{
				{Name: tagSvc, Type: databasev1.TagType_TAG_TYPE_STRING},
			}},
		},
		Fields: []*databasev1.FieldSpec{
			{Name: "total", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
			{Name: fieldValue, FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		},
	}
	p, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{ms}, nil,
		vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
	if analyzeErr != nil {
		t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
	}
	if p.hiddenTopField == "" {
		t.Fatal("hiddenTopField must be set when Top.FieldName is absent from FieldProjection")
	}
	if p.hiddenTopField != fieldValue {
		t.Fatalf("hiddenTopField: got %q, want %q", p.hiddenTopField, fieldValue)
	}
	// Node template must project the Top field so data nodes materialise it.
	nodeNames := p.nodeTemplate.GetFieldProjection().GetNames()
	sawTotal, sawValue := false, false
	for _, n := range nodeNames {
		if n == "total" {
			sawTotal = true
		}
		if n == fieldValue {
			sawValue = true
		}
	}
	if !sawTotal || !sawValue {
		t.Fatalf("nodeTemplate FieldProjection must contain both 'total' and %q; got %v", fieldValue, nodeNames)
	}
	// The original request must be untouched.
	origNames := req.GetFieldProjection().GetNames()
	if len(origNames) != 1 || origNames[0] != "total" {
		t.Fatalf("original request FieldProjection must be untouched; got %v", origNames)
	}
}

// TestAnalyzeDistributed_TopWithoutAgg_FieldNotInSchema verifies the loud-failure
// rule: when Top.FieldName does not resolve on the merged schema, AnalyzeDistributed
// returns an error rather than silently passing through.
func TestAnalyzeDistributed_TopWithoutAgg_FieldNotInSchema(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{"default"},
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		Top: &measurev1.QueryRequest_Top{
			Number:         3,
			FieldName:      "no_such_field",
			FieldValueSort: modelv1.Sort_SORT_DESC,
		},
		Limit: 10,
	}
	_, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, nil,
		vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
	if analyzeErr == nil {
		t.Fatal("AnalyzeDistributed must return an error when Top.FieldName is not in the schema")
	}
	if !strings.Contains(analyzeErr.Error(), "top field") || !strings.Contains(analyzeErr.Error(), "no_such_field") {
		t.Fatalf("error must mention the missing field name; got %v", analyzeErr)
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
	p, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, [][]*databasev1.IndexRule{indexRules}, vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
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
	p, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, [][]*databasev1.IndexRule{indexRules}, vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
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
	_, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, nil, vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
	if analyzeErr == nil {
		t.Fatal("AnalyzeDistributed must reject an unknown index rule")
	}
	if got := analyzeErr.Error(); got != "index rule nope not found" {
		t.Fatalf("error text got %q want %q (row-path parity)", got, "index rule nope not found")
	}
}

// testMeasureSchemaForGroup builds a Measure schema for the given group name,
// sharing the same tag family + field layout as testMeasureSchema so the two
// can be unioned without field-type divergence.
func testMeasureSchemaForGroup(group string) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: group},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: tagSvc, Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
		Fields: []*databasev1.FieldSpec{
			{Name: fieldValue, FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		},
	}
}

// TestAnalyzeDistributed_MultiGroup_AcceptedNatively verifies that Phase 3
// SupportsDistributedRows now returns true for multi-group requests and that
// AnalyzeDistributed with two groups succeeds and produces a valid plan.
func TestAnalyzeDistributed_MultiGroup_AcceptedNatively(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{"groupA", "groupB"},
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		Limit:           10,
	}
	if !SupportsDistributedRows(req) {
		t.Fatal("Phase 3: SupportsDistributedRows must accept multi-group requests")
	}
	msA := testMeasureSchemaForGroup("groupA")
	msB := testMeasureSchemaForGroup("groupB")
	p, analyzeErr := AnalyzeDistributed(
		req,
		[]*databasev1.Measure{msA, msB},
		nil,
		vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1},
	)
	if analyzeErr != nil {
		t.Fatalf("AnalyzeDistributed multi-group: %v", analyzeErr)
	}
	if p.nodeTemplate.GetTop() != nil || p.nodeTemplate.GetGroupBy() != nil || p.nodeTemplate.GetAgg() != nil {
		t.Fatalf("multi-group node template must stay a plain scan: %+v", p.nodeTemplate)
	}
	if len(p.measureSchemas) != 2 {
		t.Fatalf("plan must store both measure schemas, got %d", len(p.measureSchemas))
	}
}

// TestAnalyzeDistributed_MultiGroup_UnionsSchemaAcrossGroups verifies that
// when two groups have the same measure layout the plan stores both schemas.
// The merged schema is built at execute-time by BuildMultiGroupBatchSchema;
// this test just confirms the plan state is correct after Analyze.
func TestAnalyzeDistributed_MultiGroup_UnionsSchemaAcrossGroups(t *testing.T) {
	msA := testMeasureSchemaForGroup("groupA")
	// msB has an extra tag "extra_tag" not present in msA.
	msB := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: "groupB"},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: tagSvc, Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "extra_tag", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
		Fields: []*databasev1.FieldSpec{
			{Name: fieldValue, FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		},
	}
	req := &measurev1.QueryRequest{
		Name:   "demo",
		Groups: []string{"groupA", "groupB"},
		Limit:  10,
	}
	p, analyzeErr := AnalyzeDistributed(
		req,
		[]*databasev1.Measure{msA, msB},
		nil,
		vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1},
	)
	if analyzeErr != nil {
		t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
	}
	if len(p.measureSchemas) != 2 {
		t.Fatalf("plan must store both schemas, got %d", len(p.measureSchemas))
	}
	// The schema union is performed at execute time; verify the plan records
	// the second group's schema so BuildMultiGroupBatchSchema can union them.
	if p.measureSchemas[1].GetMetadata().GetGroup() != "groupB" {
		t.Fatalf("plan.measureSchemas[1] must be groupB schema, got %s", p.measureSchemas[1].GetMetadata().GetGroup())
	}
}

// TestHiddenFieldsMIterator_StripsHiddenTopField verifies that
// hiddenFieldsMIterator removes the named hidden field from each DataPoint's
// Fields slice while leaving all other fields intact. This is the Phase 4
// egress strip: the hidden Top field was appended to the nodeTemplate's
// FieldProjection so BatchTop could sort on it; this wrapper removes it from
// the final response so the wire bytes match a query without the extra projection.
func TestHiddenFieldsMIterator_StripsHiddenTopField(t *testing.T) {
	// Construct a synthetic MIterator that emits two DataPoints, each with
	// two fields: "value" (visible) and "hidden_score" (to be stripped).
	makeField := func(name string, intVal int64) *measurev1.DataPoint_Field {
		return &measurev1.DataPoint_Field{
			Name:  name,
			Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: intVal}}},
		}
	}
	dp1 := &measurev1.InternalDataPoint{
		DataPoint: &measurev1.DataPoint{
			Fields: []*measurev1.DataPoint_Field{
				makeField("value", 100),
				makeField("hidden_score", 999),
			},
		},
	}
	dp2 := &measurev1.InternalDataPoint{
		DataPoint: &measurev1.DataPoint{
			Fields: []*measurev1.DataPoint_Field{
				makeField("value", 200),
				makeField("hidden_score", 888),
			},
		},
	}
	inner := &sliceMIterator{items: [][]*measurev1.InternalDataPoint{{dp1}, {dp2}}}
	wrapped := &hiddenFieldsMIterator{inner: inner, hiddenField: "hidden_score"}

	var collected []*measurev1.InternalDataPoint
	for wrapped.Next() {
		collected = append(collected, wrapped.Current()...)
	}
	if len(collected) != 2 {
		t.Fatalf("expected 2 data points, got %d", len(collected))
	}
	for dpIdx, dp := range collected {
		if len(dp.DataPoint.Fields) != 1 {
			t.Fatalf("dp %d: expected 1 field after strip, got %d: %+v", dpIdx, len(dp.DataPoint.Fields), dp.DataPoint.Fields)
		}
		if dp.DataPoint.Fields[0].GetName() != "value" {
			t.Fatalf("dp %d: remaining field must be 'value', got %q", dpIdx, dp.DataPoint.Fields[0].GetName())
		}
		if dp.DataPoint.Fields[0].GetValue().GetInt().GetValue() != int64(100*(dpIdx+1)) {
			t.Fatalf("dp %d: field value got %d, want %d", dpIdx, dp.DataPoint.Fields[0].GetValue().GetInt().GetValue(), 100*(dpIdx+1))
		}
	}
}

// TestAnalyzeDistributed_TopNonAggUnboundsNodeLimit_MultiGroup is the Phase 5
// gate for the per-group Limit calibration. For GroupBy + Top-without-Agg
// the per-node Limit must be calibrated (not MaxUint32) so that data nodes
// do not amplify their per-node response to N_groups × MaxUint32 rows.
// GroupByFirst collapses each node's output to at most one row per group,
// so the calibration formula 2*N + N*(G-1)/G (always ≥ 2*N, ≤ 3*N) is a safe
// bound: the liaison's global BatchTop always sees the true top-N winners.
//
// Without GroupBy, the per-node limit remains MaxUint32 (see
// TestAnalyzeDistributed_TopWithoutAgg_NodeLimitIsUnbounded).
func TestAnalyzeDistributed_TopNonAggUnboundsNodeLimit_MultiGroup(t *testing.T) {
	cases := []struct {
		name      string
		topN      int32
		groups    []string
		wantLimit uint32
	}{
		{
			name:      "MultiGroup2_Top3_WithGroupBy",
			topN:      3,
			groups:    []string{"a", "b"},
			wantLimit: calibratedTopWithoutAggLimit(&measurev1.QueryRequest_Top{Number: 3, FieldName: fieldValue}, 2),
		},
		{
			name:      "MultiGroup5_Top10_WithGroupBy",
			topN:      10,
			groups:    []string{"a", "b", "c", "d", "e"},
			wantLimit: calibratedTopWithoutAggLimit(&measurev1.QueryRequest_Top{Number: 10, FieldName: fieldValue}, 5),
		},
		{
			name:      "SingleGroup_Top5_WithGroupBy",
			topN:      5,
			groups:    []string{"default"},
			wantLimit: calibratedTopWithoutAggLimit(&measurev1.QueryRequest_Top{Number: 5, FieldName: fieldValue}, 1),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schemas := make([]*databasev1.Measure, len(tc.groups))
			for groupIdx, g := range tc.groups {
				schemas[groupIdx] = testMeasureSchemaForGroup(g)
			}
			req := &measurev1.QueryRequest{
				Name:   "demo",
				Groups: tc.groups,
				// GroupBy is required: calibration only applies when GroupByFirst
				// collapses per-node output to at most one row per group.
				GroupBy: &measurev1.QueryRequest_GroupBy{
					TagProjection: projTagProj(),
				},
				TagProjection:   projTagProj(),
				FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
				Top: &measurev1.QueryRequest_Top{
					Number:         tc.topN,
					FieldName:      fieldValue,
					FieldValueSort: modelv1.Sort_SORT_DESC,
				},
				Limit: 10,
			}
			p, analyzeErr := AnalyzeDistributed(req, schemas, nil,
				vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
			if analyzeErr != nil {
				t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
			}
			if got := p.nodeTemplate.GetLimit(); got != tc.wantLimit {
				t.Fatalf("Top-without-Agg %d-group (with GroupBy) node limit: got %d, want calibrated %d",
					len(tc.groups), got, tc.wantLimit)
			}
			if p.nodeTemplate.GetTop() != nil {
				t.Fatal("node template must not carry Top; global BatchTop is the liaison's job")
			}
			if p.nodeTemplate.GetAgg() != nil {
				t.Fatal("node template must not carry Agg for a Top-without-Agg request")
			}
			// Verify the calibrated limit is strictly less than MaxUint32 when
			// Top.Number is small — the whole point is to avoid the OOM vector.
			if tc.topN < 1000 && p.nodeTemplate.GetLimit() == math.MaxUint32 {
				t.Fatal("calibrated limit must not be MaxUint32 for small Top.Number with GroupBy; that defeats the carve-out")
			}
		})
	}
}

// TestAnalyzeDistributed_RawGroupBy_NodeTemplateKeepsGroupBy verifies that
// Phase 5 propagates GroupBy to data nodes for raw GroupBy requests (GroupBy
// without Agg). Data nodes must receive the GroupBy so they run their per-node
// BatchGroupByFirst pass and emit at most one row per group, minimising wire
// bytes. The old behaviour (clearing nodeTemplate.GroupBy when Agg is nil) is
// intentionally dropped in Phase 5.
func TestAnalyzeDistributed_RawGroupBy_NodeTemplateKeepsGroupBy(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{"default"},
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: projTagProj(),
		},
		Limit: 10,
	}
	p, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, nil,
		vmeasure.VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 1})
	if analyzeErr != nil {
		t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
	}
	if p.nodeTemplate.GetGroupBy() == nil {
		t.Fatal("Phase 5: nodeTemplate must retain GroupBy for raw GroupBy (GroupBy without Agg) so data nodes run per-node BatchGroupByFirst")
	}
	if p.nodeTemplate.GetAgg() != nil {
		t.Fatal("nodeTemplate must not carry Agg for a raw GroupBy request")
	}
	if p.nodeTemplate.GetTop() != nil {
		t.Fatal("nodeTemplate must not carry Top; Top is applied liaison-side")
	}
}

// sliceMIterator is a test-only MIterator over a pre-built slice of DataPoint
// groups, used to drive hidden-field strip tests without requiring a full
// pipeline build.
type sliceMIterator struct {
	items [][]*measurev1.InternalDataPoint
	idx   int
}

func (s *sliceMIterator) Next() bool {
	if s.idx >= len(s.items) {
		return false
	}
	s.idx++
	return true
}

func (s *sliceMIterator) Current() []*measurev1.InternalDataPoint {
	return s.items[s.idx-1]
}

func (s *sliceMIterator) Close() error { return nil }
