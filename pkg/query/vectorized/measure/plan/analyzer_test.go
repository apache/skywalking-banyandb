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

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	measure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

const (
	tagSvc      = "svc"
	fieldValue  = "value"
	tagCount    = "count_tag" // TAG_TYPE_INT
	tagBinary   = "bin_tag"   // TAG_TYPE_DATA_BINARY
	tagIntArray = "arr_tag"   // TAG_TYPE_INT_ARRAY
	tagTime     = "ts_tag"    // TAG_TYPE_TIMESTAMP
	otherFamily = "other"     // a second family, for family-qualification tests
)

// testMeasureSchema builds a minimal Measure schema with one default tag
// family containing: svc/region (string), count_tag (int, for the agg-tag
// matrix), bin_tag (data_binary), arr_tag (int array), ts_tag (timestamp);
// plus one "other" family repeating the svc name with a different type, to
// pin that agg tag resolution is family-qualified (design §5.1); and one
// value field.
func testMeasureSchema() *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: defaultName},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: defaultName,
				Tags: []*databasev1.TagSpec{
					{Name: tagSvc, Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: tagCount, Type: databasev1.TagType_TAG_TYPE_INT},
					{Name: tagBinary, Type: databasev1.TagType_TAG_TYPE_DATA_BINARY},
					{Name: tagIntArray, Type: databasev1.TagType_TAG_TYPE_INT_ARRAY},
					{Name: tagTime, Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
				},
			},
			{
				Name: otherFamily,
				Tags: []*databasev1.TagSpec{
					{Name: tagSvc, Type: databasev1.TagType_TAG_TYPE_INT},
				},
			},
		},
		Fields: []*databasev1.FieldSpec{
			{Name: fieldValue, FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		},
	}
}

func projTagProj() *modelv1.TagProjection {
	return &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
		{Name: defaultName, Tags: []string{tagSvc}},
	}}
}

func TestAnalyze_BareRequest_BuildsScanWrappedInLimit(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
	}
	p, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
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
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: projTagProj(),
			FieldName:     fieldValue,
		},
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
			FieldName: fieldValue,
		},
	}
	p, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
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
	if gba.GroupBy.TagNames[0] != tagSvc {
		t.Fatalf("GroupBy.TagNames: want [svc], got %v", gba.GroupBy.TagNames)
	}
	if gba.Agg.FieldName != fieldValue {
		t.Fatalf("Agg.FieldName: want value, got %s", gba.Agg.FieldName)
	}
}

func TestAnalyze_TopBetweenGroupByAggAndLimit(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: projTagProj(),
			FieldName:     fieldValue,
		},
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
			FieldName: fieldValue,
		},
		Top: &measurev1.QueryRequest_Top{
			Number:         5,
			FieldName:      "value_sum",
			FieldValueSort: modelv1.Sort_SORT_DESC,
		},
	}
	p, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
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
	// SORT_DESC must map to a descending top (asc=false) so the vec
	// analyzer matches the row path's reverted semantics. Sort enum
	// values are SORT_UNSPECIFIED=0, SORT_DESC=1, SORT_ASC=2 — a naive
	// "==1 means asc" inverts every Top fixture.
	if top.Asc {
		t.Fatal("SORT_DESC must produce a descending Top (Asc=false)")
	}
}

func TestAnalyze_TopSortAsc_MapsToAscending(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		Top: &measurev1.QueryRequest_Top{
			Number:         3,
			FieldName:      fieldValue,
			FieldValueSort: modelv1.Sort_SORT_ASC,
		},
	}
	p, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
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
	if !top.Asc {
		t.Fatal("SORT_ASC must produce an ascending Top (Asc=true)")
	}
}

// TestAnalyze_GroupByWithoutAgg_BuildsRawGroupBy verifies the raw
// GroupBy shape: a GroupByAgg node (Agg nil) below Limit.
func TestAnalyze_GroupByWithoutAgg_BuildsRawGroupBy(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: projTagProj(),
			FieldName:     fieldValue,
		},
	}
	p, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
	if err != nil {
		t.Fatalf("GroupBy without Agg (raw groupby) must not error: %v", err)
	}
	limit, ok := p.(*Limit)
	if !ok {
		t.Fatalf("root should be *Limit, got %T", p)
	}
	gba, ok := limit.Child.(*GroupByAgg)
	if !ok {
		t.Fatalf("Limit child should be *GroupByAgg, got %T", limit.Child)
	}
	if gba.Agg != nil {
		t.Fatalf("raw GroupBy must have nil Agg, got %+v", gba.Agg)
	}
	if gba.GroupBy == nil || gba.GroupBy.TagNames[0] != tagSvc {
		t.Fatalf("GroupBy.TagNames: want [svc], got %+v", gba.GroupBy)
	}
}

// TestAnalyze_AggWithoutGroupBy_BuildsScalarReduce verifies the scalar
// reduce shape: a GroupByAgg node (GroupBy nil) below Limit.
func TestAnalyze_AggWithoutGroupBy_BuildsScalarReduce(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
			FieldName: fieldValue,
		},
	}
	p, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
	if err != nil {
		t.Fatalf("Agg without GroupBy (scalar reduce) must not error: %v", err)
	}
	limit, ok := p.(*Limit)
	if !ok {
		t.Fatalf("root should be *Limit, got %T", p)
	}
	gba, ok := limit.Child.(*GroupByAgg)
	if !ok {
		t.Fatalf("Limit child should be *GroupByAgg, got %T", limit.Child)
	}
	if gba.GroupBy != nil {
		t.Fatalf("scalar reduce must have nil GroupBy, got %+v", gba.GroupBy)
	}
	if gba.Agg == nil || gba.Agg.FieldName != fieldValue {
		t.Fatalf("Agg.FieldName: want value, got %+v", gba.Agg)
	}
}

func TestAnalyze_UnknownGroupByTag_Errors(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: defaultName, Tags: []string{"missing"}},
			}},
			FieldName: fieldValue,
		},
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
			FieldName: fieldValue,
		},
	}
	_, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
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
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: projTagProj(),
			FieldName:     fieldValue,
		},
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
			FieldName: "ghost",
		},
	}
	_, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
	if err == nil {
		t.Fatal("unknown agg field must error")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("error should mention the missing field, got %v", err)
	}
}

func TestAnalyze_NilRequest_Errors(t *testing.T) {
	_, err := Analyze(nil, testMeasureSchema(), measure.AggModeAll)
	if err == nil {
		t.Fatal("nil request must error")
	}
}

func TestAnalyze_NilSchema_Errors(t *testing.T) {
	req := &measurev1.QueryRequest{Name: "demo"}
	_, err := Analyze(req, nil, measure.AggModeAll)
	if err == nil {
		t.Fatal("nil schema must error")
	}
}

func TestAnalyze_DefaultLimit_AppliedWhenZero(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		// Limit unset (0) → default 100 per defaultLimit constant
	}
	p, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
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
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: projTagProj(),
			FieldName:     fieldValue,
		},
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
			FieldName: fieldValue,
		},
	}
	p, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
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

// aggTagReq builds a scalar-reduce (no GroupBy) request whose Agg targets a
// tag instead of a field.
func aggTagReq(fn modelv1.AggregationFunction, family, tag string) *measurev1.QueryRequest {
	return &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  fn,
			TagName:   tag,
			TagFamily: family,
		},
	}
}

// TestAnalyze_AggTagTarget_SumOverIntTag_Succeeds pins the §6 matrix cell
// "Tag TAG_TYPE_INT × SUM ✅ new": translateAgg resolves the tag, and the
// resulting model.MeasureAgg carries TagName/TagFamily with FieldName empty.
func TestAnalyze_AggTagTarget_SumOverIntTag_Succeeds(t *testing.T) {
	req := aggTagReq(modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, defaultName, tagCount)
	p, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	gba := p.(*Limit).Child.(*GroupByAgg)
	if gba.Agg.TagName != tagCount || gba.Agg.TagFamily != defaultName || gba.Agg.FieldName != "" {
		t.Fatalf("Agg: want TagName=%s TagFamily=default FieldName=\"\", got %+v", tagCount, gba.Agg)
	}
}

// TestAnalyze_AggTagTarget_CountOverStringTag_Succeeds pins the §6 matrix
// cell "Tag TAG_TYPE_STRING × COUNT ✅ new".
func TestAnalyze_AggTagTarget_CountOverStringTag_Succeeds(t *testing.T) {
	req := aggTagReq(modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT, defaultName, tagSvc)
	if _, err := Analyze(req, testMeasureSchema(), measure.AggModeAll); err != nil {
		t.Fatalf("COUNT over a string tag must succeed: %v", err)
	}
}

// TestAnalyze_AggTagTarget_CountOverBinaryTag_Succeeds pins the §6 matrix
// cell "Tag TAG_TYPE_DATA_BINARY × COUNT ✅ new".
func TestAnalyze_AggTagTarget_CountOverBinaryTag_Succeeds(t *testing.T) {
	req := aggTagReq(modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT, defaultName, tagBinary)
	if _, err := Analyze(req, testMeasureSchema(), measure.AggModeAll); err != nil {
		t.Fatalf("COUNT over a data_binary tag must succeed: %v", err)
	}
}

// TestAnalyze_AggTagTarget_SumOverStringTag_Rejected pins the §6 matrix
// cell "Tag TAG_TYPE_STRING × SUM ❌ reject" — the error must name the tag
// and its type.
func TestAnalyze_AggTagTarget_SumOverStringTag_Rejected(t *testing.T) {
	req := aggTagReq(modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, defaultName, tagSvc)
	_, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
	if err == nil {
		t.Fatal("SUM over a string tag must be rejected")
	}
	if !strings.Contains(err.Error(), tagSvc) || !strings.Contains(err.Error(), "TAG_TYPE_STRING") {
		t.Fatalf("error must name the tag and its type, got: %v", err)
	}
}

// TestAnalyze_AggTagTarget_RejectsArrayTag pins the §6 rule that array tags
// are rejected explicitly for every function, not left to silently collapse
// every row into one group (the appendKeyComponent no-op failure mode).
func TestAnalyze_AggTagTarget_RejectsArrayTag(t *testing.T) {
	req := aggTagReq(modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT, defaultName, tagIntArray)
	if _, err := Analyze(req, testMeasureSchema(), measure.AggModeAll); err == nil {
		t.Fatal("COUNT over an array tag must be rejected")
	}
}

// TestAnalyze_AggTagTarget_RejectsTimestampTag pins the §6 rule that
// TAG_TYPE_TIMESTAMP tags remain rejected as aggregation targets.
func TestAnalyze_AggTagTarget_RejectsTimestampTag(t *testing.T) {
	req := aggTagReq(modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT, defaultName, tagTime)
	if _, err := Analyze(req, testMeasureSchema(), measure.AggModeAll); err == nil {
		t.Fatal("COUNT over a timestamp tag must be rejected")
	}
}

// TestAnalyze_AggTagTarget_UnknownTag_Errors mirrors
// TestAnalyze_UnknownAggField_Errors for the tag path.
func TestAnalyze_AggTagTarget_UnknownTag_Errors(t *testing.T) {
	req := aggTagReq(modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT, defaultName, "ghost")
	_, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
	if err == nil {
		t.Fatal("unknown agg tag must error")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("error should mention the missing tag, got %v", err)
	}
}

// TestAnalyze_AggTagTarget_FamilyQualification_DistinguishesSameName pins
// design §5.1: tag names are only unique within a family. "svc" is
// TAG_TYPE_STRING in the default family (rejects SUM) but TAG_TYPE_INT in
// "other" (accepts SUM) — the family qualifier must select the right one.
func TestAnalyze_AggTagTarget_FamilyQualification_DistinguishesSameName(t *testing.T) {
	req := aggTagReq(modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, otherFamily, tagSvc)
	p, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
	if err != nil {
		t.Fatalf("SUM over other.svc (TAG_TYPE_INT) must succeed: %v", err)
	}
	gba := p.(*Limit).Child.(*GroupByAgg)
	if gba.Agg.TagFamily != otherFamily {
		t.Fatalf("Agg.TagFamily: want %s, got %s", otherFamily, gba.Agg.TagFamily)
	}
}

// TestAnalyze_AggTagTarget_BothFieldAndTagSet_Errors and its neither-set
// sibling pin translateAgg's "exactly one of field_name/tag_name" rule.
func TestAnalyze_AggTagTarget_BothFieldAndTagSet_Errors(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
			FieldName: fieldValue,
			TagName:   tagCount,
			TagFamily: defaultName,
		},
	}
	if _, err := Analyze(req, testMeasureSchema(), measure.AggModeAll); err == nil {
		t.Fatal("Agg setting both field_name and tag_name must error")
	}
}

func TestAnalyze_AggTagTarget_NeitherFieldNorTagSet_Errors(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		Agg:             &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
	}
	if _, err := Analyze(req, testMeasureSchema(), measure.AggModeAll); err == nil {
		t.Fatal("Agg setting neither field_name nor tag_name must error")
	}
}

// TestAnalyze_AggTagTarget_TagNameWithoutTagFamily_Errors pins that a tag
// target must be family-qualified (design §5.1's "qualifying is correct").
func TestAnalyze_AggTagTarget_TagNameWithoutTagFamily_Errors(t *testing.T) {
	req := aggTagReq(modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT, "", tagCount)
	if _, err := Analyze(req, testMeasureSchema(), measure.AggModeAll); err == nil {
		t.Fatal("tag_name without tag_family must error")
	}
}

// TestAnalyze_AggTagTarget_NotProjected_InjectsAndHides pins design §5.2:
// when the caller didn't project the agg's tag, the analyzer injects it
// (so BuildBatchSchema materializes a native column) and marks it hidden.
func TestAnalyze_AggTagTarget_NotProjected_InjectsAndHides(t *testing.T) {
	// projTagProj only names tagSvc — count_tag is not requested.
	req := aggTagReq(modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, defaultName, tagCount)
	p, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	gba := p.(*Limit).Child.(*GroupByAgg)
	if !gba.Agg.HideTag {
		t.Fatal("agg tag not in the caller's projection must set HideTag")
	}
	scan := gba.Children()[0].(*Scan)
	found := false
	for _, fam := range scan.Params.TagProjection {
		if fam.Family != defaultName {
			continue
		}
		for _, n := range fam.Names {
			if n == tagCount {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("analyzer must inject the agg tag into the projection, got %+v", scan.Params.TagProjection)
	}
}

// TestAnalyze_AggTagTarget_AlreadyProjected_HideTagFalse pins the other
// half of design §5.2: when the caller already projected the tag, HideTag
// stays false so the output carries both a tag and a field of that name.
func TestAnalyze_AggTagTarget_AlreadyProjected_HideTagFalse(t *testing.T) {
	req := aggTagReq(modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, defaultName, tagCount)
	req.TagProjection = &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
		{Name: defaultName, Tags: []string{tagSvc, tagCount}},
	}}
	p, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	gba := p.(*Limit).Child.(*GroupByAgg)
	if gba.Agg.HideTag {
		t.Fatal("agg tag already in the caller's projection must leave HideTag false")
	}
}

// TestAnalyze_GroupByTimeBucket_ResolvesWidth pins design §5.3's
// explicit-width case end to end: GroupBy.time_bucket resolves to a
// model.MeasureTimeBucket carrying both the raw string and its parsed
// nanosecond width.
func TestAnalyze_GroupByTimeBucket_ResolvesWidth(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: projTagProj(),
			FieldName:     fieldValue,
			TimeBucket:    &measurev1.QueryRequest_GroupBy_TimeBucket{Width: "5m"},
		},
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
			FieldName: fieldValue,
		},
	}
	p, err := Analyze(req, testMeasureSchema(), measure.AggModeAll)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	gba := p.(*Limit).Child.(*GroupByAgg)
	const wantNanos = int64(5 * 60 * 1e9)
	if gba.GroupBy.TimeBucket == nil || gba.GroupBy.TimeBucket.Width != "5m" || gba.GroupBy.TimeBucket.WidthNanos != wantNanos {
		t.Fatalf("GroupBy.TimeBucket: want Width=5m WidthNanos=%d, got %+v", wantNanos, gba.GroupBy.TimeBucket)
	}
}

// TestQueryRequest_TagAggAndTimeBucket_SurviveWireRoundTrip is the design's
// explicit wire-compatibility DoD: an old client that never sets the new
// fields is unaffected (implicit — zero values roundtrip identically), and
// the new fields survive a real proto Marshal/Unmarshal cycle.
func TestQueryRequest_TagAggAndTimeBucket_SurviveWireRoundTrip(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name: "demo",
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT,
			TagName:   tagCount,
			TagFamily: defaultName,
		},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TimeBucket: &measurev1.QueryRequest_GroupBy_TimeBucket{Width: "1h"},
		},
	}
	wire, marshalErr := proto.Marshal(req)
	if marshalErr != nil {
		t.Fatalf("Marshal: %v", marshalErr)
	}
	got := &measurev1.QueryRequest{}
	if unmarshalErr := proto.Unmarshal(wire, got); unmarshalErr != nil {
		t.Fatalf("Unmarshal: %v", unmarshalErr)
	}
	if got.GetAgg().GetTagName() != tagCount || got.GetAgg().GetTagFamily() != defaultName ||
		got.GetAgg().GetFunction() != modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT {
		t.Fatalf("Agg did not survive the wire round trip: %+v", got.GetAgg())
	}
	if got.GetGroupBy().GetTimeBucket().GetWidth() != "1h" {
		t.Fatalf("GroupBy.TimeBucket did not survive the wire round trip: %+v", got.GetGroupBy())
	}
}

// timeBucketReq builds a request with a bare time_bucket GroupBy (no tag
// key) and no Agg, for exercising resolveTimeBucket's width table directly
// through Analyze.
func timeBucketReq(width string) *measurev1.QueryRequest {
	return &measurev1.QueryRequest{
		Name:            "demo",
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TimeBucket: &measurev1.QueryRequest_GroupBy_TimeBucket{Width: width},
		},
		// A bucketed GroupBy requires Agg (translateGroupBy rejects a
		// bucketed raw GroupBy — no execution support carries the projected
		// fields for that shape yet), so every width-table case needs one.
		Agg: &measurev1.QueryRequest_Aggregation{
			Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
			FieldName: fieldValue,
		},
	}
}

// TestAnalyze_TimeBucket_WidthResolutionTable pins the full §5.3 table as
// literal test cases, including the explicit non-rejection of a width that
// is not a multiple of the measure's interval — the rule an earlier draft
// wrongly proposed.
func TestAnalyze_TimeBucket_WidthResolutionTable(t *testing.T) {
	schemaWithInterval := testMeasureSchema()
	schemaWithInterval.Interval = "1m"

	cases := []struct {
		schema    *databasev1.Measure
		name      string
		width     string
		wantNanos int64
		wantErr   bool
	}{
		{name: "width from request", width: "5m", schema: testMeasureSchema(), wantNanos: 5 * 60 * 1e9},
		{name: "width empty, measure interval present", width: "", schema: schemaWithInterval, wantNanos: 60 * 1e9},
		{name: "width empty, measure interval absent", width: "", schema: testMeasureSchema(), wantErr: true},
		{name: "width unparseable", width: "banana", schema: testMeasureSchema(), wantErr: true},
		{name: "width zero", width: "0s", schema: testMeasureSchema(), wantErr: true},
		{name: "width negative", width: "-5m", schema: testMeasureSchema(), wantErr: true},
		{
			name: "width not a multiple of the interval is accepted", width: "7m",
			schema: schemaWithInterval, wantNanos: 7 * 60 * 1e9,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Analyze(timeBucketReq(tc.width), tc.schema, measure.AggModeAll)
			if tc.wantErr {
				if err == nil {
					t.Fatal("want error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Analyze: %v", err)
			}
			gba := p.(*Limit).Child.(*GroupByAgg)
			if gba.GroupBy.TimeBucket.WidthNanos != tc.wantNanos {
				t.Fatalf("WidthNanos: want %d, got %d", tc.wantNanos, gba.GroupBy.TimeBucket.WidthNanos)
			}
		})
	}
}

// TestAnalyze_TimeBucket_BoundaryTimestampAcceptedAsWidth is a design-§5.3
// aside folded into the width table: a boundary-aligned width string is
// just an ordinary positive duration — nothing about "boundary" is special
// at resolution time (only bucketStart's floor arithmetic cares about
// boundaries), so this doubles as a plain sanity check on parsing.
func TestAnalyze_TimeBucket_BoundaryTimestampAcceptedAsWidth(t *testing.T) {
	p, err := Analyze(timeBucketReq("1000ns"), testMeasureSchema(), measure.AggModeAll)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	gba := p.(*Limit).Child.(*GroupByAgg)
	if gba.GroupBy.TimeBucket.WidthNanos != 1000 {
		t.Fatalf("WidthNanos: want 1000, got %d", gba.GroupBy.TimeBucket.WidthNanos)
	}
}

// TestAnalyze_TimeBucket_BucketOnly_NoTagKey pins design §7.2: a GroupBy may
// bucket by time alone, with no tag key at all.
func TestAnalyze_TimeBucket_BucketOnly_NoTagKey(t *testing.T) {
	p, err := Analyze(timeBucketReq("5m"), testMeasureSchema(), measure.AggModeAll)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	gba := p.(*Limit).Child.(*GroupByAgg)
	if gba.GroupBy.TagFamily != "" || len(gba.GroupBy.TagNames) != 0 {
		t.Fatalf("bucket-only GroupBy must carry no tag key, got %+v", gba.GroupBy)
	}
	if gba.GroupBy.TimeBucket == nil {
		t.Fatal("bucket-only GroupBy must still carry TimeBucket")
	}
}

// TestAnalyze_GroupBy_NeitherTagProjectionNorTimeBucket_Errors pins that an
// empty GroupBy message (no tag_projection, no time_bucket) is rejected —
// there is nothing to group by.
func TestAnalyze_GroupBy_NeitherTagProjectionNorTimeBucket_Errors(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy:         &measurev1.QueryRequest_GroupBy{},
	}
	if _, err := Analyze(req, testMeasureSchema(), measure.AggModeAll); err == nil {
		t.Fatal("an empty GroupBy message must error")
	}
}

// TestAnalyze_TimeBucket_RejectsNonTimeOrderBy pins the analyzer guard
// (design §7.2): a bucketed request whose order_by names a non-time index
// rule is rejected rather than silently ignored.
func TestAnalyze_TimeBucket_RejectsNonTimeOrderBy(t *testing.T) {
	req := timeBucketReq("5m")
	req.OrderBy = &modelv1.QueryOrder{IndexRuleName: "some_index"}
	if _, err := Analyze(req, testMeasureSchema(), measure.AggModeAll); err == nil {
		t.Fatal("a bucketed request with a non-time order_by must error")
	}
}

// TestAnalyze_TimeBucket_RejectsDescendingTimeOrderBy pins the other half of
// the ordering guard: an empty index_rule_name with Sort=SORT_DESC still
// resolves to a time-ordered scan (index.OrderByTypeTime), just descending
// instead of ascending — which would trip BatchTimeBucket's monotonicity
// guard on the very first bucket transition. This must be rejected at
// analyze time with a clear error, not surface as a confusing "bucket
// regressed" failure deep in the scan.
func TestAnalyze_TimeBucket_RejectsDescendingTimeOrderBy(t *testing.T) {
	req := timeBucketReq("5m")
	req.OrderBy = &modelv1.QueryOrder{Sort: modelv1.Sort_SORT_DESC}
	if _, err := Analyze(req, testMeasureSchema(), measure.AggModeAll); err == nil {
		t.Fatal("a bucketed request with order_by.sort=SORT_DESC must error")
	}
}

// TestAnalyze_TimeBucket_WithoutAgg_Rejected pins that a bucketed raw
// GroupBy (time_bucket set, Agg nil) is rejected rather than silently
// dropping every projected field: BatchTimeBucket's no-Agg shape reuses
// BatchAggregation's output layout (tags + bucket timestamp only), unlike
// BatchGroupByFirst's full-schema passthrough for the non-bucketed raw
// GroupBy case.
func TestAnalyze_TimeBucket_WithoutAgg_Rejected(t *testing.T) {
	req := &measurev1.QueryRequest{
		Name:            "demo",
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TimeBucket: &measurev1.QueryRequest_GroupBy_TimeBucket{Width: "5m"},
		},
	}
	if _, err := Analyze(req, testMeasureSchema(), measure.AggModeAll); err == nil {
		t.Fatal("a bucketed GroupBy without Agg must error")
	}
}

// TestAnalyze_TimeBucket_UsesIndexModeMap pins that the analyzer decides
// streaming vs. the index-mode map fallback once, from
// measureSchema.GetIndexMode(), rather than leaving it to be guessed later.
func TestAnalyze_TimeBucket_UsesIndexModeMap(t *testing.T) {
	indexModeSchema := testMeasureSchema()
	indexModeSchema.IndexMode = true
	p, err := Analyze(timeBucketReq("5m"), indexModeSchema, measure.AggModeAll)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	gba := p.(*Limit).Child.(*GroupByAgg)
	if !gba.GroupBy.TimeBucket.UseIndexModeMap {
		t.Fatal("an index-mode measure must resolve UseIndexModeMap=true")
	}
}
