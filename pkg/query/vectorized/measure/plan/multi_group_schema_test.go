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
	"slices"
	"strings"
	"testing"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// TestBuildMultiGroupBatchSchema_UnionsTagsAndFields verifies that a two-group
// input where group B has an extra tag and an extra field produces a merged
// schema that includes both groups' columns.
func TestBuildMultiGroupBatchSchema_UnionsTagsAndFields(t *testing.T) {
	msA := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: "groupA"},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
		Fields: []*databasev1.FieldSpec{
			{Name: "count", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		},
	}
	msB := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: "groupB"},
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
			{Name: "count", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
			{Name: "latency", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT},
		},
	}

	req := &measurev1.QueryRequest{
		Name:   "demo",
		Groups: []string{"groupA", "groupB"},
		// No projection — schema-walk order applies.
	}
	schema, schemaErr := BuildMultiGroupBatchSchema([]*databasev1.Measure{msA, msB}, req)
	if schemaErr != nil {
		t.Fatalf("BuildMultiGroupBatchSchema: %v", schemaErr)
	}

	// Metadata columns: timestamp, version, sid, shardID.
	if schema.TimestampIndex() < 0 {
		t.Fatal("merged schema must have timestamp column")
	}
	if schema.VersionIndex() < 0 {
		t.Fatal("merged schema must have version column")
	}
	if schema.SeriesIDIndex() < 0 {
		t.Fatal("merged schema must have seriesID column")
	}

	// Tag columns: svc (from A+B), region (from B only).
	svcIdx, hasSvc := schema.TagIndex("default", "svc")
	if !hasSvc {
		t.Fatal("merged schema must contain tag svc")
	}
	regionIdx, hasRegion := schema.TagIndex("default", "region")
	if !hasRegion {
		t.Fatal("merged schema must contain tag region (group B extra)")
	}
	if svcIdx >= regionIdx {
		t.Fatalf("svc (idx %d) must appear before region (idx %d) in tag walk order", svcIdx, regionIdx)
	}

	// Field columns: count (from A+B), latency (from B only).
	hasCount := false
	hasLatency := false
	for _, col := range schema.Columns {
		if col.Role == vectorized.RoleField {
			switch col.Name {
			case "count":
				hasCount = true
			case "latency":
				hasLatency = true
			}
		}
	}
	if !hasCount {
		t.Fatal("merged schema must contain field count")
	}
	if !hasLatency {
		t.Fatal("merged schema must contain field latency (group B extra)")
	}
}

// TestBuildMultiGroupBatchSchema_TypeDivergenceFallsBackToUnspecified verifies
// that a tag declared with different types across groups produces a
// ColumnTypeTagValue column in the merged schema (passthrough), mirroring the
// row path's FIELD_TYPE_UNSPECIFIED fallback (schema.go:165-176).
func TestBuildMultiGroupBatchSchema_TypeDivergenceFallsBackToUnspecified(t *testing.T) {
	msA := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: "groupA"},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: "rank", Type: databasev1.TagType_TAG_TYPE_INT},
				},
			},
		},
		Fields: []*databasev1.FieldSpec{
			{Name: "val", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		},
	}
	msB := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: "groupB"},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					// "rank" is STRING here — diverges from groupA's INT.
					{Name: "rank", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
		Fields: []*databasev1.FieldSpec{
			// "val" is FLOAT here — diverges from groupA's INT.
			{Name: "val", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT},
		},
	}

	req := &measurev1.QueryRequest{
		Name:   "demo",
		Groups: []string{"groupA", "groupB"},
	}
	schema, schemaErr := BuildMultiGroupBatchSchema([]*databasev1.Measure{msA, msB}, req)
	if schemaErr != nil {
		t.Fatalf("BuildMultiGroupBatchSchema: %v", schemaErr)
	}

	// "rank" tag: type divergence → ColumnTypeTagValue (passthrough).
	rankIdx, hasRank := schema.TagIndex("default", "rank")
	if !hasRank {
		t.Fatal("merged schema must contain tag rank")
	}
	if schema.Columns[rankIdx].Type != vectorized.ColumnTypeTagValue {
		t.Fatalf("rank tag type-divergence must produce ColumnTypeTagValue, got %v", schema.Columns[rankIdx].Type)
	}

	// "val" field: type divergence → ColumnTypeFieldValue (passthrough).
	for _, col := range schema.Columns {
		if col.Role == vectorized.RoleField && col.Name == "val" {
			if col.Type != vectorized.ColumnTypeFieldValue {
				t.Fatalf("val field type-divergence must produce ColumnTypeFieldValue, got %v", col.Type)
			}
			return
		}
	}
	t.Fatal("merged schema must contain field val")
}

// TestBuildMultiGroupBatchSchema_TagProjectionFilters verifies that when the
// request carries a TagProjection only projected tags appear in the output.
func TestBuildMultiGroupBatchSchema_TagProjectionFilters(t *testing.T) {
	msA := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: "groupA"},
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
			{Name: "count", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		},
	}
	req := &measurev1.QueryRequest{
		Name:   "demo",
		Groups: []string{"groupA"},
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "default", Tags: []string{"svc"}},
			},
		},
	}
	schema, schemaErr := BuildMultiGroupBatchSchema([]*databasev1.Measure{msA}, req)
	if schemaErr != nil {
		t.Fatalf("BuildMultiGroupBatchSchema: %v", schemaErr)
	}
	_, hasSvc := schema.TagIndex("default", "svc")
	if !hasSvc {
		t.Fatal("projected tag svc must appear in merged schema")
	}
	_, hasRegion := schema.TagIndex("default", "region")
	if hasRegion {
		t.Fatal("non-projected tag region must not appear in merged schema")
	}
}

// TestBuildMultiGroupBatchSchema_UnknownProjectedTagErrors verifies that a
// request projecting a tag absent from every group's schema is rejected
// loudly with the row-path-parity "<tag>: tag is not defined" error rather
// than silently dropped from the merged schema.
func TestBuildMultiGroupBatchSchema_UnknownProjectedTagErrors(t *testing.T) {
	msA := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: "groupA"},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "default", Tags: []*databasev1.TagSpec{
				{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
			}},
		},
	}
	msB := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: "groupB"},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "default", Tags: []*databasev1.TagSpec{
				{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING},
			}},
		},
	}
	req := &measurev1.QueryRequest{
		Name:   "demo",
		Groups: []string{"groupA", "groupB"},
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "default", Tags: []string{"nowhere"}},
			},
		},
	}
	_, schemaErr := BuildMultiGroupBatchSchema([]*databasev1.Measure{msA, msB}, req)
	if schemaErr == nil {
		t.Fatal("BuildMultiGroupBatchSchema must reject projected tags absent in every group")
	}
	want := "nowhere: tag is not defined"
	if schemaErr.Error() != want {
		t.Fatalf("error text got %q, want %q", schemaErr.Error(), want)
	}
}

// TestBuildMultiGroupBatchSchema_MetadataColumnsFirst asserts the fixed
// metadata column order: timestamp < version < sid < shardID.
func TestBuildMultiGroupBatchSchema_MetadataColumnsFirst(t *testing.T) {
	ms := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: "default"},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "default", Tags: []*databasev1.TagSpec{
				{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
			}},
		},
		Fields: []*databasev1.FieldSpec{
			{Name: "count", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		},
	}
	req := &measurev1.QueryRequest{Name: "demo", Groups: []string{"default"}}
	schema, schemaErr := BuildMultiGroupBatchSchema([]*databasev1.Measure{ms}, req)
	if schemaErr != nil {
		t.Fatalf("BuildMultiGroupBatchSchema: %v", schemaErr)
	}
	if schema.Columns[0].Role != vectorized.RoleTimestamp {
		t.Fatalf("col[0] must be timestamp, got %v", schema.Columns[0].Role)
	}
	if schema.Columns[1].Role != vectorized.RoleVersion {
		t.Fatalf("col[1] must be version, got %v", schema.Columns[1].Role)
	}
	if schema.Columns[2].Role != vectorized.RoleSeriesID {
		t.Fatalf("col[2] must be seriesID, got %v", schema.Columns[2].Role)
	}
	if schema.Columns[3].Role != vectorized.RoleShardID {
		t.Fatalf("col[3] must be shardID, got %v", schema.Columns[3].Role)
	}
}

// partialLayout is a node agg partial layout: shard id, the given tags, then the value.
func partialLayout(tags ...vectorized.ColumnDef) *vectorized.BatchSchema {
	cols := []vectorized.ColumnDef{{Role: vectorized.RoleShardID, Name: "shard_id", Type: vectorized.ColumnTypeInt64}}
	cols = append(cols, tags...)
	cols = append(cols, vectorized.ColumnDef{Role: vectorized.RoleField, Name: fieldValue, Type: vectorized.ColumnTypeInt64})
	return vectorized.NewBatchSchema(cols)
}

func partialTag(name string, t vectorized.ColumnType) vectorized.ColumnDef {
	return vectorized.ColumnDef{Role: vectorized.RoleTag, TagFamily: defaultName, Name: name, Type: t}
}

func layoutNames(s *vectorized.BatchSchema) []string {
	names := make([]string, 0, len(s.Columns))
	for _, def := range s.Columns {
		names = append(names, def.Name)
	}
	return names
}

func TestMultiGroupPartialSchema_FollowsProjectionOrder(t *testing.T) {
	svc := partialTag(tagSvc, vectorized.ColumnTypeString)
	zone := partialTag("zone", vectorized.ColumnTypeTagValue)
	region := partialTag("region", vectorized.ColumnTypeTagValue)
	proj := &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: defaultName, Tags: []string{"zone", tagSvc, "region"}}}}
	withZone, withRegion := partialLayout(svc, zone), partialLayout(svc, region)
	want := []string{"shard_id", "zone", tagSvc, "region", fieldValue}
	for name, groups := range map[string][]*vectorized.BatchSchema{
		"zone-first":   {withZone, withRegion},
		"region-first": {withRegion, withZone},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := multiGroupPartialSchema(groups, proj)
			if err != nil {
				t.Fatalf("multiGroupPartialSchema: %v", err)
			}
			if names := layoutNames(got); !slices.Equal(names, want) {
				t.Fatalf("layout = %v, want %v", names, want)
			}
		})
	}
}

func TestMultiGroupPartialSchema_UnprojectedTagsFollow(t *testing.T) {
	// A GroupBy key the request didn't project is still in every partial.
	key := partialTag("key", vectorized.ColumnTypeString)
	zone := partialTag("zone", vectorized.ColumnTypeTagValue)
	proj := &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: defaultName, Tags: []string{"zone"}}}}
	got, err := multiGroupPartialSchema([]*vectorized.BatchSchema{partialLayout(key), partialLayout(key, zone)}, proj)
	if err != nil {
		t.Fatalf("multiGroupPartialSchema: %v", err)
	}
	if names, want := layoutNames(got), []string{"shard_id", "zone", "key", fieldValue}; !slices.Equal(names, want) {
		t.Fatalf("layout = %v, want %v", names, want)
	}
}

func TestMultiGroupPartialSchema_RejectsDivergentGroups(t *testing.T) {
	svc := partialTag(tagSvc, vectorized.ColumnTypeString)
	floatValue := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: "shard_id", Type: vectorized.ColumnTypeInt64},
		svc,
		{Role: vectorized.RoleField, Name: fieldValue, Type: vectorized.ColumnTypeFloat64},
	})
	cases := map[string][]*vectorized.BatchSchema{
		"key tag type":    {partialLayout(svc), partialLayout(partialTag(tagSvc, vectorized.ColumnTypeInt64))},
		"aggregate value": {partialLayout(svc), floatValue},
	}
	for name, groups := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := multiGroupPartialSchema(groups, nil); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func typedTagMeasure(group string, zoneType databasev1.TagType) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: group},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: defaultName, Tags: []*databasev1.TagSpec{
			{Name: tagSvc, Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "zone", Type: zoneType},
		}}},
		Fields: []*databasev1.FieldSpec{{Name: fieldValue, FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}
}

func sumBySvcRequest(group string) *measurev1.QueryRequest {
	return &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{group},
		TagProjection:   &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: defaultName, Tags: []string{tagSvc, "zone"}}}},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: defaultName, Tags: []string{tagSvc}}}},
			FieldName:     fieldValue,
		},
		Agg: &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, FieldName: fieldValue},
	}
}

func TestPlanGroupPartialSchema_TypesNonKeyTags(t *testing.T) {
	got, err := planGroupPartialSchema(sumBySvcRequest("a"), typedTagMeasure("a", databasev1.TagType_TAG_TYPE_STRING))
	if err != nil {
		t.Fatalf("planGroupPartialSchema: %v", err)
	}
	zoneIdx, _ := got.TagIndex(defaultName, "zone")
	if zoneType := got.Columns[zoneIdx].Type; zoneType != vectorized.ColumnTypeString {
		t.Fatalf("zone is %v, want its declared string", zoneType)
	}
}

func TestMultiGroupPartialSchema_RejectsNonKeyTagDeclaredAsAnotherType(t *testing.T) {
	a, aErr := planGroupPartialSchema(sumBySvcRequest("a"), typedTagMeasure("a", databasev1.TagType_TAG_TYPE_STRING))
	b, bErr := planGroupPartialSchema(sumBySvcRequest("b"), typedTagMeasure("b", databasev1.TagType_TAG_TYPE_INT))
	if aErr != nil || bErr != nil {
		t.Fatalf("planGroupPartialSchema: %v, %v", aErr, bErr)
	}
	_, err := multiGroupPartialSchema([]*vectorized.BatchSchema{a, b}, sumBySvcRequest("a").GetTagProjection())
	if err == nil || !strings.Contains(err.Error(), "zone") {
		t.Fatalf("error = %v, want one naming zone", err)
	}
}
