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

package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumpmeasure "github.com/apache/skywalking-banyandb/banyand/internal/dump/measure"
	dumpstream "github.com/apache/skywalking-banyandb/banyand/internal/dump/stream"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// TestDecodeSeriesEntityValues_RoundTrip verifies that a marshaled Series
// round-trips through decodeSeriesEntityValues.
func TestDecodeSeriesEntityValues_RoundTrip(t *testing.T) {
	t.Run("subject_with_string_entity_tags", func(t *testing.T) {
		original := &pbv1.Series{
			Subject: "service_cpm_minute",
			EntityValues: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "checkout"}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "us-east-1"}}},
			},
		}
		require.NoError(t, original.Marshal())

		subject, evList, err := decodeSeriesEntityValues(original.Buffer)
		require.NoError(t, err)
		assert.Equal(t, "service_cpm_minute", subject)
		require.Len(t, evList, 2)
		assert.Equal(t, "checkout", evList[0].GetStr().GetValue())
		assert.Equal(t, "us-east-1", evList[1].GetStr().GetValue())
	})

	t.Run("empty_bytes_rejected", func(t *testing.T) {
		_, _, err := decodeSeriesEntityValues(nil)
		require.Error(t, err)
	})

	t.Run("empty_subject_rejected", func(t *testing.T) {
		s := &pbv1.Series{Subject: "", EntityValues: nil}
		require.NoError(t, s.Marshal())
		_, _, err := decodeSeriesEntityValues(s.Buffer)
		require.Error(t, err)
	})
}

// TestTagTypeToValueType verifies every TagType maps to the expected ValueType.
func TestTagTypeToValueType(t *testing.T) {
	cases := []struct {
		in   databasev1.TagType
		want pbv1.ValueType
	}{
		{databasev1.TagType_TAG_TYPE_STRING, pbv1.ValueTypeStr},
		{databasev1.TagType_TAG_TYPE_INT, pbv1.ValueTypeInt64},
		{databasev1.TagType_TAG_TYPE_DATA_BINARY, pbv1.ValueTypeBinaryData},
		{databasev1.TagType_TAG_TYPE_STRING_ARRAY, pbv1.ValueTypeStrArr},
		{databasev1.TagType_TAG_TYPE_INT_ARRAY, pbv1.ValueTypeInt64Arr},
		{databasev1.TagType_TAG_TYPE_TIMESTAMP, pbv1.ValueTypeTimestamp},
		{databasev1.TagType_TAG_TYPE_UNSPECIFIED, pbv1.ValueTypeUnknown},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, pbv1.TagValueSpecToValueType(c.in), "TagType=%v", c.in)
	}
}

// TestFieldTypeToValueType verifies every FieldType maps to the expected ValueType.
func TestFieldTypeToValueType(t *testing.T) {
	cases := []struct {
		in   databasev1.FieldType
		want pbv1.ValueType
	}{
		{databasev1.FieldType_FIELD_TYPE_STRING, pbv1.ValueTypeStr},
		{databasev1.FieldType_FIELD_TYPE_INT, pbv1.ValueTypeInt64},
		{databasev1.FieldType_FIELD_TYPE_FLOAT, pbv1.ValueTypeFloat64},
		{databasev1.FieldType_FIELD_TYPE_DATA_BINARY, pbv1.ValueTypeBinaryData},
		{databasev1.FieldType_FIELD_TYPE_UNSPECIFIED, pbv1.ValueTypeUnknown},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, fieldTypeToValueType(c.in), "FieldType=%v", c.in)
	}
}

// TestBuildEntityTagIndex verifies positional mapping of Entity.TagNames to EntityValues.
func TestBuildEntityTagIndex(t *testing.T) {
	entity := &databasev1.Entity{TagNames: []string{"service", "region"}}
	evList := pbv1.EntityValues{
		&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "checkout"}}},
		&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "us-east-1"}}},
	}
	idx := buildEntityTagIndex(entity, evList)
	require.Len(t, idx, 2)
	assert.Equal(t, "checkout", idx["service"].GetStr().GetValue())
	assert.Equal(t, "us-east-1", idx["region"].GetStr().GetValue())

	// A nil entity yields no index.
	assert.Nil(t, buildEntityTagIndex(nil, evList))

	// Fewer values than names: trailing names are dropped, no panic.
	short := buildEntityTagIndex(entity, evList[:1])
	require.Len(t, short, 1)
	assert.Equal(t, "checkout", short["service"].GetStr().GetValue())
}

// TestDeriveMergedRuleToTag verifies index-rule -> tag-spec resolution,
// including skipping rules for undeclared tags.
func TestDeriveMergedRuleToTag(t *testing.T) {
	measures := []*databasev1.Measure{
		{
			Metadata: &commonv1.Metadata{Name: "m1"},
			TagFamilies: []*databasev1.TagFamilySpec{
				{
					Name: "searchable",
					Tags: []*databasev1.TagSpec{
						{Name: "endpoint_id", Type: databasev1.TagType_TAG_TYPE_STRING},
						{Name: "rpc_latency_buckets", Type: databasev1.TagType_TAG_TYPE_INT_ARRAY},
					},
				},
			},
		},
	}
	rules := []*databasev1.IndexRule{
		{
			Metadata: &commonv1.Metadata{Id: 101, Name: "idx_endpoint"},
			Tags:     []string{"endpoint_id"},
		},
		{
			Metadata: &commonv1.Metadata{Id: 102, Name: "idx_buckets"},
			Tags:     []string{"rpc_latency_buckets"},
		},
		{
			Metadata: &commonv1.Metadata{Id: 103, Name: "idx_unknown"},
			Tags:     []string{"not_declared_anywhere"},
		},
		{
			// Multi-tag (composite) rule: indexed under one ruleID field as a
			// composite term, not decodable into individual tag values.
			Metadata: &commonv1.Metadata{Id: 104, Name: "idx_multi"},
			Tags:     []string{"endpoint_id", "rpc_latency_buckets"},
		},
	}
	bindings := []*databasev1.IndexRuleBinding{
		{
			Subject: &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: "m1"},
			Rules:   []string{"idx_endpoint", "idx_buckets", "idx_unknown", "idx_multi"},
		},
	}
	out := deriveMergedRuleToTag(measures, rules, bindings)
	require.Len(t, out, 2)
	assert.Equal(t, dump.IndexedTagSpec{Family: "searchable", Name: "endpoint_id", Type: pbv1.ValueTypeStr}, out[101])
	assert.Equal(t, dump.IndexedTagSpec{Family: "searchable", Name: "rpc_latency_buckets", Type: pbv1.ValueTypeInt64Arr}, out[102])
	_, hasUnknown := out[103]
	assert.False(t, hasUnknown, "rule whose tag is undefined in the bound measure should be skipped")
	_, hasMulti := out[104]
	assert.False(t, hasMulti, "multi-tag (composite) rule must be skipped: its value is not a single decodable tag")
}

// TestDeriveMergedRuleToTag_PerMeasureScope pins the collision fix: two measures
// declare a tag with the same name but a different family/type. Each rule must
// resolve within the measure it is bound to, not a group-wide merge.
func TestDeriveMergedRuleToTag_PerMeasureScope(t *testing.T) {
	measures := []*databasev1.Measure{
		{
			Metadata: &commonv1.Metadata{Name: "m_str"},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "fa", Tags: []*databasev1.TagSpec{{Name: "x", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		},
		{
			Metadata: &commonv1.Metadata{Name: "m_int"},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "fb", Tags: []*databasev1.TagSpec{{Name: "x", Type: databasev1.TagType_TAG_TYPE_INT}}},
			},
		},
	}
	rules := []*databasev1.IndexRule{
		{Metadata: &commonv1.Metadata{Id: 1, Name: "r_str"}, Tags: []string{"x"}},
		{Metadata: &commonv1.Metadata{Id: 2, Name: "r_int"}, Tags: []string{"x"}},
	}
	bindings := []*databasev1.IndexRuleBinding{
		{Subject: &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: "m_str"}, Rules: []string{"r_str"}},
		{Subject: &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: "m_int"}, Rules: []string{"r_int"}},
	}
	out := deriveMergedRuleToTag(measures, rules, bindings)
	require.Len(t, out, 2)
	assert.Equal(t, dump.IndexedTagSpec{Family: "fa", Name: "x", Type: pbv1.ValueTypeStr}, out[1],
		"rule bound to m_str must resolve tag x as STRING in family fa")
	assert.Equal(t, dump.IndexedTagSpec{Family: "fb", Name: "x", Type: pbv1.ValueTypeInt64}, out[2],
		"rule bound to m_int must resolve tag x as INT in family fb")
}

// TestDeriveMergedRuleToTag_EmptyInputs verifies nil/empty inputs return nil,
// including the no-bindings case (no rule is applied to any measure).
func TestDeriveMergedRuleToTag_EmptyInputs(t *testing.T) {
	assert.Nil(t, deriveMergedRuleToTag(nil, nil, nil))
	assert.Nil(t, deriveMergedRuleToTag(
		[]*databasev1.Measure{{Metadata: &commonv1.Metadata{Name: "m"}}},
		[]*databasev1.IndexRule{{Metadata: &commonv1.Metadata{Id: 1, Name: "r"}, Tags: []string{"x"}}},
		nil,
	), "no bindings must yield nil")
	assert.Nil(t, deriveMergedRuleToTag([]*databasev1.Measure{{Metadata: &commonv1.Metadata{Name: "m"}}}, nil,
		[]*databasev1.IndexRuleBinding{{Subject: &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: "m"}}}))
}

func stringTagValue(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

func intTagValue(v int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
}

func intFieldValue(v int64) *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: v}}}
}

func floatFieldValue(v float64) *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: v}}}
}

func mustMarshalTag(t *testing.T, tv *modelv1.TagValue) []byte {
	t.Helper()
	b, err := pbv1.MarshalTagValue(tv)
	require.NoError(t, err)
	return b
}

// TestBuildMeasureTagFamilies_PriorityOrder verifies tag resolution priority:
// column store > IndexResolver > EntityValues > null.
func TestBuildMeasureTagFamilies_PriorityOrder(t *testing.T) {
	families := []*databasev1.TagFamilySpec{
		{
			Name: "primary",
			Tags: []*databasev1.TagSpec{
				{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING}, // entity tag
				{Name: "method", Type: databasev1.TagType_TAG_TYPE_STRING},  // column tag
			},
		},
		{
			Name: "searchable",
			Tags: []*databasev1.TagSpec{
				{Name: "endpoint", Type: databasev1.TagType_TAG_TYPE_STRING}, // index-only tag
				{Name: "absent", Type: databasev1.TagType_TAG_TYPE_STRING},   // truly missing
			},
		},
	}
	entity := &databasev1.Entity{TagNames: []string{"service"}}
	evList := pbv1.EntityValues{stringTagValue("checkout")}

	row := dumpmeasure.Row{
		Tags: map[string][]byte{
			"primary.method": mustMarshalTag(t, stringTagValue("POST")),
		},
		TagTypes: map[string]pbv1.ValueType{
			"primary.method": pbv1.ValueTypeStr,
		},
	}
	indexedTyped := map[string]*modelv1.TagValue{
		"searchable.endpoint": stringTagValue("/checkout/v1"),
	}

	out := buildMeasureTagFamilies(families, entity, row, evList, indexedTyped)
	require.Len(t, out, 2)
	require.Len(t, out[0].Tags, 2)
	require.Len(t, out[1].Tags, 2)

	assert.Equal(t, "checkout", out[0].Tags[0].GetStr().GetValue())
	assert.Equal(t, "POST", out[0].Tags[1].GetStr().GetValue())
	assert.Equal(t, "/checkout/v1", out[1].Tags[0].GetStr().GetValue())
	assert.True(t, proto.Equal(pbv1.NullTagValue, out[1].Tags[1]),
		"absent tag must fall back to NullTagValue")
}

// TestBuildMeasureTagFamilies_ColumnOverridesIndexed verifies column store
// wins over index resolver when both supply the same tag.
func TestBuildMeasureTagFamilies_ColumnOverridesIndexed(t *testing.T) {
	families := []*databasev1.TagFamilySpec{
		{
			Name: "primary",
			Tags: []*databasev1.TagSpec{
				{Name: "biz_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
	}
	row := dumpmeasure.Row{
		Tags: map[string][]byte{
			"primary.biz_id": mustMarshalTag(t, stringTagValue("from-column")),
		},
		TagTypes: map[string]pbv1.ValueType{
			"primary.biz_id": pbv1.ValueTypeStr,
		},
	}
	indexedTyped := map[string]*modelv1.TagValue{
		"primary.biz_id": stringTagValue("from-index"),
	}
	out := buildMeasureTagFamilies(families, nil, row, nil, indexedTyped)
	require.Len(t, out, 1)
	require.Len(t, out[0].Tags, 1)
	assert.Equal(t, "from-column", out[0].Tags[0].GetStr().GetValue(),
		"column store must win over index resolver when both supply the same tag")
}

// TestBuildStreamTagFamilies_EntityAndColumn verifies stream tag resolution:
// column store > EntityValues > null.
func TestBuildStreamTagFamilies_EntityAndColumn(t *testing.T) {
	families := []*databasev1.TagFamilySpec{
		{
			Name: "trace",
			Tags: []*databasev1.TagSpec{
				{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING}, // entity tag
				{Name: "url", Type: databasev1.TagType_TAG_TYPE_STRING},     // column tag
				{Name: "absent", Type: databasev1.TagType_TAG_TYPE_STRING},  // null
			},
		},
	}
	entity := &databasev1.Entity{TagNames: []string{"service"}}
	evList := pbv1.EntityValues{stringTagValue("checkout")}
	row := dumpstream.Row{
		Tags: map[string][]byte{
			"trace.url": mustMarshalTag(t, stringTagValue("/checkout/v1")),
		},
		TagTypes: map[string]pbv1.ValueType{
			"trace.url": pbv1.ValueTypeStr,
		},
	}
	out := buildStreamTagFamilies(families, entity, row, evList)
	require.Len(t, out, 1)
	require.Len(t, out[0].Tags, 3)
	assert.Equal(t, "checkout", out[0].Tags[0].GetStr().GetValue())
	assert.Equal(t, "/checkout/v1", out[0].Tags[1].GetStr().GetValue())
	assert.True(t, proto.Equal(pbv1.NullTagValue, out[0].Tags[2]),
		"absent tag must fall back to NullTagValue")
}
