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

package property

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

func requireFiveTags(t *testing.T, prop *propertyv1.Property) {
	t.Helper()
	require.Len(t, prop.GetTags(), 5)
	tagKeys := make([]string, 5)
	for idx, tag := range prop.GetTags() {
		tagKeys[idx] = tag.GetKey()
	}
	assert.Contains(t, tagKeys, TagKeyGroup)
	assert.Contains(t, tagKeys, TagKeyName)
	assert.Contains(t, tagKeys, TagKeySource)
	assert.Contains(t, tagKeys, TagKeyKind)
	assert.Contains(t, tagKeys, TagKeyUpdatedAt)
}

func TestSchemaToPropertyAndBack_Group(t *testing.T) {
	original := &commonv1.Group{
		Metadata: &commonv1.Metadata{
			Name:        "test-group",
			ModRevision: 100,
		},
		Catalog:   commonv1.Catalog_CATALOG_STREAM,
		UpdatedAt: timestamppb.Now(),
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        2,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	}
	prop, convErr := SchemaToProperty(schema.KindGroup, original)
	require.NoError(t, convErr)
	assert.Equal(t, "group", prop.GetMetadata().GetName())
	assert.Equal(t, int64(100), prop.GetMetadata().GetModRevision())
	assert.Equal(t, "group_test-group", prop.GetId())
	requireFiveTags(t, prop)

	md, backErr := ToSchema(schema.KindGroup, prop)
	require.NoError(t, backErr)
	assert.Equal(t, schema.KindGroup, md.Kind)
	assert.Equal(t, "test-group", md.Name)
	got, ok := md.Spec.(*commonv1.Group)
	require.True(t, ok)
	assert.Equal(t, "test-group", got.GetMetadata().GetName())
	assert.Equal(t, commonv1.Catalog_CATALOG_STREAM, got.GetCatalog())
	assert.Equal(t, uint32(2), got.GetResourceOpts().GetShardNum())
}

func TestSchemaToPropertyAndBack_Stream(t *testing.T) {
	original := &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "test-stream", Group: "g1", ModRevision: 200},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "default", Tags: []*databasev1.TagSpec{{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING}}},
		},
		Entity:    &databasev1.Entity{TagNames: []string{"svc"}},
		UpdatedAt: timestamppb.Now(),
	}
	prop, convErr := SchemaToProperty(schema.KindStream, original)
	require.NoError(t, convErr)
	assert.Equal(t, "stream_g1/test-stream", prop.GetId())
	assert.Equal(t, "stream", prop.GetMetadata().GetName())
	requireFiveTags(t, prop)

	md, backErr := ToSchema(schema.KindStream, prop)
	require.NoError(t, backErr)
	assert.Equal(t, schema.KindStream, md.Kind)
	assert.Equal(t, "g1", md.Group)
	assert.Equal(t, "test-stream", md.Name)
	got, ok := md.Spec.(*databasev1.Stream)
	require.True(t, ok)
	assert.Equal(t, "test-stream", got.GetMetadata().GetName())
	assert.Len(t, got.GetTagFamilies(), 1)
}

func TestSchemaToPropertyAndBack_Measure(t *testing.T) {
	original := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "test-measure", Group: "g1", ModRevision: 300},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "default", Tags: []*databasev1.TagSpec{{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING}}},
		},
		Fields: []*databasev1.FieldSpec{
			{
				Name:              "value",
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			},
		},
		Entity:    &databasev1.Entity{TagNames: []string{"svc"}},
		UpdatedAt: timestamppb.Now(),
	}
	prop, convErr := SchemaToProperty(schema.KindMeasure, original)
	require.NoError(t, convErr)
	requireFiveTags(t, prop)

	md, backErr := ToSchema(schema.KindMeasure, prop)
	require.NoError(t, backErr)
	assert.Equal(t, schema.KindMeasure, md.Kind)
	assert.Equal(t, "g1", md.Group)
	got, ok := md.Spec.(*databasev1.Measure)
	require.True(t, ok)
	assert.Len(t, got.GetFields(), 1)
}

func TestSchemaToPropertyAndBack_Trace(t *testing.T) {
	original := &databasev1.Trace{
		Metadata: &commonv1.Metadata{Name: "test-trace", Group: "g1", ModRevision: 400},
		Tags: []*databasev1.TraceTagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
		},
		TraceIdTagName: "trace_id",
		SpanIdTagName:  "span_id",
		UpdatedAt:      timestamppb.Now(),
	}
	prop, convErr := SchemaToProperty(schema.KindTrace, original)
	require.NoError(t, convErr)
	requireFiveTags(t, prop)

	md, backErr := ToSchema(schema.KindTrace, prop)
	require.NoError(t, backErr)
	assert.Equal(t, schema.KindTrace, md.Kind)
	got, ok := md.Spec.(*databasev1.Trace)
	require.True(t, ok)
	assert.Len(t, got.GetTags(), 2)
	assert.Equal(t, "trace_id", got.GetTraceIdTagName())
}

func TestSchemaToPropertyAndBack_IndexRule(t *testing.T) {
	original := &databasev1.IndexRule{
		Metadata:  &commonv1.Metadata{Name: "test-ir", Group: "g1", Id: 42, ModRevision: 500},
		Tags:      []string{"svc"},
		Type:      databasev1.IndexRule_TYPE_INVERTED,
		UpdatedAt: timestamppb.Now(),
	}
	prop, convErr := SchemaToProperty(schema.KindIndexRule, original)
	require.NoError(t, convErr)
	requireFiveTags(t, prop)

	md, backErr := ToSchema(schema.KindIndexRule, prop)
	require.NoError(t, backErr)
	got, ok := md.Spec.(*databasev1.IndexRule)
	require.True(t, ok)
	assert.Equal(t, uint32(42), got.GetMetadata().GetId())
	assert.Equal(t, []string{"svc"}, got.GetTags())
}

func TestSchemaToPropertyAndBack_IndexRuleBinding(t *testing.T) {
	original := &databasev1.IndexRuleBinding{
		Metadata:  &commonv1.Metadata{Name: "test-irb", Group: "g1", ModRevision: 600},
		Rules:     []string{"rule1", "rule2"},
		Subject:   &databasev1.Subject{Name: "test-stream", Catalog: commonv1.Catalog_CATALOG_STREAM},
		UpdatedAt: timestamppb.Now(),
	}
	prop, convErr := SchemaToProperty(schema.KindIndexRuleBinding, original)
	require.NoError(t, convErr)
	requireFiveTags(t, prop)

	md, backErr := ToSchema(schema.KindIndexRuleBinding, prop)
	require.NoError(t, backErr)
	got, ok := md.Spec.(*databasev1.IndexRuleBinding)
	require.True(t, ok)
	assert.Equal(t, []string{"rule1", "rule2"}, got.GetRules())
}

func TestSchemaToPropertyAndBack_TopNAggregation(t *testing.T) {
	original := &databasev1.TopNAggregation{
		Metadata:       &commonv1.Metadata{Name: "test-topn", Group: "g1", ModRevision: 700},
		SourceMeasure:  &commonv1.Metadata{Name: "test-measure", Group: "g1"},
		FieldName:      "value",
		CountersNumber: 10,
		UpdatedAt:      timestamppb.Now(),
	}
	prop, convErr := SchemaToProperty(schema.KindTopNAggregation, original)
	require.NoError(t, convErr)
	requireFiveTags(t, prop)

	md, backErr := ToSchema(schema.KindTopNAggregation, prop)
	require.NoError(t, backErr)
	got, ok := md.Spec.(*databasev1.TopNAggregation)
	require.True(t, ok)
	assert.Equal(t, "value", got.GetFieldName())
	assert.Equal(t, int32(10), got.GetCountersNumber())
}

func TestSchemaToPropertyAndBack_Node(t *testing.T) {
	original := &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: "node-1"},
		GrpcAddress: "127.0.0.1:17912",
		Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
	}
	prop, convErr := SchemaToProperty(schema.KindNode, original)
	require.NoError(t, convErr)
	requireFiveTags(t, prop)

	md, backErr := ToSchema(schema.KindNode, prop)
	require.NoError(t, backErr)
	assert.Equal(t, "node-1", md.Name)
	got, ok := md.Spec.(*databasev1.Node)
	require.True(t, ok)
	assert.Equal(t, "127.0.0.1:17912", got.GetGrpcAddress())
}

func TestSchemaToPropertyAndBack_Property(t *testing.T) {
	original := &databasev1.Property{
		Metadata:  &commonv1.Metadata{Name: "test-prop", Group: "g1", ModRevision: 900},
		UpdatedAt: timestamppb.Now(),
	}
	prop, convErr := SchemaToProperty(schema.KindProperty, original)
	require.NoError(t, convErr)
	requireFiveTags(t, prop)

	md, backErr := ToSchema(schema.KindProperty, prop)
	require.NoError(t, backErr)
	assert.Equal(t, schema.KindProperty, md.Kind)
	got, ok := md.Spec.(*databasev1.Property)
	require.True(t, ok)
	assert.Equal(t, "test-prop", got.GetMetadata().GetName())
}

func TestToSchema_ModRevisionPropagated(t *testing.T) {
	original := &databasev1.Stream{
		Metadata:  &commonv1.Metadata{Name: "s1", Group: "g1", ModRevision: 1},
		Entity:    &databasev1.Entity{TagNames: []string{}},
		UpdatedAt: timestamppb.Now(),
	}
	prop, convErr := SchemaToProperty(schema.KindStream, original)
	require.NoError(t, convErr)
	prop.Metadata.ModRevision = 999
	md, backErr := ToSchema(schema.KindStream, prop)
	require.NoError(t, backErr)
	assert.Equal(t, int64(999), md.ModRevision)
	got := md.Spec.(*databasev1.Stream)
	assert.Equal(t, int64(999), got.GetMetadata().GetModRevision())
}

func TestToSchema_NilProperty(t *testing.T) {
	_, convErr := ToSchema(schema.KindStream, nil)
	require.Error(t, convErr)
	assert.Contains(t, convErr.Error(), "property is nil")
}

func TestToSchema_MissingSourceTag(t *testing.T) {
	prop := &propertyv1.Property{
		Metadata: &commonv1.Metadata{Name: "stream"},
		Id:       "stream_g1/name",
		Tags:     []*modelv1.Tag{},
	}
	_, convErr := ToSchema(schema.KindStream, prop)
	require.Error(t, convErr)
	assert.Contains(t, convErr.Error(), "has no source tag")
}

func TestToSchema_BadJSON(t *testing.T) {
	prop := &propertyv1.Property{
		Metadata: &commonv1.Metadata{Name: "stream"},
		Id:       "stream_g1/name",
		Tags: []*modelv1.Tag{
			{Key: TagKeySource, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "not-valid-json{{{"}}}},
		},
	}
	_, convErr := ToSchema(schema.KindStream, prop)
	require.Error(t, convErr)
	assert.Contains(t, convErr.Error(), "failed to unmarshal schema from property")
}

func TestToSchema_UnsupportedKind(t *testing.T) {
	prop := &propertyv1.Property{
		Metadata: &commonv1.Metadata{Name: "test"},
		Id:       "unknown_g1/name",
		Tags: []*modelv1.Tag{
			{Key: TagKeySource, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "{}"}}}},
		},
	}
	_, convErr := ToSchema(schema.Kind(0), prop)
	require.Error(t, convErr)
	assert.ErrorIs(t, convErr, schema.ErrUnsupportedEntityType)
}

func TestBuildPropertyID(t *testing.T) {
	assert.Equal(t, "stream_g1/name1", BuildPropertyID(schema.KindStream, &commonv1.Metadata{Group: "g1", Name: "name1"}))
	assert.Equal(t, "group_test-group", BuildPropertyID(schema.KindGroup, &commonv1.Metadata{Name: "test-group"}))
	assert.Equal(t, "measure_g1/m1", BuildPropertyID(schema.KindMeasure, &commonv1.Metadata{Group: "g1", Name: "m1"}))
}

func TestBuildPropertyIDFromMeta(t *testing.T) {
	meta := schema.TypeMeta{Kind: schema.KindStream, Group: "g1", Name: "s1"}
	assert.Equal(t, "stream_g1/s1", BuildPropertyIDFromMeta(meta))

	groupMeta := schema.TypeMeta{Kind: schema.KindGroup, Name: "test-group"}
	assert.Equal(t, "group_test-group", BuildPropertyIDFromMeta(groupMeta))
}

func TestKindFromString_AllKinds(t *testing.T) {
	allKinds := []schema.Kind{
		schema.KindGroup, schema.KindStream, schema.KindMeasure, schema.KindTrace,
		schema.KindIndexRuleBinding, schema.KindIndexRule, schema.KindTopNAggregation,
		schema.KindNode, schema.KindProperty,
	}
	for _, k := range allKinds {
		t.Run(k.String(), func(t *testing.T) {
			got, kindErr := KindFromString(k.String())
			require.NoError(t, kindErr)
			assert.Equal(t, k, got)
		})
	}
}

func TestKindFromString_Unknown(t *testing.T) {
	_, kindErr := KindFromString("nonexistent")
	require.Error(t, kindErr)
	assert.Contains(t, kindErr.Error(), "unknown kind string")
}

func TestBuildSchemaQuery(t *testing.T) {
	query := buildSchemaQuery(schema.KindStream, "", "", 0)
	assert.Equal(t, "stream", query.GetName())
	assert.Nil(t, query.GetCriteria())
	assert.Empty(t, query.GetIds())
	assert.Equal(t, []string{schema.SchemaGroup}, query.GetGroups())
}

func TestBuildSchemaQuery_WithGroup(t *testing.T) {
	query := buildSchemaQuery(schema.KindStream, "g1", "", 0)
	assert.Equal(t, "stream", query.GetName())
	assert.Equal(t, []string{schema.SchemaGroup}, query.GetGroups())
	assert.Empty(t, query.GetIds())
	require.NotNil(t, query.GetCriteria())
	cond := query.GetCriteria().GetCondition()
	require.NotNil(t, cond)
	assert.Equal(t, TagKeyGroup, cond.GetName())
	assert.Equal(t, "g1", cond.GetValue().GetStr().GetValue())
}

func TestBuildSchemaQuery_WithGroupAndName(t *testing.T) {
	query := buildSchemaQuery(schema.KindStream, "g1", "s1", 0)
	assert.Equal(t, "stream", query.GetName())
	assert.Equal(t, []string{"stream_g1/s1"}, query.GetIds())
	assert.Equal(t, []string{schema.SchemaGroup}, query.GetGroups())
	assert.Nil(t, query.GetCriteria())
}

func TestBuildSchemaQuery_KindGroup(t *testing.T) {
	query := buildSchemaQuery(schema.KindGroup, "", "mygroup", 0)
	assert.Equal(t, "group", query.GetName())
	assert.Equal(t, []string{"group_mygroup"}, query.GetIds())
	assert.Equal(t, []string{schema.SchemaGroup}, query.GetGroups())
}

func TestBuildSchemaQuery_WithSinceRevision(t *testing.T) {
	query := buildSchemaQuery(schema.KindMeasure, "", "", 100)
	assert.Equal(t, "measure", query.GetName())
	assert.Empty(t, query.GetIds())
	require.NotNil(t, query.GetCriteria())
	cond := query.GetCriteria().GetCondition()
	require.NotNil(t, cond)
	assert.Equal(t, TagKeyUpdatedAt, cond.GetName())
	assert.Equal(t, modelv1.Condition_BINARY_OP_GT, cond.GetOp())
	assert.Equal(t, int64(100), cond.GetValue().GetInt().GetValue())
}

func TestBuildSchemaQuery_WithGroupAndSinceRevision(t *testing.T) {
	query := buildSchemaQuery(schema.KindStream, "g1", "", 200)
	assert.Equal(t, "stream", query.GetName())
	assert.Empty(t, query.GetIds())
	require.NotNil(t, query.GetCriteria())
	le := query.GetCriteria().GetLe()
	require.NotNil(t, le, "expected LogicalExpression for AND criteria")
	assert.Equal(t, modelv1.LogicalExpression_LOGICAL_OP_AND, le.GetOp())
	leftCond := le.GetLeft().GetCondition()
	require.NotNil(t, leftCond)
	assert.Equal(t, TagKeyGroup, leftCond.GetName())
	assert.Equal(t, "g1", leftCond.GetValue().GetStr().GetValue())
	rightCond := le.GetRight().GetCondition()
	require.NotNil(t, rightCond)
	assert.Equal(t, TagKeyUpdatedAt, rightCond.GetName())
	assert.Equal(t, modelv1.Condition_BINARY_OP_GT, rightCond.GetOp())
	assert.Equal(t, int64(200), rightCond.GetValue().GetInt().GetValue())
}

func TestBuildSchemaQuery_WithNameIgnoresSinceRevision(t *testing.T) {
	query := buildSchemaQuery(schema.KindStream, "g1", "s1", 500)
	assert.Equal(t, "stream", query.GetName())
	assert.Equal(t, []string{"stream_g1/s1"}, query.GetIds())
	assert.Nil(t, query.GetCriteria(), "name-based lookup should not have criteria even with sinceRevision")
}

func TestBuildUpdatedSchemasQuery_Zero(t *testing.T) {
	query := buildUpdatedSchemasQuery(0)
	assert.Nil(t, query.GetCriteria())
	assert.Empty(t, query.GetName())
	assert.Equal(t, []string{schema.SchemaGroup}, query.GetGroups())
}

func TestBuildUpdatedSchemasQuery_Positive(t *testing.T) {
	query := buildUpdatedSchemasQuery(100)
	assert.NotNil(t, query.GetCriteria())
	assert.Equal(t, []string{schema.SchemaGroup}, query.GetGroups())
}

func TestBuildDeleteRequest(t *testing.T) {
	req := buildDeleteRequest(schema.KindMeasure, "g1", "m1")
	assert.Equal(t, schema.SchemaGroup, req.GetDelete().GetGroup())
	assert.Equal(t, "measure", req.GetDelete().GetName())
	assert.Equal(t, "measure_g1/m1", req.GetDelete().GetId())
	assert.NotNil(t, req.GetUpdateAt())
}

func TestNewMessageForKind_AllKinds(t *testing.T) {
	tests := []struct {
		want    proto.Message
		kind    schema.Kind
		wantErr bool
	}{
		{kind: schema.KindGroup, want: &commonv1.Group{}},
		{kind: schema.KindStream, want: &databasev1.Stream{}},
		{kind: schema.KindMeasure, want: &databasev1.Measure{}},
		{kind: schema.KindTrace, want: &databasev1.Trace{}},
		{kind: schema.KindIndexRuleBinding, want: &databasev1.IndexRuleBinding{}},
		{kind: schema.KindIndexRule, want: &databasev1.IndexRule{}},
		{kind: schema.KindTopNAggregation, want: &databasev1.TopNAggregation{}},
		{kind: schema.KindNode, want: &databasev1.Node{}},
		{kind: schema.KindProperty, want: &databasev1.Property{}},
		{kind: schema.Kind(0), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.kind.String(), func(t *testing.T) {
			got, msgErr := newMessageForKind(tt.kind)
			if tt.wantErr {
				require.Error(t, msgErr)
				assert.ErrorIs(t, msgErr, schema.ErrUnsupportedEntityType)
				return
			}
			require.NoError(t, msgErr)
			assert.IsType(t, tt.want, got)
		})
	}
}

func TestParseTags_AllFields(t *testing.T) {
	tags := []*modelv1.Tag{
		{Key: TagKeyGroup, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "g1"}}}},
		{Key: TagKeyName, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "n1"}}}},
		{Key: TagKeySource, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "src-data"}}}},
		{Key: TagKeyKind, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "stream"}}}},
		{Key: TagKeyUpdatedAt, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 12345}}}},
	}
	pt := ParseTags(tags)
	assert.Equal(t, "g1", pt.Group)
	assert.Equal(t, "n1", pt.Name)
	assert.Equal(t, "src-data", pt.Source)
	assert.Equal(t, "stream", pt.Kind)
	assert.Equal(t, int64(12345), pt.UpdatedAt)
}

func TestParseTags_Partial(t *testing.T) {
	tags := []*modelv1.Tag{
		{Key: TagKeyGroup, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "g2"}}}},
		{Key: TagKeyUpdatedAt, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 99}}}},
	}
	pt := ParseTags(tags)
	assert.Equal(t, "g2", pt.Group)
	assert.Equal(t, "", pt.Name)
	assert.Equal(t, "", pt.Source)
	assert.Equal(t, "", pt.Kind)
	assert.Equal(t, int64(99), pt.UpdatedAt)
}

func TestParseTags_Empty(t *testing.T) {
	pt := ParseTags(nil)
	assert.Equal(t, "", pt.Group)
	assert.Equal(t, "", pt.Name)
	assert.Equal(t, "", pt.Source)
	assert.Equal(t, "", pt.Kind)
	assert.Equal(t, int64(0), pt.UpdatedAt)
}

func TestParseTags_UnknownKeysIgnored(t *testing.T) {
	tags := []*modelv1.Tag{
		{Key: "unknown_key", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "val"}}}},
		{Key: TagKeyName, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "myname"}}}},
	}
	pt := ParseTags(tags)
	assert.Equal(t, "myname", pt.Name)
	assert.Equal(t, "", pt.Group)
}

func TestParseTags_ConsistentWithHelpers(t *testing.T) {
	tags := []*modelv1.Tag{
		{Key: TagKeyGroup, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "g1"}}}},
		{Key: TagKeyName, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "n1"}}}},
		{Key: TagKeyUpdatedAt, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 555}}}},
	}
	pt := ParseTags(tags)
	assert.Equal(t, "g1", pt.Group)
	assert.Equal(t, "n1", pt.Name)
	assert.Equal(t, int64(555), pt.UpdatedAt)
}

func TestGenerateCRC32ID(t *testing.T) {
	id1 := generateCRC32ID("g1", "name1")
	assert.Greater(t, id1, uint32(0))
	id2 := generateCRC32ID("g1", "name1")
	assert.Equal(t, id1, id2)
	id3 := generateCRC32ID("g1", "name2")
	assert.NotEqual(t, id1, id3)
	id4 := generateCRC32ID("g2", "name1")
	assert.NotEqual(t, id1, id4)
}
