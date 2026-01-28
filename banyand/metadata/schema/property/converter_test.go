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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

func TestSchemaToProperty_Stream(t *testing.T) {
	stream := &databasev1.Stream{
		Metadata: &commonv1.Metadata{
			Name:        "test-stream",
			Group:       "test-group",
			ModRevision: 100,
		},
		Entity: &databasev1.Entity{
			TagNames: []string{"tag1"},
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
		UpdatedAt: timestamppb.Now(),
	}

	prop, propErr := SchemaToProperty(schema.KindStream, stream)
	require.NoError(t, propErr)
	assert.NotNil(t, prop)
	assert.Equal(t, SchemaGroup, prop.Metadata.Group)
	assert.Equal(t, "stream", prop.Metadata.Name)
	assert.Equal(t, int64(100), prop.Metadata.ModRevision)
	assert.Equal(t, "stream_test-group/test-stream", prop.Id)
	assert.Len(t, prop.Tags, 5)

	tagMap := make(map[string]interface{})
	for _, tag := range prop.Tags {
		switch tag.Key {
		case TagKeyGroup:
			tagMap[tag.Key] = tag.Value.GetStr().GetValue()
		case TagKeyName:
			tagMap[tag.Key] = tag.Value.GetStr().GetValue()
		case TagKeyKind:
			tagMap[tag.Key] = tag.Value.GetStr().GetValue()
		case TagKeySource:
			tagMap[tag.Key] = tag.Value.GetStr().GetValue()
		case TagKeyUpdatedAt:
			tagMap[tag.Key] = tag.Value.GetInt().GetValue()
		}
	}
	assert.Equal(t, "test-group", tagMap[TagKeyGroup])
	assert.Equal(t, "test-stream", tagMap[TagKeyName])
	assert.Equal(t, "stream", tagMap[TagKeyKind])
	assert.NotEmpty(t, tagMap[TagKeySource])
	assert.NotZero(t, tagMap[TagKeyUpdatedAt])

	propertySchema, propErr := ToSchema(schema.KindStream, prop)
	require.NoError(t, propErr)
	equal := reflect.DeepEqual(stream, propertySchema.Spec)
	assert.True(t, equal)
}

func TestSchemaToProperty_Group(t *testing.T) {
	group := &commonv1.Group{
		Metadata: &commonv1.Metadata{
			Name:        "test-group",
			ModRevision: 50,
		},
		Catalog: commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: 2,
		},
	}

	prop, propErr := SchemaToProperty(schema.KindGroup, group)
	require.NoError(t, propErr)
	assert.NotNil(t, prop)
	assert.Equal(t, "group_test-group", prop.Id)
	assert.Equal(t, int64(50), prop.Metadata.ModRevision)
	propertySchema, propErr := ToSchema(schema.KindGroup, prop)
	require.NoError(t, propErr)
	equal := reflect.DeepEqual(group, propertySchema.Spec)
	assert.True(t, equal)
}

func TestSchemaToProperty_Measure(t *testing.T) {
	measure := &databasev1.Measure{
		Metadata: &commonv1.Metadata{
			Name:        "test-measure",
			Group:       "metrics",
			ModRevision: 200,
		},
		Entity: &databasev1.Entity{
			TagNames: []string{"service"},
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
		Fields: []*databasev1.FieldSpec{
			{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		},
	}

	prop, propErr := SchemaToProperty(schema.KindMeasure, measure)
	require.NoError(t, propErr)
	assert.NotNil(t, prop)
	assert.Equal(t, "measure_metrics/test-measure", prop.Id)
	propertySchema, propErr := ToSchema(schema.KindMeasure, prop)
	require.NoError(t, propErr)
	equal := reflect.DeepEqual(measure, propertySchema.Spec)
	assert.True(t, equal)
}

func TestSchemaToProperty_Trace(t *testing.T) {
	trace := &databasev1.Trace{
		Metadata: &commonv1.Metadata{
			Name:        "test-trace",
			Group:       "tracing",
			ModRevision: 300,
		},
		TraceIdTagName: "trace_id",
	}

	prop, propErr := SchemaToProperty(schema.KindTrace, trace)
	require.NoError(t, propErr)
	assert.NotNil(t, prop)
	assert.Equal(t, "trace_tracing/test-trace", prop.Id)
	propertySchema, propErr := ToSchema(schema.KindTrace, prop)
	require.NoError(t, propErr)
	equal := reflect.DeepEqual(trace, propertySchema.Spec)
	assert.True(t, equal)
}

func TestSchemaToProperty_IndexRule(t *testing.T) {
	indexRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{
			Name:        "rule1",
			Group:       "default",
			ModRevision: 150,
		},
		Tags: []string{"tag1"},
		Type: databasev1.IndexRule_TYPE_INVERTED,
	}

	prop, propErr := SchemaToProperty(schema.KindIndexRule, indexRule)
	require.NoError(t, propErr)
	assert.NotNil(t, prop)
	assert.Equal(t, "indexRule_default/rule1", prop.Id)
	propertySchema, propErr := ToSchema(schema.KindIndexRule, prop)
	require.NoError(t, propErr)
	equal := reflect.DeepEqual(indexRule, propertySchema.Spec)
	assert.True(t, equal)
}

func TestSchemaToProperty_IndexRuleBinding(t *testing.T) {
	binding := &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{
			Name:        "binding1",
			Group:       "default",
			ModRevision: 160,
		},
		Subject: &databasev1.Subject{Name: "stream1"},
		Rules:   []string{"rule1"},
	}

	prop, propErr := SchemaToProperty(schema.KindIndexRuleBinding, binding)
	require.NoError(t, propErr)
	assert.NotNil(t, prop)
	assert.Equal(t, "indexRuleBinding_default/binding1", prop.Id)
	propertySchema, propErr := ToSchema(schema.KindIndexRuleBinding, prop)
	require.NoError(t, propErr)
	equal := reflect.DeepEqual(binding, propertySchema.Spec)
	assert.True(t, equal)
}

func TestSchemaToProperty_TopNAggregation(t *testing.T) {
	topN := &databasev1.TopNAggregation{
		Metadata: &commonv1.Metadata{
			Name:        "topn1",
			Group:       "metrics",
			ModRevision: 170,
		},
		SourceMeasure:  &commonv1.Metadata{Name: "measure1", Group: "metrics"},
		FieldName:      "value",
		CountersNumber: 10,
	}

	prop, propErr := SchemaToProperty(schema.KindTopNAggregation, topN)
	require.NoError(t, propErr)
	assert.NotNil(t, prop)
	assert.Equal(t, "topNAggregation_metrics/topn1", prop.Id)
	propertySchema, propErr := ToSchema(schema.KindTopNAggregation, prop)
	require.NoError(t, propErr)
	equal := reflect.DeepEqual(topN, propertySchema.Spec)
	assert.True(t, equal)
}

func TestSchemaToProperty_Property(t *testing.T) {
	property := &databasev1.Property{
		Metadata: &commonv1.Metadata{
			Name:        "prop1",
			Group:       "default",
			ModRevision: 180,
		},
		Tags: []*databasev1.TagSpec{
			{Name: "key1", Type: databasev1.TagType_TAG_TYPE_STRING},
		},
	}

	prop, propErr := SchemaToProperty(schema.KindProperty, property)
	require.NoError(t, propErr)
	assert.NotNil(t, prop)
	assert.Equal(t, "property_default/prop1", prop.Id)
	propertySchema, propErr := ToSchema(schema.KindProperty, prop)
	require.NoError(t, propErr)
	equal := reflect.DeepEqual(property, propertySchema.Spec)
	assert.True(t, equal)
}

func TestToSchema_Stream(t *testing.T) {
	stream := &databasev1.Stream{
		Metadata: &commonv1.Metadata{
			Name:        "test-stream",
			Group:       "test-group",
			ModRevision: 100,
		},
		Entity: &databasev1.Entity{
			TagNames: []string{"tag1"},
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
	}

	prop, propErr := SchemaToProperty(schema.KindStream, stream)
	require.NoError(t, propErr)

	md, schemaErr := ToSchema(schema.KindStream, prop)
	require.NoError(t, schemaErr)
	assert.Equal(t, schema.KindStream, md.Kind)
	assert.Equal(t, "test-stream", md.Name)
	assert.Equal(t, "test-group", md.Group)

	resultStream, ok := md.Spec.(*databasev1.Stream)
	require.True(t, ok)
	assert.Equal(t, "test-stream", resultStream.Metadata.Name)
	assert.Equal(t, "test-group", resultStream.Metadata.Group)
	assert.Equal(t, []string{"tag1"}, resultStream.Entity.TagNames)
	equal := reflect.DeepEqual(stream, md.Spec)
	assert.True(t, equal)
}

func TestToSchema_Measure(t *testing.T) {
	measure := &databasev1.Measure{
		Metadata: &commonv1.Metadata{
			Name:        "test-measure",
			Group:       "metrics",
			ModRevision: 200,
		},
		Entity: &databasev1.Entity{
			TagNames: []string{"service"},
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
		Fields: []*databasev1.FieldSpec{
			{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		},
	}

	prop, propErr := SchemaToProperty(schema.KindMeasure, measure)
	require.NoError(t, propErr)

	md, schemaErr := ToSchema(schema.KindMeasure, prop)
	require.NoError(t, schemaErr)
	assert.Equal(t, schema.KindMeasure, md.Kind)
	assert.Equal(t, "test-measure", md.Name)
	assert.Equal(t, "metrics", md.Group)

	resultMeasure, ok := md.Spec.(*databasev1.Measure)
	require.True(t, ok)
	assert.Equal(t, "test-measure", resultMeasure.Metadata.Name)
	assert.Len(t, resultMeasure.Fields, 1)
	equal := reflect.DeepEqual(measure, md.Spec)
	assert.True(t, equal)
}

func TestToSchema_Group(t *testing.T) {
	group := &commonv1.Group{
		Metadata: &commonv1.Metadata{
			Name:        "test-group",
			ModRevision: 50,
		},
		Catalog: commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: 2,
		},
	}

	prop, propErr := SchemaToProperty(schema.KindGroup, group)
	require.NoError(t, propErr)

	md, schemaErr := ToSchema(schema.KindGroup, prop)
	require.NoError(t, schemaErr)
	assert.Equal(t, schema.KindGroup, md.Kind)
	assert.Equal(t, "test-group", md.Name)

	resultGroup, ok := md.Spec.(*commonv1.Group)
	require.True(t, ok)
	assert.Equal(t, commonv1.Catalog_CATALOG_STREAM, resultGroup.Catalog)
	assert.Equal(t, uint32(2), resultGroup.ResourceOpts.ShardNum)
	equal := reflect.DeepEqual(group, md.Spec)
	assert.True(t, equal)
}

func TestBuildPropertyID(t *testing.T) {
	tests := []struct {
		metadata *commonv1.Metadata
		name     string
		expected string
		kind     schema.Kind
	}{
		{
			name:     "stream with group",
			kind:     schema.KindStream,
			metadata: &commonv1.Metadata{Name: "mystream", Group: "mygroup"},
			expected: "stream_mygroup/mystream",
		},
		{
			name:     "measure with group",
			kind:     schema.KindMeasure,
			metadata: &commonv1.Metadata{Name: "mymeasure", Group: "metrics"},
			expected: "measure_metrics/mymeasure",
		},
		{
			name:     "group without group field",
			kind:     schema.KindGroup,
			metadata: &commonv1.Metadata{Name: "mygroup"},
			expected: "group_mygroup",
		},
		{
			name:     "index rule with group",
			kind:     schema.KindIndexRule,
			metadata: &commonv1.Metadata{Name: "rule1", Group: "default"},
			expected: "indexRule_default/rule1",
		},
		{
			name:     "index rule binding with group",
			kind:     schema.KindIndexRuleBinding,
			metadata: &commonv1.Metadata{Name: "binding1", Group: "default"},
			expected: "indexRuleBinding_default/binding1",
		},
		{
			name:     "topN aggregation with group",
			kind:     schema.KindTopNAggregation,
			metadata: &commonv1.Metadata{Name: "topn1", Group: "metrics"},
			expected: "topNAggregation_metrics/topn1",
		},
		{
			name:     "trace with group",
			kind:     schema.KindTrace,
			metadata: &commonv1.Metadata{Name: "trace1", Group: "tracing"},
			expected: "trace_tracing/trace1",
		},
		{
			name:     "property with group",
			kind:     schema.KindProperty,
			metadata: &commonv1.Metadata{Name: "prop1", Group: "default"},
			expected: "property_default/prop1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildPropertyID(tt.kind, tt.metadata)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildPropertyIDFromMeta(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		meta     schema.TypeMeta
	}{
		{
			name:     "stream with group",
			meta:     schema.TypeMeta{Kind: schema.KindStream, Name: "mystream", Group: "mygroup"},
			expected: "stream_mygroup/mystream",
		},
		{
			name:     "group without group field",
			meta:     schema.TypeMeta{Kind: schema.KindGroup, Name: "mygroup"},
			expected: "group_mygroup",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildPropertyIDFromMeta(tt.meta)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestKindFromString(t *testing.T) {
	tests := []struct {
		name        string
		kindStr     string
		expected    schema.Kind
		expectError bool
	}{
		{name: "group", kindStr: "group", expected: schema.KindGroup},
		{name: "stream", kindStr: "stream", expected: schema.KindStream},
		{name: "measure", kindStr: "measure", expected: schema.KindMeasure},
		{name: "trace", kindStr: "trace", expected: schema.KindTrace},
		{name: "indexRuleBinding", kindStr: "indexRuleBinding", expected: schema.KindIndexRuleBinding},
		{name: "indexRule", kindStr: "indexRule", expected: schema.KindIndexRule},
		{name: "topNAggregation", kindStr: "topNAggregation", expected: schema.KindTopNAggregation},
		{name: "node", kindStr: "node", expected: schema.KindNode},
		{name: "property", kindStr: "property", expected: schema.KindProperty},
		{name: "unknown", kindStr: "unknown", expectError: true},
		{name: "empty", kindStr: "", expectError: true},
		{name: "invalid", kindStr: "invalid_kind", expectError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, resultErr := KindFromString(tt.kindStr)
			if tt.expectError {
				assert.Error(t, resultErr)
			} else {
				require.NoError(t, resultErr)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestSchemaToProperty_UnsupportedKind(t *testing.T) {
	stream := &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "test", Group: "test"},
	}
	_, propErr := SchemaToProperty(schema.KindMask, stream)
	assert.Error(t, propErr)
}

func TestToSchema_MissingSourceTag(t *testing.T) {
	prop := &propertyv1.Property{
		Metadata: &commonv1.Metadata{
			Group: SchemaGroup,
			Name:  "stream",
		},
		Id:   "stream_test/test",
		Tags: []*modelv1.Tag{},
	}
	_, schemaErr := ToSchema(schema.KindStream, prop)
	assert.Error(t, schemaErr)
}

func TestRoundTrip_Stream(t *testing.T) {
	stream := &databasev1.Stream{
		Metadata:    &commonv1.Metadata{Name: "stream1", Group: "group1", ModRevision: 100},
		Entity:      &databasev1.Entity{TagNames: []string{"tag1"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}}},
	}

	prop, toPropertyErr := SchemaToProperty(schema.KindStream, stream)
	require.NoError(t, toPropertyErr)

	md, toSchemaErr := ToSchema(schema.KindStream, prop)
	require.NoError(t, toSchemaErr)
	assert.Equal(t, schema.KindStream, md.Kind)
	assert.Equal(t, "stream1", md.Name)
	assert.Equal(t, "group1", md.Group)
	equal := reflect.DeepEqual(stream, md.Spec)
	assert.True(t, equal)
}

func TestRoundTrip_Measure(t *testing.T) {
	measure := &databasev1.Measure{
		Metadata:    &commonv1.Metadata{Name: "measure1", Group: "metrics", ModRevision: 200},
		Entity:      &databasev1.Entity{TagNames: []string{"service"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING}}}},
		Fields:      []*databasev1.FieldSpec{{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}

	prop, toPropertyErr := SchemaToProperty(schema.KindMeasure, measure)
	require.NoError(t, toPropertyErr)

	md, toSchemaErr := ToSchema(schema.KindMeasure, prop)
	require.NoError(t, toSchemaErr)
	assert.Equal(t, schema.KindMeasure, md.Kind)
	assert.Equal(t, "measure1", md.Name)
	assert.Equal(t, "metrics", md.Group)
	equal := reflect.DeepEqual(measure, md.Spec)
	assert.True(t, equal)
}

func TestRoundTrip_Group(t *testing.T) {
	group := &commonv1.Group{
		Metadata:     &commonv1.Metadata{Name: "group1", ModRevision: 50},
		Catalog:      commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{ShardNum: 2},
	}

	prop, toPropertyErr := SchemaToProperty(schema.KindGroup, group)
	require.NoError(t, toPropertyErr)

	md, toSchemaErr := ToSchema(schema.KindGroup, prop)
	require.NoError(t, toSchemaErr)
	assert.Equal(t, schema.KindGroup, md.Kind)
	assert.Equal(t, "group1", md.Name)
	equal := reflect.DeepEqual(group, md.Spec)
	assert.True(t, equal)
}
