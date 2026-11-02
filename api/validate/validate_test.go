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

package validate

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

func TestValidateResourceNameFormat(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "plain", input: "sw_metric", wantErr: false},
		{name: "hyphen_underscore", input: "my-group_01", wantErr: false},
		{name: "single_char", input: "g", wantErr: false},
		{name: "internal_dot", input: "service.cpm", wantErr: false},
		{name: "uppercase", input: "RBAC-ALPHA", wantErr: false},
		{name: "leading_underscore", input: "_schema", wantErr: false},
		{name: "empty", input: "", wantErr: true},
		{name: "dot", input: ".", wantErr: true},
		{name: "dotdot", input: "..", wantErr: true},
		{name: "parent_escape", input: "../outside", wantErr: true},
		{name: "nested_parent", input: "foo/../../etc", wantErr: true},
		{name: "slash", input: "a/b", wantErr: true},
		{name: "backslash", input: `a\b`, wantErr: true},
		{name: "absolute_unix", input: "/tmp/evil", wantErr: true},
		{name: "drive_prefix", input: "C:evil", wantErr: true},
		{name: "leading_hyphen", input: "-bad", wantErr: true},
		{name: "trailing_dot", input: "bad.", wantErr: true},
		{name: "space", input: "bad name", wantErr: true},
		{name: "too_long", input: strings.Repeat("a", maxResourceNameLen+1), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			formatErr := validateResourceNameFormat(tt.input)
			if tt.wantErr {
				assert.Error(t, formatErr)
			} else {
				assert.NoError(t, formatErr)
			}
		})
	}
}

func TestGroupRejectsPathEscapeName(t *testing.T) {
	group := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: "../outside"},
		Catalog:  commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	}
	err := Group(group)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid")
}

func TestGroupAcceptsEmptyStageNodeSelector(t *testing.T) {
	group := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: "sw_metric"},
		Catalog:  commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
			Stages: []*commonv1.LifecycleStage{{
				Name:            "warm",
				ShardNum:        1,
				SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
				Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
			}},
		},
	}
	assert.NoError(t, Group(group))
	assert.NoError(t, group.Validate())
}

func TestStreamRejectsPathEscapeName(t *testing.T) {
	stream := &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "ok", Group: "foo/../bar"},
		Entity:   &databasev1.Entity{TagNames: []string{"id"}},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "default", Tags: []*databasev1.TagSpec{{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING}}},
		},
	}
	err := Stream(stream)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stream group")
}

func TestMeasureAcceptsInternalTopNResultSchema(t *testing.T) {
	measure := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "_top_n_result", Group: "group1"},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "_topN",
				Tags: []*databasev1.TagSpec{
					{Name: "name", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "direction", Type: databasev1.TagType_TAG_TYPE_INT},
					{Name: "group", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "parameters", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
		Fields: []*databasev1.FieldSpec{{
			Name:              "value",
			FieldType:         databasev1.FieldType_FIELD_TYPE_DATA_BINARY,
			EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		}},
		Entity: &databasev1.Entity{TagNames: []string{"name", "direction", "group", "parameters"}},
	}
	assert.NoError(t, Measure(measure))
	assert.NoError(t, measure.Validate())
}

func TestMeasureShardingKeyNil(t *testing.T) {
	measure := &databasev1.Measure{
		Metadata: &commonv1.Metadata{
			Name:  "test_measure",
			Group: "test_group",
		},
		Entity: &databasev1.Entity{
			TagNames: []string{"service_id"},
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
	}
	err := Measure(measure)
	assert.NoError(t, err)
}

func TestMeasurePassesWithNonPrefixShardingKey(t *testing.T) {
	measure := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "endpoint_cpm", Group: "sw_metric"},
		Entity:   &databasev1.Entity{TagNames: []string{"entity_id"}},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
		ShardingKey: &databasev1.ShardingKey{TagNames: []string{"service_id"}},
	}
	err := Measure(measure)
	assert.NoError(t, err, "Measure() must not reject a non-prefix sharding key")
}

func TestValidateTracePipelineConfig_StagePluginsRejected(t *testing.T) {
	// A config with sampler plugins declared only under stages[] must be rejected
	// with a clear error — not silently accepted (which would load zero samplers
	// and retain all traces).
	stagePlugin := &commonv1.Plugin{
		Name: "lss",
		Kind: &commonv1.Plugin_Sampler{
			Sampler: &commonv1.SamplerPlugin{
				Path:       "sampler.so",
				Symbol:     "NewSampler",
				AbiVersion: 1,
			},
		},
	}
	cfg := &commonv1.TracePipelineConfig{
		Enabled: true,
		Stages: []*commonv1.StageRule{
			{Stage: "warm", Plugins: []*commonv1.Plugin{stagePlugin}},
		},
	}
	err := validateTracePipelineConfig(cfg)
	assert.Error(t, err, "stages[].plugins with a sampler must be rejected")
	assert.Contains(t, err.Error(), "stages[].plugins are not supported in v1")
}

func TestCheckShardingKeySubset(t *testing.T) {
	tests := []struct {
		name        string
		errContains string
		entity      []string
		shardingKey []string
		wantErr     bool
	}{
		{
			name:        "single entity tag — composite-id pattern, always skip",
			entity:      []string{"entity_id"},
			shardingKey: []string{"service_id"},
			wantErr:     false,
		},
		{
			name:        "valid subset, same order",
			entity:      []string{"service_id", "instance_id", "endpoint_id"},
			shardingKey: []string{"service_id", "endpoint_id"},
			wantErr:     false,
		},
		{
			name:        "valid subset, identical to entity",
			entity:      []string{"service_id", "instance_id"},
			shardingKey: []string{"service_id", "instance_id"},
			wantErr:     false,
		},
		{
			name:        "valid subset, single sharding key tag in multi-entity",
			entity:      []string{"service_id", "instance_id"},
			shardingKey: []string{"instance_id"},
			wantErr:     false,
		},
		{
			name:        "invalid — sharding key tag not in entity",
			entity:      []string{"service_id", "instance_id"},
			shardingKey: []string{"endpoint_id"},
			wantErr:     true,
			errContains: "is not present in Entity tags",
		},
		{
			name:        "invalid — superset of entity",
			entity:      []string{"service_id"},
			shardingKey: []string{"service_id", "instance_id"},
			wantErr:     false, // single entity tag — skip
		},
		{
			name:        "invalid — superset of multi-entity",
			entity:      []string{"service_id", "instance_id"},
			shardingKey: []string{"service_id", "instance_id", "endpoint_id"},
			wantErr:     true,
			errContains: "is not present in Entity tags",
		},
		{
			name:        "invalid — wrong relative order",
			entity:      []string{"service_id", "instance_id"},
			shardingKey: []string{"instance_id", "service_id"},
			wantErr:     true,
			errContains: "is not in the same relative order",
		},
		{
			name:        "nil sharding key",
			entity:      []string{"service_id"},
			shardingKey: nil,
			wantErr:     false,
		},
		{
			name:        "empty sharding key",
			entity:      []string{"service_id"},
			shardingKey: []string{},
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			measure := &databasev1.Measure{
				Metadata: &commonv1.Metadata{Name: "test", Group: "group"},
				Entity:   &databasev1.Entity{TagNames: tt.entity},
			}
			if tt.shardingKey != nil {
				measure.ShardingKey = &databasev1.ShardingKey{TagNames: tt.shardingKey}
			}
			checkErr := CheckShardingKeySubset(measure)
			if tt.wantErr {
				assert.Error(t, checkErr)
				if tt.errContains != "" {
					assert.Contains(t, checkErr.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, checkErr)
			}
		})
	}
}

// TestUniqueTagNamesRejectsDuplicateAcrossFamilies pins the resource-wide tag
// name invariant. The query layer resolves a bare tag name against a flat map
// (logical.CommonSchema.CreateRef, and BydbQL's allTags), so two families
// sharing a name would make resolution depend on iteration order.
func TestUniqueTagNamesRejectsDuplicateAcrossFamilies(t *testing.T) {
	tagFamilies := []*databasev1.TagFamilySpec{
		{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
		}},
		{Name: "other", Tags: []*databasev1.TagSpec{
			{Name: "svc", Type: databasev1.TagType_TAG_TYPE_INT},
		}},
	}
	err := UniqueTagNames(tagFamilies)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `tag name "svc" is duplicated in tag families "default" and "other"`)
}

// TestUniqueTagNamesRejectsDuplicateWithinOneFamily falls out of the same flat
// map and was likewise unchecked; it names the single family rather than
// repeating it.
func TestUniqueTagNamesRejectsDuplicateWithinOneFamily(t *testing.T) {
	tagFamilies := []*databasev1.TagFamilySpec{
		{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "id", Type: databasev1.TagType_TAG_TYPE_INT},
		}},
	}
	err := UniqueTagNames(tagFamilies)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `tag name "id" is duplicated in tag family "default"`)
}

// TestUniqueTagNamesAcceptsDistinctNames guards against over-reach: distinct
// names across several families are the normal case.
func TestUniqueTagNamesAcceptsDistinctNames(t *testing.T) {
	tagFamilies := []*databasev1.TagFamilySpec{
		{Name: "default", Tags: []*databasev1.TagSpec{{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING}}},
		{Name: "searchable", Tags: []*databasev1.TagSpec{{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING}}},
	}
	assert.NoError(t, UniqueTagNames(tagFamilies))
}

// TestMeasureAndStreamStillLoadDuplicateTagNames pins the split deliberately:
// the shared validators are what the data nodes run when loading an
// already-persisted schema (banyand/{measure,stream}/metadata.go's
// OnAddOrUpdate, which logs and drops the resource on failure). A schema
// registered before UniqueTagNames existed must keep loading, so only the
// registry's create and update paths reject it.
func TestMeasureAndStreamStillLoadDuplicateTagNames(t *testing.T) {
	duplicated := []*databasev1.TagFamilySpec{
		{Name: "default", Tags: []*databasev1.TagSpec{{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING}}},
		{Name: "searchable", Tags: []*databasev1.TagSpec{{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING}}},
	}
	measure := &databasev1.Measure{
		Metadata:    &commonv1.Metadata{Name: "legacy_measure", Group: "group1"},
		Entity:      &databasev1.Entity{TagNames: []string{"id"}},
		TagFamilies: duplicated,
	}
	assert.NoError(t, Measure(measure))
	stream := &databasev1.Stream{
		Metadata:    &commonv1.Metadata{Name: "legacy_stream", Group: "group1"},
		Entity:      &databasev1.Entity{TagNames: []string{"id"}},
		TagFamilies: duplicated,
	}
	assert.NoError(t, Stream(stream))
	assert.Error(t, UniqueTagNames(duplicated))
}
