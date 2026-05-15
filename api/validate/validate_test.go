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
	"testing"

	"github.com/stretchr/testify/assert"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

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
