// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package measure

import (
	"testing"

	"github.com/stretchr/testify/assert"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

func TestCountsActiveSeries(t *testing.T) {
	tests := []struct {
		measure *databasev1.Measure
		name    string
		want    bool
	}{
		{
			name:    "no sharding key",
			measure: &databasev1.Measure{Metadata: &commonv1.Metadata{Name: "m"}, Entity: &databasev1.Entity{TagNames: []string{"a", "b"}}},
			want:    true,
		},
		{
			name:    "top-n result measure",
			measure: &databasev1.Measure{Metadata: &commonv1.Metadata{Name: TopNSchemaName}, Entity: &databasev1.Entity{TagNames: []string{"a", "b"}}},
			want:    false,
		},
		{
			name: "sharding key is not a subset of a multi-tag entity",
			measure: &databasev1.Measure{
				Metadata:    &commonv1.Metadata{Name: "m"},
				Entity:      &databasev1.Entity{TagNames: []string{"a", "b"}},
				ShardingKey: &databasev1.ShardingKey{TagNames: []string{"c"}},
			},
			want: false,
		},
		{
			name: "sharding key is a subset of the entity",
			measure: &databasev1.Measure{
				Metadata:    &commonv1.Metadata{Name: "m"},
				Entity:      &databasev1.Entity{TagNames: []string{"a", "b"}},
				ShardingKey: &databasev1.ShardingKey{TagNames: []string{"b"}},
			},
			want: true,
		},
		{
			name: "single tag entity with a sharding key",
			measure: &databasev1.Measure{
				Metadata:    &commonv1.Metadata{Name: "m"},
				Entity:      &databasev1.Entity{TagNames: []string{"entity_id"}},
				ShardingKey: &databasev1.ShardingKey{TagNames: []string{"service_id"}},
			},
			want: true,
		},
		{
			name: "index mode",
			measure: &databasev1.Measure{
				Metadata:  &commonv1.Metadata{Name: "m"},
				Entity:    &databasev1.Entity{TagNames: []string{"a"}},
				IndexMode: true,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, countsActiveSeries(tt.measure))
		})
	}
}
