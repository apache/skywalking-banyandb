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

package measure

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func TestTopNValue_MarshalUnmarshal(t *testing.T) {
	tests := []struct {
		topNVal *TopNValue
		name    string
	}{
		{
			name: "simple case",
			topNVal: &TopNValue{
				valueName:      "testValue",
				entityTagNames: []string{"tag1", "tag2"},
				values:         []int64{1, 2, 3},
				entities: [][]*modelv1.TagValue{
					{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc1"}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity1"}}},
					},
					{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc1"}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity2"}}},
					},
					{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc1"}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity3"}}},
					},
				},
			},
		},
		{
			name: "single",
			topNVal: &TopNValue{
				valueName:      "testValue",
				entityTagNames: []string{"tag1", "tag2"},
				values:         []int64{1},
				entities: [][]*modelv1.TagValue{
					{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc1"}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity1"}}},
					},
				},
			},
		},
	}
	decoder := generateColumnValuesDecoder()
	defer releaseColumnValuesDecoder(decoder)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// clone the original value
			originalValueName := tt.topNVal.valueName
			originalEntityTagNames := make([]string, len(tt.topNVal.entityTagNames))
			copy(originalEntityTagNames, tt.topNVal.entityTagNames)
			originalValues := make([]int64, len(tt.topNVal.values))
			copy(originalValues, tt.topNVal.values)

			originalEntities := make([][]*modelv1.TagValue, len(tt.topNVal.entities))
			for i, entityGroup := range tt.topNVal.entities {
				originalEntities[i] = make([]*modelv1.TagValue, len(entityGroup))
				for j, tagValue := range entityGroup {
					originalEntities[i][j] = proto.Clone(tagValue).(*modelv1.TagValue)
				}
			}

			// Marshal the topNValue
			dst, err := tt.topNVal.Marshal(nil)
			require.NoError(t, err)

			// Unmarshal the topNValue
			tt.topNVal.Reset()
			err = tt.topNVal.Unmarshal(dst, decoder)
			require.NoError(t, err)

			// Compare the original and decoded topNValue
			require.Equal(t, originalValueName, tt.topNVal.valueName)
			require.Equal(t, originalEntityTagNames, tt.topNVal.entityTagNames)
			require.Equal(t, originalValues, tt.topNVal.values)
			diff := cmp.Diff(originalEntities, tt.topNVal.entities, protocmp.Transform())
			require.True(t,
				diff == "",
				"entities differ: %s",
				diff,
			)
		})
	}
}
