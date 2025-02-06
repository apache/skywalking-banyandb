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

	"github.com/stretchr/testify/require"

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
	decodedTopNValue := GenerateTopNValue()
	defer ReleaseTopNValue(decodedTopNValue)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal the topNValue
			dst, err := tt.topNVal.marshal(nil)
			require.NoError(t, err)

			// Unmarshal the topNValue
			decodedTopNValue.Reset()
			err = decodedTopNValue.Unmarshal(dst, decoder)
			require.NoError(t, err)

			// Compare the original and decoded topNValue
			require.Equal(t, tt.topNVal.valueName, decodedTopNValue.valueName)
			require.Equal(t, tt.topNVal.entityTagNames, decodedTopNValue.entityTagNames)
			require.Equal(t, tt.topNVal.values, decodedTopNValue.values)
			require.Equal(t, tt.topNVal.entities, decodedTopNValue.entities)
		})
	}
}
