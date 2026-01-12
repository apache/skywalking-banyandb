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
	"encoding/base64"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
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

func TestTopNValue_Marshal_EmptyValues(t *testing.T) {
	topNVal := &TopNValue{
		valueName:      "testValue",
		entityTagNames: []string{"tag1"},
		values:         []int64{},
		entities:       [][]*modelv1.TagValue{},
	}
	_, err := topNVal.marshal(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "values is empty")
}

func TestTopNValue_Unmarshal_InvalidData(t *testing.T) {
	decoder := generateColumnValuesDecoder()
	defer releaseColumnValuesDecoder(decoder)

	tests := []struct {
		name    string
		wantErr string
		src     []byte
	}{
		{
			name:    "empty src",
			src:     []byte{},
			wantErr: "cannot unmarshal topNValue",
		},
		{
			name:    "truncated after name",
			src:     []byte{0x01, 0x01, 'a'},
			wantErr: "cannot unmarshal topNValue",
		},
		{
			name:    "truncated after encodeType",
			src:     []byte{0x01, 0x01, 'a', 0x00, 0x00},
			wantErr: "cannot unmarshal topNValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topNVal := &TopNValue{}
			err := topNVal.Unmarshal(tt.src, decoder)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestTopNValue_SetMetadata(t *testing.T) {
	topNVal := &TopNValue{}
	topNVal.setMetadata("testValue", []string{"tag1", "tag2"})
	require.Equal(t, "testValue", topNVal.valueName)
	require.Equal(t, []string{"tag1", "tag2"}, topNVal.entityTagNames)
}

func TestTopNValue_AddValue(t *testing.T) {
	topNVal := &TopNValue{}
	entityValues := []*modelv1.TagValue{
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc1"}}},
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity1"}}},
	}
	topNVal.addValue(100, entityValues)
	require.Equal(t, []int64{100}, topNVal.values)
	require.Len(t, topNVal.entities, 1)
	require.Len(t, topNVal.entities[0], 2)
	require.Equal(t, entityValues, topNVal.entities[0], "entityValues should be copied and equal")
	require.NotSame(t, &entityValues[0], &topNVal.entities[0][0], "entityValues should be a copy, not the same slice")
}

func TestTopNValue_Values(t *testing.T) {
	topNVal := &TopNValue{
		valueName:      "testValue",
		entityTagNames: []string{"tag1", "tag2"},
		values:         []int64{1, 2, 3},
		entities: [][]*modelv1.TagValue{
			{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc1"}}},
			},
		},
	}
	valueName, entityTagNames, values, entities := topNVal.Values()
	require.Equal(t, "testValue", valueName)
	require.Equal(t, []string{"tag1", "tag2"}, entityTagNames)
	require.Equal(t, []int64{1, 2, 3}, values)
	require.Equal(t, topNVal.entities, entities)
}

func TestTopNValue_Reset(t *testing.T) {
	topNVal := &TopNValue{
		valueName:      "testValue",
		entityTagNames: []string{"tag1", "tag2"},
		values:         []int64{1, 2, 3},
		entities: [][]*modelv1.TagValue{
			{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc1"}}},
			},
		},
		buf:             []byte{1, 2, 3},
		entityValues:    [][]byte{{1, 2}},
		entityValuesBuf: [][]byte{{3, 4}},
	}
	topNVal.Reset()
	require.Empty(t, topNVal.valueName)
	require.Empty(t, topNVal.entityTagNames)
	require.Empty(t, topNVal.values)
	require.Empty(t, topNVal.entities)
	require.Empty(t, topNVal.buf)
	require.Empty(t, topNVal.entityValues)
	require.Empty(t, topNVal.entityValuesBuf)
	require.Equal(t, int64(0), topNVal.firstValue)
}

func TestTopNValue_ResizeEntityValues(t *testing.T) {
	topNVal := &TopNValue{}
	result := topNVal.resizeEntityValues(5)
	require.Len(t, result, 5)
	require.Len(t, topNVal.entityValues, 5)
	result2 := topNVal.resizeEntityValues(3)
	require.Len(t, result2, 3)
	require.Len(t, topNVal.entityValues, 3)
	result3 := topNVal.resizeEntityValues(10)
	require.Len(t, result3, 10)
	require.Len(t, topNVal.entityValues, 10)
}

func TestTopNValue_ResizeEntities(t *testing.T) {
	topNVal := &TopNValue{}
	result := topNVal.resizeEntities(3, 2)
	require.Len(t, result, 3)
	require.Len(t, topNVal.entities, 3)
	for i := range result {
		require.Len(t, result[i], 0, "entities should be reset to length 0")
		require.Equal(t, 2, cap(result[i]), "entities should have capacity 2")
	}
}

func TestDataPointWithEntityValues_IntFieldValue(t *testing.T) {
	tests := []struct {
		fieldIndex    map[string]int
		name          string
		fieldName     string
		fields        []*modelv1.FieldValue
		expectedValue int64
	}{
		{
			name:          "field exists",
			fieldIndex:    map[string]int{"field1": 0},
			fields:        []*modelv1.FieldValue{{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 100}}}},
			fieldName:     "field1",
			expectedValue: 100,
		},
		{
			name:          "field not in index",
			fieldIndex:    map[string]int{"field1": 0},
			fields:        []*modelv1.FieldValue{{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 100}}}},
			fieldName:     "field2",
			expectedValue: 0,
		},
		{
			name:          "field index out of range",
			fieldIndex:    map[string]int{"field1": 5},
			fields:        []*modelv1.FieldValue{{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 100}}}},
			fieldName:     "field1",
			expectedValue: 0,
		},
		{
			name:          "field is not int type",
			fieldIndex:    map[string]int{"field1": 0},
			fields:        []*modelv1.FieldValue{{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 3.14}}}},
			fieldName:     "field1",
			expectedValue: 0,
		},
		{
			name:          "nil fieldIndex",
			fieldIndex:    nil,
			fields:        []*modelv1.FieldValue{{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 100}}}},
			fieldName:     "field1",
			expectedValue: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dp := &dataPointWithEntityValues{
				DataPointValue: &measurev1.DataPointValue{
					Fields: tt.fields,
				},
				fieldIndex: tt.fieldIndex,
			}
			result := dp.intFieldValue(tt.fieldName, nil)
			require.Equal(t, tt.expectedValue, result)
		})
	}
}

func TestDataPointWithEntityValues_TagValue(t *testing.T) {
	tests := []struct {
		tagSpec      logical.TagSpecRegistry
		name         string
		tagName      string
		tagFamilies  []*modelv1.TagFamilyForWrite
		expectedNull bool
	}{
		{
			name: "tag found",
			tagSpec: func() logical.TagSpecRegistry {
				tagSpecMap := logical.TagSpecMap{}
				tagSpecMap.RegisterTag(0, 0, &databasev1.TagSpec{Name: "tag1"})
				return tagSpecMap
			}(),
			tagFamilies: []*modelv1.TagFamilyForWrite{
				{
					Tags: []*modelv1.TagValue{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "value1"}}},
					},
				},
			},
			tagName:      "tag1",
			expectedNull: false,
		},
		{
			name:         "tag not found in spec",
			tagSpec:      logical.TagSpecMap{},
			tagFamilies:  []*modelv1.TagFamilyForWrite{},
			tagName:      "tag1",
			expectedNull: true,
		},
		{
			name: "tag family index out of range",
			tagSpec: func() logical.TagSpecRegistry {
				tagSpecMap := logical.TagSpecMap{}
				tagSpecMap.RegisterTag(5, 0, &databasev1.TagSpec{Name: "tag1"})
				return tagSpecMap
			}(),
			tagFamilies: []*modelv1.TagFamilyForWrite{
				{
					Tags: []*modelv1.TagValue{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "value1"}}},
					},
				},
			},
			tagName:      "tag1",
			expectedNull: true,
		},
		{
			name: "tag index out of range",
			tagSpec: func() logical.TagSpecRegistry {
				tagSpecMap := logical.TagSpecMap{}
				tagSpecMap.RegisterTag(0, 5, &databasev1.TagSpec{Name: "tag1"})
				return tagSpecMap
			}(),
			tagFamilies: []*modelv1.TagFamilyForWrite{
				{
					Tags: []*modelv1.TagValue{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "value1"}}},
					},
				},
			},
			tagName:      "tag1",
			expectedNull: true,
		},
		{
			name:         "nil tagSpec",
			tagSpec:      nil,
			tagFamilies:  []*modelv1.TagFamilyForWrite{},
			tagName:      "tag1",
			expectedNull: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dp := &dataPointWithEntityValues{
				DataPointValue: &measurev1.DataPointValue{
					TagFamilies: tt.tagFamilies,
				},
				tagSpec: tt.tagSpec,
			}
			result := dp.tagValue(tt.tagName)
			if tt.expectedNull {
				require.Equal(t, pbv1.NullTagValue, result)
			} else {
				require.NotEqual(t, pbv1.NullTagValue, result)
			}
		})
	}
}

func TestDataPointWithEntityValues_LocateSpecTag(t *testing.T) {
	tests := []struct {
		name       string
		tagSpec    logical.TagSpecRegistry
		tagName    string
		wantFamily int
		wantTag    int
		wantOk     bool
	}{
		{
			name: "tag found",
			tagSpec: func() logical.TagSpecRegistry {
				tagSpecMap := logical.TagSpecMap{}
				tagSpecMap.RegisterTag(1, 2, &databasev1.TagSpec{Name: "tag1"})
				return tagSpecMap
			}(),
			tagName:    "tag1",
			wantFamily: 1,
			wantTag:    2,
			wantOk:     true,
		},
		{
			name:       "tag not found",
			tagSpec:    logical.TagSpecMap{},
			tagName:    "tag1",
			wantFamily: 0,
			wantTag:    0,
			wantOk:     false,
		},
		{
			name:       "nil tagSpec",
			tagSpec:    nil,
			tagName:    "tag1",
			wantFamily: 0,
			wantTag:    0,
			wantOk:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dp := &dataPointWithEntityValues{
				tagSpec: tt.tagSpec,
			}
			familyIdx, tagIdx, ok := dp.locateSpecTag(tt.tagName)
			require.Equal(t, tt.wantOk, ok)
			if ok {
				require.Equal(t, tt.wantFamily, familyIdx)
				require.Equal(t, tt.wantTag, tagIdx)
			}
		})
	}
}

func TestStringify(t *testing.T) {
	tests := []struct {
		name     string
		tagValue *modelv1.TagValue
		expected string
	}{
		{
			name:     "string value",
			tagValue: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "test"}}},
			expected: "test",
		},
		{
			name:     "int value",
			tagValue: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 12345}}},
			expected: "12345",
		},
		{
			name:     "binary data",
			tagValue: &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte("test data")}},
			expected: base64.StdEncoding.EncodeToString([]byte("test data")),
		},
		{
			name:     "int array",
			tagValue: &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{1, 2, 3}}}},
			expected: "1,2,3",
		},
		{
			name:     "string array",
			tagValue: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"a", "b", "c"}}}},
			expected: "a,b,c",
		},
		{
			name:     "empty string array",
			tagValue: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{}}}},
			expected: "",
		},
		{
			name:     "unknown type",
			tagValue: &modelv1.TagValue{Value: &modelv1.TagValue_Null{}},
			expected: "",
		},
		{
			name:     "nil value",
			tagValue: &modelv1.TagValue{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Stringify(tt.tagValue)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestGroupName(t *testing.T) {
	tests := []struct {
		name      string
		expected  string
		groupTags []string
	}{
		{
			name:      "single tag",
			groupTags: []string{"tag1"},
			expected:  "tag1",
		},
		{
			name:      "multiple tags",
			groupTags: []string{"tag1", "tag2", "tag3"},
			expected:  "tag1|tag2|tag3",
		},
		{
			name:      "empty tags",
			groupTags: []string{},
			expected:  "",
		},
		{
			name:      "nil tags",
			groupTags: nil,
			expected:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GroupName(tt.groupTags)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildFieldIndex(t *testing.T) {
	tests := []struct {
		spec     *measurev1.DataPointSpec
		expected map[string]int
		name     string
	}{
		{
			name: "multiple fields",
			spec: &measurev1.DataPointSpec{
				FieldNames: []string{"field1", "field2", "field3"},
			},
			expected: map[string]int{
				"field1": 0,
				"field2": 1,
				"field3": 2,
			},
		},
		{
			name:     "empty fields",
			spec:     &measurev1.DataPointSpec{FieldNames: []string{}},
			expected: map[string]int{},
		},
		{
			name:     "nil spec",
			spec:     nil,
			expected: map[string]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildFieldIndex(tt.spec, nil)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildTagSpecRegistryFromSpec(t *testing.T) {
	tests := []struct {
		name    string
		spec    *measurev1.DataPointSpec
		tagName string
		wantOk  bool
		wantFam int
		wantTag int
	}{
		{
			name: "single tag family",
			spec: &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{
					{
						TagNames: []string{"tag1", "tag2"},
					},
				},
			},
			tagName: "tag1",
			wantOk:  true,
			wantFam: 0,
			wantTag: 0,
		},
		{
			name: "multiple tag families",
			spec: &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{
					{
						TagNames: []string{"tag1"},
					},
					{
						TagNames: []string{"tag2", "tag3"},
					},
				},
			},
			tagName: "tag3",
			wantOk:  true,
			wantFam: 1,
			wantTag: 1,
		},
		{
			name:    "nil spec",
			spec:    nil,
			tagName: "tag1",
			wantOk:  false,
			wantFam: 0,
			wantTag: 0,
		},
		{
			name: "tag not found",
			spec: &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{
					{
						TagNames: []string{"tag1"},
					},
				},
			},
			tagName: "tag2",
			wantOk:  false,
			wantFam: 0,
			wantTag: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildTagSpecRegistryFromSpec(tt.spec, nil)
			if tt.spec == nil {
				require.NotNil(t, result)
				return
			}
			tagSpec := result.FindTagSpecByName(tt.tagName)
			if tt.wantOk {
				require.NotNil(t, tagSpec)
				require.Equal(t, tt.wantFam, tagSpec.TagFamilyIdx)
				require.Equal(t, tt.wantTag, tagSpec.TagIdx)
			} else {
				require.Nil(t, tagSpec)
			}
		})
	}
}

func TestTopNValue_MarshalUnmarshal_EdgeCases(t *testing.T) {
	decoder := generateColumnValuesDecoder()
	defer releaseColumnValuesDecoder(decoder)

	tests := []struct {
		topNVal *TopNValue
		name    string
	}{
		{
			name: "empty entityTagNames",
			topNVal: &TopNValue{
				valueName:      "testValue",
				entityTagNames: []string{},
				values:         []int64{1, 2, 3},
				entities: [][]*modelv1.TagValue{
					{},
					{},
					{},
				},
			},
		},
		{
			name: "large values",
			topNVal: &TopNValue{
				valueName:      "testValue",
				entityTagNames: []string{"tag1"},
				values:         []int64{-9223372036854775808, 9223372036854775807, 0},
				entities: [][]*modelv1.TagValue{
					{{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 1}}}},
					{{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 2}}}},
					{{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 3}}}},
				},
			},
		},
		{
			name: "many entity tag names",
			topNVal: &TopNValue{
				valueName:      "testValue",
				entityTagNames: []string{"tag1", "tag2", "tag3", "tag4", "tag5"},
				values:         []int64{1},
				entities: [][]*modelv1.TagValue{
					{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v1"}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v3"}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v4"}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v5"}}},
					},
				},
			},
		},
		{
			name: "different tag value types",
			topNVal: &TopNValue{
				valueName:      "testValue",
				entityTagNames: []string{"tag1", "tag2", "tag3"},
				values:         []int64{1, 2},
				entities: [][]*modelv1.TagValue{
					{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "str"}}},
						{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 42}}},
						{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte("binary")}},
					},
					{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "str2"}}},
						{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 43}}},
						{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte("binary2")}},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			dst, err := tt.topNVal.marshal(nil)
			require.NoError(t, err)

			tt.topNVal.Reset()
			err = tt.topNVal.Unmarshal(dst, decoder)
			require.NoError(t, err)

			require.Equal(t, originalValueName, tt.topNVal.valueName)
			require.Equal(t, originalEntityTagNames, tt.topNVal.entityTagNames)
			require.Equal(t, originalValues, tt.topNVal.values)
			diff := cmp.Diff(originalEntities, tt.topNVal.entities, protocmp.Transform())
			require.True(t, diff == "", "entities differ: %s", diff)
		})
	}
}

func TestTopNValue_Unmarshal_InvalidEntityLength(t *testing.T) {
	decoder := generateColumnValuesDecoder()
	defer releaseColumnValuesDecoder(decoder)

	topNVal := &TopNValue{
		valueName:      "testValue",
		entityTagNames: []string{"tag1", "tag2"},
		values:         []int64{1},
		entities: [][]*modelv1.TagValue{
			{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc1"}}},
			},
		},
	}

	dst, err := topNVal.marshal(nil)
	require.NoError(t, err)

	topNVal.Reset()
	err = topNVal.Unmarshal(dst, decoder)
	require.Error(t, err)
	require.Contains(t, err.Error(), "entityValues")
}

func TestNewDataPointWithEntityValues(t *testing.T) {
	dp := &measurev1.DataPointValue{
		Timestamp: timestamppb.Now(),
		Fields: []*modelv1.FieldValue{
			{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 100}}},
		},
		TagFamilies: []*modelv1.TagFamilyForWrite{
			{
				Tags: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "tag1"}}},
				},
			},
		},
	}
	entityValues := []*modelv1.TagValue{
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity1"}}},
	}
	spec := &measurev1.DataPointSpec{
		FieldNames: []string{"field1"},
		TagFamilySpec: []*measurev1.TagFamilySpec{
			{
				TagNames: []string{"tag1"},
			},
		},
	}

	result := newDataPointWithEntityValues(dp, entityValues, 123, 456, spec, nil)
	require.NotNil(t, result)
	require.Equal(t, dp, result.DataPointValue)
	require.Equal(t, entityValues, result.entityValues)
	require.Equal(t, uint64(123), result.seriesID)
	require.Equal(t, uint32(456), result.shardID)
	require.NotNil(t, result.fieldIndex)
	require.Equal(t, 0, result.fieldIndex["field1"])
	require.NotNil(t, result.tagSpec)
}
