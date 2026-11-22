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

package index

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
)

func TestField_Marshal_Unmarshal(t *testing.T) {
	tests := []struct {
		name  string
		field Field
	}{
		{
			name: "BytesTermValue, all flags true",
			field: Field{
				Key: FieldKey{
					TimeRange: &RangeOpts{
						Upper:         &BytesTermValue{Value: []byte("upper")},
						Lower:         &BytesTermValue{Value: []byte("lower")},
						IncludesUpper: true,
						IncludesLower: false,
					},
					Analyzer:    "keyword",
					TagName:     "tag1",
					SeriesID:    common.SeriesID(12345),
					IndexRuleID: 42,
				},
				term:   &BytesTermValue{Value: []byte("test-value")},
				NoSort: true,
				Store:  true,
				Index:  true,
			},
		},
		{
			name: "FloatTermValue, all flags false",
			field: Field{
				Key: FieldKey{
					TimeRange: &RangeOpts{
						Upper:         &FloatTermValue{Value: 100.5},
						Lower:         &FloatTermValue{Value: -100.5},
						IncludesUpper: false,
						IncludesLower: true,
					},
					Analyzer:    "standard",
					TagName:     "tag2",
					SeriesID:    common.SeriesID(67890),
					IndexRuleID: 99,
				},
				term:   &FloatTermValue{Value: 3.14159},
				NoSort: false,
				Store:  false,
				Index:  false,
			},
		},
		{
			name: "Minimal field (no TimeRange, empty analyzer/tag)",
			field: Field{
				Key: FieldKey{
					TimeRange:   nil,
					Analyzer:    "",
					TagName:     "",
					SeriesID:    0,
					IndexRuleID: 0,
				},
				term:   &BytesTermValue{Value: []byte{}},
				NoSort: false,
				Store:  true,
				Index:  false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			marshaled, err := tt.field.Marshal()
			require.NoError(t, err)
			assert.NotNil(t, marshaled)
			assert.Greater(t, len(marshaled), 0)

			var unmarshaled Field
			err = unmarshaled.Unmarshal(marshaled)
			require.NoError(t, err)

			// Check FieldKey
			assert.Equal(t, tt.field.Key.Analyzer, unmarshaled.Key.Analyzer)
			assert.Equal(t, tt.field.Key.TagName, unmarshaled.Key.TagName)
			assert.Equal(t, tt.field.Key.SeriesID, unmarshaled.Key.SeriesID)
			assert.Equal(t, tt.field.Key.IndexRuleID, unmarshaled.Key.IndexRuleID)
			if tt.field.Key.TimeRange != nil {
				assert.NotNil(t, unmarshaled.Key.TimeRange)
				assert.Equal(t, tt.field.Key.TimeRange.IncludesUpper, unmarshaled.Key.TimeRange.IncludesUpper)
				assert.Equal(t, tt.field.Key.TimeRange.IncludesLower, unmarshaled.Key.TimeRange.IncludesLower)
				// Compare Upper
				switch expected := tt.field.Key.TimeRange.Upper.(type) {
				case *BytesTermValue:
					actual, ok := unmarshaled.Key.TimeRange.Upper.(*BytesTermValue)
					require.True(t, ok)
					assert.Equal(t, expected.Value, actual.Value)
				case *FloatTermValue:
					actual, ok := unmarshaled.Key.TimeRange.Upper.(*FloatTermValue)
					require.True(t, ok)
					assert.Equal(t, expected.Value, actual.Value)
				}
				// Compare Lower
				switch expected := tt.field.Key.TimeRange.Lower.(type) {
				case *BytesTermValue:
					actual, ok := unmarshaled.Key.TimeRange.Lower.(*BytesTermValue)
					require.True(t, ok)
					assert.Equal(t, expected.Value, actual.Value)
				case *FloatTermValue:
					actual, ok := unmarshaled.Key.TimeRange.Lower.(*FloatTermValue)
					require.True(t, ok)
					assert.Equal(t, expected.Value, actual.Value)
				}
			} else {
				assert.Nil(t, unmarshaled.Key.TimeRange)
			}

			// Check term value
			switch expected := tt.field.term.(type) {
			case *BytesTermValue:
				actual, ok := unmarshaled.term.(*BytesTermValue)
				require.True(t, ok)
				assert.Equal(t, expected.Value, actual.Value)
			case *FloatTermValue:
				actual, ok := unmarshaled.term.(*FloatTermValue)
				require.True(t, ok)
				assert.Equal(t, expected.Value, actual.Value)
			}

			// Check flags
			assert.Equal(t, tt.field.NoSort, unmarshaled.NoSort)
			assert.Equal(t, tt.field.Store, unmarshaled.Store)
			assert.Equal(t, tt.field.Index, unmarshaled.Index)
		})
	}
}

// The sort ladder of a field is built from its numeric key, so the key must survive the wire: the liaison builds the index
// documents of an index-mode measure and ships them to the data node.
func TestField_Marshal_Unmarshal_KeepsTheNumericKey(t *testing.T) {
	for _, key := range []int64{0, 1, -1, 42, -42, 1 << 40, -(1 << 40), math.MaxInt64, math.MinInt64} {
		field := NewBytesField(FieldKey{TagName: "ping", IndexRuleID: 7}, []byte("v"))
		field.Index, field.Store = true, true
		field.Numeric, field.NumericKey = true, key
		doc := Document{EntityValues: []byte("e"), Timestamp: 5, Version: 1, Fields: []Field{field}}
		data, err := Documents{doc}.Marshal()
		require.NoError(t, err)
		var decoded Documents
		require.NoError(t, decoded.Unmarshal(data))
		require.Len(t, decoded, 1)
		require.Len(t, decoded[0].Fields, 1)
		got := decoded[0].Fields[0]
		require.True(t, got.Numeric, "key %d", key)
		require.Equal(t, key, got.NumericKey)
		require.True(t, got.Index)
		require.True(t, got.Store)
		require.False(t, got.NoSort)
	}
	plain := NewBytesField(FieldKey{TagName: "name"}, []byte("v"))
	plain.Index = true
	data, err := Documents{{EntityValues: []byte("e"), Fields: []Field{plain, plain}}}.Marshal()
	require.NoError(t, err)
	var decoded Documents
	require.NoError(t, decoded.Unmarshal(data))
	require.Len(t, decoded[0].Fields, 2)
	for _, got := range decoded[0].Fields {
		require.False(t, got.Numeric)
		require.Zero(t, got.NumericKey)
	}
}
