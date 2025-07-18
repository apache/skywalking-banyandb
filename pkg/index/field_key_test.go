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
)

func TestFieldKey_MarshalAll_UnmarshalAll(t *testing.T) {
	tests := []struct {
		name     string
		original FieldKey
	}{
		{
			name: "complete field key with time range",
			original: FieldKey{
				TimeRange: &RangeOpts{
					Upper:         &BytesTermValue{Value: []byte("2024-01-01")},
					Lower:         &BytesTermValue{Value: []byte("2023-01-01")},
					IncludesUpper: true,
					IncludesLower: false,
				},
				Analyzer:    "standard",
				TagName:     "service_name",
				SeriesID:    123456789,
				IndexRuleID: 42,
			},
		},
		{
			name: "field key with float time range",
			original: FieldKey{
				TimeRange: &RangeOpts{
					Upper:         &FloatTermValue{Value: 1704067200.0}, // 2024-01-01 timestamp
					Lower:         &FloatTermValue{Value: 1672531200.0}, // 2023-01-01 timestamp
					IncludesUpper: false,
					IncludesLower: true,
				},
				Analyzer:    "keyword",
				TagName:     "instance_id",
				SeriesID:    987654321,
				IndexRuleID: 100,
			},
		},
		{
			name: "field key without time range",
			original: FieldKey{
				TimeRange:   nil,
				Analyzer:    "simple",
				TagName:     "endpoint",
				SeriesID:    555666777,
				IndexRuleID: 200,
			},
		},
		{
			name: "field key with empty analyzer and tag name",
			original: FieldKey{
				TimeRange:   nil,
				Analyzer:    "",
				TagName:     "",
				SeriesID:    111222333,
				IndexRuleID: 300,
			},
		},
		{
			name: "field key with unicode tag name",
			original: FieldKey{
				TimeRange: &RangeOpts{
					Upper:         &BytesTermValue{Value: []byte("测试上限")},
					Lower:         &BytesTermValue{Value: []byte("测试下限")},
					IncludesUpper: true,
					IncludesLower: true,
				},
				Analyzer:    "url",
				TagName:     "测试标签",
				SeriesID:    444555666,
				IndexRuleID: 400,
			},
		},
		{
			name: "field key with maximum values",
			original: FieldKey{
				TimeRange: &RangeOpts{
					Upper:         &FloatTermValue{Value: math.MaxFloat64},
					Lower:         &FloatTermValue{Value: -math.MaxFloat64},
					IncludesUpper: true,
					IncludesLower: true,
				},
				Analyzer:    "standard",
				TagName:     "max_tag",
				SeriesID:    math.MaxUint64,
				IndexRuleID: math.MaxUint32,
			},
		},
		{
			name: "field key with zero values",
			original: FieldKey{
				TimeRange:   nil,
				Analyzer:    "",
				TagName:     "",
				SeriesID:    0,
				IndexRuleID: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			marshaled, err := tt.original.MarshalAll()
			require.NoError(t, err)
			assert.NotNil(t, marshaled)
			assert.Greater(t, len(marshaled), 0)

			// Unmarshal
			unmarshaled := &FieldKey{}
			err = unmarshaled.UnmarshalAll(marshaled)
			require.NoError(t, err)

			// Verify TimeRange
			if tt.original.TimeRange != nil {
				assert.NotNil(t, unmarshaled.TimeRange)
				assert.Equal(t, tt.original.TimeRange.IncludesUpper, unmarshaled.TimeRange.IncludesUpper)
				assert.Equal(t, tt.original.TimeRange.IncludesLower, unmarshaled.TimeRange.IncludesLower)

				// Verify Upper bound
				if tt.original.TimeRange.Upper != nil {
					assert.NotNil(t, unmarshaled.TimeRange.Upper)
					switch expected := tt.original.TimeRange.Upper.(type) {
					case *BytesTermValue:
						actual, ok := unmarshaled.TimeRange.Upper.(*BytesTermValue)
						require.True(t, ok)
						assert.Equal(t, expected.Value, actual.Value)
					case *FloatTermValue:
						actual, ok := unmarshaled.TimeRange.Upper.(*FloatTermValue)
						require.True(t, ok)
						assert.Equal(t, expected.Value, actual.Value)
					}
				} else {
					assert.Nil(t, unmarshaled.TimeRange.Upper)
				}

				// Verify Lower bound
				if tt.original.TimeRange.Lower != nil {
					assert.NotNil(t, unmarshaled.TimeRange.Lower)
					switch expected := tt.original.TimeRange.Lower.(type) {
					case *BytesTermValue:
						actual, ok := unmarshaled.TimeRange.Lower.(*BytesTermValue)
						require.True(t, ok)
						assert.Equal(t, expected.Value, actual.Value)
					case *FloatTermValue:
						actual, ok := unmarshaled.TimeRange.Lower.(*FloatTermValue)
						require.True(t, ok)
						assert.Equal(t, expected.Value, actual.Value)
					}
				} else {
					assert.Nil(t, unmarshaled.TimeRange.Lower)
				}
			} else {
				assert.Nil(t, unmarshaled.TimeRange)
			}

			// Verify other fields
			assert.Equal(t, tt.original.Analyzer, unmarshaled.Analyzer)
			assert.Equal(t, tt.original.TagName, unmarshaled.TagName)
			assert.Equal(t, tt.original.SeriesID, unmarshaled.SeriesID)
			assert.Equal(t, tt.original.IndexRuleID, unmarshaled.IndexRuleID)
		})
	}
}

func TestFieldKey_UnmarshalAll_InvalidData(t *testing.T) {
	tests := []struct {
		name        string
		expectedErr string
		data        []byte
	}{
		{
			name:        "empty data",
			data:        []byte{},
			expectedErr: "empty data for field key",
		},
		{
			name:        "insufficient data for time range flag",
			data:        []byte{},
			expectedErr: "empty data for field key",
		},
		{
			name:        "insufficient data for series ID",
			data:        []byte{0, 0, 0, 0, 0, 0, 0}, // only 7 bytes for series ID
			expectedErr: "insufficient data for series ID",
		},
		{
			name:        "insufficient data for IndexRuleID",
			data:        []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // only 8 bytes, missing IndexRuleID
			expectedErr: "insufficient data for IndexRuleID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unmarshaled := &FieldKey{}
			err := unmarshaled.UnmarshalAll(tt.data)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestFieldKey_MarshalAll_UnmarshalAll_RoundTrip(t *testing.T) {
	// Test round-trip marshaling/unmarshaling with a complex field key
	original := FieldKey{
		TimeRange: &RangeOpts{
			Upper:         &BytesTermValue{Value: []byte("2024-12-31T23:59:59Z")},
			Lower:         &FloatTermValue{Value: 1672531200.0},
			IncludesUpper: true,
			IncludesLower: false,
		},
		Analyzer:    "standard",
		TagName:     "service.instance.name",
		SeriesID:    1234567890123456789,
		IndexRuleID: 999,
	}

	// First round-trip
	marshaled1, err := original.MarshalAll()
	require.NoError(t, err)

	unmarshaled1 := &FieldKey{}
	err = unmarshaled1.UnmarshalAll(marshaled1)
	require.NoError(t, err)

	// Second round-trip
	marshaled2, err := unmarshaled1.MarshalAll()
	require.NoError(t, err)

	unmarshaled2 := &FieldKey{}
	err = unmarshaled2.UnmarshalAll(marshaled2)
	require.NoError(t, err)

	// Verify all fields are preserved through multiple round-trips
	assert.Equal(t, original.Analyzer, unmarshaled2.Analyzer)
	assert.Equal(t, original.TagName, unmarshaled2.TagName)
	assert.Equal(t, original.SeriesID, unmarshaled2.SeriesID)
	assert.Equal(t, original.IndexRuleID, unmarshaled2.IndexRuleID)

	// Verify TimeRange is preserved
	assert.NotNil(t, unmarshaled2.TimeRange)
	assert.Equal(t, original.TimeRange.IncludesUpper, unmarshaled2.TimeRange.IncludesUpper)
	assert.Equal(t, original.TimeRange.IncludesLower, unmarshaled2.TimeRange.IncludesLower)

	// Verify Upper bound
	originalUpper, ok := original.TimeRange.Upper.(*BytesTermValue)
	require.True(t, ok)
	unmarshaledUpper, ok := unmarshaled2.TimeRange.Upper.(*BytesTermValue)
	require.True(t, ok)
	assert.Equal(t, originalUpper.Value, unmarshaledUpper.Value)

	// Verify Lower bound
	originalLower, ok := original.TimeRange.Lower.(*FloatTermValue)
	require.True(t, ok)
	unmarshaledLower, ok := unmarshaled2.TimeRange.Lower.(*FloatTermValue)
	require.True(t, ok)
	assert.Equal(t, originalLower.Value, unmarshaledLower.Value)
}
