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
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBytesTermValue_Marshal_Unmarshal(t *testing.T) {
	tests := []struct {
		name     string
		value    []byte
		expected []byte
	}{
		{
			name:     "empty bytes",
			value:    []byte{},
			expected: []byte{},
		},
		{
			name:     "simple string",
			value:    []byte("hello world"),
			expected: []byte("hello world"),
		},
		{
			name:     "binary data",
			value:    []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD},
			expected: []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD},
		},
		{
			name:     "unicode string",
			value:    []byte("测试字符串"),
			expected: []byte("测试字符串"),
		},
		{
			name:     "large data",
			value:    bytes.Repeat([]byte{0x42}, 1000),
			expected: bytes.Repeat([]byte{0x42}, 1000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create original BytesTermValue
			original := &BytesTermValue{Value: tt.value}

			// Marshal
			marshaled, err := original.Marshal()
			require.NoError(t, err)
			assert.NotNil(t, marshaled)
			assert.Greater(t, len(marshaled), 0)

			// Verify type byte is correct (0 for bytes)
			assert.Equal(t, byte(0), marshaled[0])

			// Unmarshal
			unmarshaled := &BytesTermValue{}
			err = unmarshaled.Unmarshal(marshaled)
			require.NoError(t, err)

			// Verify the unmarshaled value matches the original
			assert.Equal(t, tt.expected, unmarshaled.Value)
			assert.Equal(t, original.Value, unmarshaled.Value)
		})
	}
}

func TestBytesTermValue_Unmarshal_InvalidData(t *testing.T) {
	tests := []struct {
		name        string
		expectedErr string
		data        []byte
	}{
		{
			name:        "empty data",
			data:        []byte{},
			expectedErr: "insufficient data for term value type",
		},
		{
			name:        "wrong type byte",
			data:        []byte{1}, // float type instead of bytes type
			expectedErr: "invalid term value type, expected bytes (0), got 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unmarshaled := &BytesTermValue{}
			err := unmarshaled.Unmarshal(tt.data)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestFloatTermValue_Marshal_Unmarshal(t *testing.T) {
	tests := []struct {
		name     string
		value    float64
		expected float64
	}{
		{
			name:     "zero",
			value:    0.0,
			expected: 0.0,
		},
		{
			name:     "positive integer",
			value:    42.0,
			expected: 42.0,
		},
		{
			name:     "negative integer",
			value:    -42.0,
			expected: -42.0,
		},
		{
			name:     "positive decimal",
			value:    3.14159,
			expected: 3.14159,
		},
		{
			name:     "negative decimal",
			value:    -2.71828,
			expected: -2.71828,
		},
		{
			name:     "large positive number",
			value:    1e10,
			expected: 1e10,
		},
		{
			name:     "large negative number",
			value:    -1e10,
			expected: -1e10,
		},
		{
			name:     "small positive number",
			value:    1e-10,
			expected: 1e-10,
		},
		{
			name:     "small negative number",
			value:    -1e-10,
			expected: -1e-10,
		},
		{
			name:     "max float64",
			value:    math.MaxFloat64,
			expected: math.MaxFloat64,
		},
		{
			name:     "min float64",
			value:    -math.MaxFloat64,
			expected: -math.MaxFloat64,
		},
		{
			name:     "infinity",
			value:    math.Inf(1),
			expected: math.Inf(1),
		},
		{
			name:     "negative infinity",
			value:    math.Inf(-1),
			expected: math.Inf(-1),
		},
		{
			name:     "NaN",
			value:    math.NaN(),
			expected: math.NaN(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create original FloatTermValue
			original := &FloatTermValue{Value: tt.value}

			// Marshal
			marshaled, err := original.Marshal()
			require.NoError(t, err)
			assert.NotNil(t, marshaled)
			assert.Greater(t, len(marshaled), 0)

			// Verify type byte is correct (1 for float)
			assert.Equal(t, byte(1), marshaled[0])

			// Unmarshal
			unmarshaled := &FloatTermValue{}
			err = unmarshaled.Unmarshal(marshaled)
			require.NoError(t, err)

			// Verify the unmarshaled value matches the original
			if math.IsNaN(tt.expected) {
				assert.True(t, math.IsNaN(unmarshaled.Value))
			} else {
				assert.Equal(t, tt.expected, unmarshaled.Value)
			}
			// For NaN, we can't use direct equality comparison
			if !math.IsNaN(original.Value) {
				assert.Equal(t, original.Value, unmarshaled.Value)
			}
		})
	}
}

func TestFloatTermValue_Unmarshal_InvalidData(t *testing.T) {
	tests := []struct {
		name        string
		expectedErr string
		data        []byte
	}{
		{
			name:        "empty data",
			data:        []byte{},
			expectedErr: "insufficient data for term value type",
		},
		{
			name:        "wrong type byte",
			data:        []byte{0}, // bytes type instead of float type
			expectedErr: "invalid term value type, expected float (1), got 0",
		},
		{
			name:        "incomplete data after type byte",
			data:        []byte{1},
			expectedErr: "invalid float value length: 0",
		},
		{
			name:        "wrong float value length",
			data:        []byte{1, 0, 0, 0, 0, 0, 0}, // 7 bytes instead of 8
			expectedErr: "invalid float value length: 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unmarshaled := &FloatTermValue{}
			err := unmarshaled.Unmarshal(tt.data)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}
