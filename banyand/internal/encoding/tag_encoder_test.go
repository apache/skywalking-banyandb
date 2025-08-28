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

package encoding

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pkgencoding "github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestEncodeDecodeTagValues_Int64_WithNilValues(t *testing.T) {
	tests := []struct {
		name   string
		values [][]byte
	}{
		{
			name:   "single nil value",
			values: [][]byte{nil},
		},
		{
			name:   "mixed nil and valid int64 values",
			values: [][]byte{convert.Int64ToBytes(42), nil, convert.Int64ToBytes(100)},
		},
		{
			name:   "all nil values",
			values: [][]byte{nil, nil, nil},
		},
		{
			name:   "nil at beginning",
			values: [][]byte{nil, convert.Int64ToBytes(1), convert.Int64ToBytes(2)},
		},
		{
			name:   "nil at end",
			values: [][]byte{convert.Int64ToBytes(1), convert.Int64ToBytes(2), nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bb := &bytes.Buffer{}
			err := EncodeTagValues(bb, tt.values, pbv1.ValueTypeInt64)
			require.NoError(t, err)
			require.NotNil(t, bb.Buf)

			decoder := &pkgencoding.BytesBlockDecoder{}
			decoded, err := DecodeTagValues(nil, decoder, bb, pbv1.ValueTypeInt64, len(tt.values))
			require.NoError(t, err)
			require.Len(t, decoded, len(tt.values))

			for i, original := range tt.values {
				if original == nil {
					assert.Nil(t, decoded[i], "nil value should be decoded as nil")
				} else {
					assert.Equal(t, original, decoded[i], "non-nil value should remain unchanged")
				}
			}
		})
	}
}

func TestEncodeDecodeTagValues_Int64_WithNullStringValues(t *testing.T) {
	tests := []struct {
		name   string
		values [][]byte
	}{
		{
			name:   "single null string value",
			values: [][]byte{[]byte("null")},
		},
		{
			name:   "mixed null string and valid int64 values",
			values: [][]byte{convert.Int64ToBytes(42), []byte("null"), convert.Int64ToBytes(100)},
		},
		{
			name:   "all null string values",
			values: [][]byte{[]byte("null"), []byte("null"), []byte("null")},
		},
		{
			name:   "null string at beginning",
			values: [][]byte{[]byte("null"), convert.Int64ToBytes(1), convert.Int64ToBytes(2)},
		},
		{
			name:   "null string at end",
			values: [][]byte{convert.Int64ToBytes(1), convert.Int64ToBytes(2), []byte("null")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bb := &bytes.Buffer{}
			err := EncodeTagValues(bb, tt.values, pbv1.ValueTypeInt64)
			require.NoError(t, err)
			require.NotNil(t, bb.Buf)

			decoder := &pkgencoding.BytesBlockDecoder{}
			decoded, err := DecodeTagValues(nil, decoder, bb, pbv1.ValueTypeInt64, len(tt.values))
			require.NoError(t, err)
			require.Len(t, decoded, len(tt.values))

			for i, original := range tt.values {
				if string(original) == "null" {
					assert.Equal(t, []byte("null"), decoded[i], "null string value should remain as 'null' string")
				} else {
					assert.Equal(t, original, decoded[i], "non-null value should remain unchanged")
				}
			}
		})
	}
}

func TestEncodeDecodeTagValues_Int64_MixedNilAndNullString(t *testing.T) {
	values := [][]byte{
		convert.Int64ToBytes(42),
		nil,
		[]byte("null"),
		convert.Int64ToBytes(100),
		nil,
		[]byte("null"),
	}

	bb := &bytes.Buffer{}
	err := EncodeTagValues(bb, values, pbv1.ValueTypeInt64)
	require.NoError(t, err)
	require.NotNil(t, bb.Buf)

	decoder := &pkgencoding.BytesBlockDecoder{}
	decoded, err := DecodeTagValues(nil, decoder, bb, pbv1.ValueTypeInt64, len(values))
	require.NoError(t, err)
	require.Len(t, decoded, len(values))

	expected := [][]byte{
		convert.Int64ToBytes(42),
		nil,
		[]byte("null"),
		convert.Int64ToBytes(100),
		nil,
		[]byte("null"),
	}

	for i, expectedValue := range expected {
		assert.Equal(t, expectedValue, decoded[i], "value at index %d should match expected", i)
	}
}

func TestEncodeDecodeTagValues_Int64_ValidValues(t *testing.T) {
	values := [][]byte{
		convert.Int64ToBytes(42),
		convert.Int64ToBytes(-100),
		convert.Int64ToBytes(0),
		convert.Int64ToBytes(9223372036854775807),  // max int64
		convert.Int64ToBytes(-9223372036854775808), // min int64
	}

	bb := &bytes.Buffer{}
	err := EncodeTagValues(bb, values, pbv1.ValueTypeInt64)
	require.NoError(t, err)
	require.NotNil(t, bb.Buf)

	decoder := &pkgencoding.BytesBlockDecoder{}
	decoded, err := DecodeTagValues(nil, decoder, bb, pbv1.ValueTypeInt64, len(values))
	require.NoError(t, err)
	require.Len(t, decoded, len(values))

	for i, original := range values {
		assert.Equal(t, original, decoded[i], "valid int64 value should remain unchanged")
	}
}

func TestEncodeDecodeTagValues_Int64_EmptyInput(t *testing.T) {
	bb := &bytes.Buffer{}
	err := EncodeTagValues(bb, nil, pbv1.ValueTypeInt64)
	require.NoError(t, err)
	assert.Nil(t, bb.Buf)

	decoder := &pkgencoding.BytesBlockDecoder{}
	decoded, err := DecodeTagValues(nil, decoder, bb, pbv1.ValueTypeInt64, 0)
	require.NoError(t, err)
	assert.Nil(t, decoded)
}
