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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeAndDecodeRLE(t *testing.T) {
	arr := []uint32{1, 1, 2, 2, 3, 2, 2, 2, 1, 2, 1}
	encoded := make([]uint32, 0)
	encoded = encodeRLE(encoded, arr)
	require.Equal(t, []uint32{1, 2, 2, 2, 3, 1, 2, 3, 1, 1, 2, 1, 1, 1}, encoded)
	decoded := make([]uint32, 0)
	decoded = decodeRLE(decoded, encoded)
	require.Equal(t, arr, decoded)
}

func TestEncodeAndDecodeBitPacking(t *testing.T) {
	arr := []uint32{1, 2, 2, 2, 3, 1, 2, 3, 1, 1, 2, 1, 1, 1}
	encoded := encodeBitPacking(arr)
	decoded := make([]uint32, 0)
	decoded, err := decodeBitPacking(decoded, encoded)
	require.NoError(t, err)
	require.Equal(t, arr, decoded)
}

func TestEncodeAndDecodeDictionary(t *testing.T) {
	dict := NewDictionary()
	values := [][]byte{
		[]byte("skywalking"),
		[]byte("banyandb"),
		[]byte("hello"),
		[]byte("world"),
		[]byte("hello"),
	}
	for _, value := range values {
		dict.Add(value)
	}

	encoded := dict.Encode(nil)
	decoded := make([][]byte, 0)
	dict.Reset()
	decoded, err := dict.Decode(decoded, encoded, 5)
	require.NoError(t, err)

	expectedValues := [][]byte{[]byte("skywalking"), []byte("banyandb"), []byte("hello"), []byte("world")}
	require.Equal(t, expectedValues, dict.values)
	expectedIndices := []uint32{0, 1, 2, 3, 2}
	require.Equal(t, expectedIndices, dict.indices)
	require.Equal(t, values, decoded)
}

func TestEncodeAndDecodeDictionaryEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    [][]byte
		expected [][]byte
	}{
		{
			name:     "nil_slice",
			input:    [][]byte{nil},
			expected: [][]byte{nil},
		},
		{
			name:     "empty_slice",
			input:    [][]byte{{}},
			expected: [][]byte{{}},
		},
		{
			name:     "single_byte",
			input:    [][]byte{[]byte("a")},
			expected: [][]byte{[]byte("a")},
		},
		{
			name:     "mixed_nil_and_empty",
			input:    [][]byte{nil, {}},
			expected: [][]byte{nil, {}},
		},
		{
			name:     "mixed_empty_and_content",
			input:    [][]byte{{}, []byte("hello")},
			expected: [][]byte{{}, []byte("hello")},
		},
		{
			name:     "all_nil",
			input:    [][]byte{nil, nil, nil},
			expected: [][]byte{nil, nil, nil},
		},
		{
			name:     "all_empty",
			input:    [][]byte{{}, {}, {}},
			expected: [][]byte{{}, {}, {}},
		},
		{
			name:     "nil_empty_and_content",
			input:    [][]byte{nil, {}, []byte("test"), nil, []byte("value")},
			expected: [][]byte{nil, {}, []byte("test"), nil, []byte("value")},
		},
		{
			name:     "duplicate_empty_strings",
			input:    [][]byte{{}, []byte("a"), {}, []byte("a")},
			expected: [][]byte{{}, []byte("a"), {}, []byte("a")},
		},
		{
			name:     "duplicate_nil_values",
			input:    [][]byte{nil, []byte("b"), nil, []byte("b")},
			expected: [][]byte{nil, []byte("b"), nil, []byte("b")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dict := NewDictionary()
			for _, value := range tt.input {
				dict.Add(value)
			}

			encoded := dict.Encode(nil)
			require.NotNil(t, encoded)

			decodedDict := NewDictionary()
			decoded, err := decodedDict.Decode(nil, encoded, uint64(len(tt.input)))
			require.NoError(t, err)
			require.Len(t, decoded, len(tt.expected))

			for i, expected := range tt.expected {
				if expected == nil {
					assert.Nil(t, decoded[i], "index %d should be nil", i)
				} else {
					assert.NotNil(t, decoded[i], "index %d should not be nil", i)
					assert.Equal(t, expected, decoded[i], "index %d should match expected", i)
				}
			}
		})
	}
}

func TestEncodeAndDecodeDictionaryRoundTripConsistency(t *testing.T) {
	testCases := [][][]byte{
		// Basic cases
		{nil},
		{[]byte{}},
		{[]byte("a")},
		{[]byte("hello world")},

		// Mixed scenarios
		{nil, []byte{}, []byte("a")},
		{[]byte{}, nil, []byte("hello")},
		{nil, []byte{}, nil, []byte("test"), []byte{}},

		// Edge cases with different lengths
		{[]byte{}, []byte("x"), []byte("xx"), []byte("xxx")},
		{nil, nil, []byte{}, []byte{}, []byte("a"), []byte("ab")},

		// Large content
		{[]byte("this is a longer string for testing purposes")},
		{nil, []byte("mixed with nil and empty"), []byte{}, []byte("another string")},

		// Duplicate values (dictionary compression)
		{[]byte("hello"), []byte("world"), []byte("hello"), []byte("world")},
		{nil, []byte("test"), nil, []byte("test")},
		{[]byte{}, []byte("value"), []byte{}, []byte("value")},
	}

	for i, original := range testCases {
		t.Run(fmt.Sprintf("round_trip_%d", i), func(t *testing.T) {
			// Encode
			dict := NewDictionary()
			for _, value := range original {
				dict.Add(value)
			}
			encoded := dict.Encode(nil)
			require.NotNil(t, encoded)

			// Decode
			decodedDict := NewDictionary()
			decoded, err := decodedDict.Decode(nil, encoded, uint64(len(original)))
			require.NoError(t, err)
			require.Len(t, decoded, len(original))

			// Verify round-trip consistency
			for j, orig := range original {
				if orig == nil {
					assert.Nil(t, decoded[j], "position %d: nil should remain nil", j)
				} else {
					assert.NotNil(t, decoded[j], "position %d: non-nil should remain non-nil", j)
					assert.Equal(t, orig, decoded[j], "position %d: content should match", j)
				}
			}

			// Test that we can encode the decoded result again and get the same encoded bytes
			reEncodedDict := NewDictionary()
			for _, value := range decoded {
				reEncodedDict.Add(value)
			}
			reEncoded := reEncodedDict.Encode(nil)
			assert.Equal(t, encoded, reEncoded, "re-encoding decoded result should produce identical bytes")
		})
	}
}

func TestEncodeAndDecodeDictionaryNilEmptyStringDistinction(t *testing.T) {
	t.Run("nil vs empty string distinction", func(t *testing.T) {
		// Test that nil and empty string produce different encoded results
		nilDict := NewDictionary()
		nilDict.Add(nil)
		nilEncoded := nilDict.Encode(nil)

		emptyDict := NewDictionary()
		emptyDict.Add([]byte{})
		emptyEncoded := emptyDict.Encode(nil)

		// They should be different (nil uses 0, empty uses 1)
		assert.NotEqual(t, nilEncoded, emptyEncoded)

		// Decode and verify they decode to different results
		nilDecodedDict := NewDictionary()
		nilDecoded, err := nilDecodedDict.Decode(nil, nilEncoded, 1)
		require.NoError(t, err)
		assert.Len(t, nilDecoded, 1)
		assert.Nil(t, nilDecoded[0])

		emptyDecodedDict := NewDictionary()
		emptyDecoded, err := emptyDecodedDict.Decode(nil, emptyEncoded, 1)
		require.NoError(t, err)
		assert.Len(t, emptyDecoded, 1)
		assert.NotNil(t, emptyDecoded[0])
		assert.Equal(t, []byte{}, emptyDecoded[0])
	})

	t.Run("mixed nil and empty in same dictionary", func(t *testing.T) {
		values := [][]byte{nil, {}, []byte("test"), nil, {}}
		dict := NewDictionary()
		for _, value := range values {
			dict.Add(value)
		}

		encoded := dict.Encode(nil)
		require.NotNil(t, encoded)

		decodedDict := NewDictionary()
		decoded, err := decodedDict.Decode(nil, encoded, uint64(len(values)))
		require.NoError(t, err)
		require.Len(t, decoded, len(values))

		// Verify nil values remain nil
		assert.Nil(t, decoded[0], "first value should be nil")
		assert.Nil(t, decoded[3], "fourth value should be nil")

		// Verify empty strings remain non-nil empty
		assert.NotNil(t, decoded[1], "second value should not be nil")
		assert.Equal(t, []byte{}, decoded[1], "second value should be empty string")
		assert.NotNil(t, decoded[4], "fifth value should not be nil")
		assert.Equal(t, []byte{}, decoded[4], "fifth value should be empty string")

		// Verify content values
		assert.Equal(t, []byte("test"), decoded[2], "third value should be 'test'")
	})
}

type parameter struct {
	count       int
	cardinality int
}

var pList = [3]parameter{
	{count: 1000, cardinality: 100},
	{count: 10000, cardinality: 100},
	{count: 100000, cardinality: 100},
}

func BenchmarkEncodeDictionary(b *testing.B) {
	b.ReportAllocs()
	for _, p := range pList {
		b.Run(fmt.Sprintf("size=%d_cardinality=%d", p.count, p.cardinality), func(b *testing.B) {
			values, rawSize := generateData(p.count, p.cardinality)
			dict := NewDictionary()
			for _, value := range values {
				dict.Add(value)
			}

			encoded := make([]byte, 0)
			encoded = dict.Encode(encoded)
			compressedSize := len(encoded)
			compressRatio := float64(rawSize) / float64(compressedSize)

			b.ResetTimer()
			b.ReportMetric(compressRatio, "compress_ratio")
			for i := 0; i < b.N; i++ {
				dict.Encode(encoded[:0])
			}
		})
	}
}

func BenchmarkDecodeDictionary(b *testing.B) {
	b.ReportAllocs()
	for _, p := range pList {
		b.Run(fmt.Sprintf("size=%d_cardinality=%d", p.count, p.cardinality), func(b *testing.B) {
			values, _ := generateData(p.count, p.cardinality)
			dict := NewDictionary()
			for _, value := range values {
				dict.Add(value)
			}
			encoded := make([]byte, 0)
			encoded = dict.Encode(encoded)

			b.ResetTimer()
			decoded := NewDictionary()
			for i := 0; i < b.N; i++ {
				decoded.Reset()
				_, err := decoded.Decode(values[:0], encoded, uint64(p.count))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func generateData(count int, cardinality int) ([][]byte, int) {
	values := make([][]byte, 0)
	size := 0
	for i := 0; i < count; i++ {
		values = append(values, []byte(fmt.Sprintf("value_%d", i%cardinality)))
		size += len(values[i])
	}
	return values, size
}
