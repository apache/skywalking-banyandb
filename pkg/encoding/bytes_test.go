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

package encoding_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/encoding"
)

func TestDecodeBytes(t *testing.T) {
	src := [][]byte{
		[]byte("Hello, "),
		[]byte("world!"),
	}
	encoded := make([]byte, 0)
	for _, d := range src {
		encoded = encoding.EncodeBytes(encoded, d)
	}
	var decoded []byte
	var err error
	for i := range src {
		encoded, decoded, err = encoding.DecodeBytes(encoded)
		require.Nil(t, err)
		assert.Equal(t, src[i], decoded)
	}
}

func TestEncodeBlockAndDecode(t *testing.T) {
	slices := [][]byte{
		[]byte("Hello, "),
		[]byte("world!"),
		[]byte("Testing bytes"),
	}

	encoded := encoding.EncodeBytesBlock(nil, slices)
	require.NotNil(t, encoded)
	blockDecoder := &encoding.BytesBlockDecoder{}
	decoded, err := blockDecoder.Decode(nil, encoded, uint64(len(slices)))
	require.Nil(t, err)
	for i, slice := range slices {
		assert.Equal(t, slice, decoded[i])
	}
}

func TestEncodeBytesBlockEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    [][]byte
		expected [][]byte
	}{
		{
			name:     "nil slice",
			input:    [][]byte{nil},
			expected: [][]byte{nil},
		},
		{
			name:     "empty slice",
			input:    [][]byte{{}},
			expected: [][]byte{{}},
		},
		{
			name:     "single byte",
			input:    [][]byte{[]byte("a")},
			expected: [][]byte{[]byte("a")},
		},
		{
			name:     "mixed nil and empty",
			input:    [][]byte{nil, {}, nil},
			expected: [][]byte{nil, {}, nil},
		},
		{
			name:     "mixed empty and content",
			input:    [][]byte{{}, []byte("hello"), {}},
			expected: [][]byte{{}, []byte("hello"), {}},
		},
		{
			name:     "all nil",
			input:    [][]byte{nil, nil, nil},
			expected: [][]byte{nil, nil, nil},
		},
		{
			name:     "all empty",
			input:    [][]byte{{}, {}, {}},
			expected: [][]byte{{}, {}, {}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encoding.EncodeBytesBlock(nil, tt.input)
			require.NotNil(t, encoded)

			blockDecoder := &encoding.BytesBlockDecoder{}
			decoded, err := blockDecoder.Decode(nil, encoded, uint64(len(tt.input)))
			require.Nil(t, err)
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

func TestEncodeBytesBlockRoundTripConsistency(t *testing.T) {
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
		{[]byte(""), []byte("x"), []byte("xx"), []byte("xxx")},
		{nil, nil, []byte{}, []byte{}, []byte("a"), []byte("ab")},

		// Large content
		{[]byte("this is a longer string for testing purposes")},
		{nil, []byte("mixed with nil and empty"), []byte{}, []byte("another string")},
	}

	for i, original := range testCases {
		t.Run(fmt.Sprintf("round_trip_%d", i), func(t *testing.T) {
			// Encode
			encoded := encoding.EncodeBytesBlock(nil, original)
			require.NotNil(t, encoded)

			// Decode
			blockDecoder := &encoding.BytesBlockDecoder{}
			decoded, err := blockDecoder.Decode(nil, encoded, uint64(len(original)))
			require.Nil(t, err)
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
			reEncoded := encoding.EncodeBytesBlock(nil, decoded)
			assert.Equal(t, encoded, reEncoded, "re-encoding decoded result should produce identical bytes")
		})
	}
}

func TestEncodeBytesBlockNilEmptyStringDistinction(t *testing.T) {
	t.Run("nil vs empty string distinction", func(t *testing.T) {
		// Test that nil and empty string produce different encoded results
		nilSlice := [][]byte{nil}
		emptySlice := [][]byte{{}}

		nilEncoded := encoding.EncodeBytesBlock(nil, nilSlice)
		emptyEncoded := encoding.EncodeBytesBlock(nil, emptySlice)

		// They should be different (nil uses 0, empty uses 1)
		assert.NotEqual(t, nilEncoded, emptyEncoded)

		// Decode and verify they decode to different results
		blockDecoder := &encoding.BytesBlockDecoder{}

		nilDecoded, err := blockDecoder.Decode(nil, nilEncoded, 1)
		require.Nil(t, err)
		assert.Len(t, nilDecoded, 1)
		assert.Nil(t, nilDecoded[0])

		blockDecoder.Reset()
		emptyDecoded, err := blockDecoder.Decode(nil, emptyEncoded, 1)
		require.Nil(t, err)
		assert.Len(t, emptyDecoded, 1)
		assert.NotNil(t, emptyDecoded[0])
		assert.Equal(t, []byte{}, emptyDecoded[0])
	})
}
