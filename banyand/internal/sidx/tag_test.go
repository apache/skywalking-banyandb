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

package sidx

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

const testTagName = "test_tag"

func TestTagValueMarshaling(t *testing.T) {
	tests := []struct {
		name   string
		values [][]byte
		want   bool // whether marshaling should succeed
	}{
		{
			name:   "empty values",
			values: [][]byte{},
			want:   true,
		},
		{
			name:   "single value",
			values: [][]byte{[]byte("test")},
			want:   true,
		},
		{
			name: "multiple values",
			values: [][]byte{
				[]byte("value1"),
				[]byte("value2"),
				[]byte("value3"),
			},
			want: true,
		},
		{
			name: "values with different lengths",
			values: [][]byte{
				[]byte("a"),
				[]byte("longer_value"),
				[]byte(""),
				[]byte("medium"),
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal values using shared encoding (default to string type for basic marshaling test)
			data, err := EncodeTagValues(tt.values, pbv1.ValueTypeStr)
			if tt.want {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				return
			}

			// Unmarshal and verify
			unmarshaled, err := DecodeTagValues(data, pbv1.ValueTypeStr, len(tt.values))
			require.NoError(t, err)
			assert.Equal(t, len(tt.values), len(unmarshaled))

			for i, expected := range tt.values {
				switch {
				case expected == nil:
					assert.Nil(t, unmarshaled[i])
				case len(expected) == 0:
					// Handle empty byte slices - encoding may return nil for empty values
					assert.True(t, len(unmarshaled[i]) == 0, "Expected empty value at index %d", i)
				default:
					assert.Equal(t, expected, unmarshaled[i])
				}
			}
		})
	}
}

func TestTagValueEncoding(t *testing.T) {
	tests := []struct {
		name      string
		values    [][]byte
		valueType pbv1.ValueType
		wantErr   bool
	}{
		{
			name:      "int64 values",
			values:    [][]byte{{0x39, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}}, // int64(12345) encoded
			valueType: pbv1.ValueTypeInt64,
			wantErr:   false,
		},
		{
			name:      "string values",
			values:    [][]byte{[]byte("test string"), []byte("another string")},
			valueType: pbv1.ValueTypeStr,
			wantErr:   false,
		},
		{
			name:      "binary data",
			values:    [][]byte{{0x01, 0x02, 0x03, 0x04}, {0xFF, 0xFE}},
			valueType: pbv1.ValueTypeBinaryData,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeTagValues(tt.values, tt.valueType)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, encoded)

			// Decode and verify round-trip
			decoded, err := DecodeTagValues(encoded, tt.valueType, len(tt.values))
			require.NoError(t, err)
			assert.Equal(t, len(tt.values), len(decoded))
			for i, expected := range tt.values {
				assert.Equal(t, expected, decoded[i])
			}
		})
	}
}

func TestTagFilterGeneration(t *testing.T) {
	tests := []struct {
		name             string
		values           [][]byte
		expectedElements int
		wantNil          bool
	}{
		{
			name:             "empty values",
			values:           [][]byte{},
			expectedElements: 10,
			wantNil:          true,
		},
		{
			name: "single value",
			values: [][]byte{
				[]byte("value1"),
			},
			expectedElements: 10,
			wantNil:          false,
		},
		{
			name: "multiple values",
			values: [][]byte{
				[]byte("value1"),
				[]byte("value2"),
				[]byte("value3"),
			},
			expectedElements: 10,
			wantNil:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := generateTagFilter(tt.values, tt.expectedElements)
			if tt.wantNil {
				assert.Nil(t, filter)
				return
			}

			require.NotNil(t, filter)

			// Test that all added values might be contained
			for _, value := range tt.values {
				assert.True(t, filter.MightContain(value))
			}

			// Test that a non-added value might not be contained
			// (this could return true due to false positives, but we test anyway)
			nonExistent := []byte("non_existent_value_12345")
			// We can't assert false here due to possible false positives
			filter.MightContain(nonExistent)
		})
	}
}

func TestTagDataOperations(t *testing.T) {
	t.Run("add and check values", func(t *testing.T) {
		td := generateTagData()
		defer releaseTagData(td)

		td.name = testTagName
		td.valueType = pbv1.ValueTypeStr
		td.indexed = true
		td.filter = generateTagFilter([][]byte{}, 10) // Start with empty filter

		// Add values
		values := [][]byte{
			[]byte("value1"),
			[]byte("value2"),
			[]byte("value3"),
		}

		for _, value := range values {
			td.addValue(value)
		}

		// Check that all values are present
		for _, value := range values {
			assert.True(t, td.hasValue(value))
		}

		// Check that a non-existent value might not be present
		assert.False(t, td.hasValue([]byte("non_existent")))
	})

	t.Run("update min max for int64", func(t *testing.T) {
		td := generateTagData()
		defer releaseTagData(td)

		td.name = "int_tag"
		td.valueType = pbv1.ValueTypeInt64

		// Add int64 values (encoded as bytes)
		values := []int64{100, -50, 200, 0, 150}
		for _, v := range values {
			encoded := make([]byte, 8)
			binary.LittleEndian.PutUint64(encoded, uint64(v))
			td.values = append(td.values, encoded)
		}

		td.updateMinMax()

		// Verify min and max
		assert.NotNil(t, td.min)
		assert.NotNil(t, td.max)

		// Decode and verify values
		minVal := int64(binary.LittleEndian.Uint64(td.min))
		assert.Equal(t, int64(-50), minVal)

		maxVal := int64(binary.LittleEndian.Uint64(td.max))
		assert.Equal(t, int64(200), maxVal)
	})

	t.Run("reset functionality", func(t *testing.T) {
		td := generateTagData()

		// Set up tag data
		td.name = testTagName
		td.valueType = pbv1.ValueTypeStr
		td.indexed = true
		td.values = [][]byte{[]byte("value1"), []byte("value2")}
		td.filter = generateTagFilter(td.values, 10)
		td.min = []byte("min")
		td.max = []byte("max")

		// Reset
		td.reset()

		// Verify reset
		assert.Equal(t, "", td.name)
		assert.Equal(t, pbv1.ValueTypeUnknown, td.valueType)
		assert.False(t, td.indexed)
		assert.Equal(t, 0, len(td.values))
		assert.Nil(t, td.filter)
		assert.Nil(t, td.min)
		assert.Nil(t, td.max)

		releaseTagData(td)
	})
}

func TestTagMetadataOperations(t *testing.T) {
	t.Run("marshal and unmarshal", func(t *testing.T) {
		original := generateTagMetadata()
		defer releaseTagMetadata(original)

		// Set up metadata
		original.name = "test_tag"
		original.valueType = pbv1.ValueTypeInt64
		original.indexed = true
		original.dataBlock = dataBlock{offset: 100, size: 500}
		original.filterBlock = dataBlock{offset: 600, size: 200}
		original.min = []byte{0x01, 0x02}
		original.max = []byte{0xFF, 0xFE}

		// Marshal
		data := original.marshal(nil)
		assert.NotNil(t, data)

		// Unmarshal
		unmarshaled, err := unmarshalTagMetadata(data)
		require.NoError(t, err)
		defer releaseTagMetadata(unmarshaled)

		// Verify fields
		assert.Equal(t, original.name, unmarshaled.name)
		assert.Equal(t, original.valueType, unmarshaled.valueType)
		assert.Equal(t, original.indexed, unmarshaled.indexed)
		assert.Equal(t, original.dataBlock, unmarshaled.dataBlock)
		assert.Equal(t, original.filterBlock, unmarshaled.filterBlock)
		assert.Equal(t, original.min, unmarshaled.min)
		assert.Equal(t, original.max, unmarshaled.max)
	})

	t.Run("marshal empty metadata", func(t *testing.T) {
		tm := generateTagMetadata()
		defer releaseTagMetadata(tm)

		tm.name = "empty_tag"
		tm.valueType = pbv1.ValueTypeStr

		data := tm.marshal(nil)

		unmarshaled, err := unmarshalTagMetadata(data)
		require.NoError(t, err)
		defer releaseTagMetadata(unmarshaled)

		assert.Equal(t, tm.name, unmarshaled.name)
		assert.Equal(t, tm.valueType, unmarshaled.valueType)
		assert.False(t, unmarshaled.indexed)
		assert.Nil(t, unmarshaled.min)
		assert.Nil(t, unmarshaled.max)
	})

	t.Run("reset functionality", func(t *testing.T) {
		tm := generateTagMetadata()

		// Set up metadata
		tm.name = "test_tag"
		tm.valueType = pbv1.ValueTypeInt64
		tm.indexed = true
		tm.dataBlock = dataBlock{offset: 100, size: 500}
		tm.filterBlock = dataBlock{offset: 600, size: 200}
		tm.min = []byte("min")
		tm.max = []byte("max")

		// Reset
		tm.reset()

		// Verify reset
		assert.Equal(t, "", tm.name)
		assert.Equal(t, pbv1.ValueTypeUnknown, tm.valueType)
		assert.False(t, tm.indexed)
		assert.Equal(t, dataBlock{}, tm.dataBlock)
		assert.Equal(t, dataBlock{}, tm.filterBlock)
		assert.Nil(t, tm.min)
		assert.Nil(t, tm.max)

		releaseTagMetadata(tm)
	})
}

func TestTagPooling(t *testing.T) {
	t.Run("tagData pooling", func(t *testing.T) {
		// Get from pool
		td1 := generateTagData()
		assert.NotNil(t, td1)

		// Use and release
		td1.name = "test"
		releaseTagData(td1)

		// Get again - should be reset
		td2 := generateTagData()
		assert.NotNil(t, td2)
		assert.Equal(t, "", td2.name) // Should be reset

		releaseTagData(td2)
	})

	t.Run("tagMetadata pooling", func(t *testing.T) {
		// Get from pool
		tm1 := generateTagMetadata()
		assert.NotNil(t, tm1)

		// Use and release
		tm1.name = "test"
		releaseTagMetadata(tm1)

		// Get again - should be reset
		tm2 := generateTagMetadata()
		assert.NotNil(t, tm2)
		assert.Equal(t, "", tm2.name) // Should be reset

		releaseTagMetadata(tm2)
	})

	t.Run("bloomFilter pooling", func(t *testing.T) {
		// Generate bloom filter
		bf1 := generateBloomFilter(100)
		assert.NotNil(t, bf1)
		assert.Equal(t, 100, bf1.N())

		// Add some data
		bf1.Add([]byte("test1"))
		bf1.Add([]byte("test2"))
		assert.True(t, bf1.MightContain([]byte("test1")))

		// Release to pool
		releaseBloomFilter(bf1)

		// Get from pool again
		bf2 := generateBloomFilter(50)
		assert.NotNil(t, bf2)
		assert.Equal(t, 50, bf2.N())                       // Should be reset to new size
		assert.False(t, bf2.MightContain([]byte("test1"))) // Should not contain old data

		releaseBloomFilter(bf2)
	})
}

func TestEdgeCases(t *testing.T) {
	t.Run("invalid int64 length", func(t *testing.T) {
		// The shared encoding module panics on invalid data (fail fast)
		assert.Panics(t, func() {
			invalidValues := [][]byte{{0x01, 0x02, 0x03}} // Only 3 bytes, not 8
			_, _ = EncodeTagValues(invalidValues, pbv1.ValueTypeInt64)
		})
	})

	t.Run("marshal nil values", func(t *testing.T) {
		data, err := EncodeTagValues(nil, pbv1.ValueTypeStr)
		require.NoError(t, err)
		assert.Nil(t, data)

		values, err := DecodeTagValues(nil, pbv1.ValueTypeStr, 0)
		require.NoError(t, err)
		assert.Nil(t, values)
	})

	t.Run("updateMinMax with empty values", func(t *testing.T) {
		td := generateTagData()
		defer releaseTagData(td)

		td.valueType = pbv1.ValueTypeInt64
		td.values = [][]byte{} // empty

		td.updateMinMax()
		assert.Nil(t, td.min)
		assert.Nil(t, td.max)
	})

	t.Run("updateMinMax with non-int64 type", func(t *testing.T) {
		td := generateTagData()
		defer releaseTagData(td)

		td.valueType = pbv1.ValueTypeStr
		td.values = [][]byte{[]byte("test")}

		td.updateMinMax()
		assert.Nil(t, td.min)
		assert.Nil(t, td.max)
	})

	t.Run("hasValue with non-indexed tag", func(t *testing.T) {
		td := generateTagData()
		defer releaseTagData(td)

		td.indexed = false
		td.values = [][]byte{[]byte("value1"), []byte("value2")}

		// Should use linear search
		assert.True(t, td.hasValue([]byte("value1")))
		assert.True(t, td.hasValue([]byte("value2")))
		assert.False(t, td.hasValue([]byte("value3")))
	})
}

func TestRoundTripIntegrity(t *testing.T) {
	t.Run("complete round trip", func(t *testing.T) {
		// Create original tag metadata
		original := generateTagMetadata()
		defer releaseTagMetadata(original)

		original.name = "integration_tag"
		original.valueType = pbv1.ValueTypeInt64
		original.indexed = true
		original.dataBlock = dataBlock{offset: 1000, size: 2000}
		original.filterBlock = dataBlock{offset: 3000, size: 500}

		// Create some int64 values
		int64Values := []int64{-100, 0, 100, 200, 50}
		var encodedValues [][]byte
		for _, v := range int64Values {
			encoded := make([]byte, 8)
			binary.LittleEndian.PutUint64(encoded, uint64(v))
			encodedValues = append(encodedValues, encoded)
		}

		// Marshal values using shared encoding
		marshaledValues, err := EncodeTagValues(encodedValues, pbv1.ValueTypeInt64)
		require.NoError(t, err)

		// Marshal metadata
		marshaledMetadata := original.marshal(nil)

		// Unmarshal metadata
		unmarshaledMetadata, err := unmarshalTagMetadata(marshaledMetadata)
		require.NoError(t, err)
		defer releaseTagMetadata(unmarshaledMetadata)

		// Unmarshal values using shared encoding
		unmarshaledValues, err := DecodeTagValues(marshaledValues, pbv1.ValueTypeInt64, len(encodedValues))
		require.NoError(t, err)

		// Verify metadata integrity
		assert.Equal(t, original.name, unmarshaledMetadata.name)
		assert.Equal(t, original.valueType, unmarshaledMetadata.valueType)
		assert.Equal(t, original.indexed, unmarshaledMetadata.indexed)

		// Verify values integrity
		assert.Equal(t, len(encodedValues), len(unmarshaledValues))
		for i, original := range encodedValues {
			assert.True(t, bytes.Equal(original, unmarshaledValues[i]))
		}

		// Decode and verify int64 values
		for i, expected := range int64Values {
			decoded := int64(binary.LittleEndian.Uint64(unmarshaledValues[i]))
			assert.Equal(t, expected, decoded)
		}
	})
}

func TestTagCompression(t *testing.T) {
	t.Run("compress and decompress tag data", func(t *testing.T) {
		originalData := []byte("this is some test data that should be compressed and decompressed properly")

		// Compress
		compressed := compressTagData(originalData)
		assert.NotNil(t, compressed)
		assert.NotEqual(t, originalData, compressed)

		// Decompress
		decompressed, err := decompressTagData(compressed)
		require.NoError(t, err)
		assert.Equal(t, originalData, decompressed)
	})

	t.Run("compress empty data", func(t *testing.T) {
		compressed := compressTagData(nil)
		assert.Nil(t, compressed)

		compressed = compressTagData([]byte{})
		assert.Nil(t, compressed)
	})

	t.Run("decompress empty data", func(t *testing.T) {
		decompressed, err := decompressTagData(nil)
		require.NoError(t, err)
		assert.Nil(t, decompressed)

		decompressed, err = decompressTagData([]byte{})
		require.NoError(t, err)
		assert.Nil(t, decompressed)
	})
}

func TestTagValuesCompression(t *testing.T) {
	t.Run("encode and decode compressed values", func(t *testing.T) {
		values := [][]byte{
			[]byte("this is a longer string that should compress well"),
			[]byte("another long string with repeated words repeated words"),
			[]byte("yet another string for compression testing purposes"),
		}

		// Encode with automatic compression for string data
		compressed, err := EncodeTagValues(values, pbv1.ValueTypeStr)
		require.NoError(t, err)
		assert.NotNil(t, compressed)

		// Decode compressed data
		decompressed, err := DecodeTagValues(compressed, pbv1.ValueTypeStr, len(values))
		require.NoError(t, err)
		assert.Equal(t, len(values), len(decompressed))

		for i, expected := range values {
			assert.Equal(t, expected, decompressed[i])
		}
	})

	t.Run("compression works for repetitive data", func(t *testing.T) {
		// Create repetitive data that should compress well
		repetitiveData := make([][]byte, 100)
		for i := range repetitiveData {
			repetitiveData[i] = []byte("repeated_data_pattern_that_compresses_well")
		}

		// Encode with automatic compression
		compressed, err := EncodeTagValues(repetitiveData, pbv1.ValueTypeStr)
		require.NoError(t, err)

		// Verify decompression works
		decompressed, err := DecodeTagValues(compressed, pbv1.ValueTypeStr, len(repetitiveData))
		require.NoError(t, err)
		assert.Equal(t, repetitiveData, decompressed)
	})
}

func TestCompressionRoundTrip(t *testing.T) {
	testCases := []struct {
		name   string
		values [][]byte
	}{
		{
			name:   "small values",
			values: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
		},
		{
			name: "large values",
			values: [][]byte{
				[]byte("this is a very long string that contains a lot of data and should compress well when using zstd compression algorithm"),
				[]byte("another long string with different content but still should benefit from compression due to common patterns"),
			},
		},
		{
			name: "mixed size values",
			values: [][]byte{
				[]byte("short"),
				[]byte("this is a medium length string"),
				[]byte("this is a very very very long string that goes on and on with lots of repeated words and patterns that compression algorithms love to work with"),
				[]byte("x"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test round trip: values -> encoded -> decoded
			encoded, err := EncodeTagValues(tc.values, pbv1.ValueTypeStr)
			require.NoError(t, err)

			decoded, err := DecodeTagValues(encoded, pbv1.ValueTypeStr, len(tc.values))
			require.NoError(t, err)

			assert.Equal(t, tc.values, decoded)
		})
	}
}

func TestBloomFilterEncoding(t *testing.T) {
	t.Run("encode and decode bloom filter", func(t *testing.T) {
		// Create a bloom filter and add some data
		bf := generateBloomFilter(100)
		defer releaseBloomFilter(bf)

		testValues := [][]byte{
			[]byte("value1"),
			[]byte("value2"),
			[]byte("value3"),
		}

		for _, value := range testValues {
			bf.Add(value)
		}

		// Encode the bloom filter
		var encoded []byte
		encoded = encodeBloomFilter(encoded, bf)
		assert.NotEmpty(t, encoded)

		// Decode the bloom filter
		decodedBf, err := decodeBloomFilter(encoded)
		require.NoError(t, err)
		defer releaseBloomFilter(decodedBf)

		// Verify the decoded filter contains the same data
		assert.Equal(t, bf.N(), decodedBf.N())
		for _, value := range testValues {
			assert.True(t, decodedBf.MightContain(value))
		}
	})

	t.Run("encode empty bloom filter", func(t *testing.T) {
		var encoded []byte
		encoded = encodeBloomFilter(encoded, nil)
		assert.Empty(t, encoded)
	})

	t.Run("decode invalid data", func(t *testing.T) {
		// Test with data too short
		invalidData := []byte{0x01, 0x02}
		_, err := decodeBloomFilter(invalidData)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "too short")

		// Test with empty data
		_, err = decodeBloomFilter([]byte{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "too short")
	})
}
