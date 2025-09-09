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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/filter"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestTagFilterOpPooling(t *testing.T) {
	t.Run("pool allocation and release", func(t *testing.T) {
		// Create mock block metadata and part
		bm := &blockMetadata{
			tagsBlocks: make(map[string]dataBlock),
		}
		p := &part{}

		// Get from pool
		tfo1 := generateTagFilterOp(bm, p)
		require.NotNil(t, tfo1)
		assert.Equal(t, bm, tfo1.blockMetadata)
		assert.Equal(t, p, tfo1.part)
		assert.NotNil(t, tfo1.tagCache)

		// Get another from pool
		tfo2 := generateTagFilterOp(bm, p)
		require.NotNil(t, tfo2)
		assert.NotSame(t, tfo1, tfo2, "pool should provide different instances when available")

		// Release back to pool
		releaseTagFilterOp(tfo1)
		releaseTagFilterOp(tfo2)

		// Verify reset was called
		assert.Nil(t, tfo1.blockMetadata)
		assert.Nil(t, tfo1.part)
		assert.Empty(t, tfo1.tagCache)
	})
}

func TestTagFilterOpReset(t *testing.T) {
	// Create a tagFilterOp with data
	tfo := &tagFilterOp{
		blockMetadata: &blockMetadata{},
		part:          &part{},
		tagCache: map[string]*tagFilterCache{
			"tag1": {
				bloomFilter: filter.NewBloomFilter(100),
				min:         []byte("min"),
				max:         []byte("max"),
				valueType:   pbv1.ValueTypeStr,
			},
		},
	}

	// Call reset
	tfo.reset()

	// Verify everything is reset
	assert.Nil(t, tfo.blockMetadata)
	assert.Nil(t, tfo.part)
	assert.Empty(t, tfo.tagCache)
}

func TestTagFilterOpEq(t *testing.T) {
	tests := []struct {
		blockMetadata  *blockMetadata
		part           *part
		name           string
		tagName        string
		tagValue       string
		description    string
		expectedResult bool
	}{
		{
			name:           "nil block metadata",
			blockMetadata:  nil,
			part:           &part{},
			tagName:        "service",
			tagValue:       "order-service",
			expectedResult: false,
			description:    "should return false when blockMetadata is nil",
		},
		{
			name:           "nil part",
			blockMetadata:  &blockMetadata{tagsBlocks: make(map[string]dataBlock)},
			part:           nil,
			tagName:        "service",
			tagValue:       "order-service",
			expectedResult: false,
			description:    "should return false when part is nil",
		},
		{
			name: "tag not in block",
			blockMetadata: &blockMetadata{
				tagsBlocks: map[string]dataBlock{
					"other_tag": {offset: 0, size: 100},
				},
			},
			part:           &part{},
			tagName:        "service",
			tagValue:       "order-service",
			expectedResult: false,
			description:    "should return false when tag doesn't exist in block",
		},
		{
			name: "tag exists but no cache data",
			blockMetadata: &blockMetadata{
				tagsBlocks: map[string]dataBlock{
					"service": {offset: 0, size: 100},
				},
			},
			part:           &part{},
			tagName:        "service",
			tagValue:       "order-service",
			expectedResult: true,
			description:    "should return true (conservative) when tag exists but cache fails",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tfo := &tagFilterOp{
				blockMetadata: tt.blockMetadata,
				part:          tt.part,
				tagCache:      make(map[string]*tagFilterCache),
			}

			result := tfo.Eq(tt.tagName, tt.tagValue)
			assert.Equal(t, tt.expectedResult, result, tt.description)
		})
	}
}

func TestTagFilterOpRange(t *testing.T) {
	tests := []struct {
		blockMetadata  *blockMetadata
		part           *part
		rangeOpts      index.RangeOpts
		name           string
		tagName        string
		description    string
		expectedResult bool
		expectError    bool
	}{
		{
			name:           "nil block metadata",
			blockMetadata:  nil,
			part:           &part{},
			tagName:        "duration",
			rangeOpts:      index.RangeOpts{},
			expectedResult: false,
			expectError:    false,
			description:    "should return false when blockMetadata is nil",
		},
		{
			name:           "nil part",
			blockMetadata:  &blockMetadata{tagsBlocks: make(map[string]dataBlock)},
			part:           nil,
			tagName:        "duration",
			rangeOpts:      index.RangeOpts{},
			expectedResult: false,
			expectError:    false,
			description:    "should return false when part is nil",
		},
		{
			name: "tag not in block",
			blockMetadata: &blockMetadata{
				tagsBlocks: map[string]dataBlock{
					"other_tag": {offset: 0, size: 100},
				},
			},
			part:           &part{},
			tagName:        "duration",
			rangeOpts:      index.RangeOpts{},
			expectedResult: false,
			expectError:    false,
			description:    "should return false when tag doesn't exist in block",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tfo := &tagFilterOp{
				blockMetadata: tt.blockMetadata,
				part:          tt.part,
				tagCache:      make(map[string]*tagFilterCache),
			}

			result, err := tfo.Range(tt.tagName, tt.rangeOpts)

			if tt.expectError {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)
			}

			assert.Equal(t, tt.expectedResult, result, tt.description)
		})
	}
}

func TestTagFilterOpRangeWithCache(t *testing.T) {
	// Create a cache with numeric data
	cache := &tagFilterCache{
		valueType: pbv1.ValueTypeInt64,
		min:       encoding.Int64ToBytes(nil, 100),
		max:       encoding.Int64ToBytes(nil, 500),
	}

	tfo := &tagFilterOp{
		blockMetadata: &blockMetadata{
			tagsBlocks: map[string]dataBlock{
				"duration": {offset: 0, size: 100},
			},
		},
		part: &part{},
		tagCache: map[string]*tagFilterCache{
			"duration": cache,
		},
	}

	tests := []struct {
		rangeOpts      index.RangeOpts
		name           string
		description    string
		expectedResult bool
		expectError    bool
	}{
		{
			name: "range completely below",
			rangeOpts: index.RangeOpts{
				Lower:         &index.FloatTermValue{Value: 10},
				Upper:         &index.FloatTermValue{Value: 50},
				IncludesLower: true,
				IncludesUpper: true,
			},
			expectedResult: false,
			expectError:    false,
			description:    "should return false when range is completely below min",
		},
		{
			name: "range completely above",
			rangeOpts: index.RangeOpts{
				Lower:         &index.FloatTermValue{Value: 600},
				Upper:         &index.FloatTermValue{Value: 800},
				IncludesLower: true,
				IncludesUpper: true,
			},
			expectedResult: false,
			expectError:    false,
			description:    "should return false when range is completely above max",
		},
		{
			name: "range overlaps",
			rangeOpts: index.RangeOpts{
				Lower:         &index.FloatTermValue{Value: 200},
				Upper:         &index.FloatTermValue{Value: 300},
				IncludesLower: true,
				IncludesUpper: true,
			},
			expectedResult: true,
			expectError:    false,
			description:    "should return true when range overlaps with min/max",
		},
		{
			name: "range contains all",
			rangeOpts: index.RangeOpts{
				Lower:         &index.FloatTermValue{Value: 50},
				Upper:         &index.FloatTermValue{Value: 600},
				IncludesLower: true,
				IncludesUpper: true,
			},
			expectedResult: true,
			expectError:    false,
			description:    "should return true when range contains all values",
		},
		{
			name: "lower boundary exclusive miss",
			rangeOpts: index.RangeOpts{
				Lower:         &index.FloatTermValue{Value: 500},
				IncludesLower: false,
			},
			expectedResult: false,
			expectError:    false,
			description:    "should return false when lower boundary is exclusive and equals max",
		},
		{
			name: "upper boundary exclusive miss",
			rangeOpts: index.RangeOpts{
				Upper:         &index.FloatTermValue{Value: 100},
				IncludesUpper: false,
			},
			expectedResult: false,
			expectError:    false,
			description:    "should return false when upper boundary is exclusive and equals min",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tfo.Range("duration", tt.rangeOpts)

			if tt.expectError {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)
			}

			assert.Equal(t, tt.expectedResult, result, tt.description)
		})
	}
}

func TestTagFilterOpRangeNonNumeric(t *testing.T) {
	// Create a cache with string data (no min/max)
	cache := &tagFilterCache{
		valueType: pbv1.ValueTypeStr,
		min:       nil,
		max:       nil,
	}

	tfo := &tagFilterOp{
		blockMetadata: &blockMetadata{
			tagsBlocks: map[string]dataBlock{
				"service": {offset: 0, size: 100},
			},
		},
		part: &part{},
		tagCache: map[string]*tagFilterCache{
			"service": cache,
		},
	}

	// For non-numeric types, should always return true (conservative approach)
	result, err := tfo.Range("service", index.RangeOpts{
		Lower:         &index.FloatTermValue{Value: 100},
		Upper:         &index.FloatTermValue{Value: 200},
		IncludesLower: true,
		IncludesUpper: true,
	})

	assert.NoError(t, err)
	assert.True(t, result, "should return true for non-numeric types")
}

func TestDecodeBloomFilterFromBytes(t *testing.T) {
	// Create test bloom filter data
	originalBF := filter.NewBloomFilter(1000)
	originalBF.Add([]byte("test-value-1"))
	originalBF.Add([]byte("test-value-2"))
	originalBF.Add([]byte("test-value-3"))

	// Encode bloom filter data (similar to stream module)
	n := int64(originalBF.N())
	bits := originalBF.Bits()

	var buf bytes.Buffer
	buf.Write(encoding.Int64ToBytes(nil, n))

	encodedBits := encoding.EncodeUint64Block(nil, bits)
	buf.Write(encodedBits)

	// Test decoding
	decodedBF := filter.NewBloomFilter(0)
	result := decodeBloomFilterFromBytes(buf.Bytes(), decodedBF)

	// Verify decoded bloom filter
	assert.Equal(t, originalBF.N(), result.N())
	assert.True(t, result.MightContain([]byte("test-value-1")))
	assert.True(t, result.MightContain([]byte("test-value-2")))
	assert.True(t, result.MightContain([]byte("test-value-3")))
	assert.False(t, result.MightContain([]byte("non-existent-value")))
}

func TestTagFilterCacheIntegration(t *testing.T) {
	// This test would require setting up actual file system and tag metadata
	// For now, we test the basic structure and error handling
	t.Run("cache creation structure", func(t *testing.T) {
		cache := &tagFilterCache{
			bloomFilter: filter.NewBloomFilter(100),
			min:         []byte("min_value"),
			max:         []byte("max_value"),
			valueType:   pbv1.ValueTypeStr,
		}

		assert.NotNil(t, cache.bloomFilter)
		assert.Equal(t, []byte("min_value"), cache.min)
		assert.Equal(t, []byte("max_value"), cache.max)
		assert.Equal(t, pbv1.ValueTypeStr, cache.valueType)
	})
}

// Benchmark tests for performance verification.
func BenchmarkTagFilterOpEq(b *testing.B) {
	// Setup
	bm := &blockMetadata{
		tagsBlocks: map[string]dataBlock{
			"service": {offset: 0, size: 100},
		},
	}
	p := &part{}

	tfo := generateTagFilterOp(bm, p)
	defer releaseTagFilterOp(tfo)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tfo.Eq("service", "order-service")
	}
}

func BenchmarkTagFilterOpRange(b *testing.B) {
	// Setup
	cache := &tagFilterCache{
		valueType: pbv1.ValueTypeInt64,
		min:       encoding.Int64ToBytes(nil, 100),
		max:       encoding.Int64ToBytes(nil, 500),
	}

	tfo := &tagFilterOp{
		blockMetadata: &blockMetadata{
			tagsBlocks: map[string]dataBlock{
				"duration": {offset: 0, size: 100},
			},
		},
		part: &part{},
		tagCache: map[string]*tagFilterCache{
			"duration": cache,
		},
	}

	rangeOpts := index.RangeOpts{
		Lower:         &index.FloatTermValue{Value: 200},
		Upper:         &index.FloatTermValue{Value: 300},
		IncludesLower: true,
		IncludesUpper: true,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tfo.Range("duration", rangeOpts)
	}
}

func BenchmarkDecodeBloomFilterFromBytes(b *testing.B) {
	// Setup test data
	bf := filter.NewBloomFilter(1000)
	bf.Add([]byte("test-value"))

	n := int64(bf.N())
	bits := bf.Bits()

	var buf bytes.Buffer
	buf.Write(encoding.Int64ToBytes(nil, n))
	encodedBits := encoding.EncodeUint64Block(nil, bits)
	buf.Write(encodedBits)

	testData := buf.Bytes()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		decodedBF := filter.NewBloomFilter(0)
		decodeBloomFilterFromBytes(testData, decodedBF)
	}
}

func TestTagFilterOpErrorHandling(t *testing.T) {
	t.Run("invalid range bounds", func(t *testing.T) {
		cache := &tagFilterCache{
			valueType: pbv1.ValueTypeInt64,
			min:       encoding.Int64ToBytes(nil, 100),
			max:       encoding.Int64ToBytes(nil, 500),
		}

		tfo := &tagFilterOp{
			blockMetadata: &blockMetadata{
				tagsBlocks: map[string]dataBlock{
					"duration": {offset: 0, size: 100},
				},
			},
			part: &part{},
			tagCache: map[string]*tagFilterCache{
				"duration": cache,
			},
		}

		// Test with invalid lower bound type (using BytesTermValue instead of expected FloatTermValue)
		_, err := tfo.Range("duration", index.RangeOpts{
			Lower: &index.BytesTermValue{Value: []byte("invalid")},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "lower bound is not a float value")

		// Test with invalid upper bound type (using BytesTermValue instead of expected FloatTermValue)
		_, err = tfo.Range("duration", index.RangeOpts{
			Upper: &index.BytesTermValue{Value: []byte("invalid")},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "upper bound is not a float value")
	})
}
