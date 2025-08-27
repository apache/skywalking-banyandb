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
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/filter"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

// tagFilterOp provides a FilterOp implementation for tag filtering in sidx.
type tagFilterOp struct {
	blockMetadata *blockMetadata
	part          *part
	tagCache      map[string]*tagFilterCache
}

// tagFilterCache caches tag filter data for a specific tag.
type tagFilterCache struct {
	bloomFilter *filter.BloomFilter
	min         []byte
	max         []byte
	valueType   pbv1.ValueType
}

// Eq checks if a tag equals a specific value by reading tag data and checking bloom filter.
func (tfo *tagFilterOp) Eq(tagName string, tagValue string) bool {
	if tfo.blockMetadata == nil || tfo.part == nil {
		return false
	}

	// Check if the tag exists in the block
	tagBlock, exists := tfo.blockMetadata.tagsBlocks[tagName]
	if !exists {
		return false
	}

	// Get or create cached tag filter data
	cache, err := tfo.getTagFilterCache(tagName, tagBlock)
	if err != nil {
		logger.Errorf("failed to get tag filter cache for %s: %v", tagName, err)
		return true // Conservative approach - don't filter out
	}

	// Use bloom filter to check if the value might exist
	if cache.bloomFilter != nil {
		return cache.bloomFilter.MightContain([]byte(tagValue))
	}

	// If no bloom filter, conservatively return true
	return true
}

// Range checks if a tag is within a specific range using min/max metadata.
func (tfo *tagFilterOp) Range(tagName string, rangeOpts index.RangeOpts) (bool, error) {
	if tfo.blockMetadata == nil || tfo.part == nil {
		return false, nil
	}

	// Check if the tag exists in the block
	tagBlock, exists := tfo.blockMetadata.tagsBlocks[tagName]
	if !exists {
		return false, nil
	}

	// Get or create cached tag filter data
	cache, err := tfo.getTagFilterCache(tagName, tagBlock)
	if err != nil {
		return false, fmt.Errorf("failed to get tag filter cache for %s: %w", tagName, err)
	}

	// Only perform range check for numeric types with min/max values
	if cache.valueType != pbv1.ValueTypeInt64 || len(cache.min) == 0 || len(cache.max) == 0 {
		return true, nil // Conservative approach for non-numeric or missing min/max
	}

	// Check lower bound
	if rangeOpts.Lower != nil {
		lower, ok := rangeOpts.Lower.(*index.FloatTermValue)
		if !ok {
			return false, fmt.Errorf("lower bound is not a float value: %v", rangeOpts.Lower)
		}
		value := make([]byte, 0)
		value = encoding.Int64ToBytes(value, int64(lower.Value))
		if bytes.Compare(cache.max, value) == -1 || (!rangeOpts.IncludesLower && bytes.Equal(cache.max, value)) {
			return false, nil
		}
	}

	// Check upper bound
	if rangeOpts.Upper != nil {
		upper, ok := rangeOpts.Upper.(*index.FloatTermValue)
		if !ok {
			return false, fmt.Errorf("upper bound is not a float value: %v", rangeOpts.Upper)
		}
		value := make([]byte, 0)
		value = encoding.Int64ToBytes(value, int64(upper.Value))
		if bytes.Compare(cache.min, value) == 1 || (!rangeOpts.IncludesUpper && bytes.Equal(cache.min, value)) {
			return false, nil
		}
	}

	return true, nil
}

// getTagFilterCache retrieves or creates cached tag filter data.
func (tfo *tagFilterOp) getTagFilterCache(tagName string, tagBlock dataBlock) (*tagFilterCache, error) {
	// Check cache first
	if cache, exists := tfo.tagCache[tagName]; exists {
		return cache, nil
	}

	// Read tag metadata to get filter information
	tagMetadata, err := tfo.readTagMetadata(tagName, tagBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to read tag metadata: %w", err)
	}
	defer releaseTagMetadata(tagMetadata)

	// Create cache entry
	cache := &tagFilterCache{
		valueType: tagMetadata.valueType,
		min:       make([]byte, len(tagMetadata.min)),
		max:       make([]byte, len(tagMetadata.max)),
	}
	copy(cache.min, tagMetadata.min)
	copy(cache.max, tagMetadata.max)

	// Read bloom filter if available
	if tagMetadata.filterBlock.size > 0 {
		bf, err := tfo.readBloomFilter(tagName, tagMetadata.filterBlock)
		if err != nil {
			return nil, fmt.Errorf("failed to read bloom filter: %w", err)
		}
		cache.bloomFilter = bf
	}

	// Cache the result
	tfo.tagCache[tagName] = cache

	return cache, nil
}

// readTagMetadata reads tag metadata from the tag metadata files.
func (tfo *tagFilterOp) readTagMetadata(tagName string, tagBlock dataBlock) (*tagMetadata, error) {
	if tagBlock.size == 0 {
		return nil, fmt.Errorf("empty tag block for %s", tagName)
	}

	// Get tag metadata reader
	metaReader, exists := tfo.part.getTagMetadataReader(tagName)
	if !exists {
		return nil, fmt.Errorf("no metadata reader for tag %s", tagName)
	}

	// Read compressed metadata
	compressedData := make([]byte, tagBlock.size)
	fs.MustReadData(metaReader, int64(tagBlock.offset), compressedData)

	// Decompress data
	decompressedData, err := zstd.Decompress(nil, compressedData)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress tag metadata: %w", err)
	}

	// Unmarshal tag metadata
	tm, err := unmarshalTagMetadata(decompressedData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal tag metadata: %w", err)
	}

	return tm, nil
}

// readBloomFilter reads and decodes a bloom filter from filter data.
func (tfo *tagFilterOp) readBloomFilter(tagName string, filterBlock dataBlock) (*filter.BloomFilter, error) {
	if filterBlock.size == 0 {
		return nil, fmt.Errorf("empty filter block")
	}

	// Get filter reader
	filterReader, exists := tfo.part.getTagFilterReader(tagName)
	if !exists {
		return nil, fmt.Errorf("no filter reader for tag %s", tagName)
	}

	// Read filter data
	filterData := make([]byte, filterBlock.size)
	fs.MustReadData(filterReader, int64(filterBlock.offset), filterData)

	return decodeBloomFilter(filterData)
}

// decodeBloomFilterFromBytes decodes bloom filter data (similar to stream module).
func decodeBloomFilterFromBytes(src []byte, bf *filter.BloomFilter) *filter.BloomFilter {
	n := encoding.BytesToInt64(src)
	bf.SetN(int(n))

	m := n * filter.B
	bits := make([]uint64, 0)
	bits, _, err := encoding.DecodeUint64Block(bits[:0], src[8:], uint64((m+63)/64))
	if err != nil {
		logger.Panicf("failed to decode Bloom filter: %v", err)
	}
	bf.SetBits(bits)

	return bf
}

// reset resets the tagFilterOp for reuse.
func (tfo *tagFilterOp) reset() {
	tfo.blockMetadata = nil
	tfo.part = nil
	for key, cache := range tfo.tagCache {
		if cache.bloomFilter != nil {
			releaseBloomFilter(cache.bloomFilter)
		}
		delete(tfo.tagCache, key)
	}
}

// generateTagFilterOp gets a tagFilterOp from pool or creates new.
func generateTagFilterOp(bm *blockMetadata, p *part) *tagFilterOp {
	v := tagFilterOpPool.Get()
	if v == nil {
		return &tagFilterOp{
			blockMetadata: bm,
			part:          p,
			tagCache:      make(map[string]*tagFilterCache),
		}
	}
	tfo := v
	tfo.blockMetadata = bm
	tfo.part = p
	return tfo
}

// releaseTagFilterOp returns tagFilterOp to pool after reset.
func releaseTagFilterOp(tfo *tagFilterOp) {
	tfo.reset()
	tagFilterOpPool.Put(tfo)
}

var tagFilterOpPool = pool.Register[*tagFilterOp]("sidx-tagFilterOp")
