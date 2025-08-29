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

// Package sidx provides block structure and operations for organizing elements
// within parts for efficient storage and retrieval based on user-provided int64 keys.
package sidx

import (
	"fmt"

	"github.com/apache/skywalking-banyandb/api/common"
	internalencoding "github.com/apache/skywalking-banyandb/banyand/internal/encoding"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

const (
	// maxElementsPerBlock defines the maximum number of elements per block.
	maxElementsPerBlock = 8 * 1024
)

// block represents a collection of elements organized for storage within a part.
// Elements are sorted by seriesID first, then by user-provided int64 keys.
type block struct {
	// Tag data organized by tag name (pointer field - 8 bytes)
	tags map[string]*tagData // Runtime tag data with filtering

	// Core data arrays (all same length - pointer fields)
	userKeys []int64  // User-provided ordering keys
	data     [][]byte // User payload data
}

var blockPool = pool.Register[*block]("sidx-block")

// generateBlock gets a block from pool or creates new.
func generateBlock() *block {
	v := blockPool.Get()
	if v == nil {
		return &block{
			tags: make(map[string]*tagData),
		}
	}
	return v
}

// releaseBlock returns block to pool after reset.
func releaseBlock(b *block) {
	if b == nil {
		return
	}
	// Release tag filters back to pool
	for _, tag := range b.tags {
		if tag.filter != nil {
			releaseBloomFilter(tag.filter)
		}
		releaseTagData(tag)
	}
	b.reset()
	blockPool.Put(b)
}

// reset clears block for reuse in object pool.
func (b *block) reset() {
	b.userKeys = b.userKeys[:0]

	for i := range b.data {
		b.data[i] = b.data[i][:0]
	}
	b.data = b.data[:0]

	// Clear tag map but keep the map itself
	for k := range b.tags {
		delete(b.tags, k)
	}
}

// mustInitFromTags processes tag data for the block.
func (b *block) mustInitFromTags(elementTags [][]*tag) {
	if len(elementTags) == 0 {
		return
	}

	// Collect all unique tag names
	tagNames := make(map[string]struct{})
	for _, tags := range elementTags {
		for _, tag := range tags {
			tagNames[tag.name] = struct{}{}
		}
	}

	// Process each tag
	for tagName := range tagNames {
		b.processTag(tagName, elementTags)
	}
}

// processTag creates tag data structure for a specific tag.
func (b *block) processTag(tagName string, elementTags [][]*tag) {
	td := generateTagData()
	td.name = tagName
	td.values = make([][]byte, len(b.userKeys))

	var valueType pbv1.ValueType
	var indexed bool

	// Collect values for this tag across all elements
	for i, tags := range elementTags {
		found := false
		for _, tag := range tags {
			if tag.name == tagName {
				td.values[i] = tag.value
				valueType = tag.valueType
				indexed = tag.indexed
				found = true
				break
			}
		}
		if !found {
			td.values[i] = nil // Missing tag value
		}
	}

	td.valueType = valueType
	td.indexed = indexed

	// Create bloom filter for indexed tags
	if indexed {
		td.filter = generateBloomFilter(len(b.userKeys))
		for _, value := range td.values {
			if value != nil {
				td.filter.Add(value)
			}
		}
	}

	// Update min/max for int64 tags
	if valueType == pbv1.ValueTypeInt64 {
		td.updateMinMax()
	}

	b.tags[tagName] = td
}

// validate ensures block data consistency.
func (b *block) validate() error {
	count := len(b.userKeys)
	if count != len(b.data) {
		return fmt.Errorf("inconsistent block arrays: keys=%d, data=%d",
			len(b.userKeys), len(b.data))
	}

	// Verify sorting by userKey
	for i := 1; i < count; i++ {
		if b.userKeys[i] < b.userKeys[i-1] {
			return fmt.Errorf("block not sorted by userKey at index %d: %d < %d",
				i, b.userKeys[i], b.userKeys[i-1])
		}
	}

	// Verify tag consistency
	for tagName, tagData := range b.tags {
		if len(tagData.values) != count {
			return fmt.Errorf("tag %s has %d values but block has %d elements",
				tagName, len(tagData.values), count)
		}
	}

	return nil
}

// uncompressedSizeBytes calculates the uncompressed size of the block.
func (b *block) uncompressedSizeBytes() uint64 {
	count := uint64(len(b.userKeys))
	size := count * 8 // userKey

	// Add data payload sizes
	for _, payload := range b.data {
		size += uint64(len(payload))
	}

	// Add tag data sizes
	for tagName, tagData := range b.tags {
		nameSize := uint64(len(tagName))
		for _, value := range tagData.values {
			if value != nil {
				size += nameSize + uint64(len(value))
			}
		}
	}

	return size
}

// isFull checks if block has reached element count limit.
func (b *block) isFull() bool {
	return len(b.userKeys) >= maxElementsPerBlock
}

// Len returns the number of elements in the block.
func (b *block) Len() int {
	return len(b.userKeys)
}

// isEmpty checks if the block contains no elements.
func (b *block) isEmpty() bool {
	return len(b.userKeys) == 0
}

// getKeyRange returns the min and max user keys in the block.
func (b *block) getKeyRange() (int64, int64) {
	if len(b.userKeys) == 0 {
		return 0, 0
	}
	return b.userKeys[0], b.userKeys[len(b.userKeys)-1]
}

// mustWriteTo writes block data to files through the provided writers.
// This method serializes the block's userKeys, data, and tags
// to their respective files while updating the block metadata.
func (b *block) mustWriteTo(sid common.SeriesID, bm *blockMetadata, ww *writers) {
	if err := b.validate(); err != nil {
		panic(fmt.Sprintf("block validation failed: %v", err))
	}
	bm.reset()

	bm.seriesID = sid
	bm.uncompressedSize = b.uncompressedSizeBytes()
	bm.count = uint64(b.Len())

	// Write user keys to keys.bin and capture encoding information
	bm.keysEncodeType, bm.minKey = mustWriteKeysTo(&bm.keysBlock, b.userKeys, &ww.keysWriter)

	// Write data payloads to data.bin
	mustWriteDataTo(&bm.dataBlock, b.data, &ww.dataWriter)

	// Write each tag to its respective files
	for tagName, tagData := range b.tags {
		b.mustWriteTag(tagName, tagData, bm, ww)
	}
}

// mustWriteTag writes a single tag's data to its tag files.
func (b *block) mustWriteTag(tagName string, td *tagData, bm *blockMetadata, ww *writers) {
	tmw, tdw, tfw := ww.getWriters(tagName)

	// Create tag metadata
	tm := generateTagMetadata()
	defer releaseTagMetadata(tm)

	tm.name = tagName
	tm.valueType = td.valueType
	tm.indexed = td.indexed

	// Write tag values to data file
	bb := bigValuePool.Get()
	if bb == nil {
		bb = &bytes.Buffer{}
	}
	defer func() {
		bb.Buf = bb.Buf[:0]
		bigValuePool.Put(bb)
	}()

	// Encode tag values using the encoding module
	err := internalencoding.EncodeTagValues(bb, td.values, td.valueType)
	if err != nil {
		panic(fmt.Sprintf("failed to encode tag values: %v", err))
	}

	// Write tag data without compression
	tm.dataBlock.offset = tdw.bytesWritten
	tm.dataBlock.size = uint64(len(bb.Buf))
	tdw.MustWrite(bb.Buf)

	// Write bloom filter if indexed
	if td.indexed && td.filter != nil {
		filterData := encodeBloomFilter(nil, td.filter)
		tm.filterBlock.offset = tfw.bytesWritten
		tm.filterBlock.size = uint64(len(filterData))
		tfw.MustWrite(filterData)
	}

	// Set min/max for int64 tags
	if td.valueType == pbv1.ValueTypeInt64 {
		tm.min = td.min
		tm.max = td.max
	}

	// Marshal and write tag metadata
	bb.Buf = bb.Buf[:0]
	bb.Buf = tm.marshalAppend(bb.Buf)
	tmw.MustWrite(bb.Buf)

	// Update block metadata
	offset := tmw.bytesWritten - uint64(len(bb.Buf))
	size := uint64(len(bb.Buf))
	bm.setTagMetadata(tagName, offset, size)
}

// mustWriteKeysTo writes user keys to the keys writer and returns encoding metadata.
func mustWriteKeysTo(kb *dataBlock, userKeys []int64, keysWriter *writer) (encoding.EncodeType, int64) {
	bb := bigValuePool.Get()
	if bb == nil {
		bb = &bytes.Buffer{}
	}
	defer func() {
		bb.Buf = bb.Buf[:0]
		bigValuePool.Put(bb)
	}()

	// Encode user keys
	var encodeType encoding.EncodeType
	var firstValue int64
	bb.Buf, encodeType, firstValue = encoding.Int64ListToBytes(bb.Buf[:0], userKeys)

	// Write encoded data directly without compression
	kb.offset = keysWriter.bytesWritten
	kb.size = uint64(len(bb.Buf))
	keysWriter.MustWrite(bb.Buf)

	return encodeType, firstValue
}

// mustWriteDataTo writes data payloads to the data writer.
func mustWriteDataTo(db *dataBlock, data [][]byte, dataWriter *writer) {
	bb := bigValuePool.Get()
	if bb == nil {
		bb = &bytes.Buffer{}
	}
	defer func() {
		bb.Buf = bb.Buf[:0]
		bigValuePool.Put(bb)
	}()

	// Encode all data payloads as a block
	bb.Buf = encoding.EncodeBytesBlock(bb.Buf[:0], data)

	// Compress and write
	compressedData := zstd.Compress(nil, bb.Buf, 1)
	db.offset = dataWriter.bytesWritten
	db.size = uint64(len(compressedData))
	dataWriter.MustWrite(compressedData)
}
