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
	"github.com/apache/skywalking-banyandb/pkg/logger"
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
	for k, tag := range b.tags {
		releaseTagData(tag)
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

	// Collect values for this tag across all elements
	for i, tags := range elementTags {
		found := false
		for _, tag := range tags {
			if tag.name == tagName {
				td.values[i] = tag.value
				valueType = tag.valueType
				found = true
				break
			}
		}
		if !found {
			td.values[i] = nil // Missing tag value
		}
	}

	td.valueType = valueType

	// Create bloom filter for indexed tags
	td.filter = generateBloomFilter(len(b.userKeys))
	for _, value := range td.values {
		if value != nil {
			td.filter.Add(value)
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
	tmw, tdw, tfw := ww.GetTagWriters(tagName)

	// Create tag metadata
	tm := generateTagMetadata()
	defer releaseTagMetadata(tm)

	tm.name = tagName
	tm.valueType = td.valueType

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

	// Write bloom filter
	if td.filter != nil {
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

type blockPointer struct {
	block
	bm  blockMetadata
	idx int
}

func (bi *blockPointer) updateMetadata() {
	if len(bi.block.userKeys) == 0 {
		return
	}
	// only update minKey and maxKey since they are used for merging
	// blockWriter will recompute all fields
	bi.bm.minKey = bi.block.userKeys[0]
	bi.bm.maxKey = bi.block.userKeys[len(bi.userKeys)-1]
}

func (bi *blockPointer) copyFrom(src *blockPointer) {
	bi.reset()
	bi.bm.copyFrom(&src.bm)
	bi.appendAll(src)
}

func (bi *blockPointer) appendAll(b *blockPointer) {
	if len(b.userKeys) == 0 {
		return
	}
	bi.append(b, len(b.userKeys))
}

var log = logger.GetLogger("sidx").Named("block")

func (bi *blockPointer) append(b *blockPointer, offset int) {
	if offset <= b.idx {
		return
	}
	if len(bi.tags) == 0 && len(b.tags) > 0 {
		fullTagAppend(bi, b, offset)
	} else {
		if err := fastTagAppend(bi, b, offset); err != nil {
			if log.Debug().Enabled() {
				log.Debug().Msgf("fastTagMerge failed: %v; falling back to fullTagMerge", err)
			}
			fullTagAppend(bi, b, offset)
		}
	}

	assertIdxAndOffset("userKeys", len(b.userKeys), bi.idx, offset)
	bi.userKeys = append(bi.userKeys, b.userKeys[b.idx:offset]...)
	assertIdxAndOffset("data", len(b.data), bi.idx, offset)
	bi.data = append(bi.data, b.data[b.idx:offset]...)
}

func fastTagAppend(bi, b *blockPointer, offset int) error {
	if len(bi.tags) != len(b.tags) {
		return fmt.Errorf("unexpected number of tags: got %d; want %d", len(b.tags), len(bi.tags))
	}
	for _, t := range bi.tags {
		if _, exists := b.tags[t.name]; !exists {
			return fmt.Errorf("unexpected tag name for tag %q", t.name)
		}
		assertIdxAndOffset(t.name, len(b.tags[t.name].values), b.idx, offset)
		bi.tags[t.name].values = append(bi.tags[t.name].values, b.tags[t.name].values[b.idx:offset]...)
	}
	return nil
}

func fullTagAppend(bi, b *blockPointer, offset int) {
	existDataSize := len(bi.userKeys)

	if bi.tags == nil {
		bi.tags = make(map[string]*tagData)
	}
	if len(bi.tags) == 0 {
		for _, t := range b.tags {
			newTagData := tagData{name: t.name, valueType: t.valueType}
			for j := 0; j < existDataSize; j++ {
				newTagData.values = append(newTagData.values, nil)
			}
			assertIdxAndOffset(t.name, len(t.values), b.idx, offset)
			newTagData.values = append(newTagData.values, t.values[b.idx:offset]...)
			bi.tags[t.name] = &newTagData
		}
		return
	}

	for _, t := range b.tags {
		if existingTag, exists := bi.tags[t.name]; exists {
			assertIdxAndOffset(t.name, len(t.values), b.idx, offset)
			existingTag.values = append(existingTag.values, t.values[b.idx:offset]...)
		} else {
			newTagData := tagData{name: t.name, valueType: t.valueType}
			for j := 0; j < existDataSize; j++ {
				newTagData.values = append(newTagData.values, nil)
			}
			assertIdxAndOffset(t.name, len(t.values), b.idx, offset)
			newTagData.values = append(newTagData.values, t.values[b.idx:offset]...)
			bi.tags[t.name] = &newTagData
		}
	}

	sourceTags := make(map[string]struct{})
	for _, t := range b.tags {
		sourceTags[t.name] = struct{}{}
	}

	emptySize := offset - b.idx
	for _, t := range bi.tags {
		if _, exists := sourceTags[t.name]; !exists {
			for j := 0; j < emptySize; j++ {
				bi.tags[t.name].values = append(bi.tags[t.name].values, nil)
			}
		}
	}
}

func assertIdxAndOffset(name string, length int, idx int, offset int) {
	if idx >= offset {
		logger.Panicf("%q idx %d must be less than offset %d", name, idx, offset)
	}
	if offset > length {
		logger.Panicf("%q offset %d must be less than or equal to length %d", name, offset, length)
	}
}

func (bi *blockPointer) isFull() bool {
	return bi.bm.count >= maxBlockLength
}

func (bi *blockPointer) reset() {
	bi.idx = 0
	bi.block.reset()
	bi.bm.reset()
}

func generateBlockPointer() *blockPointer {
	v := blockPointerPool.Get()
	if v == nil {
		return &blockPointer{}
	}
	return v
}

func releaseBlockPointer(bi *blockPointer) {
	bi.reset()
	blockPointerPool.Put(bi)
}

func (b *block) mustSeqReadFrom(decoder *encoding.BytesBlockDecoder, sr *seqReaders, bm blockMetadata) {
	b.reset()
	if err := b.readUserKeys(sr, &bm); err != nil {
		panic(fmt.Sprintf("failed to read user keys: %v", err))
	}
	if err := b.readData(decoder, sr, &bm); err != nil {
		panic(fmt.Sprintf("failed to read data payloads: %v", err))
	}
	if err := b.readTagData(decoder, sr, &bm); err != nil {
		panic(fmt.Sprintf("failed to read tag data: %v", err))
	}
}

func (b *block) readUserKeys(sr *seqReaders, bm *blockMetadata) error {
	bb := bigValuePool.Get()
	if bb == nil {
		bb = &bytes.Buffer{}
	}
	defer func() {
		bb.Buf = bb.Buf[:0]
		bigValuePool.Put(bb)
	}()
	bb.Buf = bytes.ResizeOver(bb.Buf[:0], int(bm.keysBlock.size))
	sr.keys.mustReadFull(bb.Buf)
	var err error
	b.userKeys, err = encoding.BytesToInt64List(b.userKeys[:0], bb.Buf, bm.keysEncodeType, bm.minKey, int(bm.count))
	if err != nil {
		return fmt.Errorf("cannot decode user keys: %w", err)
	}
	return nil
}

func (b *block) readData(decoder *encoding.BytesBlockDecoder, sr *seqReaders, bm *blockMetadata) error {
	bb := &bytes.Buffer{}
	bb.Buf = bytes.ResizeOver(bb.Buf[:0], int(bm.dataBlock.size))
	sr.data.mustReadFull(bb.Buf)
	dataBuf, err := zstd.Decompress(bb.Buf[:0], bb.Buf)
	if err != nil {
		return fmt.Errorf("cannot decompress data: %w", err)
	}
	b.data, err = decoder.Decode(b.data[:0], dataBuf, bm.count)
	if err != nil {
		return fmt.Errorf("cannot decode data payloads: %w", err)
	}
	return nil
}

func (b *block) readTagData(decoder *encoding.BytesBlockDecoder, sr *seqReaders, bm *blockMetadata) error {
	if b.tags == nil {
		b.tags = make(map[string]*tagData)
	}
	for tagName, tagBlock := range bm.tagsBlocks {
		if err := b.readSingleTag(decoder, sr, tagName, &tagBlock, int(bm.count)); err != nil {
			return fmt.Errorf("failed to read tag %s: %w", tagName, err)
		}
	}
	return nil
}

func (b *block) readSingleTag(decoder *encoding.BytesBlockDecoder, sr *seqReaders, tagName string, tagBlock *dataBlock, count int) error {
	tmReader, tmExists := sr.tagMetadata[tagName]
	if !tmExists {
		return fmt.Errorf("tag metadata reader not found for tag %s", tagName)
	}
	tdReader, tdExists := sr.tagData[tagName]
	if !tdExists {
		return fmt.Errorf("tag data reader not found for tag %s", tagName)
	}

	bb := bigValuePool.Get()
	if bb == nil {
		bb = &bytes.Buffer{}
	}
	defer func() {
		bb.Buf = bb.Buf[:0]
		bigValuePool.Put(bb)
	}()

	bb.Buf = bytes.ResizeOver(bb.Buf[:0], int(tagBlock.size))
	tmReader.mustReadFull(bb.Buf)
	tm, err := unmarshalTagMetadata(bb.Buf)
	if err != nil {
		return fmt.Errorf("cannot unmarshal tag metadata: %w", err)
	}
	defer releaseTagMetadata(tm)

	bb.Buf = bytes.ResizeOver(bb.Buf[:0], int(tm.dataBlock.size))
	tdReader.mustReadFull(bb.Buf)
	td := generateTagData()
	td.name = tagName
	td.valueType = tm.valueType
	td.values, err = internalencoding.DecodeTagValues(td.values[:0], decoder, bb, tm.valueType, count)
	if err != nil {
		releaseTagData(td)
		return fmt.Errorf("cannot decode tag values: %w", err)
	}
	b.tags[tagName] = td
	return nil
}

var blockPointerPool = pool.Register[*blockPointer]("sidx-blockPointer")
