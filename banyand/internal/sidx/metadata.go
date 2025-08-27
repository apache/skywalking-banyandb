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
	"encoding/json"
	"fmt"
	"maps"
	"sort"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

// partMetadata contains metadata for an entire part (replaces timestamp-specific metadata from stream module).
type partMetadata struct {
	// Size information
	CompressedSizeBytes   uint64 `json:"compressedSizeBytes"`
	UncompressedSizeBytes uint64 `json:"uncompressedSizeBytes"`
	TotalCount            uint64 `json:"totalCount"`
	BlocksCount           uint64 `json:"blocksCount"`

	// Key range (replaces timestamp range from stream module)
	MinKey int64 `json:"minKey"` // Minimum user key in part
	MaxKey int64 `json:"maxKey"` // Maximum user key in part

	// Identity
	ID uint64 `json:"id"` // Unique part identifier
}

// blockMetadata contains metadata for a block within a part.
type blockMetadata struct {
	tagsBlocks       map[string]dataBlock
	tagProjection    []string
	dataBlock        dataBlock
	keysBlock        dataBlock
	seriesID         common.SeriesID
	minKey           int64
	maxKey           int64
	uncompressedSize uint64
	count            uint64
	keysEncodeType   encoding.EncodeType
}

type blockMetadataArray struct {
	arr []blockMetadata
}

func (bma *blockMetadataArray) reset() {
	for i := range bma.arr {
		bma.arr[i].reset()
	}
	bma.arr = bma.arr[:0]
}

var (
	partMetadataPool       = pool.Register[*partMetadata]("sidx-partMetadata")
	blockMetadataPool      = pool.Register[*blockMetadata]("sidx-blockMetadata")
	blockMetadataArrayPool = pool.Register[*blockMetadataArray]("sidx-blockMetadataArray")
)

// generatePartMetadata gets partMetadata from pool or creates new.
func generatePartMetadata() *partMetadata {
	v := partMetadataPool.Get()
	if v == nil {
		return &partMetadata{}
	}
	return v
}

// releasePartMetadata returns partMetadata to pool after reset.
func releasePartMetadata(pm *partMetadata) {
	if pm == nil {
		return
	}
	pm.reset()
	partMetadataPool.Put(pm)
}

// generateBlockMetadata gets blockMetadata from pool or creates new.
func generateBlockMetadata() *blockMetadata {
	v := blockMetadataPool.Get()
	if v == nil {
		return &blockMetadata{
			tagsBlocks: make(map[string]dataBlock),
		}
	}
	return v
}

// releaseBlockMetadata returns blockMetadata to pool after reset.
func releaseBlockMetadata(bm *blockMetadata) {
	if bm == nil {
		return
	}
	bm.reset()
	blockMetadataPool.Put(bm)
}

// generateBlockMetadataArray gets blockMetadataArray from pool or creates new.
func generateBlockMetadataArray() *blockMetadataArray {
	v := blockMetadataArrayPool.Get()
	if v == nil {
		return &blockMetadataArray{}
	}
	return v
}

// releaseBlockMetadataArray returns blockMetadataArray to pool after reset.
func releaseBlockMetadataArray(bma *blockMetadataArray) {
	if bma == nil {
		return
	}
	bma.reset()
	blockMetadataArrayPool.Put(bma)
}

// reset clears partMetadata for reuse in object pool.
func (pm *partMetadata) reset() {
	pm.CompressedSizeBytes = 0
	pm.UncompressedSizeBytes = 0
	pm.TotalCount = 0
	pm.BlocksCount = 0
	pm.MinKey = 0
	pm.MaxKey = 0
	pm.ID = 0
}

// reset clears blockMetadata for reuse in object pool.
func (bm *blockMetadata) reset() {
	bm.seriesID = 0
	bm.minKey = 0
	bm.maxKey = 0
	bm.dataBlock = dataBlock{}
	bm.keysBlock = dataBlock{}
	bm.uncompressedSize = 0
	bm.count = 0
	bm.keysEncodeType = 0

	// Clear maps but keep them allocated
	for k := range bm.tagsBlocks {
		delete(bm.tagsBlocks, k)
	}
	bm.tagProjection = bm.tagProjection[:0]
}

func (bm *blockMetadata) copyFrom(other *blockMetadata) {
	if other == nil {
		return
	}

	bm.seriesID = other.seriesID
	bm.minKey = other.minKey
	bm.maxKey = other.maxKey
	bm.count = other.count
	bm.uncompressedSize = other.uncompressedSize
	bm.keysEncodeType = other.keysEncodeType
	bm.dataBlock = other.dataBlock
	bm.keysBlock = other.keysBlock

	// Copy tag blocks
	if bm.tagsBlocks == nil {
		bm.tagsBlocks = make(map[string]dataBlock)
	}
	clear(bm.tagsBlocks)
	maps.Copy(bm.tagsBlocks, other.tagsBlocks)

	// Copy tag projection
	bm.tagProjection = bm.tagProjection[:0]
	bm.tagProjection = append(bm.tagProjection, other.tagProjection...)
}

// validate validates the partMetadata for consistency.
func (pm *partMetadata) validate() error {
	if pm.MinKey > pm.MaxKey {
		return fmt.Errorf("invalid key range: MinKey (%d) > MaxKey (%d)", pm.MinKey, pm.MaxKey)
	}
	if pm.CompressedSizeBytes > pm.UncompressedSizeBytes {
		return fmt.Errorf("invalid size: compressed (%d) > uncompressed (%d)",
			pm.CompressedSizeBytes, pm.UncompressedSizeBytes)
	}
	if pm.BlocksCount == 0 && pm.TotalCount > 0 {
		return fmt.Errorf("invalid counts: no blocks but has %d elements", pm.TotalCount)
	}
	return nil
}

// validate validates the blockMetadata for consistency.
func (bm *blockMetadata) validate() error {
	if bm.minKey > bm.maxKey {
		return fmt.Errorf("invalid block key range: minKey (%d) > maxKey (%d)", bm.minKey, bm.maxKey)
	}
	if bm.seriesID == 0 {
		return fmt.Errorf("invalid seriesID: cannot be zero")
	}
	if bm.dataBlock.size == 0 {
		return fmt.Errorf("invalid data block: size cannot be zero")
	}
	if bm.keysBlock.size == 0 && bm.keysEncodeType != encoding.EncodeTypeConst {
		return fmt.Errorf("invalid keys block: size cannot be zero unless using const encoding")
	}
	return nil
}

// marshal serializes partMetadata to JSON bytes.
func (pm *partMetadata) marshal() ([]byte, error) {
	data, err := json.Marshal(pm)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal partMetadata to JSON: %w", err)
	}

	return data, nil
}

// unmarshalPartMetadata deserializes partMetadata from JSON bytes.
func unmarshalPartMetadata(data []byte) (*partMetadata, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data provided")
	}

	pm := generatePartMetadata()

	if err := json.Unmarshal(data, pm); err != nil {
		releasePartMetadata(pm)
		return nil, fmt.Errorf("failed to unmarshal partMetadata from JSON: %w", err)
	}

	// Validate the metadata
	if err := pm.validate(); err != nil {
		releasePartMetadata(pm)
		return nil, fmt.Errorf("metadata validation failed: %w", err)
	}

	return pm, nil
}

// marshal serializes blockMetadata to bytes.
func (bm *blockMetadata) marshal(dst []byte) []byte {
	dst = bm.seriesID.AppendToBytes(dst)
	dst = encoding.VarUint64ToBytes(dst, uint64(bm.minKey))
	dst = encoding.VarUint64ToBytes(dst, uint64(bm.maxKey))
	dst = encoding.VarUint64ToBytes(dst, bm.dataBlock.offset)
	dst = encoding.VarUint64ToBytes(dst, bm.dataBlock.size)
	dst = encoding.VarUint64ToBytes(dst, bm.keysBlock.offset)
	dst = encoding.VarUint64ToBytes(dst, bm.keysBlock.size)
	dst = append(dst, byte(bm.keysEncodeType))
	dst = encoding.VarUint64ToBytes(dst, bm.count)
	dst = encoding.VarUint64ToBytes(dst, bm.uncompressedSize)
	dst = encoding.VarUint64ToBytes(dst, uint64(len(bm.tagsBlocks)))

	// Write tag blocks in sorted order for consistency
	if len(bm.tagsBlocks) > 0 {
		tagNames := make([]string, 0, len(bm.tagsBlocks))
		for tagName := range bm.tagsBlocks {
			tagNames = append(tagNames, tagName)
		}
		sort.Strings(tagNames)

		for _, tagName := range tagNames {
			block := bm.tagsBlocks[tagName]
			dst = encoding.EncodeBytes(dst, []byte(tagName))
			dst = encoding.VarUint64ToBytes(dst, block.offset)
			dst = encoding.VarUint64ToBytes(dst, block.size)
		}
	}

	return dst
}

// unmarshal deserializes blockMetadata from bytes.
func (bm *blockMetadata) unmarshal(src []byte) ([]byte, error) {
	if len(src) < 8 {
		return nil, fmt.Errorf("cannot unmarshal blockMetadata from less than 8 bytes")
	}
	bm.seriesID = common.SeriesID(encoding.BytesToUint64(src))
	src = src[8:]

	var n uint64
	src, n = encoding.BytesToVarUint64(src)
	bm.minKey = int64(n)

	src, n = encoding.BytesToVarUint64(src)
	bm.maxKey = int64(n)

	src, n = encoding.BytesToVarUint64(src)
	bm.dataBlock.offset = n

	src, n = encoding.BytesToVarUint64(src)
	bm.dataBlock.size = n

	src, n = encoding.BytesToVarUint64(src)
	bm.keysBlock.offset = n

	src, n = encoding.BytesToVarUint64(src)
	bm.keysBlock.size = n

	if len(src) < 1 {
		return nil, fmt.Errorf("not enough bytes to read keysEncodeType")
	}
	bm.keysEncodeType = encoding.EncodeType(src[0])
	src = src[1:]

	src, n = encoding.BytesToVarUint64(src)
	bm.count = n

	src, n = encoding.BytesToVarUint64(src)
	bm.uncompressedSize = n

	src, n = encoding.BytesToVarUint64(src)
	tagBlocksCount := n

	if tagBlocksCount > 0 {
		if bm.tagsBlocks == nil {
			bm.tagsBlocks = make(map[string]dataBlock, tagBlocksCount)
		}
		var nameBytes []byte
		var err error
		for i := uint64(0); i < tagBlocksCount; i++ {
			src, nameBytes, err = encoding.DecodeBytes(src)
			if err != nil {
				return nil, fmt.Errorf("cannot unmarshal tag name: %w", err)
			}
			tagName := string(nameBytes)

			src, n = encoding.BytesToVarUint64(src)
			offset := n

			src, n = encoding.BytesToVarUint64(src)
			size := n

			bm.tagsBlocks[tagName] = dataBlock{
				offset: offset,
				size:   size,
			}
		}
	}

	return src, nil
}

// unmarshalBlockMetadata deserializes multiple blockMetadata from bytes.
func unmarshalBlockMetadata(dst []blockMetadata, src []byte) ([]blockMetadata, error) {
	dstOrig := dst
	for len(src) > 0 {
		if len(dst) < cap(dst) {
			dst = dst[:len(dst)+1]
		} else {
			dst = append(dst, blockMetadata{})
		}
		bm := &dst[len(dst)-1]
		tail, err := bm.unmarshal(src)
		if err != nil {
			return dstOrig, fmt.Errorf("cannot unmarshal blockMetadata entries: %w", err)
		}
		src = tail
	}
	return dst, nil
}

// SeriesID returns the seriesID of the block.
func (bm *blockMetadata) SeriesID() common.SeriesID {
	return bm.seriesID
}

// MinKey returns the minimum user key in the block.
func (bm *blockMetadata) MinKey() int64 {
	return bm.minKey
}

// MaxKey returns the maximum user key in the block.
func (bm *blockMetadata) MaxKey() int64 {
	return bm.maxKey
}

// DataBlock returns the data block reference.
func (bm *blockMetadata) DataBlock() dataBlock {
	return bm.dataBlock
}

// KeysBlock returns the keys block reference.
func (bm *blockMetadata) KeysBlock() dataBlock {
	return bm.keysBlock
}

// TagsBlocks returns the tag blocks references.
func (bm *blockMetadata) TagsBlocks() map[string]dataBlock {
	return bm.tagsBlocks
}

// setSeriesID sets the seriesID of the block.
func (bm *blockMetadata) setSeriesID(seriesID common.SeriesID) {
	bm.seriesID = seriesID
}

// setKeyRange sets the key range of the block.
func (bm *blockMetadata) setKeyRange(minKey, maxKey int64) {
	bm.minKey = minKey
	bm.maxKey = maxKey
}

// setDataBlock sets the data block reference.
func (bm *blockMetadata) setDataBlock(offset, size uint64) {
	bm.dataBlock = dataBlock{offset: offset, size: size}
}

// setKeysBlock sets the keys block reference.
func (bm *blockMetadata) setKeysBlock(offset, size uint64) {
	bm.keysBlock = dataBlock{offset: offset, size: size}
}

// addTagBlock adds a tag block reference.
func (bm *blockMetadata) addTagBlock(tagName string, offset, size uint64) {
	bm.tagsBlocks[tagName] = dataBlock{offset: offset, size: size}
}

// setTagMetadata sets the tag metadata reference in the block metadata.
func (bm *blockMetadata) setTagMetadata(tagName string, offset, size uint64) {
	bm.tagsBlocks[tagName] = dataBlock{offset: offset, size: size}
}

func (bm *blockMetadata) less(other *blockMetadata) bool {
	if bm.seriesID == other.seriesID {
		return bm.minKey < other.minKey
	}
	return bm.seriesID < other.seriesID
}
