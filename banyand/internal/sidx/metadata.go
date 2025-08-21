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
	"encoding/json"
	"fmt"
	"io"

	"github.com/apache/skywalking-banyandb/api/common"
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
}

var (
	partMetadataPool  = pool.Register[*partMetadata]("sidx-partMetadata")
	blockMetadataPool = pool.Register[*blockMetadata]("sidx-blockMetadata")
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

	// Clear maps but keep them allocated
	for k := range bm.tagsBlocks {
		delete(bm.tagsBlocks, k)
	}
	bm.tagProjection = bm.tagProjection[:0]
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
	if bm.keysBlock.size == 0 {
		return fmt.Errorf("invalid keys block: size cannot be zero")
	}
	return nil
}

// validateBlockMetadata validates ordering of blocks within a part.
func validateBlockMetadata(blocks []blockMetadata) error {
	if len(blocks) == 0 {
		return nil
	}

	for i := 1; i < len(blocks); i++ {
		prev := &blocks[i-1]
		curr := &blocks[i]

		// Validate individual blocks
		if err := prev.validate(); err != nil {
			return fmt.Errorf("block %d validation failed: %w", i-1, err)
		}

		// Check ordering: seriesID first, then minKey
		if curr.seriesID < prev.seriesID {
			return fmt.Errorf("blocks not ordered by seriesID: block %d seriesID (%d) < block %d seriesID (%d)",
				i, curr.seriesID, i-1, prev.seriesID)
		}

		// For same seriesID, check key ordering
		if curr.seriesID == prev.seriesID && curr.minKey < prev.minKey {
			return fmt.Errorf("blocks not ordered by key: block %d minKey (%d) < block %d minKey (%d) for seriesID %d",
				i, curr.minKey, i-1, prev.minKey, curr.seriesID)
		}

		// Check for overlapping key ranges within same seriesID
		if curr.seriesID == prev.seriesID && curr.minKey <= prev.maxKey {
			return fmt.Errorf("overlapping key ranges: block %d [%d, %d] overlaps with block %d [%d, %d] for seriesID %d",
				i, curr.minKey, curr.maxKey, i-1, prev.minKey, prev.maxKey, curr.seriesID)
		}
	}

	// Validate the last block
	if err := blocks[len(blocks)-1].validate(); err != nil {
		return fmt.Errorf("block %d validation failed: %w", len(blocks)-1, err)
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
func (bm *blockMetadata) marshal() ([]byte, error) {
	buf := &bytes.Buffer{}

	// Write seriesID
	if err := binary.Write(buf, binary.LittleEndian, bm.seriesID); err != nil {
		return nil, fmt.Errorf("failed to write seriesID: %w", err)
	}

	// Write key range
	if err := binary.Write(buf, binary.LittleEndian, bm.minKey); err != nil {
		return nil, fmt.Errorf("failed to write minKey: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, bm.maxKey); err != nil {
		return nil, fmt.Errorf("failed to write maxKey: %w", err)
	}

	// Write data block
	if err := binary.Write(buf, binary.LittleEndian, bm.dataBlock.offset); err != nil {
		return nil, fmt.Errorf("failed to write data block offset: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, bm.dataBlock.size); err != nil {
		return nil, fmt.Errorf("failed to write data block size: %w", err)
	}

	// Write keys block
	if err := binary.Write(buf, binary.LittleEndian, bm.keysBlock.offset); err != nil {
		return nil, fmt.Errorf("failed to write keys block offset: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, bm.keysBlock.size); err != nil {
		return nil, fmt.Errorf("failed to write keys block size: %w", err)
	}

	// Write tag blocks count
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(bm.tagsBlocks))); err != nil {
		return nil, fmt.Errorf("failed to write tag blocks count: %w", err)
	}

	// Write tag blocks
	for tagName, tagBlock := range bm.tagsBlocks {
		// Write tag name length and name
		nameBytes := []byte(tagName)
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(nameBytes))); err != nil {
			return nil, fmt.Errorf("failed to write tag name length: %w", err)
		}
		if _, err := buf.Write(nameBytes); err != nil {
			return nil, fmt.Errorf("failed to write tag name: %w", err)
		}

		// Write tag block
		if err := binary.Write(buf, binary.LittleEndian, tagBlock.offset); err != nil {
			return nil, fmt.Errorf("failed to write tag block offset: %w", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, tagBlock.size); err != nil {
			return nil, fmt.Errorf("failed to write tag block size: %w", err)
		}
	}

	return buf.Bytes(), nil
}

// unmarshalBlockMetadata deserializes blockMetadata from bytes.
func unmarshalBlockMetadata(data []byte) (*blockMetadata, error) {
	bm := generateBlockMetadata()
	buf := bytes.NewReader(data)

	// Read seriesID
	if err := binary.Read(buf, binary.LittleEndian, &bm.seriesID); err != nil {
		releaseBlockMetadata(bm)
		return nil, fmt.Errorf("failed to read seriesID: %w", err)
	}

	// Read key range
	if err := binary.Read(buf, binary.LittleEndian, &bm.minKey); err != nil {
		releaseBlockMetadata(bm)
		return nil, fmt.Errorf("failed to read minKey: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &bm.maxKey); err != nil {
		releaseBlockMetadata(bm)
		return nil, fmt.Errorf("failed to read maxKey: %w", err)
	}

	// Read data block
	if err := binary.Read(buf, binary.LittleEndian, &bm.dataBlock.offset); err != nil {
		releaseBlockMetadata(bm)
		return nil, fmt.Errorf("failed to read data block offset: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &bm.dataBlock.size); err != nil {
		releaseBlockMetadata(bm)
		return nil, fmt.Errorf("failed to read data block size: %w", err)
	}

	// Read keys block
	if err := binary.Read(buf, binary.LittleEndian, &bm.keysBlock.offset); err != nil {
		releaseBlockMetadata(bm)
		return nil, fmt.Errorf("failed to read keys block offset: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &bm.keysBlock.size); err != nil {
		releaseBlockMetadata(bm)
		return nil, fmt.Errorf("failed to read keys block size: %w", err)
	}

	// Read tag blocks count
	var tagBlocksCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &tagBlocksCount); err != nil {
		releaseBlockMetadata(bm)
		return nil, fmt.Errorf("failed to read tag blocks count: %w", err)
	}

	// Read tag blocks
	for i := uint32(0); i < tagBlocksCount; i++ {
		// Read tag name
		var nameLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &nameLen); err != nil {
			releaseBlockMetadata(bm)
			return nil, fmt.Errorf("failed to read tag name length: %w", err)
		}
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(buf, nameBytes); err != nil {
			releaseBlockMetadata(bm)
			return nil, fmt.Errorf("failed to read tag name: %w", err)
		}
		tagName := string(nameBytes)

		// Read tag block
		var tagBlock dataBlock
		if err := binary.Read(buf, binary.LittleEndian, &tagBlock.offset); err != nil {
			releaseBlockMetadata(bm)
			return nil, fmt.Errorf("failed to read tag block offset: %w", err)
		}
		if err := binary.Read(buf, binary.LittleEndian, &tagBlock.size); err != nil {
			releaseBlockMetadata(bm)
			return nil, fmt.Errorf("failed to read tag block size: %w", err)
		}

		bm.tagsBlocks[tagName] = tagBlock
	}

	// Validate the metadata
	if err := bm.validate(); err != nil {
		releaseBlockMetadata(bm)
		return nil, fmt.Errorf("block metadata validation failed: %w", err)
	}

	return bm, nil
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

// getTagMetadata gets or creates a tag metadata reference in the block metadata.
func (bm *blockMetadata) getTagMetadata(tagName string) *dataBlock {
	if _, exists := bm.tagsBlocks[tagName]; !exists {
		bm.tagsBlocks[tagName] = dataBlock{}
	}
	// Return pointer to the dataBlock for the tag
	block := bm.tagsBlocks[tagName]
	return &block
}
