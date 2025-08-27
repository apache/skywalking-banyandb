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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
)

func TestPartMetadata_Validation(t *testing.T) {
	tests := []struct {
		metadata  *partMetadata
		name      string
		errMsg    string
		expectErr bool
	}{
		{
			name: "valid metadata",
			metadata: &partMetadata{
				CompressedSizeBytes:   100,
				UncompressedSizeBytes: 200,
				TotalCount:            10,
				BlocksCount:           2,
				MinKey:                1,
				MaxKey:                100,
				ID:                    1,
			},
			expectErr: false,
		},
		{
			name: "invalid key range - MinKey > MaxKey",
			metadata: &partMetadata{
				CompressedSizeBytes:   100,
				UncompressedSizeBytes: 200,
				TotalCount:            10,
				BlocksCount:           2,
				MinKey:                100,
				MaxKey:                1,
				ID:                    1,
			},
			expectErr: true,
			errMsg:    "invalid key range",
		},
		{
			name: "invalid size - compressed > uncompressed",
			metadata: &partMetadata{
				CompressedSizeBytes:   300,
				UncompressedSizeBytes: 200,
				TotalCount:            10,
				BlocksCount:           2,
				MinKey:                1,
				MaxKey:                100,
				ID:                    1,
			},
			expectErr: true,
			errMsg:    "invalid size",
		},
		{
			name: "invalid counts - no blocks but has elements",
			metadata: &partMetadata{
				CompressedSizeBytes:   100,
				UncompressedSizeBytes: 200,
				TotalCount:            10,
				BlocksCount:           0,
				MinKey:                1,
				MaxKey:                100,
				ID:                    1,
			},
			expectErr: true,
			errMsg:    "invalid counts",
		},
		{
			name: "equal min and max keys",
			metadata: &partMetadata{
				CompressedSizeBytes:   100,
				UncompressedSizeBytes: 200,
				TotalCount:            1,
				BlocksCount:           1,
				MinKey:                50,
				MaxKey:                50,
				ID:                    1,
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.metadata.validate()
			if tt.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBlockMetadata_Validation(t *testing.T) {
	tests := []struct {
		metadata  *blockMetadata
		name      string
		errMsg    string
		expectErr bool
	}{
		{
			name: "valid block metadata",
			metadata: &blockMetadata{
				seriesID:   1,
				minKey:     1,
				maxKey:     100,
				dataBlock:  dataBlock{offset: 0, size: 1024},
				keysBlock:  dataBlock{offset: 1024, size: 256},
				tagsBlocks: map[string]dataBlock{"tag1": {offset: 1280, size: 512}},
			},
			expectErr: false,
		},
		{
			name: "invalid key range - minKey > maxKey",
			metadata: &blockMetadata{
				seriesID:   1,
				minKey:     100,
				maxKey:     1,
				dataBlock:  dataBlock{offset: 0, size: 1024},
				keysBlock:  dataBlock{offset: 1024, size: 256},
				tagsBlocks: map[string]dataBlock{},
			},
			expectErr: true,
			errMsg:    "invalid block key range",
		},
		{
			name: "invalid seriesID - zero",
			metadata: &blockMetadata{
				seriesID:   0,
				minKey:     1,
				maxKey:     100,
				dataBlock:  dataBlock{offset: 0, size: 1024},
				keysBlock:  dataBlock{offset: 1024, size: 256},
				tagsBlocks: map[string]dataBlock{},
			},
			expectErr: true,
			errMsg:    "invalid seriesID",
		},
		{
			name: "invalid data block - zero size",
			metadata: &blockMetadata{
				seriesID:   1,
				minKey:     1,
				maxKey:     100,
				dataBlock:  dataBlock{offset: 0, size: 0},
				keysBlock:  dataBlock{offset: 1024, size: 256},
				tagsBlocks: map[string]dataBlock{},
			},
			expectErr: true,
			errMsg:    "invalid data block",
		},
		{
			name: "invalid keys block - zero size",
			metadata: &blockMetadata{
				seriesID:   1,
				minKey:     1,
				maxKey:     100,
				dataBlock:  dataBlock{offset: 0, size: 1024},
				keysBlock:  dataBlock{offset: 1024, size: 0},
				tagsBlocks: map[string]dataBlock{},
			},
			expectErr: true,
			errMsg:    "invalid keys block",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.metadata.validate()
			if tt.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateBlockMetadata(t *testing.T) {
	tests := []struct {
		name      string
		errMsg    string
		blocks    []blockMetadata
		expectErr bool
	}{
		{
			name:      "empty blocks",
			blocks:    []blockMetadata{},
			expectErr: false,
		},
		{
			name: "single valid block",
			blocks: []blockMetadata{
				{
					seriesID:   1,
					minKey:     1,
					maxKey:     100,
					dataBlock:  dataBlock{offset: 0, size: 1024},
					keysBlock:  dataBlock{offset: 1024, size: 256},
					tagsBlocks: map[string]dataBlock{},
				},
			},
			expectErr: false,
		},
		{
			name: "properly ordered blocks by seriesID",
			blocks: []blockMetadata{
				{
					seriesID:   1,
					minKey:     1,
					maxKey:     100,
					dataBlock:  dataBlock{offset: 0, size: 1024},
					keysBlock:  dataBlock{offset: 1024, size: 256},
					tagsBlocks: map[string]dataBlock{},
				},
				{
					seriesID:   2,
					minKey:     1,
					maxKey:     50,
					dataBlock:  dataBlock{offset: 1280, size: 1024},
					keysBlock:  dataBlock{offset: 2304, size: 256},
					tagsBlocks: map[string]dataBlock{},
				},
			},
			expectErr: false,
		},
		{
			name: "properly ordered blocks by key within same seriesID",
			blocks: []blockMetadata{
				{
					seriesID:   1,
					minKey:     1,
					maxKey:     50,
					dataBlock:  dataBlock{offset: 0, size: 1024},
					keysBlock:  dataBlock{offset: 1024, size: 256},
					tagsBlocks: map[string]dataBlock{},
				},
				{
					seriesID:   1,
					minKey:     51,
					maxKey:     100,
					dataBlock:  dataBlock{offset: 1280, size: 1024},
					keysBlock:  dataBlock{offset: 2304, size: 256},
					tagsBlocks: map[string]dataBlock{},
				},
			},
			expectErr: false,
		},
		{
			name: "improperly ordered blocks by seriesID",
			blocks: []blockMetadata{
				{
					seriesID:   2,
					minKey:     1,
					maxKey:     100,
					dataBlock:  dataBlock{offset: 0, size: 1024},
					keysBlock:  dataBlock{offset: 1024, size: 256},
					tagsBlocks: map[string]dataBlock{},
				},
				{
					seriesID:   1,
					minKey:     1,
					maxKey:     50,
					dataBlock:  dataBlock{offset: 1280, size: 1024},
					keysBlock:  dataBlock{offset: 2304, size: 256},
					tagsBlocks: map[string]dataBlock{},
				},
			},
			expectErr: true,
			errMsg:    "blocks not ordered by seriesID",
		},
		{
			name: "improperly ordered blocks by key within same seriesID",
			blocks: []blockMetadata{
				{
					seriesID:   1,
					minKey:     51,
					maxKey:     100,
					dataBlock:  dataBlock{offset: 0, size: 1024},
					keysBlock:  dataBlock{offset: 1024, size: 256},
					tagsBlocks: map[string]dataBlock{},
				},
				{
					seriesID:   1,
					minKey:     1,
					maxKey:     50,
					dataBlock:  dataBlock{offset: 1280, size: 1024},
					keysBlock:  dataBlock{offset: 2304, size: 256},
					tagsBlocks: map[string]dataBlock{},
				},
			},
			expectErr: true,
			errMsg:    "blocks not ordered by key",
		},
		{
			name: "overlapping key ranges within same seriesID",
			blocks: []blockMetadata{
				{
					seriesID:   1,
					minKey:     1,
					maxKey:     50,
					dataBlock:  dataBlock{offset: 0, size: 1024},
					keysBlock:  dataBlock{offset: 1024, size: 256},
					tagsBlocks: map[string]dataBlock{},
				},
				{
					seriesID:   1,
					minKey:     40,
					maxKey:     80,
					dataBlock:  dataBlock{offset: 1280, size: 1024},
					keysBlock:  dataBlock{offset: 2304, size: 256},
					tagsBlocks: map[string]dataBlock{},
				},
			},
			expectErr: true,
			errMsg:    "overlapping key ranges",
		},
		{
			name: "adjacent key ranges within same seriesID (valid)",
			blocks: []blockMetadata{
				{
					seriesID:   1,
					minKey:     1,
					maxKey:     50,
					dataBlock:  dataBlock{offset: 0, size: 1024},
					keysBlock:  dataBlock{offset: 1024, size: 256},
					tagsBlocks: map[string]dataBlock{},
				},
				{
					seriesID:   1,
					minKey:     51,
					maxKey:     100,
					dataBlock:  dataBlock{offset: 1280, size: 1024},
					keysBlock:  dataBlock{offset: 2304, size: 256},
					tagsBlocks: map[string]dataBlock{},
				},
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBlockMetadata(tt.blocks)
			if tt.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
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

func TestPartMetadata_Serialization(t *testing.T) {
	original := &partMetadata{
		CompressedSizeBytes:   1000,
		UncompressedSizeBytes: 2000,
		TotalCount:            50,
		BlocksCount:           5,
		MinKey:                10,
		MaxKey:                1000,
		ID:                    12345,
	}

	// Test marshaling
	data, err := original.marshal()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Test unmarshaling
	restored, err := unmarshalPartMetadata(data)
	require.NoError(t, err)
	defer releasePartMetadata(restored)

	// Verify all fields match
	assert.Equal(t, original.CompressedSizeBytes, restored.CompressedSizeBytes)
	assert.Equal(t, original.UncompressedSizeBytes, restored.UncompressedSizeBytes)
	assert.Equal(t, original.TotalCount, restored.TotalCount)
	assert.Equal(t, original.BlocksCount, restored.BlocksCount)
	assert.Equal(t, original.MinKey, restored.MinKey)
	assert.Equal(t, original.MaxKey, restored.MaxKey)
	assert.Equal(t, original.ID, restored.ID)
}

func TestBlockMetadata_Serialization(t *testing.T) {
	original := &blockMetadata{
		seriesID:  common.SeriesID(123),
		minKey:    10,
		maxKey:    100,
		dataBlock: dataBlock{offset: 1000, size: 2048},
		keysBlock: dataBlock{offset: 3048, size: 512},
		tagsBlocks: map[string]dataBlock{
			"service_id": {offset: 3560, size: 256},
			"endpoint":   {offset: 3816, size: 512},
			"status":     {offset: 4328, size: 128},
		},
	}

	// Test marshaling
	data := original.marshal(nil)
	assert.NotEmpty(t, data)

	// Test unmarshaling
	restoredArray, err := unmarshalBlockMetadata(nil, data)
	require.NoError(t, err)
	require.Len(t, restoredArray, 1)
	restored := &restoredArray[0]

	// Verify all fields match
	assert.Equal(t, original.seriesID, restored.seriesID)
	assert.Equal(t, original.minKey, restored.minKey)
	assert.Equal(t, original.maxKey, restored.maxKey)
	assert.Equal(t, original.dataBlock, restored.dataBlock)
	assert.Equal(t, original.keysBlock, restored.keysBlock)
	assert.Equal(t, len(original.tagsBlocks), len(restored.tagsBlocks))

	// Verify tag blocks match
	for tagName, originalBlock := range original.tagsBlocks {
		restoredBlock, exists := restored.tagsBlocks[tagName]
		assert.True(t, exists, "Tag block %s should exist", tagName)
		assert.Equal(t, originalBlock, restoredBlock)
	}
}

func TestPartMetadata_CorruptionDetection(t *testing.T) {
	tests := []struct {
		name        string
		errMsg      string
		corruptData []byte
	}{
		{
			name:        "empty data",
			corruptData: []byte{},
			errMsg:      "empty data provided",
		},
		{
			name:        "invalid JSON",
			corruptData: []byte("{invalid json"),
			errMsg:      "failed to unmarshal partMetadata from JSON",
		},
		{
			name:        "invalid key range in JSON",
			corruptData: []byte(`{"compressedSizeBytes":1000,"uncompressedSizeBytes":2000,"totalCount":50,"blocksCount":5,"minKey":1000,"maxKey":10,"id":12345}`),
			errMsg:      "metadata validation failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := unmarshalPartMetadata(tt.corruptData)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestPartMetadata_JSONFormat(t *testing.T) {
	// Test that the JSON format is human-readable and contains expected fields
	original := &partMetadata{
		CompressedSizeBytes:   1000,
		UncompressedSizeBytes: 2000,
		TotalCount:            50,
		BlocksCount:           5,
		MinKey:                10,
		MaxKey:                1000,
		ID:                    12345,
	}

	data, err := original.marshal()
	require.NoError(t, err)

	// Verify it's valid JSON
	var parsed map[string]interface{}
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)

	// Verify expected fields are present
	assert.Equal(t, float64(1000), parsed["compressedSizeBytes"])
	assert.Equal(t, float64(2000), parsed["uncompressedSizeBytes"])
	assert.Equal(t, float64(50), parsed["totalCount"])
	assert.Equal(t, float64(5), parsed["blocksCount"])
	assert.Equal(t, float64(10), parsed["minKey"])
	assert.Equal(t, float64(1000), parsed["maxKey"])
	assert.Equal(t, float64(12345), parsed["id"])

	// Verify JSON is human-readable (contains field names)
	jsonStr := string(data)
	assert.Contains(t, jsonStr, "compressedSizeBytes")
	assert.Contains(t, jsonStr, "minKey")
	assert.Contains(t, jsonStr, "maxKey")
}

func TestBlockMetadata_AccessorMethods(t *testing.T) {
	bm := &blockMetadata{
		seriesID:  common.SeriesID(123),
		minKey:    10,
		maxKey:    100,
		dataBlock: dataBlock{offset: 1000, size: 2048},
		keysBlock: dataBlock{offset: 3048, size: 512},
		tagsBlocks: map[string]dataBlock{
			"tag1": {offset: 3560, size: 256},
		},
	}

	// Test accessor methods
	assert.Equal(t, common.SeriesID(123), bm.SeriesID())
	assert.Equal(t, int64(10), bm.MinKey())
	assert.Equal(t, int64(100), bm.MaxKey())
	assert.Equal(t, dataBlock{offset: 1000, size: 2048}, bm.DataBlock())
	assert.Equal(t, dataBlock{offset: 3048, size: 512}, bm.KeysBlock())
	assert.Equal(t, map[string]dataBlock{"tag1": {offset: 3560, size: 256}}, bm.TagsBlocks())
}

func TestBlockMetadata_SetterMethods(t *testing.T) {
	bm := generateBlockMetadata()
	defer releaseBlockMetadata(bm)

	// Test setter methods
	bm.setSeriesID(common.SeriesID(456))
	assert.Equal(t, common.SeriesID(456), bm.seriesID)

	bm.setKeyRange(20, 200)
	assert.Equal(t, int64(20), bm.minKey)
	assert.Equal(t, int64(200), bm.maxKey)

	bm.setDataBlock(2000, 4096)
	assert.Equal(t, dataBlock{offset: 2000, size: 4096}, bm.dataBlock)

	bm.setKeysBlock(6096, 1024)
	assert.Equal(t, dataBlock{offset: 6096, size: 1024}, bm.keysBlock)

	bm.addTagBlock("test_tag", 7120, 512)
	expected := dataBlock{offset: 7120, size: 512}
	assert.Equal(t, expected, bm.tagsBlocks["test_tag"])
}

func TestMetadata_Pooling(t *testing.T) {
	// Test partMetadata pooling
	pm1 := generatePartMetadata()
	pm1.ID = 123
	pm1.MinKey = 10
	pm1.MaxKey = 100

	releasePartMetadata(pm1)

	pm2 := generatePartMetadata()
	// pm2 should be the same instance as pm1, but reset
	assert.Equal(t, uint64(0), pm2.ID)
	assert.Equal(t, int64(0), pm2.MinKey)
	assert.Equal(t, int64(0), pm2.MaxKey)

	releasePartMetadata(pm2)

	// Test blockMetadata pooling
	bm1 := generateBlockMetadata()
	bm1.seriesID = 456
	bm1.minKey = 20
	bm1.tagsBlocks["test"] = dataBlock{offset: 100, size: 200}

	releaseBlockMetadata(bm1)

	bm2 := generateBlockMetadata()
	// bm2 should be the same instance as bm1, but reset
	assert.Equal(t, common.SeriesID(0), bm2.seriesID)
	assert.Equal(t, int64(0), bm2.minKey)
	assert.Equal(t, 0, len(bm2.tagsBlocks))

	releaseBlockMetadata(bm2)
}

func TestMetadata_Reset(t *testing.T) {
	// Test partMetadata reset
	pm := &partMetadata{
		CompressedSizeBytes:   1000,
		UncompressedSizeBytes: 2000,
		TotalCount:            50,
		BlocksCount:           5,
		MinKey:                10,
		MaxKey:                1000,
		ID:                    12345,
	}

	pm.reset()

	assert.Equal(t, uint64(0), pm.CompressedSizeBytes)
	assert.Equal(t, uint64(0), pm.UncompressedSizeBytes)
	assert.Equal(t, uint64(0), pm.TotalCount)
	assert.Equal(t, uint64(0), pm.BlocksCount)
	assert.Equal(t, int64(0), pm.MinKey)
	assert.Equal(t, int64(0), pm.MaxKey)
	assert.Equal(t, uint64(0), pm.ID)

	// Test blockMetadata reset
	bm := &blockMetadata{
		seriesID:  123,
		minKey:    10,
		maxKey:    100,
		dataBlock: dataBlock{offset: 1000, size: 2048},
		keysBlock: dataBlock{offset: 3048, size: 512},
		tagsBlocks: map[string]dataBlock{
			"tag1": {offset: 3560, size: 256},
		},
	}

	bm.reset()

	assert.Equal(t, common.SeriesID(0), bm.seriesID)
	assert.Equal(t, int64(0), bm.minKey)
	assert.Equal(t, int64(0), bm.maxKey)
	assert.Equal(t, dataBlock{}, bm.dataBlock)
	assert.Equal(t, dataBlock{}, bm.keysBlock)
	assert.Equal(t, 0, len(bm.tagsBlocks))
}

func TestBlockMetadata_CopyFrom(t *testing.T) {
	tests := []struct {
		name           string
		source         *blockMetadata
		target         *blockMetadata
		expectedResult *blockMetadata
		description    string
	}{
		{
			name: "copy complete metadata",
			source: &blockMetadata{
				seriesID:         common.SeriesID(123),
				minKey:           10,
				maxKey:           100,
				uncompressedSize: 2048,
				count:            50,
				keysEncodeType:   1,
				dataBlock:        dataBlock{offset: 1000, size: 2048},
				keysBlock:        dataBlock{offset: 3048, size: 512},
				tagsBlocks: map[string]dataBlock{
					"service_id": {offset: 3560, size: 256},
					"endpoint":   {offset: 3816, size: 512},
					"status":     {offset: 4328, size: 128},
				},
				tagProjection: []string{"service_id", "endpoint", "status"},
			},
			target: &blockMetadata{},
			expectedResult: &blockMetadata{
				seriesID:         common.SeriesID(123),
				minKey:           10,
				maxKey:           100,
				uncompressedSize: 2048,
				count:            50,
				keysEncodeType:   1,
				dataBlock:        dataBlock{offset: 1000, size: 2048},
				keysBlock:        dataBlock{offset: 3048, size: 512},
				tagsBlocks: map[string]dataBlock{
					"service_id": {offset: 3560, size: 256},
					"endpoint":   {offset: 3816, size: 512},
					"status":     {offset: 4328, size: 128},
				},
				tagProjection: []string{"service_id", "endpoint", "status"},
			},
			description: "should copy all fields from source to target",
		},
		{
			name: "copy to target with existing data",
			source: &blockMetadata{
				seriesID:         common.SeriesID(456),
				minKey:           20,
				maxKey:           200,
				uncompressedSize: 4096,
				count:            100,
				keysEncodeType:   2,
				dataBlock:        dataBlock{offset: 2000, size: 4096},
				keysBlock:        dataBlock{offset: 6096, size: 1024},
				tagsBlocks: map[string]dataBlock{
					"new_tag": {offset: 7120, size: 512},
				},
				tagProjection: []string{"new_tag"},
			},
			target: &blockMetadata{
				seriesID:         common.SeriesID(999),
				minKey:           999,
				maxKey:           9999,
				uncompressedSize: 9999,
				count:            9999,
				keysEncodeType:   99,
				dataBlock:        dataBlock{offset: 9999, size: 9999},
				keysBlock:        dataBlock{offset: 9999, size: 9999},
				tagsBlocks: map[string]dataBlock{
					"old_tag": {offset: 9999, size: 9999},
				},
				tagProjection: []string{"old_tag"},
			},
			expectedResult: &blockMetadata{
				seriesID:         common.SeriesID(456),
				minKey:           20,
				maxKey:           200,
				uncompressedSize: 4096,
				count:            100,
				keysEncodeType:   2,
				dataBlock:        dataBlock{offset: 2000, size: 4096},
				keysBlock:        dataBlock{offset: 6096, size: 1024},
				tagsBlocks: map[string]dataBlock{
					"new_tag": {offset: 7120, size: 512},
				},
				tagProjection: []string{"new_tag"},
			},
			description: "should overwrite all existing data in target",
		},
		{
			name:   "copy empty metadata",
			source: &blockMetadata{},
			target: &blockMetadata{},
			expectedResult: &blockMetadata{
				tagsBlocks:    map[string]dataBlock{},
				tagProjection: []string{},
			},
			description: "should handle empty source metadata",
		},
		{
			name: "copy with nil tagsBlocks in target",
			source: &blockMetadata{
				seriesID:         common.SeriesID(789),
				minKey:           30,
				maxKey:           300,
				uncompressedSize: 1024,
				count:            25,
				keysEncodeType:   0,
				dataBlock:        dataBlock{offset: 3000, size: 1024},
				keysBlock:        dataBlock{offset: 4024, size: 256},
				tagsBlocks: map[string]dataBlock{
					"tag1": {offset: 4280, size: 128},
				},
				tagProjection: []string{"tag1"},
			},
			target: &blockMetadata{
				tagsBlocks: nil, // nil map
			},
			expectedResult: &blockMetadata{
				seriesID:         common.SeriesID(789),
				minKey:           30,
				maxKey:           300,
				uncompressedSize: 1024,
				count:            25,
				keysEncodeType:   0,
				dataBlock:        dataBlock{offset: 3000, size: 1024},
				keysBlock:        dataBlock{offset: 4024, size: 256},
				tagsBlocks: map[string]dataBlock{
					"tag1": {offset: 4280, size: 128},
				},
				tagProjection: []string{"tag1"},
			},
			description: "should initialize nil tagsBlocks map in target",
		},
		{
			name: "copy with empty tagsBlocks in target",
			source: &blockMetadata{
				seriesID:         common.SeriesID(111),
				minKey:           40,
				maxKey:           400,
				uncompressedSize: 512,
				count:            10,
				keysEncodeType:   3,
				dataBlock:        dataBlock{offset: 4000, size: 512},
				keysBlock:        dataBlock{offset: 4512, size: 128},
				tagsBlocks:       map[string]dataBlock{},
				tagProjection:    []string{},
			},
			target: &blockMetadata{
				tagsBlocks: map[string]dataBlock{
					"old_tag1": {offset: 9999, size: 9999},
					"old_tag2": {offset: 9999, size: 9999},
				},
				tagProjection: []string{"old_tag1", "old_tag2"},
			},
			expectedResult: &blockMetadata{
				seriesID:         common.SeriesID(111),
				minKey:           40,
				maxKey:           400,
				uncompressedSize: 512,
				count:            10,
				keysEncodeType:   3,
				dataBlock:        dataBlock{offset: 4000, size: 512},
				keysBlock:        dataBlock{offset: 4512, size: 128},
				tagsBlocks:       map[string]dataBlock{},
				tagProjection:    []string{},
			},
			description: "should clear existing tagsBlocks and tagProjection in target",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a copy of the target to avoid modifying the original test data
			targetCopy := &blockMetadata{}
			if tt.target.tagsBlocks != nil {
				targetCopy.tagsBlocks = make(map[string]dataBlock)
				for k, v := range tt.target.tagsBlocks {
					targetCopy.tagsBlocks[k] = v
				}
			}
			if tt.target.tagProjection != nil {
				targetCopy.tagProjection = make([]string, len(tt.target.tagProjection))
				copy(targetCopy.tagProjection, tt.target.tagProjection)
			}
			targetCopy.seriesID = tt.target.seriesID
			targetCopy.minKey = tt.target.minKey
			targetCopy.maxKey = tt.target.maxKey
			targetCopy.uncompressedSize = tt.target.uncompressedSize
			targetCopy.count = tt.target.count
			targetCopy.keysEncodeType = tt.target.keysEncodeType
			targetCopy.dataBlock = tt.target.dataBlock
			targetCopy.keysBlock = tt.target.keysBlock

			// Execute copyFrom
			targetCopy.copyFrom(tt.source)

			// Verify all fields are copied correctly
			assert.Equal(t, tt.expectedResult.seriesID, targetCopy.seriesID, "seriesID mismatch")
			assert.Equal(t, tt.expectedResult.minKey, targetCopy.minKey, "minKey mismatch")
			assert.Equal(t, tt.expectedResult.maxKey, targetCopy.maxKey, "maxKey mismatch")
			assert.Equal(t, tt.expectedResult.uncompressedSize, targetCopy.uncompressedSize, "uncompressedSize mismatch")
			assert.Equal(t, tt.expectedResult.count, targetCopy.count, "count mismatch")
			assert.Equal(t, tt.expectedResult.keysEncodeType, targetCopy.keysEncodeType, "keysEncodeType mismatch")
			assert.Equal(t, tt.expectedResult.dataBlock, targetCopy.dataBlock, "dataBlock mismatch")
			assert.Equal(t, tt.expectedResult.keysBlock, targetCopy.keysBlock, "keysBlock mismatch")

			// Verify tagsBlocks
			if tt.expectedResult.tagsBlocks == nil {
				assert.Nil(t, targetCopy.tagsBlocks, "tagsBlocks should be nil")
			} else {
				assert.NotNil(t, targetCopy.tagsBlocks, "tagsBlocks should not be nil")
				assert.Equal(t, len(tt.expectedResult.tagsBlocks), len(targetCopy.tagsBlocks), "tagsBlocks length mismatch")
				for k, v := range tt.expectedResult.tagsBlocks {
					assert.Equal(t, v, targetCopy.tagsBlocks[k], "tagsBlocks[%s] mismatch", k)
				}
			}

			// Verify tagProjection
			assert.Equal(t, len(tt.expectedResult.tagProjection), len(targetCopy.tagProjection), "tagProjection length mismatch")
			for i, v := range tt.expectedResult.tagProjection {
				assert.Equal(t, v, targetCopy.tagProjection[i], "tagProjection[%d] mismatch", i)
			}
		})
	}
}

func TestBlockMetadata_CopyFrom_DeepCopy(t *testing.T) {
	// Test that copyFrom creates a deep copy, not just references
	source := &blockMetadata{
		seriesID:         common.SeriesID(123),
		minKey:           10,
		maxKey:           100,
		uncompressedSize: 2048,
		count:            50,
		keysEncodeType:   1,
		dataBlock:        dataBlock{offset: 1000, size: 2048},
		keysBlock:        dataBlock{offset: 3048, size: 512},
		tagsBlocks: map[string]dataBlock{
			"tag1": {offset: 3560, size: 256},
		},
		tagProjection: []string{"tag1"},
	}

	target := &blockMetadata{}
	target.copyFrom(source)

	// Verify it's a deep copy by modifying the source
	source.seriesID = common.SeriesID(999)
	source.minKey = 999
	source.maxKey = 9999
	source.uncompressedSize = 9999
	source.count = 9999
	source.keysEncodeType = 99
	source.dataBlock = dataBlock{offset: 9999, size: 9999}
	source.keysBlock = dataBlock{offset: 9999, size: 9999}
	source.tagsBlocks["tag1"] = dataBlock{offset: 9999, size: 9999}
	source.tagProjection[0] = "modified_tag"

	// Target should remain unchanged
	assert.Equal(t, common.SeriesID(123), target.seriesID, "target seriesID should not change when source is modified")
	assert.Equal(t, int64(10), target.minKey, "target minKey should not change when source is modified")
	assert.Equal(t, int64(100), target.maxKey, "target maxKey should not change when source is modified")
	assert.Equal(t, uint64(2048), target.uncompressedSize, "target uncompressedSize should not change when source is modified")
	assert.Equal(t, uint64(50), target.count, "target count should not change when source is modified")
	assert.Equal(t, encoding.EncodeType(1), target.keysEncodeType, "target keysEncodeType should not change when source is modified")
	assert.Equal(t, dataBlock{offset: 1000, size: 2048}, target.dataBlock, "target dataBlock should not change when source is modified")
	assert.Equal(t, dataBlock{offset: 3048, size: 512}, target.keysBlock, "target keysBlock should not change when source is modified")
	assert.Equal(t, dataBlock{offset: 3560, size: 256}, target.tagsBlocks["tag1"], "target tagsBlocks should not change when source is modified")
	assert.Equal(t, "tag1", target.tagProjection[0], "target tagProjection should not change when source is modified")
}

func TestBlockMetadata_CopyFrom_EdgeCases(t *testing.T) {
	t.Run("copy from nil source", func(t *testing.T) {
		target := &blockMetadata{
			seriesID: 123,
			minKey:   10,
			maxKey:   100,
		}

		// This should not panic and should leave target unchanged
		assert.NotPanics(t, func() {
			target.copyFrom(nil)
		})

		// Target should remain unchanged
		assert.Equal(t, common.SeriesID(123), target.seriesID)
		assert.Equal(t, int64(10), target.minKey)
		assert.Equal(t, int64(100), target.maxKey)
	})

	t.Run("copy to nil target", func(t *testing.T) {
		source := &blockMetadata{
			seriesID: 456,
			minKey:   20,
			maxKey:   200,
		}

		// This should panic
		assert.Panics(t, func() {
			var target *blockMetadata
			target.copyFrom(source)
		})
	})

	t.Run("copy with very large values", func(t *testing.T) {
		source := &blockMetadata{
			seriesID:         common.SeriesID(^uint64(0)),    // Max uint64
			minKey:           ^int64(0),                      // Min int64
			maxKey:           ^int64(0) >> 1,                 // Max int64
			uncompressedSize: ^uint64(0),                     // Max uint64
			count:            ^uint64(0),                     // Max uint64
			keysEncodeType:   encoding.EncodeType(^uint8(0)), // Max uint8
			dataBlock:        dataBlock{offset: ^uint64(0), size: ^uint64(0)},
			keysBlock:        dataBlock{offset: ^uint64(0), size: ^uint64(0)},
			tagsBlocks: map[string]dataBlock{
				"large_tag": {offset: ^uint64(0), size: ^uint64(0)},
			},
			tagProjection: []string{"large_tag"},
		}

		target := &blockMetadata{}
		target.copyFrom(source)

		// Verify all large values are copied correctly
		assert.Equal(t, source.seriesID, target.seriesID)
		assert.Equal(t, source.minKey, target.minKey)
		assert.Equal(t, source.maxKey, target.maxKey)
		assert.Equal(t, source.uncompressedSize, target.uncompressedSize)
		assert.Equal(t, source.count, target.count)
		assert.Equal(t, source.keysEncodeType, target.keysEncodeType)
		assert.Equal(t, source.dataBlock, target.dataBlock)
		assert.Equal(t, source.keysBlock, target.keysBlock)
		assert.Equal(t, source.tagsBlocks, target.tagsBlocks)
		assert.Equal(t, source.tagProjection, target.tagProjection)
	})
}
