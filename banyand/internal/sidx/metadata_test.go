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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
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

func TestPrimaryBlockMetadata_Validation(t *testing.T) {
	tests := []struct {
		metadata  *primaryBlockMetadata
		name      string
		errMsg    string
		expectErr bool
	}{
		{
			name: "valid block metadata",
			metadata: &primaryBlockMetadata{
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
			metadata: &primaryBlockMetadata{
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
			metadata: &primaryBlockMetadata{
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
			metadata: &primaryBlockMetadata{
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
			metadata: &primaryBlockMetadata{
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

func TestValidatePrimaryBlockMetadata(t *testing.T) {
	tests := []struct {
		name      string
		errMsg    string
		blocks    []primaryBlockMetadata
		expectErr bool
	}{
		{
			name:      "empty blocks",
			blocks:    []primaryBlockMetadata{},
			expectErr: false,
		},
		{
			name: "single valid block",
			blocks: []primaryBlockMetadata{
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
			blocks: []primaryBlockMetadata{
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
			blocks: []primaryBlockMetadata{
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
			blocks: []primaryBlockMetadata{
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
			blocks: []primaryBlockMetadata{
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
			blocks: []primaryBlockMetadata{
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
			blocks: []primaryBlockMetadata{
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
			err := validatePrimaryBlockMetadata(tt.blocks)
			if tt.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
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

func TestPrimaryBlockMetadata_Serialization(t *testing.T) {
	original := &primaryBlockMetadata{
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
	data, err := original.marshal()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Test unmarshaling
	restored, err := unmarshalPrimaryBlockMetadata(data)
	require.NoError(t, err)
	defer releasePrimaryBlockMetadata(restored)

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

func TestPrimaryBlockMetadata_AccessorMethods(t *testing.T) {
	pbm := &primaryBlockMetadata{
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
	assert.Equal(t, common.SeriesID(123), pbm.SeriesID())
	assert.Equal(t, int64(10), pbm.MinKey())
	assert.Equal(t, int64(100), pbm.MaxKey())
	assert.Equal(t, dataBlock{offset: 1000, size: 2048}, pbm.DataBlock())
	assert.Equal(t, dataBlock{offset: 3048, size: 512}, pbm.KeysBlock())
	assert.Equal(t, map[string]dataBlock{"tag1": {offset: 3560, size: 256}}, pbm.TagsBlocks())
}

func TestPrimaryBlockMetadata_SetterMethods(t *testing.T) {
	pbm := generatePrimaryBlockMetadata()
	defer releasePrimaryBlockMetadata(pbm)

	// Test setter methods
	pbm.setSeriesID(common.SeriesID(456))
	assert.Equal(t, common.SeriesID(456), pbm.seriesID)

	pbm.setKeyRange(20, 200)
	assert.Equal(t, int64(20), pbm.minKey)
	assert.Equal(t, int64(200), pbm.maxKey)

	pbm.setDataBlock(2000, 4096)
	assert.Equal(t, dataBlock{offset: 2000, size: 4096}, pbm.dataBlock)

	pbm.setKeysBlock(6096, 1024)
	assert.Equal(t, dataBlock{offset: 6096, size: 1024}, pbm.keysBlock)

	pbm.addTagBlock("test_tag", 7120, 512)
	expected := dataBlock{offset: 7120, size: 512}
	assert.Equal(t, expected, pbm.tagsBlocks["test_tag"])
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

	// Test primaryBlockMetadata pooling
	pbm1 := generatePrimaryBlockMetadata()
	pbm1.seriesID = 456
	pbm1.minKey = 20
	pbm1.tagsBlocks["test"] = dataBlock{offset: 100, size: 200}

	releasePrimaryBlockMetadata(pbm1)

	pbm2 := generatePrimaryBlockMetadata()
	// pbm2 should be the same instance as pbm1, but reset
	assert.Equal(t, common.SeriesID(0), pbm2.seriesID)
	assert.Equal(t, int64(0), pbm2.minKey)
	assert.Equal(t, 0, len(pbm2.tagsBlocks))

	releasePrimaryBlockMetadata(pbm2)
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

	// Test primaryBlockMetadata reset
	pbm := &primaryBlockMetadata{
		seriesID:  123,
		minKey:    10,
		maxKey:    100,
		dataBlock: dataBlock{offset: 1000, size: 2048},
		keysBlock: dataBlock{offset: 3048, size: 512},
		tagsBlocks: map[string]dataBlock{
			"tag1": {offset: 3560, size: 256},
		},
	}

	pbm.reset()

	assert.Equal(t, common.SeriesID(0), pbm.seriesID)
	assert.Equal(t, int64(0), pbm.minKey)
	assert.Equal(t, int64(0), pbm.maxKey)
	assert.Equal(t, dataBlock{}, pbm.dataBlock)
	assert.Equal(t, dataBlock{}, pbm.keysBlock)
	assert.Equal(t, 0, len(pbm.tagsBlocks))
}
