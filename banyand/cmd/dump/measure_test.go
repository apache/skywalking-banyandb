// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// TestDumpMeasurePartFormat tests that the dump tool can parse the latest measure part format.
// This test creates a real part using the measure module's flush operation,
// then verifies the dump tool can correctly parse it.
func TestDumpMeasurePartFormat(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()

	// Use measure package to create a real part using actual flush operation
	partPath, cleanup := measure.CreateTestPartForDump(tmpPath, fileSystem)
	defer cleanup()

	// Extract part ID from path
	partName := filepath.Base(partPath)
	partID, err := strconv.ParseUint(partName, 16, 64)
	require.NoError(t, err, "part directory should have valid hex name")

	// Parse the part using dump tool functions
	p, err := openMeasurePart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err, "should be able to open part created by measure module")
	defer closeMeasurePart(p)

	// Verify part metadata
	assert.Equal(t, partID, p.partMetadata.ID)
	t.Logf("Part metadata: TotalCount=%d, BlocksCount=%d", p.partMetadata.TotalCount, p.partMetadata.BlocksCount)
	assert.Greater(t, p.partMetadata.TotalCount, uint64(0), "should have data points")
	assert.Greater(t, p.partMetadata.BlocksCount, uint64(0), "should have at least 1 block")
	assert.Greater(t, p.partMetadata.MinTimestamp, int64(0), "should have valid min timestamp")
	assert.Greater(t, p.partMetadata.MaxTimestamp, int64(0), "should have valid max timestamp")
	assert.GreaterOrEqual(t, p.partMetadata.MaxTimestamp, p.partMetadata.MinTimestamp)

	// Verify primary block metadata
	assert.Greater(t, len(p.primaryBlockMetadata), 0, "should have at least 1 primary block")
	t.Logf("Found %d primary blocks (metadata says BlocksCount=%d)", len(p.primaryBlockMetadata), p.partMetadata.BlocksCount)
	for i, pbm := range p.primaryBlockMetadata {
		t.Logf("Block %d: SeriesID=%d, MinTimestamp=%d, MaxTimestamp=%d, Offset=%d, Size=%d", i, pbm.seriesID, pbm.minTimestamp, pbm.maxTimestamp, pbm.offset, pbm.size)
	}

	// Verify we can decode all blocks
	decoder := &encoding.BytesBlockDecoder{}
	totalDataPoints := 0

	for blockIdx, pbm := range p.primaryBlockMetadata {
		// Read primary data block
		primaryData := make([]byte, pbm.size)
		fs.MustReadData(p.primary, int64(pbm.offset), primaryData)

		// Decompress
		decompressed, err := zstd.Decompress(nil, primaryData)
		require.NoError(t, err, "should decompress primary data for primary block %d", blockIdx)

		// Parse ALL block metadata entries from this primary block
		blockMetadatas, err := parseMeasureBlockMetadata(decompressed)
		require.NoError(t, err, "should parse all block metadata from primary block %d", blockIdx)
		t.Logf("Primary block %d contains %d measure blocks", blockIdx, len(blockMetadatas))

		// Process each measure block
		for bmIdx, bm := range blockMetadatas {
			// Read timestamps and versions
			timestamps, versions, err := readMeasureTimestamps(bm.timestamps, int(bm.count), p.timestamps)
			require.NoError(t, err, "should read timestamps/versions for series %d", bm.seriesID)
			assert.Len(t, timestamps, int(bm.count), "should have correct number of timestamps")
			assert.Len(t, versions, int(bm.count), "should have correct number of versions")

			totalDataPoints += len(timestamps)
			t.Logf("  Measure block %d (SeriesID=%d): read %d data points", bmIdx, bm.seriesID, len(timestamps))

			// Verify timestamps are valid
			for i, ts := range timestamps {
				assert.Greater(t, ts, int64(0), "timestamp should be positive")
				assert.GreaterOrEqual(t, ts, p.partMetadata.MinTimestamp, "timestamp should be >= min")
				assert.LessOrEqual(t, ts, p.partMetadata.MaxTimestamp, "timestamp should be <= max")
				t.Logf("    Data point %d: Version=%d, Timestamp=%s", i, versions[i], formatTimestamp(ts))
			}

			// Read field values if available
			for _, colMeta := range bm.field.columns {
				fieldValues, err := readMeasureFieldValues(decoder, colMeta.dataBlock, colMeta.name, int(bm.count), p.fieldValues, colMeta.valueType)
				require.NoError(t, err, "should read field %s for series %d", colMeta.name, bm.seriesID)
				assert.Len(t, fieldValues, int(bm.count), "field %s should have value for each data point", colMeta.name)

				// Verify specific field values
				for i, fieldValue := range fieldValues {
					if fieldValue == nil {
						continue
					}
					t.Logf("    Data point %d field %s: %s", i, colMeta.name, formatTagValueForDisplay(fieldValue, colMeta.valueType))
				}
			}

			// Read tag families if available
			for tagFamilyName, tagFamilyBlock := range bm.tagFamilies {
				// Read tag family metadata
				tagFamilyMetadataData := make([]byte, tagFamilyBlock.size)
				fs.MustReadData(p.tagFamilyMetadata[tagFamilyName], int64(tagFamilyBlock.offset), tagFamilyMetadataData)

				// Parse tag family metadata as columnFamilyMetadata (same format as fields)
				var cfm measureColumnFamilyMetadata
				_, err := cfm.unmarshal(tagFamilyMetadataData)
				require.NoError(t, err, "should parse tag family metadata %s for series %d", tagFamilyName, bm.seriesID)

				// Read each tag (column) in the tag family
				for _, colMeta := range cfm.columns {
					fullTagName := tagFamilyName + "." + colMeta.name
					tagValues, err := readMeasureTagValues(decoder, colMeta.dataBlock, fullTagName, int(bm.count), p.tagFamilies[tagFamilyName], colMeta.valueType)
					require.NoError(t, err, "should read tag %s for series %d", fullTagName, bm.seriesID)
					assert.Len(t, tagValues, int(bm.count), "tag %s should have value for each data point", fullTagName)

					// Verify specific tag values
					for i, tagValue := range tagValues {
						if tagValue == nil {
							continue
						}
						t.Logf("    Data point %d tag %s: %s", i, fullTagName, formatTagValueForDisplay(tagValue, colMeta.valueType))
					}
				}
			}
		}
	}

	// Verify we can read all the data correctly
	assert.Equal(t, int(p.partMetadata.TotalCount), totalDataPoints, "should have parsed all data points from metadata")
	t.Logf("Successfully parsed part with %d data points across %d primary blocks (metadata BlocksCount=%d)",
		totalDataPoints, len(p.primaryBlockMetadata), p.partMetadata.BlocksCount)
}

// TestDumpMeasurePartWithSeriesMetadata tests that the dump tool can parse smeta file.
// This test creates a part with series metadata and verifies the dump tool can correctly parse it.
func TestDumpMeasurePartWithSeriesMetadata(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()

	// Create a part with series metadata
	partPath, cleanup := createTestMeasurePartWithSeriesMetadata(tmpPath, fileSystem)
	defer cleanup()

	// Extract part ID from path
	partName := filepath.Base(partPath)
	partID, err := strconv.ParseUint(partName, 16, 64)
	require.NoError(t, err, "part directory should have valid hex name")

	// Parse the part using dump tool functions
	p, err := openMeasurePart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err, "should be able to open part with series metadata")
	defer closeMeasurePart(p)

	// Verify series metadata reader is available
	assert.NotNil(t, p.seriesMetadata, "series metadata reader should be available")

	// Create a dump context to test parsing
	opts := measureDumpOptions{
		shardPath:   filepath.Dir(partPath),
		segmentPath: tmpPath, // Not used for this test
		verbose:     false,
		csvOutput:   false,
	}
	ctx, err := newMeasureDumpContext(opts)
	require.NoError(t, err)
	if ctx != nil {
		defer ctx.close()
	}

	// Test parsing series metadata
	err = ctx.parseAndDisplaySeriesMetadata(partID, p)
	require.NoError(t, err, "should be able to parse series metadata")

	// Verify EntityValues are stored in partSeriesMap
	require.NotNil(t, ctx.partSeriesMap, "partSeriesMap should be initialized")
	partMap, exists := ctx.partSeriesMap[partID]
	require.True(t, exists, "partSeriesMap should contain entry for partID")
	require.NotNil(t, partMap, "partMap should not be nil")

	// Verify EntityValues are correctly stored
	// Calculate expected SeriesIDs from EntityValues
	expectedSeriesID1 := common.SeriesID(convert.Hash([]byte("service.name=test-service")))
	expectedSeriesID2 := common.SeriesID(convert.Hash([]byte("service.name=another-service")))

	assert.Contains(t, partMap, expectedSeriesID1, "partMap should contain first series")
	assert.Contains(t, partMap, expectedSeriesID2, "partMap should contain second series")
	assert.Equal(t, "service.name=test-service", partMap[expectedSeriesID1], "EntityValues should match")
	assert.Equal(t, "service.name=another-service", partMap[expectedSeriesID2], "EntityValues should match")
}

// createTestMeasurePartWithSeriesMetadata creates a test measure part with series metadata.
func createTestMeasurePartWithSeriesMetadata(tmpPath string, fileSystem fs.FileSystem) (string, func()) {
	// Use measure package to create a part
	partPath, cleanup := measure.CreateTestPartForDump(tmpPath, fileSystem)

	// Create sample series metadata file
	seriesMetadataPath := filepath.Join(partPath, "smeta.bin")

	// Create sample documents
	docs := index.Documents{
		{
			DocID:        1,
			EntityValues: []byte("service.name=test-service"),
		},
		{
			DocID:        2,
			EntityValues: []byte("service.name=another-service"),
		},
	}

	seriesMetadataBytes, err := docs.Marshal()
	if err != nil {
		panic(fmt.Sprintf("failed to marshal series metadata documents: %v", err))
	}
	fs.MustFlush(fileSystem, seriesMetadataBytes, seriesMetadataPath, storage.FilePerm)

	return partPath, cleanup
}
