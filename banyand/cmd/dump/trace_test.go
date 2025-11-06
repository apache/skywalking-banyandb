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
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// Test that the dump tool can parse the latest trace part format.
// This test creates a real part using the trace module's flush operation,
// then verifies the dump tool can correctly parse it.
func TestDumpTracePartFormat(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()

	// Use trace package to create a real part using actual flush operation
	partPath, cleanup := trace.CreateTestPartForDump(tmpPath, fileSystem)
	defer cleanup()

	// Extract part ID from path
	partName := filepath.Base(partPath)
	partID, err := strconv.ParseUint(partName, 16, 64)
	require.NoError(t, err, "part directory should have valid hex name")

	// Parse the part using dump tool functions
	p, err := openFilePart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err, "should be able to open part created by trace module")
	defer closePart(p)

	// Verify part metadata
	assert.Equal(t, partID, p.partMetadata.ID)
	t.Logf("Part metadata: TotalCount=%d, BlocksCount=%d", p.partMetadata.TotalCount, p.partMetadata.BlocksCount)
	assert.Greater(t, p.partMetadata.TotalCount, uint64(0), "should have spans")
	assert.Greater(t, p.partMetadata.BlocksCount, uint64(0), "should have at least 1 block")
	assert.Greater(t, p.partMetadata.MinTimestamp, int64(0), "should have valid min timestamp")
	assert.Greater(t, p.partMetadata.MaxTimestamp, int64(0), "should have valid max timestamp")
	assert.GreaterOrEqual(t, p.partMetadata.MaxTimestamp, p.partMetadata.MinTimestamp)

	// Verify tag types exist
	assert.Contains(t, p.tagType, "service.name")
	assert.Contains(t, p.tagType, "http.status")
	assert.Contains(t, p.tagType, "timestamp")
	assert.Contains(t, p.tagType, "tags")
	assert.Contains(t, p.tagType, "duration")

	// Verify tag types are correct
	assert.Equal(t, pbv1.ValueTypeStr, p.tagType["service.name"])
	assert.Equal(t, pbv1.ValueTypeInt64, p.tagType["http.status"])
	assert.Equal(t, pbv1.ValueTypeTimestamp, p.tagType["timestamp"])
	assert.Equal(t, pbv1.ValueTypeStrArr, p.tagType["tags"])
	assert.Equal(t, pbv1.ValueTypeInt64, p.tagType["duration"])

	// Verify primary block metadata
	assert.Greater(t, len(p.primaryBlockMetadata), 0, "should have at least 1 primary block")
	t.Logf("Found %d primary blocks (metadata says BlocksCount=%d)", len(p.primaryBlockMetadata), p.partMetadata.BlocksCount)
	for i, pbm := range p.primaryBlockMetadata {
		t.Logf("Block %d: TraceID=%s, Offset=%d, Size=%d", i, pbm.traceID, pbm.offset, pbm.size)
	}

	// The number of primary blocks might not match BlocksCount due to trace block grouping logic.
	// But we should be able to read all spans.
	assert.LessOrEqual(t, uint64(len(p.primaryBlockMetadata)), p.partMetadata.BlocksCount,
		"primary blocks should not exceed BlocksCount")

	// Verify we can decode all blocks
	decoder := &encoding.BytesBlockDecoder{}
	totalSpans := 0

	for blockIdx, pbm := range p.primaryBlockMetadata {
		// Read primary data block
		primaryData := make([]byte, pbm.size)
		fs.MustReadData(p.primary, int64(pbm.offset), primaryData)

		// Decompress
		decompressed, err := zstd.Decompress(nil, primaryData)
		require.NoError(t, err, "should decompress primary data for primary block %d", blockIdx)

		// Parse ALL block metadata entries from this primary block
		blockMetadatas, err := parseAllBlockMetadata(decompressed, p.tagType)
		require.NoError(t, err, "should parse all block metadata from primary block %d", blockIdx)
		t.Logf("Primary block %d contains %d trace blocks", blockIdx, len(blockMetadatas))

		// Process each trace block
		for bmIdx, bm := range blockMetadatas {
			// Read spans
			spans, spanIDs, err := readSpans(decoder, bm.spans, int(bm.count), p.spans)
			require.NoError(t, err, "should read spans for trace %s", bm.traceID)
			assert.Len(t, spans, int(bm.count), "should have correct number of spans")
			assert.Len(t, spanIDs, int(bm.count), "should have correct number of spanIDs")

			totalSpans += len(spans)
			t.Logf("  Trace block %d (TraceID=%s): read %d spans", bmIdx, bm.traceID, len(spans))

			// Read all tags
			for tagName, tagBlock := range bm.tags {
				tagValues, err := readTagValues(decoder, tagBlock, tagName, int(bm.count),
					p.tagMetadata[tagName], p.tags[tagName], p.tagType[tagName])
				require.NoError(t, err, "should read tag %s for trace %s", tagName, bm.traceID)
				assert.Len(t, tagValues, int(bm.count), "tag %s should have value for each span", tagName)

				// Verify specific tag values
				for i, tagValue := range tagValues {
					if tagValue == nil {
						continue
					}
					switch tagName {
					case "service.name":
						assert.NotEmpty(t, string(tagValue), "service.name should not be empty")
					case "http.status":
						status := convert.BytesToInt64(tagValue)
						assert.Contains(t, []int64{200, 404, 500}, status, "http.status should be valid")
					case "timestamp":
						ts := convert.BytesToInt64(tagValue)
						assert.Greater(t, ts, int64(0), "timestamp should be positive")
						assert.GreaterOrEqual(t, ts, p.partMetadata.MinTimestamp, "timestamp should be >= min")
						assert.LessOrEqual(t, ts, p.partMetadata.MaxTimestamp, "timestamp should be <= max")
					case "duration":
						duration := convert.BytesToInt64(tagValue)
						assert.Greater(t, duration, int64(0), "duration should be positive")
					case "tags":
						// Verify string array can be decoded
						if len(tagValue) > 0 {
							values := decodeStringArray(tagValue)
							if len(values) > 0 {
								t.Logf("    Span %d tags array: %v", i, values)
							}
						}
					}
				}
			}
		}
	}

	// Note: One primary block can contain multiple trace blocks (blockMetadata).
	// The dump tool shows primary blocks, not logical blocks.
	// BlocksCount = number of unique trace writes (blockMetadata)
	// len(primaryBlockMetadata) = number of compressed primary blocks
	// We verify we can read all the data correctly.
	assert.Equal(t, int(p.partMetadata.TotalCount), totalSpans, "should have parsed all spans from metadata")
	t.Logf("Successfully parsed part with %d spans across %d primary blocks (metadata BlocksCount=%d)",
		totalSpans, len(p.primaryBlockMetadata), p.partMetadata.BlocksCount)
}

func decodeStringArray(data []byte) []string {
	var values []string
	remaining := data
	for len(remaining) > 0 {
		var decoded []byte
		var err error
		decoded, remaining, err = unmarshalVarArray(nil, remaining)
		if err != nil {
			break
		}
		if len(decoded) > 0 {
			values = append(values, string(decoded))
		}
	}
	return values
}
