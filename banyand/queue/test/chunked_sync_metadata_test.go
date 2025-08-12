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

package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/queue"
)

// TestChunkedSyncMetadataVerification verifies that all metadata fields are properly transmitted from client to server.
func TestChunkedSyncMetadataVerification(t *testing.T) {
	setup := setupChunkedSyncTest(t, "chunked-sync-metadata-test")
	defer cleanupTestSetup(setup)

	testFiles := map[string][]byte{
		"test-metadata.json": []byte(`{"test": "metadata verification"}`),
		"test-data.bin":      []byte("test binary data for metadata verification"),
	}

	expectedMetadata := struct {
		Group                 string
		ID                    uint64
		CompressedSizeBytes   uint64
		UncompressedSizeBytes uint64
		TotalCount            uint64
		BlocksCount           uint64
		MinTimestamp          int64
		MaxTimestamp          int64
		ShardID               uint32
	}{
		ID:                    12345,
		Group:                 "test-metadata-group",
		ShardID:               42,
		CompressedSizeBytes:   1024,
		UncompressedSizeBytes: 2048,
		TotalCount:            100,
		BlocksCount:           5,
		MinTimestamp:          1640995200000,
		MaxTimestamp:          1641081600000,
	}

	dataPart := createTestDataPartWithMetadata(
		testFiles,
		expectedMetadata.Group,
		expectedMetadata.ShardID,
		expectedMetadata.CompressedSizeBytes,
		expectedMetadata.UncompressedSizeBytes,
		expectedMetadata.TotalCount,
		expectedMetadata.BlocksCount,
		expectedMetadata.MinTimestamp,
		expectedMetadata.MaxTimestamp,
		expectedMetadata.ID,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := setup.ChunkedClient.SyncStreamingParts(ctx, []queue.StreamingPartData{dataPart})
	require.NoError(t, err, "Failed to send data part with metadata")
	require.NotNil(t, result, "Sync result should not be nil")

	time.Sleep(500 * time.Millisecond)

	assert.True(t, result.Success, "Sync should be successful")
	assert.Equal(t, uint32(1), result.PartsCount, "Should have synced 1 part")
	assert.Greater(t, result.ChunksCount, uint32(0), "Should have sent at least 1 chunk")
	assert.Greater(t, result.TotalBytes, uint64(0), "Should have sent some bytes")

	receivedContexts := setup.MockHandler.GetReceivedContexts()
	require.Len(t, receivedContexts, 1, "Should have received exactly one context")

	receivedContext := receivedContexts[0]

	assert.Equal(t, expectedMetadata.ID, receivedContext.ID, "Part ID should match")
	assert.Equal(t, expectedMetadata.Group, receivedContext.Group, "Group should match")
	assert.Equal(t, expectedMetadata.ShardID, receivedContext.ShardID, "Shard ID should match")
	assert.Equal(t, expectedMetadata.CompressedSizeBytes, receivedContext.CompressedSizeBytes, "Compressed size should match")
	assert.Equal(t, expectedMetadata.UncompressedSizeBytes, receivedContext.UncompressedSizeBytes, "Uncompressed size should match")
	assert.Equal(t, expectedMetadata.TotalCount, receivedContext.TotalCount, "Total count should match")
	assert.Equal(t, expectedMetadata.BlocksCount, receivedContext.BlocksCount, "Blocks count should match")
	assert.Equal(t, expectedMetadata.MinTimestamp, receivedContext.MinTimestamp, "Min timestamp should match")
	assert.Equal(t, expectedMetadata.MaxTimestamp, receivedContext.MaxTimestamp, "Max timestamp should match")

	receivedFiles := setup.MockHandler.GetReceivedFiles()
	require.Len(t, receivedFiles, len(testFiles), "Should receive all test files")

	for fileName, expectedContent := range testFiles {
		receivedContent, exists := receivedFiles[fileName]
		require.True(t, exists, "File %s should be received", fileName)
		assert.Equal(t, expectedContent, receivedContent, "File content for %s should match", fileName)
	}

	completedParts := setup.MockHandler.GetCompletedParts()
	assert.Len(t, completedParts, 1, "Should have completed one part")
	if len(completedParts) > 0 {
		assert.True(t, completedParts[0].finishSyncCalled, "FinishSync should have been called")
		assert.True(t, completedParts[0].closeCalled, "Close should have been called")
	}

	t.Logf("Metadata verification test completed successfully:")
	t.Logf("  - Session ID: %s", result.SessionID)
	t.Logf("  - Part ID: %d", receivedContext.ID)
	t.Logf("  - Group: %s", receivedContext.Group)
	t.Logf("  - Shard ID: %d", receivedContext.ShardID)
	t.Logf("  - Compressed Size: %d bytes", receivedContext.CompressedSizeBytes)
	t.Logf("  - Uncompressed Size: %d bytes", receivedContext.UncompressedSizeBytes)
	t.Logf("  - Total Count: %d", receivedContext.TotalCount)
	t.Logf("  - Blocks Count: %d", receivedContext.BlocksCount)
	t.Logf("  - Min Timestamp: %d", receivedContext.MinTimestamp)
	t.Logf("  - Max Timestamp: %d", receivedContext.MaxTimestamp)
	t.Logf("  - Files received: %d", len(receivedFiles))
	t.Logf("  - Total bytes sent: %d", result.TotalBytes)
}

// TestChunkedSyncMultiplePartsMetadataVerification verifies metadata transmission for multiple parts.
func TestChunkedSyncMultiplePartsMetadataVerification(t *testing.T) {
	setup := setupChunkedSyncTest(t, "chunked-sync-multi-metadata-test")
	defer cleanupTestSetup(setup)

	part1Files := map[string][]byte{"part1.txt": []byte("part 1 data")}
	part2Files := map[string][]byte{"part2.txt": []byte("part 2 data")}
	part3Files := map[string][]byte{"part3.txt": []byte("part 3 data")}

	partsMetadata := []struct {
		Group                 string
		ID                    uint64
		CompressedSizeBytes   uint64
		UncompressedSizeBytes uint64
		TotalCount            uint64
		BlocksCount           uint64
		MinTimestamp          int64
		MaxTimestamp          int64
		ShardID               uint32
	}{
		{
			ID:                    1001,
			Group:                 "test-group-multi",
			ShardID:               42,
			CompressedSizeBytes:   512,
			UncompressedSizeBytes: 1024,
			TotalCount:            50,
			BlocksCount:           2,
			MinTimestamp:          1640995200000,
			MaxTimestamp:          1640998800000,
		},
		{
			ID:                    1002,
			Group:                 "test-group-multi",
			ShardID:               42,
			CompressedSizeBytes:   768,
			UncompressedSizeBytes: 1536,
			TotalCount:            75,
			BlocksCount:           3,
			MinTimestamp:          1640998800000,
			MaxTimestamp:          1641002400000,
		},
		{
			ID:                    1003,
			Group:                 "test-group-multi",
			ShardID:               42,
			CompressedSizeBytes:   1024,
			UncompressedSizeBytes: 2048,
			TotalCount:            100,
			BlocksCount:           4,
			MinTimestamp:          1641002400000,
			MaxTimestamp:          1641006000000,
		},
	}

	dataParts := []queue.StreamingPartData{
		createTestDataPartWithMetadata(
			part1Files,
			partsMetadata[0].Group,
			partsMetadata[0].ShardID,
			partsMetadata[0].CompressedSizeBytes,
			partsMetadata[0].UncompressedSizeBytes,
			partsMetadata[0].TotalCount,
			partsMetadata[0].BlocksCount,
			partsMetadata[0].MinTimestamp,
			partsMetadata[0].MaxTimestamp,
			partsMetadata[0].ID,
		),
		createTestDataPartWithMetadata(
			part2Files,
			partsMetadata[1].Group,
			partsMetadata[1].ShardID,
			partsMetadata[1].CompressedSizeBytes,
			partsMetadata[1].UncompressedSizeBytes,
			partsMetadata[1].TotalCount,
			partsMetadata[1].BlocksCount,
			partsMetadata[1].MinTimestamp,
			partsMetadata[1].MaxTimestamp,
			partsMetadata[1].ID,
		),
		createTestDataPartWithMetadata(
			part3Files,
			partsMetadata[2].Group,
			partsMetadata[2].ShardID,
			partsMetadata[2].CompressedSizeBytes,
			partsMetadata[2].UncompressedSizeBytes,
			partsMetadata[2].TotalCount,
			partsMetadata[2].BlocksCount,
			partsMetadata[2].MinTimestamp,
			partsMetadata[2].MaxTimestamp,
			partsMetadata[2].ID,
		),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	result, err := setup.ChunkedClient.SyncStreamingParts(ctx, dataParts)
	require.NoError(t, err, "Failed to send multiple parts with metadata")
	require.True(t, result.Success, "Multiple parts sync should be successful")

	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, uint32(3), result.PartsCount, "Should have synced 3 parts")
	assert.Greater(t, result.ChunksCount, uint32(0), "Should have sent at least 1 chunk")

	receivedContexts := setup.MockHandler.GetReceivedContexts()
	require.Len(t, receivedContexts, 3, "Should have received exactly 3 contexts")

	for i, expectedMetadata := range partsMetadata {
		receivedContext := receivedContexts[i]

		assert.Equal(t, expectedMetadata.ID, receivedContext.ID, "Part %d ID should match", i+1)
		assert.Equal(t, expectedMetadata.Group, receivedContext.Group, "Part %d Group should match", i+1)
		assert.Equal(t, expectedMetadata.ShardID, receivedContext.ShardID, "Part %d Shard ID should match", i+1)
		assert.Equal(t, expectedMetadata.CompressedSizeBytes, receivedContext.CompressedSizeBytes, "Part %d Compressed size should match", i+1)
		assert.Equal(t, expectedMetadata.UncompressedSizeBytes, receivedContext.UncompressedSizeBytes, "Part %d Uncompressed size should match", i+1)
		assert.Equal(t, expectedMetadata.TotalCount, receivedContext.TotalCount, "Part %d Total count should match", i+1)
		assert.Equal(t, expectedMetadata.BlocksCount, receivedContext.BlocksCount, "Part %d Blocks count should match", i+1)
		assert.Equal(t, expectedMetadata.MinTimestamp, receivedContext.MinTimestamp, "Part %d Min timestamp should match", i+1)
		assert.Equal(t, expectedMetadata.MaxTimestamp, receivedContext.MaxTimestamp, "Part %d Max timestamp should match", i+1)
	}

	receivedFiles := setup.MockHandler.GetReceivedFiles()
	expectedFileCount := len(part1Files) + len(part2Files) + len(part3Files)
	require.Len(t, receivedFiles, expectedFileCount, "Should receive all files from all parts")

	completedParts := setup.MockHandler.GetCompletedParts()
	assert.Len(t, completedParts, 3, "Should have completed three parts")
	for i, part := range completedParts {
		assert.True(t, part.finishSyncCalled, "FinishSync should have been called for part %d", i+1)
		assert.True(t, part.closeCalled, "Close should have been called for part %d", i+1)
	}

	t.Logf("Multiple parts metadata verification test completed successfully:")
	t.Logf("  - Parts synced: %d", result.PartsCount)
	t.Logf("  - Contexts received: %d", len(receivedContexts))
	t.Logf("  - Files received: %d", len(receivedFiles))
	t.Logf("  - Total bytes sent: %d", result.TotalBytes)

	for i, ctx := range receivedContexts {
		t.Logf("  - Part %d: ID=%d, Group=%s, ShardID=%d, CompressedSize=%d, TotalCount=%d",
			i+1, ctx.ID, ctx.Group, ctx.ShardID, ctx.CompressedSizeBytes, ctx.TotalCount)
	}
}
