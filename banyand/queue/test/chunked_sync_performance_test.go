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

// TestChunkedSyncLargeData tests syncing with data larger than chunk size.
func TestChunkedSyncLargeData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large data test in short mode")
	}

	setup := setupChunkedSyncTestWithChunkSize(t, "chunked-sync-large-test", 512)
	defer cleanupTestSetup(setup)

	const largeDataSize = 2048
	largeContent := make([]byte, largeDataSize)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}

	testFiles := map[string][]byte{"large-file.data": largeContent}
	dataPart := createTestDataPart(testFiles, "test-group-large", 3)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	result, err := setup.ChunkedClient.SyncStreamingParts(ctx, []queue.StreamingPartData{dataPart})
	require.NoError(t, err, "Failed to send large data part")
	require.True(t, result.Success, "Large data sync should be successful")

	time.Sleep(500 * time.Millisecond)

	receivedFiles := setup.MockHandler.GetReceivedFiles()
	require.Len(t, receivedFiles, 1, "Should receive the large file")

	receivedContent := receivedFiles["large-file.data"]
	assert.Equal(t, largeContent, receivedContent, "Large file content should match exactly")

	assert.Greater(t, result.ChunksCount, uint32(1), "Large data should require multiple chunks")
	assert.Greater(t, setup.MockHandler.GetReceivedChunksCount(), 1, "Should have received multiple chunks")

	t.Logf("Large data test completed: %d bytes in %d chunks", result.TotalBytes, result.ChunksCount)
}

// TestChunkedSyncMultiplePartsSmallFiles tests syncing multiple parts with multiple small files.
func TestChunkedSyncMultiplePartsSmallFiles(t *testing.T) {
	setup := setupChunkedSyncTestWithChunkSize(t, "chunked-sync-multi-parts-small", 1024)
	defer cleanupTestSetup(setup)

	part1Files := map[string][]byte{
		"part1-config.json": []byte(`{"part": 1, "type": "config", "data": "small"}`),
		"part1-log.txt":     []byte("Part 1 log entry: test data"),
		"part1-meta.yaml":   []byte("metadata:\n  part: 1\n  size: small"),
	}

	part2Files := map[string][]byte{
		"part2-config.json": []byte(`{"part": 2, "type": "config", "data": "small"}`),
		"part2-log.txt":     []byte("Part 2 log entry: different data"),
		"part2-meta.yaml":   []byte("metadata:\n  part: 2\n  size: small"),
	}

	part3Files := map[string][]byte{
		"part3-config.json": []byte(`{"part": 3, "type": "config", "data": "small"}`),
		"part3-log.txt":     []byte("Part 3 log entry: final data"),
		"part3-meta.yaml":   []byte("metadata:\n  part: 3\n  size: small"),
	}

	dataParts := []queue.StreamingPartData{
		createTestDataPart(part1Files, "test-group-multi-parts", 1),
		createTestDataPart(part2Files, "test-group-multi-parts", 2),
		createTestDataPart(part3Files, "test-group-multi-parts", 3),
	}

	dataParts[1].ID = 2
	dataParts[2].ID = 3

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	result, err := setup.ChunkedClient.SyncStreamingParts(ctx, dataParts)
	require.NoError(t, err, "Failed to send multiple parts")
	require.True(t, result.Success, "Multiple parts sync should be successful")

	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, uint32(3), result.PartsCount, "Should have synced 3 parts")
	assert.Greater(t, result.ChunksCount, uint32(0), "Should have sent at least 1 chunk")
	assert.Greater(t, result.TotalBytes, uint64(0), "Should have sent some bytes")

	receivedFiles := setup.MockHandler.GetReceivedFiles()
	expectedFileCount := len(part1Files) + len(part2Files) + len(part3Files)
	require.Len(t, receivedFiles, expectedFileCount, "Should receive all files from all parts")

	allExpectedFiles := make(map[string][]byte)
	for k, v := range part1Files {
		allExpectedFiles[k] = v
	}
	for k, v := range part2Files {
		allExpectedFiles[k] = v
	}
	for k, v := range part3Files {
		allExpectedFiles[k] = v
	}

	for fileName, expectedContent := range allExpectedFiles {
		receivedContent, exists := receivedFiles[fileName]
		require.True(t, exists, "File %s should be received", fileName)
		assert.Equal(t, expectedContent, receivedContent, "Content for file %s should match", fileName)
	}

	completedParts := setup.MockHandler.GetCompletedParts()
	assert.Len(t, completedParts, 3, "Should have completed three parts")
	for i, part := range completedParts {
		assert.True(t, part.finishSyncCalled, "FinishSync should have been called for part %d", i+1)
		assert.True(t, part.closeCalled, "Close should have been called for part %d", i+1)
	}

	chunksCount := setup.MockHandler.GetReceivedChunksCount()
	assert.GreaterOrEqual(t, chunksCount, 3, "Should have received at least 3 chunks (one per part minimum)")

	t.Logf("Multiple parts with small files test completed:")
	t.Logf("  - Parts synced: %d", result.PartsCount)
	t.Logf("  - Files synced: %d", len(receivedFiles))
	t.Logf("  - Chunks sent in total: %d", result.ChunksCount)
	t.Logf("  - Chunks received in file-based sync: %d", chunksCount)
	t.Logf("  - Total bytes: %d", result.TotalBytes)
}

// TestChunkedSyncMultiplePartsBigFiles tests syncing multiple parts with multiple big files.
func TestChunkedSyncMultiplePartsBigFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping multiple parts with big files test in short mode")
	}

	setup := setupChunkedSyncTestWithChunkSize(t, "chunked-sync-multi-parts-big", 512)
	defer cleanupTestSetup(setup)

	const bigFileSize = 1024

	createBigFileContent := func(partNum int, fileName string) []byte {
		content := make([]byte, bigFileSize)
		for i := range content {
			content[i] = byte((partNum*100 + len(fileName) + i) % 256)
		}
		return content
	}

	part1Files := map[string][]byte{
		"part1-large-data.bin":  createBigFileContent(1, "part1-large-data.bin"),
		"part1-bulk-export.dat": createBigFileContent(1, "part1-bulk-export.dat"),
	}

	part2Files := map[string][]byte{
		"part2-large-data.bin":  createBigFileContent(2, "part2-large-data.bin"),
		"part2-bulk-export.dat": createBigFileContent(2, "part2-bulk-export.dat"),
	}

	part3Files := map[string][]byte{
		"part3-large-data.bin":  createBigFileContent(3, "part3-large-data.bin"),
		"part3-bulk-export.dat": createBigFileContent(3, "part3-bulk-export.dat"),
	}

	dataParts := []queue.StreamingPartData{
		createTestDataPart(part1Files, "test-group-multi-parts-big", 1),
		createTestDataPart(part2Files, "test-group-multi-parts-big", 2),
		createTestDataPart(part3Files, "test-group-multi-parts-big", 3),
	}

	dataParts[1].ID = 2
	dataParts[2].ID = 3

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := setup.ChunkedClient.SyncStreamingParts(ctx, dataParts)
	require.NoError(t, err, "Failed to send multiple parts with big files")
	require.True(t, result.Success, "Multiple parts with big files sync should be successful")

	time.Sleep(1 * time.Second)

	assert.Equal(t, uint32(3), result.PartsCount, "Should have synced 3 parts")
	assert.Greater(t, result.ChunksCount, uint32(6), "Should have sent multiple chunks per file (at least 6 total)")
	assert.Equal(t, result.TotalBytes, uint64(6*bigFileSize), "Should have sent bytes for all big files")

	receivedFiles := setup.MockHandler.GetReceivedFiles()
	expectedFileCount := len(part1Files) + len(part2Files) + len(part3Files)
	require.Len(t, receivedFiles, expectedFileCount, "Should receive all files from all parts")

	allExpectedFiles := make(map[string][]byte)
	for k, v := range part1Files {
		allExpectedFiles[k] = v
	}
	for k, v := range part2Files {
		allExpectedFiles[k] = v
	}
	for k, v := range part3Files {
		allExpectedFiles[k] = v
	}

	for fileName, expectedContent := range allExpectedFiles {
		receivedContent, exists := receivedFiles[fileName]
		require.True(t, exists, "File %s should be received", fileName)
		assert.Equal(t, expectedContent, receivedContent, "Content for file %s should match exactly", fileName)
		assert.Equal(t, bigFileSize, len(receivedContent), "File %s should have correct size", fileName)
	}

	completedParts := setup.MockHandler.GetCompletedParts()
	assert.Len(t, completedParts, 3, "Should have completed three parts")
	for i, part := range completedParts {
		assert.True(t, part.finishSyncCalled, "FinishSync should have been called for part %d", i+1)
		assert.True(t, part.closeCalled, "Close should have been called for part %d", i+1)
	}

	chunksCount := setup.MockHandler.GetReceivedChunksCount()
	expectedMinChunks := expectedFileCount * 2
	assert.GreaterOrEqual(t, chunksCount, expectedMinChunks, "Should have received multiple chunks per big file")

	totalReceivedBytes := uint64(0)
	for _, part := range completedParts {
		for _, chunk := range part.receivedChunks {
			totalReceivedBytes += uint64(len(chunk))
		}
	}
	assert.Equal(t, result.TotalBytes, totalReceivedBytes, "Total received bytes should match sent bytes")

	t.Logf("Multiple parts with big files test completed:")
	t.Logf("  - Parts synced: %d", result.PartsCount)
	t.Logf("  - Files synced: %d", len(receivedFiles))
	t.Logf("  - Chunks sent in total: %d", result.ChunksCount)
	t.Logf("  - Chunks received in file-based sync: %d", chunksCount)
	t.Logf("  - Total bytes: %d", result.TotalBytes)
	t.Logf("  - Average chunks per file: %.2f", float64(chunksCount)/float64(expectedFileCount))
}

// TestChunkedSyncMultiplePartsTinyFiles tests syncing multiple parts with tiny files that can be batched into single chunks.
func TestChunkedSyncMultiplePartsTinyFiles(t *testing.T) {
	setup := setupChunkedSyncTestWithChunkSize(t, "chunked-sync-multi-parts-tiny", 2048)
	defer cleanupTestSetup(setup)

	part1Files := map[string][]byte{
		"part1-a.txt": []byte("A"),
		"part1-b.txt": []byte("B"),
		"part1-c.txt": []byte("C"),
		"part1-d.txt": []byte("D"),
		"part1-e.txt": []byte("E"),
	}

	part2Files := map[string][]byte{
		"part2-a.txt": []byte("AA"),
		"part2-b.txt": []byte("BB"),
		"part2-c.txt": []byte("CC"),
		"part2-d.txt": []byte("DD"),
		"part2-e.txt": []byte("EE"),
	}

	part3Files := map[string][]byte{
		"part3-a.txt": []byte("AAA"),
		"part3-b.txt": []byte("BBB"),
		"part3-c.txt": []byte("CCC"),
		"part3-d.txt": []byte("DDD"),
		"part3-e.txt": []byte("EEE"),
	}

	dataParts := []queue.StreamingPartData{
		createTestDataPart(part1Files, "test-group-multi-parts-tiny", 1),
		createTestDataPart(part2Files, "test-group-multi-parts-tiny", 2),
		createTestDataPart(part3Files, "test-group-multi-parts-tiny", 3),
	}

	dataParts[1].ID = 2
	dataParts[2].ID = 3

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	result, err := setup.ChunkedClient.SyncStreamingParts(ctx, dataParts)
	require.NoError(t, err, "Failed to send multiple parts with tiny files")
	require.True(t, result.Success, "Multiple parts with tiny files sync should be successful")

	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, uint32(3), result.PartsCount, "Should have synced 3 parts")
	assert.Greater(t, result.ChunksCount, uint32(0), "Should have sent at least 1 chunk")
	assert.Greater(t, result.TotalBytes, uint64(0), "Should have sent some bytes")

	receivedFiles := setup.MockHandler.GetReceivedFiles()
	expectedFileCount := len(part1Files) + len(part2Files) + len(part3Files)
	require.Len(t, receivedFiles, expectedFileCount, "Should receive all files from all parts")

	allExpectedFiles := make(map[string][]byte)
	for k, v := range part1Files {
		allExpectedFiles[k] = v
	}
	for k, v := range part2Files {
		allExpectedFiles[k] = v
	}
	for k, v := range part3Files {
		allExpectedFiles[k] = v
	}

	for fileName, expectedContent := range allExpectedFiles {
		receivedContent, exists := receivedFiles[fileName]
		require.True(t, exists, "File %s should be received", fileName)
		assert.Equal(t, expectedContent, receivedContent, "Content for file %s should match", fileName)
	}

	completedParts := setup.MockHandler.GetCompletedParts()
	assert.Len(t, completedParts, 3, "Should have completed three parts")
	for i, part := range completedParts {
		assert.True(t, part.finishSyncCalled, "FinishSync should have been called for part %d", i+1)
		assert.True(t, part.closeCalled, "Close should have been called for part %d", i+1)
	}

	chunksCount := setup.MockHandler.GetReceivedChunksCount()
	totalFileBytes := uint64(0)
	for _, content := range allExpectedFiles {
		totalFileBytes += uint64(len(content))
	}

	expectedMaxChunks := uint32((totalFileBytes + 2047) / 2048)
	assert.LessOrEqual(t, result.ChunksCount, expectedMaxChunks+1, "Should use fewer chunks due to batching")

	t.Logf("Multiple parts with tiny files test completed:")
	t.Logf("  - Parts synced: %d", result.PartsCount)
	t.Logf("  - Files synced: %d", len(receivedFiles))
	t.Logf("  - Chunks sent in total: %d", result.ChunksCount)
	t.Logf("  - Chunks received in file-based sync: %d", chunksCount)
	t.Logf("  - Total bytes: %d", result.TotalBytes)
	t.Logf("  - Average files per chunk: %.2f", float64(expectedFileCount)/float64(chunksCount))
}

// TestChunkedSyncMixedFileSizes tests syncing with mixed file sizes that can be efficiently batched.
func TestChunkedSyncMixedFileSizes(t *testing.T) {
	setup := setupChunkedSyncTestWithChunkSize(t, "chunked-sync-mixed-sizes", 1024)
	defer cleanupTestSetup(setup)

	part1Files := map[string][]byte{
		"part1-small1.txt": []byte("small1"),
		"part1-small2.txt": []byte("small2"),
		"part1-medium.txt": []byte(string(make([]byte, 200))),
		"part1-large.txt":  []byte(string(make([]byte, 800))),
		"part1-tiny.txt":   []byte("t"),
	}

	part2Files := map[string][]byte{
		"part2-small1.txt": []byte("small1"),
		"part2-small2.txt": []byte("small2"),
		"part2-medium.txt": []byte(string(make([]byte, 300))),
		"part2-large.txt":  []byte(string(make([]byte, 900))),
		"part2-tiny.txt":   []byte("t"),
	}

	part3Files := map[string][]byte{
		"part3-small1.txt": []byte("small1"),
		"part3-small2.txt": []byte("small2"),
		"part3-medium.txt": []byte(string(make([]byte, 250))),
		"part3-large.txt":  []byte(string(make([]byte, 850))),
		"part3-tiny.txt":   []byte("t"),
	}

	dataParts := []queue.StreamingPartData{
		createTestDataPart(part1Files, "test-group-mixed-sizes", 1),
		createTestDataPart(part2Files, "test-group-mixed-sizes", 2),
		createTestDataPart(part3Files, "test-group-mixed-sizes", 3),
	}

	dataParts[1].ID = 2
	dataParts[2].ID = 3

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	result, err := setup.ChunkedClient.SyncStreamingParts(ctx, dataParts)
	require.NoError(t, err, "Failed to send multiple parts with mixed file sizes")
	require.True(t, result.Success, "Multiple parts with mixed file sizes sync should be successful")

	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, uint32(3), result.PartsCount, "Should have synced 3 parts")
	assert.Greater(t, result.ChunksCount, uint32(0), "Should have sent at least 1 chunk")
	assert.Greater(t, result.TotalBytes, uint64(0), "Should have sent some bytes")

	receivedFiles := setup.MockHandler.GetReceivedFiles()
	expectedFileCount := len(part1Files) + len(part2Files) + len(part3Files)
	require.Len(t, receivedFiles, expectedFileCount, "Should receive all files from all parts")

	allExpectedFiles := make(map[string][]byte)
	for k, v := range part1Files {
		allExpectedFiles[k] = v
	}
	for k, v := range part2Files {
		allExpectedFiles[k] = v
	}
	for k, v := range part3Files {
		allExpectedFiles[k] = v
	}

	for fileName, expectedContent := range allExpectedFiles {
		receivedContent, exists := receivedFiles[fileName]
		require.True(t, exists, "File %s should be received", fileName)
		assert.Equal(t, expectedContent, receivedContent, "Content for file %s should match", fileName)
	}

	completedParts := setup.MockHandler.GetCompletedParts()
	assert.Len(t, completedParts, 3, "Should have completed three parts")
	for i, part := range completedParts {
		assert.True(t, part.finishSyncCalled, "FinishSync should have been called for part %d", i+1)
		assert.True(t, part.closeCalled, "Close should have been called for part %d", i+1)
	}

	chunksCount := setup.MockHandler.GetReceivedChunksCount()
	totalFileBytes := uint64(0)
	for _, content := range allExpectedFiles {
		totalFileBytes += uint64(len(content))
	}

	expectedLargeFileChunks := uint32(3)
	remainingBytes := totalFileBytes - uint64(800+900+850)
	expectedRemainingChunks := uint32((remainingBytes + 1023) / 1024)
	expectedMinChunks := expectedLargeFileChunks + expectedRemainingChunks

	assert.GreaterOrEqual(t, result.ChunksCount, expectedMinChunks, "Should use at least expected minimum chunks")
	assert.LessOrEqual(t, result.ChunksCount, expectedMinChunks+3, "Should not use significantly more chunks than expected")

	partChunkCounts := make([]int, 3)
	for i, part := range completedParts {
		partChunkCounts[i] = len(part.receivedChunks)
	}

	t.Logf("Mixed file sizes test completed:")
	t.Logf("  - Parts synced: %d", result.PartsCount)
	t.Logf("  - Files synced: %d", len(receivedFiles))
	t.Logf("  - Chunks sent in total: %d", result.ChunksCount)
	t.Logf("  - Chunks received in file-based sync: %d", chunksCount)
	t.Logf("  - Total bytes: %d", result.TotalBytes)
	t.Logf("  - Chunks per part: %v", partChunkCounts)
	t.Logf("  - Average files per chunk: %.2f", float64(expectedFileCount)/float64(result.ChunksCount))
	t.Logf("  - Chunking efficiency: %.2f%%", float64(totalFileBytes)/float64(result.ChunksCount*1024)*100)
}
