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

// Package test provides testing utilities and test cases for the queue package.
package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/queue"
)

// TestChunkedSyncBasic validates the chunked streaming data synchronization.
func TestChunkedSyncBasic(t *testing.T) {
	setup := setupChunkedSyncTest(t, "chunked-sync-test")
	defer cleanupTestSetup(setup)

	testFileName := "test-file.txt"
	testContent := []byte("Hello, BanyanDB chunked sync test!")
	testFiles := map[string][]byte{testFileName: testContent}
	dataPart := createTestDataPart(testFiles, "test-group", 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := setup.ChunkedClient.SyncStreamingParts(ctx, []queue.StreamingPartData{dataPart})
	require.NoError(t, err, "Failed to send data part")
	require.NotNil(t, result, "Sync result should not be nil")

	time.Sleep(500 * time.Millisecond)

	assert.True(t, result.Success, "Sync should be successful")
	assert.Equal(t, uint32(1), result.PartsCount, "Should have synced 1 part")
	assert.Greater(t, result.ChunksCount, uint32(0), "Should have sent at least 1 chunk")
	assert.Greater(t, result.TotalBytes, uint64(0), "Should have sent some bytes")
	assert.NotEmpty(t, result.SessionID, "Session ID should not be empty")

	receivedFiles := setup.MockHandler.GetReceivedFiles()
	require.Len(t, receivedFiles, 1, "Expected exactly one received file")

	receivedContent, exists := receivedFiles[testFileName]
	require.True(t, exists, "Expected file %s to be received", testFileName)
	assert.Equal(t, testContent, receivedContent, "File content should match")

	chunksCount := setup.MockHandler.GetReceivedChunksCount()
	assert.Greater(t, chunksCount, 0, "Should have received at least one chunk")

	completedParts := setup.MockHandler.GetCompletedParts()
	assert.Len(t, completedParts, 1, "Should have completed one part")
	if len(completedParts) > 0 {
		assert.True(t, completedParts[0].finishSyncCalled, "FinishSync should have been called")
	}

	t.Logf("Test completed successfully:")
	t.Logf("  - Session ID: %s", result.SessionID)
	t.Logf("  - Total bytes sent: %d", result.TotalBytes)
	t.Logf("  - Chunks sent in total: %d", result.ChunksCount)
	t.Logf("  - Duration: %d ms", result.DurationMs)
	t.Logf("  - Chunks received in file-based sync: %d", chunksCount)
	t.Logf("  - Files received: %d", len(receivedFiles))
}

// TestChunkedSyncMultipleFiles tests syncing multiple files in a single part.
func TestChunkedSyncMultipleFiles(t *testing.T) {
	setup := setupChunkedSyncTest(t, "chunked-sync-multi-test")
	defer cleanupTestSetup(setup)

	testFiles := map[string][]byte{
		"primary.data":    []byte("Primary data content for testing"),
		"timestamps.data": []byte("Timestamp data: 1234567890"),
		"metadata.json":   []byte(`{"version": 1, "type": "test"}`),
	}

	dataPart := createTestDataPart(testFiles, "test-group-multi", 2)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := setup.ChunkedClient.SyncStreamingParts(ctx, []queue.StreamingPartData{dataPart})
	require.NoError(t, err, "Failed to send data part")
	require.True(t, result.Success, "Sync should be successful")

	time.Sleep(500 * time.Millisecond)

	receivedFiles := setup.MockHandler.GetReceivedFiles()
	require.Len(t, receivedFiles, len(testFiles), "Should receive all files")

	for fileName, expectedContent := range testFiles {
		receivedContent, exists := receivedFiles[fileName]
		require.True(t, exists, "File %s should be received", fileName)
		assert.Equal(t, expectedContent, receivedContent, "Content for file %s should match", fileName)
	}

	t.Logf("Multi-file test completed: %d files synced successfully", len(receivedFiles))
}

// TestChunkedSyncEmptyData tests syncing with empty data.
func TestChunkedSyncEmptyData(t *testing.T) {
	setup := setupChunkedSyncTest(t, "chunked-sync-empty-test")
	defer cleanupTestSetup(setup)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := setup.ChunkedClient.SyncStreamingParts(ctx, []queue.StreamingPartData{})
	require.NoError(t, err, "Empty sync should not fail")
	require.NotNil(t, result, "Result should not be nil")

	assert.True(t, result.Success, "Empty sync should be successful")
	assert.Equal(t, uint32(0), result.PartsCount, "Parts count should be 0")
	assert.Equal(t, uint32(0), result.ChunksCount, "Chunks count should be 0")
	assert.Empty(t, result.SessionID, "Session ID should be empty for no data")
}
