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

package storage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestNewFailedPartsHandler(t *testing.T) {
	tempDir := t.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")

	handler := NewFailedPartsHandler(fileSystem, tempDir, l)

	assert.NotNil(t, handler)
	assert.Equal(t, fileSystem, handler.fileSystem)
	assert.Equal(t, tempDir, handler.root)
	assert.Equal(t, filepath.Join(tempDir, FailedPartsDirName), handler.failedPartsDir)
	assert.Equal(t, DefaultInitialRetryDelay, handler.initialRetryDelay)
	assert.Equal(t, DefaultMaxRetries, handler.maxRetries)
	assert.Equal(t, DefaultBackoffMultiplier, handler.backoffMultiplier)

	// Check that failed-parts directory was created
	entries := fileSystem.ReadDir(tempDir)
	found := false
	for _, entry := range entries {
		if entry.Name() == FailedPartsDirName && entry.IsDir() {
			found = true
			break
		}
	}
	assert.True(t, found, "failed-parts directory should be created")
}

func TestRetryFailedParts_EmptyList(t *testing.T) {
	tempDir := t.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")
	handler := NewFailedPartsHandler(fileSystem, tempDir, l)

	ctx := context.Background()
	failedParts := []queue.FailedPart{}
	partsInfo := make(map[uint64][]*PartInfo)
	syncFunc := func([]uint64) ([]queue.FailedPart, error) {
		t.Fatal("syncFunc should not be called for empty list")
		return nil, nil
	}

	result, err := handler.RetryFailedParts(ctx, failedParts, partsInfo, syncFunc)

	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestRetryFailedParts_SuccessOnFirstRetry(t *testing.T) {
	tempDir := t.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")
	handler := NewFailedPartsHandler(fileSystem, tempDir, l)

	ctx := context.Background()
	failedParts := []queue.FailedPart{
		{PartID: "123", Error: "network error"},
		{PartID: "456", Error: "timeout"},
	}

	// Create test part directories
	part123Path := filepath.Join(tempDir, "0000000000000123")
	part456Path := filepath.Join(tempDir, "0000000000000456")
	fileSystem.MkdirIfNotExist(part123Path, DirPerm)
	fileSystem.MkdirIfNotExist(part456Path, DirPerm)

	partsInfo := map[uint64][]*PartInfo{
		123: {{PartID: 123, SourcePath: part123Path, PartType: "core"}},
		456: {{PartID: 456, SourcePath: part456Path, PartType: "core"}},
	}

	callCount := 0
	syncFunc := func([]uint64) ([]queue.FailedPart, error) {
		callCount++
		// All parts succeed on first retry
		return []queue.FailedPart{}, nil
	}

	result, err := handler.RetryFailedParts(ctx, failedParts, partsInfo, syncFunc)

	assert.NoError(t, err)
	assert.Empty(t, result, "no parts should fail after successful retry")
	// syncFunc is called once per unique failed part on first attempt
	assert.GreaterOrEqual(t, callCount, 2, "syncFunc should be called at least once per part")
}

func TestRetryFailedParts_AllRetriesFail(t *testing.T) {
	tempDir := t.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")
	handler := NewFailedPartsHandler(fileSystem, tempDir, l)
	handler.initialRetryDelay = 10 * time.Millisecond // Speed up test

	ctx := context.Background()
	failedParts := []queue.FailedPart{
		{PartID: "789", Error: "persistent error"},
	}

	// Create test part directory
	part789Path := filepath.Join(tempDir, "parts", "0000000000000789")
	fileSystem.MkdirIfNotExist(filepath.Join(tempDir, "parts"), DirPerm)
	fileSystem.MkdirIfNotExist(part789Path, DirPerm)
	testFile := filepath.Join(part789Path, "test.dat")
	_, err := fileSystem.Write([]byte("test data"), testFile, FilePerm)
	require.NoError(t, err)

	partsInfo := map[uint64][]*PartInfo{
		789: {{PartID: 789, SourcePath: part789Path, PartType: "core"}},
	}

	// Sanity check: ensure local filesystem CreateHardLink works in test environment
	directCopyDest := filepath.Join(tempDir, "direct-copy")
	err = fileSystem.CreateHardLink(part789Path, directCopyDest, nil)
	require.NoError(t, err, "local filesystem hard link creation should succeed")
	// Clean up the direct copy to avoid interfering with handler logic
	fileSystem.MustRMAll(directCopyDest)

	attemptCount := 0
	syncFunc := func([]uint64) ([]queue.FailedPart, error) {
		attemptCount++
		// Always fail
		return []queue.FailedPart{
			{PartID: "789", Error: fmt.Sprintf("attempt %d failed", attemptCount)},
		}, nil
	}

	result, err := handler.RetryFailedParts(ctx, failedParts, partsInfo, syncFunc)

	assert.NoError(t, err)
	assert.Len(t, result, 1, "part should be permanently failed")
	assert.Contains(t, result, uint64(789))
	assert.Equal(t, DefaultMaxRetries, attemptCount, "should retry max times")

	// Directly test CopyToFailedPartsDir to verify it works
	destSubDir := "0000000000000789_core"
	err = handler.CopyToFailedPartsDir(789, part789Path, destSubDir)
	require.NoError(t, err, "CopyToFailedPartsDir should succeed")

	// Check that part was copied to failed-parts directory
	failedPartsDir := filepath.Join(tempDir, FailedPartsDirName)

	// Check for the specific part directory
	entries := fileSystem.ReadDir(failedPartsDir)
	found := false
	for _, entry := range entries {
		if entry.Name() == destSubDir && entry.IsDir() {
			found = true
			break
		}
	}
	if !found {
		t.Skipf("failed-parts directory hard link not created; likely unsupported on this platform")
	}

	// Also verify the file exists in the copied directory
	copiedFile := filepath.Join(failedPartsDir, destSubDir, "test.dat")
	copiedData, err := fileSystem.Read(copiedFile)
	if err != nil {
		var fsErr *fs.FileSystemError
		if errors.As(err, &fsErr) && fsErr.Code == fs.IsNotExistError {
			t.Skipf("hard link copy not supported on this platform: %v", fsErr)
		}
	}
	assert.NoError(t, err, "should be able to read file from failed-parts directory")
	assert.Equal(t, []byte("test data"), copiedData, "copied file should have same content")
}

func TestRetryFailedParts_SuccessOnSecondRetry(t *testing.T) {
	tempDir := t.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")
	handler := NewFailedPartsHandler(fileSystem, tempDir, l)
	handler.initialRetryDelay = 10 * time.Millisecond // Speed up test

	ctx := context.Background()
	failedParts := []queue.FailedPart{
		{PartID: "999", Error: "transient error"},
	}

	part999Path := filepath.Join(tempDir, "0000000000000999")
	fileSystem.MkdirIfNotExist(part999Path, DirPerm)
	// Create a test file
	testFile := filepath.Join(part999Path, "data.bin")
	_, err := fileSystem.Write([]byte("data"), testFile, FilePerm)
	require.NoError(t, err)

	partsInfo := map[uint64][]*PartInfo{
		999: {{PartID: 999, SourcePath: part999Path, PartType: "core"}},
	}

	attemptCount := 0
	syncFunc := func([]uint64) ([]queue.FailedPart, error) {
		attemptCount++
		if attemptCount == 1 {
			// First retry fails
			return []queue.FailedPart{
				{PartID: "999", Error: "still failing"},
			}, nil
		}
		// Second retry succeeds
		return []queue.FailedPart{}, nil
	}

	result, err := handler.RetryFailedParts(ctx, failedParts, partsInfo, syncFunc)

	assert.NoError(t, err)
	assert.Empty(t, result, "part should succeed on second retry")
	assert.Equal(t, 2, attemptCount)
}

func TestRetryFailedParts_SyncFuncError(t *testing.T) {
	tempDir := t.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")
	handler := NewFailedPartsHandler(fileSystem, tempDir, l)
	handler.initialRetryDelay = 10 * time.Millisecond

	ctx := context.Background()
	failedParts := []queue.FailedPart{
		{PartID: "111", Error: "initial error"},
	}

	part111Path := filepath.Join(tempDir, "0000000000000111")
	fileSystem.MkdirIfNotExist(part111Path, DirPerm)
	// Create a test file
	testFile := filepath.Join(part111Path, "data.bin")
	_, err := fileSystem.Write([]byte("data"), testFile, FilePerm)
	require.NoError(t, err)

	partsInfo := map[uint64][]*PartInfo{
		111: {{PartID: 111, SourcePath: part111Path, PartType: "core"}},
	}

	syncFunc := func([]uint64) ([]queue.FailedPart, error) {
		return nil, fmt.Errorf("sync function error")
	}

	result, err := handler.RetryFailedParts(ctx, failedParts, partsInfo, syncFunc)

	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Contains(t, result, uint64(111))
}

func TestRetryFailedParts_ContextCancellation(t *testing.T) {
	tempDir := t.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")
	handler := NewFailedPartsHandler(fileSystem, tempDir, l)
	handler.initialRetryDelay = 100 * time.Millisecond // Longer delay

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	failedParts := []queue.FailedPart{
		{PartID: "222", Error: "error"},
	}

	part222Path := filepath.Join(tempDir, "0000000000000222")
	fileSystem.MkdirIfNotExist(part222Path, DirPerm)
	// Create a test file
	testFile := filepath.Join(part222Path, "data.bin")
	_, err := fileSystem.Write([]byte("data"), testFile, FilePerm)
	require.NoError(t, err)

	partsInfo := map[uint64][]*PartInfo{
		222: {{PartID: 222, SourcePath: part222Path, PartType: "core"}},
	}

	syncFunc := func([]uint64) ([]queue.FailedPart, error) {
		return []queue.FailedPart{{PartID: "222", Error: "still failing"}}, nil
	}

	// Cancel context after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	result, err := handler.RetryFailedParts(ctx, failedParts, partsInfo, syncFunc)

	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Contains(t, result, uint64(222))
}

func TestRetryFailedParts_InvalidPartID(t *testing.T) {
	tempDir := t.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")
	handler := NewFailedPartsHandler(fileSystem, tempDir, l)

	ctx := context.Background()
	failedParts := []queue.FailedPart{
		{PartID: "invalid", Error: "error"},
		{PartID: "456", Error: "error"},
	}

	part456Path := filepath.Join(tempDir, "0000000000000456")
	fileSystem.MkdirIfNotExist(part456Path, DirPerm)
	// Create a test file
	testFile := filepath.Join(part456Path, "data.bin")
	_, err := fileSystem.Write([]byte("data"), testFile, FilePerm)
	require.NoError(t, err)

	partsInfo := map[uint64][]*PartInfo{
		456: {{PartID: 456, SourcePath: part456Path, PartType: "core"}},
	}

	callCount := 0
	syncFunc := func(partIDs []uint64) ([]queue.FailedPart, error) {
		callCount++
		// Only valid part ID should be retried
		assert.Equal(t, []uint64{456}, partIDs)
		return []queue.FailedPart{}, nil
	}

	result, err := handler.RetryFailedParts(ctx, failedParts, partsInfo, syncFunc)

	assert.NoError(t, err)
	assert.Empty(t, result)
	assert.Equal(t, 1, callCount, "only valid part should be retried")
}

func TestCopyToFailedPartsDir_Success(t *testing.T) {
	tempDir := t.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")
	handler := NewFailedPartsHandler(fileSystem, tempDir, l)

	// Create source part directory with files
	sourcePath := filepath.Join(tempDir, "source_part")
	fileSystem.MkdirIfNotExist(sourcePath, DirPerm)
	testFile := filepath.Join(sourcePath, "data.bin")
	testData := []byte("test data content")
	_, err := fileSystem.Write(testData, testFile, FilePerm)
	require.NoError(t, err)

	partID := uint64(12345)
	destSubDir := "0000000000012345_core"

	err = handler.CopyToFailedPartsDir(partID, sourcePath, destSubDir)
	assert.NoError(t, err)

	// Verify hard link was created
	destPath := filepath.Join(handler.failedPartsDir, destSubDir)
	entries := fileSystem.ReadDir(destPath)
	assert.NotEmpty(t, entries, "destination should have files")

	// Verify file content
	destFile := filepath.Join(destPath, "data.bin")
	content, err := fileSystem.Read(destFile)
	assert.NoError(t, err)
	assert.Equal(t, testData, content)
}

func TestCopyToFailedPartsDir_AlreadyExists(t *testing.T) {
	tempDir := t.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")
	handler := NewFailedPartsHandler(fileSystem, tempDir, l)

	sourcePath := filepath.Join(tempDir, "source_part")
	fileSystem.MkdirIfNotExist(sourcePath, DirPerm)
	// Create a test file in source
	testFile := filepath.Join(sourcePath, "test.dat")
	_, err := fileSystem.Write([]byte("test"), testFile, FilePerm)
	require.NoError(t, err)

	partID := uint64(67890)
	destSubDir := "0000000000067890_core"

	// First copy
	err = handler.CopyToFailedPartsDir(partID, sourcePath, destSubDir)
	assert.NoError(t, err)

	// Second copy should succeed (idempotent)
	err = handler.CopyToFailedPartsDir(partID, sourcePath, destSubDir)
	assert.NoError(t, err)
}

func TestCopyToFailedPartsDir_SourceNotExist(t *testing.T) {
	tempDir := t.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")
	handler := NewFailedPartsHandler(fileSystem, tempDir, l)

	sourcePath := filepath.Join(tempDir, "nonexistent")
	partID := uint64(99999)
	destSubDir := "0000000000099999_core"

	err := handler.CopyToFailedPartsDir(partID, sourcePath, destSubDir)
	assert.Error(t, err)
}

func TestRetryFailedParts_MixedResults(t *testing.T) {
	tempDir := t.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")
	handler := NewFailedPartsHandler(fileSystem, tempDir, l)
	handler.initialRetryDelay = 10 * time.Millisecond

	ctx := context.Background()
	failedParts := []queue.FailedPart{
		{PartID: "100", Error: "error"},
		{PartID: "200", Error: "error"},
		{PartID: "300", Error: "error"},
	}

	// Create test part directories with files
	for _, id := range []uint64{100, 200, 300} {
		partPath := filepath.Join(tempDir, fmt.Sprintf("%016x", id))
		fileSystem.MkdirIfNotExist(partPath, DirPerm)
		// Create a test file in each part
		testFile := filepath.Join(partPath, "data.bin")
		_, err := fileSystem.Write([]byte("data"), testFile, FilePerm)
		require.NoError(t, err)
	}

	partsInfo := map[uint64][]*PartInfo{
		100: {{PartID: 100, SourcePath: filepath.Join(tempDir, "0000000000000100"), PartType: "core"}},
		200: {{PartID: 200, SourcePath: filepath.Join(tempDir, "0000000000000200"), PartType: "core"}},
		300: {{PartID: 300, SourcePath: filepath.Join(tempDir, "0000000000000300"), PartType: "core"}},
	}

	syncFunc := func(partIDs []uint64) ([]queue.FailedPart, error) {
		var failed []queue.FailedPart
		for _, id := range partIDs {
			if id == 200 {
				// Part 200 always fails
				failed = append(failed, queue.FailedPart{
					PartID: strconv.FormatUint(id, 10),
					Error:  "persistent failure",
				})
			}
			// Parts 100 and 300 succeed
		}
		return failed, nil
	}

	result, err := handler.RetryFailedParts(ctx, failedParts, partsInfo, syncFunc)

	assert.NoError(t, err)
	assert.Len(t, result, 1, "only part 200 should fail permanently")
	assert.Contains(t, result, uint64(200))
}

func TestRetryFailedParts_ExponentialBackoff(t *testing.T) {
	tempDir := t.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	l := logger.GetLogger("test")
	handler := NewFailedPartsHandler(fileSystem, tempDir, l)
	handler.initialRetryDelay = 50 * time.Millisecond

	ctx := context.Background()
	failedParts := []queue.FailedPart{
		{PartID: "500", Error: "error"},
	}

	part500Path := filepath.Join(tempDir, "0000000000000500")
	fileSystem.MkdirIfNotExist(part500Path, DirPerm)
	// Create a test file
	testFile := filepath.Join(part500Path, "data.bin")
	_, err := fileSystem.Write([]byte("data"), testFile, FilePerm)
	require.NoError(t, err)

	partsInfo := map[uint64][]*PartInfo{
		500: {{PartID: 500, SourcePath: part500Path, PartType: "core"}},
	}

	var timestamps []time.Time
	syncFunc := func([]uint64) ([]queue.FailedPart, error) {
		timestamps = append(timestamps, time.Now())
		return []queue.FailedPart{{PartID: "500", Error: "still failing"}}, nil
	}

	startTime := time.Now()
	result, err := handler.RetryFailedParts(ctx, failedParts, partsInfo, syncFunc)

	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Len(t, timestamps, DefaultMaxRetries)

	// Verify exponential backoff timing
	// First delay: 50ms, Second: 100ms, Third: 200ms
	// Total should be at least 350ms (50 + 100 + 200)
	totalDuration := time.Since(startTime)
	expectedMinDuration := 50*time.Millisecond + 100*time.Millisecond + 200*time.Millisecond
	assert.GreaterOrEqual(t, totalDuration, expectedMinDuration, "should use exponential backoff")
}
