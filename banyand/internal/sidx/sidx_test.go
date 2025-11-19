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
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const (
	testTagStatus = "status"
)

// Test helper functions.

func waitForIntroducerLoop() {
	// Small delay to allow the introducer loop to process writes
	time.Sleep(10 * time.Millisecond)
}

func createTestOptions(t *testing.T) *Options {
	opts := NewDefaultOptions()
	opts.Memory = protector.NewMemory(observability.NewBypassRegistry())
	opts.Path = t.TempDir() // Use temporary directory for tests
	return opts
}

func createTestSIDX(t *testing.T) SIDX {
	return createTestSIDXWithOptions(t, nil)
}

func createTestSIDXWithOptions(t *testing.T, tweak func(*Options)) SIDX {
	fileSystem := fs.NewLocalFileSystem()
	opts := createTestOptions(t)
	if tweak != nil {
		tweak(opts)
	}
	sidx, err := NewSIDX(fileSystem, opts)
	require.NoError(t, err)
	require.NotNil(t, sidx)
	return sidx
}

func writeTestData(t *testing.T, sidx SIDX, reqs []WriteRequest, segmentID int64, partID uint64) {
	// Convert write requests to MemPart
	memPart, err := sidx.ConvertToMemPart(reqs, segmentID)
	require.NoError(t, err)
	require.NotNil(t, memPart)

	// Introduce the MemPart to SIDX
	sidx.IntroduceMemPart(partID, memPart)
}

func createTestTag(name, value string) Tag {
	return Tag{
		Name:      name,
		Value:     []byte(value),
		ValueType: pbv1.ValueTypeStr,
	}
}

func createTestWriteRequest(seriesID common.SeriesID, key int64, data string, tags ...Tag) WriteRequest {
	return WriteRequest{
		SeriesID: seriesID,
		Key:      key,
		Data:     []byte(data),
		Tags:     tags,
	}
}

func createTestQueryRequest(seriesIDs ...common.SeriesID) QueryRequest {
	return QueryRequest{
		SeriesIDs: seriesIDs,
	}
}

// Write Operation Tests.

func TestSIDX_Write_SingleRequest(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	// Test single write request
	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "data1", createTestTag("tag1", "value1")),
	}

	writeTestData(t, sidx, reqs, 1, 1) // Test with segmentID=1, partID=1

	// Verify stats
	stats, err := sidx.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.WriteCount.Load())
}

func TestSIDX_Write_BatchRequest(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	// Test batch write requests
	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "data1", createTestTag("tag1", "value1")),
		createTestWriteRequest(1, 101, "data2", createTestTag("tag1", "value2")),
		createTestWriteRequest(2, 200, "data3", createTestTag("tag2", "value3")),
	}

	writeTestData(t, sidx, reqs, 2, 2) // Test with segmentID=2, partID=2

	// Verify stats
	stats, err := sidx.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.WriteCount.Load()) // One batch write
}

func TestSIDX_Take_File_Snapshot(t *testing.T) {
	logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})

	t.Run("Take snapshot of existing sidx", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		snapshotDir := filepath.Join(dir, "snapshot")

		idx := createTestSIDX(t)
		defer func() {
			assert.NoError(t, idx.Close())
		}()

		// Test batch write requests
		reqs := []WriteRequest{
			createTestWriteRequest(1, 100, "data1", createTestTag("tag1", "value1")),
			createTestWriteRequest(1, 101, "data2", createTestTag("tag1", "value2")),
			createTestWriteRequest(2, 200, "data3", createTestTag("tag2", "value3")),
		}

		writeTestData(t, idx, reqs, 2, 2) // Test with segmentID=2, partID=2

		raw := idx.(*sidx)
		flushIntro, err := raw.Flush(map[uint64]struct{}{2: {}})
		require.NoError(t, err)
		require.NotNil(t, flushIntro)
		raw.IntroduceFlushed(flushIntro)
		flushIntro.Release()

		err = idx.TakeFileSnapshot(snapshotDir)
		assert.NoError(t, err)
	})
}

func TestSIDX_Write_Validation(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	tests := []struct {
		name      string
		req       WriteRequest
		expectErr bool
	}{
		{
			name:      "valid request",
			req:       createTestWriteRequest(1, 100, "data1"),
			expectErr: false,
		},
		{
			name:      "zero series ID",
			req:       WriteRequest{SeriesID: 0, Key: 100, Data: []byte("data")},
			expectErr: true,
		},
		{
			name:      "nil data",
			req:       WriteRequest{SeriesID: 1, Key: 100, Data: nil},
			expectErr: true,
		},
		{
			name:      "empty data",
			req:       WriteRequest{SeriesID: 1, Key: 100, Data: []byte("")},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sidx.ConvertToMemPart([]WriteRequest{tt.req}, 12) // Test with segmentID=12
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSIDX_Write_WithTags(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	// Write with multiple tags
	tags := []Tag{
		createTestTag("service", "user-service"),
		createTestTag("endpoint", "/api/users"),
		createTestTag(testTagStatus, "200"),
	}

	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "trace-data", tags...),
	}

	writeTestData(t, sidx, reqs, 3, 3) // Test with segmentID=3, partID=3
}

func TestSIDX_PartPaths(t *testing.T) {
	sidxIface := createTestSIDX(t)
	raw := sidxIface.(*sidx)
	defer func() {
		assert.NoError(t, raw.Close())
	}()

	const (
		flushedID = uint64(101)
		memOnlyID = uint64(202)
		missingID = uint64(303)
	)

	req := []WriteRequest{
		createTestWriteRequest(1, 100, "flushed-data"),
	}

	// Introduce a part that will be flushed to disk and another that stays in memory.
	writeTestData(t, raw, req, 1, flushedID)
	writeTestData(t, raw, req, 2, memOnlyID)

	flushIntro, err := raw.Flush(map[uint64]struct{}{flushedID: {}})
	require.NoError(t, err)
	require.NotNil(t, flushIntro)
	raw.IntroduceFlushed(flushIntro)
	flushIntro.Release()

	// Empty request should return an empty map.
	require.Empty(t, raw.PartPaths(map[uint64]struct{}{}))

	paths := raw.PartPaths(map[uint64]struct{}{
		flushedID: {},
		memOnlyID: {},
		missingID: {},
	})

	require.Len(t, paths, 1)
	expectedPath := partPath(raw.root, flushedID)
	assert.Equal(t, expectedPath, paths[flushedID])
	assert.NotContains(t, paths, memOnlyID)
	assert.NotContains(t, paths, missingID)
}

// End-to-End Integration Tests.

func TestSIDX_WriteQueryIntegration(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	// Write comprehensive test dataset
	reqs := []WriteRequest{
		// Series 1 data
		createTestWriteRequest(1, 100, "series1-data1", createTestTag("env", "prod")),
		createTestWriteRequest(1, 150, "series1-data2", createTestTag("env", "prod")),
		createTestWriteRequest(1, 200, "series1-data3", createTestTag("env", "test")),

		// Series 2 data
		createTestWriteRequest(2, 120, "series2-data1", createTestTag("env", "dev")),
		createTestWriteRequest(2, 180, "series2-data2", createTestTag("env", "dev")),
	}

	writeTestData(t, sidx, reqs, 8, 8) // Test with segmentID=8, partID=8

	// Test 1: Query single series
	queryReq := createTestQueryRequest(1)
	resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)
	var series1Keys []int64
	for res := range resultsCh {
		require.NoError(t, res.Error)
		series1Keys = append(series1Keys, res.Keys...)
	}
	if err, ok := <-errCh; ok {
		require.NoError(t, err)
	}
	assert.Greater(t, len(series1Keys), 0, "Should find data for series 1")

	// Test 2: Query multiple series
	queryReq2 := createTestQueryRequest(1, 2)
	resultsCh2, errCh2 := sidx.StreamingQuery(ctx, queryReq2)
	var allKeys []int64
	for res := range resultsCh2 {
		require.NoError(t, res.Error)
		allKeys = append(allKeys, res.Keys...)
	}
	if err, ok := <-errCh2; ok {
		require.NoError(t, err)
	}
	assert.GreaterOrEqual(t, len(allKeys), len(series1Keys), "Should find at least as much data for multiple series")

	// Test 3: Query with key range that spans both series
	minKey := int64(110)
	maxKey := int64(190)
	queryReq3 := QueryRequest{
		SeriesIDs: []common.SeriesID{1, 2},
		MinKey:    &minKey,
		MaxKey:    &maxKey,
	}

	resultsCh3, errCh3 := sidx.StreamingQuery(ctx, queryReq3)
	var rangeKeys []int64
	for res := range resultsCh3 {
		require.NoError(t, res.Error)
		rangeKeys = append(rangeKeys, res.Keys...)
	}
	if err, ok := <-errCh3; ok {
		require.NoError(t, err)
	}
	for _, key := range rangeKeys {
		assert.GreaterOrEqual(t, key, minKey)
		assert.LessOrEqual(t, key, maxKey)
	}
}

func TestSIDX_DataConsistency(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	// Write known test data
	expectedData := map[int64]string{
		100: "data-100",
		200: "data-200",
		300: "data-300",
	}

	var reqs []WriteRequest
	for key, data := range expectedData {
		reqs = append(reqs, createTestWriteRequest(1, key, data))
	}

	writeTestData(t, sidx, reqs, 9, 9) // Test with segmentID=9, partID=9

	// Query back and verify data integrity
	queryReq := createTestQueryRequest(1)
	resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)

	var keys []int64
	var data [][]byte
	for res := range resultsCh {
		require.NoError(t, res.Error)
		keys = append(keys, res.Keys...)
		data = append(data, res.Data...)
	}
	if err, ok := <-errCh; ok {
		require.NoError(t, err)
	}

	actualData := make(map[int64]string)
	for i, key := range keys {
		if i < len(data) {
			actualData[key] = string(data[i])
		}
	}

	// Verify all data was retrieved correctly
	for expectedKey, expectedValue := range expectedData {
		actualValue, exists := actualData[expectedKey]
		assert.True(t, exists, "Key %d should exist in results", expectedKey)
		assert.Equal(t, expectedValue, actualValue, "Data for key %d should match", expectedKey)
	}
}

// Error Handling and Edge Cases.

func TestSIDX_LargeDataset(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	// Write a large number of elements
	const numElements = 1000
	var reqs []WriteRequest

	for i := 0; i < numElements; i++ {
		reqs = append(reqs, createTestWriteRequest(
			common.SeriesID(i%10+1), // 10 different series
			int64(i),
			fmt.Sprintf("data-%d", i),
			createTestTag("batch", "large"),
		))
	}

	writeTestData(t, sidx, reqs, 10, 10) // Test with segmentID=10, partID=10

	// Query back and verify we can handle large result sets
	queryReq := QueryRequest{
		SeriesIDs: []common.SeriesID{1, 2, 3}, // Query subset of series
	}

	resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)
	totalElements := 0
	for res := range resultsCh {
		require.NoError(t, res.Error)
		totalElements += res.Len()
	}
	if err, ok := <-errCh; ok {
		require.NoError(t, err)
	}

	assert.Greater(t, totalElements, 0, "Should find elements in large dataset")
}

// Concurrency Tests.

func TestSIDX_ConcurrentWrites(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()
	numGoroutines := 10
	elementsPerGoroutine := 100

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	// Launch concurrent writers
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			var reqs []WriteRequest
			for i := 0; i < elementsPerGoroutine; i++ {
				seriesID := common.SeriesID(goroutineID + 1)
				key := int64(goroutineID*1000 + i)
				data := fmt.Sprintf("goroutine-%d-data-%d", goroutineID, i)

				reqs = append(reqs, createTestWriteRequest(seriesID, key, data))
			}

			// Convert to MemPart and introduce
			memPart, err := sidx.ConvertToMemPart(reqs, int64(goroutineID+12)) // Test with varied segmentID
			if err != nil {
				errors <- err
				return
			}
			sidx.IntroduceMemPart(uint64(goroutineID+12), memPart) // Test with varied partID
		}(g)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		assert.NoError(t, err)
	}

	// Verify stats reflect all writes
	stats, err := sidx.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(numGoroutines), stats.WriteCount.Load())
}

func TestSIDX_ConcurrentReadsWrites(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	// Pre-populate with some data
	initialReqs := []WriteRequest{
		createTestWriteRequest(1, 100, "initial-data"),
	}
	writeTestData(t, sidx, initialReqs, 11, 11) // Test with segmentID=11, partID=11

	var wg sync.WaitGroup
	numReaders := 5
	numWriters := 5
	duration := 100 * time.Millisecond

	// Launch concurrent readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(_ int) {
			defer wg.Done()

			start := time.Now()
			for time.Since(start) < duration {
				queryReq := createTestQueryRequest(1)
				resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)
				count := 0
				for res := range resultsCh {
					if res.Error != nil {
						continue
					}
					count += res.Len()
				}
				if err, ok := <-errCh; ok && err != nil {
					continue // Continue on error during concurrent access
				}
				_ = count // Just access the data
			}
		}(i)
	}

	// Launch concurrent writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			start := time.Now()
			writeCount := 0
			for time.Since(start) < duration {
				req := createTestWriteRequest(
					common.SeriesID(writerID+2),
					int64(writeCount),
					fmt.Sprintf("writer-%d-data-%d", writerID, writeCount),
				)
				// Convert to MemPart and introduce (ignore errors during concurrent stress)
				if memPart, err := sidx.ConvertToMemPart([]WriteRequest{req}, int64(writerID+13)); err == nil { // Test with varied segmentID
					sidx.IntroduceMemPart(uint64(writerID+13), memPart) // Test with varied partID
				}
				writeCount++
			}
		}(i)
	}

	wg.Wait()

	// Final verification - should still be able to query
	queryReq := createTestQueryRequest(1)
	resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)
	count := 0
	for res := range resultsCh {
		require.NoError(t, res.Error)
		count += res.Len()
	}
	if err, ok := <-errCh; ok {
		require.NoError(t, err)
	}
}

// Utility functions.

func ptrInt64(v int64) *int64 {
	return &v
}

// TestQueryResult_MaxBatchSize verifies that MaxBatchSize limits the total number of elements appended.
func TestQueryResult_MaxBatchSize(t *testing.T) {
	tests := []struct {
		name           string
		inputKeys      []int64
		inputData      [][]byte
		expectedKeys   []int64
		expectedData   [][]byte
		maxElementSize int
	}{
		{
			name:           "no_limit",
			maxElementSize: 0,
			inputKeys:      []int64{100, 101, 200, 201},
			inputData: [][]byte{
				[]byte("trace1"), []byte("trace1"), []byte("trace2"), []byte("trace2"),
			},
			expectedKeys: []int64{100, 101, 200, 201},
			expectedData: [][]byte{
				[]byte("trace1"), []byte("trace1"), []byte("trace2"), []byte("trace2"),
			},
		},
		{
			name:           "limit_one",
			maxElementSize: 1,
			inputKeys:      []int64{100, 101, 200},
			inputData: [][]byte{
				[]byte("trace1"), []byte("trace1"), []byte("trace2"),
			},
			expectedKeys: []int64{100},
			expectedData: [][]byte{
				[]byte("trace1"),
			},
		},
		{
			name:           "limit_two",
			maxElementSize: 2,
			inputKeys:      []int64{100, 101, 200, 201},
			inputData: [][]byte{
				[]byte("trace1"), []byte("trace2"), []byte("trace3"), []byte("trace4"),
			},
			expectedKeys: []int64{100, 101},
			expectedData: [][]byte{
				[]byte("trace1"), []byte("trace2"),
			},
		},
		{
			name:           "limit_exceeds_total",
			maxElementSize: 10,
			inputKeys:      []int64{100, 101, 102},
			inputData: [][]byte{
				[]byte("trace1"), []byte("trace2"), []byte("trace3"),
			},
			expectedKeys: []int64{100, 101, 102},
			expectedData: [][]byte{
				[]byte("trace1"), []byte("trace2"), []byte("trace3"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qr := &queryResult{
				request: QueryRequest{
					MaxBatchSize: tt.maxElementSize,
				},
			}

			block := &block{
				userKeys: tt.inputKeys,
				data:     tt.inputData,
				tags:     make(map[string]*tagData),
			}

			result := &QueryResponse{
				Keys:    make([]int64, 0),
				Data:    make([][]byte, 0),
				SIDs:    make([]common.SeriesID, 0),
				PartIDs: make([]uint64, 0),
			}

			qr.convertBlockToResponse(block, 1, 1, result)

			assert.Equal(t, tt.expectedKeys, result.Keys)
			assert.Equal(t, tt.expectedData, result.Data)

			assert.Equal(t, len(result.Keys), len(result.Data), "Keys and Data arrays should have same length")
			assert.Equal(t, len(result.Keys), len(result.SIDs), "Keys and SIDs arrays should have same length")
			assert.Equal(t, len(result.Keys), len(result.PartIDs), "Keys and PartIDs arrays should have same length")
		})
	}
}

// TestQueryResult_ConvertBlockToResponse_RespectsLimitAcrossCalls ensures subsequent calls do not exceed the limit.
func TestQueryResult_ConvertBlockToResponse_RespectsLimitAcrossCalls(t *testing.T) {
	qr := &queryResult{
		request: QueryRequest{
			MaxBatchSize: 2,
		},
	}

	result := &QueryResponse{
		Keys:    make([]int64, 0),
		Data:    make([][]byte, 0),
		SIDs:    make([]common.SeriesID, 0),
		PartIDs: make([]uint64, 0),
	}

	// First call: add trace1 data
	block1 := &block{
		userKeys: []int64{100, 101},
		data:     [][]byte{[]byte("trace1"), []byte("trace1")},
		tags:     make(map[string]*tagData),
	}
	qr.convertBlockToResponse(block1, 1, 1, result)

	// Verify first call results
	assert.Equal(t, 2, result.Len(), "First call should add 2 elements")

	// Second call: try to add trace2 data (should be skipped because limit reached)
	block2 := &block{
		userKeys: []int64{200, 201},
		data:     [][]byte{[]byte("trace2"), []byte("trace2")},
		tags:     make(map[string]*tagData),
	}
	qr.convertBlockToResponse(block2, 2, 2, result)

	assert.Equal(t, 2, result.Len(), "Second call should not add elements beyond limit")

	// Third call: try to add trace3 data (still skipped)
	block3 := &block{
		userKeys: []int64{300, 301},
		data:     [][]byte{[]byte("trace3"), []byte("trace3")},
		tags:     make(map[string]*tagData),
	}
	qr.convertBlockToResponse(block3, 3, 3, result)

	assert.Equal(t, 2, result.Len(), "Result length should remain capped at the limit")
	assert.Equal(t, [][]byte{[]byte("trace1"), []byte("trace1")}, result.Data)
	assert.Equal(t, []int64{100, 101}, result.Keys)
}

// TestQueryResponseHeap_MergeWithHeap verifies merge functionality with element count limiting.
func TestQueryResponseHeap_MergeWithHeap(t *testing.T) {
	tests := []struct {
		name         string
		shards       []*QueryResponse
		expectedKeys []int64
		expectedData []string
		limit        int
	}{
		{
			name:  "no_limit",
			limit: 0,
			shards: []*QueryResponse{
				{
					Keys: []int64{100, 300},
					Data: [][]byte{[]byte("trace1"), []byte("trace3")},
					SIDs: []common.SeriesID{1, 3},
				},
				{
					Keys: []int64{200, 400},
					Data: [][]byte{[]byte("trace2"), []byte("trace4")},
					SIDs: []common.SeriesID{2, 4},
				},
			},
			expectedKeys: []int64{100, 200, 300, 400},
			expectedData: []string{"trace1", "trace2", "trace3", "trace4"},
		},
		{
			name:  "with_limit",
			limit: 2,
			shards: []*QueryResponse{
				{
					Keys: []int64{100, 300},
					Data: [][]byte{[]byte("trace1"), []byte("trace3")},
					SIDs: []common.SeriesID{1, 3},
				},
				{
					Keys: []int64{200, 400},
					Data: [][]byte{[]byte("trace2"), []byte("trace4")},
					SIDs: []common.SeriesID{2, 4},
				},
			},
			expectedKeys: []int64{100, 200},
			expectedData: []string{"trace1", "trace2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mergeQueryResponseShards(tt.shards, tt.limit)

			assert.Equal(t, tt.expectedKeys, result.Keys)
			actual := make([]string, len(result.Data))
			for i, b := range result.Data {
				actual[i] = string(b)
			}
			assert.Equal(t, tt.expectedData, actual)

			assert.Equal(t, len(result.Keys), len(result.Data), "Keys and Data arrays should have same length")
			assert.Equal(t, len(result.Keys), len(result.SIDs), "Keys and SIDs arrays should have same length")
		})
	}
}

// TestQueryResponseHeap_MergeDescending verifies descending merge respects the element limit.
func TestQueryResponseHeap_MergeDescending(t *testing.T) {
	shards := []*QueryResponse{
		{
			Keys: []int64{200, 400}, // stored ascending
			Data: [][]byte{[]byte("trace2"), []byte("trace4")},
			SIDs: []common.SeriesID{2, 4},
		},
		{
			Keys: []int64{150, 300}, // stored ascending
			Data: [][]byte{[]byte("trace1"), []byte("trace3")},
			SIDs: []common.SeriesID{1, 3},
		},
	}

	result := mergeQueryResponseShardsDesc(shards, 3)
	assert.Equal(t, []int64{400, 300, 200}, result.Keys)
	actual := []string{string(result.Data[0]), string(result.Data[1]), string(result.Data[2])}
	assert.Equal(t, []string{"trace4", "trace3", "trace2"}, actual)

	result = mergeQueryResponseShardsDesc(shards, 2)
	assert.Equal(t, []int64{400, 300}, result.Keys)
	actual = []string{string(result.Data[0]), string(result.Data[1])}
	assert.Equal(t, []string{"trace4", "trace3"}, actual)
}

// TestQueryResponse_Reset ensures Reset clears the response content.
func TestQueryResponse_Reset(t *testing.T) {
	result := &QueryResponse{
		Keys: []int64{100, 200},
		Data: [][]byte{[]byte("trace1"), []byte("trace2")},
		SIDs: []common.SeriesID{1, 2},
		Metadata: ResponseMetadata{
			Warnings:        []string{"warn"},
			ExecutionTimeMs: 5,
		},
	}

	result.Reset()

	assert.Equal(t, 0, result.Len())
	assert.Nil(t, result.Error)
	assert.Equal(t, 0, len(result.Keys))
	assert.Equal(t, 0, len(result.Data))
	assert.Equal(t, 0, len(result.SIDs))
	assert.Equal(t, ResponseMetadata{}, result.Metadata)
}

type mockTagFilterMatcher struct {
	matchFunc func(tags []*modelv1.Tag) (bool, error)
	decoder   model.TagValueDecoder
}

func (m *mockTagFilterMatcher) Match(tags []*modelv1.Tag) (bool, error) {
	if m.matchFunc == nil {
		return true, nil
	}
	return m.matchFunc(tags)
}

func (m *mockTagFilterMatcher) GetDecoder() model.TagValueDecoder {
	return m.decoder
}

func testTagValueDecoder(valueType pbv1.ValueType, value []byte, _ [][]byte) *modelv1.TagValue {
	if value == nil {
		return pbv1.NullTagValue
	}
	switch valueType {
	case pbv1.ValueTypeStr:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{Value: string(value)},
			},
		}
	case pbv1.ValueTypeInt64:
		// For testing, we'll parse the string as int
		var intVal int64
		fmt.Sscanf(string(value), "%d", &intVal)
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Int{
				Int: &modelv1.Int{Value: intVal},
			},
		}
	default:
		return pbv1.NullTagValue
	}
}

func TestSIDX_TagFilter(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "trace1-span1",
			Tag{Name: testTagStatus, Value: []byte("200"), ValueType: pbv1.ValueTypeStr},
			Tag{Name: "service", Value: []byte("user-service"), ValueType: pbv1.ValueTypeStr},
		),
		createTestWriteRequest(1, 101, "trace1-span2",
			Tag{Name: testTagStatus, Value: []byte("404"), ValueType: pbv1.ValueTypeStr},
			Tag{Name: "service", Value: []byte("user-service"), ValueType: pbv1.ValueTypeStr},
		),
		createTestWriteRequest(1, 102, "trace1-span3",
			Tag{Name: testTagStatus, Value: []byte("200"), ValueType: pbv1.ValueTypeStr},
			Tag{Name: "service", Value: []byte("order-service"), ValueType: pbv1.ValueTypeStr},
		),
		createTestWriteRequest(1, 103, "trace1-span4",
			Tag{Name: testTagStatus, Value: []byte("500"), ValueType: pbv1.ValueTypeStr},
			Tag{Name: "service", Value: []byte("payment-service"), ValueType: pbv1.ValueTypeStr},
		),
	}

	writeTestData(t, sidx, reqs, 1, 1)

	tests := []struct {
		filterFunc    func(tags []*modelv1.Tag) (bool, error)
		name          string
		description   string
		expectedCount int
	}{
		{
			name: "filter_by_status_200",
			filterFunc: func(tags []*modelv1.Tag) (bool, error) {
				for _, tag := range tags {
					if tag.Key == testTagStatus && tag.Value.GetStr() != nil && tag.Value.GetStr().Value == "200" {
						return true, nil
					}
				}
				return false, nil
			},
			expectedCount: 2, // span1 and span3 have status=200
			description:   "Should return only spans with status=200",
		},
		{
			name: "filter_by_service_user_service",
			filterFunc: func(tags []*modelv1.Tag) (bool, error) {
				for _, tag := range tags {
					if tag.Key == "service" && tag.Value.GetStr() != nil && tag.Value.GetStr().Value == "user-service" {
						return true, nil
					}
				}
				return false, nil
			},
			expectedCount: 2, // span1 and span2 have service=user-service
			description:   "Should return only spans with service=user-service",
		},
		{
			name: "filter_by_status_500",
			filterFunc: func(tags []*modelv1.Tag) (bool, error) {
				for _, tag := range tags {
					if tag.Key == testTagStatus && tag.Value.GetStr() != nil && tag.Value.GetStr().Value == "500" {
						return true, nil
					}
				}
				return false, nil
			},
			expectedCount: 1, // only span4 has status=500
			description:   "Should return only spans with status=500",
		},
		{
			name: "filter_excludes_all",
			filterFunc: func(tags []*modelv1.Tag) (bool, error) {
				for _, tag := range tags {
					if tag.Key == testTagStatus && tag.Value.GetStr() != nil && tag.Value.GetStr().Value == "999" {
						return true, nil
					}
				}
				return false, nil
			},
			expectedCount: 0, // no spans match
			description:   "Should return no spans when filter matches nothing",
		},
		{
			name: "no_filter",
			filterFunc: func(_ []*modelv1.Tag) (bool, error) {
				return true, nil
			},
			expectedCount: 4, // all spans match
			description:   "Should return all spans when filter always returns true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create tag filter matcher
			tagFilter := &mockTagFilterMatcher{
				matchFunc: tt.filterFunc,
				decoder:   testTagValueDecoder,
			}

			// Create query request with tag filter
			queryReq := QueryRequest{
				SeriesIDs: []common.SeriesID{1},
				TagFilter: tagFilter,
			}

			// Execute query
			resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)

			var totalElements int
			var allKeys []int64
			for res := range resultsCh {
				require.NoError(t, res.Error)
				totalElements += res.Len()
				allKeys = append(allKeys, res.Keys...)
			}
			if err, ok := <-errCh; ok {
				require.NoError(t, err)
			}

			// Verify the filtered results
			assert.Equal(t, tt.expectedCount, totalElements, tt.description)

			// Verify keys are unique
			keySet := make(map[int64]bool)
			for _, key := range allKeys {
				assert.False(t, keySet[key], "Duplicate key found: %d", key)
				keySet[key] = true
			}
		})
	}
}

func TestSIDX_TagFilterWithMultipleTags(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	reqs := []WriteRequest{
		createTestWriteRequest(1, 200, "trace2-span1",
			Tag{Name: testTagStatus, Value: []byte("200"), ValueType: pbv1.ValueTypeStr},
			Tag{Name: "method", Value: []byte("GET"), ValueType: pbv1.ValueTypeStr},
			Tag{Name: "endpoint", Value: []byte("/api/users"), ValueType: pbv1.ValueTypeStr},
		),
		createTestWriteRequest(1, 201, "trace2-span2",
			Tag{Name: testTagStatus, Value: []byte("200"), ValueType: pbv1.ValueTypeStr},
			Tag{Name: "method", Value: []byte("POST"), ValueType: pbv1.ValueTypeStr},
			Tag{Name: "endpoint", Value: []byte("/api/users"), ValueType: pbv1.ValueTypeStr},
		),
		createTestWriteRequest(1, 202, "trace2-span3",
			Tag{Name: testTagStatus, Value: []byte("404"), ValueType: pbv1.ValueTypeStr},
			Tag{Name: "method", Value: []byte("GET"), ValueType: pbv1.ValueTypeStr},
			Tag{Name: "endpoint", Value: []byte("/api/orders"), ValueType: pbv1.ValueTypeStr},
		),
	}

	writeTestData(t, sidx, reqs, 2, 2)

	// Test: Filter by status=200 AND method=GET
	tagFilter := &mockTagFilterMatcher{
		matchFunc: func(tags []*modelv1.Tag) (bool, error) {
			hasStatus200 := false
			hasMethodGET := false
			for _, tag := range tags {
				if tag.Key == testTagStatus && tag.Value.GetStr() != nil && tag.Value.GetStr().Value == "200" {
					hasStatus200 = true
				}
				if tag.Key == "method" && tag.Value.GetStr() != nil && tag.Value.GetStr().Value == "GET" {
					hasMethodGET = true
				}
			}
			return hasStatus200 && hasMethodGET, nil
		},
		decoder: testTagValueDecoder,
	}

	queryReq := QueryRequest{
		SeriesIDs: []common.SeriesID{1},
		TagFilter: tagFilter,
	}

	resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)

	var totalElements int
	var keys []int64
	for res := range resultsCh {
		require.NoError(t, res.Error)
		totalElements += res.Len()
		keys = append(keys, res.Keys...)
	}
	if err, ok := <-errCh; ok {
		require.NoError(t, err)
	}

	// Only span1 should match (status=200 AND method=GET)
	assert.Equal(t, 1, totalElements, "Should return only one span matching both conditions")
	assert.Equal(t, []int64{200}, keys, "Should return key 200")
}

func TestSIDX_TagFilterWithEmptyTags(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	reqs := []WriteRequest{
		createTestWriteRequest(1, 300, "trace3-span1",
			Tag{Name: testTagStatus, Value: []byte("200"), ValueType: pbv1.ValueTypeStr},
		),
		createTestWriteRequest(1, 301, "trace3-span2"), // No tags
		createTestWriteRequest(1, 302, "trace3-span3",
			Tag{Name: testTagStatus, Value: []byte("404"), ValueType: pbv1.ValueTypeStr},
		),
	}

	writeTestData(t, sidx, reqs, 3, 3)

	// Test: Filter by status=200
	tagFilter := &mockTagFilterMatcher{
		matchFunc: func(tags []*modelv1.Tag) (bool, error) {
			for _, tag := range tags {
				if tag.Key == testTagStatus && tag.Value.GetStr() != nil && tag.Value.GetStr().Value == "200" {
					return true, nil
				}
			}
			return false, nil
		},
		decoder: testTagValueDecoder,
	}

	queryReq := QueryRequest{
		SeriesIDs: []common.SeriesID{1},
		TagFilter: tagFilter,
	}

	resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)

	var totalElements int
	for res := range resultsCh {
		require.NoError(t, res.Error)
		totalElements += res.Len()
	}
	if err, ok := <-errCh; ok {
		require.NoError(t, err)
	}

	// Only span1 should match (span2 has no tags, span3 has status=404)
	assert.Equal(t, 1, totalElements, "Should filter out spans with no matching tags")
}

func TestBlockCursorHeap_Deduplication(t *testing.T) {
	// Helper to create a test part
	createTestPart := func(id uint64) *part {
		return &part{
			partMetadata: &partMetadata{
				ID: id,
			},
		}
	}

	tests := []struct {
		name          string
		cursors       []*blockCursor
		expectedData  []string
		expectedKeys  []int64
		batchSize     int
		expectedCount int
		asc           bool
	}{
		{
			name: "duplicate data with different keys - ascending",
			cursors: []*blockCursor{
				{
					userKeys: []int64{100, 101, 102},
					data:     [][]byte{[]byte("data1"), []byte("data2"), []byte("data1")},
					seriesID: 1,
					idx:      0,
					p:        createTestPart(1),
					request:  QueryRequest{},
				},
				{
					userKeys: []int64{103, 104},
					data:     [][]byte{[]byte("data2"), []byte("data3")},
					seriesID: 1,
					idx:      0,
					p:        createTestPart(1),
					request:  QueryRequest{},
				},
			},
			batchSize:     10,
			asc:           true,
			expectedData:  []string{"data1", "data2", "data3"},
			expectedKeys:  []int64{100, 101, 104},
			expectedCount: 3,
		},
		{
			name: "duplicate data with different keys - descending",
			cursors: []*blockCursor{
				{
					userKeys: []int64{100, 101, 102},
					data:     [][]byte{[]byte("data1"), []byte("data2"), []byte("data1")},
					seriesID: 1,
					idx:      2, // Start from end for descending
					p:        createTestPart(1),
					request:  QueryRequest{},
				},
				{
					userKeys: []int64{103, 104},
					data:     [][]byte{[]byte("data2"), []byte("data3")},
					seriesID: 1,
					idx:      1, // Start from end for descending
					p:        createTestPart(1),
					request:  QueryRequest{},
				},
			},
			batchSize:     10,
			asc:           false,
			expectedData:  []string{"data3", "data2", "data1"},
			expectedKeys:  []int64{104, 103, 102},
			expectedCount: 3,
		},
		{
			name: "all duplicate data",
			cursors: []*blockCursor{
				{
					userKeys: []int64{100, 101, 102},
					data:     [][]byte{[]byte("data1"), []byte("data1"), []byte("data1")},
					seriesID: 1,
					idx:      0,
					p:        createTestPart(1),
					request:  QueryRequest{},
				},
			},
			batchSize:     10,
			asc:           true,
			expectedData:  []string{"data1"},
			expectedKeys:  []int64{100},
			expectedCount: 1,
		},
		{
			name: "no duplicates",
			cursors: []*blockCursor{
				{
					userKeys: []int64{100, 101, 102},
					data:     [][]byte{[]byte("data1"), []byte("data2"), []byte("data3")},
					seriesID: 1,
					idx:      0,
					p:        createTestPart(1),
					request:  QueryRequest{},
				},
			},
			batchSize:     10,
			asc:           true,
			expectedData:  []string{"data1", "data2", "data3"},
			expectedKeys:  []int64{100, 101, 102},
			expectedCount: 3,
		},
		{
			name: "duplicate data across batch boundaries",
			cursors: []*blockCursor{
				{
					userKeys: []int64{100, 101, 102, 103},
					data:     [][]byte{[]byte("data1"), []byte("data2"), []byte("data1"), []byte("data3")},
					seriesID: 1,
					idx:      0,
					p:        createTestPart(1),
					request:  QueryRequest{},
				},
			},
			batchSize:     2, // Small batch to test cross-batch behavior
			asc:           true,
			expectedData:  []string{"data1", "data2", "data3"},
			expectedKeys:  []int64{100, 101, 103},
			expectedCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			resultsCh := make(chan *QueryResponse, 10)

			// Create heap
			bch := generateBlockCursorHeap(tt.asc)
			defer releaseBlockCursorHeap(bch)

			// Push cursors
			bch.pushCursors(tt.cursors)

			// Run merge in a goroutine
			go func() {
				err := bch.merge(ctx, tt.batchSize, resultsCh, nil)
				require.NoError(t, err)
				close(resultsCh)
			}()

			// Collect results
			var allData []string
			var allKeys []int64
			for batch := range resultsCh {
				for i, data := range batch.Data {
					allData = append(allData, string(data))
					if i < len(batch.Keys) {
						allKeys = append(allKeys, batch.Keys[i])
					}
				}
			}

			// Verify results
			assert.Equal(t, tt.expectedCount, len(allData), "Expected count mismatch")
			assert.Equal(t, tt.expectedData, allData, "Expected data mismatch")
			assert.Equal(t, tt.expectedKeys, allKeys, "Expected keys mismatch")
		})
	}
}

func TestBlockCursorHeap_DeduplicationIntegration(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	// Write test data with duplicates (same data, different keys)
	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "duplicate_data"),
		createTestWriteRequest(1, 101, "unique_data_1"),
		createTestWriteRequest(1, 102, "duplicate_data"), // Same data as key 100
		createTestWriteRequest(1, 103, "unique_data_2"),
		createTestWriteRequest(1, 104, "duplicate_data"), // Same data as key 100 and 102
	}
	writeTestData(t, sidx, reqs, 1, 1)

	waitForIntroducerLoop()

	queryReq := createTestQueryRequest(1)
	resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)

	// Collect all data
	dataSet := make(map[string]int)
	for res := range resultsCh {
		require.NoError(t, res.Error)
		for _, data := range res.Data {
			dataSet[string(data)]++
		}
	}
	if err, ok := <-errCh; ok {
		require.NoError(t, err)
	}

	// Verify deduplication: each unique data should appear only once
	assert.Equal(t, 1, dataSet["duplicate_data"], "duplicate_data should appear only once")
	assert.Equal(t, 1, dataSet["unique_data_1"], "unique_data_1 should appear only once")
	assert.Equal(t, 1, dataSet["unique_data_2"], "unique_data_2 should appear only once")
	assert.Equal(t, 3, len(dataSet), "Should have exactly 3 unique data entries")

	// Verify that we have 3 unique results (not 5)
	totalCount := 0
	for _, count := range dataSet {
		totalCount += count
	}
	assert.Equal(t, 3, totalCount, "Should have 3 total results after deduplication")
}

func TestLoadBlockCursor_DeduplicationWithoutFilter(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	// Write test data with duplicates
	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "data1",
			Tag{Name: "tag1", Value: []byte("value1"), ValueType: pbv1.ValueTypeStr},
		),
		createTestWriteRequest(1, 101, "data2",
			Tag{Name: "tag1", Value: []byte("value2"), ValueType: pbv1.ValueTypeStr},
		),
		createTestWriteRequest(1, 102, "data1", // Duplicate data
			Tag{Name: "tag1", Value: []byte("value3"), ValueType: pbv1.ValueTypeStr},
		),
		createTestWriteRequest(1, 103, "data3",
			Tag{Name: "tag1", Value: []byte("value4"), ValueType: pbv1.ValueTypeStr},
		),
		createTestWriteRequest(1, 104, "data2", // Duplicate data
			Tag{Name: "tag1", Value: []byte("value5"), ValueType: pbv1.ValueTypeStr},
		),
	}

	writeTestData(t, sidx, reqs, 5, 5)

	// Query without filter
	queryReq := createTestQueryRequest(1)
	resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)

	// Collect all data
	dataSet := make(map[string]int)
	for res := range resultsCh {
		require.NoError(t, res.Error)
		for _, data := range res.Data {
			dataSet[string(data)]++
		}
	}
	if err, ok := <-errCh; ok {
		require.NoError(t, err)
	}

	// Verify deduplication: each unique data should appear only once
	assert.Equal(t, 1, dataSet["data1"], "data1 should appear only once despite being written twice")
	assert.Equal(t, 1, dataSet["data2"], "data2 should appear only once despite being written twice")
	assert.Equal(t, 1, dataSet["data3"], "data3 should appear only once")
	assert.Equal(t, 3, len(dataSet), "Should have exactly 3 unique data entries")
}

func TestLoadBlockCursor_DeduplicationWithFilter(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	// Write test data with duplicates and different tag values
	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "data1",
			Tag{Name: testTagStatus, Value: []byte("200"), ValueType: pbv1.ValueTypeStr},
		),
		createTestWriteRequest(1, 101, "data2",
			Tag{Name: testTagStatus, Value: []byte("404"), ValueType: pbv1.ValueTypeStr},
		),
		createTestWriteRequest(1, 102, "data1", // Duplicate data with status=200
			Tag{Name: testTagStatus, Value: []byte("200"), ValueType: pbv1.ValueTypeStr},
		),
		createTestWriteRequest(1, 103, "data3",
			Tag{Name: testTagStatus, Value: []byte("200"), ValueType: pbv1.ValueTypeStr},
		),
		createTestWriteRequest(1, 104, "data1", // Duplicate data with status=404
			Tag{Name: testTagStatus, Value: []byte("404"), ValueType: pbv1.ValueTypeStr},
		),
		createTestWriteRequest(1, 105, "data3", // Duplicate data3 with status=200
			Tag{Name: testTagStatus, Value: []byte("200"), ValueType: pbv1.ValueTypeStr},
		),
	}

	writeTestData(t, sidx, reqs, 6, 6)

	// Query with filter for status=200
	tagFilter := &mockTagFilterMatcher{
		matchFunc: func(tags []*modelv1.Tag) (bool, error) {
			for _, tag := range tags {
				if tag.Key == testTagStatus && tag.Value.GetStr() != nil && tag.Value.GetStr().Value == "200" {
					return true, nil
				}
			}
			return false, nil
		},
		decoder: testTagValueDecoder,
	}

	queryReq := QueryRequest{
		SeriesIDs: []common.SeriesID{1},
		TagFilter: tagFilter,
	}

	resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)

	// Collect all data
	dataSet := make(map[string]int)
	for res := range resultsCh {
		require.NoError(t, res.Error)
		for _, data := range res.Data {
			dataSet[string(data)]++
		}
	}
	if err, ok := <-errCh; ok {
		require.NoError(t, err)
	}

	// Verify deduplication with filter:
	// - data1 (keys 100, 102) with status=200 → only first occurrence should be kept
	// - data3 (keys 103, 105) with status=200 → only first occurrence should be kept
	// - data2 (key 101) with status=404 → filtered out
	// - data1 (key 104) with status=404 → filtered out
	assert.Equal(t, 1, dataSet["data1"], "data1 should appear only once even with multiple status=200 entries")
	assert.Equal(t, 1, dataSet["data3"], "data3 should appear only once even with multiple status=200 entries")
	assert.Equal(t, 0, dataSet["data2"], "data2 should not appear (filtered out by status)")
	assert.Equal(t, 2, len(dataSet), "Should have exactly 2 unique data entries matching the filter")
}

func TestLoadBlockCursor_DeduplicationEdgeCases(t *testing.T) {
	tests := []struct {
		expectedData  map[string]int
		name          string
		description   string
		requests      []WriteRequest
		expectedCount int
	}{
		{
			name: "all_duplicates",
			requests: []WriteRequest{
				createTestWriteRequest(1, 100, "same_data"),
				createTestWriteRequest(1, 101, "same_data"),
				createTestWriteRequest(1, 102, "same_data"),
				createTestWriteRequest(1, 103, "same_data"),
			},
			expectedData: map[string]int{
				"same_data": 1,
			},
			expectedCount: 1,
			description:   "All duplicate data should result in single entry",
		},
		{
			name: "no_duplicates",
			requests: []WriteRequest{
				createTestWriteRequest(1, 100, "data1"),
				createTestWriteRequest(1, 101, "data2"),
				createTestWriteRequest(1, 102, "data3"),
			},
			expectedData: map[string]int{
				"data1": 1,
				"data2": 1,
				"data3": 1,
			},
			expectedCount: 3,
			description:   "No duplicates should result in all entries",
		},
		{
			name: "alternating_duplicates",
			requests: []WriteRequest{
				createTestWriteRequest(1, 100, "dataA"),
				createTestWriteRequest(1, 101, "dataB"),
				createTestWriteRequest(1, 102, "dataA"),
				createTestWriteRequest(1, 103, "dataB"),
				createTestWriteRequest(1, 104, "dataA"),
			},
			expectedData: map[string]int{
				"dataA": 1,
				"dataB": 1,
			},
			expectedCount: 2,
			description:   "Alternating duplicates should result in unique entries only",
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a fresh sidx instance for each subtest
			sidx := createTestSIDX(t)
			defer func() {
				assert.NoError(t, sidx.Close())
			}()

			ctx := context.Background()

			// Use different segment and part IDs for each test
			segmentID := int64(100 + i)
			partID := uint64(100 + i)

			writeTestData(t, sidx, tt.requests, segmentID, partID)

			queryReq := createTestQueryRequest(1)
			resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)

			dataSet := make(map[string]int)
			totalCount := 0
			for res := range resultsCh {
				require.NoError(t, res.Error)
				for _, data := range res.Data {
					dataSet[string(data)]++
					totalCount++
				}
			}
			if err, ok := <-errCh; ok {
				require.NoError(t, err)
			}

			// Verify the expected data set
			for expectedKey, expectedValue := range tt.expectedData {
				assert.Equal(t, expectedValue, dataSet[expectedKey],
					"%s: %s count mismatch", tt.description, expectedKey)
			}
			assert.Equal(t, tt.expectedCount, len(dataSet),
				"%s: unique data count mismatch", tt.description)
		})
	}
}
