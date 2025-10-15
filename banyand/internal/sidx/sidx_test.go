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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
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
		createTestTag("status", "200"),
	}

	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "trace-data", tags...),
	}

	writeTestData(t, sidx, reqs, 3, 3) // Test with segmentID=3, partID=3
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
	response, err := sidx.Query(ctx, queryReq)
	require.NoError(t, err)
	require.NotNil(t, response)

	series1Keys := collectAllKeys(t, response)
	assert.Greater(t, len(series1Keys), 0, "Should find data for series 1")

	// Test 2: Query multiple series
	queryReq2 := createTestQueryRequest(1, 2)
	response2, err := sidx.Query(ctx, queryReq2)
	require.NoError(t, err)
	require.NotNil(t, response2)

	allKeys := collectAllKeys(t, response2)
	assert.GreaterOrEqual(t, len(allKeys), len(series1Keys), "Should find at least as much data for multiple series")

	// Test 3: Query with key range that spans both series
	minKey := int64(110)
	maxKey := int64(190)
	queryReq3 := QueryRequest{
		SeriesIDs: []common.SeriesID{1, 2},
		MinKey:    &minKey,
		MaxKey:    &maxKey,
	}

	response3, err := sidx.Query(ctx, queryReq3)
	require.NoError(t, err)
	require.NotNil(t, response3)

	rangeKeys := collectAllKeys(t, response3)
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
	response, err := sidx.Query(ctx, queryReq)
	require.NoError(t, err)
	require.NotNil(t, response)

	actualData := make(map[int64]string)
	for i, key := range response.Keys {
		if i < len(response.Data) {
			actualData[key] = string(response.Data[i])
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

	response, err := sidx.Query(ctx, queryReq)
	require.NoError(t, err)
	require.NotNil(t, response)

	totalElements := response.Len()

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
				response, errQuery := sidx.Query(ctx, queryReq)
				if errQuery != nil {
					continue // Continue on error during concurrent access
				}
				if response != nil {
					_ = response.Len() // Just access the data
				}
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
	response, err := sidx.Query(ctx, queryReq)
	require.NoError(t, err)
	require.NotNil(t, response)
}

// Utility functions.

func ptrInt64(v int64) *int64 {
	return &v
}

func collectAllKeys(t *testing.T, response *QueryResponse) []int64 {
	require.NoError(t, nil) // No error expected since we already have the response
	return response.Keys
}

// TestQueryResult_MaxElementSize verifies that MaxElementSize limits the total number of elements appended.
func TestQueryResult_MaxElementSize(t *testing.T) {
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
					MaxElementSize: tt.maxElementSize,
				},
			}

			block := &block{
				userKeys: tt.inputKeys,
				data:     tt.inputData,
				tags:     make(map[string]*tagData),
			}

			result := &QueryResponse{
				Keys: make([]int64, 0),
				Data: make([][]byte, 0),
				Tags: make([][]Tag, 0),
				SIDs: make([]common.SeriesID, 0),
			}

			qr.convertBlockToResponse(block, 1, result)

			assert.Equal(t, tt.expectedKeys, result.Keys)
			assert.Equal(t, tt.expectedData, result.Data)

			assert.Equal(t, len(result.Keys), len(result.Data), "Keys and Data arrays should have same length")
			assert.Equal(t, len(result.Keys), len(result.SIDs), "Keys and SIDs arrays should have same length")
			assert.Equal(t, len(result.Keys), len(result.Tags), "Keys and Tags arrays should have same length")
		})
	}
}

// TestQueryResult_ConvertBlockToResponse_RespectsLimitAcrossCalls ensures subsequent calls do not exceed the limit.
func TestQueryResult_ConvertBlockToResponse_RespectsLimitAcrossCalls(t *testing.T) {
	qr := &queryResult{
		request: QueryRequest{
			MaxElementSize: 2,
		},
	}

	result := &QueryResponse{
		Keys: make([]int64, 0),
		Data: make([][]byte, 0),
		Tags: make([][]Tag, 0),
		SIDs: make([]common.SeriesID, 0),
	}

	// First call: add trace1 data
	block1 := &block{
		userKeys: []int64{100, 101},
		data:     [][]byte{[]byte("trace1"), []byte("trace1")},
		tags:     make(map[string]*tagData),
	}
	qr.convertBlockToResponse(block1, 1, result)

	// Verify first call results
	assert.Equal(t, 2, result.Len(), "First call should add 2 elements")

	// Second call: try to add trace2 data (should be skipped because limit reached)
	block2 := &block{
		userKeys: []int64{200, 201},
		data:     [][]byte{[]byte("trace2"), []byte("trace2")},
		tags:     make(map[string]*tagData),
	}
	qr.convertBlockToResponse(block2, 2, result)

	assert.Equal(t, 2, result.Len(), "Second call should not add elements beyond limit")

	// Third call: try to add trace3 data (still skipped)
	block3 := &block{
		userKeys: []int64{300, 301},
		data:     [][]byte{[]byte("trace3"), []byte("trace3")},
		tags:     make(map[string]*tagData),
	}
	qr.convertBlockToResponse(block3, 3, result)

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
					Tags: [][]Tag{{}, {}},
					SIDs: []common.SeriesID{1, 3},
				},
				{
					Keys: []int64{200, 400},
					Data: [][]byte{[]byte("trace2"), []byte("trace4")},
					Tags: [][]Tag{{}, {}},
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
					Tags: [][]Tag{{}, {}},
					SIDs: []common.SeriesID{1, 3},
				},
				{
					Keys: []int64{200, 400},
					Data: [][]byte{[]byte("trace2"), []byte("trace4")},
					Tags: [][]Tag{{}, {}},
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
			assert.Equal(t, len(result.Keys), len(result.Tags), "Keys and Tags arrays should have same length")
		})
	}
}

// TestQueryResponseHeap_MergeDescending verifies descending merge respects the element limit.
func TestQueryResponseHeap_MergeDescending(t *testing.T) {
	shards := []*QueryResponse{
		{
			Keys: []int64{200, 400}, // stored ascending
			Data: [][]byte{[]byte("trace2"), []byte("trace4")},
			Tags: [][]Tag{{}, {}},
			SIDs: []common.SeriesID{2, 4},
		},
		{
			Keys: []int64{150, 300}, // stored ascending
			Data: [][]byte{[]byte("trace1"), []byte("trace3")},
			Tags: [][]Tag{{}, {}},
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
		Tags: [][]Tag{{}, {}},
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
	assert.Equal(t, 0, len(result.Tags))
	assert.Equal(t, 0, len(result.SIDs))
	assert.Equal(t, ResponseMetadata{}, result.Metadata)
}
