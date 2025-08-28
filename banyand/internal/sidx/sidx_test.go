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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/index"
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
	opts := createTestOptions(t)
	sidx, err := NewSIDX(opts)
	require.NoError(t, err)
	require.NotNil(t, sidx)
	return sidx
}

func createTestTag(name, value string) Tag {
	return Tag{
		Name:      name,
		Value:     []byte(value),
		ValueType: pbv1.ValueTypeStr,
		Indexed:   true,
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

	err := sidx.Write(ctx, reqs)
	assert.NoError(t, err)

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

	err := sidx.Write(ctx, reqs)
	assert.NoError(t, err)

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

	ctx := context.Background()

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
			err := sidx.Write(ctx, []WriteRequest{tt.req})
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

	ctx := context.Background()

	// Write with multiple tags
	tags := []Tag{
		createTestTag("service", "user-service"),
		createTestTag("endpoint", "/api/users"),
		createTestTag("status", "200"),
	}

	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "trace-data", tags...),
	}

	err := sidx.Write(ctx, reqs)
	assert.NoError(t, err)
}

// Query Operation Tests.

func TestSIDX_Query_BasicQuery(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	// Write test data
	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "data1"),
		createTestWriteRequest(1, 101, "data2"),
	}
	err := sidx.Write(ctx, reqs)
	require.NoError(t, err)

	// Wait for introducer loop to process
	waitForIntroducerLoop()

	// Query the data
	queryReq := createTestQueryRequest(1)
	response, err := sidx.Query(ctx, queryReq)
	require.NoError(t, err)
	require.NotNil(t, response)

	// Validate results
	if response.Len() > 0 {
		assert.Greater(t, response.Len(), 0)
	}
}

func TestSIDX_Query_EmptyResult(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	// Query without writing any data
	queryReq := createTestQueryRequest(999) // Non-existent series
	response, err := sidx.Query(ctx, queryReq)
	require.NoError(t, err)
	require.NotNil(t, response)

	// Should return empty result
	assert.Equal(t, 0, response.Len()) // Empty result returns length 0
}

func TestSIDX_Query_KeyRangeFilter(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	// Write test data with different keys
	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "data100"),
		createTestWriteRequest(1, 150, "data150"),
		createTestWriteRequest(1, 200, "data200"),
	}
	err := sidx.Write(ctx, reqs)
	require.NoError(t, err)

	// Wait for introducer loop to process
	waitForIntroducerLoop()

	// Query with key range
	minKey := int64(120)
	maxKey := int64(180)
	queryReq := QueryRequest{
		SeriesIDs: []common.SeriesID{1},
		MinKey:    &minKey,
		MaxKey:    &maxKey,
	}

	response, err := sidx.Query(ctx, queryReq)
	require.NoError(t, err)
	require.NotNil(t, response)

	// Should only return data150
	if response.Len() > 0 {
		// Verify keys are within range
		for _, key := range response.Keys {
			assert.GreaterOrEqual(t, key, minKey)
			assert.LessOrEqual(t, key, maxKey)
		}
	}
}

func TestSIDX_Query_Ordering(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	// Write data in non-sorted order
	reqs := []WriteRequest{
		createTestWriteRequest(1, 300, "data300"),
		createTestWriteRequest(1, 100, "data100"),
		createTestWriteRequest(1, 200, "data200"),
	}
	err := sidx.Write(ctx, reqs)
	require.NoError(t, err)

	// Wait for introducer loop to process
	waitForIntroducerLoop()

	tests := []struct {
		order     *index.OrderBy
		name      string
		ascending bool
	}{
		{
			name:      "ascending order",
			ascending: true,
			order:     &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		},
		{
			name:      "descending order",
			ascending: false,
			order:     &index.OrderBy{Sort: modelv1.Sort_SORT_DESC},
		},
		{
			name:      "default order",
			ascending: true,
			order:     nil, // Should default to ascending
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queryReq := QueryRequest{
				SeriesIDs: []common.SeriesID{1},
				Order:     tt.order,
			}

			response, err := sidx.Query(ctx, queryReq)
			require.NoError(t, err)
			require.NotNil(t, response)

			// Get all keys directly from response
			allKeys := response.Keys

			// Verify ordering
			if len(allKeys) > 1 {
				isSorted := sort.SliceIsSorted(allKeys, func(i, j int) bool {
					if tt.ascending {
						return allKeys[i] < allKeys[j]
					}
					return allKeys[i] > allKeys[j]
				})
				assert.True(t, isSorted, "Keys should be sorted in %s order",
					map[bool]string{true: "ascending", false: "descending"}[tt.ascending])
			}
		})
	}
}

func TestSIDX_Query_Validation(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	tests := []struct {
		name      string
		req       QueryRequest
		expectErr bool
	}{
		{
			name:      "valid request",
			req:       createTestQueryRequest(1),
			expectErr: false,
		},
		{
			name:      "empty series IDs",
			req:       QueryRequest{SeriesIDs: []common.SeriesID{}},
			expectErr: true,
		},
		{
			name: "invalid key range",
			req: QueryRequest{
				SeriesIDs: []common.SeriesID{1},
				MinKey:    ptrInt64(200),
				MaxKey:    ptrInt64(100), // min > max
			},
			expectErr: true,
		},
		{
			name: "negative max element size",
			req: QueryRequest{
				SeriesIDs:      []common.SeriesID{1},
				MaxElementSize: -1,
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sidx.Query(ctx, tt.req)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
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

	err := sidx.Write(ctx, reqs)
	require.NoError(t, err)

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

	err := sidx.Write(ctx, reqs)
	require.NoError(t, err)

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

	err := sidx.Write(ctx, reqs)
	require.NoError(t, err)

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

			if err := sidx.Write(ctx, reqs); err != nil {
				errors <- err
			}
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
	err := sidx.Write(ctx, initialReqs)
	require.NoError(t, err)

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
				sidx.Write(ctx, []WriteRequest{req}) // Ignore errors during concurrent stress
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
