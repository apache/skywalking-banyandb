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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestMockWriter_BasicOperations(t *testing.T) {
	config := DefaultMockWriterConfig()
	writer := NewMockWriter(config)
	ctx := context.Background()

	// Test initial state
	elements := writer.GetElements()
	assert.Empty(t, elements)

	writeCount, elementCount, lastWriteTime := writer.GetStats()
	assert.Equal(t, int64(0), writeCount)
	assert.Equal(t, int64(0), elementCount)
	assert.True(t, lastWriteTime.IsZero())

	// Test write operations
	writeReqs := []WriteRequest{
		{
			SeriesID: 2,
			Key:      200,
			Data:     []byte("data2"),
			Tags: []tag{
				{
					name:      "service",
					value:     []byte("test-service"),
					valueType: pbv1.ValueTypeStr,
					indexed:   true,
				},
			},
		},
		{
			SeriesID: 1,
			Key:      100,
			Data:     []byte("data1"),
			Tags: []tag{
				{
					name:      "service",
					value:     []byte("test-service"),
					valueType: pbv1.ValueTypeStr,
					indexed:   true,
				},
			},
		},
	}

	err := writer.Write(ctx, writeReqs)
	require.NoError(t, err)

	// Verify elements are stored and sorted
	elements = writer.GetElements()
	require.Len(t, elements, 2)
	assert.Equal(t, common.SeriesID(1), elements[0].SeriesID) // Should be sorted
	assert.Equal(t, int64(100), elements[0].Key)
	assert.Equal(t, common.SeriesID(2), elements[1].SeriesID)
	assert.Equal(t, int64(200), elements[1].Key)

	// Verify stats
	writeCount, elementCount, lastWriteTime = writer.GetStats()
	assert.Equal(t, int64(1), writeCount)
	assert.Equal(t, int64(2), elementCount)
	assert.False(t, lastWriteTime.IsZero())
}

func TestMockWriter_Validation(t *testing.T) {
	config := DefaultMockWriterConfig()
	config.EnableValidation = true
	writer := NewMockWriter(config)
	ctx := context.Background()

	tests := []struct {
		name        string
		writeReqs   []WriteRequest
		expectError bool
	}{
		{
			name:        "empty requests",
			writeReqs:   []WriteRequest{},
			expectError: false,
		},
		{
			name: "valid request",
			writeReqs: []WriteRequest{
				{SeriesID: 1, Key: 100, Data: []byte("data")},
			},
			expectError: false,
		},
		{
			name: "invalid SeriesID",
			writeReqs: []WriteRequest{
				{SeriesID: 0, Key: 100, Data: []byte("data")},
			},
			expectError: true,
		},
		{
			name: "nil data",
			writeReqs: []WriteRequest{
				{SeriesID: 1, Key: 100, Data: nil},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer.Clear() // Reset for each test
			err := writer.Write(ctx, tt.writeReqs)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestMockWriter_ElementLimits(t *testing.T) {
	config := DefaultMockWriterConfig()
	config.MaxElements = 3
	writer := NewMockWriter(config)
	ctx := context.Background()

	// Write up to limit
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data1")},
		{SeriesID: 1, Key: 101, Data: []byte("data2")},
		{SeriesID: 1, Key: 102, Data: []byte("data3")},
	}
	err := writer.Write(ctx, writeReqs)
	require.NoError(t, err)

	// Try to exceed limit
	writeReqs = []WriteRequest{
		{SeriesID: 1, Key: 103, Data: []byte("data4")},
	}
	err = writer.Write(ctx, writeReqs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "element limit exceeded")
}

func TestMockWriter_ErrorInjection(t *testing.T) {
	config := DefaultMockWriterConfig()
	config.ErrorRate = 100 // Always error
	writer := NewMockWriter(config)
	ctx := context.Background()

	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data")},
	}
	err := writer.Write(ctx, writeReqs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock writer error injection")
}

func TestMockWriter_DelaySimulation(t *testing.T) {
	config := DefaultMockWriterConfig()
	config.DelayMs = 50
	writer := NewMockWriter(config)
	ctx := context.Background()

	start := time.Now()
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data")},
	}
	err := writer.Write(ctx, writeReqs)
	require.NoError(t, err)
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
}

func TestMockQuerier_BasicOperations(t *testing.T) {
	config := DefaultMockQuerierConfig()
	querier := NewMockQuerier(config)
	ctx := context.Background()

	// Test initial state
	queryCount, lastQueryTime := querier.GetStats()
	assert.Equal(t, int64(0), queryCount)
	assert.True(t, lastQueryTime.IsZero())

	// Set up test elements
	elements := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data1")},
		{SeriesID: 1, Key: 101, Data: []byte("data2")},
		{SeriesID: 2, Key: 200, Data: []byte("data3")},
	}
	querier.SetElements(elements)

	// Test query operations
	queryReq := QueryRequest{
		Name:           "test-query",
		MaxElementSize: 10,
	}

	result, err := querier.Query(ctx, queryReq)
	require.NoError(t, err)
	require.NotNil(t, result)
	defer result.Release()

	// Pull results
	response := result.Pull()
	require.NotNil(t, response)
	assert.NoError(t, response.Error)
	assert.Equal(t, 3, response.Len())
	assert.Equal(t, []int64{100, 101, 200}, response.Keys)

	// Verify no more results
	response = result.Pull()
	assert.Nil(t, response)

	// Verify stats
	queryCount, lastQueryTime = querier.GetStats()
	assert.Equal(t, int64(1), queryCount)
	assert.False(t, lastQueryTime.IsZero())
}

func TestMockQuerier_QueryBatching(t *testing.T) {
	config := DefaultMockQuerierConfig()
	querier := NewMockQuerier(config)
	ctx := context.Background()

	// Set up many elements
	var elements []WriteRequest
	for i := 0; i < 10; i++ {
		elements = append(elements, WriteRequest{
			SeriesID: 1,
			Key:      int64(i),
			Data:     []byte(fmt.Sprintf("data%d", i)),
		})
	}
	querier.SetElements(elements)

	// Query with small batch size
	queryReq := QueryRequest{
		Name:           "batch-test",
		MaxElementSize: 3,
	}

	result, err := querier.Query(ctx, queryReq)
	require.NoError(t, err)
	defer result.Release()

	// First batch
	response := result.Pull()
	require.NotNil(t, response)
	assert.Equal(t, 3, response.Len())
	assert.True(t, response.Metadata.TruncatedResults)

	// Continue pulling until done
	totalElements := 3
	for {
		response = result.Pull()
		if response == nil {
			break
		}
		totalElements += response.Len()
	}
	assert.Equal(t, 10, totalElements)
}

func TestMockQuerier_ErrorInjection(t *testing.T) {
	config := DefaultMockQuerierConfig()
	config.ErrorRate = 100 // Always error
	querier := NewMockQuerier(config)
	ctx := context.Background()

	queryReq := QueryRequest{Name: "test"}
	_, err := querier.Query(ctx, queryReq)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock querier error injection")
}

func TestMockFlusher_BasicOperations(t *testing.T) {
	config := DefaultMockFlusherConfig()
	flusher := NewMockFlusher(config)

	// Test initial state
	flushCount, lastFlushTime := flusher.GetStats()
	assert.Equal(t, int64(0), flushCount)
	assert.True(t, lastFlushTime.IsZero())

	// Test flush operation
	err := flusher.Flush()
	require.NoError(t, err)

	// Verify stats
	flushCount, lastFlushTime = flusher.GetStats()
	assert.Equal(t, int64(1), flushCount)
	assert.False(t, lastFlushTime.IsZero())
}

func TestMockFlusher_DelaySimulation(t *testing.T) {
	config := DefaultMockFlusherConfig()
	config.DelayMs = 30
	flusher := NewMockFlusher(config)

	start := time.Now()
	err := flusher.Flush()
	require.NoError(t, err)
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, 30*time.Millisecond)
}

func TestMockFlusher_ErrorInjection(t *testing.T) {
	config := DefaultMockFlusherConfig()
	config.ErrorRate = 100 // Always error
	flusher := NewMockFlusher(config)

	err := flusher.Flush()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock flusher error injection")
}

func TestMockMerger_BasicOperations(t *testing.T) {
	config := DefaultMockMergerConfig()
	merger := NewMockMerger(config)

	// Test initial state
	mergeCount, lastMergeTime := merger.GetStats()
	assert.Equal(t, int64(0), mergeCount)
	assert.True(t, lastMergeTime.IsZero())

	consolidations := merger.GetConsolidations()
	assert.Empty(t, consolidations)

	// Test merge operation
	err := merger.Merge()
	require.NoError(t, err)

	// Verify stats
	mergeCount, lastMergeTime = merger.GetStats()
	assert.Equal(t, int64(1), mergeCount)
	assert.False(t, lastMergeTime.IsZero())

	// Verify consolidations
	consolidations = merger.GetConsolidations()
	require.Len(t, consolidations, 1)
	consolidation := consolidations[0]
	assert.Greater(t, consolidation.PartsInput, 0)
	assert.Greater(t, consolidation.PartsOutput, 0)
	assert.Greater(t, consolidation.ElementsProcessed, int64(0))
	assert.Greater(t, consolidation.Duration, time.Duration(0))
}

func TestMockMerger_ConsolidationRatio(t *testing.T) {
	config := DefaultMockMergerConfig()
	config.ConsolidationRatio = 0.5 // 50% reduction
	merger := NewMockMerger(config)

	err := merger.Merge()
	require.NoError(t, err)

	consolidations := merger.GetConsolidations()
	require.Len(t, consolidations, 1)
	consolidation := consolidations[0]

	// Output should be roughly half of input
	expectedOutput := int(float64(consolidation.PartsInput) * 0.5)
	if expectedOutput < 1 {
		expectedOutput = 1
	}
	assert.Equal(t, expectedOutput, consolidation.PartsOutput)
}

func TestMockMerger_ErrorInjection(t *testing.T) {
	config := DefaultMockMergerConfig()
	config.ErrorRate = 100 // Always error
	merger := NewMockMerger(config)

	err := merger.Merge()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock merger error injection")
}

func TestMockComponentSuite_Integration(t *testing.T) {
	suite := NewMockComponentSuite()
	ctx := context.Background()

	// Test write-query workflow
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data1")},
		{SeriesID: 1, Key: 101, Data: []byte("data2")},
		{SeriesID: 2, Key: 200, Data: []byte("data3")},
	}

	// Write elements
	err := suite.Writer.Write(ctx, writeReqs)
	require.NoError(t, err)

	// Sync elements to querier
	suite.SyncElements()

	// Query elements
	queryReq := QueryRequest{
		Name:           "integration-test",
		MaxElementSize: 10,
	}

	result, err := suite.Querier.Query(ctx, queryReq)
	require.NoError(t, err)
	defer result.Release()

	response := result.Pull()
	require.NotNil(t, response)
	assert.Equal(t, 3, response.Len())

	// Test flush operation
	err = suite.Flusher.Flush()
	require.NoError(t, err)

	// Test merge operation
	err = suite.Merger.Merge()
	require.NoError(t, err)
}

func TestMockComponentSuite_ErrorInjection(t *testing.T) {
	suite := NewMockComponentSuite()
	ctx := context.Background()

	// Set global error rate
	suite.SetGlobalErrorRate(100)

	// All operations should fail
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data")},
	}
	err := suite.Writer.Write(ctx, writeReqs)
	assert.Error(t, err)

	queryReq := QueryRequest{Name: "test"}
	_, err = suite.Querier.Query(ctx, queryReq)
	assert.Error(t, err)

	err = suite.Flusher.Flush()
	assert.Error(t, err)

	err = suite.Merger.Merge()
	assert.Error(t, err)
}

func TestMockComponentSuite_CustomConfigs(t *testing.T) {
	writerConfig := MockWriterConfig{
		DelayMs:          10,
		ErrorRate:        0,
		MaxElements:      100,
		EnableValidation: true,
		SortElements:     true,
	}
	querierConfig := MockQuerierConfig{
		DelayMs:          5,
		ErrorRate:        0,
		EnableValidation: true,
		MaxResultSize:    50,
	}
	flusherConfig := MockFlusherConfig{
		DelayMs:      20,
		ErrorRate:    0,
		SimulateWork: true,
	}
	mergerConfig := MockMergerConfig{
		DelayMs:            30,
		ErrorRate:          0,
		SimulateWork:       true,
		ConsolidationRatio: 0.8,
	}

	suite := NewMockComponentSuiteWithConfigs(
		writerConfig, querierConfig, flusherConfig, mergerConfig)

	ctx := context.Background()

	// Test that custom configurations work
	start := time.Now()
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data")},
	}
	err := suite.Writer.Write(ctx, writeReqs)
	require.NoError(t, err)
	writerElapsed := time.Since(start)

	start = time.Now()
	err = suite.Flusher.Flush()
	require.NoError(t, err)
	flusherElapsed := time.Since(start)

	start = time.Now()
	err = suite.Merger.Merge()
	require.NoError(t, err)
	mergerElapsed := time.Since(start)

	// Verify delays are applied
	assert.GreaterOrEqual(t, writerElapsed, 10*time.Millisecond)
	assert.GreaterOrEqual(t, flusherElapsed, 20*time.Millisecond)
	assert.GreaterOrEqual(t, mergerElapsed, 30*time.Millisecond)
}

func TestMockComponentSuite_Reset(t *testing.T) {
	suite := NewMockComponentSuite()
	ctx := context.Background()

	// Add some data
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data")},
	}
	err := suite.Writer.Write(ctx, writeReqs)
	require.NoError(t, err)

	suite.SyncElements()

	// Verify data exists
	elements := suite.Writer.GetElements()
	assert.Len(t, elements, 1)

	// Reset
	suite.Reset()

	// Verify data is cleared
	elements = suite.Writer.GetElements()
	assert.Empty(t, elements)
}

func TestMockComponents_ConcurrentOperations(t *testing.T) {
	suite := NewMockComponentSuite()
	ctx := context.Background()

	const numGoroutines = 10
	const operationsPerGoroutine = 10

	// Concurrent writes
	done := make(chan bool, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()
			for j := 0; j < operationsPerGoroutine; j++ {
				writeReqs := []WriteRequest{
					{
						SeriesID: common.SeriesID(id + 1), // Start from 1, not 0
						Key:      int64(j),
						Data:     []byte(fmt.Sprintf("data_%d_%d", id, j)),
					},
				}
				err := suite.Writer.Write(ctx, writeReqs)
				assert.NoError(t, err)
			}
		}(i)
	}

	// Wait for all writes to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify total elements
	elements := suite.Writer.GetElements()
	assert.Len(t, elements, numGoroutines*operationsPerGoroutine)

	// Verify elements are properly sorted
	for i := 1; i < len(elements); i++ {
		prev := elements[i-1]
		curr := elements[i]
		if prev.SeriesID == curr.SeriesID {
			assert.LessOrEqual(t, prev.Key, curr.Key)
		} else {
			assert.Less(t, prev.SeriesID, curr.SeriesID)
		}
	}
}

func TestMockComponents_PerformanceCharacteristics(t *testing.T) {
	// This test documents the performance characteristics of mock components

	suite := NewMockComponentSuite()
	ctx := context.Background()

	// Measure write performance
	const numElements = 1000
	var writeReqs []WriteRequest
	for i := 0; i < numElements; i++ {
		writeReqs = append(writeReqs, WriteRequest{
			SeriesID: common.SeriesID(i%10 + 1),
			Key:      int64(i),
			Data:     []byte(fmt.Sprintf("data_%d", i)),
		})
	}

	start := time.Now()
	err := suite.Writer.Write(ctx, writeReqs)
	require.NoError(t, err)
	writeDuration := time.Since(start)

	writeRate := float64(numElements) / writeDuration.Seconds()
	t.Logf("Write performance: %d elements in %v (%.0f elem/sec)",
		numElements, writeDuration, writeRate)

	// Sync and measure query performance
	suite.SyncElements()

	queryReq := QueryRequest{
		Name:           "performance-test",
		MaxElementSize: 100,
	}

	start = time.Now()
	result, err := suite.Querier.Query(ctx, queryReq)
	require.NoError(t, err)
	defer result.Release()

	resultCount := 0
	for {
		response := result.Pull()
		if response == nil {
			break
		}
		resultCount += response.Len()
	}
	queryDuration := time.Since(start)

	queryRate := float64(resultCount) / queryDuration.Seconds()
	t.Logf("Query performance: %d results in %v (%.0f results/sec)",
		resultCount, queryDuration, queryRate)

	// Document expected performance characteristics
	assert.Greater(t, writeRate, 1000.0, "Write rate should be > 1000 elem/sec")
	assert.Greater(t, queryRate, 5000.0, "Query rate should be > 5000 results/sec")
	assert.Equal(t, numElements, resultCount, "All elements should be returned")
}
