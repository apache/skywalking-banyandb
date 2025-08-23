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

func TestMockSIDX_BasicOperations(t *testing.T) {
	config := DefaultMockConfig()
	mock := NewMockSIDX(config)
	defer func() {
		assert.NoError(t, mock.Close())
	}()

	ctx := context.Background()

	// Test initial stats
	stats, err := mock.Stats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, int64(0), stats.ElementCount)
	assert.Equal(t, int64(0), stats.WriteCount.Load())
	assert.Equal(t, int64(0), stats.QueryCount.Load())

	// Test write operations
	writeReqs := []WriteRequest{
		{
			SeriesID: 1,
			Key:      100,
			Data:     []byte("data1"),
			Tags: []tag{
				{
					name:      "service",
					value:     []byte("user-service"),
					valueType: pbv1.ValueTypeStr,
					indexed:   true,
				},
			},
		},
		{
			SeriesID: 1,
			Key:      101,
			Data:     []byte("data2"),
			Tags: []tag{
				{
					name:      "service",
					value:     []byte("user-service"),
					valueType: pbv1.ValueTypeStr,
					indexed:   true,
				},
				{
					name:      "endpoint",
					value:     []byte("/api/users"),
					valueType: pbv1.ValueTypeStr,
					indexed:   true,
				},
			},
		},
	}

	err = mock.Write(ctx, writeReqs)
	require.NoError(t, err)

	// Verify stats after write
	stats, err = mock.Stats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, int64(2), stats.ElementCount)
	assert.Equal(t, int64(1), stats.WriteCount.Load())
	assert.True(t, stats.MemoryUsageBytes > 0)

	// Test query operations
	queryReq := QueryRequest{
		Name:           "series_1",
		MaxElementSize: 10,
	}

	result, err := mock.Query(ctx, queryReq)
	require.NoError(t, err)
	require.NotNil(t, result)
	defer result.Release()

	// Pull results
	response := result.Pull()
	require.NotNil(t, response)
	assert.NoError(t, response.Error)
	assert.Equal(t, 2, response.Len())
	assert.Equal(t, []int64{100, 101}, response.Keys)
	assert.Equal(t, [][]byte{[]byte("data1"), []byte("data2")}, response.Data)
	assert.Len(t, response.Tags, 2)
	assert.Len(t, response.SIDs, 2)

	// Verify no more results
	response = result.Pull()
	assert.Nil(t, response)

	// Verify query stats
	stats, err = mock.Stats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, int64(1), stats.QueryCount.Load())
}

func TestMockSIDX_WriteValidation(t *testing.T) {
	config := DefaultMockConfig()
	config.EnableStrictValidation = true
	mock := NewMockSIDX(config)
	defer func() {
		assert.NoError(t, mock.Close())
	}()

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
				{
					SeriesID: 1,
					Key:      100,
					Data:     []byte("data"),
					Tags:     []tag{},
				},
			},
			expectError: false,
		},
		{
			name: "invalid SeriesID",
			writeReqs: []WriteRequest{
				{
					SeriesID: 0, // Invalid
					Key:      100,
					Data:     []byte("data"),
					Tags:     []tag{},
				},
			},
			expectError: true,
		},
		{
			name: "nil data",
			writeReqs: []WriteRequest{
				{
					SeriesID: 1,
					Key:      100,
					Data:     nil, // Invalid
					Tags:     []tag{},
				},
			},
			expectError: true,
		},
		{
			name: "empty data",
			writeReqs: []WriteRequest{
				{
					SeriesID: 1,
					Key:      100,
					Data:     []byte{}, // Invalid
					Tags:     []tag{},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mock.Write(ctx, tt.writeReqs)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestMockSIDX_ElementLimits(t *testing.T) {
	config := DefaultMockConfig()
	config.MaxElements = 2
	mock := NewMockSIDX(config)
	defer func() {
		assert.NoError(t, mock.Close())
	}()

	ctx := context.Background()

	// Write up to limit
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data1")},
		{SeriesID: 1, Key: 101, Data: []byte("data2")},
	}
	err := mock.Write(ctx, writeReqs)
	require.NoError(t, err)

	// Try to exceed limit
	writeReqs = []WriteRequest{
		{SeriesID: 1, Key: 102, Data: []byte("data3")},
	}
	err = mock.Write(ctx, writeReqs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "element limit exceeded")
}

func TestMockSIDX_ErrorInjection(t *testing.T) {
	config := DefaultMockConfig()
	config.ErrorRate = 100 // Always error
	mock := NewMockSIDX(config)
	defer func() {
		assert.NoError(t, mock.Close())
	}()

	ctx := context.Background()

	// Test write error injection
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data")},
	}
	err := mock.Write(ctx, writeReqs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock error injection")

	// Test query error injection
	queryReq := QueryRequest{Name: "test"}
	_, err = mock.Query(ctx, queryReq)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock error injection")

	// Test flush error injection
	err = mock.Flush()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock error injection")

	// Test merge error injection
	err = mock.Merge()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock error injection")
}

func TestMockSIDX_Delays(t *testing.T) {
	config := DefaultMockConfig()
	config.WriteDelayMs = 50
	config.QueryDelayMs = 30
	config.FlushDelayMs = 20
	config.MergeDelayMs = 40
	mock := NewMockSIDX(config)
	defer func() {
		assert.NoError(t, mock.Close())
	}()

	ctx := context.Background()

	// Test write delay
	start := time.Now()
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data")},
	}
	err := mock.Write(ctx, writeReqs)
	require.NoError(t, err)
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond)

	// Test query delay
	start = time.Now()
	queryReq := QueryRequest{Name: "series_1"}
	result, err := mock.Query(ctx, queryReq)
	require.NoError(t, err)
	result.Release()
	elapsed = time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 30*time.Millisecond)

	// Test flush delay
	start = time.Now()
	err = mock.Flush()
	require.NoError(t, err)
	elapsed = time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 20*time.Millisecond)

	// Test merge delay
	start = time.Now()
	err = mock.Merge()
	require.NoError(t, err)
	elapsed = time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 40*time.Millisecond)
}

func TestMockSIDX_Sorting(t *testing.T) {
	config := DefaultMockConfig()
	mock := NewMockSIDX(config)
	defer func() {
		assert.NoError(t, mock.Close())
	}()

	ctx := context.Background()

	// Write elements out of order
	writeReqs := []WriteRequest{
		{SeriesID: 2, Key: 200, Data: []byte("data2")},
		{SeriesID: 1, Key: 102, Data: []byte("data3")},
		{SeriesID: 1, Key: 100, Data: []byte("data1")},
		{SeriesID: 2, Key: 199, Data: []byte("data4")},
	}
	err := mock.Write(ctx, writeReqs)
	require.NoError(t, err)

	// Query series 1 - should be sorted by key
	queryReq := QueryRequest{Name: "series_1"}
	result, err := mock.Query(ctx, queryReq)
	require.NoError(t, err)
	defer result.Release()

	response := result.Pull()
	require.NotNil(t, response)
	assert.Equal(t, []int64{100, 102}, response.Keys)
	assert.Equal(t, [][]byte{[]byte("data1"), []byte("data3")}, response.Data)

	// Query series 2 - should be sorted by key
	queryReq = QueryRequest{Name: "series_2"}
	result, err = mock.Query(ctx, queryReq)
	require.NoError(t, err)
	defer result.Release()

	response = result.Pull()
	require.NotNil(t, response)
	assert.Equal(t, []int64{199, 200}, response.Keys)
	assert.Equal(t, [][]byte{[]byte("data4"), []byte("data2")}, response.Data)
}

func TestMockSIDX_QueryBatching(t *testing.T) {
	config := DefaultMockConfig()
	mock := NewMockSIDX(config)
	defer func() {
		assert.NoError(t, mock.Close())
	}()

	ctx := context.Background()

	// Write many elements
	var writeReqs []WriteRequest
	for i := 0; i < 10; i++ {
		writeReqs = append(writeReqs, WriteRequest{
			SeriesID: 1,
			Key:      int64(i),
			Data:     []byte(fmt.Sprintf("data%d", i)),
		})
	}
	err := mock.Write(ctx, writeReqs)
	require.NoError(t, err)

	// Query with small batch size
	queryReq := QueryRequest{
		Name:           "series_1",
		MaxElementSize: 3,
	}
	result, err := mock.Query(ctx, queryReq)
	require.NoError(t, err)
	defer result.Release()

	// First batch
	response := result.Pull()
	require.NotNil(t, response)
	assert.Equal(t, 3, response.Len())
	assert.True(t, response.Metadata.TruncatedResults)

	// Second batch
	response = result.Pull()
	require.NotNil(t, response)
	assert.Equal(t, 3, response.Len())

	// Continue until done
	totalElements := 6
	for {
		response = result.Pull()
		if response == nil {
			break
		}
		totalElements += response.Len()
	}
	assert.Equal(t, 10, totalElements)
}

func TestMockSIDX_ConcurrentOperations(t *testing.T) {
	config := DefaultMockConfig()
	mock := NewMockSIDX(config)
	defer func() {
		assert.NoError(t, mock.Close())
	}()

	ctx := context.Background()

	// Concurrent writes
	const numGoroutines = 10
	const elementsPerGoroutine = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(seriesID int) {
			defer func() { done <- true }()
			var writeReqs []WriteRequest
			for j := 0; j < elementsPerGoroutine; j++ {
				writeReqs = append(writeReqs, WriteRequest{
					SeriesID: common.SeriesID(seriesID),
					Key:      int64(j),
					Data:     []byte(fmt.Sprintf("data%d_%d", seriesID, j)),
				})
			}
			err := mock.Write(ctx, writeReqs)
			assert.NoError(t, err)
		}(i + 1)
	}

	// Wait for all writes to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify total elements
	stats, err := mock.Stats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, int64(numGoroutines*elementsPerGoroutine), stats.ElementCount)
	assert.Equal(t, int64(numGoroutines), stats.WriteCount.Load())
}

func TestMockSIDX_CloseOperations(t *testing.T) {
	config := DefaultMockConfig()
	mock := NewMockSIDX(config)

	ctx := context.Background()

	// Close the mock
	err := mock.Close()
	require.NoError(t, err)

	// All operations should fail after close
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data")},
	}
	err = mock.Write(ctx, writeReqs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SIDX is closed")

	queryReq := QueryRequest{Name: "test"}
	_, err = mock.Query(ctx, queryReq)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SIDX is closed")

	_, err = mock.Stats(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SIDX is closed")

	err = mock.Flush()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SIDX is closed")

	err = mock.Merge()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SIDX is closed")

	// Double close should fail
	err = mock.Close()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SIDX already closed")
}

func TestMockSIDX_DynamicConfiguration(t *testing.T) {
	config := DefaultMockConfig()
	mock := NewMockSIDX(config)
	defer func() {
		assert.NoError(t, mock.Close())
	}()

	ctx := context.Background()

	// Test dynamic error rate configuration
	mock.SetErrorRate(100)
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data")},
	}
	err := mock.Write(ctx, writeReqs)
	assert.Error(t, err)

	mock.SetErrorRate(0)
	err = mock.Write(ctx, writeReqs)
	assert.NoError(t, err)

	// Test dynamic delay configuration
	mock.SetWriteDelay(50)
	start := time.Now()
	err = mock.Write(ctx, writeReqs)
	require.NoError(t, err)
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond)

	mock.SetQueryDelay(30)
	start = time.Now()
	result, err := mock.Query(ctx, QueryRequest{Name: "series_1"})
	require.NoError(t, err)
	result.Release()
	elapsed = time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 30*time.Millisecond)
}

func TestMockSIDX_UtilityMethods(t *testing.T) {
	config := DefaultMockConfig()
	mock := NewMockSIDX(config)
	defer func() {
		assert.NoError(t, mock.Close())
	}()

	ctx := context.Background()

	// Test initial state
	assert.Equal(t, int64(0), mock.GetElementCount())
	assert.Empty(t, mock.GetStorageKeys())

	// Add some data
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data1")},
		{SeriesID: 2, Key: 200, Data: []byte("data2")},
	}
	err := mock.Write(ctx, writeReqs)
	require.NoError(t, err)

	// Check state after writes
	assert.Equal(t, int64(2), mock.GetElementCount())
	keys := mock.GetStorageKeys()
	assert.Len(t, keys, 2)
	assert.Contains(t, keys, "series_1")
	assert.Contains(t, keys, "series_2")

	// Test clear
	mock.Clear()
	assert.Equal(t, int64(0), mock.GetElementCount())
	assert.Empty(t, mock.GetStorageKeys())
}

func TestMockSIDX_FlushAndMergeTimestamps(t *testing.T) {
	config := DefaultMockConfig()
	config.FlushDelayMs = 10
	config.MergeDelayMs = 10
	mock := NewMockSIDX(config)
	defer func() {
		assert.NoError(t, mock.Close())
	}()

	ctx := context.Background()

	// Initial timestamps should be zero
	stats, err := mock.Stats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, int64(0), stats.LastFlushTime)
	assert.Equal(t, int64(0), stats.LastMergeTime)

	// Perform flush
	startTime := time.Now().UnixNano()
	err = mock.Flush()
	require.NoError(t, err)
	endTime := time.Now().UnixNano()

	stats, err = mock.Stats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.GreaterOrEqual(t, stats.LastFlushTime, startTime)
	assert.LessOrEqual(t, stats.LastFlushTime, endTime)

	// Perform merge
	startTime = time.Now().UnixNano()
	err = mock.Merge()
	require.NoError(t, err)
	endTime = time.Now().UnixNano()

	stats, err = mock.Stats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.GreaterOrEqual(t, stats.LastMergeTime, startTime)
	assert.LessOrEqual(t, stats.LastMergeTime, endTime)
}
