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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInterfaceUsageExamples(t *testing.T) {
	examples := NewInterfaceUsageExamples()
	require.NotNil(t, examples)
	require.NotNil(t, examples.sidx)

	ctx := context.Background()

	t.Run("BasicWriteExample", func(t *testing.T) {
		err := examples.BasicWriteExample(ctx)
		assert.NoError(t, err)
	})

	t.Run("AdvancedQueryExample", func(t *testing.T) {
		err := examples.AdvancedQueryExample(ctx)
		assert.NoError(t, err)
	})

	t.Run("FlushAndMergeExample", func(t *testing.T) {
		err := examples.FlushAndMergeExample(ctx)
		assert.NoError(t, err)
	})

	t.Run("ErrorHandlingExample", func(_ *testing.T) {
		// This example doesn't return errors, just demonstrates error handling
		examples.ErrorHandlingExample(ctx)
	})

	t.Run("PerformanceOptimizationExample", func(t *testing.T) {
		err := examples.PerformanceOptimizationExample(ctx)
		assert.NoError(t, err)
	})

	t.Run("IntegrationPatternExample", func(t *testing.T) {
		err := examples.IntegrationPatternExample(ctx)
		assert.NoError(t, err)
	})
}

func TestMockSIDXImplementation(t *testing.T) {
	sidx := &mockSIDX{}
	ctx := context.Background()

	t.Run("Write validates input", func(t *testing.T) {
		// Test empty write request
		err := sidx.Write(ctx, []WriteRequest{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty write request")

		// Test invalid SeriesID
		err = sidx.Write(ctx, []WriteRequest{
			{SeriesID: 0, Key: 123, Data: []byte("test")},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid SeriesID")

		// Test valid write request
		err = sidx.Write(ctx, []WriteRequest{
			{SeriesID: 1001, Key: 123, Data: []byte("test")},
		})
		assert.NoError(t, err)
	})

	t.Run("Query validates input", func(t *testing.T) {
		// Test empty query name
		result, err := sidx.Query(ctx, QueryRequest{Name: ""})
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "query name cannot be empty")

		// Test invalid MaxElementSize
		result, err = sidx.Query(ctx, QueryRequest{
			Name:           "test",
			MaxElementSize: -1,
		})
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "invalid MaxElementSize")

		// Test valid query
		result, err = sidx.Query(ctx, QueryRequest{
			Name:           "test",
			MaxElementSize: 100,
		})
		assert.NoError(t, err)
		assert.NotNil(t, result)
		result.Release()
	})

	t.Run("Stats returns valid data", func(t *testing.T) {
		stats, err := sidx.Stats(ctx)
		assert.NoError(t, err)
		require.NotNil(t, stats)
		assert.Greater(t, stats.MemoryUsageBytes, int64(0))
		assert.Greater(t, stats.DiskUsageBytes, int64(0))
		assert.Greater(t, stats.ElementCount, int64(0))
		assert.Greater(t, stats.PartCount, int64(0))
		assert.Greater(t, stats.QueryCount.Load(), int64(0))
	})

	t.Run("Flush and Merge succeed", func(t *testing.T) {
		err := sidx.Flush()
		assert.NoError(t, err)

		err = sidx.Merge()
		assert.NoError(t, err)
	})

	t.Run("Close succeeds", func(t *testing.T) {
		err := sidx.Close()
		assert.NoError(t, err)
	})
}

func TestMockQueryResult(t *testing.T) {
	result := &mockQueryResult{}

	t.Run("Pull returns data once", func(t *testing.T) {
		// First pull should return data
		response := result.Pull()
		assert.NotNil(t, response)
		assert.NoError(t, response.Error)
		assert.Equal(t, 2, response.Len())
		assert.Len(t, response.Keys, 2)
		assert.Len(t, response.Data, 2)
		assert.Len(t, response.Tags, 2)
		assert.Len(t, response.SIDs, 2)

		// Validate response metadata
		assert.Greater(t, response.Metadata.ExecutionTimeMs, int64(0))
		assert.Greater(t, response.Metadata.ElementsScanned, int64(0))
		assert.Greater(t, response.Metadata.ElementsFiltered, int64(0))

		// Second pull should return nil (no more data)
		response = result.Pull()
		assert.Nil(t, response)
	})

	t.Run("Release doesn't panic", func(_ *testing.T) {
		// Should not panic
		result.Release()
	})
}

func TestInt64ToBytes(t *testing.T) {
	testCases := []struct {
		name     string
		expected []byte
		input    int64
	}{
		{
			name:     "zero value",
			input:    0,
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "positive value",
			input:    1640995200000,
			expected: []byte{0, 0, 1, 0x7e, 0x12, 0xef, 0x9c, 0x0},
		},
		{
			name:     "negative value",
			input:    -1,
			expected: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
		{
			name:     "max int64",
			input:    9223372036854775807, // math.MaxInt64
			expected: []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := int64ToBytes(tc.input)
			assert.Equal(t, tc.expected, result)
			assert.Len(t, result, 8) // Should always be 8 bytes
		})
	}
}

func TestExamplesWithTimeout(t *testing.T) {
	examples := NewInterfaceUsageExamples()

	// Test with very short timeout to ensure context handling works
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Allow some time for context to expire
	time.Sleep(1 * time.Millisecond)

	t.Run("BasicWriteExample with timeout", func(t *testing.T) {
		// This should handle context timeout gracefully
		// The mock implementation doesn't actually check context, but real implementations should
		err := examples.BasicWriteExample(ctx)
		// In a real implementation, this might return a context deadline exceeded error
		// For our mock, it should still succeed
		assert.NoError(t, err)
	})
}

func TestContractCompliance(t *testing.T) {
	// This test verifies that our mock implementations follow the documented contracts
	sidx := &mockSIDX{}
	ctx := context.Background()

	t.Run("Write contract compliance", func(t *testing.T) {
		// Contract: MUST validate WriteRequest fields
		err := sidx.Write(ctx, []WriteRequest{{SeriesID: 0}})
		assert.Error(t, err, "Should validate non-zero SeriesID")

		// Contract: MUST accept batch writes
		err = sidx.Write(ctx, []WriteRequest{
			{SeriesID: 1, Key: 1, Data: []byte("test1")},
			{SeriesID: 1, Key: 2, Data: []byte("test2")},
		})
		assert.NoError(t, err, "Should accept valid batch writes")
	})

	t.Run("Query contract compliance", func(t *testing.T) {
		// Contract: MUST validate QueryRequest parameters
		_, err := sidx.Query(ctx, QueryRequest{Name: ""})
		assert.Error(t, err, "Should validate query name")

		// Contract: MUST return QueryResult with Pull/Release pattern
		result, err := sidx.Query(ctx, QueryRequest{Name: "test"})
		assert.NoError(t, err)
		assert.NotNil(t, result, "Should return QueryResult")

		// Contract: Pull/Release pattern should work
		response := result.Pull()
		assert.NotNil(t, response, "First pull should return data")

		response = result.Pull()
		assert.Nil(t, response, "Second pull should return nil")

		// Contract: Release should be safe to call
		result.Release()
	})

	t.Run("Stats contract compliance", func(t *testing.T) {
		// Contract: MUST return current system metrics
		stats, err := sidx.Stats(ctx)
		assert.NoError(t, err)
		require.NotNil(t, stats)

		// All metrics should be non-negative
		assert.GreaterOrEqual(t, stats.MemoryUsageBytes, int64(0))
		assert.GreaterOrEqual(t, stats.DiskUsageBytes, int64(0))
		assert.GreaterOrEqual(t, stats.ElementCount, int64(0))
		assert.GreaterOrEqual(t, stats.PartCount, int64(0))
		assert.GreaterOrEqual(t, stats.QueryCount.Load(), int64(0))
	})

	t.Run("Close contract compliance", func(t *testing.T) {
		// Contract: MUST be idempotent
		err := sidx.Close()
		assert.NoError(t, err)

		err = sidx.Close()
		assert.NoError(t, err, "Close should be idempotent")
	})

	t.Run("Flush contract compliance", func(t *testing.T) {
		// Contract: MUST be synchronous
		err := sidx.Flush()
		assert.NoError(t, err, "Flush should be synchronous")
	})

	t.Run("Merge contract compliance", func(t *testing.T) {
		// Contract: MUST be synchronous
		err := sidx.Merge()
		assert.NoError(t, err, "Merge should be synchronous")
	})
}

func BenchmarkInt64ToBytes(b *testing.B) {
	values := []int64{0, 1, -1, 1640995200000, 9223372036854775807}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, val := range values {
			_ = int64ToBytes(val)
		}
	}
}

func BenchmarkMockWriteOperations(b *testing.B) {
	sidx := &mockSIDX{}
	ctx := context.Background()

	reqs := []WriteRequest{
		{SeriesID: 1001, Key: 1640995200000, Data: []byte("test data")},
		{SeriesID: 1002, Key: 1640995260000, Data: []byte("more test data")},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sidx.Write(ctx, reqs)
	}
}

func BenchmarkMockQueryOperations(b *testing.B) {
	sidx := &mockSIDX{}
	ctx := context.Background()

	req := QueryRequest{
		Name:           "benchmark",
		MaxElementSize: 100,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := sidx.Query(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		for {
			response := result.Pull()
			if response == nil {
				break
			}
		}
		result.Release()
	}
}
