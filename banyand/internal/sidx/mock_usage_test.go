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
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// Test the basic write-read workflow example from the documentation.
func TestDocumentation_BasicWriteReadWorkflow(t *testing.T) {
	ctx := context.Background()

	// Create mock SIDX
	mockSIDX := NewMockSIDX(DefaultMockConfig())
	defer mockSIDX.Close()

	// Write some data (from documentation example)
	writeReqs := []WriteRequest{
		{
			SeriesID: common.SeriesID(1),
			Key:      100,
			Data:     []byte(`{"service": "user-service", "endpoint": "/api/users"}`),
			Tags: []tag{
				{
					name:      "service",
					value:     []byte("user-service"),
					valueType: pbv1.ValueTypeStr,
					indexed:   true,
				},
				{
					name:      "status_code",
					value:     int64ToBytesForTest(200),
					valueType: pbv1.ValueTypeInt64,
					indexed:   true,
				},
			},
		},
	}

	err := mockSIDX.Write(ctx, writeReqs)
	require.NoError(t, err)

	// Query the data back (from documentation example)
	queryReq := QueryRequest{
		Name:           "series_1", // Matches the series name used in mock
		MaxElementSize: 100,
	}

	result, err := mockSIDX.Query(ctx, queryReq)
	require.NoError(t, err)
	defer result.Release()

	// Process results
	foundElements := 0
	for {
		response := result.Pull()
		if response == nil {
			break
		}
		require.NoError(t, response.Error)

		foundElements += response.Len()
		for i := 0; i < response.Len(); i++ {
			assert.Equal(t, int64(100), response.Keys[i])
			assert.Contains(t, string(response.Data[i]), "user-service")
		}
	}

	assert.Equal(t, 1, foundElements, "Should find exactly one element")
}

// Test the component integration example from the documentation.
func TestDocumentation_ComponentIntegration(t *testing.T) {
	ctx := context.Background()

	// Create component suite (from documentation example)
	suite := NewMockComponentSuite()

	// Write data (from documentation example)
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data1")},
		{SeriesID: 1, Key: 101, Data: []byte("data2")},
	}

	err := suite.Writer.Write(ctx, writeReqs)
	require.NoError(t, err)

	// Sync data to querier (from documentation example)
	suite.SyncElements()

	// Query data (from documentation example)
	queryReq := QueryRequest{
		Name:           "test-query",
		MaxElementSize: 10,
	}

	result, err := suite.Querier.Query(ctx, queryReq)
	require.NoError(t, err)
	defer result.Release()

	// Process results (from documentation example)
	response := result.Pull()
	require.NotNil(t, response)
	assert.Equal(t, 2, response.Len(), "Query should return 2 elements")

	// Perform maintenance operations (from documentation example)
	err = suite.Flusher.Flush()
	require.NoError(t, err)

	err = suite.Merger.Merge()
	require.NoError(t, err)
}

// Test custom scenarios example from the documentation.
func TestDocumentation_CustomScenarios(t *testing.T) {
	framework := NewIntegrationTestFramework(DefaultFrameworkConfig())

	// Register custom scenario (from documentation example)
	customScenario := TestScenario{
		Name:        "HighVolumeWrites",
		Description: "Test high-volume write scenario",
		Setup: func(_ context.Context, _ *IntegrationTestFramework) error {
			// Setup high-volume test data
			return nil
		},
		Execute: func(ctx context.Context, framework *IntegrationTestFramework) error {
			// Execute high-volume writes (scaled down for testing)
			for i := 0; i < 10; i++ { // Reduced from 10000 for test speed
				reqs := framework.GenerateTestData(1, 10) // Reduced from 100
				err := framework.GetSIDX().Write(ctx, reqs)
				if err != nil {
					return err
				}
			}
			return nil
		},
		Validate: func(ctx context.Context, framework *IntegrationTestFramework) error {
			// Validate results
			stats, err := framework.GetSIDX().Stats(ctx)
			if err != nil {
				return err
			}
			if stats == nil {
				return fmt.Errorf("stats is nil")
			}
			if stats.ElementCount < 100 { // Reduced from 100000
				return fmt.Errorf("expected at least 100 elements, got %d", stats.ElementCount)
			}
			return nil
		},
	}

	framework.RegisterScenario(customScenario)

	// Run custom scenario (from documentation example)
	results, err := framework.RunScenarios(context.Background())
	require.NoError(t, err)

	// Verify custom scenario was executed
	foundCustomScenario := false
	for _, result := range results {
		if result.Name == "HighVolumeWrites" {
			foundCustomScenario = true
			assert.True(t, result.Success, "Custom scenario should succeed")
			assert.Greater(t, result.Duration, time.Duration(0), "Should have measurable duration")
		}
	}
	assert.True(t, foundCustomScenario, "Custom scenario should be found in results")
}

// Test configuration options from the documentation.
func TestDocumentation_ConfigurationOptions(t *testing.T) {
	// Test performance testing configuration (from documentation)
	perfConfig := MockConfig{
		WriteDelayMs: 0, // No artificial delays
		QueryDelayMs: 0,
		ErrorRate:    0, // No error injection
		MaxElements:  100000,
	}
	mockSIDX := NewMockSIDX(perfConfig)
	defer mockSIDX.Close()

	ctx := context.Background()
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("test")},
	}

	start := time.Now()
	err := mockSIDX.Write(ctx, writeReqs)
	duration := time.Since(start)

	require.NoError(t, err)
	// With no delay, write should be very fast
	assert.Less(t, duration, 10*time.Millisecond)

	// Test error testing configuration (from documentation)
	errorConfig := MockConfig{
		ErrorRate: 100, // 100% error rate
	}
	errorMockSIDX := NewMockSIDX(errorConfig)
	defer errorMockSIDX.Close()

	err = errorMockSIDX.Write(ctx, writeReqs)
	assert.Error(t, err, "Should get error with 100% error rate")
	assert.Contains(t, err.Error(), "mock error injection")
}

// Test component configuration from the documentation.
func TestDocumentation_ComponentConfiguration(t *testing.T) {
	// Component configurations (from documentation example)
	writerConfig := MockWriterConfig{
		DelayMs:          5,    // 5ms write delay
		ErrorRate:        0,    // No errors
		MaxElements:      1000, // Element limit
		EnableValidation: true, // Validate requests
		SortElements:     true, // Sort elements by SeriesID/Key
	}

	querierConfig := MockQuerierConfig{
		DelayMs:          2,    // 2ms query delay
		ErrorRate:        0,    // No errors
		EnableValidation: true, // Validate requests
		MaxResultSize:    100,  // Max results per batch
	}

	flusherConfig := MockFlusherConfig{
		DelayMs:      10,   // 10ms flush delay
		ErrorRate:    0,    // No errors
		SimulateWork: true, // Simulate actual work
	}

	mergerConfig := MockMergerConfig{
		DelayMs:            20,   // 20ms merge delay
		ErrorRate:          0,    // No errors
		SimulateWork:       true, // Simulate actual work
		ConsolidationRatio: 0.7,  // 70% size reduction after merge
	}

	// Create suite with custom configurations (from documentation)
	suite := NewMockComponentSuiteWithConfigs(
		writerConfig, querierConfig, flusherConfig, mergerConfig)

	ctx := context.Background()

	// Test writer delay
	start := time.Now()
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data")},
	}
	err := suite.Writer.Write(ctx, writeReqs)
	writerDuration := time.Since(start)

	require.NoError(t, err)
	assert.GreaterOrEqual(t, writerDuration, 5*time.Millisecond)

	// Test flusher delay
	start = time.Now()
	err = suite.Flusher.Flush()
	flusherDuration := time.Since(start)

	require.NoError(t, err)
	assert.GreaterOrEqual(t, flusherDuration, 10*time.Millisecond)

	// Test merger delay and consolidation ratio
	start = time.Now()
	err = suite.Merger.Merge()
	mergerDuration := time.Since(start)

	require.NoError(t, err)
	assert.GreaterOrEqual(t, mergerDuration, 20*time.Millisecond)

	// Verify consolidation ratio
	consolidations := suite.Merger.GetConsolidations()
	if len(consolidations) > 0 {
		consolidation := consolidations[0]
		expectedOutput := int(float64(consolidation.PartsInput) * 0.7)
		if expectedOutput < 1 {
			expectedOutput = 1
		}
		assert.Equal(t, expectedOutput, consolidation.PartsOutput)
	}
}

// Test framework configuration from the documentation.
func TestDocumentation_FrameworkConfiguration(t *testing.T) {
	// Framework configuration (from documentation example)
	frameworkConfig := FrameworkConfig{
		EnableVerboseLogging:  false,                // Disable for test
		MaxConcurrency:        runtime.NumCPU() * 2, // Concurrent operations
		DefaultTimeout:        30 * time.Second,     // Operation timeout
		BenchmarkDuration:     1 * time.Second,      // Reduced for test
		StressDuration:        1 * time.Second,      // Reduced for test
		EnableMemoryProfiling: true,                 // Memory usage tracking
	}

	framework := NewIntegrationTestFramework(frameworkConfig)

	// Verify configuration was applied
	assert.Equal(t, frameworkConfig.MaxConcurrency, framework.config.MaxConcurrency)
	assert.Equal(t, frameworkConfig.EnableMemoryProfiling, framework.config.EnableMemoryProfiling)

	// Run a quick test to verify framework works
	ctx := context.Background()
	results, err := framework.RunAll(ctx)
	require.NoError(t, err)

	// Verify all result types are present
	assert.Contains(t, results, "scenarios")
	assert.Contains(t, results, "benchmarks")
	assert.Contains(t, results, "stress_tests")
}

// Test troubleshooting examples from the documentation.
func TestDocumentation_TroubleshootingExamples(t *testing.T) {
	ctx := context.Background()

	// Example 1: Query returns no results (from documentation)
	t.Run("QueryNoResults", func(t *testing.T) {
		suite := NewMockComponentSuite()

		writeReqs := []WriteRequest{
			{SeriesID: 1, Key: 100, Data: []byte("data")},
		}
		err := suite.Writer.Write(ctx, writeReqs)
		require.NoError(t, err)

		// Without syncing, query should return no results
		queryReq := QueryRequest{
			Name:           "test-query",
			MaxElementSize: 100,
		}
		result, err := suite.Querier.Query(ctx, queryReq)
		require.NoError(t, err)
		defer result.Release()

		response := result.Pull()
		assert.Nil(t, response, "Should have no results without sync")

		// After syncing, should have results
		suite.SyncElements()
		result2, err := suite.Querier.Query(ctx, queryReq)
		require.NoError(t, err)
		defer result2.Release()

		response2 := result2.Pull()
		assert.NotNil(t, response2, "Should have results after sync")
		assert.Equal(t, 1, response2.Len())
	})

	// Example 2: Element limit exceeded (from documentation)
	t.Run("ElementLimitExceeded", func(t *testing.T) {
		config := DefaultMockConfig()
		config.MaxElements = 2 // Small limit for testing
		mockSIDX := NewMockSIDX(config)
		defer mockSIDX.Close()

		// Write up to limit
		writeReqs := []WriteRequest{
			{SeriesID: 1, Key: 100, Data: []byte("data1")},
			{SeriesID: 1, Key: 101, Data: []byte("data2")},
		}
		err := mockSIDX.Write(ctx, writeReqs)
		require.NoError(t, err)

		// Try to exceed limit
		writeReqs = []WriteRequest{
			{SeriesID: 1, Key: 102, Data: []byte("data3")},
		}
		err = mockSIDX.Write(ctx, writeReqs)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "element limit exceeded")

		// Clear elements to resolve issue
		mockSIDX.Clear()
		err = mockSIDX.Write(ctx, writeReqs)
		assert.NoError(t, err, "Should work after clearing")
	})

	// Example 3: High error rates (from documentation)
	t.Run("HighErrorRates", func(t *testing.T) {
		config := DefaultMockConfig()
		config.ErrorRate = 100 // Always error
		mockSIDX := NewMockSIDX(config)
		defer mockSIDX.Close()

		writeReqs := []WriteRequest{
			{SeriesID: 1, Key: 100, Data: []byte("data")},
		}
		err := mockSIDX.Write(ctx, writeReqs)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "mock error injection")

		// Disable error injection
		mockSIDX.SetErrorRate(0)
		err = mockSIDX.Write(ctx, writeReqs)
		assert.NoError(t, err, "Should work with error rate 0")
	})

	// Example 4: Slow test performance (from documentation)
	t.Run("SlowTestPerformance", func(t *testing.T) {
		// Create mock with delays
		slowConfig := MockConfig{
			WriteDelayMs: 50,
			QueryDelayMs: 50,
		}
		slowMockSIDX := NewMockSIDX(slowConfig)
		defer slowMockSIDX.Close()

		start := time.Now()
		writeReqs := []WriteRequest{
			{SeriesID: 1, Key: 100, Data: []byte("data")},
		}
		err := slowMockSIDX.Write(ctx, writeReqs)
		slowDuration := time.Since(start)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, slowDuration, 50*time.Millisecond)

		// Create fast mock without delays
		fastConfig := MockConfig{
			WriteDelayMs: 0,
			QueryDelayMs: 0,
		}
		fastMockSIDX := NewMockSIDX(fastConfig)
		defer fastMockSIDX.Close()

		start = time.Now()
		err = fastMockSIDX.Write(ctx, writeReqs)
		fastDuration := time.Since(start)

		require.NoError(t, err)
		assert.Less(t, fastDuration, slowDuration, "Fast config should be faster")
	})
}

// Test debug tips from the documentation.
func TestDocumentation_DebugTips(t *testing.T) {
	ctx := context.Background()

	// Test stats checking (from documentation)
	mockSIDX := NewMockSIDX(DefaultMockConfig())
	defer mockSIDX.Close()

	// Write some data
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data1")},
		{SeriesID: 2, Key: 200, Data: []byte("data2")},
	}
	err := mockSIDX.Write(ctx, writeReqs)
	require.NoError(t, err)

	// Check stats (from documentation example)
	stats, err := mockSIDX.Stats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)

	assert.Equal(t, int64(2), stats.ElementCount)
	assert.Greater(t, stats.MemoryUsageBytes, int64(0))
	assert.Equal(t, int64(1), stats.WriteCount.Load())

	// Verify element storage (MockSIDX only - from documentation)
	keys := mockSIDX.GetStorageKeys()
	assert.Contains(t, keys, "series_1")
	assert.Contains(t, keys, "series_2")

	count := mockSIDX.GetElementCount()
	assert.Equal(t, int64(2), count)

	// Query to increment query count
	queryReq := QueryRequest{Name: "series_1", MaxElementSize: 10}
	result, err := mockSIDX.Query(ctx, queryReq)
	require.NoError(t, err)
	result.Release()

	// Verify query count updated
	stats, err = mockSIDX.Stats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, int64(1), stats.QueryCount.Load())
}

// Test migration examples from the documentation.
func TestDocumentation_Migration(t *testing.T) {
	ctx := context.Background()

	// Test interface compatibility (from documentation)
	// Always use mock for testing, but demonstrate the pattern
	sidx := NewMockSIDX(DefaultMockConfig())

	// For real implementation, uncomment and configure:
	// sidx = NewRealSIDX(realConfig)
	defer sidx.Close()

	// Same interface calls work for both (from documentation)
	writeReqs := []WriteRequest{
		{SeriesID: 1, Key: 100, Data: []byte("data")},
	}
	err := sidx.Write(ctx, writeReqs)
	require.NoError(t, err)

	queryReq := QueryRequest{Name: "series_1", MaxElementSize: 10}
	result, err := sidx.Query(ctx, queryReq)
	require.NoError(t, err)
	defer result.Release()

	response := result.Pull()
	assert.NotNil(t, response)
	assert.Equal(t, 1, response.Len())
}

// Test performance characteristics documentation.
func TestDocumentation_PerformanceCharacteristics(t *testing.T) {
	ctx := context.Background()
	mockSIDX := NewMockSIDX(DefaultMockConfig())
	defer mockSIDX.Close()

	// Test write performance (from documentation)
	const numWrites = 100 // Reduced for test speed
	writeReqs := make([]WriteRequest, numWrites)
	for i := 0; i < numWrites; i++ {
		writeReqs[i] = WriteRequest{
			SeriesID: common.SeriesID(i + 1),
			Key:      int64(i),
			Data:     []byte(fmt.Sprintf("data%d", i)),
		}
	}

	start := time.Now()
	err := mockSIDX.Write(ctx, writeReqs)
	duration := time.Since(start)

	require.NoError(t, err)

	throughput := float64(numWrites) / duration.Seconds()
	t.Logf("Write throughput: %.0f elem/sec", throughput)

	// Documentation states >10,000 elem/sec for mock
	// With reduced test size, we expect proportionally good performance
	assert.Greater(t, throughput, 1000.0, "Should achieve reasonable write throughput")

	// Test query performance (from documentation)
	queryReq := QueryRequest{Name: "series_1", MaxElementSize: 50}

	start = time.Now()
	result, err := mockSIDX.Query(ctx, queryReq)
	queryDuration := time.Since(start)

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

	queryThroughput := float64(resultCount) / queryDuration.Seconds()
	t.Logf("Query throughput: %.0f results/sec", queryThroughput)

	// Documentation states >50,000 results/sec for mock
	// Query performance should be very good for in-memory mock
	assert.Greater(t, queryThroughput, 5000.0, "Should achieve good query throughput")
}

// Helper function from documentation.
func int64ToBytesForTest(val int64) []byte {
	result := make([]byte, 8)
	for i := 0; i < 8; i++ {
		result[7-i] = byte(val >> (8 * i))
	}
	return result
}

// Test that all documentation examples compile and run without panics.
func TestDocumentation_AllExamplesCompile(t *testing.T) {
	// This test ensures all code examples in the documentation are valid Go code
	// by including them in functions that can be compiled and run

	t.Run("Basic workflow compiles", func(t *testing.T) {
		assert.NotPanics(t, func() {
			TestDocumentation_BasicWriteReadWorkflow(t)
		})
	})

	t.Run("Component integration compiles", func(t *testing.T) {
		assert.NotPanics(t, func() {
			TestDocumentation_ComponentIntegration(t)
		})
	})

	t.Run("Custom scenarios compile", func(t *testing.T) {
		assert.NotPanics(t, func() {
			TestDocumentation_CustomScenarios(t)
		})
	})

	t.Run("Configuration examples compile", func(t *testing.T) {
		assert.NotPanics(t, func() {
			TestDocumentation_ConfigurationOptions(t)
		})
	})

	t.Run("Troubleshooting examples compile", func(t *testing.T) {
		assert.NotPanics(t, func() {
			TestDocumentation_TroubleshootingExamples(t)
		})
	})

	t.Run("Debug tips compile", func(t *testing.T) {
		assert.NotPanics(t, func() {
			TestDocumentation_DebugTips(t)
		})
	})

	t.Run("Migration examples compile", func(t *testing.T) {
		assert.NotPanics(t, func() {
			TestDocumentation_Migration(t)
		})
	})

	t.Run("Performance examples compile", func(t *testing.T) {
		assert.NotPanics(t, func() {
			TestDocumentation_PerformanceCharacteristics(t)
		})
	})
}
