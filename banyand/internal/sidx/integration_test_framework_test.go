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
)

func TestIntegrationTestFramework_Creation(t *testing.T) {
	config := DefaultFrameworkConfig()
	framework := NewIntegrationTestFramework(config)

	assert.NotNil(t, framework)
	assert.NotNil(t, framework.GetSIDX())
	assert.NotNil(t, framework.GetComponents())
	assert.Equal(t, config.MaxConcurrency, framework.config.MaxConcurrency)

	// Should have default scenarios, benchmarks, and stress tests
	assert.GreaterOrEqual(t, len(framework.scenarios), 3)
	assert.GreaterOrEqual(t, len(framework.benchmarks), 2)
	assert.GreaterOrEqual(t, len(framework.stressTests), 2)
}

func TestIntegrationTestFramework_WithCustomSIDX(t *testing.T) {
	config := DefaultFrameworkConfig()
	mockSIDX := NewMockSIDX(DefaultMockConfig())
	framework := NewIntegrationTestFrameworkWithSIDX(mockSIDX, config)

	assert.NotNil(t, framework)
	assert.Equal(t, mockSIDX, framework.GetSIDX())
	assert.Nil(t, framework.GetComponents()) // Should be nil when using custom SIDX
}

func TestIntegrationTestFramework_RegisterCustomComponents(t *testing.T) {
	config := DefaultFrameworkConfig()
	framework := NewIntegrationTestFramework(config)

	initialScenarios := len(framework.scenarios)
	initialBenchmarks := len(framework.benchmarks)
	initialStressTests := len(framework.stressTests)

	// Register custom scenario
	customScenario := TestScenario{
		Name:        "CustomScenario",
		Description: "A custom test scenario",
		Execute: func(_ context.Context, _ *IntegrationTestFramework) error {
			return nil
		},
	}
	framework.RegisterScenario(customScenario)

	// Register custom benchmark
	customBenchmark := Benchmark{
		Name:        "CustomBenchmark",
		Description: "A custom benchmark",
		Execute: func(_ context.Context, _ *IntegrationTestFramework) BenchmarkResult {
			return BenchmarkResult{OperationsPerSecond: 1000}
		},
	}
	framework.RegisterBenchmark(customBenchmark)

	// Register custom stress test
	customStressTest := StressTest{
		Name:        "CustomStressTest",
		Description: "A custom stress test",
		Concurrency: 5,
		Operations:  100,
		Execute: func(_ context.Context, _ *IntegrationTestFramework, _ int) error {
			return nil
		},
	}
	framework.RegisterStressTest(customStressTest)

	// Verify components were added
	assert.Equal(t, initialScenarios+1, len(framework.scenarios))
	assert.Equal(t, initialBenchmarks+1, len(framework.benchmarks))
	assert.Equal(t, initialStressTests+1, len(framework.stressTests))
}

func TestIntegrationTestFramework_GenerateTestData(t *testing.T) {
	config := DefaultFrameworkConfig()
	framework := NewIntegrationTestFramework(config)

	seriesCount := 3
	elementsPerSeries := 5
	data := framework.GenerateTestData(seriesCount, elementsPerSeries)

	expectedTotal := seriesCount * elementsPerSeries
	assert.Len(t, data, expectedTotal)

	// Verify data structure
	for i, req := range data {
		assert.Greater(t, int(req.SeriesID), 0, "SeriesID should be positive for element %d", i)
		assert.Greater(t, req.Key, int64(0), "Key should be positive for element %d", i)
		assert.NotEmpty(t, req.Data, "Data should not be empty for element %d", i)
		assert.NotEmpty(t, req.Tags, "Tags should not be empty for element %d", i)

		// Verify specific tags
		hasSeriesTag := false
		hasSequenceTag := false
		hasServiceTag := false

		for _, tag := range req.Tags {
			switch tag.name {
			case "series_id":
				hasSeriesTag = true
			case "sequence":
				hasSequenceTag = true
			case "service":
				hasServiceTag = true
			}
		}

		assert.True(t, hasSeriesTag, "Should have series_id tag for element %d", i)
		assert.True(t, hasSequenceTag, "Should have sequence tag for element %d", i)
		assert.True(t, hasServiceTag, "Should have service tag for element %d", i)
	}
}

func TestIntegrationTestFramework_RunScenarios(t *testing.T) {
	config := DefaultFrameworkConfig()
	config.EnableVerboseLogging = true
	framework := NewIntegrationTestFramework(config)

	ctx := context.Background()
	results, err := framework.RunScenarios(ctx)
	require.NoError(t, err)

	// Should have results for all default scenarios
	assert.GreaterOrEqual(t, len(results), 3)

	// Verify result structure
	for _, result := range results {
		assert.NotEmpty(t, result.Name)
		assert.Greater(t, result.Duration, time.Duration(0))

		if !result.Success {
			t.Logf("Scenario %s failed: %v", result.Name, result.Error)
		}
	}

	// At least some scenarios should succeed
	successCount := 0
	for _, result := range results {
		if result.Success {
			successCount++
		}
	}
	assert.Greater(t, successCount, 0, "At least one scenario should succeed")
}

func TestIntegrationTestFramework_RunBenchmarks(t *testing.T) {
	config := DefaultFrameworkConfig()
	config.BenchmarkDuration = 2 * time.Second // Shorter for tests
	config.EnableVerboseLogging = true
	framework := NewIntegrationTestFramework(config)

	ctx := context.Background()
	results, err := framework.RunBenchmarks(ctx)
	require.NoError(t, err)

	// Should have results for all default benchmarks
	assert.GreaterOrEqual(t, len(results), 2)

	// Verify result structure
	for name, result := range results {
		assert.NotEmpty(t, name)
		assert.Greater(t, result.TotalOperations, int64(0))
		assert.Greater(t, result.Duration, time.Duration(0))
		assert.GreaterOrEqual(t, result.OperationsPerSecond, 0.0)

		t.Logf("Benchmark %s: %.2f ops/sec, %d total ops",
			name, result.OperationsPerSecond, result.TotalOperations)
	}
}

func TestIntegrationTestFramework_RunStressTests(t *testing.T) {
	config := DefaultFrameworkConfig()
	config.StressDuration = 2 * time.Second // Shorter for tests
	config.MaxConcurrency = 4               // Limit concurrency for tests
	config.EnableVerboseLogging = true
	framework := NewIntegrationTestFramework(config)

	ctx := context.Background()
	results, err := framework.RunStressTests(ctx)
	require.NoError(t, err)

	// Should have results for all default stress tests
	assert.GreaterOrEqual(t, len(results), 2)

	// Verify result structure
	for name, result := range results {
		assert.NotEmpty(t, name)
		assert.Greater(t, result.TotalOperations, int64(0))
		assert.Greater(t, result.Duration, time.Duration(0))
		assert.GreaterOrEqual(t, result.ThroughputOpsPerS, 0.0)
		assert.GreaterOrEqual(t, result.ErrorRate, 0.0)
		assert.LessOrEqual(t, result.ErrorRate, 1.0)
		assert.Equal(t, result.TotalOperations, result.SuccessfulOps+result.FailedOps)

		t.Logf("Stress Test %s: %.2f ops/sec, %.2f%% error rate",
			name, result.ThroughputOpsPerS, result.ErrorRate*100)
	}
}

func TestIntegrationTestFramework_RunAll(t *testing.T) {
	config := DefaultFrameworkConfig()
	config.BenchmarkDuration = 1 * time.Second
	config.StressDuration = 1 * time.Second
	config.MaxConcurrency = 2
	framework := NewIntegrationTestFramework(config)

	ctx := context.Background()
	results, err := framework.RunAll(ctx)
	require.NoError(t, err)

	// Should have all three result categories
	assert.Contains(t, results, "scenarios")
	assert.Contains(t, results, "benchmarks")
	assert.Contains(t, results, "stress_tests")

	// Verify scenario results
	scenarioResults, ok := results["scenarios"].([]ScenarioResult)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(scenarioResults), 3)

	// Verify benchmark results
	benchmarkResults, ok := results["benchmarks"].(map[string]BenchmarkResult)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(benchmarkResults), 2)

	// Verify stress test results
	stressResults, ok := results["stress_tests"].(map[string]StressTestResult)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(stressResults), 2)
}

func TestIntegrationTestFramework_PercentileCalculation(t *testing.T) {
	// Test percentile calculation with known values
	latencies := []time.Duration{
		1 * time.Millisecond,
		2 * time.Millisecond,
		3 * time.Millisecond,
		4 * time.Millisecond,
		5 * time.Millisecond,
		6 * time.Millisecond,
		7 * time.Millisecond,
		8 * time.Millisecond,
		9 * time.Millisecond,
		10 * time.Millisecond,
	}

	p50, p95, p99 := calculatePercentiles(latencies)

	// With 10 elements: P50 = 5th element (index 4), P95 = 9th element (index 8), P99 = 10th element (index 9)
	assert.Equal(t, 5*time.Millisecond, p50)
	assert.Equal(t, 9*time.Millisecond, p95)
	assert.Equal(t, 10*time.Millisecond, p99)
}

func TestIntegrationTestFramework_PercentileCalculationEmpty(t *testing.T) {
	// Test with empty slice
	var latencies []time.Duration
	p50, p95, p99 := calculatePercentiles(latencies)

	assert.Equal(t, time.Duration(0), p50)
	assert.Equal(t, time.Duration(0), p95)
	assert.Equal(t, time.Duration(0), p99)
}

func TestIntegrationTestFramework_CustomScenario(t *testing.T) {
	config := DefaultFrameworkConfig()
	framework := NewIntegrationTestFramework(config)

	var setupCalled, executeCalled, validateCalled, cleanupCalled bool

	customScenario := TestScenario{
		Name:        "TestCustomScenario",
		Description: "Test that all phases are called",
		Setup: func(_ context.Context, _ *IntegrationTestFramework) error {
			setupCalled = true
			return nil
		},
		Execute: func(_ context.Context, _ *IntegrationTestFramework) error {
			executeCalled = true
			return nil
		},
		Validate: func(_ context.Context, _ *IntegrationTestFramework) error {
			validateCalled = true
			return nil
		},
		Cleanup: func(_ context.Context, _ *IntegrationTestFramework) error {
			cleanupCalled = true
			return nil
		},
	}

	// Clear existing scenarios and add only our custom one
	framework.scenarios = []TestScenario{customScenario}

	ctx := context.Background()
	results, err := framework.RunScenarios(ctx)
	require.NoError(t, err)

	assert.Len(t, results, 1)
	assert.True(t, results[0].Success)
	assert.Equal(t, "TestCustomScenario", results[0].Name)

	// Verify all phases were called
	assert.True(t, setupCalled, "Setup should have been called")
	assert.True(t, executeCalled, "Execute should have been called")
	assert.True(t, validateCalled, "Validate should have been called")
	assert.True(t, cleanupCalled, "Cleanup should have been called")
}

func TestIntegrationTestFramework_ScenarioFailure(t *testing.T) {
	config := DefaultFrameworkConfig()
	framework := NewIntegrationTestFramework(config)

	failingScenario := TestScenario{
		Name:        "FailingScenario",
		Description: "A scenario that fails during execution",
		Execute: func(_ context.Context, _ *IntegrationTestFramework) error {
			return fmt.Errorf("intentional failure")
		},
	}

	// Clear existing scenarios and add only our failing one
	framework.scenarios = []TestScenario{failingScenario}

	ctx := context.Background()
	results, err := framework.RunScenarios(ctx)
	require.NoError(t, err)

	assert.Len(t, results, 1)
	assert.False(t, results[0].Success)
	assert.Contains(t, results[0].Error.Error(), "intentional failure")
}

func TestIntegrationTestFramework_MemoryProfiling(t *testing.T) {
	config := DefaultFrameworkConfig()
	config.EnableMemoryProfiling = true
	framework := NewIntegrationTestFramework(config)

	memoryScenario := TestScenario{
		Name:        "MemoryScenario",
		Description: "Test memory profiling",
		Execute: func(_ context.Context, _ *IntegrationTestFramework) error {
			// Allocate some memory
			_ = make([]byte, 1024*1024) // 1MB
			return nil
		},
	}

	framework.scenarios = []TestScenario{memoryScenario}

	ctx := context.Background()
	results, err := framework.RunScenarios(ctx)
	require.NoError(t, err)

	assert.Len(t, results, 1)
	assert.True(t, results[0].Success)
	// Memory usage should be tracked (may be negative due to GC)
	assert.NotZero(t, results[0].MemoryUsed)
}

func TestIntegrationTestFramework_DefaultConfiguration(t *testing.T) {
	config := DefaultFrameworkConfig()

	assert.False(t, config.EnableVerboseLogging)
	assert.Greater(t, config.MaxConcurrency, 0)
	assert.Greater(t, config.DefaultTimeout, time.Duration(0))
	assert.Greater(t, config.BenchmarkDuration, time.Duration(0))
	assert.Greater(t, config.StressDuration, time.Duration(0))
	assert.True(t, config.EnableMemoryProfiling)
}

func TestIntegrationTestFramework_BenchmarkErrorHandling(t *testing.T) {
	config := DefaultFrameworkConfig()
	framework := NewIntegrationTestFramework(config)

	errorBenchmark := Benchmark{
		Name:        "ErrorBenchmark",
		Description: "A benchmark with setup error",
		Setup: func(_ context.Context, _ *IntegrationTestFramework) error {
			return fmt.Errorf("setup error")
		},
		Execute: func(_ context.Context, _ *IntegrationTestFramework) BenchmarkResult {
			return BenchmarkResult{OperationsPerSecond: 1000}
		},
	}

	framework.benchmarks = []Benchmark{errorBenchmark}

	ctx := context.Background()
	results, err := framework.RunBenchmarks(ctx)
	require.NoError(t, err)

	assert.Len(t, results, 1)
	result := results["ErrorBenchmark"]
	assert.Equal(t, int64(1), result.ErrorCount) // Setup error should be reflected
}

func TestIntegrationTestFramework_StressTestConfiguration(t *testing.T) {
	config := DefaultFrameworkConfig()
	config.MaxConcurrency = 3
	framework := NewIntegrationTestFramework(config)

	testStress := StressTest{
		Name:        "ConcurrencyTest",
		Description: "Test concurrency configuration",
		Concurrency: 0, // Should use framework max
		Operations:  10,
		Execute: func(_ context.Context, _ *IntegrationTestFramework, _ int) error {
			// Simple operation to test concurrency
			return nil
		},
	}

	framework.stressTests = []StressTest{testStress}

	ctx := context.Background()
	results, err := framework.RunStressTests(ctx)
	require.NoError(t, err)

	assert.Len(t, results, 1)
	result := results["ConcurrencyTest"]
	assert.Equal(t, 3, result.Concurrency) // Should use framework max
	assert.Greater(t, result.TotalOperations, int64(0))
}

func BenchmarkIntegrationTestFramework_WritePerformance(b *testing.B) {
	config := DefaultFrameworkConfig()
	config.EnableVerboseLogging = false
	framework := NewIntegrationTestFramework(config)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		requests := framework.GenerateTestData(1, 10)
		err := framework.GetSIDX().Write(ctx, requests)
		if err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}
}

func BenchmarkIntegrationTestFramework_QueryPerformance(b *testing.B) {
	config := DefaultFrameworkConfig()
	config.EnableVerboseLogging = false
	framework := NewIntegrationTestFramework(config)

	ctx := context.Background()

	// Pre-populate with data
	requests := framework.GenerateTestData(5, 100)
	err := framework.GetSIDX().Write(ctx, requests)
	if err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		queryReq := QueryRequest{
			Name:           fmt.Sprintf("bench-query-%d", i),
			MaxElementSize: 50,
		}

		result, err := framework.GetSIDX().Query(ctx, queryReq)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		// Consume results
		for {
			response := result.Pull()
			if response == nil {
				break
			}
		}
		result.Release()
	}
}
