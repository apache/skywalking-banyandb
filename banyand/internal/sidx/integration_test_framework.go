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
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// IntegrationTestFramework provides a comprehensive test harness for SIDX implementations
// using mock components. It supports scenario testing, benchmarking, and stress testing.
type IntegrationTestFramework struct {
	sidx        SIDX
	components  *MockComponentSuite
	scenarios   []TestScenario
	benchmarks  []Benchmark
	stressTests []StressTest
	config      FrameworkConfig
}

// FrameworkConfig provides configuration options for the integration test framework.
type FrameworkConfig struct {
	MaxConcurrency        int
	DefaultTimeout        time.Duration
	BenchmarkDuration     time.Duration
	StressDuration        time.Duration
	EnableVerboseLogging  bool
	EnableMemoryProfiling bool
}

// DefaultFrameworkConfig returns a sensible default configuration.
func DefaultFrameworkConfig() FrameworkConfig {
	return FrameworkConfig{
		EnableVerboseLogging:  false,
		MaxConcurrency:        runtime.NumCPU() * 2,
		DefaultTimeout:        30 * time.Second,
		BenchmarkDuration:     10 * time.Second,
		StressDuration:        30 * time.Second,
		EnableMemoryProfiling: true,
	}
}

// TestScenario represents a specific use case test scenario.
type TestScenario struct {
	Setup       func(ctx context.Context, framework *IntegrationTestFramework) error
	Execute     func(ctx context.Context, framework *IntegrationTestFramework) error
	Validate    func(ctx context.Context, framework *IntegrationTestFramework) error
	Cleanup     func(ctx context.Context, framework *IntegrationTestFramework) error
	Name        string
	Description string
}

// Benchmark represents a performance benchmark test.
type Benchmark struct {
	Setup       func(ctx context.Context, framework *IntegrationTestFramework) error
	Execute     func(ctx context.Context, framework *IntegrationTestFramework) BenchmarkResult
	Cleanup     func(ctx context.Context, framework *IntegrationTestFramework) error
	Name        string
	Description string
}

// StressTest represents a stress/load test configuration.
type StressTest struct {
	Setup       func(ctx context.Context, framework *IntegrationTestFramework) error
	Execute     func(ctx context.Context, framework *IntegrationTestFramework, workerID int) error
	Validate    func(ctx context.Context, framework *IntegrationTestFramework) error
	Cleanup     func(ctx context.Context, framework *IntegrationTestFramework) error
	Name        string
	Description string
	Concurrency int
	Operations  int
}

// BenchmarkResult contains the results of a benchmark test.
type BenchmarkResult struct {
	OperationsPerSecond float64
	LatencyP50          time.Duration
	LatencyP95          time.Duration
	LatencyP99          time.Duration
	TotalOperations     int64
	Duration            time.Duration
	ErrorCount          int64
	MemoryUsedBytes     int64
}

// StressTestResult contains the results of a stress test.
type StressTestResult struct {
	TotalOperations   int64
	SuccessfulOps     int64
	FailedOps         int64
	Duration          time.Duration
	Concurrency       int
	ThroughputOpsPerS float64
	ErrorRate         float64
	MemoryUsedBytes   int64
}

// ScenarioResult contains the results of a scenario test.
type ScenarioResult struct {
	Error      error
	Name       string
	Duration   time.Duration
	MemoryUsed int64
	Success    bool
}

// NewIntegrationTestFramework creates a new integration test framework with mock implementations.
func NewIntegrationTestFramework(config FrameworkConfig) *IntegrationTestFramework {
	components := NewMockComponentSuite()

	// Create a mock SIDX that properly integrates with components
	mockSIDX := NewMockSIDX(DefaultMockConfig())

	framework := &IntegrationTestFramework{
		sidx:        mockSIDX,
		components:  components,
		config:      config,
		scenarios:   make([]TestScenario, 0),
		benchmarks:  make([]Benchmark, 0),
		stressTests: make([]StressTest, 0),
	}

	// Register default scenarios, benchmarks, and stress tests
	framework.registerDefaultScenarios()
	framework.registerDefaultBenchmarks()
	framework.registerDefaultStressTests()

	return framework
}

// NewIntegrationTestFrameworkWithSIDX creates a framework with a custom SIDX implementation.
func NewIntegrationTestFrameworkWithSIDX(sidx SIDX, config FrameworkConfig) *IntegrationTestFramework {
	return &IntegrationTestFramework{
		sidx:        sidx,
		components:  nil, // Components are not used when using custom SIDX
		config:      config,
		scenarios:   make([]TestScenario, 0),
		benchmarks:  make([]Benchmark, 0),
		stressTests: make([]StressTest, 0),
	}
}

// RegisterScenario adds a custom test scenario to the framework.
func (itf *IntegrationTestFramework) RegisterScenario(scenario TestScenario) {
	itf.scenarios = append(itf.scenarios, scenario)
}

// RegisterBenchmark adds a custom benchmark to the framework.
func (itf *IntegrationTestFramework) RegisterBenchmark(benchmark Benchmark) {
	itf.benchmarks = append(itf.benchmarks, benchmark)
}

// RegisterStressTest adds a custom stress test to the framework.
func (itf *IntegrationTestFramework) RegisterStressTest(stressTest StressTest) {
	itf.stressTests = append(itf.stressTests, stressTest)
}

// RunScenarios executes all registered test scenarios.
func (itf *IntegrationTestFramework) RunScenarios(ctx context.Context) ([]ScenarioResult, error) {
	results := make([]ScenarioResult, 0, len(itf.scenarios))

	for _, scenario := range itf.scenarios {
		result := itf.runSingleScenario(ctx, scenario)
		results = append(results, result)

		if itf.config.EnableVerboseLogging {
			fmt.Printf("Scenario %s: %v (Duration: %v)\n",
				scenario.Name, result.Success, result.Duration)
			if result.Error != nil {
				fmt.Printf("  Error: %v\n", result.Error)
			}
		}
	}

	return results, nil
}

// RunBenchmarks executes all registered benchmarks.
func (itf *IntegrationTestFramework) RunBenchmarks(ctx context.Context) (map[string]BenchmarkResult, error) {
	results := make(map[string]BenchmarkResult)

	for _, benchmark := range itf.benchmarks {
		result := itf.runSingleBenchmark(ctx, benchmark)
		results[benchmark.Name] = result

		if itf.config.EnableVerboseLogging {
			fmt.Printf("Benchmark %s: %.2f ops/sec (P95: %v, P99: %v)\n",
				benchmark.Name, result.OperationsPerSecond, result.LatencyP95, result.LatencyP99)
		}
	}

	return results, nil
}

// RunStressTests executes all registered stress tests.
func (itf *IntegrationTestFramework) RunStressTests(ctx context.Context) (map[string]StressTestResult, error) {
	results := make(map[string]StressTestResult)

	for _, stressTest := range itf.stressTests {
		result := itf.runSingleStressTest(ctx, stressTest)
		results[stressTest.Name] = result

		if itf.config.EnableVerboseLogging {
			fmt.Printf("Stress Test %s: %.2f ops/sec (Error Rate: %.2f%%)\n",
				stressTest.Name, result.ThroughputOpsPerS, result.ErrorRate*100)
		}
	}

	return results, nil
}

// RunAll executes all scenarios, benchmarks, and stress tests.
func (itf *IntegrationTestFramework) RunAll(ctx context.Context) (map[string]interface{}, error) {
	results := make(map[string]interface{})

	// Run scenarios
	scenarioResults, err := itf.RunScenarios(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run scenarios: %w", err)
	}
	results["scenarios"] = scenarioResults

	// Run benchmarks
	benchmarkResults, err := itf.RunBenchmarks(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run benchmarks: %w", err)
	}
	results["benchmarks"] = benchmarkResults

	// Run stress tests
	stressResults, err := itf.RunStressTests(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run stress tests: %w", err)
	}
	results["stress_tests"] = stressResults

	return results, nil
}

// runSingleScenario executes a single test scenario.
func (itf *IntegrationTestFramework) runSingleScenario(ctx context.Context, scenario TestScenario) ScenarioResult {
	start := time.Now()
	var memBefore, memAfter runtime.MemStats

	if itf.config.EnableMemoryProfiling {
		runtime.GC()
		runtime.ReadMemStats(&memBefore)
	}

	result := ScenarioResult{
		Name:    scenario.Name,
		Success: false,
	}

	// Setup phase
	if scenario.Setup != nil {
		if err := scenario.Setup(ctx, itf); err != nil {
			result.Error = fmt.Errorf("setup failed: %w", err)
			result.Duration = time.Since(start)
			return result
		}
	}

	// Execute phase
	if scenario.Execute != nil {
		if err := scenario.Execute(ctx, itf); err != nil {
			result.Error = fmt.Errorf("execution failed: %w", err)
			result.Duration = time.Since(start)
			return result
		}
	}

	// Validate phase
	if scenario.Validate != nil {
		if err := scenario.Validate(ctx, itf); err != nil {
			result.Error = fmt.Errorf("validation failed: %w", err)
			result.Duration = time.Since(start)
			return result
		}
	}

	// Cleanup phase
	if scenario.Cleanup != nil {
		if err := scenario.Cleanup(ctx, itf); err != nil {
			result.Error = fmt.Errorf("cleanup failed: %w", err)
			result.Duration = time.Since(start)
			return result
		}
	}

	result.Success = true
	result.Duration = time.Since(start)

	if itf.config.EnableMemoryProfiling {
		runtime.GC()
		runtime.ReadMemStats(&memAfter)
		result.MemoryUsed = int64(memAfter.Alloc - memBefore.Alloc)
	}

	return result
}

// runSingleBenchmark executes a single benchmark.
func (itf *IntegrationTestFramework) runSingleBenchmark(ctx context.Context, benchmark Benchmark) BenchmarkResult {
	var memBefore, memAfter runtime.MemStats

	if itf.config.EnableMemoryProfiling {
		runtime.GC()
		runtime.ReadMemStats(&memBefore)
	}

	// Setup phase
	if benchmark.Setup != nil {
		if err := benchmark.Setup(ctx, itf); err != nil {
			return BenchmarkResult{ErrorCount: 1}
		}
	}

	// Execute benchmark
	result := benchmark.Execute(ctx, itf)

	// Cleanup phase
	if benchmark.Cleanup != nil {
		if err := benchmark.Cleanup(ctx, itf); err != nil {
			// Log cleanup error but don't fail the benchmark
			if itf.config.EnableVerboseLogging {
				fmt.Printf("Benchmark cleanup error: %v\n", err)
			}
		}
	}

	if itf.config.EnableMemoryProfiling {
		runtime.GC()
		runtime.ReadMemStats(&memAfter)
		result.MemoryUsedBytes = int64(memAfter.Alloc - memBefore.Alloc)
	}

	return result
}

// runSingleStressTest executes a single stress test.
func (itf *IntegrationTestFramework) runSingleStressTest(ctx context.Context, stressTest StressTest) StressTestResult {
	var memBefore, memAfter runtime.MemStats

	if itf.config.EnableMemoryProfiling {
		runtime.GC()
		runtime.ReadMemStats(&memBefore)
	}

	// Setup phase
	if stressTest.Setup != nil {
		if err := stressTest.Setup(ctx, itf); err != nil {
			return StressTestResult{FailedOps: 1}
		}
	}

	// Execute stress test
	var wg sync.WaitGroup
	var totalOps, successOps, failedOps int64

	start := time.Now()
	concurrency := stressTest.Concurrency
	if concurrency <= 0 {
		concurrency = itf.config.MaxConcurrency
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			opsPerWorker := stressTest.Operations / concurrency
			for j := 0; j < opsPerWorker; j++ {
				atomic.AddInt64(&totalOps, 1)
				if err := stressTest.Execute(ctx, itf, workerID); err != nil {
					atomic.AddInt64(&failedOps, 1)
				} else {
					atomic.AddInt64(&successOps, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	// Validate phase
	if stressTest.Validate != nil {
		if err := stressTest.Validate(ctx, itf); err != nil {
			// Log validation error but don't fail the stress test
			if itf.config.EnableVerboseLogging {
				fmt.Printf("Stress test validation error: %v\n", err)
			}
		}
	}

	// Cleanup phase
	if stressTest.Cleanup != nil {
		if err := stressTest.Cleanup(ctx, itf); err != nil {
			// Log cleanup error but don't fail the stress test
			if itf.config.EnableVerboseLogging {
				fmt.Printf("Stress test cleanup error: %v\n", err)
			}
		}
	}

	result := StressTestResult{
		TotalOperations:   atomic.LoadInt64(&totalOps),
		SuccessfulOps:     atomic.LoadInt64(&successOps),
		FailedOps:         atomic.LoadInt64(&failedOps),
		Duration:          duration,
		Concurrency:       concurrency,
		ThroughputOpsPerS: float64(atomic.LoadInt64(&totalOps)) / duration.Seconds(),
		ErrorRate:         float64(atomic.LoadInt64(&failedOps)) / float64(atomic.LoadInt64(&totalOps)),
	}

	if itf.config.EnableMemoryProfiling {
		runtime.GC()
		runtime.ReadMemStats(&memAfter)
		result.MemoryUsedBytes = int64(memAfter.Alloc - memBefore.Alloc)
	}

	return result
}

// GetSIDX returns the SIDX instance for direct access during tests.
func (itf *IntegrationTestFramework) GetSIDX() SIDX {
	return itf.sidx
}

// GetComponents returns the mock component suite (if available).
func (itf *IntegrationTestFramework) GetComponents() *MockComponentSuite {
	return itf.components
}

// GenerateTestData creates test data for scenarios and benchmarks.
func (itf *IntegrationTestFramework) GenerateTestData(seriesCount, elementsPerSeries int) []WriteRequest {
	var requests []WriteRequest

	for seriesID := 1; seriesID <= seriesCount; seriesID++ {
		for i := 0; i < elementsPerSeries; i++ {
			requests = append(requests, WriteRequest{
				SeriesID: common.SeriesID(seriesID),
				Key:      time.Now().UnixNano() + int64(i*1000), // Sequential keys
				Data:     []byte(fmt.Sprintf(`{"series":%d,"seq":%d,"timestamp":%d}`, seriesID, i, time.Now().UnixNano())),
				Tags: []tag{
					{
						name:      "series_id",
						value:     int64ToBytesForTags(int64(seriesID)),
						valueType: pbv1.ValueTypeInt64,
						indexed:   true,
					},
					{
						name:      "sequence",
						value:     int64ToBytesForTags(int64(i)),
						valueType: pbv1.ValueTypeInt64,
						indexed:   true,
					},
					{
						name:      "service",
						value:     []byte(fmt.Sprintf("service-%d", seriesID%5)),
						valueType: pbv1.ValueTypeStr,
						indexed:   true,
					},
				},
			})
		}
	}

	return requests
}

// int64ToBytesForTags converts an int64 to bytes for tag values in the framework.
func int64ToBytesForTags(val int64) []byte {
	result := make([]byte, 8)
	for i := 0; i < 8; i++ {
		result[7-i] = byte(val >> (8 * i))
	}
	return result
}

// registerDefaultScenarios registers common test scenarios.
func (itf *IntegrationTestFramework) registerDefaultScenarios() {
	// Basic Write-Read Scenario
	itf.RegisterScenario(TestScenario{
		Name:        "BasicWriteRead",
		Description: "Write elements and read them back",
		Setup: func(_ context.Context, _ *IntegrationTestFramework) error {
			return nil
		},
		Execute: func(ctx context.Context, framework *IntegrationTestFramework) error {
			// Write some test data
			requests := framework.GenerateTestData(3, 10)
			if err := framework.sidx.Write(ctx, requests); err != nil {
				return fmt.Errorf("write failed: %w", err)
			}

			// Query the data back (use the series name that was written)
			queryReq := QueryRequest{
				Name:           "series_1", // Match the key used in MockSIDX write
				MaxElementSize: 100,
			}

			result, err := framework.sidx.Query(ctx, queryReq)
			if err != nil {
				return fmt.Errorf("query failed: %w", err)
			}
			defer result.Release()

			// Count results
			totalResults := 0
			for {
				response := result.Pull()
				if response == nil {
					break
				}
				if response.Error != nil {
					return fmt.Errorf("query execution error: %w", response.Error)
				}
				totalResults += response.Len()
			}

			if totalResults == 0 {
				return fmt.Errorf("no results returned from query")
			}

			return nil
		},
		Validate: func(ctx context.Context, framework *IntegrationTestFramework) error {
			stats, err := framework.sidx.Stats(ctx)
			if err != nil {
				return fmt.Errorf("failed to get stats: %w", err)
			}
			if stats == nil {
				return fmt.Errorf("stats is nil")
			}

			if stats.ElementCount == 0 {
				return fmt.Errorf("no elements found in stats")
			}

			return nil
		},
		Cleanup: func(_ context.Context, _ *IntegrationTestFramework) error {
			return nil
		},
	})

	// Flush and Merge Scenario
	itf.RegisterScenario(TestScenario{
		Name:        "FlushAndMerge",
		Description: "Test flush and merge operations",
		Setup: func(ctx context.Context, framework *IntegrationTestFramework) error {
			// Write some data first
			requests := framework.GenerateTestData(2, 5)
			return framework.sidx.Write(ctx, requests)
		},
		Execute: func(_ context.Context, framework *IntegrationTestFramework) error {
			// Test flush
			if err := framework.sidx.Flush(); err != nil {
				return fmt.Errorf("flush failed: %w", err)
			}

			// Test merge
			if err := framework.sidx.Merge(); err != nil {
				return fmt.Errorf("merge failed: %w", err)
			}

			return nil
		},
		Validate: func(ctx context.Context, framework *IntegrationTestFramework) error {
			stats, err := framework.sidx.Stats(ctx)
			if err != nil {
				return fmt.Errorf("failed to get stats: %w", err)
			}
			if stats == nil {
				return fmt.Errorf("stats is nil")
			}

			// Check that flush and merge timestamps are updated
			if stats.LastFlushTime == 0 {
				return fmt.Errorf("flush time not updated")
			}
			if stats.LastMergeTime == 0 {
				return fmt.Errorf("merge time not updated")
			}

			return nil
		},
		Cleanup: func(_ context.Context, _ *IntegrationTestFramework) error {
			return nil
		},
	})

	// Large Dataset Scenario
	itf.RegisterScenario(TestScenario{
		Name:        "LargeDataset",
		Description: "Handle large datasets efficiently",
		Setup: func(_ context.Context, _ *IntegrationTestFramework) error {
			return nil
		},
		Execute: func(ctx context.Context, framework *IntegrationTestFramework) error {
			// Write a large dataset
			requests := framework.GenerateTestData(10, 100) // 1000 elements
			if err := framework.sidx.Write(ctx, requests); err != nil {
				return fmt.Errorf("large write failed: %w", err)
			}

			// Query with pagination
			queryReq := QueryRequest{
				Name:           "large-dataset-query",
				MaxElementSize: 50,
			}

			result, err := framework.sidx.Query(ctx, queryReq)
			if err != nil {
				return fmt.Errorf("large query failed: %w", err)
			}
			defer result.Release()

			// Process all results
			totalResults := 0
			for {
				response := result.Pull()
				if response == nil {
					break
				}
				if response.Error != nil {
					return fmt.Errorf("query execution error: %w", response.Error)
				}
				totalResults += response.Len()
			}

			return nil
		},
		Validate: func(ctx context.Context, framework *IntegrationTestFramework) error {
			stats, err := framework.sidx.Stats(ctx)
			if err != nil {
				return fmt.Errorf("failed to get stats: %w", err)
			}
			if stats == nil {
				return fmt.Errorf("stats is nil")
			}

			if stats.ElementCount < 1000 {
				return fmt.Errorf("expected at least 1000 elements, got %d", stats.ElementCount)
			}

			return nil
		},
		Cleanup: func(_ context.Context, _ *IntegrationTestFramework) error {
			return nil
		},
	})
}

// registerDefaultBenchmarks registers performance benchmarks.
func (itf *IntegrationTestFramework) registerDefaultBenchmarks() {
	// Write Performance Benchmark
	itf.RegisterBenchmark(Benchmark{
		Name:        "WritePerformance",
		Description: "Measure write throughput and latency",
		Setup: func(_ context.Context, _ *IntegrationTestFramework) error {
			return nil
		},
		Execute: func(ctx context.Context, framework *IntegrationTestFramework) BenchmarkResult {
			const batchSize = 100
			const numBatches = 100

			var latencies []time.Duration
			var errorCount int64

			start := time.Now()

			for i := 0; i < numBatches; i++ {
				requests := framework.GenerateTestData(1, batchSize)

				opStart := time.Now()
				if err := framework.sidx.Write(ctx, requests); err != nil {
					errorCount++
				}
				latencies = append(latencies, time.Since(opStart))
			}

			duration := time.Since(start)
			totalOps := int64(numBatches * batchSize)

			// Calculate percentiles
			latencyP50, latencyP95, latencyP99 := calculatePercentiles(latencies)

			return BenchmarkResult{
				OperationsPerSecond: float64(totalOps) / duration.Seconds(),
				LatencyP50:          latencyP50,
				LatencyP95:          latencyP95,
				LatencyP99:          latencyP99,
				TotalOperations:     totalOps,
				Duration:            duration,
				ErrorCount:          errorCount,
			}
		},
		Cleanup: func(_ context.Context, _ *IntegrationTestFramework) error {
			return nil
		},
	})

	// Query Performance Benchmark
	itf.RegisterBenchmark(Benchmark{
		Name:        "QueryPerformance",
		Description: "Measure query throughput and latency",
		Setup: func(ctx context.Context, framework *IntegrationTestFramework) error {
			// Pre-populate with data
			requests := framework.GenerateTestData(10, 100)
			return framework.sidx.Write(ctx, requests)
		},
		Execute: func(ctx context.Context, framework *IntegrationTestFramework) BenchmarkResult {
			const numQueries = 100

			var latencies []time.Duration
			var errorCount int64
			var totalResults int64

			start := time.Now()

			for i := 0; i < numQueries; i++ {
				queryReq := QueryRequest{
					Name:           fmt.Sprintf("series_%d", (i%10)+1), // Query existing series
					MaxElementSize: 50,
				}

				opStart := time.Now()
				result, err := framework.sidx.Query(ctx, queryReq)
				if err != nil {
					errorCount++
					continue
				}

				// Count results
				for {
					response := result.Pull()
					if response == nil {
						break
					}
					if response.Error != nil {
						errorCount++
						break
					}
					totalResults += int64(response.Len())
				}
				result.Release()

				latencies = append(latencies, time.Since(opStart))
			}

			duration := time.Since(start)

			// Calculate percentiles
			latencyP50, latencyP95, latencyP99 := calculatePercentiles(latencies)

			return BenchmarkResult{
				OperationsPerSecond: float64(totalResults) / duration.Seconds(),
				LatencyP50:          latencyP50,
				LatencyP95:          latencyP95,
				LatencyP99:          latencyP99,
				TotalOperations:     totalResults,
				Duration:            duration,
				ErrorCount:          errorCount,
			}
		},
		Cleanup: func(_ context.Context, _ *IntegrationTestFramework) error {
			return nil
		},
	})
}

// registerDefaultStressTests registers stress/load tests.
func (itf *IntegrationTestFramework) registerDefaultStressTests() {
	// Concurrent Write Stress Test
	itf.RegisterStressTest(StressTest{
		Name:        "ConcurrentWrites",
		Description: "Test concurrent write performance and stability",
		Concurrency: 10,
		Operations:  1000,
		Setup: func(_ context.Context, _ *IntegrationTestFramework) error {
			return nil
		},
		Execute: func(ctx context.Context, framework *IntegrationTestFramework, workerID int) error {
			requests := framework.GenerateTestData(1, 10)
			// Modify series ID to avoid conflicts
			for i := range requests {
				requests[i].SeriesID = common.SeriesID(workerID*1000 + int(requests[i].SeriesID))
			}
			return framework.sidx.Write(ctx, requests)
		},
		Validate: func(ctx context.Context, framework *IntegrationTestFramework) error {
			stats, err := framework.sidx.Stats(ctx)
			if err != nil {
				return fmt.Errorf("failed to get stats: %w", err)
			}
			if stats == nil {
				return fmt.Errorf("stats is nil")
			}

			if stats.ElementCount == 0 {
				return fmt.Errorf("no elements written during stress test")
			}

			return nil
		},
		Cleanup: func(_ context.Context, _ *IntegrationTestFramework) error {
			return nil
		},
	})

	// Mixed Operations Stress Test
	itf.RegisterStressTest(StressTest{
		Name:        "MixedOperations",
		Description: "Test mixed read/write operations under load",
		Concurrency: 8,
		Operations:  500,
		Setup: func(ctx context.Context, framework *IntegrationTestFramework) error {
			// Pre-populate with some data
			requests := framework.GenerateTestData(5, 20)
			return framework.sidx.Write(ctx, requests)
		},
		Execute: func(ctx context.Context, framework *IntegrationTestFramework, workerID int) error {
			// Randomly choose between write and read operations
			// Use workerID to create deterministic but varied behavior
			if workerID%2 == 0 {
				// Write operation
				requests := framework.GenerateTestData(1, 5)
				for i := range requests {
					requests[i].SeriesID = common.SeriesID(workerID*100 + int(requests[i].SeriesID))
				}
				return framework.sidx.Write(ctx, requests)
			}

			// Read operation
			queryReq := QueryRequest{
				Name:           fmt.Sprintf("stress-query-%d", workerID),
				MaxElementSize: 20,
			}

			result, err := framework.sidx.Query(ctx, queryReq)
			if err != nil {
				return err
			}
			defer result.Release()

			// Consume results
			for {
				response := result.Pull()
				if response == nil {
					break
				}
				if response.Error != nil {
					return response.Error
				}
			}
			return nil
		},
		Validate: func(_ context.Context, _ *IntegrationTestFramework) error {
			return nil
		},
		Cleanup: func(_ context.Context, _ *IntegrationTestFramework) error {
			return nil
		},
	})
}

// calculatePercentiles calculates P50, P95, and P99 latency percentiles.
func calculatePercentiles(latencies []time.Duration) (p50, p95, p99 time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}

	// Sort latencies
	sortedLatencies := make([]time.Duration, len(latencies))
	copy(sortedLatencies, latencies)

	// Simple insertion sort for small datasets
	for i := 1; i < len(sortedLatencies); i++ {
		key := sortedLatencies[i]
		j := i - 1
		for j >= 0 && sortedLatencies[j] > key {
			sortedLatencies[j+1] = sortedLatencies[j]
			j--
		}
		sortedLatencies[j+1] = key
	}

	n := len(sortedLatencies)
	p50Index := (n * 50 / 100) - 1
	p95Index := (n * 95 / 100) - 1
	p99Index := (n * 99 / 100) - 1

	// Ensure indices don't go below 0 or above n-1
	if p50Index < 0 {
		p50Index = 0
	}
	if p95Index < 0 {
		p95Index = 0
	}
	if p99Index < 0 {
		p99Index = 0
	}
	if p50Index >= n {
		p50Index = n - 1
	}
	if p95Index >= n {
		p95Index = n - 1
	}
	if p99Index >= n {
		p99Index = n - 1
	}

	// For P99 with small datasets, use the max element
	if n <= 10 && p99Index < n-1 {
		p99Index = n - 1
	}

	p50 = sortedLatencies[p50Index]
	p95 = sortedLatencies[p95Index]
	p99 = sortedLatencies[p99Index]

	return p50, p95, p99
}
