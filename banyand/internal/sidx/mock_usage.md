# SIDX Mock Implementation Usage Guide

This guide provides comprehensive documentation for using the SIDX mock implementations during development and testing. The mock implementations allow the core storage team to work with SIDX interfaces before the real implementation is complete.

## Table of Contents

1. [Overview](#overview)
2. [Mock Components](#mock-components)
3. [Quick Start Guide](#quick-start-guide)
4. [Integration Examples](#integration-examples)
5. [Configuration Options](#configuration-options)
6. [Testing Scenarios](#testing-scenarios)
7. [Performance Characteristics](#performance-characteristics)
8. [Troubleshooting](#troubleshooting)
9. [Migration Guide](#migration-guide)
10. [Limitations](#limitations)

## Overview

The SIDX mock implementation provides three main components:

- **MockSIDX**: Complete in-memory implementation of the main SIDX interface
- **Mock Components**: Individual mocks for Writer, Querier, Flusher, and Merger interfaces  
- **Integration Test Framework**: Comprehensive testing harness with scenarios, benchmarks, and stress tests

### Key Features

- ✅ **Thread-safe operations** with proper synchronization
- ✅ **Configurable behavior** including delays, error injection, and resource limits
- ✅ **Realistic simulation** of performance characteristics
- ✅ **Memory profiling** and performance measurement
- ✅ **Comprehensive testing** with 100+ test cases
- ✅ **Easy integration** with existing BanyanDB patterns

## Mock Components

### MockSIDX - Main Interface Implementation

The `MockSIDX` provides a complete in-memory implementation of the SIDX interface:

```go
// Create with default configuration
mockSIDX := NewMockSIDX(DefaultMockConfig())

// Create with custom configuration
config := MockConfig{
    WriteDelayMs:           5,    // Simulate 5ms write latency
    QueryDelayMs:           2,    // Simulate 2ms query latency
    ErrorRate:              0,    // No error injection
    MaxElements:            10000, // Element limit
    EnableStrictValidation: true,  // Enable validation
}
mockSIDX := NewMockSIDX(config)
```

### Mock Component Suite

The `MockComponentSuite` provides individual component implementations:

```go
// Create suite with default configurations
suite := NewMockComponentSuite()

// Create suite with custom configurations
suite := NewMockComponentSuiteWithConfigs(
    writerConfig, querierConfig, flusherConfig, mergerConfig)

// Access individual components
writer := suite.Writer
querier := suite.Querier
flusher := suite.Flusher
merger := suite.Merger
```

### Integration Test Framework

The framework provides comprehensive testing capabilities:

```go
// Create framework with mock implementations
framework := NewIntegrationTestFramework(DefaultFrameworkConfig())

// Run all tests (scenarios + benchmarks + stress tests)
results, err := framework.RunAll(ctx)

// Run individual test types
scenarioResults, err := framework.RunScenarios(ctx)
benchmarkResults, err := framework.RunBenchmarks(ctx)
stressResults, err := framework.RunStressTests(ctx)
```

## Quick Start Guide

### Basic Write-Read Workflow

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    "github.com/apache/skywalking-banyandb/banyand/internal/sidx"
    "github.com/apache/skywalking-banyandb/api/common"
    pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func main() {
    ctx := context.Background()
    
    // Create mock SIDX
    mockSIDX := sidx.NewMockSIDX(sidx.DefaultMockConfig())
    defer mockSIDX.Close()
    
    // Write some data
    writeReqs := []sidx.WriteRequest{
        {
            SeriesID: common.SeriesID(1),
            Key:      100,
            Data:     []byte(`{"service": "user-service", "endpoint": "/api/users"}`),
            Tags: []sidx.Tag{
                {
                    Name:      "service",
                    Value:     []byte("user-service"),
                    ValueType: pbv1.ValueTypeStr,
                    Indexed:   true,
                },
                {
                    Name:      "status_code",
                    Value:     int64ToBytes(200),
                    ValueType: pbv1.ValueTypeInt64,
                    Indexed:   true,
                },
            },
        },
    }
    
    err := mockSIDX.Write(ctx, writeReqs)
    if err != nil {
        log.Fatalf("Write failed: %v", err)
    }
    
    // Query the data back
    queryReq := sidx.QueryRequest{
        Name:           "series_1", // Matches the series name used in mock
        MaxElementSize: 100,
    }
    
    result, err := mockSIDX.Query(ctx, queryReq)
    if err != nil {
        log.Fatalf("Query failed: %v", err)
    }
    defer result.Release()
    
    // Process results
    for {
        response := result.Pull()
        if response == nil {
            break
        }
        if response.Error != nil {
            log.Fatalf("Query error: %v", response.Error)
        }
        
        fmt.Printf("Found %d elements\n", response.Len())
        for i := 0; i < response.Len(); i++ {
            fmt.Printf("Key: %d, Data: %s\n", response.Keys[i], response.Data[i])
        }
    }
}

func int64ToBytes(val int64) []byte {
    result := make([]byte, 8)
    for i := 0; i < 8; i++ {
        result[7-i] = byte(val >> (8 * i))
    }
    return result
}
```

### Component Integration Example

```go
func ExampleComponentIntegration() {
    ctx := context.Background()
    
    // Create component suite
    suite := sidx.NewMockComponentSuite()
    
    // Write data
    writeReqs := []sidx.WriteRequest{
        {SeriesID: 1, Key: 100, Data: []byte("data1")},
        {SeriesID: 1, Key: 101, Data: []byte("data2")},
    }
    
    err := suite.Writer.Write(ctx, writeReqs)
    if err != nil {
        panic(err)
    }
    
    // Sync data to querier
    suite.SyncElements()
    
    // Query data
    queryReq := sidx.QueryRequest{
        Name:           "test-query",
        MaxElementSize: 10,
    }
    
    result, err := suite.Querier.Query(ctx, queryReq)
    if err != nil {
        panic(err)
    }
    defer result.Release()
    
    // Process results
    response := result.Pull()
    if response != nil {
        fmt.Printf("Query returned %d elements\n", response.Len())
    }
    
    // Perform maintenance operations
    err = suite.Flusher.Flush()
    if err != nil {
        panic(err)
    }
    
    err = suite.Merger.Merge()
    if err != nil {
        panic(err)
    }
}
```

## Integration Examples

### Integration with BanyanDB Stream Module

```go
// Example: Using MockSIDX in stream module tests
func TestStreamWithMockSIDX(t *testing.T) {
    // Create mock SIDX for testing
    mockSIDX := sidx.NewMockSIDX(sidx.MockConfig{
        WriteDelayMs: 1,  // Fast writes for testing
        QueryDelayMs: 1,  // Fast queries for testing
        ErrorRate:    0,  // No errors during testing
        MaxElements:  1000,
    })
    defer mockSIDX.Close()
    
    // Use mockSIDX in your stream implementation
    streamImpl := &StreamImplementation{
        sidx: mockSIDX,
    }
    
    // Test stream operations
    // ... your test code here
}
```

### Integration with Measure Module

```go
// Example: Using mock components in measure module
func TestMeasureWithMockComponents(t *testing.T) {
    suite := sidx.NewMockComponentSuite()
    
    // Configure for measure-specific testing
    suite.Writer.SetConfig(sidx.MockWriterConfig{
        DelayMs:          0,     // No delay for fast tests
        MaxElements:      10000, // Large capacity for measure data
        SortElements:     true,  // Ensure sorting for measure queries
        EnableValidation: true,  // Validate measure data
    })
    
    measureImpl := &MeasureImplementation{
        writer:  suite.Writer,
        querier: suite.Querier,
        flusher: suite.Flusher,
        merger:  suite.Merger,
    }
    
    // Test measure operations
    // ... your test code here
}
```

### Custom Test Scenarios

```go
// Example: Creating custom test scenarios
func ExampleCustomScenarios() {
    framework := sidx.NewIntegrationTestFramework(sidx.DefaultFrameworkConfig())
    
    // Register custom scenario
    customScenario := sidx.TestScenario{
        Name:        "HighVolumeWrites",
        Description: "Test high-volume write scenario",
        Setup: func(ctx context.Context, framework *sidx.IntegrationTestFramework) error {
            // Setup high-volume test data
            return nil
        },
        Execute: func(ctx context.Context, framework *sidx.IntegrationTestFramework) error {
            // Execute high-volume writes
            for i := 0; i < 10000; i++ {
                reqs := framework.GenerateTestData(1, 100)
                err := framework.GetSIDX().Write(ctx, reqs)
                if err != nil {
                    return err
                }
            }
            return nil
        },
        Validate: func(ctx context.Context, framework *sidx.IntegrationTestFramework) error {
            // Validate results
            stats, err := framework.GetSIDX().Stats(ctx)
            if err != nil {
                return err
            }
            if stats.ElementCount < 100000 {
                return fmt.Errorf("expected at least 100000 elements, got %d", stats.ElementCount)
            }
            return nil
        },
    }
    
    framework.RegisterScenario(customScenario)
    
    // Run custom scenario
    results, err := framework.RunScenarios(context.Background())
    if err != nil {
        panic(err)
    }
    
    for _, result := range results {
        fmt.Printf("Scenario %s: Success=%v, Duration=%v\n", 
            result.Name, result.Success, result.Duration)
    }
}
```

## Configuration Options

### MockSIDX Configuration

```go
type MockConfig struct {
    WriteDelayMs           int  // Artificial write delay (0-1000ms)
    QueryDelayMs           int  // Artificial query delay (0-1000ms)  
    FlushDelayMs           int  // Artificial flush delay (0-1000ms)
    MergeDelayMs           int  // Artificial merge delay (0-1000ms)
    ErrorRate              int  // Error injection rate (0-100%)
    MaxElements            int  // Maximum elements to prevent OOM
    EnableStrictValidation bool // Enable validation checks
}

// Default configuration for most use cases
config := DefaultMockConfig() // Returns sensible defaults

// Performance testing configuration
config := MockConfig{
    WriteDelayMs: 0,  // No artificial delays
    QueryDelayMs: 0,  
    ErrorRate:    0,  // No error injection
    MaxElements:  100000,
}

// Error testing configuration  
config := MockConfig{
    ErrorRate: 10,  // 10% error rate
}

// Slow operation simulation
config := MockConfig{
    WriteDelayMs: 50,  // 50ms write latency
    QueryDelayMs: 20,  // 20ms query latency
}
```

### Component Configuration

```go
// Writer configuration
writerConfig := MockWriterConfig{
    DelayMs:          5,     // 5ms write delay
    ErrorRate:        0,     // No errors
    MaxElements:      1000,  // Element limit
    EnableValidation: true,  // Validate requests
    SortElements:     true,  // Sort elements by SeriesID/Key
}

// Querier configuration
querierConfig := MockQuerierConfig{
    DelayMs:          2,     // 2ms query delay
    ErrorRate:        0,     // No errors
    EnableValidation: true,  // Validate requests
    MaxResultSize:    100,   // Max results per batch
}

// Flusher configuration
flusherConfig := MockFlusherConfig{
    DelayMs:      10,   // 10ms flush delay
    ErrorRate:    0,    // No errors
    SimulateWork: true, // Simulate actual work
}

// Merger configuration
mergerConfig := MockMergerConfig{
    DelayMs:            20,  // 20ms merge delay
    ErrorRate:          0,   // No errors
    SimulateWork:       true, // Simulate actual work
    ConsolidationRatio: 0.7, // 70% size reduction after merge
}
```

### Framework Configuration

```go
frameworkConfig := FrameworkConfig{
    EnableVerboseLogging:  true,                 // Detailed logging
    MaxConcurrency:        runtime.NumCPU() * 2, // Concurrent operations
    DefaultTimeout:        30 * time.Second,     // Operation timeout
    BenchmarkDuration:     10 * time.Second,     // Benchmark duration
    StressDuration:        30 * time.Second,     // Stress test duration
    EnableMemoryProfiling: true,                 // Memory usage tracking
}
```

## Testing Scenarios

### Built-in Scenarios

The framework includes several built-in scenarios:

1. **BasicWriteRead**: Write elements and read them back
2. **FlushAndMerge**: Test flush and merge operations  
3. **LargeDataset**: Handle large datasets efficiently

### Built-in Benchmarks

1. **WritePerformance**: Measure write throughput and latency
2. **QueryPerformance**: Measure query throughput and latency

### Built-in Stress Tests

1. **ConcurrentWrites**: Test concurrent write performance and stability
2. **MixedOperations**: Test mixed read/write operations under load

### Running Tests

```go
// Run all tests
framework := sidx.NewIntegrationTestFramework(sidx.DefaultFrameworkConfig())
results, err := framework.RunAll(ctx)

// Access results
scenarioResults := results["scenarios"].([]sidx.ScenarioResult)
benchmarkResults := results["benchmarks"].(map[string]sidx.BenchmarkResult)
stressResults := results["stress_tests"].(map[string]sidx.StressTestResult)

// Print results
for _, scenario := range scenarioResults {
    fmt.Printf("Scenario %s: %v (%v)\n", 
        scenario.Name, scenario.Success, scenario.Duration)
}

for name, benchmark := range benchmarkResults {
    fmt.Printf("Benchmark %s: %.2f ops/sec (P95: %v)\n",
        name, benchmark.OperationsPerSecond, benchmark.LatencyP95)
}
```

## Performance Characteristics

### Expected Performance (Mock Implementation)

| Operation | Throughput | Latency | Notes |
|-----------|------------|---------|-------|
| Write | >10,000 elem/sec | <1ms P95 | In-memory storage |
| Query | >50,000 results/sec | <1ms P95 | Linear search |
| Flush | ~100ms | N/A | Simulated disk I/O |
| Merge | ~200ms | N/A | Simulated consolidation |

### Memory Usage

- **Element storage**: ~200 bytes per element (including tags)
- **Query overhead**: ~50 bytes per query result
- **Framework overhead**: ~1MB base memory usage

### Concurrency

- **Thread-safe**: All operations are thread-safe
- **Recommended concurrency**: Up to 100 concurrent operations
- **Lock contention**: Minimal with read-write locks

## Troubleshooting

### Common Issues

#### 1. Query Returns No Results

**Problem**: Query returns empty results even after writing data.

**Solution**: 
```go
// Ensure you're using the correct series name
// MockSIDX stores data with keys like "series_1", "series_2", etc.
queryReq := QueryRequest{
    Name: "series_1", // Must match the naming pattern
    MaxElementSize: 100,
}

// For component suite, sync elements after writing
suite.Writer.Write(ctx, writeReqs)
suite.SyncElements() // Important: sync data to querier
result, err := suite.Querier.Query(ctx, queryReq)
```

#### 2. Element Limit Exceeded Errors

**Problem**: Write operations fail with "element limit exceeded" error.

**Solution**:
```go
// Increase the element limit
config := DefaultMockConfig()
config.MaxElements = 100000 // Increase limit
mockSIDX := NewMockSIDX(config)

// Or clear elements periodically
mockSIDX.Clear() // Clears all stored elements
```

#### 3. High Error Rates in Tests

**Problem**: Operations fail frequently during testing.

**Solution**:
```go
// Check error injection configuration
config := DefaultMockConfig()
config.ErrorRate = 0 // Disable error injection for stable tests
mockSIDX := NewMockSIDX(config)

// Or use SetErrorRate for dynamic control
mockSIDX.SetErrorRate(0) // Disable errors
// Run tests
mockSIDX.SetErrorRate(10) // Enable 10% error rate for error testing
```

#### 4. Slow Test Performance

**Problem**: Tests run slowly due to artificial delays.

**Solution**:
```go
// Disable artificial delays for fast tests
config := MockConfig{
    WriteDelayMs: 0, // No write delay
    QueryDelayMs: 0, // No query delay
    FlushDelayMs: 0, // No flush delay
    MergeDelayMs: 0, // No merge delay
}
mockSIDX := NewMockSIDX(config)

// Or use dynamic delay configuration
mockSIDX.SetWriteDelay(0)
mockSIDX.SetQueryDelay(0)
```

#### 5. Memory Usage Issues

**Problem**: High memory usage during testing.

**Solution**:
```go
// Limit element count
config := DefaultMockConfig()
config.MaxElements = 1000 // Smaller limit

// Clear elements periodically
if elementCount > 500 {
    mockSIDX.Clear()
}

// Monitor memory usage
stats, _ := mockSIDX.Stats(ctx)
fmt.Printf("Memory usage: %d bytes\n", stats.MemoryUsageBytes)
```

### Debug Tips

#### Enable Verbose Logging

```go
frameworkConfig := DefaultFrameworkConfig()
frameworkConfig.EnableVerboseLogging = true
framework := NewIntegrationTestFramework(frameworkConfig)
```

#### Check Stats

```go
stats, err := mockSIDX.Stats(ctx)
if err != nil {
    log.Printf("Stats error: %v", err)
} else {
    log.Printf("Elements: %d, Memory: %d bytes, Queries: %d", 
        stats.ElementCount, stats.MemoryUsageBytes, stats.QueryCount)
}
```

#### Verify Element Storage

```go
// Check what keys are stored (MockSIDX only)
keys := mockSIDX.GetStorageKeys()
log.Printf("Stored keys: %v", keys)

// Check element count
count := mockSIDX.GetElementCount()
log.Printf("Element count: %d", count)
```

## Migration Guide

### From Mock to Real Implementation

When migrating from mock implementations to the real SIDX implementation:

#### 1. Interface Compatibility

The mock implementations follow the exact same interfaces as the real implementation:

```go
// This code works with both mock and real implementations
var sidx SIDX
if useMock {
    sidx = NewMockSIDX(DefaultMockConfig())
} else {
    sidx = NewRealSIDX(realConfig) // Real implementation
}

// Same interface calls work for both
err := sidx.Write(ctx, writeReqs)
result, err := sidx.Query(ctx, queryReq)
```

#### 2. Configuration Migration

Mock configuration translates to real configuration:

```go
// Mock configuration
mockConfig := MockConfig{
    WriteDelayMs: 5,
    MaxElements: 10000,
    EnableStrictValidation: true,
}

// Equivalent real configuration
realConfig := RealConfig{
    FlushThreshold: 10000, // Similar to MaxElements
    EnableValidation: true, // Similar to EnableStrictValidation
    // Real implementation has different performance characteristics
}
```

#### 3. Performance Expectations

Adjust performance expectations when migrating:

| Aspect | Mock | Real Implementation |
|--------|------|-------------------|
| Write latency | <1ms | 1-10ms (with disk I/O) |
| Query latency | <1ms | 5-50ms (with index lookup) |
| Memory usage | High (all in memory) | Lower (disk-backed) |
| Durability | None | Full durability |

#### 4. Error Handling

Real implementation may have different error patterns:

```go
// Mock errors are artificial
if err != nil && isMockError(err) {
    // Handle mock-specific errors
}

// Real implementation errors
if err != nil {
    // Handle disk I/O errors, corruption, etc.
}
```

#### 5. Testing Strategy

Maintain both mock and real implementation tests:

```go
func TestWithMock(t *testing.T) {
    sidx := NewMockSIDX(DefaultMockConfig())
    testSIDXBehavior(t, sidx)
}

func TestWithReal(t *testing.T) {
    sidx := NewRealSIDX(testConfig)
    testSIDXBehavior(t, sidx)
}

func testSIDXBehavior(t *testing.T, sidx SIDX) {
    // Common test logic that works with both implementations
}
```

## Limitations

### Mock Implementation Limitations

#### 1. Storage Limitations

- **Memory-only**: No persistence across restarts
- **No disk I/O**: Cannot test disk-related issues
- **Linear search**: Query performance doesn't reflect real indexes
- **No compression**: Data is stored uncompressed

#### 2. Concurrency Limitations

- **Simple locking**: Uses basic read-write locks (real implementation may have more sophisticated locking)
- **No partition locking**: Real implementation may have finer-grained locking
- **No write-ahead log**: No crash recovery simulation

#### 3. Functional Limitations

- **Simplified filtering**: Tag filtering is basic compared to real implementation
- **No bloom filters**: Real implementation uses bloom filters for efficiency
- **No background operations**: No background compaction or cleanup
- **No resource limits**: Memory limits are artificial

#### 4. Performance Limitations

- **Not representative**: Performance characteristics don't match real implementation
- **No I/O simulation**: Real I/O patterns are not simulated
- **No network overhead**: Real distributed operations have network costs
- **No garbage collection**: Memory management is simplified

### Component Mock Limitations

#### 1. Writer Limitations

- **No batching optimization**: Real implementation may optimize batching
- **No write-ahead log**: No durability guarantees
- **Simplified sorting**: Sorting is basic in-memory sort

#### 2. Querier Limitations

- **Linear search**: No index structures
- **No query optimization**: No query planning or optimization
- **Basic filtering**: Tag filters are simplified

#### 3. Flusher Limitations

- **No actual persistence**: Flush operations are simulated
- **No file format**: No real file writing or format handling
- **No compression**: No data compression during flush

#### 4. Merger Limitations

- **Simulated consolidation**: Merge operations are simulated
- **No real deduplication**: Element deduplication is simplified
- **No background processing**: Merge operations are synchronous

### Testing Limitations

#### 1. Integration Testing

- **No real I/O**: Cannot test disk failures, corruption, etc.
- **No network issues**: Cannot test distributed system issues
- **No resource contention**: Cannot test real resource constraints

#### 2. Performance Testing

- **Not representative**: Benchmark results don't predict real performance
- **No I/O bottlenecks**: Cannot identify I/O-related performance issues
- **Memory-bound**: Performance is limited by memory, not disk/network

#### 3. Reliability Testing

- **No crash recovery**: Cannot test crash recovery scenarios
- **No data corruption**: Cannot test corruption detection and recovery
- **No partial failures**: Cannot test partial system failures

### Recommended Usage

Given these limitations, the mock implementations are best used for:

✅ **Development**: Early development when real implementation isn't ready
✅ **Unit testing**: Testing component interactions and logic
✅ **Integration testing**: Testing interface contracts and workflows
✅ **Functional testing**: Testing business logic and data flow
✅ **CI/CD pipelines**: Fast, reliable tests for continuous integration

❌ **Performance tuning**: Real implementation needed for performance optimization
❌ **Load testing**: Real implementation needed for realistic load testing  
❌ **Reliability testing**: Real implementation needed for failure scenarios
❌ **Production simulation**: Real implementation needed for production-like testing

---

*This documentation covers the complete usage of SIDX mock implementations. For questions or issues, refer to the test files and source code for additional examples and implementation details.*