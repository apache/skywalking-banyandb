# Fadvis Benchmark Tests

This directory contains a set of benchmark tests for evaluating the effectiveness of the `fadvis` feature. These tests compare performance differences between enabled and disabled `fadvis`, particularly focusing on memory usage and system performance when handling large files.

## Test Coverage

These benchmarks cover the following scenarios:

1. **Write Performance Tests (`BenchmarkWritePerformance`)**
   - Compare file writing performance with fadvis enabled vs. disabled
   - Test with files of different sizes

2. **Read Performance Tests (`BenchmarkReadPerformance`)**
   - Compare file reading performance with fadvis enabled vs. disabled
   - Test with files of different sizes

3. **Multiple Reads Tests (`BenchmarkMultipleReads`)**
   - Test performance differences when repeatedly reading the same file
   - Special focus on caching behavior

4. **Merge Operation Tests (`BenchmarkMergeOperations`)**
   - Simulate merge operations in BanyanDB
   - Test performance when merging different numbers of parts
   - Compare differences with fadvis enabled vs. disabled

5. **Sequential Merge Tests (`BenchmarkSequentialMergeOperations`)**
   - Test multiple consecutive merge operations
   - Observe memory usage changes during extended runtime

6. **Mixed Workload Tests (`BenchmarkMixedWorkload`)**
   - Simulate mixed read/write operations in real scenarios
   - Evaluate overall system performance and stability

7. **Concurrent Operations Tests (`BenchmarkConcurrentOperations`)**
   - Test fadvis performance under concurrent read/write operations
   - Simulate multiple client access patterns with parallel goroutines
   - Evaluate system stability under high concurrency

8. **Concurrent Merge Tests (`BenchmarkConcurrentMerges`)**
   - Evaluate performance when multiple merge operations happen concurrently
   - Test system behavior when multiple CPU cores are utilized

9. **Memory Threshold Adaptation Tests (`BenchmarkThresholdAdaptation`)**
   - Test fadvis performance with different memory threshold settings
   - Compare normal threshold vs. low memory threshold (simulating memory pressure)
   - Evaluate effectiveness of threshold-based adaptation

## How to Run the Tests

These tests are managed through the Makefile, which provides multiple running options:

### Run All Benchmark Tests

```bash
make all-benchmarks
```

### Run Specific Types of Benchmark Tests

```bash
# Write performance tests
make benchmark-write

# Read performance tests
make benchmark-read

# Merge operation tests
make benchmark-merge

# Sequential merge tests
make benchmark-sequential-merge

# Multiple reads tests
make benchmark-multiple-reads

# Mixed workload tests
make benchmark-mixed

# Concurrent operations tests
make benchmark-concurrent

# Threshold adaptation tests
make benchmark-threshold
```

### Tests with Memory Monitoring

```bash
make benchmark-memory
```

This command will monitor system memory and cache usage while running benchmark tests.

### Clean Test Results

```bash
make clean
```

### Show Help

```bash
make help
```

## Interpreting Test Results

The benchmark results will be saved to the `reports` directory. Each test will generate a separate report file with detailed performance metrics.

Key metrics to look for:

- **Operations per Second**: Higher is better, indicates throughput
- **Nanoseconds per Operation**: Lower is better, indicates latency
- **Bytes per Operation**: Memory usage per operation
- **Allocations per Operation**: Number of memory allocations

## Implementation Details

The benchmark tests use the following components:

- **fs package**: For file system operations with automatic fadvis application
- **fadvis package**: For threshold management and fadvis application
- **cgroups package**: For getting system memory information

The tests dynamically calculate fadvis thresholds based on actual system memory, using the same logic as in production code (1% of page cache size, which is typically 25% of total memory).

## Notes

- These tests are designed to run on Linux systems, as the fadvis feature is only supported on Linux.
- Some tests may require significant disk space for creating large test files.
- Memory monitoring is especially useful for observing the effects of fadvis on system cache usage.