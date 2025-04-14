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
make benchmark
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

## Interpreting Test Results

Test results will be saved in the `reports` directory, including:

- Benchmark performance data: execution time, memory allocation, etc.
- Memory monitoring data: system cache usage changes

### Key Metrics

1. **Execution Time (ns/op)**: Average execution time per operation, lower values indicate better performance
2. **Memory Allocation (B/op)**: Amount of memory allocated per operation, related to system efficiency
3. **Allocation Count (allocs/op)**: Number of memory allocations per operation, reflects memory management efficiency
4. **System Cache Usage**: System cache usage recorded in `memory_stats.txt`, reflects the effectiveness of fadvis

### Expected Results

1. **Short-term Performance**: Disabled fadvis may show slightly better performance in the short term because file data may remain in the cache
2. **Long-term Performance**: Enabled fadvis should demonstrate more stable performance and lower memory usage over extended runtime
3. **System Stability**: Enabled fadvis should show better system stability and less memory pressure when handling large quantities of large files
4. **Concurrent Performance**: The benefit of fadvis should be more pronounced under high concurrent workloads
5. **Memory Adaptation**: The system should perform better with appropriate threshold settings that adapt to available memory

## Memory Threshold Considerations

The current implementation sets the fadvis threshold at startup based on available memory at that time. This approach may have limitations:

1. **Changing Memory Pressure**: If memory availability changes significantly during runtime (due to other processes or workload changes), the initially set threshold may become suboptimal
2. **Resource Competition**: In environments with fluctuating workloads, a static threshold might not adapt well to changing conditions
3. **Long-running Services**: For services running for extended periods, memory conditions might change drastically from startup conditions

These tests help evaluate these considerations and can inform potential improvements such as periodic threshold updates or dynamic threshold adaptation.

## Notes

- These tests are primarily targeted at Linux systems, as `posix_fadvise` is a Linux-specific system call
- The tests create temporary large files, so ensure the test system has sufficient disk space
- For reliable results, it's recommended to run tests on a system with low load 