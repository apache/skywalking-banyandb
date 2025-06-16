# Fadvis Benchmark Tests

This directory contains a set of benchmark tests for evaluating the effectiveness of the `fadvis` feature. These tests compare performance differences between enabled and disabled `fadvis`, particularly focusing on memory usage and system performance when handling large files.

## Test Coverage

These benchmarks cover the following scenarios:

1. **Write Performance Tests (`BenchmarkWritePerformance`)**
   - Compare file writing performance with fadvis enabled vs. disabled
   - Test with files of different sizes

2. **Sequential Read Performance Tests (`BenchmarkSequentialRead`)**
   - Compare file reading performance with fadvis enabled vs. disabled
   - Test with files of different sizes using streaming to minimize heap allocations

3. **Multiple Reads Tests (`BenchmarkMultipleReads`)**
   - Test performance differences when repeatedly reading the same file
   - Special focus on caching behavior

4. **Mixed Workload Tests (`BenchmarkMixedWorkload`)**
   - Simulate mixed read/write operations in real scenarios
   - Evaluate overall system performance and stability

5. **BPF Tracing Tests**
   - Use eBPF to trace and analyze fadvise system calls
   - Provide detailed insights into kernel-level behavior

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

# Sequential read performance tests
make benchmark-seqread

# Multiple reads tests
make benchmark-multiple-reads

# Mixed workload tests
make benchmark-mixed
```

### Run BPF Tracing Tests

```bash
# Run with eBPF tracing (requires Linux with BPF support)
make bpf-benchmark

# Run in Docker with BPF capabilities
make docker-bpf-benchmark
```

### Clean Test Results

```bash
make clean
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
- **disk package**: For getting available disk space information

The tests dynamically calculate fadvis thresholds based on two factors:
1. System memory: Using 1% of page cache size (typically 25% of total memory)
2. Available disk space: Ensuring the threshold doesn't exceed available disk space

The system will use the smaller of these two values as the effective threshold, ensuring that fadvis behavior is optimized for both memory and disk space constraints.

## Notes

- These tests are designed to run on Linux systems, as the fadvis feature is only supported on Linux.
- Some tests may require significant disk space for creating large test files.
- Memory monitoring is especially useful for observing the effects of fadvis on system cache usage.
- The disk space awareness feature ensures that the system doesn't attempt to use more disk space than is available when applying fadvis optimizations.