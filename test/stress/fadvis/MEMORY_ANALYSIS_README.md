# Memory Analysis for Fadvise Benchmarks

## Overview

This document provides instructions for analyzing memory usage and pagecache behavior with and without fadvise in the BanyanDB system. The focus is on measuring how effectively fadvise reduces memory usage, particularly after write and merge operations.

## Approach

We use two complementary approaches to measure memory usage:

1. **Go's Built-in Memory Profiling**: Captures heap allocations and Go runtime memory statistics
2. **OS-level Pagecache Analysis**: Examines actual pagecache usage, which is the primary target of fadvise

## Running the Benchmarks

### Memory Profiling Benchmark

To run the memory profiling benchmark:

```bash
cd /path/to/skywalking-banyandb

# Run with memory profiling enabled
go test -bench=BenchmarkMergeMemoryUsage -benchmem -memprofile=mem.prof ./test/stress/fadvis/
```

This will:
- Run merge operations with and without fadvise
- Generate memory profiles before and after operations
- Output memory statistics for comparison
- Pause briefly to allow for OS-level measurements

### Analyzing the Results

#### Go Memory Profiles

To analyze the memory profiles:

```bash
# View memory profile
go tool pprof -http=:8080 mem.prof
```

This will open a web interface where you can explore memory usage patterns.

#### OS-level Pagecache Analysis

During the benchmark run, the process will pause and output its PID. Use this opportunity to analyze the pagecache usage:

```bash
# Run the analysis script on the paused process
./test/stress/fadvis/analyze_memory.sh <PID>
```

The script will collect and analyze:
- Process memory maps
- RSS (Resident Set Size)
- PSS (Proportional Set Size)
- Pagecache usage
- Other memory metrics

## Expected Results

If fadvise is working effectively:

1. The "WithFadvise" benchmark should show significantly lower pagecache usage (RSS or PSS) after operations complete
2. Total system memory usage should be lower with fadvise enabled
3. The Go memory profiles may not show significant differences, as fadvise primarily affects OS-level pagecache

## Interpreting Results

The key metrics to compare between "WithFadvise" and "WithoutFadvise" runs:

1. **RSS/PSS values**: Lower values with fadvise indicate successful memory release
2. **Cached memory**: Should be lower with fadvise
3. **System memory pressure**: Overall system memory usage should be lower

These measurements will demonstrate whether fadvise is effectively reducing memory usage after write and merge operations complete.
