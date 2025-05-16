# Memory Analysis for Fadvise Benchmarks

## Overview

This document provides instructions for analyzing memory usage and pagecache behavior with and without fadvise in the BanyanDB system. The focus is on measuring how effectively fadvise reduces memory usage, particularly after write and read operations, while also respecting available disk space constraints.

## Approach

We use three complementary approaches to measure system resource usage:

1. **Go's Built-in Memory Profiling**: Captures heap allocations and Go runtime memory statistics
2. **OS-level Pagecache Analysis**: Examines actual pagecache usage, which is the primary target of fadvise
3. **Disk Space Monitoring**: Tracks available disk space and its influence on fadvise thresholds

## Memory Profile Capture Points

The benchmarks now automatically capture pagecache statistics at key points during execution:

1. **Write Operations**:
   - `after_write_fadvis_enabled` - After write operations with fadvise enabled
   - `after_write_fadvis_disabled` - After write operations with fadvise disabled

2. **Read Operations**:
   - `after_read_fadvis_enabled` - After read operations with fadvise enabled
   - `after_read_fadvis_disabled` - After read operations with fadvise disabled
   - `after_multiple_reads_fadvis_enabled` - After multiple reads with fadvise enabled
   - `after_multiple_reads_fadvis_disabled` - After multiple reads with fadvise disabled
   - `after_seqread_fadvis_enabled` - After sequential reads with fadvise enabled
   - `after_seqread_fadvis_disabled` - After sequential reads with fadvise disabled

3. **Mixed Workloads**:
   - `after_mixed_workload_fadvis_enabled` - After mixed workload with fadvise enabled
   - `after_mixed_workload_fadvis_disabled` - After mixed workload with fadvise disabled

All pagecache statistics are saved to `/tmp/pagecache_<phase>.prof` for later analysis, and are also printed to stdout in the format `[PAGECACHE] <phase>: Rss=<value>KB, Pss=<value>KB, SharedClean=<value>KB`.

## Running the Benchmarks

### Memory Profiling Benchmark

To run the memory profiling benchmark:

```bash
# Run all benchmarks with memory profiling
go test -bench=. -memprofile=heap.prof -benchmem ./benchmark

# Run a specific benchmark with memory profiling
go test -bench=BenchmarkSequentialRead -memprofile=heap.prof -benchmem ./benchmark
```

### Analyzing Memory Profiles

To analyze the memory profiles:

```bash
# View heap profile in web browser
go tool pprof -http=0.0.0.0:8080 heap.prof

# Compare heap profiles
go tool pprof -http=0.0.0.0:8080 -base heap_base.prof heap_after.prof
```

### Analyzing Pagecache Statistics

To view the pagecache statistics files:

```bash
# List all pagecache profile files
ls -l /tmp/pagecache_*.prof

# View a specific pagecache profile
cat /tmp/pagecache_after_seqread_1gb_fadvis_enabled.prof
```

## Expected Results

When comparing scenarios with and without fadvise, you should observe:

1. Similar Go heap memory usage (measured by `-benchmem` and heap.prof)
2. Significantly lower pagecache usage with fadvise enabled (measured by pagecache_*.prof files)
3. The SharedClean value in pagecache statistics should be lower with fadvise enabled, indicating successful release of file cache
4. The CachedMemory value from /proc/meminfo should be lower with fadvise enabled
5. In limited disk space scenarios, the fadvise threshold should automatically adjust to be no larger than the available disk space

## Disk Space Awareness

The updated fadvise implementation now considers available disk space when determining thresholds:

1. The system calculates a threshold based on memory (1% of page cache)
2. It also checks the available disk space on the target filesystem
3. The effective threshold is the smaller of these two values
4. This ensures that fadvise doesn't attempt to use more disk space than is available

## Interpreting Results

The primary goal of fadvise is to reduce OS-level pagecache usage while respecting disk space constraints. Therefore:

- Focus on the `SharedClean` and `CachedMemory` values in pagecache statistics
- The B/op and allocs/op metrics from `-benchmem` provide insight into Go-level memory efficiency
- The heap.prof file can help identify any unexpected memory usage patterns in the Go code
- In disk-limited scenarios, verify that the system correctly uses the available disk space as the threshold

## Troubleshooting

If you don't see significant differences in pagecache usage:

1. Ensure the test files are large enough (at least 1GB) to trigger fadvise behavior
2. Check that the fadvise threshold is set appropriately for your system
3. Verify that the Linux kernel version supports the fadvise system call
4. Ensure there is enough memory pressure to make pagecache management relevant
5. Check that disk space monitoring is working correctly by examining the logs for disk space information
