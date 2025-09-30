# Container Performance Monitoring Example

This document shows an example of the performance data collected during the Stream vs Trace Docker test.

## Generated Files

After running the test, the following files are generated:

1. **`container-stats.json`** - Raw performance data in JSON format
2. **`performance-summary.txt`** - Human-readable performance summary

## Example Performance Summary

```
========================================
Container Performance Summary
========================================
Generated: 2024-01-15 10:30:45
Test Duration: 2024-01-15T10:25:30Z to 2024-01-15T10:30:45Z

----------------------------------------
banyandb-stream Performance Metrics
----------------------------------------
Sample Count: 60

CPU Usage:
  Average: 45.2%
  Maximum: 78.5%

Memory Usage:
  Average: 1250.5MB
  Maximum: 1450.2MB
  Limit: 4096MB
  Usage %: 30.5%

Network I/O:
  Received: 125.4MB
  Transmitted: 89.7MB

Block I/O:
  Read: 45.2MB
  Write: 12.8MB

----------------------------------------
banyandb-trace Performance Metrics
----------------------------------------
Sample Count: 60

CPU Usage:
  Average: 52.8%
  Maximum: 85.2%

Memory Usage:
  Average: 1380.3MB
  Maximum: 1620.1MB
  Limit: 4096MB
  Usage %: 33.7%

Network I/O:
  Received: 98.7MB
  Transmitted: 76.3MB

Block I/O:
  Read: 38.9MB
  Write: 15.2MB

----------------------------------------
Performance Comparison
----------------------------------------
CPU Usage Ratio (Stream/Trace): 0.86
Memory Usage Ratio (Stream/Trace): 0.91
```

## Data Points Collected

The monitoring system collects the following performance metrics every 5 seconds:

### CPU Metrics
- **Average CPU Usage**: Mean CPU utilization percentage
- **Maximum CPU Usage**: Peak CPU utilization during test
- **CPU Usage Ratio**: Comparison between Stream and Trace containers

### Memory Metrics
- **Average Memory Usage**: Mean memory consumption in MB
- **Maximum Memory Usage**: Peak memory consumption during test
- **Memory Limit**: Container memory limit
- **Memory Usage Percentage**: Percentage of limit used
- **Memory Usage Ratio**: Comparison between Stream and Trace containers

### Network I/O Metrics
- **Network Received**: Total data received from network
- **Network Transmitted**: Total data transmitted to network

### Block I/O Metrics
- **Block Read**: Total data read from disk
- **Block Write**: Total data written to disk

### Sample Information
- **Sample Count**: Number of data points collected
- **Test Duration**: Start and end timestamps of monitoring

## Usage

To run the test with performance monitoring:

```bash
# Run complete test with monitoring
./run-docker-test.sh all

# Or run just the test
./run-docker-test.sh test

# View performance summary from last test
./run-docker-test.sh summary

# View current container stats
./run-docker-test.sh stats
```

## Analysis

The performance summary provides insights into:

1. **Resource Efficiency**: Which container (Stream vs Trace) uses resources more efficiently
2. **Performance Characteristics**: CPU and memory usage patterns during different test phases
3. **I/O Patterns**: Network and disk I/O behavior differences
4. **Scalability**: How resource usage scales with workload

This data helps in understanding the performance characteristics of the Stream and Trace models under load.
