# Disk Management

BanyanDB provides comprehensive disk management capabilities to prevent disk space exhaustion and ensure optimal storage utilization. This includes both automatic forced retention cleanup and disk usage monitoring.

## Overview

BanyanDB includes a disk monitor that automatically triggers forced retention cleanup when disk usage exceeds configured thresholds. This feature helps prevent disk space exhaustion by automatically removing old data segments and snapshots.

**Note**: The disk monitor with forced retention cleanup is only available for **standalone** and **data** servers. **Liaison** servers use the `*-max-disk-usage-percent` flags to throttle writes when the write queue fills up, allowing the sync process to catch up with incoming data.

## Configuration

### Data & Standalone Servers

The following flags are used to configure forced retention cleanup for data and standalone servers:

#### Measure Service

- `--measure-retention-high-watermark float`: Disk usage percentage that triggers forced retention cleanup (0-100, default: 95.0).
- `--measure-retention-low-watermark float`: Disk usage percentage where forced retention cleanup stops (0-100, default: 85.0).
- `--measure-retention-check-interval duration`: Interval for checking disk usage (default: 5m).
- `--measure-retention-cooldown duration`: Cooldown period between forced segment deletions (default: 30s).

#### Stream Service

- `--stream-retention-high-watermark float`: Disk usage percentage that triggers forced retention cleanup (0-100, default: 95.0).
- `--stream-retention-low-watermark float`: Disk usage percentage where forced retention cleanup stops (0-100, default: 85.0).
- `--stream-retention-check-interval duration`: Interval for checking disk usage (default: 5m).
- `--stream-retention-cooldown duration`: Cooldown period between forced segment deletions (default: 30s).

#### Trace Service

- `--trace-retention-high-watermark float`: Disk usage percentage that triggers forced retention cleanup (0-100, default: 95.0).
- `--trace-retention-low-watermark float`: Disk usage percentage where forced retention cleanup stops (0-100, default: 85.0).
- `--trace-retention-check-interval duration`: Interval for checking disk usage (default: 5m).
- `--trace-retention-cooldown duration`: Cooldown period between forced segment deletions (default: 30s).

### Liaison Servers

Liaison servers use the disk usage flags to manage their write queue and prevent disk space exhaustion:

- `--measure-max-disk-usage-percent int`: The maximum disk usage percentage allowed for measure data (0-100, default: 95).
- `--stream-max-disk-usage-percent int`: The maximum disk usage percentage allowed for stream data (0-100, default: 95).
- `--trace-max-disk-usage-percent int`: The maximum disk usage percentage allowed for trace data (0-100, default: 95).

**Write Queue Mechanism**: Liaison servers maintain a write queue that buffers incoming data before syncing it to data servers. When the queue fills up and disk usage exceeds the configured threshold, the liaison server throttles incoming writes to allow the sync process to catch up and free up disk space.

### Property Service

- `--property-max-disk-usage-percent int`: The maximum disk usage percentage allowed (0-100, default: 95).

## How It Works

### Data & Standalone Servers: Forced Retention Cleanup Process

1. **Monitoring**: The disk monitor periodically checks disk usage on the service's data path
2. **Trigger**: When disk usage exceeds the high watermark, forced cleanup begins
3. **Cleanup**: The system removes old data segments and snapshots in controlled steps
4. **Cooldown**: A configurable cooldown period prevents thrashing between deletions
5. **Stop**: Cleanup continues until disk usage falls below the low watermark

### Liaison Servers: Write Queue Management

Liaison servers handle disk management differently due to their role as data coordinators:

1. **Write Queue**: Incoming data is buffered in a write queue before being synced to data servers
2. **Queue Monitoring**: The system monitors both disk usage and queue capacity
3. **Write Throttling**: When disk usage exceeds the threshold, incoming writes are throttled
4. **Sync Process**: The throttling allows the sync process to catch up and transfer data to data servers
5. **Queue Drainage**: As data is synced and removed from the queue, disk space is freed up
6. **Write Resumption**: Once disk usage drops below the threshold, normal write operations resume

### Data Preservation

- Snapshots newer than 24 hours are always preserved during cleanup
- The system follows an oldest-first deletion strategy across all groups
- Only one segment is deleted per iteration to maintain system stability
- Liaison servers preserve data integrity by ensuring all queued data is synced before cleanup

## Important Considerations

### Data & Standalone Servers
- The high watermark must be greater than the low watermark for each service
- When disk usage exceeds the high watermark, the service will start forced cleanup and may throttle writes
- The disk monitor measures usage on the service's data path only
- Snapshots newer than 24 hours are always preserved during cleanup

### Liaison Servers
- Write throttling occurs when disk usage exceeds the configured threshold
- The throttling mechanism allows the sync process to catch up with incoming data
- Liaison servers do not perform automatic data cleanup - they rely on data servers for that
- The write queue acts as a buffer to handle temporary spikes in data ingestion
- Proper tuning of the disk usage threshold is crucial for optimal performance

## Monitoring and Metrics

The disk monitor exposes the following metrics for each service (measure, stream, trace) under the `storage.retention.{service}` namespace:

- `forced_retention_active{service}` (gauge): Whether forced retention cleanup is currently active (1 = active, 0 = inactive)
- `forced_retention_runs_total{service}` (counter): Total number of forced retention cleanup runs
- `forced_retention_segments_deleted_total{service}` (counter): Total number of segments deleted during forced retention cleanup
- `forced_retention_last_run_seconds{service}` (gauge): Timestamp of the last forced retention cleanup run
- `forced_retention_cooldown_seconds{service}` (gauge): Cooldown period between forced segment deletions
- `disk_usage_percent{service}` (gauge): Current disk usage percentage for the service
- `snapshots_deleted_total{service}` (counter): Total number of snapshots deleted during cleanup

## Configuration Examples

### Standalone/Data Server Configuration

```sh
banyand standalone \
  --measure-retention-high-watermark=90.0 \
  --measure-retention-low-watermark=80.0 \
  --measure-retention-check-interval=2m \
  --stream-retention-high-watermark=85.0 \
  --stream-retention-low-watermark=75.0 \
  --trace-retention-high-watermark=95.0 \
  --trace-retention-low-watermark=85.0
```

### Liaison Server Configuration

```sh
banyand liaison \
  --measure-max-disk-usage-percent=90 \
  --stream-max-disk-usage-percent=85 \
  --trace-max-disk-usage-percent=95
```

### Environment Variables

```sh
# For standalone/data servers
export BYDB_MEASURE_RETENTION_HIGH_WATERMARK=90.0
export BYDB_MEASURE_RETENTION_LOW_WATERMARK=80.0
export BYDB_STREAM_RETENTION_HIGH_WATERMARK=85.0
export BYDB_STREAM_RETENTION_LOW_WATERMARK=75.0

# For liaison servers
export BYDB_MEASURE_MAX_DISK_USAGE_PERCENT=90
export BYDB_STREAM_MAX_DISK_USAGE_PERCENT=85
export BYDB_TRACE_MAX_DISK_USAGE_PERCENT=95
```

## Troubleshooting

### High Disk Usage

If you're experiencing high disk usage:

1. Check the current disk usage metrics: `disk_usage_percent{service}`
2. Verify that forced retention is active: `forced_retention_active{service}`
3. Monitor cleanup progress: `forced_retention_segments_deleted_total{service}`
4. Adjust watermarks if needed to trigger cleanup earlier

### Write Throttling

If writes are being throttled:

1. **For data/standalone servers**: Check if forced retention is running and wait for cleanup to complete
2. **For liaison servers**: 
   - Check if the write queue is full and sync process is behind
   - Monitor the sync process performance to data servers
   - Consider reducing the `*-max-disk-usage-percent` value to trigger throttling earlier
   - Ensure data servers have sufficient capacity to receive synced data
3. Monitor the `forced_retention_active` metric to see if cleanup is in progress

### Liaison Server Specific Issues

If liaison servers are experiencing persistent write throttling:

1. **Sync Process Bottleneck**: Check if data servers are overloaded or have high disk usage
2. **Network Issues**: Verify network connectivity between liaison and data servers
3. **Queue Size**: Consider increasing write queue capacity if temporary spikes are common
4. **Threshold Tuning**: Lower the disk usage threshold to provide more buffer space

### Configuration Validation

Ensure your configuration is valid:

- High watermark > Low watermark (for data/standalone servers)
- All percentage values are between 0 and 100
- Check interval and cooldown are positive durations
- Liaison server thresholds should account for expected queue size and sync latency
