# Disk Management

BanyanDB provides comprehensive disk management capabilities to prevent disk space exhaustion and ensure optimal storage utilization. This includes both automatic forced retention cleanup and disk usage monitoring.

## Overview

BanyanDB includes a disk monitor that automatically triggers forced retention cleanup when disk usage exceeds configured thresholds. This feature helps prevent disk space exhaustion by automatically removing old data segments and snapshots.

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

**Property Service Behavior**: The property service uses a similar disk management approach to liaison servers, where disk usage monitoring is handled at the write operation level rather than through forced cleanup.

## How It Works

### Data & Standalone Servers: Forced Retention Cleanup Process

1. **Monitoring**: The disk monitor periodically checks disk usage on the service's data path
2. **Trigger**: When disk usage exceeds the high watermark, forced cleanup begins
3. **Cleanup**: The system removes old data segments and snapshots in controlled steps
4. **Cooldown**: A configurable cooldown period prevents thrashing between deletions
5. **Stop**: Cleanup continues until disk usage falls below the low watermark

#### Results When High Watermark is Reached

When disk usage exceeds the high watermark threshold, the following actions occur:

**Immediate Actions:**
- **Forced cleanup activation**: The system sets `forced_retention_active` metric to 1
- **Logging**: An INFO-level log message is generated indicating disk usage above high watermark
- **Metrics update**: `forced_retention_runs_total` counter is incremented
- **Write throttling**: New write requests may be throttled or rejected with `STATUS_DISK_FULL` error

**Cleanup Process:**
1. **Snapshot cleanup**: First, old snapshots (older than 24 hours) are removed
2. **Disk usage recheck**: After snapshot cleanup, disk usage is checked again
3. **Segment deletion**: If still above low watermark, the oldest data segment is deleted
4. **Iterative process**: The process repeats with cooldown periods between deletions
5. **Completion**: Cleanup stops when disk usage falls below the low watermark

**Data Preservation:**
- Snapshots newer than 24 hours are always preserved
- Only one segment is deleted per iteration to maintain system stability
- The system follows an oldest-first deletion strategy across all groups

### Liaison Servers: Write Queue Management

Liaison servers handle disk management differently due to their role as data coordinators:

1. **Write Queue**: Incoming data is buffered in a write queue before being synced to data servers
2. **Queue Monitoring**: The system monitors both disk usage and queue capacity
3. **Write Throttling**: When disk usage exceeds the threshold, incoming writes are throttled
4. **Sync Process**: The throttling allows the sync process to catch up and transfer data to data servers
5. **Queue Drainage**: As data is synced and removed from the queue, disk space is freed up
6. **Write Resumption**: Once disk usage drops below the threshold, normal write operations resume

#### Results When Max Disk Usage Percent is Reached

When disk usage exceeds the configured max disk usage percentage threshold, the following actions occur:

**Immediate Actions:**
- **Health check failure**: The service's `CheckHealth()` method returns `STATUS_DISK_FULL` error
- **Write rejection**: All incoming write requests are rejected with `STATUS_DISK_FULL` status
- **Logging**: A WARN-level log message is generated indicating disk usage is too high
- **Service status**: The service becomes read-only until disk usage decreases

**Error Response Details:**
- **Status Code**: `STATUS_DISK_FULL` (status code 6)
- **Error Message**: "disk usage is too high, stop writing"
- **Client Impact**: Write operations fail immediately with the disk full error
- **Recovery**: Writes resume automatically when disk usage drops below the threshold

**Queue Behavior:**
- **Write Queue**: Data already in the queue continues to be processed and synced
- **New Writes**: New write requests are rejected at the health check level
- **Sync Process**: Continues to drain the queue and sync data to data servers

### Property Service: Write Operation Management

The property service handles disk management through write operation health checks rather than forced cleanup:

1. **Health Check**: Property update operations check disk usage before processing
2. **Write Rejection**: When disk usage exceeds the threshold, property updates are rejected
3. **Read Operations**: Query and delete operations continue to work normally
4. **No Cleanup**: The property service does not perform automatic data cleanup

#### Results When Max Disk Usage Percent is Reached (Property Service)

When disk usage exceeds the configured max disk usage percentage threshold, the following actions occur:

**Immediate Actions:**
- **Health check failure**: Property update operations' `CheckHealth()` method returns `STATUS_DISK_FULL` error
- **Update rejection**: All property update requests are rejected with `STATUS_DISK_FULL` status
- **Logging**: A WARN-level log message is generated indicating disk usage is too high
- **Service status**: Property update operations become read-only until disk usage decreases

**Error Response Details:**
- **Status Code**: `STATUS_DISK_FULL` (status code 6)
- **Error Message**: "disk usage is too high, stop writing"
- **Client Impact**: Property update operations fail immediately with the disk full error
- **Recovery**: Property updates resume automatically when disk usage drops below the threshold

**Operation Behavior:**
- **Property Updates**: Rejected at the health check level when disk usage is too high
- **Property Queries**: Continue to work normally (no disk usage check)
- **Property Deletes**: Continue to work normally (no disk usage check)
- **Snapshots**: Continue to work normally (no disk usage check)
- **Repair Operations**: Continue to work normally (no disk usage check)

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

### Property Service Configuration

```sh
banyand data \
  --property-max-disk-usage-percent=90
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

# For property service
export BYDB_PROPERTY_MAX_DISK_USAGE_PERCENT=90
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

### Property Service Specific Issues

If property service is experiencing persistent write rejection:

1. **Disk Space**: Check available disk space on the property service data path
2. **Threshold Tuning**: Lower the `property-max-disk-usage-percent` value to provide more buffer space
3. **Data Cleanup**: Consider manually cleaning up old property data if automatic cleanup is not available
4. **Read Operations**: Note that property queries and deletes continue to work even when updates are rejected

### Configuration Validation

Ensure your configuration is valid:

- High watermark > Low watermark (for data/standalone servers)
- All percentage values are between 0 and 100
- Check interval and cooldown are positive durations
- Liaison server thresholds should account for expected queue size and sync latency
- Property service threshold should account for expected property data growth and available disk space
