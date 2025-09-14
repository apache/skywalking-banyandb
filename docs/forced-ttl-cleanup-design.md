# Design: Forced TTL Cleanup on High Disk Usage

**Author**: Gemini

**Date**: 2025-09-12

**Status**: Proposed

## 1. Background

BanyanDB's current Time-To-Live (TTL) feature cleans up expired data based on a predefined, scheduled cron job (daily by default). This is effective for routine storage optimization under predictable workloads.

However, in scenarios where disk usage spikes unexpectedly—due to a sudden surge in data ingestion, a misconfigured retention policy, or delays in the regular TTL execution—the scheduled cleanup may not react quickly enough to prevent the disk from filling up. This can lead to service outages, performance degradation, and require manual intervention to restore normal operation.

This document proposes a new mechanism to trigger a forced cleanup based on disk usage watermarks, making the system more resilient and self-healing. The relevant existing TTL logic can be found in `banyand/internal/storage/rotation.go`.

The BanyanDB codebase includes three primary data services: `measure`, `trace`, and `stream`. Each of these services is configured independently using a flag and environment variable system. This design ensures that the new disk-based retention settings are consistently applied across all three services.

## 2. Goals

- To automatically trigger data cleanup when disk usage exceeds a configurable high-watermark threshold.
- To prevent disk exhaustion and related service disruptions.
- To make the storage system more resilient to sudden increases in data volume.
- To reduce the need for manual cleanup operations.

## 3. Non-Goals

- This feature is not intended to be a real-time, instantaneous cleanup mechanism. The disk usage check will be periodic.
- It will not replace the existing time-based TTL but will act as a complementary, emergency cleanup measure.

## 4. Proposed Design

The core is a per-service **Disk Monitor** and **Forced Retention Orchestrator** that work alongside the existing retention system. Each service (`measure`, `trace`, `stream`) runs one monitor on its `dataPath`, coordinating across all groups/TSDBs under that service. When disk usage exceeds the high watermark, the orchestrator removes space in small, controlled steps until usage falls below the low watermark.

### 4.1. Configuration Parameters

For each service (`measure`, `trace`, `stream`), reuse and rename the existing write-throttle flag as the forced-retention high watermark, and add low watermark and check interval. Defaults apply service-wide.

-   **`[service]-retention-high-watermark`**: Float 0-100. Triggers write throttling and forced cleanup. Default: `95.0`.
-   **`[service]-retention-low-watermark`**: Float 0-100. Stop condition for forced cleanup. Default: `85.0`.
-   **`[service]-retention-check-interval`**: Duration for disk usage checks. Default: `5m`.
-   **`[service]-retention-cooldown`**: Duration to sleep between forced deletions to avoid thrashing. Default: `30s`.

Notes:
- The high watermark replaces the old `*-max-disk-usage-percent` flag (rename in-place).
- The monitor measures the service’s resolved `dataPath` only.

### 4.2. Component: Disk Monitor (per service)

Responsible for tracking disk space and orchestrating cleanup across all groups/TSDBs in the service.

- **Lifecycle**: Initialized by the service (`measure`, `stream`, `trace`) and runs in a background goroutine. Stops immediately on service shutdown.
- **Functionality**:
    1.  On a periodic ticker (`retention-check-interval`), measure disk usage for the service `dataPath` (resolve symlinks; read the filesystem containing that path).
    2.  If usage ≥ `retention-high-watermark` and a forced run is not already active, set `isForcedCleanupActive = true` and enter the forced cleanup loop (4.3).
    3.  If usage ≤ `retention-low-watermark`, set `isForcedCleanupActive = false` and exit.
    4.  Between deletion steps, sleep for `retention-cooldown` to avoid thrashing.

### 4.3. Component: Forced Retention Orchestrator

Coordinates deletions across all groups/TSDBs under the service. Applies the following policy and safety controls:

- **Ordering**: Delete the globally oldest segment across all TSDBs in the service (not per-TSDB). Build a min-heap of candidate segments by end time; pop the oldest.
- **Granularity**: Per forced-run iteration, delete at most one segment. Sleep for `retention-cooldown` between iterations; re-check disk usage each iteration. Continue until usage ≤ low watermark or no more deletable segments.
- **Keep-one rule**: Never delete the last remaining segment of a TSDB.
- **TTL override**: It is acceptable to delete segments that have not yet reached TTL, subject to the keep-one rule.
- **Snapshots-first**: Before deleting any segments, remove snapshots older than 24 hours in the service’s snapshot directory, then re-check usage. Always keep snapshots created within the last 24 hours.
- **Serialization & shutdown**: Forced deletions are serialized with the regular TTL job; the orchestrator acquires the same exclusivity guard used by TTL and aborts promptly on service shutdown.

## 5. Implementation Plan

### 5.1. Service-level Orchestrator

Implement a per-service monitor and orchestrator in each service package:

1.  New file: `banyand/[service]/disk_monitor.go` implementing a `diskMonitor` that:
    - Measures usage for the service `dataPath`.
    - Cleans up snapshots older than 24h in `[dataPath]/snapshots` (or the service’s `snapshotDir`), preserving those created within the last 24h.
    - Iteratively deletes at most one oldest segment globally across groups, with a sleep of `retention-cooldown` between deletions, until usage ≤ low watermark.

2.  Wire lifecycle: Start the monitor in `PreRun`; stop it on service shutdown.

### 5.2. Storage API Additions (serialization with TTL)

Enhance storage to support safe, granular deletions and exclusivity with TTL:

1.  `segmentController` in `banyand/internal/storage/segment.go`:
    - Add `peekOldestSegmentEndTime() (time.Time, bool)` to get the oldest segment’s end time (skipping closed/empty as needed).
    - Add `removeOldest() (bool, error)` to delete exactly one oldest segment and update internal state.

2.  `database` in `banyand/internal/storage/tsdb.go` and `rotation.go`:
    - Introduce a shared exclusivity gate (reuse `retentionTask.running` or a new `retentionGate chan struct{}`) so forced deletions and TTL cannot overlap.
    - Expose methods:
        - `PeekOldestSegmentEndTime() (time.Time, bool)`
        - `DeleteOldestSegment() (bool, error)`
      Both acquire the exclusivity gate and no-op if the database is closed.

3.  TSDB interface: Optionally extend the public TSDB interface to include the two methods above for use by services.

### 5.3. Configuration

Update each service to rename and add flags:

- Rename `*-max-disk-usage-percent` to `*-retention-high-watermark` (migration note in CHANGES.md).
- Add `*-retention-low-watermark` (default 85), `*-retention-check-interval` (default 5m), and `*-retention-cooldown` (default 30s).
- The high watermark is used both for write throttling and as the trigger for forced cleanup.

### 5.4. Metrics (metrics-only observability)

Expose per-service metrics (namespace: storage/retention):

- `forced_retention_active{service}` (gauge)
- `forced_retention_runs_total{service}` (counter)
- `forced_retention_segments_deleted_total{service}` (counter)
- `forced_retention_last_run_seconds{service}` (gauge)
- `forced_retention_cooldown_seconds{service}` (gauge)
- `disk_usage_percent{service}` (gauge)
- `snapshots_deleted_total{service}` (counter)

### 5.5. Testing

- Unit: snapshot-first behavior, keep-one rule, single-delete per iteration, cooldown, serialization with TTL, shutdown abort.
- Integration: multi-group oldest-first ordering, high→low watermark convergence, presence of recent (<24h) snapshots, concurrent ingestion.

### 5.6. Notes on Alternatives Considered

- We chose a per-service orchestrator (instead of per-TSDB) to avoid duplicated work and to implement global oldest-first ordering across groups.
- We serialize with TTL via a shared gate to ensure safety and predictability.

### 5.7. Service Flag Updates (examples)

Each service updates its `FlagSet` to:

- Rename `*-max-disk-usage-percent` → `*-retention-high-watermark`.
- Add `*-retention-low-watermark`, `*-retention-check-interval`, `*-retention-cooldown`.

Example (Measure):

```go
// In banyand/measure/svc_data.go
flagS.Float64Var(&s.option.highWatermark, "measure-retention-high-watermark", 95.0, "disk usage high watermark (also write throttle)")
flagS.Float64Var(&s.option.lowWatermark, "measure-retention-low-watermark", 85.0, "disk usage low watermark")
flagS.DurationVar(&s.option.retentionCheckInterval, "measure-retention-check-interval", 5*time.Minute, "disk usage check interval")
flagS.DurationVar(&s.option.retentionCooldown, "measure-retention-cooldown", 30*time.Second, "cooldown between forced deletions")
```

### 5.8. Passing Configuration and Orchestration

- The monitor runs in the service and discovers all active TSDBs/groups via the existing schema repositories.
- Storage provides granular, serialized deletion methods; services call them as needed.

This design introduces a robust, self-regulating mechanism to protect BanyanDB from storage-related outages while integrating cleanly with the existing retention architecture, while respecting safety constraints (keep-one, snapshots-first, serialization, cooldown).
