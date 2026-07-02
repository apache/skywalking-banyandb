# Troubleshooting Overhead Issue

If you encounter issues with high overhead in BanyanDB, follow these troubleshooting steps to identify and resolve the problem.

## High CPU and Memory Usage

If you notice high CPU and memory usage on the BanyanDB server, follow these steps to troubleshoot the issue:

1. **Check Write and Query Rate**: Monitor the write and query rates to identify any spikes in traffic that may be causing high CPU and memory usage. Refer to the [metrics](../observability/metrics.md) documentation for more information on monitoring BanyanDB metrics.
2. **Check Merge Operation Rate**: Monitor the merge operation rate to identify any issues with data compaction that may be causing high CPU and memory usage. Refer to the [Merge File Rate](../observability/metrics.md#merge-file-rate) metric for more information.
3. **Watch the memory protector**: BanyanDB runs a memory protector that **stops query execution** when memory usage exceeds `--allowed-percent` (default **75**) of total memory, or `--allowed-bytes` when that is set. High memory therefore usually surfaces first as slow or failing queries (`memory acquisition failed`); see [Memory Acquisition Failed](./query.md#memory-acquisition-failed). Track System Memory %, RSS, and GC pause — **Key Signals #5 and #6** in [Observability › Key Signals to Watch](../observability/overview.md#key-signals-to-watch).

## High Disk Usage

If you notice high disk usage on the BanyanDB server, follow these steps to troubleshoot the issue:

1. **Check Group TTL**: Verify that the TTL policy for groups is not causing excessive data storage. If the TTL for a group is set too high, it may result in high disk usage. Use the `bydbctl` command to [update the group schema](../../interacting/bydbctl/schema/group.md#update-operation) and adjust the TTL as needed.
2. **Check Segment Interval**: Check the segment interval for groups to ensure that data is being compacted and stored efficiently. If the TTL is 7 days, the segment interval is set to 3 days. At the 10th morning, the first segment will be deleted. There will be 9 days of data in the database at most, which is more than the TTL.
3. **Check the liaison write queue (wqueue)**: On liaison nodes, writes that have not yet been forwarded to data nodes are persisted on disk in a write-ahead queue. If data nodes are slow or unreachable, this queue **accumulates on disk** (it does not drop data after a fixed window) and can fill the liaison disk, eventually triggering `STATUS_DISK_FULL`. Watch `banyandb_*_pending_data_count` / `*_total_file_parts` for `container_name="liaison"` — **Key Signal #7** in [Observability › Key Signals to Watch](../observability/overview.md#key-signals-to-watch). See [Disk Management](../disk-management.md#inspecting-the-liaison-write-queue-on-disk) for the on-disk layout.

## Cannot Write Data

When a node's disk usage gets too high, BanyanDB puts the affected module into a **read-only** state and rejects writes with `STATUS_DISK_FULL`. **Queries keep running**, and writes are accepted again automatically once disk usage drops back below the threshold. Setting a flag to `0` forces that module read-only immediately. Which flag controls the threshold depends on the node role (all default to **95%**):

- **Data / standalone nodes** — `measure-retention-high-watermark`, `stream-retention-high-watermark`, `trace-retention-high-watermark` (default `95.0`). Crossing the high watermark also triggers **forced retention cleanup**, which stops once usage falls back below the matching `*-retention-low-watermark` (default `85.0`).
- **Liaison nodes** — `measure-max-disk-usage-percent`, `stream-max-disk-usage-percent`, `trace-max-disk-usage-percent` (default `95`). This guards the liaison's on-disk write queue (wqueue); see [High Disk Usage](#high-disk-usage) above.
- **Property** — `property-max-disk-usage-percent` (default `95`).

This is **Key Signal #4 (Disk utilization)** in [Observability › Key Signals to Watch](../observability/overview.md#key-signals-to-watch); alert well before the threshold (~80–85%).

## Too Many Open Files

The BanyanDB uses LSM-tree storage engine, which may open many files. If you encounter issues with too many open files, follow these steps to troubleshoot the issue:

1. **Check File Descriptor Limit**: Verify that the file descriptor limit is set high enough to accommodate the number of open files required by BanyanDB. Use the `ulimit` command to increase the file descriptor limit if needed. Refer to the [remove system limits](../system.md#remove-system-limits) documentation for more information on setting system limits.
2. **Check Write Rate**: Monitor the write rate to identify any spikes in traffic that may be causing too many open files. High write rates can result in a large number of open files on the BanyanDB server.
3. **Check Merge Operation Rate**: Monitor the merge operation rate to identify any issues with data compaction that may be causing too many open files. Low merge operation rates can result in a large number of open files on the BanyanDB server.

## Profile BanyanDB Server

If you are unable to identify the cause of high overhead, you can profile the BanyanDB server to identify performance bottlenecks. Use the `pprof` tool to generate a CPU profile and analyze the performance of the BanyanDB server. Refer to the [profiling](../observability/profiling.md) documentation for more information on profiling BanyanDB.
