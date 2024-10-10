# Troubleshooting Overhead Issue

If you encounter issues with high overhead in BanyanDB, follow these troubleshooting steps to identify and resolve the problem.

## High CPU and Memory Usage

If you notice high CPU and memory usage on the BanyanDB server, follow these steps to troubleshoot the issue:

1. **Check Write and Query Rate**: Monitor the write and query rates to identify any spikes in traffic that may be causing high CPU and memory usage. Refer to the [metrics](../observability.md#metrics) documentation for more information on monitoring BanyanDB metrics.
2. **Check Merge Operation Rate**: Monitor the merge operation rate to identify any issues with data compaction that may be be causing high CPU and memory usage. Refer to the [metrics](../observability.md#merge-file-rate) documentation for more information on monitoring BanyanDB metrics.

## High Disk Usage

If you notice high disk usage on the BanyanDB server, follow these steps to troubleshoot the issue:

1. **Check Group TTL**: Verify that the TTL policy for groups is not causing excessive data storage. If the TTL for a group is set too high, it may result in high disk usage. Use the `bydbctl` command to [update the group schema](../../interacting/bydbctl/schema/group.md#update-operation) and adjust the TTL as needed.
2. **Check Segment Interval**: Check the segment interval for groups to ensure that data is being compacted and stored efficiently. If the TTL is 7 days, the segment interval is set to 3 days. At the 10th morning, the first segment will be deleted. There will be 9 days of data in the database at most, which is more than the TTL.

## Too Many Open Files

The BanyanDB uses LSM-tree storage engine, which may open many files. If you encounter issues with too many open files, follow these steps to troubleshoot the issue:

1. **Check File Descriptor Limit**: Verify that the file descriptor limit is set high enough to accommodate the number of open files required by BanyanDB. Use the `ulimit` command to increase the file descriptor limit if needed. Refer to the [remove system limits](../system.md#remove-system-limits) documentation for more information on setting system limits.
2. **Check Write Rate**: Monitor the write rate to identify any spikes in traffic that may be causing too many open files. High write rates can result in a large number of open files on the BanyanDB server.
3. **Check Merge Operation Rate**: Monitor the merge operation rate to identify any issues with data compaction that may be causing too many open files. Low merge operation rates can result in a large number of open files on the BanyanDB server.

## Profile BanyanDB Server

If you are unable to identify the cause of high overhead, you can profile the BanyanDB server to identify performance bottlenecks. Use the `pprof` tool to generate a CPU profile and analyze the performance of the BanyanDB server. Refer to the [profiling](../observability.md#profiling) documentation for more information on profiling BanyanDB.
