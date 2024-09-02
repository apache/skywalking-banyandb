# Troubleshooting 

## Error Checklist

When facing issues with BanyanDB, follow this checklist to effectively troubleshoot and resolve errors.

### 1. Collect Information

Gather detailed information about the error to assist in diagnosing the issue:

- **Error Message**: Note the exact error message displayed.
- **Logs**: Collect relevant log files from the BanyanDB system. See the [Logging](./observability.md#logging) section for more information.
- **Query Tracing**: If the error is related to a query, enable query tracing to capture detailed information about the query execution. See the [Query Tracing](./observability.md#query-tracing) section for more information.
- **Environment**: Document the environment details, including OS, BanyanDB version, and hardware specifications.
- **Database Schema**: Provide the schema details related to the error. See the [Schema Management](../interacting/bydbctl/schema/) section for more information.
- **Data Sample**: If applicable, include a sample of the data causing the error.
- **Configuration Settings**: Share the relevant configuration settings.
- **Data Files**: Attach any relevant data files. See the [Configuration](./configuration.md) section to find where the date files is stored.
- **Reproduction Steps**: Describe the steps to reproduce the error.

### 2. Define Error Type

Classify the error to streamline the troubleshooting process:

- **Configuration Error**: Issues related to incorrect configuration settings.
- **Network Error**: Problems caused by network connectivity.
- **Performance Error**: Slowdowns or high resource usage.
- **Data Error**: Inconsistencies or corruption in stored data.

### 3. Error Support Procedure

Follow this procedure to address the identified error type:

- Identify the error type based on the collected information.
- Refer to the relevant sections in the documentation to troubleshoot the error.
- Refer to the issues section in the SkyWalking repository for known issues and solutions.
- If the issue persists, submit a discussion in the [SkyWalking Discussion](https://github.com/apache/skywalking/discussions) for assistance.
- You can also raise a bug report in the [SkyWalking Issue Tracker](https://github.com/apache/skywalking/issues) if the issue is not resolved.
- Finally, As a OpenSource project, you could try to fix the issue by yourself and submit a pull request.

Here's an expanded section on common issues for your BanyanDB troubleshooting document:

## Common Issues

### Write Failures

**Troubleshooting Steps:**

1. **Check Disk Space**: Ensure there is enough disk space available for writes.
2. **Review Permissions**: Verify that BanyanDB has the necessary permissions to write to the target directories.
3. **Examine Logs**: Look for error messages in the logs that might indicate the cause of the failure.
4. **Network Issues**: Ensure network stability if the write operation involves remote nodes.
5. **Configuration Settings**: Double-check configuration settings related to write operations.

### Incorrect Query Results

**Diagnose and Fix:**

1. **Verify Query Syntax**: Ensure that the query syntax is correct and aligns with BanyanDB's query API.
2. **Inspect Indexes**: Ensure indexes are correctly defined and up-to-date.
3. **Review Recent Changes**: Consider any recent changes to data or schema that might affect query results.
4. **

### Performance Issues

#### Slow Write

**Steps to Address:**

1. **Resource Allocation**: Ensure adequate resources (CPU, memory) are allocated to BanyanDB.
2. **Batch Writes**: Increasing batch sizes can improve write performance.
3. **Monitor Disk I/O**: Check for disk I/O bottlenecks.

#### Slow Query

**Optimize Slow Queries:**

1. **Analyze Query Plans**: Use query tracing to understand execution plans and identify bottlenecks.
2. **Index Usage**: Ensure appropriate indexes are used to speed up queries.
3. **Refactor Queries**: Simplify complex queries where possible. For example, using `IN` instead of multiple `OR` conditions.
4. **Sharding**: Consider adding more shards to distribute query load.

#### Out of Memory (OOM)

**Handle OOM Errors:**

1. **Increase Memory Limits**: Adjust memory allocation settings for BanyanDB.
2. **Optimize Queries**: Ensure queries are efficient and not causing excessive memory usage.
3. **Data Sharding**: Distribute data across shards to reduce memory pressure.
4. **Monitor Memory Usage**: Use monitoring tools to track memory usage patterns.

### Process Crashes and File Corruption

**Steps to Diagnose and Recover:**

1. **Examine Log Files**: Check logs for error messages leading up to the crash.
2. **Check File System**: Ensure the file system is not corrupted and is functioning properly.
3. **Update Software**: Ensure BanyanDB and its dependencies are up-to-date with the latest patches.
4. **Remove Corrupted Data**: If data corruption is detected, remove the corrupted data and restore from backups if necessary.

### How to Remove Corrupted Data

Corrupted data can cause BanyanDB to malfunction or produce incorrect results. Follow these steps to safely remove corrupted data from BanyanDB:

1. **Identify the Corrupted Data**:
   Monitor the BanyanDB logs for any error messages indicating data corruption. The file is located in a part directory. You have to remove the whole part directory instead of a single file.

2. **Shutdown BanyanDB**:
   - Before making any changes to the data files, ensure that BanyanDB is not running. This prevents further corruption and ensures data integrity.
   - Send `SIGTERM` or `SIGINT` signals to the BanyanDB process to gracefully shut it down

3. **Locate the Snapshot File**:
   - In each shard of the TSDB (Time Series Database), there is a [snapshot file](../concept/tsdb.md#shard) that contains all alive parts directories.
   - Navigate to the directory where BanyanDB stores its data. This is typically specified in the [flags](./configuration.md)

4. **Remove the Corrupted File**:
   - Identify the corrupted part within the snapshot directory.
   - Remove the part's record from the snapshot file.

5. **Clean Up Part**:
   - Remove the corrupted part directory from the disk.

6. **Restart BanyanDB**:
   - Once the corrupted part is removed and the metadata is cleaned up, restart BanyanDB to apply the changes

7. **Verify the Integrity**:
   - After restarting, monitor the BanyanDB logs to ensure that the corruption issues have been resolved.
   - Run any necessary integrity checks or queries to verify that the database is functioning correctly.

8. **Prevent Future Corruptions**:
   - Monitor system resources and ensure that the hardware and storage systems are functioning correctly.
   - Keep BanyanDB and its dependencies updated to the latest versions to benefit from bug fixes and improvements.

By following these steps, you can safely remove corrupted data from BanyanDB and ensure the continued integrity and performance of your database.
