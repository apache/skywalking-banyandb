# Troubleshooting Crash Issues

If BanyanDB processes crash or encounter file corruption, follow these steps to diagnose and recover from the issue.

## Remove Corrupted Standalone Metadata

If the BanyanDB standalone process crashes due to corrupted metadata. You should remove the corrupted metadata:

1. **Shutdown BanyanDB**:
   - Before making any changes to the data files, ensure that BanyanDB is not running. This prevents further corruption and ensures data integrity.
   - Send `SIGTERM` or `SIGINT` signals to the BanyanDB process to gracefully shut it down
2. **Locate the Metadata File**:
   - The metadata file is located in the standalone directory.
   - Navigate to the directory where BanyanDB stores its standalone data. This is typically specified in the [metadata-root-path](../configuration.md#data--storage)

## Remove Corrupted Stream or Measure Data

The logs may indicate that the crash was caused by corrupted data. In such cases, it is essential to remove the corrupted data to restore the integrity of the database. Follow these steps to safely remove corrupted data from BanyanDB:

1. **Identify the Corrupted Data**:
   Monitor the BanyanDB logs for any error messages indicating data corruption. The file is located in a part directory. You have to remove the whole part directory instead of a single file.

2. **Shutdown BanyanDB**:
   - Before making any changes to the data files, ensure that BanyanDB is not running. This prevents further corruption and ensures data integrity.
   - Send `SIGTERM` or `SIGINT` signals to the BanyanDB process to gracefully shut it down

3. **Locate the Snapshot File**:
   - In each shard of the TSDB (Time Series Database), there is a [snapshot file](../../concept/tsdb.md#shard) that contains all alive parts directories.
   - Navigate to the directory where BanyanDB stores its data. This is typically specified in the [flags](../configuration.md)

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