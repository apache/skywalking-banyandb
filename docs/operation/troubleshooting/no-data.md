# Troubleshooting No Data Issue

If you encounter issues with missing data in BanyanDB, follow these troubleshooting steps to identify and resolve the problem.

## Check Data Ingestion

1. **Monitor Write Rate**: Use the BanyanDB metrics [write rate](../observability.md#write-rate)to monitor the write rate and ensure that data is being ingested into the database.
2. **Monitor Write Errors**: Monitor the [write errors](../observability.md#write-and-query-errors-rate) metric to identify any issues with data ingestion. High write errors can indicate problems with data ingestion.
3. **Review Ingestion Logs**: Check the BanyanDB logs for any errors or warnings related to data ingestion. Look for messages indicating failed writes or data loss.

## Verify the Query Time Range

Ensure that the query time range is correct and includes the data you expect to see. Incorrect time ranges can result in missing data in query results.

1. **Check Available Data**: Check the folders and files in the BanyanDB data directory to verify that the data files exist for the specified time range. Refer to the [tsdb](../../concept/tsdb.md) documentation for more information on data storage.
2. **Time Zone Settings**: Verify that the time zone settings in the query are correct and align with the data stored in BanyanDB. The BanyanDB server and bydbctl uses the time zone aligned with the system time zone by default.

## Check Data Retention Policy

Verify that the data retention policy is not deleting data prematurely. If the data retention policy is set to delete data after a certain period, it may result in missing data in query results. Please check the [Data Lifecycle](../../interacting/data-lifecycle.md) documentation for more information on data retention policies.

## Metadata Missing

If the metadata for a group, measure or stream is missing, it can result in missing data in query results. Ensure that the metadata for the group, measure or stream is correctly defined and available in the BanyanDB metadata registry.

If only the metadata is missing, you can recreate the metadata using the SkyWalking OAP, bydbctl or WebUI to restore the missing metadata. Refer to the [metadata management](../../interacting/bydbctl/schema/) documentation for more information.
