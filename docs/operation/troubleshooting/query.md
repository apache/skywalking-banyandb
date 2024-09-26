# Troubleshooting Query Issues

If you encounter issues with query results in BanyanDB, follow these troubleshooting steps to identify and resolve the problem.

## Query Syntax Errors

### Mandatory Fields Missing

The query's mandatory fields are `time_range`,  `name` and `groups`. If any of these fields are missing, the query will fail. Ensure that the query includes all the required fields and that the syntax is correct.

For `Stream`, the `tag_projection` field is mandatory. For `Measure`, either `tag_projection` or `field_projection` is required.

### Ignore Unknown Fields

If the query includes unknown fields, it won't affect the query results. BanyanDB will ignore any unknown fields in the query. This behavior allows you to add custom fields to the query without affecting the query results. But it also bring the risk of typo in the query.

For example, the following query includes an unknown field `lmit`:

```yaml
name: "network_address_alias_minute"
groups: ["measure-default"]
tagProjection:
  tagFamilies:
    - name: "default"
      tags: ["last_update_time_bucket", "represent_service_id", "represent_service_instance_id"]
    - name: "storage_only"
      tags: ["address"]
# The correct field name should be "limit", not "lmit". 
lmit: 5
```

In this case, the query will still execute successfully, but the `lmit` field will be ignored.

### Invalid Time Range

The valid time range is:

- `start_time` < `end_time`
- minimum time is `1677-09-21 00:12:43.145224192 +0000 UTC.`, which is the minimum time of `time.Time` in Go.
- maximum time is `2262-04-11 23:47:16.854775807 +0000 UTC.`, which is the maximum time of `time.Time` in Go.

If the query includes an invalid time range, the query will fail. Ensure that the time range is correct and within the valid range.

## Unexpected Query Results

### No Data or Partial Data

Please refer to the [Troubleshooting No Data Issue](./no-data.md) guide to identify and resolve the issue.

**Valid Old Data Missing**: If you see valid old data(it's TTL is not reached) is missing, some of data servers may be down, the new data is still ingested into the database, but the query results may be incomplete. Please check the [Active Data Servers](../observability.md#active-instances) to ensure that all data servers are running. The old data will be available once the data servers are back online.

### Duplicate Data

`Stream` and `Measure` handles duplicate data differently:

- `Stream`: If the same data is ingested multiple times, the `Stream` will store all the data points. The query results will include all the duplicate data points with the same entity and timestamp.
- `Measure`: If the same data is ingested multiple times, the `Measure` will store the latest data point. It uses a internal `version` field to determine the latest data point.

`Measure` data version is determined by:

1. If [`DataPointValue.version`](../../api-reference.md#datapointvalue) is set, use it as the version.
2. If [WriteRequest.message_id](../../api-reference.md#writerequest) is set, use it as the version.
3. If neither of the above fields are set, leave the version as 0.

## Query Performance Issues

Use query tracing to understand execution plans and identify bottlenecks. To enable query tracing, set the `trace` field to `true` in the [MeasureQueryRequest](../../api-reference.md#queryrequest) and [StreamQueryRequest](../../api-reference.md#queryrequest-1). The query results will include detailed tracing information to help you identify performance issues.

There are some important nodes in the trace result:

- `measure-grpc` or `stream-grpc`: It represents the overall time spent on the gRPC call.
- `data-xxx`: It represents the time spent on reading data from the data server. The `xxx` is the data server's address. Because all data servers are queried in parallel, the total time(`xxxx-grpc`) spent on reading data is the maximum time spent on reading data from all data servers.

In each data server, there are some important nodes:

- `indexScan`: Using index to fetch data.
  - `seriesIndex.Search`: It represents the time spent on searching the series index for the specified time range.
    tag:
      1. `query`: The query expression to search the series index.
      1. `matched`: The number of series matched by the query.
      1. `field_length`: The number of fields is read from the series index. For `Stream`, it's always 1. For `Measure`, it's the number of indexed tags.
  - `scan-blocks`: It represents the time spent on scanning the data blocks for the matched series.
    1. `series_num`: The number of series to scan. It should be identical to the `matched` in `seriesIndex.Search`.
    1. `part_num`: The number of data parts to scan.
    1. `part_header`: The header of the value list in `part_xxxx`
    1. `part_xxx`: The data part to scan.
    1. `block_header`: The header of the value list in `block_xxx`
    1. `block_xxx`: The data block to scan.
- `iterator`: It represents the time spent on iterating the rows in the data block for filtering, sorting and aggregation.

### Part and Block Information

If the `part_header` is:

```yaml
- key: part_header
  value: MinTimestamp, MaxTimestamp, CompressionSize, UncompressedSize,
    TotalCount, BlocksCount
```

`part_xxxx` is:

```yaml
 - key: part_377403_/tmp/measure/measure-default/seg-20240923/shard-0/000000000005c23b
   value: Sep 23 00:00:00, Sep 23 22:50:00, 37 MB, 61 MB, 736,674, 420,920
```

`377403` is the `PartID`, which means this data part is in the directory `part_377403_/tmp/measure/measure-default/seg-20240923/shard-0/000000000005c23b`. `000000000005c23b` is the hexadecimal representation of the `PartID`.

The `MinTimestamp` and `MaxTimestamp` are `Sep 23 00:00:00` and `Sep 23 22:50:00`, respectively. The `TotalCount` is 736,674, which means there are 736,674 data points in this data part. The `BlocksCount` is 420,920, which means there are 420,920 blocks in this data part. The `CompressionSize` and `UncompressedSize` are the size of the compressed and uncompressed data part, respectively.

If the `block_header` is:

```yaml
- key: block_header
  value: PartID, SeriesID, MinTimestamp, MaxTimestamp, Count, UncompressedSize
```

`block_xxx` is:

```yaml
- key: block_0
  value: 377403, 4570144289778100188, Jun 16 23:08:08, Sep 24 23:08:08,
    1, 16 B
```

The `PartID` is 377403, which means this block is in the data part `part_377403_/tmp/measure/measure-default/seg-20240923/shard-0/0000000000005c23b`. The `SeriesID` is 4570144289778100188, which means this block is for the series with the ID `4570144289778100188`. The `MinTimestamp` and `MaxTimestamp` are `Jun 16 23:08:08` and `Sep 24 23:08:08`, respectively. The `Count` is 1, which means there is only one data point in this block. The `UncompressedSize` is 16 B, which means the uncompressed size of this block is 16 bytes.
