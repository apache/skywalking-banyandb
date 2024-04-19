# TimeSeries Database(TSDB)

TSDB is a time-series storage engine designed to store and query large volumes of time-series data. One of the key features of TSDB is its ability to automatically manage data storage over time, optimize performance and ensure that the system can scale to handle large workloads. TSDB empowers `Measure` and `Stream` relevant data.

## Shard

In TSDB, the data in a group is partitioned into shards based on a configurable sharding scheme. Each shard is assigned to a specific set of storage nodes, and those nodes store and process the data within that shard. This allows BanyanDB to scale horizontally by adding more storage nodes to the cluster as needed.

Within each shard, data is stored in different segments based on time ranges. The primary index generated based on entities and the indexes generated based on indexing rules of the `Measure` types are also stored under the shard.

[shard](https://skywalking.apache.org/doc-graph/banyandb/v0.6.0/shard.png)

## Segment

Each segment is composed of multiple parts. Whenever SkyWalking sends a batch of data, BanyanDB writes this batch of data into a new Part. For data of the `Stream` type, the inverted indexes and LSM indexes generated based on the indexing rules are also stored in the segment. Since BanyanDB adopts a snapshot approach for data read and write operations, the segment also needs to maintain additional snapshot information to record the validity of the parts.

[segment](https://skywalking.apache.org/doc-graph/banyandb/v0.6.0/segment.png)

## Part

Within a part, data is split into multiple files in a columnar manner. The timestamps are stored in the `timestamps.bin` file, tags are organized as multiple files with the `.tf` suffix using column families, and fields are stored separately in the `fields.bin` file. 

In addition, each part maintains several metadata files. Among them, `metadata.json` is the metadata file for the part, storing descriptive information about the part. The `meta.bin` file serves as the entry file for the entire part, helping to index the `primary.bin` file. Through the `primary.bin` file, the actual data files or the tagFamily metadata files ending with `.tfm` can be indexed, which in turn helps locate the tag data. 

Notably, for data of the `Stream` type, since there are no field columns, the `fields.bin` file does not exist, while the rest of the structure is entirely consistent with the `Measure` type.

[measure-part](https://skywalking.apache.org/doc-graph/banyandb/v0.6.0/measure-part.png)
[stream-part](https://skywalking.apache.org/doc-graph/banyandb/v0.6.0/stream-part.png)

## Block

The figure below shows the detailed fields within each block. It is worth noting that when storing tag and field data, BanyanDB prioritizes storing data points or elements with the same tag consecutively. This helps improve the compression efficiency of the files.

[measure-block](https://skywalking.apache.org/doc-graph/banyandb/v0.6.0/measure-block.png)
[stream-block](https://skywalking.apache.org/doc-graph/banyandb/v0.6.0/stream-block.png)

## Write Path

The write path of TSDB begins when time-series data is ingested into the system. TSDB will consult the schema repository to check if the group exists, and if it does, then it will hash the SeriesID to determine which shard it belongs to.

Each shard in TSDB is responsible for storing a subset of the time-series data, and it uses a write-ahead log to record incoming writes in a durable and fault-tolerant manner. The shard also holds an in-memory index allowing fast lookups of time-series data.

When a shard receives a write request, the data is written to the buffer as a series of buckets. Each bucket is a fixed-size chunk of time-series data typically configured to be several minutes or hours long. As new data is written to the buffer, it is appended to the current bucket until it is full. Once the bucket is full, it is closed, and a new bucket is created to continue buffering writes.

Once a bucket is closed, it is stored as a single SST in a shard. The file is indexed and added to the index for the corresponding time range and resolution.

## Read Path

The read path in TSDB retrieves time-series data from disk or memory and returns it to the query engine. The read path comprises several components: the buffer, cache, and SST file. The following is a high-level overview of how these components work together to retrieve time-series data in TSDB.

The first step in the read path is to perform an index lookup to determine which blocks contain the desired time range. The index contains metadata about each data block, including its start and end time and its location on disk.

If the requested data is present in the buffer (i.e., it has been recently written but not yet persisted to disk), the buffer is checked to see if the data can be returned directly from memory. The read path determines which bucket(s) contain the requested time range. If the data is not present in the buffer, the read path proceeds to the next step.

If the requested data is present in the cache (i.e., it has been recently read from disk and is still in memory), it is checked to see if the data can be returned directly from memory. The read path proceeds to the next step if the data is not in the cache.

The final step in the read path is to look up the appropriate SST file on disk. Files are the on-disk representation of data blocks and are organized by shard and time range. The read path determines which SST files contain the requested time range and reads the appropriate data blocks from the disk.
