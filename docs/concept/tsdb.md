# TimeSeries Database(TSDB)

TSDB is a time-series storage engine designed to store and query large volumes of time-series data. One of the key features of TSDB is its ability to automatically manage data storage over time, optimize performance and ensure that the system can scale to handle large workloads. TSDB empowers `Measure` and `Stream` relevant data.

## Shard

In TSDB, the data in a group is partitioned into shards based on a configurable sharding scheme. Each shard is assigned to a specific set of storage nodes, and those nodes store and process the data within that shard. This allows BanyanDB to scale horizontally by adding more storage nodes to the cluster as needed.

Within each shard, data is stored in different segments based on time ranges. The series index generated based on entities, and the indexes generated based on indexing rules of the `Measure` types are also stored under the shard.

![shard](https://skywalking.apache.org/doc-graph/banyandb/v0.6.0/shard.png)

## Segment

Each segment is composed of multiple parts. Whenever SkyWalking sends a batch of data, BanyanDB writes this batch of data into a new Part. For data of the `Stream` type, the inverted indexes generated based on the indexing rules are also stored in the segment. Since BanyanDB adopts a snapshot approach for data read and write operations, the segment also needs to maintain additional snapshot information to record the validity of the parts.

![segment](https://skywalking.apache.org/doc-graph/banyandb/v0.6.0/segment.png)

## Part

Within a part, data is split into multiple files in a columnar manner. The timestamps are stored in the `timestamps.bin` file, tags are organized in persistent tag families as various files with the `.tf` suffix, and fields are stored separately in the `fields.bin` file. 

In addition, each part maintains several metadata files. Among them, `metadata.json` is the metadata file for the part, storing descriptive information, such as start and end times, part size, etc. 

The `meta.bin` is a skipping index file serves as the entry file for the entire part, helping to index the `primary.bin` file. 

The `primary.bin` file contains the index of each block. Through it, the actual data files or the tagFamily metadata files ending with `.tfm` can be indexed, which in turn helps locate the data in blocks. 

Notably, for data of the `Stream` type, since there are no field columns, the `fields.bin` file does not exist, while the rest of the structure is entirely consistent with the `Measure` type.

![measure-part](https://skywalking.apache.org/doc-graph/banyandb/v0.6.0/measure-part.png)
![stream-part](https://skywalking.apache.org/doc-graph/banyandb/v0.6.0/stream-part.png)

## Block

The diagram below shows the detailed fields within each block. The block is the minimal unit of tsdb, which contains several rows of data. Due to the column-based design, each block is spread over several files.

![measure-block](https://skywalking.apache.org/doc-graph/banyandb/v0.6.0/measure-block.png)
![stream-block](https://skywalking.apache.org/doc-graph/banyandb/v0.6.0/stream-block.png)

## Write Path

The write path of TSDB begins when time-series data is ingested into the system. TSDB will consult the schema repository to check if the group exists, and if it does, then it will hash the SeriesID to determine which shard it belongs to.

Each shard in TSDB is responsible for storing a subset of the time-series data. The shard also holds an in-memory index allowing fast lookups of time-series data.

When a shard receives a write request, the data is written to the buffer as a memory part and the series index and inverted index will also be updated. The worker in the background periodically flushes data, writing the memory part to the disk. After the flush operation is completed, it triggers a merge operation to combine the parts and remove invalid data. 

Whenever a new memory part is generated or a flush and merge operation is triggered, it initiates an update of the snapshot and deletes outdated snapshots.

## Read Path

The read path in TSDB retrieves time-series data from disk or memory and returns it to the query engine. The read path comprises several components: the buffer and parts. The following is a high-level overview of how these components work together to retrieve time-series data in TSDB.

The first step in the read path is to perform an index lookup to determine which parts contain the desired time range. The index contains metadata about each data part, including its start and end time.

If the requested data is present in the buffer (i.e., it has been recently written but not yet persisted to disk), the buffer is checked to see if the data can be returned directly from memory. The read path determines which memory part(s) contain the requested time range. If the data is not present in the buffer, the read path proceeds to the next step.

The next step in the read path is to look up the appropriate parts on disk. Files are the on-disk representation of blocks and are organized by shard and time range. The read path determines which parts contain the requested time range and reads the appropriate blocks from the disk. Due to the column-based storage design, it may be necessary to read multiple data files.
