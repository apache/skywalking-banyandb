# Changes by Version

Release Notes.

## 0.9.0

### Features

- Add sharding_key for TopNAggregation source measure
- API: Update the data matching rule from the node selector to the stage name.
- Add dynamical TLS load for the gRPC and HTTP server.
- Implement multiple groups query in one request.
- Replica: Replace Any with []byte Between Liaison and Data Nodes
- Replica: Support configurable replica count on Group.
- Replica: Move the TopN pre-calculation flow from the Data Node to the Liaison Node.
- Add a wait and retry to write handlers to avoid the local metadata cache being loaded.
- Implement primary block cache for measure.
- Implement versioning properties and replace physical deletion with the tombstone mechanism for the property database.
- Implement skipping index for stream.
- Add Load Balancer Feature to Liaison. 
- Implement fadvise for large files to prevent page cache pollution.
- Data Model: Introduce the `Trace` data model to store the trace/span data.
- Support dictionary encoding for low cardinality columns.
- Push down aggregation for topN query.
- Push down min/max aggregation to data nodes
- Introduce write queue mechanism in liaison nodes to efficiently synchronize stream and measure partition folders, improving write throughput and consistency
- Add trace module metadata management.
- Add chunked data sync to improve memory efficiency and performance during data transfer operations, supporting configurable chunk sizes, retry mechanisms, and out-of-order handling for both measure and stream services.
- Implement comprehensive migration system for both measure and stream data with file-based approach and enhanced progress tracking
- Backup/Restore: Add support for AWS S3, Google Cloud Storage (GCS), and Azure Blob Storage as remote targets for backup and restore operations.
- Improve TopN processing by adding "source" tag to track node-specific data, enhancing data handling across distributed nodes
- Implement Login with Username/Password authentication in BanyanDB
- Enhance flusher and introducer loops to support merging operations, improving efficiency by eliminating the need for a separate merge loop and optimizing data handling process during flushing and merging
- Enhance stream synchronization with configurable sync interval - Allows customization of synchronization timing for better performance tuning
- Refactor flusher and introducer loops to support conditional merging - Optimizes data processing by adding conditional logic to merge operations
- New storage engine for trace:
  - Data ingestion and retrieval.
  - Flush memory data to disk.
  - Merge memory data and disk data.
- Enhance access log functionality with sampling option

### Bug Fixes

- Fix the deadlock issue when loading a closed segment.
- Fix the issue that the etcd watcher gets the historical node registration events.
- Fix the crash when collecting the metrics from a closed segment.
- Fix topN parsing panic when the criteria is set.
- Remove the indexed_only field in TagSpec.

### Document

- Introduce AI_CODING_GUIDELINES.md to provide guidelines for using AI assistants (like Claude, Cursor, GitHub Copilot) in development, ensuring generated code follows project standards around variable shadowing, imports, error handling, code style and documentation

## 0.8.0

### Features

- Add the `bydbctl analyze series` command to analyze the series data.
- Index: Remove sortable field from the stored field. If a field is sortable only, it won't be stored.
- Index: Support InsertIfAbsent functionality which ensures documents are only inserted if their docIDs are not already present in the current index. There is a exception for the documents with extra index fields more than the entity's index fields.
- Measure: Introduce "index_mode" to save data exclusively in the series index, ideal for non-timeseries measures.
- Index: Use numeric index type to support Int and Float
- TopN: Group top n pre-calculation result by the group key in the new introduced `_top_n_result` measure, which is used to store the pre-calculation result.
- Index Mode: Index `measure_name` and `tags` in `entity` to improve the query performance.
- Encoding: Improve the performance of encoding and decoding the variable-length int64.
- Index: Add a cache to improve the performance of the series index write.
- Read cpu quota and limit from the cgroup file system to set gomaxprocs.
- Property: Add native storage layer for property.
- Add the max disk usage threshold for the `Measure`, `Stream`, and `Property` to control the disk usage.
- Add the "api version" service to gRPC and HTTP server.
- Metadata: Wait for the existing registration to be removed before registering the node.
- Stream: Introduce the batch scan to improve the performance of the query and limit the memory usage.
- Add memory protector to protect the memory usage of the system. It will limit the memory usage of the querying.
- Metadata: Introduce the periodic sync to sync the metadata from the etcd to the local cache in case of the loss of the events.
- Test: Add the e2e test for zipkin.
- Test: Limit the CPU and memory usage of the e2e test.
- Add taking the snapshot of data files.
- Add backup command line tool to backup the data files.
- Add restore command line tool to restore the data files.
- Add concurrent barrier to partition merge to improve the performance of the partition merge.
- Improve the write performance.
- Add node labels to classify the nodes.
- Add lifecycle management for the node.
- Property: Introduce the schema style to the property.
- Add time range parameters to stream index filter.
- UI: Add the `stages` to groups.
- Add time range return value from stream local index filter.
- Deduplicate the documents on building the series index.

### Bug Fixes

- Fix the bug that TopN processing item leak. The item can not be updated but as a new item.
- Resolve data race in Stats methods of the inverted index.
- Fix the bug when adding new tags or fields to the measure, the querying crashes or returns wrong results.
- Fix the bug that adding new tags to the stream, the querying crashes or returns wrong results.
- UI: Polish Index Rule Binding Page and Index Page.
- Fix: View configuration on Property page.
- UI: Add `indexMode` to display on the measure page.
- UI: Refactor Groups Tree to optimize style and fix bugs.
- UI: Add `NoSort` Field to IndexRule page.
- Metadata: Fix the bug that the cache load nil value that is the unknown index rule on the index rule binding.
- Queue: Fix the bug that the client remove a registered node in the eviction list. The node is controlled by the recovery loop, doesn't need to be removed in the failover process.
- UI: Add prettier to enforce a consistent style by parsing code.
- Parse string and int array in the query result table.
- Fix the bug that fails to update `Group` Schema's ResourceOpts.
- UI: Implement TopNAggregation data query page.
- UI: Update BanyanDB UI to Integrate New Property Query API.
- UI: Fix the Stream List.
- Fix the oom issue when loading too many unnecessary parts into memory.
- bydbctl: Fix the bug that the bydbctl can't parse the absolute time flag.

### Documentation

- Improve the description of the memory in observability doc.
- Update kubernetes install document to align the banyandb helm v0.3.0.
- Add restrictions on updating schema.
- Add docs for the new property storage.
- Update quick start guide to use showcase instead of the old example.

### Chores

- Fix metrics system typo.
- Bump up OAP in CI to 6d262cce62e156bd197177abb3640ea65bb2d38e.
- Update cespare/xxhash to v2 version.
- Bump up Go to 1.24.

### CVEs

- GO-2024-3321: Misuse of ServerConfig.PublicKeyCallback may cause authorization bypass in golang.org/x/crypto
- GO-2024-3333: Non-linear parsing of case-insensitive content in golang.org/x/net/html

## 0.7.0

### File System Changes

- Bump up the version of the file system to 1.1.0 which is not compatible with the previous version.
- Move the series index into segment.
- Swap the segment and the shard.
- Move indexed values in a measure from data files to index files.
- Merge elementIDs.bin and timestamps.bin into a single file.

### Features

- Check unregistered nodes in background.
- Improve sorting performance of stream.
- Add the measure query trace.
- Assign a separate lookup table to each group in the maglev selector.
- Convert the async local pipeline to a sync pipeline.
- Add the stream query trace.
- Add the topN query trace.
- Introduce the round-robin selector to Liaison Node.
- Optimize query performance of series index.
- Add liaison, remote queue, storage(rotation), time-series tables, metadata cache and scheduler metrics.
- Add HTTP health check endpoint for the data node.
- Add slow query log for the distributed query and local query.
- Support applying the index rule to the tag belonging to the entity.
- Add search analyzer "url" which breaks test into tokens at any non-letter and non-digit character.
- Introduce "match_option" to the "match" query.

### Bugs

- Fix the filtering of stream in descending order by timestamp.
- Fix querying old data points when the data is in a newer part. A version column is introduced to each data point and stored in the timestamp file.
- Fix the bug that duplicated data points from different data nodes are returned.
- Fix the bug that the data node can't re-register to etcd when the connection is lost.
- Fix memory leak in sorting the stream by the inverted index.
- Fix the wrong array flags parsing in command line. The array flags should be parsed by "StringSlice" instead of "StringArray".
- Fix a bug that the Stream module didn't support duplicated in index-based filtering and sorting
- Fix the bug that segment's reference count is increased twice when the controller try to create an existing segment.
- Fix a bug where a distributed query would return an empty result if the "limit" was set much lower than the "offset".
- Fix duplicated measure data in a single part.
- Fix several "sync.Pool" leak issues by adding a tracker to the pool.
- Fix panic when removing a expired segment.
- Fix panic when reading a disorder block of measure. This block's versions are not sorted in descending order.
- Fix the bug that the etcd client doesn't reconnect when facing the context timeout in the startup phase.
- Fix the bug that the long running query doesn't stop when the context is canceled.
- Fix the bug that merge block with different tags or fields.
- Fix the bug that the pending measure block is not released when a full block is merged.

### Documentation

- Introduce new doc menu structure.
- Add installation on Docker and Kubernetes.
- Add quick-start guide.
- Add web-ui interacting guide.
- Add bydbctl interacting guide.
- Add cluster management guide.
- Add operation related documents: configuration, troubleshooting, system, upgrade, and observability.

### Chores

- Bump up the version of infra e2e framework.
- Separate the monolithic release package into two packages: banyand and bydbctl.
- Separate the monolithic Docker image into two images: banyand and bydbctl.
- Update CI to publish linux/amd64 and linux/arm64 Docker images.
- Make the build system compiles the binary based on the platform which is running on.
- Push "skywalking-banyandb:<tag>-testing" image for e2e and stress test. This image contains bydbctl to do a health check.
- Set etcd-client log level to "error" and etcd-server log level to "warn".
- Push "skywalking-banyandb:<tag>-slim" image for the production environment. This image doesn't contain bydbctl and Web UI.
- Bump go to 1.23.

## 0.6.1

### Features

- Limit the max pre-calculation result flush interval to 1 minute.
- Use both datapoint timestamp and server time to trigger the flush of topN pre-calculation result.
- Add benchmarks for stream filtering and sorting.
- Improve filtering performance of stream.

### Bugs

- Fix the bug that topN query doesn't return when an error occurs.
- Data race in the hot series index selection.
- Remove SetSchema from measure cache which could change the schema in the cache.
- Fix duplicated items in the query aggregation top-n list.
- Fix non-"value" field in topN pre-calculation result measure is lack of data.
- Encode escaped characters to int64 bytes to fix the malformed data.

## 0.6.0

### Features

- Support etcd client authentication.
- Implement Local file system.
- Add health check command for bydbctl.
- Implement Inverted Index for SeriesDatabase.
- Remove Block Level from TSDB.
- Remove primary index.
- Measure column-based storage:
  - Data ingestion and retrieval.
  - Flush memory data to disk.
  - Merge memory data and disk data.
- Stream column-based storage:
  - Data ingestion and retrieval.
  - Flush memory data to disk.
  - Merge memory data and disk data.
- Add HTTP services to TopNAggregation operations.
- Add preload for the TopN query of index.
- Remove "TREE" index type. The "TREE" index type is merged into "INVERTED" index type.
- Remove "Location" field on IndexRule. Currently, the location of index is in a segment.
- Remove "BlockInterval" from Group. The block size is determined by the part.
- Support querying multiple groups in one request.

### Bugs

- Fix the bug that property merge new tags failed.
- Fix CPU Spike and Extended Duration in BanyanDB's etcd Watching Registration Process.
- Fix panic when closing banyand.
- Fix NPE when no index filter in the query.

### Chores

- Bump go to 1.22.
- Bump node to 2.12.2.
- Bump several tools.
- Bump all dependencies of Go and Node.
- Combine banyand and bydbctl Dockerfile.
- Update readme for bydbctl
- Introduce the go vulnerability check to "pre-push" task.

## 0.5.0

### Features

- List all properties in a group.
- Implement Write-ahead Logging
- Document the clustering.
- Support multiple roles for banyand server.
- Support for recovery buffer using wal.
- Register the node role to the metadata registry.
- Implement the remote queue to spreading data to data nodes.
- Implement the distributed query engine.
- Add mod revision check to write requests.
- Add TTL to the property.
- Implement node selector (e.g. PickFirst Selector, Maglev Selector).
- Unified the buffers separated in blocks to a single buffer in the shard.

### Bugs

- BanyanDB ui unable to load icon.
- BanyanDB ui type error
- Fix timer not released
- BanyanDB ui misses fields when creating a group
- Fix data duplicate writing
- Syncing metadata change events from etcd instead of a local channel.
- Fix parse environment variables error.
- Fix console warnings in dev mod, and optimize `vite` configuration for proxy.

### Chores

- Bump several dependencies and tools.
- Drop redundant "discovery" module from banyand. "metadata" module is enough to play the node and shard discovery role.

## 0.4.0

### Features

- Add TSDB concept document.
- [UI] Add YAML editor for inputting query criteria.
- Refactor TopN to support `NULL` group while keeping seriesID from the source measure.
- Add a sharded buffer to TSDB to replace Badger's memtable. Badger KV only provides SST.
- Add a meter system to control the internal metrics.
- Add multiple metrics for measuring the storage subsystem.
- Refactor callback of TopNAggregation schema event to avoid deadlock and reload issue.
- Fix max ModRevision computation with inclusion of `TopNAggregation`
- Enhance meter performance
- Reduce logger creation frequency
- Add units to memory flags
- Introduce TSTable to customize the block's structure
- Add `/system` endpoint to the monitoring server that displays a list of nodes' system information.
- Enhance the `liaison` module by implementing access logging.
- Add the Istio scenario stress test based on the data generated by the integration access log.
- Generalize the index's docID to uint64.
- Remove redundant ID tag type.
- Improve granularity of index in `measure` by leveling up from data point to series.
- [UI] Add measure CRUD operations.
- [UI] Add indexRule CRUD operations.
- [UI] Add indexRuleBinding CRUD operations.

### Bugs

- Fix iterator leaks and ensure proper closure and introduce a closer to guarantee all iterators are closed
- Fix resource corrupts caused by update indexRule operation
- Set the maximum integer as the limit for aggregation or grouping operations when performing aggregation or grouping operations in a query plan.

### Chores

- Bump go to 1.20.
- Set KV's minimum memtable size to 8MB
- [docs] Fix docs crud examples error
- Modified `TestGoVersion` to check for CPU architecture and Go Version
- Bump node to 18.16

## 0.3.1

### Bugs

- Fix the broken of schema chain.
- Add a timeout to all go leaking checkers.

### Chores

- Bump golang.org/x/net from 0.2.0 to 0.7.0.

## 0.3.0

### Features

- Support 64-bit float type.
- Web Application.
- Close components in tsdb gracefully.
- Add TLS for the HTTP server.
- Use the table builder to compress data.

### Bugs

- Open blocks concurrently.
- Sync index writing and shard closing.
- TimestampRange query throws an exception if no data in this time range.

### Chores

- Fixes issues related to leaked goroutines.
- Add validations to APIs.

For more details by referring to [milestone 0.3.0](https://github.com/apache/skywalking/issues?q=is%3Aissue+milestone%3A%22BanyanDB+-+0.3.0%22)

## 0.2.0

### Features

- Command line tool: bydbctl.
- Retention controller.
- Full-text searching.
- TopN aggregation.
- Add RESTFul style APIs based on gRPC gateway.
- Add "exists" endpoints to the schema registry.
- Support tag-based CRUD of the property.
- Support index-only tags.
- Support logical operator(and & or) for the query.

### Bugs

- "metadata" syncing pipeline complains about an "unknown group".
- "having" semantic inconsistency.
- "tsdb" leaked goroutines.

### Chores

- "tsdb" structure optimization.
  - Merge the primary index into the LSM-based index
  - Remove term metadata.
- Memory parameters optimization.
- Bump go to 1.19.

For more details by referring to [milestone 0.2.0](https://github.com/apache/skywalking/issues?q=is%3Aissue+milestone%3A%22BanyanDB+-+0.2.0%22)

## 0.1.0

### Features

- BanyanD is the server of BanyanDB
  - TSDB module. It provides the primary time series database with a key-value data module.
  - Stream module. It implements the stream data model's writing.
  - Measure module. It implements the measure data model's writing.
  - Metadata module. It implements resource registering and property CRUD.
  - Query module. It handles the querying requests of stream and measure.
  - Liaison module. It's the gateway to other modules and provides access endpoints to clients.
- gRPC based APIs
- Document
  - API reference
  - Installation instrument
  - Basic concepts
- Testing
  - UT
  - E2E with Java Client and OAP
