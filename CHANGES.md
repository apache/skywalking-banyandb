# Changes by Version

Release Notes.

## 0.10.2

### Bug Fixes

- Fix reuse of byte arrays in min/max implementation causing data corruption.
- Fix index-mode measure queries returning documents outside requested time range.
- Fix flaky trace query filtering caused by non-deterministic sidx tag ordering and add consistency checks for integration query cases.
- MCP: Add validation for properties and harden the mcp server.
- Fix property schema client connection not stable after data node restarted.
- Fix flaky on-disk integration tests caused by Ginkgo v2 random container shuffling closing gRPC connections prematurely.
- ui: fix query editor refresh/reset behavior and BydbQL keyword highlighting.
- Disable the rotation task on warm and cold nodes to prevent incorrect segment boundaries during lifecycle migration.
- Prevent epoch-dated segment directories (seg-19700101) from being created by zero timestamps in distributed sync paths.
- Fix SIDX streaming sync sending SegmentID as MinTimestamp instead of the actual timestamp, causing sync failures on the receiving node.
- Fix handoff controller TOCTOU race allowing disk size limit bypass, and populate sidx MinTimestamp/MaxTimestamp during replay to prevent corrupt segment creation on recovered nodes.
- Delete orphaned parts when no snapshot references them during tsTable initialization.
- Extract shared LocateAll on NodeRegistry to ensure resolveAssignments and syncer GetNodes always produce identical node lists, preventing liaison from enqueuing parts to online/healthy data nodes.
- Add validation for MATCH and IN conditions in inverted index query builder, and handle nil OR branch when all entities are specific.
- Fix wrong backup path of schema property.
- Fix lifecycle migration failure when the target stage has `close: true`.

### Chores

- Upgrade Go and npm dependencies including etcd to v3.6.10, OpenTelemetry to v1.43.0, AWS SDK, and Google Cloud libraries.
- Regenerate expired TLS test certificate with 100-year validity.

## 0.10.0

### Features

- Remove Bloom filter for dictionary-encoded tags.
- Implement BanyanDB MCP.
- Support deleting non-entity tags when updating the schema.
- Remove check requiring tags in criteria to be present in projection.
- Add sorted query support for the Property.
- Update bydbQL to add sorted query support for the Property.
- Remove the windows arch for binary and docker image.
- Support writing data with specifications.
- Persist series metadata in liaison queue for measure, stream and trace models.
- Update the dump tool to support analyzing the parts with smeta files.
- Add replication integration test for measure.
- Activate the property repair mechanism by default.
- Add snapshot time retention policy to ensure the snapshot only can be deleted after the configured minimum age(time).
- **Breaking Change**: Change the data storage path structure for property model:
  - From: `<data-dir>/property/data/shard-<id>/...`
  - To: `<data-dir>/property/data/<group>/shard-<id>/...`
- Add a generic snapshot coordination package for atomic snapshot transitions across trace and sidx.
- Support map-reduce aggregation for measure queries: map phase (partial aggregation on data nodes) and reduce phase (final aggregation on liaison).
- Add eBPF-based KTM I/O monitor for FODC agent.
- Support relative paths in configuration.
- Support 'none' node discovery and make it the default.
- Support server-side element ID generation for stream writes when clients omit element_id.
- Implement entire group deletion.

### Bug Fixes

- Fix the wrong retention setting of each measure/stream/trace.
- Fix server got panic when create/update property with high dist usage.
- Fix incorrect key range update in sidx part metadata.
- Fix panic in measure block merger when merging blocks with overlapping timestamps.
- Fix unsupported empty string tag bug.
- Fix duplicate elements in stream query results by implementing element ID-based deduplication across scan, merge, and result building stages.
- Fix data written to the wrong shard and related stream queries.
- Fix the lifecycle panic when the trace has no sidx.
- Fix panic in sidx merge and flush operations when part counts don't match expectations.
- Fix trace queries with range conditions on the same tag (e.g., duration) combined with ORDER BY by deduplicating tag names when merging logical expression branches.
- Fix sidx tag filter range check returning inverted skip decision and use correct int64 encoding for block min/max.
- Ignore take snapshot when no data.
- Fix measure standalone write handler resetting accumulated groups on error, which dropped all successfully processed events in the batch.
- Fix memory part reference leak in mustAddMemPart when tsTable loop closes.
- Fix memory part leak in syncPartContext Close and prevent double-release in FinishSync.
- Fix segment reference leaks in measure/stream/trace queries and ensure chunked sync sessions close part contexts correctly.
- Fix duplicate query execution in distributed measure Agg+TopN queries by enabling push-down aggregation, removing the wasteful double-query pattern.
- Fix nil pointer panic in segment collectMetrics during shutdown.
- Fix property schema client connection instability after data node restart.
- Fix take snapshot error when no data in the segment.
- Fix(storage): disable rotation task on warm and cold lifecycle nodes.
- Fix(storage): prevent epoch segment creation from zero timestamps.
- Fix(sidx): use MinTimestamp/MaxTimestamp instead of SegmentID in streaming sync.
- Fix(handoff): prevent size limit bypass and sidx timestamp corruption in handoff replay.
- Fix(handoff): prevent enqueuing parts for online nodes via shared LocateAll.
- Fix wrong backup path of schema property.
- Fix OOM issue during migration when a group contains a large amount of data.
- Fix lifecycle migration failure when the target stage has `close: true`.
- Fix stale sync request blocking watch session channel.
- Fix nil pointer panic in disk monitor during early initialization.
- Fix FileSystemError not matching io/fs.ErrNotExist sentinel.
- Fix(topn): deduplicate entities in TopN aggregation query.
- Fix(stream): skip element-index visit when idx/ is absent.
- Fix(metadata): widen FODC inspection broadcast deadline and parallelize InspectAll.
- Fix(measure,stream,trace): eliminate flusher/introducer data race on shutdown.
- Fix: use `topic` instead of `session_id` as the Prometheus label.
- Fix(fodc): heal reconnect deadlock and add error message when no agents for lifecycle request.
- Fix(MCP): add explicit validation for properties and tools, and harden the server.

### Chores

- Upgrade Go and npm dependencies for CVE fixes.
- Bump ui and mcp npm dependencies for CVE fixes.
