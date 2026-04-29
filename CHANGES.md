# Changes by Version

Release Notes.

## 0.10.2

### Bug Fixes

- Fix reuse of byte arrays in min/max implementation causing data corruption.
- Fix index-mode measure queries returning documents outside requested time range.
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

## 0.10.1

### Bug Fixes

- Fix reuse of byte arrays in min/max implementation causing data corruption.
- Fix flaky trace query filtering due to non-deterministic sidx tag ordering.
- Fix index-mode measure queries returning documents outside requested time range.
- Fix nil pointer panic in segment collectMetrics during shutdown.
- Fix entity tag handling in trace filter preventing TagIdx index mismatch.
- Fix unstable tag filter matching order in sidx.
- Fix property schema client connection instability after data node restart.
- Fix duplicate TopN query execution in distributed measure queries.
- Fix bydbctl validation for malformed YAML input.
- Fix FODC agent test instability in Basic Metrics Buffering test.