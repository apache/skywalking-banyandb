# Changes by Version

Release Notes.

## 0.12.0

### Features

- Add a native ICE v3 encoder that writes committed index generations compatible with existing readers.

- Add logging related flags to the FODC proxy and agent. Every BanyanDB binary now shares the same logging flags and the matching `BYDB_LOGGING_*` environment variables.
- [Breaking Change] Remove the row-based query execution path from the Stream, Measure and Trace query engines. Measure TopN pre-aggregation is unchanged and still executes row-based. The `--stream-vectorized-enabled`, `--trace-vectorized-enabled` and `--measure-vectorized-enabled` flags stay registered but no longer select an engine; `=false` now fails fast at startup on standalone, data and liaison nodes. A query shape the vectorized engine cannot plan now returns an error instead of falling through to row execution. See [Upgrading to 0.12](docs/operation/upgrade.md#upgrading-to-012).
- Push the criteria tag filter ahead of the vectorized stream merge, so a filtered index-order query bounds its merge at limit+offset. For such a query, where several rows share an ElementID, the criteria is now evaluated first and the element is represented by its first matching row in the requested sort order. Filtered timestamp-order queries are unchanged.
- `measure.v1.QueryRequest.Aggregation` can now target a tag (`tag_name` + `tag_family`) instead of a field: `SUM`/`MIN`/`MAX`/`MEAN` over an `INT` tag, and `COUNT` over any scalar tag, in both standalone and distributed queries. Wire-additive only — existing clients that only ever set `field_name` are unaffected. `COUNT_DISTINCT` is not yet implemented; see the [tag aggregation and time bucketing design](docs/design/0.12.0/tag-aggregation/README.md).
- `measure.v1.QueryRequest.GroupBy.time_bucket` groups a measure query by tumbling time windows, alone or combined with a tag `GroupBy`, in both standalone and distributed queries. The bucket width comes from the request or falls back to the measure's own `interval`. The part-scan path streams bucket-by-bucket so live memory never exceeds one bucket's groups; index-mode measures (no ascending-timestamp guarantee) use a bounded non-streaming fallback instead. A mixed-version cluster where an older data node ignores `time_bucket` is rejected with a clear error rather than silently collapsing the series to one row.

### Bug Fixes

- Reject group / stream / measure / trace (and related) resource names that are not a single path-safe identifier (`[a-zA-Z0-9_]([a-zA-Z0-9._-]*[a-zA-Z0-9])?`), so names cannot escape catalog storage roots.
- Bound protobuf `validate.rules` on schema and query identifiers (max length, allowlist pattern, repeated max_items, numeric ceilings) so untrusted inputs cannot escape storage roots or force unbounded allocations.
- Honor the configured logging level in native observability metrics instead of retaining the pre-initialization debug logger.
- Fix FODC proxy `/metrics` returning partial data or timing out when concurrent scrapes overlap.
- Bound Property, Stream, and Trace query allocations with shared memory admission and capacity hints so oversized limit/offset windows cannot force huge result buffers.
- Preserve list-all / max-limit queries (`limit=MaxUint32` or OAP `Integer.MAX_VALUE`) with incremental scan admission and result-count accounting, without over-charging Property source payloads against the liaison fallback pool.
- Register the memory protector on the liaison role so query admission uses the cgroup-backed pool instead of the 64MiB fallback.
- Bound Cluster and Rover e2e BanyanDB memory so cgroup limits enable the query budget under resource-constrained environments.
- Enforce trace query time ranges independently of the sort index, skipping row timestamp checks when the query fully covers a part.
- Keep system native `memory_state` (with `kind` labels) from being overwritten by the liaison load-shedding gauge so self-observability dashboard queries succeed.
- Retry property schema registry initialization indefinitely, logging an error every 10 attempts.
- Authorize frames on write streams that were opened before RBAC was hot-enabled, so enabling RBAC mid-stream cannot leave long-lived Measure/Stream/Trace writes unchecked.
- Reload auth config when the watched path is a symlink and its target file changes (ConfigMap-style layouts).
- Re-fetch file/DNS discovery nodes that are parked in the retry queue while discovery has not Start()-ed yet, so unbounded PreRun schema-registry retries can recover once peers become reachable without restarting liaison.
- Keep DNS discovery `ListNode` successful when some SRV addresses refuse connections but at least one node was discovered, so non-meta nodes no longer deadlock in PreRun on their own not-yet-listening gRPC address.
- Fix un-interruptible sleep on shutdown during snapshot sync retry and add jittered backoff for stream, measure, and trace.
- Report the real shard ID in `CollectDataInfo` shard info for stream, measure, and trace instead of the live table's slice index, so a node that owns only higher-numbered shards is no longer attributed to shard 0 in cross-node shard-load analysis.
- Fix a silently wrong result from the vectorized measure liaison's aggregation reduce when a single per-node partial exceeds 65,536 rows: past that boundary the consuming row index wrapped, folding early rows into a group a second time and never visiting the rest, with no error. It is reachable today on `Top`-bearing queries, whose per-node limit is unbounded. The reduce now consumes an oversized partial in bounded chunks, and `BatchSize` is capped at `MaxUint16` so it cannot reopen the same wrap from the output side. The liaison's raw `Top` and raw `GroupBy` passes are chunked the same way; their callers already page at `BatchSize`, so that part is hardening rather than a live fix.
- Reject a `--trace-vectorized-batch-size` above 65,536 at startup instead of accepting it. The trace query path indexes a batch's active rows through a `uint16` selection, so a batch size above that bound silently wrapped -- reading early rows a second time and never reading the rest. The stream path already enforced this bound; measure now does too. A node configured above the bound will fail to start rather than return wrong results, and must lower the flag.

### Document

- Add the [native inverted-index replacement design package](docs/design/0.12.0/native-inverted-index/README.md), including the implementation specification, ICE walkthrough, research plan, and visual report.
- Add the [tag aggregation and time bucketing design](docs/design/0.12.0/tag-aggregation/README.md) for the measure query engine, covering aggregation over tags, `COUNT_DISTINCT`, and `GROUP BY` time buckets.
- Add mandatory size and TDD-feasibility audits to the BanyanDB GitHub issue skill.
- Align cluster and node-discovery port documentation and remove leftover etcd references.

### Chores

- Bump google.golang.org/grpc to v1.83.2 to clear GO-2026-6443, GO-2026-6441, and GO-2026-6348.
- Bump canopy and mcp npm dependencies to clear Dependabot CVEs (fast-uri, fastify, qs).
- Bump mcp/canopy npm deps (hono, js-yaml, vitest 5) to clear Dependabot CVEs.

## 0.11.1

### Bug Fixes

- Enforce the Canopy readonly role on the `/monitoring/*` proxy the same way as `/api/*`.

## 0.11.0
