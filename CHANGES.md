# Changes by Version

Release Notes.

## 0.11.0

### Features
- Add two catalogs to the queue batch-write metrics so traffic is comparable on both ends: a per-batch-stream **batch catalog** (`total_batch_started`/`total_batch_finished`/`total_batch_latency`, buckets to ~300s) on `queue_pub`/`queue_sub` and the `lifecycle_migration` mirror, and a per-message **message catalog** (`total_message_started`/`total_message_finished`) on `queue_sub` (the publisher's existing `total_*` already counts per message). All existing `total_*` series are unchanged. Note: pub `total_batch_finished` means the batch stream's final response was received, not that every message was acked — true per-message pub acks are deferred to a follow-up that adds an `accepted_count` field to `cluster.v1.SendResponse`.
- Redesign the queue (`queue_pub`/`queue_sub`) metrics around a uniform model: keep only `total_started`, `total_finished`, `total_latency` (now a histogram) and `total_err`, plus file-sync-only `sent_bytes` (pub) / `received_bytes` (sub). Replace the `topic` label with `operation` (`batch-write`/`file-sync`/`query`/`control`) and `group`, add an `error_type` label on `total_err`, and add remote-endpoint labels (`remote_node`/`remote_role`/`remote_tier`) so the liaison↔data (hot/warm/cold) call graph can be reconstructed against `/cluster/topology` (`remote_node` equals the node `metadata.name`). The sender's identity travels on the wire via new `cluster.v1.SendRequest` (`group`, `sender_node`/`sender_role`/`sender_tier`) and `SyncMetadata` (`sender_*`) fields; pub-side `remote_role`/`remote_tier` are resolved from the connection registry. Pre-marshaled (`[]byte`) payloads on the secondary-index sync path (measure/stream series-index, stream local-index, trace sidx-series) carry their business group explicitly on the bus message (`bus.NewMessageWithNodeAndGroup`), so those `batch-write` metrics are labeled by `group` instead of falling back to empty. [Breaking Change] The previous `queue_*` metric and label names (`*_total_msg_*`, `queue_pub_send_*`, `*_inflight_*`/retry/backoff gauges, `chunked_sync_*`/`chunk_reorder_*`, and the `topic` label) are removed; update any dashboards/alerts accordingly.
- Stamp the lifecycle's tier-migration publisher's identity onto the wire so the receiving data node records a non-empty `remote_node`/`remote_role`/`remote_tier` on its `banyandb_queue_sub_total_finished` series. The lifecycle's `parseGroup` resolves the lifecycle's self identity by matching the lifecycle pod's hostname (POD_NAME via the K8s downward API, falling back to `os.Hostname()` — same precedence as `nativeNodeContext` at `banyand/backup/lifecycle/service.go`) against the data-node registry's `GrpcAddress` (host-portion match with loopback-alias and IP-literal normalization, via `hostMatches` at `banyand/backup/lifecycle/steps.go`) — `Metadata.Name` becomes `remote_node`, `Labels["type"]` becomes `remote_tier` — and calls `SetSelfNode(senderNode, "lifecycle", senderTier)` on the migration publisher. The previous `--grpc-addr` address-match (Pass 1) and `--node-labels` superset-match (Pass 2-3) fallbacks are replaced by this single host-based match because they failed on the production cluster where the data pod's `GrpcAddress` is a headless-service FQDN but the lifecycle's `--grpc-addr` is the loopback. Mirrors the liaison's existing `SetSelfNode(node.NodeID, "liaison", liaisonTier)` call in `pkg/cmdsetup/liaison.go:170-171`; no new CLI flags are introduced.
- Add `banyandb_lifecycle_last_run_timestamp_seconds` and `banyandb_lifecycle_last_run_success` gauges to the lifecycle service for at-a-glance health monitoring. `last_run_timestamp_seconds` records the wall-clock epoch (in seconds) of the most recent migration cycle; `last_run_success` is `1` on a nil error and `0` otherwise. Both are stamped by a `defer` at the end of `action()` so every return path (success, error, recovered panic) updates the pair atomically — dashboards can pin an "is the lifecycle healthy" panel on the `last_run_success` value and a "when did it last run" panel on the timestamp. Nil-gauge safe (no panic with a `BypassRegistry` metrics backend).
- Refactor the lifecycle cycle-level metrics (`banyandb_lifecycle_cycles_total`, `banyandb_lifecycle_last_run_timestamp_seconds`, `banyandb_lifecycle_last_run_success`) to carry labels `remote_node`, `remote_role`, `remote_tier`, `group`. The label form mirrors the per-message `banyandb_lifecycle_migration_*` family emitted by the queue/pub lifecycle publisher, but the two families describe DIFFERENT things: the cycle-level series describe the SENDER (the lifecycle pod's co-located data pod, captured at the cycle's last-seen group) while the per-message pub series describe the DESTINATION (the node each chunk is sent to, resolved from `getNodeInfo`). They share the same label form so dashboard matchers and regexes apply to both, but they are NOT cross-joinable on `(group, node, role, tier)` because the `node` label carries different meaning in each family. The cycle-level Inc is in `recordCycleGroup` (per group processed); the cycle-level gauges are stamped atomically at cycle end in `recordLastRun` (which first Deletes the previous-tuple series to avoid stale labeled series shadowing the new stamp, then Sets the new tuple with the cycle's start time and the success flag). On the empty-cycle path both gauges are stamped with all-empty labels. [Breaking Change] Update any alert/panel that pinned the unlabeled form (e.g. `banyandb_lifecycle_last_run_success == 1`) to use the labeled form (`banyandb_lifecycle_last_run_success{group!=""}`).
- Remove `banyandb_lifecycle_self_identity_resolution_total`. The regression-detection role moves to the now-labeled `banyandb_lifecycle_cycles_total{remote_node!=""}` (an empty `remote_node` series means the registry match failed for every group, the bug the old counter caught), plus the existing receiver-side count of empty `remote_node` on lifecycle `banyandb_queue_sub_total_finished` series. The wire-level `cluster.v1.SendRequest` sender-identity fields are unchanged.
- Vectorized measure query path is now enabled by default. The columnar pipeline replaces per-row protobuf serialization in `NewMIterator`, cutting allocations and ns/op for scan-heavy measure queries; gRPC wire format (`*measurev1.InternalDataPoint`) is byte-identical. Single-node coverage is complete: scan, GroupBy+Agg via `BatchAggregation`, scalar reduce (`Agg` without `GroupBy`), raw `GroupBy` (without `Agg`), implicit projection coverage for GroupBy/Agg fields, `TopN`/`BottomN`, `order_by` (via `logical.ParseOrderBy`, mirroring the row path's `PushDownOrder` rule), queries with hidden criteria tags, and boundary-error parity (nil time range, unknown projection, empty result) all resolve through the vec dispatch with row-path-equivalent semantics (SUM/COUNT/MIN/MAX/MEAN type semantics, first-seen carry-forward of non-key projected tags, canonical validation errors). Validated by a 6h production soak (byte-identical parity, zero divergences) and the per-workload bench gates. Distributed Map-mode partial aggregation (multi-node GroupBy+Agg / TopN), multi-measure (multi-group) requests, and non-vectorized backends continue to flow through the row path pending the distributed vectorized query work. Rollback: pass `--measure-vectorized-enabled=false` on the standalone or data-node command line and restart; the row path resumes immediately.
- Add validation to ensure Measure's ShardingKey contains all Entity tags to guarantee entity locality.
- Organize access logs under a dedicated "accesslog" subdirectory to improve log organization and separation from other application data.
- Collect BanyanDB data on e2e test failure for CI debugging.
- Add log query e2e test.
- Sync lifecycle e2e test from SkyWalking stages test.
- Add `noDuplicates` verification to all e2e expected files to detect duplicate data in query results.
- Add a program-generated trace query integration-test framework under `test/cases/trace/cmd/{generate,capture}`: layered case generation (criteria leaves, AND/OR trees, and feature-pairwise across the traceID-lookup and order-based query modes), env-gated golden capture, and a shared `SeedAll` seeder — mirroring the measure test-case framework.
- Add periodic health check for property schema connection.
- Persist segment end time in per-segment metadata so boundaries don't shift across restarts or config changes.
- Introduce fair fast/slow lane scheduling for trace part merges to prevent short merges from being blocked by long-running merges; expose queue wait time as `total_merge_queue_latency`.
- [Breaking Change] Remove etcd components. The property-based schema registry is now the only supported mode. 
  - All `--etcd-*` CLI flags have been removed. 
  - The `--namespace` CLI flag has been removed (it previously configured the etcd key prefix).
  - The `--node-discovery-mode` flag no longer accepts `etcd` (supported values: `none`, `dns`, `file`). 
  - The `--schema-registry-mode` flag only accepts `property`.
- implement panic diagnostics and FODC crash reporting pipeline.
- Schema consistency (Phase 1): introduce client-observable revision and propagation primitives. All gates are opt-in and zero-valued requests preserve prior behavior.
  - Add `mod_revision` to Group / IndexRule / IndexRuleBinding / TopNAggregation Create and Update responses.
  - Add `delete_time` to all `*ServiceDeleteResponse` messages so clients can observe tombstones.
  - Add `created_at` to Stream / Measure / Trace / Property / IndexRule / IndexRuleBinding / TopNAggregation / Group; preserved across updates.
  - Add `STATUS_SCHEMA_NOT_APPLIED` (10) for writes/queries whose `mod_revision` is ahead of the server cache.
  - Add three-way write-path `ModRevision` gate on Stream and Measure write RPCs (`<` expired, `==` succeed, `>` not-applied, `0` skipped).
  - Add per-group query-path gate via `QueryRequest.group_mod_revisions` and `QueryResponse.group_statuses` for Stream / Measure / Trace queries.
  - Add automatic time-range clamp `max(time_range.start, schema.created_at)`; multi-group uses the maximum across queried groups; nil for pre-upgrade schemas is a no-op.
  - Add `SchemaBarrierService` with `AwaitRevisionApplied`, `AwaitSchemaApplied`, `AwaitSchemaDeleted` (10 000-key cap, timeout-bounded, returns laggards on expiry).
  - Add tombstone retention/GC (default 7 days, configurable via `--schema-server-tombstone-retention`) with a per-cache count cap to bound memory under bulk deletes.
  - Reject `Create` with `updated_at <= tombstone.delete_time` to prevent replayed creates from overwriting newer deletes.
  - Guard `pkg/schema/cache` against out-of-order `EventDelete` events; expose monotonic `LatestModRevision` watermark.
- Schema consistency (Phase 2 in progress): cluster-wide barrier. Internal-only; no client-facing surface impact yet.
  - Add `NodeSchemaStatusService` (`GetMaxRevision`, `GetKeyRevisions`, `GetAbsentKeys`) registered on every cluster member that holds a schema cache, so peer liaisons and data nodes can be probed identically by the barrier fan-out (#1108).
  - Extend `queue.Client` with `NewNodeSchemaStatusClient(node)` so the barrier fan-out can borrow the existing tier1/tier2 connection pools instead of opening a parallel mesh (#1109).
  - `AwaitRevisionApplied` now fans out across the receiving liaison's frozen tier1 (peer-liaison) + tier2 (data-node) Active set, probing each member in parallel via `GetMaxRevision` with shared per-call deadline. Cross-version peers returning `codes.Unimplemented` are treated as ready so partial-upgrade clusters do not deadlock; transient RPC errors count as per-iteration laggards. Empty Active set fails fast with `codes.Unavailable`.
  - Frozen-snapshot mid-call semantics: members that transition `Active → Evictable` during a call are dropped from subsequent probes and surfaced once as a `NodeLaggard{reason="evicted_during_poll"}`; members that disappear from the route table altogether are dropped silently; late joiners are excluded from the watched set until the next call. Adds `reason` field (5) to `NodeLaggard` proto.
  - `AwaitSchemaApplied` and `AwaitSchemaDeleted` follow the same fan-out shape using `GetKeyRevisions` / `GetAbsentKeys` respectively, with per-node calls chunked at 1000 keys and a shared call-wide deadline (no equal-slice division across chunks). Per-node laggards carry the per-member `missing_keys` / `still_present_keys` they observed.
  - First-attempt re-enable of the four Phase-1-deferred distributed specs confirmed all four still flake under the cluster barrier alone (second run reproduced the `group not found` race). Preliminary guards added; later superseded by the successful re-enable below.
  - Add `pkg/schema/registry.NodeRepoRegistry`, the per-node aggregator that routes barrier and node-status RPC lookups to the same per-service `pkg/schema.schemaRepo` instances the data-node executor consults via `LoadGroup` / `LoadResource`. Each banyand service (measure / stream / trace) registers its `schemaRepo` here during PreRun under a kind bitmask covering `KindGroup` + the catalog's primary kind + `KindIndexRule` + `KindIndexRuleBinding`. `metadata.Service` exposes a `NodeRepoRegistry()` accessor and `clientService` constructs a single registry per process.
  - Repoint `NodeSchemaStatusService` at the `NodeRepoRegistry` for executor-tracked kinds: `GetMaxRevision` reads `schemaCache.notifiedModRevision` directly and `GetKeyRevisions` / `GetAbsentKeys` route per-key lookups through the per-service `schemaRepo` aggregator. TopN / Property keys still consult the property `schemaCache`. This closes the `SendMetadataEvent` eventCh-retry leak where the schemaCache watermark advanced before `schemaRepo.groupMap` applied the event, so the cluster barrier can no longer certify a key the executor is about to miss.
  - Write gate (`validateWriteRequest` for measure / stream / trace) and per-group query gate (`checkQueryGate`) read `cacheRev` through the `NodeRepoRegistry` rather than the liaison `entityRepo` locator. The locator still answers existence checks (`STATUS_NOT_FOUND` signal) and downstream navigation; only the revision scalar moves. Net contract: if `AwaitRevisionApplied(R)` on a node returns `applied=true`, the write gate, query gate, and downstream executor on that same node all see ≥ R for any key included in R.
  - Re-enable  /  /  /  in distributed mode (final pass). Distributed schema integration suite reports `Ran 28 of 28 Specs, 0 Skipped`.
  - Add observability for the schema-consistency cluster (Step 2.7, ): `schema_await_revision_applied_duration_seconds{result}`, `schema_await_schema_applied_duration_seconds{result}`, `schema_await_schema_deleted_duration_seconds{result}` track barrier latency by outcome (`applied` / `timeout` / `invalid_argument` / `error`); `schema_barrier_laggard_nodes_total{barrier,role,node}` decodes the `<role>-<Metadata.Name>` laggard identifier so dashboards can break out which member fell behind on which call. Two status counters — `schema_status_schema_not_applied_total{rpc,group,reason}` and `schema_status_expired_schema_total{rpc,group}` — cover the gate's user-visible verdict across the six write/query gate entry points; the `reason` label is fixed at `"wait_timeout"` in v0.11.0 and retained for forward-compat with optional fast-sync paths. Each Await* call also emits a structured access-log line with `min_revision` (or `keys` for the schema variants), `laggards`, `duration`, and `result`.
  - Add the schema-barrier CP-6 SLO load harness (Step 2.8) under `test/load/schema_barrier/`, runnable via `make load-test-barrier`. The harness brings up an in-process 3 data node + 1 liaison cluster, drives 100 concurrent `AwaitRevisionApplied` callers + 10 `Group.Update` ops/sec, and reports p50 / p95 / p99 / max from client-side per-call duration after a 1-minute warm-up + 5-minute measurement window. Client-side latency is bounded above by the server-side histogram so the SLO check on the client number is stricter than the CP-6 "p99 < 200ms" criterion. Override the profile via `LOAD_FLAGS` for developer smoke runs.
  - Land `pkg/test/setup.PauseDataNodeWatch` / `ResumeDataNodeWatch` (Step 1.0 follow-up): the helpers replace the `ErrWatchControlNotImplemented` stub with a working hook into `property.SchemaRegistry` so cluster-only specs can drive a single data node to fall behind the cluster while the rest stays in sync. The data node's `handleWatchEvent`, `processInitialResourceFromProperty`, and `handleDeletion` paths each gate events into a per-registry queue while paused; resume drains the queue in arrival order. This unblocks /b/c/d spec authoring without touching CP-5 / CP-6 acceptance.
  - Extend the watch-control binding to liaison processes (`pkg/test/setup.startLiaisonNode`) and add `helpers.SharedContext.LiaisonAddr` so cluster-only specs can pause the receiving liaison's own `SchemaRegistry`. The cluster barrier's `selfName` probe reads through that SR, so pausing it surfaces a laggard via the public `AwaitX` RPCs.
  - Author  cluster-barrier integration specs (`test/cases/schema/barrier_cluster.go`): all four specs now pass end-to-end.  (`AwaitSchemaApplied`) and  (`AwaitSchemaDeleted`) pin the per-key contract that a paused receiving liaison surfaces a non-empty `laggards` list and that resume drains the queue so the barrier converges.  (`AwaitRevisionApplied`) and  (cross-barrier recovery) pin the global watermark contract: the barrier's `GetMaxModRevision` advances past the target revision after resume. Distributed schema integration suite reports `32 Passed | 0 Failed | 0 Pending | 0 Skipped`.
  - Expose `cluster.v1.NodeSchemaStatusService` on data-node gRPC ports. Decouple the registration in `banyand/queue/sub/server.go`'s `Serve()` so `fodc.v1.GroupLifecycleService` (liaison-only by design) and `NodeSchemaStatusService` (per-node by design) are gated independently: the new `queue.Server.SetNodeSchemaStatusRepo(metadata.Service)` setter wires the per-node service without dragging along the liaison-shaped `GroupLifecycleService`. Liaison startup (`pkg/cmdsetup/liaison.go`) calls both `SetMetadataRepo` and `SetNodeSchemaStatusRepo`; data-node startup (`pkg/cmdsetup/data.go`) calls only the latter. Closes the gap that previously made paused data nodes invisible to the cluster barrier — the cross-version `Unimplemented`→ready fallback in `barrier_cluster.go` is now reserved for true cross-version (Phase-1) peers.
  - Repair the `GetMaxRevision` aggregation on the per-node `NodeSchemaStatusService` (`banyand/metadata/schema/property/node_status.go`). The previous implementation returned `min(schemaCache.notifiedModRevision, NodeRepoRegistry.LatestModRevision)`, but `LatestModRevision` aggregated per-service `schemaRepo` watermarks via `min` — and each `schemaRepo` only advances on events for its own catalog (`pkg/schema/init.go:72` filters by `g.Catalog`), so the min was perpetually pinned to the slowest catalog's watermark and a steady measure-only stream gated the barrier on the trace `schemaRepo` (or vice-versa). `GetMaxRevision` now reads the cache only — symmetric with the receiving liaison's `selfName` probe at `barrier_cluster.go:354-360` — and the misleading `LatestModRevision` aggregate is removed from `NodeRepoRegistry` and the `RevisionRepository` interface. Per-key gating (`GetKeyRevisions` / `GetAbsentKeys`) still routes through the registry by kind, preserving 's executor-cache convergence contract on the receiving liaison's write/query gates.
  - Fix the `notifiedModRevision` watermark advancement in `SchemaRegistry.processInitialResourceFromProperty`, `handleWatchEvent` (DELETE branch), and `handleDeletion`. Previously `AdvanceNotified` was gated on `cache.Update` / `cache.Delete` returning true, but those methods compare `latestUpdateAt` (property timestamp) while the watermark tracks `modRevision` (etcd revision). When the property timestamp is stale (e.g. a no-op Update that doesn't change the measure spec), the cache rejects the entry and the watermark cannot advance, causing `AwaitRevisionApplied(R)` to block forever even though the event has been fully processed. `AdvanceNotified` now fires unconditionally whenever an event reaches the processing stage, regardless of cache mutation outcome.
  - Fix the `modRevision` contract on no-op Update RPCs (`MeasureRegistryService.Update`, etc.). Previously `updateResource` detected unchanged content via `CheckerMap` and short-circuited without writing to the property store, but the caller had already fabricated `modRevision = time.Now().UnixNano()` and returned it. The returned revision never appeared in the property watch stream, so `AwaitRevisionApplied(R)` would hang. `updateResource` now returns `(int64, error)` — the existing property's `modRevision` for no-op updates, the new revision for real updates — so callers always return a revision the barrier can observe.
  - Add end-to-end observability for liaison internal queue pipelines with per-topic metrics for queue_sub and queue_pub, along with Grafana panels and troubleshooting docs.
  - Introduce the migration tool with `copy`, `verify`, and `analyze` subcommands for both measure and stream data.
- Support displaying a measure's indexed tags in the dump tool, resolved per part so peak memory is bounded by the part rather than a segment-wide series map.
- Snapshot/backup and data inspection no longer reopen idle-closed segments, avoiding cold-segment nil-index panics and index lock-file churn.
- Add opt-in vectorized measure query tracing over raw-frame distributed queries, including a trace envelope and fixed trace-label vocabulary.
- Enhance segment lifecycle: `refCount` now counts only active users, decoupled from "open" (`index != nil`), adding a "dormant" state (open, `refCount == 0`). A `DecRef` to zero no longer closes a segment; idle reclaim and retention delete act only at `refCount == 0`, so an in-flight snapshot/inspect is no longer torn down mid-operation (fixing the cold-node nil-index panic and bluge lock churn) while idle segments still release their bluge writers.
- Speed up GCS backup uploads: write each object and its checksum metadata in one request, dropping the per-object `Update` round-trip.
- Lifecycle migration now archives rows whose measure/stream schema was deleted from the registry, instead of aborting the group.
- Canopy M3 metadata CRUD: add IndexRule + IndexRuleBinding create/edit/delete forms with admin-only edit/delete, pure `validate*()` module covering 8 input groups (group, stream, measure, trace, index rule, index rule binding, interval, lifecycle stages), `useFocusTrap` + `useDirtyGuard` modal hooks, live `m3-indexrule` e2e (CRUD round-trip + MF2 server-authority negative), and opt-in `m3-handoff` e2e + zero-dep static server for the design-bundle comparison.
- Throttle lifecycle migration under receiver memory pressure so it no longer OOM-kills the destination data node, via three always-on mechanisms: per-chunk `SYNC_STATUS_SERVER_BUSY` load-shed (the sender backs off and retries); a bounded memory wait before introducing an external series-index segment (`{measure,stream,trace}-lifecycle-receive-mem-wait-timeout`, default 5m); and sender-side bounded-backoff retry of transient send failures — target node restarting/`no nodes available`, disconnect, or `SERVER_BUSY` — across both the file-chunk and row-replay node-pick paths (`lifecycle-send-retry-timeout`, default 15m). A part that exhausts its budget is recorded incomplete and resumed next cycle.
- Cut the lifecycle external series-index deduplicator's memory from ~1.8 GB to near-zero per segment by bumping bluge to a memory-efficient implementation: duplicates are now resolved via `_id` term-dictionary point lookups (`Dictionary.Contains`) instead of decompressing and retaining every segment's stored fields, removing the dominant heap source during external-segment introduction. No public API change and identical dedup semantics (verified by a differential test); this is the root-cause fix that the throttling above backstops.
- Add the storage-node in-merge trace retention filter (`PIPELINE_EVENT_MERGE`): a per-group sampler chain evaluates traces at merge time and drops non-retained traces from both the core part and the coupled sidx (`sidx.Merge` gains a keep predicate; a nil predicate restores the wholesale merge). Chain execution fails open on panic/error/length-mismatch/timeout, with a consecutive-timeout circuit breaker. Samplers are native Go plugins (`.so`) loaded at startup from `--trace-pipeline-config` (a `TracePipelineConfig` in protojson) via `loadSamplerPlugin` (trusted-dir escape guard + dual ABI check + recover-wrapped open/ctor); this is the static bootstrap ingress — the dynamic `TracePipelineRegistryService` RPC remains a follow-up. The filter stages traces in bounded chunks — a trace-boundary flush fires once staged bytes cross a budget derived from the memory protector's limit (`memLimit/(4*CPUs)`, so the aggregate across concurrent merges stays ~`memLimit/4`, clamped to a 16MB floor and the fast/slow lane threshold) — so peak staged heap stays bounded regardless of merge size instead of holding the whole merge in memory. No operator flag; the budget self-tunes from the limit and core count. Verified end-to-end by a reusable, cross-topology integration test (`test/cases/tracepipeline` + out-of-process standalone/distributed runners that build a CGO server + real `.so` and exercise the `plugin.Open` path; opt-in soak via `TRACE_PIPELINE_SOAK`).
- Register/update/remove in-merge trace retention samplers at runtime without restarting the data node, replacing the static `--trace-pipeline-config` boot loader (now removed). The sampler pipeline config is carried on `common.v1.Group` (new optional `pipeline` field, with `TracePipelineConfig`/`SamplerPlugin` relocated from `pipeline/v1` into the leaf `common/v1` to avoid a `common`→`pipeline` import cycle) and delivered through the existing `KindGroup` schema watch; the data node rebuilds its group-keyed sampler registry on `OnAddOrUpdate(KindGroup)`, fail-open and gated to `ROLE_DATA` + native pipeline. The plugin cache is keyed by `(path, symbol, configHash)` with an ABI re-check on every miss; samplers are never `Close`d (plugins are immortal). Per-group `merge_grace` is honored, validation rejects stage-only configs, and the registry exposes `sampler_{active_count,register,update,remove,load_failed}` metrics. Verified by the cross-topology `tracepipeline` integration suites plus a dynamic register/update/remove + mid-run panic-injection soak (`TRACE_PIPELINE_SOAK`).
- Add FODC memory-pressure pprof capture: agent grabs heap/goroutine profiles when RSS nears the cgroup limit, served via the proxy.
- Add a Claude/Codex plugin packaging the BanyanDB MCP server with a parse-only `validate_bydbql` tool (backed by the `mcp/tools/bydbql-parse` Go validator) and a BydbQL skill for read-only natural-language-to-BydbQL generation over STREAM/MEASURE/TRACE/PROPERTY resources.

### Bug Fixes

- FODC: expose node labels reliably and without collisions. The proxy overlaid each agent's node labels onto every metric under their raw keys, so a data node's `type=hot/warm/cold` tier label overwrote the merge metrics' own intrinsic `type="file"/"mem"` dimension on any node whose agent had resolved its labels. Two fixes: (1) the agent now stamps its node labels **live** from the cluster collector (which keeps resolving after startup) under a `node_` prefix per metric — so the tier reliably appears as `node_type` even when the local node's role had not resolved at agent startup, instead of being frozen at a one-shot startup snapshot; and (2) the proxy applies the same `node_` namespacing to any node labels it overlays. Node labels can therefore never clobber a metric-intrinsic label of the same name (the tier is `node_type`, the part type stays `type`). Two collector reliability fixes back this: (a) the agent now re-polls its current node (role/labels) on every cluster-state interval instead of only once at startup — previously a node that had not yet assumed its role within the startup budget (e.g. a slow data node) stayed `ROLE_UNSPECIFIED` with no labels for the life of the process; and (b) it dials its local node over the IPv4 loopback (`127.0.0.1`) instead of `localhost`, which could resolve to the IPv6 loopback (`::1`) and fail with "cannot assign requested address" in pods without an IPv6 loopback. Separately, the proxy↔agent gRPC message-size limit is raised from the 4MB default to 32MB (`--grpc-max-msg-size` default and the agent client call options): a large data node's metrics push can exceed 4MB, which the proxy rejected with `ResourceExhausted`, breaking that node's metrics stream into an endless reconnect loop so it was never collected. Finally, the agent defers recording metrics until its node role resolves (bounded by a grace period): otherwise it would buffer metrics under an unresolved identity at startup that, once the role resolved, lingered as duplicate "ghost" series (the same series with `node_role="ROLE_UNSPECIFIED"` and no `node_type`). The resolved identity is also sticky: once the role resolves, a later regression to `ROLE_UNSPECIFIED` from the live provider is ignored, so a transient regression cannot create a ghost ring-buffer that never evicts.
- Close BanyanDB merge write-path durability gap that allowed torn parts to be created by a crash between data write and metadata commit. Metadata files (`metadata.json` for trace/measure/stream, `manifest.json` for sidx, plus `traceID.filter` and `tag.type`) now go through a new `WriteAtomic` (write-tmp + fsync + rename + fsync-dir) sequence; data writers (`seqWriter.Close`, `localFileSystem.Write`) now propagate fdatasync errors instead of silently dropping them. `mustOpenFilePart` / `mustOpenPart` in each engine cleans up safe post-rename `.tmp` leftovers on open. (#13862, root cause for #13861)
- Fix bydbctl command tests using global stdout capture, which caused race-enabled runs to corrupt captured command output.
- Use `topic` instead of `session_id` as the Prometheus label on liaison `queue_sub` chunk-ordering counters to avoid unbounded metric cardinality.
- Fix flaky trace query filtering caused by non-deterministic sidx tag ordering and add consistency checks for integration query cases.
- Fix index-mode measure queries returning documents outside the requested time range when a widened segment overlaps the query window.
- MCP: Add validation for properties and harden the mcp server.
- Fix property schema client connection not stable after data node restarted.
- Fix flaky on-disk integration tests caused by Ginkgo v2 random container shuffling closing gRPC connections prematurely.
- Fix snapshot error when there is no data in a segment.
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
- Fix stale sync request blocking watch session channel, causing repeated "channel full, skipping session" errors when a watch stream is in backoff.
- Fix nil pointer panic in disk monitor when group schema is not yet initialized during early startup, and ensure monitor loop survives recovered panics.
- Fix `FileSystemError` not satisfying `errors.Is(err, io/fs.ErrNotExist)`, which prevented the segment controller from cleaning up half-born segment directories and left groups in a permanent zombie state after a crash or partial sync.
- Fix lifecycle migration panic when a stream shard's snapshot has no element index (`idx/`) directory.
- Avoid FODC lifecycle inspection failing on busy data nodes by raising the per-broadcast `CollectDataInfo` / `CollectLiaisonInfo` deadline from 5s to 30s and parallelizing per-group inspection in the cluster-internal `InspectAll`.
- Fix flaky `file_snapshot` subtest in measure/stream/trace by waiting until every introduced mem part has been flushed to disk, instead of only checking the latest snapshot creator.
- Fix deadlock when fodc-agent reconnects to fodc-proxy after a pod rotation.
- Fix flaky `TestCollectWithPartialClosedSegments` by raising `SegmentIdleTimeout` so wall-clock variance on slow CI does not mark still-open segments as idle.
- Fix FODC lifecycle cache poisoning where transient `InspectAll` failures were cached for 10 minutes and masked liaison recovery; raise FODC agent and proxy timeouts from 10s to 40s.
- Fix FODC `/cluster/lifecycle` dropping zero-valued group fields (e.g. `replicas=0`, `close=false`) under `encoding/json` + `omitempty`; switch to `protojson` so all fields are emitted (nil nested messages serialize as `null`).
- Fix trace `block_writer` panic on out-of-order timestamps within the same traceID, which dropped one trace-write batch per panic in multi-agent SkyWalking deployments. Spans of a single trace originate from independently-clocked services, and trace storage is organized by traceID rather than timestamp, so per-traceID timestamp monotonicity is not a writer invariant.
- Fix nil-pointer panic on cold-tier data nodes when FODC `InspectAll` raced with idle-segment cleanup.
- Add `GroupLifecycleInfo.errors` to surface per-group collection failures from FODC `InspectAll` instead of silently dropping the affected node entry.
- Fix `CollectDataInfo` and `CollectLiaisonInfo` not handling `CATALOG_PROPERTY` groups.
- Fix lifecycle migration where the receiving node could create segments shorter than the configured `SegmentInterval`.
- Fail fast on incompatible storage version at boot. Previously the server would start in a degraded `SERVING` state with affected groups un-loaded because the property schema-registry retry loop swallowed the version-incompatibility panic. Compatible versions are listed in `banyand/internal/storage/versions.yml`.
- Release bluge index writers on segment rotation so `analysisWorker` pools sized from `GOMAXPROCS` don't accumulate across rotations. Two layered defects kept the existing idle-segment reclaim path from running: `segmentIdleTimeout` defaulted to `0` (which disabled the 10-minute reclaim ticker), and `incRef` refreshed `lastAccessed` on every rotation tick so `closeIdleSegments` never observed an idle segment. Defaults to `time.Hour`, moves the `lastAccessed` bump to real read/write call sites, and rewrites `closeIdleSegments` to take its own CAS-bumped snapshot so a concurrent reopen cannot have its only ref dropped under the reclaimer (apache/skywalking#13874).
- Fix incorrect counts and missing trace fields in the lifecycle migration report.
- Fix lifecycle migration placing data in the wrong target segment when the source segment interval is not a multiple of the target stage's interval, by row-level replaying parts that straddle a target-segment boundary instead of chunk-copying them into a single segment.
- Fix trace query identity-tag projection: when `trace_id`/`span_id` are explicitly projected, reconstruct them from span identity at response build time instead of requesting them as stored tags, and preserve tag order with null-filled per-span value alignment in the distributed trace result iterator.
- Fix measure, stream, and trace queries returning data from segments already expired by the TTL. Retention removes a segment only on its next scheduled run, so a fully expired segment can linger on disk and keep serving TTL-expired data; queries now skip segments whose whole time range is past the retention deadline, matching retention's own removal condition.
- Trace storage metrics now expose the `storage` sub-scope, matching the `stream_storage_*` naming. The `StorageMetricsFactory` for trace switched from the root `trace` scope to `trace.storage`, so per-segment inverted-index metrics (`inverted_index_total_updates`, `inverted_index_total_doc_count`, `inverted_index_total_term_searchers_started`) are now emitted as `banyandb_trace_storage_*` instead of `banyandb_trace_*`, aligning the dashboard query names. Other trace metrics (`trace_tst_*`, `trace_scheduler_*`) are unchanged.
- Fix flaky measure snapshot tests that gated on the part directory appearing in `tab/` as the flush-completion signal. That directory is created by the first line of `memPart.mustFlush`, before the mem→file introduction reaches the in-memory snapshot and before the `.snp` manifest is persisted, so under `-race`/CI load `TakeFileSnapshot` could observe only mem parts and `Close` could drop the in-flight flush; gate on the persisted `.snp` manifest instead.
- Fix FODC proxy corrupting Prometheus metric types. The agent dropped the `# TYPE` line while parsing banyandb `/metrics`, the `StreamMetrics` proto carried no type field, and the proxy guessed the type from a name-suffix heuristic — downgrading counters to gauge, mislabeling `_count`-suffixed counters as histograms, and splitting summaries into two conflicting `# TYPE` lines. Capture the type with the Prometheus `expfmt` parser, store it in the flight recorder, thread it through a new `Metric.type` enum over gRPC, and emit the real type from the proxy; pre-upgrade (untyped) samples fold into the matching typed family so a mixed-version rollout never emits two conflicting `# TYPE` lines for one metric.
- Fix FODC agent labeling metrics with `node_role="ROLE_UNSPECIFIED"`. The agent resolved the node role exactly once at startup via a single `GetCurrentNode` poll whose endpoint retries spanned only ~1s; when the sibling lifecycle/banyandb gRPC server was not yet listening (`connect: cannot assign requested address`) the role fell back to `ROLE_UNSPECIFIED` permanently, so most nodes never reported their real `ROLE_DATA`/`ROLE_LIAISON`. Retry the initial node-role resolution with exponential backoff until a non-unspecified role is obtained or a 25s budget elapses.
- Fix lifecycle row-replay OOM on large measure parts by streaming the dump reader, pooling size-classed marshal buffers, and bounding in-flight batch bytes (default 32 MiB); peak heap drops ~1.5 GB→~296 MB.
- Consolidate lifecycle migration report errors into a single flat list of structured, stage-aware entries.
- Fix backup container OOM from overlapping scheduled runs; serialize runs and upload small snapshot files concurrently.
- Fix block metadata reset before unmarshal for stream and trace.
- Lifecycle only handles stream/measure/trace snapshots, skipping other catalogs.

### Document

- Add a code-accurate, API-first "Storage & File Format" doc and correct stale storage/format descriptions: fix the on-disk hierarchy to `group → segment → shard → part` (in `tsdb.md`, `data-model.md`, `clustering.md`, `disk-management.md`, including the `dump` CLI path examples), correct the measure field-values file name (`fv.bin`, not `fields.bin`), clarify that the `GORILLA`/`ZSTD` enums are schema hints (the engine uses delta/dictionary + size-thresholded zstd), document the measure `index_mode` two-engine split and the trace span-store/sidx layout, and fix the property repair Merkle-tree SHA/snapshot-state descriptions. Replace the file-structure diagrams with inline mermaid.
- Refresh observability for FODC-proxy scraping: add two dashboards built for the deployment where Prometheus scrapes the FODC proxy `/metrics` as the single target, split by aggregation dimension — `grafana-fodc-nodes.json` for node/pod-level health and resources aggregated by `pod_name` (fleet overview, per-node health table, a "Topology: Pod-to-Pod Flows" table joining the publisher's and subscriber's views of each directed `source → target` flow into one row, resources, disk-by-path, Go runtime) and `grafana-fodc-workload.json` for business/data-level throughput and latency aggregated by `group` (cluster workload summary, liaison ingestion/query/publish + write-queue backlog, data storage/inverted-index, and the internal queue with per-operation throughput & p99 by group) — and remove the stale direct-scrape `grafana-cluster.json`. Audit and validate the observability prose/PromQL against a live cluster, replace the out-of-date self-observability-write image with an inline mermaid write-flow diagram, add a "Key Signals to Watch" section, and split the oversized `observability.md` into an `observability/` folder (`overview`, `logging`, `metrics`, `providers`, `profiling`, `tracing`). Reconcile the FODC overview's HTTP API surface (`/metrics`, `/metrics-windows`, `/cluster/topology`, `/cluster/lifecycle`, `/diagnostics`) with the proxy code and `apis.md`.
- Add a "Cluster Topology Rendering" doc (`docs/operation/fodc/topology.md`) that joins the FODC `/cluster/topology` node inventory with cluster signals to render a directed topology with two edge layers — the **request pipeline** (liaison→data, weighted from `banyandb_queue_pub_*` metrics) and the **lifecycle tier migration** (hot→warm→cold; path from the lifecycle service's entries in `/cluster/topology` `calls`, weight from the `banyandb_lifecycle_migration_*` family). A data node and its lifecycle sidecar render inside one pod boundary with the containers labeled `data` / `lifecycle`. Includes the per-edge PromQL, three captured sample inputs (topology, queue metrics, and the migration metrics taken while the daily run was in flight), and a stdlib-only join script (`render_topology.py`) that emits Graphviz DOT and Mermaid; cross-linked from the FODC overview and the metrics queue reference.

### Chores

- Upgrade Go and npm dependencies including etcd to v3.6.10, OpenTelemetry to v1.43.0, AWS SDK, and Google Cloud libraries.
- Regenerate expired TLS test certificate with 100-year validity.
- Set Ginkgo `--repeat` to 0 in the flaky-test workflow so the hourly run completes within the 50-minute timeout.
- Refactor the dump tool into a reusable `banyand/dump` parser library.
- Strip macOS AppleDouble (`._*`) and `__MACOSX/` metadata from every release tarball (src, banyand, bydbctl, fodc-agent, fodc-proxy) so downstream users running `make generate` from a downloaded source tarball no longer hit "invalid control character" errors when `buf generate` walks the resource-fork files; export `COPYFILE_DISABLE=1` and filter `._*` files at the source.

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
- Fix entity tag handling in trace filter to prevent TagIdx index mismatch when filtering with both entity and non-entity tags.
- Fix OOM issue cause during migration when a group contains a large amount of data.

### Document

- Add read write benchmark document for 0.9.0 release.
- Add design of KTM.
- Add FODC overview doc.
- Remove Java client doc, and recreate client APIs docs.
- Add common issue documentation.

### Chores

- Upgrade Node.js support from 20.12 to 24.6.0, and align CI, license checks, and documentation
- Add Claude Code skill for vendor dependency updates.
- Upgrade Go vendor dependencies and sync BPF2GO_VERSION with cilium/ebpf library.

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
- Enhance access log functionality with sampling option.
- Implement a resilient publisher with circuit breaker and retry logic with exponential backoff.
- Optimize gRPC message size limits: increase server max receive message size to 16MB and client max receive message size to 32MB for better handling of large time-series data blocks.
- Add query access log support for stream, measure, trace, and property services to capture and log all query requests for monitoring and debugging purposes.
- Implement comprehensive version compatibility checking for both regular data transmission and chunked sync operations, ensuring proper API version and file format version validation with detailed error reporting and graceful handling of version mismatches.
- **Breaking Change**: Rename disk usage configuration flags and implement forced retention cleanup:
  - `*-max-disk-usage-percent` → `*-retention-high-watermark` (measure, stream, trace, property services)
  - Add new retention configuration flags: `*-retention-low-watermark`, `*-retention-check-interval`, `*-retention-cooldown`
  - Implement disk monitor with forced retention cleanup for data/standalone servers
  - Add comprehensive disk management documentation with configuration guides and troubleshooting
- Implement cluster mode for trace.
- Implement Trace views.
- Use Fetch request to instead of axios request and remove axios.
- Implement Trace Tree for debug mode.
- Implement bydbQL.
- UI: Implement the Query Page for BydbQL.
- Refactor router for better usability.
- Implement the handoff queue for Trace.
- Add dump command-line tool to parse and display trace part data with support for CSV export and human-readable timestamp formatting.
- Implement backoff retry mechanism for sending queue failures.
- Implement memory load shedding and dynamic gRPC buffer sizing for liaison server to prevent OOM errors under high-throughput write traffic.
- Add stream dump command to parse and display stream shard data with support for CSV export, filtering, and projection.

### Bug Fixes

- Fix the deadlock issue when loading a closed segment.
- Fix the issue that the etcd watcher gets the historical node registration events.
- Fix the crash when collecting the metrics from a closed segment.
- Fix topN parsing panic when the criteria is set.
- Remove the indexed_only field in TagSpec.
- Fix returning empty result when using IN operatior on the array type tags.
- Fix memory leaks and OOM issues in streaming processing by implementing deduplication logic in priority queues and improving sliding window memory management.
- Fix etcd prefix matching any key that starts with this prefix.
- Fix the sorting timestamps issue of the measure model when there are more than one segment.
- Fix comparison issues in TopN test cases

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
- Fix data race between flusher/merger/syncer senders and the introducer loop during shutdown, caught by the Go race detector under CI pressure.
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
