# Metrics

BanyanDB exposes metrics for monitoring and analysis.

> **Scrape source — the FODC proxy.** In a cluster deployment, metrics are collected by scraping the **FODC proxy** `/metrics` endpoint (see [FODC overview](../fodc/overview.md)), which is the **single Prometheus scrape target**. The proxy aggregates every BanyanDB node's metrics and adds per-node identity labels, so all the PromQL below is written for that scheme:
>
> - `$job` — the Prometheus scrape job for the FODC proxy.
> - `$role` — the node role, matched via the **`container_name`** label (`liaison` or `data`).
> - `$pod` — a BanyanDB node, matched via the **`pod_name`** label (the full node identity, e.g. `banyandb-data-hot-0`).
>
> Because the proxy is the only target, the Prometheus-synthesized `instance`, `job`, and `up` labels describe the **proxy**, not individual BanyanDB nodes — use `$role` / `$pod` to scope a query to a node. Original BanyanDB labels (`group`, `kind`, `method`, `service`, `path`, `operation`, `remote_node`, …) are preserved on every sample. `$__rate_interval` is the Grafana rate-interval variable. Every expression below carries the `{job=~"$job", container_name=~"$role", pod_name=~"$pod"}` selector; a few liaison-only metrics (the write queue and its sync loop) are pinned with the literal `container_name="liaison"` instead.
>
> (If you are *not* running the FODC proxy, BanyanDB also exposes its own metrics on port `2121`; scrape each pod directly and substitute the Kubernetes `pod`/`instance` target labels for `$pod` below.)

## Dashboards

The metrics are presented through **two core Grafana dashboards**, split by aggregation dimension (see [Metrics Providers](providers.md)):

- **[BanyanDB Cluster — Nodes](../grafana-fodc-nodes.json)** — node/pod-level health and resources, aggregated by **`pod_name`**: *Fleet Overview*, *Per-node Health*, *Resources*, *Disk by Path*, and *Go Runtime*.
- **[BanyanDB Cluster — Workload](../grafana-fodc-workload.json)** — business/data-level throughput and latency, aggregated by **`group`**: *Cluster Workload Summary*, *Liaison: Ingestion, Query & Publish*, *Data: Storage*, *Data: Inverted Index*, and *Data: Internal Queue*.

When trace sampling plugins are enabled, additionally install **[BanyanDB — Trace Sampling Plugins](../grafana-fodc-trace-plugin.json)**. It separates optional plugin execution, sampling outcomes, drop-set capacity, and finalization panels from the core workload dashboard.

The sections below mirror the two core dashboards row-for-row; the [Trace Plugin Metrics](#trace-plugin-metrics-reference) section is the complete reference behind the optional dashboard. A standalone [Internal queue metrics reference](#internal-queue-metrics-reference-queue_sub--queue_pub) at the end documents the `queue_sub` / `queue_pub` model in depth.

---

# Nodes dashboard

Node and process health, aggregated per `pod_name`. Use this dashboard to find an overloaded, low-on-disk, or restarting node.

## Fleet Overview

### Reporting Nodes

The number of BanyanDB nodes currently reporting through the FODC proxy. (The Prometheus `up` metric reflects the proxy target, not individual nodes, so node liveness is derived from the per-node `banyandb_system_up_time` gauge instead.)

**Expression**: `count(banyandb_system_up_time{job=~"$job", container_name=~"$role", pod_name=~"$pod"})`

### Nodes by Role

The reporting-node count broken down by role (`liaison` / `data`), via the `container_name` label.

**Expression**: `count(banyandb_system_up_time{job=~"$job", container_name=~"$role", pod_name=~"$pod"}) by (container_name)`

### Discovered Nodes

`banyandb_metadata_dns_discovery_total_nodes_count` (`--node-discovery-mode=dns`) and `banyandb_metadata_file_discovery_total_nodes_count` (`--node-discovery-mode=file`) report the number of nodes in the discovery cache of each liaison, data or lifecycle process running that mode: the nodes whose metadata was fetched successfully. In DNS mode only the nodes resolved from the configured `--node-discovery-dns-srv-addresses` are counted, so the value is what that process's SRV addresses cover, not necessarily the whole cluster. The count falls when DNS stops listing a node, but a dead node that DNS still lists stays counted. Neither metric exists with `--node-discovery-mode=none`, the default and the only mode for a standalone node.

### Total CPU Cores

The total number of CPU cores available to the selected nodes: the sum of each node's `banyandb_system_cpu_limit_cores`, which comes from `GOMAXPROCS`. It follows the container's cgroup CPU quota, but setting the `GOMAXPROCS` environment variable overrides the quota-derived value.

**Expression**: `sum(banyandb_system_cpu_limit_cores{job=~"$job", container_name=~"$role", pod_name=~"$pod"})`

### Total Memory Used

The total resident memory used across the selected nodes, in bytes.

**Expression**: `sum(process_resident_memory_bytes{job=~"$job", container_name=~"$role", pod_name=~"$pod"})`

### Total Disk Used

The total disk space used across the selected nodes, in bytes (summed over all storage paths). See **Resources → Disk Usage %** for the used/total percentage and **Disk by Path** for the per-path breakdown.

**Expression**: `sum(banyandb_system_disk{job=~"$job", container_name=~"$role", pod_name=~"$pod", kind="used"})`

### Node Uptime

The per-node uptime gauge, in seconds since the process started: it is monotonically increasing and starts again from 0 on a restart, so a value dropping to ~0 indicates a restart. When a node freezes but its FODC agent keeps running, the proxy keeps returning the last reported value, so the series stays and its value stops changing. Once the agent has been unreachable for about 5 minutes (`--agent-cleanup-timeout`), the proxy drops its series, so a series disappearing indicates a node that stopped reporting.

**Expression**: `banyandb_system_up_time{job=~"$job", container_name=~"$role", pod_name=~"$pod"}` (one series per `pod_name`)

## Per-node Health

A single table joining the key per-node signals so an unhealthy node stands out at a glance. The columns are the metrics documented in **Resources** below, each reduced `by (pod_name)`:

- Uptime — `banyandb_system_up_time{…}`
- CPU (cores) — `sum(rate(process_cpu_seconds_total{…}[$__rate_interval])) by (pod_name)`
- RSS — `sum(process_resident_memory_bytes{…}) by (pod_name)`
- Memory % — `max(banyandb_system_memory_state{…, kind="used_percent"}) by (pod_name)`
- Disk % — `max by (pod_name) (banyandb_system_disk{…, kind="used_percent"})`

## Resources

`Resources` metrics monitor per-node resource usage.

### CPU Usage

CPU consumed per node as a fraction of its CPU limit (`banyandb_system_cpu_limit_cores`, the GOMAXPROCS value), so it can exceed 100% when the CPU quota is fractional and under-reports when the quota is below one core. Because the limit is read from `GOMAXPROCS`, setting the `GOMAXPROCS` environment variable overrides the cgroup quota. Sustained high utilization correlates with rising query/merge latency.

**Expression**: `sum(rate(process_cpu_seconds_total{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (pod_name) / max by (pod_name) (banyandb_system_cpu_limit_cores{job=~"$job", container_name=~"$role", pod_name=~"$pod"})`

### RSS Memory

Resident set size per node, in bytes. The peak over the rate interval is used so transient spikes are not hidden.

**Expression**: `max by (pod_name) (max_over_time(process_resident_memory_bytes{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval]))`

### System Memory %

The fraction of host memory used per node. As process memory approaches the memory protector limit (`--allowed-percent`, default 75% of the cgroup limit), query execution is throttled, so high memory surfaces as query slowdown first.

**Expression**: `max(banyandb_system_memory_state{job=~"$job", container_name=~"$role", pod_name=~"$pod", kind="used_percent"}) by (pod_name)`

### Disk Usage %

The fraction of disk used per node (the fullest storage path). BanyanDB rejects writes with `STATUS_DISK_FULL` once usage crosses the disk-full threshold (default 95%), so alert well before that — commonly at ~80–85%. The threshold is configurable: a liaison uses `--measure-max-disk-usage-percent`, `--stream-max-disk-usage-percent` and `--trace-max-disk-usage-percent`, while a data or standalone node uses `--measure-retention-high-watermark`, `--stream-retention-high-watermark` and `--trace-retention-high-watermark` (forced retention starts deleting segments there).

**Expression**: `max by (pod_name) (banyandb_system_disk{job=~"$job", container_name=~"$role", pod_name=~"$pod", kind="used_percent"})`

### Network Usage

Bytes received and sent per second, per node and interface (`name`).

**Expression1 (recv)**: `sum(rate(banyandb_system_net_state{job=~"$job", container_name=~"$role", pod_name=~"$pod", kind="bytes_recv"}[$__rate_interval])) by (pod_name, name)`
**Expression2 (sent)**: `sum(rate(banyandb_system_net_state{job=~"$job", container_name=~"$role", pod_name=~"$pod", kind="bytes_sent"}[$__rate_interval])) by (pod_name, name)`

## Disk by Path

BanyanDB can place data on multiple storage paths; these panels break `banyandb_system_disk` down by the `path` label so a single full volume is visible even when the node total looks healthy.

### Disk Used by Path

**Expression**: `sum(banyandb_system_disk{job=~"$job", container_name=~"$role", pod_name=~"$pod", kind="used"}) by (pod_name, path)`

### Disk Total by Path

**Expression**: `sum(banyandb_system_disk{job=~"$job", container_name=~"$role", pod_name=~"$pod", kind="total"}) by (pod_name, path)`

### Disk Used % by Path

**Expression**: `max by (pod_name, path) (banyandb_system_disk{job=~"$job", container_name=~"$role", pod_name=~"$pod", kind="used_percent"})`

## Go Runtime

Standard Go process metrics, per node. Rising goroutine counts, GC pause, or allocation rate are early indicators of memory pressure or a leak.

### Goroutines

**Expression**: `sum(go_goroutines{job=~"$job", container_name=~"$role", pod_name=~"$pod"}) by (pod_name)`

### GC Pause (avg)

Average garbage-collection pause, derived from the GC duration summary.

**Expression**: `sum(rate(go_gc_duration_seconds_sum{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (pod_name) / sum(rate(go_gc_duration_seconds_count{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (pod_name)`

### Heap In-Use

Heap memory currently in use, per node.

**Expression**: `sum(go_memstats_heap_inuse_bytes{job=~"$job", container_name=~"$role", pod_name=~"$pod"}) by (pod_name)`

### Heap Next-GC / Alloc Rate

The heap size that will trigger the next GC, alongside the allocation rate.

**Expression1 (next_gc)**: `sum(go_memstats_next_gc_bytes{job=~"$job", container_name=~"$role", pod_name=~"$pod"}) by (pod_name)`
**Expression2 (alloc_rate)**: `sum(rate(go_memstats_alloc_bytes_total{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (pod_name)`

## Process Health (reference)

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `banyandb_memory_protector_usage`, `banyandb_memory_protector_limit` | Gauge | — | Bytes in use as tracked by the memory protector, and its limit (`--allowed-percent`). Query execution is throttled as usage approaches the limit. |
| `banyandb_panic_total` | Counter | `component` | Panics recovered in the process, by component. Lazily registered, so absent until the first panic. |
| `banyandb_{measure,stream,trace}_metadata_total_panics` | Counter | — | Panics in the schema-metadata handlers of each model. Created at `0`, so `increase()` sees the first panic. |
| `banyandb_{observability,measure,stream_storage,trace_storage}_scheduler_tasks_panic` | Gauge | `job` (and `group` for the storage ones) | Cumulative number of scheduler tasks that panicked; it only grows, so use `increase()`. |
| `banyandb_storage_retention_{measure,stream,trace}_forced_retention_segments_deleted_total` | Counter | `service` | Segments deleted by forced retention after disk usage crossed the high watermark. Created at `0`. |

---

# Workload dashboard

Business- and data-level throughput and latency, aggregated per `group`. Use this dashboard to see which business group / operation is slow, erroring, or backlogged.

## Cluster Workload Summary

Cluster-wide RED stats. Each summand is wrapped in `or vector(0)` so a not-yet-registered counter reports `0` rather than "No Data".

### Cluster Write Rate

Total points / elements / span rows written per second across measures, streams and traces. Only the liaison counts client writes; every node (liaison included) also counts its `_monitoring_log` stream and, in native mode, the `_monitoring` measure. A part a data node receives from a liaison through file sync is not counted again, so a client write is counted once, on the liaison. To alert on client writes, exclude the built-in groups with `group!~"_monitoring|_monitoring_log"`.

**Expression**: `(sum(rate(banyandb_measure_total_written{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) or vector(0)) + (sum(rate(banyandb_stream_tst_total_written{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) or vector(0)) + (sum(rate(banyandb_trace_tst_total_written{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) or vector(0))`

### Cluster Query Rate

Query operations per second on the liaison servers. A measure, stream, trace or TopN query that spans several groups is counted once per group (a property or BydbQL query once), and a BydbQL request is also counted under the service it is dispatched to.

**Expression**: `sum(rate(banyandb_liaison_grpc_total_started{job=~"$job", container_name=~"$role", pod_name=~"$pod", method="query"}[$__rate_interval]))`

### Error Rate

Cluster error rate per minute, combining liaison gRPC, registry (excluding `NotFound`), stream-receive, queue-publisher (excluding `canceled` / `stream_canceled`), and liaison write-queue sync-loop errors. Several of these counters are lazily registered (absent until the first error), hence the `or vector(0)` guards. Unlike `total_stream_err`, the liaison `total_err` also counts requests the client canceled or that hit their deadline.

**Expression**: `(sum(rate(banyandb_liaison_grpc_total_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_liaison_grpc_total_registry_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_liaison_grpc_total_stream_msg_received_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_queue_pub_total_err{job=~"$job", container_name=~"$role", pod_name=~"$pod", error_type!~"canceled|stream_canceled"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_measure_total_sync_loop_err{job=~"$job", container_name="liaison", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_stream_tst_total_sync_loop_err{job=~"$job", container_name="liaison", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_trace_tst_total_sync_loop_err{job=~"$job", container_name="liaison", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0))`

## Liaison: Ingestion, Query & Publish

The liaison node's front door — gRPC ingestion/query, the schema registry, the on-disk write queue (wqueue), and the tier-2 publish pipeline to data nodes.

### Query Rate by Service

Query rate split by the `service` label (measure / stream / trace / property / topn / bydbql).

**Expression**: `sum(rate(banyandb_liaison_grpc_total_started{job=~"$job", container_name=~"$role", pod_name=~"$pod", method="query"}[$__rate_interval])) by (service)`

### Query Latency by Group

Average query latency in seconds, per `group` — total latency divided by total started. Because a distributed query fans out to all nodes, the slowest group dominates the overall query latency.

**Expression**: `sum(rate(banyandb_liaison_grpc_total_latency{job=~"$job", container_name=~"$role", pod_name=~"$pod", method="query"}[$__rate_interval])) by (group) / sum(rate(banyandb_liaison_grpc_total_started{job=~"$job", container_name=~"$role", pod_name=~"$pod", method="query"}[$__rate_interval])) by (group)`

**Expression (p99)**: `histogram_quantile(0.99, sum(rate(banyandb_liaison_grpc_query_latency_seconds_bucket{job=~"$job", container_name=~"$role", pod_name=~"$pod", status!="canceled"}[$__rate_interval])) by (le))`

The p99 is per request: `banyandb_liaison_grpc_query_latency_seconds` (labels `service` and `status`, one of `ok`, `err`, `canceled` or `deadline_exceeded`) is observed once for each query RPC a client sends, however many groups it spans, and a BydbQL request is observed only under `bydbql`. `canceled` is a query the client canceled and `deadline_exceeded` one whose deadline expired. `status!="canceled"` leaves out the client cancellations but keeps the timeouts, so the p99 includes a slow query that times out.

### gRPC Error Rate

Liaison error rate per `group`, plus registry (excluding `NotFound`), stream-receive and write-stream errors (the write-stream term is per `service`: `total_stream_err` has no `group` label). Series without a group label (for example property queries and BydbQL) are matched by `group=~"$group|"`. All terms but `total_stream_err` (created at `0`) are lazily registered, so they are simply absent on a healthy cluster.

**Expression1 (grpc)**: `sum(rate(banyandb_liaison_grpc_total_err{job=~"$job", container_name=~"$role", pod_name=~"$pod", group=~"$group|"}[$__rate_interval])) by (group)`
**Expression2 (registry)**: `sum(rate(banyandb_liaison_grpc_total_registry_err{job=~"$job", container_name=~"$role", pod_name=~"$pod", group=~"$group|"}[$__rate_interval])) by (group)`
**Expression3 (stream recv)**: `sum(rate(banyandb_liaison_grpc_total_stream_msg_received_err{job=~"$job", container_name=~"$role", pod_name=~"$pod", group=~"$group|"}[$__rate_interval])) by (group)`
**Expression4 (write stream)**: `sum(rate(banyandb_liaison_grpc_total_stream_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (service)`

`total_stream_msg_received_err` counts failures of a single message on a stream; `total_stream_err` (`method="write"`) counts a whole measure / stream / trace write stream that ends with an error, except `Canceled` and `DeadlineExceeded`. A stream whose client deadline expires while the data node is still busy is counted, because the liaison fails it with `ResourceExhausted`.

### Registry Operation Rate

The rate of schema-registry operations and non-query gRPC calls.

**Expression1 (registry)**: `sum(rate(banyandb_liaison_grpc_total_registry_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval]))`
**Expression2 (non-query)**: `sum(rate(banyandb_liaison_grpc_total_started{job=~"$job", container_name=~"$role", pod_name=~"$pod", method!="query"}[$__rate_interval]))`

### Write Rate by Group

Write operations per second on the liaison, per `group`, for measures, streams and traces (charted as separate series rather than added, since each data type uses different `group` values).

**Expression1**: `sum(rate(banyandb_measure_total_written{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression2**: `sum(rate(banyandb_stream_tst_total_written{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression3**: `sum(rate(banyandb_trace_tst_total_written{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Dropped Writes (reference)

A write listener that cannot store a row of a batch logs it and goes on with the rest; the batch is still acknowledged, because the acknowledgement covers the batch and means the rows were applied, not that each one was (see the [0.12.0 write contract](../../design/0.12.0/streaming-api/README.md)). These counters make that loss visible. Each liaison and data node counts the rows its own write listener skipped, before anything was stored:

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `banyandb_stream_write_dropped_total` | Counter | `group`, `reason` | Stream elements a write listener skipped. |
| `banyandb_measure_write_dropped_total` | Counter | `group`, `reason` | Measure data points a write listener skipped. |
| `banyandb_trace_write_dropped_total` | Counter | `group`, `reason` | Trace spans a write listener skipped. |

`reason` is one of:

- `group_not_found`: the node does not hold the group, for example because the group was deleted or has not reached the node yet.
- `schema_not_found`: the node holds the group but has no schema for the stream, measure or trace.
- `invalid_timestamp`: the timestamp is outside the supported range or finer than a millisecond.
- `invalid_element`: the row does not match its schema, for example it has more tag families than the schema declares.
- `decode`: the row's bytes are not a write request.
- `invalid_type`: the row, or a whole message, is of a type the listener does not take. `group` is empty for this reason and for `decode`, because the row was never read.

`group` is the row's group only when the node holds that group; otherwise it is empty. A row for a deleted or unknown group (`group_not_found`), or one that names a group the node never held, is counted with an empty `group`, so the label is bounded by the groups a node holds, whatever a writer sends. The log line of the drop still names the group.

**Aggregating across nodes.** A row is counted at most once, by the node whose listener skipped it:

- A write the liaison drops never reaches a data node, and a write a data node drops was not dropped by the liaison, so the liaison and data-node counts are disjoint and can be summed across roles.
- A write that tells its caller which rows failed is not counted: `bydbctl data import` writes failed rows again within its retry budget and reports those it could not write, so those rows are neither lost silently nor counted once per attempt.

So `sum` over every node is the number of rows lost silently; `sum by (container_name)` shows where.

A failure after the rows were stored, such as an index write, is logged but not counted here: the rows themselves are kept. In the native self-observability mode these counters are stored in the `_monitoring` group under the same names without the `banyandb_` prefix; a row of BanyanDB's own logs that a stream write listener skips is counted in `stream_write_dropped_total`, not in `native_log_dropped_total`, which counts what the log sink lost before the write.

**Expression (dropped rows per second by model, group and reason)**: `(sum(rate(banyandb_stream_write_dropped_total{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group, reason)) or (sum(rate(banyandb_measure_write_dropped_total{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group, reason)) or (sum(rate(banyandb_trace_write_dropped_total{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group, reason))`

### Publish Throughput & Success

The tier-2 publisher's success throughput by `operation` (for `batch-write` with `remote_role="liaison"`, `total_finished` means sent, not succeeded), alongside file-sync bytes sent by `group`. See the [queue reference](#internal-queue-metrics-reference-queue_sub--queue_pub) for the full model.

**Expression1 (success)**: `sum(rate(banyandb_queue_pub_total_finished{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (operation)`
**Expression2 (bytes)**: `sum(rate(banyandb_queue_pub_sent_bytes{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Write Queue Length (wqueue)

The liaison buffers writes in an **on-disk write queue (wqueue)** before syncing each part to the data nodes. These gauges (scoped with the literal `container_name="liaison"`) expose the queue per `pod_name`, `group`, `seg` and `shard` (after a liaison restart, a shard's series appears with its first write): `unsynced_parts` (including in-memory parts) and `oldest_unsynced_part_age_seconds` (`0` when no part has an enqueue time); for trace both count core parts only. If the shard has no data node to sync to, or the sync is stuck, parts stay in the queue, the age keeps rising and the queue can eventually fill the liaison disk (`STATUS_DISK_FULL`). A part that fails to sync is retried for about 7 seconds (1s + 2s + 4s; one failed part after another), then dropped from the queue and copied to `failed-parts/` on a best-effort basis; `total_sync_failed_parts` (created at `0`, use `increase()`) counts it, so an increase means data may be at risk. Read the liaison backlog from `unsynced_parts` and `oldest_unsynced_part_age_seconds`, not from `total_file_parts`, `total_mem_part` or `pending_data_count`: those three are registered on a liaison but never set, so they have no series there. A liaison has no segments, so these gauges carry `seg=""` (alongside `group` and `shard`); do not filter them by `seg`. The matching `*_total_sync_loop_*` counters (`started`/`finished`/`latency`/`bytes`, and the lazily-registered `*_total_sync_loop_err`) describe the loop that drains it; `*_total_sync_loop_err` counts a failed loop (for measure and stream, no data node), not parts that fail to sync.

The `banyandb_{measure,stream_tst,trace_tst}_file_sync_queue_latency_seconds` histograms measure a different boundary: from a core part's commit into the liaison wqueue until that part receives a successful file-sync acknowledgement from each target data node. This includes queue residence, merge/flush scheduling, transfer, and retries. A merged part retains the oldest enqueue time of its inputs. Samples are labeled by `remote_node`; failed acknowledgements and legacy parts without an enqueue timestamp are not sampled.

**Expression1 (unsynced parts)**: `sum(banyandb_{measure,stream_tst,trace_tst}_unsynced_parts{job=~"$job", container_name="liaison", pod_name=~"$pod", group=~"$group"}) by (pod_name, group)`
**Expression2 (oldest age)**: `max by (pod_name, group) (banyandb_{measure,stream_tst,trace_tst}_oldest_unsynced_part_age_seconds{job=~"$job", container_name="liaison", pod_name=~"$pod", group=~"$group"})`
**Expression3 (sync failed)**: `round(sum(increase(banyandb_{measure,stream_tst,trace_tst}_total_sync_failed_parts{job=~"$job", container_name="liaison", pod_name=~"$pod", group=~"$group"}[$__rate_interval])) by (pod_name, group))`
**Expression4 (queue-to-data p99)**: `histogram_quantile(0.99, sum(rate(banyandb_{measure,stream_tst,trace_tst}_file_sync_queue_latency_seconds_bucket{job=~"$job", container_name="liaison", pod_name=~"$pod"}[$__rate_interval])) by (le, pod_name, group, remote_node))`

> The backlog panel charts `unsynced_parts`, `total_sync_failed_parts` and `oldest_unsynced_part_age_seconds` for `measure`, `stream_tst` and `trace_tst` as separate series (nine queries; the age is on the right axis). The neighboring latency panel adds the three queue-to-data histograms. The `{measure,stream_tst,trace_tst}` brace above is shorthand for the three concrete metric names.

### Publish & Wqueue-to-data Latency p99

The 99th-percentile tier-2 publisher RPC latency is shown beside the full queue-to-data latency for measure, stream and trace core parts. The first series starts when an RPC is sent; the other three start when the part enters the liaison wqueue.

**Expression1 (publisher RPC)**: `histogram_quantile(0.99, sum(rate(banyandb_queue_pub_total_latency_bucket{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (le, operation, group))`
**Expression2 (queue-to-data)**: `histogram_quantile(0.99, sum(rate(banyandb_{measure,stream_tst,trace_tst}_file_sync_queue_latency_seconds_bucket{job=~"$job", container_name="liaison", pod_name=~"$pod"}[$__rate_interval])) by (le, group, remote_node))`

## Data: Storage

`Storage` metrics monitor the data nodes' on-disk state, grouped by the `group` tag. View different `pod_name`s to find a hot node or uneven data distribution.

### Total Data

The total number of data elements (points/rows) stored, per `group`, for measures, streams and traces.

**Expression1**: `sum(banyandb_measure_total_file_elements{job=~"$job", container_name=~"$role", pod_name=~"$pod"}) by (group)`
**Expression2**: `sum(banyandb_stream_tst_total_file_elements{job=~"$job", container_name=~"$role", pod_name=~"$pod"}) by (group)`
**Expression3**: `sum(banyandb_trace_tst_total_file_elements{job=~"$job", container_name=~"$role", pod_name=~"$pod"}) by (group)`

### Merge File Rate

Merge-file operations per minute, per `group`. A surge means many small files are being merged, which raises disk I/O and CPU and can slow queries.

**Expression1**: `sum(rate(banyandb_measure_total_merge_loop_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group) * 60`
**Expression2**: `sum(rate(banyandb_stream_tst_total_merge_loop_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group) * 60`
**Expression3**: `sum(rate(banyandb_trace_tst_total_merge_loop_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group) * 60`

### Merge File Latency

Average merge-file latency in seconds, per `group` (total merge latency ÷ `total_merge_loop_started`). A surge indicates slow merges, often from high disk I/O. In measure and stream, `total_merge_loop_started` advances on every pass of the merge loop, including passes that find nothing to merge, whereas `total_merged{type="file"}` counts merges that ran; divide by it for the exact per-merge average. The trace `total_merge_latency`, `total_merged` and `total_merged_parts` carry an extra `lane` label besides `type`, which the sums above aggregate away.

**Expression1**: `sum(rate(banyandb_measure_total_merge_latency{job=~"$job", container_name=~"$role", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_measure_total_merge_loop_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression2**: `sum(rate(banyandb_stream_tst_total_merge_latency{job=~"$job", container_name=~"$role", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_stream_tst_total_merge_loop_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression3**: `sum(rate(banyandb_trace_tst_total_merge_latency{job=~"$job", container_name=~"$role", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_trace_tst_total_merge_loop_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Merge File Partitions

Average number of partitions merged per merge-loop pass, per `group` (merged parts ÷ `total_merge_loop_started`). A surge suggests the server is under heavy write load.

**Expression1**: `sum(rate(banyandb_measure_total_merged_parts{job=~"$job", container_name=~"$role", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_measure_total_merge_loop_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression2**: `sum(rate(banyandb_stream_tst_total_merged_parts{job=~"$job", container_name=~"$role", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_stream_tst_total_merge_loop_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression3**: `sum(rate(banyandb_trace_tst_total_merged_parts{job=~"$job", container_name=~"$role", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_trace_tst_total_merge_loop_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Trace Plugin Metrics (reference)

Trace merge and finalization sampling can run an ordered chain of plugins. The host-provided catalogs below cover plugin
lifecycle, execution, sampling outcomes, resource-safety fallbacks, telemetry safety, and finalization state. These
metrics are group-scoped and deliberately omit segment, shard, and schema labels. Link-level metrics carry the configured
`plugin_name`, allowing a slow or faulty link to be isolated when several plugins are enabled.

#### Plugin lifecycle and loading

These metrics use the `banyandb_trace_pipeline_` prefix and describe configuration reconciliation rather than calls to `Decide`.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `banyandb_trace_pipeline_sampler_active_count` | Gauge | `group` | Number of active sampler plugins registered for the group. |
| `banyandb_trace_pipeline_sampler_register_total` | Counter | `group`, `result` | Initial pipeline registrations, where `result` is `success` or `rejected`. |
| `banyandb_trace_pipeline_sampler_update_total` | Counter | `group`, `result` | Updates to an existing pipeline, where `result` is `success` or `rejected`. |
| `banyandb_trace_pipeline_sampler_remove_total` | Counter | `group` | Explicit pipeline removals. |
| `banyandb_trace_pipeline_sampler_load_failed` | Counter | `group`, `name`, `reason` | Individual plugin load failures. The pipeline reconciliation path fails open. |

#### Plugin execution and batching

Chain-level metrics are emitted once per decision batch; execution metrics are emitted once per plugin link.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `banyandb_trace_tst_pipeline_plugin_batches` | Counter | `group`, `result` | Chain batches: `success`, `timeout`, or `circuit_open`. |
| `banyandb_trace_tst_pipeline_plugin_executions` | Counter | `group`, `plugin_name`, `result` | Calls to each plugin's `Decide`, split by result. |
| `banyandb_trace_tst_pipeline_plugin_execution_duration_seconds` | Histogram | `group`, `plugin_name`, `result` | Wall time inside each `Decide` call. |
| `banyandb_trace_tst_pipeline_plugin_batch_traces` | Histogram | `group`, `result` | Trace count presented in each chain batch. |
| `banyandb_trace_tst_pipeline_plugin_link_bypasses` | Counter | `group`, `plugin_name`, `reason` | Links bypassed after error, mismatch, or panic. |

**Expression1 (execution rate by plugin/result)**: `sum(rate(banyandb_trace_tst_pipeline_plugin_executions{job=~"$job", container_name="data", pod_name=~"$pod"}[$__rate_interval])) by (group, plugin_name, result)`

**Expression2 (p99 execution duration by plugin)**: `histogram_quantile(0.99, sum(rate(banyandb_trace_tst_pipeline_plugin_execution_duration_seconds_bucket{job=~"$job", container_name="data", pod_name=~"$pod", result="success"}[$__rate_interval])) by (group, plugin_name, le))`

**Expression3 (successful execution time per evaluated trace)**: `sum by (group, plugin_name) (rate(banyandb_trace_tst_pipeline_plugin_execution_duration_seconds_sum{job=~"$job", container_name="data", pod_name=~"$pod", result="success"}[$__rate_interval])) / on (group) group_left sum by (group) (rate(banyandb_trace_tst_pipeline_traces_evaluated{job=~"$job", container_name="data", pod_name=~"$pod"}[$__rate_interval]))`

**Expression4 (mean successful decision-batch size)**: `sum by (group) (rate(banyandb_trace_tst_pipeline_traces_evaluated{job=~"$job", container_name="data", pod_name=~"$pod"}[$__rate_interval])) / sum by (group) (rate(banyandb_trace_tst_pipeline_plugin_batches{job=~"$job", container_name="data", pod_name=~"$pod", result="success"}[$__rate_interval]))`

**Expression5 (chain timeout/circuit-open rate)**: `sum(rate(banyandb_trace_tst_pipeline_plugin_batches{job=~"$job", container_name="data", pod_name=~"$pod", result=~"timeout|circuit_open"}[$__rate_interval])) by (group, result)`

**Expression6 (fail-open link rate)**: `sum(rate(banyandb_trace_tst_pipeline_plugin_link_bypasses{job=~"$job", container_name="data", pod_name=~"$pod"}[$__rate_interval])) by (group, plugin_name, reason)`

Execution `result` is one of `success`, `decide_error`, `length_mismatch`, `panic`, or `late`. `late` means the plugin returned after the host had already timed out and abandoned its worker. `pipeline_plugin_link_bypasses{group,plugin_name,reason}` independently counts the three per-link fail-open causes: `decide_error`, `length_mismatch`, and `panic`. A chain batch can therefore be `success` while one link was bypassed and retained its input.

#### Alert recommendations

Not every useful plugin metric should become a universal alert:

- **Alert immediately:** any `panic`, `length_mismatch`, or `late` execution. These indicate broken plugin behavior or a call that outlived the merge timeout. Example: `sum by (group, plugin_name, result) (increase(banyandb_trace_tst_pipeline_plugin_executions{job=~"$job", container_name="data", result=~"panic|length_mismatch|late"}[5m])) > 0`.
- **Alert immediately:** any `circuit_open` batch because sampling is now being bypassed for the group. Example: `sum by (group) (increase(banyandb_trace_tst_pipeline_plugin_batches{job=~"$job", container_name="data", result="circuit_open"}[5m])) > 0`.
- **Warn on sustained failures:** `decide_error` or `timeout` may be transient, so alert on a nonzero sustained rate or an operator-chosen error ratio rather than a single occurrence. Always retain the `plugin_name` split for decide errors; timeout is chain-level because the host may abandon the chain before a link returns.
- **Dashboard/SLO, not a fixed default alert:** execution latency, execution time per evaluated trace, and batch size depend on plugin logic and trace shape. Establish a per-plugin p99 SLO from a healthy baseline and keep it comfortably below `--trace-pipeline-decide-timeout` (default 5 seconds). A single repository-wide latency threshold would generate false positives for intentionally expensive plugins.
- **Dashboard only:** raw execution and evaluated-trace rates. Their ratio explains batching and overhead, but low traffic or a merge-grace window naturally makes both zero.

These are reference queries rather than universal preinstalled alert rules. Operators should copy the immediate health expressions into their alert manager and derive latency thresholds from a healthy baseline for each `(group, plugin_name)` pair.

#### Trace retention outcomes and safety

These `banyandb_trace_tst_` metrics supply the trace-level denominators and expose fail-open or bounded-retention behavior around plugin execution.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `banyandb_trace_tst_pipeline_traces_evaluated` | Counter | `group` | Traces evaluated by a successfully completed, batch-aligned plugin chain. |
| `banyandb_trace_tst_pipeline_traces_retained` | Counter | `group` | Logical traces retained in output by plugin verdicts or safety fallbacks. |
| `banyandb_trace_tst_pipeline_traces_dropped` | Counter | `group` | Evaluated traces removed from core and secondary-index output. |
| `banyandb_trace_tst_pipeline_traces_immature` | Counter | `group` | Traces not eligible for sampling because their fragments are still inside the maturity boundary. |
| `banyandb_trace_tst_pipeline_oversized_traces_bypassed` | Counter | `group` | Traces retained because their staged representation exceeded the per-trace budget. |
| `banyandb_trace_tst_pipeline_plugin_errors` | Counter | `group`, `reason` | Chain-level plugin errors that caused fail-open retention. `reason` has a single value, `decide_failed_open`. |
| `banyandb_trace_tst_pipeline_ambiguous` | Counter | `group` | Traces whose fragment state could not produce an unambiguous drop decision. |
| `banyandb_trace_tst_pipeline_sidx_pruned` | Counter | `group` | Secondary-index element rows (one per indexed span per sidx) removed after confirmed trace drops. |
| `banyandb_trace_tst_pipeline_guard_bloom_probes` | Counter | `group` | Bloom-filter probes used to check trace fragments outside the selected parts. |
| `banyandb_trace_tst_pipeline_guard_deferred` | Counter | `group` | Proposed drops deferred because the fragment guard found a possible external fragment. |
| `banyandb_trace_tst_pipeline_guard_budget_exhausted` | Counter | `group` | Fragment-guard checks that exhausted their configured work budget. |
| `banyandb_trace_tst_pipeline_guard_publication_rejected` | Counter | `group` | Publications rejected after guard validation became stale. |
| `banyandb_trace_tst_pipeline_guard_lossless_retry` | Counter | `group` | Rejected publications retried without dropping traces. |
| `banyandb_trace_tst_pipeline_guard_bypassed` | Counter | `group` | Merges retained without sampling because the fragment guard was unavailable or inapplicable. |
| `banyandb_trace_tst_pipeline_traces_retained_by_ceiling` | Counter | `group` | Proposed drops retained after the bounded drop-ID set reached its ceiling. |
| `banyandb_trace_tst_pipeline_merges_ceiling_reached` | Counter | `group`, `lane` | Merges whose drop-ID set reached its ceiling, split by merge lane. |
| `banyandb_trace_tst_pipeline_drop_set_budget_bytes` | Gauge | `group` | Resolved per-merge memory budget for confirmed dropped trace IDs. |
| `banyandb_trace_tst_pipeline_drop_set_entries` | Histogram | `group`, `lane` | Dropped trace IDs held per merge; shows ceiling headroom. |

The effective drop ratio is `rate(banyandb_trace_tst_pipeline_traces_dropped[5m]) /
rate(banyandb_trace_tst_pipeline_traces_evaluated[5m])`. Compare retained, dropped, and bypass counters together: a
lower drop ratio can be intentional plugin behavior or a fail-open safety mechanism.

#### Plugin telemetry host health

Plugins that implement `HostAware` may publish bounded business metrics and logs through the host façade. These counters show when the host enforces those bounds.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `banyandb_trace_pipeline_plugin_telemetry_series_rejected_total` | Counter | `group`, `plugin_name` | Series rejected by host safety bounds. |
| `banyandb_trace_pipeline_plugin_log_dropped_total` | Counter | `group`, `plugin_name` | Plugin log lines dropped by the host rate limiter. |
| `banyandb_trace_pipeline_plugin_telemetry_panic_total` | Counter | `group`, `plugin_name` | Panics recovered inside the plugin telemetry adapter. |

#### Finalization state

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `banyandb_trace_pipeline_finalize_rounds` | Gauge | `group` | Highest finalization round count observed across the group's cooled shards. |
| `banyandb_trace_pipeline_finalize_terminal` | Gauge | `group` | `1` when any cooled shard is terminal and cannot run another finalization round; otherwise `0`. |

#### Plugin-defined business metrics

Metrics created by a plugin through `Host.Meter` are forced under the `banyandb_trace_pipeline_plugin_` prefix and always
carry `group` and `plugin_name`; plugin-declared labels follow them. Names beyond that prefix are plugin-defined and
therefore cannot be enumerated in the host catalog. The host caps each plugin instrument at 100 distinct label-value
series and directs overflow to a bounded sentinel series. See
[Plugin Telemetry](../../design/archive/0.11.0/post-trace-pipeline.md#27-plugin-telemetry-meter--logger) for the complete contract.

The first-party `sw-trace-sampler` and `zipkin-trace-sampler` publish the following bounded decision and row metrics:

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `banyandb_trace_pipeline_plugin_trace_sampler_decisions_total` | Counter | `group`, `plugin_name`, `verdict`, `rule` | Trace evaluations attributed to the first matching sampler rule. |
| `banyandb_trace_pipeline_plugin_trace_sampler_rows_total` | Counter | `group`, `plugin_name` | All span or segment rows carried by trace evaluations when a projected row-aligned column is available. |
| `banyandb_trace_pipeline_plugin_trace_sampler_rows_dropped_total` | Counter | `group`, `plugin_name`, `rule` | Rows belonging to traces the sampler proposed dropping, attributed to the fixed drop rule. |
| `banyandb_trace_pipeline_plugin_trace_sampler_row_count_unavailable_total` | Counter | `group`, `plugin_name` | Trace evaluations whose metadata-only projection exposed no row count; these traces are omitted from `rows_total`. |

`verdict` is `keep` or `drop`. `rule` is one of the fixed built-in values `duration`, `error`, `healthy_sample`,
`healthy_rejected`, `no_keep_rule`, `decode_failure_duration`, `decode_failure_error`, and `decode_failure_tags`, or a
fixed positional tag-rule slot from `tag_00` through `tag_31`. Tag slots follow `keepTagRules` configuration order; tag
keys and values are never copied into metric labels. Configurations with more than 32 tag rules are rejected.

This counter reports sampler evaluations, not unique traces or committed storage deletions. A trace may be evaluated in
multiple merge or finalization rounds, and BanyanDB safety guards may retain a trace for which a plugin returned a drop
verdict. Compare requested drops by rule with the engine's committed drop rate rather than treating them as equal:

`sum by (group, plugin_name, rule) (rate(banyandb_trace_pipeline_plugin_trace_sampler_decisions_total{verdict="drop"}[5m]))`

`sum by (group) (rate(banyandb_trace_tst_pipeline_traces_dropped[5m]))`

For the first-party default SkyWalking policy, projected duration and error columns make the row count available. The
proposed dropped-row ratio is:

`sum by (group, plugin_name) (rate(banyandb_trace_pipeline_plugin_trace_sampler_rows_dropped_total[5m]))`
` / sum by (group, plugin_name) (rate(banyandb_trace_pipeline_plugin_trace_sampler_rows_total[5m]))`

Break down the dropped rows by the responsible rule with
`sum by (group, plugin_name, rule) (rate(banyandb_trace_pipeline_plugin_trace_sampler_rows_dropped_total[5m]))`.
Alert on any increase of `trace_sampler_row_count_unavailable_total` before treating the ratio as complete. Like the
decision counter, this is the plugin's proposed row-drop ratio, not the committed storage deletion ratio after guards.

## Data: Inverted Index

`Inverted Index` metrics monitor the series index of measures, streams and traces, grouped by the `group` tag. Rapid growth or churn signals a cardinality problem that bloats the index and slows queries.

### Series Write Rate

Series index update operations per second, per `group`, for measures, streams and traces.

**Expression1 (measure)**: `sum(rate(banyandb_measure_inverted_index_total_updates{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression2 (stream)**: `sum(rate(banyandb_stream_storage_inverted_index_total_updates{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression3 (trace)**: `sum(rate(banyandb_trace_storage_inverted_index_total_updates{job=~"$job", container_name=~"$role", pod_name=~"$pod", group=~"$group"}[$__rate_interval])) by (group)`

### Series Term Search Rate

Series term-search operations per second, per `group`. A high value means reads are fetching many series — often a high-cardinality symptom.

**Expression1 (measure)**: `sum(rate(banyandb_measure_inverted_index_total_term_searchers_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression2 (stream)**: `sum(rate(banyandb_stream_storage_inverted_index_total_term_searchers_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression3 (trace)**: `sum(rate(banyandb_trace_storage_inverted_index_total_term_searchers_started{job=~"$job", container_name=~"$role", pod_name=~"$pod", group=~"$group"}[$__rate_interval])) by (group)`

### Total Series

The total number of series (index documents) stored, per `group`, for measures, streams and traces. `inverted_index_total_doc_count` has one series per segment (labels `group` and `seg`, the segment start formatted as `YYYYMMDD` for a day interval or `YYYYMMDDHH` for an hour interval) and counts the live documents of the segment's series index, one per series. A segment that is closed when idle or deleted by retention removes its series, so the sum covers the open segments only.

**Expression1 (measure)**: `sum(banyandb_measure_inverted_index_total_doc_count{job=~"$job", container_name=~"$role", pod_name=~"$pod"}) by (group)`
**Expression2 (stream)**: `sum(banyandb_stream_storage_inverted_index_total_doc_count{job=~"$job", container_name=~"$role", pod_name=~"$pod"}) by (group)`
**Expression3 (trace)**: `sum(banyandb_trace_storage_inverted_index_total_doc_count{job=~"$job", container_name=~"$role", pod_name=~"$pod", group=~"$group"}) by (group)`

### Active Series

The number of distinct series written per `group` in the last complete UTC hour, for measures and streams. `active_series` (labels `group` and `shard`) is a HyperLogLog estimate reported by liaison and standalone nodes; data nodes do not report it. Each (group, shard) is written through one liaison, so take the maximum per shard and sum the shards. Traces, Top-N result measures and imported data are not counted. The value changes once an hour, and the hours are cut by when a write arrives, not by its data timestamp. A (group, shard) has no sample for the hour in which the node started, for the hour in which the group's shard count changed and the hour after, and when the previous hour had no writes. The hour in which the writing liaisons change (a liaison is added, removed or restarted) can read low.

**Expression1 (measure)**: `sum by (group) (max by (group, shard) (banyandb_measure_active_series{job=~"$job"}))`
**Expression2 (stream)**: `sum by (group) (max by (group, shard) (banyandb_stream_storage_active_series{job=~"$job"}))`

### Stream tst: Write Rate

Write rate into the stream **time-series-table (tst) inverted index** (the per-element index, distinct from the series index above), per `group`.

**Expression**: `sum(rate(banyandb_stream_tst_inverted_index_total_updates{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Stream tst: Term Search Rate

Term-search rate on the stream tst inverted index, per `group`.

**Expression**: `sum(rate(banyandb_stream_tst_inverted_index_total_term_searchers_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Stream tst: Total Documents

Total documents in the stream tst inverted index, per `group`. Every shard of a segment writes the same `{group, seg}` series, so the value reflects the documents of a single shard rather than the sum of all shards.

**Expression**: `sum(banyandb_stream_tst_inverted_index_total_doc_count{job=~"$job", container_name=~"$role", pod_name=~"$pod"}) by (group)`

## Data: Internal Queue

One panel per queue **operation**, each showing subscribe throughput (`started`/`finished`) and p99 latency, broken down by `group`. These are the subscribe view of the internal queue; the full model is in the [queue reference](#internal-queue-metrics-reference-queue_sub--queue_pub). The four operations are `query`, `file-sync`, `batch-write` and `control`.

Each panel uses the same three expressions, with `operation` pinned to the panel's value (`<op>` ∈ `query` | `file-sync` | `batch-write` | `control`):

**Started**: `sum(rate(banyandb_queue_sub_total_started{job=~"$job", container_name=~"$role", pod_name=~"$pod", operation="<op>"}[$__rate_interval])) by (group)`
**Finished**: `sum(rate(banyandb_queue_sub_total_finished{job=~"$job", container_name=~"$role", pod_name=~"$pod", operation="<op>"}[$__rate_interval])) by (group)`
**p99 latency**: `histogram_quantile(0.99, sum(rate(banyandb_queue_sub_total_latency_bucket{job=~"$job", container_name=~"$role", pod_name=~"$pod", operation="<op>"}[$__rate_interval])) by (le, group))`

> `batch-write` covers both plain writes and the secondary-index sync (measure/stream series-index, stream local-index, trace sidx-series). All of these carry their business `group`, so the per-group breakdown is complete for every operation.

The batch-write section also exposes per-batch and per-message throughput. Its phase-latency panel keeps the legacy publisher/subscriber `total_batch_latency` p99 and adds the publisher's first-frame-to-seal `batch_open_duration_seconds` p99 and seal-to-terminal `batch_admission_duration_seconds` p99. The admission-timeout panel uses `rate(banyandb_queue_pub_batch_admission_timeout_total[$__rate_interval])` by `group` and `remote_node`; it is empty until the first deadline is reached.

---

# Internal queue metrics reference (`queue_sub` / `queue_pub`)

Liaison nodes run an internal gRPC **queue server** (`server-queue-sub`, wired via `sub.NewServerWithPorts` in `pkg/cmdsetup/liaison.go`) and **queue clients** (`server-queue-pub`) for the tier-1/tier-2 pipelines. Prometheus metrics use the namespaces `banyandb_queue_sub_*` and `banyandb_queue_pub_*` (built from `observability.RootScope` + `queue_sub` / `queue_pub` sub-scopes). Data nodes expose only the `queue_sub_*` families.

Both namespaces share one model: the base metrics `total_started`, `total_finished`, `total_latency` (a histogram), and `total_err`, labeled by `operation` (`batch-write` / `file-sync` / `query` / `control`) and `group`, plus the **remote endpoint** of the flow — `remote_node` (the peer's BanyanDB node name, equal to its `/cluster/topology` `metadata.name`), `remote_role` (`liaison` / `data`), and `remote_tier` (`hot` / `warm` / `cold`, data only). `total_err` adds an `error_type` label. File-sync additionally exposes byte counters: `sent_bytes` (pub) and `received_bytes` (sub). The **local** end of each flow is the scrape target itself (`pod_name` / `node_role` / `node_type` from the FODC proxy), so joining the scrape labels with the `remote_*` labels reconstructs the liaison↔data(hot/warm/cold) call graph. For a worked recipe that fuses these metrics with the FODC `/cluster/topology` node inventory to render the topology, see [Cluster Topology Rendering](../fodc/topology.md).

### `queue_sub` — inbound server

| Metric (suffix after `banyandb_queue_sub_`) | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `total_started`, `total_finished` | Counter | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Subscribe RPCs started / finished, per operation and group. A persistent `started − finished` gap indicates backlog or stuck handlers. |
| `total_latency` | Histogram | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Handling latency (`_bucket` / `_sum` / `_count`); use `histogram_quantile` for p50/p99. |
| `total_err` | Counter | …, `error_type` | Errors by type. `error_type` is one of `empty_topic`, `invalid_topic`, `unknown_topic`, `unmarshal_error`, `marshal_error`, `no_listener`, `handler_error`, `send_error` (message path) or `ctx_done`, `eof`, `stream_error`, `switch`, `finish_sync_err`, `gap_too_large`, `buffer_full`, `buffer_timeout`, `checksum_mismatch`, `part_failed` (file-sync). `empty_topic` and `invalid_topic` are recorded before the operation is known, so those samples have `operation=""`. Lazily registered, so absent (not zero) on a healthy cluster. |
| `received_bytes` | Counter | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Bytes received, **file-sync only** (`operation="file-sync"`). |
| `total_batch_started`, `total_batch_finished`, `total_batch_latency`, `total_message_started`, `total_message_finished` | Counter (`total_batch_latency`: Histogram) | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Batch-mode requests started / finished with their handling latency, and messages started / finished (a batch request counts all the messages in it). |

**Troubleshooting:** a growing `total_started − total_finished` gap (or rising `total_latency` p99) for a `group`/`operation` points at slow or stuck consumers. `total_err` broken down by `error_type` distinguishes transport issues (`stream_error`, `checksum_mismatch`, `buffer_full`, `buffer_timeout`) from completion issues (`finish_sync_err`, `part_failed`) and handler failures (`handler_error`). For file-sync, `received_bytes` together with `total_latency` separates partial completion from healthy throughput.

### `queue_pub` — outbound batch client

| Metric (suffix after `banyandb_queue_pub_`) | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `total_started`, `total_finished` | Counter | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Sends started / finished per operation and target node. For `operation="batch-write"` with `remote_role="liaison"` both advance together right after each `Send`, so `total_finished` means "sent", not "succeeded", and the started − finished difference is not meaningful. |
| `total_latency` | Histogram | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Send latency (`_bucket` / `_sum` / `_count`); use `histogram_quantile` for p99. |
| `total_err` | Counter | …, `error_type` | Send errors by type. `error_type` is one of `non_transient`, `canceled`, `stream_canceled`, `retry_exhausted`, `recv_error`, `server_rejected`, `send_error`, `decode_error`, `invalid_topic` (Send path), `idle_timeout` (scan streams) or `stream_error`, `recv_error`, `checksum_mismatch`, `out_of_order`, `session_not_found`, `completion_error`, `server_busy` (file-sync). Lazily registered. |
| `sent_bytes` | Counter | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Bytes sent, **file-sync only**. |
| `total_batch_started`, `total_batch_finished` | Counter | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Batch streams opened / terminal responses received. |
| `total_batch_latency` | Histogram | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Legacy stream-creation-to-terminal-response duration. It includes both time that the client holds the stream open and post-seal admission time. |
| `batch_open_duration_seconds` | Histogram | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | First successfully sent frame to the batch seal. Long-lived client streams are expected and use buckets through 24 hours. |
| `batch_admission_duration_seconds` | Histogram | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Batch seal to terminal response, transport failure, or admission deadline. This is the phase governed by the batch publisher timeout. |
| `batch_admission_timeout_total` | Counter | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Admission deadlines reached, once per still-pending target. Lazily registered and therefore absent on a healthy cluster. |

**Troubleshooting:** `total_err` by `error_type` separates transport failures (`recv_error`) from application-level `SendResponse` errors (`server_rejected`) and exhausted retries (`retry_exhausted`). For `batch-write`, use `batch_open_duration_seconds` to identify client-side stream lifetime and `batch_admission_duration_seconds` plus `batch_admission_timeout_total` to isolate post-seal liaison admission. Do not use the legacy `total_batch_latency` alone for this distinction. For `file-sync`, `total_latency` measures only the transfer RPC; the engine-level `file_sync_queue_latency_seconds` families above include the part's preceding wqueue residence.

Metrics are only registered when `metadata` implements `metadata.Service` and `MetricsRegistry()` is non-nil (e.g. after `SetMetricsRegistry` in bootstrap). `NewWithoutMetadata()` leaves `queue_pub` metrics disabled and logs a warning (`queue_pub metrics disabled: ...`). The `total_err` counters above are registered lazily on first occurrence, so they are simply absent (not zero) on a healthy cluster.

### Example PromQL snippets

Saturation (scope by node with the proxy labels):

- **Subscribe throughput:** `sum(rate(banyandb_queue_sub_total_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (operation)` (and the matching `banyandb_queue_sub_total_finished`)
- **Subscribe p99 latency:** `histogram_quantile(0.99, sum(rate(banyandb_queue_sub_total_latency_bucket{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (le, operation))`
- **File-sync bytes received:** `sum(rate(banyandb_queue_sub_received_bytes{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
- **Publisher success rate:** `sum(rate(banyandb_queue_pub_total_finished{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (operation)`
- **Publisher errors by type:** `sum(rate(banyandb_queue_pub_total_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (operation, error_type)`
- **Publisher file-sync bytes sent (by tier):** `sum(rate(banyandb_queue_pub_sent_bytes{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (remote_tier)`
- **Batch-open p99:** `histogram_quantile(0.99, sum(rate(banyandb_queue_pub_batch_open_duration_seconds_bucket{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (le, group))`
- **Post-seal admission p99:** `histogram_quantile(0.99, sum(rate(banyandb_queue_pub_batch_admission_duration_seconds_bucket{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (le, group))`
- **Admission timeouts by target:** `sum(rate(banyandb_queue_pub_batch_admission_timeout_total{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group, remote_node)`

**Suggested alerts (tune thresholds per cluster):**

- Non-zero sustained `rate(banyandb_queue_pub_total_err{error_type="retry_exhausted"}[5m])` on liaison.
- Non-zero `increase(banyandb_queue_pub_batch_admission_timeout_total[5m])`, or post-seal admission p99 approaching the configured write timeout.
- A sustained `rate(banyandb_queue_sub_total_started[5m]) - rate(banyandb_queue_sub_total_finished[5m])` gap (subscribe backlog), or `total_latency` p99 above an environment-specific ceiling, for a single `group` / `operation`.

### Aggregate pipeline error rate (optional)

To combine subscribe-side and publisher-side queue failures (per-minute scaling as elsewhere in this doc; each term wrapped in `or vector(0)` so a missing counter doesn't blank the result):

**Expression**: `(sum(rate(banyandb_queue_sub_total_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_queue_pub_total_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0))`
