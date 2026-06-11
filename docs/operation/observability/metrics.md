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

The metrics are presented through **two complementary Grafana dashboards**, split by aggregation dimension (see [Metrics Providers](providers.md)):

- **[BanyanDB Cluster — Nodes](../grafana-fodc-nodes.json)** — node/pod-level health and resources, aggregated by **`pod_name`**: *Fleet Overview*, *Per-node Health*, *Topology: Pod-to-Pod Flows*, *Resources*, *Disk by Path*, and *Go Runtime*.
- **[BanyanDB Cluster — Workload](../grafana-fodc-workload.json)** — business/data-level throughput and latency, aggregated by **`group`**: *Cluster Workload Summary*, *Liaison: Ingestion, Query & Publish*, *Data: Storage*, *Data: Inverted Index*, and *Data: Internal Queue*.

The sections below mirror the two dashboards row-for-row; each metric entry corresponds to one panel and uses that panel's expression. A standalone [Internal queue metrics reference](#internal-queue-metrics-reference-queue_sub--queue_pub) at the end documents the `queue_sub` / `queue_pub` model in depth.

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

### Total CPU Cores

The total number of CPU cores across the selected nodes.

**Expression**: `sum(banyandb_system_cpu_num{job=~"$job", container_name=~"$role", pod_name=~"$pod"})`

### Total Memory Used

The total resident memory used across the selected nodes, in bytes.

**Expression**: `sum(banyandb_system_memory_state{job=~"$job", container_name=~"$role", pod_name=~"$pod", kind="used"})`

### Total Disk Used

The total disk space used across the selected nodes, in bytes (summed over all storage paths). See **Resources → Disk Usage %** for the used/total percentage and **Disk by Path** for the per-path breakdown.

**Expression**: `sum(banyandb_system_disk{job=~"$job", container_name=~"$role", pod_name=~"$pod", kind="used"})`

### Node Uptime

The per-node uptime gauge. A value dropping to ~0 indicates a restart; a series disappearing indicates a node that stopped reporting.

**Expression**: `banyandb_system_up_time{job=~"$job", container_name=~"$role", pod_name=~"$pod"}` (one series per `pod_name`)

## Per-node Health

A single table joining the key per-node signals so an unhealthy node stands out at a glance. The columns are the metrics documented in **Resources** below, each reduced `by (pod_name)`:

- Uptime — `banyandb_system_up_time{…}`
- CPU (cores) — `sum(rate(process_cpu_seconds_total{…}[$__rate_interval])) by (pod_name)`
- RSS — `sum(process_resident_memory_bytes{…}) by (pod_name)`
- Memory % — `max(banyandb_system_memory_state{…, kind="used_percent"}) by (pod_name)`
- Disk % — `sum(banyandb_system_disk{…, kind="used"}) by (pod_name) / sum(banyandb_system_disk{…, kind="total"}) by (pod_name)`

## Topology: Pod-to-Pod Flows

A single **table** in which each row is one directed flow **`source` → `target`** (bare pod names) per `operation`, showing the **publisher's view and the subscriber's view of the same traffic side by side**: `Pub msg/s` / `Sub msg/s`, `Pub p99` / `Sub p99`, `Pub err/s` / `Sub err/s`, and `Pub B/s` / `Sub B/s`.

The two sides record the same edge with mirrored labels — on a `queue_pub` series the scrape target (`pod_name`) is the sender and `remote_node` (a full DNS node name) the receiver; on a `queue_sub` series it is the inverse. Each query therefore canonicalizes the edge key with `label_replace`: the full DNS `remote_node` is shortened to its first label (which equals the peer's `pod_name`), and source/target are swapped on the sub side. All eight queries then emit the same `(source, target, operation)` key, and Grafana's **merge** transformation folds them into one row per edge:

**Pub side (sender view; the `or` of each metric over two selectors implements the `$pod`/`$role` *edge-involvement* filter — see below — and the tier-migration mirror is unioned in the same way so lifecycle edges get a pub column too):**

```promql
sum by (source, target, operation) (label_replace(label_replace(
  rate(banyandb_queue_pub_total_finished{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])
    or rate(banyandb_queue_pub_total_finished{job=~"$job", remote_role=~"$role", remote_node=~"($pod)\\..*"}[$__rate_interval])
    or rate(banyandb_lifecycle_migration_total_finished{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])
    or rate(banyandb_lifecycle_migration_total_finished{job=~"$job", remote_role=~"$role", remote_node=~"($pod)\\..*"}[$__rate_interval]),
  "source", "$1", "pod_name", "(.*)"), "target", "$1", "remote_node", "([^.:]+).*"))
```

**Sub side (receiver view; inverted mapping — `total_message_finished` is the receiver's per-message catalog, the unit-for-unit match to the publisher's per-message `total_finished`):**

```promql
sum by (source, target, operation) (label_replace(label_replace(
  rate(banyandb_queue_sub_total_message_finished{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])
    or rate(banyandb_queue_sub_total_message_finished{job=~"$job", remote_role=~"$role", remote_node=~"($pod)\\..*"}[$__rate_interval]),
  "source", "$1", "remote_node", "([^.:]+).*"), "target", "$1", "pod_name", "(.*)"))
```

The p99 columns apply the same relabeling inside `histogram_quantile(0.99, sum by (le, source, target, operation) (…))`; the error and byte columns swap in `_total_err` and `sent_bytes` / `received_bytes`. **Tier-migration edges pair up naturally**: the pub view is `banyandb_lifecycle_migration_*` (whose `pod_name` is the data pod the sidecar shares) and the sub view is the receiver's `queue_sub` series with `remote_role="lifecycle"` (whose sender shortens to the same data pod) — the keys align, so a migration row also carries both sides.

Reading the table:

- The two sides describe the **same traffic — never add them**. Both `Pub msg/s` (`queue_pub_total_finished`) and `Sub msg/s` (`queue_sub_total_message_finished`) now count the **same per-message unit**, so a healthy steady-state edge shows them ≈ equal on every edge — including the liaison→liaison **tier-1 write routing**, where each write the SkyWalking OAP sends to its connected liaison is re-published **round-robin to the owner liaison** (including itself), one wire stream per OAP write stream, gathered by the receiver and dispatched **once at stream EOF** — the receiver's message catalog still tallies each message in that batch, so the columns match. An *unsteady* or growing `Pub`/`Sub` gap is backlog or loss in flight. **Caveat:** `Sub p99` reads the per-stream `queue_sub_total_latency`, so for that batch-mode traffic it times the whole batch dispatch, not one message — compare it against `Pub p99` (per-message send) only with that in mind.
- A populated Pub cell with an **empty Sub cell** (or vice versa) is also signal: an uninstrumented side (older servers record `query`/`control` only on the receiver) or a one-sided counter (`sent_bytes` exists only on pub/migration, `received_bytes` only on sub; both are file-sync-only).
- Empty `err` cells mean **no errors** (the counters are lazily registered).
- `$pod` / `$role` select flows **involving** a matching node on **either end**. A naive `pod_name=~"$pod"` filter would hit the two sides asymmetrically (pub rows are scraped from the sender, sub rows from the receiver) and split the merged rows; instead every query unions two selectors — one matching the scrape-target side (`pod_name` / `container_name`) and one matching the remote side (`remote_node=~"($pod)\\..*"`, a prefix match on the full DNS node name, and `remote_role=~"$role"`, which shares the `liaison`/`data`/`lifecycle` vocabulary with `container_name`) — so both views of a surviving edge stay in the row. There is deliberately **no `$group` filter**: `query`/`control` and the tier-1 write routing carry `group=""`, which an all-value `.+` silently drops — validated against the live cluster, where a group filter hid all publisher-side query/control rows.
- For the full directed-graph rendering of the same data (with node health and the structural `calls` layer), see [Cluster Topology Rendering](../fodc/topology.md).

## Resources

`Resources` metrics monitor per-node resource usage.

### CPU Usage

CPU cores consumed per node. Sustained high utilization correlates with rising query/merge latency.

**Expression**: `sum(rate(process_cpu_seconds_total{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (pod_name)`

### RSS Memory

Resident set size per node, in bytes. The peak over the rate interval is used so transient spikes are not hidden.

**Expression**: `max by (pod_name) (max_over_time(process_resident_memory_bytes{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval]))`

### System Memory %

The percentage of system memory used per node. As it approaches the memory protector limit (`--allowed-percent`, default 75), query execution is throttled, so high memory surfaces as query slowdown first.

**Expression**: `max(banyandb_system_memory_state{job=~"$job", container_name=~"$role", pod_name=~"$pod", kind="used_percent"}) by (pod_name)`

### Disk Usage %

The fraction of disk used per node (summed over all storage paths). BanyanDB rejects writes with `STATUS_DISK_FULL` once usage crosses the disk-full threshold (default 95%), so alert well before that — commonly at ~80–85%.

**Expression**: `sum(banyandb_system_disk{job=~"$job", container_name=~"$role", pod_name=~"$pod", kind="used"}) by (pod_name) / sum(banyandb_system_disk{job=~"$job", container_name=~"$role", pod_name=~"$pod", kind="total"}) by (pod_name)`

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

**Expression**: `sum(banyandb_system_disk{job=~"$job", container_name=~"$role", pod_name=~"$pod", kind="used"}) by (pod_name, path) / sum(banyandb_system_disk{job=~"$job", container_name=~"$role", pod_name=~"$pod", kind="total"}) by (pod_name, path)`

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

---

# Workload dashboard

Business- and data-level throughput and latency, aggregated per `group`. Use this dashboard to see which business group / operation is slow, erroring, or backlogged.

## Cluster Workload Summary

Cluster-wide RED stats. Each summand is wrapped in `or vector(0)` so a not-yet-registered counter reports `0` rather than "No Data".

### Cluster Write Rate

Total write operations per second across measures, streams and traces.

**Expression**: `(sum(rate(banyandb_measure_total_written{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) or vector(0)) + (sum(rate(banyandb_stream_tst_total_written{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) or vector(0)) + (sum(rate(banyandb_trace_tst_total_written{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) or vector(0))`

### Cluster Query Rate

Query operations per second on the liaison servers.

**Expression**: `sum(rate(banyandb_liaison_grpc_total_started{job=~"$job", container_name=~"$role", pod_name=~"$pod", method="query"}[$__rate_interval]))`

### Error Rate

Cluster error rate per minute, combining liaison gRPC, registry, stream-receive, queue-publisher, and liaison write-queue sync-loop errors. Several of these counters are lazily registered (absent until the first error), hence the `or vector(0)` guards.

**Expression**: `(sum(rate(banyandb_liaison_grpc_total_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_liaison_grpc_total_registry_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_liaison_grpc_total_stream_msg_received_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_queue_pub_total_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_measure_total_sync_loop_err{job=~"$job", container_name="liaison", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_stream_tst_total_sync_loop_err{job=~"$job", container_name="liaison", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_trace_tst_total_sync_loop_err{job=~"$job", container_name="liaison", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0))`

## Liaison: Ingestion, Query & Publish

The liaison node's front door — gRPC ingestion/query, the schema registry, the on-disk write queue (wqueue), and the tier-2 publish pipeline to data nodes.

### Query Rate by Service

Query rate split by the `service` label (measure / stream / trace / property / topn).

**Expression**: `sum(rate(banyandb_liaison_grpc_total_started{job=~"$job", container_name=~"$role", pod_name=~"$pod", method="query"}[$__rate_interval])) by (service)`

### Query Latency by Group

Average query latency in seconds, per `group` — total latency divided by total started. Because a distributed query fans out to all nodes, the slowest group dominates the overall query latency.

**Expression**: `sum(rate(banyandb_liaison_grpc_total_latency{job=~"$job", container_name=~"$role", pod_name=~"$pod", method="query"}[$__rate_interval])) by (group) / sum(rate(banyandb_liaison_grpc_total_started{job=~"$job", container_name=~"$role", pod_name=~"$pod", method="query"}[$__rate_interval])) by (group)`

### gRPC Error Rate

Liaison error rate, broken down by `service`/`method`, plus registry and stream-receive errors. All terms are lazily registered, so they are simply absent on a healthy cluster.

**Expression1 (grpc, by service/method)**: `sum(rate(banyandb_liaison_grpc_total_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (service, method)`
**Expression2 (registry)**: `sum(rate(banyandb_liaison_grpc_total_registry_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval]))`
**Expression3 (stream recv)**: `sum(rate(banyandb_liaison_grpc_total_stream_msg_received_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval]))`

### Registry Operation Rate

The rate of schema-registry operations and non-query gRPC calls.

**Expression1 (registry)**: `sum(rate(banyandb_liaison_grpc_total_registry_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval]))`
**Expression2 (non-query)**: `sum(rate(banyandb_liaison_grpc_total_started{job=~"$job", container_name=~"$role", pod_name=~"$pod", method!="query"}[$__rate_interval]))`

### Write Rate by Group

Write operations per second on the liaison, per `group`, for measures, streams and traces (charted as separate series rather than added, since each data type uses different `group` values).

**Expression1**: `sum(rate(banyandb_measure_total_written{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression2**: `sum(rate(banyandb_stream_tst_total_written{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression3**: `sum(rate(banyandb_trace_tst_total_written{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Publish Throughput & Success

The tier-2 publisher's success throughput by `operation`, alongside file-sync bytes sent by `group`. See the [queue reference](#internal-queue-metrics-reference-queue_sub--queue_pub) for the full model.

**Expression1 (success)**: `sum(rate(banyandb_queue_pub_total_finished{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (operation)`
**Expression2 (bytes)**: `sum(rate(banyandb_queue_pub_sent_bytes{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Write Queue Length (wqueue)

The liaison buffers writes in an **on-disk write queue (wqueue)** before syncing each part to the data nodes. These gauges (scoped with the literal `container_name="liaison"`) expose the queue depth per `pod_name`/`group`. The wqueue accumulates on disk — it does not drop data after a fixed window — so a sustained rise in any of these, especially `pending`, signals a slow or unavailable downstream data node and can eventually fill the liaison disk (`STATUS_DISK_FULL`). The matching `*_total_sync_loop_*` counters (`started`/`finished`/`latency`/`bytes`, and the lazily-registered `*_total_sync_loop_err`) describe the loop that drains it.

**Expression1 (file parts)**: `sum(banyandb_{measure,stream_tst,trace_tst}_total_file_parts{job=~"$job", container_name="liaison", pod_name=~"$pod"}) by (pod_name, group)`
**Expression2 (mem parts)**: `sum(banyandb_{measure,stream_tst,trace_tst}_total_mem_part{job=~"$job", container_name="liaison", pod_name=~"$pod"}) by (pod_name, group)`
**Expression3 (pending)**: `sum(banyandb_{measure,stream_tst,trace_tst}_pending_data_count{job=~"$job", container_name="liaison", pod_name=~"$pod"}) by (pod_name, group)`

> The panel charts all three families for `measure`, `stream_tst` and `trace_tst` as separate series (nine queries); the `{measure,stream_tst,trace_tst}` brace above is shorthand for the three concrete metric names.

### Publish Send Latency p99

The 99th-percentile tier-2 send latency, per `operation`, from the publisher latency histogram.

**Expression**: `histogram_quantile(0.99, sum(rate(banyandb_queue_pub_total_latency_bucket{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (le, operation))`

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

Average merge-file latency in seconds, per `group` (total merge latency ÷ merge operations). A surge indicates slow merges, often from high disk I/O.

**Expression1**: `sum(rate(banyandb_measure_total_merge_latency{job=~"$job", container_name=~"$role", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_measure_total_merge_loop_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression2**: `sum(rate(banyandb_stream_tst_total_merge_latency{job=~"$job", container_name=~"$role", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_stream_tst_total_merge_loop_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression3**: `sum(rate(banyandb_trace_tst_total_merge_latency{job=~"$job", container_name=~"$role", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_trace_tst_total_merge_loop_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Merge File Partitions

Average number of partitions merged per merge-file operation, per `group` (merged parts ÷ merge operations). A surge suggests the server is under heavy write load.

**Expression1**: `sum(rate(banyandb_measure_total_merged_parts{job=~"$job", container_name=~"$role", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_measure_total_merge_loop_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression2**: `sum(rate(banyandb_stream_tst_total_merged_parts{job=~"$job", container_name=~"$role", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_stream_tst_total_merge_loop_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression3**: `sum(rate(banyandb_trace_tst_total_merged_parts{job=~"$job", container_name=~"$role", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_trace_tst_total_merge_loop_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`

## Data: Inverted Index

`Inverted Index` metrics monitor the series index of measures and streams, grouped by the `group` tag. Rapid growth or churn signals a cardinality problem that bloats the index and slows queries.

### Series Write Rate

Series index update operations per second, per `group`, for measures and streams.

**Expression1 (measure)**: `sum(rate(banyandb_measure_inverted_index_total_updates{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression2 (stream)**: `sum(rate(banyandb_stream_storage_inverted_index_total_updates{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Series Term Search Rate

Series term-search operations per second, per `group`. A high value means reads are fetching many series — often a high-cardinality symptom.

**Expression1 (measure)**: `sum(rate(banyandb_measure_inverted_index_total_term_searchers_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression2 (stream)**: `sum(rate(banyandb_stream_storage_inverted_index_total_term_searchers_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Total Series

The total number of series (index documents) stored, per `group`, for measures and streams.

**Expression1 (measure)**: `sum(banyandb_measure_inverted_index_total_doc_count{job=~"$job", container_name=~"$role", pod_name=~"$pod"}) by (group)`
**Expression2 (stream)**: `sum(banyandb_stream_storage_inverted_index_total_doc_count{job=~"$job", container_name=~"$role", pod_name=~"$pod"}) by (group)`

### Stream tst: Write Rate

Write rate into the stream **time-series-table (tst) inverted index** (the per-element index, distinct from the series index above), per `group`.

**Expression**: `sum(rate(banyandb_stream_tst_inverted_index_total_updates{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Stream tst: Term Search Rate

Term-search rate on the stream tst inverted index, per `group`.

**Expression**: `sum(rate(banyandb_stream_tst_inverted_index_total_term_searchers_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Stream tst: Total Documents

Total documents in the stream tst inverted index, per `group`.

**Expression**: `sum(banyandb_stream_tst_inverted_index_total_doc_count{job=~"$job", container_name=~"$role", pod_name=~"$pod"}) by (group)`

## Data: Internal Queue

One panel per queue **operation**, each showing subscribe throughput (`started`/`finished`) and p99 latency, broken down by `group`. These are the data-node (subscribe) view of the internal queue; the full model is in the [queue reference](#internal-queue-metrics-reference-queue_sub--queue_pub). The four operations are `query`, `file-sync`, `batch-write` and `control`.

Each panel uses the same three expressions, with `operation` pinned to the panel's value (`<op>` ∈ `query` | `file-sync` | `batch-write` | `control`):

**Started**: `sum(rate(banyandb_queue_sub_total_started{job=~"$job", container_name=~"$role", pod_name=~"$pod", operation="<op>"}[$__rate_interval])) by (group)`
**Finished**: `sum(rate(banyandb_queue_sub_total_finished{job=~"$job", container_name=~"$role", pod_name=~"$pod", operation="<op>"}[$__rate_interval])) by (group)`
**p99 latency**: `histogram_quantile(0.99, sum(rate(banyandb_queue_sub_total_latency_bucket{job=~"$job", container_name=~"$role", pod_name=~"$pod", operation="<op>"}[$__rate_interval])) by (le, group))`

> `batch-write` covers both plain writes and the secondary-index sync (measure/stream series-index, stream local-index, trace sidx-series). All of these carry their business `group`, so the per-group breakdown is complete for every operation.

---

# Internal queue metrics reference (`queue_sub` / `queue_pub`)

Liaison nodes run an internal gRPC **queue server** (`server-queue-sub`, wired via `sub.NewServerWithPorts` in `pkg/cmdsetup/liaison.go`) and **queue clients** (`server-queue-pub`) for the tier-1/tier-2 pipelines. Prometheus metrics use the namespaces `banyandb_queue_sub_*` and `banyandb_queue_pub_*` (built from `observability.RootScope` + `queue_sub` / `queue_pub` sub-scopes). Data nodes expose the same families where the corresponding services run.

Both namespaces share one model: the base metrics `total_started`, `total_finished`, `total_latency` (a histogram), and `total_err`, labeled by `operation` (`batch-write` / `file-sync` / `query` / `control`) and `group`, plus the **remote endpoint** of the flow — `remote_node` (the peer's BanyanDB node name, equal to its `/cluster/topology` `metadata.name`), `remote_role` (`liaison` / `data`, plus `lifecycle` on `queue_sub` for inbound tier-migration traffic — see the [lifecycle migration family](#lifecycle_migration--the-tier-migration-mirror-of-queue_pub)), and `remote_tier` (`hot` / `warm` / `cold`, data only). `total_err` adds an `error_type` label. File-sync additionally exposes byte counters: `sent_bytes` (pub) and `received_bytes` (sub). The **local** end of each flow is the scrape target itself (`pod_name` / `node_role` / `node_type` from the FODC proxy), so joining the scrape labels with the `remote_*` labels reconstructs the liaison↔data(hot/warm/cold) call graph. For a worked recipe that fuses these metrics with the FODC `/cluster/topology` node inventory to render the topology, see [Cluster Topology Rendering](../fodc/topology.md).

### `queue_sub` — inbound server

| Metric (suffix after `banyandb_queue_sub_`) | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `total_started`, `total_finished` | Counter | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Dispatch units started / finished: per message for ordinary traffic, per part for file-sync; **batch-mode** streams (the liaison tier-1 write routing) count once per collected batch, dispatched at stream EOF, with `total_finished` stamped when the stream ends. A persistent `started − finished` gap indicates backlog or stuck handlers. |
| `total_latency` | Histogram | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Handling latency (`_bucket` / `_sum` / `_count`); use `histogram_quantile` for p50/p99. |
| `total_err` | Counter | …, `error_type` | Errors by type. Lazily registered, so absent (not zero) on a healthy cluster. |
| `received_bytes` | Counter | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Bytes received, **file-sync only** (`operation="file-sync"`). |
| `total_message_started`, `total_message_finished` | Counter | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | **Message catalog** (`queue_sub` only) — one count per dispatched message on every path: per message for ordinary traffic, per part for file-sync, and per message *within* a batch-mode stream (where `total_started`/`total_finished` count the whole batch once). This is the unit-for-unit match to the publisher's per-message `total_finished`, so the two sides compare ≈1:1; `total_message_started − total_message_finished` is a unit-consistent subscribe-backlog gap. |
| `total_batch_started`, `total_batch_finished`, `total_batch_latency` | Counter / Histogram | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | **Batch catalog** — one count per batch-mode wire stream: started at stream open, finished + latency observed at EOF dispatch. `total_batch_latency` buckets extend to ~300s for minutes-long migration/replay streams. Also present on `queue_pub` and the `lifecycle_migration` mirror. |

**Troubleshooting:** a growing `total_started − total_finished` gap (or rising `total_latency` p99) for a `group`/`operation` points at slow or stuck consumers. `total_err` broken down by `error_type` distinguishes transport issues (`stream_error`, `recv_error`, `checksum_mismatch`, `out_of_order`) from completion issues (`finish_sync_err`, `part_failed`). For file-sync, `received_bytes` together with `total_latency` separates partial completion from healthy throughput. On data nodes, series with `remote_role="lifecycle"` are inbound **tier-migration** traffic from the lifecycle service (which stamps its identity — its co-located data node's name and tier — on the wire), distinct from the liaison request pipeline.

### `queue_pub` — outbound batch client

| Metric (suffix after `banyandb_queue_pub_`) | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `total_started`, `total_finished` | Counter | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Sends started / finished per operation and target node; `total_finished` is the success rate. |
| `total_latency` | Histogram | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Send latency (`_bucket` / `_sum` / `_count`); use `histogram_quantile` for p99. |
| `total_err` | Counter | …, `error_type` | Send errors by type. `error_type` is one of `non_transient`, `canceled`, `stream_canceled`, `retry_exhausted`, `recv_error`, `server_rejected`, `send_error`, `decode_error`, `invalid_topic` (Send/publish path) or `stream_error`, `recv_error`, `checksum_mismatch`, `out_of_order`, `session_not_found`, `completion_error` (file-sync). Lazily registered. |
| `sent_bytes` | Counter | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Bytes sent, **file-sync only**. |
| `total_batch_started`, `total_batch_finished`, `total_batch_latency` | Counter / Histogram | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | **Batch catalog** — one count per batch-mode wire stream: started at stream open (`group=""`), finished + latency observed when the stream's final response is received. Note `total_batch_finished` means the final response arrived, **not** that every message was acked (per-message acks await a future `cluster.v1.SendResponse.accepted_count`). `total_batch_latency` buckets extend to ~300s. Mirrored on `queue_sub` and the `lifecycle_migration` family. |

**Troubleshooting:** `total_err` by `error_type` separates transport failures (`recv_error`) from application-level `SendResponse` errors (`server_rejected`) and exhausted retries (`retry_exhausted`). A persistent `total_started − total_finished` gap, or rising `total_latency` p99 for a given `remote_node` / `remote_tier`, indicates slow or unavailable data nodes.

Metrics are only registered when `metadata` implements `metadata.Service` and `MetricsRegistry()` is non-nil (e.g. after `SetMetricsRegistry` in bootstrap). `NewWithoutMetadata(omr)` — used by the lifecycle service's tier-migration publisher — leaves the regular `queue_pub` family disabled and registers the same instrument set under `banyandb_lifecycle_migration_*` instead (see [below](#lifecycle_migration--the-tier-migration-mirror-of-queue_pub)). The `total_err` counters above are registered lazily on first occurrence, so they are simply absent (not zero) on a healthy cluster.

### `lifecycle_migration` — the tier-migration mirror of `queue_pub`

The **lifecycle** service migrates data hot→warm→cold through a queue client built without a metadata service (`pub.NewWithoutMetadata`), so its traffic does **not** appear under `banyandb_queue_pub_*`. Instead the same instruments — `total_started`, `total_finished`, `total_latency`, `total_err`, `sent_bytes`, plus the batch catalog `total_batch_started`/`total_batch_finished`/`total_batch_latency` — are registered under **`banyandb_lifecycle_migration_*`**, with the same labels (`operation`, `group`, `remote_node`, `remote_role`, `remote_tier`, plus `error_type` on `total_err`), so the migration layer can be queried independently of the write pipeline. `operation` follows the underlying traffic: `file-sync` for migrated part shipping and `batch-write` for row replay.

The lifecycle sidecar serves these series (plus the run-health gauges below) at `/metrics` on its own HTTP port (`--lifecycle-http-port`, default `17915`) instead of the `2121` observability listener; the co-located FODC agent polls that port, so the series arrive through the proxy scrape with `container_name="lifecycle"` and the `pod_name` shared with the co-located data node. On the **receiving** data node the same flows are mirrored as `banyandb_queue_sub_*` series with `remote_role="lifecycle"` — the migration publisher stamps its identity (its co-located data node's name and tier) onto the wire.

**Example:** migration throughput per target node — `sum(rate(banyandb_lifecycle_migration_total_finished{job=~"$job"}[$__rate_interval])) by (operation, remote_node)`; errors — `sum(rate(banyandb_lifecycle_migration_total_err{job=~"$job"}[$__rate_interval])) by (error_type)`.

For the worked recipe that turns this family into weighted hot→warm→cold edges in a topology graph, see [Cluster Topology Rendering](../fodc/topology.md#lifecycle-migration-layer).

### Lifecycle run health (`banyandb_lifecycle_*`)

The lifecycle scheduler also exposes three service-level series:

| Metric | Type | Meaning |
| --- | --- | --- |
| `banyandb_lifecycle_cycles_total` | Counter | Migration cycles executed by the scheduler. |
| `banyandb_lifecycle_last_run_timestamp_seconds` | Gauge | Unix time at which the most recent migration run **started**; stamped on every return path (success, error, recovered panic). |
| `banyandb_lifecycle_last_run_success` | Gauge | Outcome of the most recent run: `1` success, `0` failure. |

**Suggested alerts:** `banyandb_lifecycle_last_run_success == 0`, and `time() - banyandb_lifecycle_last_run_timestamp_seconds` exceeding the migration schedule interval (a missed run).

### Example PromQL snippets

Saturation (scope by node with the proxy labels):

- **Subscribe throughput:** `sum(rate(banyandb_queue_sub_total_started{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (operation)` (and the matching `banyandb_queue_sub_total_finished`)
- **Subscribe p99 latency:** `histogram_quantile(0.99, sum(rate(banyandb_queue_sub_total_latency_bucket{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (le, operation))`
- **File-sync bytes received:** `sum(rate(banyandb_queue_sub_received_bytes{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (group)`
- **Publisher success rate:** `sum(rate(banyandb_queue_pub_total_finished{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (operation)`
- **Publisher errors by type:** `sum(rate(banyandb_queue_pub_total_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (operation, error_type)`
- **Publisher file-sync bytes sent (by tier):** `sum(rate(banyandb_queue_pub_sent_bytes{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])) by (remote_tier)`

**Suggested alerts (tune thresholds per cluster):**

- Non-zero sustained `rate(banyandb_queue_pub_total_err{error_type="retry_exhausted"}[5m])` on liaison.
- A sustained `rate(banyandb_queue_sub_total_started[5m]) - rate(banyandb_queue_sub_total_finished[5m])` gap (subscribe backlog), or `total_latency` p99 above an environment-specific ceiling, for a single `group` / `operation`.

### Aggregate pipeline error rate (optional)

To combine subscribe-side and publisher-side queue failures (per-minute scaling as elsewhere in this doc; each term wrapped in `or vector(0)` so a missing counter doesn't blank the result):

**Expression**: `(sum(rate(banyandb_queue_sub_total_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_queue_pub_total_err{job=~"$job", container_name=~"$role", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0))`
