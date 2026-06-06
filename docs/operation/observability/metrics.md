# Metrics

BanyanDB exposes metrics for monitoring and analysis.

> **Scrape source — the FODC proxy.** In a cluster deployment, metrics are collected by scraping the **FODC proxy** `/metrics` endpoint (see [FODC overview](../fodc/overview.md)), which is the **single Prometheus scrape target**. The proxy aggregates every BanyanDB node's metrics and adds per-node identity labels, so all the PromQL below is written for that scheme:
>
> - `$job` — the Prometheus scrape job for the FODC proxy.
> - `$pod` — a BanyanDB node, matched via the **`pod_name`** label (the full node identity, e.g. `banyandb-data-hot-0`).
> - `$role` — the node role, matched via the **`container_name`** label (`liaison` or `data`).
>
> Because the proxy is the only target, the Prometheus-synthesized `instance`, `job`, and `up` labels describe the **proxy**, not individual BanyanDB nodes — use `$pod` / `$role` to scope a query to a node. Original BanyanDB labels (`group`, `kind`, `method`, `service`, `topic`, `node`, …) are preserved on every sample. `$__rate_interval` is the Grafana rate-interval variable.
>
> (If you are *not* running the FODC proxy, BanyanDB also exposes its own metrics on port `2121`; scrape each pod directly and substitute the Kubernetes `pod`/`instance` target labels for `$pod` below.)

## Stats

`Stats` metrics are used to monitor the overall status of BanyanDB. The following metrics are available:

### Write Rate

The write rate is the number of write operations per second. It is calculated by summing the total number of written operations for measures, streams and traces.

**Expression**: `sum(rate(banyandb_measure_total_written{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) + sum(rate(banyandb_stream_tst_total_written{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) + sum(rate(banyandb_trace_tst_total_written{job=~"$job", pod_name=~"$pod"}[$__rate_interval]))`

### Total Memory

The total memory is the total physical memory available on the system, which means total amount of RAM on the system.

**Expression**: `sum(banyandb_system_memory_state{job=~"$job", pod_name=~"$pod", kind="total"})`

### Disk Usage

The total disk space used across the selected nodes, in bytes (summed over all storage paths). See **Resource Usage → Disk Usage** below for the used/total percentage.

**Expression**: `sum(banyandb_system_disk{job=~"$job", pod_name=~"$pod", kind="used"})`

### Query Rate

The query rate is the number of query operations per second. It is the query rate on the liaison server.

**Expression**: `sum(rate(banyandb_liaison_grpc_total_started{job=~"$job", pod_name=~"$pod", method="query"}[$__rate_interval]))`

### Total CPU

The total CPU is the total number of CPUs available on the system.

**Expression**: `sum(banyandb_system_cpu_num{job=~"$job", pod_name=~"$pod"})`

### Write and Query Errors Rate

The write and query errors rate is the number of write and query errors per minute. It is calculated by summing the total number of write and query errors from liaison and data servers.

Each term is wrapped in `or vector(0)` so the panel reports `0` rather than "No Data" when an error counter has not been registered yet (several error counters are lazily registered on first occurrence).

**Expression**: `(sum(rate(banyandb_liaison_grpc_total_err{job=~"$job", pod_name=~"$pod", method="query"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_liaison_grpc_total_stream_msg_sent_err{job=~"$job", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_liaison_grpc_total_stream_msg_received_err{job=~"$job", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_queue_sub_total_err{job=~"$job", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0))`

### Registry Operation Rate

The registry operation rate is the number of registry operations per second. It is calculated by summing the total number of registry operations.

**Expression**: `(sum(rate(banyandb_liaison_grpc_total_registry_started{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) or vector(0)) + (sum(rate(banyandb_liaison_grpc_total_started{job=~"$job", pod_name=~"$pod", method!="query"}[$__rate_interval])) or vector(0))`

### Active Instances

The number of BanyanDB nodes currently reporting through the FODC proxy. (The Prometheus `up` metric reflects the proxy target, not individual nodes, so node liveness is derived from the per-node `banyandb_system_up_time` gauge instead.)

**Expression**: `count(banyandb_system_up_time{job=~"$job", pod_name=~"$pod"})`

## Resource Usage

`Resource Usage` metrics are used to monitor the resource usage of BanyanDB on the node. The following metrics are available:

### CPU Usage

The CPU usage is the fraction of CPU used per node. If it is over 80%, it may indicate that the CPU is overloaded.

**Expression**: `max(rate(process_cpu_seconds_total{job=~"$job", pod_name=~"$pod"}[$__rate_interval]) / banyandb_system_cpu_num{job=~"$job", pod_name=~"$pod"}) by (pod_name)`

### RSS memory usage

The RSS memory usage is the fraction of system memory held as resident memory per node. If it is over 80%, it may indicate that the memory is almost full.

**Expression**: `max_over_time(process_resident_memory_bytes{job=~"$job", pod_name=~"$pod"}[$__rate_interval]) / on(pod_name) group_left() sum(banyandb_system_memory_state{job=~"$job", pod_name=~"$pod", kind="total"}) by (pod_name)`

### Disk Usage

The disk usage is the percentage of disk space used per node. If the disk usage is over 80%, it may indicate that the disk is almost full.

**Expression**: `sum(banyandb_system_disk{job=~"$job", pod_name=~"$pod", kind="used"}) by (pod_name) / sum(banyandb_system_disk{job=~"$job", pod_name=~"$pod", kind="total"}) by (pod_name)`

### Network Usage

The network usage is the number of bytes sent and received per second.

**Expression1**: `sum(rate(banyandb_system_net_state{job=~"$job", pod_name=~"$pod", kind="bytes_recv"}[$__rate_interval])) by (pod_name, name)`

**Expression2**: `sum(rate(banyandb_system_net_state{job=~"$job", pod_name=~"$pod", kind="bytes_sent"}[$__rate_interval])) by (pod_name, name)`

## Storage

`Storage` metrics are used to monitor the storage status of BanyanDB. The following metrics are available:

### Write Rate

The write rate is the number of write operations per second for measures, streams and traces, grouped by the `group` tag. The three data types use different `group` values, so they are charted as separate series rather than added together.

You can view the write rate of different nodes (`pod_name`) to find out the hot node.

**Expression1**: `sum(rate(banyandb_measure_total_written{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression2**: `sum(rate(banyandb_stream_tst_total_written{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression3**: `sum(rate(banyandb_trace_tst_total_written{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Query Latency

The query latency is the average query latency in seconds. It is calculated by summing the total query latency and dividing by the total number of queries.

You can view the query latency of different nodes to find out the node with high query latency. Because BanyanDB will fetch all nodes to query, the node with high query latency will affect the overall query latency.

**Expression**: `sum(rate(banyandb_liaison_grpc_total_latency{job=~"$job", pod_name=~"$pod", method="query"}[$__rate_interval])) by (group) / sum(rate(banyandb_liaison_grpc_total_started{job=~"$job", pod_name=~"$pod", method="query"}[$__rate_interval])) by (group)`

### Total Data

The total data is the total number of data points stored in BanyanDB. It's grouped by the `group` tag.

You can view the total data of different nodes to find out the node with high data points. If the difference between the total data of different nodes is too large, it may indicate that the data is not evenly distributed.

**Expression1**: `sum(banyandb_measure_total_file_elements{job=~"$job", pod_name=~"$pod"}) by (group)`
**Expression2**: `sum(banyandb_stream_tst_total_file_elements{job=~"$job", pod_name=~"$pod"}) by (group)`
**Expression3**: `sum(banyandb_trace_tst_total_file_elements{job=~"$job", pod_name=~"$pod"}) by (group)`

### Merge File Rate

The merge file rate is the number of merge file operations per minute. It is calculated by summing the total number of merge file operations. It's grouped by the `group` tag.

If the value surges, it may indicate that too many small files are being merged. It may bring following problems:

- Increase the disk I/O
- Slow down the query performance
- Increase the CPU usage

**Expression1**: `sum(rate(banyandb_measure_total_merge_loop_started{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group) * 60`
**Expression2**: `sum(rate(banyandb_stream_tst_total_merge_loop_started{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group) * 60`
**Expression3**: `sum(rate(banyandb_trace_tst_total_merge_loop_started{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group) * 60`

### Merge File Latency

The merge file latency is the average merge file latency in seconds. It is calculated by summing the total merge file latency and dividing by the total number of merge file operations. It's grouped by the `group` tag.

If the value surges, it may indicate that the merge file operation is slow. It may be caused by the high disk I/O and other resource usage. It may bring following problems:

- Slow down the query performance
- Increase the CPU usage
- Increase the memory usage

**Expression1**: `sum(rate(banyandb_measure_total_merge_latency{job=~"$job", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_measure_total_merge_loop_started{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression2**: `sum(rate(banyandb_stream_tst_total_merge_latency{job=~"$job", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_stream_tst_total_merge_loop_started{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression3**: `sum(rate(banyandb_trace_tst_total_merge_latency{job=~"$job", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_trace_tst_total_merge_loop_started{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Merge File Partitions

The merge file partitions is the average number of partitions merged per merge file operation. It is calculated by summing the total number of partitions merged and dividing by the total number of merge file operations. It's grouped by the `group` tag.

If the value surges, it may indicate that too many partitions are being merged. It may because the partition number is too large that indicates the server is under a high write load.

**Expression1**: `sum(rate(banyandb_measure_total_merged_parts{job=~"$job", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_measure_total_merge_loop_started{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression2**: `sum(rate(banyandb_stream_tst_total_merged_parts{job=~"$job", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_stream_tst_total_merge_loop_started{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression3**: `sum(rate(banyandb_trace_tst_total_merged_parts{job=~"$job", pod_name=~"$pod", type="file"}[$__rate_interval])) by (group) / sum(rate(banyandb_trace_tst_total_merge_loop_started{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Series Write Rate

The series write rate is the number of series write operations per second. It is calculated by summing the total number of series write operations for measures and streams. It's grouped by the `group` tag.

If the value surges, it may indicate that the old series are being updated frequently by the new series. It may be caused by the high cardinality of the series and bring following problems:

- Increase the series inverted index size
- Slow down the query performance

**Expression1**: `sum(rate(banyandb_measure_inverted_index_total_updates{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression2**: `sum(rate(banyandb_stream_storage_inverted_index_total_updates{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`

#### Series Term Search Rate

The series term search rate is the number of series term search operations per second. It is calculated by summing the total number of series term search operations for measures and streams. It's grouped by the `group` tag.

If the value is too large, it may indicate that reading operation fetch too many series. It may be caused by the high cardinality of the series and bring following problems:

- Slow down the query performance
- Increase the CPU usage
- Increase the memory usage

**Expression1**: `sum(rate(banyandb_stream_storage_inverted_index_total_term_searchers_started{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`
**Expression2**: `sum(rate(banyandb_measure_inverted_index_total_term_searchers_started{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Total Series

The total series is the total number of series stored in BanyanDB. It's grouped by the `group` tag.

If the value is too large, it may indicate that the high cardinality of the series. It may bring following problems:

- Increase the series inverted index size
- Slow down the query performance

**Expression1**: `sum(banyandb_measure_inverted_index_total_doc_count{job=~"$job", pod_name=~"$pod"}) by (group)`
**Expression2**: `sum(banyandb_stream_storage_inverted_index_total_doc_count{job=~"$job", pod_name=~"$pod"}) by (group)`

## Stream Inverted Index

`Stream Inverted Index` metrics are used to monitor the stream inverted index status of BanyanDB. The following metrics are available:

### Stream Inverted Index Write Rate

The write rate is the number of write operations per second. It is calculated by summing the total number of written operations for streams. It's grouped by the `group` tag.

If the value is too large, it may indicate that too many data points are being indexed and bring following problems:

- Increase the inverted index size
- Slow down the query performance
- Increase the CPU usage
- Increase the memory usage

**Expression**: `sum(rate(banyandb_stream_tst_inverted_index_total_updates{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Term Search Rate

The term search rate is the number of term search operations per second. It is calculated by summing the total number of term search operations for streams. It's grouped by the `group` tag.

If the value is too large, it may indicate that reading operation fetch too many data points. It may bring following problems:

- Slow down the query performance
- Increase the CPU usage
- Increase the memory usage

**Expression**: `sum(rate(banyandb_stream_tst_inverted_index_total_term_searchers_started{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`

### Total Documents

The total documents is the total number of documents stored in the stream inverted index. It's grouped by the `group` tag.

If the value is too large, it may indicate that too many data points are being indexed and bring following problems:

- Increase the inverted index size
- Slow down the query performance
- Increase the CPU usage
- Increase the memory usage

**Expression**: `sum(banyandb_stream_tst_inverted_index_total_doc_count{job=~"$job", pod_name=~"$pod"}) by (group)`

## Liaison internal queue (`queue_sub` / `queue_pub`)

Liaison nodes run an internal gRPC **queue server** (`server-queue-sub`, wired via `sub.NewServerWithPorts` in `pkg/cmdsetup/liaison.go`) and **queue clients** (`server-queue-pub`) for tier-1/tier-2 pipelines. Prometheus metrics use the namespaces `banyandb_queue_sub_*` and `banyandb_queue_pub_*` (built from `observability.RootScope` + `queue_sub` / `queue_pub` sub-scopes). Data nodes may expose the same metric families where the corresponding services run.

Both namespaces share one model: the base metrics `total_started`, `total_finished`, `total_latency` (histogram), and `total_err`, labeled by `operation` (`batch-write` / `file-sync` / `query` / `control`) and `group`, plus the **remote endpoint** of the flow — `remote_node` (the peer's BanyanDB node name, equal to its `/cluster/topology` `metadata.name`), `remote_role` (`liaison` / `data`), and `remote_tier` (`hot` / `warm` / `cold`, data only). `total_err` adds an `error_type` label. File-sync additionally exposes byte counters: `sent_bytes` (pub) and `received_bytes` (sub). The **local** end of each flow is the scrape target itself (`pod_name` / `node_role` / `node_type` from the FODC proxy), so joining the scrape labels with the `remote_*` labels reconstructs the liaison↔data(hot/warm/cold) call graph.

### `queue_sub` — inbound server

| Metric (suffix after `banyandb_queue_sub_`) | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `total_started`, `total_finished` | Counter | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Subscribe RPCs started / finished, per operation and group. A persistent `started − finished` gap indicates backlog or stuck handlers. |
| `total_latency` | Histogram | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Handling latency (`_bucket` / `_sum` / `_count`); use `histogram_quantile` for p50/p99. |
| `total_err` | Counter | …, `error_type` | Errors by type. Lazily registered, so absent (not zero) on a healthy cluster. |
| `received_bytes` | Counter | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Bytes received, **file-sync only** (`operation="file-sync"`). |

**Troubleshooting:** a growing `total_started − total_finished` gap (or rising `total_latency` p99) for a `group`/`operation` points at slow or stuck consumers. `total_err` broken down by `error_type` distinguishes transport issues (`stream_error`, `recv_error`, `checksum_mismatch`, `out_of_order`) from completion issues (`finish_sync_err`, `part_failed`). For file-sync, `received_bytes` together with `total_latency` separates partial completion from healthy throughput.

### `queue_pub` — outbound batch client

| Metric (suffix after `banyandb_queue_pub_`) | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `total_started`, `total_finished` | Counter | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Sends started / finished per operation and target node; `total_finished` is the success rate. |
| `total_latency` | Histogram | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Send latency (`_bucket` / `_sum` / `_count`); use `histogram_quantile` for p99. |
| `total_err` | Counter | …, `error_type` | Send errors by type. `error_type` is one of `non_transient`, `canceled`, `stream_canceled`, `retry_exhausted`, `recv_error`, `server_rejected` (Send path) or `stream_error`, `recv_error`, `checksum_mismatch`, `out_of_order`, `session_not_found`, `completion_error` (file-sync). Lazily registered. |
| `sent_bytes` | Counter | `operation`, `group`, `remote_node`, `remote_role`, `remote_tier` | Bytes sent, **file-sync only**. |

**Troubleshooting:** `total_err` by `error_type` separates transport failures (`recv_error`) from application-level `SendResponse` errors (`server_rejected`) and exhausted retries (`retry_exhausted`). A persistent `total_started − total_finished` gap, or rising `total_latency` p99 for a given `remote_node` / `remote_tier`, indicates slow or unavailable data nodes.

Metrics are only registered when `metadata` implements `metadata.Service` and `MetricsRegistry()` is non-nil (e.g. after `SetMetricsRegistry` in bootstrap). `NewWithoutMetadata()` leaves `queue_pub` metrics disabled and logs a warning (`queue_pub metrics disabled: ...`). The `total_err` counters above are registered lazily on first occurrence, so they are simply absent (not zero) on a healthy cluster.

### Example PromQL snippets

Saturation (scope by node with the proxy labels):

- **Subscribe throughput:** `sum(rate(banyandb_queue_sub_total_started{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (operation)` (and the matching `banyandb_queue_sub_total_finished`)
- **Subscribe p99 latency:** `histogram_quantile(0.99, sum(rate(banyandb_queue_sub_total_latency_bucket{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (le, operation))`
- **Part-sync bytes received:** `sum(rate(banyandb_queue_sub_received_bytes{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (group)`
- **Publisher success rate:** `sum(rate(banyandb_queue_pub_total_finished{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (operation)`
- **Publisher errors by type:** `sum(rate(banyandb_queue_pub_total_err{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (operation, error_type)`
- **Publisher file-sync bytes sent (by tier):** `sum(rate(banyandb_queue_pub_sent_bytes{job=~"$job", pod_name=~"$pod"}[$__rate_interval])) by (remote_tier)`

**Suggested alerts (tune thresholds per cluster):**

- Non-zero sustained `rate(banyandb_queue_pub_total_err{error_type="retry_exhausted"}[5m])` on liaison.
- A sustained `rate(banyandb_queue_sub_total_started[5m]) - rate(banyandb_queue_sub_total_finished[5m])` gap (subscribe backlog), or `total_latency` p99 above an environment-specific ceiling, for a single `group` / `operation`.

### Aggregate pipeline error rate (optional)

To combine subscribe-side and publisher-side queue failures (per minute scaling as elsewhere in this doc; each term wrapped in `or vector(0)` so a missing counter doesn't blank the result):

**Expression**: `(sum(rate(banyandb_queue_sub_total_err{job=~"$job", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0)) + (sum(rate(banyandb_queue_pub_total_err{job=~"$job", pod_name=~"$pod"}[$__rate_interval])*60) or vector(0))`
