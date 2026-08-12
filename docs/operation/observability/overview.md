# Observability

This document outlines the observability features of BanyanDB, which include metrics, profiling, and tracing. These features help monitor and understand the performance, behavior, and overall health of BanyanDB.

This guide is split into:

- [Logging](logging.md) — log levels, per-module levels, and slow-query logging.
- [Metrics](metrics.md) — the full metric catalog (stats, resource usage, storage, trace plugins, inverted index, internal queue) with PromQL.
- [Metrics Providers](providers.md) — scraping metrics with Prometheus (through the FODC proxy), the Grafana dashboard, and native self-observability.
- [Profiling](profiling.md) — `pprof` endpoints.
- [Query Tracing](tracing.md) — per-query execution traces.

## Key Signals to Watch

If you watch nothing else, watch these. They are organized with the two canonical monitoring methods: **RED** (Rate, Errors, Duration) for the request paths — BanyanDB's write and query paths — and **USE** (Utilization, Saturation, Errors) for resources [1][2][3]. The detailed metric definitions are in the [Metrics](metrics.md) page; the thresholds here are **industry reference values to tune per cluster**, except where a BanyanDB default is named.

| # | Signal (method) | BanyanDB metric(s) | Watch for | Failure it predicts |
| --- | --- | --- | --- | --- |
| 1 | **Query latency, p99** (RED · Duration) | Query Latency (`banyandb_liaison_grpc_total_latency` ÷ `_total_started`) | p99 over a short (~1 min) window trending up | The **earliest** warning of saturation — latency degrades before requests start failing [1]. Track success vs. error latency separately (a slow error is worse than a fast one) [1]. |
| 2 | **Write & query rate** (RED · Rate) | Write Rate (`banyandb_*_total_written`), Query Rate (`banyandb_liaison_grpc_total_started{method="query"}`) | a sudden drop (ingestion stall) or spike (overload) vs. baseline | ingestion stall upstream, or a load surge that will drive saturation |
| 3 | **Error rate** (RED · Errors) | Error Rate — the dashboard combines `banyandb_liaison_grpc_total_err` + `_total_registry_err` + `_total_stream_msg_received_err`, the publisher's `banyandb_queue_pub_total_err`, and the liaison write-queue `banyandb_{measure,stream_tst,trace_tst}_total_sync_loop_err` (all `or vector(0)`, several lazily registered) | any sustained non-zero rate | failing writes/queries; correlate with the saturation signals below |
| 4 | **Disk utilization** (USE · Saturation) | Disk Usage % (`banyandb_system_disk` used ÷ total, per `pod_name`) | alert at **~80–85%** [4]; BanyanDB **rejects writes with `STATUS_DISK_FULL`** once usage crosses the disk-full threshold (**default 95%**): `{measure,stream,trace}-retention-high-watermark` on data/standalone nodes (which also drives forced retention cleanup down to the `-low-watermark`, default 85%), `{measure,stream,trace}-max-disk-usage-percent` on liaison nodes, and `property-max-disk-usage-percent` for property | a write outage — affected components go read-only [4] |
| 5 | **Memory / GC pressure** (USE · Saturation) | System Memory % (`banyandb_system_memory_state{kind="used_percent"}`), `process_resident_memory_bytes`, `go_gc_duration_seconds`, `go_memstats_*` | memory near the protector limit; rising GC pause | BanyanDB's **memory protector** (`--allowed-percent`, **default 75**, or `--allowed-bytes`) **stops query execution** to avoid OOM, so high memory shows up as query slowdown first [2] |
| 6 | **CPU utilization** (USE · Utilization) | CPU Usage (`rate(process_cpu_seconds_total)` ÷ `banyandb_system_cpu_num`) | sustained high utilization | rising query/merge latency |
| 7 | **Cross-node queue backpressure** (USE · Saturation) | `banyandb_queue_pub_total_started`−`total_finished` gap, `banyandb_queue_pub_total_err{error_type="retry_exhausted"}`; **wqueue** `banyandb_*_pending_data_count` / `*_total_file_parts` (liaison) | rising **depth _and_ duration** | a slow/unavailable data node. The liaison wqueue is **on disk**, so it accumulates (it does not drop data after a fixed window) and can eventually fill liaison disk → `STATUS_DISK_FULL` [5] |
| 8 | **Merge / compaction health** (LSM) | Merge File Rate/Latency/Partitions (`banyandb_*_total_merge_loop_started`, `_merge_latency`, `_merged_parts`), part counts (`*_total_file_parts`) | merge-latency spikes correlated with write-latency spikes; steadily growing part/partition counts | a compaction storm or backlog → write-latency spikes, query slowdown, oversized/broken parts [6] |
| 9 | **Cardinality / series growth** | Total Series (`banyandb_*_inverted_index_total_doc_count`), `_total_updates` (churn), `_total_term_searchers_started` | rapid `doc_count` growth, high churn, or high term-search rate | a cardinality explosion → memory pressure, inverted-index bloat, and slow queries |
| 10 | **Node liveness / membership** | Active Instances (`count(banyandb_system_up_time)` by `container_name`), per-node `banyandb_system_up_time` | reporting-node count below expected; a node's uptime dropping to ~0 (restart) or its series disappearing (gone) | lost capacity, under-replication, and query gaps |
| 11 | **Trace plugins** (RED) | [Plugin health and cost](metrics.md#trace-plugin-metrics-reference) | failures or p99 growth | sampling bypass or slow merges |

**Priority order:** 1–3 (RED) tell you whether users are affected *right now*; 4–7 (USE/backpressure) are the
**leading indicators** that catch trouble before it becomes user-visible; 8–11 catch classic slow-burn database and
optional trace-pipeline failures. A practical first alert set is query p99 latency, error rate, disk > 85%, memory near
`--allowed-percent`, and sustained wqueue/`queue_pub` backlog. When trace plugins are enabled, also alert immediately on
plugin panic, verdict mismatch, late completion, or an open circuit.

## Trace Plugin Monitoring

Trace plugins execute during mature trace merges and finalization, so their traffic is naturally bursty and may be zero
during the merge-grace window. Use rates over a suitable window and compare latency with a healthy baseline for each
`(group, plugin_name)` pair rather than applying one global latency threshold.

Start with these signals:

1. **Availability:** confirm the configured sampler count and watch registration, update, removal, and load failures in
   [Plugin lifecycle and loading](metrics.md#plugin-lifecycle-and-loading).
2. **Fail-open health:** alert on `panic`, `length_mismatch`, or `late` plugin executions and on `circuit_open` chain
   batches. Monitor sustained `decide_error` and `timeout` rates in
   [Plugin execution and batching](metrics.md#plugin-execution-and-batching).
3. **Cost:** graph successful p99 execution duration and successful execution time per evaluated trace by `plugin_name`.
   Use batch trace count to distinguish a slower plugin from a change in batch shape.
4. **Sampling effect:** monitor evaluated, retained, dropped, immature, and safety-bypassed traces in
   [Trace retention outcomes and safety](metrics.md#trace-retention-outcomes-and-safety).
5. **Host safety:** watch rejected plugin-defined series, dropped plugin log lines, and recovered telemetry panics in
   [Plugin telemetry host health](metrics.md#plugin-telemetry-host-health).
6. **Finalization:** watch the group-level round and terminal gauges in [Finalization state](metrics.md#finalization-state).

The [complete trace plugin metric reference](metrics.md#trace-plugin-metrics-reference) lists every host-provided plugin
and supporting trace-pipeline metric, its labels, and its interpretation. Metrics created by a plugin through
`Host.Meter` use the bounded prefix and labels documented under
[Plugin-defined business metrics](metrics.md#plugin-defined-business-metrics).

> Sources: [1] Google SRE — *Monitoring Distributed Systems* (Four Golden Signals) <https://sre.google/sre-book/monitoring-distributed-systems/> · [2] B. Gregg — *The USE Method* <https://www.brendangregg.com/usemethod.html> · [3] *The RED Method* (Grafana / T. Wilkie) <https://grafana.com/blog/the-red-method-how-to-instrument-your-services/> · [4] Elasticsearch disk watermarks 85/90/95% <https://www.elastic.co/docs/troubleshoot/elasticsearch/fix-watermark-errors> · [5] Prometheus remote-write backpressure <https://prometheus.io/docs/practices/remote_write/> · [6] Grafana Mimir — scaling to 1B active series (compaction/flush latency) <https://grafana.com/blog/2022/04/08/how-we-scaled-our-new-prometheus-tsdb-grafana-mimir-to-1-billion-active-series/>
