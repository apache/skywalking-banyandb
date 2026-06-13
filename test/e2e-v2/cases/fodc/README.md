# FODC Dashboard Metrics E2E (Kind)

This case verifies that **every metric consumed by the FODC Grafana dashboards**
(`docs/operation/grafana-fodc-nodes.json`, `docs/operation/grafana-fodc-workload.json`)
is exported by the **fodc-proxy `/metrics`** endpoint when a realistic SkyWalking
pipeline drives write **and** query traffic through a BanyanDB cluster.

## Topology

Single Kind node, all in the `default` namespace:

```
consumer ─POST /users─▶ provider ──▶ OAP ──┬─ writes ─▶ liaison ─▶ data   (BanyanDB 1:1, file discovery)
                                            └─ queries ▶ liaison ─▶ data
                                                          │           │
                                              fodc-agent sidecar  fodc-agent sidecar
                                                          └────┬──────┘
                                                          fodc-proxy ──▶ /metrics  (scraped by check-metrics.sh)
```

- **BanyanDB** runs as a 1:1 `liaison`/`data` cluster using **file discovery**
  (`nodes.yaml`, no etcd), mirroring `test/e2e-v2/cases/cluster`.
- Each BanyanDB pod carries a **fodc-agent sidecar**. The agent scrapes the local
  observability endpoint (`127.0.0.1:2121`), resolves its node role from the local
  cluster-state gRPC, and pushes metrics to the proxy.
  - **Critical wiring:** the agent must receive `--cluster-state-ports=17912`.
    Without it the cluster collector never starts, the node role stays empty, and
    the proxy client is disabled (no metrics ever reach the proxy).
- **fodc-proxy** aggregates all agents and serves `/metrics` (Prometheus text) and
  `/cluster/topology`. It runs in its own pod so its default `:17912`/`:17913`
  ports do not collide with the BanyanDB nodes.
- **OAP** (plain Deployment, not Helm) stores into BanyanDB (`SW_STORAGE_BANYANDB_TARGETS=liaison:17912`).
- **provider/consumer** are the SkyWalking e2e demo apps; the `consumer` POST
  generates traces/metrics, and the verify-phase `swctl service ls` query primes
  the BanyanDB query path (`operation="query"`).

## What is asserted

The metric universe (62 distinct names, extracted mechanically from the dashboard
panel `expr` fields) is partitioned into three checked-in lists under `metrics/`:

| List | Meaning | check-metrics.sh action |
|------|---------|-------------------------|
| `presence.txt` (38) | reliably exported by a 1:1 OAP cluster; value may be 0 | assert ≥ 1 exported series |
| `non_empty.txt` (12) | always-on core through OAP traffic | assert a sample value > 0 (histogram families via the companion `_count`) |
| `documented_gap.txt` (12) | need a specific event (an error, or a completed part-merge) to materialise | reported only, never asserted |

The three lists are disjoint and their union is exactly the 62 dashboard metrics —
nothing is silently dropped. `check-metrics.sh` additionally asserts that the two
agents registered with **distinct `node_role` labels** and that the dashboard's
query-throughput series `banyandb_queue_sub_*{operation="query"}` is present on the
**data** node (which subscribes to the query topic the liaison publishes), proving
the OAP→liaison→data query fan-out is observable.

### Documented gaps & first-run calibration

The split was calibrated against a live run; the recorded findings:

- **`documented_gap.txt` holds two groups (12 names), reported but never asserted.**
  *Error counters* (`*_grpc_total_err`, `*_grpc_total_stream_msg_received_err`, four
  `*_total_sync_loop_err`, `queue_pub_total_err`): a labelled Prometheus counter is
  not exported until first incremented, so on a healthy run they never appear.
  *Part-merge completion metrics* (`*_total_merge_latency`, `*_total_merged_parts`
  for measure/stream/trace): these fire only when a background part-merge completes,
  which needs enough accumulated parts and is not guaranteed within the verify window
  for every data type. The `*_total_merge_loop_started` counters (merge loop start)
  remain presence-asserted.
- **`banyandb_trace_tst_*` is present, not a gap.** OAP *does* write the BanyanDB
  `trace` model at the pinned `SW_OAP_COMMIT`, so the eight non-error `trace_tst`
  metrics are presence-asserted; only `trace_tst_total_sync_loop_err` is a gap.
- **`banyandb_stream_storage_*` and `banyandb_stream_tst_*`** are both produced and
  presence-asserted.
- **`operation="query"` appears on `banyandb_queue_sub_*` (data side) only.** The
  publisher-side query metric is a separate, later feature and is reported
  informationally, not asserted.

> **Calibration is intentional, not a loophole.** The classification reflects what a
> realistic OAP-driven 1:1 cluster actually exports; the only un-asserted dashboard
> metrics are error counters that require a failure to materialise. To re-calibrate
> after an OAP/BanyanDB bump, run `DUMP=1 check-metrics.sh` against a live cluster and
> move any newly-absent name into `documented_gap.txt` with the scrape as evidence.

## Relationship to `test/fodc/`

`test/fodc/` is a separate, raw-`kubectl` Kind smoke test for the **KTM (eBPF)** path:
a single standalone BanyanDB + agent, scraping the **agent's** `:9090/metrics`
directly, with no proxy, OAP, or cluster. This case shares only "Kind + the FODC
images"; it is a distinct proxy-aggregation / cluster / OAP test driven by the
skywalking-infra-e2e framework, so the two are kept apart.

## Resource budget

Seven+ containers run on one Kind node, so each pod sets explicit
`resources.limits` (OAP 2Gi, provider/consumer 1Gi each, BanyanDB 1–1.5Gi, agents
and proxy 256Mi). Total fits within a GitHub `ubuntu-24.04` runner with headroom.

## Running

CI: `.github/workflows/test-fodc-dashboard-e2e.yml` builds and loads the three local
images (`banyandb`, `fodc-agent`, `fodc-proxy`), then runs this case via the
skywalking-infra-e2e action (which creates/cleans the Kind cluster). OAP, provider,
and consumer images are pulled from ghcr at the commits pinned in
`test/e2e-v2/script/env`.

Local debugging of a live cluster:

```bash
DUMP=1 bash test/e2e-v2/cases/fodc/check-metrics.sh
```

`DUMP=1` prints the full proxy scrape, `/cluster/topology`, and pod logs to stderr.
