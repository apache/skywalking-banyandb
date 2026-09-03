# Trace Vectorized-Query Soak Runbook

This document covers how to run and interpret the two-instrument soak for the
trace vectorized-query path in SkyWalking BanyanDB. It mirrors the structure
of `docs/soak/g5d-runbook.md` (the measure soak) but is specific to the trace
engine additions introduced in `scripts/soak-vectorized.sh SOAK_ENGINE=trace`.

## Overview

The trace soak uses **two instruments** that cover complementary failure modes:

| Instrument | Entry point | What it proves |
|---|---|---|
| **1 — Container correctness + survival** | `SOAK_ENGINE=trace ./scripts/soak-vectorized.sh` | Parity (row→vec byte-match), crash/restart survival, goroutine-leak absence, write-load liveness, vec-flag honored |
| **2 — In-process DQB sustained bench** | `DQB_SOAK=1 DQB_ENGINE=trace ./run-docker.sh` | `vtrace.QueryCount()` liveness, heap `inuse_space` leak gate, budget hard-stop + first-block exception engagement |

Instrument 2 is the **authoritative** gate for memory leak and vec-liveness
because `vtrace.QueryCount()` is a package-private in-process `atomic.Int64`
that cannot be read from a separate driver process. Instrument 1 provides the
crash/restart, OS-level goroutine, and multi-hour parity signals that an
in-process bench cannot reproduce.

CI gates on **both** instruments independently (two different exit statuses and
two different JSON artifacts). A pass from one instrument does not subsume the
other.

### OAP-independence rationale

OAP→BanyanDB trace-module routing is version- and configuration-dependent. The
soak self-generates all trace writes via `TraceService.Write` (deterministic,
version-keyed spans, two separate groups) and does not depend on OAP traffic
for the parity fixture. The OAP/provider/consumer services present in the
compose stack are background noise only.

## MANDATORY: Both instruments run inside resource-limited containers

**Neither instrument runs on the host directly.** An uncontained invocation is
a hard error, not a silent fallback.

- **Instrument 1**: BanyanDB runs as a compose service with
  `deploy.resources.limits` (2 GB / 2 CPU). The `soak-driver` also runs as a
  compose service (512 MB / 1 CPU) via `docker compose run --rm soak-driver`.
  The driver reaches BanyanDB over the compose network (`banyandb:17912`,
  `banyandb:6060`). No driver binary runs on the host.
- **Instrument 2**: runs only via `run-docker.sh`, which builds the test image
  and executes the `go test` binary inside a `--cpus/--memory`-limited
  container. `ValidateSoak()` in `config.go` hard-fails (`DQB_IN_CONTAINER`
  gate) if `DQB_IN_CONTAINER != 1`. A direct `go test` on the host will be
  rejected.

The cgroup memory limit is what makes the heap-growth gate and the
budget-engagement gate (the `--trace-vectorized-query-memory-mib` hard-stop)
reproducible. Every run records the effective CPU/memory limits and image
digests in its artifacts.

**Single exception**: pure config/unit tests (catalog parse, query-build,
compare logic — no cluster, no data) may run on the host via `go test` directly.

## Prerequisites

- Docker daemon running (`docker info` succeeds); Docker Compose v2.
- Host RAM headroom: at least 8 GB free. The Instrument 1 compose stack peaks
  at approximately 6.5 GB total across all services.
- Disk: at least 10 GB free under the repo root for snapshots, profiles, logs.
- Go toolchain installed (to build the `soak-driver` image and the querybench
  test binary).
- The `vectorized-query` branch checked out.

Check headroom:

```bash
free -h   # look at "available" column
df -h .   # look at "Avail" column
```

## Instrument 1 — Container Correctness + Survival Soak

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `SOAK_ENGINE` | `measure` | Must be `trace` for this runbook |
| `WARMUP_MIN` | `60` | Minutes to wait before baseline (0 in SMOKE — trace uses self-seeded data, not OAP warmup) |
| `SOAK_HOURS` | `48` | Phase 1 duration in hours |
| `PPROF_INTERVAL_MIN` | `30` | Minutes between heap/goroutine pprof captures |
| `PARITY_INTERVAL_MIN` | `5` | Minutes between `replay-and-diff` runs |
| `SEED_ROWS` | `1000` | Unused by the trace path (trace uses `--traces`/`--spans` defaults); present in the script for the measure path |
| `SOAK_TRACE_SPANS_PER_TRACE` | _(driver default: 5)_ | Passed as `--spans` to `seed-fixture` when set; if unset, the driver's compiled-in default is used |
| `SOAK_TRACE_SERVICES` | _(unset)_ | Documented in the script header but **not currently passed to the driver**; reserved for a future `--services` flag |
| `SOAK_WRITE_RPS` | `500` | Rate cap in spans/second for the background write-load loop |
| `SOAK_HEAP_GROWTH_MAX_PCT` | `10` | Advisory threshold recorded in `summary.json`; the authoritative gate is Instrument 2 |
| `SMOKE` | _(unset)_ | Set to `1` for a ~25-minute condensed run (overrides durations: `WARMUP_MIN=0`, `SOAK_HOURS=0.34`, intervals=1 min) |

### SMOKE invocation

```bash
cd /path/to/repo
SOAK_ENGINE=trace SMOKE=1 ./scripts/soak-vectorized.sh
```

Expected runtime: 25–35 minutes.

### Production 48-hour invocation

```bash
cd /path/to/repo
SOAK_ENGINE=trace ./scripts/soak-vectorized.sh
```

Optional overrides example:

```bash
SOAK_ENGINE=trace \
  SOAK_HOURS=48 \
  PPROF_INTERVAL_MIN=30 \
  PARITY_INTERVAL_MIN=5 \
  SOAK_WRITE_RPS=500 \
  ./scripts/soak-vectorized.sh
```

### Phase 0 — capture golden baseline

1. `docker compose up -d banyandb` — standalone BanyanDB, resource-limited
   (2 GB / 2 CPU).
2. Health-check `http://localhost:17913/api/healthz` (120-attempt cap, 5 s
   intervals).
3. `docker compose run --rm soak-driver seed-fixture --engine trace
   --addr banyandb:17912 [--spans N]` — creates two groups:
   - `bench-trace-fixture` (retain TTL 30 days, keeps baseline data alive for
     the full run).
   - `bench-trace-load` (TTL 1 day, rolling expiry for write-load traffic).
   Creates the `sw` Trace resource + `timestamp`/`duration` index rules +
   binding in both groups; writes the deterministic fixture into
   `bench-trace-fixture`; prints `T1_MS` (highest span timestamp in unix ms).
4. Wait 8 s for schema-server flush to persist (schema segments must land on
   disk before snapshot).
5. `docker compose run --rm soak-driver record-baseline --engine trace
   --addr banyandb:17912 --catalog /catalog/trace.json --until T1_MS
   --out /artifacts/baseline.json` — runs all 5 catalog queries over
   `[T1_MS - 7d, T1_MS]`, persists `[]*Trace` as proto-JSON. **Fails non-zero
   if any catalog query returns zero traces** (engine-agnostic guard).
6. Stop BanyanDB → `cp -a data/. data-snapshot/` → `docker compose down -v`.

### Phase 1 — soak

1. Restore snapshot → `docker compose up -d banyandb`; health-check.
2. Initial pprof-grab (heap + goroutine) → `pprof-start/`.
3. Background loops (fail-tolerant — WARN on a bad tick, never abort):
   - **parity** (every `PARITY_INTERVAL_MIN`): `replay-and-diff --engine trace`
     re-runs the catalog against the live post-restore data; writes
     `diff-<ts>.json`.
   - **pprof** (every `PPROF_INTERVAL_MIN`): heap + goroutine snapshots.
   - **write-load** (continuous, rate-capped at `SOAK_WRITE_RPS` spans/s):
     deterministic writes into `bench-trace-load` using timestamps offset 365
     days past the fixture base time (never overlaps the parity window); drives
     flush/merge/compaction/expiry stress; records span count + last-success.
   - **RSS+disk advisory sampler** (`docker stats` → `rss-trend.csv`).
   - **log capture** (rotation-bounded `--since 60s` snapshots → `banyand.log`)
     and memory-alert grep
     (`budget|MemoryTracker|panic|vectorized` → `memory-alerts.log`).
4. Wait `SOAK_HOURS` → kill loops → final pprof-grab (`pprof-end/`) → final
   `replay-and-diff` (sets `FINAL_PASS`) → write `summary.json`.

### Tapered monitor (recommended for unattended runs)

In a separate terminal:

```bash
./scripts/soak-monitor.sh                    # watches the most recent run
./scripts/soak-monitor.sh dist/soak/<ts>     # watches a specific run
```

The monitor greps `diff-*.json` for `"pass": false`, checks that `banyand.log`
is not stale, and alerts on any `memory-alerts.log` growth. It exits 0 when
`summary.json` appears with no alerts, 2 if any alert fired.

To abort safely, press Ctrl-C. The `trap` in the script runs
`docker compose down -v` before exiting.

## Instrument 2 — In-Process DQB Sustained Bench (Authoritative Gate)

Instrument 2 boots an in-process distributed cluster (liaison + data nodes) and
reads `vtrace.QueryCount()` and `runtime.ReadMemStats` directly — same process —
to prove liveness and the absence of a cursor-release leak. This topology is the
only one that can read the package-private atomic counter.

As a side effect, the in-process distributed boot exercises
`EncodeTraceResultFrame` / `DecodeTraceResultFrame` and the liaison itersort
merge path for correctness.

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `DQB_SOAK` | `0` | Set to `1` to activate `TestTraceVecSoak` (otherwise the test skips) |
| `DQB_ENGINE` | `measure` | Must be `trace` for the soak |
| `SOAK_HEAP_GROWTH_MAX_PCT` | `10` | Maximum allowable `HeapInuse` growth from post-warmup baseline to end, as a percentage; gate fails if exceeded |
| `DQB_QUERY_MEMORY_MIB` | `256` | `--trace-vectorized-query-memory-mib` passed to the in-process cluster for the parity phase |
| `DQB_IN_CONTAINER` | _(must be 1)_ | Hard-fail gate; set automatically by `run-docker.sh` |
| `DQB_REPORT_DIR` | `.omx/bench-reports/distributed-query` | Directory for `soak-trace-vec-result.json`; set automatically by `run-docker.sh` |

### SMOKE / quick invocation

```bash
cd /path/to/repo
DQB_SOAK=1 DQB_ENGINE=trace ./test/integration/distributed/querybench/run-docker.sh \
  --cpus 4 --memory 8g
```

### Production invocation (custom budget threshold)

```bash
DQB_SOAK=1 DQB_ENGINE=trace SOAK_HEAP_GROWTH_MAX_PCT=10 \
  ./test/integration/distributed/querybench/run-docker.sh \
  --cpus 4 --memory 8g
```

`run-docker.sh` builds the querybench image, then runs `orchestrate.sh` inside
the container. When `DQB_SOAK=1`, `orchestrate.sh` runs only
`TestTraceVecSoak` (not the full benchmark matrix) and exits.

A direct host `go test` invocation will fail immediately:

```
invalid soak config: DQB_SOAK=1 requires DQB_IN_CONTAINER=1; invoke via
test/integration/distributed/querybench/run-docker.sh
```

### What `TestTraceVecSoak` does

**Phase 1 — parity fixture (200 iterations, liveness + leak gate)**

- Boots an in-process cluster; seeds 1000-trace × 20-span uniform fixture
  (budget 256 MiB, never truncates → parity clean).
- Warmup: 3 query iterations to stabilize JIT paths and GC.
- Snapshots post-warmup `vtrace.QueryCount()` + `runtime.ReadMemStats.HeapInuse`.
- Runs 200 query iterations, sampling `QueryCount` every 20 iterations to assert
  monotonicity.
- Forces GC; snapshots final `QueryCount` + `HeapInuse`.

**Phase 2 — budget-engagement scenario (50 iterations)**

- Boots a second in-process cluster with `QueryMemoryMiB = 2` MiB (the compiled
  `soakBudgetMiB` constant) + heavy-tail fixture (500-span traces at 1 KiB/span)
  so the budget hard-stop fires on every query.
- Runs 50 iterations; asserts each returns a bounded result set and heap stays
  near-flat (cursor-release path releases skipped cursors without leaking).

### Instrument 2 gates

| Gate | Pass condition |
|---|---|
| **QueryCount monotonic** | `vtrace.QueryCount()` is non-decreasing across every 20-iteration sample |
| **Liveness** | Final `QueryCount` delta from post-warmup baseline ≥ 200 (vec fired every iteration) |
| **Heap-leak (authoritative)** | `HeapInuse` growth from post-warmup baseline to end ≤ `SOAK_HEAP_GROWTH_MAX_PCT` % (default 10) |
| **Budget result bound** | Every budget-scenario result has trace count ≤ query limit |
| **Budget heap** | Budget-scenario `HeapInuse` growth ≤ `SOAK_HEAP_GROWTH_MAX_PCT` % |
| **Budget liveness** | `QueryCount` delta ≥ 50 over the budget scenario |

## Instrument 1 Gates vs Advisory Signals

### Gates (hard failures)

| Signal | Pass condition |
|---|---|
| **Parity (primary)** | `diff-final.json` has `"pass": true` — every catalog query byte-matches baseline via `proto.Equal` per `Trace` (spans sorted by `(span_id, bytes)`, trace order preserved for ordered queries) |
| **Goroutine drift** | `goroutine_count_end / goroutine_count_start ≤ 1.05` |
| **No crash/restart** | BanyanDB container uptime continuous; `banyand.log` not stale (`soak-monitor.sh`) |
| **No memory-alert lines** | `memory-alerts.log` has 0 lines matching `budget\|MemoryTracker\|panic\|vectorized` |
| **Write-load alive** | `write_load_spans > 0` in `summary.json` |

### Advisory signals (human review only, not gates)

- `rss-trend.csv` — container RSS and data-dir disk usage sampled every 60 s.
- Container heap `inuse_space` pprof diff (`pprof-start/` vs `pprof-end/`). Use
  `go tool pprof -inuse_space -base pprof-start/heap-*.pb.gz pprof-end/heap-*.pb.gz`
  to inspect. The 48 h diff is polluted by write-load/merge/compaction churn
  unrelated to the cursor-release path; the authoritative leak gate is
  Instrument 2.

## Trace Catalog

The catalog is `cmd/soak-driver/catalog/trace.json`. All 5 shapes target
`bench-trace-fixture` / Trace `sw` and carry a pinned `limit`.

| ID | Shape | Filter | Order | Limit |
|---|---|---|---|---|
| `by_id_single` | Point lookup | `trace_id = "trace-0000000100"` | none | 1 |
| `by_id_batch` | Batch lookup | `trace_id IN [98..102]` | none | 5 |
| `tag_newest` | Tag filter + sort | `service_id = "svc-0"` | `timestamp DESC` | 50 |
| `tag_slowest` | Tag filter + sort | `service_id = "svc-0" AND state = 0` | `duration DESC` | 50 |
| `tag_complex` | Multi-condition | `state = 0 AND service_id = "svc-0" AND duration ∈ [1000, 2000]` | `timestamp DESC` | 20 |

All queries project all 8 tags: `trace_id`, `state`, `service_id`,
`service_instance_id`, `endpoint_id`, `duration`, `span_id`, `timestamp`.

## Two-Group Design

| Group | TTL | Purpose |
|---|---|---|
| `bench-trace-fixture` | 30 days | Parity fixture; immutable during Phase 1; baseline never ages out |
| `bench-trace-load` | 1 day | Rolling write-load target; data self-expires; bounded disk growth |

Write-load writes use timestamps offset 365 days past the fixture base time
(`2024-01-01T00:00:00Z + 365 days`) so they never land in the parity query
window `[T1_MS - 7d, T1_MS]`.

## Artifacts

### Instrument 1 artifacts

All artifacts land under `dist/soak/<YYYYMMDDTHHMMSS>/`:

```
dist/soak/<ts>/
  run.log                    # tee'd full orchestrator log
  baseline.json              # array of traceBaselineRecord (proto-JSON Traces)
  data-snapshot/             # raw BanyanDB /data dir from Phase 0
  pprof-start/
    heap-<unix>.pb.gz        # gzip pprof heap
    goroutine-<unix>.txt     # goroutine dump (debug=1 text format)
  pprof-<ts>/                # one directory per PPROF_INTERVAL tick
  pprof-end/                 # final heap + goroutine before teardown
  diff-<ts>.json             # one per PARITY_INTERVAL tick
  diff-final.json            # final canonical parity report (sets FINAL_PASS)
  banyand.log                # rotation-bounded BanyanDB stdout/stderr
  memory-alerts.log          # lines matching budget|MemoryTracker|panic|vectorized
  rss-trend.csv              # advisory: ts_utc, rss_bytes, disk_bytes
  summary.json               # machine-readable run summary
```

#### `summary.json` fields (trace engine)

```json
{
  "run_ts": "20240101T120000",
  "engine": "trace",
  "smoke": "false",
  "warmup_min": 0,
  "soak_hours": 48,
  "t1_ms": 1704067200000,
  "final_parity_pass": true,
  "goroutine_count_start": 120,
  "goroutine_count_end": 122,
  "memory_alert_lines": 0,
  "write_load_spans": 8640000,
  "write_load_alive": true,
  "write_load_last_ok": "2024-01-03T11:59:00Z",
  "heap_growth_max_pct_threshold": 10,
  "banyandb_image_digest": "sha256:...",
  "banyandb_cpu_nanocpu_limit": "2000000000",
  "banyandb_memory_bytes_limit": "2147483648",
  "soak_driver_image_digest": "sha256:...",
  "artefacts_dir": "/path/to/dist/soak/<ts>"
}
```

### Instrument 2 artifact

`soak-trace-vec-result.json` — written to `DQB_REPORT_DIR` (default
`.omx/bench-reports/distributed-query/`):

```json
{
  "engine": "trace",
  "iterations": 200,
  "query_count_delta": 200,
  "query_count_monotonic": true,
  "liveness_pass": true,
  "heap_inuse_baseline_bytes": 12345678,
  "heap_inuse_end_bytes": 12500000,
  "heap_growth_pct": 1.25,
  "heap_growth_max_pct": 10,
  "heap_leak_pass": true,
  "budget_scenario_result_bound": true,
  "budget_scenario_heap_pass": true,
  "budget_scenario_pass": true
}
```

## Reading the Artifacts

### Quick all-green check

```bash
# Instrument 1: final parity + write-load + alerts
jq '{final_parity_pass, write_load_alive, memory_alert_lines,
     goroutine_count_start, goroutine_count_end}' dist/soak/<ts>/summary.json

# Instrument 1: all intermediate diffs
jq -r '.pass' dist/soak/<ts>/diff-*.json | sort | uniq -c

# Instrument 2: all gates
jq '{liveness_pass, heap_leak_pass, budget_scenario_pass, query_count_monotonic,
     heap_growth_pct, heap_growth_max_pct}' \
  .omx/bench-reports/distributed-query/soak-trace-vec-result.json
```

### Goroutine drift

```bash
# Ratio must be ≤ 1.05
jq '{goroutine_count_start, goroutine_count_end}' dist/soak/<ts>/summary.json
```

If the ratio exceeds 1.05, diff the goroutine text files:

```bash
diff dist/soak/<ts>/pprof-start/goroutine-*.txt \
     dist/soak/<ts>/pprof-end/goroutine-*.txt | head -80
```

## Failure Modes

### Parity divergence

**Symptom**: any `diff-<ts>.json` has `"pass": false`; `summary.json`
`"final_parity_pass"` is `false`.

**Triage**:

```bash
jq '.divergences' dist/soak/<ts>/diff-<ts>.json
```

**Parity-FAIL disambiguation**: a divergence after Phase 1 could be caused by
the reader or by a merge/data artifact accumulated on the Phase-1 data. There is
no second query engine to cross-check against — the row path was removed in
0.12.0 — so the control is the Phase-0 snapshot, which the soak has not written
to. Restore it into a scratch data dir and replay the same catalog:

```bash
# Replay the failing catalog query against the untouched Phase-0 snapshot:
rm -rf /tmp/soak-replay && cp -a dist/soak/<ts>/data-snapshot /tmp/soak-replay
SOAK_DATA_DIR=/tmp/soak-replay \
  docker compose -f test/soak/docker-compose.soak.yaml up -d banyandb
docker compose -f test/soak/docker-compose.soak.yaml run --rm soak-driver \
  replay-and-diff --engine trace \
  --addr banyandb:17912 \
  --catalog /catalog/trace.json \
  --baseline /artifacts/baseline.json \
  --report /artifacts/diff-snapshot-debug.json
```

If `diff-snapshot-debug.json` passes, the baseline data still reads correctly and
the divergence came from what Phase 1 did to the data — merge, compaction or
expiry. If it also diverges, the reader is at fault.

### Memory-alert lines

**Symptom**: `memory-alerts.log` is non-empty.

**Action**: check `banyand.log` around the alert timestamp. The keywords that
triggered the alert (`budget`, `MemoryTracker`, `panic`, `vectorized`) indicate
which subsystem logged the event.

### Goroutine leak

**Symptom**: `goroutine_count_end / goroutine_count_start > 1.05`.

**Action**: identify new goroutine stacks via the text diff above, then file a
bug against the vectorized trace pipeline with the diff attached.

### Instrument 2 liveness gate FAIL

**Symptom**: `liveness_pass: false` — `query_count_delta < 200`.

**Meaning**: the query path was not reached on every iteration — iterations are
erroring out before they query. Examine the test's `t.Log` output for
iteration-level error messages.

### Instrument 2 heap-leak gate FAIL

**Symptom**: `heap_leak_pass: false` — `heap_growth_pct > heap_growth_max_pct`.

**Action**: the cursor-release path is the primary suspect. Enable pprof in the
test binary and compare heap profiles at warmup vs end; look for retained objects
in `pkg/query/vectorized/trace`. File a bug with the heap diff attached.

### OOMKill

**Symptom**: a container disappears; `docker compose ps` shows it as `exited`.

**Action**:

```bash
docker inspect banyandb | jq '.[].State.OOMKilled'
```

If `true`, BanyanDB exceeded its 2 GB memory limit. Do not increase the limit
without re-evaluating the host budget. Consider reducing `SOAK_WRITE_RPS` or
the `--trace-vectorized-query-memory-mib` value to reduce peak RSS.

### Write-load stalled

**Symptom**: `write_load_alive: false` or `write_load_spans: 0` in
`summary.json`.

**Action**: the write-load background loop logs `WARN` on each failed sweep.
Check `run.log` for `WARN: parity divergence` or write-load sweep errors.
A stall means no flush/merge/compaction stress during Phase 1.

## Verification Steps

1. **SMOKE (both instruments)**:
   ```bash
   SOAK_ENGINE=trace SMOKE=1 ./scripts/soak-vectorized.sh
   DQB_SOAK=1 DQB_ENGINE=trace ./test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
   ```
   Both should complete without errors, `summary.json` should have
   `final_parity_pass: true`;
   `soak-trace-vec-result.json` should have all pass fields `true`.

2. **Parity has teeth** (negative control): inject one extra span into the
   Phase-1 fixture and re-run `replay-and-diff` — `diff-final.json` must show
   `"pass": false`.

3. **Measure regression**: `SOAK_ENGINE=measure SMOKE=1 ./scripts/soak-vectorized.sh`
   must still pass unchanged (measure path is byte-for-byte unmodified).

## Named v1 Gap — Production Distributed-Topology Soak

Instrument 2 boots an in-process distributed cluster (liaison + data nodes) and
incidentally exercises `EncodeTraceResultFrame` / `DecodeTraceResultFrame` and
the liaison itersort merge path **for correctness** over in-process gRPC.

What it does **not** cover:

- Real network fan-out between separate OS processes or containers.
- On-wire `BANYAND_TRACE_NATIVE_WIRE` frame behavior under real TCP.
- Per-node container RSS and disk usage under production distributed load.
- Multi-node OOM-kill behavior.

This is the **named v1 gap**: a follow-up production distributed-topology soak
covering real network fan-out, on-wire frames, and multi-node container RSS.
The in-process wire/merge path is already covered for correctness; what remains
is the production transport layer.

## Teardown

The row-based query path was removed in 0.12.0
([apache/skywalking#13998](https://github.com/apache/skywalking/issues/13998)),
so there is no engine to roll back to; a run is torn down, not reverted.

Tear down with:

```bash
docker compose -f test/soak/docker-compose.soak.yaml down -v
```
