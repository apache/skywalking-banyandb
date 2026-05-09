# G5d Soak Harness Runbook

This document covers how to run and interpret the G5d soak test for the
vectorized-query path in SkyWalking BanyanDB.

## Prerequisites

- Docker daemon running and reachable (`docker info` succeeds).
- Docker Compose v2 (`docker compose version`).
- At least ~6 GB RAM headroom on the host (the compose stack peaks at ~5.4 GB).
  The host must have **no swap** — the resource limits in the compose file are the
  only protection against OOM kills.
- At least 10 GB free disk under the repo root for data snapshots, profiles, and logs.
- Go toolchain installed (used to build `soak-driver` and the BanyanDB image).
- The `vectorized-query` branch checked out locally.

Check headroom before starting:

```bash
free -h          # look at "available" column
df -h .          # look at "Avail" column
```

## Smoke Validation

Run a condensed ~30-minute smoke to verify the harness is wired correctly
before committing to a 48-hour run:

```bash
cd /path/to/repo
SMOKE=1 ./scripts/soak-vectorized.sh
```

Expected behaviour:

| Step | What you should see |
|---|---|
| Build | `soak-driver built at bin/soak-driver` |
| Phase 0 up | `BanyanDB is healthy` |
| Warmup | 2-minute pause |
| Snapshot | `Copying data to dist/soak/<ts>/data-snapshot/` |
| Baseline | `[record-baseline] written to dist/soak/<ts>/baseline.json` |
| Phase 1 up | `BanyanDB is healthy` |
| pprof-start | `Initial pprof captured` |
| Soak loop | Parity and pprof messages every 1 minute |
| pprof-end | `Final pprof captured` |
| Summary | `Summary written to dist/soak/<ts>/summary.json` |

Expected runtime: 25–35 minutes.

Healthy artefact tree after smoke:

```
dist/soak/<ts>/
  baseline.json              – array of per-query baseline records
  data-snapshot/             – raw BanyanDB data directory (vec-off)
  pprof-start/
    heap-<unix>.pb.gz
    goroutine-<unix>.txt
  pprof-<ts>/                – one dir per interval
    heap-<unix>.pb.gz
    goroutine-<unix>.txt
  pprof-end/
    heap-<unix>.pb.gz
    goroutine-<unix>.txt
  diff-<ts>.json             – one per parity interval + diff-final.json
  banyand.log                – full stdout/stderr from the BanyanDB container
  memory-alerts.log          – lines matching MemoryTracker / budget exhausted
  summary.json               – machine-readable summary
```

Check `summary.json` for a healthy smoke result:

```json
{
  "final_parity_pass": true,
  "goroutine_count_start": <N>,
  "goroutine_count_end":   <M>,   // M/N ≤ 1.05 is healthy
  "memory_alert_lines": 0
}
```

## Production 48-Hour Run

```bash
cd /path/to/repo
./scripts/soak-vectorized.sh
```

Key environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `WARMUP_MIN` | 60 | Minutes of OAP write traffic before snapshot |
| `SOAK_HOURS` | 48 | Total Phase 1 duration |
| `PPROF_INTERVAL_MIN` | 30 | Minutes between heap/goroutine captures |
| `PARITY_INTERVAL_MIN` | 5 | Minutes between replay-and-diff runs |

Monitoring during the run:

```bash
# Live BanyanDB logs
tail -f dist/soak/<ts>/banyand.log

# MemoryTracker alerts (should stay empty)
tail -f dist/soak/<ts>/memory-alerts.log

# Live parity results
ls -ltr dist/soak/<ts>/diff-*.json
cat dist/soak/<ts>/diff-<latest>.json | jq .pass

# Container resource usage
docker stats banyandb skywalking-oap
```

To abort safely, press Ctrl-C. The `trap` in the script runs
`docker compose down -v` before exiting, leaving no dangling containers.

## Reading the Artefacts Against the Four Acceptance Criteria

### Criterion 1 — ≥48h staging run with `--measure-vectorized-enabled=true`

Check `summary.json`:

```bash
jq '{smoke, soak_hours}' dist/soak/<ts>/summary.json
```

`smoke` must be `"false"` (or absent) and `soak_hours` must be `48`.
The script logs a timestamped start and end line in the terminal output;
their difference confirms the wall-clock duration.

### Criterion 2 — No parity regression on `[]*measurev1.InternalDataPoint`

Every `diff-*.json` file must have `"pass": true`.
Check them all at once:

```bash
jq -r '.pass' dist/soak/<ts>/diff-*.json | sort | uniq -c
```

All lines should read `true`. A quick overview:

```bash
jq -s '[.[].pass] | {total: length, passing: map(select(. == true)) | length}' \
   dist/soak/<ts>/diff-*.json
```

The final canonical result is `diff-final.json` and is also reflected in
`summary.json` under `"final_parity_pass"`.

### Criterion 3 — No MemoryTracker budget exhaustion

```bash
wc -l dist/soak/<ts>/memory-alerts.log
cat  dist/soak/<ts>/memory-alerts.log
```

A healthy run produces an empty file (0 lines). Any non-zero line count is a
failure; see the **Failure Modes** section.

`summary.json` also records `"memory_alert_lines"` for CI consumption.

### Criterion 4 — No goroutine leak (heap delta ≤5%)

Compare the goroutine counts in the start and end profiles:

```bash
# Count goroutines in start profile
grep -c '^goroutine ' dist/soak/<ts>/pprof-start/goroutine-*.txt

# Count goroutines in end profile
grep -c '^goroutine ' dist/soak/<ts>/pprof-end/goroutine-*.txt
```

Calculate ratio: `end / start`. A ratio ≤1.05 passes.

`summary.json` captures `goroutine_count_start` and `goroutine_count_end`
for automated evaluation.

For a heap size delta, use the `go tool pprof` binary:

```bash
go tool pprof -top dist/soak/<ts>/pprof-start/heap-*.pb.gz
go tool pprof -top dist/soak/<ts>/pprof-end/heap-*.pb.gz
```

The in-use bytes between start and end must not grow unboundedly.

## Failure Modes

### MemoryTracker budget exhaustion

**Symptom**: `memory-alerts.log` is non-empty; BanyanDB may slow down or log
errors.

**Action**:
1. Check `banyand.log` for context around the alert timestamp.
2. Reduce query concurrency in `catalog/default.json` (fewer simultaneous
   queries) or lower field projections.
3. If the budget is misconfigured, adjust the BanyanDB `--measure-memory-budget`
   flag in the compose service command.

### Goroutine leak

**Symptom**: `goroutine_count_end / goroutine_count_start > 1.05`.

**Action**:
1. Diff the goroutine profiles:
   ```bash
   diff dist/soak/<ts>/pprof-start/goroutine-*.txt \
        dist/soak/<ts>/pprof-end/goroutine-*.txt | head -80
   ```
2. Identify which goroutine stacks appear only in the end profile.
3. File a bug against the vectorized pipeline with the diffed profiles attached.

### Parity divergence

**Symptom**: Any `diff-*.json` has `"pass": false`; `summary.json`
`"final_parity_pass"` is `false`.

**Action**:
1. Open the failing diff file:
   ```bash
   jq '.divergences' dist/soak/<ts>/diff-<ts>.json
   ```
2. The `first_diffs` array shows the first 3 mismatched data points (index,
   baseline string, replay string).
3. Reproduce the query manually:
   ```bash
   bin/soak-driver replay-and-diff \
     --addr localhost:17912 \
     --catalog cmd/soak-driver/catalog/default.json \
     --baseline dist/soak/<ts>/baseline.json \
     --report /tmp/debug-diff.json
   ```
4. Check whether the divergence is in field values, tag values, or shard IDs.
5. File a bug against the vectorized query path with the diff JSON attached.

### OOMKill

**Symptom**: A container disappears; `docker compose ps` shows it as `exited`.

**Action**:
1. `docker inspect <container> | jq '.[].State.OOMKilled'` — if `true`, the
   container exceeded its memory limit.
2. Check `docker stats` history for which container breached its limit.
3. The per-container limits in `test/soak/docker-compose.soak.yaml` must be
   respected. Do **not** increase them without re-evaluating the host budget.
4. If BanyanDB OOM-kills, reduce the vectorized batch size or the number of
   concurrent queries in the catalog.

## Rollback

To run Phase 1 with the flag disabled (reproducing the baseline behaviour):

```bash
BANYANDB_VEC_ENABLED=false \
SOAK_DATA_DIR=./test/soak/data \
docker compose -f test/soak/docker-compose.soak.yaml up -d
```

This is also the normal state after any failed Phase 1: tear down the stack
with `docker compose -f test/soak/docker-compose.soak.yaml down -v` and
restart with `BANYANDB_VEC_ENABLED=false`.
