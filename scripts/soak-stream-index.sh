#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership.  Apache Software Foundation (ASF) licenses this file to you
# under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Native inverted-index soak for the STREAM secondary index.
#
# The existing harness (scripts/soak-vectorized.sh) proves PARITY and
# SURVIVAL: results match a baseline, and heap/goroutines/RSS survive. It
# measures no query latency and no CPU utilization — the RSS sampler samples
# only memory, and nothing records p50/p95/p99. This script is the companion
# that closes those two gaps against the log-shaped stream fixture
# (test/soak/cmd/stream_index.go).
#
# Why a separate script rather than a new SOAK_ENGINE branch: the G5d
# orchestrator is 900 lines with an OAP warmup phase and a distributed frame
# gate that neither applies here. A pilot has to be cheap to re-run and easy to
# reason about; folding into soak-vectorized.sh comes after the harness proves
# itself.
#
# What it adds over the G5d harness:
#   - latency-probe: per-query p50/p95/p99/max AND row counts, per interval.
#     Row counts matter as much as latency: a fast query that returned nothing
#     because the time pruner over-pruned is a false win.
#   - a CPU utilization sampler (docker stats CPUPerc), sampled for BOTH the
#     banyandb and soak-driver containers, because "work moved into the server"
#     is only visible in the ratio.
#   - a native-index metrics scraper for the pkg/index/metrics surface,
#     including native_time_segments_total{class=...} and
#     native_time_candidates_pruned_total. The last one is the signal for the
#     bug this soak exists to catch: the streaming merge used to write zero
#     time bounds, so every merged segment claimed "no timestamps", could never
#     be pruned, and every query silently fell back to per-document checks.
#   - a gate block that turns all of the above into pass/fail.
#
# Configuration (env vars):
#   SOAK_HOURS          soak duration in hours (default 2 — the pilot)
#   SMOKE=1             short preset (~10 min total), overrides SOAK_HOURS
#   SOAK_IDX_SERIES     fixture series (default 200)
#   SOAK_IDX_ELEMENTS   fixture elements per series (default 200)
#   SOAK_WRITE_RPS      background write-load cap (default 500; 0 disables)
#   PROBE_WORKERS       concurrent latency-probe workers (default 8)
#   PROBE_INTERVAL_MIN  latency sampling interval (default 1)
#   PARITY_INTERVAL_MIN replay-and-diff interval (default 1)
#   PPROF_INTERVAL_MIN  pprof capture interval (default 5)
#   RESOURCE_INTERVAL_SEC docker stats scrape interval (default 15)
#
# Usage:
#   ./scripts/soak-stream-index.sh              # ~2h pilot
#   SMOKE=1 ./scripts/soak-stream-index.sh      # ~10 min smoke
#
# Artefacts land in dist/soak-index/<timestamp>/.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/test/soak/docker-compose.soak-index.yaml"
ENGINE="stream-index"
ENGINE_CATALOG="/catalog/stream_index.json"
BANYANDB_UP_SERVICE="banyandb"
BANYANDB_GRPC_CONTAINER="banyandb:17912"
BANYANDB_PPROF_CONTAINER="banyandb:6060"
DATA_DIR="${REPO_ROOT}/test/soak/data-index"

# ── configuration ──────────────────────────────────────────────────────────
SOAK_HOURS="${SOAK_HOURS:-2}"
SOAK_IDX_SERIES="${SOAK_IDX_SERIES:-200}"
SOAK_IDX_ELEMENTS="${SOAK_IDX_ELEMENTS:-200}"
SOAK_WRITE_RPS="${SOAK_WRITE_RPS:-500}"
PROBE_WORKERS="${PROBE_WORKERS:-4}"
PROBE_INTERVAL_MIN="${PROBE_INTERVAL_MIN:-1}"
PARITY_INTERVAL_MIN="${PARITY_INTERVAL_MIN:-1}"
PPROF_INTERVAL_MIN="${PPROF_INTERVAL_MIN:-5}"
RESOURCE_INTERVAL_SEC="${RESOURCE_INTERVAL_SEC:-15}"

if [[ "${SMOKE:-}" == "1" ]]; then
  # ~10 min total: Phase 0 setup dominates, Phase 1 soaks for ~4 min. Long
  # enough to exercise every loop at least twice and to catch the flush/schema
  # timing bugs; short enough to iterate. See test/CLAUDE.md on smoke ratios.
  SOAK_HOURS=0.07   # ~4 min
  PROBE_INTERVAL_MIN=0.5
  PARITY_INTERVAL_MIN=0.5
  PPROF_INTERVAL_MIN=1
  RESOURCE_INTERVAL_SEC=5
  SOAK_IDX_SERIES=120
  SOAK_IDX_ELEMENTS=120
  PROBE_WORKERS=2
fi

SOAK_HOURS_SEC=$(awk "BEGIN{printf \"%d\", ${SOAK_HOURS}*3600}")
PARITY_INTERVAL_SEC=$(awk "BEGIN{printf \"%d\", ${PARITY_INTERVAL_MIN}*60}")
PPROF_INTERVAL_SEC=$(awk "BEGIN{printf \"%d\", ${PPROF_INTERVAL_MIN}*60}")

# Isolate from other compose workflows on a shared host: the project name and
# the global container name are both collision points.
export COMPOSE_PROJECT_NAME="${SOAK_COMPOSE_PROJECT:-soakidx}"
export BANYANDB_CONTAINER_NAME="${SOAK_BANYANDB_CONTAINER:-banyandb-soakindex}"
# Bind host ports to random free ports so the pilot coexists with another
# banyand on the standard ports; the driver is containerized.
export SOAK_HOST_GRPC=0 SOAK_HOST_HTTP=0 SOAK_HOST_PPROF=0 SOAK_HOST_METRICS=0
export SOAK_UID="$(id -u)"
export SOAK_GID="$(id -g)"

RUN_TS="$(date +%Y%m%dT%H%M%S)"
DIST="${REPO_ROOT}/dist/soak-index/${RUN_TS}"
SNAPSHOT_DIR="${DIST}/data-snapshot"

# ── helpers ────────────────────────────────────────────────────────────────
log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

compose_cmd() {
  SOAK_DATA_DIR="${DATA_DIR}" SOAK_DIST_DIR="${DIST}" docker compose -f "${COMPOSE_FILE}" "$@"
}

# wait_healthy waits on the container's own compose healthcheck via docker
# inspect, so it needs no host-published port.
wait_healthy() {
  log "Waiting for BanyanDB to become healthy..."
  local attempts=0
  until [[ "$(docker inspect -f '{{.State.Health.Status}}' "${BANYANDB_CONTAINER_NAME}" 2>/dev/null)" == "healthy" ]]; do
    attempts=$(( attempts + 1 ))
    if (( attempts > 120 )); then
      log "ERROR: BanyanDB did not become healthy after 120 attempts"
      return 1
    fi
    sleep 5
  done
  log "BanyanDB is healthy."
}

# driver runs the soak-driver inside the compose network with DIST bind-mounted
# as /artifacts, so latency reports and baselines land on the host.
driver() {
  SOAK_DATA_DIR="${DATA_DIR}" SOAK_DIST_DIR="${DIST}" \
    docker compose -f "${COMPOSE_FILE}" run --rm \
    -v "${DIST}:/artifacts" \
    soak-driver "$@"
}

cleanup() {
  log "Caught signal — tearing down compose stack..."
  compose_cmd down -v --remove-orphans 2>/dev/null || true
  log "Cleanup complete."
}
trap cleanup INT TERM EXIT

mkdir -p "${DIST}" "${SNAPSHOT_DIR}" "${DATA_DIR}"
# Tee from the first command onward so a set -e exit still leaves evidence.
exec > >(tee -a "${DIST}/run.log") 2>&1

log "Artefacts: ${DIST}"
log "Config: SOAK_HOURS=${SOAK_HOURS} series=${SOAK_IDX_SERIES} elements=${SOAK_IDX_ELEMENTS} workers=${PROBE_WORKERS} write_rps=${SOAK_WRITE_RPS}"

# ── build ──────────────────────────────────────────────────────────────────
log "Building soak-driver and banyandb images from the current source tree..."
# `compose up` reuses any cached image and will NOT rebuild on source changes,
# so without an explicit build a run can boot a stale binary.
compose_cmd build soak-driver "${BANYANDB_UP_SERVICE}"

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  PHASE 0 — Baseline                                                     ║
# ╚══════════════════════════════════════════════════════════════════════════╝
log "=== PHASE 0: Baseline ==="

# Phase 0 MUST start empty. DATA_DIR is a host bind mount, so `compose down -v`
# (which only drops volumes) leaves it behind, and a previous run's persisted
# schema would make every registry Create return AlreadyExists — silently
# keeping the OLD schema (e.g. a stale index-rule type or id) no matter what
# this run defines.
log "Wiping ${DATA_DIR} so Phase 0 starts clean..."
rm -rf "${DATA_DIR:?}"/*

compose_cmd up -d "${BANYANDB_UP_SERVICE}"
wait_healthy

BANYANDB_IMAGE_DIGEST=$(docker inspect "${BANYANDB_CONTAINER_NAME}" --format '{{.Image}}' 2>/dev/null || echo unknown)
BANYANDB_CPU_LIMIT=$(docker inspect "${BANYANDB_CONTAINER_NAME}" --format '{{.HostConfig.NanoCpus}}' 2>/dev/null || echo unknown)
BANYANDB_MEM_LIMIT=$(docker inspect "${BANYANDB_CONTAINER_NAME}" --format '{{.HostConfig.Memory}}' 2>/dev/null || echo unknown)
log "banyandb image=${BANYANDB_IMAGE_DIGEST} cpus_nanocpu=${BANYANDB_CPU_LIMIT} memory_bytes=${BANYANDB_MEM_LIMIT}"

log "Seeding the log-shaped stream fixture..."
SEED_OUT=$(driver seed-fixture \
  --engine "${ENGINE}" \
  --addr "${BANYANDB_GRPC_CONTAINER}" \
  --series "${SOAK_IDX_SERIES}" \
  --elements "${SOAK_IDX_ELEMENTS}")
log "seed-fixture output: ${SEED_OUT}"
T1_MS=$(echo "${SEED_OUT}" | grep -E '^[0-9]+$' | tail -1)
if [[ -z "${T1_MS}" ]] || ! [[ "${T1_MS}" =~ ^[0-9]+$ ]]; then
  log "ERROR: seed-fixture did not return a valid T1 timestamp"
  exit 1
fi
log "T1 snapshot timestamp: ${T1_MS} ms"

# Schema flush is async (~5 s); seed-fixture polls until data is queryable, but
# the schema server also needs to persist so the snapshot captures schema segs.
sleep 8

log "Recording baseline..."
driver record-baseline \
  --engine "${ENGINE}" \
  --addr "${BANYANDB_GRPC_CONTAINER}" \
  --catalog "${ENGINE_CATALOG}" \
  --until "${T1_MS}" \
  --out /artifacts/baseline.json

baseline_count=$(python3 -c \
  "import json; d=json.load(open('${DIST}/baseline.json')); print(sum(len(r.get('elements') or []) for r in d))" \
  2>/dev/null || echo 0)
log "Baseline records captured: ${baseline_count}"
if [[ "${baseline_count}" == "0" ]]; then
  log "ERROR: baseline contains zero records despite seeding — abort"
  exit 1
fi

log "Stopping BanyanDB to snapshot data..."
compose_cmd stop
log "Copying data to ${SNAPSHOT_DIR}..."
cp -a "${DATA_DIR}/." "${SNAPSHOT_DIR}/"
snap_size=$(du -sb "${SNAPSHOT_DIR}" 2>/dev/null | awk '{print $1}')
log "Snapshot size: ${snap_size:-0} bytes"

log "Tearing down Phase 0 stack..."
trap - EXIT
compose_cmd down -v --remove-orphans
trap cleanup INT TERM EXIT

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  PHASE 1 — Soak                                                         ║
# ╚══════════════════════════════════════════════════════════════════════════╝
log "=== PHASE 1: Soak (duration=${SOAK_HOURS}h) ==="

# Restore rather than continue, so the run also proves the native index
# survives a stop/start from persisted segments.
log "Restoring data snapshot..."
rm -rf "${DATA_DIR:?}"/*
cp -a "${SNAPSHOT_DIR}/." "${DATA_DIR}/"

compose_cmd up -d "${BANYANDB_UP_SERVICE}"
wait_healthy

mkdir -p "${DIST}/pprof-start"
# Capture the starting goroutine count so the gate has a real baseline: the
# driver prints the count as its last stdout line.
START_GOROUTINES=$(driver pprof-grab --addr "${BANYANDB_PPROF_CONTAINER}" \
  --out-dir /artifacts/pprof-start 2>/dev/null | tail -1 || true)
START_GOROUTINES="${START_GOROUTINES##*[!0-9]}"
START_GOROUTINES="${START_GOROUTINES:-0}"
log "Initial pprof captured (goroutines=${START_GOROUTINES})."

# ── background loops ───────────────────────────────────────────────────────
# Every loop is fail-tolerant: one bad sample must not kill the observation
# window. Setup above is fail-fast so a misconfigured run dies in minutes.

# 1. BanyanDB log tail, bounded via --since snapshots rather than -f >>.
(
  while true; do
    docker logs --since 60s "${BANYANDB_CONTAINER_NAME}" >> "${DIST}/banyand.log" 2>&1 || true
    sleep 60
  done
) &
LOGS_PID=$!

# 2. Memory-alert grep. A "budget"/"MemoryTracker"/"panic" line is a signal the
#    RSS curve alone would not surface until it is already too late.
(
  tail -f "${DIST}/banyand.log" 2>/dev/null | \
    grep --line-buffered -iE "budget|MemoryTracker|panic|vectorized" \
    >> "${DIST}/memory-alerts.log" || true
) &
GREP_PID=$!

# 3. CPU + RSS + disk sampler. CPU is the net-new signal: the G5d harness
#    records the cgroup LIMIT but never samples UTILIZATION. Both containers are
#    sampled because work migrating from driver to server only shows in the ratio.
RES_CSV="${DIST}/resource-trend.csv"
echo "ts_utc,cpu_pct,rss_bytes,mem_pct,block_io,disk_bytes" > "${RES_CSV}"
(
  while true; do
    TS_NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    read -r cpu mem_used mem_pct blockio <<< "$(docker stats "${BANYANDB_CONTAINER_NAME}" --no-stream \
      --format '{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}|{{.BlockIO}}' 2>/dev/null | tr -d ' ' | \
      awk -F'|' '{split($2,m,"/"); print $1, m[1], $3, $4}')"
    # MemUsage is a human string like "512MiB"; normalize to bytes.
    mem_bytes=$(python3 -c "
import sys,re
s='${mem_used}'
m=re.match(r'([0-9.]+)([A-Za-z]+)',s or '')
if not m: print(0); sys.exit()
v=float(m.group(1)); u=m.group(2).lower()
mult={'b':1,'kb':1024,'mb':1024**2,'gb':1024**3,'kib':1024,'mib':1024**2,'gib':1024**3}.get(u,1)
print(int(v*mult))
" 2>/dev/null || echo 0)
    cpu_pct=$(python3 -c "
import re
m=re.match(r'([0-9.]+)', '${cpu}' or '')
print(m.group(1) if m else 0)
" 2>/dev/null || echo 0)
    DISK=$(du -sb "${DATA_DIR}" 2>/dev/null | awk '{print $1}' || echo 0)
    echo "${TS_NOW},${cpu_pct},${mem_bytes},${mem_pct},${blockio},${DISK}" >> "${RES_CSV}" || true
    sleep "${RESOURCE_INTERVAL_SEC}"
  done
) &
RES_PID=$!

# 4. Native-index metrics scraper. This is the index-specific evidence the
#    G5d harness never records.
METRICS_CSV="${DIST}/index-metrics.csv"
METRICS_HEADER="ts_utc,sidx_doc_count,idx_doc_count,sidx_snapshot_bytes,idx_snapshot_bytes,time_disjoint,time_contained,time_overlap_trie,time_overlap_fallback,candidates_pruned,idx_updates,idx_errors"
echo "${METRICS_HEADER}" > "${METRICS_CSV}"
# metric_value <name-substring> — sums every matching sample line's value.
metric_value() {
  docker exec "${BANYANDB_CONTAINER_NAME}" sh -c "wget -qO- http://127.0.0.1:2121/metrics" 2>/dev/null \
    | grep -E "$1" | awk '{s+=$NF} END{printf "%d", s+0}'
}
metric_value_class() {
  docker exec "${BANYANDB_CONTAINER_NAME}" sh -c "wget -qO- http://127.0.0.1:2121/metrics" 2>/dev/null \
    | grep -F "native_time_segments_total" | grep -F "class=\"$1\"" | awk '{s+=$NF} END{printf "%d", s+0}'
}
(
  while true; do
    TS_NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    S="banyandb_stream_storage_"
    E="banyandb_stream_tst_"
    echo "${TS_NOW},$(metric_value "${S}inverted_index_total_doc_count"),$(metric_value "${E}inverted_index_total_doc_count"),$(metric_value "${S}inverted_index_native_snapshot_bytes"),$(metric_value "${E}inverted_index_native_snapshot_bytes"),$(metric_value_class disjoint),$(metric_value_class contained),$(metric_value_class overlap_trie),$(metric_value_class overlap_fallback),$(metric_value "${S}native_time_candidates_pruned_total"),$(metric_value "${E}inverted_index_total_updates"),$(metric_value "${E}inverted_index_total_errors")" >> "${METRICS_CSV}" || true
    sleep "${RESOURCE_INTERVAL_SEC}"
  done
) &
METRICS_PID=$!

SOAK_END=$(( $(date +%s) + SOAK_HOURS_SEC ))

# 5. Latency probe. One probe process per interval, each running the whole
#    catalog concurrently across PROBE_WORKERS, so percentiles describe
#    concurrent execution rather than a serialised approximation.
(
  while (( $(date +%s) < SOAK_END )); do
    sleep 1
    STAMP="$(date +%Y%m%dT%H%M%S)"
    mkdir -p "${DIST}/latency"
    driver latency-probe \
      --addr "${BANYANDB_GRPC_CONTAINER}" \
      --catalog "${ENGINE_CATALOG}" \
      --until "${T1_MS}" \
      --workers "${PROBE_WORKERS}" \
      --interval-min "${PROBE_INTERVAL_MIN}" \
      --duration-min 1 \
      --out "/artifacts/latency/latency-${STAMP}.json" \
      > "${DIST}/latency-${STAMP}.log" 2>&1 || log "WARN: latency-probe ${STAMP} exited non-zero"
  done
) &
PROBE_PID=$!

# 6. Parity loop. Same authority as the G5d harness: results must equal the
#    baseline. Failures are counted, not fatal, so a divergence count is
#    available at the end.
(
  while (( $(date +%s) < SOAK_END )); do
    sleep "${PARITY_INTERVAL_SEC}"
    (( $(date +%s) >= SOAK_END )) && break
    STAMP="$(date +%Y%m%dT%H%M%S)"
    driver replay-and-diff \
      --engine "${ENGINE}" \
      --addr "${BANYANDB_GRPC_CONTAINER}" \
      --catalog "${ENGINE_CATALOG}" \
      --baseline /artifacts/baseline.json \
      --report "/artifacts/diff-${STAMP}.json" > "${DIST}/diff-${STAMP}.log" 2>&1 || \
      log "WARN: replay-and-diff ${STAMP} reported divergences"
  done
) &
PARITY_PID=$!

# 7. pprof loop. The goroutine count is recorded per capture so the leak gate
# compares post-warmup samples rather than "restart" against "end of run": the
# start baseline predates any query or write traffic, so its delta measures
# steady-state goroutine buildup rather than a leak.
echo "ts_utc,goroutines" > "${DIST}/goroutine-trend.csv"
(
  while (( $(date +%s) < SOAK_END )); do
    sleep "${PPROF_INTERVAL_SEC}"
    (( $(date +%s) >= SOAK_END )) && break
    STAMP="$(date +%Y%m%dT%H%M%S)"
    mkdir -p "${DIST}/pprof-${STAMP}"
    CNT=$(driver pprof-grab --addr "${BANYANDB_PPROF_CONTAINER}" \
      --out-dir "/artifacts/pprof-${STAMP}" 2>/dev/null | tail -1 || echo 0)
    CNT="${CNT##*[!0-9]}"
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ),${CNT:-0}" >> "${DIST}/goroutine-trend.csv" || true
  done
) &
PPROF_PID=$!

# 8. Background write load, into the short-TTL load group so it stays OUTSIDE
#    the parity window and self-expires. This is what forces merges and
#    compaction, which is where the time-bounds and mem-segment gates bite.
WRITELOG="${DIST}/write-load.log"
if [[ "${SOAK_WRITE_RPS}" != "0" ]]; then
  (
    while (( $(date +%s) < SOAK_END )); do
      REMAIN=$(( SOAK_END - $(date +%s) ))
      (( REMAIN <= 0 )) && break
      SLEEP_SEC=$(( REMAIN > 30 ? 30 : REMAIN ))
      driver write-load \
        --engine "${ENGINE}" \
        --addr "${BANYANDB_GRPC_CONTAINER}" \
        --traces 20 \
        --spans 50 \
        --rps "${SOAK_WRITE_RPS}" \
        --duration "${SLEEP_SEC}s" >> "${WRITELOG}" 2>&1 || log "WARN: write-load sweep failed"
    done
  ) &
  WRITE_PID=$!
else
  log "SOAK_WRITE_RPS=0 — background write load disabled"
fi

# Container health is polled too: a crash+restart mid-soak would otherwise show
# up only as an unexplained latency curve.
echo "ts_utc,health" > "${DIST}/health-trend.csv"
(
  while true; do
    STATUS=$(docker inspect -f '{{.State.Health.Status}}' "${BANYANDB_CONTAINER_NAME}" 2>/dev/null || echo unknown)
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ),${STATUS}" >> "${DIST}/health-trend.csv"
    sleep "${RESOURCE_INTERVAL_SEC}"
  done
) &
HEALTH_PID=$!

log "Soak running for ${SOAK_HOURS}h; waiting..."
sleep "${SOAK_HOURS_SEC}"

log "=== Phase 1 complete — stopping loops ==="
for pid in "${LOGS_PID}" "${GREP_PID}" "${RES_PID}" "${METRICS_PID}" "${PROBE_PID}" "${PARITY_PID}" "${PPROF_PID}" "${HEALTH_PID}"; do
  kill "${pid}" 2>/dev/null || true
done
if [[ -n "${WRITE_PID:-}" ]]; then kill "${WRITE_PID}" 2>/dev/null || true; fi
# The loop subshells above each launched `docker compose run` children that
# outlive them, so a bare `wait` would block on those orphans until the outer
# timeout SIGKILLs this script — which skips the EXIT trap and leaves the
# compose stack running. Kill ONLY this run's descendants: a host-wide
# `pkill -f 'docker compose.*run'` would also kill another soak or an unrelated
# compose job owned by the same user.
LOOP_PIDS=("${LOGS_PID}" "${GREP_PID}" "${RES_PID}" "${METRICS_PID}" "${PROBE_PID}" "${PARITY_PID}" "${PPROF_PID}" "${HEALTH_PID}")
[[ -n "${WRITE_PID:-}" ]] && LOOP_PIDS+=("${WRITE_PID}")
for pid in "${LOOP_PIDS[@]}"; do
  # Reap the loop's own `docker compose run` children before killing the loop.
  pkill -TERM -P "${pid}" 2>/dev/null || true
  kill "${pid}" 2>/dev/null || true
done
sleep 3

# Final captures before teardown: the stack still has to be up for these.
mkdir -p "${DIST}/pprof-final"
FINAL_GOROUTINES=$(driver pprof-grab --addr "${BANYANDB_PPROF_CONTAINER}" --out-dir /artifacts/pprof-final 2>/dev/null | tail -1 || echo 0)
log "Final pprof captured (goroutines=${FINAL_GOROUTINES:-?})"

FINAL_STAMP=$(date +%Y%m%dT%H%M%S)
driver replay-and-diff \
  --engine "${ENGINE}" \
  --addr "${BANYANDB_GRPC_CONTAINER}" \
  --catalog "${ENGINE_CATALOG}" \
  --baseline /artifacts/baseline.json \
  --report "/artifacts/diff-final.json" > "${DIST}/diff-final.log" 2>&1 || \
  log "WARN: final replay-and-diff reported divergences"

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  Gates                                                                  ║
# ╚══════════════════════════════════════════════════════════════════════════╝
log "=== Evaluating gates ==="
GATE_STATUS=0
python3 "${REPO_ROOT}/scripts/soak-stream-index-gates.py" \
  --dist "${DIST}" \
  --data-dir "${DATA_DIR}" \
  --goroutines-start "${START_GOROUTINES:-0}" \
  --goroutines-final "${FINAL_GOROUTINES:-0}" \
  --json "${DIST}/gates.json" || GATE_STATUS=$?
log "Gate evaluator exit status: ${GATE_STATUS}"

log "Tearing down compose stack..."
trap - EXIT
compose_cmd down -v --remove-orphans

log "Done. Artefacts in ${DIST}"
if [[ -f "${DIST}/gates.json" ]]; then
  python3 -c "
import json
g=json.load(open('${DIST}/gates.json'))
print('VERDICT:', 'PASS' if g['pass'] else 'FAIL')
" 2>/dev/null || true
fi
# Propagate the verdict: a failed gate must fail the run, not print FAIL and
# exit 0.
exit "${GATE_STATUS}"