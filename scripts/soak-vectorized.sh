#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# G5d soak harness orchestrator.
#
# Configuration (set as env vars before running):
#   SOAK_ENGINE         – engine to test: measure|trace (default measure)
#   WARMUP_MIN          – minutes OAP has to write data before baseline snapshot (default 60)
#   SOAK_HOURS          – duration of the Phase 1 soak run in hours (default 48)
#   PPROF_INTERVAL_MIN  – minutes between each pprof capture (default 30)
#   PARITY_INTERVAL_MIN – minutes between each replay-and-diff run (default 5)
#   SMOKE               – set to 1 for a quick ~30-min smoke run (overrides durations)
#
# Trace-engine additional env (SOAK_ENGINE=trace):
#   SOAK_TRACE_SPANS_PER_TRACE – spans per trace in the fixture (default: driver default)
#   SOAK_TRACE_SERVICES        – number of services in the fixture (unused by driver directly;
#                                kept for documentation / future --services flag)
#   SOAK_WRITE_RPS             – max spans/s for background write-load (default 500)
#   SOAK_HEAP_GROWTH_MAX_PCT   – advisory heap-growth threshold pct recorded in summary (default 10)
#
# Artefacts are written under dist/soak/<timestamp>/ relative to the repo root.
#
# Usage:
#   ./scripts/soak-vectorized.sh
#   SMOKE=1 ./scripts/soak-vectorized.sh
#   SOAK_ENGINE=trace SMOKE=1 ./scripts/soak-vectorized.sh

set -euo pipefail

# ── engine selection ──────────────────────────────────────────────────────────
SOAK_ENGINE="${SOAK_ENGINE:-measure}"
# Topology: `standalone` is a single node; `distributed` is 1 liaison + 2 data
# nodes. The distinction is not cosmetic — a data node only emits the native
# columnar wire frame when it is distributed, so a standalone run exercises the
# vec COMPUTE path and always falls back to protobuf on the wire.
SOAK_TOPOLOGY="${SOAK_TOPOLOGY:-standalone}"
if [[ "${SOAK_TOPOLOGY}" != "standalone" && "${SOAK_TOPOLOGY}" != "distributed" ]]; then
  echo "ERROR: SOAK_TOPOLOGY must be 'standalone' or 'distributed' (got '${SOAK_TOPOLOGY}')"
  exit 1
fi
if [[ "${SOAK_TOPOLOGY}" == "distributed" && "${SOAK_ENGINE}" == "measure" ]]; then
  echo "ERROR: SOAK_TOPOLOGY=distributed is wired for the trace/stream branch only"
  exit 1
fi
if [[ "${SOAK_ENGINE}" != "measure" && "${SOAK_ENGINE}" != "trace" && "${SOAK_ENGINE}" != "stream" ]]; then
  echo "ERROR: SOAK_ENGINE must be 'measure', 'trace' or 'stream' (got '${SOAK_ENGINE}')"
  exit 1
fi

# ── configuration ────────────────────────────────────────────────────────────
WARMUP_MIN="${WARMUP_MIN:-60}"
SOAK_HOURS="${SOAK_HOURS:-48}"
PPROF_INTERVAL_MIN="${PPROF_INTERVAL_MIN:-30}"
PARITY_INTERVAL_MIN="${PARITY_INTERVAL_MIN:-5}"

if [[ "${SMOKE:-}" == "1" ]]; then
  # SMOKE skips the OAP-warmup wait — parity is driven by deterministic
  # data seeded by `soak-driver seed-fixture`, so we don't depend on OAP
  # propagation timing.
  WARMUP_MIN=0
  SOAK_HOURS=0.34   # ~20 min — fits inside a tractable smoke window
  PPROF_INTERVAL_MIN=1
  PARITY_INTERVAL_MIN=1
fi

SEED_ROWS="${SEED_ROWS:-1000}"

# Trace-engine supplementary knobs (only active when SOAK_ENGINE=trace).
SOAK_TRACE_SPANS_PER_TRACE="${SOAK_TRACE_SPANS_PER_TRACE:-}"
SOAK_TRACE_SERVICES="${SOAK_TRACE_SERVICES:-}"
# Stream-engine supplementary knobs (only active when SOAK_ENGINE=stream).
SOAK_STREAM_SERIES="${SOAK_STREAM_SERIES:-}"
SOAK_STREAM_ELEMENTS="${SOAK_STREAM_ELEMENTS:-}"
SOAK_WRITE_RPS="${SOAK_WRITE_RPS:-500}"
SOAK_HEAP_GROWTH_MAX_PCT="${SOAK_HEAP_GROWTH_MAX_PCT:-10}"

SOAK_HOURS_SEC=$(awk "BEGIN{printf \"%d\", ${SOAK_HOURS}*3600}")
WARMUP_SEC=$(( WARMUP_MIN * 60 ))
PPROF_INTERVAL_SEC=$(( PPROF_INTERVAL_MIN * 60 ))
PARITY_INTERVAL_SEC=$(( PARITY_INTERVAL_MIN * 60 ))

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/test/soak/docker-compose.soak.yaml"
# MEASURE path uses the default catalog; TRACE path uses its own catalog.
CATALOG_MEASURE="${REPO_ROOT}/cmd/soak-driver/catalog/default.json"
CATALOG_TRACE="${REPO_ROOT}/cmd/soak-driver/catalog/trace.json"
BANYANDB_GRPC="localhost:17912"
BANYANDB_PPROF="localhost:6060"
# Inside the compose network the DB service is reachable by service name.
BANYANDB_GRPC_CONTAINER="banyandb:17912"
BANYANDB_PPROF_CONTAINER="banyandb:6060"
# The service compose brings up (its deps follow), the container health/inspect
# reads, and the roles pprof is captured from. Standalone has one of each; the
# distributed topology queries through the liaison but must profile all three,
# because frame ENCODE buffers live on the data nodes and DECODE plus the
# cross-node merge live on the liaison — a single-process capture sees neither.
BANYANDB_UP_SERVICE="banyandb"
PPROF_ROLES=( "node:banyandb:6060" )
DATA_DIRS=( "${REPO_ROOT}/test/soak/data" )
if [[ "${SOAK_TOPOLOGY}" == "distributed" ]]; then
  COMPOSE_FILE="${REPO_ROOT}/test/soak/docker-compose.soak-distributed.yaml"
  BANYANDB_GRPC_CONTAINER="soak-liaison:17912"
  BANYANDB_PPROF_CONTAINER="soak-liaison:6060"
  BANYANDB_UP_SERVICE="soak-liaison"
  PPROF_ROLES=( "liaison:soak-liaison:6060" "data-1:soak-data-1:6060" "data-2:soak-data-2:6060" )
  DATA_DIRS=(
    "${REPO_ROOT}/test/soak/data-distributed/liaison"
    "${REPO_ROOT}/test/soak/data-distributed/data-1"
    "${REPO_ROOT}/test/soak/data-distributed/data-2"
  )
fi

# Isolate this soak from other docker-compose workflows on a shared host. The
# compose project name (default "soak" = the compose-file dir basename) and the
# global container name "banyandb" are both collision points: another workflow's
# `compose up`/`down` referencing the same project or a container literally named
# "banyandb" can replace this soak's DB mid-run. Override both with unique names.
export COMPOSE_PROJECT_NAME="${SOAK_COMPOSE_PROJECT:-soakvec}"
if [[ "${SOAK_TOPOLOGY}" == "distributed" ]]; then
  export COMPOSE_PROJECT_NAME="${SOAK_COMPOSE_PROJECT:-soakdist}"
  export BANYANDB_CONTAINER_NAME="${SOAK_BANYANDB_CONTAINER:-soak-liaison}"
else
  export BANYANDB_CONTAINER_NAME="${SOAK_BANYANDB_CONTAINER:-banyandb-soakvec}"
fi

# Pass host UID/GID to compose so the BanyanDB container writes the
# bind-mounted /data dir as the host user (otherwise root-owned files
# break snapshot/restore from the host shell).
export SOAK_UID="$(id -u)"
export SOAK_GID="$(id -g)"

RUN_TS="$(date +%Y%m%dT%H%M%S)"
DIST="${REPO_ROOT}/dist/soak/${RUN_TS}"
DATA_DIR="${REPO_ROOT}/test/soak/data"
SNAPSHOT_DIR="${DIST}/data-snapshot"

# ── helpers ──────────────────────────────────────────────────────────────────
log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

compose_cmd() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

wait_banyandb_healthy() {
  log "Waiting for BanyanDB to become healthy..."
  local attempts=0
  until curl -sf "http://localhost:17913/api/healthz" >/dev/null 2>&1; do
    attempts=$(( attempts + 1 ))
    if (( attempts > 120 )); then
      log "ERROR: BanyanDB did not become healthy after 120 attempts"
      return 1
    fi
    sleep 5
  done
  log "BanyanDB is healthy."
}

# wait_banyandb_container_healthy waits on the container's own compose
# healthcheck via `docker inspect`, so it needs no host-published port. The TRACE
# path uses this (its driver reaches BanyanDB over the compose network), letting
# the soak run even when the host's standard banyand ports are already taken by
# another instance — the trace banyandb publishes only random host ports.
wait_banyandb_container_healthy() {
  log "Waiting for BanyanDB container to become healthy..."
  local attempts=0
  until [[ "$(docker inspect -f '{{.State.Health.Status}}' "${BANYANDB_CONTAINER_NAME}" 2>/dev/null)" == "healthy" ]]; do
    attempts=$(( attempts + 1 ))
    if (( attempts > 120 )); then
      log "ERROR: BanyanDB container did not become healthy after 120 attempts"
      return 1
    fi
    sleep 5
  done
  log "BanyanDB container is healthy."
}

# soak_driver_container runs the soak-driver inside the compose network via
# `docker compose run --rm`. The DIST directory is bind-mounted as /artifacts
# so the driver can read/write baseline.json, diff-*.json, pprof dirs, etc.
# The driver reaches BanyanDB by service name over the compose network.
#
# Usage: soak_driver_container <subcommand> [args...]
soak_driver_container() {
  SOAK_DIST_DIR="${DIST}" compose_cmd run --rm \
    -v "${DIST}:/artifacts" \
    soak-driver "$@"
}

# pprof_grab_all captures heap+goroutine from every role in PPROF_ROLES into
# <out_root>/<role>/ (standalone keeps its single "node" role). Returns the
# goroutine count of the FIRST role so existing callers keep their contract.
pprof_grab_all() {
  local out_root="$1" role spec name addr first_count=""
  for spec in "${PPROF_ROLES[@]}"; do
    name="${spec%%:*}"
    addr="${spec#*:}"
    mkdir -p "${out_root}/${name}"
    local count
    count=$(soak_driver_container pprof-grab \
      --addr "${addr}" \
      --out-dir "/artifacts/${out_root#${DIST}/}/${name}" 2>/dev/null | tail -1 || true)
    [[ -z "${first_count}" ]] && first_count="${count}"
  done
  echo "${first_count}"
}

# capture_frame_totals reads the columnar-frame counters off each node and echoes
# "encoded_sum decoded" so the summary can record them. Without this the run's
# central evidence lives only in a live scrape and is gone once compose tears the
# stack down — the first distributed run finished with no frame numbers on disk.
capture_frame_totals() {
  local engine="$1" enc=0 dec=0 spec name host v
  for spec in "${PPROF_ROLES[@]}"; do
    name="${spec%%:*}"; host="${spec#*:}"; host="${host%%:*}"
    [[ "${name}" == liaison* ]] && continue
    v=$(docker exec "${host}" sh -c "wget -qO- http://127.0.0.1:2121/metrics" 2>/dev/null \
      | grep -oE "vec_frame_encoded\{engine=\"${engine}\"\} [0-9]+" | grep -oE '[0-9]+$' || echo 0)
    enc=$(( enc + ${v:-0} ))
  done
  dec=$(docker exec "${BANYANDB_CONTAINER_NAME}" sh -c "wget -qO- http://127.0.0.1:2121/metrics" 2>/dev/null \
    | grep -oE "vec_frame_decoded\{engine=\"${engine}\"\} [0-9]+" | grep -oE '[0-9]+$' || echo 0)
  echo "${enc} ${dec:-0}"
}

# assert_frames_flowing is the Phase -1 gate. The whole point of the distributed
# soak is the native columnar frame, and a run that silently falls back to
# protobuf would still produce six clean hours of parity — which is exactly how a
# 48h standalone soak passed while never encoding a single frame. So prove the
# frame is carrying traffic before committing the window, and abort if it is not.
#
# The counters are published as gauges refreshed by a periodic collector, so a
# freshly-served query is not visible until the next tick; poll rather than probe.
assert_frames_flowing() {
  local engine="$1" deadline=$(( SECONDS + 180 )) enc_total dec liaison_ok=0
  log "Phase -1: verifying the ${engine} columnar frame is actually in use..."
  while (( SECONDS < deadline )); do
    enc_total=0
    for spec in "${PPROF_ROLES[@]}"; do
      local name="${spec%%:*}" host="${spec#*:}"
      host="${host%%:*}"
      [[ "${name}" == liaison* ]] && continue
      local v
      v=$(docker exec "${host}" sh -c "wget -qO- http://127.0.0.1:2121/metrics" 2>/dev/null \
        | grep -oE "vec_frame_encoded\{engine=\"${engine}\"\} [0-9]+" | grep -oE '[0-9]+$' || echo 0)
      enc_total=$(( enc_total + ${v:-0} ))
    done
    dec=$(docker exec "${BANYANDB_CONTAINER_NAME}" sh -c "wget -qO- http://127.0.0.1:2121/metrics" 2>/dev/null \
      | grep -oE "vec_frame_decoded\{engine=\"${engine}\"\} [0-9]+" | grep -oE '[0-9]+$' || echo 0)
    (( ${dec:-0} > 0 )) && liaison_ok=1
    if (( enc_total > 0 && liaison_ok == 1 )); then
      log "Phase -1 OK: frames encoded=${enc_total} across data nodes, decoded=${dec} on the liaison."
      return 0
    fi
    sleep 10
  done
  log "ERROR: Phase -1 FAILED — encoded=${enc_total:-0} decoded=${dec:-0} for engine ${engine}."
  log "       The cluster is not using the columnar wire frame; a soak now would prove nothing."
  return 1
}

# soak_driver is kept for the MEASURE path (host binary) — unchanged from the
# original harness. The TRACE path uses soak_driver_container instead.
soak_driver() {
  "${REPO_ROOT}/bin/soak-driver" "$@"
}

# ── cleanup trap ─────────────────────────────────────────────────────────────
cleanup() {
  log "Caught signal — tearing down compose stack..."
  compose_cmd down -v --remove-orphans 2>/dev/null || true
  log "Cleanup complete."
}
trap cleanup INT TERM EXIT

# ── prepare output dirs ───────────────────────────────────────────────────────
mkdir -p "${DIST}" "${SNAPSHOT_DIR}" "${DATA_DIR}"

# Tee everything from this point into the run log so silent failures leave
# evidence behind. Earlier output (build) is already in stdout.
exec > >(tee -a "${DIST}/run.log") 2>&1

log "Run artefacts will be written to: ${DIST}"
log "Config: ENGINE=${SOAK_ENGINE} WARMUP_MIN=${WARMUP_MIN} SOAK_HOURS=${SOAK_HOURS} PPROF_INTERVAL_MIN=${PPROF_INTERVAL_MIN} PARITY_INTERVAL_MIN=${PARITY_INTERVAL_MIN}"

# ── engine dispatch ───────────────────────────────────────────────────────────
# The MEASURE path is the original harness, preserved byte-for-byte in
# behavior.  The TRACE path is gated entirely in the `if` branch below;
# nothing in the measure path is touched.

if [[ "${SOAK_ENGINE}" == "trace" || "${SOAK_ENGINE}" == "stream" ]]; then
# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  TRACE/STREAM ENGINE — Instrument 1 (containerized correctness + survival)║
# ╚══════════════════════════════════════════════════════════════════════════╝
# This branch is engine-parameterized: everything is driven by ${SOAK_ENGINE}
# (trace|stream) and the ${ENGINE_CATALOG} catalog, so the identical two-phase
# orchestration serves both engines.
ENGINE="${SOAK_ENGINE}"
ENGINE_CATALOG="/catalog/${ENGINE}.json"
# Engine-specific seed args: trace tunes spans/trace; stream tunes series/elements.
if [[ "${ENGINE}" == "trace" ]]; then
  SEED_ARGS=( ${SOAK_TRACE_SPANS_PER_TRACE:+--spans "${SOAK_TRACE_SPANS_PER_TRACE}"} )
else
  SEED_ARGS=( ${SOAK_STREAM_SERIES:+--series "${SOAK_STREAM_SERIES}"} ${SOAK_STREAM_ELEMENTS:+--elements "${SOAK_STREAM_ELEMENTS}"} )
fi

# Bind banyandb's host ports to random free ports (host port 0). The trace
# driver is containerized and reaches banyandb over the compose network, and
# readiness is checked via the container healthcheck — so no fixed host port is
# needed, and the soak coexists with another banyand already on the standard ports.
export SOAK_HOST_GRPC=0 SOAK_HOST_HTTP=0 SOAK_HOST_PPROF=0 SOAK_HOST_METRICS=0

# Build (or refresh) the soak-driver image so it embeds this branch's binary.
log "Building soak-driver container image..."
SOAK_DIST_DIR="${DIST}" compose_cmd build soak-driver
log "soak-driver image built."

# Build banyand from the current source tree too. `compose up` reuses any
# cached image and will NOT rebuild on source changes, so without an explicit
# build Phase 0 can boot a stale binary that lacks new flags and dies with
# "unknown flag". Build once; both phases reuse it.
log "Building banyand container image (current source)..."
compose_cmd build "${BANYANDB_UP_SERVICE}"
log "banyand image built."

# Record image digest for reproducibility.
# tr -d '\n' because the `|| echo unknown` fallback and docker's own output can
# each contribute a newline, which lands inside the JSON string in summary.json.
DRIVER_IMAGE_DIGEST=$(docker inspect \
  "$(SOAK_DIST_DIR="${DIST}" compose_cmd images -q soak-driver 2>/dev/null | head -1)" \
  --format '{{.Id}}' 2>/dev/null | tr -d '\n' || echo "unknown")
DRIVER_IMAGE_DIGEST="${DRIVER_IMAGE_DIGEST:-unknown}"
log "soak-driver image digest: ${DRIVER_IMAGE_DIGEST}"

# ── PHASE 0 — Baseline ─────────────────────────────────────────────────
log "=== ${SOAK_ENGINE} PHASE 0: Baseline ==="

# Phase 0 MUST start from an empty data dir. The dir is a host bind mount, so
# `compose down -v` (which only drops volumes) leaves it behind: a previous run's
# elements would pollute the baseline, and — worse — its persisted schema would
# make every registry Create return AlreadyExists, silently keeping the OLD
# schema (e.g. a stale index-rule type/id) no matter what this run defines.
for d in "${DATA_DIRS[@]}"; do
  log "Wiping ${d} so Phase 0 starts from a clean slate..."
  mkdir -p "${d}"
  rm -rf "${d:?}"/*
done

SOAK_DATA_DIR="${DATA_DIR}" compose_cmd up -d "${BANYANDB_UP_SERVICE}"
wait_banyandb_container_healthy

# Record BanyanDB container image digest.
BANYANDB_IMAGE_DIGEST=$(docker inspect "${BANYANDB_CONTAINER_NAME}" --format '{{.Image}}' 2>/dev/null || echo "unknown")
log "BanyanDB image digest: ${BANYANDB_IMAGE_DIGEST}"

# Record effective resource limits from running container.
BANYANDB_CPU_LIMIT=$(docker inspect "${BANYANDB_CONTAINER_NAME}" \
  --format '{{.HostConfig.NanoCpus}}' 2>/dev/null || echo "unknown")
BANYANDB_MEM_LIMIT=$(docker inspect "${BANYANDB_CONTAINER_NAME}" \
  --format '{{.HostConfig.Memory}}' 2>/dev/null || echo "unknown")
log "BanyanDB limits: cpus_nanocpu=${BANYANDB_CPU_LIMIT} memory_bytes=${BANYANDB_MEM_LIMIT}"

log "Seeding deterministic trace fixture..."
# seed-fixture prints T1_MS as the last line of stdout; the driver also
# prints a T1_MS=<value> diagnostic line — grab the bare integer final line.
SEED_OUT=$(soak_driver_container seed-fixture \
  --engine "${ENGINE}" \
  --addr "${BANYANDB_GRPC_CONTAINER}" \
  "${SEED_ARGS[@]}")
log "seed-fixture output: ${SEED_OUT}"
T1_MS=$(echo "${SEED_OUT}" | grep -E '^[0-9]+$' | tail -1)
if [[ -z "${T1_MS}" ]] || ! [[ "${T1_MS}" =~ ^[0-9]+$ ]]; then
  log "ERROR: seed-fixture did not return a valid T1 timestamp"
  exit 1
fi
log "T1 snapshot timestamp: ${T1_MS} ms"

# Trace flush is async (~5 s); seed-fixture polls until queryable, but also
# wait for schema-server flush to persist so the snapshot captures schema segs.
log "Waiting 8s for schema-server flush to persist..."
sleep 8

log "Recording ${ENGINE} baseline..."
soak_driver_container record-baseline \
  --engine "${ENGINE}" \
  --addr "${BANYANDB_GRPC_CONTAINER}" \
  --catalog "${ENGINE_CATALOG}" \
  --until "${T1_MS}" \
  --out /artifacts/baseline.json

# Verify baseline is non-empty. The trace driver exits non-zero if any catalog
# query returns empty traces, so reaching here means the baseline is usable;
# this count is purely diagnostic.
baseline_trace_count=$(python3 -c \
  "import json; d=json.load(open('${DIST}/baseline.json')); print(sum(len(r.get('traces') or r.get('elements') or r.get('data_points') or []) for r in d))" \
  2>/dev/null || echo 0)
log "Baseline records captured: ${baseline_trace_count}"
if [[ "${baseline_trace_count}" == "0" ]]; then
  log "ERROR: baseline contains zero records despite seeding — abort"
  exit 1
fi

log "Stopping BanyanDB to snapshot data..."
compose_cmd stop

log "Copying data to ${SNAPSHOT_DIR}..."
for d in "${DATA_DIRS[@]}"; do
  sub="$(basename "${d}")"
  mkdir -p "${SNAPSHOT_DIR}/${sub}"
  cp -a "${d}/." "${SNAPSHOT_DIR}/${sub}/"
done

snap_size=$(du -sb "${SNAPSHOT_DIR}" 2>/dev/null | awk '{print $1}')
log "Snapshot size: ${snap_size:-0} bytes"

log "Tearing down Phase 0 stack..."
trap - EXIT
compose_cmd down -v --remove-orphans
trap cleanup INT TERM EXIT

# ── PHASE 1 — Soak ─────────────────────────────────────────────────────
log "=== ${SOAK_ENGINE} PHASE 1: Soak (duration=${SOAK_HOURS}h) ==="

log "Restoring data snapshot..."
rm -rf "${DATA_DIR:?}"/*
for d in "${DATA_DIRS[@]}"; do
  sub="$(basename "${d}")"
  mkdir -p "${d}"
  rm -rf "${d:?}"/*
  cp -a "${SNAPSHOT_DIR}/${sub}/." "${d}/"
done

SOAK_DATA_DIR="${DATA_DIR}" compose_cmd up -d "${BANYANDB_UP_SERVICE}"
wait_banyandb_container_healthy

# Phase -1 (distributed only): the vectorized engine is the only query engine,
# but that does NOT prove the columnar frame is on the wire. Drive one
# replay so there is traffic to observe, then gate on the frame counters and
# abort before committing the soak window if they are flat.
if [[ "${SOAK_TOPOLOGY}" == "distributed" ]]; then
  log "Phase -1: driving one replay to generate frame traffic..."
  # Same invocation the parity loop uses. A divergence here is not fatal — the
  # frame gate below is what decides — but the command must actually RUN, so its
  # output is logged rather than discarded: an invocation error produces no
  # traffic at all, which the gate would then report as "no frames" and blame on
  # the engine.
  soak_driver_container replay-and-diff \
    --engine "${ENGINE}" \
    --addr "${BANYANDB_GRPC_CONTAINER}" \
    --catalog "${ENGINE_CATALOG}" \
    --baseline /artifacts/baseline.json \
    --report /artifacts/diff-phase-minus-1.json > "${DIST}/phase-minus-1.log" 2>&1 || \
    { log "WARN: Phase -1 probe replay reported a diff — first failures:"; grep -E "FAIL|error" "${DIST}/phase-minus-1.log" | head -4 | sed "s/^/    /"; }
  assert_frames_flowing "${ENGINE}" || exit 1
fi

# Initial pprof grab via the driver container (reaches banyandb:6060 over the
# compose network).
mkdir -p "${DIST}/pprof-start"
pprof_grab_all "${DIST}/pprof-start" >/dev/null
log "Initial pprof captured."

# Record driver container resource limits (inspect just after first run so the
# container has been created by compose).
DRIVER_CPU_LIMIT=$(docker inspect \
  "$(SOAK_DIST_DIR="${DIST}" compose_cmd ps -q soak-driver 2>/dev/null | head -1)" \
  --format '{{.HostConfig.NanoCpus}}' 2>/dev/null || echo "unknown")
DRIVER_MEM_LIMIT=$(docker inspect \
  "$(SOAK_DIST_DIR="${DIST}" compose_cmd ps -q soak-driver 2>/dev/null | head -1)" \
  --format '{{.HostConfig.Memory}}' 2>/dev/null || echo "unknown")
log "soak-driver limits (from last run): cpus_nanocpu=${DRIVER_CPU_LIMIT} memory_bytes=${DRIVER_MEM_LIMIT}"

# Tail BanyanDB logs into persistent log files in the background.
# Use --since snapshots instead of unbounded -f >> to keep the log bounded.
(
  while true; do
    docker logs --since 60s "${BANYANDB_CONTAINER_NAME}" 2>&1 >> "${DIST}/banyand.log" || true
    sleep 60
  done
) &
LOGS_PID=$!

# Grep for memory-alert keywords in the background.
(
  tail -f "${DIST}/banyand.log" 2>/dev/null | \
    grep --line-buffered -iE "budget|MemoryTracker|panic|vectorized" \
    >> "${DIST}/memory-alerts.log" || true
) &
GREP_PID=$!

# RSS + disk advisory sampler — writes to rss-trend.csv (not a gate).
RSS_CSV="${DIST}/rss-trend.csv"
echo "ts_utc,rss_bytes,disk_bytes" > "${RSS_CSV}"
(
  while true; do
    TS_NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    RSS=$(docker stats "${BANYANDB_CONTAINER_NAME}" --no-stream --format '{{.MemUsage}}' 2>/dev/null \
      | awk -F'/' '{print $1}' | tr -d ' MiGBkb' || echo 0)
    # Sum every node's dir: DATA_DIR alone is the standalone path and is empty in
    # the distributed topology, which silently produced an all-zero disk series.
    DISK=$(du -sbc "${DATA_DIRS[@]}" 2>/dev/null | tail -1 | awk '{print $1}' || echo 0)
    echo "${TS_NOW},${RSS},${DISK}" >> "${RSS_CSV}" || true
    sleep 60
  done
) &
RSS_PID=$!

SOAK_END=$(( $(date +%s) + SOAK_HOURS_SEC ))

# Background pprof loop — driver container grabs heap+goroutine every interval.
(
  while (( $(date +%s) < SOAK_END )); do
    sleep "${PPROF_INTERVAL_SEC}"
    (( $(date +%s) >= SOAK_END )) && break
    INTERVAL_TS="$(date +%Y%m%dT%H%M%S)"
    mkdir -p "${DIST}/pprof-${INTERVAL_TS}"
    pprof_grab_all "${DIST}/pprof-${INTERVAL_TS}" >/dev/null || \
      log "WARN: pprof-grab failed at ${INTERVAL_TS}"
    log "pprof captured: pprof-${INTERVAL_TS}"
  done
) &
PPROF_LOOP_PID=$!

# Background parity loop — driver container replays trace catalog every interval.
(
  while (( $(date +%s) < SOAK_END )); do
    sleep "${PARITY_INTERVAL_SEC}"
    (( $(date +%s) >= SOAK_END )) && break
    DIFF_TS="$(date +%Y%m%dT%H%M%S)"
    DIFF_REPORT="/artifacts/diff-${DIFF_TS}.json"
    soak_driver_container replay-and-diff \
      --engine "${ENGINE}" \
      --addr "${BANYANDB_GRPC_CONTAINER}" \
      --catalog "${ENGINE_CATALOG}" \
      --baseline /artifacts/baseline.json \
      --report "${DIFF_REPORT}" || \
      log "WARN: parity divergence detected — see ${DIST}/diff-${DIFF_TS}.json"
    log "parity check done: diff-${DIFF_TS}.json"
  done
) &
PARITY_LOOP_PID=$!

# Background write-load loop — continuous deterministic trace writes into the
# rolling load group, rate-capped at SOAK_WRITE_RPS. Fail-tolerant: a bad
# sweep logs WARN and continues.
# The loop runs in a backgrounded subshell, so its variables cannot reach the
# parent that writes summary.json. Persist progress to files the parent reads.
WRITE_LOAD_ROWS_FILE="${DIST}/write-load-rows"
WRITE_LOAD_LAST_OK_FILE="${DIST}/write-load-last-ok"
echo 0 > "${WRITE_LOAD_ROWS_FILE}"
echo "never" > "${WRITE_LOAD_LAST_OK_FILE}"
(
  WRITE_DURATION_SEC=$(( PARITY_INTERVAL_SEC > 30 ? PARITY_INTERVAL_SEC : 30 ))
  sweep_total=0
  while (( $(date +%s) < SOAK_END )); do
    SWEEP_ROWS=$(soak_driver_container write-load \
      --engine "${ENGINE}" \
      --addr "${BANYANDB_GRPC_CONTAINER}" \
      --rps "${SOAK_WRITE_RPS}" \
      --duration "${WRITE_DURATION_SEC}s" 2>&1 | \
      grep -oE '[0-9]+ (spans|elements)' | awk '{print $1}' | tail -1)
    # Sanitize to digits only: under pipefail a non-zero pipeline could otherwise
    # leave a multiline value (e.g. "56000\n0"), breaking the arithmetic below.
    SWEEP_ROWS=${SWEEP_ROWS//[^0-9]/}
    sweep_total=$(( sweep_total + ${SWEEP_ROWS:-0} ))
    echo "${sweep_total}" > "${WRITE_LOAD_ROWS_FILE}"
    date -u +%Y-%m-%dT%H:%M:%SZ > "${WRITE_LOAD_LAST_OK_FILE}"
    log "write-load sweep: +${SWEEP_ROWS:-0} spans, total=${sweep_total}"
    # Brief pause to avoid tight loop if the soak window just ended.
    sleep 5 || true
  done
) &
WRITE_LOAD_PID=$!

log "${SOAK_ENGINE} soak running for ${SOAK_HOURS} hours. Loops started (pids: pprof=${PPROF_LOOP_PID} parity=${PARITY_LOOP_PID} write-load=${WRITE_LOAD_PID})."

# Wait for soak duration.
REMAINING=$(( SOAK_END - $(date +%s) ))
if (( REMAINING > 0 )); then
  sleep "${REMAINING}"
fi

log "Soak window complete. Collecting final artefacts..."

# Stop background loops gracefully.
kill "${PPROF_LOOP_PID}" "${PARITY_LOOP_PID}" "${WRITE_LOAD_PID}" 2>/dev/null || true
wait "${PPROF_LOOP_PID}" "${PARITY_LOOP_PID}" "${WRITE_LOAD_PID}" 2>/dev/null || true
kill "${LOGS_PID}" "${GREP_PID}" "${RSS_PID}" 2>/dev/null || true

# Final pprof.
mkdir -p "${DIST}/pprof-end"
pprof_grab_all "${DIST}/pprof-end" >/dev/null
log "Final pprof captured."

# Read the frame counters before compose tears the stack down.
FRAME_ENCODED=0
FRAME_DECODED=0
if [[ "${SOAK_TOPOLOGY}" == "distributed" ]]; then
  read -r FRAME_ENCODED FRAME_DECODED <<<"$(capture_frame_totals "${ENGINE}")"
  log "Columnar frames: encoded=${FRAME_ENCODED} across data nodes, decoded=${FRAME_DECODED} on the liaison."
fi

# Final parity check (sets FINAL_PASS).
FINAL_DIFF="/artifacts/diff-final.json"
soak_driver_container replay-and-diff \
  --engine "${ENGINE}" \
  --addr "${BANYANDB_GRPC_CONTAINER}" \
  --catalog "${ENGINE_CATALOG}" \
  --baseline /artifacts/baseline.json \
  --report "${FINAL_DIFF}" && FINAL_PASS=true || FINAL_PASS=false
log "Final parity check: pass=${FINAL_PASS}"

# Goroutine counts from first and final pprof captures.
extract_goroutine_total() {
  local dir="$1"
  local f
  # Standalone writes goroutine-*.txt directly under the pprof dir; the
  # distributed topology captures one sub-dir per role, so look one level down
  # too and prefer the liaison, which is the role the summary's start/end pair
  # has always described.
  f=$(ls "${dir}"/goroutine-*.txt 2>/dev/null | head -1)
  if [[ -z "${f}" ]]; then
    f=$(ls "${dir}"/liaison/goroutine-*.txt 2>/dev/null | head -1)
  fi
  if [[ -z "${f}" ]]; then
    f=$(ls "${dir}"/*/goroutine-*.txt 2>/dev/null | head -1)
  fi
  if [[ -z "${f}" ]]; then
    echo 0
    return
  fi
  awk '/^goroutine profile: total/ {print $4; exit}' "${f}" 2>/dev/null || echo 0
}
GOROUTINE_START=$(extract_goroutine_total "${DIST}/pprof-start")
GOROUTINE_END=$(extract_goroutine_total "${DIST}/pprof-end")
MEMORY_ALERTS=$(wc -l < "${DIST}/memory-alerts.log" 2>/dev/null || echo 0)

# Advisory: write-load liveness (rows advancing). Read the totals the background
# subshell persisted to disk — its variables never reach this parent shell.
WRITE_LOAD_ROWS=$(cat "${WRITE_LOAD_ROWS_FILE}" 2>/dev/null || echo 0)
WRITE_LOAD_LAST_OK=$(cat "${WRITE_LOAD_LAST_OK_FILE}" 2>/dev/null || echo never)
WRITE_LOAD_ALIVE=false
if (( WRITE_LOAD_ROWS > 0 )); then
  WRITE_LOAD_ALIVE=true
fi

cat > "${DIST}/summary.json" <<EOF
{
  "run_ts": "${RUN_TS}",
  "engine": "${SOAK_ENGINE}",
  "smoke": "${SMOKE:-false}",
  "warmup_min": ${WARMUP_MIN},
  "soak_hours": ${SOAK_HOURS},
  "t1_ms": ${T1_MS},
  "final_parity_pass": ${FINAL_PASS},
  "topology": "${SOAK_TOPOLOGY}",
  "frames_encoded": ${FRAME_ENCODED:-0},
  "frames_decoded": ${FRAME_DECODED:-0},
  "goroutine_count_start": ${GOROUTINE_START},
  "goroutine_count_end": ${GOROUTINE_END},
  "memory_alert_lines": ${MEMORY_ALERTS},
  "write_load_spans": ${WRITE_LOAD_ROWS},
  "write_load_alive": ${WRITE_LOAD_ALIVE},
  "write_load_last_ok": "${WRITE_LOAD_LAST_OK}",
  "heap_growth_max_pct_threshold": ${SOAK_HEAP_GROWTH_MAX_PCT},
  "banyandb_image_digest": "${BANYANDB_IMAGE_DIGEST}",
  "banyandb_cpu_nanocpu_limit": "${BANYANDB_CPU_LIMIT}",
  "banyandb_memory_bytes_limit": "${BANYANDB_MEM_LIMIT}",
  "soak_driver_image_digest": "${DRIVER_IMAGE_DIGEST}",
  "artefacts_dir": "${DIST}"
}
EOF

log "Summary written to ${DIST}/summary.json"
log "=== ${SOAK_ENGINE} soak complete. Artefacts: ${DIST} ==="

trap - EXIT INT TERM
compose_cmd down -v --remove-orphans
exit 0
fi

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  MEASURE ENGINE (default) — original harness, behavior unchanged        ║
# ╚══════════════════════════════════════════════════════════════════════════╝

# ── build soak-driver ────────────────────────────────────────────────────────
log "Building soak-driver..."
mkdir -p "${REPO_ROOT}/bin"
(cd "${REPO_ROOT}" && go build -o bin/soak-driver ./cmd/soak-driver)
log "soak-driver built at bin/soak-driver"

log "Config: WARMUP_MIN=${WARMUP_MIN} SOAK_HOURS=${SOAK_HOURS} PPROF_INTERVAL_MIN=${PPROF_INTERVAL_MIN} PARITY_INTERVAL_MIN=${PARITY_INTERVAL_MIN}"

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  PHASE 0 — Baseline                                                      ║
# ╚══════════════════════════════════════════════════════════════════════════╝
log "=== PHASE 0: Baseline ==="

SOAK_DATA_DIR="${DATA_DIR}" compose_cmd up -d
wait_banyandb_healthy

log "Waiting for OAP to become healthy (schema install + agent chain)..."
oap_attempts=0
until docker compose -f "${COMPOSE_FILE}" ps oap --format '{{.Status}}' 2>/dev/null | grep -q '(healthy)'; do
  oap_attempts=$(( oap_attempts + 1 ))
  if (( oap_attempts > 60 )); then
    log "ERROR: OAP did not become healthy after 5 min — abort"
    exit 1
  fi
  sleep 5
done
log "OAP healthy."

if (( WARMUP_SEC > 0 )); then
  log "Warming up for ${WARMUP_MIN} minutes to let OAP populate data..."
  sleep "${WARMUP_SEC}"
fi

log "Seeding deterministic fixture (${SEED_ROWS} rows into soak/soak_metric)..."
T1_MS=$(soak_driver seed-fixture --addr "${BANYANDB_GRPC}" --rows "${SEED_ROWS}" | tail -1)
if [[ -z "${T1_MS}" ]] || ! [[ "${T1_MS}" =~ ^[0-9]+$ ]]; then
  log "ERROR: seed-fixture did not return a valid T1 timestamp"
  exit 1
fi
log "T1 snapshot timestamp: ${T1_MS} ms"

# seed-fixture polls until the rows are visible to query, so by the
# time it returns the measure data is queryable. Schema-property has a
# 5s flush timeout — wait once more before snapshotting so the schema
# segs land on disk for Phase 1.
log "Waiting 8s for schema-server flush to persist..."
sleep 8

log "Recording baseline..."
soak_driver record-baseline \
  --addr "${BANYANDB_GRPC}" \
  --catalog "${CATALOG_MEASURE}" \
  --until "${T1_MS}" \
  --out "${DIST}/baseline.json"

# Verify the baseline has data points. The baseline JSON's data_points
# field is an array of protojson-encoded DataPoint messages; an empty
# slice means writes weren't visible and parity is meaningless.
baseline_dp=$(python3 -c "import json; d=json.load(open('${DIST}/baseline.json')); print(sum(len(r.get('data_points') or []) for r in d))" 2>/dev/null || echo 0)
log "Baseline data points captured: ${baseline_dp}"
if [[ "${baseline_dp}" == "0" ]]; then
  log "ERROR: baseline contains zero data points despite seed of ${SEED_ROWS} rows"
  exit 1
fi

log "Stopping BanyanDB to snapshot data..."
compose_cmd stop banyandb

log "Copying data to ${SNAPSHOT_DIR}..."
cp -a "${DATA_DIR}/." "${SNAPSHOT_DIR}/"

snap_size=$(du -sb "${SNAPSHOT_DIR}" 2>/dev/null | awk '{print $1}')
log "Snapshot size: ${snap_size:-0} bytes"

log "Tearing down Phase 0 stack..."
# Disable trap during intentional down so we don't double-down.
trap - EXIT
compose_cmd down -v --remove-orphans
trap cleanup INT TERM EXIT

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  PHASE 1 — Soak                                                          ║
# ╚══════════════════════════════════════════════════════════════════════════╝
log "=== PHASE 1: Soak (duration=${SOAK_HOURS}h) ==="

log "Restoring data snapshot..."
rm -rf "${DATA_DIR:?}"/*
cp -a "${SNAPSHOT_DIR}/." "${DATA_DIR}/"

SOAK_DATA_DIR="${DATA_DIR}" compose_cmd up -d
wait_banyandb_healthy

# Initial pprof grab.
mkdir -p "${DIST}/pprof-start"
soak_driver pprof-grab --addr "${BANYANDB_PPROF}" --out-dir "${DIST}/pprof-start"
log "Initial pprof captured."

# Tail BanyanDB logs into persistent log files in the background.
compose_cmd logs -f banyandb 2>&1 >> "${DIST}/banyand.log" &
LOGS_PID=$!

# Grep for MemoryTracker budget exhaustion in the background.
(
  tail -f "${DIST}/banyand.log" 2>/dev/null | \
    grep --line-buffered -i "MemoryTracker\|budget exhausted\|memory budget" \
    >> "${DIST}/memory-alerts.log" || true
) &
GREP_PID=$!

# Background pprof + parity loops.
SOAK_END=$(( $(date +%s) + SOAK_HOURS_SEC ))

(
  while (( $(date +%s) < SOAK_END )); do
    sleep "${PPROF_INTERVAL_SEC}"
    (( $(date +%s) >= SOAK_END )) && break
    INTERVAL_TS="$(date +%Y%m%dT%H%M%S)"
    PPROF_DIR="${DIST}/pprof-${INTERVAL_TS}"
    mkdir -p "${PPROF_DIR}"
    soak_driver pprof-grab --addr "${BANYANDB_PPROF}" --out-dir "${PPROF_DIR}" || \
      log "WARN: pprof-grab failed at ${INTERVAL_TS}"
    log "pprof captured: ${PPROF_DIR}"
  done
) &
PPROF_LOOP_PID=$!

(
  while (( $(date +%s) < SOAK_END )); do
    sleep "${PARITY_INTERVAL_SEC}"
    (( $(date +%s) >= SOAK_END )) && break
    DIFF_TS="$(date +%Y%m%dT%H%M%S)"
    DIFF_REPORT="${DIST}/diff-${DIFF_TS}.json"
    soak_driver replay-and-diff \
      --addr "${BANYANDB_GRPC}" \
      --catalog "${CATALOG_MEASURE}" \
      --baseline "${DIST}/baseline.json" \
      --report "${DIFF_REPORT}" || \
      log "WARN: parity divergence detected — see ${DIFF_REPORT}"
    log "parity check done: ${DIFF_REPORT}"
  done
) &
PARITY_LOOP_PID=$!

log "Soak running for ${SOAK_HOURS} hours. Loops started (pids: pprof=${PPROF_LOOP_PID} parity=${PARITY_LOOP_PID})."

# Wait for soak duration.
REMAINING=$(( SOAK_END - $(date +%s) ))
if (( REMAINING > 0 )); then
  sleep "${REMAINING}"
fi

log "Soak window complete. Collecting final artefacts..."

# Stop background loops gracefully.
kill "${PPROF_LOOP_PID}" "${PARITY_LOOP_PID}" 2>/dev/null || true
wait "${PPROF_LOOP_PID}" "${PARITY_LOOP_PID}" 2>/dev/null || true

# Final pprof.
mkdir -p "${DIST}/pprof-end"
soak_driver pprof-grab --addr "${BANYANDB_PPROF}" --out-dir "${DIST}/pprof-end"
log "Final pprof captured."

# Final parity check.
FINAL_DIFF="${DIST}/diff-final.json"
soak_driver replay-and-diff \
  --addr "${BANYANDB_GRPC}" \
  --catalog "${CATALOG_MEASURE}" \
  --baseline "${DIST}/baseline.json" \
  --report "${FINAL_DIFF}" && FINAL_PASS=true || FINAL_PASS=false
log "Final parity check: pass=${FINAL_PASS}"

# Stop log tailing.
kill "${LOGS_PID}" "${GREP_PID}" 2>/dev/null || true

# Write summary manifest.
# Goroutine count is read from the "goroutine profile: total N" header
# line that /debug/pprof/goroutine?debug=1 writes. Cheaper and more
# robust than counting per-goroutine entries.
extract_goroutine_total() {
  local dir="$1"
  local f
  # Standalone writes goroutine-*.txt directly under the pprof dir; the
  # distributed topology captures one sub-dir per role, so look one level down
  # too and prefer the liaison, which is the role the summary's start/end pair
  # has always described.
  f=$(ls "${dir}"/goroutine-*.txt 2>/dev/null | head -1)
  if [[ -z "${f}" ]]; then
    f=$(ls "${dir}"/liaison/goroutine-*.txt 2>/dev/null | head -1)
  fi
  if [[ -z "${f}" ]]; then
    f=$(ls "${dir}"/*/goroutine-*.txt 2>/dev/null | head -1)
  fi
  if [[ -z "${f}" ]]; then
    echo 0
    return
  fi
  awk '/^goroutine profile: total/ {print $4; exit}' "${f}" 2>/dev/null || echo 0
}
GOROUTINE_START=$(extract_goroutine_total "${DIST}/pprof-start")
GOROUTINE_END=$(extract_goroutine_total "${DIST}/pprof-end")
MEMORY_ALERTS=$(wc -l < "${DIST}/memory-alerts.log" 2>/dev/null || echo 0)

cat > "${DIST}/summary.json" <<EOF
{
  "run_ts": "${RUN_TS}",
  "smoke": "${SMOKE:-false}",
  "warmup_min": ${WARMUP_MIN},
  "soak_hours": ${SOAK_HOURS},
  "t1_ms": ${T1_MS},
  "final_parity_pass": ${FINAL_PASS},
  "goroutine_count_start": ${GOROUTINE_START},
  "goroutine_count_end": ${GOROUTINE_END},
  "memory_alert_lines": ${MEMORY_ALERTS},
  "artefacts_dir": "${DIST}"
}
EOF

log "Summary written to ${DIST}/summary.json"
log "=== Soak complete. Artefacts: ${DIST} ==="

# Intentional final teardown — disable trap first.
trap - EXIT INT TERM
compose_cmd down -v --remove-orphans
