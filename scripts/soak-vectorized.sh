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
#   WARMUP_MIN          – minutes OAP has to write data before baseline snapshot (default 60)
#   SOAK_HOURS          – duration of the vec-on Phase 1 run in hours (default 48)
#   PPROF_INTERVAL_MIN  – minutes between each pprof capture (default 30)
#   PARITY_INTERVAL_MIN – minutes between each replay-and-diff run (default 5)
#   SMOKE               – set to 1 for a quick ~30-min smoke run (overrides durations)
#
# Artefacts are written under dist/soak/<timestamp>/ relative to the repo root.
#
# Usage:
#   ./scripts/soak-vectorized.sh
#   SMOKE=1 ./scripts/soak-vectorized.sh

set -euo pipefail

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

SOAK_HOURS_SEC=$(awk "BEGIN{printf \"%d\", ${SOAK_HOURS}*3600}")
WARMUP_SEC=$(( WARMUP_MIN * 60 ))
PPROF_INTERVAL_SEC=$(( PPROF_INTERVAL_MIN * 60 ))
PARITY_INTERVAL_SEC=$(( PARITY_INTERVAL_MIN * 60 ))

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/test/soak/docker-compose.soak.yaml"
CATALOG="${REPO_ROOT}/cmd/soak-driver/catalog/default.json"
BANYANDB_GRPC="localhost:17912"
BANYANDB_PPROF="localhost:6060"

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

# ── build soak-driver ────────────────────────────────────────────────────────
log "Building soak-driver..."
mkdir -p "${REPO_ROOT}/bin"
(cd "${REPO_ROOT}" && go build -o bin/soak-driver ./cmd/soak-driver)
log "soak-driver built at bin/soak-driver"

# ── prepare output dirs ───────────────────────────────────────────────────────
mkdir -p "${DIST}" "${SNAPSHOT_DIR}" "${DATA_DIR}"

# Tee everything from this point into the run log so silent failures leave
# evidence behind. Earlier output (build) is already in stdout.
exec > >(tee -a "${DIST}/run.log") 2>&1

log "Run artefacts will be written to: ${DIST}"
log "Config: WARMUP_MIN=${WARMUP_MIN} SOAK_HOURS=${SOAK_HOURS} PPROF_INTERVAL_MIN=${PPROF_INTERVAL_MIN} PARITY_INTERVAL_MIN=${PARITY_INTERVAL_MIN}"

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  PHASE 0 — Baseline (vec-off)                                          ║
# ╚══════════════════════════════════════════════════════════════════════════╝
log "=== PHASE 0: Baseline (BANYANDB_VEC_ENABLED=false) ==="

SOAK_DATA_DIR="${DATA_DIR}" BANYANDB_VEC_ENABLED=false compose_cmd up -d
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
  --catalog "${CATALOG}" \
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
# ║  PHASE 1 — Soak (vec-on)                                               ║
# ╚══════════════════════════════════════════════════════════════════════════╝
log "=== PHASE 1: Soak (BANYANDB_VEC_ENABLED=true, duration=${SOAK_HOURS}h) ==="

log "Restoring data snapshot..."
rm -rf "${DATA_DIR:?}"/*
cp -a "${SNAPSHOT_DIR}/." "${DATA_DIR}/"

SOAK_DATA_DIR="${DATA_DIR}" BANYANDB_VEC_ENABLED=true compose_cmd up -d
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
      --catalog "${CATALOG}" \
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
  --catalog "${CATALOG}" \
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
  f=$(ls "${dir}"/goroutine-*.txt 2>/dev/null | head -1)
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
