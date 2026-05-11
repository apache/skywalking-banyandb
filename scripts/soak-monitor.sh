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

# G5d soak monitor — polls the most recent run under dist/soak/ on a
# tapered cadence and writes a one-line status per tick. Each line is
# tagged either OK or ALERT; ALERT means at least one of:
#   - banyand.log has not been touched in >10 min (container hung)
#   - memory-alerts.log gained any line (acceptance criterion 3 violated)
#   - any diff-*.json shows "pass": false (acceptance criterion 2 violated)
#   - docker compose health probe reports degraded
#
# Cadence (matching the operator request):
#   ticks 1..8 — every 15 min (covers the first 2 h)
#   ticks 9..  — every 60 min (covers the remainder of the 48 h window)
#
# Exits automatically when the run's summary.json appears (soak
# complete) or on Ctrl-C. Returns non-zero if any ALERT line was
# emitted — so a wrapper can chain to a notification mechanism.
#
# Usage:
#   ./scripts/soak-monitor.sh                       # watch most recent run
#   ./scripts/soak-monitor.sh dist/soak/20260512T101010   # specific run
#
# Env overrides (rarely needed):
#   FIRST_PHASE_TICKS    number of 15-min ticks before slowing down (default 8)
#   FAST_INTERVAL_SEC    fast cadence in seconds (default 900)
#   SLOW_INTERVAL_SEC    slow cadence in seconds (default 3600)
#   LOG_STALE_SEC        ALERT threshold for banyand.log freshness (default 600)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/test/soak/docker-compose.soak.yaml"

FIRST_PHASE_TICKS="${FIRST_PHASE_TICKS:-8}"
FAST_INTERVAL_SEC="${FAST_INTERVAL_SEC:-900}"
SLOW_INTERVAL_SEC="${SLOW_INTERVAL_SEC:-3600}"
LOG_STALE_SEC="${LOG_STALE_SEC:-600}"

# Resolve the run directory.
if (( $# >= 1 )); then
  RUN="$1"
else
  RUN="$(ls -td "${REPO_ROOT}"/dist/soak/2026* 2>/dev/null | head -1 || true)"
fi
if [[ -z "${RUN:-}" || ! -d "${RUN}" ]]; then
  echo "[soak-monitor] ERROR: no soak run directory found (looked under dist/soak/2026*)"
  exit 1
fi

LOG="${RUN}/monitor.log"
echo "[soak-monitor] watching ${RUN}"
echo "[soak-monitor] writing status to ${LOG}"

# Tee from this point on so the status log persists alongside the run.
exec > >(tee -a "${LOG}") 2>&1

count_or_zero() {
  local n
  n="$(eval "$1" 2>/dev/null | wc -l | tr -d ' ')"
  echo "${n:-0}"
}

# Compose health: true if every service shows "healthy" (or has no
# healthcheck — only those without one report empty).
compose_health() {
  local status
  status="$(docker compose -f "${COMPOSE_FILE}" ps --format '{{.Name}} {{.State}} {{.Health}}' 2>/dev/null || true)"
  if [[ -z "${status}" ]]; then
    echo "down"
    return
  fi
  # If anything is "unhealthy" or "exited", flag.
  if echo "${status}" | grep -qE 'unhealthy|exited|restarting|dead'; then
    echo "degraded"
    return
  fi
  echo "healthy"
}

alert_count=0
tick=0
trap 'echo "[soak-monitor] stopped (ticks=${tick} alerts=${alert_count})"' EXIT

while true; do
  tick=$(( tick + 1 ))
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  # banyand.log freshness. During Phase 0 the file does not yet exist
  # — treat that as "fresh" so the alert doesn't fire prematurely.
  if [[ -f "${RUN}/banyand.log" ]]; then
    last_log_ts="$(stat -c %Y "${RUN}/banyand.log" 2>/dev/null || echo 0)"
    log_age=$(( $(date +%s) - last_log_ts ))
  else
    log_age=0
  fi

  # MemoryTracker exhaustion lines (file appears only in Phase 1).
  if [[ -f "${RUN}/memory-alerts.log" ]]; then
    mem_alerts="$(wc -l < "${RUN}/memory-alerts.log" 2>/dev/null | tr -d ' ')"
    mem_alerts="${mem_alerts:-0}"
  else
    mem_alerts=0
  fi

  # Parity divergence reports
  diff_fail="$(count_or_zero "grep -l '\"pass\": *false' ${RUN}/diff-*.json")"

  # pprof captures so far
  pprof_n="$(count_or_zero "ls -d ${RUN}/pprof-*")"

  # Summary present means soak finished
  summary_present="no"
  if [[ -f "${RUN}/summary.json" ]]; then summary_present="yes"; fi

  health="$(compose_health)"

  status="OK"
  reasons=""
  if (( mem_alerts > 0 )); then
    status="ALERT"
    reasons="${reasons} memory_alerts=${mem_alerts}"
  fi
  if (( diff_fail > 0 )); then
    status="ALERT"
    reasons="${reasons} diff_fail=${diff_fail}"
  fi
  if (( log_age > LOG_STALE_SEC )) && [[ "${summary_present}" == "no" ]]; then
    status="ALERT"
    reasons="${reasons} log_stale=${log_age}s"
  fi
  if [[ "${health}" != "healthy" && "${summary_present}" == "no" ]]; then
    status="ALERT"
    reasons="${reasons} health=${health}"
  fi
  if [[ "${status}" == "ALERT" ]]; then
    alert_count=$(( alert_count + 1 ))
  fi

  printf "[%s] %s tick=%d pprof=%s mem_alerts=%s diff_fail=%s log_age=%ds health=%s summary=%s%s\n" \
    "${ts}" "${status}" "${tick}" "${pprof_n}" "${mem_alerts}" "${diff_fail}" "${log_age}" \
    "${health}" "${summary_present}" "${reasons}"

  if [[ "${summary_present}" == "yes" ]]; then
    echo "[soak-monitor] soak complete (summary.json present) — exiting"
    if (( alert_count > 0 )); then
      exit 2
    fi
    exit 0
  fi

  if (( tick < FIRST_PHASE_TICKS )); then
    sleep "${FAST_INTERVAL_SEC}"
  else
    sleep "${SLOW_INTERVAL_SEC}"
  fi
done
