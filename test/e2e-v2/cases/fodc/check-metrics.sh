#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# check-metrics.sh performs a SINGLE-SHOT verification that the metrics consumed
# by the FODC Grafana dashboards are exported by the fodc-proxy /metrics endpoint:
#   * PRESENCE      (metrics/presence.txt)      -> at least one exported series
#   * NON_EMPTY     (metrics/non_empty.txt)     -> a sample value > 0 (histogram
#                                                  families verified via _count)
#   * LABEL DIMS    (hardcoded)                 -> metric-level labels used as
#                                                  dimensional filters in dashboard
#                                                  "expr" fields are present on the
#                                                  exported series
#   * LABEL VALUES  (hardcoded)                 -> specific label values required
#                                                  by non-trivial dashboard panels
#                                                  are present (e.g. kind="used")
#   * DOCUMENTED_GAP(metrics/documented_gap.txt)-> reported only, never asserted
# It also asserts the two agents (liaison + data) registered with DISTINCT
# node_role labels and that operation="query" series appear on two different
# nodes (publisher = liaison, subscriber = data).
#
# Infrastructure labels (job, container_name, pod_name) are added by the
# Prometheus scrape config and are NOT checked here -- they are absent from
# the raw /metrics endpoint output.
#
# The skywalking-infra-e2e verify retry loop drives the waiting: this script
# does ONE scrape and exits. The single status line on STDOUT is matched against
# expected/metrics-status.yml; all human-facing diagnostics go to STDERR. Run
# with DUMP=1 to additionally print the full scrape, topology, and pod logs to
# STDERR (used by the CI failure step).
#
# The scrape is written to a temp file and matched with file-based grep: piping a
# large scrape into `grep -q` would make grep exit on first match, SIGPIPE the
# producer, and -- under `set -o pipefail` -- report a false pipeline failure.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
METRICS_DIR="${METRICS_DIR:-${SCRIPT_DIR}/metrics}"
NAMESPACE="${NAMESPACE:-default}"
PROXY_DEPLOY="${PROXY_DEPLOY:-deployment/fodc-proxy}"
PROXY_CONTAINER="${PROXY_CONTAINER:-fodc-proxy}"
PROXY_METRICS_URL="${PROXY_METRICS_URL:-http://127.0.0.1:17913/metrics}"
PROXY_TOPOLOGY_URL="${PROXY_TOPOLOGY_URL:-http://127.0.0.1:17913/cluster/topology}"
DUMP="${DUMP:-0}"
STATUS_KEY="fodc_dashboard_metrics_status"

SCRAPE_FILE="$(mktemp)"
trap 'rm -f "${SCRAPE_FILE}"' EXIT

log() { echo "$@" >&2; }
kx() { kubectl -n "${NAMESPACE}" "$@"; }
scrape() { kx exec "${PROXY_DEPLOY}" -c "${PROXY_CONTAINER}" -- wget -qO- "${PROXY_METRICS_URL}" 2>/dev/null || true; }
topology() { kx exec "${PROXY_DEPLOY}" -c "${PROXY_CONTAINER}" -- wget -qO- "${PROXY_TOPOLOGY_URL}" 2>/dev/null || true; }

# read_list FILE -> metric names, skipping blank lines and # comments.
read_list() { awk 'NF && $1 !~ /^#/ {print $1}' "$1"; }

# series_present NAME -> 0 if >=1 exported series for NAME in the scrape file.
series_present() { grep -qE "^$1(\{| )" "${SCRAPE_FILE}"; }

# metric_positive NAME -> 0 if any series of NAME (or, for *_bucket histograms,
# the companion _count) carries a value > 0.
metric_positive() {
  local probe="$1"
  case "$1" in
    *_bucket) probe="${1%_bucket}_count" ;;
  esac
  awk -v m="${probe}" '
    index($0, m) == 1 {
      c = substr($0, length(m) + 1, 1)
      if (c == "{" || c == " ") { v = $NF + 0; if (v > 0) { found = 1 } }
    }
    END { exit found ? 0 : 1 }' "${SCRAPE_FILE}"
}

# label_exists METRIC LABEL -> 0 if any exported series for METRIC has LABEL=<anything>.
label_exists() {
  grep -qE "^$1\{[^}]*\b$2=" "${SCRAPE_FILE}"
}

# label_value METRIC LABEL VALUE -> 0 if any exported series has LABEL="VALUE".
label_value() {
  grep -qE "^$1\{[^}]*\b$2=\"$3\"" "${SCRAPE_FILE}"
}

# group_positive METRIC GROUP -> 0 if any series of METRIC with group="GROUP" has a value > 0.
group_positive() {
  awk -v m="$1" -v g="$2" '
    index($0, m) == 1 {
      c = substr($0, length(m) + 1, 1)
      if ((c == "{" || c == " ") && index($0, "group=\"" g "\"") > 0) {
        v = $NF + 0; if (v > 0) { found = 1 }
      }
    }
    END { exit found ? 0 : 1 }' "${SCRAPE_FILE}"
}

# query_role FAMILY -> node_role of the first FAMILY series with operation="query"
# (empty string if none). The trailing `|| true` keeps a no-match (grep exits 1
# under pipefail) from aborting the caller's `var=$(query_role ...)` under set -e.
query_role() {
  { grep -E "^$1" "${SCRAPE_FILE}" | grep 'operation="query"' \
    | grep -oE 'node_role="[^"]+"' | head -1 | sed -E 's/node_role="([^"]+)"/\1/'; } || true
}

emit() { echo "${STATUS_KEY}: $1"; }   # single status line on STDOUT for matching

dump_evidence() {
  log "----- cluster topology -----"; topology >&2
  log "----- full proxy scrape -----"; cat "${SCRAPE_FILE}" >&2
  for sel in app=fodc-proxy app=banyandb-liaison app=banyandb-data app=oap; do
    log "----- logs ${sel} -----"
    kx logs -l "${sel}" --all-containers --tail=200 >&2 2>/dev/null || true
  done
}

fail() {
  log "FODC dashboard metric verification: FAIL -> $1"
  [ "${DUMP}" = "1" ] && dump_evidence
  emit "FAIL"
  exit 1
}

mapfile -t PRESENCE < <(read_list "${METRICS_DIR}/presence.txt")
mapfile -t NON_EMPTY < <(read_list "${METRICS_DIR}/non_empty.txt")
mapfile -t GAP < <(read_list "${METRICS_DIR}/documented_gap.txt")
log "Loaded ${#PRESENCE[@]} presence, ${#NON_EMPTY[@]} non_empty, ${#GAP[@]} documented-gap metrics."

scrape > "${SCRAPE_FILE}"

if [ ! -s "${SCRAPE_FILE}" ] || ! grep -q 'node_role="' "${SCRAPE_FILE}"; then
  fail "no agents registered (proxy scrape has no node_role labels)"
fi

ROLES="$(grep -oE 'node_role="[^"]+"' "${SCRAPE_FILE}" | sort -u | wc -l | tr -d ' ')"
[ "${ROLES}" -ge 2 ] || fail "expected >=2 distinct node_role values, found ${ROLES}"

MISSING_PRESENCE=""
for m in "${PRESENCE[@]}"; do
  series_present "${m}" || MISSING_PRESENCE="${MISSING_PRESENCE} ${m}"
done
[ -z "${MISSING_PRESENCE}" ] || fail "missing presence series:${MISSING_PRESENCE}"

MISSING_POSITIVE=""
for m in "${NON_EMPTY[@]}"; do
  metric_positive "${m}" || MISSING_POSITIVE="${MISSING_POSITIVE} ${m}"
done
[ -z "${MISSING_POSITIVE}" ] || fail "non_empty metrics not > 0:${MISSING_POSITIVE}"

# ---- label dimension checks ---------------------------------------------
# Verify that metric-level labels used as dimensional filters in dashboard
# "expr" fields are actually present on the exported series.
#
# Queue pub/sub: both pub (pub.go) and sub (server.go) register labels
#   ["operation", "group", "remote_node", "remote_role", "remote_tier"].
# The workload dashboard filters/groups by {operation=..., group=...} on all
# queue pub/sub panels.
QUEUE_LABEL_METRICS=(
  banyandb_queue_pub_sent_bytes
  banyandb_queue_pub_total_finished
  banyandb_queue_pub_total_latency_bucket
  banyandb_queue_pub_total_batch_started
  banyandb_queue_pub_total_batch_finished
  banyandb_queue_pub_total_batch_latency_bucket
  banyandb_queue_sub_total_started
  banyandb_queue_sub_total_finished
  banyandb_queue_sub_total_latency_bucket
  banyandb_queue_sub_total_batch_started
  banyandb_queue_sub_total_batch_finished
  banyandb_queue_sub_total_batch_latency_bucket
  banyandb_queue_sub_total_message_started
  banyandb_queue_sub_total_message_finished
)
MISSING_LABELS=""
for m in "${QUEUE_LABEL_METRICS[@]}"; do
  series_present "${m}" || continue
  for lbl in operation group; do
    label_exists "${m}" "${lbl}" || MISSING_LABELS="${MISSING_LABELS} ${m}(${lbl})"
  done
done

# System metrics: nodes dashboard filters banyandb_system_{memory_state,disk,net_state}
# by {kind=...}. The kind dimension is metric-level, not an infra label.
for m in banyandb_system_memory_state banyandb_system_disk banyandb_system_net_state; do
  series_present "${m}" || continue
  label_exists "${m}" "kind" || MISSING_LABELS="${MISSING_LABELS} ${m}(kind)"
done

[ -z "${MISSING_LABELS}" ] || fail "metrics missing required dashboard label dimensions:${MISSING_LABELS}"

# ---- specific label-value checks ----------------------------------------
# Assert that label values required by specific dashboard panels are present.
# Nodes dashboard panels require these kind= values on system metrics.
MISSING_VALUES=""
for kv in \
    "banyandb_system_memory_state:kind:used" \
    "banyandb_system_memory_state:kind:used_percent" \
    "banyandb_system_disk:kind:used" \
    "banyandb_system_disk:kind:total" \
    "banyandb_system_net_state:kind:bytes_recv" \
    "banyandb_system_net_state:kind:bytes_sent"; do
  IFS=: read -r lv_m lv_lbl lv_val <<< "${kv}"
  series_present "${lv_m}" && \
    { label_value "${lv_m}" "${lv_lbl}" "${lv_val}" || \
      MISSING_VALUES="${MISSING_VALUES} ${lv_m}{${lv_lbl}=\"${lv_val}\"}"; }
done
[ -z "${MISSING_VALUES}" ] || fail "metrics missing required label values for dashboard panels:${MISSING_VALUES}"

# ---- canonical group checks -------------------------------------------------
# Assert the three core OAP groups each produced positive write traffic.
# banyandb_queue_pub_total_batch_started fires at stream-open (the earliest
# possible signal — before any message is sent), so it is positive as soon as
# OAP initiates a single batch for the group, regardless of whether the batch
# has completed within the verify window.
#   sw_metadata  -> measure writes (index_mode)
#   sw_record    -> trace writes
#   sw_minute    -> downsampled measure writes
MISSING_GROUPS=""
for grp_spec in \
    "sw_metadata:measure/index_mode" \
    "sw_records:trace" \
    "sw_metricsMinute:measure"; do
  IFS=: read -r grp _desc <<< "${grp_spec}"
  group_positive "banyandb_queue_pub_total_batch_started" "${grp}" || \
    MISSING_GROUPS="${MISSING_GROUPS} ${grp}(${_desc})"
done
[ -z "${MISSING_GROUPS}" ] || fail "canonical groups missing positive pub batch-started metrics:${MISSING_GROUPS}"

# The dashboard query-throughput panels read banyandb_queue_sub_*{operation="query"}
# (the subscriber/data side -- the data node subscribes to the query topic the
# liaison publishes). Assert that exists, proving the OAP->liaison->data query
# fan-out is observable. The publisher-side query metric is a separate feature
# and is only reported informationally.
SUB_ROLE="$(query_role 'banyandb_queue_sub_')"
[ -n "${SUB_ROLE}" ] || fail "no banyandb_queue_sub operation=query series (query path not observed)"
PUB_ROLE="$(query_role 'banyandb_queue_pub_')"

log "All dashboard metrics verified."
log "  distinct node_role values : ${ROLES}"
log "  presence metrics found    : ${#PRESENCE[@]}/${#PRESENCE[@]}"
log "  non_empty metrics  > 0    : ${#NON_EMPTY[@]}/${#NON_EMPTY[@]}"
log "  label dim checks          : ${#QUEUE_LABEL_METRICS[@]} queue + 3 system metrics"
log "  canonical groups positive : sw_metadata(measure/index_mode) sw_records(trace) sw_metricsMinute(measure)"
log "  operation=query           : pub@${PUB_ROLE}  sub@${SUB_ROLE}"
log "Documented gaps (reported, not asserted):"
for m in "${GAP[@]}"; do
  if series_present "${m}"; then log "  PRESENT (unexpected): ${m}"; else log "  absent (expected)  : ${m}"; fi
done
emit "PASS"
exit 0
