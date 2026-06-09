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
#   * DOCUMENTED_GAP(metrics/documented_gap.txt)-> reported only, never asserted
# It also asserts the two agents (liaison + data) registered with DISTINCT
# node_role labels and that operation="query" series appear on two different
# nodes (publisher = liaison, subscriber = data).
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
log "  operation=query           : pub@${PUB_ROLE}  sub@${SUB_ROLE}"
log "Documented gaps (reported, not asserted):"
for m in "${GAP[@]}"; do
  if series_present "${m}"; then log "  PRESENT (unexpected): ${m}"; else log "  absent (expected)  : ${m}"; fi
done
emit "PASS"
exit 0
