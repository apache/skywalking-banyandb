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

# check-native-logs.sh asserts that the FODC agents and the proxy store their
# own logs in BanyanDB (native self-stored logs, phases 3 and 4).
#
# The agent beside the data node writes to that node's cluster server; the
# agent beside the liaison and the proxy write through the liaison's public
# write API (--logging-native-route=public). The data node and the liaison keep
# the log schema with --logging-native-receiver-enabled alone and collect nothing
# of their own, so every stored row is an FODC process's. Each must be stored
# under its own pod name as node_id, and each must report its counters on its
# /metrics.
#
# The skywalking-infra-e2e verify retry loop drives the waiting: this script
# does ONE pass and exits. The single status line on STDOUT is matched against
# expected/native-logs.yml; all diagnostics go to STDERR.

set -euo pipefail

NAMESPACE="${NAMESPACE:-default}"
PROXY_DEPLOY="${PROXY_DEPLOY:-deployment/fodc-proxy}"
PROXY_CONTAINER="${PROXY_CONTAINER:-fodc-proxy}"
QUERY_URL="${QUERY_URL:-http://liaison:17913/api/v1/stream/data}"
STATUS_KEY="fodc_native_logs_status"

log() { echo "$@" >&2; }
kx() { kubectl -n "${NAMESPACE}" "$@"; }
emit() { echo "${STATUS_KEY}: $1"; }
fail() {
  log "FAIL: $*"
  emit "FAIL"
  exit 1
}

pod_of() { kx get pods -l "app=$1" -o jsonpath='{.items[0].metadata.name}'; }
data_pod="$(pod_of banyandb-data)"
liaison_pod="$(pod_of banyandb-liaison)"
proxy_pod="$(pod_of fodc-proxy)"
[ -n "${data_pod}" ] && [ -n "${liaison_pod}" ] && [ -n "${proxy_pod}" ] || fail "pods not found"

# counters prints a process's native-log counters that are not zero, to show
# on a failure why its rows are missing.
counters() {
  kx exec "$1" -c "$2" -- wget -qO- "$3" 2>/dev/null |
    awk '$1 ~ /^banyandb_logging_native_log_/ && $2 != 0 { printf "%s=%s ", $1, $2 }' || true
}
agent_counters() { counters "$1" fodc-agent http://127.0.0.1:9090/metrics; }
proxy_counters() { counters "${PROXY_DEPLOY}" "${PROXY_CONTAINER}" http://127.0.0.1:17913/metrics; }

# The proxy's container has wget and reaches the liaison by its Service name.
# The time range keeps each scan small: the last 30 minutes hold only FODC rows.
begin="$(date -u -d '-30 min' +%Y-%m-%dT%H:%M:%SZ)"
end="$(date -u -d '+10 min' +%Y-%m-%dT%H:%M:%SZ)"
limit=900

# query prints the "node_id:node_type" of each row, one per line. $1 is the
# criteria JSON, or empty. node_id is an entity tag, so a node_id criteria
# selects series before the limit cuts the rows.
query() {
  local criteria=$1 body response
  body="{\"name\":\"log\",\"groups\":[\"_monitoring_log\"],\"limit\":${limit},\"timeRange\":{\"begin\":\"${begin}\",\"end\":\"${end}\"},${criteria:+\"criteria\":${criteria},}\"projection\":{\"tagFamilies\":[{\"name\":\"searchable\",\"tags\":[\"node_id\",\"node_type\"]}]}}"
  response="$(kx exec "${PROXY_DEPLOY}" -c "${PROXY_CONTAINER}" -- \
    wget -qO- --header='Content-Type: application/json' --post-data="${body}" "${QUERY_URL}" 2>/dev/null || true)"
  echo "${response}" | jq -e '.elements' >/dev/null 2>&1 || { log "query failed: $(echo "${response}" | head -c 300)"; return 1; }
  echo "${response}" | jq -r '.elements[] | [.tagFamilies[].tags[]] | map({(.key): .value.str.value}) | add | "\(.node_id):\(.node_type)"'
}
node_eq() { echo "{\"condition\":{\"name\":\"node_id\",\"op\":\"BINARY_OP_EQ\",\"value\":{\"str\":{\"value\":\"$1\"}}}}"; }

# Each process has rows under its own pod name, and only of its own type.
# $1 pod, $2 node_type, $3 its counters.
stored_as() {
  local rows got
  rows="$(query "$(node_eq "$1")")" || fail "the query of $1 failed"
  got="$(printf '%s\n' "${rows}" | sed '/^$/d' | sort -u | paste -sd, -)"
  log "rows of $1: ${got:-none}"
  [ "${got}" = "$1:$2" ] || fail "rows of $1 are ${got:-none}, want $1:$2; its counters: $3"
}
stored_as "${data_pod}" fodc-agent "$(agent_counters deployment/banyandb-data)"
stored_as "${liaison_pod}" fodc-agent "$(agent_counters deployment/banyandb-liaison)"
stored_as "${proxy_pod}" fodc-proxy "$(proxy_counters)"

# The data node and the liaison store no rows of their own: they only keep the
# schema. Rows are matched by node_type, so a replaced FODC pod's rows still
# count as FODC rows. A cut sample suffices, as each process logs all the time.
rows="$(query "")" || fail "the query of all rows failed"
others="$(printf '%s\n' "${rows}" | sed '/^$/d' | sort -u | { grep -vE ':fodc-(agent|proxy)$' || true; } | paste -sd, -)"
[ -z "${others}" ] || fail "rows of other processes: ${others}"

written() {
  kx exec "$1" -c "$2" -- wget -qO- "$3" 2>/dev/null |
    awk '$1 == "banyandb_logging_native_log_written_total" { print ($2 > 0) ? "yes" : "no" }'
}
[ "$(written "deployment/banyandb-data" fodc-agent http://127.0.0.1:9090/metrics)" = "yes" ] || fail "the data node's agent reports nothing written"
[ "$(written "deployment/banyandb-liaison" fodc-agent http://127.0.0.1:9090/metrics)" = "yes" ] || fail "the liaison's agent reports nothing written"
[ "$(written "${PROXY_DEPLOY}" "${PROXY_CONTAINER}" http://127.0.0.1:17913/metrics)" = "yes" ] || fail "the proxy reports nothing written"

emit "PASS"
