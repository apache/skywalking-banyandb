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

# check-schema-consistency.sh asserts that a HEALTHY cluster reports no schema
# inconsistency. This is the steady-state guard for the schema consistency
# checker: on this cluster the registry, every node's cache and every node's
# runtime genuinely agree, so any INCONSISTENT verdict here is a false positive
# in the checker itself -- exactly the regression class that unit tests, which
# feed hand-built fingerprints, cannot catch.
#
# It also gives /cluster/lifecycle its first e2e coverage.
#
# The skywalking-infra-e2e verify retry loop drives the waiting: this script
# does ONE fetch and exits. The single status line on STDOUT is matched against
# expected/schema-consistency.yml; all diagnostics go to STDERR.
#
# UNKNOWN is tolerated: a group whose divergence has not yet survived the
# checker's consecutive-cycle suppression, or whose nodes have not all answered
# yet, is still converging and is not a failure.

set -euo pipefail

NAMESPACE="${NAMESPACE:-default}"
PROXY_DEPLOY="${PROXY_DEPLOY:-deployment/fodc-proxy}"
PROXY_CONTAINER="${PROXY_CONTAINER:-fodc-proxy}"
PROXY_LIFECYCLE_URL="${PROXY_LIFECYCLE_URL:-http://127.0.0.1:17913/cluster/lifecycle}"
STATUS_KEY="fodc_schema_consistency_status"

RESPONSE_FILE="$(mktemp)"
trap 'rm -f "${RESPONSE_FILE}"' EXIT

log() { echo "$@" >&2; }
kx() { kubectl -n "${NAMESPACE}" "$@"; }
emit() { echo "${STATUS_KEY}: $1"; }

fail() {
  log "FAIL: $*"
  log "---- /cluster/lifecycle response ----"
  cat "${RESPONSE_FILE}" >&2 || true
  emit "FAIL"
  exit 1
}

kx exec "${PROXY_DEPLOY}" -c "${PROXY_CONTAINER}" -- \
  wget -qO- "${PROXY_LIFECYCLE_URL}" > "${RESPONSE_FILE}" 2>/dev/null || true

[ -s "${RESPONSE_FILE}" ] || fail "empty response from ${PROXY_LIFECYCLE_URL}"

# Field names are proto names: the proxy marshals groups with
# protojson.MarshalOptions{UseProtoNames: true}, so the key is
# schema_consistency (snake_case), never schemaConsistency. Enum values are
# emitted as their full name strings. fodc/proxy/internal/api's
# TestLifecycleGroupsJSONContract pins both, so this script cannot silently rot.
count_status() {
  jq -r --arg s "$1" \
    '[.groups[]? | select(.schema_consistency.status == $s)] | length' "${RESPONSE_FILE}"
}

# A group whose verdict is absent means the check never ran for it -- a wiring
# failure, not a healthy cluster.
CHECKED="$(jq -r '[.groups[]? | select(.schema_consistency != null)] | length' "${RESPONSE_FILE}")"
[ "${CHECKED}" != "0" ] || fail "no group carried a schema consistency verdict; the check never ran"

CONSISTENT="$(count_status CONSISTENCY_STATUS_CONSISTENT)"
INCONSISTENT="$(count_status CONSISTENCY_STATUS_INCONSISTENT)"
UNKNOWN="$(count_status CONSISTENCY_STATUS_UNKNOWN)"
UNSPECIFIED="$(count_status CONSISTENCY_STATUS_UNSPECIFIED)"

# Every verdict must fall into one of the four known statuses. A shortfall means
# the status strings this script matches on no longer match what the proxy emits,
# which would otherwise make the INCONSISTENT check vacuously pass forever.
BUCKETED=$((CONSISTENT + INCONSISTENT + UNKNOWN + UNSPECIFIED))
if [ "${BUCKETED}" != "${CHECKED}" ]; then
  jq '[.groups[]? | select(.schema_consistency != null)
       | {name, status: .schema_consistency.status}]' "${RESPONSE_FILE}" >&2 || true
  fail "only ${BUCKETED}/${CHECKED} verdicts matched a known status string; the enum contract drifted"
fi

if [ "${INCONSISTENT}" != "0" ]; then
  log "${INCONSISTENT} group(s) reported schema inconsistency on a healthy cluster:"
  jq '.groups[]? | select(.schema_consistency.status == "CONSISTENCY_STATUS_INCONSISTENT")
      | {name, issues: .schema_consistency.issues}' "${RESPONSE_FILE}" >&2 || true
  fail "schema consistency check produced false positives"
fi

# Without this, a cluster where every group is stuck at UNKNOWN (nodes never
# answering, roster mismatch, suppression never clearing) would pass: it has
# verdicts and zero INCONSISTENT, yet the checker concluded nothing at all.
if [ "${CONSISTENT}" = "0" ]; then
  jq '[.groups[]? | select(.schema_consistency != null)
       | {name, status: .schema_consistency.status}]' "${RESPONSE_FILE}" >&2 || true
  fail "no group reached CONSISTENT (${UNKNOWN} UNKNOWN, ${UNSPECIFIED} UNSPECIFIED); the checker never concluded"
fi

log "Schema consistency verified."
log "  groups with a verdict : ${CHECKED}"
log "  CONSISTENT            : ${CONSISTENT}"
log "  INCONSISTENT          : ${INCONSISTENT}"
log "  UNKNOWN               : ${UNKNOWN}"
emit "PASS"
exit 0
