#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: $0 <grpc> <http> <metrics> <no-auth-grpc> <no-auth-http>" >&2
  exit 2
fi

grpc_addr=$1
http_addr=$2
metrics_addr=$3
noauth_grpc_addr=$4
noauth_http_addr=$5
descriptor_set=${RBAC_DESCRIPTOR_SET:-/tmp/rbac-api.bin}
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
config_dir=${RBAC_CONFIG_DIR:-${script_dir}}
work_dir=$(mktemp -d)
trap 'rm -rf "${work_dir}"' EXIT

fail() {
  echo "$*" >&2
  exit 1
}

expect_grpc_at() {
  local address=$1
  local expected_code=$2
  local username=$3
  local password=$4
  local method=$5
  local payload=${6:-{}}
  local output_file="${work_dir}/grpc-$(echo "${method}" | tr '/.' '--')-${expected_code}.log"
  local -a auth_args=()
  local exit_code
  if [[ -n ${username} ]]; then
    auth_args=(-H "username: ${username}" -H "password: ${password}")
  fi

  set +e
  printf '%s\n' "${payload}" | timeout 30s grpcurl -plaintext -protoset "${descriptor_set}" \
    "${auth_args[@]}" -d @ "${address}" "${method}" >"${output_file}" 2>&1
  exit_code=$?
  set -e

  if [[ ${expected_code} == OK ]]; then
    [[ ${exit_code} -eq 0 ]] || { cat "${output_file}" >&2; fail "${method}: expected OK, grpcurl exited ${exit_code}"; }
    return
  fi
  if [[ ${exit_code} -eq 0 ]] || ! grep -Fq "Code: ${expected_code}" "${output_file}"; then
    cat "${output_file}" >&2
    fail "${method}: expected ${expected_code}"
  fi
}

expect_grpc() {
  expect_grpc_at "${grpc_addr}" "$@"
}

expect_http_at() {
  local address=$1
  local expected_status=$2
  local username=$3
  local password=$4
  local method=$5
  local path=$6
  local body=${7:-}
  shift 7 || true
  local response_file="${work_dir}/http-${expected_status}-$(echo "${path}" | tr '/' '-').body"
  local -a auth_args=()
  local -a body_args=()
  local actual_status
  if [[ -n ${username} ]]; then
    auth_args=(--user "${username}:${password}")
  fi
  if [[ -n ${body} ]]; then
    body_args=(--header 'Content-Type: application/json' --data "${body}")
  fi
  actual_status=$(curl --silent --show-error --output "${response_file}" --write-out '%{http_code}' \
    --request "${method}" "${auth_args[@]}" "${body_args[@]}" "$@" "http://${address}${path}")
  [[ ${actual_status} == "${expected_status}" ]] || { cat "${response_file}" >&2; fail "${path}: expected HTTP ${expected_status}, got ${actual_status}"; }
}

expect_http() {
  expect_http_at "${http_addr}" "$@"
}

scrape_metrics() {
  curl --fail --silent --show-error "http://${metrics_addr}/metrics" >"$1"
}

metric_value() {
  local input_file=$1
  local metric=$2
  shift 2
  local lines
  lines=$(grep -E "^${metric}(\\{|[[:space:]])" "${input_file}" || true)
  local required_label
  for required_label in "$@"; do
    lines=$(printf '%s\n' "${lines}" | grep -F "${required_label}" || true)
  done
  printf '%s\n' "${lines}" | awk '{ total += $NF } END { print total + 0 }'
}

assert_metric_value() {
  local input_file=$1
  local expected=$2
  local metric=$3
  shift 3
  local actual
  actual=$(metric_value "${input_file}" "${metric}" "$@")
  awk -v actual="${actual}" -v expected="${expected}" 'BEGIN { exit !(actual == expected) }' || \
    fail "${metric} $*: expected ${expected}, got ${actual}"
}

assert_metric_delta() {
  local before_file=$1
  local after_file=$2
  local expected=$3
  local metric=$4
  shift 4
  local before after
  before=$(metric_value "${before_file}" "${metric}" "$@")
  after=$(metric_value "${after_file}" "${metric}" "$@")
  awk -v before="${before}" -v after="${after}" -v expected="${expected}" 'BEGIN { exit !((after - before) == expected) }' || \
    fail "${metric} $*: expected delta ${expected}, got $(awk -v b="${before}" -v a="${after}" 'BEGIN { print a-b }')"
}

wait_metric_value() {
  local expected=$1
  local metric=$2
  shift 2
  local attempt current
  for attempt in $(seq 1 30); do
    scrape_metrics "${work_dir}/wait.prom"
    current=$(metric_value "${work_dir}/wait.prom" "${metric}" "$@")
    [[ ${current} == "${expected}" ]] && return
    sleep 1
  done
  fail "${metric} $*: did not become ${expected}; last value ${current}"
}

main_container() {
  local published_port=${grpc_addr##*:}
  local container_id mapped_ports
  while read -r container_id; do
    [[ -n ${container_id} ]] || continue
    mapped_ports=$(docker port "${container_id}" 17912/tcp 2>/dev/null || true)
    if grep -Eq ":${published_port}$" <<<"${mapped_ports}"; then
      printf '%s\n' "${container_id}"
      return
    fi
  done < <(docker ps --filter label=com.docker.compose.service=banyandb --format '{{.ID}}')
  fail "cannot identify the RBAC BanyanDB container"
}

snapshot_fingerprint() {
  docker exec "$1" sh -c 'find /tmp -path "*/snapshots/*" -print | sort | cksum'
}

assert_bounded_decision_labels() {
  local input_file=$1
  local line decision permission reason
  while IFS= read -r line; do
    decision=$(sed -n 's/.*decision="\([^"]*\)".*/\1/p' <<<"${line}")
    permission=$(sed -n 's/.*permission="\([^"]*\)".*/\1/p' <<<"${line}")
    reason=$(sed -n 's/.*reason="\([^"]*\)".*/\1/p' <<<"${line}")
    [[ ${decision} == allow || ${decision} == deny ]] || fail "unbounded RBAC decision label: ${decision}"
    case ${permission} in
      authenticated|health|cluster:read|cluster:admin|schema:read|schema:write|data:read|data:write) ;;
      *) fail "unbounded RBAC permission label: ${permission}" ;;
    esac
    case ${reason} in
      granted|unauthenticated|permission_missing|executor_unavailable|health_exempt) ;;
      *) fail "unbounded RBAC reason label: ${reason}" ;;
    esac
    if grep -Eq '(username|password|principal|role|group)=' <<<"${line}"; then
      fail "RBAC metric exposes identity or policy cardinality: ${line}"
    fi
  done < <(grep '^banyandb_rbac_decisions_total{' "${input_file}" || true)
}

cluster_state=banyandb.database.v1.ClusterStateService/GetClusterState
node_query=banyandb.database.v1.NodeQueryService/GetCurrentNode
snapshot=banyandb.database.v1.SnapshotService/Snapshot
api_version=banyandb.common.v1.Service/GetAPIVersion
group_list=banyandb.database.v1.GroupRegistryService/List
internal_query=banyandb.measure.v1.MeasureService/InternalQuery
stream_write=banyandb.stream.v1.StreamService/Write
stream_delete=banyandb.stream.v1.StreamService/DeleteExpiredSegments
measure_delete=banyandb.measure.v1.MeasureService/DeleteExpiredSegments
trace_delete=banyandb.trace.v1.TraceService/DeleteExpiredSegments
decision_metric=banyandb_rbac_decisions_total
reload_metric=banyandb_rbac_policy_reload_total
revision_metric=banyandb_rbac_policy_revision

# A separate no-auth server proves legacy compatibility without weakening the RBAC server.
expect_grpc_at "${noauth_grpc_addr}" OK '' '' "${cluster_state}"
expect_http_at "${noauth_http_addr}" 200 '' '' GET /api/v1/cluster/state ''

before_metrics="${work_dir}/before.prom"
after_metrics="${work_dir}/after.prom"
scrape_metrics "${before_metrics}"
assert_metric_value "${before_metrics}" 1 "${reload_metric}" 'result="success"'
assert_metric_value "${before_metrics}" 0 "${reload_metric}" 'result="failure"'
assert_metric_value "${before_metrics}" 1 "${revision_metric}"

# Authentication, global cluster RPCs, unavailable schema/data executors, and all
# generated public fallbacks are exercised directly. RPCs without HTTP bindings stay gRPC-only.
expect_grpc Unauthenticated bydb-admin wrong-password "${api_version}"
expect_grpc OK bydb-auth-only auth-only-secret "${api_version}"
expect_grpc Unauthenticated bydb-admin wrong-password "${cluster_state}"
expect_grpc PermissionDenied bydb-auth-only auth-only-secret "${cluster_state}"
expect_grpc PermissionDenied bydb-reader reader-secret "${cluster_state}"
expect_grpc OK bydb-admin admin-secret "${cluster_state}"
expect_grpc PermissionDenied bydb-reader reader-secret "${node_query}"
expect_grpc OK bydb-admin admin-secret "${node_query}"

container_id=$(main_container)
before_denied_snapshot=$(snapshot_fingerprint "${container_id}")
expect_grpc PermissionDenied bydb-reader reader-secret "${snapshot}"
expect_http 403 bydb-reader reader-secret POST /api/v1/snapshot '{}'
after_denied_snapshot=$(snapshot_fingerprint "${container_id}")
[[ ${after_denied_snapshot} == "${before_denied_snapshot}" ]] || fail "denied Snapshot created a filesystem artifact"
expect_grpc OK bydb-admin admin-secret "${snapshot}"
after_grpc_snapshot=$(snapshot_fingerprint "${container_id}")
[[ ${after_grpc_snapshot} != "${after_denied_snapshot}" ]] || fail "allowed gRPC Snapshot created no filesystem artifact"
expect_http 200 bydb-admin admin-secret POST /api/v1/snapshot '{}'
after_http_snapshot=$(snapshot_fingerprint "${container_id}")
[[ ${after_http_snapshot} != "${after_grpc_snapshot}" ]] || fail "allowed HTTP Snapshot created no filesystem artifact"

expect_grpc PermissionDenied bydb-admin admin-secret "${group_list}"
expect_grpc PermissionDenied bydb-admin admin-secret "${stream_write}"
for fallback_method in "${internal_query}" "${stream_delete}" "${measure_delete}" "${trace_delete}"; do
  expect_grpc Unimplemented bydb-admin admin-secret "${fallback_method}"
  expect_grpc PermissionDenied bydb-reader reader-secret "${fallback_method}"
done

# The gateway repeats every available binding and must ignore forged forwarded identity.
expect_http 401 bydb-admin wrong-password GET /api/v1/cluster/state ''
expect_http 403 bydb-auth-only auth-only-secret GET /api/v1/cluster/state ''
expect_http 403 bydb-reader reader-secret GET /api/v1/cluster/state ''
expect_http 200 bydb-admin admin-secret GET /api/v1/cluster/state ''
expect_http 403 bydb-admin admin-secret GET /api/v1/group/schema/lists ''
expect_http 403 bydb-reader reader-secret GET /api/v1/cluster/state '' \
  --header 'Grpc-Metadata-Username: bydb-admin' --header 'Grpc-Metadata-Password: admin-secret'

scrape_metrics "${after_metrics}"
assert_metric_delta "${before_metrics}" "${after_metrics}" 1 "${decision_metric}" \
  'method="banyandb.common.v1.Service/GetAPIVersion"' 'decision="deny"' 'reason="unauthenticated"'
assert_metric_delta "${before_metrics}" "${after_metrics}" 1 "${decision_metric}" \
  'method="banyandb.common.v1.Service/GetAPIVersion"' 'decision="allow"' 'reason="granted"'
assert_metric_delta "${before_metrics}" "${after_metrics}" 1 "${decision_metric}" \
  'method="banyandb.database.v1.ClusterStateService/GetClusterState"' 'decision="deny"' 'reason="unauthenticated"'
assert_metric_delta "${before_metrics}" "${after_metrics}" 5 "${decision_metric}" \
  'method="banyandb.database.v1.ClusterStateService/GetClusterState"' 'decision="deny"' 'reason="permission_missing"'
assert_metric_delta "${before_metrics}" "${after_metrics}" 2 "${decision_metric}" \
  'method="banyandb.database.v1.ClusterStateService/GetClusterState"' 'decision="allow"' 'reason="granted"'
assert_metric_delta "${before_metrics}" "${after_metrics}" 1 "${decision_metric}" \
  'method="banyandb.database.v1.NodeQueryService/GetCurrentNode"' 'decision="deny"' 'reason="permission_missing"'
assert_metric_delta "${before_metrics}" "${after_metrics}" 1 "${decision_metric}" \
  'method="banyandb.database.v1.NodeQueryService/GetCurrentNode"' 'decision="allow"' 'reason="granted"'
assert_metric_delta "${before_metrics}" "${after_metrics}" 2 "${decision_metric}" \
  'method="banyandb.database.v1.SnapshotService/Snapshot"' 'decision="deny"' 'reason="permission_missing"'
assert_metric_delta "${before_metrics}" "${after_metrics}" 2 "${decision_metric}" \
  'method="banyandb.database.v1.SnapshotService/Snapshot"' 'decision="allow"' 'reason="granted"'
assert_metric_delta "${before_metrics}" "${after_metrics}" 2 "${decision_metric}" \
  'method="banyandb.database.v1.GroupRegistryService/List"' 'decision="deny"' 'reason="executor_unavailable"'
assert_metric_delta "${before_metrics}" "${after_metrics}" 1 "${decision_metric}" \
  'method="banyandb.stream.v1.StreamService/Write"' 'decision="deny"' 'reason="executor_unavailable"'
for fallback_method in "${internal_query}" "${stream_delete}" "${measure_delete}" "${trace_delete}"; do
  assert_metric_delta "${before_metrics}" "${after_metrics}" 1 "${decision_metric}" "method=\"${fallback_method}\"" 'decision="allow"' 'reason="granted"'
  assert_metric_delta "${before_metrics}" "${after_metrics}" 1 "${decision_metric}" "method=\"${fallback_method}\"" 'decision="deny"' 'reason="permission_missing"'
done
assert_bounded_decision_labels "${after_metrics}"

# One accepted and one rejected live reload prove atomic last-known-good behavior
# and exact bounded reload/revision metric changes without a server restart.
docker exec "${container_id}" sh -ec 'cp /etc/banyandb/security.revoked.yaml /tmp/security.yaml; chmod 0600 /tmp/security.yaml'
wait_metric_value 2 "${revision_metric}"
[[ $(main_container) == "${container_id}" ]] || fail "valid reload restarted BanyanDB"
expect_grpc PermissionDenied bydb-admin admin-secret "${cluster_state}"
docker exec "${container_id}" sh -ec 'cp /etc/banyandb/security.malformed.yaml /tmp/security.yaml; chmod 0600 /tmp/security.yaml'
wait_metric_value 1 "${reload_metric}" 'result="failure"'
[[ $(main_container) == "${container_id}" ]] || fail "malformed reload restarted BanyanDB"
expect_grpc PermissionDenied bydb-admin admin-secret "${cluster_state}"
scrape_metrics "${work_dir}/reload.prom"
assert_metric_value "${work_dir}/reload.prom" 2 "${reload_metric}" 'result="success"'
assert_metric_value "${work_dir}/reload.prom" 1 "${reload_metric}" 'result="failure"'
assert_metric_value "${work_dir}/reload.prom" 2 "${revision_metric}"
assert_metric_delta "${after_metrics}" "${work_dir}/reload.prom" 2 "${decision_metric}" \
  'method="banyandb.database.v1.ClusterStateService/GetClusterState"' 'decision="deny"' 'reason="permission_missing"'

# Invalid enabled policy startup is intentionally outside Compose readiness. It
# joins the case network only so standalone can resolve its own hostname during
# initialization; it publishes no ports and is not a Compose service.
image_name=$(docker inspect --format '{{.Config.Image}}' "${container_id}")
network_name=$(docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}' "${container_id}")
set +e
timeout --kill-after=5s 30s docker run --rm --network "${network_name}" --hostname invalid-rbac --entrypoint /bin/sh \
  --volume "${config_dir}/security-invalid.yaml:/etc/banyandb/security.invalid.template.yaml:ro" \
  "${image_name}" -ec \
  'cp /etc/banyandb/security.invalid.template.yaml /tmp/security-invalid.yaml; chmod 0600 /tmp/security-invalid.yaml;
  exec /banyand standalone --auth-config-file=/tmp/security-invalid.yaml' \
  >"${work_dir}/invalid-start.log" 2>&1
invalid_exit=$?
set -e
[[ ${invalid_exit} -ne 0 && ${invalid_exit} -ne 124 ]] || { cat "${work_dir}/invalid-start.log" >&2; fail "invalid enabled policy did not fail startup"; }
grep -Eq 'compile auth config|unknown permission' "${work_dir}/invalid-start.log" || {
  cat "${work_dir}/invalid-start.log" >&2
  fail "startup failed for an unrelated reason"
}

printf 'status: success\n'
