#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The non-blocking nightly continuation of the distributed direct-client case. It first
# provisions the normal public-path fixture, then exercises E-DST-03 by replacing the shared
# watched policy while both liaisons serve requests, and E-DST-04 by restarting one liaison
# while traffic keeps reaching the other. The reader's alpha grant is revoked in revision two,
# so a post-reload allow is a concrete fail-open rather than merely a transient response.

set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: $0 <grpc-a> <http-a> <grpc-b> <http-b>" >&2
  exit 2
fi

grpc_a=$1
http_a=$2
grpc_b=$3
http_b=$4
descriptor_set=${RBAC_DESCRIPTOR_SET:-/tmp/rbac-cluster-api.bin}
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
work_dir=$(mktemp -d)
stop_file="${work_dir}/stop"
traffic_failures="${work_dir}/traffic-failures.log"
traffic_pids=()
trap 'stop_traffic; rm -rf "${work_dir}"' EXIT

alpha=alpha
alpha_marker=alpha_marker
measure_query=banyandb.measure.v1.MeasureService/Query
policy_revision_metric=banyandb_rbac_policy_revision

fail() {
  echo "$*" >&2
  exit 1
}

stop_traffic() {
  : >"${stop_file}"
  local pid
  for pid in "${traffic_pids[@]:-}"; do
    wait "${pid}" || true
  done
  traffic_pids=()
}

grpc_code_at() {
  local address=$1
  local username=$2
  local password=$3
  local method=$4
  local payload=$5
  local output_file exit_code code
  output_file=$(mktemp "${work_dir}/grpc.XXXXXX")

  set +e
  printf '%s\n' "${payload}" | timeout 30s grpcurl -plaintext -protoset "${descriptor_set}" \
    -H "username: ${username}" -H "password: ${password}" -d @ "${address}" "${method}" >"${output_file}" 2>&1
  exit_code=$?
  set -e
  if [[ ${exit_code} -eq 0 ]]; then
    printf 'OK\n'
    return
  fi
  code=$(sed -n 's/.*Code: \([[:alnum:]_]*\).*/\1/p' "${output_file}" | tail -n 1)
  if [[ -n ${code} ]]; then
    printf '%s\n' "${code}"
    return
  fi
  printf 'transport-error\n'
}

query_body() {
  local now begin end
  now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  begin=$(date -u -d '-1 hour' +%Y-%m-%dT%H:%M:%SZ)
  end=$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)
  printf '{"groups":["%s"],"name":"%s","timeRange":{"begin":"%s","end":"%s"},' "${alpha}" "${alpha_marker}" "${begin}" "${end}"
  printf '"tagProjection":{"tagFamilies":[{"name":"default","tags":["id"]}]},"fieldProjection":{"names":["value"]}}'
}

reader_code() {
  grpc_code_at "$1" bydb-reader reader-secret "${measure_query}" "$(query_body)"
}

expect_reader_code() {
  local address=$1
  local expected=$2
  local code
  code=$(reader_code "${address}")
  [[ ${code} == "${expected}" ]] || fail "${address}: expected ${expected}, got ${code}"
}

liaison_container() {
  local service=$1
  local address=$2
  local port=${address##*:}
  local container mapped_ports
  while read -r container; do
    [[ -n ${container} ]] || continue
    mapped_ports=$(docker port "${container}" 17912/tcp 2>/dev/null || true)
    if grep -Eq ":${port}$" <<<"${mapped_ports}"; then
      printf '%s\n' "${container}"
      return
    fi
  done < <(docker ps --filter "label=com.docker.compose.service=${service}" --format '{{.ID}}')
  fail "cannot identify ${service} for ${address}"
}

policy_revision() {
  local address=$1
  curl --fail --silent --show-error "http://${address}/metrics" |
    awk -v metric="${policy_revision_metric}" '$1 == metric { print $NF; exit }'
}

wait_for_revision() {
  local address=$1
  local expected=$2
  local attempt current
  for attempt in $(seq 1 60); do
    current=$(policy_revision "${address}" || true)
    [[ ${current} == "${expected}" ]] && return
    sleep 1
  done
  fail "${address}: policy revision did not become ${expected}; last value ${current:-none}"
}

traffic_worker() {
  local address=$1
  local result_file=$2
  local code
  while [[ ! -e ${stop_file} ]]; do
    code=$(reader_code "${address}")
    printf '%s\n' "${code}" >>"${result_file}"
    case ${code} in
      OK|PermissionDenied) ;;
      *) printf '%s %s\n' "${address}" "${code}" >>"${traffic_failures}"; return ;;
    esac
  done
}

start_traffic() {
  local address=$1
  local result_file=$2
  rm -f "${stop_file}"
  traffic_worker "${address}" "${result_file}" &
  traffic_pids+=("$!")
}

assert_traffic() {
  local result_file=$1
  [[ -s ${result_file} ]] || fail "no concurrent request reached the liaison"
  [[ ! -s ${traffic_failures} ]] || { cat "${traffic_failures}" >&2; fail "traffic observed an unavailable or malformed authorization outcome"; }
}

wait_for_revoked_reader() {
  local attempt code
  for attempt in $(seq 1 60); do
    code=$(reader_code "${grpc_a}")
    case ${code} in
      PermissionDenied) return ;;
      OK) fail "restarted liaison admitted the reader before loading the revoked policy" ;;
      transport-error|Unavailable) sleep 1 ;;
      *) fail "restarted liaison returned ${code} instead of PermissionDenied" ;;
    esac
  done
  fail "restarted liaison did not become ready with the revoked policy"
}

# E-DST-01/02 establish the real distributed fixture before the two stress journeys begin.
bash "${script_dir}/check-rbac-cluster.sh" "${grpc_a}" "${http_a}" "${grpc_b}" "${http_b}" >/dev/null
expect_reader_code "${grpc_a}" OK
expect_reader_code "${grpc_b}" OK

liaison_a=$(liaison_container liaison-a "${grpc_a}")
liaison_b=$(liaison_container liaison-b "${grpc_b}")

# E-DST-03: both endpoints serve concurrent calls while one shared watched file is replaced.
start_traffic "${grpc_a}" "${work_dir}/reload-a.log"
start_traffic "${grpc_b}" "${work_dir}/reload-b.log"
sleep 1
docker exec "${liaison_a}" sh -ec 'cp /etc/banyandb/security.revoked.template.yaml /rbac-policy/security.yaml; chmod 0600 /rbac-policy/security.yaml'
wait_for_revision "${http_a}" 2
wait_for_revision "${http_b}" 2
stop_traffic
assert_traffic "${work_dir}/reload-a.log"
assert_traffic "${work_dir}/reload-b.log"
expect_reader_code "${grpc_a}" PermissionDenied
expect_reader_code "${grpc_b}" PermissionDenied

# E-DST-04: liaison B keeps serving the revoked decision while liaison A restarts. The first
# ready response from A must already be PermissionDenied, proving it loaded the shared policy
# before exposing its public gRPC method.
start_traffic "${grpc_b}" "${work_dir}/restart-b.log"
sleep 1
docker restart "${liaison_a}" >/dev/null
wait_for_revoked_reader
stop_traffic
assert_traffic "${work_dir}/restart-b.log"
expect_reader_code "${grpc_b}" PermissionDenied
docker exec "${liaison_a}" sh -ec 'test "$(stat -c %a /rbac-policy/security.yaml)" = 600'
docker exec "${liaison_b}" sh -ec 'test "$(stat -c %a /rbac-policy/security.yaml)" = 600'

printf 'status: success\n'
