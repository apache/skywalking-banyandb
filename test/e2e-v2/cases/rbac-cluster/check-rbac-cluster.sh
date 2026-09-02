#!/usr/bin/env bash
#
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

# The distributed direct-client stage of #14016, owning E-DST-01 and E-DST-02. Both liaisons
# read one mounted policy revision, so the same principal asking for the same group must get
# the same answer from either endpoint; and a marker written through one endpoint must be
# readable through the other, which is what proves public RBAC left the internal schema and
# data flows alone.

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
work_dir=$(mktemp -d)
trap 'rm -rf "${work_dir}"' EXIT

alpha=alpha
beta=beta
alpha_marker=alpha_marker
beta_marker=beta_marker

group_create=banyandb.database.v1.GroupRegistryService/Create
group_list=banyandb.database.v1.GroupRegistryService/List
measure_create=banyandb.database.v1.MeasureRegistryService/Create
await_applied=banyandb.schema.v1.SchemaBarrierService/AwaitSchemaApplied
measure_query=banyandb.measure.v1.MeasureService/Query
measure_write=banyandb.measure.v1.MeasureService/Write

now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
window_begin=$(date -u -d '-1 hour' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-1H +%Y-%m-%dT%H:%M:%SZ)
window_end=$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v+1H +%Y-%m-%dT%H:%M:%SZ)

fail() {
  echo "$*" >&2
  exit 1
}

grpc_call_at() {
  local address=$1
  local expected_code=$2
  local username=$3
  local password=$4
  local method=$5
  local payload=${6:-{\}}
  local output_file="${work_dir}/grpc-$(echo "${method}" | tr '/.' '--')-${expected_code}-$RANDOM.log"
  local -a auth_args=()
  local exit_code

  if [[ -n ${username} ]]; then
    auth_args=(-H "username: ${username}" -H "password: ${password}")
  fi
  set +e
  printf '%s\n' "${payload}" | timeout 60s grpcurl -plaintext -protoset "${descriptor_set}" \
    "${auth_args[@]}" -d @ "${address}" "${method}" >"${output_file}" 2>&1
  exit_code=$?
  set -e

  if [[ ${expected_code} == OK ]]; then
    [[ ${exit_code} -eq 0 ]] || { cat "${output_file}" >&2; fail "${address} ${method}: expected OK, grpcurl exited ${exit_code}"; }
    cat "${output_file}"
    return
  fi
  if [[ ${exit_code} -eq 0 ]] || ! grep -Fq "Code: ${expected_code}" "${output_file}"; then
    cat "${output_file}" >&2
    fail "${address} ${method}: expected ${expected_code}"
  fi
}

http_call_at() {
  local address=$1
  local expected_status=$2
  local username=$3
  local password=$4
  local method=$5
  local path=$6
  local body=${7:-}
  local response_file="${work_dir}/http-${expected_status}-$(echo "${path}" | tr '/' '-')-$RANDOM.body"
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
    --request "${method}" "${auth_args[@]}" "${body_args[@]}" "http://${address}${path}")
  [[ ${actual_status} == "${expected_status}" ]] || {
    cat "${response_file}" >&2
    fail "${address}${path}: expected HTTP ${expected_status}, got ${actual_status}"
  }
  cat "${response_file}"
}

assert_write_succeeded() {
  local response=$1
  local message_id=$2

  grep -Fq '"status": "STATUS_SUCCEED"' <<<"${response}" || {
    printf '%s\n' "${response}" >&2
    fail "write ${message_id}: response did not report STATUS_SUCCEED"
  }
  grep -Fq "\"messageId\": \"${message_id}\"" <<<"${response}" || fail "write ${message_id}: response omitted its message ID"
}

wait_for_measure_query() {
  local address=$1
  local payload=$2
  local output_file="${work_dir}/measure-ready.log"
  local attempt exit_code

  for attempt in $(seq 1 60); do
    set +e
    printf '%s\n' "${payload}" | timeout 60s grpcurl -plaintext -protoset "${descriptor_set}" \
      -H 'username: bydb-admin' -H 'password: admin-secret' -d @ "${address}" "${measure_query}" >"${output_file}" 2>&1
    exit_code=$?
    set -e
    [[ ${exit_code} -eq 0 ]] && return
    sleep 1
  done
  cat "${output_file}" >&2
  fail "the distributed measure fixture did not become query-ready"
}

wait_for_write_succeeded() {
  local address=$1
  local username=$2
  local password=$3
  local payload=$4
  local message_id=$5
  local attempt response

  for attempt in $(seq 1 60); do
    response=$(grpc_call_at "${address}" OK "${username}" "${password}" "${measure_write}" "${payload}")
    if grep -Fq '"status": "STATUS_SUCCEED"' <<<"${response}"; then
      assert_write_succeeded "${response}" "${message_id}"
      printf '%s\n' "${response}"
      return
    fi
    sleep 1
  done
  printf '%s\n' "${response}" >&2
  fail "write ${message_id}: the distributed data route did not become ready"
}

wait_for_marker() {
  local address=$1
  local payload=$2
  local marker=$3
  local attempt response

  for attempt in $(seq 1 60); do
    response=$(grpc_call_at "${address}" OK bydb-admin admin-secret "${measure_query}" "${payload}")
    if grep -Fq "${marker}" <<<"${response}"; then
      printf '%s\n' "${response}"
      return
    fi
    sleep 1
  done
  printf '%s\n' "${response}" >&2
  fail "${address}: marker ${marker} did not become readable"
}

group_body() {
  printf '{"group":{"metadata":{"name":"%s"},"catalog":"CATALOG_MEASURE","resourceOpts":{"shardNum":1,' "$1"
  printf '"segmentInterval":{"unit":"UNIT_DAY","num":1},"ttl":{"unit":"UNIT_DAY","num":7}}}}'
}

measure_body() {
  printf '{"measure":{"metadata":{"group":"%s","name":"%s"},"entity":{"tagNames":["id"]},' "$1" "$2"
  printf '"tagFamilies":[{"name":"default","tags":[{"name":"id","type":"TAG_TYPE_STRING"}]}],'
  printf '"fields":[{"name":"value","fieldType":"FIELD_TYPE_INT","encodingMethod":"ENCODING_METHOD_GORILLA",'
  printf '"compressionMethod":"COMPRESSION_METHOD_ZSTD"}]}}'
}

measure_query_body() {
  printf '{"groups":[%s],"name":"%s","timeRange":{"begin":"%s","end":"%s"},' "$1" "$2" "${window_begin}" "${window_end}"
  printf '"tagProjection":{"tagFamilies":[{"name":"default","tags":["id"]}]},"fieldProjection":{"names":["value"]}}'
}

write_frame() {
  printf '{"metadata":{"group":"%s","name":"%s"},"messageId":"%s",' "$1" "$2" "$3"
  printf '"dataPoint":{"timestamp":"%s","tagFamilies":[{"tags":[{"str":{"value":"%s"}}]}],' "${now}" "$4"
  printf '"fields":[{"int":{"value":"1"}}]}}'
}

# ---------------------------------------------------------------------------
# Bootstrap through endpoint A and wait for convergence through the protected barrier API,
# which the data node has to have applied before the read half can run.
# ---------------------------------------------------------------------------
for group in "${alpha}" "${beta}"; do
  grpc_call_at "${grpc_a}" OK bydb-admin admin-secret "${group_create}" "$(group_body "${group}")" >/dev/null
done
grpc_call_at "${grpc_a}" OK bydb-admin admin-secret "${measure_create}" "$(measure_body "${alpha}" "${alpha_marker}")" >/dev/null
grpc_call_at "${grpc_a}" OK bydb-admin admin-secret "${measure_create}" "$(measure_body "${beta}" "${beta_marker}")" >/dev/null

barrier_keys=$(printf '{"keys":[{"kind":"group","name":"%s"},{"kind":"group","name":"%s"},' "${alpha}" "${beta}")
barrier_keys+=$(printf '{"kind":"measure","group":"%s","name":"%s"},' "${alpha}" "${alpha_marker}")
barrier_keys+=$(printf '{"kind":"measure","group":"%s","name":"%s"}],' "${beta}" "${beta_marker}")
barrier_keys+='"minRevisions":["0","0","0","0"]}'
applied=$(grpc_call_at "${grpc_b}" OK bydb-admin admin-secret "${await_applied}" "${barrier_keys}")
grep -Fq '"applied": true' <<<"${applied}" || fail "the fixture schema did not converge across both liaisons"
wait_for_measure_query "${grpc_b}" "$(measure_query_body "\"${alpha}\"" "${alpha_marker}")"

# ---------------------------------------------------------------------------
# E-DST-02: write through A, read through B. Internal liaison-to-data flow must stay healthy
# while the public boundary is authorized.
# ---------------------------------------------------------------------------
wait_for_write_succeeded "${grpc_a}" bydb-writer writer-secret \
  "$(write_frame "${alpha}" "${alpha_marker}" 1 alpha-only)" 1 >/dev/null
wait_for_write_succeeded "${grpc_a}" bydb-admin admin-secret \
  "$(write_frame "${beta}" "${beta_marker}" 2 beta-only)" 2 >/dev/null

through_b=$(wait_for_marker "${grpc_b}" \
  "$(measure_query_body "\"${alpha}\"" "${alpha_marker}")" alpha-only)
grep -Fq 'alpha-only' <<<"${through_b}" || fail "the reader's query through endpoint B omitted the alpha marker"
grep -Fq 'beta-only' <<<"${through_b}" && fail "the reader's query through endpoint B leaked a beta marker"

# ---------------------------------------------------------------------------
# E-DST-01: the same principal asking for the same group gets the same answer from either
# endpoint, over direct gRPC and over the bound gateway route.
# ---------------------------------------------------------------------------
for endpoint in "${grpc_a}" "${grpc_b}"; do
  alpha_read=$(grpc_call_at "${endpoint}" OK bydb-reader reader-secret "${measure_query}" \
    "$(measure_query_body "\"${alpha}\"" "${alpha_marker}")")
  grep -Fq 'alpha-only' <<<"${alpha_read}" || fail "${endpoint}: the reader's query omitted the alpha marker"
  grep -Fq 'beta-only' <<<"${alpha_read}" && fail "${endpoint}: the reader's query leaked the beta marker"
  grpc_call_at "${endpoint}" PermissionDenied bydb-reader reader-secret "${measure_query}" \
    "$(measure_query_body "\"${beta}\"" "${beta_marker}")"
  grpc_call_at "${endpoint}" PermissionDenied bydb-reader reader-secret "${measure_query}" \
    "$(measure_query_body "\"${alpha}\",\"${beta}\"" "${alpha_marker}")"
  grpc_call_at "${endpoint}" PermissionDenied bydb-reader reader-secret "${measure_write}" \
    "$(write_frame "${alpha}" "${alpha_marker}" 3 alpha-only)"
  grpc_call_at "${endpoint}" PermissionDenied bydb-writer writer-secret "${measure_write}" \
    "$(write_frame "${beta}" "${beta_marker}" 4 leaked-by-writer)"
  grpc_call_at "${endpoint}" PermissionDenied bydb-auth-only auth-only-secret "${measure_query}" \
    "$(measure_query_body "\"${alpha}\"" "${alpha_marker}")"

  listed=$(grpc_call_at "${endpoint}" OK bydb-reader reader-secret "${group_list}" '{}')
  grep -Fq "\"${alpha}\"" <<<"${listed}" || fail "${endpoint}: the reader's Group.List omits ${alpha}"
  grep -Fq "\"${beta}\"" <<<"${listed}" && fail "${endpoint}: the reader's Group.List leaks ${beta}"
done

for endpoint in "${http_a}" "${http_b}"; do
  http_alpha_read=$(http_call_at "${endpoint}" 200 bydb-reader reader-secret POST /api/v1/measure/data \
    "$(measure_query_body "\"${alpha}\"" "${alpha_marker}")")
  grep -Fq 'alpha-only' <<<"${http_alpha_read}" || fail "${endpoint}: the reader's HTTP query omitted the alpha marker"
  grep -Fq 'beta-only' <<<"${http_alpha_read}" && fail "${endpoint}: the reader's HTTP query leaked the beta marker"
  http_call_at "${endpoint}" 403 bydb-reader reader-secret POST /api/v1/measure/data \
    "$(measure_query_body "\"${beta}\"" "${beta_marker}")" >/dev/null
  http_call_at "${endpoint}" 401 bydb-reader wrong-password POST /api/v1/measure/data \
    "$(measure_query_body "\"${alpha}\"" "${alpha_marker}")" >/dev/null
done

# A refused frame must have reached no storage on either endpoint's path.
beta_after=$(grpc_call_at "${grpc_b}" OK bydb-admin admin-secret "${measure_query}" \
  "$(measure_query_body "\"${beta}\"" "${beta_marker}")")
grep -Fq 'leaked-by-writer' <<<"${beta_after}" && fail "a refused write frame reached storage"

printf 'status: success\n'
