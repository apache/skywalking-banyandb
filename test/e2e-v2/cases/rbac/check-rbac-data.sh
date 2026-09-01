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

# The data and special-path stage of the direct standalone RBAC case, owning the marker
# seeding half of E-DIR-01, the native-query half of E-DIR-04, E-DIR-05 and E-DIR-06 for issue
# #14016. It provisions the property fixture the measure fixture from the schema stage does not
# cover, then drives the native read matrix, the Property mutation matrix, all three streaming
# write protocols and the ByDBQL post-transform decision over both direct gRPC and the bound
# grpc-gateway routes against a deployed container.
#
# It runs after check-rbac-schema.sh, which provisions the alpha/beta measure fixture, and
# before check-rbac.sh, which finishes by rewriting the mounted policy to a revoked and then a
# malformed revision.

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <grpc> <http>" >&2
  exit 2
fi

grpc_addr=$1
http_addr=$2
descriptor_set=${RBAC_DESCRIPTOR_SET:-/tmp/rbac-api.bin}
work_dir=$(mktemp -d)
trap 'rm -rf "${work_dir}"' EXIT

alpha=alpha
beta=beta
alpha_marker=alpha_marker
beta_marker=beta_marker
alpha_prop=alpha-prop
beta_prop=beta-prop
prop_name=endpoint

group_create=banyandb.database.v1.GroupRegistryService/Create
property_schema_create=banyandb.database.v1.PropertyRegistryService/Create
await_applied=banyandb.schema.v1.SchemaBarrierService/AwaitSchemaApplied
measure_query=banyandb.measure.v1.MeasureService/Query
measure_write=banyandb.measure.v1.MeasureService/Write
stream_write=banyandb.stream.v1.StreamService/Write
trace_write=banyandb.trace.v1.TraceService/Write
property_query=banyandb.property.v1.PropertyService/Query
property_apply=banyandb.property.v1.PropertyService/Apply
property_delete=banyandb.property.v1.PropertyService/Delete
bydbql_query=banyandb.bydbql.v1.BydbQLService/Query

now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
window_begin=$(date -u -d '-1 hour' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-1H +%Y-%m-%dT%H:%M:%SZ)
window_end=$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v+1H +%Y-%m-%dT%H:%M:%SZ)

fail() {
  echo "$*" >&2
  exit 1
}

# grpc_call runs one RPC and asserts the status code. The payload is passed on stdin so a JSON
# body containing spaces or braces survives, and so a streaming method can be given more than
# one message by separating them with newlines.
grpc_call() {
  local expected_code=$1
  local username=$2
  local password=$3
  local method=$4
  local payload=${5:-{\}}
  local output_file="${work_dir}/grpc-$(echo "${method}" | tr '/.' '--')-${expected_code}-$RANDOM.log"
  local -a auth_args=()
  local exit_code

  if [[ -n ${username} ]]; then
    auth_args=(-H "username: ${username}" -H "password: ${password}")
  fi
  set +e
  printf '%s\n' "${payload}" | timeout 30s grpcurl -plaintext -protoset "${descriptor_set}" \
    "${auth_args[@]}" -d @ "${grpc_addr}" "${method}" >"${output_file}" 2>&1
  exit_code=$?
  set -e

  if [[ ${expected_code} == OK ]]; then
    [[ ${exit_code} -eq 0 ]] || { cat "${output_file}" >&2; fail "${method}: expected OK, grpcurl exited ${exit_code}"; }
    cat "${output_file}"
    return
  fi
  if [[ ${exit_code} -eq 0 ]] || ! grep -Fq "Code: ${expected_code}" "${output_file}"; then
    cat "${output_file}" >&2
    fail "${method}: expected ${expected_code}"
  fi
}

http_call() {
  local expected_status=$1
  local username=$2
  local password=$3
  local method=$4
  local path=$5
  local body=${6:-}
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
    --request "${method}" "${auth_args[@]}" "${body_args[@]}" "http://${http_addr}${path}")
  [[ ${actual_status} == "${expected_status}" ]] || {
    cat "${response_file}" >&2
    fail "${path}: expected HTTP ${expected_status}, got ${actual_status}"
  }
  cat "${response_file}"
}

property_group_body() {
  printf '{"group":{"metadata":{"name":"%s"},"catalog":"CATALOG_PROPERTY","resourceOpts":{"shardNum":1}}}' "$1"
}

property_schema_body() {
  printf '{"property":{"metadata":{"group":"%s","name":"%s"},' "$1" "${prop_name}"
  printf '"tags":[{"name":"marker","type":"TAG_TYPE_STRING"}]}}'
}

property_body() {
  printf '{"property":{"metadata":{"group":"%s","name":"%s"},"id":"1",' "$1" "${prop_name}"
  printf '"tags":[{"key":"marker","value":{"str":{"value":"%s"}}}]}}' "$2"
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

stream_frame() {
  printf '{"metadata":{"group":"%s","name":"%s"},"messageId":"%s",' "$1" "$2" "$3"
  printf '"element":{"elementId":"%s","timestamp":"%s",' "$4" "${now}"
  printf '"tagFamilies":[{"tags":[{"str":{"value":"%s"}}]}]}}' "$5"
}

trace_frame() {
  printf '{"metadata":{"group":"%s","name":"%s"},"version":"%s",' "$1" "$2" "$3"
  printf '"tags":[{"str":{"value":"%s"}}],"span":"c3Bhbgo="}' "$4"
}

# ---------------------------------------------------------------------------
# Bootstrap: the schema stage already provisioned the alpha/beta measure fixture. Properties
# live in groups of their own catalog, so the administrator provisions those here and waits for
# them through the protected barrier API rather than by sleeping.
# ---------------------------------------------------------------------------
for group in "${alpha_prop}" "${beta_prop}"; do
  grpc_call OK bydb-admin admin-secret "${group_create}" "$(property_group_body "${group}")" >/dev/null
  grpc_call OK bydb-admin admin-secret "${property_schema_create}" "$(property_schema_body "${group}")" >/dev/null
done
barrier_keys=$(printf '{"keys":[{"kind":"group","name":"%s"},{"kind":"group","name":"%s"}],' "${alpha_prop}" "${beta_prop}")
barrier_keys+='"minRevisions":["0","0"]}'
applied=$(grpc_call OK bydb-admin admin-secret "${await_applied}" "${barrier_keys}")
grep -Fq '"applied": true' <<<"${applied}" || fail "the property fixture did not converge through AwaitSchemaApplied"

# ---------------------------------------------------------------------------
# Seed group-unique markers through the protected write API, then read them back. The seeding
# itself is part of the proof: only a principal holding data:write in the group may do it.
# ---------------------------------------------------------------------------
grpc_call OK bydb-admin admin-secret "${measure_write}" \
  "$(write_frame "${alpha}" "${alpha_marker}" 1 alpha-only)" >/dev/null
grpc_call OK bydb-admin admin-secret "${measure_write}" \
  "$(write_frame "${beta}" "${beta_marker}" 2 beta-only)" >/dev/null

# ---------------------------------------------------------------------------
# Native reads are all-or-nothing: the exact reader sees its own group, is denied another, and
# is denied a request mixing the two rather than served the part of it it is allowed.
# ---------------------------------------------------------------------------
alpha_read=$(grpc_call OK bydb-reader reader-secret "${measure_query}" "$(measure_query_body "\"${alpha}\"" "${alpha_marker}")")
grep -Fq 'beta-only' <<<"${alpha_read}" && fail "the alpha reader's query leaked a beta marker"
grpc_call PermissionDenied bydb-reader reader-secret "${measure_query}" "$(measure_query_body "\"${beta}\"" "${beta_marker}")"
grpc_call PermissionDenied bydb-reader reader-secret "${measure_query}" \
  "$(measure_query_body "\"${alpha}\",\"${beta}\"" "${alpha_marker}")"
grpc_call OK bydb-reader-all reader-all-secret "${measure_query}" \
  "$(measure_query_body "\"${alpha}\",\"${beta}\"" "${alpha_marker}")" >/dev/null
grpc_call PermissionDenied bydb-auth-only auth-only-secret "${measure_query}" \
  "$(measure_query_body "\"${alpha}\"" "${alpha_marker}")"

http_alpha_read=$(http_call 200 bydb-reader reader-secret POST /api/v1/measure/data \
  "$(measure_query_body "\"${alpha}\"" "${alpha_marker}")")
grep -Fq 'beta-only' <<<"${http_alpha_read}" && fail "the alpha reader's HTTP query leaked a beta marker"
http_call 403 bydb-reader reader-secret POST /api/v1/measure/data \
  "$(measure_query_body "\"${beta}\"" "${beta_marker}")" >/dev/null
http_call 401 bydb-reader wrong-password POST /api/v1/measure/data \
  "$(measure_query_body "\"${alpha}\"" "${alpha_marker}")" >/dev/null

# ---------------------------------------------------------------------------
# A forbidden frame ends each stream and reaches no storage. The allowed frame in front of it
# proves the stream itself was open, so the denial is the frame's and not the stream's. The
# same allowed-then-forbidden sequence is sent through Measure, Stream and Trace writes.
# ---------------------------------------------------------------------------
grpc_call OK bydb-writer writer-secret "${measure_write}" \
  "$(write_frame "${alpha}" "${alpha_marker}" 3 alpha-only)" >/dev/null
forbidden_frames="$(write_frame "${alpha}" "${alpha_marker}" 4 alpha-only)
$(write_frame "${beta}" "${beta_marker}" 5 leaked-by-writer)"
grpc_call PermissionDenied bydb-writer writer-secret "${measure_write}" "${forbidden_frames}"
grpc_call PermissionDenied bydb-reader reader-secret "${measure_write}" \
  "$(write_frame "${alpha}" "${alpha_marker}" 6 alpha-only)"
stream_frames="$(stream_frame "${alpha}" "${alpha_marker}" 7 alpha-element alpha-only)
$(stream_frame "${beta}" "${beta_marker}" 8 beta-element leaked-by-writer)"
grpc_call PermissionDenied bydb-writer writer-secret "${stream_write}" "${stream_frames}"
trace_frames="$(trace_frame "${alpha}" "${alpha_marker}" 1 alpha-only)
$(trace_frame "${beta}" "${beta_marker}" 2 leaked-by-writer)"
grpc_call PermissionDenied bydb-writer writer-secret "${trace_write}" "${trace_frames}"
beta_after=$(grpc_call OK bydb-admin admin-secret "${measure_query}" "$(measure_query_body "\"${beta}\"" "${beta_marker}")")
grep -Fq 'leaked-by-writer' <<<"${beta_after}" && fail "a refused write frame reached storage"

# ---------------------------------------------------------------------------
# Property mutations are decided by the group of the record they name, before the handler runs,
# so a refused mutation is observably absent afterwards.
# ---------------------------------------------------------------------------
grpc_call PermissionDenied bydb-reader reader-secret "${property_apply}" "$(property_body "${alpha_prop}" alpha-only)"
grpc_call PermissionDenied bydb-writer writer-secret "${property_apply}" "$(property_body "${beta_prop}" leaked-by-writer)"
absent=$(grpc_call OK bydb-admin admin-secret "${property_query}" \
  "{\"groups\":[\"${alpha_prop}\",\"${beta_prop}\"],\"name\":\"${prop_name}\",\"ids\":[\"1\"]}")
grep -Fq 'leaked-by-writer' <<<"${absent}" && fail "a refused property apply reached storage"

grpc_call OK bydb-writer writer-secret "${property_apply}" "$(property_body "${alpha_prop}" alpha-only)" >/dev/null
grpc_call PermissionDenied bydb-reader reader-secret "${property_delete}" \
  "{\"group\":\"${alpha_prop}\",\"name\":\"${prop_name}\",\"id\":\"1\"}"
still_there=$(grpc_call OK bydb-admin admin-secret "${property_query}" \
  "{\"groups\":[\"${alpha_prop}\"],\"name\":\"${prop_name}\",\"ids\":[\"1\"]}")
grep -Fq 'alpha-only' <<<"${still_there}" || fail "a refused property delete removed the record"
deleted=$(grpc_call OK bydb-writer writer-secret "${property_delete}" \
  "{\"group\":\"${alpha_prop}\",\"name\":\"${prop_name}\",\"id\":\"1\"}")
grep -Fq '"deleted": true' <<<"${deleted}" || fail "the alpha writer could not delete its own property"

http_call 403 bydb-writer writer-secret PUT "/api/v1/property/data/${beta_prop}/${prop_name}/1" \
  "$(property_body "${beta_prop}" leaked-by-writer)" >/dev/null
http_call 403 bydb-reader reader-secret DELETE "/api/v1/property/data/${alpha_prop}/${prop_name}/1" '' >/dev/null

# ---------------------------------------------------------------------------
# ByDBQL is decided by the native request it transformed into, so neither its casing, nor a
# comment, nor the route that carried it can address a group the decision does not see.
# ---------------------------------------------------------------------------
allowed_query="SELECT * FROM MEASURE ${alpha_marker} IN ${alpha}"
grpc_call OK bydb-reader reader-secret "${bydbql_query}" "{\"query\":\"${allowed_query}\"}" >/dev/null
for refused in \
  "SELECT * FROM MEASURE ${beta_marker} IN ${beta}" \
  "SELECT * FROM MEASURE ${alpha_marker} IN ${alpha}, ${beta}" \
  "select * from measure ${beta_marker} in ${beta}"; do
  grpc_call PermissionDenied bydb-reader reader-secret "${bydbql_query}" "{\"query\":\"${refused}\"}"
done
grpc_call PermissionDenied bydb-auth-only auth-only-secret "${bydbql_query}" "{\"query\":\"${allowed_query}\"}"

http_call 200 bydb-reader reader-secret POST /api/v1/bydbql/query "{\"query\":\"${allowed_query}\"}" >/dev/null
http_call 403 bydb-reader reader-secret POST /api/v1/bydbql/query \
  "{\"query\":\"SELECT * FROM MEASURE ${beta_marker} IN ${beta}\"}" >/dev/null

printf 'status: success\n'
