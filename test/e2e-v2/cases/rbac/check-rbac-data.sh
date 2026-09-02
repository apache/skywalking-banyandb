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

if [[ $# -ne 3 ]]; then
	echo "usage: $0 <grpc> <http> <metrics>" >&2
	exit 2
fi

grpc_addr=$1
http_addr=$2
metrics_addr=$3
descriptor_set=${RBAC_DESCRIPTOR_SET:-/tmp/rbac-api.bin}
work_dir=$(mktemp -d)
trap 'rm -rf "${work_dir}"' EXIT

alpha=alpha
beta=beta
alpha_stream=alpha-stream
beta_stream=beta-stream
alpha_trace=alpha-trace
beta_trace=beta-trace
alpha_marker=alpha_marker
beta_marker=beta_marker
alpha_prop=alpha-prop
beta_prop=beta-prop
prop_name=endpoint

group_create=banyandb.database.v1.GroupRegistryService/Create
property_schema_create=banyandb.database.v1.PropertyRegistryService/Create
measure_schema_create=banyandb.database.v1.MeasureRegistryService/Create
stream_schema_create=banyandb.database.v1.StreamRegistryService/Create
trace_schema_create=banyandb.database.v1.TraceRegistryService/Create
await_applied=banyandb.schema.v1.SchemaBarrierService/AwaitSchemaApplied
stream_query=banyandb.stream.v1.StreamService/Query
measure_query=banyandb.measure.v1.MeasureService/Query
measure_topn=banyandb.measure.v1.MeasureService/TopN
measure_write=banyandb.measure.v1.MeasureService/Write
stream_write=banyandb.stream.v1.StreamService/Write
trace_write=banyandb.trace.v1.TraceService/Write
trace_query=banyandb.trace.v1.TraceService/Query
property_query=banyandb.property.v1.PropertyService/Query
property_apply=banyandb.property.v1.PropertyService/Apply
property_delete=banyandb.property.v1.PropertyService/Delete
bydbql_query=banyandb.bydbql.v1.BydbQLService/Query
decision_metric=banyandb_rbac_decisions_total

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

scrape_metrics() {
  curl --fail --silent --show-error "http://${metrics_addr}/metrics" >"$1"
}

metric_value() {
  local input_file=$1
  local metric=$2
  shift 2
  local lines required_label
  lines=$(grep -E "^${metric}(\\{|[[:space:]])" "${input_file}" || true)
  for required_label in "$@"; do
    lines=$(printf '%s\n' "${lines}" | grep -F "${required_label}" || true)
  done
  printf '%s\n' "${lines}" | awk '{ total += $NF } END { print total + 0 }'
}

assert_metric_increased() {
  local before_file=$1
  local after_file=$2
  local metric=$3
  shift 3
  local before after
  before=$(metric_value "${before_file}" "${metric}" "$@")
  after=$(metric_value "${after_file}" "${metric}" "$@")
  awk -v before="${before}" -v after="${after}" 'BEGIN { exit !(after > before) }' || \
    fail "${metric} $*: expected an increase, before ${before}, after ${after}"
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
      granted|unauthenticated|permission_missing|executor_unavailable|health_exempt|invalid_request) ;;
      *) fail "unbounded RBAC reason label: ${reason}" ;;
    esac
    if grep -Eq '(username|password|principal|role|group)=' <<<"${line}"; then
      fail "RBAC metric exposes identity or policy cardinality: ${line}"
    fi
  done < <(grep '^banyandb_rbac_decisions_total{' "${input_file}" || true)
}

wait_for_measure_query() {
  local payload=$1
  local output_file="${work_dir}/measure-ready.log"
  local attempt exit_code
  for attempt in $(seq 1 60); do
    set +e
    printf '%s\n' "${payload}" | timeout 30s grpcurl -plaintext -protoset "${descriptor_set}" \
      -H 'username: bydb-admin' -H 'password: admin-secret' -d @ "${grpc_addr}" "${measure_query}" >"${output_file}" 2>&1
    exit_code=$?
    set -e
    [[ ${exit_code} -eq 0 ]] && return
    sleep 1
  done
  cat "${output_file}" >&2
  fail "the measure fixture did not become query-ready"
}

wait_for_grpc_marker() {
  local method=$1
  local payload=$2
  local marker=$3
  local output_file="${work_dir}/marker-$(echo "${method}" | tr '/.' '--').log"
  local attempt exit_code
  for attempt in $(seq 1 60); do
    set +e
    printf '%s\n' "${payload}" | timeout 30s grpcurl -plaintext -protoset "${descriptor_set}" \
      -H 'username: bydb-admin' -H 'password: admin-secret' -d @ "${grpc_addr}" "${method}" >"${output_file}" 2>&1
    exit_code=$?
    set -e
    if [[ ${exit_code} -eq 0 ]] && grep -Fq "${marker}" "${output_file}"; then
      cat "${output_file}"
      return
    fi
    sleep 1
  done
  cat "${output_file}" >&2
  fail "${method}: marker ${marker} did not become readable"
}

assert_write_succeeded() {
  local response=$1
  local identifier=$2
  grep -Fq '"status": "STATUS_SUCCEED"' <<<"${response}" || fail "write ${identifier}: response did not report STATUS_SUCCEED"
}

property_group_body() {
  printf '{"group":{"metadata":{"name":"%s"},"catalog":"CATALOG_PROPERTY","resourceOpts":{"shardNum":1}}}' "$1"
}

property_schema_body() {
  printf '{"property":{"metadata":{"group":"%s","name":"%s"},' "$1" "${prop_name}"
  printf '"tags":[{"name":"marker","type":"TAG_TYPE_STRING"}]}}'
}

measure_schema_body() {
  printf '{"measure":{"metadata":{"group":"%s","name":"%s"},"entity":{"tagNames":["id"]},' "$1" "$2"
  printf '"tagFamilies":[{"name":"default","tags":[{"name":"id","type":"TAG_TYPE_STRING"}]}],'
  printf '"fields":[{"name":"value","fieldType":"FIELD_TYPE_INT",'
  printf '"encodingMethod":"ENCODING_METHOD_GORILLA","compressionMethod":"COMPRESSION_METHOD_ZSTD"}]}}'
}

data_group_body() {
  printf '{"group":{"metadata":{"name":"%s"},"catalog":"%s","resourceOpts":{"shardNum":1,' "$1" "$2"
  printf '"segmentInterval":{"unit":"UNIT_DAY","num":1},"ttl":{"unit":"UNIT_DAY","num":7}}}}'
}

stream_schema_body() {
  printf '{"stream":{"metadata":{"group":"%s","name":"%s"},"entity":{"tagNames":["id"]},' "$1" "$2"
  printf '"tagFamilies":[{"name":"default","tags":[{"name":"id","type":"TAG_TYPE_STRING"}]}]}}'
}

trace_schema_body() {
  printf '{"trace":{"metadata":{"group":"%s","name":"%s"},"tags":[' "$1" "$2"
  printf '{"name":"trace_id","type":"TAG_TYPE_STRING"},{"name":"span_id","type":"TAG_TYPE_STRING"},'
  printf '{"name":"timestamp","type":"TAG_TYPE_TIMESTAMP"},{"name":"marker","type":"TAG_TYPE_STRING"}],'
  printf '"traceIdTagName":"trace_id","spanIdTagName":"span_id","timestampTagName":"timestamp"}}'
}

property_body() {
  printf '{"property":{"metadata":{"group":"%s","name":"%s"},"id":"1",' "$1" "${prop_name}"
  printf '"tags":[{"key":"marker","value":{"str":{"value":"%s"}}}]}}' "$2"
}

measure_query_body() {
  printf '{"groups":[%s],"name":"%s","timeRange":{"begin":"%s","end":"%s"},' "$1" "$2" "${window_begin}" "${window_end}"
  printf '"tagProjection":{"tagFamilies":[{"name":"default","tags":["id"]}]},"fieldProjection":{"names":["value"]}}'
}

stream_query_body() {
  printf '{"groups":[%s],"name":"%s","timeRange":{"begin":"%s","end":"%s"},' "$1" "$2" "${window_begin}" "${window_end}"
  printf '"projection":{"tagFamilies":[{"name":"default","tags":["id"]}]}}'
}

trace_query_body() {
  printf '{"groups":[%s],"name":"%s","timeRange":{"begin":"%s","end":"%s"},' "$1" "$2" "${window_begin}" "${window_end}"
  printf '"criteria":{"condition":{"name":"trace_id","op":"BINARY_OP_EQ",'
  printf '"value":{"str":{"value":"%s"}}}},"tagProjection":["trace_id","span_id","marker"]}' "$3"
}

topn_query_body() {
  printf '{"groups":[%s],"name":"%s","timeRange":{"begin":"%s","end":"%s"},' "$1" "$2" "${window_begin}" "${window_end}"
  printf '"topN":1,"fieldValueSort":"SORT_DESC"}'
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
  printf '"tags":[{"str":{"value":"trace-%s"}},{"str":{"value":"span-%s"}},' "$3" "$3"
  printf '{"timestamp":"%s"},{"str":{"value":"%s"}}],"span":"c3Bhbgo="}' "${now}" "$4"
}

# ---------------------------------------------------------------------------
# Bootstrap: the schema stage already provisioned the alpha/beta measure fixture. The remaining
# catalogs need their own groups and schemas so every allowed frame reaches a real handler and
# every denied frame can be checked against storage through its native query API.
# ---------------------------------------------------------------------------
for group in "${alpha_prop}" "${beta_prop}"; do
  grpc_call OK bydb-admin admin-secret "${group_create}" "$(property_group_body "${group}")" >/dev/null
  grpc_call OK bydb-admin admin-secret "${property_schema_create}" "$(property_schema_body "${group}")" >/dev/null
done
grpc_call OK bydb-admin admin-secret "${measure_schema_create}" "$(measure_schema_body "${beta}" "${alpha_marker}")" >/dev/null
grpc_call OK bydb-admin admin-secret "${group_create}" "$(data_group_body "${alpha_stream}" CATALOG_STREAM)" >/dev/null
grpc_call OK bydb-admin admin-secret "${group_create}" "$(data_group_body "${beta_stream}" CATALOG_STREAM)" >/dev/null
grpc_call OK bydb-admin admin-secret "${stream_schema_create}" "$(stream_schema_body "${alpha_stream}" "${alpha_marker}")" >/dev/null
grpc_call OK bydb-admin admin-secret "${stream_schema_create}" "$(stream_schema_body "${beta_stream}" "${beta_marker}")" >/dev/null
grpc_call OK bydb-admin admin-secret "${group_create}" "$(data_group_body "${alpha_trace}" CATALOG_TRACE)" >/dev/null
grpc_call OK bydb-admin admin-secret "${group_create}" "$(data_group_body "${beta_trace}" CATALOG_TRACE)" >/dev/null
grpc_call OK bydb-admin admin-secret "${trace_schema_create}" "$(trace_schema_body "${alpha_trace}" "${alpha_marker}")" >/dev/null
grpc_call OK bydb-admin admin-secret "${trace_schema_create}" "$(trace_schema_body "${beta_trace}" "${beta_marker}")" >/dev/null
barrier_keys=$(printf '{"keys":[{"kind":"group","name":"%s"},{"kind":"group","name":"%s"},' "${alpha_prop}" "${beta_prop}")
barrier_keys+=$(printf '{"kind":"measure","group":"%s","name":"%s"},' "${beta}" "${alpha_marker}")
barrier_keys+=$(printf '{"kind":"group","name":"%s"},{"kind":"stream","group":"%s","name":"%s"},' \
  "${alpha_stream}" "${alpha_stream}" "${alpha_marker}")
barrier_keys+=$(printf '{"kind":"group","name":"%s"},{"kind":"stream","group":"%s","name":"%s"},' \
  "${beta_stream}" "${beta_stream}" "${beta_marker}")
barrier_keys+=$(printf '{"kind":"group","name":"%s"},{"kind":"trace","group":"%s","name":"%s"},' \
  "${alpha_trace}" "${alpha_trace}" "${alpha_marker}")
barrier_keys+=$(printf '{"kind":"group","name":"%s"},{"kind":"trace","group":"%s","name":"%s"}],' \
  "${beta_trace}" "${beta_trace}" "${beta_marker}")
barrier_keys+='"minRevisions":["0","0","0","0","0","0","0","0","0","0","0"]}'
applied=$(grpc_call OK bydb-admin admin-secret "${await_applied}" "${barrier_keys}")
grep -Fq '"applied": true' <<<"${applied}" || fail "the property fixture did not converge through AwaitSchemaApplied"
wait_for_measure_query "$(measure_query_body "\"${alpha}\"" "${alpha_marker}")"
scrape_metrics "${work_dir}/before-data.prom"

# ---------------------------------------------------------------------------
# Seed group-unique markers through the protected write API, then read them back. The seeding
# itself is part of the proof: only a principal holding data:write in the group may do it.
# ---------------------------------------------------------------------------
alpha_seed=$(grpc_call OK bydb-admin admin-secret "${measure_write}" \
  "$(write_frame "${alpha}" "${alpha_marker}" 1 alpha-only)")
assert_write_succeeded "${alpha_seed}" 1
beta_seed=$(grpc_call OK bydb-admin admin-secret "${measure_write}" \
  "$(write_frame "${beta}" "${beta_marker}" 2 beta-only)")
assert_write_succeeded "${beta_seed}" 2

# ---------------------------------------------------------------------------
# Native reads are all-or-nothing: the exact reader sees its own group, is denied another, and
# is denied a request mixing the two rather than served the part of it it is allowed.
# ---------------------------------------------------------------------------
alpha_read=$(grpc_call OK bydb-reader reader-secret "${measure_query}" "$(measure_query_body "\"${alpha}\"" "${alpha_marker}")")
grep -Fq 'alpha-only' <<<"${alpha_read}" || fail "the alpha reader's query omitted its marker"
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
grep -Fq 'alpha-only' <<<"${http_alpha_read}" || fail "the alpha reader's HTTP query omitted its marker"
grep -Fq 'beta-only' <<<"${http_alpha_read}" && fail "the alpha reader's HTTP query leaked a beta marker"
http_call 403 bydb-reader reader-secret POST /api/v1/measure/data \
  "$(measure_query_body "\"${beta}\"" "${beta_marker}")" >/dev/null
http_call 401 bydb-reader wrong-password POST /api/v1/measure/data \
  "$(measure_query_body "\"${alpha}\"" "${alpha_marker}")" >/dev/null
grpc_call PermissionDenied bydb-reader reader-secret "${measure_topn}" \
  "$(topn_query_body "\"${beta}\"" "${beta_marker}")"
http_call 403 bydb-reader reader-secret POST /api/v1/measure/topn \
  "$(topn_query_body "\"${beta}\"" "${beta_marker}")" >/dev/null

# ---------------------------------------------------------------------------
# A forbidden frame ends each stream and reaches no storage. A successful call first proves the
# same principal and schema reach the real handler; the following two-frame call then proves the
# denial belongs to its forbidden frame rather than stream establishment.
# ---------------------------------------------------------------------------
measure_allowed=$(grpc_call OK bydb-writer writer-secret "${measure_write}" \
  "$(write_frame "${alpha}" "${alpha_marker}" 3 alpha-only)")
assert_write_succeeded "${measure_allowed}" 3
forbidden_frames="$(write_frame "${alpha}" "${alpha_marker}" 4 alpha-before-deny)
$(write_frame "${beta}" "${beta_marker}" 5 leaked-by-writer)"
grpc_call PermissionDenied bydb-writer writer-secret "${measure_write}" "${forbidden_frames}"
grpc_call PermissionDenied bydb-reader reader-secret "${measure_write}" \
  "$(write_frame "${alpha}" "${alpha_marker}" 6 alpha-only)"
stream_allowed=$(grpc_call OK bydb-writer writer-secret "${stream_write}" \
  "$(stream_frame "${alpha_stream}" "${alpha_marker}" 7 alpha-only alpha-only)")
assert_write_succeeded "${stream_allowed}" 7
stream_frames="$(stream_frame "${alpha_stream}" "${alpha_marker}" 8 alpha-before-deny alpha-before-deny)
$(stream_frame "${beta_stream}" "${beta_marker}" 9 leaked-by-writer leaked-by-writer)"
grpc_call PermissionDenied bydb-writer writer-secret "${stream_write}" "${stream_frames}"
trace_allowed=$(grpc_call OK bydb-writer writer-secret "${trace_write}" \
  "$(trace_frame "${alpha_trace}" "${alpha_marker}" 1 alpha-only)")
assert_write_succeeded "${trace_allowed}" 1
trace_frames="$(trace_frame "${alpha_trace}" "${alpha_marker}" 2 alpha-before-deny)
$(trace_frame "${beta_trace}" "${beta_marker}" 3 leaked-by-writer)"
grpc_call PermissionDenied bydb-writer writer-secret "${trace_write}" "${trace_frames}"

measure_after=$(wait_for_grpc_marker "${measure_query}" "$(measure_query_body "\"${alpha}\"" "${alpha_marker}")" alpha-before-deny)
grep -Fq 'leaked-by-writer' <<<"${measure_after}" && fail "a refused measure frame reached storage"
stream_after=$(grpc_call OK bydb-admin admin-secret "${stream_query}" \
  "$(stream_query_body "\"${alpha_stream}\"" "${alpha_marker}")")
grep -Fq 'leaked-by-writer' <<<"${stream_after}" && fail "a refused stream frame reached storage"
trace_after=$(grpc_call OK bydb-admin admin-secret "${trace_query}" \
  "$(trace_query_body "\"${alpha_trace}\"" "${alpha_marker}" trace-1)")
grep -Fq 'leaked-by-writer' <<<"${trace_after}" && fail "a refused trace frame reached storage"
beta_after=$(grpc_call OK bydb-admin admin-secret "${measure_query}" "$(measure_query_body "\"${beta}\"" "${beta_marker}")")
grep -Fq 'leaked-by-writer' <<<"${beta_after}" && fail "a refused measure frame reached storage"
beta_stream_after=$(grpc_call OK bydb-admin admin-secret "${stream_query}" \
  "$(stream_query_body "\"${beta_stream}\"" "${beta_marker}")")
grep -Fq 'leaked-by-writer' <<<"${beta_stream_after}" && fail "a refused stream frame reached storage"
beta_trace_after=$(grpc_call OK bydb-admin admin-secret "${trace_query}" \
  "$(trace_query_body "\"${beta_trace}\"" "${beta_marker}" trace-3)")
grep -Fq 'leaked-by-writer' <<<"${beta_trace_after}" && fail "a refused trace frame reached storage"

# The exact reader can query every native family in its own scope and is denied the other
# group's request before any handler can reveal whether that resource contains data.
grpc_call OK bydb-reader reader-secret "${stream_query}" \
  "$(stream_query_body "\"${alpha_stream}\"" "${alpha_marker}")" >/dev/null
grpc_call PermissionDenied bydb-reader reader-secret "${stream_query}" \
  "$(stream_query_body "\"${beta_stream}\"" "${beta_marker}")"
grpc_call PermissionDenied bydb-reader reader-secret "${stream_query}" \
  "$(stream_query_body "\"${alpha_stream}\",\"${beta_stream}\"" "${alpha_marker}")"
grpc_call OK bydb-reader reader-secret "${trace_query}" \
  "$(trace_query_body "\"${alpha_trace}\"" "${alpha_marker}" trace-1)" >/dev/null
grpc_call PermissionDenied bydb-reader reader-secret "${trace_query}" \
  "$(trace_query_body "\"${beta_trace}\"" "${beta_marker}" trace-3)"
http_call 200 bydb-reader reader-secret POST /api/v1/stream/data \
  "$(stream_query_body "\"${alpha_stream}\"" "${alpha_marker}")" >/dev/null
http_call 403 bydb-reader reader-secret POST /api/v1/stream/data \
  "$(stream_query_body "\"${beta_stream}\"" "${beta_marker}")" >/dev/null
http_call 200 bydb-reader reader-secret POST /api/v1/trace/data \
  "$(trace_query_body "\"${alpha_trace}\"" "${alpha_marker}" trace-1)" >/dev/null
http_call 403 bydb-reader reader-secret POST /api/v1/trace/data \
  "$(trace_query_body "\"${beta_trace}\"" "${beta_marker}" trace-3)" >/dev/null

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
reader_property=$(grpc_call OK bydb-reader reader-secret "${property_query}" \
  "{\"groups\":[\"${alpha_prop}\"],\"name\":\"${prop_name}\",\"ids\":[\"1\"]}")
grep -Fq 'alpha-only' <<<"${reader_property}" || fail "the alpha reader's Property query omitted its marker"
grpc_call PermissionDenied bydb-reader reader-secret "${property_query}" \
  "{\"groups\":[\"${beta_prop}\"],\"name\":\"${prop_name}\",\"ids\":[\"1\"]}"
grpc_call PermissionDenied bydb-reader reader-secret "${property_query}" \
  "{\"groups\":[\"${alpha_prop}\",\"${beta_prop}\"],\"name\":\"${prop_name}\",\"ids\":[\"1\"]}"
http_property=$(http_call 200 bydb-reader reader-secret POST /api/v1/property/data/query \
  "{\"groups\":[\"${alpha_prop}\"],\"name\":\"${prop_name}\",\"ids\":[\"1\"]}")
grep -Fq 'alpha-only' <<<"${http_property}" || fail "the alpha reader's HTTP Property query omitted its marker"
http_call 403 bydb-reader reader-secret POST /api/v1/property/data/query \
  "{\"groups\":[\"${beta_prop}\"],\"name\":\"${prop_name}\",\"ids\":[\"1\"]}" >/dev/null
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
allowed_query="SELECT * FROM MEASURE ${alpha_marker} IN ${alpha} TIME > '-1h'"
grpc_call OK bydb-reader reader-secret "${bydbql_query}" "{\"query\":\"${allowed_query}\"}" >/dev/null
for refused in \
  "SELECT * FROM MEASURE ${beta_marker} IN ${beta}" \
  "SELECT * FROM MEASURE ${alpha_marker} IN ${alpha}, ${beta}" \
  "select * from measure ${beta_marker} in ${beta}" \
  "SELECT * FROM STREAM ${beta_marker} IN ${beta_stream}" \
  "SELECT * FROM PROPERTY ${prop_name} IN ${beta_prop}" \
  "SELECT * FROM TRACE ${beta_marker} IN ${beta_trace} ORDER BY timestamp DESC" \
  "SHOW TOP 1 FROM MEASURE ${beta_marker} IN ${beta}"; do
  grpc_call PermissionDenied bydb-reader reader-secret "${bydbql_query}" "{\"query\":\"${refused}\"}"
done
grpc_call PermissionDenied bydb-reader reader-secret "${bydbql_query}" \
  "{\"query\":\"SELECT * FROM MEASURE ${beta_marker} IN ${beta} WHERE id = ?\",\"params\":[{\"str\":{\"value\":\"alpha-only\"}}]}"
grpc_call InvalidArgument bydb-reader reader-secret "${bydbql_query}" \
  "{\"query\":\"SELECT * FROM MEASURE ${beta_marker} IN ${beta} -- IN ${alpha}\"}"
grpc_call PermissionDenied bydb-auth-only auth-only-secret "${bydbql_query}" "{\"query\":\"${allowed_query}\"}"

http_call 200 bydb-reader reader-secret POST /api/v1/bydbql/query "{\"query\":\"${allowed_query}\"}" >/dev/null
http_call 403 bydb-reader reader-secret POST /api/v1/bydbql/query \
  "{\"query\":\"SELECT * FROM MEASURE ${beta_marker} IN ${beta}\"}" >/dev/null

# Every activated data path must emit bounded, secret-safe decisions. The baseline was taken
# after fixture creation, so each increase below comes only from the data workflow above.
scrape_metrics "${work_dir}/after-data.prom"
for method in "${stream_query}" "${measure_query}" "${trace_query}" "${property_query}"; do
  assert_metric_increased "${work_dir}/before-data.prom" "${work_dir}/after-data.prom" "${decision_metric}" \
    "method=\"${method}\"" 'permission="data:read"' 'decision="allow"' 'reason="granted"'
  assert_metric_increased "${work_dir}/before-data.prom" "${work_dir}/after-data.prom" "${decision_metric}" \
    "method=\"${method}\"" 'permission="data:read"' 'decision="deny"' 'reason="permission_missing"'
done
assert_metric_increased "${work_dir}/before-data.prom" "${work_dir}/after-data.prom" "${decision_metric}" \
  "method=\"${measure_topn}\"" 'permission="data:read"' 'decision="deny"' 'reason="permission_missing"'
for method in "${measure_write}" "${stream_write}" "${trace_write}"; do
  assert_metric_increased "${work_dir}/before-data.prom" "${work_dir}/after-data.prom" "${decision_metric}" \
    "method=\"${method}\"" 'permission="data:write"' 'decision="allow"' 'reason="granted"'
  assert_metric_increased "${work_dir}/before-data.prom" "${work_dir}/after-data.prom" "${decision_metric}" \
    "method=\"${method}\"" 'permission="data:write"' 'decision="deny"' 'reason="permission_missing"'
done
for method in "${property_apply}" "${property_delete}"; do
  assert_metric_increased "${work_dir}/before-data.prom" "${work_dir}/after-data.prom" "${decision_metric}" \
    "method=\"${method}\"" 'permission="data:write"' 'decision="allow"' 'reason="granted"'
  assert_metric_increased "${work_dir}/before-data.prom" "${work_dir}/after-data.prom" "${decision_metric}" \
    "method=\"${method}\"" 'permission="data:write"' 'decision="deny"' 'reason="permission_missing"'
done
assert_metric_increased "${work_dir}/before-data.prom" "${work_dir}/after-data.prom" "${decision_metric}" \
  "method=\"${bydbql_query}\"" 'permission="data:read"' 'decision="allow"' 'reason="granted"'
assert_metric_increased "${work_dir}/before-data.prom" "${work_dir}/after-data.prom" "${decision_metric}" \
  "method=\"${bydbql_query}\"" 'permission="data:read"' 'decision="deny"' 'reason="permission_missing"'
assert_bounded_decision_labels "${work_dir}/after-data.prom"

printf 'status: success\n'
