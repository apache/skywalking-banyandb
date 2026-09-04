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

# The schema stage of the direct standalone RBAC case, owning the schema-bootstrap half of
# E-DIR-01 and the Group.List/schema half of E-DIR-04 for issue #14015. It provisions the
# alpha/beta fixture as the administrator, waits for it through the protected SchemaBarrier
# API rather than by sleeping, and then drives the scoped CRUD/List/Exist matrix over both
# direct gRPC and the bound grpc-gateway routes against a deployed container.
#
# It runs before check-rbac.sh, which finishes by rewriting the mounted policy to a revoked
# and then a malformed revision.

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
gamma=gamma
alpha_marker=alpha_marker
beta_marker=beta_marker

group_get=banyandb.database.v1.GroupRegistryService/Get
group_exist=banyandb.database.v1.GroupRegistryService/Exist
group_create=banyandb.database.v1.GroupRegistryService/Create
group_update=banyandb.database.v1.GroupRegistryService/Update
group_list=banyandb.database.v1.GroupRegistryService/List
measure_create=banyandb.database.v1.MeasureRegistryService/Create
measure_get=banyandb.database.v1.MeasureRegistryService/Get
measure_exist=banyandb.database.v1.MeasureRegistryService/Exist
measure_list=banyandb.database.v1.MeasureRegistryService/List
measure_delete=banyandb.database.v1.MeasureRegistryService/Delete
await_applied=banyandb.schema.v1.SchemaBarrierService/AwaitSchemaApplied
await_revision=banyandb.schema.v1.SchemaBarrierService/AwaitRevisionApplied
measure_query=banyandb.measure.v1.MeasureService/Query

fail() {
  echo "$*" >&2
  exit 1
}

# grpc_call runs one RPC and asserts the status code. The payload is passed on stdin so a
# JSON body containing spaces or braces survives.
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

# ---------------------------------------------------------------------------
# Bootstrap: the administrator provisions both groups and one measure in each,
# then waits for them through the protected barrier API. No sleeps.
# ---------------------------------------------------------------------------
for group in "${alpha}" "${beta}"; do
  grpc_call OK bydb-admin admin-secret "${group_create}" "$(group_body "${group}")" >/dev/null
done
grpc_call OK bydb-admin admin-secret "${measure_create}" "$(measure_body "${alpha}" "${alpha_marker}")" >/dev/null
grpc_call OK bydb-admin admin-secret "${measure_create}" "$(measure_body "${beta}" "${beta_marker}")" >/dev/null

barrier_keys=$(printf '{"keys":[{"kind":"group","name":"%s"},{"kind":"group","name":"%s"},' "${alpha}" "${beta}")
barrier_keys+=$(printf '{"kind":"measure","group":"%s","name":"%s"},' "${alpha}" "${alpha_marker}")
barrier_keys+=$(printf '{"kind":"measure","group":"%s","name":"%s"}],' "${beta}" "${beta_marker}")
barrier_keys+='"minRevisions":["0","0","0","0"]}'
applied=$(grpc_call OK bydb-admin admin-secret "${await_applied}" "${barrier_keys}")
grep -Fq '"applied": true' <<<"${applied}" || fail "the fixture schema did not converge through AwaitSchemaApplied"

# ---------------------------------------------------------------------------
# Scoped reads: the exact reader sees alpha and is denied beta; the wildcard
# reader sees both; an unauthorized Exist is denied rather than answered false.
# ---------------------------------------------------------------------------
grpc_call OK bydb-reader reader-secret "${group_get}" "{\"group\":\"${alpha}\"}" >/dev/null
grpc_call PermissionDenied bydb-reader reader-secret "${group_get}" "{\"group\":\"${beta}\"}"
grpc_call OK bydb-reader-all reader-all-secret "${group_get}" "{\"group\":\"${beta}\"}" >/dev/null
grpc_call PermissionDenied bydb-auth-only auth-only-secret "${group_get}" "{\"group\":\"${alpha}\"}"

grpc_call OK bydb-reader reader-secret "${group_exist}" "{\"group\":\"${alpha}\"}" >/dev/null
grpc_call PermissionDenied bydb-reader reader-secret "${group_exist}" "{\"group\":\"${beta}\"}"

grpc_call OK bydb-reader reader-secret "${measure_list}" "{\"group\":\"${alpha}\"}" >/dev/null
grpc_call PermissionDenied bydb-reader reader-secret "${measure_list}" "{\"group\":\"${beta}\"}"
grpc_call OK bydb-reader reader-secret "${measure_get}" \
  "{\"metadata\":{\"group\":\"${alpha}\",\"name\":\"${alpha_marker}\"}}" >/dev/null
grpc_call PermissionDenied bydb-reader reader-secret "${measure_exist}" \
  "{\"metadata\":{\"group\":\"${beta}\",\"name\":\"${beta_marker}\"}}"

# A request carrying no resolvable group is malformed, not unauthorized.
grpc_call InvalidArgument bydb-admin admin-secret "${group_get}" '{}'
grpc_call InvalidArgument bydb-admin admin-secret "${measure_create}" '{}'

# ---------------------------------------------------------------------------
# Scoped writes: the exact writer mutates alpha only, and a denied mutation
# leaves the preloaded schema exactly as the administrator can still observe it.
# ---------------------------------------------------------------------------
grpc_call PermissionDenied bydb-reader reader-secret "${measure_create}" "$(measure_body "${alpha}" "reader_marker")"
grpc_call PermissionDenied bydb-writer writer-secret "${measure_create}" "$(measure_body "${beta}" "writer_marker")"
for absent in reader_marker writer_marker; do
  for group in "${alpha}" "${beta}"; do
    exists=$(grpc_call OK bydb-admin admin-secret "${measure_exist}" \
      "{\"metadata\":{\"group\":\"${group}\",\"name\":\"${absent}\"}}")
    grep -Fq '"hasMeasure": true' <<<"${exists}" && fail "a denied create left ${group}/${absent} behind"
  done
done
grpc_call OK bydb-writer writer-secret "${measure_create}" "$(measure_body "${alpha}" "writer_marker")" >/dev/null

# A dedicated resource proves the delete path so alpha_marker stays available to the read and
# transport assertions below.
grpc_call OK bydb-admin admin-secret "${measure_create}" "$(measure_body "${alpha}" "alpha_doomed")" >/dev/null
grpc_call PermissionDenied bydb-reader reader-secret "${measure_delete}" \
  "{\"metadata\":{\"group\":\"${alpha}\",\"name\":\"alpha_doomed\"}}"
still_there=$(grpc_call OK bydb-admin admin-secret "${measure_exist}" \
  "{\"metadata\":{\"group\":\"${alpha}\",\"name\":\"alpha_doomed\"}}")
grep -Fq '"hasMeasure": true' <<<"${still_there}" || fail "a denied delete removed ${alpha}/alpha_doomed"
deleted=$(grpc_call OK bydb-writer writer-secret "${measure_delete}" \
  "{\"metadata\":{\"group\":\"${alpha}\",\"name\":\"alpha_doomed\"}}")
grep -Fq '"deleted": true' <<<"${deleted}" || fail "the alpha writer could not delete its own measure"

# Group upserts are scoped by the group body name, so a writer bound to a group that does not
# exist yet may bootstrap it, and only it.
grpc_call PermissionDenied bydb-reader reader-secret "${group_create}" "$(group_body "${gamma}")"
grpc_call PermissionDenied bydb-writer writer-secret "${group_create}" "$(group_body "${gamma}")"
grpc_call OK bydb-writer-gamma writer-gamma-secret "${group_create}" "$(group_body "${gamma}")" >/dev/null
grpc_call OK bydb-writer-gamma writer-gamma-secret "${group_update}" "$(group_body "${gamma}")" >/dev/null
grpc_call PermissionDenied bydb-writer-gamma writer-gamma-secret "${group_update}" "$(group_body "${alpha}")"

# ---------------------------------------------------------------------------
# Group.List visibility, over gRPC and over the bound gateway route.
# ---------------------------------------------------------------------------
reader_list=$(grpc_call OK bydb-reader reader-secret "${group_list}" '{}')
grep -Fq "\"${alpha}\"" <<<"${reader_list}" || fail "the alpha reader's Group.List omits ${alpha}"
grep -Fq "\"${beta}\"" <<<"${reader_list}" && fail "the alpha reader's Group.List leaks ${beta}"
grep -Fq "\"${gamma}\"" <<<"${reader_list}" && fail "the alpha reader's Group.List leaks ${gamma}"

admin_list=$(grpc_call OK bydb-admin admin-secret "${group_list}" '{}')
for group in "${alpha}" "${beta}" "${gamma}"; do
  grep -Fq "\"${group}\"" <<<"${admin_list}" || fail "the wildcard administrator's Group.List omits ${group}"
done
grpc_call PermissionDenied bydb-auth-only auth-only-secret "${group_list}" '{}'

http_reader_list=$(http_call 200 bydb-reader reader-secret GET /api/v1/group/schema/lists '')
grep -Fq "\"${alpha}\"" <<<"${http_reader_list}" || fail "the alpha reader's HTTP Group.List omits ${alpha}"
grep -Fq "\"name\":\"${beta}\"" <<<"${http_reader_list}" && fail "the alpha reader's HTTP Group.List leaks ${beta}"
http_call 403 bydb-auth-only auth-only-secret GET /api/v1/group/schema/lists '' >/dev/null

# ---------------------------------------------------------------------------
# Transport parity: every bound gateway route reaches the same decision, and
# 401, 400 and 403 stay distinguishable.
# ---------------------------------------------------------------------------
http_call 200 bydb-reader reader-secret GET "/api/v1/group/schema/${alpha}" '' >/dev/null
http_call 403 bydb-reader reader-secret GET "/api/v1/group/schema/${beta}" '' >/dev/null
http_call 401 bydb-reader wrong-password GET "/api/v1/group/schema/${alpha}" '' >/dev/null
http_call 200 bydb-reader reader-secret GET "/api/v1/measure/schema/lists/${alpha}" '' >/dev/null
http_call 403 bydb-reader reader-secret GET "/api/v1/measure/schema/lists/${beta}" '' >/dev/null
http_call 200 bydb-reader reader-secret GET "/api/v1/measure/schema/${alpha}/${alpha_marker}" '' >/dev/null
http_call 403 bydb-reader reader-secret GET "/api/v1/measure/schema/${beta}/${beta_marker}" '' >/dev/null
http_call 403 bydb-writer writer-secret POST /api/v1/measure/schema "$(measure_body "${beta}" "http_marker")" >/dev/null
http_call 200 bydb-writer writer-secret POST /api/v1/measure/schema "$(measure_body "${alpha}" "http_marker")" >/dev/null
http_call 403 bydb-writer writer-secret DELETE "/api/v1/measure/schema/${beta}/${beta_marker}" '' >/dev/null
beta_intact=$(grpc_call OK bydb-admin admin-secret "${measure_exist}" \
  "{\"metadata\":{\"group\":\"${beta}\",\"name\":\"${beta_marker}\"}}")
grep -Fq '"hasMeasure": true' <<<"${beta_intact}" || fail "a denied HTTP delete removed ${beta}/${beta_marker}"

# ---------------------------------------------------------------------------
# SchemaBarrier scope: key waits need every resolved group; the cluster-wide
# revision wait needs a wildcard schema read.
# ---------------------------------------------------------------------------
alpha_key=$(printf '{"keys":[{"kind":"group","name":"%s"}],"minRevisions":["0"]}' "${alpha}")
both_keys=$(printf '{"keys":[{"kind":"group","name":"%s"},{"kind":"group","name":"%s"}],"minRevisions":["0","0"]}' \
  "${alpha}" "${beta}")
grpc_call OK bydb-reader reader-secret "${await_applied}" "${alpha_key}" >/dev/null
grpc_call PermissionDenied bydb-reader reader-secret "${await_applied}" "${both_keys}"
grpc_call OK bydb-reader-all reader-all-secret "${await_applied}" "${both_keys}" >/dev/null
grpc_call PermissionDenied bydb-reader reader-secret "${await_revision}" '{"minRevision":"0"}'
revision=$(grpc_call OK bydb-reader-all reader-all-secret "${await_revision}" '{"minRevision":"0"}')
grep -Fq '"applied": true' <<<"${revision}" || fail "revision zero must be a real allow case for a wildcard reader"

# ---------------------------------------------------------------------------
# A data read carrying no time range is malformed, not unauthorized: the schema stage owns no
# data assertion beyond proving that the data families are classified rather than fail-closed.
# The scoped data matrix itself belongs to check-rbac-data.sh.
# ---------------------------------------------------------------------------
grpc_call InvalidArgument bydb-admin admin-secret "${measure_query}" \
  "{\"groups\":[\"${alpha}\"],\"name\":\"${alpha_marker}\"}"

printf 'status: success\n'
