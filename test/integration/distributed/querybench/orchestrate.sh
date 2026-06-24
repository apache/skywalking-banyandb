#!/usr/bin/env bash
# Licensed to Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Apache Software Foundation (ASF) licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Inner orchestrator. Runs inside the resource-limited Docker container that
# run-docker.sh starts. Builds the test binary once, then invokes it as a
# fresh process for each (mode, scenario, cardinality) combo so heap and
# CPU profiles describe exactly that combo. After the matrix completes,
# invokes the binary one more time in merge mode to produce the unified
# report.

set -euo pipefail

cd /work

REPORT_DIR="${DQB_REPORT_DIR:-/work/.omx/bench-reports/distributed-query}"
SHARD_DIR="${REPORT_DIR}/shards"
PROFILE_DIR="${REPORT_DIR}/profiles"
BINARY="/tmp/dqb.test"

ENGINE="${DQB_ENGINE:-measure}"
MATRIX="${DQB_MATRIX:-A}"
CARDINALITIES="${DQB_CARDINALITIES:-1024,10000,100000,1000000,2000000}"
if [[ "${ENGINE}" == "trace" ]]; then
  CARDINALITIES="${DQB_CARDINALITIES:-1000,10000,100000,1000000,2000000}"
  SCENARIOS="${DQB_SCENARIOS:-trace_by_id,trace_tag_filter}"
else
  SCENARIOS="${DQB_SCENARIOS:-scan_all,top_with_filter}"
fi
MODES="${DQB_MODES:-row,vec}"

echo "[orchestrate] report_dir=${REPORT_DIR}"
echo "[orchestrate] engine=${ENGINE}"
echo "[orchestrate] matrix=${MATRIX}"
echo "[orchestrate] cardinalities=${CARDINALITIES}"
echo "[orchestrate] scenarios=${SCENARIOS}"

# Pin the write-time base across every (mode, scenario, cardinality) combo so
# row and vec see byte-identical DataPoint timestamps. Without this, each
# process picks its own time.Now() and the correctness gate hashes diverge
# even when the logical query results match.
export DQB_BASE_NANOS=$(date -u +%s%N)
echo "[orchestrate] base_nanos=${DQB_BASE_NANOS}"

mkdir -p "${SHARD_DIR}"
# Clear stale shards and profiles so the merged report reflects only this run.
rm -f "${SHARD_DIR}"/*.json
rm -rf "${PROFILE_DIR}"

echo "[orchestrate] building test binary..."
go test -c -o "${BINARY}" ./test/integration/distributed/querybench

IFS=',' read -ra CARD_ARR <<< "${CARDINALITIES}"
IFS=',' read -ra SCEN_ARR <<< "${SCENARIOS}"
IFS=',' read -ra MODE_ARR <<< "${MODES}"

run_variant() {
  local card=$1
  local spans_per_trace=$2
  local span_dist=$3
  local selectivity=$4
  local trace_id_batch=$5
  local shard_num=$6
  local data_nodes=$7
  local span_bytes=$8
  for mode in "${MODE_ARR[@]}"; do
    for scenario in "${SCEN_ARR[@]}"; do
      echo "[orchestrate] === engine=${ENGINE} cardinality=${card} mode=${mode} scenario=${scenario} s=${spans_per_trace} dist=${span_dist} sel=${selectivity} k=${trace_id_batch} shard=${shard_num} nodes=${data_nodes} bytes=${span_bytes} ==="
      DQB_ENGINE="${ENGINE}" \
        DQB_MATRIX="${MATRIX}" \
        DQB_MODE="${mode}" \
        DQB_SCENARIO="${scenario}" \
        DQB_CARDINALITY="${card}" \
        DQB_SPANS_PER_TRACE="${spans_per_trace}" \
        DQB_SPAN_DIST="${span_dist}" \
        DQB_FILTER_SELECTIVITY="${selectivity}" \
        DQB_TRACE_ID_BATCH="${trace_id_batch}" \
        DQB_SHARD_NUM="${shard_num}" \
        DQB_DATA_NODES="${data_nodes}" \
        DQB_SPAN_BYTES="${span_bytes}" \
        DQB_QUERY_MEMORY_MIB="${DQB_QUERY_MEMORY_MIB:-256}" \
        "${BINARY}" -test.run TestDistributedQueryBench -test.v
    done
  done
}

run_matrix_a() {
  for card in "${CARD_ARR[@]}"; do
    run_variant "${card}" "${DQB_SPANS_PER_TRACE:-20}" "${DQB_SPAN_DIST:-uniform}" "${DQB_FILTER_SELECTIVITY:-0.01}" "${DQB_TRACE_ID_BATCH:-1}" "${DQB_SHARD_NUM:-2}" "${DQB_DATA_NODES:-2}" "${DQB_SPAN_BYTES:-1024}"
  done
}

run_matrix_b() {
  local card="${DQB_MATRIX_B_CARDINALITY:-1000000}"
  run_variant "${card}" "5" "uniform" "0.01" "1" "2" "2" "${DQB_SPAN_BYTES:-1024}"
  run_variant "${card}" "20" "uniform" "0.01" "1" "2" "2" "${DQB_SPAN_BYTES:-1024}"
  run_variant "${card}" "100" "uniform" "0.01" "1" "2" "2" "${DQB_SPAN_BYTES:-1024}"
  run_variant "${card}" "20" "heavytail" "0.01" "1" "2" "2" "${DQB_SPAN_BYTES:-1024}"
  for selectivity in 0.001 0.01 0.1; do
    run_variant "${card}" "20" "uniform" "${selectivity}" "1" "2" "2" "${DQB_SPAN_BYTES:-1024}"
  done
  for trace_id_batch in 1 10 100; do
    run_variant "${card}" "20" "uniform" "0.01" "${trace_id_batch}" "2" "2" "${DQB_SPAN_BYTES:-1024}"
  done
  run_variant "${card}" "20" "uniform" "0.01" "1" "6" "2" "${DQB_SPAN_BYTES:-1024}"
  run_variant "${card}" "20" "uniform" "0.01" "1" "2" "4" "${DQB_SPAN_BYTES:-1024}"
}

if [[ "${ENGINE}" == "trace" ]]; then
  case "${MATRIX}" in
    A)
      run_matrix_a
      ;;
    B)
      run_matrix_b
      ;;
    both)
      run_matrix_a
      run_matrix_b
      ;;
    *)
      echo "unknown DQB_MATRIX=${MATRIX}" >&2
      exit 2
      ;;
  esac
else
  for card in "${CARD_ARR[@]}"; do
    run_variant "${card}" "${DQB_SPANS_PER_TRACE:-20}" "${DQB_SPAN_DIST:-uniform}" "${DQB_FILTER_SELECTIVITY:-0.01}" "${DQB_TRACE_ID_BATCH:-1}" "${DQB_SHARD_NUM:-2}" "${DQB_DATA_NODES:-2}" "${DQB_SPAN_BYTES:-1024}"
  done
fi

echo "[orchestrate] === merging shards ==="
DQB_ENGINE="${ENGINE}" DQB_MERGE=1 "${BINARY}" -test.run TestDistributedQueryBench -test.v

echo "[orchestrate] done"
