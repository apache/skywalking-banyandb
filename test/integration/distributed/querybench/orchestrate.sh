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

CARDINALITIES="${DQB_CARDINALITIES:-1024,10000,100000,1000000,2000000}"
SCENARIOS="${DQB_SCENARIOS:-scan_all,top_with_filter}"

echo "[orchestrate] report_dir=${REPORT_DIR}"
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

for card in "${CARD_ARR[@]}"; do
  for mode in row vec; do
    for scenario in "${SCEN_ARR[@]}"; do
      echo "[orchestrate] === cardinality=${card} mode=${mode} scenario=${scenario} ==="
      DQB_MODE="${mode}" \
        DQB_SCENARIO="${scenario}" \
        DQB_CARDINALITY="${card}" \
        "${BINARY}" -test.run TestDistributedQueryBench -test.v
    done
  done
done

echo "[orchestrate] === merging shards ==="
DQB_MERGE=1 "${BINARY}" -test.run TestDistributedQueryBench -test.v

echo "[orchestrate] done"
