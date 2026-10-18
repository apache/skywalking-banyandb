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

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../../../.." && pwd)
cd "${REPO_ROOT}"

source_fingerprint() {
  git ls-files -z --cached --others --exclude-standard -- '*.go' go.mod go.sum '*.sh' '*.yml' '*.yaml' '*.proto' '*.rs' '*.h' '*.c' '*.py' '*Dockerfile*' test/cases/measure/qlgen/generated/suite.json |
    LC_ALL=C sort -zu | xargs -0 sha256sum | sha256sum | cut -d ' ' -f 1
}

MODE=${1:-}
if [[ "${MODE}" == "fingerprint" ]]; then
  source_fingerprint
  exit 0
fi
if [[ "${MODE}" != "rpc" && "${MODE}" != "kernel" ]]; then
  echo "usage: run-t17-frozen.sh fingerprint|rpc|kernel" >&2
  exit 2
fi

# This driver never builds. A source fingerprint sidecar, captured around the
# coordinated prebuild, binds each reused binary to the frozen source tree.
BINARY=${DQB_T17_BINARY:?set DQB_T17_BINARY to the prebuilt binary under this workspace}
HOST_BINARY=$(realpath "${BINARY}")
if [[ ! -x "${HOST_BINARY}" || "${HOST_BINARY}" != "${REPO_ROOT}/"* ]]; then
  echo "DQB_T17_BINARY must be an executable within the mounted workspace" >&2
  exit 2
fi
SOURCE_SHA=$(source_fingerprint)
if [[ ! -r "${HOST_BINARY}.source.sha256" || "$(cat "${HOST_BINARY}.source.sha256")" != "${SOURCE_SHA}" ]]; then
  echo "binary source sidecar is missing or differs; rebuild only before the coordinated timing freeze" >&2
  exit 2
fi
BINARY_SHA=$(sha256sum "${HOST_BINARY}" | cut -d ' ' -f 1)
CONTAINER_BINARY="/work/${HOST_BINARY#"${REPO_ROOT}/"}"
IMAGE=${DQB_DOCKER_IMAGE:-banyandb-distributed-querybench:go1.26}
REPORT_ROOT=${DQB_T17_REPORT_ROOT:-.omx/bench-reports/qlb-t17-${MODE}-final}
mkdir -p "${REPORT_ROOT}"
REPORT_ROOT=$(realpath "${REPORT_ROOT}")
if [[ "${REPORT_ROOT}" != "${REPO_ROOT}/"* ]]; then
  echo "report root must be under the mounted workspace" >&2
  exit 2
fi
exec > >(tee -a "${REPORT_ROOT}/driver.log") 2>&1
printf '%s\n' "${SOURCE_SHA}" > "${REPORT_ROOT}/source.sha256"
printf '%s\n' "${BINARY_SHA}" > "${REPORT_ROOT}/binary.sha256"

OVERLOAD_REPORT=""
OVERLOAD_SHA=""
if [[ "${MODE}" == "rpc" ]]; then
  : "${DQB_T17_OVERLOAD_REPORT:?set a verified revised overload/recovery report}"
  OVERLOAD_REPORT=$(realpath "${DQB_T17_OVERLOAD_REPORT}")
  if [[ ! -r "${OVERLOAD_REPORT}" || "${OVERLOAD_REPORT}" != "${REPO_ROOT}/"* ]]; then
    echo "overload/recovery evidence must be readable under this workspace" >&2
    exit 2
  fi
  OVERLOAD_SHA=$(sha256sum "${OVERLOAD_REPORT}" | cut -d ' ' -f 1)
fi
BASE_NANOS=${DQB_BASE_NANOS:-$(date -u +%s%N)}

check_frozen() {
  if [[ "$(source_fingerprint)" != "${SOURCE_SHA}" || "$(sha256sum "${HOST_BINARY}" | cut -d ' ' -f 1)" != "${BINARY_SHA}" ]]; then
    echo "frozen source or binary changed during measurement" >&2
    exit 1
  fi
  if [[ -n "${OVERLOAD_REPORT}" && "$(sha256sum "${OVERLOAD_REPORT}" | cut -d ' ' -f 1)" != "${OVERLOAD_SHA}" ]]; then
    echo "overload/recovery evidence changed during measurement" >&2
    exit 1
  fi
}

for repetition in 1 2 3 4 5; do
  check_frozen
  RUN_DIR="${REPORT_ROOT}/r${repetition}"
  if [[ -e "${RUN_DIR}" ]]; then
    echo "refusing to replace existing repetition ${RUN_DIR}" >&2
    exit 2
  fi
  mkdir -p "${RUN_DIR}"
  CONTAINER_REPORT="/work/${RUN_DIR#"${REPO_ROOT}/"}"
  echo "[T17] ${MODE} repetition ${repetition}; source=${SOURCE_SHA}; binary=${BINARY_SHA}"
  DOCKER_ARGS=(--rm --user "$(id -u):$(id -g)" --cpus 4 --memory 8g --memory-swap 8g --pids-limit 4096
    -v "${REPO_ROOT}:/work" -w /work -e GOMAXPROCS=4)
  if [[ "${MODE}" == "rpc" ]]; then
    CONTAINER_OVERLOAD=""
    if [[ -n "${OVERLOAD_REPORT}" ]]; then CONTAINER_OVERLOAD="/work/${OVERLOAD_REPORT#"${REPO_ROOT}/"}"; fi
    docker run "${DOCKER_ARGS[@]}" \
      -e RUN_DISTRIBUTED_QUERY_BENCH=1 -e DQB_IN_CONTAINER=1 -e DQB_ENGINE=measure \
      -e DQB_CPU_LIMIT=4 -e DQB_MEMORY_LIMIT=8g -e DQB_DOCKER_IMAGE="${IMAGE}" \
      -e DQB_MODES=vec -e DQB_SCENARIOS=qlb_t17 -e DQB_CARDINALITIES=1024,32768 \
      -e DQB_QUERY_WORKERS=1 -e DQB_QUERY_MEMORY_MIB=256 -e DQB_QUERY_ITERATIONS=100 -e DQB_WARMUP_ITERATIONS=5 \
      -e DQB_WRITERS=4 -e DQB_PROFILE=1 -e DQB_BASE_NANOS="${BASE_NANOS}" \
      -e DQB_TEST_BINARY="${CONTAINER_BINARY}" -e DQB_REPORT_DIR="${CONTAINER_REPORT}" \
      -e DQB_T17_OVERLOAD_REPORT="${CONTAINER_OVERLOAD}" \
      "${IMAGE}" bash /work/test/integration/distributed/querybench/orchestrate.sh > "${RUN_DIR}/container.log" 2>&1
  else
    docker run "${DOCKER_ARGS[@]}" "${IMAGE}" "${CONTAINER_BINARY}" \
      -test.run '^$' -test.bench '^BenchmarkSortT17$' -test.benchmem -test.benchtime=1s -test.count=1 \
      -test.cpuprofile="${CONTAINER_REPORT}/cpu.pprof" -test.memprofile="${CONTAINER_REPORT}/heap.pprof" \
      > "${RUN_DIR}/kernel.txt" 2>&1
  fi
  check_frozen
  echo "[T17] ${MODE} repetition ${repetition} complete: ${RUN_DIR}"
done
if [[ "${MODE}" == "kernel" ]]; then
  python3 "${SCRIPT_DIR}/summarize-t17-kernel.py" "${REPORT_ROOT}"
fi
