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
IMAGE=${DQB_DOCKER_IMAGE:-banyandb-distributed-querybench:go1.25}
CPUS=${DQB_CPU_LIMIT:-4}
MEMORY=${DQB_MEMORY_LIMIT:-8g}
MEMORY_SWAP=${DQB_MEMORY_SWAP_LIMIT:-${MEMORY}}
PIDS_LIMIT=${DQB_PIDS_LIMIT:-4096}
BUILD_IMAGE=1
DOCKER_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image)
      IMAGE=$2
      shift 2
      ;;
    --cpus)
      CPUS=$2
      shift 2
      ;;
    --memory)
      MEMORY=$2
      MEMORY_SWAP=${DQB_MEMORY_SWAP_LIMIT:-${MEMORY}}
      shift 2
      ;;
    --memory-swap)
      MEMORY_SWAP=$2
      shift 2
      ;;
    --pids-limit)
      PIDS_LIMIT=$2
      shift 2
      ;;
    --pull-image)
      BUILD_IMAGE=0
      shift
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

mkdir -p "${REPO_ROOT}/.omx/bench-reports/distributed-query"

if [[ "${BUILD_IMAGE}" == "1" ]]; then
  docker build -t "${IMAGE}" -f "${SCRIPT_DIR}/Dockerfile" "${SCRIPT_DIR}"
else
  docker pull "${IMAGE}"
fi

DOCKER_ARGS+=(--rm)
DOCKER_ARGS+=(--cpus "${CPUS}")
DOCKER_ARGS+=(--memory "${MEMORY}")
DOCKER_ARGS+=(--memory-swap "${MEMORY_SWAP}")
DOCKER_ARGS+=(--pids-limit "${PIDS_LIMIT}")
DOCKER_ARGS+=(-e RUN_DISTRIBUTED_QUERY_BENCH=1)
DOCKER_ARGS+=(-e DQB_IN_CONTAINER=1)
DOCKER_ARGS+=(-e DQB_CPU_LIMIT="${CPUS}")
DOCKER_ARGS+=(-e DQB_MEMORY_LIMIT="${MEMORY}")
DOCKER_ARGS+=(-e DQB_DOCKER_IMAGE="${IMAGE}")
DOCKER_ARGS+=(-e DQB_CARDINALITIES="${DQB_CARDINALITIES:-1024,10000,100000,1000000,2000000}")
DOCKER_ARGS+=(-e DQB_SCENARIOS="${DQB_SCENARIOS:-scan_all,top_with_filter}")
DOCKER_ARGS+=(-e DQB_QUERY_WORKERS="${DQB_QUERY_WORKERS:-4}")
DOCKER_ARGS+=(-e DQB_QUERY_ITERATIONS="${DQB_QUERY_ITERATIONS:-50}")
DOCKER_ARGS+=(-e DQB_WARMUP_ITERATIONS="${DQB_WARMUP_ITERATIONS:-3}")
DOCKER_ARGS+=(-e DQB_WRITERS="${DQB_WRITERS:-4}")
DOCKER_ARGS+=(-e DQB_PROFILE="${DQB_PROFILE:-1}")
DOCKER_ARGS+=(-e DQB_REPORT_DIR="${DQB_REPORT_DIR:-/work/.omx/bench-reports/distributed-query}")
DOCKER_ARGS+=(-v "${REPO_ROOT}:/work")
DOCKER_ARGS+=(-w /work)

exec docker run "${DOCKER_ARGS[@]}" "${IMAGE}" bash /work/test/integration/distributed/querybench/orchestrate.sh
