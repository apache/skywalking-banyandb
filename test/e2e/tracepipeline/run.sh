#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# E2E gate for the trace pipeline native plugin: build the latencystatussampler
# .so + a CGO banyand-server, launch a standalone process whose pipeline
# reconciler observes a property-schema-driven sampler registration, and run
# the ginkgo entrypoint against it. This replaces the in-process integration
# suite (test/integration/standalone/pipeline, deleted) that forked a
# banyand-server binary via pkg/test/setup/external.go, which conflicted with
# the project's "integration tests run in a single Go process" principle.
#
# Required behaviour (asserted by ginkgo in e2e_test.go):
#   (a) standalone boots with --trace-pipeline-native-plugin-enabled=true
#   (b) property-schema RegisterSamplerRuntime fires reconcilePipeline and
#       plugin.Open succeeds on the staged .so
#   (c) UpdateSamplerRuntime and RemoveSamplerRuntime converge on the running
#       node without restarting it
#   (d) InvalidConfig (missing .so) leaves the prior sampler set active and
#       keeps the node alive (fail-open)
#   (e) After the standalone is stopped and relaunched from the same data dir,
#       the sampler is re-applied from the schema store without a second
#       RegisterSamplerRuntime call (this is what only a process restart can
#       prove; the in-package unit tests in banyand/trace/pipeline_watch_test.go
#       cover everything else).
#
# Env knobs:
#   BANYAND_SERVER_CGO_BIN  path of the CGO banyand-server binary
#                           (default: ${REPO_ROOT}/banyand/build/bin/dev/banyand-server)
#   PLUGIN_OUTPUT_DIR       directory holding latencystatussampler.so
#                           (default: ${REPO_ROOT}/build/bin/plugins)
#   SKIP_BUILD              if "true", assume both binaries are already built
#   E2E_TIMEOUT             overall wall-clock budget (default 10m)
#   KEEP_TEMP               if "true", do not delete temp dirs on exit (debug)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

BANYAND_SERVER_CGO_BIN="${BANYAND_SERVER_CGO_BIN:-${REPO_ROOT}/banyand/build/bin/dev/banyand-server}"
PLUGIN_OUTPUT_DIR="${PLUGIN_OUTPUT_DIR:-${REPO_ROOT}/build/bin/plugins}"
PLUGIN_SO_PATH="${PLUGIN_SO_PATH:-${PLUGIN_OUTPUT_DIR}/latencystatussampler.so}"
SKIP_BUILD="${SKIP_BUILD:-false}"
E2E_TIMEOUT="${E2E_TIMEOUT:-10m}"
KEEP_TEMP="${KEEP_TEMP:-false}"

WORK_DIR=""
SERVER_PID=""

log()  { echo -e "\n=== $* ==="; }
fail() { echo "E2E FAILURE: $*" >&2; exit 1; }

cleanup() {
  local rc=$?
  if [[ -n "${SERVER_PID}" ]]; then kill "${SERVER_PID}" 2>/dev/null || true; wait "${SERVER_PID}" 2>/dev/null || true; fi
  if [[ "${KEEP_TEMP}" != "true" && -n "${WORK_DIR}" && -d "${WORK_DIR}" ]]; then
    rm -rf "${WORK_DIR}"
  fi
  if [[ -n "${WORK_DIR}" && -d "${WORK_DIR}" ]]; then
    echo "KEEP_TEMP=true: temp dir retained at ${WORK_DIR}"
    echo "--- standalone stdout+stderr ---"
    [[ -f "${WORK_DIR}/log/standalone.log" ]] && tail -200 "${WORK_DIR}/log/standalone.log" || true
  fi
  exit "${rc}"
}
trap cleanup EXIT

build_binaries() {
  if [[ "${SKIP_BUILD}" == "true" ]]; then
    log "SKIP_BUILD=true — assuming ${BANYAND_SERVER_CGO_BIN} and ${PLUGIN_SO_PATH} already exist"
    [[ -x "${BANYAND_SERVER_CGO_BIN}" ]] || fail "banyand-server binary missing at ${BANYAND_SERVER_CGO_BIN}"
    [[ -f "${PLUGIN_SO_PATH}" ]] || fail "plugin .so missing at ${PLUGIN_SO_PATH}"
    return
  fi
  log "Building latencystatussampler.so + CGO banyand-server (CGO_ENABLED=1; no -race)"
  ( cd "${REPO_ROOT}" && make build-trace-pipeline-plugin build-trace-pipeline-server )
}

allocate_ports() {
  # Pick 3 ports: grpc (public client), http (metrics + health), property
  # (property-schema gRPC; same listener as schema-server-grpc-port when mode
  # is property).
  local start=$(( ( RANDOM % 10000 ) + 40000 ))
  GRPC_PORT=$(( start ))
  HTTP_PORT=$(( start + 1 ))
  PROPERTY_PORT=$(( start + 2 ))
}

start_standalone() {
  local data_dir="$1"
  local log_dir="$2"
  local trusted_dir="$3"
  log "Launching standalone banyand-server (grpc=${GRPC_PORT}, http=${HTTP_PORT}, property=${PROPERTY_PORT})"
  "${BANYAND_SERVER_CGO_BIN}" standalone \
    --logging-env=dev \
    --logging-level=info \
    --grpc-host=127.0.0.1 \
    --grpc-port="${GRPC_PORT}" \
    --http-host=127.0.0.1 \
    --http-port="${HTTP_PORT}" \
    --http-grpc-addr="127.0.0.1:${GRPC_PORT}" \
    --node-host-provider=flag \
    --node-host=127.0.0.1 \
    --schema-server-grpc-host=127.0.0.1 \
    --schema-server-grpc-port="${PROPERTY_PORT}" \
    --schema-registry-mode=property \
    --node-discovery-mode=file \
    --node-discovery-file-path="${data_dir}/nodes.yaml" \
    --stream-root-path="${data_dir}/stream" \
    --measure-root-path="${data_dir}/measure" \
    --property-root-path="${data_dir}/property" \
    --trace-root-path="${data_dir}/trace" \
    --schema-server-root-path="${data_dir}/schema" \
    --trace-pipeline-native-plugin-enabled=true \
    --trace-pipeline-trusted-plugin-dir="${trusted_dir}" \
    --trace-max-merge-parts=2 \
    --trace-flush-timeout=500ms \
    --schema-server-flush-timeout=500ms \
    > "${log_dir}/standalone.log" 2>&1 &
  SERVER_PID=$!
}

wait_for_health() {
  log "Waiting for standalone gRPC :${GRPC_PORT} to become reachable"
  for _ in $(seq 1 60); do
    if (echo > "/dev/tcp/127.0.0.1/${GRPC_PORT}") >/dev/null 2>&1; then
      echo "standalone ready (pid=${SERVER_PID})"
      return
    fi
    sleep 1
  done
  fail "standalone did not become reachable on :${GRPC_PORT} within 60s; tail of log:"
  tail -120 "${WORK_DIR}/log/standalone.log" 2>/dev/null || true
}

run_ginkgo() {
  local phase="$1"
  log "Running ginkgo entrypoint (phase=${phase})"
  local ldflags="-X github.com/apache/skywalking-banyandb/pkg/test/flags.eventuallyTimeout=30s -X github.com/apache/skywalking-banyandb/pkg/test/flags.consistentlyTimeout=10s -X github.com/apache/skywalking-banyandb/pkg/test/flags.LogLevel=error"
  ( cd "${REPO_ROOT}" && \
    GRPC_ADDR="127.0.0.1:${GRPC_PORT}" \
    PROPERTY_ADDR="127.0.0.1:${PROPERTY_PORT}" \
    HTTP_PORT="${HTTP_PORT}" \
    TRUSTED_DIR="${WORK_DIR}/trusted" \
    SO_NAME="latencystatussampler.so" \
    E2E_PHASE="${phase}" \
    DATA_DIR="${WORK_DIR}/data" \
    PLUGIN_SO_PATH="${PLUGIN_SO_PATH}" \
    go test \
      -tags trace_pipeline_e2e \
      -timeout "${E2E_TIMEOUT}" \
      -count=1 \
      -ldflags "${ldflags}" \
      ./test/e2e/tracepipeline/...
  )
}

main() {
  command -v go >/dev/null || fail "go not found on PATH"
  command -v make >/dev/null || fail "make not found on PATH"

  build_binaries
  allocate_ports

  WORK_DIR="$(mktemp -d -t banyand-trace-pipeline-e2e.XXXXXX)"
  mkdir -p "${WORK_DIR}/data/stream" "${WORK_DIR}/data/measure" "${WORK_DIR}/data/property" \
           "${WORK_DIR}/data/trace" "${WORK_DIR}/data/schema" \
           "${WORK_DIR}/log" "${WORK_DIR}/trusted"

  # Stage the .so under its bare filename in the trusted dir; the validator
  # requires the schema-stored Path to be relative to the trusted dir.
  cp "${PLUGIN_SO_PATH}" "${WORK_DIR}/trusted/latencystatussampler.so"

  start_standalone "${WORK_DIR}/data" "${WORK_DIR}/log" "${WORK_DIR}/trusted"
  wait_for_health

  # Phase 1: dynamic lifecycle on the live server (Register/Update/Remove/
  # InvalidConfig). Ginkgo exits 0; server is still running afterwards.
  run_ginkgo "lifecycle"

  # Phase 2: process-restart replay — stop the server, relaunch from the same
  # data dir, then assert sampler_active_count > 0 WITHOUT a second
  # RegisterSamplerRuntime call (the schema store is the source of truth).
  log "Stopping standalone (pid=${SERVER_PID}) to test schema-store replay"
  kill "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
  SERVER_PID=""

  start_standalone "${WORK_DIR}/data" "${WORK_DIR}/log" "${WORK_DIR}/trusted"
  wait_for_health

  run_ginkgo "restart"

  log "E2E PASSED: dynamic lifecycle + schema-store replay against a real banyand-server"
}

main "$@"