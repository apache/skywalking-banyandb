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

# G9e runner — drives the storage-backed W1..W8 benchmarks (real flushed measure
# parts decoded through the production queryResult -> vectorized pipeline ->
# egress path), runs the in-Go gate enforcement test, and emits a markdown
# report at
# dist/bench/vectorized-YYYYMMDD-HHMMSS.md. Exit non-zero if any gate fails.
#
# Usage:
#   ./scripts/bench-vectorized.sh
#   COUNT=3 BENCHTIME=1s ./scripts/bench-vectorized.sh   # quicker sanity run
#   BYDB_BENCH_LEGACY_SCHEMA=1 ./scripts/bench-vectorized.sh  # the passthrough baseline
#
# The gates compare the native column selection against the previous passthrough
# selection over the same stored part, plus checked-in absolute allocs/op budgets;
# see banyand/measure/vectorized_gates_benchmark_test.go. The old prebuilt-proto
# harness in pkg/query/vectorized/measure was retired (it hid storage decode).
#
# Macro benchmarks live at test/integration/standalone/benchmark/ and run
# separately via `make test-ci PKG=./test/integration/standalone/benchmark/...`
# — they require booting a real Measure module and so don't fit the inner
# go-test bench loop.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COUNT="${COUNT:-5}"
BENCHTIME="${BENCHTIME:-2s}"
PKG="./banyand/measure"
TIMESTAMP="$(date -u +%Y%m%d-%H%M%S)"
OUT_DIR="${ROOT_DIR}/dist/bench"
REPORT="${OUT_DIR}/vectorized-${TIMESTAMP}.md"
RAW="${OUT_DIR}/vectorized-${TIMESTAMP}.txt"
GATES_LOG="${OUT_DIR}/vectorized-gates-${TIMESTAMP}.log"

mkdir -p "${OUT_DIR}"

echo "G9e bench runner"
echo "  pkg:        ${PKG}"
echo "  count:      ${COUNT}"
echo "  benchtime:  ${BENCHTIME}"
echo "  output:     ${REPORT}"
echo

echo "==> Running storage-backed W1..W8 benchmarks..."
go test "${PKG}" \
  -run='^$' \
  -bench='^BenchmarkVectorizedStorage$' \
  -benchmem \
  -count="${COUNT}" \
  -benchtime="${BENCHTIME}" \
  -timeout=30m \
  | tee "${RAW}"

echo
echo "==> Running gate enforcement test (RUN_BENCH_GATES=1)..."
gate_status=0
RUN_BENCH_GATES=1 go test "${PKG}" \
  -run='^TestVectorizedStorageGates$' \
  -count=1 \
  -timeout=30m \
  -v \
  | tee "${GATES_LOG}" || gate_status=$?

echo
echo "==> Writing report ${REPORT}"

commit="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
host="$(uname -mnsr 2>/dev/null || echo unknown)"
go_version="$(go version 2>/dev/null || echo unknown)"

{
  echo "# Vectorized Query Path — Storage-backed Bench Report"
  echo
  echo "- Generated: \`${TIMESTAMP}\` UTC"
  echo "- Commit:    \`${commit}\`"
  echo "- Host:      \`${host}\`"
  echo "- Go:        \`${go_version}\`"
  echo "- Count:     ${COUNT}"
  echo "- Benchtime: ${BENCHTIME}"
  echo
  echo "## Benchmark output"
  echo
  echo '```'
  cat "${RAW}"
  echo '```'
  echo
  echo "## Gate enforcement"
  echo
  if [ "${gate_status}" -eq 0 ]; then
    echo "**PASS** — all gates met."
  else
    echo "**FAIL** — at least one gate violated. See log below."
  fi
  echo
  echo '```'
  cat "${GATES_LOG}"
  echo '```'
} > "${REPORT}"

echo
if [ "${gate_status}" -ne 0 ]; then
  echo "GATE VIOLATION — see ${REPORT}"
  exit "${gate_status}"
fi

echo "All gates passed. Report: ${REPORT}"
