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

# G5a runner — drives the paired W1..W5 microbenchmarks, runs the in-Go gate
# enforcement test, and emits a markdown report at
# dist/bench/vectorized-YYYYMMDD-HHMMSS.md. Exit non-zero if any gate fails.
#
# Usage:
#   ./scripts/bench-vectorized.sh
#   COUNT=3 BENCHTIME=1s ./scripts/bench-vectorized.sh   # quicker sanity run
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
PKG="./pkg/query/vectorized/measure"
TIMESTAMP="$(date -u +%Y%m%d-%H%M%S)"
OUT_DIR="${ROOT_DIR}/dist/bench"
REPORT="${OUT_DIR}/vectorized-${TIMESTAMP}.md"
RAW="${OUT_DIR}/vectorized-${TIMESTAMP}.txt"
GATES_LOG="${OUT_DIR}/vectorized-gates-${TIMESTAMP}.log"

mkdir -p "${OUT_DIR}"

echo "G5a bench runner"
echo "  pkg:        ${PKG}"
echo "  count:      ${COUNT}"
echo "  benchtime:  ${BENCHTIME}"
echo "  output:     ${REPORT}"
echo

echo "==> Running paired microbenchmarks..."
go test "${PKG}" \
  -run='^$' \
  -bench='^Benchmark(RowPath|VectorizedPath)_W[1-5]$' \
  -benchmem \
  -count="${COUNT}" \
  -benchtime="${BENCHTIME}" \
  -timeout=30m \
  | tee "${RAW}"

echo
echo "==> Running gate enforcement test (RUN_BENCH_GATES=1)..."
gate_status=0
RUN_BENCH_GATES=1 go test "${PKG}" \
  -run='^TestBenchGates_PerWorkload$' \
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
  echo "# Vectorized Query Path — Microbench Report"
  echo
  echo "- Generated: \`${TIMESTAMP}\` UTC"
  echo "- Commit:    \`${commit}\`"
  echo "- Host:      \`${host}\`"
  echo "- Go:        \`${go_version}\`"
  echo "- Count:     ${COUNT}"
  echo "- Benchtime: ${BENCHTIME}"
  echo
  echo "## Paired benchmark output"
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
