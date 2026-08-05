#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with this work for
# additional information regarding copyright ownership. The ASF licenses this
# file to you under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# Capture a frozen controlled merge seed from the production-shaped fixture.
# The seed underpins the Phase 1 disabled/enabled alternating comparison: every
# alternating run clones the same seed, so any difference in resource use is
# attributable to the pipeline rather than to selection drift.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
FIXTURE=${FIXTURE:-"$ROOT/.scratch/trace-pipeline-merge-performance/generated-fixture-ticket05"}
OUTPUT=${OUTPUT:-"$ROOT/.scratch/trace-pipeline-merge-performance/baseline-report"}
IMAGE=${IMAGE:-golang:1.25.12}
DATA_CPUS=${DATA_CPUS:-0-3}
SEED_OUT=${SEED_OUT:-"$OUTPUT/controlled-seed"}
BIN="$ROOT/.scratch/trace-pipeline-merge-performance/bin/trace-merge-benchmark"

if [[ ! -x "$BIN" && ! -f "$BIN" ]]; then
  mkdir -p "$(dirname "$BIN")"
  go build -o "$BIN" "$ROOT/banyand/cmd/trace-merge-benchmark"
fi

if [[ -f "$SEED_OUT/seed.json" ]]; then
  echo "controlled seed already captured at $SEED_OUT/seed.json" >&2
  exit 0
fi

run_dir="$OUTPUT/seed-capture"
mkdir -p "$run_dir"
docker run --rm -v "$FIXTURE":/fixture:ro -v "$run_dir":/run "$IMAGE" bash -c \
  'mkdir -p /run/source /run/data/sidx/latency /run/data/sidx/start_time; cp -a /fixture/shard/. /run/source/; chmod 0777 /run'
container="banyandb-trace-seed-capture"
docker rm -f "$container" >/dev/null 2>&1 || true
docker run -d --name "$container" --cpuset-cpus="$DATA_CPUS" --cpus=4 --memory=8g --memory-swap=8g --pids-limit=512 \
  -e GOMAXPROCS=4 \
  -v "$ROOT":/workspace:ro -v "$run_dir":/run -w /workspace "$IMAGE" \
  /workspace/.scratch/trace-pipeline-merge-performance/bin/trace-merge-benchmark capture-seed \
  --source=/run/source --data=/run/data --schedule=/workspace/${FIXTURE#"$ROOT/"}/schedule.json \
  --output=/run/controlled-seed >"$run_dir/container.id"
docker logs -f "$container" >"$run_dir/data-node.log" 2>&1 &
LOG_PID=$!
if ! docker wait "$container" >"$run_dir/wait.code"; then
  kill "$LOG_PID" 2>/dev/null || true
  docker rm -f "$container" >/dev/null 2>&1 || true
  echo "seed capture failed; see $run_dir/data-node.log" >&2
  exit 1
fi
wait "$LOG_PID" 2>/dev/null || true
if [[ $(cat "$run_dir/wait.code") != "0" ]]; then
  docker rm -f "$container" >/dev/null 2>&1 || true
  echo "seed capture exited with status $(cat "$run_dir/wait.code"); see $run_dir/data-node.log" >&2
  exit 1
fi
docker rm -f "$container" >/dev/null 2>&1 || true
if [[ ! -d "$run_dir/controlled-seed" ]] || [[ ! -f "$run_dir/controlled-seed/seed.json" ]]; then
  echo "seed capture produced no seed.json" >&2
  exit 1
fi
mkdir -p "$SEED_OUT"
cp -a "$run_dir/controlled-seed"/. "$SEED_OUT"/
echo "controlled seed captured at $SEED_OUT"
