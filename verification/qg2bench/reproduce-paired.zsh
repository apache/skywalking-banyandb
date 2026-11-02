#!/usr/bin/env zsh
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
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

set -e
module_cache=$(go env GOMODCACHE)
build_cache=$(go env GOCACHE)
candidate=$PWD
baseline=${QG2_BASELINE_WORKTREE:-/tmp/qg1-baseline-check}
[[ $(git -C "$baseline" rev-parse HEAD) == 111193cab562b2d120a3e003b609f22923c9ff55 ]]
mkdir -p "$baseline/verification/qg2bench"
cp "$candidate/verification/qg2bench/paired_test.go" "$candidate/verification/qg2bench/qg2_benchmark_test.go" "$baseline/verification/qg2bench/"
for rows in 64 8192; do
  for round in 0 1 2 3 4; do
    if (( round % 2 == 0 )); then order='baseline current'; else order='current baseline'; fi
    for version in $=order; do
      if [[ "$version" == baseline ]]; then workspace=$baseline; is_baseline=1; else workspace=$candidate; is_baseline=0; fi
      docker run --rm --name "qg2-paired-${rows}-${round}-${version}" --cpuset-cpus=0,1 --memory=4g --memory-swap=4g \
        -e GOMAXPROCS=2 -e QG2_PAIRED=1 -e QG2_BASELINE="$is_baseline" -e QG2_ROWS="$rows" -e QG2_ROUND="$round" \
        -e GOCACHE=/gocache -e GOPATH=/go -v "$workspace:/workspace" -v "$module_cache:/go/pkg/mod" \
        -v "$build_cache:/gocache" -w /workspace golang:1.26.10 /bin/sh -c \
        'go test -tags=querymetrics ./verification/qg2bench -run "^$" -bench "^BenchmarkQG2RPC$" -benchtime=1x -count=1 -timeout=10m -v' \
        > "/tmp/qg2-paired-${rows}-${round}-${version}.log" 2>&1
done
done
done
