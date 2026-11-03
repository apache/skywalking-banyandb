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
mkdir -p verification/qg3bench/logs
for rows in 64 8192; do
  for round in 0 1 2; do
    docker run --rm --name "qg3-bench-${rows}-${round}" --cpuset-cpus=0,1 --memory=4g --memory-swap=4g \
      -e GOMAXPROCS=2 -e QG3_ROWS="$rows" -e QG3_ROUND="$round" \
      -e GOCACHE=/gocache -e GOPATH=/go -v "$PWD:/workspace" -v "$module_cache:/go/pkg/mod" \
      -v "$build_cache:/gocache" -w /workspace golang:1.26.10 /bin/sh -c \
      'go test -tags=querymetrics ./verification/qg3bench -run "^$" -bench "^BenchmarkQG3RPC$" -benchtime=1x -count=1 -timeout=10m -v' \
      > "verification/qg3bench/logs/${rows}-${round}.log" 2>&1
  done
done
