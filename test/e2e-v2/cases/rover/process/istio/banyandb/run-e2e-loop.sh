#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
set -e

# Run Rover Istio BanyanDB e2e. Use in a loop: run, fix failures, re-run until pass.
# Usage: ./run-e2e-loop.sh
# Prerequisites: build BanyanDB image first:
#   source test/e2e-v2/script/env && export TAG=$(git rev-parse HEAD)
#   make release && make -C test/docker build

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../../.." && pwd)"
cd "$ROOT"

source test/e2e-v2/script/env
export TAG="${TAG:-$(git rev-parse HEAD)}"
export PATH="/tmp/skywalking-infra-e2e/bin:$PATH"

E2E_CONFIG="test/e2e-v2/cases/rover/process/istio/banyandb/e2e-banyandb.yaml"

kind delete cluster --name kind 2>/dev/null || true
exec e2e run -c "$E2E_CONFIG" -B
