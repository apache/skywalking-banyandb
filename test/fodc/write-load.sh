#!/bin/bash

# Licensed to Apache Software Foundation (ASF) under one or more
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

# write-load.sh generates I/O on BanyanDB by creating property groups and
# querying them via the HTTP REST API. This ensures KTM eBPF tracepoints
# observe real disk I/O so that ktm_* metrics become non-zero.
#
# The script runs in the background and keeps generating load until killed.

set -uo pipefail

POD_NAME=${POD_NAME:-banyand-fodc-ktm}
BANYAND_CONTAINER=${BANYAND_CONTAINER:-banyand}
BANYAND_API=${BANYAND_API:-http://127.0.0.1:17913/api}
LOAD_INTERVAL=${LOAD_INTERVAL:-1}

http_post() {
  local path="$1"
  local body="$2"
  kubectl exec "${POD_NAME}" -c "${BANYAND_CONTAINER}" -- \
    sh -c "wget -qO- --header 'Content-Type: application/json' \
    --post-data '${body}' \
    '${BANYAND_API}${path}'" 2>/dev/null || true
}

http_get() {
  local path="$1"
  kubectl exec "${POD_NAME}" -c "${BANYAND_CONTAINER}" -- \
    sh -c "wget -qO- '${BANYAND_API}${path}'" 2>/dev/null || true
}

echo "write-load: waiting for pod to be ready..."
kubectl wait --for=condition=Ready "pod/${POD_NAME}" --timeout=300s

echo "write-load: creating property groups to generate write I/O..."
for i in $(seq 1 5); do
  http_post "/v1/group/schema" \
    "{\"group\":{\"metadata\":{\"name\":\"e2e-fodc-${i}\"},\"catalog\":\"CATALOG_PROPERTY\",\"resource_opts\":{\"shard_num\":1}}}"
done

echo "write-load: entering continuous read/write loop..."
round=0
while true; do
  round=$((round + 1))

  # Create another group (write I/O)
  http_post "/v1/group/schema" \
    "{\"group\":{\"metadata\":{\"name\":\"e2e-fodc-loop-${round}\"},\"catalog\":\"CATALOG_PROPERTY\",\"resource_opts\":{\"shard_num\":1}}}"

  # List all groups (read I/O)
  http_get "/v1/group/schema/lists"

  # Health check (read I/O)
  http_get "/healthz"

  if [ "$((round % 10))" -eq 0 ]; then
    echo "write-load: completed ${round} rounds"
  fi

  sleep "${LOAD_INTERVAL}"
done
