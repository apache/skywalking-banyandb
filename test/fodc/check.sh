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

set -euo pipefail

POD_NAME=${POD_NAME:-banyand-fodc-ktm}
CONTAINER_NAME=${CONTAINER_NAME:-fodc-agent}
METRICS_URL=${METRICS_URL:-http://127.0.0.1:9090/metrics}
WAIT_TIMEOUT=${WAIT_TIMEOUT:-300s}
MAX_WAIT_SECONDS=${MAX_WAIT_SECONDS:-60}
SLEEP_SECONDS=${SLEEP_SECONDS:-2}

kubectl wait --for=condition=Ready "pod/${POD_NAME}" --timeout="${WAIT_TIMEOUT}"

deadline=$((SECONDS + MAX_WAIT_SECONDS))
last_metrics=""
ktm_status_disabled=""

while [ "${SECONDS}" -lt "${deadline}" ]; do
  metrics="$(kubectl exec "${POD_NAME}" -c "${CONTAINER_NAME}" -- sh -c "wget -qO- ${METRICS_URL}" || true)"
  if [ -n "${metrics}" ]; then
    last_metrics="${metrics}"
  fi

  ktm_status="$(printf '%s\n' "${metrics}" | awk '/^ktm_status[[:space:]]/ {print $2; exit}')"
  if [ -n "${ktm_status}" ]; then
    if awk -v v="${ktm_status}" 'BEGIN {exit (v >= 1.0) ? 0 : 1}'; then
      echo "KTM is enabled. ktm_status=${ktm_status}"
      exit 0
    fi
    ktm_status_disabled="${ktm_status}"
    break
  fi

  sleep "${SLEEP_SECONDS}"
done

echo "KTM metrics check failed."
if [ -n "${ktm_status_disabled}" ]; then
  echo "Found ktm_status but it is disabled (value ${ktm_status_disabled})."
else
  echo "ktm_status metric not found."
fi

if [ -n "${last_metrics}" ]; then
  echo "Last metrics sample (ktm_ lines only):"
  printf '%s\n' "${last_metrics}" | awk '/^ktm_/ {print}'
fi

echo "FODC logs (tail):"
kubectl logs "${POD_NAME}" -c "${CONTAINER_NAME}" --tail=200

exit 1
