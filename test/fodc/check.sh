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
MAX_WAIT_SECONDS=${MAX_WAIT_SECONDS:-120}
SLEEP_SECONDS=${SLEEP_SECONDS:-2}
PRINT_KTM_ON_SUCCESS=${PRINT_KTM_ON_SUCCESS:-true}
PRINT_KTM_SAMPLES_ON_SUCCESS=${PRINT_KTM_SAMPLES_ON_SUCCESS:-false}
MAX_KTM_SAMPLE_LINES=${MAX_KTM_SAMPLE_LINES:-40}
REQUIRED_KTM_METRIC_COUNT=${REQUIRED_KTM_METRIC_COUNT:-8}
REQUIRED_KTM_METRICS=${REQUIRED_KTM_METRICS:-"ktm_status ktm_degraded \
ktm_fadvise_calls_total ktm_cache_lookups_total \
ktm_sys_read_latency_seconds_count ktm_sys_pread_latency_seconds_count \
ktm_sys_read_latency_seconds_sum ktm_sys_pread_latency_seconds_sum"}
FAILURE_LOG_TAIL_LINES=${FAILURE_LOG_TAIL_LINES:-200}
NONZERO_KTM_METRICS=${NONZERO_KTM_METRICS:-"ktm_sys_read_latency_seconds_sum ktm_sys_pread_latency_seconds_sum"}

echo "Preflight: Host kernel and tracing info"
echo "Kernel: $(uname -r)"
if [ -r /sys/kernel/btf/vmlinux ]; then
  echo "BTF: /sys/kernel/btf/vmlinux is readable"
else
  echo "BTF: /sys/kernel/btf/vmlinux is NOT readable"
fi
if [ -d /sys/kernel/tracing ]; then
  echo "Tracefs: /sys/kernel/tracing exists"
else
  echo "Tracefs: /sys/kernel/tracing is missing"
fi

kubectl wait --for=condition=Ready "pod/${POD_NAME}" --timeout="${WAIT_TIMEOUT}"

deadline=$((SECONDS + MAX_WAIT_SECONDS))
last_metrics=""
ktm_status_inactive=""
last_ktm_metric_count=0
last_missing_metrics=""

while [ "${SECONDS}" -lt "${deadline}" ]; do
  metrics="$(kubectl exec "${POD_NAME}" -c "${CONTAINER_NAME}" -- sh -c "wget -qO- ${METRICS_URL}" || true)"
  if [ -n "${metrics}" ]; then
    last_metrics="${metrics}"
  fi

  ktm_metric_names="$(printf '%s\n' "${metrics}" | awk '/^ktm_[a-zA-Z0-9_]+([[:space:]]|\{)/ {name = $1; sub(/\{.*/, "", name); print name}' | sort -u)"
  ktm_metric_count="$(printf '%s\n' "${ktm_metric_names}" | awk 'NF {count++} END {print count + 0}')"
  last_ktm_metric_count="${ktm_metric_count}"

  missing_metrics=""
  for required_metric in ${REQUIRED_KTM_METRICS}; do
    if ! printf '%s\n' "${ktm_metric_names}" | awk -v metric="${required_metric}" '$0 == metric {found = 1} END {exit found ? 0 : 1}'; then
      missing_metrics="${missing_metrics} ${required_metric}"
    fi
  done
  last_missing_metrics="$(echo "${missing_metrics}" | xargs)"

  ktm_status="$(printf '%s\n' "${metrics}" | awk '/^ktm_status[[:space:]]/ {print $2; exit}')"
  if [ -n "${ktm_status}" ]; then
    if awk -v v="${ktm_status}" 'BEGIN {exit (v == 2.0) ? 0 : 1}'; then
      if [ "${ktm_metric_count}" -ge "${REQUIRED_KTM_METRIC_COUNT}" ] && [ -z "${last_missing_metrics}" ]; then
        # Verify at least one I/O metric has a value > 0
        has_nonzero=false
        for nz_metric in ${NONZERO_KTM_METRICS}; do
          nz_val="$(printf '%s\n' "${metrics}" | awk -v m="${nz_metric}" '$0 ~ "^"m"[[:space:]]" {print $2; exit}')"
          if [ -n "${nz_val}" ] && awk -v v="${nz_val}" 'BEGIN {exit (v > 0) ? 0 : 1}'; then
            has_nonzero=true
            break
          fi
        done
        if ! ${has_nonzero}; then
          sleep "${SLEEP_SECONDS}"
          continue
        fi
        echo "KTM smoke check passed. ktm_status=${ktm_status}, ktm_metric_count=${ktm_metric_count}"
        if [ "${PRINT_KTM_ON_SUCCESS}" = "true" ]; then
          echo "::group::KTM success details"
          echo "Detected unique ktm_* metric names:"
          printf '%s\n' "${ktm_metric_names}"
          if [ "${PRINT_KTM_SAMPLES_ON_SUCCESS}" = "true" ]; then
            echo "KTM metric samples (first unique entries):"
            printf '%s\n' "${metrics}" | awk -v max_lines="${MAX_KTM_SAMPLE_LINES}" '
              /^ktm_[a-zA-Z0-9_]+([[:space:]]|\{)/ {
                metric_name = $1
                sub(/\{.*/, "", metric_name)
                if (!(metric_name in shown)) {
                  print $0
                  shown[metric_name] = 1
                  printed++
                }
                if (printed >= max_lines) {
                  exit
                }
              }
            '
          fi
          echo "::endgroup::"
        fi
        exit 0
      fi
    else
      ktm_status_inactive="${ktm_status}"
    fi
  fi

  sleep "${SLEEP_SECONDS}"
done

echo "KTM metrics check failed."
if [ -n "${ktm_status_inactive}" ]; then
  echo "Found ktm_status but KTM is not in strict mode (value ${ktm_status_inactive}, expected == 2)."
else
  echo "ktm_status metric not found."
fi
echo "Found unique ktm_* metrics: ${last_ktm_metric_count} (required >= ${REQUIRED_KTM_METRIC_COUNT})"
if [ -n "${last_missing_metrics}" ]; then
  echo "Missing required ktm metrics:${last_missing_metrics}"
fi

if [ -n "${last_metrics}" ]; then
  echo "::group::KTM metrics (ktm_ lines only)"
  printf '%s\n' "${last_metrics}" | awk '/^ktm_/ {print}'
  echo "::endgroup::"
  echo "::group::KTM metrics (full scrape)"
  printf '%s\n' "${last_metrics}"
  echo "::endgroup::"
fi

echo "::group::FODC logs (tail)"
kubectl logs "${POD_NAME}" -c "${CONTAINER_NAME}" --tail="${FAILURE_LOG_TAIL_LINES}"
echo "::endgroup::"

exit 1
