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

# Per-tier data presence check for migrated measure and stream groups via
# the liaison HTTP API. Probes the most recent N days; for each day issues
# a query shaped to the granularity that probe target stores:
#
#   measure:
#     - sw_metricsDay    : full-day window  (00:00:00 .. 23:59:59 UTC)
#     - sw_metricsHour   : the 08:00 hour   (08:00:00 .. 08:59:59 UTC)
#     - sw_metricsMinute : the 08:08 minute (08:08:00 .. 08:08:59 UTC)
#   stream:
#     - sw_recordsLog / log                              : the 08:00 hour
#     - sw_recordsBrowserErrorLog / browser_error_log    : the 08:00 hour
#
# A trailing "★MISSING" marker is appended whenever the probe returns zero
# data points / elements. There is no per-tier row-count threshold — any
# non-zero response counts as "present". Days beyond a group's retention
# are expected to be missing (record groups typically keep far fewer days
# than metrics).
#
# Prereqs: kubectl port-forward to a liaison HTTP port (default 17913).
#
# Override via env:
#   LIAISON_HTTP (default: http://127.0.0.1:17913)
#   DAYS         (default: 15) - number of most recent UTC days to probe
#   CATALOGS     (default: "measure stream") - which catalogs to probe
set -euo pipefail

LIAISON_HTTP="${LIAISON_HTTP:-http://127.0.0.1:17913}"
DAYS="${DAYS:-15}"
CATALOGS="${CATALOGS:-measure stream}"

# Emit today, today-1, ..., today-(N-1) in YYYY-MM-DD UTC. GNU and BSD
# `date` use different flags for "N days ago" — probe once and pick the
# right invocation.
if date -u -d "0 days ago" +%Y-%m-%d >/dev/null 2>&1; then
    DATE_FLAVOR=gnu
elif date -u -v-0d +%Y-%m-%d >/dev/null 2>&1; then
    DATE_FLAVOR=bsd
else
    echo "ERROR: neither GNU nor BSD date supports date-arithmetic on this host" >&2
    exit 1
fi

day_minus() {
    local i="$1"
    case "$DATE_FLAVOR" in
        gnu) date -u -d "${i} days ago" +%Y-%m-%d ;;
        bsd) date -u -v-"${i}d" +%Y-%m-%d ;;
    esac
}

# Count rows in the response body without invoking jq/python:
#   - /api/v1/measure/data carries one `"timestamp"` field per data point
#   - /api/v1/stream/data  carries one `"elementId"` field per element
# `grep -o` returns exit 1 when there are zero matches; isolate it in a
# subshell + `|| true` so empty responses don't trip `set -e`/pipefail.
probe_measure() {
    local group="$1" measure="$2" begin="$3" end="$4"
    local body resp
    body=$(cat <<EOF
{
  "groups": ["$group"],
  "name": "$measure",
  "stages": ["hot","warm","cold"],
  "timeRange": {"begin": "$begin", "end": "$end"},
  "tagProjection": {"tagFamilies":[{"name":"storage-only","tags":["entity_id"]}]},
  "fieldProjection": {"names":[]},
  "limit": 100000
}
EOF
)
    resp=$(curl -s -XPOST "$LIAISON_HTTP/api/v1/measure/data" \
        -H 'Content-Type: application/json' -d "$body" || true)
    printf '%s' "$resp" | { grep -o '"timestamp"' || true; } | wc -l | tr -d ' '
}

probe_stream() {
    local group="$1" stream="$2" begin="$3" end="$4"
    local body resp
    body=$(cat <<EOF
{
  "groups": ["$group"],
  "name": "$stream",
  "stages": ["hot","warm","cold"],
  "timeRange": {"begin": "$begin", "end": "$end"},
  "projection": {"tagFamilies":[{"name":"searchable","tags":["unique_id"]}]},
  "limit": 100000
}
EOF
)
    resp=$(curl -s -XPOST "$LIAISON_HTTP/api/v1/stream/data" \
        -H 'Content-Type: application/json' -d "$body" || true)
    printf '%s' "$resp" | { grep -o '"elementId"' || true; } | wc -l | tr -d ' '
}

echo "data presence check across most recent $DAYS day(s)"
echo "  liaison : $LIAISON_HTTP"
echo "  catalogs: $CATALOGS"
echo

scan() {
    local probe_fn="$1" label="$2" group="$3" name="$4" begin_suffix="$5" end_suffix="$6" window_label="$7"
    echo "-- $label ($group / $name, window: $window_label) --"
    local bad=0 i=0 day n marker
    while [ "$i" -lt "$DAYS" ]; do
        day=$(day_minus "$i")
        n=$("$probe_fn" "$group" "$name" "${day}${begin_suffix}" "${day}${end_suffix}")
        marker=""
        if [ "${n:-0}" -eq 0 ]; then
            marker="  ★MISSING"
            bad=$((bad + 1))
        fi
        printf "  %s: %s%s\n" "$day" "$n" "$marker"
        i=$((i + 1))
    done
    echo "  $bad day(s) missing"
    echo
}

case " $CATALOGS " in *" measure "*)
    scan probe_measure "Day tier"    sw_metricsDay    service_apdex_day    T00:00:00Z T23:59:59Z "full day"
    scan probe_measure "Hour tier"   sw_metricsHour   service_apdex_hour   T08:00:00Z T08:59:59Z "08:00..09:00"
    scan probe_measure "Minute tier" sw_metricsMinute service_apdex_minute T08:08:00Z T08:08:59Z "08:08..08:09"
;; esac

case " $CATALOGS " in *" stream "*)
    scan probe_stream "Log records"           sw_recordsLog             log               T08:00:00Z T08:59:59Z "08:00..09:00"
    scan probe_stream "Browser error records" sw_recordsBrowserErrorLog browser_error_log T08:00:00Z T08:59:59Z "08:00..09:00"
;; esac
