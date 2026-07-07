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

# End-to-end verification of the memory-pressure pprof feature against a real
# BanyanDB cluster + fodc-agent + fodc-proxy. All diagnostics go to stderr so the
# only stdout on success is the single line "status: passed" that infra-e2e
# compares against expected/pressure-status.yml; any failed assertion exits non-zero.

set -euo pipefail

NS=${NS:-default}
AGENT_CONTAINER=${AGENT_CONTAINER:-fodc-agent}
# infra-e2e exposes service/fodc-proxy:17913 and sets these env vars (dashes -> underscores).
PROXY_HOST=${service_fodc_proxy_host:-127.0.0.1}
PROXY_PORT=${service_fodc_proxy_17913:-17913}
PROXY_HTTP="http://${PROXY_HOST}:${PROXY_PORT}"
MAX_WAIT=${MAX_WAIT:-180}
SLEEP=${SLEEP:-5}

log() { echo "[check-pressure] $*" >&2; }

POD="$(kubectl get pod -n "${NS}" -l app=banyandb-standalone -o jsonpath='{.items[0].metadata.name}')"
[ -n "${POD}" ] || { log "no banyandb-standalone pod found"; exit 1; }
log "using data pod ${POD}, proxy ${PROXY_HTTP}"

# 1) Wait until the agent reports at least one capture via its own /metrics.
deadline=$((SECONDS + MAX_WAIT))
captured=0
while [ "${SECONDS}" -lt "${deadline}" ]; do
  # `|| true` keeps a transient kubectl/wget failure (pod not ready yet) from tripping
  # `set -e`/pipefail and aborting the poll loop instead of retrying.
  total="$(kubectl exec -n "${NS}" "${POD}" -c "${AGENT_CONTAINER}" -- sh -c 'wget -qO- http://localhost:9090/metrics' 2>/dev/null \
    | awk '/^fodc_agent_pressure_capture_total/ {print $2; exit}' || true)"
  if [ -n "${total}" ] && awk -v v="${total}" 'BEGIN { exit (v >= 1) ? 0 : 1 }'; then
    captured=1
    log "fodc_agent_pressure_capture_total=${total}"
    break
  fi
  sleep "${SLEEP}"
done
if [ "${captured}" != 1 ]; then
  log "no pressure capture observed within ${MAX_WAIT}s; recent agent logs:"
  kubectl logs -n "${NS}" "${POD}" -c "${AGENT_CONTAINER}" --tail=100 >&2 || true
  exit 1
fi

# 2) The captured profile files exist on the agent's volume.
evtdir="$(kubectl exec -n "${NS}" "${POD}" -c "${AGENT_CONTAINER}" -- sh -c 'ls -d /tmp/pressure-profiles/*/ 2>/dev/null | head -1' | tr -d '\r')"
[ -n "${evtdir}" ] || { log "no capture event directory on agent volume"; exit 1; }
files="$(kubectl exec -n "${NS}" "${POD}" -c "${AGENT_CONTAINER}" -- sh -c "ls '${evtdir}'")"
for f in heap.pprof goroutine.pprof meta.json; do
  echo "${files}" | grep -q "${f}" || { log "missing ${f} in ${evtdir}"; exit 1; }
done
log "event dir ${evtdir} has heap.pprof, goroutine.pprof, meta.json"

# 3) meta.json reflects a real cgroup limit and both profiles.
meta="$(kubectl exec -n "${NS}" "${POD}" -c "${AGENT_CONTAINER}" -- sh -c "cat '${evtdir}meta.json'")"
echo "${meta}" | python3 -c '
import sys, json
m = json.load(sys.stdin)
assert m["cgroupLimitBytes"] > 0, "cgroupLimitBytes must be positive (cgroup limit set)"
assert m["rssBytes"] > 0, "rssBytes must be positive"
assert len(m["profiles"]) == 2, "expected heap + goroutine"
'
log "meta.json: cgroupLimitBytes>0, 2 profiles"

# 4) The proxy lists the event metadata over HTTP.
list="$(curl -sf "${PROXY_HTTP}/pressure-profiles")"
echo "${list}" | python3 -c '
import sys, json
recs = json.load(sys.stdin)
assert len(recs) >= 1, "proxy listed no pressure profiles"
r = next(x for x in recs if len(x["profiles"]) == 2)
assert r["cgroup_limit_bytes"] > 0
heap = next(p for p in r["profiles"] if p["type"] == "heap")
assert heap["size_bytes"] > 0, "listed heap size_bytes must be positive"
with open("/tmp/pp_target.txt", "w") as fh:
    fh.write(r["pod_name"] + " " + r["profile_id"] + " " + str(heap["size_bytes"]) + "\n")
'
read -r TARGET_POD TARGET_ID TARGET_HEAP_SIZE < /tmp/pp_target.txt
log "proxy /pressure-profiles lists pod=${TARGET_POD} id=${TARGET_ID} heap_size=${TARGET_HEAP_SIZE}"

# 5) Download a profile through the proxy and confirm it is a complete, valid pprof.
# No -f: capture the status code ourselves so a non-200 surfaces clearly (the || only trips
# on a transport-level failure, e.g. connection refused).
http_code="$(curl -s -o /tmp/pressure-heap.pprof -w '%{http_code}' \
  "${PROXY_HTTP}/pressure-profiles/${TARGET_POD}/${TARGET_ID}/heap")" \
  || { log "heap download request failed (curl exit $?)"; exit 1; }
[ "${http_code}" = "200" ] || { log "heap download returned HTTP ${http_code}, want 200; body:"; cat /tmp/pressure-heap.pprof >&2 || true; exit 1; }

# The body must be non-empty and its byte count must match the size the proxy advertised in
# the listing, proving the whole stream arrived rather than a truncated or empty body.
[ -s /tmp/pressure-heap.pprof ] || { log "downloaded heap profile is empty"; exit 1; }
dl_size="$(wc -c < /tmp/pressure-heap.pprof | tr -d ' ')"
[ "${dl_size}" = "${TARGET_HEAP_SIZE}" ] \
  || { log "downloaded heap size ${dl_size} != advertised ${TARGET_HEAP_SIZE}"; exit 1; }

# Parse it with pprof, keeping the output and checking the exit status explicitly (piping to
# /dev/null would hide a parse failure). A real heap profile exposes an "inuse_space" sample
# type, so assert on the content too, not just the exit code.
if ! raw="$(go tool pprof -raw /tmp/pressure-heap.pprof 2>&1)"; then
  log "go tool pprof failed to parse heap.pprof: ${raw}"
  exit 1
fi
grep -q "Samples:" <<<"${raw}" || { log "pprof output has no 'Samples:' section: ${raw}"; exit 1; }
grep -q "inuse_space" <<<"${raw}" || { log "pprof output missing heap 'inuse_space' sample type"; exit 1; }
log "downloaded heap.pprof OK: ${dl_size} bytes, matches listing, valid pprof with inuse_space"

echo "status: passed"
