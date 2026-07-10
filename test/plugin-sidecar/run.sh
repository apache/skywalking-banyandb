#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
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
#
# SHIP GATE: prove the CGO "-plugins" host image + carrier-mount design works
# in REAL Kubernetes (kind). banyandb images ONLY — no OAP, no producer, no
# traffic generator.
#
# Flow: create kind cluster -> (build if needed) -> kind load BOTH images ->
#       kubectl apply standalone (carrier initContainer -> emptyDir /plugins)
#       -> wait Ready -> assertions -> teardown (always).
#
# Required assertions (the gate):
#   (a) banyand pod reaches Ready              (CGO/dynamic host boots in-cluster)
#   (b) /plugins/latencystatussampler.so present in the banyand container
#   (c) register a group whose pipeline references latencystatussampler.so and
#       assert the data node LOADS it with NO failure (plugin.Open succeeded):
#         - the fail-open ERROR is ABSENT, AND
#         - sampler_active_count{group}>0 (a positive load-success signal).
# Stretch: write a drop-eligible + a keep trace, await merge, assert the
#          eligible one is dropped (best-effort; does not fail the gate).
#
# Env knobs:
#   TAG            image tag (default: latest)
#   HOST_IMAGE     default: apache/skywalking-banyandb:${TAG}-plugins
#   CARRIER_IMAGE  default: apache/skywalking-banyandb:${TAG}-plugins-carrier
#   SKIP_BUILD     if "true", do not build images (assume present / to be loaded)
#   KEEP_CLUSTER   if "true", do not delete the kind cluster on exit (debugging)
#
# NOTE on the "stretch" (write a drop-eligible + keep trace, merge, assert the
# eligible one is dropped): that end-to-end DROP behavior is already proven by
# the in-process trace_pipeline integration suite
# (test/integration/standalone/pipeline), which boots the SAME "-plugins" host
# binary + carrier .so via --trace-pipeline-trusted-plugin-dir and asserts the
# sampler drops the eligible trace on merge. This kind gate deliberately scopes
# to what only real Kubernetes can prove — the CGO host boots as a pod and the
# carrier-mounted .so is loadable in-cluster (assertions a/b/c) — rather than
# re-deriving the drop logic (which needs the trace-write path, not available
# via bydbctl) more flakily over a port-forward.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

CLUSTER="${CLUSTER:-banyandb-plugin-sidecar}"
TAG="${TAG:-latest}"
HOST_IMAGE="${HOST_IMAGE:-apache/skywalking-banyandb:${TAG}-plugins}"
CARRIER_IMAGE="${CARRIER_IMAGE:-apache/skywalking-banyandb:${TAG}-plugins-carrier}"
SKIP_BUILD="${SKIP_BUILD:-false}"
KEEP_CLUSTER="${KEEP_CLUSTER:-false}"
RUN_STRETCH="${RUN_STRETCH:-true}"

POD="banyandb-plugin"
GROUP="test-trace-pipeline"
KCTX="kind-${CLUSTER}"
PF_PID=""

log()  { echo -e "\n=== $* ==="; }
fail() { echo "GATE FAILURE: $*" >&2; exit 1; }

cleanup() {
  local rc=$?
  if [[ -n "${PF_PID}" ]]; then kill "${PF_PID}" 2>/dev/null || true; fi
  if [[ "${rc}" -ne 0 ]]; then
    log "FAILURE diagnostics (rc=${rc})"
    kubectl --context "${KCTX}" get pods -o wide 2>/dev/null || true
    kubectl --context "${KCTX}" describe pod "${POD}" 2>/dev/null | tail -40 || true
    echo "--- banyandb container logs (tail) ---"
    kubectl --context "${KCTX}" logs "${POD}" -c banyandb 2>/dev/null | tail -60 || true
    echo "--- install-plugins initContainer logs ---"
    kubectl --context "${KCTX}" logs "${POD}" -c install-plugins 2>/dev/null | tail -20 || true
  fi
  if [[ "${KEEP_CLUSTER}" != "true" ]]; then
    log "Deleting kind cluster ${CLUSTER}"
    kind delete cluster --name "${CLUSTER}" 2>/dev/null || true
  fi
  exit "${rc}"
}
trap cleanup EXIT

build_images() {
  if [[ "${SKIP_BUILD}" == "true" ]]; then
    log "SKIP_BUILD=true — assuming ${HOST_IMAGE} and ${CARRIER_IMAGE} already exist"
    return
  fi
  if docker image inspect "${HOST_IMAGE}" >/dev/null 2>&1 && \
     docker image inspect "${CARRIER_IMAGE}" >/dev/null 2>&1; then
    log "Reusing existing images ${HOST_IMAGE} and ${CARRIER_IMAGE}"
    return
  fi
  log "Building companion static binaries + plugin host + carrier images (amd64)"
  ( cd "${REPO_ROOT}" && PLATFORMS=linux/amd64 make -C banyand release )
  ( cd "${REPO_ROOT}" && PLATFORMS=linux/amd64 TAG="${TAG}" BINARYTYPE=plugins make -C banyand docker )
  ( cd "${REPO_ROOT}" && PLATFORMS=linux/amd64 TAG="${TAG}" make -C banyand docker.plugins-carrier )
}

main() {
  command -v kind >/dev/null    || fail "kind not found on PATH"
  command -v kubectl >/dev/null || fail "kubectl not found on PATH"
  command -v envsubst >/dev/null || fail "envsubst not found on PATH (install gettext-base)"
  command -v go >/dev/null || fail "go not found on PATH (needed to run the pipeline register helper)"

  # Defensive: kill only OUR stale kubectl port-forwards — ones targeting this
  # test's pod ("pod/${POD}") — left by a prior run, so the ephemeral-port
  # forwards below can always bind. Narrowed (not every `port-forward`) so a
  # developer's unrelated port-forwards on the same machine are left alone.
  for p in $(pgrep -x kubectl 2>/dev/null || true); do
    cmd="$(tr '\0' ' ' < "/proc/$p/cmdline" 2>/dev/null)"
    case "$cmd" in
      *port-forward*"pod/${POD}"*) kill "$p" 2>/dev/null || true ;;
    esac
  done

  build_images

  log "Creating kind cluster ${CLUSTER}"
  kind create cluster --name "${CLUSTER}" --config "${SCRIPT_DIR}/kind.yaml" --wait 180s

  log "Loading BOTH plugin images into kind (lockstep, same tag)"
  kind load docker-image "${HOST_IMAGE}" --name "${CLUSTER}"
  kind load docker-image "${CARRIER_IMAGE}" --name "${CLUSTER}"

  log "Deploying standalone banyandb (carrier initContainer -> emptyDir /plugins)"
  HOST_IMAGE="${HOST_IMAGE}" CARRIER_IMAGE="${CARRIER_IMAGE}" \
    envsubst '${HOST_IMAGE} ${CARRIER_IMAGE}' < "${SCRIPT_DIR}/banyandb-standalone.yaml" \
    | kubectl --context "${KCTX}" apply -f -

  # ---- Assertion (a): pod reaches Ready (CGO/dynamic host boots) ----
  log "Assertion (a): waiting for pod/${POD} to become Ready"
  kubectl --context "${KCTX}" wait --for=condition=Ready "pod/${POD}" --timeout=240s \
    || fail "(a) pod ${POD} did not become Ready — the CGO host image failed to boot in-cluster"
  kubectl --context "${KCTX}" get pods -o wide
  echo "(a) PASS: banyandb pod (CGO/dynamic -plugins host image) is Ready"

  # ---- Assertion (b): the carrier delivered the .so into the shared /plugins ----
  # The host (banyandb) container is distroless — it has NO shell, so we cannot
  # `kubectl exec ... ls` into it. Instead verify delivery from the carrier
  # initContainer's own log: it runs `cp /plugins/*.so /shared/ && ls -l /shared`
  # into the emptyDir the host mounts at /plugins, so its log lists the .so it
  # placed there.
  log "Assertion (b): latencystatussampler.so delivered to the shared /plugins by the carrier initContainer"
  local initlog
  initlog="$(kubectl --context "${KCTX}" logs "${POD}" -c install-plugins 2>/dev/null || true)"
  echo "${initlog}"
  echo "${initlog}" | grep -q "latencystatussampler.so" \
    || fail "(b) install-plugins initContainer log does not show latencystatussampler.so — the carrier did not populate the shared volume"
  echo "(b) PASS: latencystatussampler.so delivered to the shared /plugins volume (per the carrier initContainer log)"

  # ---- Assertion (c): register the pipeline; assert the data node LOADS it ----
  # Registered through the property schema registry (the store the data node's
  # trace schemaRepo watches), NOT the GroupRegistryService HTTP/gRPC endpoint
  # (whose store the pipeline reconcile does not observe, and whose HTTP gateway
  # drops the nested pipeline field). Each port-forward uses a fresh EPHEMERAL
  # local port and is torn down after use, so a stale forward from a prior run
  # can never block the bind (the failure that previously wedged the poll).
  local reg_lport=$(( (RANDOM % 10000) + 40000 ))
  local met_lport=$(( (RANDOM % 10000) + 30000 ))

  log "Assertion (c): registering group + pipeline referencing latencystatussampler.so (property schema :17916 via local ${reg_lport})"
  kubectl --context "${KCTX}" port-forward "pod/${POD}" "${reg_lport}:17916" >/tmp/pf-reg.log 2>&1 &
  local reg_pf=$!
  # Wire into the EXIT cleanup trap so an early exit between here and the kill
  # below (e.g. `go build` failing under `set -e`) still reaps this forward.
  PF_PID="${reg_pf}"
  sleep 4
  local register_bin="${RUNNER_TEMP:-/tmp}/plugin-sidecar-register"
  ( cd "${REPO_ROOT}" && go build -o "${register_bin}" ./test/plugin-sidecar/register )
  "${register_bin}" \
    --property-schema-addr "localhost:${reg_lport}" \
    --node-name "${POD}:17912" \
    --group "${GROUP}" \
    --so latencystatussampler.so \
    || { kill "${reg_pf}" 2>/dev/null || true; PF_PID=""; fail "register helper failed"; }
  kill "${reg_pf}" 2>/dev/null || true
  # Forward is dead; clear PF_PID so cleanup doesn't act on a stale PID (the
  # metrics forward below sets PF_PID to its own PID next).
  PF_PID=""

  log "Port-forwarding metrics :2121 via local ${met_lport}"
  kubectl --context "${KCTX}" port-forward "pod/${POD}" "${met_lport}:2121" >/tmp/pf-metrics.log 2>&1 &
  PF_PID=$!
  local up="false"
  for _ in $(seq 1 30); do
    if curl -fsS "http://localhost:${met_lport}/metrics" >/dev/null 2>&1; then up="true"; break; fi
    sleep 1
  done
  [[ "${up}" == "true" ]] || { cat /tmp/pf-metrics.log 2>/dev/null || true; fail "metrics port-forward to :2121 did not come up"; }

  # The standalone data node's property-schema client warms up before it syncs
  # a freshly-registered group (a hardcoded ~60s init-wait plus additional
  # standalone metadata warmup). The plugin ALWAYS loads once the reconcile
  # fires — that is instant — but the cold-start reconcile latency is highly
  # variable on kind (observed ~4.5-10 min across runs; instant on an already
  # warm node). This wait is purely that standalone schema-sync warmup, wholly
  # orthogonal to the plugin packaging/mount this gate proves. Poll up to
  # ~15 min to reliably cover the observed worst case with margin (the isolated
  # CI job's timeout is 60 min). A genuine load FAILURE (fail-open ERROR /
  # incLoadFailed) fails FAST below regardless of this budget.
  log "Assertion (c): waiting for the data node to load the sampler (standalone schema-sync warmup; can take several minutes)"
  local loaded="false"
  for i in $(seq 1 900); do
    # Negative signal: the fail-open ERROR must NOT appear.
    if kubectl --context "${KCTX}" logs "${POD}" -c banyandb 2>/dev/null \
        | grep -q "sampler plugin load failed"; then
      echo "--- load-failure log line found ---"
      kubectl --context "${KCTX}" logs "${POD}" -c banyandb | grep "sampler plugin load failed" | tail -5
      fail "(c) data node logged a sampler LOAD FAILURE — plugin.Open rejected the mounted .so (parity broken)"
    fi
    # Positive signal: sampler_active_count{group} > 0.
    local active
    active="$(curl -fsS http://localhost:${met_lport}/metrics 2>/dev/null \
      | grep '^banyandb_trace_pipeline_sampler_active_count' \
      | grep "group=\"${GROUP}\"" || true)"
    if [[ -n "${active}" ]]; then
      local val="${active##* }"
      if awk "BEGIN{exit !(${val}>0)}" 2>/dev/null; then
        echo "metric: ${active} (after ~${i}s)"
        loaded="true"
        break
      fi
    fi
    (( i % 30 == 0 )) && echo "(c) ${i}s elapsed, waiting for reconcile..."
    sleep 1
  done

  # Final negative-signal check regardless of timing.
  if kubectl --context "${KCTX}" logs "${POD}" -c banyandb 2>/dev/null | grep -q "sampler plugin load failed"; then
    fail "(c) data node logged a sampler LOAD FAILURE after reconcile"
  fi
  [[ "${loaded}" == "true" ]] \
    || fail "(c) sampler_active_count{group=${GROUP}} never became >0 — the pipeline did not load the plugin"

  echo "(c) PASS: plugin.Open succeeded on the carrier-mounted .so in real K8s"
  echo "         - fail-open ERROR ABSENT; sampler_active_count{group=${GROUP}}>0"
  echo "--- load-success metrics ---"
  curl -fsS http://localhost:${met_lport}/metrics 2>/dev/null \
    | grep -E '^banyandb_trace_pipeline_sampler_(active_count|register_total|load_failed)' | grep "${GROUP}" || true

  log "SHIP GATE PASSED: (a) Ready, (b) .so mounted, (c) plugin loaded in real Kubernetes"
}

main "$@"
