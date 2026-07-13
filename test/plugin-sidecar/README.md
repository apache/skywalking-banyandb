<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Plugin-sidecar ship gate (kind)

A self-contained [kind](https://kind.sigs.k8s.io/) test that proves the
trace-pipeline plugin packaging works in **real Kubernetes** before the PR
ships. banyandb images ONLY — no OAP, no producer/consumer, no traffic
generator.

## What it proves

Deploys a **standalone** BanyanDB (the standalone role is `ROLE_DATA`, which
hosts plugins) from the CGO/dynamic **`-plugins` host image** (distroless
base), and delivers the `.so` from the **`-plugins-carrier`-tagged carrier
image** (same `apache/skywalking-banyandb` repo) via an **initContainer + a
shared `emptyDir` mounted at `/plugins`** (the version-agnostic path; the
busybox carrier provides `cp`). On Kubernetes ≥1.31 an OCI image volume is the
alternative — noted in `banyandb-standalone.yaml`, with the initContainer path
being the one that runs.

Required assertions (the gate):

- **(a)** the banyand pod reaches **Ready** — the CGO/dynamic host actually
  boots in-cluster;
- **(b)** `latencystatussampler.so` was delivered into the shared `/plugins`
  volume — verified from the carrier `install-plugins` initContainer's log
  (the distroless host container has no shell to `exec ls` into);
- **(c)** a group whose trace pipeline references
  `SamplerPlugin.path=latencystatussampler.so` is registered, and the data node
  **loads it with no failure** — `plugin.Open` succeeded on the mounted `.so`.
  Evidence: the fail-open ERROR (`sampler plugin load failed; keeping previous
  good set`) is **absent**, and `banyandb_trace_pipeline_sampler_active_count{group="test-trace-pipeline"} > 0`.
  The group/schema/pipeline are registered through the **property schema
  registry** (`register/main.go`, port 17916) — the store the data node's trace
  schemaRepo watches to fire the pipeline reconcile. (The GroupRegistryService
  HTTP/gRPC endpoint writes a different store that the pipeline reconcile does
  not observe, and its HTTP gateway also drops the nested pipeline field.)

The end-to-end DROP behavior (drop-eligible vs keep trace over a merge) is
already covered by the in-process `trace_pipeline` integration suite
(`test/integration/standalone/pipeline`) against the same artifacts; this gate
scopes to what only real Kubernetes can prove.

## Run

```sh
make -C test/plugin-sidecar e2e-plugin-sidecar
```

Builds the amd64 `-plugins` host + carrier images if they are not already
present, creates a kind cluster, `kind load`s BOTH images (same tag =
lockstep parity), applies the manifest, runs the assertions, and tears the
cluster down (always, even on failure, unless `KEEP_CLUSTER=true`).

Knobs: `TAG`, `HOST_IMAGE`, `CARRIER_IMAGE`, `SKIP_BUILD=true`,
`KEEP_CLUSTER=true`.

Runtime note: the standalone data node's property-schema client warms up
before it first syncs a freshly-registered group, so assertion (c) waits for
the pipeline reconcile. The plugin load itself is instant once the reconcile
fires, but the cold-start reconcile latency on kind is highly variable
(observed ~4.5-10 min across runs; instant on an already-warm node) — this is
standalone schema-sync warmup, orthogonal to the plugin packaging/mount the
gate proves. The (c) poll therefore waits up to ~15 min (a genuine load
FAILURE fails fast regardless). Total gate runtime is ~7-18 min depending on
how quickly the cold node's schema sync settles.

## Files

- `kind.yaml` — single-node kind cluster (node image 1.30.x).
- `banyandb-standalone.yaml` — the standalone Pod: carrier initContainer →
  `emptyDir` at `/plugins` → host container with the two plugin flags.
  `${HOST_IMAGE}`/`${CARRIER_IMAGE}` are substituted by `run.sh`.
- `register/main.go` — a tiny helper that writes the trace group + schema +
  pipeline through the property schema registry client (assertion c).
- `run.sh` — the driver; `Makefile` — the `e2e-plugin-sidecar` target.
