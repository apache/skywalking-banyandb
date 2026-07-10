# Developing Trace-Pipeline Sampler Plugins

A single, linear walkthrough — **implement → verify → debug → deploy** — for a
trace-pipeline sampler plugin. It is the entry point that ties together the
reference material:

- **Contract & source layout:** [`plugins/README.md`](../../plugins/README.md)
- **Reference plugin:** [`pkg/pipeline/sdk/_example/segment-tail-sampler`](../../pkg/pipeline/sdk/_example/segment-tail-sampler)
- **SDK API:** the package doc on `pkg/pipeline/sdk`
- **Offline test kit:** `pkg/pipeline/sdk/sdktest`
- **Packaging & deploy:** [`plugins.md`](plugins.md)
- **Debugging runbook:** [`plugins-debugging.md`](plugins-debugging.md)

## What a plugin is

A sampler plugin is a native Go `.so` the data node loads at runtime to decide,
per trace, keep-or-drop during merge/finalize. It is a `package main` built with
`go build -buildmode=plugin` that exports exactly two SDK symbols. **Read the
ABI/toolchain lock in [`plugins/README.md`](../../plugins/README.md) before
building for production** — a `.so` must be built with the exact toolchain/SDK
of the host it loads into, or `plugin.Open` rejects it (safe and loud, never
silent). For first-party plugins this parity is guaranteed by lockstep builds.

Prerequisites: Go (pinned via `go.mod`), a C toolchain (`gcc`/`clang`, since
plugins require `CGO_ENABLED=1`), Linux or macOS (not Windows).

## Step 1 — Implement

Create `plugins/skywalking/<name>/main.go`:

```go
package main

import "github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"

var ABIVersion = sdk.ABIVersion // re-export, unchanged — MUST equal the host's

// NewSampler is the default constructor symbol; config is the canonical JSON of
// the SamplerPlugin.config google.protobuf.Struct.
func NewSampler(config []byte) (sdk.Sampler, error) { /* parse config, return sampler */ }

func main() {} // required by -buildmode=plugin
```

Your `sdk.Sampler` implements `Kind`, `Project`, `Decide`, `Close`:

- **`Project()`** declares up-front which tag columns / span data your `Decide`
  reads. **Only projected columns are decoded** — reading an unprojected tag
  returns null, the #1 cause of "unexpected sampling" (Step 3 catches this).
- **`Decide(batch *sdk.TraceBatch) (sdk.Verdict, error)`** returns a keep-mask
  aligned to `batch.Traces`. The batch is READ-ONLY.

See the reference plugin and `plugins/README.md`'s "The plugin contract" for the
full detail.

## Step 2 — Verify offline (no `.so`, no cluster)

Write a `_test.go` next to `main.go` using the `sdktest` kit — the fastest loop,
before you ever build a `.so`:

```go
batch := sdktest.NewTrace("t1").
    Tag("status", "error").TagAs("duration", valuetype.ValueTypeInt64, int64(900)).
    Build()
verdict, report := sdktest.Run(sampler, sdktest.Batch(batch))
// report flags a sampler whose verdict changes when unprojected columns are
// added (the differential projection guard). Use sdktest.RunChain for a chain.
```

Then build the `.so` locally to confirm it compiles in plugin mode:

```sh
make build-plugins   # → $(PLUGIN_OUTPUT_DIR), default build/bin/plugins
```

Worked example: `plugins/skywalking/latencystatussampler/main_test.go`.

## Step 3 — Debug

Use the symptom-driven runbook: [`plugins-debugging.md`](plugins-debugging.md).
It covers, offline via `sdktest`, the two field symptoms:

- **"nothing is being sampled"** → check the feature flag, `cfg.enabled`, the
  event gate, plugin load failure (`sampler_load_failed` metric / the fail-open
  ERROR log), grace, and an all-keep verdict.
- **"unexpected data is sampled"** → reproduce with `sdktest`; the differential
  guard flags a `Decide` that depends on an unprojected column; check keep-mask
  length, chain AND semantics, and late-data assembly.

On a live node the load signals are `sampler_active_count{group}` (>0 = loaded)
and `sampler_load_failed{group,name,reason}`; a load failure is loud (ERROR log)
and fail-open (the group keeps its previous sampler set, node stays up).

## Step 4 — Deploy

First-party plugins ship in the carrier image
(`apache/skywalking-banyandb:<tag>-plugins-carrier`) and are **mounted** into the
plugin-capable host image (`apache/skywalking-banyandb:<tag>-plugins`) at
`/plugins`. Enable on the **data-node** pod:

```
-trace-pipeline-native-plugin-enabled=true
-trace-pipeline-trusted-plugin-dir=/plugins
```

Reference the plugin by filename in the group's `TracePipelineConfig`:

```json
{ "enabled": true, "enabledEvents": ["PIPELINE_EVENT_MERGE"],
  "plugins": [ { "name": "latency-status",
    "sampler": { "path": "latencystatussampler.so", "abiVersion": 1,
                 "config": { "thresholdMs": 500, "successValue": "success" } } } ] }
```

Mount the carrier at `/plugins` via an **OCI image volume** (K8s ≥1.31) or an
**initContainer + emptyDir** (portable) — full instructions, both mechanisms,
third-party plugins, and Kubernetes manifests are in [`plugins.md`](plugins.md)
and [`examples/kubernetes/plugins/`](../../examples/kubernetes/plugins).

## Step 5 — Validate the deployment shape in Kubernetes

The `test/plugin-sidecar` kind gate proves the CGO host + carrier-mount design
works in real Kubernetes (host boots, carrier delivers the `.so`, the node loads
it). Run it locally before shipping a packaging change:

```sh
make -C test/plugin-sidecar e2e-plugin-sidecar
```

It also runs automatically in CI (`.github/workflows/test-plugin-sidecar-e2e.yml`)
on PRs that touch the plugin surface.

## Third-party plugins

A third-party `.so` is **not** committed here — the vendor builds it against the
pinned SDK + toolchain of the target release (see the ABI/toolchain lock) and
mounts it at the reserved subdirectory `/plugins/thirdparty`. See
[`plugins.md`](plugins.md).
