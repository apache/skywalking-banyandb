# Trace-Pipeline Sampler Plugins

BanyanDB's trace pipeline can retain/drop traces in-merge, at segment
finalization, and during lifecycle migration using operator-installed native Go
plugins ("samplers"). This document covers packaging and deploying plugins on a running cluster. See
[`docs/design/archive/0.11.0/post-trace-pipeline.md`](../design/archive/0.11.0/post-trace-pipeline.md) for
the plugin ABI/contract, and
[`plugins/README.md`](../../plugins/README.md) for authoring a first-party
plugin. For diagnosing "nothing is being sampled" / "unexpected data is being
sampled" symptoms offline, see
[`docs/operation/plugins-debugging.md`](plugins-debugging.md).

## Data-node hosting

- Plugins are loaded **on demand at group-schema reconcile**, on the **data
  node role** (the standalone service also satisfies this role; the
  liaison never hosts plugins). Lifecycle agents load their own sampler chains
  before migration; see [Lifecycle-agent hosting](#lifecycle-agent-hosting).
- The feature is gated by two flags, both off by default:
  - `-trace-pipeline-native-plugin-enabled` (bool) — the data-node feature switch.
  - `-trace-pipeline-trusted-plugin-dir` (string) — the **single** directory
    `.so` paths are resolved within. There is no second flag and no
    multi-directory support.
- A plugin load failure is **fail-open and loud**: the group keeps its
  previous good sampler set (or retains all traces, on first load), an ERROR
  log line is emitted, and the `sampler_load_failed{group,name,reason}` metric
  increments. It is never a node crash or a hard stop.
- Go plugins **cannot be unloaded**; once a `.so` is opened it stays mapped
  for the process lifetime.

## Images

Plugins ship as **two** opt-in images, built and tagged together (see the
lockstep note below):

| Image | Base | Contents | Role |
|---|---|---|---|
| default (`apache/skywalking-banyandb:<tag>`) | `busybox:stable-glibc` | `CGO_ENABLED=0` static `banyand-server` | Normal image; hosts **no** plugins, ever |
| slim (`...:<tag>-slim`) | `busybox:stable-glibc` | `CGO_ENABLED=0` static `banyand-server` | Slim default; hosts no plugins |
| **plugins host** (`...:<tag>-plugins`) | `gcr.io/distroless/base-debian12` | `CGO_ENABLED=1` dynamic `banyand-server` and `/lifecycle` + an **empty** `/plugins` | The plugin-capable host; `.so` are mounted in, not baked |
| **plugins carrier** (`...:<tag>-plugins-carrier`) | `busybox:stable-glibc` | the built first-party `.so` at `/plugins` | Mounted into the host at `/plugins` to deliver plugins |

All four are tags on the **same** `apache/skywalking-banyandb` repository.

**A Go plugin cannot be loaded into a `CGO_ENABLED=0` statically-linked
host** — `plugin.Open` requires the host to be CGO-enabled and dynamically
linked, and the `.so` must share the host's exact Go toolchain, module graph
(including `pkg/pipeline/sdk`), CGO mode, race mode, and a compatible libc.
The default and slim images can host **no** plugins; use the `-plugins` host
image.

**The host image ships `/plugins` empty and carries no `.so`.** Plugins —
first-party and third-party alike — are delivered by **mounting** them at the
trusted dir. The first-party carrier image (the `-plugins-carrier`-tagged
image on the same repo) holds the SkyWalking-maintained `.so` and is mounted
at `/plugins`.

> **Lockstep parity (mandatory).** `plugin.Open` requires the `.so` and the
> host binary to share an exact toolchain/module-graph/libc. The host binary
> and the carrier `.so` are compiled from **one shared builder base at one
> commit in one CI run**, and both images are published at the **same `<tag>`**
> (the carrier tag equals the host tag). Always deploy the carrier at the same
> tag as the host image. A separate image is fully supported; independently
> *versioning* the carrier is not — any drift makes `plugin.Open` reject the
> `.so` (data-node fail-open or lifecycle preflight failure; see below).

## Enabling data-node plugins

Use the `-plugins` **host** image for the **data-node** pod (liaison pods
never host plugins) and set the two flags:

```
-trace-pipeline-native-plugin-enabled=true
-trace-pipeline-trusted-plugin-dir=/plugins
```

Mount the carrier at `/plugins`, then reference a plugin by filename in the
group's pipeline config (`TracePipelineConfig` on the `Group`,
`SamplerPlugin.path`), e.g.:

```json
{
  "enabled": true,
  "enabledEvents": ["PIPELINE_EVENT_MERGE"],
  "plugins": [
    {
      "name": "sw-trace-sampler",
      "sampler": {
        "path": "sw-trace-sampler.so",
        "abiVersion": 1,
        "config": { "durationThresholdMs": 500, "keepErrors": true, "healthySampleRate": 0.1 }
      }
    }
  ]
}
```

Attaching this `pipeline` to the group is what **activates** the plugin: the data
node watches the schema registry and, on the group add/update, reconciles the
pipeline and calls `plugin.Open` on the mounted `.so`. Register it through any
standard schema API — all go through `GroupRegistryService` and preserve the
nested `pipeline` field: `bydbctl group update -f group.yaml` (with a `pipeline:`
block), `PUT /api/v1/group/schema/{group}`, or the gRPC `GroupRegistryService.Update`
(see `test/cases/tracepipeline/ops.go`). Verify with
`banyandb_trace_pipeline_sampler_active_count{group=…}` `>0` (metrics port `2121`);
a bad `.so` fails open + loud (`…_sampler_load_failed`), leaving the node healthy.
After activation, import the optional
[Trace Sampling Plugins Grafana dashboard](grafana-fodc-trace-plugin.json) to monitor
execution latency, sampling outcomes, fail-open retention, drop-set capacity, and
finalization through the FODC proxy.

## Lifecycle-agent hosting

The lifecycle agent can run the same sampler plugins before sending trace data
from one storage stage to the next. Use the `-plugins` host image with its
`/lifecycle` executable, mount the carrier at `/plugins`, and configure the agent:

```text
--trace-sampling-enabled=true
--trace-sampling-trusted-dir=/plugins
```

These flags are independent of the data-node `trace-pipeline-*` flags. The
lifecycle executable and its `.so` files must satisfy the same build compatibility
requirements described above. Plugin execution defaults to a one-second timeout
per batch (`--trace-sampling-timeout=1s`); three consecutive timeouts open the
circuit (`--trace-sampling-circuit-break=3`) and retain subsequent batches.

Configure the group's `pipeline.stages[].plugins` chain for the **source** stage:
`stage: hot` controls sampling during hot-to-warm migration. The lifecycle agent
does not use top-level `pipeline.plugins` or `enabledEvents` to choose its chain.
`pipeline.enabled` and the schema selectors still apply. See
[Trace retention sampling](lifecycle.md#trace-retention-sampling) for a complete
schema example, API configuration, and recovery requirements.

Unlike data-node reconciliation, a lifecycle preflight failure (including a
missing or incompatible plugin, or a disabled lifecycle plugin host) stops the
affected migration before sending data. During sampling, a plugin error bypasses that link while other successful links
still apply; a timeout retains the affected batch. Completed sampling decisions are persisted
before replay; recovery can reuse those decisions without loading the plugin
again. Keep the agent's snapshot and progress storage across restarts. Delivery
remains at least once, so a crash can replay rows acknowledged before their checkpoint was persisted.

## Delivering the carrier (first-party) and third-party plugins

Each host uses exactly **one** trusted dir
(`-trace-pipeline-trusted-plugin-dir` for data nodes or
`--trace-sampling-trusted-dir` for lifecycle agents; every `SamplerPlugin.path`
resolves within its host's directory). First-party plugins mount at
`/plugins` (`SamplerPlugin.path=<name>.so`); third-party plugins mount at the
reserved subdirectory `/plugins/thirdparty`
(`SamplerPlugin.path=thirdparty/<name>.so`). Because the host image ships
`/plugins` **empty**, mounting the carrier there shadows nothing.

Two delivery mechanisms — both work for first- and third-party:

1. **OCI image volume** (Kubernetes ≥1.31, beta in 1.33): mount the carrier
   image's filesystem read-only at `/plugins`; mount a third-party plugin
   image as a *nested* volume at `/plugins/thirdparty`. No `cp`, no shell.
   See [`examples/kubernetes/plugins/first-party-image-volume.yaml`](../../examples/kubernetes/plugins/first-party-image-volume.yaml)
   and [`examples/kubernetes/plugins/third-party-image-volume.yaml`](../../examples/kubernetes/plugins/third-party-image-volume.yaml).
2. **initContainer + emptyDir** (portable to all Kubernetes versions): one
   shared `emptyDir` at `/plugins`; a first-party initContainer (the busybox
   carrier image) `cp`s its `.so` into `/plugins/`, and a third-party
   initContainer `cp`s into `/plugins/thirdparty/`. The carrier's busybox base
   provides the shell/`cp` this needs (a `scratch`/`.so`-only image cannot do
   it — use the image-volume mechanism for such images).
   See [`examples/kubernetes/plugins/first-party-init-container.yaml`](../../examples/kubernetes/plugins/first-party-init-container.yaml)
   and [`examples/kubernetes/plugins/third-party-init-container.yaml`](../../examples/kubernetes/plugins/third-party-init-container.yaml).

First-party parity is guaranteed by lockstep (deploy the carrier at the host's
tag). Third-party `.so` parity is the **operator's** responsibility — they
build against the pinned SDK + toolchain published for that release (this
repo's `go.mod` at that tag; `pkg/pipeline/sdk`). The loader rejects a skewed `.so`
(`plugin.Open` "different version of package …", or an `ABIVersion` mismatch).
On a data node, this fails open (ERROR log +
`sampler_load_failed` metric), and the affected group keeps its previous sampler
set. Other groups, and the node itself, are unaffected. On a lifecycle agent,
this fails migration preflight.

## Building and verifying plugins before deploying

- `plugins/README.md` covers the plugin contract, the ABI/toolchain lock, and
  `make build-plugins` (local, dev-only `.so` builds).
- `pkg/pipeline/sdk/sdktest` is the offline dev-toolkit: build fixtures, run
  `Decide` against a sampler (with a differential guard that flags a
  sampler reading a column it never projected), run a chain of samplers, and
  drive a real, loaded `.so` — all without a database or a cluster. See
  `plugins/skywalking/sw-trace-sampler/main_test.go` for a worked
  example.
- `docs/operation/plugins-debugging.md` is the symptom-driven runbook for
  "nothing is being sampled" / "unexpected data is being sampled".

## Licensing

The `-plugins` **host** image's runtime layer is
`gcr.io/distroless/base-debian12` (a minimal Debian "bookworm" base: glibc +
`ca-certificates` + a few runtime libs, with no shell, apt, or package
manager), which — unlike the default `busybox:stable-glibc` image — carries a
glibc runtime not present in the default image. Distroless is chosen over
`debian:bookworm-slim` specifically to minimize this standing CVE surface. The
**carrier** image is `busybox:stable-glibc` (the same base as the default
image, so no new base components) plus only the first-party `.so`. See
[`dist/NOTICE-plugins`](../../dist/NOTICE-plugins) for the addendum this adds
on top of the default image's `NOTICE`/`dist/LICENSE` (`make license-dep`),
and run `BINARYTYPE=plugins make -C banyand license-plugins-report` against a
built host image to regenerate its exact package manifest. Treat the
`-plugins` host image as carrying a standing glibc-CVE-tracking cost the
default image does not have, and scan/patch both plugin images accordingly.

## Helm

There is no Helm chart in this repository — see
[`docs/installation/kubernetes.md`](../installation/kubernetes.md). The
following wiring into the `apache/skywalking-banyandb-helm` chart is tracked
as a follow-up PR in that repository; the manifests in
`examples/kubernetes/plugins/` are the plain-Kubernetes reference in the
meantime:

- `plugins.enabled` → use the `-plugins` **host** image for the data-node
  container and set the two flags above.
- `plugins.image` → the **carrier** image (the `-plugins-carrier`-tagged image
  on the `apache/skywalking-banyandb` repo), mounted at `/plugins`. Its tag is
  the host image tag + `-carrier` (lockstep parity).
- `plugins.mountMode: imageVolume | initContainer` → selects the delivery
  mechanism.
- Optional third-party hooks mount an operator image onto the subdirectory
  `/plugins/thirdparty`.
- These chart settings target the **data-node** pod (liaison never hosts).
  Lifecycle-agent deployments need their own plugin image, mount, and
  `trace-sampling-*` flags as described above.
