# Trace-Pipeline Sampler Plugins

BanyanDB's trace pipeline can retain/drop traces in-merge (and at segment
finalization) using operator-installed native Go plugins ("samplers"). This
document covers packaging and deploying plugins on a running cluster. See
[`docs/design/post-trace-pipeline.md`](../design/post-trace-pipeline.md) for
the plugin ABI/contract, and
[`plugins/README.md`](../../plugins/README.md) for authoring a first-party
plugin. For diagnosing "nothing is being sampled" / "unexpected data is being
sampled" symptoms offline, see
[`docs/operation/plugins-debugging.md`](plugins-debugging.md).

## Overview

- Plugins are loaded **on demand at group-schema reconcile**, on the **data
  node role only** (the standalone service also satisfies this role; the
  liaison never hosts plugins).
- The feature is gated by two flags, both off by default:
  - `-trace-pipeline-native-plugin-enabled` (bool) — the whole feature switch.
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
| **plugins host** (`...:<tag>-plugins`) | `gcr.io/distroless/base-debian12` | `CGO_ENABLED=1` dynamic `banyand-server` + an **empty** `/plugins` | The plugin-capable host; `.so` are mounted in, not baked |
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
> `.so` (fail-open, loud; see below).

## Enabling plugins

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
      "name": "latency-status",
      "sampler": {
        "path": "latencystatussampler.so",
        "abiVersion": 1,
        "config": { "thresholdMs": 500, "successValue": "success" }
      }
    }
  ]
}
```

## Delivering the carrier (first-party) and third-party plugins

The loader supports exactly **one** trusted dir
(`-trace-pipeline-trusted-plugin-dir`, a single flag; every
`SamplerPlugin.path` resolves within it). First-party plugins mount at
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
repo's `go.mod` at that tag; `pkg/pipeline/sdk`). Mismatch is always safe and
loud: the loader rejects a skewed `.so` (`plugin.Open` "different version of
package …", or an `ABIVersion` mismatch), fails open (ERROR log +
`sampler_load_failed` metric), and the affected group simply keeps its
previous sampler set. Other groups, and the node itself, are unaffected.

## Building and verifying plugins before deploying

- `plugins/README.md` covers the plugin contract, the ABI/toolchain lock, and
  `make build-plugins` (local, dev-only `.so` builds).
- `pkg/pipeline/sdk/sdktest` is the offline dev-toolkit: build fixtures, run
  `Decide` against a sampler (with a differential guard that flags a
  sampler reading a column it never projected), run a chain of samplers, and
  drive a real, loaded `.so` — all without a database or a cluster. See
  `plugins/skywalking/latencystatussampler/main_test.go` for a worked
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
- Target the **data-node** pod only (liaison never hosts).
