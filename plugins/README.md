# Trace-pipeline sampler plugins

This directory is the in-repo source home for SkyWalking-maintained
post-trace-pipeline sampler plugins. Every plugin here is a first-class Go
package: it participates in `go build ./...`, `go vet`, `golangci-lint`, and
the license-header check, and it is compiled into the `-plugins` variant of
the `banyand` server image (see `docs/operation/plugins.md`).

```
plugins/
  README.md                             # this file
  skywalking/
    internal/tracesampler/              # shared §6.1/§6.2 sampler engine (not a plugin)
    sw-trace-sampler/main.go            # segment schema (sw_trace): duration/is_error/tags rules
    zipkin-trace-sampler/main.go        # zipkin_span schema (sw_zipkinTrace): duration/query rules
```

`internal/tracesampler` is a shared library, not a plugin: it has no `main.go`,
so `make build-plugins` (which only builds `plugins/*/*/` dirs containing a
`main.go`) skips it, and the `sw-trace-sampler` / `zipkin-trace-sampler` mains
each compile it into their own `.so`. The two mains differ only in a `Schema`
value naming the columns each trace schema keeps the rule inputs in — see
[First-party sampler config](#first-party-sampler-config) for that mapping and
the keys both plugins accept.

Because every `keepTagRules` entry resolves to the flattened array, a rule naming
a first-class column could never match. Each `Schema` therefore carries its
model's full column inventory (`FirstClassColumns`) and the engine rejects such a
rule at construction instead of letting it silently never fire — so
`{tagKey: service_id, equals: ...}`, the natural way to write "keep everything
from the payment service", fails loudly rather than dropping exactly those traces.

The `skywalking/` namespace holds plugins maintained by the SkyWalking
project itself. A future third-party vendor would get its own sibling
namespace (e.g. `plugins/acme/`) — but see the "Third-party plugins" section
below: this repo only *documents* and *builds* first-party plugins; a
third-party `.so` is built and shipped by its own vendor, not committed here.

This is only the **source** layout. At runtime, the first-party `.so` files
built here are shipped in a carrier image (the `-plugins-carrier`-tagged image
on the same `apache/skywalking-banyandb` repo) and **mounted** into the
plugin-capable `-plugins` host image at `/plugins` (the trusted plugin
directory, empty in the host image); third-party `.so` files are mounted at
the reserved subdirectory `/plugins/thirdparty` — see
`docs/operation/plugins.md`.

## The plugin contract

A plugin is a `package main` built with `go build -buildmode=plugin` that
exports exactly two symbols, defined by `pkg/pipeline/sdk`:

```go
var  ABIVersion = sdk.ABIVersion             // re-export, unchanged
func NewSampler(config []byte) (sdk.Sampler, error)
func main() {}                               // required by -buildmode=plugin
```

- `ABIVersion` must equal the host's compiled `sdk.ABIVersion` *exactly*. A
  mismatch is a fail-fast load error (never silent corruption).
- `NewSampler` is the constructor the engine looks up by default; a
  `SamplerPlugin.symbol` in the group's pipeline config can override the
  symbol name for a plugin package that exports more than one sampler.
- `config` is the canonical JSON encoding of the `SamplerPlugin.config`
  `google.protobuf.Struct`; the plugin unmarshals it into its own typed
  config.
- The `sdk.Sampler` interface (`Kind`, `Project`, `Decide`, `Close`) and the
  vectorized `sdk.TraceBatch`/`sdk.TraceBlock` types are the full contract —
  see the package doc on `pkg/pipeline/sdk` for the authoritative reference.

## First-party sampler config

`sw-trace-sampler` and `zipkin-trace-sampler` share one engine
(`internal/tracesampler`) and accept the same keys. This is the behavioral
reference; for `bydb.yml` wiring, the `SW_STORAGE_BANYANDB_*` environment
overrides, and tuning guidance, see SkyWalking's
[BanyanDB storage guide](https://skywalking.apache.org/docs/main/next/en/setup/backend/storages/banyandb/).

| Key | Type | Meaning |
|---|---|---|
| `durationThresholdMs` | int | Keep a trace whose end-to-end envelope reaches this many **milliseconds**. `0` disables. |
| `keepErrors` | bool | Keep a trace carrying an error. Rejected on a schema that expresses error neither as a column nor as an array key. On the Zipkin schema see the truncation caveat below. |
| `errorTag` | string | Override the tag `keepErrors` reads. Ignored unless `keepErrors` is set. |
| `keepTagRules` | list \| string | Sure-keep rules over the searchable tags: `{tagKey, exists\|equals\|in\|regex}`, or the compact `key=value,key=~regex,key` string form. |
| `healthySampleRate` | float | Fraction in `[0,1]` of the remaining traces to keep, by FNV-1a hash of the trace ID. Accepts a quoted number, since SkyWalking's config loader stringifies float placeholders. |

**Unknown keys are rejected.** Every option is a keep rule, so a key that
silently missed would leave a sampler with no rules that drops the whole group —
the failure mode is data loss, not a stale setting. A wholly empty config (`{}`,
which `pipeline_loader` also substitutes for an absent `SamplerPlugin.config`) is
rejected for the same reason. Note Go matches field names case-insensitively, so
this catches wrong words, not wrong capitalization; the reference
`_example/segment-tail-sampler` uses `snake_case`, so a config copied from it is
refused outright.

**Zipkin `keepErrors` misses long error values.** OAP's `SpanForward` skips both
the bare key and `key=value` when either the value or the joined string exceeds
`Tag.TAG_LENGTH` (256 chars), so an `error` tag carrying a long exception message
reaches `query` in neither form and `keepErrors` cannot see it — the loudest
errors are the ones that go missing. Until OAP records the bare key before that
length check, catch those with a `keepTagRules` entry on a short-valued tag, such
as an `http.status_code` regex.

**Per-schema inputs.** The two mains differ only in this mapping:

| Input | `sw-trace-sampler` | `zipkin-trace-sampler` |
|---|---|---|
| Searchable tags | `tags` | `query` |
| Error signal | `is_error` column | `error` key inside `query` |
| Per-row start | `start_time` | `timestamp_millis` |
| Per-row duration | `latency` (ms) | `duration` (µs) |

Both start columns are BanyanDB timestamp columns, so both arrive in
**nanoseconds** whatever the tag is called; only the duration unit differs, and
`Schema.DurationTagNanosPerUnit` normalizes it. `durationThresholdMs` is
milliseconds on both.

**Duration is an envelope, not a per-span test:**

```
envelope = max(start + duration) − min(start)   over every row of the trace
keep     if envelope ≥ durationThresholdMs
```

so a trace slow only through sequential spans is caught. It is deliberately not
the intrinsic `MaxTS − MinTS`, which is the spread of per-row *start* timestamps
(and `0` for a single-row trace).

**Order and fail-open.** Rules are OR-ed in the order duration, errors, tag
rules, healthy sample; the first match wins, so order affects cost only.

A rule that cannot be evaluated **keeps** the trace. An absent duration or
error column means "can't tell", not "not slow" / "no error": those columns
are schema-declared, so their absence implies the block was written under a
different schema — typically the wrong plugin attached to the group — and
dropping there would discard exactly what the option was enabled to save.

Two cases are deliberately NOT treated that way, because they are ordinary
data rather than a schema mismatch: an absent tag array (a trace with no
searchable tags simply matches no tag rule), and an error column that is
present but not truthy (an unset flag really does mean "not an error", unlike
an absent measurement). A row carrying a start but no duration still anchors
the envelope's left edge — Zipkin's `duration` is optional, so requiring both
would measure a trace from a later span and understate it.

## ABI / toolchain lock (read this before building for production)

A Go plugin is loaded via `plugin.Open`, which requires the `.so` and the
host process to share the **exact same**:

- Go toolchain version (this repo pins `go 1.25.13` via `go.mod`; `GOTOOLCHAIN=auto`
  resolves it identically in CI, in the `-plugins` Docker builder stage, and
  in a local `make build-plugins`),
- `pkg/pipeline/sdk` package build (and its full transitive module graph),
- CGO mode (`CGO_ENABLED=1`) and race-detector mode (`-race` or not),
- and a compatible libc (the `-plugins` runtime image is
  `gcr.io/distroless/base-debian12` and its builder stage is
  `golang:1.25-bookworm` — both Debian 12/bookworm, same glibc 2.36, for
  exactly this reason).

Any drift and `plugin.Open` rejects the `.so` outright ("different version of
package …"). This is by design: mismatch is always safe and loud, never
silent. For the first-party plugins in this directory, parity is guaranteed
by lockstep — the `-plugins` Dockerfile builder base compiles the host binary
and every plugin `.so` from the same commit in the same CI run, and the host
image (`skywalking-banyandb:<tag>-plugins`) and the carrier image
(`skywalking-banyandb:<tag>-plugins-carrier`, same repo) are published in
lockstep (see `docs/operation/plugins.md`). Always deploy the carrier at the
host's tag + `-carrier`; do not build a first-party `.so` on a different
machine/toolchain and expect it to load.

## Building locally

Requires a C toolchain (`gcc` or `clang`) because Go plugins require
`CGO_ENABLED=1`. Go plugins are not supported on Windows.

```sh
make build-plugins        # builds every plugins/<vendor>/<name>/main.go into
                           # $(PLUGIN_OUTPUT_DIR) (default build/bin/plugins)
```

This reuses the same `-trimpath` flags as `make build-trace-pipeline-server`
(root `Makefile`), matching the flags the `-plugins` Docker builder stage
uses, so a locally built `.so` loads into a locally built
`build-trace-pipeline-server` binary — but NOT necessarily into the released
`-plugins` image (which is built by its own Dockerfile stage; see the
toolchain-lock note above).

## Deploying (first-party)

Every plugin under this directory is built into the
`apache/skywalking-banyandb:<tag>-plugins-carrier` **carrier** image, which is
**mounted** at `/plugins` into the plugin-capable
`apache/skywalking-banyandb:<tag>-plugins` **host** image (whose `/plugins` is
empty). An operator enables plugin hosting on the **data node** (the role that
runs merge/finalize) with two flags:

```
-trace-pipeline-native-plugin-enabled=true
-trace-pipeline-trusted-plugin-dir=/plugins
```

then references a plugin by filename in the group's pipeline config, e.g.
`SamplerPlugin.path = "sw-trace-sampler.so"`. See
`docs/operation/plugins.md` for the full deployment guide (image-volume and
initContainer mount mechanisms, third-party plugins, and Kubernetes examples).

## Adding a new first-party plugin

1. Create `plugins/skywalking/<name>/main.go` implementing `sdk.Sampler`
   (`ABIVersion`, `NewSampler`, empty `main()`).
2. Add a `_test.go` using `pkg/pipeline/sdk/sdktest` to verify `Decide`
   offline (no `.so`, no cluster) before ever building a `.so` — see
   `pkg/pipeline/sdk/sdktest` and the seed plugin's test for the pattern.
3. `make build-plugins` locally to confirm it compiles as a plugin.
4. The carrier image picks up any new `plugins/*/*/main.go` automatically (the
   builder stage globs the directory) — no Dockerfile change needed for a new
   plugin, only for a new *toolchain* requirement.

## Third-party plugins

Third-party/operator plugins are **not** stored in this repository. An
operator builds their `.so` against the pinned SDK + toolchain published for
each release (see `docs/operation/plugins.md`) and mounts it at the reserved
subdirectory `/plugins/thirdparty`, keeping the first-party carrier mounted at
`/plugins`.
