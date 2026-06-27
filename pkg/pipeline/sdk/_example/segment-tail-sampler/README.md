# segment-tail-sampler (reference plugin)

A worked example of a BanyanDB post-trace sampler plugin built on
[`pkg/pipeline/sdk`](../../). It implements the Scenario 6.1 (`sw_trace`) sampler
from [`docs/design/post-trace-pipeline.md`](../../../../../docs/design/post-trace-pipeline.md)
and exists to show plugin authors three things:

1. **Read the proto config.** `SamplerPlugin.config` is a
   `google.protobuf.Struct`. The engine serializes it to canonical JSON and
   passes it to `NewSampler([]byte)`, which unmarshals it into a typed `config`.
2. **Declare a projection.** `Project()` returns only the tag columns the
   verdict reads (the error tag + each rule's tag key), and opts into the
   span-id column only when a span-count rule is configured. Span bodies are
   never read, so the merge raw fast path is preserved.
3. **Extract tags and spans.** `Decide` decodes tag values by their
   `pbv1.ValueType` (via `TagColumn.At`) and reads the span-id column
   (`TraceBlock.Len` / `SpanIDs`).

It is the `sampler` kind of the generic `Plugin`: because `sdk.Sampler` embeds
`sdk.Plugin`, the sampler also implements `Kind() sdk.Kind` (returning
`sdk.KindSampler`). In a `TracePipelineConfig` or `StageRule` it is one link in
an ordered `plugins` chain (a sequential pipe); a chain of samplers keeps a
trace only if every link keeps it.

## Keep logic

A trace is kept when **any** sure-keep rule matches; otherwise a deterministic
hash of `trace_id` admits a fraction of the healthy remainder:

1. `duration ≥ duration_threshold` (free from the intrinsic `MinTS`/`MaxTS`).
2. `keep_errors` and the error tag is truthy on any span.
3. any `keep_tag_rules` entry matches any span (`exists` / `equals` / `in` / `regex`).
4. `min_span_count` configured and the trace has at least that many spans.
5. else `hash(trace_id) < healthy_sample_rate`.

## Config

Set as the `config` `Struct` of a `sampler` plugin link in the
`TracePipelineConfig`'s `plugins` chain; shown here as the JSON the engine hands
to the plugin:

```json
{
  "duration_threshold": "0.500s",
  "keep_errors": true,
  "error_tag": "is_error",
  "healthy_sample_rate": 0.1,
  "keep_tag_rules": [
    { "tag_key": "db.type",  "equals": "PostgreSQL" },
    { "tag_key": "mq.queue", "equals": "queue-songs-ping" }
  ]
}
```

## Build

Build with the **same Go toolchain and the same pinned `pkg/pipeline/sdk`** as
the running data node (Go's `plugin` package requires it):

```sh
go build -buildmode=plugin -trimpath \
  -o segment-tail-sampler.so \
  ./pkg/pipeline/sdk/_example/segment-tail-sampler
```

Then place `segment-tail-sampler.so` in the data node's trusted plugin
directory and reference it by `path` in `SamplerPlugin`.
