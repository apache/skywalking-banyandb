# Debugging Trace-Pipeline Sampler Plugins (Offline / Config)

This is the offline/config half of the trace-pipeline sampler-plugin
debugging runbook: everything here can be checked from configuration and
existing metrics/logs, or reproduced with the offline dev toolkit
(`pkg/pipeline/sdk/sdktest`) — no live cluster access is required beyond
reading its config and metrics.

> The live-observability half of this runbook (per-decision metrics, a
> decision-sample debug log, and a `banyand-plugin decide`/capture-replay
> workflow) is **not part of this document** — those tools are planned in a
> follow-up PR and do not exist yet. Do not search for metrics or commands
> beyond what is listed below; they are not yet built.

See [`docs/operation/plugins.md`](plugins.md) for deployment and
[`plugins/README.md`](../../plugins/README.md) for the plugin contract.

## Symptom: "my data is not being sampled" (nothing is dropped)

For data-node merge/finalization sampling, work through these checks in order.
For migration sampling, see [Lifecycle migration](#lifecycle-migration) below.

1. **Is the feature enabled on this node?** `-trace-pipeline-native-plugin-enabled`
   must be `true` and `-trace-pipeline-trusted-plugin-dir` must be set to a
   real, existing directory. Both are data-node flags; a liaison node never
   hosts plugins, so pointing plugins at a liaison silently does nothing.
2. **Is the pipeline enabled for this group?** The group's `TracePipelineConfig.enabled`
   must be `true`. Also check `enabled_events`: an empty `enabled_events`
   defaults the in-merge filter (`PIPELINE_EVENT_MERGE`) **on**, but does
   **not** default segment finalization (`PIPELINE_EVENT_FINALIZE`) on — if
   you expected finalization-time retention and didn't list it explicitly, it
   never runs.
3. **Were any samplers actually installed for this group?** Check the
   `sampler_active_count{group}` gauge. Zero means no sampler chain was ever
   built for this group — reconcile either never ran (config not applied) or
   every plugin failed to load (see the next check).
4. **Did a plugin fail to load?** Check `sampler_load_failed{group,name,reason}`
   and the data node's log for the ERROR line *"sampler plugin load failed;
   keeping previous good set (fail-open)"*. A load failure is fail-open and
   loud: the group keeps whatever sampler set it had before (or retains all
   traces, if this is the first-ever load) — it never crashes the node or
   silently corrupts data. Common `reason` values point at ABI/toolchain
   skew (see `plugins/README.md`'s ABI/toolchain-lock section — the most
   common cause is a third-party `.so` built with a different Go
   toolchain/SDK than the running host).
5. **Is the sampler chain actually deciding to keep everything?** This is
   where you leave the cluster and go offline: reproduce the sampler's
   config and representative trace shapes with
   `pkg/pipeline/sdk/sdktest.NewTrace(...)...Build()`, then run
   `sdktest.Run` (single sampler) or `sdktest.RunChain` (the configured
   chain) and inspect the returned `Verdict.Keep`. If every fixture you'd
   expect to be dropped comes back `true`, the sampler's own logic (or its
   config, e.g. a threshold set too permissively) is the cause, not the
   engine.
6. **Check which host owns the chain.** Data-node merge/finalization uses the
   top-level `pipeline.plugins`. Lifecycle migration uses
   `pipeline.stages[].plugins` for the source stage. The API accepts
   `schema_names`, `schema_name_regex`, and `stages`; the lifecycle agent applies
   the schema selectors when resolving its source-stage chain. A stage-only
   configuration does not install a data-node merge sampler.

## Symptom: "unexpected data is being sampled" (wrong traces kept or dropped)

1. **Suspect a column the sampler reads but never projects.** `Sampler.Decide`
   only ever sees the tag columns, span-ID column, and span-body column the
   sampler's own `Project()` declared — anything else decodes to an absent
   column (`TraceBlock.Tag` returns `nil`). If a sampler's logic quietly
   depends on an unprojected column when you hand-build a test fixture with
   every column present, it will behave differently in production (where the
   engine decodes only what was projected). Catch this **offline**:
   `sdktest.Run` runs `Decide` twice — once on the fixture as given, once on
   a copy trimmed to exactly `Project()`'s columns — and reports every trace
   ID where the two verdicts disagree in `Report.ProjectionDivergedIDs`. A
   non-empty result means this is very likely your root cause.
2. **Suspect a bypassed chain link.** A chain link that panics, returns an
   error, or returns a keep-mask of the wrong length is bypassed — the
   running keep-mask is left unchanged for that link (fail-open per link),
   and the data node logs a WARN line (*"sampler link failed; bypassing
   (retain)"* or *"sampler verdict length mismatch; bypassing (retain)"*).
   Reproduce the same samplers and fixture with `sdktest.RunChain` and
   inspect `ChainReport.Bypassed` — each entry names the link's index and a
   `Reason` (`decide_error`, `length_mismatch`, or `panic`), matching exactly
   what the engine's `sdk.EvaluateChain` (the shared implementation the
   engine's merge chain runs too — no separate re-implementation to drift
   from it) would have logged in production.
3. **Suspect the whole chain failed open.** A whole-chain timeout or an open
   circuit breaker retains **every** trace in the batch — a different
   failure mode from a single bypassed link (which only drops that one
   link's constraint). This is currently only visible via the data node's
   WARN/ERROR logs (`"timeout"` / `"circuit_open"`); no dedicated metric for
   it exists yet.
4. **Suspect cross-part/late-data assembly.** Retention decisions are made
   per merge, over whatever parts are being merged together at that moment;
   a trace whose spans arrive in multiple merges can be evaluated more than
   once as its span-set grows. This is a property of the merge grace window
   (`TracePipelineConfig.merge_grace`, default 2h) and how the merge
   scheduler groups parts, not of any single sampler. Destructive in-merge
   merge and finalization sampling use the resolved group `merge_grace` for both
   maturity and fragment-boundary range expansion. The fixed fallback is 2h when
   the group omits a positive value. Verify that the deployment's maximum delay
   between fragments of one trace fits within that grace.

## Lifecycle migration

For sampling while moving traces between storage stages, check the following:

1. Enable `--trace-sampling-enabled` on the lifecycle agent and set
   `--trace-sampling-trusted-dir` to the directory containing compatible `.so`
   files. The data-node plugin flags do not enable the lifecycle host. A missing
   or incompatible plugin fails migration preflight; inspect the agent's error
   log rather than the data-node sampler metrics.
2. Check `pipeline.enabled`, schema selectors, and the source-stage chain.
   For hot-to-warm migration, configure `pipeline.stages[].stage: hot`.
   Top-level `pipeline.plugins` and `enabled_events` do not select migration
   samplers. With no applicable source-stage chain, migration remains unsampled.
3. Inspect the lifecycle report's top-level `trace_sampling` entries for each
   artifact's `manifest` counts and fail-open reasons, plus `resume_count`.
   Subtract `kept_traces` from `traces` to calculate dropped traces. Migration assembles
   traces across snapshot parts within each segment/shard, and conservatively
   retains unsafe inputs such as oversized traces or possible segment-boundary
   fragments. Data-node per-merge behavior described above is a separate path.
4. After a crash, reuse the same progress and snapshot storage. Prepared sampling
   decisions are durable and can be replayed without loading the plugin again;
   changing plugin configuration does not resample those prepared decisions.
   Missing or inconsistent recovery artifacts stop migration instead of silently
   starting over. Unacknowledged delivery may be repeated after recovery.

See [Trace retention sampling](lifecycle.md#trace-retention-sampling) for the
configuration example, report fields, and recovery contract, and
[Lifecycle-agent hosting](plugins.md#lifecycle-agent-hosting) for plugin packaging.
