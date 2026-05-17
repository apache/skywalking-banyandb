# G9f Throughout-Vec Measure Query — Operator Runbook

## What G9f does

G9f makes vec the sole production path for distributed measure queries. A
cluster running with `--measure-vectorized-enabled=true` (the cluster-wide
flag-on mode):

- Encodes every data-node response on `TopicInternalMeasureQuery` as a vec
  columnar binary frame body (4-byte magic `0x00 'V' 'F' 'R'`, version 1,
  uvarint nrows/ncols, per-column blocks). The leading `0x00` byte is a
  varint tag for protobuf field number 0 — it deterministically forces a
  flag-off node's `proto.Unmarshal` of the body into
  `*measurev1.InternalQueryResponse{}` to return a non-nil error, so a
  partial rollout fails loud instead of silently mis-decoding.
- Routes GroupBy + Agg + scalar reduce + raw GroupBy + distributed
  Top-over-Agg through `BatchAggregation` (Map on data nodes, Reduce on
  the liaison) with a vec-native `(shard, group)` replica dedup.

## What flag-off (the rollback) does

`--measure-vectorized-enabled=false` (the default) keeps the row path and
the proto wire body. Flag-off is the validated rollback path: the row
path is still exercised by the standalone test suite and by the row
distributed analyzer. A node that flips back to flag-off after sitting
flag-on continues to serve correctly.

## The hard-cutover contract

There is **one mode per cluster**. Every node — liaisons AND data nodes —
must agree on the flag. The G9f spec calls this Principle 3 (fail-loud
guard), and the codec is designed so that a botched partial rollout fails
loudly:

- A flag-on data node's raw frame body fed to a flag-off liaison →
  the liaison's `proto.Unmarshal` rejects the body with a decode error
  (the leading `0x00` is the engineered fail-loud guarantee).
- A flag-off data node's proto body fed to a flag-on liaison →
  the liaison's `RawFrameCodec.Unmarshal` rejects the body via the
  leading-byte magic check.

A nil/empty body is the legitimate empty-result carve-out and is NOT
magic-validated — sub.go sends a body-less SendResponse for an empty
distributed result.

## Boot-time announcement (the audit trail)

Every measure service announces its wire mode at PreRun:

```
G9f wire mode (...)
  measure_vectorized_enabled=<bool>
  measure_wire_mode_raw=<bool>
```

The three deployment shapes — standalone, distributed data node, distributed
liaison — all log this line. Grep for it during/after a rollout to confirm
every node landed on the intended mode:

```
journalctl -u banyandb-data | grep -F 'G9f wire mode'
journalctl -u banyandb-liaison | grep -F 'G9f wire mode'
```

Mixed values across the cluster ⇒ partial rollout ⇒ on-call action.

**Current state (G9f.5 complete):** all three services — standalone,
distributed data svc, distributed liaison — publish
`measure_wire_mode_raw` from `--measure-vectorized-enabled` at PreRun.
The data-node Rev (`banyand/query/processor.go::measureInternalQueryProcessor.Rev`)
detects flag-on + the vec MIterator's `RawFrameSource` capability and
emits the response as a raw columnar frame body (G9f.5.b's
`DrainPipelineToFrame`). The liaison's distributedPlan
(`pkg/query/logical/measure/measure_plan_distributed.go`) detects the
`[]byte` body shape and routes it through `ReduceFramesToInternalDataPoints`
(agg path) or `DecodeFramesToInternalDataPoints` (non-agg path), then
flows the decoded data points into the row-side
`pushedDownAggregatedIterator` / `sortableElements` so the existing
downstream merge surface is preserved.

The raw-frame path is engaged only when the iterator exposes
`RawFrameSource` — multi-group merger / hidden-tag wrapper / trace
queries fall through to the proto path inside `Rev`. Under flag-on,
those proto fall-throughs would then fail the liaison codec's bad-magic
guard; if you hit a query that triggers one of those wrappers under
flag-on, the runbook recommends turning the flag back off until the
follow-up gate (full vec-distributed-plan replacement) lands.

## Detecting a botched partial rollout

| Symptom                                                      | Likely cause                                          |
|--------------------------------------------------------------|-------------------------------------------------------|
| Liaison error log: `RawFrameCodec: invalid raw frame magic`  | Flag-on liaison received a proto body from a data node still flag-off |
| Liaison error log: `vectorized.measure.frame: bad magic`     | Same as above, surfaced one layer up by the decoder   |
| Liaison error log: `proto: cannot parse invalid wire-format` | Flag-off liaison received a vec raw frame from a data node already flag-on |
| Empty results where row path returns rows                    | A flag-off liaison silently parsed a 0x00-leading raw frame as a zero-row proto (this is the failure mode the magic byte exists to prevent — re-check the magic-error logs above; if absent, suspect a bug in the codec dispatcher) |

## Rollout procedure

1. Confirm the row path is healthy on the current cluster — run the soak
   suite under flag-off, watch the per-query latency / error / dedup
   metrics for the agreed observation window.
2. Drain query traffic from one liaison at a time, restart it with
   `--measure-vectorized-enabled=true`, return it to the pool. Watch its
   `G9f wire mode` log line and confirm no `RawFrameCodec` /
   `frame: bad magic` errors in the first 5 minutes of resumed traffic.
3. Once every liaison is flag-on, repeat (2) for the data nodes one at a
   time. The codec carve-out for empty bodies makes the in-flight
   transition between (1) and (2) safe: the liaison will accept either a
   nil empty body or a raw frame from already-converted data nodes, and
   the engineered fail-loud guard turns any genuine mis-pair into a
   loud error rather than a silent empty result.
4. Tag the rollout completion in the deploy log; soak for the agreed
   stability window; only then declare flag-on stable.

## Rollback procedure

1. Revert the cluster to `--measure-vectorized-enabled=false` in the same
   one-at-a-time order: liaisons first, then data nodes.
2. The row path is still the canonical fallback, so once the flag is
   off on every node the cluster returns to pre-G9f behaviour.
3. Capture the trigger for the rollback in a postmortem; per the G9f
   spec, every fail-loud error counted as a SEV ≥ 3 incident.

## Diagnostic dump

If you suspect a frame bug rather than a rollout skew:

- Capture the raw response bytes via the queue subscriber logs (the
  `failed to send query response` path includes the request body
  signature).
- Decode the first 16 bytes — bytes 0–3 are the magic `0x00 'V' 'F' 'R'`,
  byte 4 is the wire version, bytes 5+ are uvarints (`nrows`, `ncols`).
- The integration test
  `pkg/query/vectorized/measure/TestTopologyMatrix_WithTop` reproduces
  the full Map → Encode → Decode → Reduce → Top pipeline over a
  36-cell `(shards, replicas)` topology grid; it is the canonical
  smoke test to bisect against if the wire decode looks wrong.

## Related code

- `api/data/codec.go` — `RawFrameCodec`, `ProtoCodec`, dispatch on
  per-process wire mode.
- `pkg/query/vectorized/measure/frame/{encode,decode,validate}.go` —
  the columnar binary frame surface, plus the load-bearing
  `Validate Header` fail-loud preflight.
- `pkg/query/vectorized/measure/aggregation.go` plus
  `aggregation_reduce.go` — `BatchAggregation` in AggModeAll /
  AggModeMap / AggModeReduce, the `(shard, group)` replica dedup.
- `pkg/query/vectorized/measure/reduce.go` — `ReduceRawFrames`,
  `ReducePartialBatches`, `ApplyTopToReduce` — the liaison-side
  composer.
- `banyand/measure/svc_{standalone,data,liaison}.go` — boot-time
  announcement of the wire mode.
