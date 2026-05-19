# Measure Vec Flag-Off Rollback

This runbook documents the kill-switch for the vectorized distributed measure query path and explains how to roll back to the legacy row-path if a production incident requires it.

## Background

Since Phase 6 of the vectorized distributed plan (see `banyand/dquery/measure.go`), the routing decision in the distributed measure query handler is a single boolean:

```
data.MeasureWireModeRaw() == true  →  vecplan.AnalyzeDistributed  (production default)
data.MeasureWireModeRaw() == false →  logical_measure.DistributedAnalyze  (rollback path)
```

There is no longer a `useVecDistributedMeasurePlan` predicate or a per-shape gate. Every request shape (non-agg, Agg, OrderBy-by-index-rule, multi-group, Top, GroupBy, and combinations) goes through the vec plan when the flag is on.

## What the flag controls

`data.MeasureWireModeRaw()` reads a per-process atomic boolean set at startup by `data.SetMeasureWireModeRaw(true)`. When true, the data-node wire codec switches from `proto.Marshal`/`proto.Unmarshal` (`InternalQueryResponse`) to Apache Arrow record-batch frames. The liaison node's distributed handler then dispatches to `vecplan.AnalyzeDistributed` instead of the row-path `distributedPlan`.

## How to flip it off in a running cluster

The flag is set at process startup. To disable it:

1. Locate the BanyanDB node configuration (typically `bydb.yaml` or the Helm values file).
2. Set the measure wire mode to proto (row) mode. The exact configuration key is `measure.wireMode: proto` (or the equivalent environment variable `BYDB_MEASURE_WIRE_MODE=proto`).
3. Perform a rolling restart of liaison and data nodes. Because the flag controls per-process codec selection, both liaison and data nodes must run the same mode; a mixed-mode cluster will return decode errors.

After restart, verify with:

```bash
bydbctl measure query --name <name> --group <group> --limit 1
```

A successful response confirms the row path is active.

## Which code path takes over when off

When `data.MeasureWireModeRaw()` returns false, the liaison's `Rev` handler in `banyand/dquery/measure.go` calls:

```go
logical_measure.DistributedAnalyze(queryCriteria, schemas)
```

This returns a `distributedPlan` (in `pkg/query/logical/measure/measure_plan_distributed.go`) that:

- Broadcasts `InternalQueryRequest` to data nodes using `proto.Marshal` wire encoding.
- Receives `*measurev1.InternalQueryResponse` from each data node.
- Performs a k-way sort-merge via `sortedMIterator` using `sort.NewItemIter`.
- Deduplicates by `(Sid, Timestamp)` hash and version across replicas.

The `[]byte` raw-frame branch that previously existed in this code path was removed in Phase 6 — it was unreachable once the liaison always routes raw-mode requests through vec.

## Operational constraints of the flag-off path

- **Higher latency**: each data node serialises results to proto, sends over the wire, and the liaison deserialises before merge. Under the vec path, data stays in columnar Arrow format across the wire and into the merge.
- **No native multi-group merge**: the row-path `sortedMIterator` merges across data nodes but does not perform cross-group native columnar aggregation. Multi-group queries still work but via the proto round-trip.
- **No native Top-N merge**: Top-N is evaluated per data node; the liaison re-sorts and truncates. Under vec, Top-N is evaluated natively in the columnar pipeline.
- **Proto size overhead**: `InternalQueryResponse` encodes every data point with its full tag and field set in proto format, which is larger than Arrow record batches for wide schemas.

## When to use the rollback

Use the flag-off path only during incident response when the vec path produces incorrect results or panics that cannot be fixed with a hot patch. The vec path is the production default and has been validated by the full distributed integration suite. After the incident is resolved, re-enable raw wire mode and perform another rolling restart.

## Relevant source locations

| File | Purpose |
|---|---|
| `banyand/dquery/measure.go` | Liaison routing — branches on `data.MeasureWireModeRaw()` |
| `api/data/codec.go` | `SetMeasureWireModeRaw` / `MeasureWireModeRaw` definitions |
| `pkg/query/logical/measure/measure_plan_distributed.go` | Row-path distributed plan (`distributedPlan`) |
| `pkg/query/vectorized/measure/plan/` | Vec distributed plan (`vecplan.AnalyzeDistributed`) |
