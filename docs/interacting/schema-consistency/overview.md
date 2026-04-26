# Schema Consistency Overview

BanyanDB is an eventually-consistent system. Schema mutations (Create / Update / Delete) are accepted by the schema server, persisted, and then propagated to every data node's in-memory cache asynchronously. Most clients tolerate this lag, but request paths that depend on a freshly-changed schema — read-after-write, delete-then-recreate, multi-stage rollouts — need a way to **observe** propagation and **gate** their requests on it.

Phase 1 (shipped in [v0.11.0](../../../CHANGES.md)) adds the client-observable surface for that.

## What is new in v0.11.0

The new client surface falls into three groups.

### 1. Response fields — clients read them

| Field | Where | What it tells the client |
|---|---|---|
| `mod_revision` (`int64`) | `*ServiceCreateResponse`, `*ServiceUpdateResponse` for Group / IndexRule / IndexRuleBinding / TopNAggregation | The schema's revision after the call. Use this in subsequent gated writes/queries. |
| `delete_time` (`int64`, Unix ns) | every `*ServiceDeleteResponse` | The tombstone's persisted delete time. Required for tombstone-aware re-create. |
| `created_at` (`google.protobuf.Timestamp`) | Stream / Measure / Trace / Property / IndexRule / IndexRuleBinding / TopNAggregation / Group | The first-creation timestamp; preserved across updates. Drives the time-range clamp. |
| `group_statuses` (`map<string, Status>`) | Stream / Measure / Trace `QueryResponse` | Per-group outcome of the query gate. |

### 2. Request fields — clients send them

| Field | Where | What it does |
|---|---|---|
| `Metadata.mod_revision` (`int64`) | `WriteRequest` for Stream / Measure | Gates the write against the cached schema revision. `0` skips the gate. |
| `group_mod_revisions` (`map<string, int64>`) | Stream / Measure / Trace `QueryRequest` | Per-group gate. Empty map skips, `0` for a group skips that group. |

### 3. Barrier RPCs — clients block on propagation

`SchemaBarrierService` (proto: `banyandb.schema.v1.SchemaBarrierService`) exposes three RPCs:

| RPC | Use it to |
|---|---|
| `AwaitRevisionApplied(min_revision, timeout)` | Wait until every data node's cache observes `mod_revision >= min_revision`. |
| `AwaitSchemaApplied(keys, min_revisions, timeout)` | Wait until each named schema key is present at the requested revision on every node. |
| `AwaitSchemaDeleted(keys, timeout)` | Wait until each named schema key is absent from every node. |

See [Barrier RPCs](barriers.md) for the full reference.

## Status codes that may surface to clients

| Status | Code | Meaning |
|---|---|---|
| `STATUS_SUCCEED` | 1 | Gate passed. Continue. |
| `STATUS_EXPIRED_SCHEMA` | 7 | Client's `mod_revision` is **behind** the cache. Client is using a stale schema view; refresh and retry. |
| `STATUS_SCHEMA_NOT_APPLIED` | 10 | Client's `mod_revision` is **ahead** of the cache; the schema mutation has not yet propagated. Call `AwaitRevisionApplied` (or wait briefly) and retry. |

## Backward compatibility

Every gate is opt-in. A client that does not set `Metadata.mod_revision` (or sends `0`) and does not populate `QueryRequest.group_mod_revisions` (or sends `0` per group) sees exactly the prior behavior — no rejection, no clamp, no waiting.

The new response fields are additive on the wire; older clients that ignore unknown fields keep working unchanged. The only enum addition is `STATUS_SCHEMA_NOT_APPLIED = 10`. Strict-unmarshal clients that reject unknown enum values must be upgraded before deploying against a v0.11.0 server. See the migration appendix in [Scenarios](scenarios.md#migration-appendix) for details.

## When you need these tools

- You write a schema and immediately read or write data against it → gate the data write or query, or block on `AwaitSchemaApplied`.
- You delete a schema and recreate the same key → use `delete_time` and `AwaitSchemaDeleted` to confirm tombstone propagation, then satisfy the re-create invariant.
- You drive a fan-out workflow (dashboards, batch pipelines) that joins multiple groups with independent versioning → use `group_mod_revisions` per group.
- You bootstrap a large schema set and want to know when every node has caught up → call `AwaitRevisionApplied` once with the maximum revision.

When none of those apply (single-group, schema is created long before the writes/queries it gates), you can ignore the entire surface.

## Related documents

- [Barrier RPCs](barriers.md) — full reference for the three `SchemaBarrierService` calls
- [Revision Gates](revision-gates.md) — write/query gate semantics, time-range clamp, tombstone invariant
- [Scenarios](scenarios.md) — six worked workflows that combine the tools
- [API Reference](../../api-reference.md) — generated proto reference
- [Data Lifecycle](../data-lifecycle.md) — TTL-based data expiry (separate from schema GC)
