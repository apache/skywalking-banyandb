# Revision Gates

The barrier RPCs let clients **block** on propagation. The revision gates let clients **send a write or query that fails fast** when the schema view does not align. Both surfaces use the same `mod_revision` value the server returned on the originating Create/Update.

This page describes the three places a revision matters at request time:

1. The **write-path gate** (`Metadata.mod_revision` on `WriteRequest`)
2. The **query-path gate** (`group_mod_revisions` on `QueryRequest`)
3. The **time-range clamp** driven by `created_at`

It also covers the **tombstone invariant** that gates `Create` after a delete.

## Write-path gate

Stream and Measure write RPCs read `Metadata.mod_revision` on each `WriteRequest` and compare it against the cached schema revision for that group/name.

| Client `mod_revision` | Cache state | Server response status |
|---|---|---|
| `0` | any | `STATUS_SUCCEED` — gate skipped |
| `== cache.ModRevision` | match | `STATUS_SUCCEED` — write proceeds |
| `< cache.ModRevision` | client behind | `STATUS_EXPIRED_SCHEMA` — fail fast, refresh schema, retry |
| `> cache.ModRevision` | client ahead | held until `WaitForSchema` budget elapses, then `STATUS_SCHEMA_NOT_APPLIED` |

The "ahead" branch lets the server absorb a brief propagation lag without surfacing a transient error to the client. The hold budget is server-configurable; the default is small (sub-second) so a write either commits quickly or returns promptly with `STATUS_SCHEMA_NOT_APPLIED`.

### When to use it

- A client wrote a schema update, then writes a data point that depends on the update having applied. Pass the new `mod_revision` to the write so the server can confirm it landed against the right schema.
- A client retries an idempotent write across a schema change. If the schema advances during retry, the gate flags the stale request rather than silently writing against the wrong shape.

### Sample

```go
import (
    commonv1  "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
    measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
)

writeClient.Send(&measurev1.WriteRequest{
    Metadata: &commonv1.Metadata{
        Group:       "sw_metric",
        Name:        "service_cpm_minute",
        ModRevision: knownRev,  // the value returned by Update / Get
    },
    DataPoint: dp,
    MessageId: nextMessageID(),
})
```

## Query-path gate

Stream, Measure, and Trace `QueryRequest` messages accept a `map<string, int64> group_mod_revisions` (field 50). Each entry maps a group from the `groups` list to the `mod_revision` the client last observed for that group. The matching `QueryResponse.group_statuses` map (field 50) reports the gate outcome per group.

| Map state | Server behavior |
|---|---|
| Empty map | Gate skipped for the entire query — backward-compatible default. |
| Group present, value `0` | Gate skipped for that group. |
| Group present, value matches cache | `group_statuses[group] = STATUS_SUCCEED` |
| Group present, value below cache | `group_statuses[group] = STATUS_EXPIRED_SCHEMA`; query short-circuits, `elements` is empty |
| Group present, value above cache | held until the wait budget; on expiry `STATUS_SCHEMA_NOT_APPLIED` |
| Group absent from map (but present in `groups`) | Gate skipped for that group; partial-coverage opt-in |

The per-group structure exists because multi-group queries should not be forced to take `max(rev_a, rev_b)` and block on the slowest group; each group is gated independently. See [Scenarios](scenarios.md#5-multi-group-cross-cutting-query) for a worked example.

### Sample

```go
import streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"

resp, err := streamClient.Query(ctx, &streamv1.QueryRequest{
    Groups: []string{"sw_metric", "sw_metric_minute"},
    Name:   "service_resp_time",
    GroupModRevisions: map[string]int64{
        "sw_metric":        rev1,
        "sw_metric_minute": rev2,
    },
    TimeRange:  trange,
    Projection: proj,
})
if err != nil { return err }

for group, status := range resp.GetGroupStatuses() {
    if status != modelv1.Status_STATUS_SUCCEED {
        log.Printf("group %s gate: %s", group, status)
    }
}
```

## Time-range clamp

Queries whose `time_range.start` falls before a schema's `created_at` are silently clamped:

```
effective_start = max(request.time_range.start, schema.created_at)
```

For a multi-group query, the server uses the **maximum** `created_at` across the queried groups, so the result set never reaches into a window before any of the participating schemas existed.

| Range relative to `created_at` | Result |
|---|---|
| `time_range.end < created_at` | Empty result set, no error |
| `time_range.start < created_at <= time_range.end` | Window is clipped; only data on or after `created_at` is returned |
| `time_range.start >= created_at` | Untouched; full window honored |
| `created_at == nil` (schema predates v0.11.0) | No-op; clamp does not fire |

The clamp is **opt-in** by virtue of `group_mod_revisions`. If a query does not specify any per-group revision, the server does not invoke the clamp at all — pre-existing queries that do not yet know about the gate continue to behave as before. This protects bydbctl and other tools that may issue queries without schema awareness.

### When the clamp matters

- A schema was deleted and recreated with the same name (delete-then-recreate). A query that asks for the previous month must not return data from before the new schema's `created_at`. The clamp guarantees this without requiring the client to know about the delete.
- A new measure was added today. A dashboard that asks for "last 7 days" gets data from today onward; no fabricated zeroes for the six days before the schema existed.

## Tombstone invariant for re-create

When a schema is deleted, the server retains a tombstone with `delete_time` (Unix nanoseconds). A subsequent `Create` for the same key is rejected with gRPC status `codes.InvalidArgument` and message `"updated_at_before_tombstone"` if:

```
incoming_schema.updated_at <= tombstone.delete_time
```

This prevents a replayed or stale `Create` from silently overwriting a more-recent delete.

### How to satisfy it

Client code should derive `UpdatedAt` from the wall clock at submission time. As long as the recreate is genuinely subsequent to the delete, the invariant holds. If a client batches operations, it must ensure timestamps advance — for example by re-stamping the spec right before the `Create` rather than reusing a captured value from earlier.

### How to recover

If the server returns `InvalidArgument: updated_at_before_tombstone`:

1. Re-stamp the spec's `UpdatedAt` to `time.Now()` (or any value strictly greater than the tombstone's `delete_time`).
2. Re-issue the Create.

The error indicates a clock or batching issue; it is not retried automatically because retrying with the same payload would loop indefinitely.

## Pitfalls

- **Threading `mod_revision` through retries.** A retry that captured the original `mod_revision` may find the schema has advanced. Re-fetch the spec (or the most recent `mod_revision` from a registry sync) on each retry rather than reusing a stale value.
- **Mixing gated and ungated traffic.** A workflow that gates writes but issues ungated queries (`group_mod_revisions` empty) loses the clamp. If the workflow depends on the clamp for correctness, it must populate per-group revisions even when it does not care about the gate's three-way split.
- **Treating `STATUS_EXPIRED_SCHEMA` and `STATUS_SCHEMA_NOT_APPLIED` as the same.** They are opposites: expired ⇒ refresh schema, not-applied ⇒ wait or call `AwaitRevisionApplied`. Lumping them into a single error path masks real misconfiguration.
- **Forgetting that the clamp is opt-in.** Tools such as bydbctl issue queries without `group_mod_revisions`. Their results are not clamped; an integration test that depends on the clamp must populate the map.
