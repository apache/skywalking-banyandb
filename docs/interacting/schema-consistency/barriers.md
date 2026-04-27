# Schema Barrier RPCs

`SchemaBarrierService` (proto package `banyandb.schema.v1`) lets a client block synchronously until the cluster's schema state matches a target. All three RPCs share the same shape: a request carrying the target plus a timeout, and a response carrying `bool applied` and a list of `NodeLaggard` entries when the call did not converge in time.

This page is a reference. For end-to-end workflows that combine barriers with the write and query gates, see [Scenarios](scenarios.md).

## SchemaKey

Several requests reference schemas by `SchemaKey`:

```protobuf
message SchemaKey {
  string kind  = 1;  // "measure", "stream", "trace", "property",
                     // "index_rule", "index_rule_binding", "group", "top_n_aggregation"
  string group = 2;  // group name; empty for kind="group"
  string name  = 3;  // resource name; empty for kind="group" if referring to the group itself
}
```

For a Group key, set `kind = "group"`, `name = "<group-name>"`, `group = ""`.

## NodeLaggard

When `applied == false`, the response carries one entry per data node that has not converged:

```protobuf
message NodeLaggard {
  string             node                  = 1;
  int64              current_mod_revision  = 2;  // populated by AwaitRevisionApplied
  repeated SchemaKey missing_keys          = 3;  // populated by AwaitSchemaApplied
  repeated SchemaKey still_present_keys    = 4;  // populated by AwaitSchemaDeleted
}
```

In standalone mode there is one `NodeLaggard` entry at most. In cluster mode (Phase 2) the list reports every laggard node.

## AwaitRevisionApplied

Block until every data node's cache observes `mod_revision >= min_revision`.

```protobuf
rpc AwaitRevisionApplied(AwaitRevisionAppliedRequest)
    returns (AwaitRevisionAppliedResponse);

message AwaitRevisionAppliedRequest {
  int64                     min_revision = 1;
  google.protobuf.Duration  timeout      = 2;
}

message AwaitRevisionAppliedResponse {
  bool                  applied  = 1;
  repeated NodeLaggard  laggards = 2;
}
```

### Semantics

- The server returns as soon as the watermark reaches `min_revision`. Polling is internal; the client gets one synchronous answer.
- On timeout the response has `applied = false` and lists every laggard node with its `current_mod_revision`. The call **never** returns indefinitely.
- A `min_revision` of `0` returns `applied = true` immediately.
- A nil or zero `timeout` falls back to a server default (5 s in the standalone implementation).

### Go example

```go
import (
    "context"
    "time"

    schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
    "google.golang.org/protobuf/types/known/durationpb"
)

resp, err := barrier.AwaitRevisionApplied(ctx, &schemav1.AwaitRevisionAppliedRequest{
    MinRevision: targetRev,
    Timeout:     durationpb.New(10 * time.Second),
})
if err != nil {
    return err  // gRPC-level error; not a timeout
}
if !resp.GetApplied() {
    for _, l := range resp.GetLaggards() {
        log.Printf("node %s at rev %d, target %d", l.GetNode(), l.GetCurrentModRevision(), targetRev)
    }
    return fmt.Errorf("schema did not propagate within timeout")
}
```

## AwaitSchemaApplied

Block until each requested key is present at or above its target revision on every node.

```protobuf
rpc AwaitSchemaApplied(AwaitSchemaAppliedRequest)
    returns (AwaitSchemaAppliedResponse);

message AwaitSchemaAppliedRequest {
  repeated SchemaKey         keys          = 1;
  repeated int64             min_revisions = 2;
  google.protobuf.Duration   timeout       = 3;
}

message AwaitSchemaAppliedResponse {
  bool                  applied  = 1;
  repeated NodeLaggard  laggards = 2;
}
```

### Semantics

- `keys` and `min_revisions` are **parallel arrays**: `min_revisions[i]` applies to `keys[i]`. If `min_revisions` is empty or shorter than `keys`, the missing entries default to `0` (meaning "any revision; just be present").
- `keys` is capped at **10 000** server-side. Exceeding the cap returns `InvalidArgument`. Chunk larger sets across multiple calls; merge `applied` results with logical AND and union laggards.
- On timeout, each laggard's `missing_keys` enumerates exactly the keys not yet at the target on that node.
- `applied = true` is returned only when every key on every node is at or above the target.

### Go example

```go
keys := []*schemav1.SchemaKey{
    {Kind: "measure", Group: "sw_metric", Name: "service_cpm_minute"},
    {Kind: "measure", Group: "sw_metric", Name: "service_resp_time"},
}
revs := []int64{firstRev, secondRev}

resp, err := barrier.AwaitSchemaApplied(ctx, &schemav1.AwaitSchemaAppliedRequest{
    Keys:         keys,
    MinRevisions: revs,
    Timeout:      durationpb.New(15 * time.Second),
})
if err != nil { return err }
if !resp.GetApplied() {
    for _, l := range resp.GetLaggards() {
        for _, k := range l.GetMissingKeys() {
            log.Printf("node %s missing %s/%s/%s", l.GetNode(), k.GetKind(), k.GetGroup(), k.GetName())
        }
    }
}
```

## AwaitSchemaDeleted

Block until each named key is **absent** from every node's cache.

```protobuf
rpc AwaitSchemaDeleted(AwaitSchemaDeletedRequest)
    returns (AwaitSchemaDeletedResponse);

message AwaitSchemaDeletedRequest {
  repeated SchemaKey         keys    = 1;
  google.protobuf.Duration   timeout = 2;
}

message AwaitSchemaDeletedResponse {
  bool                  applied  = 1;
  repeated NodeLaggard  laggards = 2;
}
```

### Semantics

- Same 10 000-key cap as `AwaitSchemaApplied`.
- On timeout each laggard's `still_present_keys` enumerates the keys the call was waiting to see disappear.
- This RPC reports tombstone propagation, not tombstone GC. A key that has tombstoned is "absent" for the purpose of this call even before the tombstone is physically removed by the GC loop.

### Go example

```go
resp, err := barrier.AwaitSchemaDeleted(ctx, &schemav1.AwaitSchemaDeletedRequest{
    Keys: []*schemav1.SchemaKey{
        {Kind: "measure", Group: "sw_metric", Name: "service_cpm_minute"},
    },
    Timeout: durationpb.New(10 * time.Second),
})
if err != nil { return err }
if !resp.GetApplied() {
    // Inspect resp.Laggards[i].StillPresentKeys
}
```

## Choosing the right barrier

| Situation | RPC |
|---|---|
| You just got a `mod_revision = R` back from a Create/Update and want every node to see R or later | `AwaitRevisionApplied(R)` |
| You created several schemas at once and need to confirm each one is visible | `AwaitSchemaApplied(keys, revs)` |
| You want to confirm a deleted schema is gone before re-creating | `AwaitSchemaDeleted(keys)` |
| You don't know the revisions but want presence | `AwaitSchemaApplied(keys, []int64{})` (zero-filled) |

## Common pitfalls

- **Setting an absurdly small timeout.** Even on a healthy cluster, schema propagation has measurable latency. Set timeouts in the seconds range, not milliseconds. The barrier is a synchronous safety net; treat it like one.
- **Forgetting that `min_revisions` is a parallel array.** If you pass three keys but only two revisions, the third key is gated on revision `0` (any revision). That is rarely what the caller meant.
- **Calling the barrier from the same context that issued the schema mutation, with a context deadline tighter than the timeout.** The context cancellation wins. Either propagate enough deadline budget or use a fresh context for the barrier.
- **Treating a non-error response with `applied = false` as an error.** It is a soft outcome — the client may want to act on the laggards (alert, fall back to the un-gated path) rather than abort.
