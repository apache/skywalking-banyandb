# Scenarios

Six worked workflows that combine the new tools. Code samples are Go pseudocode against the BanyanDB Go client; adapt the calls to your language by following the proto definitions.

The scenarios assume the following short-form imports:

```go
import (
    commonv1   "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
    databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
    measurev1  "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
    modelv1    "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
    schemav1   "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
    "google.golang.org/protobuf/types/known/durationpb"
)
```

---

## 1. Read-your-writes after a fresh Create

**Problem:** A test or migration creates a measure and immediately writes data points. Without the gate, a brief propagation lag can cause the write to land before the data node has loaded the schema.

**Tools:** Create response → `mod_revision` → `AwaitRevisionApplied` → gated write.

```go
createResp, err := measureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
    Measure: spec,
})
if err != nil { return err }
rev := createResp.GetModRevision()  // non-zero in v0.11.0+

if _, err := barrier.AwaitRevisionApplied(ctx, &schemav1.AwaitRevisionAppliedRequest{
    MinRevision: rev,
    Timeout:     durationpb.New(10 * time.Second),
}); err != nil { return err }

writeClient.Send(&measurev1.WriteRequest{
    Metadata:  &commonv1.Metadata{Group: spec.Metadata.Group, Name: spec.Metadata.Name, ModRevision: rev},
    DataPoint: dp,
    MessageId: nextMessageID(),
})
```

**Why both barrier and gate?** The barrier is a synchronous "schema is here" check; the gate on the write protects against a race where the cache evicts and reloads the schema between the barrier and the write.

---

## 2. Schema update with dependent writes

**Problem:** A service updates a measure's tag list and then writes data points that use the new tag. Until propagation completes, writes that include the new tag should be held or rejected, not silently dropped.

**Tools:** Update response → new `mod_revision` → `AwaitRevisionApplied` → gated writes with retry on `STATUS_SCHEMA_NOT_APPLIED`.

```go
updateResp, err := measureRegClient.Update(ctx, &databasev1.MeasureRegistryServiceUpdateRequest{
    Measure: updatedSpec,
})
if err != nil { return err }
newRev := updateResp.GetModRevision()

// Best-effort barrier; the gate is the source of truth.
_, _ = barrier.AwaitRevisionApplied(ctx, &schemav1.AwaitRevisionAppliedRequest{
    MinRevision: newRev,
    Timeout:     durationpb.New(5 * time.Second),
})

for _, dp := range pendingPoints {
    status := writeOne(ctx, dp, newRev)
    switch status {
    case modelv1.Status_STATUS_SUCCEED:
        // ok
    case modelv1.Status_STATUS_SCHEMA_NOT_APPLIED:
        // Cache caught up while the call was held but did not converge in budget.
        // Sleep briefly or call AwaitRevisionApplied again, then retry.
        retry(dp, newRev)
    case modelv1.Status_STATUS_EXPIRED_SCHEMA:
        // The schema has already advanced past newRev. Re-read the spec.
        return refreshAndRetry()
    }
}
```

**Pitfall:** A retry that reuses `newRev` after another concurrent update returns `STATUS_EXPIRED_SCHEMA`. Always reload the spec (or the latest `mod_revision`) before retrying past one cycle.

---

## 3. Delete-then-recreate (idempotent shape)

**Problem:** A reconciliation loop deletes a measure and recreates it with the same shape. The recreate must happen after the tombstone has propagated, and must satisfy the tombstone invariant.

**Tools:** Delete response → `delete_time` → `AwaitSchemaDeleted` → Create with `updated_at > delete_time`.

```go
delResp, err := measureRegClient.Delete(ctx, &databasev1.MeasureRegistryServiceDeleteRequest{
    Metadata: meta,
})
if err != nil { return err }
delTime := delResp.GetDeleteTime()  // Unix nanoseconds

if _, err := barrier.AwaitSchemaDeleted(ctx, &schemav1.AwaitSchemaDeletedRequest{
    Keys:    []*schemav1.SchemaKey{{Kind: "measure", Group: meta.Group, Name: meta.Name}},
    Timeout: durationpb.New(10 * time.Second),
}); err != nil { return err }

// Re-stamp UpdatedAt strictly after the tombstone's delete_time.
freshSpec := proto.Clone(originalSpec).(*databasev1.Measure)
freshSpec.UpdatedAt = timestamppb.Now()  // wall clock is past delTime by construction

if _, err := measureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
    Measure: freshSpec,
}); err != nil { return err }
```

**Pitfall:** Reusing `originalSpec` without re-stamping `UpdatedAt` can fail with `InvalidArgument: updated_at_before_tombstone` if the captured timestamp predates the delete.

---

## 4. Shape-break (incompatible recreate)

**Problem:** A measure's shape needs to change in a way that is not a backward-compatible update (e.g., dropping an entity tag). The standard pattern is delete + recreate with the new shape, then resume writes against the new shape.

**Tools:** Same as scenario 3, plus the time-range clamp and the write gate to harden the cutover.

```go
// 1. Tear down the old shape.
delResp, _ := measureRegClient.Delete(ctx, &databasev1.MeasureRegistryServiceDeleteRequest{Metadata: meta})
_, _ = barrier.AwaitSchemaDeleted(ctx, &schemav1.AwaitSchemaDeletedRequest{
    Keys: []*schemav1.SchemaKey{{Kind: "measure", Group: meta.Group, Name: meta.Name}},
    Timeout: durationpb.New(10 * time.Second),
})

// 2. Stand up the new shape.
freshSpec.UpdatedAt = timestamppb.Now()
createResp, _ := measureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: freshSpec})
newRev := createResp.GetModRevision()
_, _ = barrier.AwaitRevisionApplied(ctx, &schemav1.AwaitRevisionAppliedRequest{
    MinRevision: newRev,
    Timeout:     durationpb.New(10 * time.Second),
})

// 3. Resume writes — the gate will reject any in-flight write that still carries the old revision.
writeNewShape(ctx, newRev)

// 4. Queries before the recreate's created_at return empty automatically (clamp);
//    queries after see only new-shape data.
queryResp, _ := measureClient.Query(ctx, &measurev1.QueryRequest{
    Groups: []string{meta.Group},
    Name:   meta.Name,
    GroupModRevisions: map[string]int64{meta.Group: newRev},
    TimeRange: trange,  // start may predate the recreate; the server clamps to created_at
    ...
})
```

The integration suite §6.8 (`shape-break: delete+apply new shape creates the new measure`) is the canonical regression for this pattern.

**Pitfall:** Issuing queries without `group_mod_revisions` opts out of the clamp. After a shape-break, ungated queries can return mixed-shape elements until TTL eventually expires the old data. Either gate the query or wait for TTL.

---

## 5. Multi-group cross-cutting query

**Problem:** A dashboard or batch pipeline reads from multiple groups in one `QueryRequest`. Each group has its own independent `mod_revision`, and a stale revision on one group should not block or corrupt the rest.

**Tools:** Per-group `group_mod_revisions` map + per-group `group_statuses` inspection.

```go
revs := map[string]int64{
    "sw_metric":        latestRev("sw_metric"),
    "sw_metric_minute": latestRev("sw_metric_minute"),
}

resp, err := measureClient.Query(ctx, &measurev1.QueryRequest{
    Groups:            []string{"sw_metric", "sw_metric_minute"},
    Name:              "service_resp_time",
    GroupModRevisions: revs,
    TimeRange:         trange,
    ...
})
if err != nil { return err }

for group, status := range resp.GetGroupStatuses() {
    switch status {
    case modelv1.Status_STATUS_SUCCEED:
        // group contributed normally
    case modelv1.Status_STATUS_EXPIRED_SCHEMA:
        log.Printf("group %s schema advanced; refresh and retry", group)
    case modelv1.Status_STATUS_SCHEMA_NOT_APPLIED:
        log.Printf("group %s lagging propagation", group)
    }
}
```

### Partial coverage

Omit a group from the map to leave it ungated. Useful when one group is known stable but another is being actively rolled out:

```go
GroupModRevisions: map[string]int64{
    "sw_metric": rev1,
    // "sw_metric_minute" intentionally absent — caller does not gate it
},
```

The §4.5.4 and §4.5.5 integration tests in `test/cases/schema/query_gate.go` exercise the mixed-status and partial-coverage variants.

**Pitfall:** Building `group_mod_revisions` from a single global watermark forces every group to wait for the slowest. Either populate per-group revisions or omit the slow group from the map entirely.

---

## 6. Bulk schema bootstrap

**Problem:** A migration creates many schemas at startup (dozens to thousands of measures across multiple groups). The caller wants to know when everything is visible before opening data ingest.

**Tools:** Many Creates → collect max revision → one `AwaitRevisionApplied(maxRev)` → optional `AwaitSchemaApplied` for per-key confirmation.

```go
var maxRev int64
keys := make([]*schemav1.SchemaKey, 0, len(specs))
revs := make([]int64, 0, len(specs))

for _, m := range specs {
    resp, err := measureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: m})
    if err != nil { return err }
    r := resp.GetModRevision()
    if r > maxRev { maxRev = r }
    keys = append(keys, &schemav1.SchemaKey{Kind: "measure", Group: m.Metadata.Group, Name: m.Metadata.Name})
    revs = append(revs, r)
}

// Coarse check: all schemas visible at any revision >= maxRev.
if _, err := barrier.AwaitRevisionApplied(ctx, &schemav1.AwaitRevisionAppliedRequest{
    MinRevision: maxRev,
    Timeout:     durationpb.New(30 * time.Second),
}); err != nil { return err }

// Optional fine check: each key at its own target revision (chunk if > 10000).
for _, batch := range chunk(keys, revs, 10000) {
    resp, err := barrier.AwaitSchemaApplied(ctx, &schemav1.AwaitSchemaAppliedRequest{
        Keys:         batch.keys,
        MinRevisions: batch.revs,
        Timeout:      durationpb.New(30 * time.Second),
    })
    if err != nil { return err }
    if !resp.GetApplied() {
        return fmt.Errorf("bulk bootstrap failed; %d laggards", len(resp.GetLaggards()))
    }
}

// Open ingest.
```

**Pitfall:** Calling `AwaitSchemaApplied` with more than 10 000 keys returns `InvalidArgument`. Chunk the keys; the server is intentionally bounded.

---

## Migration appendix

### Existing clients on `mod_revision = 0`

Clients that do not set `Metadata.mod_revision` (because they predate v0.11.0 or because their workload does not need the gate) keep working unchanged. Every gate is a no-op when the client value is zero.

### `STATUS_SCHEMA_NOT_APPLIED` enum value

This is a new enum value (code 10). Strict-unmarshal clients — those that throw on unknown enum values rather than treating them as the default — must be upgraded before deploying against a v0.11.0 server.

For the **Java OAP client** specifically: during the migration window, treat `STATUS_SCHEMA_NOT_APPLIED` the same as `STATUS_EXPIRED_SCHEMA` (refresh schema and retry). Once the client-side enum is updated, the two cases can be split and handled distinctly.

### Tombstone retention

The default tombstone retention is **7 days**, configurable via `--schema-server-tombstone-retention`. Existing tombstones become eligible for GC after `tombstone_retention` elapses from their `delete_time`. Operators on resource-constrained nodes can tune the value; the floor is 1 hour.

A re-create attempted after the tombstone is GC'd is no longer subject to the `updated_at_before_tombstone` invariant — there is no tombstone to compare against. This is the expected behavior; the invariant exists to prevent replays during the retention window, not as a permanent ordering check.

### Time-range clamp on pre-v0.11.0 schemas

Schemas created before v0.11.0 have `created_at == nil`. The clamp is a no-op for those schemas — queries return their full configured time range as before. Once such a schema is updated, `created_at` is **not** retroactively populated; only fresh Creates stamp it.
