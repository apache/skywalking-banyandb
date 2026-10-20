# Proto-Free BydbQL Measure Planning with a Reusable Plan Cache

Status: **partially implemented.** Step 3 (the local schema source, §5) ships in 0.12.0 as
`pkg/bydbql/schema_source.go` and `banyand/liaison/grpc/bydbql_schema_source.go`; the plan IR, the
plan cache and the remaining steps are not implemented.

This document specifies a restructuring of how the liaison turns a BydbQL measure query into an
executable request:

1. `pkg/bydbql` stops building request protobufs. It compiles a statement into a proto-free,
   schema-bound **plan**, and a separate **lowering** step renders that plan as the
   `measurev1.QueryRequest` the query services already accept.
2. The liaison resolves measure schemas from its **local, watch-fed schema view** instead of the
   cluster-wide metadata fan-out it uses today.
3. The compiled plan is **cached and reused** across requests, keyed by query text and
   invalidated by a schema-dependency set.

(2) removes the dominant per-request cost on its own, with no cache. It is therefore sequenced
first and measured before (3) is built (§9).

---

## 1. Motivation

A BydbQL measure query is recompiled from scratch on every request, and the compile is far more
expensive than it looks.

`bydbQLService.Query` (`banyand/liaison/grpc/bydbql.go:127-142`) caches only the *parse*: the
`preparedCache` is an LRU keyed by query text that holds the AST (`*bydbql.PreparedStatement`).
Everything after the parse runs again per request:

- `Transformer.TransformBound` calls `MeasureRegistry().GetMeasure(ctx, ...)` **once per queried
  group** (`pkg/bydbql/transformer.go:330`).
- That call is cached nowhere. It resolves through `banyand/metadata/client.go:406` to the
  property `SchemaRegistry.GetMeasure` (`banyand/metadata/schema/property/client.go:767`) →
  `getResource` (`:539`) → `getSchema` (`:486`) → `queryAndRepairSchemas` (`:438`) →
  `collectSchemas` (`:396`) → `broadcastAll`: **a gRPC schema query to every metadata node**,
  followed by `repairInconsistentNodes` (`:449`), which may issue *writes* back to lagging
  nodes. Property mode is the only supported registry mode (`client.go:174`), so this is the
  universal path, not a fallback.
- `SHOW TOP ... WHERE ...` fans out **three** times: twice in the transformer (the TopN rule at
  `transformer.go:712`, then its source measure at `:720`) and again in
  `banyand/dquery/topn.go:54`.

The prepared-statement cache thus optimizes away a ~220µs parse while leaving a multi-node RPC
fan-out on the hot path.

The schema knowledge that fan-out buys is then spent building a `measurev1.QueryRequest`, which
the next stage immediately takes apart again: `banyand/dquery/measure.go:80` hands it to
`AnalyzeDistributed` (`pkg/query/vectorized/measure/plan/distributed.go:155`), and data nodes
translate it into `model.MeasureQueryOptions` (`pkg/query/vectorized/measure/plan/analyzer.go:112`).
For the BydbQL path the first of those hops is in-process — `measureService.Query` publishes on
`queue.Local()` (`banyand/liaison/grpc/measure.go:489`) — so the proto exists purely as an
intermediate format.

The redundancy is directly visible: one hop after the transformer's fan-out, the same request
re-reads the same measure schema from local memory (`banyand/dquery/measure.go:181`).

## 2. Goals and non-goals

**Goals**

- `pkg/bydbql` produces a plan, not a request protobuf, for measure `SELECT` statements.
- The plan is compiled once per query template per schema state, and reused.
- Measure schema resolution stops fanning out across the metadata cluster per request.
- No change to any wire format, to data nodes, or to the result of any existing query (with the
  observable exceptions listed in §8).

**Non-goals**

- **Stream, trace and property.** They keep the existing transform-to-proto path. `TransformResult`
  continues to carry a proto for them.
- **TopN.** Its plan cannot be invalidated safely today (§7). It is deferred to a follow-up.
- **A plan-native wire format.** The plan is lowered to the existing proto at the liaison; data
  nodes are untouched. Eliminating the data-node-side re-analysis is separate work.
- **Replacing the query engine's planner.** The BydbQL plan is a liaison-side, compiled form of
  one statement. It does not target `model.MeasureQueryOptions` (`pkg/query/model/model.go:98`),
  which is a data-node struct carrying `index.Query`, series `Entities` and shard state — not a
  logical plan.

## 3. Current pipeline

```
BydbQL text
  └─ preparedCache.getOrPrepare(query)     bydbql.go:127   cached (AST), literal queries bypass
  └─ stmt.Bind(params)                     bydbql.go:138   per-request overlay; errors → InvalidArgument
  └─ transformer.TransformBound(...)       bydbql.go:142   per request; errors → Internal
       ├─ GetMeasure per group             transformer.go:330   cluster fan-out + read-repair
       ├─ time.Now() baked into time range transformer.go:351
       └─ builds *measurev1.QueryRequest
  └─ AuthorizeTransformedRequest(proto)    bydbql.go:147   type switch over request protos
  └─ protojson.Marshal(proto)              bydbql.go:160   debug log
  └─ measureSvc.Query(proto)               bydbql.go:179   → queue.Local() → dquery
```

The only production consumer of `bydbql.Transformer` is this path (`banyand/liaison/grpc/server.go:192`
and `bydbql.go`). `bydbctl` and the MCP tools use `ParseQuery` only.

## 4. Design

### 4.1 Three phases

The single `Transform` step splits into three, chosen so that **everything schema-dependent is
parameter-independent, and everything parameter-dependent is schema-independent**. That property
is what makes the middle artifact cacheable.

```
compile   schema resolution     → cached; keyed by query text + schema dependency set
  bind    parameters + now      → per request
   lower  build the request     → per request, before authorization
```

**Compile** does all schema-dependent work, once: resolves each projected column to tag family +
tag or to a field (`convertTagAndField`, `transformer.go:1382`); validates GROUP BY, aggregation
and TOP against the schema (`:746`, `:846`, `:1654`); resolves ORDER BY; and, for every
comparison leaf in the WHERE tree, records the tag's family, name and `TagType`. It never reads a
parameter value and never reads the clock. It also records the schema dependency set (§6).

**Bind** substitutes parameter values from the per-request overlay (`resolvedParam`,
`pkg/bydbql/prepared.go`) and evaluates the time range against a supplied `now`. Value coercion is
type-directed by the `TagType` recorded at compile time — `setGrammarConditionValue`
(`transformer.go:1230`) takes no schema input other than `tagSpec.tag.Type`, so no schema access is
needed here.

**Lower** renders the bound plan as `*measurev1.QueryRequest`. It is the only code in the package
that constructs a request message.

### 4.2 The plan

A `MeasurePlan` is immutable after compile and safe to share across concurrent requests. It holds:

- groups (in statement order, duplicates preserved), measure name, stages, trace flag;
- the resolved tag projection (reusing `pkg/query/model.TagProjection`, `model.go:48`) and field
  projection;
- resolved GROUP BY, aggregation, TOP and ORDER BY specs;
- **count slots** for LIMIT, OFFSET and TOP N — each either a literal or a placeholder index;
- a **criteria tree** whose leaves carry `{name, binOp, family, tagType, wasSingle, values}`, where
  each value is either a literal or a placeholder reference;
- a **time spec** (§4.3);
- the schema dependency set (§6).

Schema-level enums and value types (`databasev1.TagType`, `modelv1.Sort`,
`modelv1.AggregationFunction`) are used as-is rather than mirrored into parallel declarations: they
describe data, not requests. The boundary this design moves is the *request message*.

### 4.3 What must stay per-request

Two things cannot be resolved at compile time, and both are easy to get subtly wrong.

**Value-list arity.** Array parameters expand in place inside `IN`, `MATCH` and `HAVING` lists,
and three sites branch on the resulting length:

- MATCH, `transformer.go:1061`: one value lowers to `Str`, several to `StrArray`;
- HAVING, `transformer.go:1201-1209`: `wasSingle && len(values) == 1` lowers to a scalar, anything
  else to an array — *a different oneof arm for the same logical input*;
- the in-place binder, `binder.go:242-248` (`collectSingleOrArray`), which applies the same
  `wasSingle && len == 1` rule.

The leaf must therefore carry `wasSingle` — whether the source was written as a single value or as
a parenthesized list — explicitly. `prepared.go:walkValueList` only numbers placeholder slots and
does not record it. Losing it silently collapses `HAVING x (?)` bound to a one-element array into
`HAVING x ?`.

**The whole time range — not only relative bounds.** It is tempting to treat only `'-30m'` and
`'now'` as clock-dependent. But `convertTimeRange` (`transformer.go:1313`) sets `end = now` for
`TIME >` and `TIME >=`, so even `TIME > '2024-01-01T00:00:00Z'` depends on the wall clock. `<` and
`<=` set `begin = defaultBeginTime` (a constant), and `=` sets `begin == end`.

The time spec is therefore `{op, begin, end}` with each bound drawn from
`{absolute, relative, now, epoch, placeholder}`, resolved against `now` at bind time, then
truncated to milliseconds (`transformer.go:1357`). It must **not** be implemented as "cache the
resolved `TimeRange` and patch `End`": that is wrong for `=`, and for any relative `begin`.

### 4.4 Lowering happens before authorization

Three downstream couplings accept a request proto and nothing else, and all three stay untouched
if lowering precedes them:

- **Authorization.** `AuthorizeTransformedRequest` (`bydbql.go:147`) → `RequestScopes(ScopeRepeatedGroups, ...)`
  → `rbac_extractors.go:107-130`, a concrete type switch over the five request protos whose
  `default:` resolves to `DecisionInvalidRequest`. **Passing it a plan would deny every measure
  query.** The 11-case contract test at `rbac_data_contract_test.go:835-870` pins this.
- **Debug logging**, `protojson.Marshal` at `bydbql.go:160`.
- **Dispatch**, the type assertions at `bydbql.go:171-203`.

The per-request order is therefore today's order, with lowering supplying the proto the
transformer used to build: *prepare → bind → (plan cache) → lower → authorize → dispatch*.

## 5. Schema source: the fan-out is removable without a cache

The liaison already holds every measure schema in memory. `entityRepo.measureMap`
(`banyand/liaison/grpc/discovery.go:255`) is populated from schema watch events with the complete
spec (`OnAddOrUpdate`, stored at `:323`, removed on delete at `:368`) and read by `getMeasure`
(`:404`). It is reachable from `bydbQLService` today: `measureService` embeds `*discoveryService`
(`measure.go:64`), which owns `entityRepo` (`discovery.go:46`).

Compile reads measure schemas from this view through a narrow interface:

```go
// MeasureSchemaSource resolves the measure schemas a compile needs.
type MeasureSchemaSource interface {
	// Measure returns the schema for (group, name). local reports whether it
	// came from the liaison's watch-fed view rather than a registry fallback.
	Measure(ctx context.Context, group, name string) (m *databasev1.Measure, local bool, err error)
}
```

`pkg/bydbql` defines the interface; the liaison adapts `entityRepo.getMeasure`. When no source is
supplied, `Transformer` falls back to today's `metadata.Repo` path, so `pkg/bydbql` stays free of
any `banyand/liaison` dependency and existing callers (tests, fixtures) are unaffected.

On a local miss — typically the window just after a measure is created, before the watch event
arrives — the adapter falls back to `MeasureRegistry().GetMeasure` and reports `local == false`.
**A compile that used the fallback is never cached** (§6.3).

This changes *which* view of the schema a BydbQL compile sees: today the quorum-freshest copy,
afterward the liaison's watch-fed copy. That is the same view the execution already uses one hop
later (`dquery/measure.go:181`), so compile and execution come to agree on the schema rather than
potentially disagreeing during a rollout.

This step alone removes the fan-out for **every** measure query — cached or not, parameterized or
literal. Its effect should be measured before the cache is built; if it removes the bulk of the
cost, the cache's remaining value is compile CPU, and its scope can be reconsidered.

## 6. Plan cache and invalidation

### 6.1 A separate cache

The plan cache is a new `planCache`, not an extension of `preparedCache`
(`banyand/liaison/grpc/bydbql_cache.go`), for four reasons:

- `preparedCache`'s correctness rests on the parse depending only on the query text, so "entries
  never need invalidation" (`bydbql_cache.go:36-40`). A schema-bound plan breaks exactly that.
- `store` uses `ContainsOrAdd` (`bydbql_cache.go:186`) because concurrent misses produce identical
  values. Two concurrent plan compiles can bind to *different* schema states; `ContainsOrAdd` would
  silently keep whichever landed first, possibly the older. The plan cache needs `Add` plus an
  explicit `Remove` on staleness.
- An explicit `Remove` fires the LRU eviction callback, and `noteEvicted`
  (`bydbql_cache.go:92`) would then record every schema rollout as cache thrash — corrupting the
  re-parse signal that the top-K tracker's Space-Saving design (`bydbql.go:292` and following)
  exists to keep clean.
- Its metrics are parse-shaped (`hit | miss | bypass`). The plan cache needs its own
  `bydbqlPlanCacheTotal{hit | miss | stale | uncacheable}`, so a schema rollout reads as `stale`
  rather than as a miss storm.

The literal-query bypass stays in `preparedCache`, and the plan cache follows it: caching plans for
literal queries yields unique keys that evict useful templates. Literal queries still get the
fan-out removal from §5.

Concurrent misses on one key collapse through `singleflight` (`golang.org/x/sync`, already a
dependency at `go.mod:55`), so a hot template compiles once rather than once per in-flight request.

### 6.2 The dependency set

A cache entry is keyed by query text and tagged with a **dependency set** of
`(kind, group, name, revision)` tuples recorded during compile — one per schema object the compile
read. For a measure query that is the measure in every queried group.

Every group must be covered, not only the first. `convertTagAndField`
(`transformer.go:1409-1425`) merges all groups' tag specs into a single dictionary with
last-group-wins on a name collision, so when one measure name carries divergent schemas across
groups, the compiled coercion types depend on the whole set. (That merge behavior is preserved
as-is; changing it is a separate question, §10.) The dependency set is deduplicated;
`QueryRequest.Groups` is **not** — `IN default, default` preserves the duplicate today.

### 6.3 Validation on lookup

On a hit, each dependency's current revision is re-read before the plan is reused. Four rules make
this correct; the first design draft broke three of them.

1. **Snapshot before fetching.** The dependency's revision is read *before* its schema is fetched
   at compile time. Reading it after lets a concurrent change land in between and be tagged as
   already observed — a stale plan that validates as fresh.
2. **Compare with `!=`, not `>`.** `ModRevision` is not a logical counter: it is
   `time.Now().UnixNano()` taken on whichever node handled the write
   (`banyand/metadata/schema/property/client.go:790` on create, `:809` on update). Under clock skew a newer write can carry
   a *smaller* revision, and a `>` comparison would silently skip the invalidation.
3. **Absence invalidates.** `ResourceRevision` returns `(0, false)` for a resource it does not hold
   — including a deleted measure. Under a `>` rule, `0 > N` is false and **a deleted measure would
   serve its cached plan indefinitely**, where today the query fails loudly. Any `!ok` means stale.
4. **Read revisions through `resolveQueryGateRevision`** (`banyand/liaison/grpc/schema_revision_registry.go:84`),
   the helper the query gate already uses (`measure.go:449-454`), so the cache and the gate can
   never disagree about a node's revision. It also provides the `entityRepo` locator fallback when
   the registry does not track the kind.

In addition: a failed compile is never cached (as `preparedCache` already does for parse errors),
and a compile that read any schema through the registry fallback (`local == false`) is never
cached, since the local view has not yet caught up with what it read.

## 7. Why TopN is deferred

A TopN plan cannot be invalidated safely against any in-memory state the liaison has.

- `NodeRepoRegistry` cannot answer for `KindTopNAggregation`: the schema cache returns `(0, false)`
  for it explicitly (`pkg/schema/cache.go:686-689`), and `measureRegistryKinds`
  (`banyand/measure/metadata.go:59`) excludes it with a comment directing readers to the property
  `schemaCache` — which is unexported and unreachable from the liaison.
- The TopN rule's `sourceMeasure` may live in a different group from the query, and is only
  discoverable *after* the rule is fetched, so the dependency set cannot be known before the first
  fetch.

Caching TopN would need either a TTL — accepting staleness when a rule is repointed — or extending
`measureRegistryKinds` to track the kind. The follow-up should do the latter. TopN can still adopt
the proto-free compile/lower split without a cache, but gains nothing from it until then.

## 8. Compatibility

**Wire and data nodes.** No change. The plan is lowered to the same `measurev1.QueryRequest`
before authorization and dispatch.

**Observable changes.**

- **Status code for type-coercion errors (open, §10).** A string parameter bound against an `INT`
  tag fails today inside `Transform`, which `bydbql.go:142` maps to `codes.Internal`. Moving
  coercion into bind maps it to `codes.InvalidArgument` (`bydbql.go:138`). That is arguably a fix
  — the request *is* invalid — but clients may match on the code. If adopted, record it in
  `CHANGES.md`.
- **`SELECT *` projection order becomes deterministic.** `convertTagAndField` expands `SELECT *` by
  ranging Go maps (`transformer.go:1429`, `:1434`), and `convertGroupBy` does the same for group
  keys (`:829`, `:831`), so tag order varies *per request* today — which is why
  `binder_equivalence_test.go:457` needs `protocmp.SortRepeatedFields`. A cache would freeze one
  arbitrary order per liaison replica: stable locally, divergent across replicas. All four sites
  sort by name instead (§9, step 0). Results are unaffected; only the order of projected tags in
  the request and response changes.
- **Fewer metadata read-repairs.** The transformer's fan-out is one of the callers that
  opportunistically repairs lagging metadata nodes (`repairInconsistentNodes`,
  `schema/property/client.go:449`). Other readers still trigger it, so this is a reduction in
  frequency, not elimination — but it is a behavioral change in the metadata tier and should be
  called out in the release notes rather than discovered.

## 9. Implementation plan

Each step lands green on its own.

**Step 0 — Determinism** (standalone commit). Sort the four map-iteration sites in §8. Add
`pkg/bydbql/determinism_test.go` asserting identical `prototext` output across 100 runs of a
`SELECT *` query. Without this, "lowering is byte-identical to the transformer" is not a
well-defined claim.

**Step 1 — Golden corpus** (standalone commit, green by construction). The existing suites are not
the safety net they appear to be: `pkg/bydbql/bydbql_test.go` (3888 lines, 226 specs) is a pure
parser suite with **no** transformer calls, and direct transformer coverage is about eight calls
in `binder_equivalence_test.go` plus three error-path calls in `prepared_internal_test.go`. Add
`pkg/bydbql/transform_golden_test.go`, table-driven over the 104 `.ql` files in
`test/cases/measure/data/input/`, against a static fake schema and a fixed `now`. Land it against
the **current** transformer: it is the fixed point the refactor is measured against.

**Step 2 — Injectable clock.** Add a `now` field to `transformRun`, set once per transform, and
replace the four `time.Now()` calls (`transformer.go:265`, `:351`, `:436`, `:647`). Prerequisite
for step 1's time coverage and for the bind phase.

**Step 3 — Local schema source.** Add `pkg/bydbql/schema_source.go` (§5) and
`banyand/liaison/grpc/bydbql_schema_source.go` adapting `entityRepo.getMeasure` with registry
fallback; wire it at `server.go:189-196`. **Measure the effect here** (§5), and re-scope steps 4–5
if warranted.

*Status: implemented.* `SchemaSource` covers measure, stream and trace (stream and trace have the
same per-request registry read and the same watch-fed maps), and the TopN source-measure lookup
uses it too; TopN aggregations and properties still read the registry. A miss falls back to the
registry. Per §11.4, steps 4–5 are deferred.

**Step 4 — Plan, compile, bind, lower.** Add `pkg/bydbql/plan_measure.go` (the plan types of §4.2
and the time spec of §4.3), `plan_compile.go`, `plan_bind.go` and `plan_lower.go`. Reimplement
`transformMeasureQuery` as compile → bind → lower, so the transform path and the plan path share
one implementation and cannot drift. `TransformResult` gains a `Plan` field alongside the existing
`QueryRequest`.

**Step 5 — Plan cache.** Add `banyand/liaison/grpc/bydbql_plan_cache.go` (§6): its own LRU with
`Add` and explicit `Remove`, `singleflight` per key, dependency-set validation on hit, and its own
metrics. Insert it between `getOrPrepare` and dispatch in `bydbql.go`, keeping lowering ahead of
`AuthorizeTransformedRequest`. Construct it next to `preparedCache` (`server.go:381`) with its own
size and byte-bound flags, mirroring `--bydbql-prepared-cache-size` and
`--bydbql-prepared-cache-max-bytes` (`server.go:472-475`).

## 10. Test plan

Because lowering reproduces today's proto, most existing coverage needs no change:

- `bydbql_test.go` and `binder_test.go` exercise only the AST and binder.
- The 300 `.ql`/YAML fixture pairs under `test/cases/*/data/input/` compare the *produced* proto
  against a hand-written one (e.g. `test/cases/measure/data/data.go:203-217`), and keep passing
  through the lowered proto — no fixture regeneration. Switching the measure harness to the plan
  path gives byte-equivalence over all 104 measure fixtures, end to end against a live server.
- The 40-case `binder_equivalence_test.go` compares literal against parameterized renderings of the
  same query and keeps working on lowered protos.

New tests:

- **Golden equivalence** (step 1): `proto.Equal(compile → bind → lower, Transform)` across the
  corpus, with the current transformer kept as the oracle for one release.
- **Invalidation, asserted on output shape** rather than on a counter:
  - bump a measure's revision *and* change a tag from `STRING` to `INT`; assert the lowered
    condition's value switches oneof arm;
  - delete the measure; assert the next lookup recompiles and **errors** rather than serving the
    old plan;
  - move the revision *backwards*; assert it still invalidates (the case a `>` rule gets wrong);
  - compile through the registry fallback; assert nothing is stored;
  - unchanged revisions; assert a hit issues **zero** schema-source calls, via a counting fake.
- **Parameter shape on a cache hit**: array expansion in `IN`, `MATCH` and `HAVING` on the *second*
  execution of a cached plan, including the `wasSingle` scalar-versus-array distinction of §4.3.
- **Time**: every comparator against a fixed `now`, including `TIME > <absolute>` producing
  `end = now`, and `=` producing `begin == end`.
- **Concurrency**: under `-race`, N goroutines lowering one cached plan while another changes the
  measure's revision.
- **Benchmark**: `bydbQLService.Query` before and after, with a fake registry that simulates
  fan-out latency — once after step 3 and again after step 5.

Suites: `go test ./pkg/bydbql/... ./banyand/liaison/grpc/...`, then `test/cases/measure/`.

## 11. Open questions

Decided:

1. **Coercion-error status code** (§8). **Decision: accept** the `Internal` → `InvalidArgument`
   change and note it in `CHANGES.md`. A wrongly typed parameter is a client error, and
   `InvalidArgument` is the code the binder already uses for its other parameter errors.
2. **TopN follow-up** (§7). **Decision: a separate follow-up.** Extend `measureRegistryKinds` to
   track `KindTopNAggregation`, then cache TopN plans under the same dependency-set rules. The
   redundant third fan-out at `banyand/dquery/topn.go:54` is removed in the same change.
3. **Multi-group schema divergence.** **Decision: reject, in a separate change.** This design keeps
   the last-group-wins merge of §6.2 so that lowering stays byte-identical. A follow-up rejects a
   query over groups whose same-named measures disagree on a tag's type, rather than silently
   coercing it to one group's type; it changes behaviour on its own and gets its own release note.

Measured:

4. **Is the cache needed after step 3?** **Decision: no — build step 3, defer steps 4–5.**
   A local benchmark of Bind + `TransformBound` over six representative measure queries gives:

   | Cost per query | Measured |
   |---|---|
   | Real property-registry `GetMeasure`, loopback, 1 metadata node | mean 438 µs, p99 1.33 ms |
   | Same, 3 metadata nodes | mean 566 µs, p99 1.94 ms |
   | Transform with a local in-memory schema (after step 3) | 3–6 µs, 35–61 allocations |
   | Parse, already cached by `preparedCache` | 80–290 µs |

   The fan-out is paid once per group and is 100–200× the remaining compile CPU, so step 3
   removes about 99% of the transform cost. A plan cache could save at most 3–6 µs per request,
   which does not justify the dependency-set invalidation and `singleflight` machinery. Revisit
   steps 4–5 only if production profiles show transform CPU, not metadata latency, mattering.
   Loopback understates production latency, which only strengthens the case for step 3.
