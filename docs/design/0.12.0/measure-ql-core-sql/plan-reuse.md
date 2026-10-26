# Reusable measure QL plans for parameterized OAP traffic

Status: **implemented by T05**, with verification recorded in
[T05 verification](t05-verification.md) as historical ticket evidence and the
[PR 1 verification record](pr1-verification.md) for the merged cache foundation.
PR 2 extends reusable metadata to aggregate calls, grouping columns and native
entity identities; see its [verification record](pr2-verification.md). Native
eligibility, requests and lifecycle selection remain fresh for each execution.
Expression and sort recipes remain extensions for subsequent Catalog B PRs.
This extends the [engine design](datafusion-engine.md) on PR #1412. It changes no
protobuf, RPC, native request or frame format.

## 1. Decision and expected benefit

Cache an immutable, schema-bound QL plan template per reusable query shape. On a
hit, bind new parameter values and instantiate fresh execution state. Reuse applies
to existing parameterized measure QL as well as the new B constructs; limiting the
cache to complex queries would miss OAP's repeated simple templates.

PR #1412 already caches parsed `PreparedStatement` objects in
`banyand/liaison/grpc/bydbql_cache.go`; `pkg/bydbql/prepared.go` supplies immutable
placeholder numbering and per-request binding overlays. That avoids parsing but
not schema binding or planning. Keep that cache and add a separate compiled-plan
cache, because only the latter needs schema invalidation.

The [earlier plan-cache design](../bydbql-plan-cache/README.md) deferred compiled
plans after measuring 3–6 microseconds for Bind + transformation with local schema
reads. Preserve that historical result. This decision revisits the deferral for
Catalog B: typed raw source/output resolution and time-range planning add work,
while OAP repeatedly varies bound values. PR 2 adds aggregate metadata; later
tickets add general expression planning.
Benchmark both simple legacy templates and new plans; do not assume the earlier
measurement proves either a large benefit or no benefit for this engine.

The expected savings are **QL planning CPU, allocations and planning latency**.
Binding, dependency checks, operator initialization, native planning/execution,
network and storage work still happen. No result caching or cross-request data
snapshot is introduced, and end-to-end speedup must be measured separately.

## 2. Reusable template versus execution instance

| Cached immutable template through PR 2 | Fresh for every execution |
|---|---|
| Parsed syntax and placeholder constraints | Parameter values, native list values and validation |
| Resolved raw slots, source identities, types and output names | Bound time ranges and one captured now |
| Legacy native lowering metadata and typed raw projection/time recipes | Newly lowered native requests and optional RawPlan |
| Aggregate calls, grouping columns and native entity identities | Native eligibility proofs, branch requests and local/merge state |
| Schema dependency revisions and local watch generations | Dependency checks, current authorization and native gates |
| Output mapping and source schema metadata | Operator instances, query budgets, borrowed owners and result encoding |

Do not cache a running Pipeline, PullOperator, mutable bound request, result batch,
cursor, memory lease or permission outcome. A template is a recipe; every request
owns its bound values and execution state. Local hash aggregation and native
aggregate merging instantiate fresh state from the reusable aggregate metadata.
General expression kernels and sorting remain future template extensions.

The implemented request flow is:

```text
exact QL text -> prepared syntax lookup -> validate parameter overlay
    -> compiled-plan lookup + schema dependency validation
    -> hit: immutable MeasureTemplate
       miss: capture schemas -> compile -> revalidate -> publish if reusable
    -> MeasureTemplate.Bind with fresh parameters and one execution clock
    -> native QueryRequest + optional raw/aggregate recipe and native alternative
    -> current authorization -> admission/revision/lifecycle gates
    -> native batches -> optional local aggregation or keyed merge -> release handle
```

Parameters do not enter the compiled key. Time ranges, native criteria/list values,
LIMIT/OFFSET and the execution clock are rebuilt for each binding. An explicit
LIMIT 0 can choose an empty recipe; contradictory typed time bounds also avoid a
scan. Native eligibility and schema gates use current metadata rather than cached
topology or authorization decisions.

PR 2 rechecks native aggregate eligibility after binding and current lifecycle
selection before each branch. Integer overflow, nullable keys, count bounds and
provider restrictions select the complete local fallback when proof is missing.
Later expression and full-sort/top-k features must preserve this division.

## 3. Cache key and parameter types

The current cache key is **exact parameterized QL text**. The dialect and source
resolution context are fixed by the current API; there is no separate session,
function-registry or inferred parameter-type signature in the implemented key.
Prepared-statement constraints and schema resolution validate values, and each
execution binds its own TagValue overlay. Bound values, time ranges, principals,
trace IDs and native node addresses are excluded. Current authorization runs on
every request. Literal queries bypass compiled reuse; no literal-to-parameter SQL
normalizer is introduced.

If future features introduce a session namespace, planning setting, function
registry generation or underconstrained expression type that changes compilation,
its compilation-context/type identity must become part of the key before that
feature can share templates safely. The same applies if a future policy changes
query shape. This is an extension requirement, not an implemented context/type
signature or a new parameter wire variant in PR 1.

## 4. Dependency validation and schema changes

Record every schema object that contributed to compilation: the measure in every
queried group, and group/index-rule/index-binding objects if their definitions
were inspected to make a cached decision. Otherwise defer that decision to current
native planning. Cache the plan of QL scan leaves, not native topology or per-shard
execution plans. Preserve the query's original group list; deduplicating dependency
records must not deduplicate requested groups.

Reuse the earlier design's revision rules: compare with `!=`, never `>`; a missing
object invalidates; use the query gate's revision resolver; a compile using registry
fallback instead of a coherent local view is executed but not cached. A revision
may decrease, and zero/unavailable revisions must not certify a reusable plan.

Extend the local Go schema adapter to capture a coherent read-only schema object,
its revision and a local watch generation together. `entityRepo` already replaces
its measure/locator entries under one mutex; read paired metadata under that lock.
Validate that the captured object and `resolveQueryGateRevision` agree. A registry
revision observed with an older local schema object must not label that object as
current. If agreement cannot be established, bypass caching and preserve the
normal schema-gate/fallback behavior.

On a hit, validate the complete dependency set locally. On a miss, capture the
set before analysis, then revalidate after compilation and before publication.
Watch generation changes on replacement/deletion, closing same-revision/ABA
cases. If any dependency changed during compile, discard the candidate and retry
once; if the schema keeps changing, execute uncached under normal gates. Do not
spin indefinitely or let an older concurrent compile replace a validated new one.

Recheck dependencies before dispatch if compilation/binding waited long enough
for an observed generation change. Existing schema gates still apply. This does
not provide atomic schema/data snapshots or synchronously eliminate watch lag;
it prevents cache reuse beyond the local schema view's normal freshness contract.
Deletion, recreation and index removal must be tested explicitly.

## 5. Cache ownership, concurrency and metrics

Use a dedicated liaison-local LRU bounded by both entry count and estimated bytes.
Keep parse-cache and plan-cache metrics separate; schema invalidation is not parse
thrashing. Implemented defaults are 4,000 templates and 64 MiB of estimated retained plan
bytes. They are bounded starting settings, not values tuned by a large working-set
benchmark. The [user guide](../../../interacting/bydbql.md#57-compiled-plan-cache-controls)
lists the cache flags, default/zero behavior and metrics.
Existing prepared-cache limits remain separate. Include key strings, owned plan
nodes, expression recipes and dependency metadata in byte estimates; do not retain
entire mutable repositories through a closure.

Share one immutable plan across concurrent executions using an explicit plan
handle. Eviction removes lookup eligibility but cannot invalidate an active
execution. Account for evicted templates until their last handle is released;
retired-but-in-use plans still consume service memory. If the cache budget cannot
admit a new plan, execute it uncached under the request budget. Bound concurrent
compilation as well as cached entries. Temporary compile allocations are not free.

Collapse concurrent misses for one key/context using a singleflight-style
coordinator. Each caller can cancel its wait independently. Do not run shared
compilation on the first caller's cancellable context: use bounded compile work
whose lifetime is controlled by the remaining waiters. Cancel when none remain,
and remove in-flight bookkeeping on every completion/error. Do not cache failures.
Publication validates dependency generations and occurs once per stable version.

Observe hit/miss/stale/bypass/oversize, compile and dependency-check duration,
instantiation duration, waiters, entry bytes and retained-plan bytes. Add plan
cache outcomes to existing QL tracing/access-log context without exposing parameter
values. Avoid SQL-text or template-hash metric labels with unbounded cardinality;
use existing bounded diagnostic reporting where individual templates are needed.

## 6. Implementation and acceptance

The [implementation tickets](implementation-tickets.md) assign template/instance
separation to T03 and the complete concurrent cache to T05. Dependency validation,
miss coalescing, lifecycle and memory bounds are parts of that capability, with
real RPC cases and OAP-shaped planning/concurrency benchmarks. Each later feature
must prove cold/warm/uncached equivalence.

Introduce template/instance separation with the typed planner, then add compiled
caching before declaring B1–B3 complete. Cover legacy parameterized measure queries
through the same immutable compilation/lowering representation while preserving
their native request/output semantics. Leave stream/trace/property/SHOW TOP N plan
caching as separate scope; their existing parse cache continues to work.

Expected changes: plan-template and bind/instantiate code in `pkg/bydbql`, a new
`banyand/liaison/grpc/bydbql_plan_cache.go`, coherent schema-snapshot helpers, cache
configuration/metrics and QL orchestration. No protobuf or native wire changes.
The implementation keeps literal-query and incoherent-schema bypass paths.

Required tests:

- One template, many values: compile once; produce different correct results and
  newly bound native requests. A cache hit must skip name/type analysis, structural
  rewrites and physical recipe construction.
- Fresh now, changed time ranges, empty/non-empty IN arrays, LIMIT 0/nonzero,
  null/non-null parameters and explicit casts across repeated hits.
- Concurrent hits under `-race`: independent scratch/state, cancellation, budgets,
  batch owners and authorization; no shared mutable operators.
- Updates/deletion/recreation, lower revision values, same-revision watch changes,
  index-binding changes, multi-group dependencies, mismatched local views and
  changes during compile. Unrelated objects do not invalidate a dependency set.
- Cold bursts compile once per stable key/version; canceled waiters do not cancel
  other requests; eviction while executing is safe; count/byte limits include
  retired in-use plans; fallback and oversized plans execute correctly uncached.
- Permission revoked between hits: execution is denied despite a cached plan.
- Cached and uncached plans produce identical results, errors and native requests
  for both legacy and extended QL semantics.

For T05, the user narrowed performance verification to **the same parameterized
text and changing values, with compiled caching enabled versus disabled**. Use
one simple legacy shape and one wide typed shape. Measure serial planning ns/op,
B/op and allocations/op, then fixed-data RPC latency and process CPU/allocations
with eight workers. Hold the dataset at 1,024 rows, with 20 warmups and 1,000 timed
requests per variant in each of five resource-limited repetitions. Cache counters
prove warm reuse and zero timed compilations. Cold concurrency, invalidation and
capacity stay in correctness tests. The earlier 2,500/4,000/over-limit working-set
and churn benchmark proposal is superseded for this ticket; defaults are bounded
starting settings, not tuned from a working-set benchmark.

The architectural acceptance gate is actual immutable plan reuse on a hit with
correct per-request binding. Record measured benefits and any simple-query
regression before choosing final cache defaults; no unmeasured latency claim is
part of this design.
