# Reusable measure QL plans for parameterized OAP traffic

Status: **included in the proposed Catalog B implementation**; not implemented.
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
Catalog B: type analysis, logical rewrites, aggregate decomposition and physical
expression planning now add work, while OAP repeatedly varies bound values.
Benchmark both simple legacy templates and new plans; do not assume the earlier
measurement proves either a large benefit or no benefit for this engine.

The expected savings are **QL planning CPU, allocations and planning latency**.
Binding, dependency checks, operator initialization, native planning/execution,
network and storage work still happen. No result caching or cross-request data
snapshot is introduced, and end-to-end speedup must be measured separately.

## 2. Reusable template versus execution instance

| Cached immutable template | Fresh for every execution |
|---|---|
| Parsed syntax and placeholder constraints | Parameter values, list expansion and explicit conversions |
| Resolved slots, source identities, types/nullability and output labels | Bound time ranges and one captured now |
| Analyzed logical plan and value-independent rewrites | Safe value-dependent simplifications and guarded plan choices |
| Aggregate decomposition, hidden projections and dependency set | Newly lowered native requests and authorization decisions |
| Typed physical expression recipes and batch-operator topology | Operator instances, scratch buffers, hashes/heaps and aggregate state |
| Native scan lowering recipes and exactness conditions | Current topology/stages/gates, scan readers, trace and cancellation |
| Output mapping and known output schema | Budgets, borrowed owners, batches and result encoding |

Do not cache a running Pipeline, PullOperator, native request mutated by binding,
result batch, cursor, memory lease or permission outcome. A physical plan template
is a recipe for these objects, not the objects themselves. Immutable expression
code may be shared; mutable evaluator scratch is execution-owned.

The pipeline becomes:

```text
exact query text + compilation context
    -> compiled-plan lookup + dependency validation
    -> hit: immutable typed plan template
       miss: prepared-cache lookup -> analyze -> structural optimize -> physical recipe
    -> bind parameters / capture now
    -> instantiate guarded choices, fresh requests and fresh batch operators
    -> authorize all lowered native requests -> execute -> release execution handle
```

Make the template parameter-independent. Do not constant-fold a placeholder using
its first value or bake in a relative timestamp. Compile column-to-parameter
comparison kernels once; keep threshold values in the binding overlay. Rebuild
IN-list lookup sets per execution without changing the cached expression tree.
Varying array length, LIMIT/OFFSET or time range does not create a new cache entry.

When a physical choice depends on a bound value, cache a validated guard and its
alternative recipes, or retain a conservative generic plan. Apply only that small
choice at instantiation. For example, LIMIT 0 can select Empty, and an absent versus
finite limit selects full sort versus top-k. A proof of exact pushdown that depends
on parameter nullness must be checked again. Never reuse a first-request pruning
or aggregate decision when later values invalidate its proof.

## 3. Cache key and parameter types

Key by exact QL text plus the compilation-context fingerprint: dialect/semantic
mode, any session/default namespace used in resolution, and planning settings or
function-registry generation that actually change the plan. Current fully specified
sources need no implicit namespace. Do not put bound values, time ranges, auth
principals, trace IDs or current node addresses in the key. Current permissions
are checked for every execution; compiled resource resolution is not user-specific.
If a future policy changes query shape, its policy identity must become part of
the key before that policy can use the shared cache.

Infer placeholder types from source columns and explicit casts first. If a new
expression remains underconstrained, infer only its logical type from the bound
TagValue variant and include that type signature in the key; a null-only ambiguous
expression requires an explicit cast. Values and array lengths remain excluded.
Typed NULL uses its contextual type rather than creating a null-specific plan.
Do not introduce parameter-value-dependent selectivity plans for B.

Preserve the prepared cache's policy of caching parameterized templates rather
than rewriting literal SQL into new normalized templates. Literal queries bypass
this cache initially. This directly serves OAP's same-text/different-parameters
workload without adding a SQL normalizer or changing parameter semantics.

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
thrashing. Proposed starting limits are 4,000 templates and 64 MiB for compiled
plans, to be confirmed with the benchmark working set and measured plan sizes.
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
This is implementation work for the plan, not code shipped by this document.

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

Benchmark an OAP-style working set with the **same parameterized text and changing
values**, single-threaded and concurrent. Include one hot template, about 2,500
and 4,000 distinct templates, and a set exceeding each cache limit. Compare:
(1) existing parse cache plus local schema transform, (2) new typed planner with
compiled caching disabled, and (3) the compiled cache warm/cold/invalidation paths.
Measure ns/op, allocations/op, CPU profiles, p50/p95/p99 planning latency, cache
hit ratio and live/retained bytes. Then measure fixed-data end-to-end Query latency
separately so native scan/network cost cannot masquerade as planning savings.

The architectural acceptance gate is actual immutable plan reuse on a hit with
correct per-request binding. Record measured benefits and any simple-query
regression before choosing final cache defaults; no unmeasured latency claim is
part of this design.
