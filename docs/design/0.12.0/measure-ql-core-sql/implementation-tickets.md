# Catalog B implementation tickets and verification contracts

Status: **11 capability tickets planned; the former T01 verification work is complete.**
The [T01 verification record](t01-verification.md) preserves its implemented tests
and measured baseline. That work now supports T02; it is not counted as a completed
engine capability. No implementation work or benchmark evidence is discarded.
Base: PR #1412, reviewed at `012115c5`.

This document implements the [language contract](README.md),
[batch engine design](datafusion-engine.md) and [compiled-plan reuse design](plan-reuse.md).
The existing `QLB-T01` labels and `qlb_t01` benchmark remain valid. All other ticket
commands/scenarios below are proposed until their owning capability is implemented.

## Parent QLB: deliver Catalog B through the existing Query RPC

**Classification: tracking parent.** Its 11 capability tickets are listed below.
Each delivers an executable query capability or a substantial execution optimization,
with real RPC end-to-end proof and a benchmark of that same behavior. The parent
owns composition and release acceptance; drafting does not publish GitHub issues.

**Ticket sizing rule:** define the delivered query operation and the performance
question before accepting a ticket boundary. Its benchmark must exercise the new
execution path and measure latency, CPU, allocation or retained-state cost against
an explicit baseline/reference. Adding a generic benchmark command to an error,
syntax spelling or helper change does not make it a sufficiently sized ticket.
Those changes and their tests belong inside the capability that needs them.
Do not split error handling, numeric finalization, fixtures or instrumentation
into independently completable tickets. Internal implementation steps may be small;
the capability closes only after its full e2e and benchmark gates pass.

**Fixed boundary:** QL grammar/planning, liaison execution and Go-only extraction
of existing native batch execution. Protobuf measure requests and QL share native
scan/reduction operators; QL adds typed liaison operators above that seam. Reuse
PR #1412's RecordBatch, owners, schema source and admission machinery. No changes
to protobuf/generated API contracts, RPCs, HTTP, data-node operators, persisted
formats or frame formats; no Rust runtime or Arrow IPC dependency.

**Readiness:** capability tickets remain planned blocked leaves until their listed
prerequisites and actual interfaces are available on the target branch. This
worktree contains the PR #1412 base; this document does not certify its merge into
main. Revalidate the seam, independent expected results and focused RED evidence
before queueing work. A compatible cutover can use a missing new-seam contract as
RED evidence, but must also prove activation by the real caller. Preserve the
completed T01 support work when implementing T02.

### Dependency and benchmark map

IDs are preserved to keep existing code, artifacts and references unambiguous.
Retired IDs are mapped below and must not be queued as separate tickets.

| Ticket | Delivered capability | Prerequisites | Why its benchmark is necessary |
|---|---|---|---|
| T02 | Native/QL shared batch execution, including verification infrastructure | PR #1412; T01 support complete | Measure extraction overhead, buffer retention and egress allocations against the existing RPC path |
| T03 | Typed raw queries with aliases and WHERE time predicates (B1/B2) | T02 | Measure typed projection/binding cost and exact time pruning versus residual scanning |
| T05 | Reusable plans for concurrent parameterized OAP queries | T03 | Measure warm planning savings, cold-burst work, cache churn and retained-plan memory |
| T07 | SQL-correct local grouped/scalar aggregation, including AVG (B3) | T03 | Measure aggregate throughput and group-state growth for raw fallback execution |
| T08 | Native multi-aggregate pushdown and keyed batch merge, including AVG (B3) | T07 | Compare native fan-out/merge with the complete local aggregation path |
| T10 | Typed vector expressions, casts and masked conditionals (B7) | T03 | Compare batch kernels with scalar evaluation and measure short-circuit work savings |
| T12 | Field WHERE feeding local aggregation and pagination (B8) | T07, T10 | Measure complete-scan cost, selection density and filtered aggregate throughput |
| T14 | Group filtering with HAVING and compatible membership syntax (B4) | T08, T10, T12 | Measure aggregate/filter composition, hidden-call reuse and surviving-group cost |
| T15 | Visible-tuple DISTINCT (B6) | T07, T10 | Measure deduplication throughput and retained keys across duplicate ratios |
| T16 | Computed/ordinal grouping and selectable time buckets (B9) | T05, T08, T10 | Measure key computation plus aggregation across group cardinalities |
| T17 | Multi-key ORDER BY with full sort and bounded pagination (B5) | T05, T12, T14, T15, T16 | Compare compact top-k against full sort and measure sort/candidate memory |

| Former ticket | Current owner | Treatment |
|---|---|---|
| T01 verification harness and baseline | T02 | Completed supporting work; keep code, labels, scenario and evidence |
| T04 time lowering | T03 | Part of the typed raw-query capability |
| T06 concurrent compilation | T05 | Part of the complete OAP plan-cache lifecycle |
| T09 AVG finalization | T07 and T08 | Local SUM/COUNT state in T07; native decomposition/reuse in T08 |
| T11 predicates and CASE/COALESCE | T10 | Part of one executable vector-expression capability |
| T13 CONTAINS spelling | T14 | Grammar compatibility acceptance within group-filter queries |
| T18 top-k pagination | T17 | Full sort and finite-limit specialization of the same ordering capability |

Suggested delivery order: T02, T03, T05, T07, T08, T10, T12, T14, T15, T16, T17.
T07 and T10 can develop independently after T03. Native multi-aggregate execution
follows a correct local reference; caching ships with invalidation, concurrency
and resource bounds together. These are completion boundaries, not a requirement
to write every internal component in one patch.

## Verification required by every child

### Real endpoints and independent fixtures

Extend the existing shared measure cases and register the new table in both
`test/integration/standalone/query/vectorized_test.go` and
`test/integration/distributed/query/vectorized_test.go`. The latter runs a liaison
and two data nodes. Use the real `BydbQLService.Query` client, write fixtures through
existing services, wait for visibility outside measurement, and assert actual
`measure_result` values or gRPC errors. Never substitute a local transformer or
fixture scan for these end-to-end checks.

The current `test/cases/measure/data/data.go` helper requires a native request YAML
and only checks the QL result variant. It does **not** verify QL result values.
The completed T01 work adds a separate QL-only verifier using `.ql`, `#!params:`
and independently authored expected measure-response YAML/error specifications. Native equivalence
is an additional oracle where applicable, not a prerequisite for expressing QL
tests. Preserve timestamps when significant; ignore only explicitly nondeterministic
identity/trace fields. Compare unordered rows as a multiset until ORDER BY applies.
Check types, nulls, family/field names, hidden-slot exclusion and ordered rows.

Use this small fixture F for the examples below. `time = T + offset`, with T a
fixed UTC millisecond aligned to the Unix-epoch 2-second grid and covered by the
test retention window; `host` is an entity tag, `zone` a nullable
string tag, `v` a nullable INT64 field and `f` a nullable DOUBLE field. `rid` is a
unique integer tag for identifying rows, not an incidental storage SID. A separate
array tag `labels` is specified in T14. Existing schemas have no NOT NULL flag:
non-null grouping proofs must come from verified native guarantees or exact
predicates such as host IN ('a','b','c'), never just observing this fixture.
Ensure distributed rows actually cover both nodes, and add a
replica/version fixture to verify deduplication rather than assuming placement.

| rid | offset | host | zone | v | f |
|---|---|---|---|---:|---:|
| 1 | 0 s | a | x | 0 | 0.25 |
| 2 | 1 s | a | x | 2 | 0.75 |
| 3 | 2 s | b | y | 4 | 1.5 |
| 4 | 3 s | b | y | NULL | NULL |
| 5 | 4 s | c | NULL | 6 | 2.5 |
| 6 | 5 s | c | empty string | NULL | 4.0 |

F has six rows, `COUNT(v)=4`, `SUM(v)=12`, `AVG(v)=3.0`, and `AVG(f)=1.8`.
Host a has `AVG(f)=0.5`. Empty/null-only and >100-row/group fixtures are separate
extensions, so expected values do not change silently. Floating results use an
explicit tolerance (absolute 1e-12 for these small values); integer counts and
identities compare exactly. Do not generate goldens with the new evaluator.

### Commands and evidence

Each capability below names **E(Tnn)** and **P(Tnn)** for its own behavior.
These are mandatory command recipes. Existing support work has labels `measure-ql`
and `QLB-T01`; retain them as compatibility coverage. Its runnable E(T01) is:

```sh
go test ./test/integration/standalone/query -count=1 -timeout=30m \
  -run '^TestPropertyIntegrationQuery$' \
  -ginkgo.label-filter='measure-ql && QLB-T01' -ginkgo.fail-on-empty

go test ./test/integration/distributed/query -count=1 -timeout=30m \
  -run '^TestPropertyQuery$' \
  -ginkgo.label-filter='measure-ql && QLB-T01' -ginkgo.fail-on-empty
```

Replace `QLB-T01` with the owning ticket ID for E(Tnn). Run all cases listed in
that ticket in both topologies. Fail on an empty selection, skipped required
cases or missing expected test names. Once T05 lands, each applicable feature
case also runs cold, warm with changed parameters, and uncached with identical
semantics. Add focused package tests first, using the ticket's listed packages:

```sh
make test-ci PKG='./pkg/bydbql ./banyand/liaison/grpc'
```

`make test-ci` includes race detection. Substitute the relevant package list;
`pkg/query/ql` is a proposed module, not present on the base. Exercise cancellation,
owner poisoning and concurrent binding in focused tests. Cluster-wide failure
tests still go through the real RPC; test-only clocks/observers/fault hooks may
control scheduling or inspect counts, but cannot replace storage or QL execution.
Every ticket records its RED command/result, GREEN counts, revision and benchmark
artifact path before completion. Do not count a source-text assertion as activation.

### Benchmark infrastructure and shared gates

Extend `test/integration/distributed/querybench`, including its config, workload,
orchestrator and report handling with one scenario per active capability as it
lands: `qlb_t02`, `qlb_t03`, `qlb_t05`, `qlb_t07`, `qlb_t08`, `qlb_t10`, `qlb_t12`,
`qlb_t14`, `qlb_t15`, `qlb_t16`, `qlb_t17`. Keep implemented `qlb_t01` as the
unchanged-production baseline; retired IDs do not get new standalone scenarios. This harness starts real endpoints and is Docker-gated. Its
entrypoint is `TestDistributedQueryBench`, **not** a Go `Benchmark` function.
Keep native `scan_all`/`top_with_filter` as controls. The standalone benchmark suite
currently skips its workloads and cannot satisfy these gates. One real distributed
RPC benchmark per ticket is mandatory; both topologies must pass E(Tnn).

The existing P(T01) baseline command is:

```sh
DQB_ENGINE=measure DQB_MODES=vec \
DQB_SCENARIOS=qlb_t01 DQB_CARDINALITIES=1024,100000 \
DQB_QUERY_WORKERS=1 DQB_QUERY_ITERATIONS=1000 DQB_WARMUP_ITERATIONS=20 \
DQB_PROFILE=1 DQB_REPORT_DIR=/work/.omx/bench-reports/qlb-t01-r1 \
test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
```

For P(Tnn), substitute its scenario and a unique report directory. Run five
isolated repetitions, changing `r1` to `r2` etc.; retain all reports. Every scenario
must emit its named variants, expected row count/checksum, timings and allocation
records; zero samples, skipped scenarios or missing variants fail verification.
Use 1,024 and 100,000 input rows unless a ticket specifies otherwise. Also run
1,000,000 rows for T07, T08, T12, T14, T15, T16 and T17. Bounded low-budget
resource-stress variants may fail
with the asserted error rather than return a result. Such a variant measures
resource bounds of its owning query capability; it does not replace that ticket's
successful-query benchmark. Ordinary parse/bind errors stay in correctness tests.
Keep setup and visibility waits outside timing. Validate results before timing and at sampled points during
measurement so a fast wrong/error path cannot pass.

Every benchmark records p50/p95/p99 RPC latency, throughput, CPU profile,
allocations/bytes per query, input/output rows, leaf calls, operator peak retained
bytes, lease charges and post-close live reservations. Add planning/cache metrics
when T05 lands. These process-wide profiles include the client, liaison and data
nodes; use focused planner/kernel benchmarks for a **planning CPU** claim.
Counters become required when their owning ticket lands; T01 marks unavailable
operator/cache observations as unavailable, never as measured zeros.

These are proposed acceptance budgets, not measured claims:

- **G1, compatibility:** compare unchanged legacy QL and native controls with
  PR #1412 and the immediate prerequisite revision on the same machine/container,
  dataset, schema, concurrency and Go build. Median-of-five p50 and allocated
  bytes/query must be <=1.10x baseline; p95 <=1.15x. T01 captures the baseline.
  Save uncertainty and raw runs; an unstable comparison is inconclusive, not a
  pass. Freeze comparison inputs before optimizing. New unsupported SQL has no
  fictitious old-version latency baseline.
- **G2, bounded resources:** charge all live buffers/state, including native
  retained frames and final response, to the shared query budget. At 16 MiB and
  256 MiB limits, either return the correct result or ResourceExhausted before an
  over-budget allocation. Live execution reservations and owner pins return to
  baseline after Close/cancellation. The current lease is monotonic: report its
  cumulative charges separately; do not assert that spent charges are refunded.
  T01 only captures existing resource observations; shared accounting and its
  enforcement start in T02 and remain mandatory for subsequent production changes.
- **G3, per-ticket work:** each leaf below adds a numerical work/retention or
  speed gate to G1/G2. Instrument operation counts in test builds. For new SQL,
  benchmark its real RPC and its specified reference/algorithm; do not claim
  lower latency merely because an internal counter improved.

Where a ticket names `K(Tnn)`, add focused `BenchmarkQLB/Tnn` sub-benchmarks under
`pkg/query/ql`, with independently written scalar/reference code in `_test.go`:

```sh
go test ./pkg/query/ql -run '^$' -bench '^BenchmarkQLB$/^T10$' \
  -benchmem -benchtime=2s -count=5
```

Replace T10 as required. Require matching benchmark records; Go succeeds on an
empty regex match. Use the same inputs and output checks for both algorithms.
These supplement P(Tnn), never replace it. Cache planning benchmarks live in
`banyand/liaison/grpc` and are named explicitly in T05.

## Capability tickets

### T02 — Deliver shared native/QL batch execution

**Supporting work already complete:** former T01 supplies the QL-only verifier,
independent F fixtures, four real RPC cases and the five-run `qlb_t01` baseline.
Its invalid-query case remains a correctness assertion. T02 still requires the
production batch cutover and its measured execution results before completion.

**Performance question:** what latency, allocation and retained-buffer cost does
the shared batch boundary introduce or remove relative to the existing RPC path?
Measure wide/hidden projections and aggregate outputs, not just helper call counts.

**Boundary/activation:** factor `DistributedPlan` output before
`iteratorFromBatches[WithSchema]`/`vectorizedMIterator`; activate both
`dquery.measureQueryProcessor.Rev`'s existing collector and QL's NativeScan through
that Go-only seam. Factor common group/revision/topology preparation so QL cannot
bypass it. Preserve native request semantics, hidden columns, trace/status and
final serialization. Inject shared accounting and explicit schema/Close ownership.

**E2E cases:** `raw-and-aggregate-equivalence`, `hidden-columns-and-empty`,
`multi-group-and-replica-dedup`, and `gate-error-cancel-budget` compare existing
goldens and statuses, including an early canceled client and a denied group.
Assert both real callers activate the seam; QL creates zero intermediate
DataPoints. Repeated-column views survive native descriptor reuse/poisoning.

**Benchmark:** P(T02) compares legacy raw and aggregate QL against T01 plus a wide
hidden-column variant. G3: zero intermediate DataPoint construction, no live owner
or reservation delta after 1,000 queries, and G1/G2. Document native eager frame
residency rather than claiming this extraction makes execution streaming.

**Checks/readiness:** E(T02), P(T02); focused `./banyand/dquery`,
`./banyand/liaison/grpc`, `./pkg/query/vectorized/measure/...`. Blocked by base/seam
verification; T01 support is available;
new-seam contract RED plus real-caller counters prove the compatible cutover.

### T03 — Execute typed raw queries with aliases and time predicates

**Performance question:** how much does typed binding/projection cost, and how much
native work does exact time pruning save compared with a complete residual scan?

**Boundary/activation:** `bydbqlService.Query` compiles immutable typed templates
and creates fresh Scan/Projection/Filter/Limit state. Add source shorthand, raw AS,
SlotID/type/nullability/source metadata and existing-response mapping. Legacy
parameterized measure queries use the template/instance seam without changing
native semantics. Unfinished aggregate shapes retain legacy lowering or are
explicitly rejected until T07/T08 admits them.

Time binding supports fixed INTERVAL, one execution clock, WHERE/TIME intersection,
contradiction-to-Empty and exact millisecond boundary arithmetic. Pure-time OR can
prune by an envelope only while retaining its full residual. A residual prevents
native aggregate/final-limit pushdown. Mixed field/time OR follows in T12.

**E2E cases:** `raw-aliases` projects host AS node and v AS value with F's types/nulls;
`repeated-source` preserves independent descriptors for repeated projections;
`duplicate-and-ambiguous-name` rejects invalid output names before scans. Cover
both source spellings, multiple tag families, `__ql` collisions, non-measure dispatch,
absent extended limit, LIMIT 0, legacy default limits and >100 extended raw rows.
`time-boundaries`: time > T+1s AND time <= T+3s returns rid {3,4}; equality T+2s
returns {3}. `time-or` returns {1,6}; `time-intersection-empty` performs no scan.
`fresh-now-and-fractional-boundary` covers a changed injected clock/parameters,
sub-millisecond bounds and checked domain extremes through the real RPC.

**Benchmark:** P(T03) compares raw aliases/plain and wide repeated projections,
exact time predicates versus equivalent legacy TIME, and residual OR versus its
complete-scan reference. Vary time selectivity and changed bound ranges while
retaining text. Record RPC latency, allocations, copied bytes and scanned rows.
G3: no per-row name/type resolution; metadata-only aliases copy zero value bytes;
one clock capture per request; zero scan calls for Empty/LIMIT 0 and one native
leaf otherwise. Exact time queries retain their native pruning, with G1/G2.

**Checks/readiness:** E(T03), P(T03); focused `./pkg/bydbql`, `./pkg/query/ql`,
`./banyand/liaison/grpc`. Blocked by T02. Alias mapping, time boundaries, ownership
and final pagination are one raw-query contract; none is a separate ticket.

### T05 — Reuse compiled plans safely under concurrent OAP traffic

**Performance question:** do warm compiled templates reduce planning CPU/allocation,
and does cold-miss coalescing prevent duplicate work without harming concurrency?

**Boundary/activation:** the Query caller adds the separate bounded compiled cache
from [plan reuse](plan-reuse.md). Ship coherent schema/revision/watch-generation
capture, complete dependency validation, registry-fallback bypass, count/byte caps,
in-use handles and fresh binding/authorization/operators as one capability.
Shared cold compilation follows remaining waiters, not the first caller's context.
Bound compile concurrency/in-flight keys; publish once per stable version, cancel
all-waiter work and remove failed entries. Literal queries keep bypass behavior.

**E2E cases:** `same-text-new-values` returns different correct F rows on hits;
`clock-array-limit-null-rebind` covers fresh now, empty IN, LIMIT 0 and typed null;
`schema-replace-delete-recreate`, `multi-group-dependencies` and
`permissions-and-fallback` cover full invalidation, unrelated updates, authorization
and uncached fallback. Inject lower/same revision watch changes and compile races
through test hooks with actual RPCs. `cold-burst` releases 32 callers for one key
and compiles once while each returns its bound result. `first-waiter-cancel`,
`all-waiters-cancel`, `evict-in-flight` and `schema-change-during-compile` establish
lifetime safety under race/owner poisoning; failures are not cached.

**Benchmark:** P(T05), 1,024 input rows, compares base parse/local transformation,
typed uncached, cold, warm and invalidation paths at workers 1/8/32. Working sets:
1, 2,500, 4,000 and above both count/byte limits. Warm the whole admitted set, then
vary values on the same valid texts. Measure RPC and planning latency, planning
CPU/allocations, miss amplification, churn and live/retired-plan bytes separately.
Add `BenchmarkQLBPlanReuse/T05` in `banyand/liaison/grpc`; run with `-run '^$'
`-bench '^BenchmarkQLBPlanReuse$/^T05$' -benchmem -benchtime=2s -count=5`.

G3: 1,000 serial warm requests perform zero analysis/optimization/recipe builds;
one compile per synchronized stable-key cold burst; >=99% hits for a warmed set
fitting both limits. For a wide typed projection with eight parameterized time
predicates, warm planning median ns/op and B/op each <=0.80x typed uncached.
Compile and retained-plan caps include in-use retired plans; canceled work leaves
zero in-flight entries. G1/G2 apply. Report simple-query and RPC results separately;
planning savings do not imply an end-to-end speedup.

**Checks/readiness:** E(T05), P(T05), focused planning benchmark;
`./pkg/bydbql`, `./banyand/liaison/grpc` and the schema adapter's owning package.
Blocked by T03. Use established run.Go helpers. Invalidation, concurrency and
bounds cannot be deferred to independent fixes after enabling reuse.

### T07 — Execute complete SQL aggregation over raw batches

**Performance question:** how do raw aggregation throughput, allocations and
retained group state scale with rows, groups and nullable inputs compared with an
independent scalar reference?

**Boundary/activation:** the QL aggregate lowering chooses one complete native raw
scan plus local HashAggregate for extended COUNT/SUM/MIN/MAX/AVG over named columns. AVG/MEAN uses float64 SUM/non-null
COUNT finalization, sharing state with matching output calls; never native MEAN.
Introduce the shared typed/null-aware group codec and dense group-ID state arrays
through this real caller. Scalar empty COUNT is int64 zero, other aggregates null;
grouped empty input has no groups. Native scan deduplication happens before local
aggregation. Preserve existing count-distinct/stage restrictions; do not silently
admit additional COUNT(DISTINCT) shapes.

**E2E cases:** `aggregate-F` produces count(v)=4, sum(v)=12, min(v)=0, max(v)=6;
`nullable-keys` keeps zone NULL and empty string separate; `null-only-and-empty`
checks scalar/grouped results; `complete-input` uses 513 groups plus replicas and
does not truncate at 100. Native-null-key and floating COUNT shapes must select
raw local execution. Native count precision beyond 2^53 is separately checked at
the adapter/state boundary with synthetic counts, not billions of fake e2e rows.

`average-F` expects AVG(v)=3.0 and AVG(f)=1.8; `sub-one-average` expects host a
AVG(f)=0.5. Zero count produces null; unequal shard populations use combined
sum/count, not an average of averages. Keep legacy native MEAN fixtures unchanged.

**Benchmark:** P(T07) uses 1/1,024/N groups, null mixes and batches spanning 65,536
rows; compare single/multiple aggregates and AVG versus SUM+COUNT+AVG with
shared state. K(T07) compares dense states with an independent scalar reference. G3: one
native raw leaf, exact N input rows after dedup, state cardinality equals actual
groups, and fixed-width aggregate state grows with groups rather than rows.
Report key/state capacity bytes separately from native residency; G1/G2 apply.

**Checks/readiness:** E(T07), P(T07), K(T07); focused `./pkg/query/ql`,
`./pkg/bydbql`, `./banyand/liaison/grpc`. Blocked by T03. Bind/query tests expose
new multi-output semantics; null-collapsing legacy helpers are not the oracle.

### T08 — Execute native multi-aggregate queries with keyed merge

**Performance question:** when does native aggregate fan-out/merge reduce cost
versus T07's one raw scan, and what are the cost/bounds of multiple branches and
high-cardinality keys?

**Boundary/activation:** aggregate capability lowering chooses native SUM/MIN/MAX/
exact COUNT leaves where equivalence is proven, otherwise T07. Add auxiliary exact
non-null COUNT state when a finalized aggregate loses empty/null information;
deduplicate visible and auxiliary calls. Remove child final pagination, bound
branch concurrency and merge batch columns by the full group tuple plus bucket
timestamp. All leaves share the parent lease,
bound values, deadline and authorization checks. Branch failures fail the query.

Lower eligible AVG/MEAN to SUM and exact non-null COUNT, finalize as float64
and reuse matching visible/auxiliary calls. Unsafe count precision, nullable keys
or lost aggregate information select T07. AVG's state/finalization is part of the
aggregate operation, not an independent division helper ticket.

**E2E cases:** `sum-max-repeated-call` selects SUM(v) twice and MAX(v) by host,
with an exact host IN ('a','b','c') predicate, producing (a,2,2), (b,4,4), (c,6,6)
for sum/max. Expect three leaves: SUM(v), MAX(v) and auxiliary exact COUNT(v),
with no fourth leaf for the repeated SUM. In a separate case, add an all-null host
and include it in the IN predicate to prove count-based normalization to null.
Native eligibility must be established from actual provider contracts.
`bucket-and-multiple-keys` checks distinct buckets/keys with deliberately permuted
branch arrival. `missing-null-empty-groups` validates union/normalization or the
documented local fallback. `branch-failure-cap-and-denial` covers >100 groups,
finite complete-scan-cap exhaustion via a test seam, failed child gates and
cancellation before any unauthorized leaf dispatch.

`native-local-average-parity` reuses T07's fractional/sub-one/empty and unequal
partition goldens with identical SQL semantics. `sum-count-average-reuse` proves
native branch reuse, including auxiliary counts for null normalization.

**Benchmark:** P(T08) compares native split/merge, forced-local same-semantics
reference and equivalent individual native queries on a fixed dataset. Include
AVG alone and SUM+COUNT+AVG for integer/float targets, reporting raw fallback cost.
G3: eligible AVG needs at most two distinct leaves, exposing its matching SUM/COUNT
adds no leaves, and unsafe AVG makes one local raw scan. For other combinations,
leaf count equals unique eligible visible/auxiliary calls (three above).
Intermediate DataPoints are zero; merge work <= sum of input group rows plus one
output visit per group; no branch receives a fresh full budget. K(T08) covers
different branch order and high key cardinality;
G1/G2 apply. Record split-versus-raw latency/cost without assuming pushdown wins.

**Checks/readiness:** E(T08), P(T08), K(T08); focused `./pkg/query/ql`,
`./pkg/bydbql`, `./banyand/liaison/grpc`. Blocked by T07. Fixed-data oracles must
not imply a common snapshot across separate native queries during concurrent writes.

### T10 — Evaluate typed vector expressions and masked conditionals

**Performance question:** does compiling scalar-or-column kernels avoid per-row
interpreter cost, and does masked evaluation save work when branches are unused?

**Boundary/activation:** SELECT expressions activate the shared typed evaluator:
arithmetic, decimal/exponent floats, unary signs, CAST/::type, comparisons,
AND/OR/NOT, IS NULL, IN, CASE and COALESCE. Preserve ::TAG/::FIELD and accepted
legacy identifiers containing '-'/'*'. WHERE/HAVING use these kernels in T12/T14.
Constants remain scalars; selections use one physical index space (nil=all,
empty=none); split before uint16 capacity, skip zero-active batches and evaluate
conditional arms only for eligible rows. No new parameter wire types.

**Numeric decisions:** INT/BIGINT=int64, FLOAT/DOUBLE=float64. Checked integer
arithmetic; int/int division truncates toward zero, mixed numeric operations
promote to double. Division by zero, overflow and invalid/out-of-range casts fail
with InvalidArgument. Double-to-int truncates after finite/range checks. No implicit
string conversion; CAST(? AS DOUBLE) accepts bound strings. Null propagates.
Pin timestamp units/timezone and accepted cast pairs in binder tests.

**E2E cases:** `arithmetic-F`: v+2={2,4,6,NULL,8,NULL}; `cast-parameter` changes bound
string doubles; `integer-division` gives -5 / 2 = -2; `numeric-errors` covers invalid
casts, overflow and zero division; `lexer-compat` preserves legacy names. Boolean
output needs a supported cast; timestamps/arrays use existing response mapping.
`masked-case`: CASE WHEN v=0 THEN 0 ELSE 8 / v END={0,4,2,NULL,1,NULL};
`coalesce-F`={0,2,4,-1,6,-1}; `three-valued-cast` distinguishes true/false/null;
`unreachable-invalid-arm` never raises errors from masked rows. Cover typed null,
empty/non-empty IN and changed parameters under repeated execution.

**Benchmark:** P(T10) measures expression projection through RPC; K(T10) compares
independent scalar evaluation at batches 1/1,024/65,536 with dense/selected/null
inputs. Vary constant/column/parameter expressions, null density, branch depth
and first-arm hit rates 0/50/100%; measure ns/active-row, allocations and RPC cost.
G3: constants allocate no full-length arrays; resolve offsets/types once per batch;
100% first-arm matches evaluate zero later-arm rows; median kernel ns/active-row
<= scalar reference at batches >=1,024. G1/G2 apply. Error cases prove correctness
within this capability; successful expression execution supplies its benchmark.

**Checks/readiness:** E(T10), P(T10), K(T10); focused `./pkg/bydbql`,
`./pkg/query/ql`. Blocked by T03. Numeric policy, masking, casts and predicate
kernels form one usable expression layer with a real projection caller.

### T12 — Filter raw fields before grouping, projection and final limits

**Performance question:** how do residual filtering and downstream aggregation
cost change with selectivity, hidden-column width and complete native input size?

**Boundary/activation:** capability classification keeps field predicates and
inexact mixed predicates above the complete native raw scan. QL Filter narrows
selection; local aggregates consume only surviving rows. Preserve required hidden
inputs and forbid child aggregate/limit pushdown across any residual.

**E2E cases:** `field-filter-F`, v > CAST(? AS DOUBLE) with "1", selects rid {2,3,5};
`filter-before-aggregate` returns SUM(v)=12 and COUNT(v)=3; `float-filter` f > 0.5
selects {2,3,5,6}. `time-field-or` proves an exact time envelope alone is insufficient.
`filter-before-limit-and-hidden-input` uses >100 rows with matches after row 100,
omits the filtered field from SELECT and still returns the correct final limit.
FALSE and UNKNOWN both drop rows. Test a low-budget cancellation/error path.

**Benchmark:** P(T12) varies selectivity 0/1/50/100%, raw projection versus local
aggregation and hidden-column widths. G3: exactly one raw leaf, all required input
rows examined (no intermediate 100/final-limit cap), zero native aggregate leaves
when a residual remains, and aggregate input count equals filter TRUE count.
K(T12) reports filtered rows/s and copied bytes; G1/G2 apply.

**Checks/readiness:** E(T12), P(T12), K(T12); focused `./pkg/query/ql`,
`./pkg/bydbql`, `./banyand/liaison/grpc`. Blocked by T07/T10. Float criteria never
become unsupported tag-only protobuf criteria.

### T14 — Execute grouped queries with HAVING and membership predicates

**Performance question:** what does filtering completed groups cost, and do hidden
or repeated aggregates trigger unnecessary native work or result materialization?

**Boundary/activation:** post-aggregate QL Filter resolves grouped expressions and
visible/hidden aggregate calls, evaluates three-valued truth and removes hidden
slots at egress. HAVING precedes final pagination and follows complete merge.
SELECT aliases are outside HAVING scope; reject nested aggregates/non-grouped
sources. Include CONTAINS/NOT CONTAINS as aliases of legacy infix HAVING/NOT HAVING
membership, preserving placeholder/list semantics. Clause and infix positions
remain unambiguous. This spelling compatibility is a subtask, not its own ticket.

**E2E cases:** `having-F`, SUM(v)>3 by host, returns b/c with totals 4/6;
`hidden-aggregate` selects only host and exposes no SUM field; `having-null`
handles null-only groups. `membership-and-having` combines membership WHERE with
post-aggregate HAVING and a T12 residual variant. Invalid references fail before
dispatch. For labels ['red','blue'], ['blue'], [] on three non-null rows,
`contains-single` red returns the first row; `not-contains-single` returns the
other two. Both spellings and scalar/list/array/null bindings follow independent
legacy membership expectations.

**Benchmark:** P(T14) measures the full native/local aggregate -> HAVING pipeline
at group selectivity 0/1/50/100%, hidden/repeated aggregate mixes, and membership
array lengths 0/1/16/256. Compare against equivalent aggregation without HAVING
and separately account for fewer output rows. K(T14) measures filtering group
batches. G3: repeated SELECT/HAVING calls add zero branches; filter input equals
completed groups, not raw rows; hidden slots are never serialized. Both membership
spellings produce the same work; their alias alone is not the performance claim.
G1/G2 apply; report group-filter CPU, retained state, output bytes and RPC latency.

**Checks/readiness:** E(T14), P(T14), K(T14); focused `./pkg/bydbql`,
`./pkg/query/ql`. Blocked by T08/T10/T12. Grammar compatibility, reference errors
and membership cases are acceptance details of executable grouped queries.

### T15 — Implement DISTINCT on the complete visible tuple

**Performance question:** how do distinct-tuple throughput and retained key bytes
scale with duplicate ratio, cardinality and variable-width values?

**Boundary/activation:** DISTINCT lowers to T07's group table with visible
projection slots and no aggregate states. Hidden sort inputs cannot affect
equality; reject ORDER BY expressions not derivable from visible DISTINCT output.
Null equals null for grouping, distinct from zero/empty string; signed zero is
normalized and all NaNs share one group key.

**E2E cases:** `distinct-zone-F` returns {x,y,NULL,empty string}; `distinct-tuple`
uses two columns with deliberate concatenation collisions and replicated input;
`distinct-numeric` covers null/zero/-0/NaN using independently specified writes;
`distinct-before-limit` deduplicates >100 raw rows before final pagination.
Timestamp/array tuples use explicit structural keys when their type is admitted;
unsupported projected key types fail during binding.

**Benchmark:** P(T15) covers 1%, 50% and 100% distinct tuples, narrow/wide strings
and empty input. G3: group count equals unique visible tuples, aggregate state
count is zero, and retained key storage scales with unique tuple bytes rather
than duplicate count. K(T15) measures hashing/equality throughput; G1/G2 apply.

**Checks/readiness:** E(T15), P(T15), K(T15); focused `./pkg/query/ql`,
`./pkg/bydbql`. Blocked by T07/T10. The legacy first-row-per-group operator is not
an acceptable replacement for tuple DISTINCT.

### T16 — Evaluate computed group keys and resolve GROUP BY ordinals

**Performance question:** what is the cost of evaluating computed keys and feeding
them to grouping, compared with equivalent precomputed/simple keys?

**Boundary/activation:** bind group ordinals after star expansion, validate them,
evaluate each distinct key expression once per input batch and feed T07's group
table. Match SELECT/group expressions structurally. Expose TIME_BUCKET outputs
through the existing native bucket semantics/response mapping; bucket time remains
part of merge identity when native aggregation is eligible.

**E2E cases:** `computed-group-F` groups by CASE WHEN v >= 4 THEN 'high' ELSE 'low'
and returns sums high=10, low=2; `ordinal-equivalence` GROUP BY 1 gives the same
result. `ordinal-errors` rejects zero, negative, out-of-range and aggregate targets;
`bucket-projection` uses 2-second buckets with SUM(v) {2,4,6} at T,T+2s,T+4s.
Test group-only queries, null keys and cache hits with changed expression params.

**Benchmark:** P(T16) compares equivalent explicit/ordinal grouping and simple/
computed keys across low/high cardinality. G3: explicit and ordinal forms have
identical physical work counts; one evaluation per unique group expression per
active row, one raw scan for computed grouping, and no per-row AST/name lookup.
K(T16) measures key evaluation plus grouping; G1/G2 apply.

**Checks/readiness:** E(T16), P(T16), K(T16); focused `./pkg/bydbql`,
`./pkg/query/ql`. Blocked by T05/T08/T10. Calendar functions/windows/gapfill are excluded.

### T17 — Order and paginate results with full sort and compact top-k

**Performance question:** when does finite-limit top-k reduce CPU and retained
bytes versus full sort, and do wide rows/large offsets keep state bounded?

**Boundary/activation:** one typed lexicographic comparator serves full sort and
ORDER BY + finite LIMIT/OFFSET. Resolve keys once; preserve exact int64 comparison,
bytewise strings, per-key ASC/DESC and NULLS FIRST/LAST. Defaults are ASC NULLS LAST
and DESC NULLS FIRST; NaNs follow other non-null numerics in ascending order.
Equal complete keys have unspecified order; add unique tie keys to test goldens.
Checked uint64 k=OFFSET+LIMIT selects compact top-k; copy candidates into owned
storage so a retained row does not pin its source batch. Final pagination follows
all filter/group/distinct work. LIMIT 0 selects Empty. Both modes are budgeted;
no spill or unbounded fallback on exhaustion.

**E2E cases:** `sort-F`, v DESC NULLS LAST, rid ASC, returns {5,3,2,1,4,6};
`alias-and-hidden-sort` checks alias precedence/output exclusion; `group-sort`
returns c,b,a by SUM(v) DESC; `exact-wide-values` covers int64s above 2^53,
strings/nulls/NaNs. `distinct-order-scope` applies T15 restrictions.
`topk-F` adds LIMIT 2 OFFSET 1 and returns {3,2}; `topk-full-sort-parity` includes
ties, nulls and hidden keys. `limit-zero-and-overflow` checks no scans/checked k;
`distinct-having-pagination` prevents early pagination. `wide-row-cancel-budget`
keeps sparse candidates from large batches and verifies eviction/Close/error
cleanup. Test full-sort budget exhaustion too.

**Benchmark:** P(T17) measures full sort and top-k on 1/3/8 keys, sorted/reverse/
tied/random data, narrow/wide rows, k=10/100/1,000 and large offsets. Include 1 MiB
source batches with one retained narrow candidate each. K(T17) compares independent
tuple sorting with full sort, and top-k with full sort plus identical slicing.
G3: sort expressions evaluate once per row; for N>=1,024 comparator calls stay
<=4*N*ceil(log2(N)) on the full-sort workloads. Candidates <=k; candidate buffers
<=2x live payload plus declared bounded descriptor/heap capacity; zero source-batch
pins after copying and releasing current input. Separate native frame residency.
For N>=100,000 and k<=100 on the same random data, top-k median ns/op <=0.80x full
sort+slice. G1/G2 apply; report complete RPC and retained-memory costs as well.

**Checks/readiness:** E(T17), P(T17), K(T17); focused `./pkg/query/ql`,
`./pkg/bydbql`, `./banyand/liaison/grpc`. Blocked by T05/T12/T14/T15/T16 so composed
cache/time/filter queries have all prerequisites. Full sort can be an internal
implementation step/reference, but the ordering ticket closes with both modes.

## Parent completion and composed workloads

The parent closes only when every child has its E/P evidence and relevant K
records, and the following composition cases run in both topologies. Attribute
their ownership to the final implementing child (normally T17); they do not
become an unbounded extra implementation ticket.

```sql
SELECT host AS node, SUM(v) AS total, MAX(v) AS peak
FROM metrics.qlb
WHERE time >= ? AND v > CAST(? AS DOUBLE)
GROUP BY host
HAVING SUM(v) > 3
ORDER BY total DESC NULLS LAST, node ASC
LIMIT 1 OFFSET 1
```

On F with start T and threshold "1", this returns node b, total 4, peak 4.
Change the same query's threshold to "4": HAVING leaves only c, OFFSET removes
it, so the warm-cache result is empty. Change the start time to exclude c and
test another empty result. Also compose CASE/COALESCE + ordinal grouping + AVG,
and visible-tuple DISTINCT + multi-key top-k. Check cold/warm/uncached equivalence,
fresh authorization, errors, traces and no cross-request mutable state.

Run the complete `measure-ql` label in both suites using the E commands without
the ticket filter. Run existing measure/stream/trace/property/TopN compatibility
cases affected by dispatch changes, and focused race/owner tests. Benchmark the
composition as an additional `qlb_t17` variant with changing OAP parameters,
workers 1/8/32 and G1/G2. Report planned-versus-observed cache savings, native
pushdown coverage, residual scan cost, and peak/live/cumulative memory separately.

Finally compare implementation changes against the PR #1412 base: protobuf and
generated API files, native transport/frame encoders, persisted formats and
data-node execution behavior must be unchanged. Go-only extraction of distributed
batch results must retain existing native RPC golden results. Every feature has
real endpoint proof and performance evidence; a green parser test or an empty
benchmark selection cannot close a ticket.
