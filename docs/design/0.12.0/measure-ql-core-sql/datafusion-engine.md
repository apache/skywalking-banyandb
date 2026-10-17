# DataFusion-inspired measure QL engine

Status: **implemented through T08 in PR 2.** Shared batches, typed raw queries
and compiled-plan reuse are merged through PR #1417. This branch adds local SQL
aggregation and eligible native aggregate split/merge. Expressions, HAVING,
DISTINCT, computed grouping and general sorting remain later PRs. Current scope
and evidence are in the [PR 2 verification record](pr2-verification.md).

This refines the [Catalog B implementation plan](README.md), originally reviewed
against PR #1412 at `012115c5`. The companion [research note](datafusion-research.md)
records primary sources and the examined DataFusion revision. SQL extensions run
in Go on the liaison and share native batches and existing native operators.
Protobuf definitions, native request contracts, persisted formats and frame v4
wire encoding remain unchanged by this PR.

## 1. Decision

Adopt DataFusion's separation of typed logical expressions, logical plans, physical
expressions and batch execution. Keep PR #1412's Arrow-style `RecordBatch` as the
intermediate carrier. QL operators consume columnar input and produce columnar
output; only the final existing Query response adapter creates DataPoints.

DataFusion is a Rust query engine built on Arrow. Its modules are useful references,
but importing the engine would introduce a second runtime and language integration.
For Catalog B, adapt the architecture and algorithms in Go. Do not embed Rust,
replace the QL parser, introduce Arrow IPC, or convert every batch into Arrow Go
objects merely to obtain an Arrow-shaped interface.
[DataFusion project](https://github.com/apache/datafusion),
[extension architecture](https://datafusion.apache.org/contributor-guide/architecture.html).

| Option | Reuse gained | Cost | Decision for B |
|---|---|---|---|
| Existing Go batches + DataFusion-inspired planner/operators | PR #1412 buffers, owners, scans, frames and lifecycle | Implement the missing SQL semantics/kernels | Recommended |
| Apache Arrow Go as the internal batch SDK | Standard arrays, builders and available compute kernels | Adapt every operator; reconcile constants, selections, null representation and ownership; budget kernel allocations | Revisit if a measured kernel/interchange need justifies migration |
| Embed DataFusion through FFI | Rust planner, optimizer and execution operators | Rust toolchain/runtime, language bridge, cancellation/memory/error integration, QL syntax and native scan adapters | Outside B |

Arrow Go arrays alone do not supply the SQL planner or all of DataFusion's
operators. An Arrow buffer interface also does not provide a ready Go-to-DataFusion
query API. The latter requires an execution integration, not just compatible data.
[Arrow Go](https://github.com/apache/arrow-go),
[Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).

## 2. What PR #1412 already provides

| Existing code | Reuse | Limitation to address in QL |
|---|---|---|
| [RecordBatch](../../../../pkg/query/vectorized/batch.go) | Equal-length columns, active-row selection, borrowed owners | Selection is an extension to the Arrow batch model; `nil` and empty mean different things |
| [TypedColumn](../../../../pkg/query/vectorized/typed_column.go), [ConstColumn](../../../../pkg/query/vectorized/const_column.go) | Flat typed arrays and scalar repetition; borrowed numeric buffers | Some operators assume flat columns; constants must not be expanded unconditionally |
| [BatchSchema](../../../../pkg/query/vectorized/schema.go) | Immutable physical layout, source roles | SQL needs nullable logical types, stable slot identities and separate display names |
| [PullOperator/Pipeline](../../../../pkg/query/vectorized/operator.go) | Pull, fuse and breaker lifecycle | SQL operators need the same cancellation and ownership discipline |
| [Frame v4](../../../../pkg/query/vectorized/frame/columnar.go) | Column buffers and owner-pinned decoding | Custom envelope and constant encoding, not Arrow IPC |
| [BatchAggregation](../../../../pkg/query/vectorized/measure/aggregation.go) | Multi-spec accumulation structure, constants/runs | Carries non-key tags, legacy numeric/empty semantics; not a drop-in SQL aggregate |
| [BatchGroupBy](../../../../pkg/query/vectorized/measure/groupby.go) | Key lookup and row retention patterns | `appendKeyComponent` omits null markers; null can equal a zero placeholder; arrays unsupported |
| [BatchTop](../../../../pkg/query/vectorized/measure/top.go) | Heap selection approach | One numeric key, fixed null-low order, row-per-entry storage; not general SQL sort |
| [BatchLimit](../../../../pkg/query/vectorized/measure/limit.go) | Selection slicing | uint32 counters and finite-window contract; SQL needs presence-aware limit and checked wider counts |

"Arrow-style" here means schema plus typed columns, validity information and
batch kernels. It does not imply binary interoperability: in-memory null bits
currently mean null rather than valid; strings use Go string slices; frame v4
has Arrow-like offset/value buffers but also its own envelope. Preserve those
working representations and translate only at a real interchange seam.
[Arrow columnar specification](https://arrow.apache.org/docs/format/Columnar.html).

## 3. SQL schema and vector expression contract

Associate each physical batch with an immutable QL schema: stable `SlotID`, logical
type, nullable flag, output name, source identity and final-response mapping.
Physical execution resolves slots to column offsets once. Aliases change display
metadata, not identity or native field names. Timestamp has a logical unit/timezone
even when physically int64; boolean is a distinct logical type rather than an
untyped 0/1 convention.

Add a liaison-local boolean column/bitmap and scalar value support. If this needs
an additive in-memory ColumnType/constructor, do not assign it a frame or protobuf
encoding; verify native serialization remains unchanged. QL-only types never reach
native scan requests. General role/name assumptions in shared serializers must
not leak into the SQL schema; apply the existing response mapping only at egress.

A compiled expression evaluates one batch to `Scalar` or `Column`, with an explicit
logical type and nullability. Constants, bound parameters and captured now remain
scalars until an operator actually needs an array. Physical expressions contain
column offsets and typed kernels, with no AST walks or schema-name maps per row.
This adapts DataFusion's `ColumnarValue` design; PR #1412's ConstColumn supplies
much of the corresponding storage behavior.
[ColumnarValue](https://docs.rs/datafusion-expr/latest/datafusion_expr/enum.ColumnarValue.html).

Every expression honors the input selection. The first implementation uses one
physical index space: column results have the batch's physical length, only active
rows need evaluation, and the output preserves selection. A gather/sort may
compact into dense output, resetting selection. Kernels dispatch on type once and
loop over active rows. No `[]map[string]any`, `[][]any` or per-cell protobuf dispatch.

Normalize native TagValue/FieldValue passthrough columns into typed columns once
at the leaf when an expression needs them; arrays carried by native protobuf cells
must be decoded there. They are not valid generic SQL kernel inputs. Split batches
before uint16 selections exceed 65,536 positions, and skip zero-active batches
without treating them as end-of-input.

CASE and COALESCE evaluate only still-eligible rows in each arm. For example,
`CASE WHEN value = 0 THEN 0 ELSE 1 / value END` must not divide by zero on rows
handled by the first arm. AND/OR/NOT use validity plus boolean values for SQL's
three-valued logic. Constant folding must respect those masks and avoid evaluating
unreachable error-producing branches. Float literals and explicit string-parameter
casts stay inside this expression layer; no new parameter wire variants.

Projection may borrow input buffers, but pinning Owners alone does not make a
shared pooled Column object safe: resetting that object can destroy the view.
Use independently owned column views/descriptors and retain their backing owners,
or copy when a safe view is unavailable. Batch release returns descriptors and
buffers exactly once. This is required for aliases and repeated column projections.

T10 implements this contract in `pkg/query/ql/expression*.go`. Immutable,
schema-resolved recipes retain symbolic parameters in the compiled template;
binding creates fresh typed expressions, and operator construction chooses
typed kernels and source offsets. Boolean values stay local to the evaluator,
without a native ColumnType or encoding change. A batch-local protobuf boundary
decoder handles only selected rows; borrowed source slices and owned scratch
buffers remain separate. CASE, COALESCE and boolean short-circuiting pass physical
row masks to children, including constant expressions that may fail if evaluated.

Native sources already emit at most 65,536 physical rows. The public expression
operator rejects an oversized external batch before constructing uint16 row
indices, rather than silently losing rows. Scalar numeric/string/bytes results
reach final egress as ConstColumn. Constant array payloads remain scalar through
evaluation; final projection allocates tracked row headers for the existing
serializer, whose native ConstColumn does not support arrays. This is an egress
compatibility cost, recorded separately from the scalar-constant kernel gate.
Computed timestamps use UTC Unix nanoseconds and become timestamp TagValues
only in the final QL adapter. See the [language guide](../../../interacting/bydbql.md#57-typed-select-expressions)
for the checked arithmetic, cast, null and result-mapping rules.

T12 reuses those compiled kernels in `WrapFilter`. A BOOL result narrows selection
without copying output value columns; only TRUE passes. The operator borrows the
input schema, columns and metadata until the next pull, skips empty selections,
and charges typed decoding scratch, row masks and selection storage before
allocation. The output-copy field is structurally zero, verified by column-identity
checks in unit tests and the kernel benchmark; it is not measured copy telemetry.
Charged decoding scratch is outside that invariant.
EOF, evaluation errors, cancellation and repeated Close release state and close
the source exactly once. Oversized external batches reject before uint16 conversion.
The filter owns the source explicitly; its shared expression kernel owns scratch.
Planner and runtime share BOOLEAN/NULL predicate validation.

The planner keeps safe necessary tag predicates and a conservative time envelope
in the native request, then evaluates the full residual before visible projection,
T07 aggregation and final pagination. A time branch under OR cannot exclude rows
matched by another branch. Hidden predicate inputs survive time filtering but
never reach final output. A residual disables T08 native aggregation and input
LIMIT/OFFSET pushdown. Cached recipes retain symbolic values; each execution binds
fresh parameters and one clock. Legacy tag-only MATCH and collection HAVING keep
their original routing. T14 adds typed collection membership composition with
new residuals; MATCH composition remains rejected.
Names-only routing preserves ordinary legacy queries across groups whose unused
column types differ. Typed schema validation runs when typed execution is needed.
One input collector extends the native scan with hidden columns without changing
visible slots; SELECT and WHERE share array binding and tag ambiguity metadata.
See the [WHERE contract](../../../interacting/bydbql.md#58-field-and-expression-filters).

T14 adds immutable `HavingPlan` metadata to the same execution recipe. Binding
resolves source grouping keys and aggregate calls, deduplicates repeated calls,
and retains hidden keys/calls until the predicate has run. SELECT aliases do not
change HAVING name resolution. Local aggregation and the completed native merge
both feed the shared filter kernel before visible projection and final pagination.
Group-only HAVING uses local grouping; scalar empty aggregation still produces its
SQL empty row before the predicate. No child pagination can remove input groups.

The runtime exposes separate raw WHERE and HAVING statistics even though both
stages share evaluation, borrowing and memory accounting. Each bound execution
owns fresh scratch and reservations; cached recipes retain no bound values or
operator state. CONTAINS/NOT CONTAINS reuse legacy all-terms membership rules,
including scalar, duplicate, null and empty-list behavior. Typed unindexed arrays
work through the existing batch decoder. The QL dispatcher checks current binding
and rule metadata before native dispatch, including cache hits, and rejects typed
membership on indexed arrays. It does not filter binding validity windows because
native schema caches also apply those bindings. Scalar/no-membership plans skip
the metadata check. This preserves the native storage boundary while preventing
new predicates from silently filtering truncated arrays. The new operator spelling
is contextual and measure-only. The
[T14 verification record](t14-verification.md) preserves the reproducer and boundary.

## 4. Plan stages and the small optimizer

Keep the public QL entrypoint small: prepare an immutable template, compile/bind
against the existing schema source, and execute through an injected native scan
adapter. Internally, use these stages:

```text
compiled-plan lookup + validate schema dependencies
    -> miss: QL AST + schema source
             -> analyze names, types, grouping and parameter constraints
             -> value-independent rewrites + immutable physical recipe
    -> bind parameters / one execution clock
    -> guarded value-dependent choices + fresh batch operators / native requests
    -> authorize all leaves
    -> execute batches -> final measure_result serialization
```

The logical nodes needed for B are `Scan`, `Filter`, `Projection`, `Aggregate`,
`Distinct`, `Sort`, `Limit` and `Empty`. Aggregate has a list of calls and group
expressions, independent of the native one-aggregate request restriction. A physical
`AggregateMerge` implements split-request B3; a local `HashAggregate` implements
residual field filters/computed groups. Neither belongs in the parsed grammar.

Start with ordered rules, not a general cost-based search:

1. Bind names/types and insert explicit coercions; expand ordinals and reject
   illegal aggregate references before any execution.
2. Fold safe literals and normalize time expressions symbolically. Resolve
   parameter/time-dependent constants and contradictions only per execution.
3. Intern repeated expressions/aggregate calls, preserving requested output names.
4. Push required projections, including hidden filter/group/order inputs.
5. Classify predicates and move only exact predicates entirely into the scan.
6. Choose native aggregation plus keyed merge or local aggregation by capability.
7. Rewrite DISTINCT to grouping on visible output slots with no aggregate calls.
8. Combine ORDER BY + LIMIT/OFFSET into top-k; retain final offset/limit semantics.

Keep logical and physical trees printable for tests and QUERY_TRACE. Do not add a
new EXPLAIN RPC or build join reordering, a statistics framework or external sort
for B. DataFusion's optimizer provides the separation and
rule-testing model; our rule set is deliberately smaller.
[Optimizer design](https://datafusion.apache.org/library-user-guide/query-optimizer.html).

Cache the immutable schema-bound template, including typed expressions and physical
recipes, for repeated parameterized measure queries. This includes existing OAP
query shapes. Validate dependencies on hits and create new requests/operators for
each execution; no running operator or parameter-dependent result is cached.
The [plan reuse design](plan-reuse.md) defines keys, coherent schema capture,
invalidation, concurrency, memory bounds and performance validation. It supersedes
the earlier deferral of compiled caching for the simpler PR #1412 transformer.

## 5. Native scan provider and the batch-preserving seam

Model native pushdown after DataFusion's `Exact`, `Inexact`, `Unsupported` filter
contract. Exact means the native request enforces the entire predicate with the
required null/type semantics. Inexact means it only prunes a superset, such as a
time envelope around an OR; the complete residual filter stays above the scan.
Unsupported means liaison evaluation. Inexact/residual filters prevent LIMIT and
aggregate pushdown below them. Projection must retain residual inputs even if
absent from SELECT.
[TableProvider contract](https://docs.rs/datafusion-catalog/latest/datafusion_catalog/trait.TableProvider.html).

Expose additional local facts for safe planning: supported aggregate/type/key
combinations, whether rows are globally deduplicated/reduced, and proven global
ordering. Per-series `BatchHints.TimeOrder` is not global ORDER BY evidence. Do
not drop a sort because timestamps happen to be ordered inside each series.

The existing native path is already columnar until its last adapter:

```text
DistributedPlan.executeRows / executeRowsMultiGroup / executeAgg
    -> merged or reduced RecordBatch values
    -> iteratorFromBatches[WithSchema]
    -> vectorizedMIterator.Next -> serializeBatchToProto
    -> dquery.measureQueryProcessor.Rev collects DataPoints
    -> measureService.Query returns protobuf response
```

Factor the common distributed execution into a Go-only batch-returning helper
before `iteratorFromBatches` constructs `MIterator`. Both existing native RPC
collection and QL NativeScan use that helper. The returned handle owns output
schema, batches, trace/status state and Close. Native RPC callers retain their
existing limit/hidden-column adapters; QL callers retain required hidden slots and
apply SQL pagination later. Lower child requests using the existing complete-scan
sentinel; no zero-as-unbounded assumption and no loss of native node-level logic.
The sentinel is still a finite native count: if an input reaches that cap without
proof of exhaustion, fail explicitly rather than certify a complete SQL input.

Factor liaison group lifecycle/revision gates and dquery schema/topology setup so
both adapters run the same preparation. Wire the batch executor through a Go
interface/in-process module, never across the cluster transport. Authorization
still examines lowered native requests before any dispatch. Preserve scope,
stage, revision, cancellation, trace and cleanup behavior.

Do not call `measureService.Query`, collect DataPoints, and reconstruct batches
for each QL leaf. Also do not use RawFrameSource as a universal shortcut: hidden
column iterator wrappers do not uniformly preserve it. The seam must be explicit
and tested for raw, aggregate, empty and multi-group outputs.
[Distributed execution](../../../../pkg/query/vectorized/measure/plan/distributed.go),
[iterator adapter](../../../../pkg/query/vectorized/measure/adapter.go),
[dquery collector](../../../../banyand/dquery/measure.go).

This change exposes existing batch results; it does not make native broadcast and
reduce fully streaming. PR #1412 still retains frames/batch slices in places. Count
that residency and fail within the query budget. Streaming those internals is a
separate optimization, not a promised benefit of the pull interface.

## 6. B3 aggregation and B6/B9 grouping

Preserve the catalog's native strategy: eligible aggregate calls become separate
native requests, and `AggregateMerge` aligns returned **batches** by the complete
SQL grouping tuple. It owns a group-ID table and columnar output slots, not a map
of protobuf rows. Repeated calls share a branch; aliases remain separate output
slots. Missing branch/group values, empty scalar results and branch failure are
explicit states, not positional joins.

For computed group keys, aggregate arguments, or residual WHERE, use one native
raw scan followed by local vector evaluation and HashAggregate. Give the group
table each active row's typed key and obtain dense group IDs. Each aggregate owns
compact state arrays indexed by group ID: sum, non-null count, extrema/seen flags,
or exact distinct sets. Do not instantiate a heap object per aggregate per row.
DataFusion's grouped-accumulator approach is the reference; its full partial-state
wire/distributed machinery is not needed here. See the pinned sources in the
[research note](datafusion-research.md).

Use one QL key codec for aggregate merging, DISTINCT and computed grouping. Encode
logical types, explicit null markers and variable-length boundaries; define array
structure if admitted. Null, zero and empty string must differ. Normalize signed
zero; choose and test one NaN equivalence policy consistently across hashing and
comparison. For QL grouping/DISTINCT, canonicalize all NaNs together and place them
after other non-null numerics in ascending total ordering. SQL predicate equality
and group-key equality remain separate operations.

DISTINCT is HashAggregate with visible projected slots as keys and zero aggregate
states. Group-only B9 uses the same path. This shares memory accounting and key
semantics without copying the legacy first-row-per-group behavior. The legacy
key codec cannot implement this correctly as-is; keep the SQL codec liaison-local.

Do not silently inherit native numeric or empty-input quirks. The extended QL
contract is COUNT -> int64, AVG -> float64, empty scalar count -> 0 and other
empty scalar aggregates -> null; grouped empty input emits no groups. AVG uses
sum/count state, never native MEAN's clamped final value. A native pushdown is
eligible only if precision and null/empty behavior match or can be normalized
without information loss. Nullable SUM/MIN/MAX may need auxiliary exact non-null
COUNT state to distinguish an all-null group from a real zero or an extremum;
deduplicate these auxiliary calls with visible calls. If native results cannot
provide that state exactly, choose the local path. Converting an imprecise float COUNT back
to int64 cannot restore precision: use local int64 counting when exact native
counting cannot be established. Native grouping's missing null markers likewise
preclude SQL pushdown when group keys may be null: require a proof that those
keys are non-null, or group raw batches locally. A liaison merge cannot recover
groups already collapsed on the data node. Existing schema definitions have no
NOT NULL flag: a proof must use a verified provider guarantee or an exact native
predicate excluding nulls, not the observed contents of a test fixture.
Record this restriction in plan traces
and benchmarks; do not repair native operators or hide the extra scan cost in B.
Any branch using SUM/COUNT for AVG shares one
bound clock/filter, but multiple requests still have no common storage snapshot.

Preserve legacy single-request behavior on its existing path. Mark extended
operator plans' numeric semantics explicitly in the documentation; do not make
semantic choices depend on a runtime optimizer toggle. Reference-mode tests must
compare identical semantics with pushdown enabled or disabled.

## 7. B4/B5/B7/B8 operators and reuse choices

| Operator | Mechanism | Existing code decision |
|---|---|---|
| Filter / HAVING | Boolean vector + validity narrows selection; HAVING consumes aggregate slots | New QL kernels; reuse selection/lifecycle helpers |
| Projection / aliases | Compiled scalar-or-column expressions, metadata aliases, safe borrowed views | Reuse typed/constant columns and owners |
| CASE / COALESCE / CAST | Typed, masked batch evaluation; checked conversions | New kernels; no row-protobuf interpreter |
| Sort / TopK | Shared lexicographic comparator over several slots, per-key direction/null policy | New QL sorter; reuse heap pattern, not BatchTop's fixed comparator |
| DISTINCT / GROUP BY | One null-aware group table; emit key columns only when no aggregates | Reuse batch allocation patterns, not the legacy key codec |
| OFFSET / LIMIT | Selection slicing, uint64 counters, absent versus zero | Adapt helper locally; no native contract change |

For sorting, evaluate keys once per batch. Use compact row references into retained
column buffers and emit gathered batches; retained source bytes must be charged.
For top-k, clone accepted candidates into a compact retained batch/arena so keeping
one row does not pin an unbounded number of entire input blocks. Eviction releases
or reuses that candidate's storage. Bound by retained bytes as well as k. No
unbounded full sort fallback after exhausting memory.

ORDER BY defaults, hidden keys, output aliases, field-filter-before-aggregation,
HAVING membership compatibility, cast syntax and existing response limitations
remain as defined in the [scope plan](README.md). New boolean values may participate
in computation; final protobuf output still requires a supported value mapping.

## 8. Shared budget and ownership

Adopt the memory-reservation principle, not DataFusion's runtime. One QL execution
owns a parent admission lease and shared operator accounting. Existing
`DistributedPlan` creates separate trackers in several paths; merely passing the
same context will not share those limits. Inject one accounting context through
native batch execution, keyed merge and QL operators. No branch receives a fresh
full query budget.

Current `query.BudgetLease` supports Charge but no refund, whereas MemoryTracker
supports Reserve/Release. Do not pretend their interfaces already match. Add a
Go-only accounting adapter that reserves before allocation and records live
operator ownership; until lease refunds exist, lease charging remains conservative
and monotonic. Avoid duplicate charges when ownership transfers between operators.
Budget exhaustion must remain an error even if the cumulative accounting is more
conservative than peak live memory. A reclaimable lease redesign is not required
for Catalog B.

Charge decoded/native-retained batches, SQL columns, key/state buffers, merge
output, top-k candidates and final response separately by owner. Returning a batch
from an operator does not free its memory: transfer the reservation to its consumer
and release on actual disposal. Reuse Owners and clone/retain rules; cancellation
and early LIMIT must close unopened/active branches and all retained data.

## 9. Delivery and evidence

The [implementation tickets](implementation-tickets.md) refine these milestones
into 11 executable capabilities with real RPC end-to-end and benchmark gates.
Every benchmark measures its capability's execution cost. The completed T01
verification infrastructure supports T02's batch cutover; syntax/error/helper
work remains inside its owning capability rather than a separate ticket.

1. **Batch seam first:** factor native batch execution and compare final serialized
   output to the existing path. Prove no per-row protobuf creation before final QL
   egress. Cover hidden order/top columns, empty schemas, multi-group types,
   revision gates and replica deduplication.
2. **Typed plan + B1/B2 + reuse:** schema slots, analyzer, provider capabilities,
   scalar/column expressions, aliases and time lowering. Add the bounded compiled
   cache and template/instance separation here; prove repeated parameterized legacy
   and extended queries reuse analysis and physical recipes safely.
3. **B3:** first activate local SQL grouping/aggregate state for cases requiring
   exact COUNT or nullable keys; then native multi-request AggregateMerge,
   SQL null/empty normalization and AVG finalization, exactness guards and budgets.
4. **Expression kernels + B4/B7/B8:** masked evaluation, HAVING, field filters,
   raw-input local aggregation. B8 now exercises the same evaluator rather than a
   separate later filter engine.
5. **B6/B9:** shared group table, DISTINCT and computed/ordinal grouping.
6. **B5:** lexicographic sort/top-k and final pagination; then composition tests.

Each stage uses golden logical/physical plan tests plus result tests. Compare
pushdown and local reference execution on fixed data, with batch partitions of
1, 1024 and storage block sizes; dense/selected/empty/constant inputs; null versus
zero/empty; aliases and repeated expressions; all grouping keys; overflow and NaN;
and CASE branches that would error if eagerly evaluated. DataFusion SQL examples
are useful test inputs only where the dialect/semantics match, not an unquestioned
oracle for BanyanDB's legacy behavior.

Run ownership/race tests with the existing cursor poison mode and owner-pin counts.
Assert cancellation and budget failure release live reservations. Benchmark batch
kernels, native B3 merge, raw local aggregate and top-k across cardinality/null/
selection mixes. Record scan calls, intermediate DataPoint allocations (target
zero), bytes copied, peak retained memory and latency. Compare against PR #1412
single-query performance and the prior response-materializing QL design.

Acceptance: B1–B9 compose through typed batches; the semantic/pushdown equivalence
tests pass; existing Query responses remain compatible; and protobuf definitions
and native frame wire formats are unchanged. This is the full Catalog B target;
only the subset through T08 identified above is implemented on this branch.
