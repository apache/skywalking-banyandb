# Measure QL Catalog B: Core SQL Implementation Plan

This branch delivers **PR 1 of 4**: the shared execution foundation, typed raw
measure QL and compiled-plan reuse. See the [PR 1 verification record](pr1-verification.md)
for its isolated scope and current evidence. Subsequent PRs deliver aggregation,
expressions/filtering, and complete grouping/output composition.

Status: **T02 shared batch execution, T03 typed raw queries and T05 compiled-plan
reuse are implemented in [PR #1417](https://github.com/apache/skywalking-banyandb/pull/1417);
eight later capability tickets are outside this PR.** The isolated branch passes
451 independent measure/QL cases in each topology, unit/race checks and the focused
planning benchmark. The latest single RPC comparison has higher cache-on latency
and CPU despite lower allocated bytes; no end-to-end latency gain is established.
See the [PR 1 verification record](pr1-verification.md) for the current results.

The T01/T02/T03/T05 verification documents retain historical ticket-stage evidence.
T05's earlier five-run comparison recorded modest median savings and mixed tails;
T03's additional compatibility comparison was stopped without a verdict. T02's
inconclusive/stopped statistical acceptance is preserved. These historical records
are not measurements of the latest PR head, and the PR 1 refresh does not turn
those outcomes into passes.

The delivered foundation includes Go-only native buffering, ownership and indexed
array/empty-selector compatibility repairs, listed in the PR 1 verification
record. Native requests, frame versions, protobuf definitions and persisted
formats remain compatible. SQL operators continue to run on the liaison.

The [DataFusion-inspired engine design](datafusion-engine.md) defines the batch
execution architecture and revised delivery order. Its [source research](datafusion-research.md)
maps upstream mechanisms to Catalog B. This document retains the language scope,
wire constraints and acceptance cases; the engine design refines intermediate
representation, aggregate exactness and the native execution seam.
The [plan reuse design](plan-reuse.md) adds compiled-template caching for OAP's
repeated measure QL with different parameters, including legacy query shapes.
The [implementation tickets](implementation-tickets.md) define 11 executable
capabilities, their dependencies, independent expected results and real RPC
end-to-end commands. Every capability answers a concrete performance question
with a mandatory benchmark. Syntax, error handling, fixtures and small helpers
are implementation details within those tickets.

## 1. Base and fixed scope

Implement B1–B9 **on top of PR #1412**, using QL parsing/planning and operators on
the liaison. Do not change protobuf definitions, generated protobuf files, RPCs,
HTTP routes, native measure request contracts, persisted formats or native frame
versions. SQL extensions run on the liaison. The delivered foundation also contains
shared Go buffering/ownership and native read/dispatch compatibility repairs;
these preserve existing wire and persisted contracts and are listed in the PR 1
verification record. Continue using `BydbQLService.Query` and its existing `measure_result` response.

The baseline reviewed for this plan is PR #1412's commit `012115c5`. Its source
specifications are:

- `docs/design/0.12.0/measure-ql-gaps/README.md`: B1–B9, the logical-operator
  foundation, B1–B3 first, and B3 as several native requests merged on the liaison.
- `docs/design/0.12.0/bydbql-plan-cache/README.md`: compile/bind/lower separation,
  unchanged wire/data nodes (§8), and the implemented local schema source.
  Its historical decision (§11.4) deferred caching for the old transformer;
  [Catalog B now includes compiled-plan reuse](plan-reuse.md) for its richer planner.
- `docs/design/0.12.0/measure-batch-v2/README.md` and
  `docs/design/0.12.0/measure-scan-path-v2/README.md`: existing measure execution
  infrastructure, which QL reuses rather than replaces.

`012115c5` is the historical design-review baseline. PR #1412 merged as
`15635181b`; PR 1 is rebased onto main at `f9424047d`, including the documentation
fixes from #1415 and native aggregation optimization from #1413. The benchmark
refresh predates #1413; its measured source and later rebase checks are recorded
separately in the PR 1 verification record.

The gaps document proposes results as columns and rows but does not specify their
wire encoding. For this scope, that is an **Arrow-style columnar representation**
using PR #1412's `vectorized.RecordBatch`, adapted to the existing response only
at final egress. It is not a row-oriented Go table or Arrow IPC. The broader
external table/streaming interface is
not part of this implementation. The streaming design's future table-header work
must not introduce a protobuf dependency here.
That future streaming result-shape decision needs revisiting separately; this
plan does not change it or implement streaming. Deployment follows 0.12's
whole-cluster upgrade policy, without mixed-version negotiation or fallback code.

| ID | Deliverable | Location |
|---|---|---|
| B1 | Time in WHERE, `now()`, fixed-duration `INTERVAL` | Grammar, binding, time-range lowering |
| B2 | `AS` aliases | Binding and liaison result renaming |
| B3 | Multiple aggregates | Split existing requests; merge by group key on liaison |
| B4 | Post-aggregation HAVING; new array-membership spelling | Grammar and liaison filter |
| B5 | Multiple sort keys, columns/aliases/aggregates, NULLS FIRST/LAST | Liaison sort/top-k |
| B6 | SELECT DISTINCT | Liaison projected-row deduplication |
| B7 | Arithmetic, CASE, COALESCE, CAST and `::type` | Shared typed expression evaluator |
| B8 | Field filters and float literals | Liaison row filter before aggregation |
| B9 | GROUP BY expressions and select-list ordinals | Binding and computed grouping keys |

Exclude general C functions, D query composition, S DDL, writes, windows and gap
filling. Field-filter pushdown is explicitly later engine work. Keep existing
aggregate behavior and count-distinct restrictions unless a QL-only extension is
explicitly described below; do not turn B into an engine numeric-semantics rewrite.

## 2. Reuse PR #1412's QL and scan infrastructure

Extend `pkg/bydbql` rather than introduce a second endpoint or standalone dialect
service. Keep `Prepare`/`Bind`, positional parameter overlays and the existing
prepared cache. Add an immutable logical measure plan in `plan_measure.go`, an
analyzer, a small ordered rewrite pass, physical lowering and typed expressions.
Logical aggregate nodes carry all aggregate calls; physical lowering chooses
native split requests or a local batch aggregate. The liaison executor consumes
the resulting batch operators through a small execution interface; native scan
leaves still lower to ordinary `measurev1.QueryRequest` values.

Use PR #1412's `SchemaSource` and `NewTransformerWithSource`, including its local
watch-fed reads and existing registry fallback on a miss. Shared schemas remain
read-only. Compile schema identities and types separately from parameter binding;
bind all parameters and a single captured `now` per execution; lower scan leaves
before authorization. Keep the existing parse cache and add a separate bounded
compiled-plan cache for parameterized measure QL. Cache typed logical plans and
physical execution recipes, with schema dependencies; bind values and instantiate
fresh operators per request. Dependency validation, singleflight compilation,
invalidation and OAP-shaped benchmarks are specified in [plan reuse](plan-reuse.md).

Preserve stream, trace, property and TopN dispatch. Legacy measure queries that
need only one native request keep their existing fast path and output behavior.
Route new measure constructs through the plan executor within the same Query RPC.
This is a capability choice after parsing, not an endpoint or environment switch.

Add `FROM group.measure` as measure shorthand alongside `FROM MEASURE m IN g`.
Keep legacy TIME, stages, QUERY_TRACE, parameters and index-based ordering forms.
Resolve new ORDER BY names against output aliases and source columns, then the
legacy index-rule interpretation where applicable; pin collisions in tests.
Require explicit qualification/disambiguation for ambiguous tag/field names.

Arithmetic needs deliberate lexer work: legacy identifiers can contain `-` and
`*`. Preserve accepted legacy syntax and parse extended expressions with clear
operator tokenization. Initially require whitespace around ambiguous arithmetic
operators in unquoted expressions; quote identifier names containing those
characters. Test `a - b`, `a * b` and legacy `a-b`/`a*b` explicitly rather than
silently changing their meaning. A later stricter dialect is outside this plan.

## 3. Existing request and response contract

### Parameters

Keep `QueryRequest.params` as the existing repeated `TagValue`. Preserve textual
placeholder order, array expansion in IN/MATCH/array-membership predicates, and
single-value versus parenthesized-list distinctions. Extend the preparer's walker
and binder together, including immutability/concurrent-binding coverage.

B8 adds float **literals**, which the liaison evaluates without a native float
condition. There is no float or boolean parameter variant in the current wire
contract. A caller can bind a string and write `CAST(? AS DOUBLE)` or
`CAST(? AS BOOLEAN)`; conversion is explicit and checked. Do not add a parameter
oneof arm or silently reinterpret all string parameters as numbers.

### Internal table and result adapter

Represent ordered logical slots over typed columnar batches so aliases, aggregate
results, computed values and hidden sort/filter columns have stable identities.
Expressions return a scalar or a column; constants stay scalar until expansion
is needed. Use no intermediate DataPoints or row maps. At the final adapter:

- Direct tags keep their tag family and existing TagValue encoding; aliases rename
  the returned tag, not the storage reference.
- Fields, aggregates and computed int/float/string/bytes values become named
  entries in `DataPoint.fields`, using the existing FieldValue variants. Emit these
  entries in select-list order within that container.
- The built-in time remains `DataPoint.timestamp`. A named timestamp expression
  or time alias is emitted as a timestamp TagValue in a synthetic `__ql` tag family.
  Reject an output family/name collision rather than overwriting user data.
- Computed arrays use the corresponding existing TagValue in `__ql`. Nulls use
  the existing explicit null variants. A boolean may be used in predicates/CASE;
  direct boolean output is rejected because the response has no boolean variant.
  Require an explicit cast to INT (0/1) or STRING for that projection.
- Hidden slots never reach the response. Reject duplicate names within an output
  container; do not emit ambiguous named fields. Preserve trace and group statuses.
  Synthesized aggregate rows must not claim a real raw-row SID/version.

These are proposed encoding rules within the existing contract, not a new wire
format. The existing response cannot describe a mixed tag/field select-list order
or attach column descriptors to an empty result. Return the existing empty
measure response for empty results and LIMIT 0. Document these limits; do not
promise an external generic SQL table schema or encode one in an opaque payload.

## 4. Planning and execution order

```text
compiled-plan lookup + dependency validation
  -> miss: prepare -> analyze -> structural rewrite -> physical template
  -> bind parameters and now -> instantiate guarded choices and native leaves
  -> authorize every native request before starting any scan
  -> native scan(s) and optional aggregate-result merge
  -> residual WHERE (raw-input plans only)
  -> computed group keys / local aggregation (when pushdown is unsafe)
  -> HAVING
  -> SELECT expressions, retaining hidden ORDER BY slots
  -> DISTINCT on visible slots
  -> ORDER BY / top-k
  -> OFFSET / LIMIT
  -> existing measure_result adapter
```

Use pull-based operators with context-aware NextBatch and Close. Factor a Go-only
native batch executor after distributed merge/reduce and before MIterator's
protobuf serialization, retaining native lifecycle/gates/deduplication. Both the
existing native response collector and QL use that executor; QL must not call the
unary measure service and reconstruct batches from its DataPoints. A fixture
adapter supplies batches for tests. The engine design names the exact extraction
sites and ownership requirements.

Classify each scan predicate as Exact, Inexact or Unsupported. Keep a residual
filter for inexact/unsupported conditions. Never push aggregation or a final limit
below that filter. An OR stays residual unless the whole expression is exact;
a safe time envelope is pruning only. Retain hidden predicate/group/sort inputs.

Native limit zero defaults to 100. Internal scans feeding merge/filter/sort/group
operators must request complete input with the existing list-all sentinel and
budget enforcement, not zero or the final SQL limit. Prove this on more than 100
rows/groups. No repeated OFFSET scans over changing data. Native execution still
materializes frames and some batch slices; account for that memory and fail
explicitly on budget exhaustion. The batch interface alone does not make native
collection streaming. Protobuf result materialization happens only at final egress.

Keep final OFFSET/LIMIT on the liaison unless equivalence of pushdown is proven.
LIMIT 0 requires presence-aware plan state and skips execution. Preserve legacy
omitted-limit behavior on legacy plans; extended operator plans must not inherit
an accidental intermediate 100-row cap. Document their final result-limit policy
explicitly: no semantic cap when LIMIT is omitted, subject to admission budgets.

## 5. B1–B3 first

### B1: time predicate lowering

Resolve `time >= now() - INTERVAL '1h'` and timestamp/string parameters to the
existing TimeRange. Capture now once across all branches and do not cache resolved
clock values. Support fixed intervals; calendar/timezone functions belong to C.
Intersect a legacy TIME range with a new WHERE time predicate when both appear.

Native TimeRange is inclusive at both endpoints. Keep the typed planner's intervals
half-open and convert the exclusive upper endpoint to the final included millisecond
when lowering to the native request. Implement strict/inclusive boundaries on the
stored millisecond lattice with checked arithmetic: equality is a one-millisecond
interval; `>` advances a representable bound; `<=` advances the internal exclusive upper endpoint.
Use correct ceiling/floor rules for sub-millisecond literals. Contradictions produce
empty input locally. Test storage-domain extremes and reject unrepresentable ranges;
do not silently apply legacy truncation or invent an implicit upper bound of now.
Existing standalone TIME conversion remains compatible.

### B2: aliases

Bind aliases to output slots, retaining original scan references. ORDER BY sees
select aliases first; WHERE does not. GROUP BY uses source expressions or ordinals;
HAVING uses grouping expressions and aggregates rather than select aliases.
Validate output name collisions before scans. Verify aliases through the existing
measure_result, including tags and multiple aggregates targeting the same field.

### B3: split native requests and merge

For pushdown-compatible queries, collect unique aggregate calls from SELECT,
HAVING and ORDER BY, including auxiliary exact non-null COUNT calls needed to
normalize nullable SUM/MIN/MAX when finalized native results lose that information.
Lower one existing native aggregate request per distinct visible or auxiliary
call. All branches share bound time range, criteria, groups, stages, group keys
and revision context. Deduplicate repeated calls regardless of output aliases.

Remove child final pagination/top-k. Obtain complete grouped results and merge by
an encoded tuple of every grouping tag plus bucket timestamp when present. Scalar
aggregation has one constant key. Never zip replies by position, join by incidental
SID, or match columns by their display names. The key encoder uses types, null
markers and length-delimited values to avoid collisions.

Track the union of group keys and distinguish absent groups from null aggregate
values. Apply extended QL empty semantics: scalar COUNT is zero, other empty
scalar aggregates null, and grouped empty input has no rows. If the native
result cannot establish a complete group domain for a shape, use a QL raw-input
plan for that shape rather than dropping groups. Reject duplicate keys within a
branch as a violated adapter invariant. A branch failure cancels siblings and
fails the whole query; group gate failures remain distinguishable from empty data.
Use bounded fan-out and one parent budget/deadline.

Reuse existing SUM/MIN/MAX/COUNT and time-bucket execution only when its type,
precision and null/key behavior preserve the typed QL plan. The engine design
lists non-equivalent cases requiring local execution. B3's oracle is eligible
individual native queries on fixed data, merged by key with explicit QL
normalization, plus pushdown-versus-local equivalence tests. For AVG/MEAN in extended operator plans,
lower to native SUM and COUNT over the same target and divide on the liaison as
float, returning null for zero count. Never use native MEAN for this path: its
integer division and sub-one clamp are incompatible with SQL AVG. Reuse identical
SUM/COUNT branches already needed by other output slots. Keep existing single-scan
legacy behavior unchanged and document the extended-plan AVG semantics; no native
aggregate helper changes are needed. Test integer, fractional and sub-one means.
Do not average independent finalized averages or sum arbitrary shard-distinct
counts; the native service owns distributed reduction and count-distinct eligibility.

Separate aggregate requests do not share a storage snapshot. They share bound
parameters and now, but concurrent writes may affect branches differently. Record
this existing-read-consistency limitation; do not claim snapshot consistency or
add a snapshot protocol. The raw-input path below is required for residual
operators, not the default replacement for the catalog's B3 design.

## 6. Remaining liaison operators

Build a shared typed expression evaluator before composing B4–B9. Implement
literals, source/output slots, arithmetic, comparisons, AND/OR/NOT, IS NULL, IN,
searched/simple CASE, COALESCE and explicit casts. CASE/COALESCE short-circuit.
Use three-valued predicate logic; WHERE/HAVING retain only TRUE. Define numeric
promotion, integer division, overflow and invalid-cast behavior in tests. Support
INT/BIGINT, DOUBLE/FLOAT, BOOLEAN, STRING and TIMESTAMP casts, keeping `::TAG` and
`::FIELD` as name disambiguators. Reject unsupported projected types at binding.

- **B4:** Add a HAVING clause after GROUP BY, evaluated after merged aggregation.
  Allow hidden aggregates; reject non-grouped source references and nested
  aggregates. Add `CONTAINS`/`NOT CONTAINS` for array membership, mapping to existing
  behavior. Preserve infix HAVING in WHERE for legacy queries; clause position
  distinguishes it from the new HAVING clause.
- **B5:** Sort lexicographically by typed keys, with ASC/DESC and independent NULLS
  FIRST/LAST. Default ASC to NULLS LAST and DESC to NULLS FIRST. Use exact int64
  comparisons, bytewise string ordering and a consistent non-finite-float policy.
  LIMIT uses top-k retaining OFFSET + LIMIT rows after checked addition; unbounded
  sorting is budgeted. Equal sort keys have unspecified relative order.
- **B6:** DISTINCT deduplicates the entire visible projected tuple, treating nulls
  as equal for this purpose. Hidden sort slots do not affect equality. With
  DISTINCT, reject ORDER BY expressions not derivable from visible output.
- **B7:** Evaluate projection and predicate expressions with the same typed tree.
  Parse decimal/exponent floats and unary signs. No implicit string-to-number
  conversion. Aliases do not alter source resolution.
- **B9:** Evaluate computed group keys before aggregation. GROUP BY ordinals are
  positive select-list positions after star expansion; reject out-of-range or
  aggregate-containing targets. Match group expressions structurally and expose
  selectable TIME_BUCKET results without changing native bucketing semantics.
- **B8:** Execute field predicates on raw input before local aggregation and final
  pagination. Reuse the existing native raw scan to retrieve required fields;
  do not put float literals into tag-only protobuf criteria. For computed grouping,
  aggregate arguments or residual field filters, fold all aggregates locally over
  the filtered batches using SQL group IDs and typed state arrays. Native helpers
  are reused only where their semantics match; use the QL null-aware key codec,
  empty-input handling and AVG finalization described in the engine design.
  Charge group/distinct state. Data-node field-filter pushdown stays out of scope.

Preserve current count-distinct routing/stage restrictions on pushed plans. A
liaison raw-input implementation must establish native deduplication semantics
and exact whole-query distinctness before admitting shapes outside those rules;
otherwise reject them explicitly rather than changing the engine contract.

## 7. Ownership, authorization and observability

The existing Query RPC owns admission, cancellation, access logging and parameter
redaction. Lower every child to a native request and run existing resource
authorization before any dispatch; do not pass a Go plan to the concrete-proto
authorization switch. Preserve all group scopes for multi-group legacy syntax.
Reject incompatible schema types during compilation rather than last-group-wins
resolution. Never cache authorization decisions.

Reuse PR #1412's query lease and memory tracking for retained native frames/batches,
QL columns, keyed merge maps, aggregate state, DISTINCT, top-k and final encoding.
Inject shared accounting: existing native paths create separate trackers, and the
lease has no refund method. The engine design specifies conservative charging
and reservation ownership rather than assuming these interfaces already compose.
Admit raw-input plans as scans. Account for overlapping frame/batch residency;
close all inputs and release owned data on success, error and cancellation.
Retained values must own or retain their backing buffers according to the batch
contract. Do not retain borrowed strings after a native batch is released.

Propagate existing trace/stage/revision context where available; do not add new
request fields. Aggregate child traces and operator row counts into the existing
trace result. Preserve native error identities and group statuses. New parse/bind
errors are InvalidArgument; exhaustion is ResourceExhausted. Any change to legacy
error mappings is explicit and covered by compatibility tests.

## 8. Implementation milestones

The [ticket plan](implementation-tickets.md) is the implementation and verification
checklist. Each leaf requires result/error assertions through both standalone and
distributed Query RPCs, plus a measured distributed RPC benchmark. The milestones
below summarize delivery; they are not standalone implementation tickets.

| Milestone | Work | Acceptance gate |
|---|---|---|
| 0. Shared batch execution (T02) | Completed T01 verification support plus shared native batch execution, gates and accounting | Actual values/errors verified; no intermediate DataPoints; measured cutover overhead and retained bytes; ownership; no protocol diff |
| 1. B1–B2 + reuse (T03, T05) | Typed raw queries with aliases/time lowering; complete bounded concurrent cache | Correct rebinding and invalidation; measured planning savings, pruning cost and cache churn |
| 2. B3 (T07, T08) | Complete SQL local aggregation, then eligible native split/merge; AVG belongs to both paths | Native/local equivalence and measured cost; exact COUNT; nullable keys; >100 groups; empty results; cancellation |
| 3. B4/B7/B8 (T10, T12, T14) | Complete vector expressions, field WHERE and grouped HAVING queries with membership compatibility | Checked arithmetic and masking; measured kernels/filter/group costs; residual-before-aggregate/limit |
| 4. B6/B9 (T15–T16) | DISTINCT and computed/ordinal grouping reuse the SQL group table | Visible-key equality; explicit null encoding; expression/ordinal equivalence; state budgets |
| 5. B5 + composition (T17) | Multi-key ordering with full sort and compact top-k, hidden slots and final pagination | Full-sort/top-k parity and measured cost; bounded retained bytes; all B1–B9 compose under cold/warm/uncached execution |

Implementation files are in `pkg/bydbql`, liaison QL orchestration, a focused
`pkg/query/ql` module, and the Go-only native batch-execution seam in dquery and
the distributed measure planner. Reuse batch helpers and add SQL kernels without
changing data-node behavior. No implementation
ticket edits `api/proto`, generated contracts or native transport formats.

## 9. Verification and completion

Test prepare/compile/bind/lower against fixture schemas and verify execution using
both a fixture scan adapter and the real existing Query RPC. B1–B2 single-scan
lowering must match native requests where the old syntax has equivalent semantics.
B3 compares keyed merges with individual aggregate requests on a fixed dataset.
Operator compositions use a raw reference evaluation on that same dataset.

```sql
SELECT host AS node, SUM(value) AS total, MAX(value) AS peak
FROM metrics.cpu
WHERE time >= now() - INTERVAL '1h'
GROUP BY host
HAVING SUM(value) > 10
ORDER BY total DESC NULLS LAST, node ASC
LIMIT 20 OFFSET 5
```

Add a variant with `value > CAST(? AS DOUBLE)` to prove the planner moves filtering
and aggregation to the liaison when needed. Cover aliases across response
containers, duplicate-name rejection, empty results, hidden slots, null-only
groups, multiple tag families, multiple groups/stages, shard/replica deduplication,
count-distinct restrictions, denied groups, failed child gates and cancellation.
Retain existing stream/trace/property/TopN and legacy membership fixtures.

Run focused `pkg/bydbql` and affected liaison/operator tests first, then the
existing standalone/distributed query suites. Run race checks for concurrent
binding and cancellation/ownership. Include the plan-reuse design's OAP-style
warm/cold/invalidation benchmarks with varying parameters and working-set sizes.
Benchmark native single-aggregate queries,
B3 multi-request merges, raw filtered aggregation and high-cardinality sort/group/
DISTINCT with allocation and peak-memory measurements. Show native pushdown is
retained; report the cost where a residual operator requires raw input.

Completion requires end-to-end B1–B9 coverage through the existing RPC, unchanged
legacy fixtures, bounded intermediate state, no silent input truncation, and an
empty diff for protobuf/generated contracts and native wire formats against the
PR #1412 implementation base. No protobuf generation or new RPC work is needed.
Every ticket must also attach the [specified benchmark evidence](implementation-tickets.md),
including correctness checks, measured resource use and its numerical acceptance
gate. Missing/skipped tests or zero matching benchmark records cannot count as a pass.
