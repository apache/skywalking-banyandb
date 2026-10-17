# Generated measure QL integration tests

This package generates QL statements and their expected responses from a typed
query model and a small, fixed data set. It does not translate native measure
query requests, execute a server during generation, or capture expected output
from the implementation under test.

## Generate and verify

From the repository root:

```sh
make generate-ql-test-cases
make check-ql-test-cases
go test ./test/cases/measure/qlgen
```

Generation writes `generated/suite.json`. The check command regenerates the suite
in memory and compares its exact bytes with that artifact; it does not rewrite
files. Commit the artifact with changes to the model, fixture, or generation
rules. Do not edit individual generated cases or expected responses.

The same artifact runs through both real integration topologies:

```sh
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/standalone/query \
  -ginkgo.label-filter=QL-generated
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/distributed/query \
  -ginkgo.label-filter=QL-generated
```

Run these sequentially when sharing one workspace. Each suite installs generated
index rules and their binding before creating the measure in its own cluster.
It writes the fixture through the normal write RPC and waits for its complete
contents through both scan and indexed `BydbQLService.Query` cases, retrying
identical writes while schema and index caches converge. Every generated case
then uses that same QL endpoint. Setup, readiness checks, and case
verification issue no native measure query. Existing integration assertions still
verify dispatch and, in the distributed topology, columnar frame traffic and
released owner pins.

## Model and independent oracle

`fixture.go` defines the schema, index rules and binding, and typed rows,
including integer and float fields, string and integer tags, arrays, nulls, empty
values, and distinct timestamps. `FixtureWrites` converts those rows into write
requests. The fixture digest identifies the schema, indexes, binding, and values.
Each row has a unique `rid` entity because indexed tags are stored per series.

`model.go`, `render.go`, and `oracle.go` define the generator's query domain,
render QL, and calculate expected rows independently. The oracle filters fixture
values, evaluates time boundaries, projects columns, groups and aggregates,
orders results, and applies pagination. It does not invoke the production
parser, transformer, planner, execution operators, or native query API.

Times are placeholders until execution, when the runner substitutes a recent
fixture base aligned to two seconds. This keeps expectations deterministic while
remaining within retention and gives time buckets an exact epoch-aligned base.
Unordered results are compared as multisets; ordered cases assert row order.
Comparison retains types, nulls, column names, and timestamps while ignoring
storage identity and trace metadata. Trace cases additionally require spans;
rejection cases require the generated gRPC status.

## Coverage contract

QL has unbounded literal values and nesting, so a finite generated suite cannot
enumerate every possible statement. This suite makes its bounded coverage
explicit rather than treating a list of examples as exhaustive coverage.

`generate.go` and `rules.go` expand projection, predicate, time, parameter,
pagination, ordering, aggregation, grouping, trace, and rejection families. The
artifact's feature manifest records three states. The required inventory is
declared independently of generated cases in `coverage_contract.go`; removing a
generation branch cannot silently remove its coverage obligation.

| State | Required evidence |
| --- | --- |
| `supported` | Generated QL cases with independently calculated success expectations. |
| `rejection` | Generated QL cases with an expected error status. |
| `pending` | An explicit reason naming the missing implementation or fixture. |

These are generation obligations. Actual engine support requires passing the
generated cases in both topologies; a complete manifest cannot make a failing
RPC test pass. Parse and bind rejections assert `InvalidArgument`; the existing
RPC maps transformer validation failures to `Internal`, which those cases retain.

`cover.go` additionally selects a deterministic cover of all feasible pairs in
the declared interaction dimensions: projection, filter, time window, direction,
binding, and pagination. It enumerates valid complete assignments first, so a
pair is required only when some valid assignment contains it. Sorted dimension
names and stable tie-breaking keep selection reproducible. The artifact reports
the dimensions, candidate and selected counts, required and covered pairs, and
uncovered pairs. Pair coverage describes those declared dimensions; it does not
claim coverage of every higher-order combination or every grammar production.

The suite records its format version, fixed generation seed, fixture digest,
features, cases, and pair report. Loading rejects unknown fields, trailing data,
stale fixtures or versions, duplicate cases, missing feature witnesses, and an
incomplete pair report. It also checks the manifest against the audited inventory,
including pending reasons. The generator and unit tests provide deterministic
regeneration checks before either integration suite consumes the artifact.

`TestMeasureGrammarCoverageAudit` in `coverage_contract_test.go` guards grammar
drift with a fingerprint of parser-tagged grammar structure, lexer initialization,
keywords, and syntax guards. A grammar change requires reviewing the capability
inventory and its generated witnesses or pending gaps before updating that
fingerprint. Updating the fingerprint alone does not establish semantic coverage.
This audit uses production grammar metadata in tests; generation and the expected
result oracle remain independent of the production parser and execution engine.

## T03 typed raw-query coverage

The `t03_*.go` model generates extended raw queries independently of the legacy
query model. Its separate `ql_t03` fixture contains 129 rows and three tag families,
nullable int/float fields, a tag/field name collision, and a user `__ql` family.
The original fixture and its 151 regression cases retain their expectations.

Generated T03 families cover both source spellings, direct and repeated aliases,
family-qualified references, timestamp aliases, duplicate and ambiguous output
names, exact and fractional time comparisons, pure-time OR, legacy TIME
intersection, contradictions, changing bound ranges and execution clocks,
fixed-duration intervals, and absent/present/zero LIMIT with more than 100 rows.
Timestamp parameters and string parameters share the independent exact-time
oracle. Timestamp aliases retain the raw timestamp and add a timestamp TagValue
in the `__ql` family.

Run the new ticket independently of the known legacy regressions:

```sh
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/standalone/query \
  -ginkgo.label-filter=QLB-T03
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/distributed/query \
  -ginkgo.label-filter=QLB-T03
```

T03 requires the instrumented build. Each generated case checks that no
intermediate DataPoints are constructed and that live result and memory
reservations return to their previous values. Empty plans, LIMIT 0 and compile
errors assert zero native leaf calls. Clock cases install a serialized Go test
clock around the actual QL RPC, require exactly one clock capture, and restore
the clock afterwards. No clock value is added to the protobuf contract.

## T05 compiled-plan reuse processes

The same artifact also declares generated `reuse_scenarios`, which describe
repeated RPCs and a finite set of cache lifecycle actions. The scenario validator
requires every rebind family and lifecycle kind, so deleting a generation branch
cannot silently remove its obligation. The generator evaluates each response
from the fixture rows and never captures a running server's answer.

The rebind cohorts keep the exact query text while changing scalar values, time
ranges, array lengths, LIMIT values, nullable projected results, and execution
clocks. Empty array bindings and NULL time bindings retain their existing
`InvalidArgument` contract; the next valid binding must still succeed on the
same cached template. Every cohort then executes again with the compiled cache
bypassed against those same independent results and errors. The isolated `ql_t05` fixture has the T03 row model under a
separate schema identity, so schema deletion and recreation do not alter T03's
fixture.

Generated lifecycle scenarios run 32 callers against a cold compilation barrier,
cancel the first or every waiter, replace a schema during compilation, evict a
plan while its RPC owns a handle, replace/delete/recreate a schema, change a
second queried group, and update an unrelated measure. Additional cases inject
same or lower local revisions through the real schema watch adapter, reject a
plan larger than a tiny cache budget while executing it uncached, and preserve
literal-query bypass. The cold burst keeps all 32 compilation waiters concurrent
while limiting downstream native execution to four queries; this avoids making
the cache case a stress test of the finite storage admission pool. Permission
checks inject real compiled authorization
snapshots and authenticated principals into the RPC context; the normal
post-lowering authorization decision must reject a revoked scope on a cache hit.
The same process then withholds the local borrowed schema object while retaining
the native locator: the RPC must use the actual registry fallback, execute the
same independent expected result, and leave no admitted compiled entry.

These instrumented hooks control barriers, schema views and request context.
They do not replace the planner, storage, authorization decision or QL endpoint.
Every scenario requires zero in-flight compilations, waiters, compile reservations,
retired plan bytes, intermediate DataPoints and live result/memory growth after
its RPCs finish.

```sh
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/standalone/query \
  -ginkgo.label-filter=QLB-T05
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/distributed/query \
  -ginkgo.label-filter=QLB-T05
```

Run the two topologies sequentially. These are correctness and ownership cases;
cache-on/cache-off timing uses the separate, bounded T05 benchmark.

## T07 complete SQL aggregation

The artifact declares `aggregate_scenarios`, independent `aggregate_readiness`
queries, and `aggregate_rejections`. Its 41 scenarios each bind the exact same
statement three times: the full input, null-only or smaller input, and empty
input. The runner executes these calls cold and warm, then repeats all three
with the compiled cache bypassed. Seven additional statements reject unsupported
aggregation shapes before scanning. The existing 293 legacy and T03 cases and
18 T05 processes keep their original query texts and expected values.

`ql_t07` contains exactly fixture F's six rows. `ql_t07_groups` separately adds
513 non-entity zone groups with one, two or three rows each across two host
entities, for 1,026 logical rows.
Both measures receive stale versions followed by the final rows with matching
series/timestamps, in the existing replicated group. QL-only raw readiness checks
every final value before aggregate cases run. No native query supplies expected
results or readiness.

`t07_model.go`, `t07_generate.go` and `t07_oracle.go` cover scalar and grouped
COUNT/SUM/MIN/MAX/AVG/MEAN over integer and floating fields, multiple and repeated
calls, tag counts, qualified names, hidden grouping keys, nullable string/integer/
float keys, residual time OR, final LIMIT/OFFSET, and native source spelling that
activates SQL aggregation. The scalar oracle evaluates typed rows directly and
encodes grouping tuples with JSON, independently of the engine's group codec.
NULL and empty strings remain separate. Counts retain integer type; AVG/MEAN
divide the complete sum by the non-null count. Null-only and empty scalar COUNT
is zero; other scalar aggregates are null, and grouped empty input has no rows.

Every successful aggregate RPC checks exact consumed rows after deduplication,
actual group cardinality, one local aggregate execution, absence of source
identity/timestamps on synthesized rows, and restored group/native memory
ownership. These structural assertions require the instrumented build:

```sh
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/standalone/query \
  -ginkgo.label-filter=QLB-T07 -ginkgo.fail-on-empty
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/distributed/query \
  -ginkgo.label-filter=QLB-T07 -ginkgo.fail-on-empty
```

Run the topologies sequentially. T07 does not alter the existing legacy MEAN or
COUNT(DISTINCT) fixtures. Native aggregate pushdown and keyed merging are T08;
T07's new SQL semantics execute over one complete native raw scan.

## T08 native aggregate pushdown and keyed merge

The artifact adds `native_aggregate_scenarios`, `native_aggregate_readiness`,
and `native_aggregate_rejections`. The 30 scenarios retain three identical query
texts with independently evaluated changed bindings. Eligible native cohorts
run cold, warm, with the compiled cache bypassed, and with native aggregation
disabled so the identical SQL also exercises T07. Unsafe cohorts must use T07
automatically. A checked integer-SUM overflow retains its existing RPC error and
must open no native aggregate branch. Every expected result is generated from
fixture rows; neither readiness nor verification issues a native Query RPC.

`ql_t08` uses the complete entity tuple `(host, region)`, unequal per-entity
populations, nullable fields and keys, an all-null entity, sub-one floating means,
and integer extrema above 2^53. `ql_t08_groups` contains 129 distinct entity tuples
and 258 logical rows. Stale and final versions share each logical row's identity.
Bounded raw QL visibility windows verify every final value before aggregation.
The indexed non-entity `key` is constant within each full entity, as required by
the series index. Different entities retain NULL, empty and ordinary string keys.
The setup installs the key index and binding, then retries writes until a separate
indexed QL visibility witness succeeds; raw windows still verify every row.
The oracle uses typed integer extrema and exact rational sums and averages,
independently of native aggregate state and the production grouping codec.

The runner proves SUM/COUNT/AVG leaf reuse, complete tuple-key merging beyond the
default 100 rows, null-only and empty semantics, cold/warm binding isolation, and
fallback for incomplete or unproven entity keys, nullable non-entity grouping,
integer arithmetic, floating extrema, tag counts, and residual time predicates.
It also injects a group cap, first and later branch errors and cancellation, and
authorization denial after a cached plan succeeds, and child revision-gate failure.
Failed RPCs return no partial
rows, release all tracked state and native results, and allow a subsequent query
to succeed. Bucket grouping remains in its later grammar ticket; T08 does not
claim a new bucket syntax through these RPC cases.

```sh
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/standalone/query \
  -ginkgo.label-filter=QLB-T08 -ginkgo.fail-on-empty
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/distributed/query \
  -ginkgo.label-filter=QLB-T08 -ginkgo.fail-on-empty
```

Run these sequentially. The existing 293 cases, 18 T05 scenarios, 41 T07
scenarios and seven T07 rejections keep their query texts, expected values and
pair-coverage report unchanged.

## T10 typed expression projections

The artifact adds `expression_scenarios`, `expression_readiness` and
`expression_rejections`. The 25 scenarios bind the identical statement three
times, then repeat with the compiled cache bypassed. There are 21 additional
rejection statements. Every value is calculated by `t10_oracle.go` from the six
F rows; checked integer arithmetic uses `math/big`, independently of production
kernels. No parser, planner, execution operator or native Query RPC supplies
expected values. The original cases, pair report and T05/T07/T08 scenarios retain
their query texts and expectations.

The separate `ql_t10` measure keeps array tags unindexed and adds accepted legacy
field names `metric-value` and `rate_total`. QL-only readiness checks every field,
array and null before expression calls run. The independent expression model
generates arithmetic, decimal/exponent floats, unary signs, explicit casts,
comparisons, SQL boolean/null truth tables, empty and nonempty membership,
simple/searched/nested CASE and COALESCE. Conditional cases contain invalid casts
and division by zero in unreachable values and conditions. Further scenarios
cover typed nulls, mixed numeric result types, unchanged legacy identifiers,
computed default names, disjoint time selections, LIMIT/OFFSET, LIMIT 0, and
changing string parameters including recovery after a runtime conversion error.
The native registry rejects `*` inside stored field names; parser and Compile
unit tests retain that lexer compatibility using a synthetic schema. The RPC
fixture exercises the valid hyphen name and the multiplication operator.

Computed numeric and string outputs use existing fields. Computed arrays and
timestamps use existing TagValue variants in the `__ql` family. Boolean values
are explicitly cast to integer or string. Timestamp casts use UTC and Unix
nanoseconds; expected epoch values are rendered from symbolic fixture offsets,
without rounding through floating point.

The RPC adapter checks one typed projection per nonempty call, no intermediate
DataPoints or native Query RPCs, released allocation reservations and native
owners, and zero scans for LIMIT 0 and type rejections. A first-branch-match
scenario requires zero later-arm rows. Cached and bypassed execution must both
match the independently generated expectations. Run the topologies sequentially:

```sh
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/standalone/query \
  -ginkgo.label-filter=QLB-T10 -ginkgo.fail-on-empty
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/distributed/query \
  -ginkgo.label-filter=QLB-T10 -ginkgo.fail-on-empty
```

Expression WHERE is covered by T12 below. HAVING and computed grouping remain
in T14 and T16.

## T12 typed field filters

The artifact adds 40 scenarios with three bindings each and seven separate
rejections. The generator adds one independent `ql_t12` fixture: six published F rows plus
256 later rows with unique timestamps and entity IDs. The latter rows prove
that a filter can match after the native default row 100, omit its input field
from SELECT, and apply final LIMIT/OFFSET correctly. An omitted final limit
returns all 256 matching rows. No native Query RPC is used as an oracle or as
readiness: visibility is checked through QL only.

The T12 model renders WHERE expressions from the same finite test AST used by
T10. Its independent scalar interpreter evaluates arithmetic with `math/big`,
SQL three-valued logic, masked CASE/COALESCE/AND/OR, membership including empty
bound arrays, timestamp comparisons and NaN total ordering. It determines
TRUE/FALSE/UNKNOWN row counts before any server is started, then projects source
rows or aggregates selected rows. It never imports production expression kernels
or request planning code. The grammar tests separately compile and rebind the
generated statements through public Prepare/Compile/Bind interfaces.

Generated witnesses cover:

- `v > CAST(? AS DOUBLE)` selects F entity IDs 2, 3, 5; SUM(v)=12 and COUNT(v)=3.
- Bare NULL, typed NULL, TRUE and FALSE under legacy TIME bounds, followed by
  float field comparisons, nullable boolean predicates, explicit boolean casts,
  arithmetic, scalar and bound-array IN/NOT IN, and NaN/Inf ordering.
- Searched/simple CASE and COALESCE in WHERE; protected division and unreachable
  invalid branches remain unevaluated.
- Mixed time/field OR, tag/field OR, legacy TIME intersection, and safe entity-tag
  conjunctions with a remaining field residual.
- Hidden filter inputs, filtering before computed projection and grouping,
  empty/all-null aggregate inputs, omitted and zero final limits, and matches
  beyond row 100 with final pagination.
- Changed bindings on one cached template, cache bypass, invalid values/types,
  and successful recovery after each rejection.
- Existing public fault hooks apply a one-byte memory budget and cancellation
  after acquiring a compiled template. These requests verify RPC status and
  resource release, followed by normal recovery. Operator tests separately cover
  cancellation while filtering and selection-allocation rejection.

With `-tags=querymetrics`, successful complete-input calls assert one raw logical
scan, the topology's raw data-node fanout, zero native aggregate merges, zero
intermediate protobuf datapoints and the structural zero-copy counter invariant.
Unit and kernel-benchmark column-identity checks establish borrowing; the RPC
counter alone is not copy telemetry. Every examined candidate belongs to TRUE,
FALSE or UNKNOWN, and a local aggregate receives
exactly the TRUE rows. Final raw pagination may stop future batch pulls, so its
correct tail result is checked without requiring a full filter-row counter.
Every call checks live native owners, trackers, filter/expression/aggregate
reservations and release after success or failure.

A separate generated compatibility section adds two groups containing the same
legacy measure. Both expose `host STRING` and `kept INT`; their unused field
varies between INT and FLOAT, and their unused tag between STRING and INT. Each
group has its own entity host, so the result does not depend on cross-group SID
deduplication. The legacy statement explicitly projects `host, kept::FIELD`, uses
`FROM MEASURE ... IN (...)` and TIME bounds, and binds a nonempty host IN array.
Three bindings return both groups, one group, then no rows; cache bypass repeats
the same independent expectations. Public compilation tests require a nil QL
execution plan, and the RPC helper checks that no typed filter, expression or
aggregate executes. QL-only readiness reads each source group separately. This
adds one compatibility spec to the 47 field-filter specs without relaxing their
complete-input or native-aggregation assertions.

The independently audited `t12.*` coverage obligations and catalog B8 require
witnesses; deleting a scenario cannot silently remove its coverage claim.
The grammar fingerprint includes the custom WHERE parser as well as T10's
expression parser and lexer.

```bash
go test -tags=querymetrics ./test/integration/standalone/query -run '^TestPropertyIntegrationQuery$' \
  -ginkgo.label-filter=QLB-T12 -ginkgo.fail-on-empty
go test -tags=querymetrics ./test/integration/distributed/query -run '^TestPropertyQuery$' \
  -ginkgo.label-filter=QLB-T12 -ginkgo.fail-on-empty
```

These generated cases activate expression SELECT projection only.

## Engine regressions exposed by this suite

The initial standalone and distributed runs each execute all 151 cases, with
142 passing and nine failing. The failures remain active regression tests:

- Indexed string-array projection and filtering fail because indexed elements
  are stored as raw bytes while the array decoder expects packed values. This
  affects `generated_projection_0`, string-array `HAVING` and `NOT HAVING`, and
  five generated interaction cases.
- `generated_in_int_list_0` expects zero rows for an empty entity `IN` list. The
  engine instead reports that a series is required.

These expected results are retained. Do not capture the failing engine output
as a new expectation, mark these cases pending, or remove their coverage claims
to make the integration gate pass. Fixes should make the same generated cases
pass in both topologies. This is separate from capabilities whose generation
coverage is explicitly pending below.

## Explicit pending gaps

The authoritative inventory is `coverageContract` in `coverage_contract.go`.
`PendingCoverage()` derives its pending entries, also emitted into
`generated/suite.json`:

| Capability | Missing implementation or fixture |
| --- | --- |
| Catalog B4 | Post-aggregate `HAVING` and new membership syntax require T14. |
| Catalog B5 | Multiple sort keys and null placement require T17. |
| Catalog B6 | `SELECT DISTINCT` requires T15. |
| Catalog B8 | Field predicates require T12. |
| Catalog B9 | Computed and ordinal grouping require T16. |
| Low-valued float means | Legacy native mean clamps values below one; the oracle keeps arithmetic mean semantics. |
| Stage selection | Needs a generated staged-group fixture. |
| Index ordering | Needs generated index rules and bindings. |
| `SHOW TOP` | Needs a separate persisted TopN rule fixture. |
| Custom `MATCH` analyzers | Need generated analyzer and index fixtures. |
| Null predicates | Null equality needs an explicit legacy and SQL semantics contract. |
| Empty arrays | Storage collapses empty and null arrays; distinct results need storage and QL contracts. |
| Relative and integer time values | Need clock-controlled semantics and rejection witnesses. |
| Quoted, dotted, and keyword identifiers | Need generated schemas and escaping witnesses. |
| Multiple groups | Need schema compatibility, deduplication, and authorization fixtures. |
| Grouping shapes | Field-only, bucket-only, and default-width grouping need independent legacy result contracts. |
| Predicate bindings | Empty, scalar, array, null, byte, and timestamp bindings need positional type and expansion witnesses. |
| Aggregate shapes | Ungrouped, empty, nullable, and tag-target aggregates need independent legacy result contracts. |
| Syntax boundaries | Depth 64/65, clause order, and malformed or empty projection need generated boundary witnesses. |
| Strict time bounds | Exact endpoint rows need explicit semantics; current legacy time bounds are inclusive. |

Pending entries are visibility into missing coverage, not passing tests. In
particular, low-valued float means are not accepted by copying the current
engine's behavior into expected results.

## Adding a QL capability

1. Extend the typed model and renderer with the capability's valid syntax and
   values. Add any required schema or fixture data.
2. Define its semantics in the independent oracle. Add oracle unit tests with
   directly calculated results for boundaries, nulls, and errors.
3. Extend generation rules to produce the capability's cases and relevant
   combinations. Add or refine interaction dimensions and feasibility constraints
   when the capability interacts with existing clauses.
4. Audit `coverage_contract.go`, replace corresponding pending features with
   supported or rejection witnesses, and update the grammar fingerprint only
   after reviewing affected grammar forms and semantics. Do not silently omit a
   grammar form or weaken the oracle to match a known engine defect.
5. Regenerate, run the deterministic check and unit tests, then run the generated
   QL label in both integration topologies. Implementing a new QL capability does
   not require a native query counterpart or a protobuf change.
