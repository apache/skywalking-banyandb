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
queries, and `aggregate_rejections`. Scenarios bind the exact same
statement three times: the full input, null-only or smaller input, and empty
input. The runner executes these calls cold and warm, then repeats all three
with the compiled cache bypassed. Additional statements reject unsupported
aggregation shapes before scanning. The legacy and T03 cases and
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

The QG2 cases use literal expected values for scalar and grouped aggregate composition, computed arguments, COUNT DISTINCT expressions, repeated aggregates, HAVING, ordered aliases with pagination, and changed expression parameters. They include all-null and empty windows, nested-aggregate and depth rejection, and run through the same standalone and distributed Query RPC harness. Computed aggregate inputs use local aggregation over complete raw input.

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
and `native_aggregate_rejections`. The scenarios retain three identical query
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

Expression WHERE is covered by T12 below, grouped HAVING by T14, and computed
grouping by T16.

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

## T14 grouped HAVING and collection membership

The artifact adds `having_scenarios`, `having_rejections`,
`having_legacy_scenarios`, `having_review_scenarios` and QL-only readiness for
five independent fixtures.
There are 118 typed scenarios, 16 legacy scalar membership scenarios and eight
reference/type rejection statements, plus 21 review regression scenarios (163
T14 integration specs). Each successful scenario binds the exact
same SQL three times, then repeats with compiled-cache bypass. Empty array
bindings rejected by public preparation do not acquire the compiled cache; the
artifact records that boundary separately from later binding failures. Eligible native
aggregation scenarios also run with native aggregation disabled. The original
cases, expectations and pair report remain unchanged.

`ql_t14` contains eight rows: host a has SUM(v)=2, b=4, c=6, and n has only
null values. Floating field f has those same exact totals. Labels are constant
within each entity: a has ['red','blue'], b has ['blue'], c has an empty array,
and n has a null array. Integer arrays have the corresponding [1,2], [2], empty
and null values. Arrays remain unindexed so a typed residual reads their complete
stored values. Storage's empty-array/null normalization is explicit in raw
visibility expectations. `ql_t14_groups` separately contains 129 host groups and
258 rows, proving complete grouping and HAVING before final LIMIT/OFFSET.
Native cohorts bind all relevant host entities explicitly, retaining the source
bound proof required by T08 eligibility; public tests assert native plans exist.

`t14_model.go`, `t14_generate.go` and `t14_oracle.go` render clause and infix
positions and evaluate expected results independently of production planning or
kernels. The scalar oracle first filters fixture rows, groups them using JSON
tuples, evaluates aggregate expressions, then applies three-valued HAVING and
final pagination. Generated witnesses cover visible/hidden/repeated aggregates,
hidden grouping keys, group-only and scalar queries, empty/null groups, CASE,
COALESCE, masked errors, source references, aggregate annotations and casts.
SELECT aliases, nested aggregates and nongrouped sources reject before dispatch.
SUM(v)>3 returns b/c with totals 4/6; selecting only host exposes no hidden SUM.

Collection membership uses the current measure index's all-terms, by-value
contract. Scalar or array RHS terms must all occur in the source; duplicate terms
do not change the match. A scalar source matches an equal scalar term. NULL
sources and NULL requirements do not match; NOT complements that boolean result.
This membership contract differs from SQL's three-valued IN. Parenthesized and
single placeholder forms preserve expansion/coercion, and empty bound arrays and
NULL members inside lists retain their existing binding rejection. Both HAVING /
NOT HAVING and CONTAINS / NOT CONTAINS execute string/int arrays and scalar tags,
literal/single/list/array/null bindings, conversion failure and recovery, mixed
field predicates and grouped HAVING. With v>0, red selects only row 2; its negation
selects rows 3 and 5. Explicit `::TAG` references activate the typed residual path.

`ql_t14_legacy` separately indexes STRING host and INT zone, both included in its
entity. These generated statements use `FROM MEASURE`, TIME and unannotated
infix membership so public compilation and RPC counters require the established
native operator path with its existing QL batch boundary and no typed WHERE,
HAVING or aggregation. They pin scalar value equality, distinct and duplicate requested
terms, NULL, both spellings, changed bindings and cache bypass through actual QL
RPCs. Neither these cases nor any other generator case issues a protobuf native
query as its oracle.

The review matrix independently writes a measure named `contains-svc` in a group
named `contains-svc`, with a scalar tag named `contains`. Five cohorts use bare
projection and equality, mixed-case CONTAINS, NOT CONTAINS, and the typed aliases
with a field residual. Their changing parameters pin ordinary identifiers and
contextual operators through real QL RPCs. A separate `ql_t14_indexed` fixture
writes STRING_ARRAY labels ['red','blue'] and INT_ARRAY nums [1,2] and activates
both index rules. Its readiness reads only control tags and fields; it does not
ask the broken native decoder to project arrays. Sixteen cohorts cross both
array types, four operator spellings, and typed versus unannotated membership
with a field residual. Every binding requires `InvalidArgument` containing
`indexed array`, unchanged native leaf/batch dispatch counts, balanced resources,
and subsequent successful recovery. Cold, warm, and cache-bypassed executions
all enforce the same rejection. The compiler-only tests accept these otherwise
valid recipes because schemas do not expose the current index bindings; the
real RPC boundary must perform the metadata check on each execution.

Instrumented calls require HAVING input counts to equal completed groups, while
WHERE counts remain raw rows. Native leaf count and retained sum/count lanes stay
constant for repeated SELECT/HAVING calls. Independent responses exclude hidden
aggregate slots and source row identity. Every call checks balanced filter/HAVING
reservations and released native results, aggregate/expression state and trackers.

```sh
go test -tags=querymetrics ./test/integration/standalone/query -run '^TestPropertyIntegrationQuery$' \
  -ginkgo.label-filter=QLB-T14 -ginkgo.fail-on-empty
go test -tags=querymetrics ./test/integration/distributed/query -run '^TestPropertyQuery$' \
  -ginkgo.label-filter=QLB-T14 -ginkgo.fail-on-empty
```

Run the topologies sequentially. The independently audited `t14.*` obligations
and catalog B4 require generated witnesses. The grammar fingerprint includes the
custom post-aggregate HAVING parser as well as the existing expression parsers.

### Existing native indexed-array limitation

A real T14 standalone smoke attempted a pure legacy indexed STRING_ARRAY query:

```sql
SELECT rid FROM MEASURE ql_t14_legacy IN replicated_group
TIME BETWEEN ? AND ? WHERE labels HAVING ? ORDER BY TIME ASC
```

With fixture labels ['red','blue'] and a scalar 'red' binding, its RPC failed with
`Unknown: STATUS_INTERNAL_ERROR, panic: unmarshalVarArray failed: invalid variable array`.
The failure and original indexed-array fixture are recorded in
`.omx/t14-standalone-smoke-v1.log` and `.omx/t14-standalone-smoke-v1.json`.
`banyand/internal/storage/index.go:appendSearchHit` keeps an individual stored
index term; `banyand/measure/query.go:buildStoredIndexValue` decodes it as a complete
array via `mustDecodeTagValue`, whose STRING_ARRAY decoder requires packed array
delimiters. The same source inspection shows that INT_ARRAY projection can lose
all but one element. A later one-row real QL RPC review probe confirmed [1,2]
projects as [2], so new typed membership for 1 would silently drop a valid row.
That probe is recorded in `.omx/t14-claude-review-indexed-probe.log`; no baseline
RPC rerun is claimed.

T14 does not change that native storage/decoder path or turn its observed output
into an expected result. QL-evaluated indexed-array membership now rejects before
native dispatch, including compiled-cache hits. The generated typed array cases
retain full independent array expectations using unindexed data and `::TAG`
residuals; pure legacy
compatibility witnesses use indexed scalar tags. Earlier indexed-array regression
queries and expectations are retained unchanged. Fixing native indexed arrays is
separate work; this limitation prevents claiming repaired legacy indexed-array
execution from the T14 success cases.

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

## T15 complete visible tuple DISTINCT

The typed `t15Query` model generates 18 scenarios with three changed bindings
and three bind rejections, producing 21 T15 specs. `t15_oracle.go` evaluates the existing independent
scalar/aggregate model, applies WHERE and HAVING, encodes complete visible tuples
with JSON structure and explicit numeric equality normalization, and then applies
final pagination. It does not parse or execute production QL. The audited
`t15.*` inventory and required scenario names prevent silently dropping a bounded
capability witness. Timestamp ordering compares typed values, not JSON text.

The isolated `ql_t15` fixture writes 2,201 rows through the normal writer and
checks every row through 35 QL-only visibility windows. There are 135 unique
host/zone tuples; seventeen occurrences of each large-fixture tuple cross
series identities and native batches. The deliberately colliding tuples
`("ab", "c")` and `("a", "bc")` must remain separate. Additional witnesses cover
NULL versus empty string, NULL versus zero, independently specified positive and
negative zero writes, and two independently specified NaN payload bit patterns.
The fixture model retains NaN bits as a serializable marker; only writer/oracle
conversion materializes a floating NaN. Expected protobuf JSON uses `"NaN"`.
The public result comparator equates NaN payloads while keeping NaN distinct
from ordinary numbers. Successful storage/transport behavior is established by
the real endpoint tests, not by generator success.

Scenarios cover scalar int/float/string/timestamp outputs, aliases, casts, CASE,
field WHERE with changed parameters, hidden COUNT in HAVING, hidden GROUP BY
inputs excluded from equality, empty input, LIMIT
0, and cached/bypassed equivalence. Arrays and hidden ORDER BY inputs reject
before scans. The original T15 tuple pagination uses
LIMIT 200 and expects all 135 tuples from 2,201 inputs; a separate visible TIME
ordering scenario checks LIMIT/OFFSET with deterministic rows.

Three scenarios additionally verify unordered finite-LIMIT stopping with constant
and field-filtered values, and complete consumption when OFFSET exceeds the
number of unique tuples. Their expected examined-row counts come from the
independent fixture model. These witnesses have at most one unique value, so
their expected rows do not depend on unspecified input order. Ordered pagination
and the public operator tests cover complete consumption and multiple keys.

Every successful RPC checks actual retained distinct cardinality before pagination,
zero DISTINCT aggregate states, no intermediate DataPoints/native Query RPCs,
and restored tracked memory/native result ownership. Instrumentation is required;
uninstrumented zero counters are never interpreted as observed work.
For unordered finite LIMIT, retained cardinality is capped at OFFSET + LIMIT;
it is not the cardinality of unseen input. The three stopping scenarios also
check exact examined-row counts across changed bindings and cache bypass.

```sh
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/standalone/query \
  -ginkgo.label-filter=QLB-T15 -ginkgo.fail-on-empty
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/distributed/query \
  -ginkgo.label-filter=QLB-T15 -ginkgo.fail-on-empty
```

Run the two topologies sequentially. Existing generated cases and scenarios retain
their query texts and expected values.

## T16 computed grouping, ordinals and buckets

`t16_generate.go` enumerates 42 scenarios with three bindings each and 23
no-scan rejection cases, producing 65 T16 specs. The finite model and independent
oracle are in `t16_model.go` and `t16_oracle.go`; generation never asks the query
planner or running engine for expected results. `t16_fixture.go` defines the
six-row F fixture, a six-row two-column star fixture, and a 1,153-row fixture
spanning native batches. Twelve QL-only readiness queries establish complete
fixture visibility.

Scenarios cover multiple arithmetic/CASE/cast keys, repeated keys, structural
matching, ordinal equivalence, star expansion, group-only execution, nullable
keys, hidden keys, changed expression parameters, WHERE/HAVING/DISTINCT, and
explicit/default/aligned time buckets. The F CASE witness returns high=10 and
low=2; two-second bucket sums are 2,4,6. A native bucket cohort checks five actual
reduction leaves, then forces local execution against the same independent
expected results. Parameterized ordinal cases change values while retaining the
same six input rows, so rebinding cannot be hidden by changing only scan bounds.

The runner executes 252 successful cold/warm and bypassed calls, three extra
forced-local parity calls, 23 rejected calls and 23 successful recovery calls:
301 verification RPCs per topology, besides readiness. Actual key-root entries
must equal independently identified unique roots times observed active batches.
Logical key row applications must equal unique roots times active WHERE rows;
constant scalar computation is counted separately. Repeated projections and
ordinal references must not add work. The bulk case checks 2,302 key cells
over 1,151 selected rows. Local cases require one complete QL raw scan, while
native cases require their declared branch count. Every call verifies restored
tracked memory and native ownership, with no intermediate DataPoints or native
Query RPCs. `-tags=querymetrics` is mandatory for these work assertions.

Mixed raw-time/bucket grouping separately verifies all six original timestamp
values, the three bucket values and local execution; bucket reduction must not
erase the original time key. T16 aligns its fixture base to six seconds so
independent two- and three-second bucket expectations remain epoch aligned.
Review regressions cover canonical duration widths, scoped HAVING over visible
and hidden raw-time keys, timestamp/field alias collisions, and DISTINCT with
visible versus hidden bucket keys. Structural work identity ignores rendering
parentheses and equivalent source qualification, while retaining each parameter
position even when two bindings have equal values.

The old T07 rejection `t07_invalid_shape_6` now uses a zero-width bucket instead
of a supported two-second bucket. Other prior queries and expected results are
preserved. The grammar fingerprint covers the new grouping parser, and
`make check-ql-test-cases` verifies deterministic generation.

```sh
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/standalone/query \
  -ginkgo.label-filter=QLB-T16 -ginkgo.fail-on-empty
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/distributed/query \
  -ginkgo.label-filter=QLB-T16 -ginkgo.fail-on-empty
```

Run the topologies sequentially. General ORDER BY/full-sort/top-k is covered by T17 below.


## T17 typed ordering and bounded pagination

`t17_generate.go` declares 32 scenarios with three bindings each, 12 no-scan
rejections, and four resource cases: 48 T17 specs. Twelve readiness pages reuse
the existing six-row F and 1,153-row T16 fixtures and add eight wide rows. The
wide fixture has exact int64 values above 2^53 and at both int64 limits, bytewise
strings including empty/NUL/non-ASCII values, nulls, infinities, two NaNs, signed
zero and string payloads over 16 KiB. Special floats are real protobuf doubles in
write requests and valid protobuf JSON strings (`"NaN"`, `"Infinity"`,
`"-Infinity"`) in expected responses. Generation never removes them to make JSON
encoding succeed. `FixtureDigest` hashes their encoded typed values as well as
the schema and the reused fixtures.

`t17_model.go` renders a finite typed ORDER BY model; `t17_oracle.go` evaluates
keys once, inserts complete tuples into an independently ordered list, and
slices only after WHERE, grouping, HAVING and visible-tuple DISTINCT. It imports
no production comparator, planner or execution operator. Exact integer order
never converts through float64. Null placement is resolved separately from
numeric direction, and ascending NaNs follow other non-null numbers. Every
multirow golden has a unique complete ordering key; equal complete-key order is
not asserted. The public `Generate` tests pin worked row orders and the composed
parent query independently of the finite oracle.

Families cover default and explicit null placement, both directions, one and
multiple keys, ties with a unique tie key, alias precedence, hidden raw and
aggregate sort keys, bare/qualified sources, bound expressions, grouped aliases,
HAVING, visible DISTINCT ordering, TIME_BUCKET metadata, empty intersections and
LIMIT 0. F ordered by `v DESC NULLS LAST, rid ASC` has rids 5,3,2,1,4,6; LIMIT 2
OFFSET 1 has 3,2. Four hidden-WHERE regressions keep the f predicate input outside the visible,
sort and grouping keys across raw, computed SELECT, grouped and DISTINCT stages.
Raw threshold bindings select rids 3,5,6, then 6, then none. The computed timestamp
alias uses `observed::TAG DESC NULLS LAST, rid ASC` and reverses the first order
to 6,5,3 while retaining each source timestamp. Grouped and DISTINCT
cases additionally verify top-k/full-sort parity. The full parent composition returns node b and total 4 at a
threshold of 0, then an empty page at threshold 4. Further compositions combine
CASE/COALESCE, ordinal grouping and AVG, and visible DISTINCT with multikey top-k.
The valid maximum LIMIT/OFFSET pair computes k=8,589,934,590 in uint64 and yields
an empty page; out-of-range individual values reject before scanning.

The runner uses the real BydbQL RPC for cold, warm/changed-binding and bypassed
execution. Each finite LIMIT family also forces full sort through the instrumented
context hook and compares the same independent ordered response. Sort statistics
assert exact input/output/key applications and independently modeled expression
kernel rows. Actual key-root calls must equal the finite model's root count times
observed active batches. Large full sorts obey the numerical comparison bound.
Candidates stay within k, source pins stay zero after copying, and payload
capacity stays within twice live payload plus separately reported descriptors.
Every call checks balanced sort reservations, total live reservations and native
result ownership. Readiness and verification issue no native Query RPCs.

Resource cases project a bound 15,360-byte string over the existing 1,153 narrow
rows. A 16 MiB shared budget exhausts full-sort candidates and a larger top-k;
the same 16 MiB budget admits a compact k=5 control before each pressure call. Cancellation is injected after a real input batch
has been copied, through the public instrumented hook, for both algorithms.
Resource errors require positive actual candidate acceptance, restored ownership
and successful subsequent RPC recovery. The low-level operator tests separately
verify allocation admission before allocation; the RPC hook controls cancellation
without replacing scanning or sorting. Initial 512 KiB and 2 MiB controls exposed
native preflights of 1,311,232 bytes and a further 2,988,800 bytes before candidate
ownership. The 16 MiB limit admits the narrow source; wider projected candidates
still exceed it for full sort and large k. This changes only the generated stress
fixture's limit and payload, not production admission limits.

The only earlier generated query changes are the two obsolete T03 ORDER BY
rejections: `t03_invalid_order_0` now names `absent_sort_key::FIELD` and
`t03_invalid_order_1` names `absent_sort_key::TAG`. Their former alias/source
orders are supported T17 cases. The ambiguous-alias rejection remains unchanged;
the existing T15 hidden DISTINCT sort key remains a valid rejection. All older
expected responses remain byte-identical. The longstanding legacy regressions
and pending coverage gaps above retain their original expectations and status.

```sh
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/standalone/query \
  -ginkgo.label-filter='measure-ql && QLB-T17' -ginkgo.fail-on-empty
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/distributed/query \
  -ginkgo.label-filter='measure-ql && QLB-T17' -ginkgo.fail-on-empty
```

Run the topologies sequentially. The runner reports completed/registered spec
counts, verification RPC counts (259 per complete topology), readiness page counts, actual retry-inclusive
readiness RPCs, and the fixture digest. `make check-ql-test-cases` verifies the
exact generated artifact; the grammar fingerprint is updated only after the
ORDER BY grammar source freezes and its coverage inventory is audited.

### Composable count-distinct coverage

The aggregate model also generates `COUNT(DISTINCT column)` alongside ordinary
aggregates. Its independent oracle excludes null values, counts an empty string
as a value, and deduplicates typed fixture values before counting. Scalar and
grouped cases cover multiple distinct aggregates, aliases, changed time bindings,
and empty input. Ordered cases compose distinct counts with `SUM`, `HAVING`,
multiple grouping keys, and deterministic tie-breaking. A 1,153-group fixture
checks a seven-row page at offset 1,100 after complete aggregation and ordering,
projecting `rid`, `COUNT(DISTINCT rid)`, and `SUM(v)`. Multiple grouping keys are
covered separately in the six-row fixture. This keeps the large-page correctness
case within the native input budget: on a host without a cgroup memory limit,
the default query allowance is 8 MiB. The wider three-tag version exceeds that
allowance even with ordinary `COUNT`, independently of count-distinct state.
Every scenario runs through the QL endpoint with cold, warm, and bypassed plan
caches in both integration topologies.

### Parent and child query shapes

`parent_child_fixture.go` and `parent_child_generate.go` add nine neutral generated
scenarios to the same artifact. One measure has parent-qualified child entities
and two time buckets: parent a has three distinct children and sum 42, b has two
and sum 17, and c has one and sum 4. Repeated a/child1 contributes to the sum in
both buckets while contributing once to its distinct count. The independent
oracle groups and sums all input before applying pagination. Literal unit checks
pin those results, active-parent count 3, idle scalar zero, and exact boundaries.

The cohort preserves the legacy scalar and TOP 500 statement shapes and adds
extended count-plus-sum ordering and scalar parent counting. Legacy TIME BETWEEN
explicitly includes a row exactly at its upper millisecond; extended >=/< windows
exclude that boundary. Legacy TOP verifies order when ranks are unique. Extended
ordering uses service_id as a deterministic tie break. A separate 137-parent
fixture reuses the local /child suffix under distinct qualified entity IDs. Its
global distinct child count is 137; tied middle and last pages contain parents
100–106 and 133–136. Fixture projections remain narrow under the native budget.
Every scenario changes bindings on the same statement and repeats with the plan
cache bypassed. Setup writes through the measure API and establishes visibility
through QL alone. No native query provides readiness or expected results.

```sh
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/standalone/query \
  -ginkgo.label-filter='measure-ql && QL-parent-child' -ginkgo.fail-on-empty
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/distributed/query \
  -ginkgo.label-filter='measure-ql && QL-parent-child' -ginkgo.fail-on-empty
```

`parent_child_legacy_scalar` and `parent_child_global_children` require one
integer-zero row for idle scalar input. The QL response boundary supplies that
row after successful empty native execution; grouped empty results remain empty.
The extended scalar alias in `parent_child_parent_total` independently exercises
the local aggregate's zero result. `parent_child_scalar_pages` verifies that
LIMIT 1 retains the zero row, LIMIT 0 suppresses even a nonempty scalar count,
and OFFSET 1 suppresses the idle scalar row. The native protobuf query API is unchanged.

The 137-parent fixture uses indexed QL readiness pages of at most 32 identities.
On a host without a cgroup memory limit, its full grouped count-plus-sum query
exceeds the default 8 MiB native query allowance. Verification also runs in the
standard four-CPU, 8 GiB container with no additional swap. Its unchanged default
protector recognizes the cgroup limit and uses 75% of it; no protector flags or
tracker budgets are overridden. The full 137-parent query succeeds there. The
idle scalar cases retain their integer-zero expectations.

After the empty-scalar correction, both container selections pass all nine specs,
including cached and bypassed executions with changed bindings. The final reports
are `/mnt/d/qlb-empty-scalar-standalone-report.json` and
`/mnt/d/qlb-empty-scalar-distributed-report.json`.

### Indexed LIKE and ILIKE

`pattern_fixture.go`, `pattern_generate.go`, `pattern_review_generate.go` and
`pattern_oracle.go` generate 31 scenarios for neutral names in an explicit keyword index and an index with its
analyzer omitted. Tokenized and missing indexes are rejection controls. One
identity has two timestamps, ensuring COUNT DISTINCT is applied after pattern
selection and field residuals. Tests retain separate case-distinct names.

The independent oracle tokenizes SQL `%`, `_` and escapes and uses dynamic
programming over Unicode scalars. It never calls production parser, evaluator or
native automaton helpers. `testdata/pattern-postgres.sql` and the saved JSON pin
20 ASCII/escape/NULL/empty/Unicode-character expectations from PostgreSQL 17.6
under C.UTF-8; provenance records the image digest. Unicode simple-fold behavior
has explicit independent unit expectations, including the final-sigma cycle and
no multi-character expansion.

The generated cohort covers whole-value/exact/prefix/suffix/infix matches,
consecutive percent signs, punctuation/newline/Unicode scalars, empty and NULL,
custom/disabled/default escapes, literal versus bound percent, invalid binding
recovery, omitted analyzer parity, field residuals, grouped COUNT DISTINCT/SUM
and HAVING, native index ordering with pagination, and explicit unsupported
Boolean/index/type shapes. Missing-index validation also runs with a NULL
pattern. Six additional ambiguity scenarios reject tags covered by two keyword
rules (both numeric-ID orders), standard plus keyword rules, or multi-tag plus
keyword rules (both binding orders). NULL and unmatched patterns must reject too.
The ambiguity tags are separate from the readiness indexes. Six additional
fixtures preserve `like`, `ilike`, `escape` and their hyphenated resource/group
names through ordinary indexed readiness and actual LIKE/ESCAPE queries.
LIKE/ILIKE/ESCAPE are contextual expression words; the audited grammar fingerprint
still includes their custom expression parser and the global reservation list.
These narrow source-name witnesses do not cover the broader pending quoted/dotted
identifier inventory. Each scenario executes through the QL RPC with normal and bypassed
plan caches. Ownership counters must return to their previous values.

```sh
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/standalone/query \
  -ginkgo.label-filter='measure-ql && QL-patterns' -ginkgo.fail-on-empty
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/distributed/query \
  -ginkgo.label-filter='measure-ql && QL-patterns' -ginkgo.fail-on-empty
```

Run topologies sequentially. The focused benchmark and before/after exact-query
control are documented in `verification/dpo85bench/README.md` at repository root.
