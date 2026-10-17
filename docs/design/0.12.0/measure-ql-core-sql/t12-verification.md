# T12 raw field WHERE filtering

Implementation base: `8f7014ad` (T10), on PR #1412. Verification uses Go 1.26.9.
Changes are limited to QL parsing/planning, liaison batch operators, generated
QL-only cases and the owning benchmark. Protobuf, native/data-node operators,
frame and persisted formats, and memory-protector policy remain unchanged.
Temporary-file spilling remains deferred.
The post-review protocol is `.omx/t12-review-fixes-frozen-protocol.json`; hashes
for 582 production, test, fixture and harness files are
`.omx/t12-review-fixes-source-manifest.sha256`. The matrix and acceptance checks
are unchanged from the original T12 freeze.

## Execution contract

Measure WHERE now accepts T10's typed arithmetic, casts, comparisons, null tests,
IN, CASE, COALESCE and SQL boolean expressions over raw columns and bound values.
Only TRUE passes; FALSE and UNKNOWN drop the row. Bare NULL is a boolean UNKNOWN
in this context; a numeric or string predicate requires an explicit boolean cast.
The [language guide](../../../interacting/bydbql.md#58-field-and-expression-filters)
documents syntax, parameters, masks and compatibility limits.

The legacy grammar remains the first parser. Ordinary tag-only criteria retain
their routing and binding rules. Field references and mixed time/tag/field OR
activate typed residual execution. New WHERE grammar reuses T10's expression
parser and includes both grammars in the generated coverage fingerprint. A flat
legacy predicate normalized into a typed tree is depth-checked before recursive
compilation. Column kind and family qualification retain their existing meaning.

The native request retains only safe necessary tag constraints and a conservative
time envelope. Time comparisons retain T03's millisecond lattice, TIME intersection
and single captured clock. An OR branch without a time constraint cannot be pruned
by a time condition in its sibling. The complete residual remains authoritative.
Cached templates hold immutable symbolic recipes; each bind supplies fresh values
and time. No field/float predicate becomes an unsupported native tag criterion.

The pipeline is native raw scan, optional time residual, typed WHERE, visible
projection or T07 local aggregation, final OFFSET/LIMIT, then the existing response
adapter. Hidden predicate inputs survive time filtering and disappear at final
projection. A residual disables T08 native aggregation and input LIMIT/OFFSET
pushdown. Final LIMIT can stop later pulls after enough TRUE rows; it cannot cap
input before filtering. Omitted LIMIT has no implicit 100-row cap. LIMIT 0 skips
native execution. No intermediate DataPoints are constructed.

`WrapFilter` shares the compiled expression kernels rather than introducing a
second evaluator. Output borrows the physical input columns, schema and metadata,
and changes only selection. Nil selection means all rows; an empty selection
means none. Empty batches are skipped rather than treated as EOF. Existing
selections and null masks are respected. Physical length 65,536 is valid;
65,537-row external batches reject before uint16 conversion.

Legacy tag-only MATCH and infix collection HAVING/NOT HAVING remain available.
Their composition with new typed residuals rejects explicitly; T14 owns those
membership kernels and post-aggregate HAVING. Aggregate arguments/computed
grouping, DISTINCT and general multi-key sorting remain later capabilities.

## Ownership and resources

Expression metadata, cloned literals, typed scratch, masks and selections reserve
the shared query tracker before allocation. Buffer growth charges old/new overlap.
Borrowed columns are not copied into filter output. `filter_copied_bytes` is a
structural zero invariant, not independently measured copy telemetry. Unit tests
and K's pre-timing checks assert that output columns are the same objects as the
input columns; the RPC counter alone does not prove this. Charged protobuf-to-typed
scratch decoding is outside the invariant. Zero output copies is not a
zero-allocation claim.

EOF, source/evaluation errors, budget rejection, cancellation and idempotent Close
release filter state and source ownership exactly once. Unit tests exercise
selected/dense/nullable batches, no-match batches before a later match, metadata
and column identity, source errors, cancellation during pull, buffer growth,
oversized input, repeated Close and before-aggregate/pagination ordering. No new
pool is introduced; operator buffers are reused within an execution.

## Claude review fixes

The review found one legacy compatibility regression and five maintainability or
evidence issues. The fixes retain the T12 grammar and benchmark protocol:

- Legacy WHERE routing inspects column names before typed schema resolution.
  Ordinary tag-only queries can therefore ignore incompatible types in unused
  columns across groups. Queries requiring typed execution still validate types.
- Copy counters are documented as structural invariants. Unit and K column
  identity assertions provide the borrowing evidence; RPC counters alone do not.
- SELECT and WHERE bindings share one RHS IN-array expansion helper, including
  empty arrays. Parameter values remain fresh on every bind.
- Planner binding and runtime construction use `ql.ResolvePredicate` for the same
  BOOLEAN/NULL validation and immutable cloning rules.
- The filter owns its source directly. Its expression kernel borrows the source
  schema during compilation and subsequently owns scratch only.
- WHERE uses a dedicated hidden-input collector instead of changing visible
  slots. Typed columns and native entity metadata resolve once; safe tag pushdown
  and the native entity guard share one ambiguity map.

The public regression fails before the compatibility fix for unused field, tag,
and combined type differences, then passes afterward. RED/GREEN evidence is
`.omx/t12-review-planner-red.log` and `.omx/t12-review-planner-green.log`.
Public tests also compare unchanged legacy requests, reject incompatible actual
field filters, and exercise combined SELECT/WHERE array binding without exposing
hidden inputs. The generated compatibility case below verifies real RPC routing.

## Generated real-RPC coverage

The generator adds a dedicated 262-row fixture: six independently specified F
rows and 256 later rows. Forty scenarios each run three bindings, plus seven
rejections, producing 47 field-filter specs. One generated compatibility spec
adds two groups with matching selected types and incompatible unused field/tag
types. Three IN-array bindings return both groups, one group and no rows, with
cache reuse and bypass through the legacy QL-only RPC path. Together these make
**48 new specs**. Expected values come from the independent
fixture evaluator, never a native query or production QL execution. Changed
bindings, warm cache reuse and cache bypass are checked. Earlier generated query
texts and expected results remain unchanged.

Coverage includes v > 1 selecting F rows 2/3/5 with SUM 12 and COUNT 3, float
comparison, SQL null/IN behavior, masked errors, time/field OR and TIME intersection,
hidden fields, filtering before aggregation and pagination, matches after row 100,
omitted LIMIT, LIMIT 0, constant TRUE/FALSE/NULL, invalid predicates, low-budget
rejection, cancellation and successful recovery. The generator audits 36 T12
obligations. The grammar fingerprint is
`dfb57f35a1a04586cc86159469600da691177f281546cba4d5ad7062775ca7aa`.

Both post-review 4-CPU/8-GiB Docker topology runs pass **340 selected specs**:
48 T12 and 292 earlier-ticket specs. Standalone package runtime is 40.789 seconds;
distributed is 68.401 seconds. Reports are
`.omx/t12-review-fixes-standalone.json` and
`.omx/t12-review-fixes-distributed.json`. Suite lifecycle/report nodes are excluded.

## Focused benchmark protocol

P(T12), `qlb_t12`, uses a dedicated versioned fixture with nullable integer v,
nullable float f, and non-null `score = row % 100`. Sixteen variants combine four
predicate thresholds (0/1/50/100), raw v output versus local SUM(v)/COUNT(v), and
one versus two hidden predicate fields. Narrow predicates test `score < ?`; wide
predicates also require `COALESCE(f, 0) > -1000`, true for this entire fixture.
The independent oracle computes actual pass counts, including a partial last
100-row cycle. The one-percent cohort contains only null v values, so its
aggregate is SUM=NULL/COUNT=0 even when rows pass the predicate.

Successful input sizes are 1,024 and 16,384, with 100 timed/5 warmup queries and
20 timed/3 warmups, one worker and 256-MiB query trackers. Five fresh 4-CPU/8-GiB
Docker repetitions start a fresh cluster process for each cardinality. CPU/heap
profiles, process CPU, allocation snapshots and every latency sample are retained.
Warmups and six retained timed responses receive full independent value checks
outside timing; timed calls check output count. No query contains LIMIT. This
measures selectivity/width/output work, with no old-version speed baseline for
previously unsupported field SQL.

Work checks require one logical raw scan (two distributed data-node RPCs), all N
input rows examined, TRUE/output counts equal to the fixture oracle, and local
aggregate input equal to TRUE rows. Native aggregate/merge work, intermediate
DataPoints and the structural output-copy invariant must be zero. Column-identity
tests provide the copy evidence. Filter reservations must
be positive and balance releases. All tracked live memory, results, owners and
flatten references restore baseline. Every timed call hits the compiled cache.
Missing/forged evidence and shared cluster PIDs across cardinalities are rejected.
Physical storage reads, retained native-frame bytes and total parent lease charges
are unavailable in the timing reports and explicitly labeled.

All measurement containers enforce a 400,000/100,000 CPU quota, an
8,589,934,592-byte memory cap and zero swap allowance. No other T12 test,
resource probe or benchmark runs concurrently with timing. Unrelated background
services remain running on the host; measurements describe this bounded setup.

K(T12), `BenchmarkFilterT12`, has 24 variants: physical sizes 1/1,024/65,536,
four thresholds and dense/selected-null inputs. It times the complete public
one-batch lifecycle, including kernel construction, Init, drain to EOF and Close.
Five one-second samples record examined/passing throughput, ns/examined row,
allocations, charged selection/state bytes and output-copy bytes. Fixtures and
full row/column-identity checks stay outside timing. The zero-percent case still
examines the batch and reaches EOF; no sentinel changes its result. Actual pass
and UNKNOWN counts disclose threshold rounding and nulls. This is throughput
evidence, without a scalar speedup claim.

K(T12) completes all **24 variants × five samples** in 146.190 seconds. Every
case passes its independent examined/TRUE/UNKNOWN/selection checks, pre-timing
column-identity checks and cleanup assertions. Raw samples are
`.omx/t12-review-fixes-kernel.log`; medians and all five samples are
`.omx/t12-review-fixes-kernel-median.json`.

At physical length 65,536, medians are:

| Threshold | Dense ns/examined row | Selected nullable ns/examined row | Dense TRUE rows | Selected TRUE rows |
| --- | ---: | ---: | ---: | ---: |
| 0 | 17.00 | 21.17 | 0 | 0 |
| 1 | 25.88 | 26.55 | 656 | 328 |
| 50 | 22.31 | 23.16 | 32,786 | 13,120 |
| 100 | 16.25 | 23.05 | 65,536 | 26,215 |

Selected inputs examine 32,768 even physical positions and contain 6,553 UNKNOWN
rows. Dense inputs examine all 65,536 positions. Examined throughput is about
38–62 million rows/second for these typed inputs. At length 1,024, medians are
20–37 ns/examined row; one-row lifecycle medians are 3.8–6.0 microseconds.
These include constructor/compiler and Close overhead. They exclude native RPC,
protobuf input decoding and final response costs, which P(T12) includes.

At physical length 65,536 the lifecycle allocates roughly 133–205 KiB in 20–21
objects, including boolean/null scratch and a selection when needed. A 100%
TRUE dense batch borrows its original selection; a selected nullable batch still
needs a narrowed selection to remove UNKNOWN rows. All cases fit the kernel's
16-MiB tracker. Charged reservation bounds and selection bytes are retained
separately from measured heap allocations; zero output copies does not mean zero
allocation or zero predicate scratch.

P(T12) completes **five repetitions × 32 variants = 160 measured variants**, with
all independent value checks, required work counts, cleanup assertions and
**320 nonempty CPU/heap profiles** passing. Reports and raw samples are under
`.omx/bench-reports/t12-review-fixes-r1` through `t12-review-fixes-r5`;
`.omx/t12-review-fixes-rpc-summary.json` retains all per-run values, counters and
medians. All 582 frozen source hashes remain unchanged after measurement.

The table shows medians of five per-run measurements for the narrow predicate at
16,384 input rows. MB is decimal allocated bytes/query for the shared client,
liaison and two data nodes. All rows are examined even at threshold zero.

| Threshold | Output | TRUE rows | p50 ms | p95 ms | Allocated MB/query | Process CPU ms/query |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 0 | Raw | 0 | 24.573 | 27.254 | 21.545 | 52.5 |
| 1 | Raw | 164 | 24.829 | 27.606 | 21.502 | 54.0 |
| 50 | Raw | 8,200 | 37.549 | 41.896 | 28.451 | 73.5 |
| 100 | Raw | 16,384 | 50.268 | 57.374 | 36.568 | 92.0 |
| 0 | Scalar aggregate | 0 | 24.983 | 27.875 | 21.404 | 54.0 |
| 1 | Scalar aggregate | 164 | 24.698 | 27.312 | 21.325 | 53.0 |
| 50 | Scalar aggregate | 8,200 | 25.652 | 28.256 | 21.530 | 56.0 |
| 100 | Scalar aggregate | 16,384 | 26.405 | 29.757 | 21.290 | 56.5 |

Raw output grows with passing rows; scalar aggregation always returns one row.
Filtering reduces downstream work, while the required complete native scan keeps
roughly a 24-ms cost in these narrow cases. Adding the second hidden float field
and its masked COALESCE predicate increases median p50 by about 2.3–3.4 ms,
allocations by 3.2–4.4 MB/query and process CPU by 4–7.5 ms/query. This includes the
extra native input column's cost, rather than attributing the whole difference
to a boolean kernel. The complete wide/narrow matrix remains in the summary.

At 1,024 rows, narrow raw p50 grows from 3.598 ms with zero output to 5.743 ms
with all rows returned; scalar aggregate p50 is 3.404–3.624 ms across thresholds.
The wide full-output raw case takes 6.184 ms; wide scalar aggregates take
3.937–4.069 ms. There is no prior unsupported-SQL or old-ticket speed-gate claim.

Across five 16,384-row runs, narrow raw-zero p50 ranges 24.412–25.008 ms and
raw-full ranges 49.521–50.757 ms. Scalar-full ranges 25.225–27.020 ms. Raw samples
and ranges are retained; medians are observations of this setup, not universal
latency guarantees. Two representative CPU profiles parse successfully and show
native decoding, allocation/GC and final materialization among the measured work;
they do not provide a precise isolated filter-CPU attribution.

Every measured query examines N filter rows; native source/frame observations
are 2N because both physical leaves return replicated input. Predicate TRUE rows
match the oracle, aggregate input equals TRUE rows, and final output is the
selected raw count or one scalar row. All timed calls hit the compiled cache.
Filter reservation counters are about 55.5–105.9 kB/query at both input sizes:
scratch is reused across the native batches. Peak tracked process reservations
are at most 66,676,490 bytes at 1,024 rows and 232,994,172 at 16,384. All final
live memory, native results, owners and flatten references return to baseline.

### Why the successful size is 16,384

The initial 32,768-row smoke fails at `raw_narrow_p100`. A fresh single-query
diagnostic reproduces the failure after all 32,768 rows pass WHERE and serialize
to final DataPoints. `ReserveResponseEncoding` then requests 3,210,728 bytes,
while the existing cumulative parent lease has spent 201,162,588 of 201,326,592
bytes (192 MiB). Live tracker memory returns to zero after rejection; raw-50%
and aggregate-100% recovery queries both pass.

The 8-GiB container's unchanged protector assigns a 6-GiB protected allowance,
1.5-GiB query pool and 192-MiB maximum per-query parent lease. The 256-MiB query
tracker setting does not enlarge that separate lease. Admission plus native
receive/frame charges already consume 151,332,476 cumulative bytes before QL
output. Tracker Release refunds live reservations; it does not refund cumulative
parent charges. The extra required hidden field increases native input width
(a code-based inference relative to T10, without rerunning old benchmarks).

The successful size changes before source/protocol freeze. The failed 32,768-row
smoke remains `.omx/t12-rpc-large-smoke.log`; the minimized diagnosis is
`.omx/t12-32768-encoding-limit.json` and `.omx/t12-single-raw-diagnostic.log`.
Temporary Go-overlay instrumentation is removed. No production/policy change
is made to fit the benchmark.

## Large-input native limits

These native-boundary probes precede the review fixes. Native execution, memory
policy, resource harness and probe queries are unchanged, so their original
outcomes remain recorded separately from the post-review K/P measurements.

Owning resource probes use `aggregate_narrow_p50`, with a complete raw scan and
expected TRUE counts 50,000/500,000. At 256 MiB, all 100k/1m fixture rows are first
verified independently through bounded QL-only host/time reads, outside measurement.
The probes then issue five full queries each. All ten reject with ResourceExhausted
in native frame production before QL filtering/aggregation begins. Each has two
physical native leaf RPCs, zero native aggregate leaves, no partial output, zero
post-close memory/results/owner/flatten-buffer counts, and successful bounded QL
recovery. These are protection/cleanup results, not successful filter throughput.

| Rows / query tracker | Outcome | Process peak tracked bytes |
| --- | --- | ---: |
| 100,000 / 16 MiB | First bounded readiness query rejects; no owning samples | 24,694,306 |
| 100,000 / 256 MiB | Fixture verified; five native memory rejections; recovery passes | 552,784,810–590,317,554 |
| 1,000,000 / 256 MiB | Fixture verified; five native memory rejections; recovery passes | 586,686,133–587,242,584 |

The 16-MiB native decoder requests 15,882,848 bytes while 1,845,108 are held,
exceeding 16,777,216; readiness stops immediately. The 1m/16-MiB repetition is not
run because the same prerequisite is already blocked. The 100k/256-MiB query
fails near 263.9 MB plus a 13.2-MB reservation; the 1m query fails at 261,665,542
plus 23,585,072 bytes against 268,435,456. Peak counters sum three independent
trackers in one process; their bound is three query-tracker budgets, not one.
Individual tracker peaks, retained native-frame bytes and total cumulative parent
lease charges remain unavailable and are labeled accordingly.

Reports are `.omx/t12-resource-reports/qlb_t12_resources_*`. The 16-MiB readiness
test remains a recorded blocker, rather than an asserted owning-query pass.
These native limits preserve T07's known limitation and deferred spilling decision.
Spilling local aggregate state alone would not remove native decoding limits or
the cumulative final-encoding allowance; those boundaries need their own work.

## Verification

Full normal tests pass for `pkg/bydbql`, `pkg/query/ql`, `banyand/liaison/grpc`, the
QL generator and CLI/MCP parser consumers. Tagged race tests pass for the planner,
filter engine, liaison, generator and focused T12 benchmark/report harness.
`make lint` passes. Generated artifacts pass deterministic regeneration checks.
The report validator rejects incomplete variants, changed protocols, forged
counts, missing or unbalanced reservations, native aggregate work and leaks.

Post-review logs are `.omx/t12-review-planner-{normal,race}.log`,
`.omx/t12-review-engine-{normal,race}.log`,
`.omx/t12-review-generator-{full,race,check,lint}-final.log`,
`.omx/t12-review-fixes-grpc-normal-serial.log`,
`.omx/t12-review-fixes-grpc-race.log`,
`.omx/t12-review-fixes-consumers-targeted.log`,
`.omx/t12-review-fixes-bench-contract-race.log` and
`.omx/t12-review-fixes-make-lint.log`. The combined normal-consumer run has one
liaison startup failure: overlapping normal/race suite processes both bind the
existing default port 17912. Its log preserves `address already in use`; the
full normal liaison suite passes when run alone (20.705 seconds), and the full
tagged race suite passes (29.720 seconds). Parser and benchmark package tests in
the combined run pass. No source change is made for this scheduling failure.
At the pre-commit checkpoint, all 62 changed/new Go files pass the explicit
formatter diff; all 63 Go/shell files have ASF headers, shell syntax passes,
and the Git index is empty.

`make check` runs after commit: it requires a clean committed tree and stages
formatting changes. Lint, generator consistency, source-header checks,
shell syntax and the affected formatter checks run without staging user work.
No old-ticket performance matrix is rerun and T02's earlier inconclusive
performance acceptance remains unchanged.
