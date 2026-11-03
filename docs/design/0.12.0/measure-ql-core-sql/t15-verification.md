# T15 visible-tuple DISTINCT

This record preserves the initial T15 implementation and its measured baseline.
The subsequent [memory follow-up](t15-memory-verification.md) replaces per-key
allocations, adds unordered finite-LIMIT stopping and reduces shared native
frame buffering. Its source changes and measurements are recorded separately;
the complete-consumption and native-source descriptions below refer to this
initial baseline.

Implementation base: `a6f1250b` (T14), on PR #1412. Verification uses Go 1.26.9.
The real RPC cases, focused kernel/RPC benchmarks and resource probes are complete.
Protobuf, native/data-node operators, persisted/frame formats and
memory-protector policy are unchanged. Temporary-file spilling remains deferred.

## Execution contract

`SELECT DISTINCT` keys the entire visible projected tuple after WHERE,
aggregation and HAVING, before final OFFSET/LIMIT. Aliases do not change equality;
hidden predicate/group/aggregate inputs and incidental native timestamps or series
identities do not enter the key. Explicitly selected timestamps remain values.
The existing response format separates tag families and fields, so it does not
expose a generic mixed-column SQL schema.

The liaison reuses T07's null-aware group table with no aggregate calls or state
lanes. Null differs from zero and empty values. Variable-width key cells include
length boundaries; signed floating zeros normalize together and all NaNs share
one key. Integer, double, string, binary and timestamp outputs are admitted;
projected arrays reject during binding. Existing COUNT(DISTINCT) restrictions
remain in place. DISTINCT adds no general sorting support, and hidden ORDER BY
inputs reject before dispatch.

The operator consumes complete input before emitting unique keys. It shares the
query tracker with its input and final response, reserves before allocation and
keeps old and new buffers charged during growth. Final LIMIT does not bound the
number of keys retained. Budget failure, cancellation, upstream errors, early
Close and normal completion release live reservations and close upstream owners.
Dedicated DISTINCT counters distinguish this key table from an upstream local
aggregate or native merge.

Typed named GROUP BY without aggregate calls now reaches the existing local
group table. This also supplies the zero-state benchmark reference and permits
DISTINCT to discard unselected named grouping keys. Existing legacy
`FROM MEASURE ... IN ...` grouping retains its native routing. Computed keys,
ordinals and time-bucket extensions remain T16 work.

## Findings during verification

The generated HAVING composition exposed a parser gap: SELECT aggregate arguments
accepted an outer qualifier, such as `SUM(v)::FIELD`, but rejected the inner
`SUM(v::FIELD)` form already accepted inside HAVING expressions. Both forms now
resolve the same measure input. Combining inner and outer qualifiers rejects,
and inner qualification is restricted to measure queries. Public RED/GREEN
evidence is `.omx/t15-qualified-aggregate-{red,guard-red}.log`; generated coverage
and its grammar fingerprint include this form.

The four-value zone witness selects rows with `CAST(rid AS INT)` bounds. This
deliberately uses the existing QL residual evaluator: a bare range on the
fixture's unindexed entity tag reaches an unsupported native condition. The
case does not establish support for that native condition.

The host reports no cgroup memory limit, disabling its protector and selecting
the existing 64-MiB admission pool with an 8-MiB per-query fallback. Initial host
attempts exhausted that fallback before DISTINCT execution. The unchanged T08
native route failed too. Controlled end-to-end runs therefore use the same
four-CPU, 8-GiB, no-additional-swap Docker limits as timing, without changing
protector policy. Diagnosis is `.omx/t15-native-fallback-diagnosis.md`.

A controlled run reached the 2,201-row timestamp result but spent minutes in the
test verifier's unordered all-pairs comparison. Exact rows now use normalized
protobuf keys to narrow candidate matches, followed by the same full comparison
and one-to-one duplicate matching. Float-bearing responses, recursive unknown
fields and encoding failures retain the original matcher, preserving approximate
nontransitive equality. A shuffled 1,024-row public regression first timed out at
five seconds; the full data package now completes in about 0.3 seconds. The
generated SQL and expected values were preserved. Evidence is
`.omx/t15-comparator-{red,green,race,lint}.log`.

The first RPC schedule selected 16,384 rows as its larger successful-query input.
Its first wide-string DISTINCT warmup failed with native `query memory pool
exhausted`. A fresh cluster with only that wide dataset independently verified
all 16,384 raw rows, then reproduced the error on its first DISTINCT request.
DISTINCT execution and reservation counters remained zero and cleanup passed.
Preceding benchmark variants and six-dataset fixture pressure are therefore not
required to trigger the failure. The separate admission allowance can also be
lower than the configured 256-MiB tracker; the generic error does not establish
the precise rejecting owner or byte count.

The same fresh probe at 8,192 rows returned the correct 81 tuples, exactly
23,328 encoded key bytes, no aggregate states and no live resources after Close.
The successful-query schedule therefore uses a uniform 8,192-row second input
for all 14 variants, keeping widths, duplicate ratios, budget and CPU/container
limits unchanged. This does not establish 16,384-row wide-input support. Initial
evidence remains in `.omx/bench-reports/t15-failed-initial-r1`,
`.omx/t15-rpc-r1-failed-16384.log`, `.omx/t15-fresh-wide16384.log` and
`.omx/t15-fresh-wide8192.log`; the initial schedule and source manifest are
preserved separately before the final timing snapshot.

An untimed replay then loaded the actual six-dataset fixture at 8,192 rows per
dataset and passed all 14 variant oracles and post-query cleanup checks. The
paired DISTINCT/GROUP BY checksums match at 81, 4,096 and 8,192 unique tuples and
zero for empty input. This is prerequisite correctness evidence, not a timing
sample: `.omx/t15-full14-untimed8192.log`.

## Verification protocol

Public planner and batch-operator tests establish RED/GREEN behavior and resource
ownership. Generated QL-only scenarios use independent fixture writes and an
independent response oracle, then execute through the real BydbQL Query RPC in
standalone and distributed topologies. New scenarios repeat changed bindings
against cold/warm compiled plans and cache bypass. Neither setup readiness nor
verification issues a native measure Query request.

The focused `BenchmarkDistinctTupleKernel` benchmark times complete DISTINCT
construction, ingestion, draining and Close. An independently implemented Go
tuple-map oracle checks the values outside timing; it is not a timed competitor.
The eight timed variants use 1,000 input rows, 1%, 50% and 100% unique tuples,
eight or 256 padding bytes per string, plus empty input at each width. Each
variant has five one-second samples. Its scope is deduplication and retained
keys; it excludes storage and protobuf response construction.

The distributed `qlb_t15` scenario compares DISTINCT with an explicit GROUP BY
over the same visible two-column tuple, without aggregates. Both return the same
independently checked values. The matrix contains 14 variants: DISTINCT and GROUP
BY at 1%, 50% and 100% unique tuples, each with narrow/wide strings, plus the two
empty-input variants. The 1% cardinality uses `max(1, floor(rows / 100))` and the
actual count is reported. This is a reference for execution cost and memory,
not an old-version speedup claim: T14 does not implement SELECT DISTINCT.

Ordinary input cardinalities are 1,024 and 8,192. Each has five isolated Docker
repetitions, one worker, a 256-MiB query budget, four CPUs, an 8-GiB container limit
and no additional swap. The small input has 100 timed queries and five warmups per
variant; the larger input has 20 and three. Correctness/readiness and expected
checksums precede timing; sampled timed responses are verified afterward. CPU and
allocation profiles include the client, liaison and both data nodes. Actual
complete response byte sizes are measured outside the timing/profile window.
Unavailable per-operator or lease observations are explicitly unavailable.

Separate bounded resource probes use 100,000 and 1,000,000 input rows with 16-MiB
and 256-MiB query budgets. Their outcomes and native input prerequisites are
reported separately from successful-query throughput. No new cache matrix or
old-ticket benchmark suite is added.

## Results

### Correctness and compatibility

Both complete, selected QL suites pass under four CPUs, 8 GiB and no additional
swap, using the same production planner/operator sources as the focused benchmarks:

| Topology | Selected specs | Existing specs | New T15 specs | Outcome | Suite time |
|---|---:|---:|---:|---|---:|
| Standalone | 521 | 503 | 18 | All pass | 153.0 s |
| Distributed | 521 | 503 | 18 | All pass | 248.7 s |

The T15 cases add 21 coverage obligations, 15 changed-binding scenarios and three
binding rejections. The fixture contains 2,201 independently specified writes and
35 QL-only visibility windows. Existing generated case sections remain equal to
T14; feature inventory, grammar fingerprint and fixture digest change with T15.
Evidence is `.omx/t15-docker-standalone-v3.{log,json}`,
`.omx/t15-docker-distributed-final.{log,json}` and
`.omx/t15-generated-artifact-baseline.log`. Unselected Ginkgo cases are outside
this run, not counted as selected passes.

Public planner/operator and generated-oracle tests pass, including race runs;
the liaison gRPC race suite and CLI/MCP parser packages pass. Full `make lint`
and deterministic `make check-ql-test-cases` pass for the completed production
and generated-case sources; reporting-only follow-up checks are recorded below.
All 55 changed Go/shell files have ASF headers, `git diff --check` is clean, and
no protobuf or native/data-node source is changed. The 644-file timed source manifest
is `.omx/t15-timed-source-manifest.sha256`; the 645-file final manifest adds the
reporting regression and is `.omx/t15-final-source-manifest.sha256`. The schedule is
`.omx/t15-frozen-protocol.json`. Performance verification below does not rerun
the old G1 legacy benchmark matrix or change T02's prior gate verdict.
After the schedule correction, full lint and the benchmark package's focused
normal/querymetrics tests pass again. At the timing freeze, four benchmark source
files differed from the completed E2E snapshot; production, kernel and generated
cases were identical. That delta is `.omx/t15-refreeze-delta.json`, and the last
full-lint log is `.omx/t15-refrozen-make-lint.log`. The subsequent report-only fix
and its affected checks are described with the RPC results below.

### Focused performance

The complete kernel has 40 samples, five for each variant. Every sample has zero
aggregate state lanes, exact unique-tuple count/key bytes and zero live tracker
bytes after Close. Medians below time construction, ingestion, draining and Close;
they do not include storage or protobuf responses.

| Padding/string | Unique/input | Median µs/op (min–max) | Heap B/op | Allocs/op | Retained key B |
|---:|---:|---:|---:|---:|---:|
| 8 | 0/0 | 4.85 (4.27–4.88) | 4,104 | 40 | 0 |
| 8 | 10/1000 | 159.54 (157.09–171.02) | 6,064 | 67 | 480 |
| 8 | 500/1000 | 296.61 (285.52–312.78) | 71,888 | 567 | 24,000 |
| 8 | 1000/1000 | 411.66 (388.09–475.13) | 138,896 | 1,069 | 48,000 |
| 256 | 0/0 | 4.70 (4.51–5.59) | 4,104 | 40 | 0 |
| 256 | 10/1000 | 208.38 (206.31–211.20) | 12,304 | 67 | 5,440 |
| 256 | 500/1000 | 544.80 (519.53–630.10) | 336,849 | 567 | 272,000 |
| 256 | 1000/1000 | 871.92 (862.18–925.28) | 667,857 | 1,069 | 544,000 |

Key storage is exactly 48 or 544 bytes per unique tuple for these two kernel
fixtures. Heap allocation includes group/output capacity and is not the retained
key-byte metric. These results establish the measured cost and unique-key scaling,
not speedup over another implementation. Raw evidence is `.omx/t15-kernel.log`
and `.omx/t15-kernel-analysis.{json,md}`.

The five isolated distributed repetitions validate 140 flat result records,
8,400 timed RPCs, 280 nonempty CPU/heap profile files and 840 independently
checked complete response envelopes. Every measured query hits the compiled plan
cache, uses one raw scan/two physical leaf calls, emits the expected unique tuple
count and retains zero aggregate states. Exact key bytes, balanced reservations
and post-Close owner/tracker/pool baselines pass in every variant.

The table shows medians of five per-run quantiles. The ratio is the median of
five within-run DISTINCT/GROUP BY p50 ratios; it is not the quotient of the two
displayed medians. Heap allocation includes client, liaison and both data nodes.

| Rows | Tuple shape | DISTINCT p50/p95/p99 ms | GROUP BY p50/p95/p99 ms | Paired p50 ratio | Heap MiB/query D/G | Key B/query |
|---:|---|---:|---:|---:|---:|---:|
| 1,024 | p001_narrow | 5.06/7.17/9.23 | 4.86/7.61/9.29 | 1.060 | 2.20/2.22 | 320 |
| 1,024 | p001_wide | 5.97/8.51/9.80 | 6.04/8.27/9.95 | 1.009 | 3.53/3.45 | 2,880 |
| 1,024 | p050_narrow | 6.96/9.92/11.31 | 6.73/10.33/11.27 | 1.016 | 3.02/3.03 | 16,384 |
| 1,024 | p050_wide | 8.32/11.84/12.45 | 8.39/12.33/13.58 | 0.960 | 4.99/5.01 | 147,456 |
| 1,024 | p100_narrow | 8.36/13.09/14.23 | 8.41/12.54/13.73 | 0.973 | 3.85/3.85 | 32,768 |
| 1,024 | p100_wide | 10.75/14.96/16.78 | 10.38/14.89/16.17 | 1.006 | 6.51/6.63 | 294,912 |
| 1,024 | empty | 2.29/3.29/4.00 | 2.31/3.08/4.32 | 0.980 | 0.56/0.56 | 0 |
| 8,192 | p001_narrow | 14.64/19.64/20.79 | 15.02/19.60/21.73 | 0.972 | 12.19/12.12 | 2,592 |
| 8,192 | p001_wide | 19.93/26.90/27.73 | 20.04/25.99/27.86 | 1.010 | 23.54/23.48 | 23,328 |
| 8,192 | p050_narrow | 25.65/32.93/34.11 | 25.91/34.06/35.74 | 0.992 | 18.51/18.45 | 131,072 |
| 8,192 | p050_wide | 35.69/42.52/46.17 | 36.85/42.35/45.01 | 0.972 | 36.04/35.73 | 1,179,648 |
| 8,192 | p100_narrow | 36.27/47.50/48.47 | 36.35/46.41/46.55 | 0.996 | 24.55/24.52 | 262,144 |
| 8,192 | p100_wide | 50.18/59.99/64.79 | 48.44/59.06/60.54 | 1.031 | 44.87/44.24 | 2,359,296 |
| 8,192 | empty | 2.13/2.64/2.73 | 2.09/2.54/2.68 | 0.995 | 0.56/0.56 | 0 |

Across shapes, median paired p50 ratios range from 0.960 to 1.060. The
8,192-row, fully unique wide tuple has a 1.031 median p50 ratio and a 1.071
median p99 ratio; its five p99 ratios range from 1.024 to 1.145. These are
observed costs against the same reused group-table algorithm, not a claim of
statistical significance, independent-algorithm speedup or a legacy G1 verdict.

All five runs retain exactly 32 or 288 key bytes per unique tuple, and key
capacity rather than input duplicates determines the arena reservation. Empty
input retains no keys. Full per-run latency/CPU/allocation arrays, key capacity
and actual complete response bytes are in `.omx/t15-rpc-analysis.{json,md}`;
the five original reports are `.omx/bench-reports/t15-r{1..5}/distributed-querybench.json`.
CPU/allocation profiles include the whole distributed process. Exact native
retained-frame bytes, physical storage rows, cumulative parent-lease charges
and per-operator/tracker high-water marks remain explicitly unavailable.

The initial merge validated T15 shards but omitted the append into the flat
report. A real shard-to-report regression reproduced zero of 28 variants, then
passed with the one-line append fix. Only report processing and its tests differ
from timed sources. All five reports were regenerated without running queries;
SHA-256 checks preserve all ten raw shards and 280 profiles byte for byte.
Original empty reports remain as `distributed-querybench.pre-merge-fix.{json,md}`.
Evidence is `.omx/t15-merge-{red,green,normal,querymetrics,lint}.log`,
`.omx/t15-remerge-r{1..5}.log`, `.omx/t15-report-only-delta.json` and
`.omx/t15-raw-artifacts-before-merge.sha256`.

### Large resource probes

The resource protocol runs one 100%-unique narrow-string dataset, rather than
expanding the ordinary 14-variant matrix. At 256 MiB, independently checked,
bounded QL readiness reads verified all 100,000 rows in 112 queries and all
1,000,000 rows in 992 queries. Five DISTINCT attempts and five GROUP BY reference
attempts at each size returned `ResourceExhausted` with no partial response.
The error explicitly identifies native raw-frame materialization before local
grouping. At 100,000 rows, one reported reservation was 263,942,027 bytes followed
by a rejected 14,096,000-byte request against 268,435,456 bytes. At 1,000,000 rows,
the native pipeline rejected another allocation before frame emission completed.
These are native input limits, not measured DISTINCT throughput or key-table
capacity. All attempts released live resources and both datasets passed subsequent
small-query recovery.

At 16 MiB, the first bounded readiness query already returned
`ResourceExhausted`; zero rows were verified. Cleanup passed, but owning-query
execution and recovery were not certified. The same failed prerequisite was not
repeated with a 1,000,000-row write. Its report is explicitly marked not run.

Artifacts are `.omx/t15-resource-reports/qlb_t15_resources_*` and
`.omx/t15-resource-{100000-256,100000-16,1000000-256}.log`. Resource probes preceded
the final aggregate-argument grammar, test-verifier and benchmark-report fixes;
their query texts, key-table implementation, native operators and memory policy
are unchanged.
Their elapsed/CPU/allocation observations overlap correctness checks and are not
used as isolated performance measurements. Global high-water reservations combine
one liaison and two independent data-node trackers; exact per-operator peaks and
total parent-lease charges remain unavailable.
