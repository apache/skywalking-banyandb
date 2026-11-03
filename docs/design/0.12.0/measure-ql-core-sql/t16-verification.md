# T16 computed grouping, ordinals and selectable buckets

Implemented on `9271e223` (T15), on PR #1412; the verified working-tree source is
identified below. T16 covers computed GROUP BY keys,
ordinal references and selectable TIME_BUCKET values. General sorting remains
T17. Protobuf/generated APIs, native operators, persisted/frame formats and
protector policy remain unchanged; temporary-file spilling is deferred.

## Execution contract

Plain multi-column grouping already works. T16 evaluates computed keys through
the existing typed batch kernels before passing their values to the shared SQL
group table. Multiple keys can combine CASE, casts, arithmetic and ordinary
columns. Equivalent resolved key expressions share a compiled key output;
repeating a key in SELECT does not evaluate it again. WHERE precedes key
evaluation; HAVING follows completed aggregation, followed by visible projection,
DISTINCT and final pagination. Computed keys use local SQL aggregation.

GROUP BY ordinals are positive integer SELECT positions after star expansion.
Zero, negative, out-of-range and aggregate-containing targets reject before
dispatch. Ordinals reuse the selected expression, including its parameter
identity. Separately written placeholders retain independent bindings and cannot
be collapsed merely because the surrounding expression text matches. Output
aliases name results; existing source-resolution rules remain in force.

TIME_BUCKET widths use existing native duration parsing and Unix-epoch floor
boundaries, including exact boundary behavior. Empty widths use the measure's
interval. Bucket timestamps remain part of grouping and native merge identity.
Extended default-width queries spanning different source intervals reject with
an explicit-width requirement. Original time and a computed bucket occupy separate
keys; grouping by both forces local execution to preserve original timestamps.
Eligible native aggregates retain their existing execution operators; local
fallbacks preserve the same bucket values. Projected timestamp aliases use the
existing expression response mapping. Calendar functions, windows and gap filling
remain outside this ticket.

Bucket matching uses resolved widths, so a two-second default, `2s` and `2000ms`
identify the same expression. HAVING first resolves completed grouped expressions;
an unmatched bucket can use raw time only when raw time is itself grouped. WHERE
and ungrouped projections reject TIME_BUCKET. Output slots retain their identity
through binding and HAVING, including a timestamp tag and field sharing an alias.
DISTINCT preserves bucket timestamp metadata only when the direct bucket key is
visible; hidden buckets do not change visible-tuple equality or supply timestamps.

The shared query tracker accounts for key-expression recipes and buffers, group
keys/state and final output. Compiled key recipes count toward the bounded plan
cache, while bindings and mutable operator state belong to one execution.
Close, cancellation and budget errors must restore live reservations and owners.
Group-expression metrics count actual root evaluation entries before scalar
memoization. KeyEvaluations counts active row applications from those entries;
it is not the number of scalar computations. Nested VectorRows and memoized
ScalarEvaluations report kernel work separately. A failed first key must not
count a later key that never starts. The fixed CASE controls also check exact
vector, scalar and selected-arm work independently of plan shape.

## Verification scope

Public tests use Prepare/Compile/Bind and complete batch/RPC execution with
independent expected values. The generated QL-only cases use independently
specified fixture writes and a separate finite-model oracle. Changed bindings,
cache hits and bypass exercise the same query texts. New scenarios do not require
native Query requests for readiness or verification. Existing native cases remain
compatibility coverage.

Fixture F has six rows with nullable values. CASE grouping at v >= 4 produces
sums high=10 and low=2, with identical results from ordinal references. Two-second
buckets produce sums 2,4,6 at T,T+2s,T+4s. Additional bounded witnesses cover
multiple/repeated keys, nulls, star ordinals, invalid targets, WHERE/HAVING/DISTINCT
composition and more than one native input batch.

The focused performance question is key evaluation plus aggregation compared with
equivalent stored keys, and explicit expressions compared with ordinal references.
The planned RPC matrix has six controls: stored, computed explicit and computed
ordinal keys at low/high group cardinality. Output schema, aggregate semantics,
input widths, expected values and scan counts remain comparable. A focused kernel
benchmark measures the same work without storage or final protobuf serialization.
No new cache, sorting or prior-ticket performance matrix is added.

Correctness, source freeze and independent work/cleanup checks precede timing.
Resource-limited Docker runs use four CPUs, 8 GiB, no additional swap, one query
worker and a 256-MiB query tracker. Successful-query measurements and bounded
large-input/low-budget probes are recorded separately. Admission failures cannot
be hidden by raising limits or counted as successful-query throughput.

## Review corrections

The follow-up fixes HAVING timestamp scope, canonical duration matching,
cross-container output names and visible bucket metadata through DISTINCT.
HAVING recipes refer to stable completed-output slots, so rebinding a timestamp
key cannot accidentally redirect an aggregate with the same display name.
Generated cases cover selected and hidden raw-time keys, equivalent bucket
widths, native/local bucket parity, DISTINCT over selected and hidden buckets,
same-container rejection, changed bindings and recovery after invalid queries.
The original 28 T16 scenarios and all prior artifact sections retain identical
query, expected-result and work objects; 14 scenarios and nine rejections are
added. Only the star fixture's interval changes from one to two seconds.

The finite oracle now identifies work structurally, independently of production
recipes. Parameter positions remain distinct even for equal bound values;
parentheses and equivalent source qualification do not create extra roots.
Runtime root calls are observed at evaluation entry. A public failure regression
proves that a later key which never starts contributes no work. Constant roots
retain one scalar computation while reporting their actual per-batch entries
and logical row applications. Fixed CASE benchmark expectations independently
count vector rows and selected arms; explicit/ordinal agreement alone cannot
satisfy the gate.

Related bounded cleanups preserve the original syntax-error location when a
GROUP BY rewrite fails, name GROUP BY in aggregate-argument errors, reject
boolean keys with a readable message, remove unused timestamp replacement code,
and use the declared bucket index consistently in native merge. Existing native
duration-parser behavior, including the separately noted day-suffix edge cases,
is unchanged. Parser redesign and native duration changes remain separate work.

## Correctness and compatibility results

The generator adds 42 scenarios with three bindings and 23 no-scan rejections:
65 T16 specs. Each topology executes 301 verification RPCs, plus 12 readiness
queries for the three new fixtures. All successful cold/warm and bypassed calls
check the independent oracle, actual key-root entries, exact logical row work
and resource release. Structural oracle identity preserves parameter positions
and ignores redundant parentheses and equivalent source qualification. The native
bucket cohort executes five real reduction leaves and additionally forces local
execution against the same expected values. The mixed raw-time/bucket scenario
preserves six original timestamps alongside three bucket values and asserts
local execution. The bulk witness evaluates two unique keys over 1,151 active
rows, for exactly 2,302 key cells.

Both resource-limited topologies pass the complete selected compatibility run:

| Topology | New T16 specs | All selected QL specs | Existing native specs |
|---|---:|---:|---:|
| Standalone | 65 | 589 | 120 |
| Distributed | 65 | 589 | 120 |

The 1,418 selected specs all pass. Unselected Ginkgo specs are outside this
verification. Reports are `.omx/t16-review-{focused,full}-{standalone,distributed}.json`,
with the matching logs and `.omx/t16-review-full-e2e-summary.json`.
The generated artifact SHA256 is
`f2271e6205a3d1046145c19261a8cb381321d1f0ef263432035afca34327873e`.
Prior generated scenarios and expected results remain unchanged, except that
T07's obsolete two-second-bucket rejection now rejects a zero-width bucket.

Public Prepare/Compile/Bind and complete operator suites pass in normal and
querymetrics builds: planner 326/327, QL runtime 210/219, liaison 495/499.
All 219 instrumented QL race tests and 55 focused planner race tests pass, as do
142 benchmark-harness tests. They include concurrent/fresh binding, source-name
collisions, estimated cache size, default widths, bucket boundaries before the
epoch, checked timestamp underflow, cancellation, budget failure and cleanup.
RED/GREEN evidence is retained in `.omx/t16-review-core-*.log`,
`.omx/t16-review-runtime-*.log` and `.omx/t16-generated-review-*.log`; package
counts are recorded in `.omx/t16-review-unit-summary.json`. Deterministic
`make check-ql-test-cases` and full `make lint` pass. The full linter additionally
caught and corrected a test helper's context-argument position. No protobuf,
generated API, native/data-node operator, protector or frame source changes are
present. The complete compatibility run and timings use the same frozen source.

## Frozen performance protocol

The six RPC controls are `simple_low`, `computed_explicit_low`,
`computed_ordinal_low`, and their high-cardinality counterparts. Low cardinality
has eight groups; high cardinality has N/2. Each query reads two INT fields and
returns `key`, `key_again` and nullable `total`. Stored and computed keys produce
identical schema and independently checked values. The repeated selected key
must not add evaluations. Computed explicit/ordinal forms must have identical
logical row/kernel work counts. Each variant checks actual root calls against
its observed active batch count; native batch boundaries may vary across calls.
Timings and reservation scheduling are recorded separately.

An initial T16 untimed 32,768-row admission probe established the fixed
successful-query schedule. Review corrections reuse it unchanged. Five isolated
repetitions use 100 timed calls and
five warmups per variant at 1,024 rows, and 20 calls/three warmups at 32,768 rows:
3,600 timed RPCs across 60 variant records. Each repetition uses fresh benchmark
processes for its two cardinalities and the same prebuilt binary. Independent
full results are checked before timing and on six retained responses afterward;
all raw latency samples, process allocations and CPU/heap profiles are retained.

The focused kernel measures equivalent stored/computed keys plus grouping at
the same cardinalities, with five one-second samples per control. A small
two-second-bucket control additionally checks identical output metadata and
values on both sides. This supplements the RPC comparison without adding a
bucket, cache, sorting or old-ticket RPC matrix.

Source/toolchain provenance is `.omx/t16-review-timing-provenance.json`; the frozen
2,435-file manifest is `.omx/t16-review-timed-source-manifest.sha256`. Its SHA256 is
`3be3ff9706e3ec23ac77f9b3174b9ee5dbe08c7854e770b766754402acbdec3f`.
The full E2E snapshot and final timing source are identical. Focused E2E preceded
only a test helper's context-argument ordering correction; its separate manifest
is `.omx/t16-review-focused-source-manifest.sha256`. Go is 1.26.9 linux/amd64, with
Docker image ID
`4cec35c89954c6a562dcc10d347b67e18a46d32347dbd02d47405191935771ac`.
No compilation or other verification runs overlap the final timings. These
measurements compare current feature paths, without changing the historical
G1/T02 verdict or asserting a cross-revision compatibility speedup.

## Large-input and low-budget probes

These initial T16 probes are bounded resource outcomes, separate from successful-query
throughput. Their source and binaries remain identified by
`.omx/t16-timing-provenance.json`; the review corrections do not change native
frame materialization or tracker limits, and the large fixture runs are not
repeated. The owning request is only `computed_explicit_high`, with five attempts after
complete QL-only fixture readiness. The same frozen binary and container limits
are used throughout; configured budgets and protector policy are unchanged.

| Rows | Budget | Readiness | Owning attempts | Recovery |
|---:|---:|---|---|---|
| 100,000 | 256 MiB | 100,000 rows checked with 112 QL queries | 5 protected rejections | Pass |
| 1,000,000 | 256 MiB | 1,000,000 rows checked with 992 QL queries | 5 protected rejections | Pass |
| 100,000 | 16 MiB | First bounded readiness query is rejected | Not reached | Readiness cleanup passes |
| 1,000,000 | 16 MiB | Same unchanged prerequisite blocker | Not repeated | Not claimed |

At 256 MiB, error chains identify the native raw-frame pipeline before local
grouping: all ten attempts have zero aggregate input rows and zero computed-key
evaluations. For 100,000 rows, a native tracker rejects an additional 17,320,160
bytes with 261,956,532 bytes already used against 268,435,456. At one million,
the corresponding request is 22,704,688 bytes with 251,962,036 already used.
No partial response escapes; reservations, owners and tracked pool references
return to baseline, and a subsequent bounded QL query succeeds after each cohort.
Process-wide peak reservations include one liaison and two separate data-node
trackers and must not be interpreted as one query tracker exceeding its limit.

At 16 MiB the first host-pruned readiness query requests 20,520,160 bytes with
1,577,568 already used against 16,777,216; it rejects before the computed-group
request can run. Repeating the million-row setup would exercise the same
prerequisite, so it is omitted rather than reported as a completed grouping
probe. These results do not establish 100,000/million-row computed-group
throughput. Per-operator peaks, native retained-frame bytes and physical storage
reads remain unavailable; the public small-budget operator tests independently
cover T16 expression/grouping cleanup. Streaming native input and disk spilling
remain separate future work.

Artifacts: `.omx/bench-reports/qlb-t16-resources/qlb_t16_resources_*rows_*mib.json`,
`.omx/t16-resource-summary.json` and `.omx/t16-resources-*.log`.

## Successful-query measurements

All five repetitions pass: 60 variant records, 3,600 timed RPCs, matching result
hashes, exact explicit/ordinal work, and restored tracker/owner/pool baselines.
The 50 kernel records also pass their independent schema/value and cleanup
checks. Source and binary hashes match before and after every run.

RPC p50 values below are medians of five runs, in milliseconds. The last column
is the median of the five within-run ratios, which need not equal the ratio of
the displayed medians.

| Input rows | Groups | Stored key | Computed explicit | Computed ordinal | Explicit/stored p50 |
|---:|---:|---:|---:|---:|---:|
| 1,024 | 8 | 3.894 | 4.005 | 4.078 | 1.025 |
| 1,024 | 512 | 5.800 | 6.029 | 5.959 | 1.000 |
| 32,768 | 8 | 36.667 | 37.543 | 38.550 | 1.012 |
| 32,768 | 16,384 | 89.957 | 91.090 | 90.974 | 1.009 |

The corresponding explicit/stored median paired p95 ratios are 1.019, 1.063,
1.070 and 1.048. Median paired allocated-byte ratios are 1.018, 1.024, 0.993 and
1.000. At 32,768 rows, median process CPU per query is 70.0/72.5 ms for
stored/computed low-cardinality keys and 153.5/161.5 ms for high cardinality.
These process-wide measurements include the client, liaison, data nodes,
serialization and GC; small allocation differences do not establish a feature
speedup. The retained profiles cover the complete process and cannot isolate
T16 CPU precisely.

Ordinal/explicit median paired p50 ratios span 0.984–1.015 and p95 ratios
0.969–1.060. Individual p50 ratios range 0.928–1.086; p95 can reach 1.276 and
p99 1.275. These short runs do not establish a latency advantage for either
spelling. Their complete results and logical work counts are identical. This
is measured execution cost with retained uncertainty, not a new fixed latency
gate or a claim about historical G1.

Every computed query evaluates one unique key per active input row; stored
controls evaluate zero. Both spellings perform one QL raw scan and two native
leaves. Observed native source/frame rows are 2N because both serving nodes hold
fixture replicas; aggregation receives N deduplicated rows. Low-cardinality keys
retain 80 encoded bytes and 128 state bytes. High-cardinality keys retain 5,120/
8,192 bytes at 1,024 rows and 163,840/262,144 bytes at 32,768 rows. SUM retains
two state lanes; repeating the visible key adds no state. Checked complete response
sizes match all three spellings: 463/28,240 bytes for low/high at 1,024 rows and
454/908,011 bytes at 32,768 rows.

Kernel medians include key evaluation, grouping and owned output. Both bucket
controls additionally emit the same timestamp metadata. Times are microseconds
per operation; extra bytes compare computed with stored keys.

| Input rows | Groups | Stored µs/op | Computed µs/op | Time ratio | Extra B/op |
|---:|---:|---:|---:|---:|---:|
| 1,024 | 8 | 129.547 | 188.772 | 1.457 | 39,360 |
| 1,024 | 512 | 216.582 | 281.176 | 1.298 | 39,360 |
| 32,768 | 8 | 3,999.588 | 5,131.944 | 1.283 | 42,336 |
| 32,768 | 16,384 | 7,354.925 | 8,499.208 | 1.156 | 42,341 |
| 1,024 (2s bucket) | 512 | 226.125 | 249.447 | 1.103 | 15,320 |

Compiled computed keys add real CPU and allocation cost to the isolated group
kernel. Buffers are reused across batches: the measured extra allocation grows
from about 39 KB at 1,024 rows to 42 KB over 32,768 rows, rather than allocating
an entire input-sized expression column. Group keys/state still grow with actual
group cardinality. Full RPC cost is dominated by additional shared work, so the
observed large-input latency differences are smaller than the kernel ratios.

Raw reports, samples and profiles:
`.omx/bench-reports/qlb-t16-review-r{1..5}/distributed-querybench.json`,
the corresponding `shards/` and `profiles/`, `.omx/t16-review-rpc-r{1..5}.log`,
`.omx/t16-review-kernel.log`, and `.omx/t16-review-benchmark-analysis.json`.
The fixed run plan is `.omx/t16-review-bench-run-plan.json`; actual Docker container IDs
are retained in `.omx/qlb-t16-review-{rpc-r*,kernel}.cid`.
Benchmark/kernel binary SHA256 values are
`16f993e1882606871e95d669395710c4ea294580f37dc9efcca5200e265543ee` and
`b94ae5667a67db52496708226e324b340be8258f6987d3fbf84a126e91f9c89e`.

T16's correctness, exact-work and available cleanup/resource assertions pass.
Large native-input materialization limits and unavailable operator-specific
peaks remain explicit; neither is hidden by widening the benchmark or changing
budgets. The final source, boundary, test and artifact audit is
`.omx/t16-review-final-audit.json`. Initial pre-review reports remain in
`.omx/t16-*` and `.omx/bench-reports/qlb-t16-r{1..5}`; the measurements above use
the corrected source and `t16-review-*` artifacts. T17 is the remaining Catalog B
capability ticket.
