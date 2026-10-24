# Catalog B PR 4: grouping and result shaping

This branch integrates main at `86659118` with the expression/filter dependency
from [PR #1432](https://github.com/apache/skywalking-banyandb/pull/1432) at
`349222f7`. PR #1432 is still open as this record is prepared. The PR 4 extraction
baseline is `4682fc5f`; changes after it add T15 DISTINCT, T16 computed/ordinal
grouping and selectable time buckets, T17 ordering/pagination, and exact
count-distinct composition. The PR must follow #1432 when merging.

## Behavior and boundaries

```text
parameterized QL -> reusable plan -> fresh bindings and native scan
  -> WHERE -> complete aggregation -> HAVING -> visible expressions
  -> DISTINCT -> typed multi-key sorting -> final OFFSET/LIMIT
  -> existing measure response
```

Computed group keys evaluate before aggregation. Exact count-distinct retains
complete value sets and never sums overlapping native partial counts. DISTINCT
uses the visible tuple, with shared memory accounting and early stopping only
where unordered finite pagination permits it. Finite ordered pages retain at
most OFFSET + LIMIT owned candidates; unlimited ordering uses budgeted full sort.
NULL placement and deterministic tie-break keys are explicit query choices.

Existing native queries, lifecycle-safe empty scalar-count completion and native
memory protection retain their merged behavior. Latest main's native schema
coercion, batch merge and array fixes are preserved. This PR adds no protobuf,
frame format, persisted storage, toolchain or workflow change. The standalone
suite now declares a 4 GiB server allowance, giving these larger combined
fixtures a bounded 128 MiB per-query admission limit; the diagnosis is below.

JOINs, CTEs, directory synchronization and full DPO application parity are outside
this PR. Parent-child generated cases operate on one measure. Spill and retained
page sessions remain deferred. Historical T02 uncertainty and deferred successful
100k/1m owning-query throughput are not changed by this verification.

## Verification on the isolated branch

Validation uses source revision `d612c057` (subsequent verification-only edits do
not change source). `make generate`, `make lint` and `make check` pass; generated
QL fixtures are current. Race tests pass for `pkg/bydbql`, `pkg/query/ql`, all
`pkg/query/vectorized/...` packages, the focused measure/QL/cache liaison tests,
and the querybench package (567.636 seconds). The broader liaison exception is
recorded below.

Standalone generated QL and native measure compatibility integration passes
**935/935** selected cases in 187.974 seconds, inside a four-CPU/8-GiB container
with no extra swap. Report: `/mnt/d/qlb-pr4-standalone-final-report.json`.
Distributed integration passes **935/935** selected cases in 299.957 seconds
under the same container limits. Report:
`/mnt/d/qlb-pr4-distributed-report.json`. Both suites select
`vec independent verification.*(scanning measures|QL)` with `-ginkgo.fail-on-empty`;
this covers native measure compatibility and generated QL-only execution, not
all stream/trace suites.

The race checks are:

```sh
go test -race -tags=querymetrics ./pkg/bydbql ./pkg/query/ql ./pkg/query/vectorized/...
go test -race -tags=querymetrics ./banyand/liaison/grpc -run 'Measure|QL|BydbQL|Compiled' -count=1
go test -race -tags=querymetrics ./test/integration/distributed/querybench
```

Historical ticket evidence remains in [T15](t15-verification.md), [T16](t16-verification.md),
[T17](t17-verification.md), the [T15 memory follow-up](t15-memory-verification.md)
and the [count-distinct follow-up](implementation-tickets.md#dpo-81-count-distinct-composition-follow-up).
These historical records do not certify the current PR head.

## Scoped benchmark refresh

The final source is measured in isolated four-CPU/8-GiB Docker containers with
no extra swap, `GOMAXPROCS=4`, Go 1.26.10, and a 256 MiB RPC query tracker.
Timed runs start only after integration and race checks finish. Three repetitions
characterize this extraction; they do not replace historical five-repetition
gates or change T02's inconclusive verdict.

All **51 kernel controls × 3 repetitions (153 samples)** pass, with zero cgroup
memory events. Examples of medians:

| Control | Result |
| --- | --- |
| 100k-row top-k vs full sort, page sizes 10 / 100 | Time ratios 0.0308 / 0.0309; allocated-byte ratios 0.000725 / 0.001878. |
| DISTINCT finite page vs full-input control | 15 vs 1,000 input rows; 18.36 vs 404.31 microseconds; 22.2 vs 146.2 KB allocated. |
| Exact count-distinct, 64 / 65,536 unique keys | 8.48 / 14.66 ms; 6.6 KB / 2.69 MB allocated. Ordinary count is about 2.55 ms / 1.9 KB. |

These are operator controls, including the expected cost of exact distinct state;
the 100k-row kernel is not evidence of successful 100k-row owning RPC throughput.
Kernel artifacts are `/mnt/d/qlb-pr4-bench/kernel.txt` and `kernel-summary.json`.
RPC measurements use 1,024/8,192 rows for T15 and 1,024/32,768 rows for T16/T17.
Each small variant uses five warmups and 100 measured calls; each larger variant
uses three warmups and 20 measured calls. T17 composed queries use 96 calls at
one/eight workers. All variants use QL. The three repetitions produce **18 reports,
144 variant measurements, 9,072 timed calls and 288 profiles**. All timed calls
succeed; exact-result/hash/cache/cleanup checks pass and all cgroup memory events
are zero. No budget or protocol adjustment was made during timing.
The table reports the median of each repetition's subject/reference ratio,
then the range across controls. KB and MB here are decimal units.

| Comparison | Rows | p50 latency ratio | p95 latency ratio | Allocated-byte ratio |
| --- | ---: | ---: | ---: | ---: |
| DISTINCT / equivalent GROUP BY, seven controls | 1,024 | 0.977–1.046 | 0.930–1.067 | 0.993–1.016 |
| DISTINCT / equivalent GROUP BY, seven controls | 8,192 | 1.011–1.087 | 0.959–1.071 | 0.987–1.010 |
| Computed explicit/ordinal / stored-key grouping, four controls | 1,024 | 1.009–1.039 | 1.106–1.248 | 1.021–1.036 |
| Computed explicit/ordinal / stored-key grouping, four controls | 32,768 | 0.991–1.028 | 0.966–1.056 | 0.993–1.000 |
| Top-k / full-sort-then-slice | 1,024 | 0.7028 | 0.6548 | 0.6385 |
| Top-k / full-sort-then-slice | 32,768 | 0.3255 | 0.3258 | 0.5549 |

DISTINCT and its GROUP BY control share group-table machinery; this is not an
independent-algorithm comparison. The small computed-grouping p95 overhead is
observed and retained, without a statistical-significance claim from three runs.
Top-k median p50 is 3.481 vs 4.948 ms at 1,024 rows and 42.842 vs 131.605 ms at
32,768 rows. The composed-query concurrency control also passes: one/eight-worker
median p50 is 2.762/7.961 ms (1,024 rows) and 37.744/100.992 ms (32,768 rows).
These concurrency latencies include contention; they are not speedup ratios.

Raw reports, profiles and reproduction scripts are retained under
`/mnt/d/qlb-pr4-bench/`. `summary.txt`, `rpc-summary.json`, `kernel-summary.json`
and `comparisons-qlb_t15-qlb_t16-qlb_t17.json` hold the aggregate measurements.
The non-documentation source manifest (5,007 tracked and nonignored untracked
files) SHA-256 is
`41394d48bcd3eef361b289df16b76b0568ced32266b660a6e8f0b0229027c0c4`;
source and binary provenance are unchanged across all runs. The scenarios and
controls are documented in the [querybench guide](../../../../test/integration/distributed/querybench/README.md).

The untimed T17 overload prerequisite passes with 32 peak in-flight calls:
top-k accepts 94 and rejects 2 for admission; full sort accepts 93 and rejects 3.
All accepted results are exact, cleanup reaches zero, and same-cluster recovery
plus held-capacity rollback/recovery pass. Cgroup memory events remain zero.
Report: `/mnt/d/qlb-pr4-bench/overload/t17-overload-1024.json`.

## Standalone fixture allowance diagnosis

The initial standalone run passed 917/935 selected cases; 18 DISTINCT/sort cases
hit the previously configured 32 MiB per-query admission limit. A single
`t15_scalar_zone` reproduces without preceding tests. A temporary reservation
probe recorded used=33,536,457 bytes, next charge=25,680, assigned=33,554,432;
the shared 256 MiB pool held only that query's reservation. The rejection came
from the native scan's cumulative admission charge, not exhausted process RAM.

With the fixture's explicit server allowance changed from 1 GiB to 4 GiB, the
per-query limit is 128 MiB. The DISTINCT, bulk top-k and cancellation controls
pass 3/3; successful control reservations reach 66,160,761 bytes (about 63 MiB).
The Docker limit stays four CPUs/8 GiB/no extra swap. Production protection and
small-budget operator rejection tests are unchanged. This configuration change
is not a memory optimization or a revised performance-gate pass. All temporary
protector instrumentation was removed before final validation. Red/probe/control
logs are `/mnt/d/pr4-{distinct-red,budget-probe,budget-control}.log`.

## Pre-existing main test failure

The broader liaison race suite fails `TestExportRelay_TwoRelaysShareOneRate`.
The isolated test also fails on unchanged main `86659118` (one of three runs):
a relay emits a 98,304-byte piece after its peer finishes, while the test expects
every piece to remain at most 65,536 bytes. The limiter computes piece size from
the currently active relay count. Export code/tests are identical on main and
this branch; no export fix is included. Base evidence is
`/mnt/d/pr4-base-export-control.log`. Focused measure/QL/cache liaison race tests
pass. This record does not describe the broader liaison suite as passing.
