# Catalog B PR 2: SQL aggregation and native split/merge

This branch is based on merged PR #1417 at `e05ed7f22`. It extracts T07 and T08
from the combined Catalog B implementation. Expressions, field WHERE, HAVING,
SELECT DISTINCT, computed grouping, selectable time buckets and general sorting
remain in PRs 3 and 4. Legacy scalar COUNT(DISTINCT) keeps the merged PR 1 behavior;
extended exact count-distinct composition is not part of this PR.

## Execution and compatibility

```text
parameterized QL -> cached schema-resolved metadata -> fresh binding
  -> authorization/admission/revision/lifecycle gates
  -> eligible: sequential existing native aggregates -> keyed batch merge
     otherwise: complete deduplicated native scan -> local hash aggregation
  -> output aliases -> final LIMIT/OFFSET -> existing measure_result
```

Extended queries support named scalar grouping and multiple COUNT/SUM/MIN/MAX/
AVG/MEAN calls. Null keys differ from empty strings and zero. Counts are exact
int64; integer SUM overflow fails. AVG uses sum and non-null count rather than
the legacy native MEAN behavior. Repeated calls share work. Scalar empty input
returns zero COUNT and null other aggregates; grouped empty input returns no rows.

Native eligibility is conservative: one measure group, a provable stage, one
exact time interval and complete non-null entity identity. Unsafe integer sums,
floating extrema, non-entity keys, residual time predicates and uncertain stage
selection retain the complete local path. Native branches run sequentially and
share the parent allowance; each source closes before the next opens. Partial
failures do not return partial SQL groups. Local key/state growth reserves memory
before allocation, and cleanup releases owners and reservations.

No protobuf definitions, RPCs, native operator implementations, persisted formats
or native frame versions change. Memory protection remains active. Disk spilling
and retained-result pagination remain deferred.

The generated QL-only contracts derive expected values from independent fixture
arithmetic. Existing raw-query cases remain separate compatibility controls.
T07 covers local grouping, null/empty semantics, shared calls and pagination;
T08 compares cached, bypassed and forced-local execution, including branch reuse,
lifecycle changes, cancellation, authorization/revision failures and recovery.

The extraction retains the main-branch compatibility fixes and the CI regression
fixes from PR 1. Shared benchmark fixture dispatch now lives in the test entrypoint,
so ordinary Go package compilation and vulnerability analysis do not depend on
helpers present only in `_test.go` files.

## Isolated correctness checks (2026-10-21)

- Planner, QL operators, liaison, fixture generator and measure-data unit suites pass.
- The planner/operator/liaison/generator suites also pass with `-race -tags=querymetrics`.
- The unchanged deterministic generation check passes, preserving PR 1's scalar
  count pagination and empty-entity regression cases alongside the new scenarios.
- Both independent measure/QL selections pass **530/530** cases in four-CPU,
  8-GiB Docker containers: standalone in 73.315 s and distributed in 92.942 s.
  These include 79 new T07/T08 specs and the 451 inherited compatibility specs.
- Full `make lint` passes. All 78 relative documentation links/anchors resolve.

Reports are `/mnt/d/qlb-pr2-standalone-report.json` and
`/mnt/d/qlb-pr2-distributed-report.json`. Unit, race, generator and lint logs use
the `/mnt/d/qlb-pr2-` prefix. Other integration blocks were not selected.

## Inherited toolchain blocker

On 2026-10-21, `make vuln-check` fails for the inherited Go 1.26.9 standard
library: [GO-2026-6809](https://pkg.go.dev/vuln/GO-2026-6809), affecting `crypto/x509`,
is fixed in Go 1.26.10. The pre-aggregation PR 1 tree reports the same reachable
vulnerability with the same toolchain. This branch changes neither `go.mod` nor
dependencies, and no aggregation compilation error remains.

Logs are `/mnt/d/qlb-pr2-vuln.log` and
`/mnt/d/qlb-pr2-main-vuln-control.log`. The scan is not counted as passed. A separate
toolchain patch is needed before CI can become green; aggregation benchmarks
retain the existing Go 1.26.9 protocol and do not claim patched-toolchain results.

## Evidence boundaries

[T07 verification](t07-verification.md) and [T08 verification](t08-verification.md)
preserve historical measurements at their original revisions. The current branch
includes #1413's native aggregation optimization and PR 1's ownership/accounting
repairs; historical figures are not treated as measurements of this isolated head.

Fresh measurements use only the existing T07/T08 workloads, in four-CPU, 8-GiB
Docker containers with no extra swap and Go 1.26.9. T07 compares dense local
aggregation with its independent scalar reference. T08 compares the same SQL
through native and forced-local execution, with existing individual-native RPC
bundles as additional controls. Kernel and RPC observations remain separate;
neither eligibility nor a specialized scalar comparison guarantees a speedup.


## Fresh kernel measurements (2026-10-21)

All 40 existing variants completed five one-second samples each (200 observations),
with independent value/null checks and no cgroup OOM or memory-max events. The
following values are medians of five samples; the complete matrix is retained in
`/mnt/d/qlb-pr2-bench/kernel-summary.json`.

T07 runs 65,537 input rows in the first three rows below; the million-group rows
use one million inputs. The scalar reference specializes Go map keys and omits
the public operator lifecycle, typed key codec and tracked allocation checks.

| Groups | Shape | Dense ms | Scalar ms | Dense B/op | Scalar B/op |
|---:|---|---:|---:|---:|---:|
| 1 | sum-count-avg | 6.925 | 1.128 | 2,872 | 384 |
| 1,024 | sum-count-avg | 7.945 | 2.243 | 138,296 | 321,784 |
| 65,537 | sum-count-avg | 42.125 | 31.060 | 11,574,845 | 20,981,057 |
| 1,000,000 | avg | 545.762 | 629.366 | 99,909,512 | 300,868,672 |
| 1,000,000 | sum-count-avg | 618.048 | 669.411 | 99,926,680 | 333,016,168 |

The general operator is slower at low group counts; high-cardinality allocation
volume is lower. Both million-group shapes use two state lanes: 16,000,000 used
state bytes, 16,777,216 state-capacity bytes and 41,165,888 key-capacity bytes.
They fit the 128-MiB operator tracker, excluding caller-owned fixtures and the
untimed scalar oracle. This is not a million-row RPC acceptance result.

T08 compares the complete native-group merge with its specialized scalar map.
The table shows aligned branch rows; the permuted variants also pass and have
similar measured cost. Both orders visit 2N input groups and 3N merge entries.

| Finalized groups | Batch merge | Scalar reference | Batch B/op |
|---:|---:|---:|---:|
| 1 | 6.147 µs | 0.130 µs | 4,096 |
| 1,024 | 725.915 µs | 248.269 µs | 160,472 |
| 65,537 | 59.607 ms | 27.840 ms | 12,632,837 |

This kernel comparison establishes no CPU speedup. Native pushdown can reduce
RPC raw-row transport despite the more expensive general merge; that question
is measured separately with the same SQL in native and forced-local modes.


## Fresh local-aggregation RPC measurements

T07 completed five repetitions at each cardinality: 40 variant measurements and
2,400 timed queries. Each cardinality runs in a fresh process, with one RPC
worker, 100 timed/5 warmup queries at 1,024 rows and 20 timed/3 warmup queries at
32,768 rows. Setup, visibility and full scalar checks are untimed. All values,
structural counters and post-close ownership/reservation checks pass.

The table reports medians of the five repetition summaries. Allocated MB uses
decimal bytes. CPU and allocated bytes cover the whole benchmark process,
including client, liaison, data nodes and background work. Twenty large-query
samples per repetition do not establish a precise tail-latency guarantee.

| Input rows | Shape | p50/p95 ms | Allocated MB/query | Process CPU ms/query |
|---:|---|---:|---:|---:|
| 1,024 | AVG, one group | 3.967 / 5.966 | 1.253 | 10.0 |
| 1,024 | COUNT/SUM/MIN/MAX, 1,024 groups | 10.980 / 16.616 | 4.399 | 22.4 |
| 1,024 | SUM/COUNT/AVG, N groups | 4.204 / 6.140 | 1.418 | 10.0 |
| 1,024 | Scalar SUM | 3.664 / 5.545 | 1.148 | 9.3 |
| 32,768 | AVG, one group | 40.647 / 44.270 | 30.395 | 79.5 |
| 32,768 | COUNT/SUM/MIN/MAX, 1,024 groups | 51.895 / 59.484 | 38.333 | 106.5 |
| 32,768 | SUM/COUNT/AVG, N groups | 46.745 / 53.571 | 34.350 | 86.0 |
| 32,768 | Scalar SUM | 37.153 / 39.518 | 27.066 | 75.0 |

This characterizes newly supported SQL without claiming a latency improvement
over main, which cannot execute those extended aggregate shapes. The full
repetition summary is `/mnt/d/qlb-pr2-bench/t07-rpc-summary.json`.


## Fresh native-versus-local RPC comparison

T08 completed five repetitions at each cardinality: 110 variant measurements and
6,600 timed logical iterations. It uses the same schedules as T07 above. The
individual-native controls time complete two/three-request bundles; their latency
and CPU are not costs of a single native request.

| Raw rows | Shape | Pushdown p50 ms | Local p50 ms | p50 ratio | Allocation ratio | CPU ratio |
|---:|---|---:|---:|---:|---:|---:|
| 1,024 | Float AVG | 4.803 | 4.107 | 1.169 | 1.006 | 1.327 |
| 1,024 | Shared float SUM/COUNT/AVG | 5.011 | 3.999 | 1.253 | 0.969 | 1.474 |
| 1,024 | Integer MIN/MAX/COUNT | 7.284 | 4.255 | 1.712 | 1.453 | 2.061 |
| 1,024 | Integer AVG fallback control | 4.333 | 4.122 | 1.051 | 1.013 | 1.031 |
| 32,768 | Float AVG | 23.927 | 43.968 | 0.544 | 0.327 | 0.822 |
| 32,768 | Shared float SUM/COUNT/AVG | 24.643 | 45.082 | 0.547 | 0.326 | 0.764 |
| 32,768 | Integer MIN/MAX/COUNT | 38.779 | 43.472 | 0.892 | 0.509 | 1.229 |
| 32,768 | Integer AVG fallback control | 43.307 | 41.443 | 1.045 | 1.013 | 1.060 |

Ratios divide the five-run pushdown median by the corresponding local median.
At 32,768 rows, float AVG/shared calls reduce observed p50 by about 45% and
allocated bytes by about 67%. Integer extrema improve p50 by about 11% and
allocation by about 49%, but increase process CPU by about 23%. All three native
shapes are slower at 1,024 rows; the integer case also allocates more and uses
more CPU. Eligibility is not a cost model or a universal speedup guarantee.

For the large float AVG/shared/integer-extrema cases, the five paired p50 ratios
range from 0.442–0.594, 0.462–0.673 and 0.833–0.946 respectively. The fallback
control executes the same local path in both modes and retains run variation;
its differences are not an optimization effect. These are observations with
small large-query sample counts, not a newly inferred statistical gate pass.

## Combined benchmark provenance and reproduction

The two RPC protocols completed 9,000 timed logical iterations across 150 variant
measurements and 20 fresh cardinality processes, with 300 nonempty CPU/heap
profiles. This comprises 7,200 QL calls and 4,200 native Query calls (11,400 actual
timed transport calls), because individual-native controls issue bundles. All
independent result, structural-counter and post-close cleanup checks pass.

Nine captured RPC-container memory-event records and the kernel-container record
have zero OOM/max events. T07 repetition 1's event record was not captured and is
unavailable, rather than counted as zero. Two untimed report merges were recovered
from retained shards after correcting the external wrapper; measured queries
were not rerun. The retained immutable wrapper uses `env -u DQB_MODE` for merge
mode and captures later memory-event records on exit.

Timing froze source at `199d95522`. The source check confirms 2,405 tracked and
generated source/contract files match the manifests, including 1,141 production
files; both measured executable hashes match. Documentation updates after timing
do not change those sources. All artifacts are under `/mnt/d/qlb-pr2-bench/`,
including `rpc-summary.json`, `t08-comparison-summary.json`, `kernel-summary.json`,
latency samples, profiles, `binary-sha256.txt`, source manifests and exact runners.

Reproduce the kernels with `BenchmarkAggregate(Kernel|MillionGroups)` and
`BenchmarkNativeAggregateMerge` in `pkg/query/ql`, with `-tags=querymetrics`,
`-benchmem -benchtime=1s -count=5 -run '^$'`. The querybench `qlb_t07` and `qlb_t08`
scenarios retain the protocols in their ticket records: cardinalities 1,024 and
32,768, one worker, the timed/warmup schedules above, and profiling enabled.
The existing Docker runner and its README describe the required environment.
This refresh adds no earlier-ticket matrix or new workload.

Process-wide peak reservations are not isolated native residency or a per-query
lease size. Physical storage reads, native retained-frame bytes and total parent
lease charges remain unavailable. No large-RPC success or historical T02
acceptance is inferred from the operator-only million-group measurements.
