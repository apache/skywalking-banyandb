# T07 complete SQL aggregation over native batches

Historical ticket-stage record. Source revisions, measurements and limitations
below retain their original context. See the [PR 2 verification record](pr2-verification.md)
for the current isolated aggregation branch and refreshed measurements.

Implementation base: `e193815a` (T05 plus measure QL documentation), on PR #1412.
Verification uses Go 1.26.9. No protobuf, frame format, native operator, or stored
format changes. Final production source and generated-contract content hashes are
recorded in `.omx/t07-source-manifest.sha256`.

## Execution contract

Extended measure queries lower to one complete native raw scan, optional exact
time residual, local HashAggregate, final projection and pagination, then the
existing response serializer. There is one logical scan with one storage leaf
per data node. Native version/replica deduplication precedes local aggregation.
The native request carries no aggregation/grouping, zero offset and an unbounded
limit. The native source closes before synthesized groups are serialized.

Source shorthand, aliases, WHERE time, or multiple aggregate calls select this
execution path. Existing single-aggregate native syntax remains unchanged,
including native MEAN/AVG and COUNT(DISTINCT) routing/stage restrictions.
Extended COUNT(DISTINCT), TOP, TIME_BUCKET and ORDER BY are rejected explicitly.
Scalar tag/field grouping is supported, including hidden keys; expressions,
array grouping and group-only queries remain later tickets.

COUNT returns exact int64 and ignores nulls. Numeric SUM/MIN/MAX preserve int64
or float64; integer SUM overflow fails. AVG/MEAN use a float64 sum and exact
non-null count. Matching calls share lanes. Integer AVG keeps a floating sum
separate from an explicitly selected exact integer SUM, so AVG alone does not
inherit int64-sum overflow. Float SUM/COUNT/AVG together use the same two lanes
as AVG alone. Scalar empty input returns COUNT zero and other calls null;
grouped empty input returns no groups. Pagination applies to finalized groups.
LIMIT 0 skips storage; contradictory time bounds still synthesize the scalar
empty-input row when pagination allows it.

Grouping encodes type, explicit nullness, and variable-width lengths. Null,
empty string and zero differ; signed zeros share a key, and all NaNs share one
canonical key. Open-addressed uint32 group-ID slots retain full encoded keys to
resolve collisions. Dense state arrays scale with actual groups and geometric
capacity rather than input rows. The index and key headers reserve 24 bytes per
capacity slot; key payload reservations conservatively cover allocation classes.
Key bytes/capacity and used/allocated state bytes are reported separately.

Every growing array reserves the shared query allowance before allocation,
including overlap during growth. Native cells are borrowed only while consuming
a batch; retained keys own their bytes. Cancellation and construction, source,
growth, output or encoding errors close owners and return live reservations.
The implementation adds no new memory pool.

## Correctness evidence

The initial planner RED is `.omx/t07-planner-red.log`: an aliased multi-aggregate
query was rejected by typed raw execution. Operator tests independently check
fixture F, native passthrough and constant columns, null keys, signed zero/NaN,
selection, poisoned owners, exact COUNT above 2^53, checked sums, cancellation,
allocation failures and batches crossing 65,536 rows.

The generator preserves all 293 existing cases and 18 T05 reuse scenarios.
T07 adds 41 aggregate scenarios with three changed bindings each, executed both
cached and bypassed, plus seven invalid shapes. Expected values come from an
independent scalar fixture model. No native Query RPC supplies an oracle.
Readiness verifies every final version through bounded raw QL windows. The
513-group fixture has 1,026 logical rows, two entity hosts, distinct non-entity
group keys and stale/final writes; this isolates SQL groups from per-series
native allocation costs. Its unordered output is validated with unique keys
and sorted on both test sides before the existing typed/null/tolerance comparator.
This comparison does not assert an ORDER BY contract.

## Focused benchmark protocol

P(T07) has four distributed RPC variants only: scalar SUM, four aggregates over
1,024 nullable groups, one-group AVG, and SUM/COUNT/AVG over N integer groups.
Final LIMIT 1 bounds N-group egress; instrumentation must still prove N input
rows and N groups. Independent scalar checks validate values and nulls before
timing and on retained samples. Setup and visibility are outside timing.

The original successful-RPC protocol targeted 1,024, 100,000 and 1,000,000
input rows at 100/5, 20/3 and 10/2 timed/warmup calls. The first run passed
1,024 rows, then failed before local aggregation at 100,000 rows. The native
limit and the revised focused measurement protocol are recorded below. This
ticket-specific schedule replaces the shared old 1,000-call boilerplate. Ten
samples cannot establish a stable tail distribution;
all samples and profiles are retained. There is no old-version latency baseline
for previously unsupported SQL, and old ticket matrices are not repeated.

K(T07) compares the complete dense operator with an independent scalar map
reference over 65,537 rows, 1/1,024/N groups, nullable input and four aggregate
shapes, including AVG versus shared SUM/COUNT/AVG. It validates results before
timing. Resource probes use 16/256-MiB tracker settings over 100,000 and 1,000,000 groups and
permit only complete correct results or ResourceExhausted, followed by clean
recovery and no owner/reservation leaks. Protector admission remains active and
can be tighter than the configured vectorized tracker.

## Final test runs

Both final resource-limited topology runs passed all 215 selected specs,
including all 48 T07 specs and the 167 earlier ticket specs. Reports:
`.omx/t07-standalone-docker.json` and `.omx/t07-distributed-docker.json`.
The old 293 generated cases, 18 reuse scenarios, reuse readiness and pair report
also compare unchanged to the implementation base as decoded JSON values.

Host standalone verification passed too. Host distributed verification passed
214/215, with only the complete 513-group response rejected by admission. This
host reports an invalid/unbounded cgroup limit, so its fallback query allowance
is only 8 MiB (64-MiB pool divided by eight). The same full test succeeds in the
8-GiB container with normal cgroup-based admission. No production allowance was
changed, and the required input/group cardinalities were retained. Earlier
readiness failures led to bounded raw visibility windows; a slow quadratic
unordered protobuf comparison led to unique-key sorting in the test adapter.

Focused packages, generator, querybench and the full gRPC suite passed race
checks with `querymetrics`. `make lint` passed after fixing new lint findings.
The final readiness-helper functional run passed in 6.018 seconds, its focused
querybench race run passed in 146.562 seconds, and tagged lint reported zero issues.
Relevant logs are `.omx/t07-aggregate-race.log`, `.omx/t07-planner-race.log`,
`.omx/t07-grpc-race.log`, `.omx/t07-generator-race.log` and
`.omx/t07-make-lint-complete.log`. Deterministic generator checks and whitespace checks
passed. This ticket does not alter the nine known pre-existing generated failures.

A final topology command, run in the Go benchmark image with the workspace and
module/build caches mounted, is:

```sh
go test -tags=querymetrics ./test/integration/distributed/query \
  -count=1 -timeout=30m -run '^TestPropertyQuery$' \
  -ginkgo.label-filter='measure-ql && (QLB-T01 || QLB-T02 || QLB-T03 || QLB-T05 || QLB-T07)' \
  -ginkgo.fail-on-empty
```

Use `./test/integration/standalone/query` and
`^TestPropertyIntegrationQuery$` for the other topology. Containers use
`--cpus 4 --memory 8g --memory-swap 8g --pids-limit 4096`.

## Kernel measurements

The limited-container kernel run uses the command below. All 28 variants have
five nonempty samples and pass independent value/null checks. The table reports
medians over five samples; the first 12 shapes use 65,537 rows and the last two
use 1,000,000 rows.

```sh
go test -tags=querymetrics ./pkg/query/ql -run '^$' \
  -bench '^BenchmarkAggregate(Kernel|MillionGroups)$' \
  -benchmem -benchtime=1s -count=5 -timeout=20m
```

| Groups | Shape | Dense ms | Scalar ms | Dense allocated B/op | Scalar allocated B/op |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | sum | 6.807 | 1.082 | 2,312 | 352 |
| 1 | multiple | 6.889 | 1.098 | 3,248 | 400 |
| 1 | avg | 6.868 | 1.082 | 2,312 | 352 |
| 1 | sum-count-avg | 6.825 | 1.085 | 2,872 | 384 |
| 1,024 | sum | 7.731 | 2.086 | 121,368 | 289,016 |
| 1,024 | multiple | 8.037 | 2.204 | 179,592 | 338,168 |
| 1,024 | avg | 7.764 | 2.093 | 121,368 | 289,016 |
| 1,024 | sum-count-avg | 7.855 | 2.105 | 138,296 | 321,784 |
| 65,537 | sum | 27.658 | 27.258 | 11,557,683 | 18,883,872 |
| 65,537 | multiple | 38.652 | 27.753 | 15,777,938 | 22,029,649 |
| 65,537 | avg | 28.773 | 28.256 | 11,557,675 | 18,883,887 |
| 65,537 | sum-count-avg | 32.831 | 28.015 | 11,574,840 | 20,981,065 |
| 1,000,000 | avg | 538.491 | 582.169 | 99,909,512 | 301,065,352 |
| 1,000,000 | sum-count-avg | 583.593 | 636.658 | 99,926,680 | 333,065,352 |

The scalar reference is a specialized Go map over int keys. The dense path runs
the public operator lifecycle, general typed key codec, memory admission and
columnar output. It is slower at low group counts; these results do not claim a
CPU speedup. At high cardinality its allocation volume is lower. AVG and shared
SUM/COUNT/AVG both use 16 state bytes per group; four aggregates use 32 bytes.
At 65,537 groups, geometric capacity is 131,072, so two-lane allocated state is
2,097,152 bytes versus 1,048,592 used bytes. Key capacity is separately
4,194,384 bytes. This distinguishes retained state from total allocated bytes.
The final harness collects validation-only garbage before starting the dense
timer, excluding reclamation of independent oracle maps. The initial 24-variant
run is retained as `.omx/t07-kernel-docker.log`; final artifacts are
`.omx/t07-kernel-final.log` and `.omx/t07-kernel-summary-final.json`.

At one million groups both AVG shapes use 16,000,000 state bytes and
16,777,216 capacity bytes, with 41,165,888 bytes of key capacity. Their local
operator-owned state fits a 128-MiB tracker, excluding caller-owned fixture inputs
and the untimed scalar oracle. All million expected keys, values and nulls are
checked exactly once before timing; post-Close reservations are zero.

## Successful distributed RPC measurements

All five repetitions passed their eight variants in fresh 4-CPU, 8-GiB
containers. Each cardinality starts a fresh cluster process. The 1,024-row
variants use five warmups and 100 timed requests; the 32,768-row variants use
three warmups and 20 timed requests. There is one RPC worker. Warmups and full
scalar comparisons are untimed. CPU profiling is active during measured requests;
the heap profile and its collection run afterward.

The table reports the median of each repetition's metric. MB uses decimal
bytes. Allocation and CPU measurements cover the whole benchmark process,
including RPC, native execution and background work; they are not local
aggregate-only costs. These small samples describe the observed distributions
and do not establish a stable tail-latency guarantee.

| Input rows | Shape | p50 ms | p95 ms | Allocated MB/query | Process CPU ms/query |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,024 | AVG, one group | 3.816 | 5.359 | 1.430 | 9.7 |
| 1,024 | COUNT/SUM/MIN/MAX, 1,024 groups | 10.761 | 16.976 | 4.769 | 21.5 |
| 1,024 | SUM/COUNT/AVG, N groups | 4.135 | 6.034 | 1.595 | 9.8 |
| 1,024 | Scalar SUM | 3.498 | 5.274 | 1.325 | 8.6 |
| 32,768 | AVG, one group | 46.993 | 52.975 | 43.551 | 92.5 |
| 32,768 | COUNT/SUM/MIN/MAX, 1,024 groups | 58.940 | 69.388 | 54.913 | 122.0 |
| 32,768 | SUM/COUNT/AVG, N groups | 52.572 | 59.309 | 48.699 | 101.5 |
| 32,768 | Scalar SUM | 40.620 | 44.101 | 38.014 | 81.5 |

Every timed interval records exact post-deduplication input/group counts, one
local aggregate execution per request, two native leaf calls per request (one
per data node), no intermediate DataPoints, and zero post-Close owner, pool,
result or reservation deltas. SUM/COUNT/AVG retains the same two state lanes as
AVG. Plan compilation is zero during timing and every request hits the compiled
cache. Full samples, CPU/heap profiles and metrics are in
`.omx/bench-reports/t07-bounded-r1` through `t07-bounded-r5`; the validated
cross-run summary is `.omx/t07-rpc-summary.json`.

After these measurements, only untimed readiness/recovery helpers changed to
prune hosts and project one field explicitly. Production execution, measured
queries, fixtures and timing protocol stayed the same; the P5 binaries and
profiles are retained. The resource probes below use the rebuilt helper.

The report explicitly marks physical storage rows, native retained frame bytes
and total parent-lease charges unavailable. Its process-wide peak reservation
must not be interpreted as native residency alone. Prior-ticket latency matrices
were not rerun; compatibility evidence here is the earlier 167 E2E specs.

## Inherited native execution bound

The original large-RPC performance gate is **not met**. The native execution
boundary eagerly retains replica frames and reserves a conservative estimate
before constructing the QL operator. In an 8-GiB container, the memory protector
allows 75% (6 GiB), divides its query pool by four and its per-query allowance by
eight, leaving 192 MiB for a query. This is independent of a larger vectorized
tracker flag.

A reproducible one-query diagnostic first verified all 100,000 final fixture
rows through bounded raw QL windows, then executed the exact four-aggregate RPC.
It failed in approximately 100 ms with zero local aggregate executions, input
rows or state reservations. The first native frame requested 157,505,864 bytes
while the lease had already charged 51,609,596 of its 201,326,592-byte allowance.
The second replica frame would require another similar reservation. The failure
is at `reserveNativeFrames` in `pkg/query/vectorized/measure/plan/distributed.go`;
its estimate charges 128 bytes per column cell plus 128 bytes per row, 13
additional encoded-payload copies and shared workspace. Live reservations return
to zero after the rejection. The first original run is retained at
`.omx/bench-reports/t07-r1` and `.omx/t07-rpc-r1.log`; exact diagnostics are
`.omx/t07-charge-diag.log` and `.omx/t07-charge-diag-2.log`.

Changing the QL aggregate cannot remove this pre-execution reservation. Native
operators, frame accounting and protector policy are outside this QL-only ticket.
The successful RPC measurements therefore use 1,024 and 32,768 input rows with
the same four shapes and five fresh 4-CPU/8-GiB repetitions. Larger real RPCs at
100,000 and 1,000,000 rows remain explicit resource outcomes, not successful SQL
throughput results. A separate local million-group kernel run checks actual
large aggregate execution without replacing the missing successful native RPC
experiment. The original large-RPC requirement remains visible rather than being
marked passed by smaller queries or a relaxed memory policy.

## Resource outcomes and remaining gates

Both 256-MiB distributed resource probes verified their complete fixtures before
issuing five N-group SUM/COUNT/AVG requests. Every request returned
ResourceExhausted before local aggregation, without a partial response. All live
owner, pool, result and reservation checks returned to baseline, and a bounded
raw QL recovery query passed afterward. These are successful rejection/recovery
checks, not successful large-query performance measurements.

| Input rows/groups | Tracker setting | Aggregate outcomes | Process peak reservations | Recovery |
| ---: | ---: | --- | --- | --- |
| 100,000 | 256 MiB | 5 ResourceExhausted; 0 aggregate executions | 558,215,922–577,992,738 B | Passed |
| 1,000,000 | 256 MiB | 5 ResourceExhausted; 0 aggregate executions | 523,869,908–591,298,391 B | Passed |

The process peak sums three independent node trackers and stays below their
combined 768-MiB limit. At 100,000 rows, the native data-node raw-frame emitter
rejects 258,911,011 used bytes plus 26,359,256 requested bytes against a
268,435,456-byte tracker. At one million rows, native pipeline advancement
rejects 264,746,587 plus 21,880,784 bytes against the same tracker. These
data-node failures differ from the earlier liaison protector failure with a
1-GiB tracker setting; neither reaches local aggregate state allocation.

Artifacts are `.omx/bench-reports/t07-res-final-100000-256` and
`.omx/bench-reports/t07-res-final-1000000-256`, with logs
`.omx/t07-res-final-100000-256.log` and
`.omx/t07-res-final-1000000-256.log`. The validated summary is
`.omx/t07-resource-summary.json`.

The 16-MiB real-RPC resource gate remains **blocked during native readiness**.
The final setup prunes one of sixteen hosts, explicitly projects RID plus one
INT field, and checks 1,024 rows per time window against an independent oracle.
Even its first window needs 13,829,731 used bytes plus 3,299,800 requested bytes
against a 16,777,216-byte native tracker. It fails before any owning aggregate
probe or recovery query, so there is no passing resource verdict or result
artifact for that configuration. Evidence is
`.omx/t07-res-fixed2-100000-16.log`; prior readiness attempts are retained too.
The million-row/16-MiB configuration was not repeated after this setup blocker.
Functional tests verify host/window coverage, request binding and fail-fast
handling independently; they do not replace the blocked real-RPC check.

The local operator's separate 262,145-group tests exercise 16-MiB rejection,
256-MiB success and complete release. The million-group kernel succeeds within
128 MiB of operator-owned live reservations. Those tests establish local
allocation behavior without implying that inherited native execution fits the
same allowance. Thus correctness is delivered, while the original successful
large-RPC gate and the 16-MiB real-RPC gate remain explicitly outstanding.

The subsequent scope decision defers disk spilling to a later ticket, including
its generated E2E tests and focused benchmarks. T07 retains existing memory
protection and ResourceExhausted behavior. The measurements and limitations above
remain unchanged; no spilling implementation is pending within T07.
