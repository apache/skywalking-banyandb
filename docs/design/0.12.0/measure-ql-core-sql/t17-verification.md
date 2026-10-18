# T17 typed ordering and compact top-k

Implemented on `8ee2a074` (T16), on PR #1412. T17 adds multi-key measure ORDER BY,
budgeted full sorting and a finite-LIMIT heap using the same typed comparator.
Committed as `b13cdc6d`. The measurements below describe the T17 verification
revision. The subsequent [legacy integration repair](legacy-integration-verification.md)
resolves its nine legacy-suite failures and passes 934/934 measure cases in each
topology. The revised [concurrent admission contract](implementation-tickets.md#concurrent-admission-acceptance-revised-2026-10-18)
separates expected overload rejection from timed-query acceptance. Successful 100k/1m throughput is now explicitly deferred by the
[2026-10-19 acceptance revision](implementation-tickets.md#large-input-acceptance-revised-2026-10-19);
the [revised harness verification](t17-acceptance-verification.md)
records the passing overload/recovery and timing-protocol checks;
the historical measurements below are not rerun or reclassified by that repair.

## Execution contract

The planner resolves source columns, visible aliases, ordinal references and
completed aggregate/group outputs once. Each execution binds its own parameters,
output types and mutable operator state. Computed timestamp aliases resolve
`::TAG` against the bound output container, including a timestamp and field
sharing an alias. Alias names never enter a per-row lookup. Simple native
timestamp and explicit index-rule ordering retain their existing paths.

```text
native column batches
  -> WHERE
  -> grouping / aggregation
  -> HAVING and completed projections
  -> visible-tuple DISTINCT
  -> full sort or compact top-k
  -> OFFSET / LIMIT and visible output
  -> existing measure response
```

Optional stages are omitted when absent. Sorting sees every completed row after
the preceding stages. Hidden WHERE inputs remain in the native scan projection;
hidden ordering columns and aggregate calls survive until sorting and are then
excluded from the response. DISTINCT permits only keys derivable from its visible
tuple. LIMIT 0 performs no native scan.

Each key has independent ASC/DESC and NULLS FIRST/LAST. Defaults are ASC NULLS LAST
and DESC NULLS FIRST. Integers compare exactly, strings and bytes compare byte by
byte, and floating NaNs follow other non-null numerics in ascending order.
Descending reverses non-null ordering without reversing NULL placement. Complete
ties have unspecified relative order; test goldens include a unique tie key.

Unlimited results use cancellable heapsort with bounded comparator work. Finite
LIMIT uses a max-heap of the best `k = uint64(OFFSET) + uint64(LIMIT)` rows, then
sorts those candidates before slicing. The maximum supported sum is 8,589,934,590;
it does not wrap uint32. Large offsets can approach full-sort retention.

Accepted candidates copy all reachable variable values, including strings,
binary data, arrays and boxed protobuf values. Eviction reuses candidate cell
slices. No candidate retains a source-batch owner. The shared tracker reserves
key buffers, candidate descriptors/payload and output before allocation; candidate
payload capacity is bounded by twice the live payload plus declared descriptor
capacity. Boxed integer-array accounting includes native slice storage, because
packed wire size can be much smaller than the cloned Go allocation. Full sort
and top-k fail on budget exhaustion, with no spill or unbounded fallback.
Close, cancellation and source errors release reservations and owners.

## Generated endpoint verification

The independent finite model generates 32 sorting families with three bindings,
12 no-scan rejections and four resource specs: 48 T17 specs per topology.
Cold/warm and bypass calls, forced full-sort parity, compact-budget controls and
recovery queries produce 259 verification RPCs per topology. Readiness uses
12 QL-only pages; actual retry-inclusive counts are logged separately. No new
case uses a native query for readiness or expected output.

Coverage includes F ordering and pagination, defaults and explicit NULL placement,
hidden keys, alias precedence, exact int64s above 2^53, strings, NaNs, grouped and
hidden aggregate keys, HAVING, bucket outputs, computed/ordinal grouping with AVG,
visible DISTINCT, changed expression/time/page bindings, LIMIT 0, wide offsets,
multi-batch input, cancellation, real candidate-budget errors and cleanup.
Four cross-stage families use a hidden WHERE-only field in raw, computed,
grouped and DISTINCT queries. The computed family also orders a qualified
timestamp alias.

All 1,569 earlier generated cases and their expected responses are retained.
Only the two obsolete T03 sorting-rejection queries change to genuinely absent,
qualified keys; their original supported forms now have positive T17 witnesses.
The complete artifact contains 1,695 cases. Its SHA256 is
`3c2de0c2474d059b86402101535ebce27af5ed5418a6a9500aec443e2ddb84fe`.
The audited grammar fingerprint includes the new ORDER BY parser.

Focused four-CPU, 8-GiB Docker runs with no additional swap pass all 48 specs in
both topologies. Each reports exactly 259 verification RPCs. Standalone readiness
uses 12 calls; distributed uses 22 calls for the same 12 pages.
Evidence is `.omx/t17-generated-focused-{standalone,distributed}-v4.{log,json}`
and `.omx/t17-generated-focused-v4-summary.json`.

Public tests cover Prepare/Compile/Bind, WrapRaw/WrapSort and final response
egress, with independent expected values. RED/GREEN regressions retain evidence
for hidden WHERE projection, qualified timestamp aliases, invalid ordinals and
under-accounted boxed arrays. Normal/tagged runtime and planner suites, focused
race/ownership checks, liaison tests, the focused benchmark harness and
`make lint` pass. Logs are `.omx/t17-*-final.log`,
`.omx/t17-planner-reviewed-*.log` and `.omx/t17-runtime-*.log`.

The complete measure selection includes every `measure-ql` case, without a
ticket filter, plus existing native measure and Top-N compatibility cases:

| Topology | Ticketed QL | Legacy QL | Native measure | Top-N | Total passing |
|---|---:|---:|---:|---:|---:|
| Standalone | 637/637 | 142/151 | 120/120 | 20/20 | 919/928 |
| Distributed | 637/637 | 142/151 | 120/120 | 20/20 | 919/928 |

At the T17 verification revision, both full suites were red on the same nine
documented legacy regressions (resolved by the follow-up linked above).
Eight indexed-array cases fail in the native variable-array decoder:
`generated_projection_0`, `generated_having_labels`,
`generated_not_having_labels` and `generated_pair_002/004/008/013/017`.
`generated_in_int_list_0` reports that a series is required for empty entity IN.
Their expectations and active status were unchanged; that run did not count them
as passed tests or sorting regressions. Historical full reports are
`.omx/t17-generated-full-{standalone,distributed}-v2.json`, with category analyses
and `.omx/t17-generated-full-v2-summary.json`.

Other existing compatibility cases also pass:

| Topology | Stream | Trace | Property |
|---|---:|---:|---:|
| Standalone | 97/97 | 68/68 | 24/24 |
| Distributed | 97/97 | 68/68 | 12/12 |

The standalone property selection includes 12 default and 12 on-disk cases.
Reports are `.omx/t17-other-compat-{standalone,distributed}.json` and
`.omx/t17-generated-property-{standalone,distributed}.json`. The complete
functional ledger is `.omx/t17-generated-verification-summary.json`.

## Focused benchmark protocol

The kernel matrix has nine orthogonal shapes, avoiding a Cartesian expansion:
1/3/8 keys, deterministic random/ordered/reverse/tied input, narrow/wide payloads,
k=10/100/1,000 and a large offset. Random source order uses an independent seeded
permutation, including the one-key control. Standard-library tuple sorting is an
independent oracle; full sort and top-k copy and return the same page.
Actual key roots, row applications, kernel work, comparisons, retained candidates,
descriptor/payload bounds and cleanup are checked before timing. A separate
32-batch probe poisons 1-MiB source buffers after copying one narrow candidate
from each batch and checks owner and pool references.

The RPC comparison uses the same three-key SQL and ten-row page at 1,024 and
32,768 inputs. A querymetrics-only context forces reference full sorting through
the existing liaison test hook; the default path uses top-k. The only additional
workload is the planned composed filter/group/HAVING/order/page query with three
changing bindings and 1/8/32 callers. No unrelated cache or older-ticket matrix
is added. Warm-ups and retained timed samples check exact results; every timed
call checks row count and contributes actual work and cleanup counters.
Cache counters report observed reuse; they do not claim measured
uncached planning savings.

These successful RPC sizes were fixed after admission, before final timing.
The kernel supplies the 100,000-row algorithm comparison. Successful 100,000-
and 1,000,000-row RPC throughput is not established: earlier tickets already
record the unchanged native input-materialization ceiling, and T17 does not
repeat that failing large-fixture approach or change the ceiling. See
[T16's resource evidence](t16-verification.md). The PR #1412 legacy performance
matrix is also not rerun; existing native golden tests establish correctness,
not a new G1 latency verdict. These limits prevent closing the full planned
performance acceptance from the focused sort measurements alone.

Final timing uses five fresh resource-limited Docker repetitions and prebuilt
binaries. The kernel uses a normal build; RPC work counters require querymetrics.
Source, binary, generated-artifact and retained-failure digests guard the run.
The repeatable driver is
`test/integration/distributed/querybench/run-t17-frozen.sh`; raw samples and
CPU/heap profiles remain under `.omx/bench-reports/qlb-t17-*-final`.

## RPC measurements

All five repetitions validate the four admitted variants at both sizes:
3,120 successful timed RPCs, including 240 retained samples checked against the
independent exact oracle. Every sort pair returns the same ten-row page.
The following are medians of the five per-run measurements. CPU and allocations
include the client, liaison and two data nodes; they do not isolate sort CPU.

| Input rows | Mode | p50 ms | p95 ms | p99 ms | CPU ms/RPC | Allocated KiB/RPC |
|---|---|---:|---:|---:|---:|---:|
| 1,024 | Full sort + slice | 5.602 | 8.544 | 9.659 | 12.7 | 2,505.17 |
| 1,024 | Top-k | 4.037 | 6.268 | 7.908 | 10.6 | 1,645.05 |
| 32,768 | Full sort + slice | 143.733 | 177.362 | 185.521 | 239.5 | 71,082.98 |
| 32,768 | Top-k | 45.120 | 55.160 | 58.330 | 102.0 | 38,598.39 |

The ratios of median top-k/full-sort p50 are 0.721 and 0.314; median paired
ratios are 0.753 and 0.310. These are observations, separate from the kernel's
0.80 algorithm gate. Comparator calls are exactly 17,303 versus 1,403 at 1,024
rows and 881,572 versus 33,376 at 32,768 rows across the five repetitions.
Keys evaluate once per active row, with two native leaves per RPC, no
intermediate DataPoints and identical completed output. Source/frame counters
include both replicas and report twice the logical sort input.

Tracked per-query sort peak reservations fall from 1,361,088 to 61,496 bytes at
1,024 rows and from 42,247,360 to 61,496 bytes at 32,768 rows. Candidate counts
fall from N to 15. These peaks include conservative descriptor/payload bounds
and key/output buffers; they are not measured RSS. The process-wide concurrent
tracker high-water marks are about 58.47 and 260.9 MiB for these comparisons,
and cannot independently identify native frame residency. Cumulative tracker
lease charges per RPC fall from 53.15 to 51.90 MiB and 167.97 to 127.48 MiB;
spent charges are not peak live memory.

The composed workload validates three different result hashes and, in every
variant/repetition, 96 measured cache hits with zero measured compiles/bypasses.
At 1,024 inputs, 96 calls examine 81,920 rows, pass 27,968 and complete/sort
1,536 groups; at 32,768 they examine 2,621,440 rows, pass 937,664 and complete/sort
the same 1,536 groups. Each group has the expected three SUM/MAX state lanes;
the repeated SUM in HAVING adds no state. The residual field predicate requires
local grouping, so this workload does not claim new native aggregate pushdown.
Existing T08 endpoint cases remain compatibility coverage.

| Input rows | Callers | p50 ms | QPS | Process tracker peak MiB |
|---|---:|---:|---:|---:|
| 1,024 | 1 | 3.156 | 307.34 | 57.09 |
| 1,024 | 8 | 8.609 | 811.45 | 422.03 |
| 32,768 | 1 | 40.735 | 27.01 | 228.00 |
| 32,768 | 8 | 106.529 | 69.91 | 1,298.82 |

The composed operator retains two sort candidates and about 17.69 KiB of sort
peak reservations per query. Higher client concurrency increases both throughput
and latency/residency; it does not reduce per-query work. All measured variants
restore live reservations, native results, owner pins and flatten-pool references
to baseline.

Native retained-frame bytes, per-tracker peaks, aggregate peaks, total parent
lease charges, physical storage rows and uncached planning-time savings remain
unavailable; the report names them explicitly. Fresh Docker invocations are
recorded by the frozen driver; absent cgroup-v2 container-ID metadata is not
invented. Exact samples, counters and profiles are retained in
`.omx/bench-reports/qlb-t17-rpc-final/r{1,2,3,4,5}` and
`rpc-analysis.{json,md}`.

## Kernel measurements

Five fresh Docker repetitions produce all 135 expected records: nine shapes,
three modes and five samples. Both 100,000-row small-k gates pass. Median
top-k/full-sort time ratios are 0.0251 for k=10 and 0.0265 for k=100, below the
specified 0.80 bound. The following are median milliseconds per complete
operator execution, or per independent tuple sort.

| Rows | Keys / input / payload | k / offset | Tuple | Full sort + slice | Top-k | Top-k/full |
|---:|---|---|---:|---:|---:|---:|
| 100,000 | 3 / random / 16 B | 10 / 0 | 44.403 | 378.005 | 9.488 | 0.0251 |
| 100,000 | 3 / random / 16 B | 100 / 0 | 45.518 | 382.801 | 10.155 | 0.0265 |
| 100,000 | 3 / random / 16 B | 90,100 / 90,000 | 45.995 | 391.749 | 361.375 | 0.9225 |
| 1,024 | 1 / ordered / 16 B | 10 / 0 | 0.073 | 1.018 | 0.057 | 0.0561 |
| 32,768 | 1 / random / 1 KiB | 100 / 0 | 13.080 | 69.978 | 2.196 | 0.0314 |
| 32,768 | 3 / reverse / 1 KiB | 1,000 / 0 | 2.259 | 81.301 | 45.872 | 0.5642 |
| 32,768 | 3 / reverse / 16 B | 100 / 0 | 2.978 | 60.828 | 26.427 | 0.4345 |
| 32,768 | 8 / random / 16 B | 100 / 0 | 14.327 | 123.288 | 7.570 | 0.0614 |
| 32,768 | 8 / tied / 16 B | 1,000 / 0 | 15.750 | 176.934 | 17.190 | 0.0972 |

The tuple baseline shallow-copies tuple descriptors and borrows fixture payloads.
The public operator includes typed evaluation, owned candidate copies, tracked
allocation, output and cleanup. Full sorting costs substantially more than that
plain baseline, especially when the standard-library sorter recognizes ordered
input. The comparison makes that cost visible; the full-sort path has no speed
gate against the tuple baseline.

For both 100,000-row small-k controls, full sort performs 3,019,492 comparisons,
below the 6,800,000 bound. Top-k performs 100,440 and 108,507. All three keys have
300,000 row applications, 294 actual root entries across 98 batches and 100,000
arithmetic-kernel rows. Candidate peaks are exactly 10/100; tracked sort peaks
are 44,560/124,480 bytes, versus about 81.1 MB for full sort.

Large offsets retain much of the input: the 90,000-offset page holds 90,100
candidates, about 72.8 MB of tracked peak reservations, and approaches full-sort
time. Reverse input causes frequent candidate replacements; the 1-KiB, k=1,000
case allocates about 34.1 MB cumulatively while retaining about 2.94 MB at peak.
Bounded live state does not imply bounded total allocation over the entire scan.
Every shape passes key-work, comparator, ownership, capacity and cleanup checks.
The 1-MiB poisoned-source probe is correctness/resource evidence outside timing.
Raw records, profiles and the passed numerical gate are retained in
`.omx/bench-reports/qlb-t17-kernel-final` and `kernel-summary.json`.

## Frozen provenance

Both timing phases use Go 1.26.9, four CPUs, 8 GiB and no additional swap, with
immutable image
`sha256:4cec35c89954c6a562dcc10d347b67e18a46d32347dbd02d47405191935771ac`.
The source fingerprint, including generated QL cases, is
`53f8d0b6cfd41f22e2ea8bdab7635c44c014ebdb4a1c683113a16d126ee80e1a`.
The querymetrics RPC binary SHA256 is
`2af184567918fc6b75b72a00ecdf79176001540741284be45e4c1bb1cc19c65f`;
the normal kernel binary SHA256 is
`dfe7d2fd45bfa5298c3f1d46e3ad442e00715aa652085fe6f36d2c34985188f7`.
The driver checks these digests and the retained 32-caller failure evidence around
each repetition. Builds, tests and functional Docker runs finish before timing.
The ledger is `.omx/t17-final-provenance.json`; the complete driver log is
`.omx/t17-final-timing-driver.log`.

## Native concurrency limitation

The subsequent [admission diagnosis](t17-admission-diagnosis.md) reproduces this
blocker at `b4be414f` and proves that 32 held fanout reservations exceed the
configured shared pool. It preserves the original blocked performance verdict.

Untimed admission succeeds for the raw full-sort/top-k pair and composed
1/8-caller variants at both sizes, without changing the 256-MiB tracker setting.
The 1,024-row 32-caller composed probe fails at native broadcast admission with
`ResourceExhausted: query memory pool exhausted`. A separate forced-full-sort
32-caller control reproduces the same failure: 95 of 96 calls complete with exact
results, and the rejected call never reaches WHERE, aggregation or sorting.
Both controls restore all tracked resources to zero.

Native fanout reserves receive/parser buffers and part of the remaining allowance
before opening streams. Concurrent reservations can exhaust the service-wide
pool even though local sort candidates remain small. This is existing native
input residency, not top-k retention. Protector policy, receive buffering and
memory limits are unchanged. The same 32-caller failure is not repeated at the
larger size.

The historical benchmark protocol required all five variants. The revised
[harness](t17-acceptance-verification.md) separates overload and recovery evidence
from its four timed variants.
The later criteria revision separates the 32-caller overload check; the following
status describes the original protocol, not a new resource-gate verdict. An explicit
retained failure report enables partial measurements of the four admitted variants while
keeping the 32-caller variant marked `blocked`, with zero latency/allocation
samples and the evidence digest. Overall status remains
`partial_known_32_blocked`; it cannot be reported as a passed acceptance gate.
Artifacts are `.omx/t17-admission-1024-r3/t17-admission-1024.json`,
`.omx/t17-admission-32768/t17-admission-32768.json` and
`.omx/t17-concurrency-control/fullsort32-control/t17-admission-1024.json`.

## Compatibility boundary

T17 changes QL parsing/planning/execution and test infrastructure. Its diff from
`8ee2a074` leaves protobuf definitions/generated APIs, native execution operators,
transport/frame formats, persisted data and protector policy unchanged. The
earlier approved T15 shared-buffer changes remain part of the parent history;
they are not new T17 changes. Temporary-file spilling remains deferred.
