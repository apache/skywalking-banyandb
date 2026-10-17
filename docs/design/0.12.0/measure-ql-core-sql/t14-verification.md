# T14 HAVING and compatible collection membership

Implementation base: `8c69d7d1` (T12), on PR #1412. Verification uses Go 1.26.9.
Changes are limited to QL grammar/planning, liaison batch execution, generated
QL-only integration cases and this ticket's benchmark. Protobuf, native/data-node
operators, indexed storage, frame/persisted formats and memory-protector policy
remain unchanged. Temporary-file spilling remains deferred.

The post-review frozen protocol is `.omx/t14-review-fixes-frozen-protocol.json`.
Source hashes for 618 production, test, generated-fixture and benchmark-harness
files are `.omx/t14-review-fixes-source-manifest.sha256`. The original 609-file
freeze and `.omx/t14-frozen-protocol.json` retain earlier measurements; the same
19 kernel and 32 RPC variants are rerun after the review fixes. Documentation
is outside the timing manifest.
All timing containers use Go 1.26.9, a four-CPU quota, an 8-GiB memory cap and no
additional swap. Kernel and RPC timings run sequentially, after integration,
package/race checks and resource probes finish. Unrelated host services remain
running; these measurements describe that bounded setup.

## Execution contract

HAVING evaluates typed expressions over original grouping names and named
aggregate calls. Only TRUE retains a completed group; FALSE and UNKNOWN remove
it. SELECT aliases do not enter HAVING scope. Nested calls, computed aggregate
arguments and nongrouped source references fail before native dispatch. Hidden
calls/keys survive until filtering, then visible projection removes them. Repeated
calls share state and native branches. Group-only HAVING uses local grouping;
scalar empty aggregation produces its SQL empty row before HAVING can remove it.

The existing native raw scan and eligible native aggregate operators are shared
with protobuf queries. A local aggregate or complete native keyed merge feeds
the same QL filter kernel, followed by visible projection and final OFFSET/LIMIT.
HAVING never limits aggregate input or individual native branches. Raw WHERE and
HAVING expose separate statistics. Cached plans retain immutable expression and
slot metadata; every bind supplies fresh values and execution state.

CONTAINS/NOT CONTAINS preserve legacy infix HAVING/NOT HAVING membership. Integer
and string requests require all terms. A scalar source can satisfy one term or
duplicates of that term, but cannot satisfy distinct requested terms. NULL
sources, scalar NULL requests and empty literal lists do not match; negation
complements this result. Empty array parameters retain the existing binding error.
Typed membership composes with field/time residuals and post-aggregate HAVING.
Ordinary legacy queries retain native routing; MATCH composition stays rejected.
CONTAINS is a contextual, measure-only operator, preserving existing `contains`
tags and `contains-svc` resource/group names across legacy query kinds.
See the [language guide](../../../interacting/bydbql.md#59-group-filtering-and-collection-membership).

Filtering borrows input value columns and changes selection. Partial matches
retain uint16 row indices; all/no-match cases avoid a retained selection. Scratch,
expression metadata and selection reservations charge before allocation and
balance on EOF, errors, cancellation and repeated Close. Copy counters are
structural zero invariants. Unit and kernel pre-timing column-identity checks
prove borrowing; the RPC counters alone do not measure copies. Charged decoding
scratch is outside that invariant. No new pool is introduced.

## Review fixes

Claude Code's independent Spec/Standards reviews are retained in
`.omx/t14-claude-review.md`. Two main findings were reproduced before the fixes:
the global CONTAINS keyword rejected legacy names, and newly accepted typed
membership silently filtered a truncated indexed integer array. The parser now
recognizes the operator contextually and restricts the new alias to measures.

Before dispatch, QL-evaluated array membership checks current index bindings and
their rules through the existing metadata registries. A matching indexed array
returns InvalidArgument; metadata lookup failure also fails closed. This check
runs after authorization on every execution, including compiled-plan cache hits.
It does not filter binding validity windows because native caches apply expired
bindings too. Queries without typed array membership skip these metadata reads.
The immutable compiled plan remains reusable; index state is not cached in it.
This adds metadata work to array-membership RPC timings, reported inclusively.

The one-row indexed INT_ARRAY regression first fails with silently wrong results,
then passes with pre-dispatch rejection (`.omx/t14-fix-indexed-{red,green}.log`).
A separate real-RPC regression warms both membership spellings on an unindexed
array, adds an expired binding, then checks two cache hits with no recompilation
and no data-node dispatch. It is RED without the guard and GREEN with it:
`.omx/t14-fix-cached-index-{red,green}.log`. This correctness probe adds no timing
variant or old-ticket benchmark matrix.

The remaining changes share WHERE/HAVING nesting scans and expression-recipe
recursion, centralize membership setup and aggregate helpers, validate DISTINCT
consistently, and clarify hidden-output naming. HAVING now requires an explicit
visible-output slice; nil rejects and closes the source, while an explicit empty
slice remains valid. Constructor-selected metric callbacks replace stage booleans,
and statistics share one source traversal. Benchmark suffixes and unavailable
metric names also have a single definition. Public RED/GREEN evidence is under
`.omx/t14-review-planner-*`, `.omx/t14-review-runtime-*` and
`.omx/t14-review-bench-*`. The review's panic-wording concern was rejected because
the original log explicitly records PANIC from `unmarshalVarArray`.

Actual serialized response bytes now come from `proto.Size` of the six retained,
independently checked full BydbQL responses after timing and profiling stop.
Reports require total/min/max sizes, all at least the empty envelope size, with
overflow-safe consistency checks. Ten report mutations verify rejection of missing
or fabricated byte evidence. The original ticket's output-byte requirement is
restored; the native source reference below is corrected.

## Generated real-RPC coverage and package checks

The generator adds **163 T14 specs** and audits **35 T14 coverage obligations**:
118 typed cohorts, 16 ordinary indexed-scalar compatibility cohorts and eight
reference rejections, plus 21 review cohorts (five contextual-identifier cases and
16 indexed-array rejection cases). Applicable cohorts use three bindings, warm changed values,
cache bypass and recovery. Expected values come from independently specified
fixture arithmetic, never a native Query response or the production evaluator.
Both string/integer arrays and both membership spellings remain covered.

The fixture extends F with null-only and array cases; host SUM(v) is a=2, b=4,
c=6 and null-only n=NULL. Coverage includes visible/hidden/repeated aggregates,
TRUE/FALSE/UNKNOWN, empty/null-only scalar results, group-only HAVING, alias and
reference rejection, casts/CASE/COALESCE, membership scalar/list/array/duplicate/
null bindings, field residual composition, cancellation, budget rejection and
recovery. A separate 129-group fixture checks omitted LIMIT and pagination after
complete group filtering. Explicit non-null grouping predicates activate eligible
native cases; local and native routes are checked through the public bound plan.

All earlier 340 specs remain intact. Both four-CPU/eight-GiB Docker topologies
pass **503 selected It specs**, excluding lifecycle/report nodes:

| Topology | Package runtime | Final report |
|---|---:|---|
| Standalone | 110.372 s | `.omx/t14-review-fixes-standalone.json` |
| Liaison and two data nodes | 143.686 s | `.omx/t14-review-fixes-distributed.json` |

`make check-ql-test-cases` passes. The grammar fingerprint is
`b6aab8e3bd74654f05f500af81638b68602dfc39caa6849ddda5c106b9b0901d`.
Planner RED/GREEN logs cover clause parsing, membership composition and scope;
runtime RED/GREEN checks cover filter ordering, membership semantics and ownership.
They are retained under `.omx/t14-planner-*` and `.omx/t14-engine-*`.

Final normal and querymetrics/race checks pass for `pkg/bydbql`, `pkg/query/ql`,
the liaison, generated cases and the owning benchmark contracts. CLI and MCP
parser consumers also pass. Logs include `.omx/t14-review-planner-{normal,race}-final.log`,
`.omx/t14-review-runtime-{normal,race}.log`,
`.omx/t14-review-generated-all-{normal,race}-final-v2.log`,
`.omx/t14-review-grpc-{normal,race}.log`, `.omx/t14-review-parser-consumers.log`
and `.omx/t14-review-bench-*`. The final generated race run covers all
`test/cases/measure/...` packages. Repository `make lint` passes with zero issues
(`.omx/t14-review-make-lint.log`). Formatting,
ASF headers, shell syntax and `git diff --check` pass before the source freeze.

## Focused kernel benchmark

K(T14), `BenchmarkHavingT14`, times construction, Init, complete drain and Close
over an already completed borrowed group batch. It excludes raw scanning,
aggregation and protobuf output. Fifteen variants combine 1/1,024/65,536 groups
with no HAVING or thresholds 0/1/50/100. Four more vary membership array length
0/1/16/256 over 1,024 rows. Half the nonempty arrays match their first element;
the other half require a full nonmatching scan. Input element count describes
array width, not actual comparison count.

All **19 variants × five one-second samples** pass independent physical-row and
selection checks, borrowed-column checks and balanced reservations. Runtime is
114.282 seconds. Raw results are `.omx/t14-review-kernel.log`; all samples and medians
are `.omx/t14-review-kernel-median.json`.

| Completed groups | Predicate | Selected | Median ns/group | Heap bytes/op | Allocs/op |
|---:|---|---:|---:|---:|---:|
| 1 | none | 1 | 822.40 | 872 | 10 |
| 1 | 50 | 1 | 4,949.00 | 5,888 | 30 |
| 1,024 | none | 1,024 | 0.82 | 872 | 10 |
| 1,024 | 0 | 0 | 22.04 | 7,928 | 30 |
| 1,024 | 1 | 11 | 26.99 | 7,952 | 31 |
| 1,024 | 50 | 524 | 27.96 | 9,080 | 31 |
| 1,024 | 100 | 1,024 | 21.50 | 7,928 | 30 |
| 65,536 | none | 65,536 | 0.01 | 872 | 10 |
| 65,536 | 0 | 0 | 17.30 | 136,952 | 30 |
| 65,536 | 1 | 656 | 22.11 | 138,360 | 31 |
| 65,536 | 50 | 32,786 | 22.81 | 210,680 | 31 |
| 65,536 | 100 | 65,536 | 16.74 | 136,952 | 30 |

Thresholds use `row % 100 < threshold`, including the final partial cycle.
Membership medians are 25.55/32.34/40.40/168.10 ns per input group at lengths
0/1/16/256, respectively, with 0/512/512/512 passing groups. The long-array cost
reflects element scanning. The no-HAVING baseline borrows projection without
examining values, so its per-group denominator does not imply a scan rate.
Small-batch setup cost and partial-selection allocations are visible; this
benchmark makes no claim that adding a predicate makes the kernel faster.

## Focused RPC benchmark protocol

P(T14), `qlb_t14`, has 12 grouped variants at each input size: native/local routes,
each with a baseline, thresholds 0/1/50/100 and hidden-AVG 50%. Both routes compute
the same 16 group means from `host=row%16`, nullable `f=(row%29-14)/4`, with NULL
every seventh row and duplicate writes. The baseline selects AVG twice. Predicate
variants repeat AVG in SELECT and HAVING; hidden variants select only host.
All use complete input and share the same two aggregation state lanes. Native
execution has exactly two SUM/COUNT branches; local execution has one raw scan.
An existing test hook forces local execution for the reference route.

Quantile thresholds come from independently computed means. A nominal 1% of 16
groups rounds up; ties can retain more. Thresholds 0/100 reject/retain all groups.
The hidden variant omits two output fields relative to its route's baseline;
its latency difference includes narrower output, rather than isolating HAVING.

At 1,024 rows, eight additional variants compare both membership spellings at
array lengths 0/1/16/256. Their separate fixed 1,024-row fixture supplies 256
candidates per width. A native entity predicate selects width; typed membership
and `f::FIELD >= 0` precede COUNT aggregation and `HAVING COUNT(f::FIELD) > 0`.
Empty arrays yield no groups. Other widths pass 128 rows and produce eight groups
with count 16 each. Membership does not scale with the main 32,768-row fixture.

This is **32 variants per repetition**, five fresh containers, one worker,
256-MiB query budgets, 100 timed/5 warmup calls at 1,024 rows and 20 timed/3 warmup
calls at 32,768. Each cardinality starts a fresh cluster process. All warmups and
six retained timed responses receive full independent checks outside timing;
every timed call checks result count. Each timed call must hit the compiled-plan
cache without compilation. Reports retain all latencies and CPU/heap profiles.

Required work: raw WHERE sees only membership candidates; HAVING sees completed
groups; hidden outputs never serialize; repeated calls add no branches/lanes;
there are no intermediate DataPoints or leaked reservations/results/owners/pool
references. Native source counters are 4N and frame counters 4G across two
branches and two replicated physical leaves; local counters are 2N with N rows
reaching local aggregation. These are decoded/transport work, not physical
storage reads. Native merge state is 18G bytes; local state is 16G (membership
8G), excluding keys/capacity/other reservations.

CPU and allocation snapshots cover the client, liaison and both nodes in one
process. Shared filter-kernel profiles do not isolate HAVING CPU. Process peak
reservations are a global high-water mark and can include earlier variants;
per-operator peak retained bytes, native retained frame bytes, total cumulative
parent lease charges and physical storage reads are explicitly unavailable.
Available stage reservation totals balance releases; the monotonic tracker-lease
counter is cumulative and is not represented as refunded admission.

The owning command is documented in the
[benchmark guide](../../../../test/integration/distributed/querybench/README.md#t14-filtering-completed-groups-and-collection-membership).
No earlier-ticket timing matrix is expanded. G1's historical T02 inconclusive
comparison remains unchanged; these are measurements of the new capability and
its current aggregation reference, not a cross-revision compatibility verdict.

## RPC results

All five post-review repetitions pass: **160 case records, 11,200 timed latencies
and 320 nonempty CPU/heap profiles**. Independent result/count/work/cache/cleanup
and response-byte checks pass; all 618 frozen source hashes remain unchanged.
Reports are `.omx/bench-reports/t14-review-r{1..5}/distributed-querybench.json`;
summaries with all five observations are `.omx/t14-review-rpc-summary.json`.
Independent post-run checks are `.omx/t14-review-rpc-analysis.log`.

The table gives medians of five per-run measurements. CPU covers the client,
liaison and both data nodes. Output bytes describe the actual full RPC envelope,
averaged over the six checked timed responses per variant.

| Input rows | Route / predicate | Output groups | p50 ms | p95 ms | CPU ms/query | Allocated KiB/query | Output bytes |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1,024 | local / none | 16 | 3.636 | 4.939 | 8.7 | 1,526.6 | 1,315 |
| 1,024 | local / 50 | 8 | 3.633 | 5.426 | 9.0 | 1,534.0 | 659 |
| 1,024 | local / hidden 50 | 8 | 3.607 | 5.378 | 8.8 | 1,562.3 | 275 |
| 1,024 | native / none | 16 | 3.964 | 5.609 | 11.6 | 1,153.0 | 1,315 |
| 1,024 | native / 50 | 8 | 3.902 | 5.592 | 11.1 | 1,165.0 | 659 |
| 1,024 | native / hidden 50 | 8 | 3.929 | 5.807 | 11.5 | 1,161.7 | 275 |
| 32,768 | local / none | 16 | 48.340 | 55.706 | 99.0 | 48,567.7 | 1,315 |
| 32,768 | local / 50 | 8 | 47.594 | 55.323 | 99.0 | 48,209.0 | 659 |
| 32,768 | local / hidden 50 | 8 | 48.380 | 53.519 | 97.5 | 48,477.5 | 275 |
| 32,768 | native / none | 16 | 20.340 | 23.998 | 58.0 | 10,505.8 | 1,315 |
| 32,768 | native / 50 | 8 | 18.859 | 23.609 | 52.5 | 10,468.9 | 659 |
| 32,768 | native / hidden 50 | 8 | 19.448 | 24.318 | 55.5 | 10,408.6 | 275 |

Across grouped predicate variants, median paired p50 ratios to their route's
no-HAVING baseline span 0.900–1.038; p95 ratios span 0.965–1.100. Allocation ratios
span 0.985–1.024. These are medians of within-repetition ratios, which need not
equal ratios of the table's separate medians. They are descriptive, not confidence
bounds or proof of a general speedup. The small native baseline's five p50 values
span 3.777–4.310 ms. Completed-group filtering remains a small part of the
complete native scan/decode/aggregate queries in this fixture.

Thresholds 0/1/50/100 retain exactly 0/1/8/16 of 16 completed groups at both input
sizes, with actual envelopes of 2/84/659/1,315 bytes. Each native query still uses
two branches/four physical leaf calls and two merge state lanes (288 used state
bytes). Each local query uses one scan/two physical leaf calls and two lanes
(256 used state bytes). HAVING50 reserves 26,996 stage bytes per query, including
metadata/scratch, and retains a 16-byte selection. Releases match reservations;
keys, capacity and native buffers remain separately accounted. Repeated/hidden
calls introduce no extra branch/state lane. Hidden output fields are absent from
checked responses; their narrower eight-group envelope is 275 bytes.

Membership uses the fixed 256-candidate cohort per length:

| Array length | CONTAINS p50 ms | Legacy infix p50 ms | CONTAINS allocated KiB/query | Legacy allocated KiB/query | Output bytes |
|---:|---:|---:|---:|---:|---:|
| 0 | 5.010 | 5.195 | 1,476.5 | 1,474.4 | 2 |
| 1 | 5.337 | 5.620 | 1,632.3 | 1,629.9 | 427 |
| 16 | 6.807 | 6.720 | 2,598.9 | 2,635.4 | 427 |
| 256 | 24.404 | 24.041 | 22,717.8 | 22,647.7 | 427 |

Both spellings have identical physical work and stage counts. These timings include
current binding/rule metadata validation on every array-membership call, including
cache hits. This safety check adds metadata work; compiled templates still avoid
parsing and compilation. Longer arrays also increase native decoding, transport
and protobuf allocation; the RPC slope is not solely membership comparison cost.
Representative profiles open successfully with `go tool pprof`: native aggregation
includes bit decoding and native batch aggregation; long membership arrays include
array decoding, protobuf work and garbage collection. Summaries are
`.omx/t14-review-native-cpu-top.log` and `.omx/t14-review-membership-cpu-top.log`.
K(T14) isolates the group-filter kernel cost. Earlier pre-review artifacts remain
available under `.omx/t14-rpc-*` and `.omx/bench-reports/t14-r{1..5}`; the current
tables supersede them, without asserting a controlled cross-revision speedup.

## Resource probes and native limits

These probes predate the review fixes and are retained as evidence of the existing
native limits. Post-review generated RPC cases, the cached-index regression and
the same focused benchmarks verify budgets/cleanup for the changed paths. No
large-input throughput claim is added.

Separate fresh four-CPU/eight-GiB processes probe 100,000/1,000,000 rows at 256 MiB
and 100,000 at 16 MiB. Before owning queries, bounded QL windows independently
verify every fixture row. Five attempts per native/local route then check either
the complete independently expected result or ResourceExhausted without partial
output, followed by cleanup and small-query recovery.

| Rows | Budget | Fixture readiness | Native HAVING | Local HAVING |
|---:|---:|---|---|---|
| 100,000 | 256 MiB | 100,000 rows / 16 windows | 5 correct results, 16 input / 8 selected groups | 5 ResourceExhausted before aggregation/HAVING |
| 1,000,000 | 256 MiB | 1,000,000 rows / 128 windows | 5 ResourceExhausted before HAVING | 5 ResourceExhausted before aggregation/HAVING |
| 100,000 | 16 MiB | First readiness query exhausted | Owning query blocked | Owning query blocked |

At 100,000/256 MiB the representative local failure requests 12,517,408 bytes
after 262,138,987 bytes are already charged against 268,435,456. At
1,000,000/256 MiB, native input decoding requests 21,880,784 after roughly
264,745,051 bytes; failure occurs in the first SUM branch before merge/HAVING.
The 16-MiB readiness query requests roughly 15.1 MiB after roughly 14.1 MiB is
already charged. The same prerequisite blocks a meaningful 1,000,000/16-MiB
owning probe, so it is not repeated or reported as a passed capability test.

All attempted paths release live resources; successful-readiness runs also pass
small-query recovery. The blocked readiness run verifies cleanup but does not
claim owning-query/recovery evidence. Process-global reservation high-water
ranges are 501,510,652–582,353,446 bytes for 100,000/256 MiB and
591,295,319–591,745,252 for 1,000,000/256 MiB. These combine one liaison and two
independent data-node trackers and can include earlier readiness, so comparing
them with a single 256-MiB tracker would be incorrect. They are not per-route peaks.
Probe elapsed/allocation observations overlap other verification jobs and are
not used as isolated performance measurements.

Reports are `.omx/t14-resource-reports/qlb_t14_resources_<rows>rows_<budget>mib.json`;
logs are `.omx/t14-resources-{100k-256,1m-256,100k-16}.log`. Large failed inputs
provide resource-bound evidence, not HAVING throughput. Spilling local aggregate
state alone would not bypass earlier native input decoding/materialization limits.

## Existing indexed-array projection limitation

The initial real standalone smoke fails on the old membership spelling:

```sql
SELECT rid FROM MEASURE ql_t14_legacy IN replicated_group
TIME BETWEEN ? AND ? WHERE labels HAVING ? ORDER BY TIME ASC
```

Binding the string `red` returns STATUS_INTERNAL_ERROR with
`unmarshalVarArray failed: invalid variable array: invalid query message`.
Evidence remains in `.omx/t14-standalone-smoke-v1.log` and its JSON report.
This is a QL RPC reproducer, not a native Query oracle. No baseline-revision
RPC rerun is claimed.

Read-only source analysis locates the existing boundary: standalone writing stores
indexed array elements as separate index fields and skips the packed TSDB column
(`banyand/measure/write_standalone.go`). Index projection retains only the last
stored term (`banyand/internal/storage/index.go`, `appendSearchHit`), which measure
query decoding treats as a packed array (`banyand/measure/query.go`). STRING_ARRAY
then lacks the expected packed delimiter and can panic; INT_ARRAY can silently
become a one-element array. Hidden criterion projection reaches this path even
when SELECT omits the array. The liaison has not yet received a batch and cannot
repair it with a QL filter or a `::TAG` qualifier.

The review adds a real-RPC INT_ARRAY reproducer: an acknowledged `[1,2]` write
projects as `[2]`; typed CONTAINS 1 and legacy membership plus a field residual
silently return no row, whereas CONTAINS 2 returns it. Evidence is
`.omx/t14-claude-review-indexed-probe.log`. This new accepted-query exposure is
fixed by the pre-dispatch guard, rather than left as a documentation-only limit.

T14 leaves native storage/decoder paths unchanged. All typed string/integer
array cases and both spellings remain on unindexed arrays, alongside generated
indexed-array rejection cases with no data dispatch. Ordinary native-route
compatibility uses indexed scalar string/integer sources to pin all-terms,
duplicates, binding and null behavior. This does not establish that native indexed
array projection works.
