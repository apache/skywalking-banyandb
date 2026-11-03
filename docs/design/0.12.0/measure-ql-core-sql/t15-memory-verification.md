# T15 DISTINCT memory follow-up

This follow-up implements the three memory improvements requested after the
[initial T15 verification](t15-verification.md): compact pooled keys, reduced
native frame buffering and unordered finite-LIMIT stopping. The initial T15
working tree is preserved in `.omx/t15-memory-before/working-changes.tar.gz`, with
a source digest and runnable kernel/RPC binaries. That is the comparison base;
T14 does not implement SELECT DISTINCT.

Protobuf definitions, generated APIs, persisted and frame formats, admission
allowances and protector policy are unchanged. The approved buffering work
changes shared native Go execution code, so native-query compatibility is part
of verification. Temporary-file spilling remains deferred.

## Allocation and execution contracts

The SQL group table stores each full encoded key in an append-only chunk and
keeps a 12-byte chunk/offset/length reference. It still compares complete encoded
keys after hashing. The existing null, type, variable-width boundary, signed-zero,
NaN and selected-timestamp rules are unchanged. Aggregation and DISTINCT share
this table; DISTINCT has no aggregate state lanes.

Chunks grow from 256 bytes to 64 KiB. Only 64-KiB chunks use the repository's
tracked native pool, whose combined idle capacity is bounded at 8 MiB. Smaller
and oversized chunks remain query-owned. Chunk capacity, descriptors and old/new
growth overlap are reserved before allocation or checkout. A tight query budget
can use a smaller chunk instead of being rejected solely by preferred growth.
Returning chunks releases the query reservation but can leave bounded idle pool
memory. The pool reduces allocations; it does not provide extra query allowance.

Output batches borrow immutable key bytes through their documented lifetime.
Final protobuf egress copies values before Close returns chunks to the pool.
Public tests retain final string and binary responses across Close and repeated
pool reuse. Cancellation and budget errors restore tracked bytes and checked-out
pool references.

An unordered DISTINCT with finite, nonzero LIMIT stops after OFFSET + LIMIT
unique tuples, including within a selected batch. Addition is widened before
summing the uint32 values. It closes upstream without pulling another batch.
Explicit ordering and absent LIMIT still consume complete input; LIMIT 0 skips
execution. If input has too few unique tuples, the operator must consume it all.
Upstream aggregation, expression evaluation and native result materialization
can do work before the DISTINCT operator receives a batch. The bound therefore
applies to DISTINCT's examined rows, retained keys and further pulls, not total
storage work or total query memory.

For native scalar scan batches, drain encodes each borrowed batch before the
next pull and releases its storage owners immediately. Validated wire buffers
are joined directly into the existing unary frame, without coalescing typed
columns first. Conversion-dependent schemas retain the prior fallback. Encoded
chunks, the final frame and transport overlap remain reserved; the single-frame
response still requires memory proportional to input. The `frame-encode` span
covers encoding through concatenation, including intervening source pulls, and
reports final rows, batches and bytes once.

## Regression evidence

The compact-key public regression first failed because 4,096 keys reserved
163,904 bytes against its 150-KiB bound, then passed with the chunk arena.
Additional tests cover a 4,400-byte aggregation budget, oversized keys, full-key
collisions, error cleanup and final-response ownership.
Evidence: `.omx/t15-memory-arena-{red,green,suite,querymetrics,final-race,lint}.log`.

The pagination public regression failed on complete input consumption, then
passed with exact examined-row and source-pull assertions. Coverage includes
cross-batch duplicates, selection vectors, OFFSET, explicit ordering, uint32
sum overflow, null/NaN/timestamps, grouping/HAVING composition, cancellation,
budget failures and upstream Close errors.
Evidence: `.omx/t15-memory-limit-{red,green,normal,querymetrics,race-final}.log`.

The native drain regression first observed storage still pinned at the next
pull. A second public pipeline case uses three 256-row batches with 2,048-byte
cells and an 8-MiB tracker: the old drain exceeded its budget; the new drain
passes. Frame concatenation tests cover malformed buffers, schema differences,
nulls, constants, variable-width offsets and value ownership.
Evidence: `.omx/t15-memory-scan-{red,budget-red,native-final,race,race-lifecycle}.log`.

The generated QL-only suite adds three scenarios, each with changed bindings and
cache bypass. A constant LIMIT 1 expects examined counts [1,1,0]; a filtered
LIMIT 1 expects [1,0,0]; OFFSET 1 over a single unique constant expects complete
input counts [2201,8,0]. The independent fixture oracle supplies these counts.
All previous 15 DISTINCT scenarios and all other generated suite sections remain
equal to the preserved T15 baseline. Only three scenarios and two inventory IDs
are added. Evidence: `.omx/t15-memory-generator-{red,green}.log` and
`.omx/t15-memory-generated-baseline-audit.json`.

Final integration runs select the same QL capabilities as the initial T15 suite,
plus the three generated stopping cases and the existing native vectorized
measure-scanning table. They use four CPUs, 8 GiB, no additional swap and the
frozen production sources. All selected cases pass:

| Topology | QL specs | Existing native specs | Total passed | Test time |
|---|---:|---:|---:|---:|
| Standalone | 524 | 120 | 644 | 188.24 s |
| Distributed | 524 | 120 | 644 | 305.71 s |

The 21 T15 QL specs use only BydbQL Query RPCs for query/readiness verification.
The 120 native specs are existing compatibility cases, not raw counterparts
added to the new scenarios. Evidence is
`.omx/t15-memory-final-{standalone,distributed}.{log,json}` and
`.omx/t15-memory-final-e2e-summary.json`. Unselected specs are not counted as
passes. Earlier QL-only runs also passed 524 specs per topology before the scalar
receive optimization; the combined runs above verify the final sources.

Normal QL, planner, frame and complete measure-package tests pass, as do
`trace_pipeline,querymetrics` QL/planner/benchmark-harness tests. Full `make lint`
and deterministic `make check-ql-test-cases` pass. All 73 changed Go files carry
ASF headers, the Git index remains empty and no protobuf paths change.
Evidence is `.omx/t15-memory-final-{normal,instrumented,make-lint}.log`,
`.omx/t15-memory-generation-check.log` and `.omx/t15-memory-final-audit.json`.

## Native admission diagnosis

The first buffering change alone reduced process-wide peak reservations in the
fresh 16,384-row wide-string replay from 465,894,510 bytes to 296,765,422 for
DISTINCT and 292,738,926 for equivalent GROUP BY. Both requests still failed
before local grouping. These are process-wide reservation observations, not RSS,
per-operator peaks or successful-query throughput.

A temporary Go overlay traced the exact rejection without changing production
protector code. The liaison lease had a 201,326,592-byte assignment, had charged
131,892,444 bytes and rejected the next 80,282,848-byte charge in
`reserveNativeFrames`, for the second replica frame. This occurs before frame
decode, merge and DISTINCT. Shared pool pressure and stale readiness leases were
not the rejecting condition. With the existing defaults, the 8-GiB container
selects a 6-GiB protector limit, a 1.5-GiB query pool and a 192-MiB per-query
admission cap, independently of the 256-MiB tracker setting.

The receive guard charged scalar frames using a protobuf worst-case allowance:
13 extra payload copies after ingress, plus 128 bytes per cell and row metadata.
The follow-up inspects scalar frame buffers without decoded-column allocation,
including constant expansion and schema compatibility. Homogeneous time-ordered
raw frames use contiguous borrowed output ranges; interleaved winners materialize
into the existing batch copy path. The scalar reservation retains the 128-byte
cell/row guard and separately covers decoder alignment copies, constant payload,
schema descriptors and workspace. Non-scalar, heterogeneous, grouping, aggregate,
TOP and indexed-order paths keep the conservative fallback. No allowance is
raised and no admission charge is refunded.

The exact 16,384-row replay then passed for both query forms. All 16,384 rows
were independently verified through 16 QL windows; DISTINCT and GROUP BY each
returned 163 tuples, 46,944 encoded key bytes, 70,896 bytes of key capacity and
zero aggregate state lanes. Tracker-backed lease charges were 93,259,250 and
93,258,738 bytes respectively. Post-Close owner, tracker and pool baselines and
the subsequent small-query recovery passed. These two untimed requests establish
correctness under the original limits, not a throughput or latency improvement.
The final source replay passes the same assertions after helper extraction and
overflow checks. Evidence is `.omx/t15-memory-rpc-final.{log,json}` and
`.omx/t15-memory-rpc-final-protocol.json`; the earlier successful scalar replay
remains in `.omx/t15-memory-rpc-scalar.log` and its report directory.

A public shared-batch regression first requested 48,760,960 bytes against a
32-MiB tracker on the old receive path, then passed while borrowing output
columns. Interleaved winners exercise the copy fallback. Mixed-protobuf rejection
and recovery, cancellation, poisoned frame ownership through native response
Close and compact constant expansion are covered separately. Frame, measure
frame and distributed-plan race suites pass. Evidence is
`.omx/t15-memory-scalar-{red,tests,race}.log`.

The diagnostic overlay and failed intermediate replay remain separate from
successful timing evidence. The original 100,000/1,000,000-row probe results in
the initial T15 record describe that baseline, not the revised scalar path.

## Measurement scope

Memory comparisons use the preserved initial T15 binaries and the completed
follow-up in resource-limited Docker containers: four CPUs, 8 GiB, no additional
swap and Go 1.26.9. Timing starts after correctness and source freeze; concurrent
compilation and integration jobs are excluded.

Five fresh before/after pairs alternate side order. Each side runs in a fresh
container, using the same image ID, fixture timestamp base and query schedule.
The source manifest covers 2,330 Go/module/generated-case and benchmark-script
files; it is checked before every workload and after the final one. Binary
digests and build metadata are preserved separately. Kernel binaries use normal
builds on both sides; RPC binaries use `querymetrics` on both sides. Evidence is
`.omx/t15-memory-{source-freeze,timed-protocol}.json`,
`.omx/t15-memory-frozen-source.sha256` and
`.omx/t15-memory-after/build-metadata.json`.

The existing eight DISTINCT kernel cases retain their original 1,000-row,
duplicate-ratio and string-width schedule. A separate two-control pagination
kernel uses the same 1,000 unique wide tuples and LIMIT 10 OFFSET 5. A literal
oracle establishes identical ten output tuples. Complete-consumption control
examines and retains 1,000 keys; unordered early stopping examines and retains
15. The control flag does not establish new SQL ORDER BY support.

The RPC comparison keeps the original 14 variants, 1,024/8,192 input rows,
single worker, 256-MiB tracker and complete independent result/cleanup checks.
Both DISTINCT and the equivalent GROUP BY reference benefit from the shared
arena and native buffering, so old/new binaries are required to attribute memory
savings. Historical T15 measurements remain historical; no retrospective T02
latency-gate verdict or unrelated cache benchmark is added.

The fixed schedule contains 40 before and 50 after kernel samples. Each RPC side
has 140 flat variant records, 8,400 timed requests and 280 CPU/heap profile files.
Allocation and latency observations include client, liaison and both data nodes.
The eight legacy kernel cases do not expose key capacity or examined rows; those
metrics remain unavailable there. RPC and the two new pagination controls expose
key capacity directly. Comparisons report medians, observed ranges and per-pair
ratios, without a new latency pass threshold or a statistical significance claim.

The first comparison attempt completed its first RPC workloads but failed in
report merge because the temporary wrapper retained execution-only environment
variables. Clearing them made an untimed merge of those same shards pass. The
complete five-pair comparison was restarted in the fresh
`.omx/t15-memory-runs/paired-five-v2` directory. The incomplete attempt is
preserved under `paired-five` and excluded from final comparisons. No production
source, binary, query schedule or resource limit changed for this correction.
Evidence is `.omx/t15-memory-wrapper-{fix.json,merge-check.log}`.

## Kernel results

The fixed five pairs contain 40 baseline and 50 candidate samples. Each sample
passes exact tuple/key-byte and zero-state assertions and returns live tracker
bytes to zero. The table reports medians; per-run values, observed ranges and
paired ratios are retained in `.omx/t15-memory-kernel-final-analysis.json`.

| Padding/string | Unique/input | Median µs/op before → after | B/op before → after | Allocs/op before → after |
|---:|---:|---:|---:|---:|
| 8 | 0/0 | 4.20 → 4.53 | 4,104 → 4,136 | 40 → 40 |
| 8 | 10/1000 | 156.06 → 159.22 | 6,064 → 6,336 | 67 → 63 |
| 8 | 500/1000 | 250.82 → 252.82 | 71,888 → 74,200 | 567 → 85 |
| 8 | 1000/1000 | 350.42 → 338.24 | 138,896 → 143,856 | 1,069 → 89 |
| 256 | 0/0 | 4.39 → 4.41 | 4,104 → 4,136 | 40 → 40 |
| 256 | 10/1000 | 206.70 → 209.45 | 12,304 → 13,752 | 67 → 66 |
| 256 | 500/1000 | 528.63 → 358.71 | 336,849 → 107,264 | 567 → 84 |
| 256 | 1000/1000 | 887.68 → 451.68 | 667,857 → 144,128 | 1,069 → 86 |

At 1,000 unique wide tuples, allocated bytes fall by 78.4% and heap allocations
by 92.0%; median kernel time changes from 887.68 to 451.68 µs. With narrow
tuples, chunk rounding increases allocated bytes by about 3.2–4.5% for nonempty
input while sharply reducing objects at high cardinality. The ten-key wide case
uses 11.8% more bytes. Empty input adds 32 bytes and no heap objects. This is a
trade-off, not a claim that every input shape uses fewer bytes or less CPU.
Key equality and encoded key bytes remain unchanged.

The after-only pagination controls produce the same ten independently checked
tuples for LIMIT 10 OFFSET 5. They time complete construction, consumption,
drain and Close. Their medians are:

| Control | Examined rows / retained keys | Key capacity B | Median µs/op | Heap B/op | Allocs/op |
|---|---:|---:|---:|---:|---:|
| full_input | 1,000 | 610,776 | 451.20 | 146,176 | 87 |
| early_stop | 15 | 16,840 | 23.33 | 22,160 | 69 |

Early stopping reduces retained key capacity by 97.2% and allocated bytes by
84.8% in this fixed kernel. It examines 15 rows rather than 1,000. These figures
exclude native input work; they do not establish an equivalent full-RPC speedup.

## Full-RPC results

All five pairs pass the unchanged 14-variant matrix at both cardinalities. The
complete comparison validates 280 flat result records, 16,800 timed RPCs, 560
nonempty CPU/heap profiles and 1,680 independently checked complete response
envelopes. Each timed request hits the compiled plan cache, has the expected
unique tuple count/key bytes, retains zero aggregate states and restores tracked
reservations and owners after Close. Response hashes, row counts and schedules
match across sides. Raw source/binary and artifact digests pass.

The table shows DISTINCT medians across five runs. Heap allocation includes the
client, liaison and both data nodes. Key capacity is retained query accounting,
not RSS or allocated bytes. The p50 ratio is the median of five within-pair
after/before time ratios, summarized separately from each side's time medians.
All 28 DISTINCT/GROUP BY combinations, raw observations, latency quantiles and
observed ranges are in
`.omx/t15-memory-runs/paired-five-v2/analysis.json`.

| Rows | Shape | Heap MiB/query before → after | Allocs/query before → after | Key capacity KiB before → after | Paired p50 ratio |
|---:|---|---:|---:|---:|---:|
| 1,024 | p001_narrow | 2.20 → 1.81 | 15,324 → 15,219 | 0.75 → 1.20 | 0.889 |
| 1,024 | p001_wide | 3.44 → 3.61 | 15,110 → 15,176 | 5.88 → 4.42 | 0.884 |
| 1,024 | p050_narrow | 2.93 → 2.62 | 28,744 → 28,045 | 28.06 → 42.05 | 0.950 |
| 1,024 | p050_wide | 4.97 → 5.01 | 30,549 → 30,013 | 268.50 → 202.34 | 0.933 |
| 1,024 | p100_narrow | 3.86 → 3.45 | 44,538 → 43,500 | 56.06 → 84.07 | 0.961 |
| 1,024 | p100_wide | 6.69 → 6.47 | 44,486 → 43,116 | 536.50 → 340.39 | 0.916 |
| 1,024 | empty | 0.56 → 0.56 | 5,929 → 6,010 | 0.00 → 0.00 | 0.952 |
| 8,192 | p001_narrow | 12.25 → 8.99 | 75,179 → 74,987 | 5.59 → 6.45 | 0.822 |
| 8,192 | p001_wide | 23.67 → 24.30 | 77,594 → 77,513 | 44.00 → 34.71 | 0.957 |
| 8,192 | p050_narrow | 18.85 → 14.88 | 184,404 → 179,947 | 224.06 → 272.18 | 0.895 |
| 8,192 | p050_wide | 36.48 → 35.47 | 184,332 → 180,064 | 2,144.50 → 1,296.84 | 0.967 |
| 8,192 | p100_narrow | 24.46 → 20.27 | 292,261 → 284,284 | 448.06 → 480.23 | 0.975 |
| 8,192 | p100_wide | 46.28 → 44.38 | 292,907 → 284,827 | 4,288.50 → 2,529.52 | 1.006 |
| 8,192 | empty | 0.56 → 0.57 | 5,932 → 6,015 | 0.00 → 0.00 | 1.088 |

For 8,192 unique wide tuples, key capacity falls from 4,391,424 to 2,590,224
bytes (41.0%). Median allocated bytes fall from 48,523,617 to 46,539,481 per RPC
(4.1%); whole-process heap objects fall from about 292,907 to 284,827. Full-RPC
allocation also includes storage, merge bookkeeping, client and final protobuf
output. Its savings are much smaller than the isolated DISTINCT kernel's.

This case has a median paired p50 ratio of 1.006, with individual pairs ranging
from 0.904 to 1.026; p99 ratios range from 0.880 to 1.016, with median 0.995.
Its separately summarized p50 medians are 50.25 and 48.78 ms. Their direction
differs from the median paired ratio because the two summaries combine the five
runs differently. These observations do not establish a latency pass gate or a
statistical speedup. Across all DISTINCT shapes, median paired p50 ratios span
0.822–1.088 and p99 ratios span 0.779–1.192; the largest tail increase is the
8,192-cardinality empty-input case (2.74 to 3.22 ms p99 medians).

The trade-offs remain visible. Narrow-key chunk headroom can increase retained
key capacity, including 458,816 to 491,752 bytes for 8,192 unique narrow tuples,
even while whole-RPC allocated bytes decrease. The low-cardinality wide cases
allocate slightly more whole-process bytes. Direct chunk encoding and final
concatenation can allocate both chunk and joined payloads while releasing storage
earlier. The whole-process counters do not isolate each allocation source.
The scalar receive path borrows
contiguous metadata/header buffers but retains copies for irregular merges.
Neither change makes memory proportional only to output cardinality.

GROUP BY reference cases receive the same arena and native-buffer improvements;
they are not an independent old algorithm. Comparisons therefore use the
preserved initial T15 binaries. Native unary materialization, conservative
monotonic admission charging and the absence of spilling remain limits. The
16,384-row success above establishes that regression case; it does not establish
100,000/1,000,000-row throughput for the revised implementation.

Final evidence: `.omx/t15-memory-final-analysis.log`,
`.omx/t15-memory-artifact-verification.log`,
`.omx/t15-memory-runs/paired-five-v2/{run.json,artifacts.sha256,analysis.json}`.
The historical initial T15 reports remain unchanged. The incomplete wrapper
attempt and its untimed derived merge report remain separate; its raw timing
shards and profiles are preserved. Measurements used the uncommitted source
snapshot identified above; the delivery commit follows verification.
