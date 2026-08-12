# Trace Pipeline Merge Optimization Plan

## Status

Execution record. Phases 1 through 10 are closed. The original Phase 3 resource targets are retained below as historical
goals, together with their measured disposition; they are not reported as passed. The final serialized SkyWalking
integration and acceptance run passed in a resource-limited Docker matrix.

## Opening Baseline (Before Adaptive Batching)

The opening controlled mature-merge comparison used the same frozen production selection for the pipeline-disabled and native
retain-all variants. Each variant ran in five fresh resource-limited Docker processes. All ten runs passed the selection,
maturity, row-count, secondary-index, and logical-ledger correctness gates.

In that pre-adaptive baseline, each retain-all run made one plugin call, evaluated and retained 33,353 complete trace IDs, dropped no traces, and
preserved all 147,126 span rows. Compressed logical write amplification over the core and both secondary indexes remained
identical at 1.0009 times.

Compared with the pipeline-disabled medians, the native retain-all path currently adds approximately:

- 113.5 MiB of allocated bytes, an increase of 59.6%;
- 466,941 allocations, an increase of 16.6%;
- 87.1 MiB of peak Go heap, an increase of 108.9%; and
- 85.8 MiB of peak RSS, an increase of 75.3%.

The differential allocation profile points primarily to `stageRawTrace`, `blockMetadata.copyFrom`,
`traceEvaluationStager.stage`, and `assembleStagedEvaluationBatch`. Profile values overlap and must not be added together.

The retain-all median wall and CPU times are lower than the disabled medians in the opening comparison, but this is not
evidence of a speedup. The disabled wall-time series has 9.87% coefficient of variation and the CPU profile is dominated
by variable filesystem syscalls. Physical process write-byte counters are diagnostic only because both variants produce
byte-identical logical output while those counters differ substantially.

## Invariants

Every optimization must preserve the following behavior:

1. The production picker selects the frozen input IDs in the same dispatch order.
2. All physical blocks for one trace ID are assembled into exactly one logical sampler decision.
3. Trace ordering in the merged output remains valid.
4. Timeout, panic, malformed-verdict, and plugin-error paths fail open.
5. The per-trace oversized bypass never evaluates a partial trace.
6. The fragment guard pins and validates conjunction parts before allowing a drop.
7. Core and secondary-index output remains logically correct.
8. The two-hour merge grace is unchanged.
9. The pipeline-disabled path does not perform plugin staging or projection work.
10. Existing controlled correctness gates are not weakened to obtain better performance numbers.

## Phase Boundary Criteria

Every phase has four components:

- **Scope.** What is in this phase and what is not.
- **Exit gate.** An explicit checklist that must pass before the phase closes.
- **Hard invariant.** What must NOT change as a result of this phase.
- **Dependencies.** Which phases must close before this phase can begin.

A phase does not start until its dependencies are met. A phase does not close until its exit gate passes every item.
Phases are ordered sequentially within the *Core Track*; the *Parallel Workstreams* (Phase 8 and Phase 9) run
independently and may overlap with any core phase.

A phase is considered *closed* only after re-running the relevant slice of the per-phase validation checklist below.
Closing a phase authorizes the next phase to begin; it does not authorize accepting the original optimization targets.

## Core Track

### Phase 1 — Measurement Foundation

**Scope.** Establish variance-controlled, comparable measurement for every subsequent optimization.

**Entries from original plan.** P0.

**Hard invariants.** Existing correctness gates are not weakened. Logical write-amplification (not `/proc/self/io`) is
the gate. `/proc/self/io` counters remain diagnostic only.

**Exit gate.**

- [x] CPU-set pinning in place alongside `GOMAXPROCS=2` in the canonical two-CPU, 4 GiB container.
- [x] Pre-dispatch and post-introduction pprof bases captured independently.
- [x] Environment capture (image digest, CPU set, filesystem, kernel, Go version, binary checksum, plugin checksum,
  storage device, clone method) recorded per suite.
- [x] Disabled/enabled series stability documented across ≥5 alternating runs.
- [x] Wall-time and CPU deltas are not compared until the disabled/enabled series are sufficiently stable.

**Dependencies.** None.

**Boundary rationale.** No structural optimization can be evaluated without first proving that the test harness can
detect deltas. This phase is the gate for evidence, not for performance. It measures the controlled mature merge and
the serialized integration workload already frozen by the performance-test design; capacity sweep and ingestion
throughput calibration are outside this plugin-overhead plan.

**Acceptance evidence.** The final Phase 1 suite is
`.scratch/trace-pipeline-merge-performance/phase1-phase4-final-2c4g-v1/suite.json`, with its diagram-first report at
`report.html`. It contains five serialized disabled runs and ten controlled runs alternating five disabled and five
retain-all processes. Every run used CPU set `0-1`, `GOMAXPROCS=2`, a 4 GiB cgroup memory limit, a separately pinned
controller, one shard, and complete image, binary, fixture, schedule, filesystem, device, and clone identities. All
controlled runs captured distinct pre-dispatch and post-introduction heap, allocation, block, and mutex profiles plus a
controlled CPU profile.

The disabled controlled wall/CPU coefficients of variation were 0.729%/0.782%; retain-all was 1.220%/0.592%. The five
serialized runs had 1.535% wall-time CV, 0.317% CPU CV, exact ledgers, 3,219 writes, 286 ordinary merges, and median
logical write amplification of 0.9472. The controlled mature gate is the frozen, all-mature production-picker selection,
not the unstable number of mature-containing selections produced during continuous writing.

---

### Phase 2 — Staging-Budget Correctness

**Scope.** Repair memory-safety and accounting correctness in the staging byte budget before any structural change.

**Entries from original plan.** P1.

**Hard invariants.** Aggregate staging budget and per-trace oversized budget remain logically separate. Oversized
fail-open behavior preserved.

**Exit gate.**

- [x] Metadata and structural overhead (maps, slice capacity, trace-group descriptors, verdict state) counted in
  staged bytes.
- [x] Maximum trace-count limit added per batch.
- [x] Benchmark derives staging budget from real container memory limit, not an unlimited no-op protector.
- [x] Failing staging-budget tests cover boundary, oversize, and oversized-bypass interactions.

**Dependencies.** Phase 1.

**Boundary rationale.** Pooling and arena work in later phases would hide budget bugs and break the fail-open contract
if the budget is wrong. Correctness must precede performance.

**Implemented accounting contract.** The byte budget includes retained payload capacity, raw and decoded metadata,
map buckets, staged-slice backing capacity, per-trace evaluation descriptors, exact-drop map reserve, verdict state,
and transient projected-column copies. The aggregate byte cap, per-trace oversized cap, and logical trace-count cap
are evaluated independently. A batch is flushed only at a complete trace boundary; a single oversized trace remains
fail-open and never reaches the sampler partially. The trace-count cap is 65,536 unless the byte budget implies a
smaller bound. Controlled benchmark reports record the container memory limit and all three derived staging limits,
and readiness rejects a report whose staging memory limit differs from the recorded cgroup limit.

---

### Phase 3 — Retain-All Structural Optimization

**Scope.** Eliminate the dominant retain-all allocation sources via contiguous arenas, pooled vectors, and incremental
trace grouping. This is the phase that targets the 50% allocation/bytes reduction goal.

**Entries from original plan.** P2, P3, P4.

**Hard invariants.** Deep-copy lifetime guarantee preserved. Reader may overwrite buffers; timed-out plugin may outlive
the merge call. Invalid trace ordering rejected. Fragment-guard conjunction parts pinned and validated before any drop.

**Exit gate.**

- [x] One contiguous arena per staged raw block (or bounded staging chunk) holds spans, tag values, and tag metadata.
- [x] Pooled: `blockMetadata`, staged-block slices, trace-group descriptors, `sdk.TraceBlock` vectors, trace-ID vectors,
  fragment-guard ranges, decision masks — each with capacity limits and oversized discard.
- [x] Logical trace groups built incrementally with one descriptor per trace (first/last block indexes, accumulated
  min/max ts, staged-byte estimate, fragment-guard range).
- [x] Pooled objects fully reset before reuse; race tests pass.
- [x] Original resource target disposition recorded from the final five-pair controlled series; unmet residuals are
  quantified and assigned to the projection and common-I/O phases rather than reported as successful.

**Dependencies.** Phase 1, Phase 2.

**Boundary rationale.** P2/P3/P4 attack the same hot allocation sites (`stageRawTrace`, `blockMetadata.copyFrom`,
`traceEvaluationStager.stage`, `assembleStagedEvaluationBatch`). Splitting them across phases would force re-validation
of shared lifecycles repeatedly. They close together.

**Implementation and measurement note.** Raw spans, tag values, and tag metadata now share one arena per staged raw
block. Raw metadata descriptors use a contiguous bounded vector, and logical trace groups are updated while blocks are
staged rather than rediscovered before every decision. Staged blocks, group descriptors, SDK trace blocks, trace IDs,
fragment-guard ranges, and decision masks have bounded reuse. A decision batch that times out is deliberately not reset
or returned to a pool because the timed-out plugin goroutine may still read it. Raw arena retention is bounded across the
whole cache, and raw metadata caches are bounded by object count; a per-object limit alone retained almost an entire
merge and increased heap residency. Arena storage uses BanyanDB's `pkg/bytes.Buffer`; all staging pools are registered
through `pkg/pool`, with `Discard` balancing internal-pool lifecycle accounting when a timeout or capacity bound makes an
object unsafe or unsuitable for reuse.

The focused `BenchmarkStageRawTrace` result is approximately 5,584 B/op and 8 allocs/op for the arena path versus 23,856 B/op and 125
allocs/op for the former per-value-copy shape. The complete package race run passes. A five-pair alternating controlled
run before bounding aggregate pool retention preserved every correctness ledger and reduced the retain-all allocation
count, but exposed excess post-merge heap retention. After bounding the caches, a diagnostic controlled retain-all run
used 319.3 MiB allocated, 3,277,411 allocations, 161.8 MiB peak heap, and 180.7 MiB peak RSS while preserving 147,126
rows and all ledgers. Against the opening retain-all medians, those single-run deltas are approximately -5.8%, -15.5%,
-19.6%, and -22.6%, respectively; they are diagnostic rather than an acceptance series.

The final five-pair alternating series closes the target investigation. Relative to the opening retain-all medians, the
final retain-all medians changed as follows: allocated bytes from 350,463,856 to 278,189,568 (-20.62%), allocation count
from 3,877,039 to 3,200,466 (-17.45%), peak heap from 185,520,488 to 118,567,352 (-36.09%), and peak RSS from 191,930,368
to 172,855,296 (-9.94%). Only the original 30% peak-heap target passed; the 50% allocation targets and 30% RSS target did
not pass.

This result is explicitly accepted as the Phase 3 disposition, not relabeled as target success. Exact profiles show that
the cold merge still requires timeout-safe ownership of the deep-copied trace payload, while decoder, writer, and common
merge work remains in both variants. Removing that residual requires a safe projection-ownership/decode change in Phase
7 or common merge-I/O work in Phase 8; more framework pooling would either retain too much memory or violate the timeout
lifetime invariant. Wall and CPU values are recorded but are not credited as a speedup because filesystem timing remains
an attribution risk.

---

### Phase 4 — Adaptive Budget Validation

**Scope.** Validate the existing resource-derived staging budget under cold, steady-state, and naturally varying merge
sizes. The budget is a memory-safety ceiling rather than a calibrated performance knob; this phase does not add an
operator or benchmark override.

**Entries from original plan.** P5.

**Hard invariants.** The production budget formula remains derived solely from the detected cgroup memory and CPU
limits. Logical evaluated trace count, per-trace verdict, and merged output remain correct when natural input variation
causes complete-trace batch boundaries. Both byte and trace-count limits remain enforced.

**Exit gate.**

- [x] Every sampled merge reports peak charged staging bytes, batch bytes and trace count, flush reason, and peak
  concurrent staged bytes.
- [x] Cold first-merge cost and subsequent same-process reuse are reported separately without changing the budget.
- [x] The serialized 24-hour workload validates naturally varying merge sizes under the canonical two-CPU, 4 GiB
  container and its resource-derived 256 MiB budget.
- [x] At least one naturally budget-limited merge, or explicit evidence that the production-shaped workload never
  reaches the limit, is documented without manufacturing smaller batches.
- [x] Core and secondary-index ledgers, complete-trace decisions, peak heap, peak RSS, allocations, plugin calls, CPU
  time, wall time, and logical write amplification remain reported.

**Dependencies.** Phase 3.

**Boundary rationale.** A fixed budget selected from one seed would not generalize to varying production inputs or
container sizes. Phase 3 must be stable before its resource-derived ceiling and cross-merge reuse can be validated.

**Adaptive decision-batch implementation.** The resource-derived staging and per-trace limits remain hard safety
ceilings. Each MERGE or FINALIZE selection now derives a separate preferred decision-batch limit from the selected core
parts' `CompressedSizeBytes`, `UncompressedSpanSizeBytes`, and `BlocksCount`. The estimator uses the greater of compressed
and uncompressed payload bytes plus a runtime-structure estimate per stored block. It then balances the estimated input
around a nominal reusable window derived from the hard limit and the bounded BanyanDB staging-arena pool. A two-CPU,
4 GiB process therefore keeps its 256 MiB hard ceiling while targeting approximately 32 MiB decision batches. Small
estimated merges remain a single call; larger merges are split only at complete-trace boundaries. Exact runtime charged
bytes remain authoritative, and the oversized single-trace fail-open limit is unchanged.

The fragment guard continues to receive the hard ceiling rather than the preferred decision limit. The per-decision
trace-count bound is derived from the preferred limit. Benchmark events report the metadata estimate, hard ceiling,
preferred limit, planned batch count, effective trace-count limit, actual batch sizes and reasons, and peak staged bytes.
The metadata estimate is advisory: an underestimate causes additional complete-trace batches, while the runtime byte
accounting and hard per-trace bypass preserve safety and correctness.

An initial two-CPU, 4 GiB controlled diagnostic estimated 85,716,327 bytes from the frozen selection and resolved a
28,572,109-byte preferred limit under the unchanged 268,435,456-byte hard ceiling. Runtime accounting produced five
complete-trace batches, peaked at 28,575,614 staged bytes, evaluated and retained all 33,353 traces, and preserved every
core and secondary-index ledger. Compared with the preceding five-run retain-all medians, this single diagnostic reduced
peak heap from 167.1 MiB to 106.8 MiB and peak RSS from 199.7 MiB to 151.6 MiB. It is evidence that adaptive batching
controls the live set, not a replacement for the required alternating acceptance series.

The balanced DR/RD acceptance series subsequently ran five fresh Docker processes per mode against the same selection.
All ten runs preserved 147,126 rows, all three logical ledgers, the mature-only selection, and complete-trace decisions.
The retain-all runs consistently made five sampler calls and peaked at 27.25 MiB of charged staged memory. Their median
was 266.32 MiB allocated, 3,200,439 allocations, 109.28 MiB peak heap, and 158.93 MiB peak RSS. Relative to the previous
one-call retain-all medians, this is a 12.4% allocated-byte reduction, 2.3% allocation-count reduction, 34.6% peak-heap
reduction, and 20.4% peak-RSS reduction. Correctness and live-heap control pass, but the allocated-byte,
allocation-count, and RSS targets remain open. The measured wall and CPU deltas remain non-blocking and are not credited
as a pipeline speedup.

A separate five-merge retain-all diagnostic used five identical seed clones sequentially in one two-CPU, 4 GiB process,
without changing the 256 MiB hard ceiling or the metadata-derived preferred limit. The cold merge allocated 263.10 MiB.
The four reuse merges allocated 225.93-238.53 MiB while CPU time stayed between 6.56 and 6.64 seconds. Reused arena
capacities are included in the authoritative runtime charge, so the warm merges used six complete-trace sampler calls
instead of the cold merge's five. Peak staged memory remained 27.25 MiB and every ledger remained correct. Absolute RSS
in this diagnostic includes the history of earlier merges in the same process and must not be interpreted as an isolated
per-merge RSS delta.

**Final full-day validation.** The current-commit report is
`.scratch/trace-pipeline-merge-performance/phase4-final-2c4g-v2/pilot/report.json` at commit `742c2dc8`. It replayed all
3,219 writes in serialized publication/merge order, completed 286 ordinary MERGE rounds, and then performed one cooled
FINALIZE. The primary trajectory contained input selections from 8 to 15 parts, 8 to 147,126 rows, and 5,631 to
36,948,453 compressed bytes, with 281 distinct input-byte sizes and depth ranges from `(0,0)` through `(2,4)`.

All ordinary rounds correctly bypassed sampling because each selected set contained hot data. FINALIZE selected 20
mature parts and processed 325,570 rows containing 74,576 complete traces. Ten sampler calls retained every trace, with
zero drops, oversized bypasses, lossless retries, or ledger differences. Core, `latency`, and `start_time` ledgers all
matched, and logical write amplification was 0.9639.

The final selection's metadata estimate was 190,169,855 bytes. The unchanged 4 GiB resource formula produced a
268,435,456-byte hard staging ceiling, while the adaptive planner chose a 31,694,976-byte preferred decision limit and
six planned batches. Runtime charging totaled 307,138,095 bytes across the decisions and peaked at 31,698,325 live
staged bytes. Thus natural production-shaped input did trigger adaptive byte-limit batching, but it did not approach the
hard ceiling because the lower preferred limit controlled the live set as designed. Primary wall/CPU time was
96.27/55.68 seconds with 963,196,888 allocated bytes; cooldown wall/CPU time was 15.04/15.14 seconds with 480,141,576
allocated bytes. Process cgroup peak was 296,116,224 bytes, 6.9% of the 4 GiB limit. Separate primary and cooldown CPU,
heap, allocation, block, and mutex profiles accompany the report.

---

### Phase 5 — Decision Path Overhead

**Scope.** Reduce per-batch decision overhead in the chain execution and fragment-guard paths.

**Entries from original plan.** P6, P7.

**Hard invariants.** Timeout, panic, malformed-verdict, per-link fail-open, timeout circuit breaker, and concurrent
merge safety all preserved. Conjunction snapshot and coverage catalog pinned before plugin evaluation; lazy allocation
must not weaken publication-time revalidation.

**Exit gate.**

- [x] Sampler-wrapper slice built once per batch (or per worker) rather than per batch.
- [x] Chain conjunction mask reused within bounded lifetime.
- [x] Single retain mask for single-sampler chains.
- [x] Reusable decision worker (or empirical evidence that the current goroutine/channel/timer pattern is cheaper)
  selected.
- [x] Fragment-guard ranges stored in compact integers; bounded guard-range vectors pooled; drop-specific probe state
  allocated lazily.
- [x] AlwaysKeep verification: with no fragment-guard confirmations, no throughput regression.

**Dependencies.** Phase 3.

**Boundary rationale.** Lower priority than raw staging because the opening controlled round only makes two plugin
calls. Following Phase 3 keeps the structural changes from masking any decision-path gains.

**Implementation and measurement note.** The chain now filters and stores its active samplers once, records observation
metrics without constructing per-batch wrappers, and lets a single-sampler chain copy its verdict directly into the
pooled decision mask. Healthy decisions reuse one worker and timer for the merge lifetime. A timeout abandons that worker
and its caller-owned storage rather than risking reuse while plugin code may still read it, preserving fail-open ownership
and circuit-breaker behavior. Fragment-guard trace ranges remain compact pooled integer pairs, while their block scratch
is populated lazily only for plugin-proposed drops and is retained only within the existing bounded staging-vector pool.

The focused 512-trace benchmarks reduced `BenchmarkMergeChainRunObserved` from approximately 613 ns/op, 48 B/op, and
2 allocs/op to 539-617 ns/op, 0 B/op, and 0 allocs/op. `BenchmarkMergeChainExecuteObserved` fell from approximately
2.24-2.36 us/op, 496 B/op, and 8 allocs/op to 1.80-1.87 us/op, 0 B/op, and 0 allocs/op.

Five fresh two-CPU, 4 GiB Docker runs used the frozen 15-part mature selection and AlwaysKeep plugin. Every run made five
plugin calls, evaluated and retained 33,353 traces, wrote 147,126 rows, and preserved the core and two secondary-index
ledgers. Against the preceding adaptive-batch acceptance medians, wall time changed by +2.97%, CPU time by +2.34%,
allocated bytes by -4.10%, allocation count by +0.009%, peak heap by -1.93%, and peak RSS by +3.61%. Wall-time and CPU
coefficients of variation remained below 1.6%. The full-merge result therefore shows no regression beyond the 5%
tolerance; only the focused measurements are credited as a decision-path improvement.

---

### Phase 6 — Drop-Path Optimization

**Scope.** Reduce overhead in the deterministic-drop path. **Does not begin** until retain-all overhead is reduced.

**Entries from original plan.** P9.

**Hard invariants.** Secondary-index lookup remains exact. No probabilistic structure can authorize a deletion. Core
filtering and secondary-index pruning costs reported separately.

**Exit gate.**

- [x] Duplication between per-batch mature-drop set and merge-wide dropped-ID set removed.
- [x] Each dropped trace ID stored once.
- [x] Compact exact lookup representation for secondary-index pruning evaluated and selected.
- [x] Bounded drop-set storage reused between batches.
- [x] Guard confirmation objects constructed only when plugin proposes a drop.
- [x] Sweep at 1% / 35% / 99% deletion ratios completed; fixed cost and nonlinear behavior documented.

**Dependencies.** Phase 4, Phase 5.

**Boundary rationale.** Different workload characteristic from retain-all. The original plan explicitly defers this
until retain-all overhead is controlled; honoring that deferral keeps the comparison meaningful.

**Implementation.** An effective decision mask now replaces the temporary per-batch drop map. A confirmed drop is
appended directly to one merge-wide collector, while a guard-deferred proposal is changed to retain in the same mask.
The sorted staged groups and sorted decision trace IDs are then walked together, eliminating a second lookup during core
output. The collector records each trace ID once in core merge order and is acquired lazily on the first confirmed drop.
Its ID and lookup storage is returned after SIDX publication to a 4 MiB aggregate-bounded BanyanDB internal pool; this is
a reuse bound, not a limit on a live merge. Oversized collectors remain correct and are discarded instead of pooled.

SIDX pruning lazily builds an open-addressed exact index over the collected IDs. Each slot stores a 32-bit hash
fingerprint and an ID-vector offset, but a fingerprint match is always followed by an exact byte comparison. Therefore a
collision cannot authorize deletion. Unknown or malformed encodings fail open. The predicate compares encoded bytes
without constructing a string per SIDX row. The fragment guard is still entered only for a plugin-proposed drop, and
only a confirmed guard decision reaches the collector.

**Exact-lookup evaluation.** A five-sample microbenchmark used 33,353 service-prefixed IDs and 33,353 lookups per
operation. All candidates performed zero measured allocations during lookup. Median times were:

| Deletion ratio | Go map | Sorted slice | Selected compact hash |
| ---: | ---: | ---: | ---: |
| 1% | 0.707 ms | 2.581 ms | 1.136 ms |
| 35% | 1.271 ms | 5.973 ms | 1.711 ms |
| 99% | 1.443 ms | 8.923 ms | 2.492 ms |

The Go map is the fastest isolated lookup, but it has bucket overhead and previously participated in duplicate
per-batch and merge-wide storage. The sorted vector has the smallest auxiliary representation but is 2.3-3.6 times
slower than the compact hash. The compact hash was selected because it keeps one ordered ID vector, uses packed slots,
preserves exactness, and its full-merge results below reduce both CPU and allocation cost. Evidence is in
`.scratch/trace-pipeline-merge-performance/phase6-drop-set-benchmark-v2.txt`.

**Controlled deletion sweep.** Before and after results are medians of five fresh two-CPU, 4 GiB Docker processes over
the immutable 15-part mature selection. The 1%, 35%, and 99% configurations produced 325, 11,778, and 33,032 effective
drops; the fragment guard deferred 0, 1, and 12 plugin proposals respectively. Every run matched the exact core,
`latency`, and `start_time` oracles.

| Effective deletion | Wall | CPU | Allocated bytes | Allocations | Peak heap | SIDX time |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0.974% | -1.21% | -1.96% | -4.60% | -8.40% | -2.91% | -3.78% |
| 35.313% | -0.72% | -0.61% | -4.68% | -9.64% | -1.13% | -4.30% |
| 99.038% | -2.81% | -2.11% | -7.75% | -13.48% | -7.40% | -3.43% |

Peak RSS changed by -3.17%, +2.86%, and +6.77%; unlike peak heap, this process-level high-water mark did not track the
drop-set live storage and is not credited as an improvement. One low wall-time outlier made the after 1% wall-time
coefficient of variation 8.17%, and a low baseline outlier made the before 35% coefficient 7.59%; the resource and
correctness conclusions do not depend on those timing samples. Baseline and optimized reports are in
`.scratch/trace-pipeline-merge-performance/phase6-before-v1` and
`.scratch/trace-pipeline-merge-performance/phase6-after-v1`.

The fixed component is the 294,252 SIDX predicate invocations across two indexes: removing the escaping decode string
eliminated approximately 294,000 allocations at every ratio. The nonlinear components are the collector/index size and
probe cost, which grow with confirmed drops, while core and SIDX output writes shrink as more traces are removed. No
pathological growth appeared at 99%: allocated bytes and wall/CPU time were lowest there. A final one-run diagnostic
reported core versus combined SIDX elapsed time as 2,207.5/401.7 ms at 1%, 2,138.1/374.0 ms at 35%, and
1,936.0/206.5 ms at 99%. Its reports are in
`.scratch/trace-pipeline-merge-performance/phase6-breakdown-v1`.

---

### Phase 7 — Projection Path Optimization

**Scope.** Optimize the tag/span projection path used by real SkyWalking policies. The AlwaysKeep benchmark does not
exercise this path.

**Entries from original plan.** P8.

**Hard invariants.** SDK read-only input contract preserved. Timed-out plugin goroutine cannot observe recycled memory.
Deep-copy lifetime protection retained unless an equally safe ownership mechanism replaces it.

**Exit gate.**

- [x] Copied tag values and span bodies packed into column arenas.
- [x] Aggregate block and column vectors reused.
- [x] Unique raw blocks decode only columns requested by the combined plugin projection; fragmented traces retain the
  existing full decode required to consolidate their physical blocks into one output block.
- [x] Ref-counting was not adopted. Timed-out batches discard their immutable arenas rather than resetting or pooling
  them, and a blocking-plugin test proves that their bytes remain unchanged after the merge returns.
- [x] The default SkyWalking real-policy workload shows a measurable improvement on the frozen mature fixture.

**Dependencies.** Phase 3.

**Boundary rationale.** Fundamentally different code path from the retain-all work. Separate phase keeps the diff
reviewable and prevents attribution confusion in the final integration report.

**Implementation and evidence.** `TagColumn.AtInto` lets a plugin reuse one decoded `sdk.Value`; the SkyWalking duration
and dedicated-error paths use it. The merge no longer disables raw output merely because a projection is present. A
unique physical trace block is kept in raw output form while only requested tag/span columns are decoded into an owned
per-trace `pkg/bytes.Buffer`. Aggregate trace-block, tag-column, tag-value, span, and span-ID vectors are bounded and
reused. A fragmented trace still takes the decoded consolidation path because its physical blocks must become one
ordered output block, but only projected data is copied into the decision batch. Both paths preserve the read-only SDK
contract. Normal completion resets the arenas; timeout completion discards them so a late plugin cannot observe recycled
memory.

The focused five-run SDK comparison reduced the median SkyWalking duration-rule cost from 247.5 to 154.4 ns/trace
(-37.6%) and the dedicated-error cost from 126.6 to 88.86 ns/trace (-29.8%). The Zipkin duration path fell from 240.5 to
154.6 ns/trace (-35.7%); its in-array error path does not use `AtInto` and remained statistically unchanged.

The 24-hour accelerated default-policy check used the same two-CPU, 4 GiB container, full two-times fixture, default
SkyWalking configuration, and frozen ledgers before and after the implementation. Both runs evaluated 74,576 complete
traces, dropped exactly 26,380, retained 48,196, reproduced verdict SHA
`e1be6f66e676dc95d33223585f59a648da7556f6c98af1f13801978fd2b19107`, and preserved all core and secondary-index
ledgers at a 35.3733104484% deletion ratio. For the sampling cooldown, wall time changed from 14.862 to 4.828 seconds
(-67.5%), CPU from 15.116 to 4.664 seconds (-69.1%), allocated bytes from 1,022,124,168 to 636,793,768 (-37.7%), and
allocations from 16,046,788 to 8,254,193 (-48.6%). Physical write bytes fell 96.5% primarily because Phase 8 removed
pathological common writer flushes; that reduction is not credited to projection work. The single-run cgroup peak rose
7.5% (240,168,960 to 258,064,384 bytes), so Phase 10 must remeasure resident memory as a multi-run integration series
rather than treating this one run as a memory improvement. Evidence is under
`.scratch/trace-pipeline-merge-performance/phase789-before-skywalking` and `phase789-final-skywalking`.

A second acceptance series exercised one frozen mature production-picker selection in ten fresh Docker processes,
strictly alternating five base and five optimized runs. Every run evaluated 33,353 complete traces, dropped 11,598
after exact fragment-guard confirmation, and reproduced all three expected ledgers. The base and optimized coefficients
of variation were 0.80%/0.78% for wall time and 0.75%/1.07% for CPU. Median wall time changed from 6.304 to 2.598 seconds
(-58.8%), CPU from 6.551 to 2.638 seconds (-59.7%), allocated bytes from 524,554,048 to 343,743,000 (-34.5%), and
allocations from 7,279,373 to 3,720,275 (-48.9%). Peak RSS rose 7.4%, within the 10% controlled tolerance; peak Go heap
rose 22.1% and is carried into Phase 10 rather than hidden. The combined timing includes Phase 8 common-I/O gains, while
the SDK microbench and allocation profile isolate the projection contribution. Alternating evidence is under
`.scratch/trace-pipeline-merge-performance/phase7-controlled-alternating-v1`.

---

## Parallel Workstreams

### Phase 8 — Common Merge I/O Investigation

**Scope.** Investigate filesystem-level merge I/O (`Fadvise` frequency and granularity, small sequential writes, writer
buffering and flush size, atomic-file synchronization, secondary-index writer behavior) as an ordinary merge
investigation.

**Entries from original plan.** P10.

**Hard invariants.** Pipeline-disabled baseline must improve or be preserved. Any gain here is **not** credited as a
plugin-framework optimization.

**Exit gate.**

- [x] Each axis profiled with before/after evidence.
- [x] Disabled-mode baseline correctness and resource regression gates pass within the 10% median tolerance.

**Dependencies.** None (parallel workstream).

**Boundary rationale.** Independent of the plugin framework. Runs as a workstream that may overlap with any core
phase. The "not credited as plugin optimization" invariant prevents scope-creep across phases.

**Implementation and evidence.** The uncached sequential writer now buffers small writes and performs its durability
sync plus cache-drop at close instead of flushing and advising the kernel after every `Write`. Sequential readers retain
per-read cache advice because batching it produced a repeatable resident-memory regression; they also advise the whole
file at close. Read-only files no longer issue an fsync on `Close`, and a file already synchronized by its sequential
writer is not synchronized a second time. Trace and SIDX block writers now close both the sequential wrapper and the
retained base file, making descriptor ownership explicit. The core and SIDX atomic publication barriers remain
unchanged.

Five fresh disabled-mode runs before and after the change used the identical selection SHA
`45aa16460d7ca36fc0ddaa1bc1d2e73f45109dbaab86328012798d5011e60b7b`, 147,126 rows, byte-identical core/latency/
start-time ledgers, and the two-CPU, 4 GiB container:

| Metric | Before median | After median | Change | After CV |
| --- | ---: | ---: | ---: | ---: |
| Wall time | 7.312 s | 2.432 s | -66.7% | 0.53% |
| CPU time | 7.340 s | 2.384 s | -67.5% | 0.48% |
| Allocated bytes | 203,043,568 | 195,628,368 | -3.7% | 1.43% |
| Allocations | 2,811,223 | 2,798,380 | -0.5% | 0.005% |
| Physical write bytes | 958,722,048 | 40,251,392 | -95.8% | 0.0% |
| Peak Go heap | 83,148,744 | 79,303,784 | -4.6% | 24.00% |
| Peak RSS | 125,186,048 | 132,263,936 | +5.7% | 16.70% |

The peak series contains one Go-GC outlier, but both medians remain within the fixed 10% regression tolerance; end RSS
also fell 4.4%. An earlier 256 KiB read-advice batching attempt produced a +27.8% peak-RSS median and was rejected. The
accepted implementation restores per-read advice, trading some syscall reduction for bounded residency.

Syscall evidence isolates the accepted mechanism: `fadvise64` calls fell from 1,135,840 to 635,093 (-44.1%), writes from
500,772 to 8,960 (-98.2%), and fsync calls from 1,356 to 15 (-98.9%), while the 49 `fdatasync` calls and six publication
renames were unchanged. The remaining advice calls are the intentionally retained reader behavior. Before profiles
attributed 61–69% cumulative CPU to `seqWriter.Write`, 42–44% to `Fadvise`, and 27–31% to `Flush`; the accepted five-run
series still cuts CPU 67.5%. The two SIDX children retained approximately the same absolute time, so no SIDX-specific
storage rewrite was justified. Evidence is under `.scratch/trace-pipeline-merge-performance/phase8-disabled-final-v5`
and `phase8-strace-final-v3`.

---

### Phase 9 — Plugin-Local Benchmarks

**Scope.** Benchmark real plugin logic outside the merge framework so plugin-local costs are not confused with merge
I/O or framework staging.

**Entries from original plan.** P11.

**Hard invariants.** Plugins are measured against representative complete-trace batches from the mature seed. Plugin-local
fixes are kept separate from framework fixes.

**Exit gate.**

- [x] Independent measurements for projected tag/span decoding, complete-trace latency calculation, error and status
  rules, trace-ID hashing, regex/tag rules, verdict allocation, and decision throughput across representative batch
  sizes use complete traces from the frozen mature seed.
- [x] Each plugin-local cost attributed to a specific function call site.

**Dependencies.** None (parallel workstream).

**Boundary rationale.** Outside the merge framework; can run any time after the mature seed is frozen. Parallel
workstream.

**Per-axis measurements.** All figures are ns per logical trace from
`plugins/skywalking/internal/tracesampler` (`BenchmarkDecide*`), median of 3 runs at `-benchtime 2000x`, taken after
the escape-free decode work landed. Both first-party schemas are reported because they exercise different paths: the
segment schema reads a dedicated `is_error` column, while Zipkin has none and detects errors inside the flattened tag
array.

| Axis | Benchmark | sw | zipkin |
| --- | --- | ---: | ---: |
| Projected tag decoding, 2 / 8 / 32 entries | `_ArrayEntries` (unescaped) | 70 / 149 / 496 | 67 / 147 / 491 |
| Same, escaped values | `_ArrayEntries` (escaped) | 157 / 377 / 1261 | 159 / 380 / 1265 |
| Complete-trace latency envelope | `durationOnly` | 238 | 241 |
| Same, per row count 1 / 4 / 16 / 64 | `_RowCount` | 342 / 1176 / 4405 / 17333 | 494 / 1718 / 6632 / 25917 |
| Error rule, dedicated column vs in-array | `errorsOnly` | 123 | 382 |
| Trace-ID hashing | `sampleOnly` | 17 | 15 |
| Tag rules: equals / regex / 5xx regex | `_RegexRule` | 373 / 384 / 387 | 376 / 386 / 385 |
| Tag rules by match position 0 / 16 / 31 | `_TagMatchPosition` | 971 / 1038 / 1106 | 1034 / 1194 / 1327 |
| Verdict allocation, 1 / 16 / 64 / 256 traces | `_KeepSliceAllocation` | 204 / 135 / 135 / 127 | 517 / 384 / 381 / 384 |
| Decision throughput, 1 / 16 / 64 / 128 / 256 | `_BatchSize` | 1136 / 922 / 902 / 897 / 905 | 1655 / 1327 / 1315 / 1325 / 1332 |

Two shape facts fall out. Verdict allocation is one `allocs/op` at every batch size — the `make([]bool, len(batch.Traces))`
in `Decide` — with `B/op` equal to the trace count, so it is already at its floor and per-trace cost only falls as the
batch amortizes it. Decision throughput is flat from 16 traces upward; the `traces=1` column is that same fixed cost
divided by one, not a batching penalty.

**Cost attribution to call sites.** From `-cpuprofile` at `-benchtime 30000x`, one profile per axis. Percentages are
flat unless marked cumulative.

| Axis | Dominant call site | Share |
| --- | --- | ---: |
| Latency envelope | `runtime.duffcopy` under `sdk.(*TagColumn).At` (`hasSlowTrace` 91.8% cum) | 57.1% |
| Error rule, dedicated column | `runtime.duffcopy` under `sdk.(*TagColumn).At` (`At` 51.9% cum) | 44.4% |
| Error rule, in-array | `indexbytebody` (`arrayEntries` 55.3% cum, `matchEntries` 39.5% cum) | 39.5% |
| Tag rules | `indexbytebody` (`arrayEntries` 66.7% cum, `matchEntries` 21.3% cum) | 40.0% |
| Trace-ID hashing | `hash/fnv.(*sum64a).Write` | 60.0% |
| Verdict allocation | `make([]bool, …)` in `Decide` | 1 alloc/op |

The `duffcopy` entries are one finding, not two: `sdk.Value` is **112 bytes** and `TagColumn.At` returns it by value, so
every per-row column read on the duration and dedicated-error paths copies 112 bytes. That copy — not the decode — is
the majority cost of both, and it is an SDK-side shape, so fixing it (an `AtInto(*Value)`, or accessors that avoid
materializing the struct) is a framework change rather than a plugin-local one and belongs outside this workstream.

On the array paths the remaining cost is the delimiter scan itself plus the string comparisons in `matchEntries`; the
decode overhead that used to dominate here was removed by the escape-free path (`tagRulesOnly` 68.4 → 24.4 µs per
3000-trace batch, -64%).

**Why the remaining gap is not closable inside the plugin.** Go's `internal` rule makes the two halves mutually
unreachable: `plugins/skywalking/internal/tracesampler` is importable only from `plugins/skywalking/**`, and the fixture
that holds the seed (`banyand/internal/benchmark/tracefixture`, plus the catalog parser in `.../sourcecatalog`) only
from `banyand/**`. The framework side sidesteps this because `EvaluateSampler` takes the sampler as an `sdk.Sampler`
and reads `pluginPath` only for a checksum — it never imports the plugin. A plugin-local benchmark has no such escape.
Closing this therefore needs either a committed golden fixture under the plugin's `testdata/` (exported once from the
real capture, at the cost of drifting from the seed) or making the loader reachable outside `banyand/internal` (a
framework change). Both are out of scope for the plugin-local workstream, so the item stays open deliberately rather
than being closed on synthetic data.

**Mature-seed benchmark.** `SamplerBatchBuilder` materializes complete logical traces in schedule order with only the
requested projection. The opt-in benchmark reads the immutable mature source, frozen catalog/schedule, expected sampler
artifact, and real plugin. Its preflight rejects malformed verdict lengths, nondeterministic decisions, input mutation,
missing source traces, cancellation, and default-policy verdict drift. The test reproduces 74,576 evaluated traces,
26,380 drops, and the authoritative 35.3733104484% ratio before any timing begins.

The following values are medians of five runs at three full-fixture decisions per run on two CPUs. The normal policy
cases use 512-trace complete batches; bytes and allocations are the verdict slices only (74,592 B/op and 146 allocs/op).

| Mature-seed axis | Median ns/trace |
| --- | ---: |
| Complete-trace duration envelope | 305.2 |
| Dedicated error column | 169.1 |
| Trace-ID hash/sample | 40.24 |
| Tag equals | 196.5 |
| Tag regex | 240.8 |
| HTTP status regex | 254.2 |
| Default SkyWalking policy | 473.4 |
| Projected span-body traversal | 17.12 |
| Projected span-ID traversal | 13.64 |

Batch-size evidence separates policy work from verdict allocation:

| Complete traces/batch | 1 | 16 | 64 | 256 | 512 | 4,096 | 8,192 | 16,384 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Default policy, ns/trace | 531.9 | 508.1 | 499.8 | 493.9 | 482.9 | 500.2 | 483.0 | 462.9 |
| Verdict-only, ns/trace | 20.52 | 6.261 | 5.614 | 5.314 | 5.419 | 5.195 | 5.252 | 5.158 |
| Verdict allocations/op | 74,576 | 4,661 | 1,166 | 292 | 146 | 19 | 10 | 5 |

The verdict buffer remains one boolean per trace and one allocation per batch; there is no hidden per-trace allocation.
SkyWalking's default sampler deliberately requests no span bodies, so there is no honest plugin-local span parser to
time. The two span rows above build the real seed with `Spans` or `SpanIDs` projection and measure traversal of every
projected value; raw-column decode and arena ownership are measured at the Phase 7 merge boundary. This separation avoids
inventing a non-production parser merely to populate a benchmark cell.
The default-policy CPU profile attributes the real-seed work to `Sampler.hasSlowTrace`, `TagColumn.AtInto`,
`DecodeTagValueInto`, `Sampler.hasErrorColumn`, and `hash/fnv.(*sum64a).Write`, matching the synthetic call-site study.
Raw results, medians, and profiles are under
`.scratch/trace-pipeline-merge-performance/phase9-seed-benchmark`.

**Follow-up optimization disposition.** The plugin-local optimization follow-up is complete. It measured and rejected
two additional rewrites: replacing the existing per-entry SIMD-assisted `IndexByte` calls with one scalar scan regressed
the representative paths by 47-73%, while changing tag matching from rules-outer to entries-outer regressed them by
28-34%. Refining the latter with precomputed rule operands did not recover the lost inlining. Both experiments were
reverted, leaving the measured faster implementation unchanged. No plugin optimization or plugin branch remains to be
integrated before Phase 10.


---

### Phase 10 — Final Integration

**Scope.** Run the serialized 24-hour SkyWalking workload with default settings and demonstrate that every acceptance
gate is met.

**Entries from original plan.** P12.

**Hard invariants.** All gates from the original plan (selection, maturity, whole-trace, ledger) unchanged. Two-hour
merge grace unchanged. No unfinished queued, running, or in-flight merge work.

**Plugin monitoring.** The Phase 10 merge observer mirrors the production execution catalog into every merge event and
the aggregate run report. Chain batch result/count/trace totals are recorded alongside the link measurements. Each
configured link is keyed by its stable `plugin_name`; calls, total `Decide` wall time, maximum call time, result, and
fail-open reason are never combined across plugins. The standalone HTML report plots total and maximum execution time per
plugin and lists chain and link result counts. These values are reconciled with the existing
`pluginCalls` and `tracesEvaluated` counters so the benchmark cannot silently omit an executed link. Production Prometheus
metrics remain the deployment view; the benchmark mirror provides the same attribution without adding a metrics scraper
to the resource-limited container.

**Exit gate.**

- [x] Nonzero plugin calls and evaluated traces.
- [x] Independently validated mature-trace deletion ratio near 35%.
- [x] Correct core and secondary-index ledgers.
- [x] Grace bypass for hot selections.
- [x] No OOM, timeout, panic, malformed verdict, or unexpected lossless retry.
- [x] Every configured plugin has a nonempty name, only `success` executions, no link bypass, and execution calls that
      reconcile exactly with the merge observer's plugin-call total.
- [x] Chain batch count/trace totals plus per-plugin total, mean, and maximum execution time and time per evaluated trace
      are present in JSON and HTML.
- [x] No regression in pipeline-disabled baseline.
- [x] All proposed targets met (or explicitly accepted with quantified impact).

**Dependencies.** Phases 4, 6, 7, 8, and 9; all are closed.

**Boundary rationale.** Final acceptance gate. Only the targets frozen here become blocking.

**Final integration result.** The paired matrix ran five pipeline-disabled and five default-SkyWalking repetitions in
fresh Docker processes. The data node was pinned to two CPUs and limited to 4 GiB memory, no additional swap, 512 PIDs,
and `GOMAXPROCS=2`. Every run used one shard, the same 325,570-row, 3,219-write fixture spread over one logical day, a
two-hour merge grace period, and the same publication schedule. The fixture SHA-256 was
`8c9289bed26d7696a44b4937c5670b2709707b03904cf9de3241d279d4081438`; the schedule SHA-256 was
`f7b651db0fe965362696139d19bc9ec452269868bba236e3e3b5615830652bcd`.

All ten runs passed their row, core-ledger, secondary-index-ledger, sampling, and plugin-observability gates and reached
an idle boundary. The 24-hour primary phase produced the same 286 merge rounds and no sampler calls in either mode
because the selected data remained inside the two-hour grace window. Each SkyWalking run then performed one sampled
finalize merge. It made 12 calls, evaluated 74,576 complete traces, retained 48,196, and dropped 26,380. The resulting
35.3733% deletion ratio matched the independent verdict oracle. Core, latency-index, and start-time-index output each
reconciled to 89,157 rows. No run reported a timeout, circuit-open result, plugin bypass, oversized-trace bypass, lossless
retry, malformed verdict, panic, or OOM.

The like-for-like primary phase measures the common merge path before the intentional sampled finalize work. Medians are
over five fresh processes.

| Primary metric | Pipeline disabled | SkyWalking | Delta | Disposition |
| --- | ---: | ---: | ---: | --- |
| Wall time | 77.201 s | 79.048 s | +2.39% | Accepted |
| CPU time | 28.974 s | 28.371 s | -2.08% | No regression claimed |
| Allocated bytes | 975.6 MB | 967.5 MB | -0.83% | No regression claimed |
| Allocation count | 18,115,242 | 18,115,605 | +0.002% | Passed |
| Cgroup memory peak | 160.5 MiB | 160.6 MiB | +0.06% | Passed |
| Logical write amplification | 0.947236 | 0.947236 | 0.00% | Passed |

The end-to-end comparison includes the extra sampled finalize merge and therefore quantifies both its cost and its
storage benefit rather than treating that work as an ordinary-path regression.

| End-to-end metric | Pipeline disabled | SkyWalking | Delta |
| --- | ---: | ---: | ---: |
| Wall time | 77.403 s | 83.768 s | +8.22% |
| CPU time | 28.976 s | 32.828 s | +13.30% |
| Allocated bytes | 932.8 MiB | 1,500.6 MiB | +60.88% |
| Cgroup memory peak | 160.5 MiB | 242.2 MiB | +50.91% |
| Active merge time | 50.116 s | 52.789 s | +5.33% |
| Final core plus secondary-index bytes | 85.13 MiB | 40.18 MiB | -52.80% |
| Logical write amplification | 0.947236 | 0.801949 | -15.34% |

The plugin itself accounted for a median 16.980 ms of `Decide` wall time per SkyWalking run, 227.7 ns per evaluated
trace, with a median maximum call of 3.514 ms. Across all five runs, the exact histogram reconciled all 60 calls: one at
or below 0.25 ms, four at or below 0.5 ms, 25 at or below 1 ms, 25 at or below 2.5 ms, and five at or below 5 ms. There
were no overflows. Most sampled-finalize cost therefore belongs to trace reconstruction, filtering, and rewriting rather
than plugin execution.

The paired suites, raw merge events, profiles, and standalone comparison report are under
`.scratch/trace-pipeline-merge-performance/phase10-paired-v2`. This closes Phase 10 without adding a production budget or
configuration flag.

---

## Phase Dependency Graph

```
Phase 1: Measurement Foundation
        │
Phase 2: Staging-Budget Correctness
        │
Phase 3: Retain-All Structural Optimization
        │
   ┌────┴────┐
Phase 4   Phase 5
(Batch)   (Decision Path)
   │         │
   │         │
Phase 6   Phase 7
(Drop)    (Projection)
   │         │
   └────┬────┘
        │
Phase 10: Final Integration

(parallel at all times)
Phase 8: Common Merge I/O
Phase 9: Plugin-Local Benchmarks
```

## Original Target Disposition

The first targets were hypotheses about how much of the opening retain-all cost was removable framework allocation. The
final controlled evidence preserves them as historical targets instead of silently reweighting them after measurement.

| Metric | Opening retain-all median | Final retain-all median | Change | Original target | Disposition |
| --- | ---: | ---: | ---: | ---: | --- |
| Allocated bytes | 350,463,856 | 278,189,568 | -20.62% | -50% | Not met; projection/common-I/O residual |
| Allocation count | 3,877,039 | 3,200,466 | -17.45% | -50% | Not met; projection/common-I/O residual |
| Peak Go heap | 185,520,488 | 118,567,352 | -36.09% | -30% | Met |
| Peak RSS | 191,930,368 | 172,855,296 | -9.94% | -30% | Not met; later-phase residual |
| Logical correctness | Lossless | Lossless | No change | No change | Met |

Phase 3 and Phase 4 close with this quantified acceptance. This does not waive final integration acceptance: Phase 10
must report the residual against the then-current disabled baseline and either meet the final product budget or explicitly
accept its measured impact. Wall-time and CPU deltas remain non-blocking and are never interpreted as a pipeline speedup
solely because the retain-all median is lower.

## Validation Checklists (per phase)

Each phase applies the relevant subset of the items below. Do not close a phase whose applicable items have not all
passed.

1. Unit tests for buffer ownership, reset, pooling, trace grouping, ordering, and budget boundaries.
2. Timeout, panic, malformed-verdict, and plugin-error fail-open tests.
3. Oversized single-trace and multi-block trace tests.
4. Fragmented trace tests spanning selected and conjunction parts.
5. Raw and projected merge-path tests.
6. Race-enabled tests for shared pools, plugin instances, and decision workers.
7. The exact frozen controlled selection in both pipeline-disabled and native AlwaysKeep modes.
8. At least five fresh Docker processes per variant in alternating order.
9. Exact core and secondary-index ledger reconciliation.
10. Updated CPU, allocation, heap, block, and mutex profiles for one representative process per variant.

Do not proceed to a phase whose dependencies are still open. Do not proceed to deterministic dropping if AlwaysKeep
output is not identical to the disabled output or if the current framework overhead has not been either reduced or
explicitly accepted with quantified impact.

## Mapping Back to Original P0–P12

| Original | Phase | Reason |
| --- | --- | --- |
| P0 | Phase 1 | Measurement foundation precedes everything. |
| P1 | Phase 2 | Memory-safety correction before any structural work. |
| P2 | Phase 3 | Combined with P3/P4; same hot allocation sites. |
| P3 | Phase 3 | Combined with P2/P4. |
| P4 | Phase 3 | Combined with P2/P3. |
| P5 | Phase 4 | Empirical tuning after structural changes. |
| P6 | Phase 5 | Combined with P7; lower priority than raw staging. |
| P7 | Phase 5 | Combined with P6. |
| P8 | Phase 7 | Different code path; separate phase. |
| P9 | Phase 6 | Deferred until retain-all is tamed; different workload. |
| P10 | Phase 8 | Independent workstream; not plugin-specific. |
| P11 | Phase 9 | Independent workstream; outside merge framework. |
| P12 | Phase 10 | Final integration. |

## Rejected Shortcuts

The following approaches are outside this plan:

- special-casing AlwaysKeep so it bypasses the pipeline;
- removing timeout-safe copies without a replacement lifetime mechanism;
- writing retained output before `Decide` and attempting to roll it back;
- making a two-pass merge the default without proving the I/O tradeoff;
- reducing the two-hour merge grace to manufacture plugin calls;
- relaxing the selection, maturity, whole-trace, or ledger correctness gates;
- using probabilistic membership to authorize deletion; and
- treating the current lower retain-all wall time as a framework speedup.
