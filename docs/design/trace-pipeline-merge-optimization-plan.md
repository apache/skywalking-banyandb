# Trace Pipeline Merge Optimization Plan

## Status

Proposed for review. Do not treat the targets in this document as frozen acceptance gates until they are approved.

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

- [ ] CPU-set pinning in place alongside `GOMAXPROCS=4`.
- [ ] Pre-dispatch and post-introduction pprof bases captured independently.
- [ ] Environment capture (image digest, CPU set, filesystem, kernel, Go version, binary checksum, plugin checksum,
  storage device, clone method) recorded per suite.
- [ ] Disabled/enabled series stability documented across ≥5 alternating runs.
- [ ] Wall-time and CPU deltas are not compared until the disabled/enabled series are sufficiently stable.

**Dependencies.** None.

**Boundary rationale.** No structural optimization can be evaluated without first proving that the test harness can
detect deltas. This phase is the gate for evidence, not for performance. It measures the controlled mature merge and
the serialized integration workload already frozen by the performance-test design; capacity sweep and ingestion
throughput calibration are outside this plugin-overhead plan.

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
- [ ] Controlled re-run demonstrates ≥50% reduction in allocated bytes and allocation count, ≥30% reduction in peak heap
  and peak RSS.

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

The remaining target is therefore open. The profiles show that the first fresh-process batch must still allocate the
deep-copied payload, while the default 512 MiB staging budget lets this seed complete in one decision batch and gives the
pools no within-run reuse. Phase 4's batch-size sweep is required to determine whether bounded chunk reuse can meet the
allocation and peak-memory targets. Until that sweep completes, Phase 3 is structurally complete but not closed as a
performance gate.

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
- [ ] The serialized 24-hour workload validates naturally varying merge sizes under the canonical two-CPU, 4 GiB
  container and its resource-derived 256 MiB budget.
- [ ] At least one naturally budget-limited merge, or explicit evidence that the production-shaped workload never
  reaches the limit, is documented without manufacturing smaller batches.
- [ ] Core and secondary-index ledgers, complete-trace decisions, peak heap, peak RSS, allocations, plugin calls, CPU
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

- [ ] Duplication between per-batch mature-drop set and merge-wide dropped-ID set removed.
- [ ] Each dropped trace ID stored once.
- [ ] Compact exact lookup representation for secondary-index pruning evaluated and selected.
- [ ] Bounded drop-set storage reused between batches.
- [ ] Guard confirmation objects constructed only when plugin proposes a drop.
- [ ] Sweep at 1% / 35% / 99% deletion ratios completed; fixed cost and nonlinear behavior documented.

**Dependencies.** Phase 4, Phase 5.

**Boundary rationale.** Different workload characteristic from retain-all. The original plan explicitly defers this
until retain-all overhead is controlled; honoring that deferral keeps the comparison meaningful.

---

### Phase 7 — Projection Path Optimization

**Scope.** Optimize the tag/span projection path used by real SkyWalking policies. The AlwaysKeep benchmark does not
exercise this path.

**Entries from original plan.** P8.

**Hard invariants.** SDK read-only input contract preserved. Timed-out plugin goroutine cannot observe recycled memory.
Deep-copy lifetime protection retained unless an equally safe ownership mechanism replaces it.

**Exit gate.**

- [ ] Copied tag values and span bodies packed into column arenas.
- [ ] Aggregate block and column vectors reused.
- [ ] Columns decoded only when requested by the combined plugin projection.
- [ ] Ref-counted immutable batch buffers (if adopted) demonstrated safe with timed-out plugins.
- [ ] Real-policy benchmark (e.g. error/status rules + regex/tag rules) shows measurable improvement.

**Dependencies.** Phase 3.

**Boundary rationale.** Fundamentally different code path from the retain-all work. Separate phase keeps the diff
reviewable and prevents attribution confusion in the final integration report.

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

- [ ] Each axis profiled with before/after evidence.
- [ ] Disabled-mode baseline regression test passing within tolerance.

**Dependencies.** None (parallel workstream).

**Boundary rationale.** Independent of the plugin framework. Runs as a workstream that may overlap with any core
phase. The "not credited as plugin optimization" invariant prevents scope-creep across phases.

---

### Phase 9 — Plugin-Local Benchmarks

**Scope.** Benchmark real plugin logic outside the merge framework so plugin-local costs are not confused with merge
I/O or framework staging.

**Entries from original plan.** P11.

**Hard invariants.** Plugins are measured against representative complete-trace batches from the mature seed. Plugin-local
fixes are kept separate from framework fixes.

**Exit gate.**

- [ ] Independent measurements for projected tag/span decoding, complete-trace latency calculation, error and status
  rules, trace-ID hashing, regex/tag rules, verdict allocation, decision throughput across representative batch sizes.
  *Every axis is measured (table below), but on SYNTHETIC batches, not the mature seed — see "Remaining gap".*
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

**Remaining gap (why this phase is not closed).** The hard invariant requires plugins to be measured "against
representative complete-trace batches from the mature seed". These benchmarks build their batches synthetically in
`benchBatch`/`benchEntries` — realistic in SHAPE (production tag keys, both first-party configs, complete traces) but
not drawn from the frozen selection. The machinery to close this exists — `tracefixture.EvaluateSampler` already drives
a sampler over complete generated traces via `buildSamplerBlock` — so the work is to add a seed-backed benchmark
alongside the synthetic ones, not to build a fixture. Until then the first exit-gate item stays open and no
production-ratio claim should be made from the numbers above.

---

### Phase 10 — Final Integration

**Scope.** Run the serialized 24-hour SkyWalking workload with default settings and demonstrate that every acceptance
gate is met.

**Entries from original plan.** P12.

**Hard invariants.** All gates from the original plan (selection, maturity, whole-trace, ledger) unchanged. Two-hour
merge grace unchanged. No unfinished queued, running, or in-flight merge work.

**Exit gate.**

- [ ] Nonzero plugin calls and evaluated traces.
- [ ] Independently validated mature-trace deletion ratio near 35%.
- [ ] Correct core and secondary-index ledgers.
- [ ] Grace bypass for hot selections.
- [ ] No OOM, timeout, panic, malformed verdict, or unexpected lossless retry.
- [ ] No regression in pipeline-disabled baseline.
- [ ] All proposed targets met (or explicitly accepted with quantified impact).

**Dependencies.** Phase 4, Phase 6, Phase 7 (and Phase 8 / Phase 9 if those axes feed into the final run).

**Boundary rationale.** Final acceptance gate. Only the targets frozen here become blocking.

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

## Targets Becoming Blocking

These targets are proposals for review and are not yet frozen gates.

| Metric | Current extra cost | Proposed first target |
| --- | ---: | --- |
| Allocated bytes | 121.9 MiB | Reduce by at least 50% |
| Allocation count | 1.07 million | Reduce by at least 50% |
| Peak Go heap | 91.5 MiB | Reduce by at least 30% |
| Peak RSS | 98.4 MiB | Reduce by at least 30% |
| Pipeline-disabled performance | Baseline | No regression greater than 5% |
| Logical correctness | Lossless | No change allowed |

Until Phase 1 closes, no target is blocking. After Phase 4, the 50% allocation reduction and 30% heap/RSS reduction
become blocking. After Phase 6, the deletion ratio near 35% becomes blocking. Wall-time and CPU targets remain
**non-blocking** until CPU pinning and repeated runs reduce environmental variance below the current 9.87% CV.

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
