# Trace Merge Drop-Set Bounding

## Status

This document specifies a bound on the merge-wide dropped-trace-ID collector introduced by Phase 6 of the
[merge optimization plan](trace-pipeline-merge-optimization-plan.md), which was unbounded in the number of dropped traces
per merge.

Implemented, except the validation. The mechanism (DS-1, DS-1b), the budget resolver (DS-2), the ceiling seam (DS-3),
observability (DS-4), activation (DS-5), and the production instruments of section 6 are in the tree. The ceiling is
therefore **live** on every merge.

The workload validation has **not** run. It rides the Phase 10 SkyWalking acceptance workload as two runs: one at
default settings, which measures how much headroom real traffic has against the ceiling, and one at
`--allowed-bytes=256MiB`, which lowers the ceiling enough for real traffic to reach it and so is the only run that
exercises the capped path. Until both produce reports the ceiling is active with no workload-level evidence behind it,
and the memory bound rests on unit-level residency checks. See the execution plan for the gate and for why it replaced
the containerised controlled run.

**The bound is applied to the sampling decision, not to the pruning predicate.** When a merge reaches its drop-set
ceiling it stops dropping traces and retains the remainder. Secondary-index pruning therefore stays exact for every drop
that is performed, the secondary index remains exactly the set of retained traces, and no query-layer change is
required. Phase 6's hard invariant — *"Secondary-index lookup remains exact. No probabilistic structure can authorize a
deletion."* — is preserved verbatim.

**Every merge is treated identically.** One budget function, one constant, no lane parameter. A finalize round is one
more merge: same ceiling, one round, no continuation mechanism, no watermark, and no change to the finalize round
machinery, its persisted state, or its scheduling. `banyand/trace/finalize_state.go` is not touched;
`banyand/trace/finalize_scanner.go` gains telemetry only, and its decisions are unchanged (section 7).

The accepted cost is bounded deletion: a capped finalize round leaves traces undeleted, and they are only revisited by
the existing re-round path, which is capped at eight rounds per shard for its lifetime. Section 5 states the numbers.
Finalize I/O does not change, because the round count does not change.

Four features that would each lift part of this limit are specified and deferred in section 9: bloom-filter pruning,
continuation rounds, the sampling watermark, and a per-lane finalize ceiling. None is a prerequisite; each is an
independent later choice, and section 9 records what must be true before it can be taken.

## 1. Problem

`droppedTraceIDs` (`banyand/trace/drop_set.go`) holds every trace ID the core merge dropped, for the whole duration of
one merge, so that each sibling secondary-index merge can prune the same IDs (`banyand/trace/merger.go:843-856`):

- `ids []string` — ascending, deduplicated, one entry per dropped trace.
- `slots []uint64` — an open-addressed index built lazily on first lookup, sized to the next power of two at or above
  `2N`, so between `2N` and `4N` slots of eight bytes.

It is released only after every secondary-index merge has published (`banyand/trace/merger.go:826`).
`maxPooledDroppedTraceIDBytes = 4 MiB` (`banyand/trace/drop_set.go:26`) bounds only what is returned to the reuse pool;
it is not a limit on a live merge, as Phase 6 already recorded.

Nothing bounds `N`. The pipeline's other working sets are budgeted — `stageBudgetFromLimit` gives each merge
`memLimit/(4 × CPUs)` for staged bytes so the aggregate stays near `memLimit/4`, plus a per-trace budget and a
trace-count cap (`banyand/trace/merger.go:194-243`). The drop set sits outside that accounting: staging is bounded per
batch and reset at every flush, while the drop set accumulates across all batches of the merge.

### 1.1 Measured residency

Measured on the real structure with 42-byte service-prefixed IDs, each body allocated per entry as
`blockMetadata.unmarshal` does (`banyand/trace/block_metadata.go:170`). Reserved heap is `HeapSys` — what the container
sees.

| Dropped traces | Live heap | `ids` | `slots` | ID bodies | Live before index | Reserved heap |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 7.6 MiB | 1.8 | 2.0 | 3.8 | 5.6 MiB | 19 MiB |
| 1,000,000 | 71.1 MiB | 16.9 | 16.0 | 38.1 | 55.1 MiB | 163 MiB |
| 4,000,000 | 281.3 MiB | 64.7 | 64.0 | 152.6 | 217.3 MiB | 615 MiB |
| 16,000,000 | 1,113.2 MiB | 246.8 | 256.0 | 610.4 | 857.2 MiB | 2,270 MiB |

Roughly 73 to 80 bytes per entry live. Reserved heap is close to twice live heap because `ids` grows by append doubling
and `slots` is a single contiguous allocation — 256 MB at sixteen million entries. Reserved heap, not live heap,
terminates a container.

### 1.2 Trigger

The exposure is the ordinary finalize path, not a rare large hot merge:

- `runFinalizeRound` selects **every cooled part in the shard** into **one** merge
  (`banyand/trace/finalizer.go:92-119`). The only quantity gate is disk headroom (`:141`).
- `warrantsFinalize` returns true unconditionally when `FinalizeGeneration == 0`
  (`banyand/trace/finalize_scanner.go:211`), so a shard's first finalize round rewrites its entire segment in one merge
  with one drop set.
- The scanner ticks every ten minutes (`banyand/trace/finalize_scanner.go:32`) over cooled segments only, so that first
  round has no natural chunking.

At a ninety percent drop rate a shard holding twenty million traces produces an eighteen million entry drop set: about
1.3 GiB live, 2.6 GiB reserved, inside a process also serving queries and hot merges.

Hot merges are milder, bounded by the merge policy and the lane threshold, but up to `CPUs/2 + 1` run concurrently
(`banyand/trace/merger.go:266-283`), each with its own set, alongside the node-wide serialized finalize round.

The protector is consulted once at round start (`banyand/trace/finalizer.go:58`) and never again as the set grows.

## 2. Goals and non-goals

**Goals.**

1. A hard, resource-derived ceiling on drop-set residency per merge and in aggregate.
2. Secondary-index contents remain exactly the retained-trace set. No orphan entries, no missing entries.
3. No change to any query path, plan, or operator.
4. No change to the finalize round machinery, its persisted state, or its scheduling.
5. Byte-identical behaviour below the ceiling.
6. The deletion lost at the ceiling is bounded and observable.

**Non-goals.**

1. Recovering the deletion lost at the ceiling. Section 9 lists four ways to do that later; none is in scope.
2. Bounding the finalize round's part selection.
3. Changing the fragment guard, the staging budgets, or the plugin decision path.
4. Reducing the cost of the exact lookup selected by Phase 6.

## 3. Design

### 3.1 Retain instead of drop at the ceiling

The ceiling changes what the sampler is *allowed to act on*, never how the secondary index is pruned. Once a merge's
drop set is full, every subsequent proposed drop becomes a retention:

- The trace is written to the output part.
- It is counted as retained, under a distinct counter — retained-by-ceiling, not retained-by-verdict.
- The fragment guard is **not** consulted for it. The guard exists to confirm drops; a trace that will be retained needs
  no confirmation, and skipping it saves the guard's bloom probes.

Because recording and dropping stop at the same instant, the drop set stays complete with respect to the drops actually
performed. That is the whole correctness argument, and it is why nothing downstream changes:

| Consumer | Change |
| --- | --- |
| `keepEncoded` predicate (`banyand/trace/drop_set.go:84`) | none |
| `filterBlockPointer` (`banyand/internal/sidx/merge.go:255`) | none |
| Guard revalidation gate (`banyand/trace/merger.go:926`) | none — the set is never released, so `dropped.len()` stays truthful |
| Output part `traceIDFilter` | none |
| Finalize state, scanner, scheduling | none |
| Query plan, engine, limit pushdown | none |
| Phase 6 hard invariant | none |

The state is a single one-way flag plus the existing set. No release-mid-merge semantics, no count-versus-length
divergence.

```go
type dropTracker struct {
    exact  *droppedTraceIDs // never released early; lives to SIDX publication as today
    maxIDs int              // derived from the resolved budget; 0 means unlimited
    full   bool             // one-way within a merge
}
```

### 3.2 Where the ceiling is enforced

One seam, in `resolveStagedDrops` (`banyand/trace/merger.go:1945-1996`), before either `recordDroppedTraceID` call site
and before the guard:

```go
for traceIdx, traceID := range assembledBatch.vectors.traceIDs {
    if keepMask[traceIdx] {
        // ... existing retained accounting ...
        continue
    }
    if !tracker.canAccept() {
        // Ceiling reached: retain rather than drop, and do not enter the guard
        // for a trace that will be kept.
        keepMask[traceIdx] = true
        // ... retained accounting + retained-by-ceiling counter ...
        continue
    }
    if filter.guard == nil {
        recordDroppedTraceID(...)
        continue
    }
    // ... existing guard path, unchanged ...
}
```

`flushStaged` (`banyand/trace/merger.go:2002-2035`) needs no change: it already writes every trace whose mask entry is
true. The ceiling may be reached mid-batch; per-trace mask flipping handles that with no batch-boundary special case.

### 3.3 Cap predicate

Cap in bytes, accounted exactly. The pool's sizing function already computes most of it
(`banyand/trace/drop_set.go:35-37`). The index **will** be built, so the projection is not optional:

```
liveBytes(N)      = cap(ids) * sizeof(string) + bodyBytes
projectedIndex(N) = nextPow2(max(2, 2N)) * 8      // exactly what buildIndex will allocate
```

`maxIDs` is derived once, from the budget and one entry's price, so the steady-state cost is one integer comparison per
proposed drop rather than a byte computation. Because `nextPow2` is discontinuous, price the slot share at the worst-case
ratio of four slots per entry:

| Layout | Header + slack | Body | Slots (worst case) | Bytes/entry at a 42-byte ID |
| --- | ---: | ---: | ---: | ---: |
| Today (`[]string`) | 20 | `allocClass(len)` | 32 | 100 |
| Arena + `uint32` offsets (§3.5) | 5 | `len` | 32 | 81 |

**The body term is derived from an observed ID length, not fixed.** A constant priced for the 42-byte
service-prefixed ID of §1.1 would let a deployment with longer IDs overshoot the budget proportionally — about 1.4x at 96
bytes, 3x at 256 — so the ceiling would not be a ceiling. Trace-ID length is uniform within a shard, so the first
recorded ID prices the whole merge; `allocClass` rounds it up to the allocator's size-class stride (16 bytes below 256,
32 above) because a ceiling must never under-count residency. At 42 bytes this yields exactly 100, so deriving the price
generalizes the fixed constant rather than re-tuning it, and the ceilings in §3.4 are unchanged for that case.

The derivation re-runs if a longer ID ever appears, which can only lower `maxIDs`. Uniformity is therefore an expectation
that makes one sample sufficient, not an assumption the memory bound rests on.

### 3.4 One budget for every merge

Hot lane, slow lane, and finalize round all resolve the same ceiling from the same function. There is no per-lane budget
and no special case for finalize.

```go
const dropSetAggregateDivisor = 16
const defaultDropSetBudget    = 16 << 20 // protector disabled; mirrors stageBudgetFromLimit's limit==0 fallback
const minimumDropSetBudget    =  1 << 20

// resolveDropSetBudget returns the per-merge drop-set ceiling. It mirrors
// stageBudgetFromLimit: divide by CPUs() so the aggregate across concurrent merges
// stays near memLimit/dropSetAggregateDivisor. Finalize is not special-cased — it is
// one more merge and is charged the same. See §5 for what that costs it and §9 for
// the deferred alternative.
func resolveDropSetBudget(opt option) uint64 {
    if testDropSetBudgetOverride > 0 { return testDropSetBudgetOverride }
    limit := protectorLimit(opt)
    if limit == 0 { return defaultDropSetBudget }
    return max(limit/(dropSetAggregateDivisor*uint64(max(1, cgroups.CPUs()))), minimumDropSetBudget)
}
```

Resulting ceilings, at 100 bytes per entry today and 81 with section 3.5:

| memLimit | CPUs | Budget | Traces per merge | Aggregate worst case |
| ---: | ---: | ---: | ---: | ---: |
| 512 MiB | 4 | 8 MiB | 84 k / 103 k | 32 MiB (6.3%) |
| 4 GiB | 8 | 32 MiB | 335 k / 414 k | 192 MiB (4.7%) |
| 16 GiB | 16 | 64 MiB | 671 k / 828 k | 640 MiB (3.9%) |

Aggregate assumes `CPUs/2 + 2` merges — the hot lanes plus the finalize round — all simultaneously at the ceiling. That
is a bound, not an expectation. Below four CPUs the `CPUs` divisor is smaller than `CPUs/2 + 2`, so the aggregate can
exceed `memLimit/16` by up to 2.5x on a one- or two-CPU node; the absolute figures there are small, and the existing
`stageBudgetFromLimit` has the same property by the same convention.

Two deliberate departures from `stageBudgetFromLimit`:

1. **A 1 MiB floor, not `defaultStageBudgetFloor`'s 16 MiB.** Some floor is needed, or a divided budget on a small node
   collapses to a ceiling too small for sampling to make useful progress. But a *generous* floor is exactly what breaks
   the aggregate bound above, so it is set two orders of magnitude lower than staging's. It binds only when
   `limit < 16 MiB × CPUs`, where the aggregate stays a small share of any plausible limit (128 MiB / 16 CPUs: 10 MiB,
   under 8 %). On implausibly small limits the floor does dominate the bound (32 MiB / 16 CPUs: 31 %); that is accepted,
   and pinned by a test rather than left to chance.
2. **No independent entry cap.** Staging needs `defaultMaxStagedTraceCount` because its per-trace byte estimate is
   coarse; here the byte accounting is exact. The omission must be commented as deliberate.

### 3.5 Arena-backed IDs (separable)

The ceiling *is* deletion capacity, so bytes per entry converts directly into traces deleted per merge. Replacing
`ids []string` with a byte arena plus `uint32` offsets removes both the sixteen-byte string headers and the size-class
rounding, raising the ceiling by about twenty-four percent.

It does not change the lookup shape. `keepEncoded` compares `convert.StringToBytes(dropped.ids[storedIdx-1])` against
the probe; with an arena it compares `arena[offs[i]:offs[i+1]]`. Same single slot probe, same single `bytes.Equal`, no
extra indirection. The Phase 6 index and its exact-confirmation step are untouched.

It also makes `bodyBytes` exactly `len(arena)`, so the cap predicate stops depending on a size-class estimate.

Swapping the index for binary search over `ids` remains out of scope; see section 10.

### 3.6 Dynamic trigger

The static ceiling does not cover pressure arriving from elsewhere in the process. The switch to retain-only fires on
**either** condition:

1. `liveBytes(N) + projectedIndex(N+1) > budget` — the static ceiling.
2. `pm.State() == protector.StateHigh`, sampled every 4,096 recorded drops — dynamic pressure, regardless of the
   ceiling.

The second closes the gap in section 1.2. Its failure mode is benign: it retains traces, so a spurious trigger costs
deletion, never correctness.

## 4. Consequence: which traces survive

Traces are proposed for dropping in core-merge order, which is ascending trace ID. So the traces a capped merge spares
are the **lexicographically largest** IDs in that merge. SkyWalking trace IDs are service-prefixed, so within one capped
round the spared set is correlated with service name rather than being a uniform sample.

A later round's ascending walk encounters the previous survivors and deletes the next `maxIDs` of them, so the bias
erodes as long as rounds keep happening. It becomes permanent when rounds stop — see section 5.2. The bias must be
documented in operator docs, because "sampling retained more traces for services late in the alphabet" is otherwise an
inexplicable observation.

## 5. Cost: bounded deletion

### 5.1 Hot merges: benign

A capped hot merge's output has `FinalizeGen` min-propagated from its inputs (`banyand/trace/merger.go:1168-1176`), so
the spared traces stay selectable by later hot merges and by finalize. Nothing is lost, only deferred.

### 5.2 Finalize: bounded by the existing round budget

A finalize round's output is stamped `gNext` and the next round selects on `FinalizeGen < gNext+1`
(`banyand/trace/finalizer.go:74,108,1166-1167`, and the field comment at `banyand/trace/finalize_state.go:45-52`), so a
capped round's output **is** eligible for a later round. No new mechanism is added to exploit that; the existing gates
apply unchanged:

- `warrantsFinalize` requires `unsampledBytes >= max(floorBytes, ratio × total)` once `FinalizeGeneration != 0`
  (`banyand/trace/finalize_scanner.go:211-219`), and the counter was reset by the round that just ran
  (`banyand/trace/finalizer.go:193-204`), so a later round waits on **new ingest** rather than on the leftover work.
- `cooldownNs` paces rounds.
- `finalizeMaxRoundsDefault = 8` (`banyand/trace/pipeline_registry.go:96`) is a hard per-shard lifetime cap, after which
  the shard is marked `Terminal` and never scanned again (`banyand/trace/finalize_scanner.go:196-203`).

So total deletion for a shard's lifetime is bounded by eight rounds times the ceiling:

| memLimit | CPUs | Ceiling per round | Lifetime deletion bound (8 rounds) |
| ---: | ---: | ---: | ---: |
| 512 MiB | 4 | 84 k / 103 k | 670 k / 824 k |
| 4 GiB | 8 | 335 k / 414 k | 2.7 M / 3.3 M |
| 16 GiB | 16 | 671 k / 828 k | 5.4 M / 6.6 M |

A shard with eighteen million droppable traces at 4 GiB and eight CPUs therefore leaves roughly fifteen million
permanently undeleted. **This is the accepted cost of the design.** It is a deliberate trade: memory safety and a
minimal, self-contained change, against deletion throughput on large shards. Section 9's deferred features are the
engineering levers.

The operator lever is **the protector limit**, not the machine's memory. The ceiling scales linearly with
`protector.GetLimit()`, which is `--allowed-bytes` when set and otherwise a valid cgroup memory limit
(`banyand/protector/protector.go:245-269`). A node with neither — no `--allowed-bytes` and no readable or finite cgroup
limit, which is the bare-metal default — leaves the protector *disabled*, and `resolveDropSetBudget` then returns the flat
`defaultDropSetBudget` regardless of how much RAM the machine has: about 168,000 traces per merge and 1.3 million per
shard lifetime, whether the box has 8 GiB or 512 GiB.

That is the same shape as `stageBudgetFromLimit`'s flat `limit == 0` fallback, so it is consistent with how the rest of
the merge pipeline treats an unmeasurable node. The consequence differs, though, and is worth stating rather than
inferring: a flat staging budget only slows a merge, while a flat drop-set ceiling bounds deletion permanently. **On such
a node, adding RAM does nothing for sampling; setting `--allowed-bytes` (or running under a cgroup limit) is what raises
the ceiling.** Deriving a budget from physical RAM instead was rejected: it would add a memory-discovery path that the
rest of the codebase deliberately routes through the protector, and a node running with no memory limit at all has no
budget for staging either.

The failure is invisible without section 6's counter, which is why that counter and its warning log are part of this
specification rather than a follow-up.

### 5.3 Finalize I/O is unchanged

Round count does not change, and a round rewrites the same parts it does today. A capped round writes *more* bytes than
an uncapped one, because it retains traces it would otherwise have removed — but that is strictly less work than the
unbounded status quo does, and there is no additional rewrite. This design adds no finalize write amplification.

## 6. Observability

Without this, the design's cost is invisible: a shard quietly under-deletes and nothing says so. With no continuation
mechanism, observability is the *only* thing standing between a capped shard and a silent one.

On `banyand_trace_tst_`, per merge:

| Instrument | Labels | Answers |
| --- | --- | --- |
| `pipeline_traces_retained_by_ceiling` (counter) | `group` | How much deletion was lost. Distinct from retained-by-verdict; both also increment `pipeline_traces_retained` |
| `pipeline_merges_ceiling_reached` (counter) | `group,lane` | Which lane is capping — `finalize` is the one that matters |
| `pipeline_drop_set_budget_bytes` (gauge) | `group` | The resolved ceiling: the denominator for the two above |
| `pipeline_drop_set_entries` (histogram) | `group,lane` | How close merges are running to that ceiling |

The histogram is emitted **whether or not a merge caps**, and that is the point: the two counters only fire once
deletion has already been lost, so on their own they are a lagging indicator. Watching the upper buckets approach
`pipeline_drop_set_budget_bytes / dropSetBytesPerEntry` is what gives an operator warning *before* the ceiling bites. A
zero budget emits nothing rather than a zero gauge, so "no ceiling" never reads as "a zero-byte ceiling".

The storage boundary remains per merge, but observability is deliberately rolled up to the group. Segment and shard
labels would multiply long-lived series with storage rotation and are not needed to decide whether the group needs more
protector headroom. The lane label remains where it distinguishes ordinary and finalize work. These series are deleted
with the group-level table metrics rather than when an individual segment or shard closes.

On `banyand_trace_pipeline_`, per group, published once after a complete scan (§7):

| Instrument | Labels | Answers |
| --- | --- | --- |
| `finalize_rounds` (gauge) | `group` | Highest round count among the group's cooled shards |
| `finalize_terminal` (gauge) | `group` | Whether any cooled shard is terminal and can never delete another trace |

- The resolved budget, the `full` flag, and the retained-by-ceiling count in the benchmark event, next to
  `StagingHardLimit` (`banyand/trace/merger.go:712-716`).
- A warning log when a finalize round hits the ceiling, naming the group, shard, and count. This is the one condition an
  operator can act on, by raising the protector limit (§5.2 — not by adding RAM, if the protector is disabled).
- All three must describe the merge output that was actually **published**. A capped attempt that the fragment guard
  rejects is retried without sampling, and the retry's output drops nothing; reporting a ceiling for it would send an
  operator after a merge that never hit one.

Nothing is added to the persisted finalize state; a capped round is visible through metrics and logs only.

## 7. Required changes

| Site | Change |
| --- | --- |
| `banyand/trace/drop_set.go:28-59` | Add `dropTracker` with `canAccept`, exact `liveBytes`/`projectedIndex`. Optionally arena-back the IDs (§3.5). |
| `banyand/trace/merger.go:1945-1996` | Ceiling check before the guard and before both record sites; flip `keepMask`; new counter. |
| `banyand/trace/merger.go:2050-2066` | `traceEvaluationStager` holds the tracker instead of a bare `*droppedTraceIDs`. |
| `banyand/trace/merger.go:194-243` | Add `resolveDropSetBudget` and `testDropSetBudgetOverride`. One function, no lane parameter. |
| `banyand/trace/merger.go:638-650` | Hot filter carries the budget. |
| `banyand/trace/finalizer.go:156-168` | Finalize filter carries the **same** budget. Nothing else in the finalize path changes. |
| `banyand/trace/merger.go:712-716` | Emit budget, capped flag, retained-by-ceiling count. |
| `banyand/trace/metrics.go:318-334` | Retained-by-ceiling and capped-merge counters. |
| `post-trace-pipeline.md:630-632` | Note that a merge may stop dropping at its ceiling; the pruning predicate and lockstep publication are unchanged. |
| `trace-pipeline-merge-optimization-plan.md` Phase 6 | Record that the collector is now ceiling-bounded; the phase's exactness invariant is unchanged. |

Not changed: `keepEncoded`, `filterBlockPointer`, the SIDX merge signature, the output part's `traceIDFilter`,
`banyand/trace/finalize_state.go`, every query path.

`banyand/trace/finalize_scanner.go` gains **telemetry only**: the scan pre-filter publishes each shard's finalize state
(section 6) before deciding, because it is the only place a terminal shard is still visited. Selection, generation
stamping, cooldown, and the round cap are untouched, and the pre-filter returns exactly what it returned before — pinned
by `TestSegmentMayWarrantObservedMatchesUnobserved`.

## 8. Rejected: prune with the retained-ID bloom filter

Recorded here rather than in section 9 because the reason is a query-layer finding that outlives this design.

The alternative was to release the exact set at the ceiling and prune with the output part's retained-ID bloom filter
(`banyand/trace/block_writer.go:236,283`, read back at `banyand/trace/merger.go:1181`), which is free — it is built,
persisted, and re-read today. A bloom has no false negatives, so a negative proves non-membership and the deletion stays
exact; a false positive can only *retain* an entry for a dropped trace, at about 0.042 percent
(`pkg/filter/bloom_filter.go:29-34`). That would have bounded memory with **no** deletion loss at all.

The reasoning is sound but the conclusion drawn from it — that a surviving entry for a dropped trace is query-invisible
— is false on the default query engine:

- The pushed-down limit is `limit + offset`, the user-visible limit (`pkg/query/logical/trace/trace_analyzer.go:95`).
- It reaches Phase 1 as `maxRows` and caps the stream of **secondary-index-derived** distinct trace IDs before any core
  scan (`banyand/trace/query_vectorized.go:361-388`, `pkg/query/vectorized/trace/limit.go:50-66`,
  `limit_carry.go:56-88`).
- The core scan runs afterwards (`banyand/trace/query_vectorized.go:398-431`). A trace with no spans yields nothing, and
  nothing backfills: the distributed plan sends `Limit = maxTraceSize` to data nodes
  (`pkg/query/logical/trace/trace_plan_distributed.go:135-136`) and the liaison only trims.
- The vectorized engine is enabled by default (`pkg/query/vectorized/trace/config.go:49-55`).

So an orphan entry inside the top-N window consumes a result slot and the query silently returns fewer traces than the
limit. Tag filters make this more likely, not less: they are pushed into the secondary index
(`pkg/query/logical/trace/trace_plan_tag_filter.go:135-152`) and a surviving element keeps all its tag columns
(`banyand/internal/sidx/merge.go:252-283`), so an orphan can match and enter the window. The row path tolerates it
(`banyand/trace/query.go:366-370` skips zero-cursor traces and keeps pulling) but it is not the default.

A projection served entirely from secondary-index tags would turn this from a short count into wrong rows. No such path
exists today — Phase 1 carries only trace ID, key, and part ID (`banyand/trace/streaming_pipeline.go:40-46`,
`pkg/query/vectorized/trace/limit_carry.go:56-88`) and result tags come only from the core block scan
(`banyand/trace/query_vectorized.go:246-289`) — but it is an obvious future optimization, and a memory design whose
correctness depends on nobody adding it is not safe.

Note the corollary, which is why this section matters even though the bloom is rejected: **fail-open-on-ceiling — "stop
pruning, keep every entry" — is not an acceptable fallback either.** It produces orphans for one hundred percent of
post-ceiling drops where the bloom produced them for 0.042 percent. Retaining the *traces* is the only fallback that
keeps the index exact.

## 9. Deferred features

Each of these lifts part of section 5.2's deletion bound. None is a prerequisite for this design, none is required for
it to be correct, and they are independent of one another. Listed in the order their prerequisites are likely to be met.

### 9.1 Per-lane finalize ceiling

**Buys.** About `CPUs` times more deletion per finalize round — at 4 GiB / 8 CPUs, 256 MiB and ~2.7 million traces per
round instead of 32 MiB and ~335,000, so the lifetime bound rises from ~2.7 million to ~21 million traces per shard.
Equally memory-safe, since the finalize scanner is concurrency-1 node-wide
(`banyand/trace/finalize_scanner.go:34-37`).

**Costs.** A second resolver and a second call site, so two budget constants that can drift apart, and a test table that
has to cover both.

**Prerequisite.** Evidence that the lifetime bound is actually being hit — the retained-by-ceiling counter from section 6
being non-zero on real shards.

**Why not now.** The whole point of one budget is that finalize is not special. This is the cheapest way to change that
decision if the data says it was wrong, which is why it is listed first.

### 9.2 Continuation rounds

**Buys.** Removes the ingest gate, so leftover work clears itself. A capped round would persist a pending flag,
`warrantsFinalize` and `shardMayWarrant` would both honour it (bypassing the `unsampledBytes` threshold but not the
cooldown), and a separate bounded `ContinuationRounds` counter would keep `FinalizeRounds` free for ingest-driven work.

**Costs.** New persisted finalize state, changes in two scanner predicates that must move together, a new bound to
choose, and a new crash-ordering case to verify against the DD6 argument. It also multiplies finalize rewrite volume by
roughly the number of extra rounds, since every round rewrites the whole shard whatever it deletes — the reason section
5.3's "I/O is unchanged" holds only while continuation is absent.

**Prerequisite.** 9.1 first, because raising the ceiling is strictly cheaper per unit of deletion recovered than adding
rounds.

### 9.3 Sampling watermark

**Buys.** Only useful with 9.2. Without it, every continuation round re-runs the sampler over all surviving traces,
re-deciding traces earlier rounds already retained by verdict, so plugin work scales with rounds times survivors. A
watermark — the last trace ID a capped round actually **decided** — lets the next round skip evaluation at or below it,
which retains those traces, exactly the verdict they already had. Round count becomes `ceil(droppable / maxIDs)`,
provable rather than hoped for.

**Costs.** One more persisted field, and one sharp edge: the watermark must be set from the last trace *decided*, never
the last trace *read*, or traces that were staged but never evaluated would be exempted from sampling forever.

**Prerequisite.** 9.2.

### 9.4 Bloom-filter pruning

**Buys.** The only option that removes the deletion bound entirely rather than raising it: memory bounded with no
retention loss at all.

**Costs and prerequisite.** Section 8. It requires making the Phase-1 limit refillable — over-fetch and backfill after
the core-scan join, or apply the cap post-join — which is a query-layer change worth doing on its own merits and is the
hard prerequisite here. Until then this option is unavailable, not merely unattractive.

## 10. Other rejected alternatives

- **Unbounded exact set (status quo).** Residency scales with shard size, which is unbounded, and reserved heap is twice
  live heap. Rejected.
- **Swapping the index for binary search over the ascending `ids`.** Saves sixteen to thirty-two bytes per entry, but
  `keepEncoded` runs once per secondary-index element and elements outnumber traces. Phase 6 measured the sorted slice at
  2.3 to 3.6 times slower than the selected compact hash (`banyand/trace/drop_set_test.go:73-86`). Rejected.
- **Spilling the drop set to disk with on-disk verification.** Bounded, exact, and it would preserve full deletion — but
  on a heavy-drop merge most probes are true positives, so most lookups hit disk. Rejected on cost.
- **Chunking the finalize round's part selection.** Would bound `N` by construction with no deletion loss. Rejected on
  effectiveness, not complexity: whole-shard selection is what puts every fragment of a trace in the same merge set, so
  chunking would leave cross-chunk fragmented traces permanently guard-deferred
  (`banyand/trace/merger.go:1965-1995`) at a rate set by fragmentation, which is worse and less predictable than the
  bound in section 5.2.

## 11. Invariants

Preserved unchanged:

1. Secondary-index lookup remains exact. No probabilistic structure authorizes a deletion. *(Phase 6, verbatim.)*
2. All physical blocks for one trace ID are assembled into exactly one sampler decision.
3. The fragment guard pins and validates conjunction parts before any drop.
4. Core and secondary-index outputs are introduced atomically.
5. Timeout, panic, malformed-verdict, and plugin-error paths fail open.
6. The pipeline-disabled path performs no drop-set work.
7. Finalize round selection, generation stamping, persisted state, cooldown, and round cap are exactly as before.

Introduced:

8. **The drop set is complete with respect to the drops performed.** Recording and dropping stop at the same instant.
9. **A secondary index contains exactly the entries of its part's retained traces** — no orphans, none missing.
10. **Drop-set residency never exceeds the resolved budget**, and `full` is one-way within a merge.
11. **The ceiling only ever converts a drop into a retention.** It never changes a retention into a drop, never
    evaluates a partial trace, and never bypasses the guard for a trace that is dropped.
12. **A capped merge's spared traces remain eligible for a later merge or round.** For hot merges this follows from
    `FinalizeGen` min-propagation; for finalize, from the unchanged generation predicate — subject to the round bound in
    section 5.2, which is a bound on deletion, not a correctness claim.

## 12. Risks

1. **Silent under-deletion.** The headline risk, and the one with no mitigation inside this design. A shard can reach its
   lifetime round cap with millions of undeleted traces and look healthy. Mitigation: section 6's counter and warning log
   are not optional, and section 9.1 is the cheapest response if the counter says the bound is being hit.
2. **A protector-disabled node gets the worst ceiling and the least obvious lever.** Section 5.2: the flat fallback
   applies regardless of RAM, and **neither** validation run can observe it: both execute under a limit, and the
   capped-path run enables the protector explicitly via `--allowed-bytes`. Mitigation: documented in section 5.2 and at `resolveDropSetBudget`; the counter still fires, so the condition is
   detectable even though the validation run will not surface it.
3. **Retention grows storage.** Traces retained by ceiling occupy disk that the sampler intended to reclaim. The
   finalize disk-headroom gate (`banyand/trace/finalizer.go:141`) still protects the write path, but a shard whose
   sampling is ceiling-bound will not shrink as an operator expects. Belongs in operator documentation next to the
   counter.
4. **Ordering bias.** Section 4. Mitigation: documentation.
5. **Guard probe interaction.** Skipping the guard for ceiling-retained traces changes how many bloom probes a merge
   performs, so guard-probe metrics shift on capped merges. Expected, not a defect; assert it rather than be surprised.
6. **Pool churn.** Sets above `maxPooledDroppedTraceIDBytes` (4 MiB) are not pooled, and every ceiling in section 3.4
   exceeds it, so a capped merge's set is garbage rather than reused. Pre-existing behaviour; measure before changing the
   pool bound.

## 13. Execution

The ticket-level breakdown, with per-ticket boundaries, tests, and rollback, is in
[trace-drop-set-bounding-plan.md](trace-drop-set-bounding-plan.md). In outline: the mechanism lands inert (production
budget zero, which the tracker treats as unlimited), and a single activation ticket turns it on and is the rollback
boundary for the series.

## 14. Open questions

1. `dropSetAggregateDivisor = 16`. It sets both the memory bound and, through section 5.2, the lifetime deletion bound.
   Settle before activation, because the exit-gate measurements are stated against it.
2. Whether `maxRounds` should become operator-tunable. It is per-group in `finalizeConfig`
   (`banyand/trace/pipeline_registry.go:132`) but the v1 proto carries no override field
   (`banyand/trace/pipeline_registry.go:135-137`), so today the only lever on section 5.2's bound is memory. Not required
   by this design; worth deciding separately.
