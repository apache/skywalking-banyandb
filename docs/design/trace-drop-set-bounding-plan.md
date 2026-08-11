# Trace Merge Drop-Set Bounding — Execution Plan

Execution plan for [trace-drop-set-bounding.md](trace-drop-set-bounding.md). That document holds the rationale,
measurements, and rejected alternatives; this one holds the work. Read the spec's sections 3 and 5 before starting any
ticket.

## Status

DS-1, DS-1b, DS-2, DS-3, DS-4 and DS-5 are implemented, plus the production instruments of spec §6. The ceiling is
active on every merge.

Outstanding:

- **DS-5's validation has not been executed.** It now rides the Phase 10 SkyWalking acceptance workload as two runs — one
  at default settings for the headroom measurement, one at `--allowed-bytes=256MiB` to exercise the capped path. Until
  both produce reports the ticket is code-complete but not closed, and the memory bound rests on unit-level residency
  checks. This is the one item that gates calling the series done.
- **DS-9** (arena-backed IDs) not started. Optional, but it raises the ceiling ~24% and so improves the deletion bound
  DS-5's validation runs will measure — worth landing before those runs rather than after.
- **DS-11** (documentation) not started.

## Shape of the work

Eight tickets. One PR each; CI runs on pull requests, not on pushes.

The series is deliberately small. A finalize round is treated as one more merge, so nothing touches
`banyand/trace/finalize_state.go`, `banyand/trace/finalize_scanner.go`, the round scheduling, or any query path. The four
features that would recover the deletion bounded by spec §5.2 are deferred, listed at the end of this document as
prospective tickets with their prerequisites.

**The mechanism lands inert.** DS-3 wires the ceiling into the decision loop while the production budget stays zero,
which `dropTracker` treats as unlimited. Nothing in production behaviour changes until DS-5 activates it. That
single-ticket activation boundary is the rollback plan for the whole series — revert DS-5 and the mechanism is dormant
again with no code removed.

```
DS-1 ──> DS-1b ──> DS-3 ──> DS-4 ──┐
DS-2 ──────────────────────────────┴──> DS-5 ──> DS-11
DS-9 (independent, any time after DS-1; best before DS-5)
```

**Definition of done for every ticket:** its own tests pass; `make lint` clean; the existing `banyand/trace` suite passes
**unmodified** unless the ticket explicitly lists a test it changes; and the diff touches neither `pkg/query/...` nor the
two finalize files named above.

## Stage 1 — Mechanism, inert

### DS-1 — `dropTracker` with exact residency accounting

**Size.** S. **Depends on.** Nothing.

**Scope.** A self-contained structure and its arithmetic. No call sites.

- `dropTracker` wrapping the existing `*droppedTraceIDs`, with `canAccept() bool` and a one-way `full` flag.
- `liveBytes()` extending the pool's existing sizing (`banyand/trace/drop_set.go:35-37`) with the ID-body bytes.
- `projectedIndexBytes(n int) int64` returning exactly what `buildIndex` will allocate for `n` entries.
- `maxIDsForBudget(budget uint64) int`, where **`budget == 0` means unlimited** — this is what keeps DS-3 inert. DS-1b
  adds the observed-ID-length parameter; land the sentinel and the `max(1, …)` clamp here.
- A residency benchmark reporting bytes per entry.

**Out of scope.** Any change to `merger.go`. Reading the protector. The arena (DS-9).

**Files.** `banyand/trace/drop_set.go`, `banyand/trace/drop_set_test.go`.

**Tests.**

- `TestProjectedIndexBytesMatchesBuildIndex` — for `n` in `{0, 1, 2, 3, 4, 5, 7, 8, 9, 1023, 1024, 1025, 65535, 65536,
  65537}`, build a real set of `n` IDs, call `buildIndex`, assert `projectedIndexBytes(n) == int64(len(slots))*8`. The
  power-of-two boundaries are the point: `nextPow2(2n)` is discontinuous and the projection must be exact, not
  conservative.
- `TestCanAcceptStopsAtBudget` — for budgets 1 MiB / 8 MiB / 32 MiB, add IDs until `canAccept` is false; assert (a) it
  never returns true again, (b) `liveBytes() + projectedIndexBytes(len+1)` is at or below the budget at the stop point,
  (c) `buildIndex` then leaves measured residency at or below the budget.
- `TestZeroBudgetIsUnlimited` — `maxIDsForBudget(0)` yields a tracker that accepts far past any ceiling.
- `BenchmarkDropSetResidency` — `b.ReportMetric` bytes per entry at 100 k and 1 M entries, so DS-9 has a baseline.

**Exit.** Projection exact at every tested `n`. Measured residency at or below budget for all three budgets.

**Rollback.** Dead code; safe to leave.

---

### DS-1b — Derive bytes/entry from the observed ID length

**Size.** S. **Depends on.** DS-1.

**Scope.** A fixed bytes/entry constant priced for the 42-byte ID of spec §1.1 lets longer trace IDs overshoot the budget
proportionally, so the ceiling is not a ceiling (about 1.4x at 96 bytes, 3x at 256). Replace it with a price derived from
an observed length.

- `allocClassBytes(n)` — round up to the allocator's size-class stride (16 below 256 bytes, 32 above), an upper bound
  because a ceiling must never under-count.
- `dropSetBytesPerEntry(idLen)` = header + slack + `allocClassBytes(idLen)` + worst-case slots. At `idLen == 42` this is
  exactly 100, so the change generalizes the old constant rather than re-tuning it and §3.4's ceilings are unchanged for
  that case.
- The tracker carries `budget` instead of a precomputed `maxIDs` and derives `maxIDs` when the first ID is recorded, via
  a `record` method. Three states stay distinct: `budget == 0` unlimited, `maxIDs == 0` with a non-zero budget "not yet
  derived, admit the entry that derives it", and the `max(1, …)` clamp keeps a non-zero budget bounding.
- `record` re-derives if a longer ID appears, so per-shard uniformity is self-correcting rather than load-bearing.
- `liveBytes` prices bodies with `allocClassBytes` too, so the predicate and the derivation speak in the same units.

**Out of scope.** The arena layout (DS-9), which later replaces only the body term.

**Files.** `banyand/trace/drop_set.go`, `banyand/trace/drop_set_test.go`.

**Tests.**

- `TestBytesPerEntryTracksIDLength` — the regression test. At lengths 42/64/96/128/256, drive a tracker to its ceiling and
  assert `residentBytes()` stays at or below the budget, and that a longer ID yields a strictly smaller ceiling.
- `TestTrackerRederivesOnLongerID` — a longer ID lowers the ceiling; a subsequent shorter one does not raise it.
- `TestTinyBudgetStillBounds` and `TestZeroBudgetIsUnlimited` updated for the derived form: the clamp and the unlimited
  sentinel must both survive it.

**Exit.** Residency at or below budget at every tested ID length — the invariant that is false with a fixed constant.

---

### DS-2 — Budget resolution

**Size.** XS. **Depends on.** Nothing (parallel to DS-1).

**Scope.** One resolver, no consumers.

- `resolveDropSetBudget(opt option)` — `limit/(dropSetAggregateDivisor*CPUs)`, floor `minimumDropSetBudget`,
  `defaultDropSetBudget` when the limit is zero. **No lane parameter and no second function**: finalize is charged the
  same as a hot merge (spec §3.4).
- `testDropSetBudgetOverride`, mirroring `testStageBudgetOverride` (`banyand/trace/merger.go:189`).
- Constants, with comments covering why the floor is 1 MiB rather than staging's 16 MiB and when it binds, why there is
  no independent entry cap, and — pointing at spec §9.1 — why finalize is not given a larger ceiling despite being
  concurrency-1.

**Out of scope.** Wiring the budget into a filter (DS-5).

**Files.** `banyand/trace/merger.go` (budget block near `:194-243`), new `banyand/trace/drop_set_budget_test.go`.

**Tests.**

- `TestResolveDropSetBudget` — table over `(limit, CPUs)` covering 0, 512 MiB, 4 GiB, 16 GiB × 1, 4, 8, 16 CPUs; exact
  expected bytes including the zero-limit fallback and the minimum clamp.
- `TestDropSetAggregateBoundHolds` — for each row, assert `budget*(CPUs/2+2) <= limit/8`, and assert the documented
  small-node overshoot explicitly for CPUs 1 and 2 rather than letting it look like a failure. The aggregate invariant
  becomes a test rather than a comment in the spec.
- `TestDropSetFloorBoundRegime` — the floor-bound regime the rows above are all too large to reach: assert the floor
  binds, that the aggregate bound still holds above a plausible smallest limit, and that below it the floor dominates.
  Without this, the floor's effect on the bound is untested.
- `TestDropSetBudgetOverrideWins`.

**Exit.** Table passes. Aggregate bound asserted, including the small-CPU exception.

**Rollback.** Unused function; safe.

---

### DS-3 — Ceiling enforcement in the decision loop (inert)

**Size.** M. **Depends on.** DS-1.

**Scope.** The behavioural seam, with the production budget still zero so the ceiling is unreachable outside tests.

- `traceEvaluationStager` holds a `dropTracker` instead of a bare `*droppedTraceIDs`
  (`banyand/trace/merger.go:2050-2066`).
- In `resolveStagedDrops` (`banyand/trace/merger.go:1945-1996`): a ceiling check placed **before the guard and before both
  `recordDroppedTraceID` sites**. When the tracker is full, set `keepMask[traceIdx] = true`, run the retained accounting,
  increment a new retained-by-ceiling counter, and `continue` — the guard is not consulted for a trace that will be kept.
- `incPipelineTracesRetainedByCeiling` in `banyand/trace/metrics.go` (alongside `:318-334`), plus the `observation` hook.
- `filter.budget` plumbed through `mergeFilter`, left at zero by both construction sites.

**Out of scope.** Activating the budget (DS-5). Benchmark-event fields and the warning log (DS-4). Any change to
`keepEncoded`, `filterBlockPointer`, the SIDX merge signature, the output part's `traceIDFilter`, the finalize state or
scanner, or any query path.

**Files.** `banyand/trace/merger.go`, `banyand/trace/metrics.go`, the merge-observation type.

**Tests.**

- `TestMergeCeilingPrunesSidxExactly` — the load-bearing test. Force a two-entry ceiling on a merge whose sampler proposes
  five drops. Assert the output part contains exactly the three spared traces plus all verdict-retained ones, and that for
  **each** sidx instance the element set equals the output part's trace-ID set — strict equality in both directions, so
  both an orphan and a missing entry fail.
- `TestMergeCeilingRetainsTheAscendingTail` — the spared traces are the lexicographically largest of the proposed drops,
  pinning the bias of spec §4 as observed behaviour rather than a claim.
- `TestMergeCeilingSkipsGuard` — the guard's bloom-probe count equals the number of *pre-ceiling* proposed drops.
- `TestMergeCeilingStillRevalidatesGuard` — a capped merge that dropped at least one trace still runs
  `filter.guard.revalidate` (`banyand/trace/merger.go:926`). This is the regression the rejected bloom design would have
  introduced; assert it even though this design cannot reach it.
- `TestMergeCeilingCounterAccounting` — retained-by-ceiling equals proposed drops beyond the ceiling; total retained
  equals verdict-retained plus ceiling-retained.
- `TestCappedFinalizeRoundLeavesStateUnchanged` — a capped finalize round writes the same `finalize.json` fields a normal
  round does: generation advanced, counter reset, no new fields, `FinalizeRounds` incremented once. Guards the spec §11
  invariant 7 that finalize scheduling is untouched.
- `TestMergeCeilingInertWithoutOverride` — with no override, a merge proposing many drops records every one; nothing is
  retained by ceiling.
- Unmodified: `TestMergeFilter_CoupledSidxPrune` (`banyand/trace/pipeline_chain_test.go:997`),
  `TestMergeFilter_IdempotentReMerge`, `banyand/trace/pipeline_chain_filter_test.go:508`.

**Exit.** All of the above, with zero edits to existing tests.

---

### DS-4 — Observability

**Size.** S. **Depends on.** DS-3.

**Scope.** Making the ceiling visible before it can fire in production. With no continuation mechanism this is the only
signal that a shard is under-deleting, so it is part of the series rather than a follow-up.

- Benchmark-event fields beside `StagingHardLimit` (`banyand/trace/merger.go:712-716`): resolved budget, capped flag,
  retained-by-ceiling count.
- A capped-merge counter split by lane.
- One `Warn` per capped finalize round, naming group, shard, and retained-by-ceiling count. Exactly one: not per batch,
  not per trace.

**Out of scope.** Budget activation. Anything that changes a merge decision. Anything persisted.

**Files.** `banyand/trace/merger.go`, `banyand/trace/merge_benchmark_observer.go`, `banyand/trace/metrics.go`,
`banyand/trace/finalizer.go` (log call only).

**Tests.**

- `TestBenchmarkEventCarriesDropSetCeiling` — forced ceiling; all three fields populated and consistent with the counters.
- `TestCappedFinalizeRoundWarnsOnce` — log capture asserts exactly one warning for a capped round and none for an uncapped
  one.

**Exit.** Both tests, plus the new fields visible in a forced-ceiling run's report.

---

## Stage 2 — Activation

### DS-5 — Activate the budget

**Size.** XS (the diff). Validation rides the Phase 10 SkyWalking acceptance workload rather than standing up its own
harness, so it costs one extra run of an existing workload plus a flag. **Depends on.** DS-2, DS-4.

**Scope.** The only ticket in the series that changes production behaviour.

- Hot filter construction (`banyand/trace/merger.go:638-650`) and finalize filter construction
  (`banyand/trace/finalizer.go:156-168`) both carry `resolveDropSetBudget`. Same call, same value.
- No operator flag, matching the staging budget's precedent (`banyand/trace/merger.go:191-193`).

**Files.** `banyand/trace/merger.go`, `banyand/trace/finalizer.go`.

**Tests.**

- `TestFiltersCarryResolvedDropSetBudget` — both construction sites carry the resolver's value, and they are equal. The
  equality assertion is deliberate: it is what would fail if someone reintroduced a lane-specific budget without updating
  spec §3.4.
- `TestProductionBudgetMergeReachesNoCeiling` — an ordinary-sized merge at the resolved budget records every drop. Assert
  the *derived ceiling* the budget yields, not merely that this merge did not cap, or the test passes for any budget above
  a few kilobytes and stops guarding the default constant.
- `TestLosslessRetryClearsCeilingReporting` — a capped attempt rejected by the fragment guard is retried without
  sampling, and the retry's published output drops nothing; the capped-merge counter, `DropSetCapped`, and the finalize
  warning must all describe that published output, not the discarded attempt. Verify this test fails without the fix.
- An in-process residency check over a few hundred proposed drops is worth adding, clearly labelled as **not** a
  workload-level measurement.

### DS-5 validation: two SkyWalking integration runs

Validation rides the Phase 10 SkyWalking acceptance workload of the
[merge optimization plan](trace-pipeline-merge-optimization-plan.md) rather than the containerised controlled run that
earlier drafts of this plan specified. Section "Why not the controlled run" below records that decision.

Both runs read the instruments of spec §6; without them neither run can answer anything about the ceiling.

**Run A — default settings, the headroom measurement.** The standard serialized 24-hour workload, unmodified. At a 4 GiB
limit and `GOMAXPROCS=2` the budget is 128 MiB, a ceiling near 1.34 million traces in one merge, so real traffic will not
reach it. That is the result, not a failure to test:

- [ ] `pipeline_drop_set_entries` p99 recorded against `pipeline_drop_set_budget_bytes / dropSetBytesPerEntry`. **This
      ratio is the deliverable** — it is how much headroom production-shaped traffic actually has, and therefore whether
      the ceiling is load-bearing at default settings at all.
- [ ] `pipeline_traces_retained_by_ceiling` is zero on every shard, confirming activation is behaviour-neutral here.
- [ ] Phase 10's existing gates still pass: correct core and secondary-index ledgers, no OOM, no unexpected lossless
      retry, no regression against the pipeline-disabled baseline.
- [ ] Observed trace-ID length recorded. Real SkyWalking IDs are the first workload test of DS-1b's derived
      bytes/entry and of the per-shard uniformity it expects; an ID longer than the 42 bytes spec §1.1 measured is
      exactly the case the derived price exists for.

**Run B — `--allowed-bytes=256MiB`, the capped-path measurement.** `resolveDropSetBudget` reads the protector limit and
`--allowed-bytes` sets it directly, so this is a supported production flag, not a test seam. At 2 CPUs it yields an 8 MiB
budget and a ceiling near 84,000 traces, which the same workload reaches comfortably. This is the only run that
exercises the code this ticket adds:

- [ ] `pipeline_traces_retained_by_ceiling` non-zero, with the shard label identifying where.
- [ ] For every shard, secondary-index contents equal exactly the retained-trace set — spec invariant 9, now under real
      trace IDs, real drop ratios, real fragment-guard behaviour and live query traffic rather than one frozen selection.
- [ ] Queries ordered by `latency` and `start_time` return the same traces as the pipeline-disabled baseline for the
      traces both retain. No short result sets, which is what a violation of the "no orphan index entries" invariant
      would look like from outside.
- [ ] Peak heap and RSS bounded, with drop-set residency attributable via `pipeline_drop_set_entries` ×
      `dropSetBytesPerEntry` and at or below the resolved budget.
- [ ] Traces left undeleted recorded as a number — this quantifies spec §5.2 on a real workload and is the input to any
      later decision to take a deferred feature (spec §9).
- [ ] `finalize_rounds` progression and any `finalize_terminal` transitions recorded.
- [ ] No OOM, panic, malformed verdict, or unexpected lossless retry.

Report both runs under `.scratch/trace-drop-set-bounding/ds5-<n>`.

**Why not the controlled run.** `banyand/cmd/trace-merge-benchmark` over `banyand/internal/benchmark/tracebaseline` runs
ten alternating single-merge container processes on a frozen seed, five per variant, under a canonical
`GOMAXPROCS=2` / `MemoryMax=4 GiB` readiness gate. Its unique contribution is a reproducible paired comparison with exact
oracles and CV bounds. But at canonical resources the ceiling sits near 1.34 million traces while the existing frozen
selection holds 33,353 — roughly 40x too small to make a merge cap. Reaching it would need capturing a ~1.4 million-trace
seed, and shrinking container memory instead would fail the readiness gate and make the result incomparable to the
Phase 6 series.

The integration runs cover more of what matters here and cost far less: aggregate concurrency across hot lanes and the
finalize round, which a single-merge run never exercises; real trace IDs; and end-to-end query behaviour. What they give
up is reproducibility — live traffic is not replayable, so there is no byte-identical paired comparison. That trade is
accepted deliberately: the claims at stake are about a ceiling that either fires or does not, and about index exactness
when it does, neither of which needs byte-identical inputs to establish.

**Exit.** Run A's headroom ratio recorded; Run B's capped path clean on index exactness, query results, and bounded
residency; undeleted-trace count recorded. Until both runs have reports, DS-5 is code-complete but **not** closed: the
ceiling is live in production with no workload-level evidence behind it.

**Rollback.** Revert this ticket alone. The mechanism returns to inert; no other ticket needs touching.

---

## Independent

### DS-9 — Arena-backed IDs

**Size.** M. **Depends on.** DS-1. Independent of everything else, but worth landing **before** DS-5: it raises the
ceiling by about 24 percent, so it directly improves the deletion bound that DS-5 will measure.

**Scope.** Replace `ids []string` with a byte arena plus `uint32` offsets. The Phase 6 index, the fingerprint check, and
the exact `bytes.Equal` confirmation are untouched — the probe becomes `arena[offs[i]:offs[i+1]]` instead of
`convert.StringToBytes(ids[i])`, same single probe and single comparison. `bodyBytes` becomes exactly `len(arena)`, so
`maxIDsForBudget` stops depending on a size-class estimate.

**Out of scope.** Replacing the index with binary search. Explicitly rejected: spec §10.

**Files.** `banyand/trace/drop_set.go`, `banyand/trace/drop_set_test.go`.

**Tests.**

- Existing `drop_set_test.go` assertions pass unchanged, including prefix/extension non-matching, fail-open on malformed
  input, and the ascending-order and duplicate-suppression behaviour of `add`.
- `BenchmarkDropSetLookup` at 1 / 35 / 99 percent shows no regression against DS-1's baseline; the ticket states the
  tolerance and sample count it used. A regression outside noise blocks the ticket — this path was tuned in Phase 6 and is
  not worth trading for memory the ceiling already bounds.
- `BenchmarkDropSetResidency` shows the expected bytes-per-entry reduction; `maxIDsForBudget` rises accordingly.
- `TestProjectedIndexBytesMatchesBuildIndex` still exact.

**Exit.** No lookup regression; residency reduced; DS-1's projection test still exact.

---

### DS-11 — Documentation

**Size.** S. **Depends on.** DS-5.

**Scope.**

- `docs/design/post-trace-pipeline.md:630-632` — a merge may stop dropping at its ceiling; the pruning predicate, lockstep
  publication, and atomicity are unchanged.
- `docs/design/trace-pipeline-merge-optimization-plan.md` Phase 6 — the collector is now ceiling-bounded; the phase's
  exactness invariant is unchanged, not revised.
- Operator documentation: the retained-by-ceiling counter and what to do about it (raise memory), the ordering bias of
  spec §4, and — the item an operator is most likely to be surprised by — that a ceiling-bound shard will not shrink as
  much as the sampler's configuration implies (spec §12 risk 2).

**Tests.** None. Review-only.

---

## Deferred

Prospective tickets, not part of this series. Each corresponds to a spec §9 subsection, which holds the numbers and the
reasoning. Listed in prerequisite order; none is required for the series above to be complete and correct.

| Ticket | Buys | Prerequisite | Spec |
| --- | --- | --- | --- |
| **DF-1** Per-lane finalize ceiling | ~`CPUs`x the lifetime deletion bound (~2.7 M to ~21 M traces per shard at 4 GiB / 8 CPUs) | DS-5's retained-by-ceiling number showing the bound is being hit | §9.1 |
| **DF-2** Continuation rounds | Removes the ingest gate so leftover work clears itself | DF-1 — raising the ceiling is cheaper per unit of deletion recovered than adding rounds | §9.2 |
| **DF-3** Sampling watermark | Makes DF-2's round count `ceil(droppable / maxIDs)` and its plugin cost proportional to the shard rather than to shard × rounds | DF-2 | §9.3 |
| **DF-4** Bloom-filter pruning | Removes the deletion bound entirely rather than raising it | A refillable Phase-1 query limit — a query-layer change, worth doing on its own merits | §9.4, §8 |

DF-2 is the one to scope carefully when its turn comes: it adds persisted finalize state, changes two scanner predicates
that must move together, and reintroduces the finalize write amplification that spec §5.3 currently avoids.

---

## Decisions to settle before DS-5

1. `dropSetAggregateDivisor = 16`. It sets both the memory bound and, through spec §5.2, the lifetime deletion bound.
   DS-2's table encodes whatever is chosen, so changing it later means changing that table.
2. Whether DS-9 lands before DS-5. Recommended: yes, so the deletion bound DS-5 measures is the one the design will
   actually ship with.
