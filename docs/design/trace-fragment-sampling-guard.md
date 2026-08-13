# Trace Fragment Guard for Sampling Design

## Status

Core enforcement implemented using the resolved merge-grace safety contract.

## Context and Problem Statement

BanyanDB's trace merge policy selects file parts primarily by compressed size to control write amplification. The
selected parts are merged in trace-ID order, and an enabled post-trace sampler may discard traces from the merge output.

A trace can have fragments in both selected and unselected parts. Sampling only the selected fragments can therefore
produce an incorrect partial decision: the selected fragment may be dropped while another fragment remains visible in an
unselected part. Expanding every merge selection to all timestamp-connected parts avoids some partial decisions, but
production data shows that broad and transitively overlapping part ranges can expand a small merge into a full-shard
rewrite.

This design retains the existing size-based part selection and adds a per-trace boundary guard. Part timestamps cheaply
identify the unselected parts that could contain another fragment. Existing trace-ID Bloom filters then confirm whether
those candidate parts might contain the trace. Bloom filters are consulted only for traces that the sampler
provisionally wants to drop.

This document refines the in-merge filtering semantics described in the post-trace pipeline design. In particular, grace
time is not treated as proof of trace completeness unless the deployment enforces a corresponding fragment-gap or
sealing contract.

## Design Boundary

### Execution Boundary

The guard applies to destructive trace filtering during both the Hot-stage merge event and finalization. It does not
change the current size-tiered merge picker, merge fan-out, output part structure, or the lossless behavior of merges
that have no active sampler. Finalization-wide selection and scheduling remain separate concerns, but a finalization
DROP is subject to the same outside-catalog and publication checks as an ordinary merge DROP.

### Data-Visibility Boundary

The guard reasons about a pinned shard snapshot. Its universe includes:

- selected file parts;
- unselected file parts;
- hot parts;
- memparts;
- parts already participating in another in-flight merge;
- newly visible parts discovered during pre-publication revalidation.

Parts that cannot be inspected remain uncertainty sources and cause affected traces to be retained for the current
merge.

### Time Boundary

Part minimum and maximum timestamps are span event-time bounds, not ingestion-time bounds. The temporal pruning step is
strictly safe only when grace is at least an enforced maximum separation between fragments of the same trace. Sealing
an event-time range prevents future arrivals, but does not prove that an already-visible same-trace fragment is absent
from a part farther than grace.

This document calls the value used for interval expansion guard grace. The implementation uses the resolved per-group
`merge_grace` for both interval expansion and the whole-selection maturity delay. A deployment must ensure fragments of
one trace do not arrive farther apart than this grace; the engine cannot infer or enforce that ingestion property.

The engine default for merge grace is 2h when a group does not configure a positive override. A merge must resolve the
group override or engine default once and carry that immutable value through filtering and publication revalidation.
The current runtime uses merge grace as a whole-selection maturity gate: one selected part newer than `now − grace`
makes the entire merge lossless. Because compaction is part-count-driven rather than timer-driven, an ineligible merge
is not guaranteed to run again at the two-hour frontier. Its effective filtering delay is at least two hours plus the
wait for a later eligible compaction; finalization is an independent backstop.

Using one value makes the configured maturity contract match the boundary checked before a destructive DROP. It also
permits an early whole-merge bypass when a segment is too narrow to contain any trace range expanded by merge grace,
avoiding projection, staging, sampler, and Bloom work when no DROP could be authorized. Without a trustworthy
fragment-arrival contract, operators should not enable destructive sampling for that group.

The design uses symmetric time expansion because the size-based picker can select an arbitrary temporal subset. An
outside fragment may be earlier or later than the selected fragment.

### Concurrency Boundary

A stable snapshot and pre-publication epoch validation cover parts that become visible while a merge is running. They do
not prove that no future late or backdated fragment will arrive after publication.

Strict protection against future arrivals requires an ingestion watermark, a sealed segment, or a policy that rejects or
quarantines late fragments. Defining that cluster-wide late-write protocol is outside this design.

### Segment Boundary

The current trace table is scoped to an event-time segment, while one trace can cross adjacent segments. A trace whose
grace-expanded range reaches a segment boundary is retained unless the relevant neighboring segment is included under
the same visibility and ownership fence.

### Failure Boundary

All uncertainty fails open. A missing timestamp, invalid range, missing or corrupt Bloom filter, sampler failure,
snapshot conflict, ownership change, or incomplete fragment assembly causes retention rather than destructive deletion.

Retention by this guard is merge-local deferral. It is not a permanent KEEP decision and does not prevent the trace from
being evaluated in a later merge or finalization pass.

## Goals

1. Prevent a sampler DROP from removing selected fragments when a currently visible unselected part may contain the same
   trace ID.
2. Preserve the existing size-based input selection, input fan-out, and direct rewrite scope.
3. Avoid probing every unselected Bloom filter for every merged trace.
4. Apply Bloom confirmation only to provisional sampler DROP traces and only against time-relevant outside parts.
5. Assemble all selected physical fragments of a trace before calculating its time range or invoking the sampler.
6. Keep core-part and secondary-index publication consistent when a trace is deferred or dropped.
7. Bound additional CPU and memory through per-merge indexes, batching, and early termination.
8. Make uncertainty observable and fail open.

## Non-Goals

1. Replacing the current size-tiered merge policy with leveled compaction.
2. Physically colocating every fragment of a trace in one part.
3. Guaranteeing that no fragment can arrive after a merge commits.
4. Defining a cluster-wide event watermark or late-write rejection protocol.
5. Eliminating Bloom-filter false positives.
6. Removing the need for finalization as a backstop.
7. Changing sampler policy, plugin ordering, or plugin projection semantics.
8. Providing permanent trace-level KEEP or DROP tombstones.
9. Solving arbitrary trace-ID reuse within the retention period.
10. Specifying backward compatibility for file parts that do not contain persisted per-trace timestamp bounds.
11. Guaranteeing unchanged output bytes or lifetime write amplification when conservative deferrals retain more traces.

## Terminology

| Term | Meaning |
|---|---|
| Selected parts | File parts chosen by the existing merge policy. |
| Outside parts | All visible parts in the relevant shard and segment scope that are not selected by this merge. |
| Selected trace range | The minimum and maximum event timestamps across every selected physical fragment with the same trace ID. |
| Guard grace | The interval expansion used to find time-relevant outside parts. |
| Guard range | The selected trace range expanded earlier and later by guard grace. |
| Candidate outside part | An outside part whose part-level event-time range intersects the guard range. |
| Provisional DROP | A DROP returned by the sampler before boundary confirmation. |
| Deferred trace | A trace retained in this merge because an outside fragment is possible or the guard is uncertain. |
| Base epoch | The snapshot epoch used to select inputs and construct the boundary guard. |

## Design Invariants

1. The merge picker receives the same eligible file parts and produces the same selected input set as it did before this
   guard.
2. All physical blocks with the same trace ID in the selected inputs form one logical evaluation unit.
3. A sampler KEEP is final for the current merge and requires no Bloom check.
4. A sampler DROP becomes effective only after boundary confirmation.
5. Bloom filters are never used as evidence of absence when they are missing, unreadable, or incompatible.
6. A Bloom positive causes deferral unless an optional exact metadata lookup proves the trace ID absent.
7. A valid Bloom filter contains every trace ID in its immutable part and cannot produce a false negative.
8. Part-level time ranges may create false candidates but must not exclude timestamps actually stored in that part.
9. A trace with missing or invalid per-trace timestamps is deferred.
10. A selected trace is never partially written to the output.
11. Secondary-index entries are removed only for trace IDs whose DROP survives boundary confirmation.
12. A merge output is not published against an invalidated snapshot without successful revalidation.
13. Exhausting a boundary-check CPU, memory, I/O, or probe budget causes deferral.

## Design Overview

The merge request captures and pins the selected inputs, the base snapshot epoch, the event-time coverage of the pinned
catalog, and lightweight metadata for every outside part. Outside metadata consists of the part identity, minimum and
maximum event timestamps, segment identity, availability state, and a reference to its trace-ID Bloom filter. The
catalog also records the enforced fragment gap, which is the same resolved merge grace used by the maturity gate.

Before incurring sampler work, the runtime also checks that native sampling is enabled, a sampler exists, the relevant
event is enabled, the segment has a non-empty merge-grace-expanded interior, and every selected part is old enough for
the whole-selection maturity gate. Finalization
uses the greater of merge grace and finalize grace for maturity. Failure of any check leaves the lossless path unchanged.

The merge assembles selected blocks by trace ID and computes one selected trace range. The sampler evaluates the
assembled trace. Sampler KEEP traces are written immediately according to normal ordered-output rules. For each
provisional DROP, the guard identifies only outside parts whose event-time envelopes intersect the grace-expanded
selected trace range. It probes the Bloom filters of those candidates and stops at the first positive or uncertainty.

If no candidate part exists, or all candidate Bloom filters are negative, the trace may be dropped within the documented
time and snapshot boundary. If any candidate filter is positive or unavailable, the trace is written to the output and
deferred for later evaluation.

The outside interval index is created once per merge. With the normal small part count, a compact sorted array and
integer comparisons are preferred. An interval tree is warranted only if profiling shows that part counts make linear
candidate discovery material.

## Key Process

### 1. Select and Pin Inputs

The existing merge policy selects file parts without modification. Selection and in-flight pinning retain their current
atomic exclusion behavior.

The dispatcher also captures the base snapshot epoch and the identity of every visible part. Parts excluded from merge
eligibility still remain in the outside-part universe.

### 2. Build the Outside-Part Guard

The merge request records the part-level time range and Bloom-filter reference for every outside part. Outside intervals
are ordered and indexed once.

The index preserves the identity of each overlapping part rather than only the union of time intervals because Bloom
confirmation must be directed to individual parts.

Invalid part bounds, unavailable filters, and segment or ownership ambiguity are recorded as uncertainty rather than
silently omitted.

### 3. Assemble a Complete Selected Trace

The merge stream groups all selected physical blocks with the same trace ID. The selected trace range is the minimum and
maximum across all grouped blocks.

The sampler receives the complete selected trace projection, not only the first staged block. Batch boundaries may occur
only between trace IDs.

Persisted per-block minimum and maximum timestamps are required for an efficient implementation. The current in-memory
timestamp fields are not serialized in block metadata. New file parts must persist these bounds, or the merge must
reconstruct them from the configured timestamp column. Failure to reconstruct them causes deferral.

Projection handling must enter the decoded assembly path whenever tags, span IDs, or spans are requested. Timestamp
aggregation and sampler input assembly must cover the same complete set of physical blocks.

### 4. Obtain the Sampler Verdict

The sampler chain runs with its existing fail-open behavior. A sampler error, panic, timeout, malformed mask, or
incomplete projection retains the affected batch.

A KEEP requires no boundary work. Only provisional DROP traces enter temporal pruning and Bloom confirmation.

### 5. Discover Time Candidates

The guard symmetrically expands the selected trace range by grace. It finds every outside part whose part range
intersects that guard range.

The check is inclusive at the configured boundary and uses overflow-safe timestamp arithmetic. It does not consider only
an immediate predecessor or successor because outside intervals can be nested and the selected parts are not temporally
contiguous.

If the guard range reaches a segment boundary and the neighboring segment is not covered by the same snapshot fence, the
trace is deferred.

### 6. Confirm with Bloom Filters

The trace ID is tested only against candidate outside parts. Candidate checks terminate at the first positive, missing
filter, or corrupt filter.

For cache locality, a merge implementation may group provisional DROP trace IDs by candidate part and probe one part's
filter in a batch. This optimization must preserve early deferral semantics.

An optional exact lookup in sorted primary metadata may verify Bloom positives. Exact verification is an optimization
for reducing false deferrals, not a correctness requirement.

Cancellation or exhaustion of the merge's boundary-check resource budget defers every unresolved trace.

### 7. Apply the Final Merge Action

The action matrix is:

| Sampler result | Time candidates | Bloom confirmation | Merge action |
|---|---:|---|---|
| KEEP | Not evaluated | Not evaluated | Write the trace. |
| DROP | None | Not evaluated | Drop within the documented grace assumption. |
| DROP | One or more | All negative | Drop. |
| DROP | One or more | Any positive | Write and defer the trace. |
| DROP | One or more | Missing or corrupt filter | Write and defer the trace. |
| DROP or unknown | Invalid trace range or incomplete assembly | Not trusted | Write and defer the trace. |
| DROP or unknown | Boundary-check budget exhausted or canceled | Incomplete | Write and defer every unresolved trace. |

The guard records each confirmed DROP together with the authoritative aggregate trace range it calculated. Only those
guard-owned confirmed DROP tokens are passed to secondary-index pruning and publication revalidation.

### 8. Revalidate Before Publication

Before the core and secondary-index outputs are introduced, the merge worker compares a pinned current snapshot,
selected-input presence, and table-lifecycle ownership with the captured base state. If new parts appeared, provisional
DROP trace IDs are revalidated against those parts using the same time-candidate and Bloom rules. This potentially
expensive work runs outside the serialized introducer.

The introducer then holds the publication fence and performs a constant-time epoch and ownership check. Publication is
allowed only when the epoch is exactly the one successfully prevalidated. Unavailable metadata, ownership changes, or
an epoch change after prevalidation rejects the filtered output.

Because dropped payload is already absent from a provisional output, an unsafe output cannot be repaired in place. It is
discarded, while the pinned inputs remain authoritative, and the merge is retried or performed losslessly.

### 9. Release and Retry

Successful publication releases input pins through the existing lifecycle. Rejected output files and secondary-index
artifacts are removed. The same pinned inputs are then merged once without a sampler or guard, guaranteeing compaction
progress without another destructive decision. A failed lossless retry is reported as the merge error and the original
inputs remain authoritative.

## Correctness Properties and Limitations

### Visible-Snapshot Protection

Under accurate part bounds, complete selected-trace assembly, and a stable snapshot, a Bloom-negative result for every
candidate part proves that none of those candidate parts contains the trace ID.

Temporal pruning proves that non-candidate parts can be ignored only when guard grace is at least an enforced maximum
same-trace fragment separation. A seal alone is insufficient because it says nothing about fragments already present
outside grace. Without the maximum-gap proof, enforcement defers or uses a future all-outside-filter fallback.

### False Positives

Part time ranges are envelopes over unrelated traces and can span minutes or hours. They may nominate parts that cannot
contain the selected trace. Bloom confirmation removes most resulting false deferrals. Remaining Bloom false positives
safely defer sampling.

### Late Arrivals

Epoch validation covers concurrent part introductions visible before publication. It does not cover a future fragment
arriving afterward. Operators requiring strict whole-trace semantics must pair this design with sealing or late-write
quarantine.

### Convergence

Broad part envelopes can repeatedly nominate the same outside parts. Bloom negatives allow unrelated traces to progress,
while Bloom positives defer possible fragments until a later merge includes them together. Finalization remains the
backstop for traces that do not converge through ordinary merge selection, but it uses the same guard rather than
treating a cooled partial selection as complete.

## Cost Model

The baseline all-part Bloom strategy performs approximately the number of selected traces multiplied by the number of
outside parts in Bloom probes.

The hybrid strategy performs cheap integer interval comparisons first, then probes approximately the number of
provisional DROP traces multiplied by their average number of time-candidate parts. Sampler KEEP traces incur no Bloom
work.

The captured 26-part production shard provides an initial validation fixture. Its per-trace time map contains 37,288
unique trace IDs and has SHA-256 `02eda3eed08d17590527d02fd5f56610fba5fe0bd7252f9575df9bbdd6c9df7f`.
The replay uses the latest fixture event time, `2026-07-29T23:59:56.171Z`, as its deterministic active-ingestion clock.
Using the later download time would incorrectly classify every tail part as cooled and would not reproduce the state in
which the small files were new.

With the 2h merge-grace default, the maturity frontier is `2026-07-29T21:59:56.171Z`. The twelve-part small-file window previously
used by the prototype is newer than that frontier. Production would merge it losslessly without calling the sampler or
guard: all 254 traces are retained, and drops and Bloom probes are both zero. It is also only the best scoring window,
not a dispatched default-policy merge; the frozen fixture's default picker selects no merge.

The corrected guard replay derives its inputs from metadata by selecting the six parts at or before the frontier:
`05ee`, `0b5b`, `1002`, `1191`, `165a`, and `1c36`. They are 34,856,465 compressed bytes and contain 31,832 selected
trace IDs. All 20 newer parts remain in the outside catalog. For this conservative replay, the declared maximum
fragment gap is also 2h. Only `21d4` intersects any selected trace's 2h guard range;
it contains all 31 exact selected/outside crossings.

| Strategy over all provisional DROP traces | Candidate pairs | Bloom probes | Retained | Dropped | False deferrals | False negatives |
|---|---:|---:|---:|---:|---:|---:|
| Hybrid, 2h | 6,652 | 6,652 | 34 | 31,798 | 3 | 0 |
| Time only, 2h | 6,652 | 0 | 6,652 | 25,180 | 6,621 | 0 |
| All outside Bloom filters | Not pruned | 629,241 | 900 | 30,932 | 869 | 0 |

The corrected hybrid uses approximately 1.1 percent of the all-Bloom probes while preserving every observed crossing.
A recent small part added to the six cooled inputs makes the whole selection hot, so sampling and guard work return to
zero. A large part can also be recent; event-time maturity, not file size alone, controls the gate.

In a 100-iteration, three-run prototype benchmark, median guard time was 1.09ms for the hybrid, 0.45ms for time-only,
and 33.17ms for all-Bloom. The hot small-part maturity check took approximately 31ns and performed no sampling or
guard work. All four prototype paths reported zero allocations. These numbers exclude merge I/O and sampler execution.

The cooled six-part selection is a maturity-and-guard fixture, not evidence that the default ordinary picker dispatched
those exact inputs. A production-realistic picker replay requires a historical pre-merge snapshot or a synthetic tier
with enough similarly sized, cooled parts to pass the dispatch threshold.

The maximum observed adjacent same-trace fragment gap in this fixture was 71.225s. The 2h range therefore covered the
observation, while this remains workload evidence rather than the enforced maximum-gap proof required for destructive
temporal pruning.

The merge still reads only the inputs selected by the existing policy, so the guard does not directly increase input
fan-out or rewrite scope. More conservative deferrals can enlarge output parts and cause more bytes to survive into
later merges. Total lifetime write amplification can therefore increase indirectly and must be measured.

## Observability

The implementation must expose bounded-cardinality metrics for:

- traces evaluated by the sampler;
- provisional KEEP and DROP verdicts;
- traces with no time candidate;
- time-candidate trace-part pairs;
- Bloom probes;
- Bloom negatives and early positive exits;
- Bloom-positive deferrals;
- missing or corrupt filter deferrals;
- invalid timestamp deferrals;
- segment-boundary deferrals;
- boundary-check budget deferrals;
- whole-merge maturity deferrals and age past the eligibility frontier;
- confirmed drops;
- snapshot revalidation attempts and failures;
- discarded provisional outputs;
- retries performed losslessly;
- candidate parts per provisional DROP;
- filter bytes loaded;
- boundary-guard latency.

Logs should identify the group, shard, segment, merge type, input count, base epoch, and failure reason. Trace IDs must
not be used as unbounded metric labels.

The current enforcement path exposes bounded counters for guard bypasses, Bloom probes, deferred traces, budget
exhaustion, publication rejection, and lossless retries, in addition to the existing sampler evaluated/retained/dropped
counters. Publication rejection logs include the group, shard, merge type, reason, rechecked trace count, and Bloom
probe count. Per-reason trace-level labels are intentionally avoided.

## Test Plan

### Unit Tests

1. Verify symmetric interval intersection for earlier and later outside parts.
2. Verify inclusive grace-boundary behavior.
3. Verify overflow-safe handling near minimum and maximum timestamp values.
4. Verify nested outside intervals and non-contiguous selected parts.
5. Verify temporal pruning is rejected when guard grace is smaller than the catalog's enforced gap.
6. Verify that sampler KEEP performs no Bloom probes.
7. Verify that provisional DROP with no time candidate is dropped.
8. Verify that all-negative candidate filters permit DROP.
9. Verify that a positive, missing, or corrupt candidate filter defers the trace.
10. Verify that optional exact lookup can clear a Bloom false positive without changing negative-filter semantics.
11. Verify that invalid or missing per-trace timestamps fail open.
12. Verify aggregation of minimum and maximum timestamps across multiple selected blocks with the same trace ID.
13. Verify that a trace larger than the staging budget remains one decision unit.
14. Verify that only guard-owned confirmed DROP tokens reach secondary-index pruning and revalidation.
15. Verify persisted per-trace timestamp bounds and their presence marker after close and reopen.
16. Verify sequential block reads and slow multi-block merges preserve and aggregate those timestamp bounds.
17. Verify that each persisted part Bloom filter contains every trace ID written to that part.
18. Verify that cancellation and every boundary-check budget limit fail open.
19. Verify that closing a guard releases its pinned catalog exactly once.
20. Verify that an unset group value resolves to the 2h engine default and an explicit group value takes precedence.
21. Verify whole-merge maturity at one nanosecond before, exactly at, and one nanosecond after the 2h frontier.
22. Verify exact earlier and later 2h guard boundaries.
23. Verify that finalization applies the same outside-part guard and defers a trace found in a skipped part.
24. Verify that a segment with no guardable interior bypasses projection, sampler, and Bloom work.

### Property and Randomized Tests

1. Generate random part intervals, trace fragment placements, selections, and grace values. Under an enforced
   fragment-gap bound, assert that every trace crossing selected and outside parts is either deferred or retained by the
   sampler.
2. Randomize nested intervals to ensure candidate discovery does not depend on monotonic maximum timestamps.
3. Randomize Bloom false positives and assert that they can only increase retention.
4. Inject missing metadata and assert that no uncertain trace is dropped.
5. Compare the hybrid result with an exhaustive exact trace-ID oracle for a stable snapshot.

### Integration Tests

1. Place one trace in selected and unselected file parts and verify deferral after a provisional DROP.
2. Place unrelated traces in time-overlapping parts and verify that Bloom negatives allow sampling.
3. Place an outside fragment earlier than every selected fragment and verify symmetric protection.
4. Include memparts, hot parts, and in-flight parts in the outside universe.
5. Verify ordered core output when provisional drops are overridden to deferral.
6. Verify core and secondary-index consistency for mixed confirmed drops and deferred traces.
7. Verify fail-open behavior for sampler timeout, panic, invalid mask, and projection failure.
8. Verify segment-boundary deferral when an adjacent segment is unavailable.
9. Verify adjacent-segment confirmation when both segments are covered by a common fence.
10. Verify tags-only, span-ID-only, and full-span projections use complete multi-block trace input.
11. Verify an outside mempart without a usable filter causes deferral.
12. Verify an outside in-flight part remains represented by a conservative catalog entry.
13. Verify both segment edges at exactly the 2h guard boundary and one nanosecond on either side.

### Concurrency and Recovery Tests

1. Introduce an overlapping part after selection and before publication; verify epoch revalidation prevents unsafe
   publication.
2. Introduce an unrelated part during the same window; verify successful revalidation is possible.
3. Change shard ownership during the merge; verify the provisional output is rejected.
4. Crash before output completion, after core output, after secondary-index output, and before introduction; verify the
   old snapshot remains authoritative.
5. Fail provisional-output cleanup and verify recovery removes orphan artifacts without losing input parts.
6. Race ordinary merge selection, finalization, mempart flushing, and handoff.
7. Block pre-publication Bloom revalidation and verify the serialized introducer continues accepting new parts.
8. Change the snapshot after successful prevalidation and verify the filtered output is discarded and the same pinned
   inputs are retried losslessly.

### Captured-Shard Regression

1. Replay all 26 captured parts and their 37,288 unique trace IDs at the deterministic active-ingestion clock.
2. Verify that the default picker dispatches no merge from the frozen snapshot; do not label a scoring candidate as an
   ordinary merge selection.
3. Verify that the recent twelve-part small-file window bypasses the sampler and guard with zero drops and Bloom probes.
4. Derive the six cooled inputs from the 2h frontier, assert their exact identities and 34,856,465-byte total, and retain
   all 20 other parts in the outside catalog.
5. Compare every selected/outside decision with the exact trace-placement oracle and require zero false negatives.
6. Assert 6,652 candidate pairs and Bloom probes, 34 retained traces, 31,798 drops, and three false deferrals for the
   hybrid guard.
7. Add one recent small part to the cooled selection and verify that the whole merge bypasses sampling.
8. Verify the observed 71.225s fragment gap and document that fixture observation is not a maximum-gap proof.
9. Add a synthetic narrow outside part whose same-trace fragment is beyond grace and document that it falls outside the
   time-safety contract.

### Performance Tests

1. Benchmark merge CPU with no guard, all-part Bloom probing, temporal-only guarding, and the hybrid guard.
2. Measure Bloom probes, hash cost, cache misses, and candidate discovery time as trace and part counts grow.
3. Measure sampler DROP ratios from zero to one hundred percent.
4. Benchmark linear interval scanning against an interval tree and retain the simpler structure until profiling
   justifies replacement.
5. Measure memory added by staged trace ranges, candidate mappings, and batched Bloom probes.
6. Confirm that selected input parts, merged input bytes, and output fan-out are unchanged from the current policy.
7. Run sustained ingestion with concurrent merges and verify that epoch conflicts do not cause retry storms.
8. Profile the 2h hybrid, time-only, and all-Bloom strategies over cooled inputs, plus the constant-time maturity bypass
   over recent small inputs. Measure retained output bytes, later rewrite bytes, probe-budget deferrals, finalization
   backlog, and time from maturity frontier to the next eligible compaction.

## Acceptance Criteria

1. The selected input set is identical with the guard enabled and disabled.
2. No provisional DROP is published when a candidate outside Bloom filter is positive or uncertain.
3. All selected fragments with the same trace ID receive one final action.
4. Core and secondary-index snapshots agree on every confirmed DROP.
5. Snapshot conflicts fail open without losing input data.
6. The captured-shard regression has zero false negatives against the exact placement oracle.
7. With 2h merge grace used for both maturity and boundary expansion, the cooled captured-shard case produces
   exactly 6,652 time-candidate pairs and Bloom probes, versus 629,241 probes for all-Bloom confirmation.
8. Missing timestamp metadata and segment-boundary ambiguity never produce destructive deletion.
9. Benchmarks show no increase in selected merge bytes or merge fan-out.
10. Probe-budget exhaustion retains affected traces.
11. Any selected part newer than the maturity frontier causes zero sampler calls, guard calls, Bloom probes, and drops.
12. Finalization cannot publish a DROP that the ordinary merge guard would defer for the same visible catalog.
13. Publication rejection leaves the selected inputs authoritative and completes one lossless retry.

## TDD Implementation Sequence

1. Persist the timestamp presence marker and bounds, restore them during sequential reads, and aggregate them during slow
   block merges.
2. Assemble every selected physical block for one trace into a complete sampler and guard input.
3. Build and pin the outside catalog with conservative coverage, fallible membership adapters, the base epoch, and an
   enforced gap equal to resolved merge grace.
4. Implement provisional DROP resolution, including validation, saturating expansion, interval candidate discovery,
   tri-state membership checks, cumulative probe budgets, and guard-owned confirmed DROP tokens.
5. Drive core and secondary-index pruning only from those tokens.
6. Revalidate every token against a pinned current snapshot outside the introducer, rejecting changed ownership, missing
   selected inputs, incomplete deltas, positive or uncertain filters, and exhausted budgets. Carry the validated epoch to
   the introducer for a constant-time publication-fence check.
7. Add captured-shard replay and shadow-mode integration before enabling destructive enforcement.

## Rollout

The feature should first run in shadow mode. Shadow mode records provisional sampler results, temporal candidates, Bloom
outcomes, and the final action that enforcement would take, but it does not drop traces.

After captured-shard replay and production shadow metrics confirm acceptable candidate and deferral rates, enforcement
can be enabled for a limited set of trace groups whose verified fragment-arrival bound fits within their resolved merge
grace. Rollout should monitor Bloom probes, deferred traces, epoch conflicts, merge latency, sampler
effectiveness, and finalization backlog.

Rollback disables boundary-aware destructive filtering and returns merges to fail-open retention without changing the
part-selection policy or invalidating existing parts.

## Open Questions

1. How the system will establish and expose the enforced maximum fragment-gap contract required before destructive
   enforcement is enabled.
2. Whether exact primary-metadata verification is worthwhile for Bloom positives.
3. Whether adjacent-segment metadata can be fenced cheaply enough to reduce boundary deferrals.
4. Whether persisted per-trace timestamp bounds require a format version change or can be reconstructed during
   transitional merges.
5. Whether repeated Bloom-positive deferrals require a dedicated finalization priority signal.
