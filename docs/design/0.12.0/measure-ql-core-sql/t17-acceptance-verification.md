# T17 revised acceptance harness verification

Implemented on `a8c376f0`, verified on 2026-10-18. This applies the approved
[concurrent admission criteria](implementation-tickets.md#concurrent-admission-acceptance-revised-2026-10-18).
Only benchmark/reporting code, tests, launch scripts and documentation change.
Production execution, protobuf, memory protection and container limits are unchanged.

## Report contract

Revision `2026-10-18-v1` separates `performance`, `overload` and `recovery`.
Four timed variants remain: raw full-sort/top-k and composed queries at 1/8 callers.
Any warmup or timed query failure invalidates the timed cohort. Every response is
retained and checked exactly after timing/profiling stop, rather than checking
only six complete responses. Required sample counts, work metrics, allocations,
profiles and cleanup evidence remain enforced.

The timed runner requires `DQB_T17_OVERLOAD_REPORT`, validates the complete
artifact and records its SHA-256. The merged JSON and Markdown retain the
component verdicts, revision and evidence reference on each timed variant.
The 1,024-row overload artifact is an explicit shared admission reference for both
timed cardinalities, not a claimed 32,768-row overload measurement.

Historical five-variant and `partial_known_32_blocked` reports retain their old
validation rules. New runs reject `DQB_T17_BLOCKED_32_REPORT`; old rejection-only
proof cannot establish revised recovery acceptance. Mixed historical/revised
matrices, changed evidence, missing timed cohorts, incomplete responses and
fabricated overload timing fields are rejected.

`performance=pass` establishes a complete, correct measurement cohort and its
work/resource checks. It does not establish a five-repetition statistical result,
full G1 compatibility acceptance, large-input throughput or historical T02 acceptance.
Keeping every response also changes client retention from the original protocol;
old/new protocol timings and allocations are not an unchanged-baseline comparison.

## Overload and recovery

`TestQLBT17Overload` checks observed cgroup limits: four CPUs, 8 GiB RAM and zero
additional swap. Both sort modes run 96 calls in three waves of 32 callers using
the same three bindings. Each call must either return the exact oracle result or
the specific native broadcast `ResourceExhausted` rejection. Other failures or
partial results fail verification. After cleanup, all three bindings must succeed
through the same cluster RPC without changing limits or restarting the cluster.

The actual fixed-container run passed in 11.08 seconds:

| Sort mode | Attempts | Exact successes | Capacity rejections | Peak in flight | Recovery queries |
|---|---:|---:|---:|---:|---:|
| Top-k | 96 | 94 | 2 | 32 | 3/3 |
| Full sort | 96 | 93 | 3 | 32 | 3/3 |

All recorded cleanup counters return to zero after overload and recovery. A
separate deterministic probe uses the real protector, tracker and receive scope;
it holds reservations until rejection, verifies rollback and transport ownership,
then checks full release and a successful subsequent admission. It does not pin
exactly 31 admissions as a permanent contract. This proves rejection even when
RPC scheduling happens to admit all calls.

Evidence: `.omx/t17-revised-overload.log` and
`.omx/t17-revised-overload/t17-overload-1024.json`.

## Timed-path verification

One complete verification matrix used the prebuilt querymetrics executable in
4-CPU/8-GiB/no-extra-swap Docker, with independent processes for each cardinality.
Every measured query succeeds and every response passes exact post-timing checks:

| Input rows | Full sort | Top-k | Composed, 1 caller | Composed, 8 callers | Total |
|---|---:|---:|---:|---:|---:|
| 1,024 | 100 | 100 | 96 | 96 | 392 |
| 32,768 | 20 | 20 | 96 | 96 | 232 |

All 624 timed calls pass, and all eight final report rows carry separate passing
component verdicts. No rejected call contributes a timing/throughput sample.
This single matrix verifies the harness, not a new performance improvement claim.

The first report merge dropped scenario-level acceptance metadata when flattening
variants. `TestQLBT17MergedReportPreservesAcceptance` reproduced that defect before
the fix. The fixed merger preserves the verdicts and evidence in JSON and Markdown.
Only the untimed merge was rerun after this reporting fix; the original successful
timing shards and profiles were preserved. Original and merge-binary digests are
recorded separately in `.omx/t17-revised-provenance.json`.

Evidence: `.omx/t17-revised-rpc.log`, `.omx/t17-revised-rpc/shards/`,
`.omx/t17-revised-rpc/distributed-querybench.{json,md}`,
`.omx/t17-revised-merge-red.log` and `.omx/t17-revised-merge-final.log`.

## Checks and remaining scope

The querybench package suites with and without `querymetrics`, focused T17 race
checks, scoped lint, shell syntax checks and whitespace checks pass. The contract
suite checks missing/changed evidence, unexpected errors, wrong results, leaked
resources, incomplete recovery, missing observed limits, invented timing fields,
legacy/revised protocol mixing and final-report propagation.
Logs: `.omx/t17-revised-final-unit-{normal,tagged}.log`,
`.omx/t17-revised-final-race.log`, `.omx/t17-revised-final-lint-v2.log`.

The revised overload/recovery contract has direct evidence. The separate
100k/1m successful RPC throughput, full compatibility performance comparisons and
historical T02 acceptance gaps remain open. Spilling and retained-result sessions
remain deferred. The implementation does not broaden the benchmark workload.

## Subsequent benchmark campaign

The [five-repetition campaign](t17-benchmark-campaign.md) after commit `1179c97a`
passed all 3,120 timed RPCs. The separate 100k probe demonstrated bounded native
input rejection and recovery; the 1m probe failed fixture readiness with a content
mismatch and provides no throughput evidence. Both outcomes remain explicit.
