# Catalog B PR 4: grouping and result shaping

This branch integrates main at `86659118` with the expression/filter dependency
from [PR #1432](https://github.com/apache/skywalking-banyandb/pull/1432) at
`349222f7`. PR #1432 is still open as this record is prepared. The PR 4 extraction
baseline is `4682fc5f`; changes after it add T15 DISTINCT, T16 computed/ordinal
grouping and selectable time buckets, T17 ordering/pagination, and exact
count-distinct composition. The PR must follow #1432 when merging.

## Behavior and boundaries

```text
parameterized QL -> reusable plan -> fresh bindings and native scan
  -> WHERE -> complete aggregation -> HAVING -> visible expressions
  -> DISTINCT -> typed multi-key sorting -> final OFFSET/LIMIT
  -> existing measure response
```

Computed group keys evaluate before aggregation. Exact count-distinct retains
complete value sets and never sums overlapping native partial counts. DISTINCT
uses the visible tuple, with shared memory accounting and early stopping only
where unordered finite pagination permits it. Finite ordered pages retain at
most OFFSET + LIMIT owned candidates; unlimited ordering uses budgeted full sort.
NULL placement and deterministic tie-break keys are explicit query choices.

Existing native queries, lifecycle-safe empty scalar-count completion and native
memory protection retain their merged behavior. Latest main's native schema
coercion, batch merge and array fixes are preserved. This PR adds no protobuf,
frame format, persisted storage, toolchain or workflow change. The standalone
suite now declares a 4 GiB server allowance, giving these larger combined
fixtures a bounded 128 MiB per-query admission limit; the diagnosis is below.

JOINs, CTEs, directory synchronization and full DPO application parity are outside
this PR. Parent-child generated cases operate on one measure. Spill and retained
page sessions remain deferred. Historical T02 uncertainty and deferred successful
100k/1m owning-query throughput are not changed by this verification.

## Verification on the isolated branch

Validation is in progress. Final results and scoped benchmark measurements will
be recorded here before publishing the PR. Historical ticket evidence remains
in [T15](t15-verification.md), [T16](t16-verification.md),
[T17](t17-verification.md), the [T15 memory follow-up](t15-memory-verification.md)
and the [count-distinct follow-up](implementation-tickets.md#dpo-81-count-distinct-composition-follow-up).
These historical records do not certify the current PR head.

## Standalone fixture allowance diagnosis

The initial standalone run passed 917/935 selected cases; 18 DISTINCT/sort cases
hit the previously configured 32 MiB per-query admission limit. A single
`t15_scalar_zone` reproduces without preceding tests. A temporary reservation
probe recorded used=33,536,457 bytes, next charge=25,680, assigned=33,554,432;
the shared 256 MiB pool held only that query's reservation. The rejection came
from the native scan's cumulative admission charge, not exhausted process RAM.

With the fixture's explicit server allowance changed from 1 GiB to 4 GiB, the
per-query limit is 128 MiB. The DISTINCT, bulk top-k and cancellation controls
pass 3/3; successful control reservations reach 66,160,761 bytes (about 63 MiB).
The Docker limit stays four CPUs/8 GiB/no extra swap. Production protection and
small-budget operator rejection tests are unchanged. This configuration change
is not a memory optimization or a revised performance-gate pass. All temporary
protector instrumentation was removed before final validation. Red/probe/control
logs are `/mnt/d/pr4-{distinct-red,budget-probe,budget-control}.log`.

## Pre-existing main test failure

The broader liaison race suite fails `TestExportRelay_TwoRelaysShareOneRate`.
The isolated test also fails on unchanged main `86659118` (one of three runs):
a relay emits a 98,304-byte piece after its peer finishes, while the test expects
every piece to remain at most 65,536 bytes. The limiter computes piece size from
the currently active relay count. Export code/tests are identical on main and
this branch; no export fix is included. Base evidence is
`/mnt/d/pr4-base-export-control.log`. Focused measure/QL/cache liaison race tests
pass. This record does not describe the broader liaison suite as passing.
