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
suite retains its previously merged explicit 1 GiB server query allowance.

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
