# T08 native aggregate branches and keyed merge

Historical ticket-stage record. Source revisions, measurements and limitations
below retain their original context. See the [PR 2 verification record](pr2-verification.md)
for the current isolated aggregation branch and refreshed measurements.

Implementation base: `89952fae` (T07), on PR #1412. Verification uses Go 1.26.9.
The change is confined to QL planning, the QL execution adapter and verification.
It changes no protobuf, RPC, persisted format, native operator, frame codec or
memory-protector policy. Disk spilling remains deferred.
Frozen benchmark source/contract hashes are in `.omx/t08-source-manifest.sha256`;
the final working-tree hashes are in `.omx/t08-final-source-manifest.sha256`.
Only a resource-test helper's context-argument order changed after timing, to
meet revive's convention. Production, benchmark and generated-contract hashes
are unchanged between the manifests.

## Execution contract

An extended aggregate query keeps its complete T07 local plan. Binding can also
construct a native alternative when the existing provider can preserve its SQL
semantics. The liaison chooses between these alternatives after the parent
revision gate and creation-time clamp. Selection reads current watched lifecycle
options on every request, including cache hits, and rechecks before each branch.
No cached template retains bound values, native sources or mutable operator state.

Eligible float SUM/AVG calls lower to native SUM and non-null COUNT. Matching
visible SUM/COUNT/AVG/MEAN calls reuse those same two branches. Integer MIN/MAX
use their native calls and share one auxiliary COUNT. COUNT zero normalizes
finalized native SUM/extrema sentinels to SQL null. COUNT returns int64 even
when the native field's COUNT representation is float64. QL never invokes native
MEAN to implement extended AVG.

Branches run sequentially, with zero offset and an unbounded result limit. Each
source closes before the next opens. The first source opens before local state
allocation so the native processor initializes the shared tracker with its real
configured allowance. Every branch and the merge reuse the parent context,
deadline, admission lease and tracker; none receives a fresh full allowance.
Child revision failures fail the query rather than returning incomplete groups.

The merge indexes the complete typed group tuple, retaining owned key bytes and
dense state arrays. It accepts independently ordered branch rows and unions
groups missing from a source. Type/null markers and variable-width lengths
distinguish null, zero and empty values. Presence markers reject duplicate
finalized keys and incomplete positive-count values. Output starts only after
every branch finishes successfully. Projection and final LIMIT/OFFSET precede
the existing response serializer; intermediate DataPoints remain zero.

Retained key/state/table growth reserves memory before allocation, including
overlap during growth. Cancellation, source/open/close errors, caps, malformed
schemas and allocation failures close sources and release local reservations.
The implementation adds no pool. The existing lease is monotonic; released live
memory does not refund its cumulative charge.

## Provider bounds and fallback

The native implementation has contracts that a SQL optimizer must respect:

| Native behavior | T08 decision |
| --- | --- |
| Integer SUM wraps instead of checking overflow. | Integer SUM/AVG stays local unless a group has at most one possible timestamp. |
| Float COUNT is returned as float64. | Prove a count bound no greater than 2^53; validate finite, integral, nonnegative results against it, then convert to int64. |
| Native MEAN clamps sub-one results; integer MEAN truncates. | Use SUM/COUNT and QL float64 division. |
| Native floating extrema use finite sentinels and ignore NaNs. | Float MIN/MAX stays local. |
| Native group-key encoding does not distinguish null placeholders. | Push only entity keys proven non-null by exact EQ/IN predicates. |
| Aggregate replica dedup keys omit measure-group and lifecycle-stage identity. Local map aggregates can also collapse multiple shards into one shard label. | Require one measure group, one provable stage and one complete entity per SQL group. |

The initial native subset requires normal storage mode, one exact nonempty time
interval, and entity grouping tags from one family. Every grouped entity tag
must have a positive exact EQ/IN constraint. Every entity tag omitted from
GROUP BY must have one exact bound value. Scalar aggregation requires one
complete bound entity. Non-entity keys, tag aggregates, residual time filters,
multi-group requests, index mode and uncertain stage selection use the complete
T07 fallback. More than 32 distinct native leaves also selects that fallback.

Normal writes are millisecond-aligned; native raw scans deduplicate versions at
(complete entity, timestamp). A group's count therefore cannot exceed the number
of millisecond positions in its half-open bound interval. Binding computes that
bound without signed subtraction overflow and requires it to fit exactly in
float64. A result LIMIT cannot provide this proof because it limits groups.

Single-stage selection permits implicit hot storage when the group defines no
stages, or one explicitly configured non-hot stage with a nonempty selector.
Explicit/default hot selectors, unknown or multiple stages and missing watched
options fall back. Tests replace watched options while retaining the same bound
plan to verify that this decision is not cached.

The first real E2E run exposed a separate native predicate limitation: an AND
of entity IN lists discarded valid Cartesian selections and failed with
`series is required`. QL now lowers these conjunctions to a balanced OR of
complete assignments for both native and local paths. Repeated constraints
intersect; contradictions and literal empty IN produce empty SQL input.
Expansion is capped at 512 tuples. Existing empty-array parameter rejection
remains unchanged. The native entity parser itself is unchanged.

## Generated E2E evidence

The generator adds 30 T08 scenarios with three bound windows each, one integer
overflow rejection and 20 raw QL visibility witnesses over two fixtures. Every
expected result comes from fixture arithmetic, never a native Query RPC. Cached,
bypassed and forced-local executions use the same SQL contracts. The fixture
includes versioned writes, two entity keys, fractional/negative/all-null values,
large precise integers, and 129 groups to detect native default truncation.
Indexed non-entity keys are constant within each entity, matching their native
index substitution contract.

The scenarios check native branch reuse, exact COUNT normalization, hidden and
qualified tuple keys, scalar/all-null/empty results, weighted averages, unsafe
fallbacks, LIMIT 0, cap failure, first/later branch errors, cancellation,
authorization revocation, child revision failures and successful recovery.
Structural counters distinguish one QL merge from its two/three native batch
requests and from one local raw scan. An added Compile/Bind contract keeps the
residual-time scenario disjoint for every changed binding.

Both final 4 CPU/8 GiB Docker topology runs pass all 246 selected specs: 31 T08
and 215 earlier ticket specs. Reports are `.omx/t08-standalone-final.json` and
`.omx/t08-distributed-final.json`. The full tagged planner/operator/liaison suites
also pass with race detection. Lifecycle guard tests run in normal and tagged
builds. Final validation logs are listed below.

The merge has an operator test retaining a bucket timestamp in its composite
key. Extended QL TIME_BUCKET remains explicitly rejected by T07; selectable
bucket planning and its real-RPC coverage belong to T16. Thus the original T08
`bucket-and-multiple-keys` E2E item is not claimed as passed here. Multiple entity
keys are covered through both real topologies now. Separate native branches do
not establish a common snapshot during concurrent writes.

## Focused benchmark protocol

K(T08), `BenchmarkNativeAggregateMerge`, compares the complete batch merge with
an independent scalar map reference for SUM/COUNT/AVG. It varies only 1/1,024/
65,537 finalized groups and aligned/permuted native branch ordering. All keys,
values, nulls and shared-lane counts are checked before timing. The column path
reports two branches, 2N input group rows and 3N merge visits, plus state/key
capacity and allocation cost. Run five one-second samples per variant in the
same 4 CPU/8 GiB Docker limit.

P(T08), `qlb_t08`, compares native and forced-local execution of identical QL,
plus equivalent individual native Query RPC bundles. It has only four shapes:
float AVG, shared float SUM/COUNT/AVG with a repeated AVG, integer MIN/MAX/COUNT,
and an integer AVG fallback control. Eleven variants run at 1,024 and 32,768 raw
rows over 16 entity hosts. The frozen schedule is 100 timed/5 warmup queries at
1,024 and 20 timed/3 warmup queries at 32,768, one worker, five fresh processes
per cardinality, a 256-MiB query tracker, and CPU/heap profiles. Setup/readiness
and complete independent value checks are outside timing; retained timed samples
are checked afterward. The individual control measures an entire sequence of
two/three RPCs rather than one native call. These controls are not E2E oracles.

The benchmark validates actual branch/scan counts, aggregate/merge state size,
input/output visits, serialization counts and post-close reservations, native
results, owner pins and flatten-pool references. Native source/frame counters
are logical instrumented work, not physical storage reads. Total parent lease
charges and native retained frame bytes are unavailable and remain labeled so.

No earlier ticket benchmark matrices are repeated, and no universal pushdown
speedup or retrospective T02 compatibility pass is assumed.

K(T08) completed all 12 variants with five samples each in
`.omx/t08-kernel-docker-final.log`; medians are in
`.omx/t08-kernel-median.json`. No other test or benchmark ran concurrently.

| Finalized groups | Branch row order | Batch merge | Scalar reference | Batch allocated bytes |
| --- | --- | --- | --- | --- |
| 1 | aligned | 5.713 us | 0.127 us | 4,096 |
| 1 | permuted | 5.715 us | 0.127 us | 4,096 |
| 1,024 | aligned | 703.622 us | 229.004 us | 160,472 |
| 1,024 | permuted | 703.077 us | 234.317 us | 160,472 |
| 65,537 | aligned | 54.480 ms | 24.642 ms | 12,632,832 |
| 65,537 | permuted | 56.306 ms | 24.608 ms | 12,632,837 |

The general merge is slower than this specialized two-int-key scalar map.
It performs typed key encoding, tracked allocations, source validation and
batch output construction. The scalar reference uses concrete Go struct keys
and has no source/batch lifecycle. These measurements do not establish a kernel
speedup. They verify independence from branch order and linear work: two native
branches, 2N input group rows and 3N total merge visits. At 65,537 groups, used
state is 1,179,666 bytes and allocated state is 2,359,296 bytes, including presence
markers. Key capacity is 5,242,976 bytes. All tracked reservations return to zero.

P(T08) completed five fresh repetitions for each cardinality: 110 measured
variants in total, with all independent value checks, structural counters and
post-close resource checks passing. There are 220 nonempty CPU/heap profiles.
Reports and raw latency samples are under `.omx/bench-reports/t08-r1` through
`t08-r5`; `.omx/t08-rpc-summary.json` preserves per-run summaries. The following
numbers are medians of the five per-run measurements. A ratio below one favors
pushdown. The fallback control uses the same local path in both modes.

| Raw rows | Shape | Pushdown p50 ms | Local p50 ms | p50 ratio | Allocation ratio | CPU/query ratio |
| --- | --- | --- | --- | --- | --- | --- |
| 1,024 | float AVG | 4.407 | 3.933 | 1.120 | 0.757 | 1.337 |
| 1,024 | shared float SUM/COUNT/AVG | 4.275 | 4.130 | 1.035 | 0.777 | 1.295 |
| 1,024 | integer MIN/MAX/COUNT | 5.839 | 3.849 | 1.517 | 1.128 | 1.856 |
| 1,024 | integer AVG fallback | 3.760 | 3.804 | 0.988 | 1.002 | 0.989 |
| 32,768 | float AVG | 20.669 | 49.449 | 0.418 | 0.213 | 0.577 |
| 32,768 | shared float SUM/COUNT/AVG | 20.126 | 49.499 | 0.407 | 0.211 | 0.572 |
| 32,768 | integer MIN/MAX/COUNT | 31.067 | 50.043 | 0.621 | 0.322 | 0.846 |
| 32,768 | integer AVG fallback | 50.301 | 52.228 | 0.963 | 1.005 | 0.958 |

At 32,768 rows, the five paired p50 ratios range from 0.406 to 0.432 for float
AVG, 0.392 to 0.452 for shared float calls and 0.588 to 0.648 for integer extrema.
Float allocation drops from roughly 48.9 MB to 10.3 MB/query; integer extrema
drops from 48.5 MB to 15.6 MB/query. This is the benefit of returning finalized
native groups instead of raw values to QL, despite the repeated native scans.

Small queries expose branch startup and repeated scan costs. Their paired p50
ratios range from 0.969 to 1.211 for float AVG, 0.975 to 1.165 for shared float
calls and 1.481 to 1.632 for integer extrema. The latter is a consistent
regression: three native branches cost more than one small raw scan. Safe native
eligibility is not a cost model, and T08 does not claim that every eligible query
is faster. The identical fallback controls range from 0.940 to 1.034 at 1,024
rows and 0.947 to 1.034 at 32,768, illustrating remaining run variation. Only
20 latency samples per large variant are retained in each run, so these runs
do not establish a precise tail-latency acceptance interval.

Individual native controls measure two/three serial Query RPCs, not one call:

| Raw rows | Float AVG bundle p50 ms | Shared float bundle p50 ms | Integer extrema bundle p50 ms |
| --- | --- | --- | --- |
| 1,024 | 4.480 | 4.648 | 6.475 |
| 32,768 | 20.627 | 19.742 | 32.615 |

Every pushed request merges 32 finalized input rows for two branches, or 48 for
three, into 16 groups. The corresponding merge visits are 48 and 64. Shared
float calls still use two state lanes and two branches; integer AVG still uses
one raw scan. Reservations/release totals match and all measured post-close
reservation, owner, result and pool-reference deltas are zero. Structural T08
work/resource checks pass; the original bucket E2E item remains T16 work and
prior tickets' compatibility decisions are unchanged.

## Final validation

- Both real topologies: 246 selected specs each, including 31 T08 specs, under
  4 CPU/8 GiB limits (`.omx/t08-{standalone,distributed}-final.json`).
- Full normal planner, operator, liaison, generator and querybench packages:
  `.omx/t08-normal-final.log`.
- The same full packages with `-race -tags=querymetrics`:
  `.omx/t08-race-final.log`. The context-argument-only test fix was checked again
  with the focused resource race test (`.omx/t08-resource-style-final.log`).
- `make lint`: `.omx/t08-make-lint-verified.log`; formatter diff is empty in
  `.omx/t08-format-diff-final.log`.
- `make check-ql-test-cases license-check`:
  `.omx/t08-generated-license-final.log`. License-eye selected zero files in this
  local invocation; all added Go files have the repository's ASF header.
- Decoded generated cases, reuse/aggregate scenarios and readiness, rejections
  and the pair report are unchanged from `89952fae`. T08 contracts are additive.

`make check` validates license headers and repository formatting against a clean
committed tree. Runtime reports, raw samples, profiles and source manifests listed
here are local verification artifacts; this document retains their measured
outcomes in the repository.

## Resource probes

Public MergeNativeAggregate tests execute 100,000 and 1,000,000 finalized groups
at 16/256-MiB tracker limits. Successful output is checked key by key against an
independent scalar oracle. A rejected query must return the budget error before
any output; Close must return live reservations to zero and a small query must
succeed using the same tracker afterward.

| Finalized groups | 16 MiB | 256 MiB |
| --- | --- | --- |
| 100,000 | Complete and correct | Complete and correct |
| 1,000,000 | Budget rejection at 131,072 retained groups; no output | Complete and correct |

All four cases pass in normal and tagged race builds. Evidence:
`.omx/t08-merge-resource-normal.log` and `.omx/t08-merge-resource-race.log`.
The caller-owned input fixture and oracle are outside this operator tracker;
these probes verify retained merge/output state, not native frame accounting.
Real RPC cap/error/cancellation cases and the P(T08) resource counters verify the
integration boundary separately. Prior T07 native readiness/resource blockers
and the deferred spilling decision remain unchanged.
