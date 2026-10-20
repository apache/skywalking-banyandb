# Catalog B PR 1: execution foundation, raw QL and plan reuse

This PR delivers T02, T03 and T05 on merged PR #1412 (`15635181b`). It is the
first of four Catalog B PRs. It adds typed raw measure queries with aliases and
WHERE time, shares native columnar execution between QL and structured requests,
and reuses immutable compiled measure templates across parameter bindings.

Later PRs deliver SQL aggregation/pushdown, expressions/filters/HAVING and complete
grouping/output composition. Existing legacy aggregate syntax remains available.
No protobuf definition, RPC, persisted format or native frame version changes.

## Execution and compatibility

```text
QL text + parameters
  -> prepared syntax + bounded compiled-template cache
  -> fresh binding, requests and query clock
  -> current authorization, admission and schema/lifecycle gates
  -> native batch scan
  -> optional typed raw projection/time residual/final pagination
  -> existing measure_result
```

Native protobuf queries enter the same batch foundation directly. The cache
retains recipes and schema dependencies, not parameter values, authorization
choices or query results. Evicted templates remain accounted for while active
request handles retain them. Watch generations and revisions invalidate stale
plans; cancellation and memory limits apply to compilation and execution.

This extraction includes the shared-path compatibility repairs from the complete
Catalog B implementation: preserve indexed array values, retain deterministic
wildcard order, treat impossible/empty entity selectors correctly, preserve merge
tails before decoder reuse, and bound frame concatenation/scalar merge ownership.
Successful empty legacy QL scalar COUNT(DISTINCT) produces one integer-zero row;
grouped empty output, final pagination, errors and raw protobuf behavior are
preserved. An isolation regression also found that a scalar distinct count with
LIMIT 1 returned 3 instead of 8 in distributed execution: native per-node limits
pruned distinct partials before final reduction. QL lowering now removes input
limits for this scalar shape, including native defaults, and handles LIMIT 0 or
an offset past the single row as an empty SQL page. Native memory budgets still
bound complete input. These repairs and their regression tests belong with the
foundation.

The QL-only generator includes six empty-entity Boolean combinations and seven
scalar count/pagination cases over its neutral RID fixture. Expectations come
from fixture data and independent literal checks; no native query supplies the
expected result. Cache reuse scenarios rebind identical query text and compare
cached and bypassed execution. Existing native measure cases run separately as
compatibility controls.

## Scoped benchmark refresh (2026-10-20)

Go 1.26.9, GOMAXPROCS=4, four-CPU Docker quota, 8 GiB memory and no swap. The
host's other validation jobs were paused during timed work. Container memory
events report no OOMs. Production source hashes and executable hashes were
captured with the raw measurements.

Raw projection uses 1,024 immutable int64 rows, five one-second repetitions per
case. Medians compare a borrowed kernel with an independent scalar value-copy
reference in the same executable:

| Width | Borrowed ns/op | Copy ns/op | Borrowed B/op | Copy B/op |
|---|---:|---:|---:|---:|
| 1 | 1,148 | 8,526 | 1,024 | 8,432 |
| 32 | 9,370 | 356,500 | 7,728 | 265,856 |

Borrowed cases copy no value payload. This is a kernel comparison, not a measured
whole-PR speedup against main. Residual-time cases select half the input and are
recorded separately because their result cardinality differs from the copy
control.

Planning uses the same prepared statement and alternating bindings. Timed work
includes fresh binding and native-request construction, excluding parsing,
networking and execution. Five one-second repetitions per shape/setting:

| Shape | Cache off ns/op | Cache on ns/op | Off B/op | On B/op |
|---|---:|---:|---:|---:|
| Legacy simple | 34,074 | 4,023 | 14,212 | 1,504 |
| Typed wide | 232,327 | 15,910 | 108,520 | 7,441 |

Typed-wide on/off ratios are 0.0685 for time and 0.0686 for allocated bytes,
meeting the focused planning target of at most 0.80 for each.

The RPC comparison uses 1,024 fixture rows, two binding sets, eight workers,
20 warmups and 1,000 timed queries per variant. Cache off/on run in distinct fresh
processes; each process contains its setting's two query shapes. All 4,000 timed
queries passed result checks, and retained response samples matched independent
oracle hashes for both bindings. Each response contains 32 rows. The existing
benchmark configuration uses a 256-MiB query budget in both cache modes.

| Shape | Cache off p50/p95 ms | Cache on p50/p95 ms | Off/on B per query |
|---|---:|---:|---:|
| Legacy simple | 5.390 / 11.038 | 6.200 / 14.129 | 494,213 / 484,801 |
| Typed wide | 9.398 / 19.865 | 10.946 / 21.525 | 847,589 / 748,791 |

Cache-on variants record 1,000 hits and zero timed compiles; cache-off variants
record 1,000 timed compiles. Post-close owner pins, flatten-pool references, live
results and reserved-byte deltas are zero for every variant. CPU totals for
cache-off/on are 2.97/3.47 seconds for legacy simple and 5.02/5.51 seconds for typed
wide; these include clients, liaison and two data nodes.

This single RPC observation establishes correctness and cache activity, not a
statistical RPC latency verdict. In this refreshed run, cache-on latency and CPU
were higher for both shapes while allocated bytes were lower. An earlier run
before the scalar pagination correction showed lower cache-on medians and mixed
tail latency; its artifacts are retained separately. The source change and run
variation do not establish a causal explanation for the RPC differences. No
end-to-end latency improvement is claimed. Historical T02 uncertainty/stopped measurements remain recorded in
[t02-verification.md](t02-verification.md); this PR does not reopen that campaign
or turn old inconclusive outcomes into passes. Earlier T03/T05 evidence remains
separate from this refresh.

Reproduce the focused kernels with `BenchmarkRawProjection` in `pkg/query/ql`
and `BenchmarkQLBPlanReuse/T05` in `banyand/liaison/grpc`, using `-benchmem`,
`-benchtime=1s`, `-count=5` and `-run '^$'`. The existing distributed querybench
scenario `qlb_t05` reproduces the bounded RPC comparison with
`DQB_CARDINALITY=1024`, `DQB_QUERY_WORKERS=8`, `DQB_QUERY_ITERATIONS=1000`,
`DQB_WARMUP_ITERATIONS=20` and `DQB_PROFILE=0`. Use the querybench Docker runner
and the resource limits above.

Full local evidence is retained under `/mnt/d/qlb-pr1-bench/`: `raw.txt`,
`planning.txt`, `micro-summary.json`, `rpc-summary.json`, binary/source hash
manifests, exact invocation scripts, latency samples and container memory events.
The raw kernel remains unchanged from that initial measurement. Planning and RPC
were refreshed after the scalar pagination correction under
`/mnt/d/qlb-pr1-bench/refresh-scalar-pagination/`; its
`verify-source.py --production` confirms the final production sources match the
refresh's manifest (866 files, zero changes). The initial manifest correctly
records that the planner changed between those two measurements.

## Isolated branch verification

- Full `make lint` passes; the affected planner/liaison/generator scopes pass
  again after the scalar pagination correction.
- Planner, QL operators, vectorized packages, liaison, measure storage, native
  receive, query/dquery and generator/oracle unit suites pass.
- Focused race tests cover compiled-plan concurrency/cancellation, fresh bindings,
  borrowed batch ownership, native scan closure and scalar pagination/completion.
- Both independent measure/QL selections pass **451/451** cases: standalone in
  47.874 s and distributed in 71.967 s. Other integration blocks were not selected.
- The deterministic artifact contains 306 generated QL cases, with 132/132 feasible
  interaction pairs, plus 18 plan-reuse scenarios. `make check-ql-test-cases` passes.
- `make check` checks license headers, formatting and a clean tracked worktree
  before publication. No protobuf definitions differ from main.

Final local reports are `/mnt/d/qlb-pr1-standalone-report.json` and
`/mnt/d/qlb-pr1-distributed-report.json`; check, unit and race logs use the same
`/mnt/d/qlb-pr1-` prefix. These results validate PR 1 independently of later Catalog
B features. Historical statistical performance acceptance is not reclassified.
