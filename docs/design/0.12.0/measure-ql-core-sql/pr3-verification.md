# Catalog B PR 3: typed expressions and filtering

This branch is based on merged PR #1424 at `e409a6c6`. It extracts T10, T12 and
T14 from the combined Catalog B implementation. SELECT DISTINCT, computed/ordinal
grouping, selectable time buckets, multi-key ordering and extended exact
count-distinct composition remain in PR 4. Legacy native query forms retain their
existing behavior.

## Execution and compatibility

```text
parameterized QL -> cached typed recipes -> fresh binding
  -> authorization/admission/revision/lifecycle gates
  -> native raw batches -> raw WHERE -> local aggregation if needed
     or eligible native aggregates -> complete keyed batch merge
  -> HAVING -> visible projection/typed SELECT -> final LIMIT/OFFSET
  -> existing measure_result
```

T10 adds checked arithmetic, explicit casts, comparisons, SQL null semantics and
masked CASE/COALESCE/boolean evaluation. A masked branch is not evaluated on rows
that do not need it. Constants remain scalar; expressions resolve source slots
before execution. Computed SELECT outputs currently apply to raw queries.

T12 evaluates field and mixed tag/time/field predicates before grouping or final
pagination. Only TRUE passes; FALSE and UNKNOWN remove the row. Safe native tag
and time pruning does not replace residual evaluation. Residuals disable native
aggregate and input-limit pushdown. Predicate-only columns stay hidden.

T14 evaluates HAVING after local aggregation or the complete native aggregate
merge. Grouping source names and named aggregate calls resolve independently of
SELECT aliases. Repeated calls share work; hidden calls and grouping keys are
removed after filtering. CONTAINS/NOT CONTAINS preserve legacy all-terms collection
membership behavior and are contextual, measure-only spellings.

The filter stages borrow input value columns and change selection. Their masks,
decoding scratch and expression state charge the query allowance, with release on
EOF, error, cancellation and Close. Cached plans retain no bound values, running
operators or reservations. Disk spilling is not implemented.

Typed membership on indexed arrays still fails before dispatch, including on
cache hits using current index metadata. The inherited native indexed-array
projection repair is preserved; this PR does not remove the conservative typed
membership restriction. Unindexed arrays and indexed scalar membership remain
supported. MATCH combined with typed residual expressions remains unsupported.

No protobuf definitions, generated API contracts, persisted formats, native frame
versions or production memory-protector settings change. The merged PR 2 standalone
suite's explicit 1 GiB server allowance remains in place.

## Isolated correctness checks (2026-10-23)

The branch uses main's Go 1.26.10 toolchain. All checks use the isolated PR 3
worktree, retaining the merged standalone suite's explicit memory allowance.

- Planner, QL operators, vectorized measure/frame/plan, liaison, query, generator
  and benchmark-harness package suites pass with `-race -tags=querymetrics`.
- Regeneration reproduces the committed suite exactly. The deterministic
  `make check-ql-test-cases` check passes. New feature cases use the generator's
  independent fixture oracle; existing raw cases remain compatibility controls.
- The complete selected independent measure/QL suite passes **787/787** cases
  in each topology: standalone in 173.040 s and distributed in 209.581 s. This
  includes the 530 inherited cases and 257 expression/filter/HAVING cases.
- The 257 new-feature cases also pass in `America/Los_Angeles` (115.548 s).
- Lint required extracting WHERE binding into a dedicated helper without changing
  its behavior. After that refactor, planner/liaison race suites pass again and
  all **48/48 T12 cases** pass in each topology (21.033 s and 40.527 s).
- Full `make lint` and `make check` (license and format checks) pass.
  The language guide's section numbers and relative
  documentation links were checked after adding the three new syntax sections.

Integration runs use four-CPU, 8-GiB Docker containers with no extra swap and
Go 1.26.10. Reports are `/mnt/d/qlb-pr3-{standalone,distributed}-report.json`,
`/mnt/d/qlb-pr3-standalone-la-report.json` and
`/mnt/d/qlb-pr3-filter-final-{standalone,distributed}-report.json`. Package, lint,
fixture and binding-refactor logs use `/mnt/d/pr3-`. Other integration blocks
were not selected. No protobuf, workflow, dependency or production-protector
configuration changes are included.

Historical [T10](t10-verification.md), [T12](t12-verification.md) and
[T14](t14-verification.md) records retain their original bases, resource limits,
measurements and limitations. They do not certify this rebased intermediate branch.
The original [T02 acceptance](t02-verification.md) remains inconclusive/stopped;
large-input throughput acceptance and disk spilling remain deferred. Re-running
PR 3 checks does not reclassify historical gates or broaden benchmark scope.

## Fresh scoped benchmark characterization (2026-10-23)

The tested source matches `c924ff520f62b01ff50748c8df73d4888cb2c38f` across
2,438 captured Go, grammar, module and fixture inputs. Binaries use Go 1.26.10
and `querymetrics`. Every run uses a four-CPU, 8-GiB Docker container with
`memory.swap.max=0`, `GOMAXPROCS=4` and one query worker. RPC query trackers are
256 MiB. The host is shared; our integration/lint work completed before timing,
but these quotas do not guarantee an otherwise idle host.

This is a bounded check of the rebased branch: three 200-ms kernel samples and
three fresh RPC repetitions per scenario. It does not replace the historical
five-run ticket protocols or establish a statistically resolved latency gate.
Only T10, T12 and T14 are measured; no cache/concurrency matrix or large-input
resource probe was added.

All **97 kernel variants**, **18 RPC cardinality reports**, **222 measured RPC
variants** and **14,280 timed calls** pass. The harness verifies independent
fixture results, structural execution counters and cleanup; the report recheck
also verifies all response hashes, sample counts, zero final owner/result/pool
references and zero live reservations. All **444 CPU/heap profiles** exist.
Container memory events report no limit hits or OOM events.

### Kernels

Medians below are microseconds per 1,024-row batch. T10 reuses value buffers and
compares typed kernels with its independent row interpreter. T12 includes the
complete filter lifecycle; T14 includes completed-group filtering and borrowed
projection, excluding scan/aggregation/encoding.

| T10 expression | Typed kernel | Scalar interpreter |
| --- | ---: | ---: |
| Constant | 0.156 | 63.087 |
| Arithmetic | 15.233 | 60.331 |
| CASE, 50% first-arm hits | 35.025 | 114.213 |
| CASE, 100% first-arm hits | 21.688 | 89.669 |

Tiny batches still pay setup/dispatch costs: one-row arithmetic measures
0.259 us typed versus 0.155 us scalar. These results do not claim that typed
execution always wins. The all-first-arm CASE checks that no later-arm rows run;
constants check that no full value vector is allocated.

| Nominal passing percentage | T12 dense filter, us | T12 selected rows | T14 HAVING, us |
| --- | ---: | ---: | ---: |
| 0% | 38.683 | 0 | 28.377 |
| 1% | 33.894 | 11 | 43.606 |
| 50% | 33.783 | 524 | 41.333 |
| 100% | 24.273 | 1,024 | 32.972 |

The T14 baseline without HAVING is 1.183 us. Both filters borrow the input
columns; the copied-byte invariant is zero. The 1% and 50% masks retain 22 and
1,048 bytes of selected row indexes, respectively; all/zero-match cases retain
no selection. The fixed expression/lifecycle allocation remains visible in
`kernel-summary.json`; zero-copy does not mean zero allocation.

### Distributed RPCs

Each cardinality starts a fresh process and cluster. The existing frozen
schedules remain 100 timed calls plus five warmups at 1,024 rows, and 20 timed
calls plus three warmups at the larger cardinality. T10/T14 use 32,768 rows;
T12 keeps its pre-existing 16,384-row successful protocol. Each reported value
is the median across three repetitions, not a pooled latency percentile.

| T10 projection, 32,768 rows | p50, ms | p95, ms | Allocated MB/query |
| --- | ---: | ---: | ---: |
| Raw control | 103.867 | 121.073 | 56.964 |
| Arithmetic | 101.808 | 120.652 | 56.765 |
| Parameter cast | 98.877 | 125.328 | 56.303 |
| Masked CASE/COALESCE | 113.718 | 131.849 | 57.619 |

The RPC results are mixed: masked CASE adds work at this size, while other
small differences are not evidence of an RPC speedup. At 1,024 rows, raw control,
arithmetic and masked CASE p50 are 6.468, 6.970 and 5.955 ms, respectively.
The kernel comparison isolates expression work more directly.

| T12 narrow predicate, 16,384 rows | Raw p50, ms | Raw allocated MB/query | Aggregate p50, ms |
| --- | ---: | ---: | ---: |
| 0% | 22.986 | 15.362 | 21.493 |
| 1% | 23.925 | 15.295 | 24.499 |
| 50% | 38.770 | 22.709 | 22.914 |
| 100% | 49.245 | 29.800 | 24.306 |

All variants examine the complete input. Lower raw output reduces downstream
encoding/allocation; scalar aggregate outputs remain bounded. The full report
also includes the two-hidden-field variants and exact fixture passing counts.

| T14, 32,768 input rows / 16 completed groups | Without HAVING p50, ms | 50% HAVING p50, ms | Without / with allocated MB/query |
| --- | ---: | ---: | ---: |
| Native aggregate merge | 21.624 | 21.238 | 10.740 / 10.724 |
| Local aggregation | 40.731 | 40.793 | 31.002 / 31.519 |

HAVING runs after all 16 groups are completed, so it cannot avoid input scanning
or aggregation. Differences at this small group count are descriptive, not a
speedup claim. CONTAINS and legacy membership also pass at array lengths
0/1/16/256. At length 256, their p50 values are 26.151/25.927 ms and allocations
23.191/23.114 MB/query; the full report retains all membership widths and hidden
HAVING variants. Hidden HAVING changes response width, so its timing is not a
pure filter comparison.

### Reproduction artifacts

Artifacts remain outside the source tree at `/mnt/d/qlb-pr3-bench/`:

- `run-all.sh`, `run-kernel.sh`, `run-rpc.sh`: exact commands and container flags.
- `kernel.txt`, `kernel-summary.json`, `rpc-summary.json`, `summary.txt`: raw
  kernel output and medians for every variant.
- `qlb_t{10,12,14}-r{1,2,3}/`: raw RPC logs, sample reports, profiles and cgroup
  limits/events. `summarize.py` checks the retained reports and cleanup.
- `source-manifest.json`, `source-verification-final.json`, `working-tree.patch`,
  `commit.txt`, `binary-sha256.txt`: source and executable provenance. Binaries
  were built before the commit was created; the source verification confirms
  the captured inputs match `c924ff52` exactly.

SHA-256 of `source-manifest.json`:
`105669b6ef4af5b16e6b6679eb1b472247d405696dc93393ecda8895591f9f42`.
Kernel binary SHA-256:
`7d78abc20e9d3888100f1b1f920818e89354686c2332baeef836d9d6d3ba5dee`.
RPC binary SHA-256:
`0b2fe9b51ceb4f65b4dece9dd67ac887edf6abc96aaacb6401a603e163e19107`.
