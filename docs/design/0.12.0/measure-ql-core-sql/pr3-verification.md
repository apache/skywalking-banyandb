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
- Full `make lint` passes. The language guide's section numbers and relative
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
