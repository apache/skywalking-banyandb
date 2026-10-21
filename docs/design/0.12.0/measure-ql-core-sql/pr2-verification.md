# Catalog B PR 2: SQL aggregation and native split/merge

This branch is based on merged PR #1417 at `e05ed7f22`. It extracts T07 and T08
from the combined Catalog B implementation. Expressions, field WHERE, HAVING,
SELECT DISTINCT, computed grouping, selectable time buckets and general sorting
remain in PRs 3 and 4. Legacy scalar COUNT(DISTINCT) keeps the merged PR 1 behavior;
extended exact count-distinct composition is not part of this PR.

## Execution and compatibility

```text
parameterized QL -> cached schema-resolved metadata -> fresh binding
  -> authorization/admission/revision/lifecycle gates
  -> eligible: sequential existing native aggregates -> keyed batch merge
     otherwise: complete deduplicated native scan -> local hash aggregation
  -> output aliases -> final LIMIT/OFFSET -> existing measure_result
```

Extended queries support named scalar grouping and multiple COUNT/SUM/MIN/MAX/
AVG/MEAN calls. Null keys differ from empty strings and zero. Counts are exact
int64; integer SUM overflow fails. AVG uses sum and non-null count rather than
the legacy native MEAN behavior. Repeated calls share work. Scalar empty input
returns zero COUNT and null other aggregates; grouped empty input returns no rows.

Native eligibility is conservative: one measure group, a provable stage, one
exact time interval and complete non-null entity identity. Unsafe integer sums,
floating extrema, non-entity keys, residual time predicates and uncertain stage
selection retain the complete local path. Native branches run sequentially and
share the parent allowance; each source closes before the next opens. Partial
failures do not return partial SQL groups. Local key/state growth reserves memory
before allocation, and cleanup releases owners and reservations.

No protobuf definitions, RPCs, native operator implementations, persisted formats
or native frame versions change. Memory protection remains active. Disk spilling
and retained-result pagination remain deferred.

The generated QL-only contracts derive expected values from independent fixture
arithmetic. Existing raw-query cases remain separate compatibility controls.
T07 covers local grouping, null/empty semantics, shared calls and pagination;
T08 compares cached, bypassed and forced-local execution, including branch reuse,
lifecycle changes, cancellation, authorization/revision failures and recovery.

The extraction retains the main-branch compatibility fixes and the CI regression
fixes from PR 1. Shared benchmark fixture dispatch now lives in the test entrypoint,
so ordinary Go package compilation and vulnerability analysis do not depend on
helpers present only in `_test.go` files.

## Isolated correctness checks (2026-10-21)

- Planner, QL operators, liaison, fixture generator and measure-data unit suites pass.
- The planner/operator/liaison/generator suites also pass with `-race -tags=querymetrics`.
- The unchanged deterministic generation check passes, preserving PR 1's scalar
  count pagination and empty-entity regression cases alongside the new scenarios.
- Both independent measure/QL selections pass **530/530** cases in four-CPU,
  8-GiB Docker containers: standalone in 73.315 s and distributed in 92.942 s.
  These include 79 new T07/T08 specs and the 451 inherited compatibility specs.
- Full `make lint` passes. All 78 relative documentation links/anchors resolve.

Reports are `/mnt/d/qlb-pr2-standalone-report.json` and
`/mnt/d/qlb-pr2-distributed-report.json`. Unit, race, generator and lint logs use
the `/mnt/d/qlb-pr2-` prefix. Other integration blocks were not selected.

## Inherited toolchain blocker

On 2026-10-21, `make vuln-check` fails for the inherited Go 1.26.9 standard
library: [GO-2026-6809](https://pkg.go.dev/vuln/GO-2026-6809), affecting `crypto/x509`,
is fixed in Go 1.26.10. The pre-aggregation PR 1 tree reports the same reachable
vulnerability with the same toolchain. This branch changes neither `go.mod` nor
dependencies, and no aggregation compilation error remains.

Logs are `/mnt/d/qlb-pr2-vuln.log` and
`/mnt/d/qlb-pr2-main-vuln-control.log`. The scan is not counted as passed. A separate
toolchain patch is needed before CI can become green; aggregation benchmarks
retain the existing Go 1.26.9 protocol and do not claim patched-toolchain results.

## Evidence boundaries

[T07 verification](t07-verification.md) and [T08 verification](t08-verification.md)
preserve historical measurements at their original revisions. The current branch
includes #1413's native aggregation optimization and PR 1's ownership/accounting
repairs; historical figures are not treated as measurements of this isolated head.

Fresh measurements use only the existing T07/T08 workloads, in four-CPU, 8-GiB
Docker containers with no extra swap and Go 1.26.9. T07 compares dense local
aggregation with its independent scalar reference. T08 compares the same SQL
through native and forced-local execution, with existing individual-native RPC
bundles as additional controls. Kernel and RPC observations remain separate;
neither eligibility nor a specialized scalar comparison guarantees a speedup.
