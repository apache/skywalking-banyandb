# Combined measure QL follow-up verification

This delivery combines the three local features on merged main `b54a9fc76`
(PR #1438), without importing the historical Catalog B branch again:

| Capability | Original commit | Integrated commit |
| --- | --- | --- |
| Indexed LIKE/ILIKE and shared native wildcard execution | `145962b6b` | `145962b6b` |
| Scalar aggregate composition and computed aggregate inputs | `532e5c4aa` | `fe086e543` |
| Exact bounded decimal sums | `1a0050540` | `790d37f25` |

Decimal aggregation depends on the computed-input machinery. Merge resolution
preserves both pattern filtering and computed-input restrictions on native
aggregate pushdown. Generated expectations were regenerated from the combined
source, and the grammar audit includes all three capabilities.

The final integration changes also make the native scan's exclusion of decimal
storage columns explicit, consolidate batch-request pattern binding, and satisfy
the repository's lint and license rules. No protobuf definitions, persisted
formats or native decimal frame types are added. Calendar TIME_BUCKET extensions,
JOIN, index-visibility recovery, spilling and retained-result sessions are excluded.

## Combined-feature acceptance

Three additional generated scenarios use the existing neutral pattern fixture:

- LIKE and ILIKE before `COALESCE(SUM(value + 1), 0)` and a decimal computed sum.
- Grouped decimal sums with HAVING, numeric ordering, LIMIT and OFFSET.
- Rebinding matched, unmatched and NULL patterns, with normal and bypassed caches.

The decimal ordering witness compares `12.0` and `5.0`, so string ordering cannot
pass. Expected values use the independent pattern oracle and small integer
arithmetic rather than the production expression or decimal implementation.
The pattern cohort contains 34 scenarios, 123 bindings and 246 verification
RPCs per topology. Existing generated decimal cases retain the independent
PostgreSQL oracle and a sum whose coefficient exceeds int64.

## Checks

The combined parser, QL, decimal, vectorized execution, native index, liaison,
query processor and queue package tests passed. The generator package and
`make check-ql-test-cases` passed. Native scan/merge and distributed query tests
passed after the lint fixes. Full `make lint` and the separate benchmark-package
lint passed. All three benchmark packages compile on the combined source.

The broad generated RPC cohort passed **444/444** scenarios in each topology,
covering T07, T08, T14, T15, T16, T17 and indexed patterns. Both deployments ran
sequentially in Docker with four CPUs, 8 GiB memory and no extra swap. These
runs preceded the final lint-only corrections; final-binary feature results and
binary hashes are recorded in [follow-up-rpc-verification.json](follow-up-rpc-verification.json).

The broad filter is:

```text
measure-ql && (QL-patterns || QLB-T07 || QLB-T08 || QLB-T14 || QLB-T15 || QLB-T16 || QLB-T17)
```

After committing the integration, run `make check` from the clean worktree.
This repository target checks license headers and formatting; it is not a
substitute for the package and real RPC tests above. Local commands use
`source /tmp/pr4-env.sh` (Go 1.26.10), with the existing tools selected through
`tool_bin=/mnt/d/skywalking-banyandb/bin` and
`LINTER=/mnt/d/qlb-t01-tools/golangci-lint`.

## Benchmark evidence

The included [aggregate composition](../../../../verification/qg2bench/README.md),
[bounded decimal](../../../../verification/qg3bench/README.md) and
[shared wildcard](../../../../verification/dpo85bench/wildcard-reuse/README.md)
benchmarks retain their original workloads, source provenance and limitations.
They were not rerun as performance measurements for this combined branch.
Their saved timings must not be described as measurements of the final combined
query or as new statistical gates. The new integration cases verify composition
and cleanup; they do not broaden the benchmark scope.
