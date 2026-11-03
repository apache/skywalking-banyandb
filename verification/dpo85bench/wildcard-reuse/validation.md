# Shared wildcard verification

Verified locally on 2026-10-28 against merged baseline
`b54a9fc76e74baa1c0ecbf75513a082c32649a72` plus the feature and shared-wildcard
working-tree snapshot captured in the provenance. Protobuf definitions and persisted index formats are
unchanged.

Both legacy native wildcard byte patterns and LIKE/ILIKE's typed wildcard options
now use one compiler and posting-list collector. No separate SQL-pattern native
execution remains. The common engine supports escape and case options and owns
allocation, work, expansion and cancellation checks.

| Check | Observed result |
|---|---|
| Full native package | Passed; 2.048 s |
| Native wildcard/pattern race checks | Passed; 1.250 s |
| Typed-glob expansion-bound regression under race | Passed; 1.027 s |
| Full bydbql, model, QL, vectorized measure, measure plan and query processor packages | Passed |
| Storage pattern/wildcard/index/series checks | Passed |
| Scoped native and affected-package lint | 0 issues |
| Deterministic generated artifact check | Passed |
| Generated standalone QL cohort | 31/31 scenarios passed; 15.700 s |
| Generated distributed QL cohort | 31/31 scenarios passed; 29.338 s |

The generated cohort has 108 parameter bindings. Each runs with plan caching
enabled and bypassed: 216 verification RPCs per topology. Both deployments ran
sequentially under Docker with four CPUs, 8 GiB memory and no extra swap. Seven
readiness pages required seven standalone RPCs and eighteen distributed RPCs
including visibility retries; these setup calls are separate from verification.
All feature queries use QL. The independent oracle and neutral fixture are
unchanged.

[Integration provenance](integration-verification.json) records exact commands,
image/binary hashes, counters and timings. Production files and generated
artifacts match the frozen source. A later native unit-test addition is recorded
separately; dependency-package unit tests are not linked into the integration
binaries. No integration run was interrupted or retried.

The legacy wildcard regression first showed that explicit work, term and byte
budgets were ignored and cancellation could be missed during a no-match walk.
The shared engine passes these checks. The old Vellum wildcard conversion is
retained only as a test oracle for Unicode, newline, literal backslash,
metacharacter, empty-value and invalid-UTF-8 compatibility. Additional tests cover
typed glob escaping/case folding, mixed-syntax budgets, long dynamic state
bitsets, more than 16 legacy inputs, retained cache ownership and cleanup on
failure. SQL length/count defaults do not restrict legacy requests.

Both legacy and typed glob requests must supply an expansion bound. Removing the
typed-glob validation through a temporary Go overlay made its explicit regression
fail; the actual frozen source passes. SQL syntax retains its default bounded
worker policy.

The [native benchmark](results.md) ran only after integration and package checks
finished. It preserves all 18 prescribed fresh-process samples, with no retries,
under two pinned CPUs, a two-CPU quota, 4 GiB memory and no extra swap. Every timed
query validates row identities and reservation release. Its results describe the
unchanged native wildcard request, not end-to-end QL latency.
