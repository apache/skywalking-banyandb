# Post-rebase review fixes and verification

The combined branch was rebased onto main `39d33bc5d` with no conflicts.
`git range-diff` confirmed all four patches were unchanged. The resulting head
was `cab0cb0c7`; its feature commits were `026667c14`, `d6f51e055` and `075f36a40`.
Fresh standalone and distributed feature runs each passed 145/145 scenarios.
The older [integration record](follow-up-verification.md) retains its original
commit IDs, binary hashes and counts; those observations were not relabeled.

## Review fixes

An accepted integer-first decimal `IN` or `NOT IN` selected the integer kernel
and panicked while reading a decimal column. Comparisons now choose the exact
decimal kernel if any operand is decimal, preserving each operand's scale.
The existing NULL and short-circuit membership behavior is unchanged.

Wildcard limit errors now identify work, expanded terms, allocation/automaton
bytes, pattern count/length or candidate count. The limits remain unchanged.
Wrapped errors retain the native sentinel and remote ResourceExhausted mapping.

The QG2 and QG3 reproduction scripts resolve module and build caches through
`go env GOMODCACHE` and `go env GOCACHE`, including quoted paths with spaces.
The benchmark workload and historical timing evidence remain unchanged.

## Regression evidence

Expression-execution tests failed before the fix and passed afterward. They
cover integer literals/columns, decimal operand order, a decimal later in the
list, different scales, NULL candidates and later matching values, for both
IN and NOT IN. They also assert query memory is released on closure.

The generator adds 16 scenarios over the existing six-row fixture. Its sum is
12, so 12 equals the scale-0 sum and the scale-2 sum of `v * 100`, but not the
scale-1 sum 1.2. Independent worked expectations cover the full, null-only and
empty windows. All 48 bindings run with normal and bypassed plan caches,
producing 96 new QL RPCs in each topology; no raw-query comparison was added.

The new `qg3_decimal_membership_integer_first_in` case was run through the real
standalone QL RPC with the old comparator supplied by a Go source overlay. It
failed with the expected index-out-of-range panic and Internal response. With
the fix, the feature cohort passed **161/161** scenarios in both standalone and
distributed deployments, including all 16 new scenarios and their cleanup
assertions. Containers ran sequentially with four CPUs, 8 GiB memory and no
extra swap, using Go 1.26.10 and `-tags=querymetrics`.

[Machine-readable evidence](review-fixes-rpc-verification.json) records the
pre-fix failure, pre-fix post-rebase runs, fixed runs, binary hashes and changed
source hashes. Fixed-run hashes describe the working tree on `cab0cb0c7`, not
an already committed revision.

## Other checks

The QL, parser, decimal, generator and native index package suites passed.
Native receive tests verify that the expanded error messages still map to
ResourceExhausted and worker failures still fail the query. Full `make lint`,
`make check-ql-test-cases` and `make license-check` passed. Both reproduction scripts passed zsh
syntax checking and 26 mocked Docker invocations using cache paths with spaces.

No new performance claim is made: comparator selection happens during operator
compilation, error formatting occurs only on rejected requests, and the script
changes only affect cache-path discovery. The existing benchmark workloads
were not rerun or expanded for these fixes.
