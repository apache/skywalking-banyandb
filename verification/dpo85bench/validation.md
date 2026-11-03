# LIKE/ILIKE verification before wildcard unification

This records the review-fix snapshot before the shared native wildcard refactor.
See [current wildcard-refactor validation](wildcard-reuse/validation.md) for final
source results.

Verified locally on 2026-10-28 against merged baseline
`b54a9fc76e74baa1c0ecbf75513a082c32649a72` plus the uncommitted feature changes.
The original benchmark hashes are included in [campaign provenance](benchmark-provenance.json).
Verbose logs, binaries and source archives remain local in ignored directories.
The final feature cohorts below were rebuilt after all review fixes. Their exact
Docker commands, image/binary hashes, Go source manifest and counters are retained
in [review-fix provenance](review-fix-verification.json).

The first real QueryRPC RED run used an isolated baseline archive with only the
new generated fixture and two initial scalar scenarios. Fixture/schema/index
readiness succeeded. Both feature queries failed with InvalidArgument and
`unexpected WHERE expression token "LIKE"` or `"ILIKE"` as expected.
The baseline outcome was two failed scenarios after successful fixture readiness.

The final generated cohort has 31 scenarios and 108 individual query bindings,
repeated with normal and bypassed compiled-plan caches (216 verification RPCs
per topology). It uses only the QL query endpoint, including readiness. Normal
schema/write RPCs install the neutral dataset. The independent DP matcher,
PostgreSQL reference cases and bounded coverage manifest supply expected values.

| Verification | Result |
|---|---|
| Generated standalone cohort | 31/31 scenarios passed; 14.511 s |
| Generated distributed cohort | 31/31 scenarios passed; 31.132 s |
| Complete qlgen package | Passed; 67.448 s |
| Deterministic artifact regeneration check | Passed |
| Scoped generator/harness/benchmark lint | 0 issues |
| Initial benchmark harness compilation | Passed before review fixes |
| Full parser package after review fixes | Passed; 0.486 s |
| Full model and liaison packages after review fixes | Passed; liaison 35.795 s |
| Changed native/codec/worker/plan/queue admission tests | Passed |
| Scoped lint after review fixes | 0 issues |
| Import-boundary check | Passed |
| PostgreSQL reference | 20 saved neutral cases; version/image provenance beside fixture |

The table preserves the observed outcomes; verbose execution logs remain local.
The six added ambiguity scenarios exercise both keyword-rule orders, mixed
analyzers and multi-tag rules, including NULL and no-match patterns. Six additional
scenarios query measures/groups named LIKE, ILIKE, ESCAPE and their hyphenated
forms. There are seven fixture-readiness pages per topology; readiness used seven
RPCs in standalone and eighteen in distributed mode including visibility retries.
These setup calls are separate from the 216 verification RPCs.

Both integration topologies ran sequentially in Docker with four CPUs and 8 GiB
memory, without additional swap. The benchmark uses its separate stricter
resource settings. The only initial generated-query error was an invalid HAVING
alias, corrected to the existing `COUNT(DISTINCT name)` expression. The first
distributed run then exposed lost final pagination after native scan pushdown;
the unchanged paging expectations passed after retaining the final QL page and
pushing only its bounded candidate window.

Normal and error executions restore query memory reservations and result-owner
counts and do not construct intermediate protobuf DataPoints. These checks do
not claim every optional placement or every schema migration is supported. The
pattern contract deliberately rejects OR/NOT, non-whole-value indexes and
unsupported source shapes.

## Export test follow-up

A broader liaison run initially failed the export goroutine-count test. A 100-run
reproduction and before/after stacks identified an incomplete connection warm-up:
the deliberately failing Plan could cancel a sibling before its gRPC transport
finished starting. Its normal connection goroutines then appeared after the
baseline count. The test now completes a successful dry run against both nodes
before counting; its failed-plan calls and leak assertion remain in place.
Production export behavior is unchanged.

After the fix, 200 consecutive isolated executions, 20 race-detector executions,
the full liaison package and scoped lint pass. The [design verification record](../../docs/design/0.12.0/measure-ql-core-sql/indexed-patterns-verification.md#broader-suite-failure-and-fix)
summarizes the original failure, diagnostic stacks and final outcomes. This later test-only
fix does not change the benchmark's archived production sources or timed code.
