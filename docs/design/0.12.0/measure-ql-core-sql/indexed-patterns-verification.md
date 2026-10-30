# Indexed measure patterns: verification

Baseline: merged main `b54a9fc76e74baa1c0ecbf75513a082c32649a72` (PR #1438).
Implementation branch: `feat/measure-ql-like-ilike`.

## Behavioral evidence

The initial generated standalone RPC cohort was built from an isolated copy of the baseline plus only the new generated tests. Its two cases failed because LIKE/ILIKE were not accepted by the parser; fixture readiness succeeded. The final feature cohort passes **31/31 scenarios in standalone and 31/31 in distributed mode**. Its 108 bindings run with compiled-plan reuse enabled and bypassed, for 216 verification RPCs per topology. Raw protobuf queries are not used as feature acceptance. Final binaries were rebuilt after the shared-wildcard refactor, with no production-source drift; [latest integration results](../../../../verification/dpo85bench/wildcard-reuse/validation.md) retain the evidence. A later native unit-test addition is not linked into those integration binaries.

The cohort covers literal and bound patterns, case modes, Unicode, escapes, punctuation, empty and NULL values, rejection/recovery, missing and tokenized indexes, scalar and grouped COUNT DISTINCT, field residuals, HAVING and native indexed pagination. Expected values use an independent dynamic-programming matcher and 20 saved PostgreSQL 17.6 C.UTF-8 reference examples over neutral data. Locale-specific Unicode equivalence is outside the contract.

The first distributed run caught an OFFSET boundary defect in the new planner optimization: it cleared the QL page after pushing it to native requests, but distributed batch consumers see the sorted native window before final egress pagination. The fix pushes only OFFSET + LIMIT with native offset zero and retains the final QL page. The unchanged expected-result test now passes in both modes.

## Review corrections

Two correctness findings were reproduced before their fixes:

- Index selection previously chose the lowest-ID eligible rule, while writes use the first valid rule covering the tag. A two-keyword-rule fixture returned zero rows through the chosen field despite a control hit in the populated field. Because binding order can differ across nodes, the resolver now rejects distinct valid covering rules instead of choosing by ID or iteration order. Tests cover both orders, mixed analyzers, multi-tag rules, duplicate bindings, incomplete identities and rules referencing removed tags.
- Globally reserving LIKE, ILIKE and ESCAPE broke existing measure/group names, including `like-rate` and `escape-group`. The parser regression first failed 102 subcases. Those words are now recognized only in expression context; resource-name compatibility and actual pattern queries pass together.

The cleanup also centralizes pattern limits, derives automaton capacity from the byte limit, and shares metadata loading between pattern and array-membership validation. A public QL test first observed duplicate binding lookups; it now verifies one load per execution, including cached plans and LIMIT 0, while still observing live index changes.

The request codec moved from `api/data` to the vectorized measure package. Named encode/decode reservation helpers preserve the original estimates and ownership lifetimes. Added tests check maximum-size pattern fields, admission before encoding/decoding, cleanup on failure, and the distinction between a valid wire size and sufficient query memory. The conservative worker decode allowance was documented, not reduced.

The generated cohort adds six ambiguous-index and six contextual-name scenarios to the original 19. It contains 108 bindings, each executed with plan caching enabled and bypassed. Benchmark samples and provenance are now versioned independently of ignored logs and binary/source archives. The historical timings are explicitly labeled as preceding these fixes.

## Shared wildcard refactor

LIKE/ILIKE now lower to typed options on the existing native wildcard search. Legacy byte patterns and typed SQL/glob options share one compiler, mutable automaton and candidate collector. The separate native SQL-pattern execution branch and production Vellum wildcard compiler have been removed. The common compiler adds escape/case options, pre-allocation accounting, retained cache ownership and cancellation during no-match traversal.

Compatibility tests use the old Vellum conversion as an independent oracle for legacy wildcard behavior: one Unicode scalar per `?`, no LF consumed by `*` or `?`, literal backslash and regexp metacharacters, empty strings and invalid UTF-8. Long patterns and more than 16 legacy operands remain supported under memory limits; the SQL input limits are not applied to them. Tests also enforce caller expansion bounds for both legacy and typed glob requests.

The original wildcard interface first failed explicit work/memory/term-budget and no-match cancellation checks. Those tests now pass, as do the full native suite, race checks and scoped lint. The final QL cohorts pass 31/31 scenarios in standalone (15.700 s) and distributed (29.338 s), with 216 verification RPCs each in sequential four-CPU, 8-GiB Docker containers. [Validation and provenance](../../../../verification/dpo85bench/wildcard-reuse/validation.md) record the exact scope.

A separate narrow comparison measures the unchanged native wildcard request against the original merged implementation on 10,000 indexed names. Across three fresh-process 100-query samples per arm/version, median prefix latency is 376.028 → 38.448 µs, leading wildcard 1,668.145 → 705.705 µs, and no-match wildcard 1,922.068 → 702.785 µs. Physical Go allocations fall in all three arms; conservative reservations rise because compiler allocations are now tracked. These are native-request measurements, not QL RPC latency or a statistical gate. See [all samples, ranges and reproduction](../../../../verification/dpo85bench/wildcard-reuse/results.md).

## Focused checks

Passed:

- Full `pkg/bydbql`, `pkg/query/ql` and native index suites.
- Pattern-specific race tests for native budgets and concurrent QL bindings.
- Full API data codecs, query model, vectorized measure plan, standalone processor and distributed-query suites.
- Storage pattern/index-order/budget checks and queue topic/admission/error checks.
- Two-group propagation and eligibility checks; mixed successful/unsupported workers fail the whole traced query.
- Generated fixture, oracle, grammar audit and deterministic artifact checks.
- Scoped lint for changed production/test packages and `make check-import-boundaries`; no protobuf source or generated-file changes.

Native tests independently compare 200 patterns against 80 values, exercise admitted and paged dictionaries, reopen persisted indexes, and check cancellation, work/term/memory bounds and cleanup. Cached programs remain continuously charged to their execution owner; transport envelope reservations survive until response collection or error cleanup.

## Broader-suite failure and fix

The first full liaison suite passed. A later run failed `TestExportPlan_FailedPlanLeavesNoGoroutine` because its process-wide goroutine count rose from 18 to 20 after the export test's warm-up. Initial isolated candidate and baseline executions each passed five times. A subsequent 100-run reproduction failed four times, exposing an intermittent warm-up race. The original suite failure, reproduction and diagnostic stacks are retained in local ignored logs. The decisive outputs were `goroutines grew from 18 to 20` and four failures in 100 isolated executions.

The warm-up used a deliberately failing Plan. One node's expired-session error canceled its sibling while the sibling's lazy gRPC connection was still starting. The baseline count therefore excluded some normal reader, writer and keepalive goroutines that appeared during the repeated calls and remained for the connection's lifetime. The captured failures contained no surviving export sender or node-plan stack. This was a test-baseline error, not evidence of a production export leak.

The test now performs a successful dry-run Plan and verifies that both nodes answered before recording its baseline. It retains the same 20 failed calls and goroutine leak assertion, and also checks each failure's `FailedPrecondition` status. No production export code changed. After the fix, 200 consecutive test executions, 20 executions under the race detector, the complete liaison suite and scoped lint pass.

## Historical QL feature benchmark

The fixed campaign completed three fresh-process rounds, with two pinned CPUs, 4 GiB container memory and an explicit 3 GiB query allowance. It compares LIKE/ILIKE COUNT DISTINCT against independently precomputed equivalent IN controls, plus the same exact-name query on baseline and candidate. All fixture construction and expected-name discovery are outside timing. Matching arms select eight names with two metric rows each; no-match arms select none. These are standalone RPC timings; distributed correctness is verified separately.

| Distinct names | LIKE median | ILIKE median | Equivalent IN median | Pattern allocation per operation | Peak tracked memory |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 4.643 ms | 4.840 ms | 0.592 ms | About 1.08 MB / 19,900 allocations | 2.66 MiB |
| 10,000 | 34.648 ms | 34.535 ms | 0.505 ms | About 8.10 MB / 167,000 allocations | 11.44 MiB |

These are medians of three 100-operation means, not request percentiles. Matching patterns read 16 native rows and no-match patterns read zero. No-match latency stays near matching latency, showing that leading-wildcard dictionary exploration dominates these cases. The IN control is given the matching names, so it excludes discovery cost. Dictionary work/term counters are checked in focused native tests but are not exposed by this RPC harness.

The unchanged exact-name control shifts in opposite directions: baseline to candidate is 0.414 to 0.462 ms at 1,000 names, and 0.386 to 0.358 ms at 10,000. No uniform regression or improvement is established. All prescribed samples are retained; no percentage gate or repeat-until-pass timing was used. See [full results, dispersion and resource measurements](../../../../verification/dpo85bench/results.md) and the [reproduction harness](../../../../verification/dpo85bench/README.md). The versioned benchmark report includes all 54 samples and campaign provenance. Its measured source predates the review fixes; these numbers are not claimed as timings of the corrected implementation. Raw logs, binary snapshots and source archives remain local and ignored, with no published-document links to them.
