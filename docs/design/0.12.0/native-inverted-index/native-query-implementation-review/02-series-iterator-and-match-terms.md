# Draft Q2: native SeriesIterator and MatchTerms

Classification: **implemented bounded execution ticket; affected verification passed** (two bounded operations, one review/queue unit; not a tracking parent)

> **Current status (2026-10-04).** The historical planning text below records the
> original queue boundary and its RED criteria; it is not a statement that the
> callers are still blocked. Native SeriesIterator is active in `bydbctl analyze
> series`; the Stream equality path has a real native Searcher/`eq.Execute`
> integration test; and the `banyand/stream` element-index factory uses the
> native Store without a legacy fallback. Its expanded compatibility coverage
> includes native presence, range, MATCH, and sort behavior, external ingest,
> and automatic maintenance, with lease, write, reopen, and snapshot-content
> tests. Focused tests and operation benchmarks pass. Affected-package lint
> passes; the broad test command timed out in the bydbctl suite after 300s, so
> this document does not claim the combined Q2 change is fully released or that
> every owner failure-injection case is covered.

## Combined boundary

Merge the existing dictionary-diagnostic `SeriesIterator` operation and Stream exact `MatchTerms` operation into one caller-activated execution ticket. Q2 is cohesive native dictionary access: enumeration and exact membership share Q1's pinned-view ownership. Q2 switches the `bydbctl analyze series` caller and Stream `eq.Execute` in one reviewable unit while keeping their internal implementations and tests separately observable. Q2 depends on Q1's native read-only adapter ownership and creates the series/time-constrained membership and timestamp-projection helper consumed by Q3. Its SeriesIterator portion is committed-read-only; its live MatchTerms portion is blocked on NATIVE-OWNER. It is not two independently queueable leaves.

No legacy `Query`, `Search`, or collector construction/delegation is permitted. Native aggregation, scoring, Boolean composition, MATCH, range, sort, and generic parser/planner work remain excluded.

### SeriesIterator operation

Classification: **implemented bounded sub-operation in this merged workpackage**

## Summary
Replace the dictionary-backed `SeriesIterator` operation for the `bydbctl analyze series` caller. It streams series identities only; it does not switch `Search` or series filtering.

## Boundary

The proposed native SeriesIterator seam is a bounded dictionary iterator on `ReadOnlyGeneration`/nativeice `Reader`, activated by the existing `bydbctl analyze series` caller; `index.SeriesStore.SeriesIterator` is not the native implementation seam. Values are copied before leaving the iterator; iterator close is idempotent. The MatchTerms sub-operation uses a private context-capable native membership seam once NATIVE-OWNER exists. No retired query/search/collector object, parser, wildcard matcher, projection, sort, aggregation, or score crosses either seam.

## Requirements

- Enumerate distinct dictionary terms in the established diagnostic order from one pinned generation. This is dictionary metadata enumeration, not live query hits: while the segment remains pinned, a fully deleted term remains visible to this API. A newly opened post-compaction view may drop it; that generation transition is tested separately.
- Check context while expanding/iterating and preserve typed corruption errors.
- Never mix segment coordinate spaces or retain pooled/mapped bytes after `Close`; bound dictionary/candidate state.

## Acceptance criteria

- **Historical RED criterion:** the original pre-implementation queue used the following command as its discovery guard: `set -o pipefail; go test ./bydbctl/internal/cmd ./pkg/index/native ./pkg/index/internal/nativeice -list '^TestNativeSeriesIterator' | grep -q '^TestNativeSeriesIterator'`. It is retained as historical planning evidence, not current failure evidence.

- **Independent fixture:** a hand-authored fixture has exactly five dictionary terms (`cpu/a`, `cpu/b`, `db/a`, `db/b`, `z/a`). The `db/b` document is fully deleted while its source segment remains pinned, so the expected iterator sequence is `[cpu/a,cpu/b,db/a,db/b,z/a]` and count 5. A separately compacted new generation may drop `db/b`; the test must assert the pinned-generation distinction. This API is not used for live query hits. Expected encoded identities come from the fixture manifest/retained oracle, not native enumeration.
- **End-to-end:** invoke `bydbctl analyze series <dir>` with its existing one-minute context and assert emitted subject counts; cancel a direct iterator after two values and assert `context.Canceled` plus exactly-once close.
- **Compatibility/resource:** retained oracle and native existing segment generations cross-open; no new writer is required; reads do not alter files; malformed dictionary entry fails typed and bounded; concurrent iterators use immutable mappings with independent cursors.

- **Native-path guard:** the test installs a forbidden retired Writer/Reader/plugin-instantiation and Query/Search/collector spy and fails if the iterator invokes it; it also requires a native-execution counter to be non-zero.
- **Paired microbenchmark (proposed, not run):** first require discovery with `set -o pipefail; go test ./pkg/index/native ./pkg/index/internal/nativeice -list '^BenchmarkNativeSeriesIterator' | grep -q '^BenchmarkNativeSeriesIterator'`; then run `go test ./pkg/index/native ./pkg/index/internal/nativeice -run '^$' -bench '^BenchmarkNativeSeriesIterator/(native|oracle)$' -benchmem -count=5`. Compare five standard Go subbenchmark samples: target native median `ns/op <` oracle and median `B/op <=` oracle; this is initial microbenchmark evidence, not a fresh-process paired macro benchmark; report honestly if unmet.
## Scope and readiness (historical boundary; current activation is stated above)

Caller: `bydbctl/internal/cmd/analyze.go` (only this analysis operation). The native SeriesIterator is active for this caller. Exclude `Search`, `StoredFields`, prefix/wildcard semantics, writer/publication changes, and CLI output redesign. The expanded implementation also verifies the Stream element-index factory through its focused integration tests; that verification is not a claim that all repository gates have passed.

Dependencies: Q1 native read-only adapter/ownership, then NATIVE-OWNER for live membership; this Q2 workpackage cannot queue independently. Relevant design rows: NQ-T01, NQ-T04, NQ-T05.

Decision: this bounded sub-operation remains separately testable, but queues and merges only as Q2 workpackage; no general series-query foundation.

### MatchTerms operation

Classification: **implemented bounded sub-operation in this merged workpackage**

## Summary
Replace the exact indexed-term posting operation used by Stream equality filters. Switch only the real `eq.Execute` caller; retain the existing posting-list/timestamp result contract. Q2 also creates the shared series/time-constrained membership and timestamp-projection helper consumed by Q3.

## Boundary

`Searcher.MatchTerms(index.Field)` returns matching document and timestamp postings for one encoded field term, series identity, and optional time range. Native code owns term encoding, membership, deletion masking, and bounded posting decode. No `Query`, `Search`, collector, Boolean composition, MATCH analyzer, range, or scoring object is delegated.

## Requirements

- Exact bytes for string and numeric terms; preserve series and inclusive/exclusive timestamp constraints.
- Delete/mask before exposing document or timestamp postings; return empty postings for absent terms.
- Check context during dictionary/posting work (thread context through the native seam even though the legacy interface needs extension), release cursors once, and enforce term/posting budgets.

## Acceptance criteria

- **Historical RED criterion:** the original pre-implementation queue used the following command as its discovery guard: `set -o pipefail; go test ./pkg/query/logical/stream ./pkg/index/native ./pkg/index/internal/nativeice -list '^TestNativeMatchTerms' | grep -q '^TestNativeMatchTerms'`. It is retained as historical planning evidence, not current failure evidence.

- **Independent fixture:** the explicit table is doc 10 = `ok`, ts 100; doc 11 = `bad`, ts 300; doc 12 = `ok`, ts 200; doc 13 = deleted `ok`, ts 400. `status=ok` must yield docs `{10,12}` and timestamps `{100,200}`; `100 < ts <= 300` yields doc `{12}`. Expected sets are declared by the hand-authored fixture, independent of native postings.
- **End-to-end:** execute actual Stream `eq.Execute` through a real `GetSearcher`, verify document/timestamp postings and query result, restart, and repeat from the same bytes.
- **Compatibility/resource:** retained oracle and native existing bytes cross-open; no new writer is required; read-only files remain identical; malformed term/posting offsets return typed corruption; posting expansion obeys configured bounds and does not retain cursors.

- **Native-path guard:** the test installs a forbidden retired Writer/Reader/plugin-instantiation and Query/Search/collector spy and fails if `eq.Execute` invokes it; it also requires a native-execution counter to be non-zero.
- **Paired microbenchmark (proposed, not run):** first require discovery with `set -o pipefail; go test ./pkg/query/logical/stream ./pkg/index/native ./pkg/index/internal/nativeice -list '^BenchmarkNativeMatchTerms' | grep -q '^BenchmarkNativeMatchTerms'`; then run `go test ./pkg/query/logical/stream ./pkg/index/native ./pkg/index/internal/nativeice -run '^$' -bench '^BenchmarkNativeMatchTerms/(native|oracle)$' -benchmem -count=5`. Compare five standard Go subbenchmark samples: target native median `ns/op <` oracle and median `B/op <=` oracle; this is initial microbenchmark evidence, not a fresh-process paired macro benchmark; report honestly if unmet.
## Scope and readiness

Caller: `pkg/query/logical/stream/index_filter.go:eq.Execute`. A real native Searcher-backed `eq.Execute` integration test passes. Exclude OR/AND/NOT/HAVING/IN, MATCH/analyzers, range, and sort from this bounded operation; the expanded Stream integration separately covers the factory's write/sort/reopen/snapshot path.

Dependencies: Q1 native read-only adapter/ownership, then NATIVE-OWNER for live membership; this Q2 workpackage cannot queue independently. Relevant design rows: NQ-T01, NQ-T02, NQ-T04, NQ-T05.

Decision: this bounded sub-operation remains separately testable, but queues and merges only as Q2 workpackage; not a query-engine foundation.

## Combined readiness and dependencies

The original queue decision was planned-blocked pending Q1/NATIVE-OWNER. Those prerequisites and the expanded caller/factory seams now have focused evidence. The remaining work is final cross-package verification and any failures it exposes; this document does not claim those gates are complete. Q3 consumes Q2's shared series/time membership and timestamp-projection helper.

Decision: the two operations fit one bounded review/queue unit as explicitly requested; do not split them into separate issue drafts.

## Current implementation evidence (2026-10-04)

The Q1 read-only owner prerequisite in the current worktree has immutable root pinning,
native exact-term reads, asynchronous persistence, restart recovery, bounded
compaction, and conservative manifest/segment garbage-collection gates. The
owner API exposes `Compact` and `CollectGarbage`; scheduling those operations
remains an explicit database-owner responsibility, not an automatic lifecycle
claim. The current verification covers native fixtures, deletion masks,
multi-segment reads, reopen, cancellation, and pinned-view safety. It does not
claim every failure-injection case (callback-triggered close during all queue
states or a stale-compaction barrier) as independently tested. The nativeice
publisher does have a post-manifest directory-sync injection test; the owner
handles that typed uncertain-publication error as a terminal durability fault:
the admitted in-memory root remains readable, its durable watermark does not
advance, and later writes are rejected until reopen validates the directory.
This deliberately avoids retrying immutable names that may already be visible.
Newly persisted handles are promoted to validated disk-backed readers through
a copy-on-write current-root swap; pinned older roots retain their immutable
payload until those views release, as required by the read-view contract.

Q2 bounded operation activation is implemented and covered by focused caller tests: `bydbctl analyze series` uses native SeriesIterator, and a real Stream `eq.Execute` test uses the native Searcher. The native `banyand/stream` element-index constructor is covered by lease, write, sort, reopen, and snapshot-content integration tests; operation-level benchmark evidence remains distinct from full repository gates.

The available read-only SeriesIterator microbenchmark was run with
`-benchmem -count=5` (fixture setup excluded): native medians were about
8.17µs/2,520 B/94 allocs versus the oracle's 20.82µs/4,201 B/106 allocs.
This is native-package evidence only; it is not a product-throughput claim.

A bounded MatchTerms operation benchmark now also runs five samples against a
closed/reopened native fixture and a separately closed/reopened legacy oracle
fixture built from the same four documents. It verifies string `ok`, numeric
canonical token `42`, series filtering, deletion of doc 13, and `100 < ts <=
300` yielding external doc 12/timestamp 200 before timing. Native medians were
about 22.0µs/1,611 B/68 allocs versus oracle 62.8µs/34,631 B/265 allocs.
This remains an operation microbenchmark; setup/reopen costs are outside timing.

## Final verification evidence

The affected-package lint command passed:

`/tmp/golangci-lint-1.64.8 run ./pkg/index/... ./pkg/query/logical/... ./banyand/stream ./banyand/internal/storage ./banyand/internal/wqueue ./bydbctl/internal/cmd`

The corresponding broad test command passed all packages before timing out in
`./bydbctl/internal/cmd` at its five-minute test deadline; the captured result is
`/tmp/q2-completion-tests.log` (exit code in `/tmp/q2-completion-tests.log.exit`).
This is verification evidence only, not a general performance or full-repository
completion claim.

The native CLI regression subset did pass:

`go test ./bydbctl/internal/cmd -run '^TestAnalyzeSeries(NativePopulated|EmptyDoesNotCreateFiles)$' -count=1 -timeout=1m`

The broad timeout was in the existing Ginkgo `index_rule_test.go` setup while
waiting for `standaloneServerWithAuth`, not in the focused native analyze tests.

The focused CLI `IndexRuleSchema Operation` suite also passed (4 specs):

`go test ./bydbctl/internal/cmd -run '^TestCmd$' -ginkgo.focus 'IndexRuleSchema Operation' -count=1 -timeout=2m`

Evidence is in `/tmp/q2-completion-cli-indexrules.log` with exit code `0` in
`/tmp/q2-completion-cli-indexrules.log.exit`. The affected test command passed
when run without the broad bydbctl suite; whole-repository compile-only checks
and the completed native/nativeice/storage/wqueue race checks also passed. The
full 88-spec bydbctl suite remains incomplete because of its five-minute
deadline, so no full-repository completion claim is made.
