# Draft Q3: native MatchField and Range

> **Status (2026-10-04).** Q2's native owner, shared series/time membership,
> and expanded Stream element-index cutover prerequisites are present and have
> focused tests. This remains the historical Q3 plan: the original
> MatchField/Range acceptance matrix and final repository gates are not claimed
> complete here.

Classification: **planned blocked execution ticket** (two bounded operations, one review/queue unit; not a tracking parent)

## Combined boundary

Merge corrected indexed-presence `MatchField` and full byte/numeric `Range` posting evaluation into one Stream caller-activated execution ticket. Q3 is cohesive native field-dictionary selection: presence and range share Q2's series/time membership helper. Q3 switches Stream `not.Execute` and `rangeOp.Execute` in one reviewable unit while keeping each operation's semantic fixture and guard separately observable. It depends on Q1's native read-only adapter ownership and Q2's shared series/time membership and timestamp-projection helper; live Stream activation also requires NATIVE-OWNER. These are not independently queueable issues.

No legacy `Query`, `Search`, or collector construction/delegation is permitted. Pure live-universe NOT, Boolean composition, MATCH/analyzer, prefix/wildcard, sort, aggregation, and scoring remain excluded.

### MatchField operation

Classification: **planned blocked sub-operation in this merged workpackage**

## Summary
Implement field-presence membership for Stream negative filters and switch the real `not.Execute` caller. This is the accepted DEC-003 correction: field-aware NOT subtracts from indexed-present documents; bare/pure NOT's live universe is not implemented by this leaf.

## Boundary

The proposed native field-dictionary seam returns postings for documents with an actual posting for the requested field/series/time scope, plus matching timestamps; `Searcher.MatchField` is the compatibility caller contract, not permission to construct retired search objects. Presence is posting membership—not schema `Fields()`, stored-field existence, or an empty range. A zero-token analyzed field is absent; an empty keyword is present only if an encoded empty-term posting exists. No legacy query/search/collector bridge, Boolean algebra, or pure-NOT live-universe API.

## Requirements

- Distinguish field-present from live-document universe and preserve series/time bounds.
- Apply deletion masks before results escape; retain empty/singleton posting fast paths with bounded larger sets.
- Propagate cancellation and close owned dictionary/posting cursors exactly once; typed corruption/resource errors.

## Acceptance criteria

- **RED planned, not run:** `go test ./pkg/query/logical/stream ./pkg/index/native ./pkg/index/internal/nativeice -run 'TestNativeMatchField(Presence|ZeroToken|EmptyKeyword|Deleted|CallerCutover)$'`. The current audited `MatchField` delegates to an empty `Range`; expected RED is a planned contract gap, not fabricated execution evidence. A required discovery guard before that run is `set -o pipefail; go test ./pkg/query/logical/stream ./pkg/index/native ./pkg/index/internal/nativeice -list '^TestNativeMatchField' | grep -q '^TestNativeMatchField'`; zero matching tests is a failure, not green.

- **Independent fixture:** live docs `d1` indexed `f=x`, `d2` indexed `f=y`, `d3` stores `f` but has no posting, and deleted `d4` indexed `f=x`. Expected `MatchField(f)` is `{d1,d2}`; a field-aware NOT `f=x` uses present universe and returns `{d2}`. A separate pure-NOT test is explicitly expected to use live `{d1,d2,d3}` and is out of this leaf. Empty keyword is present only in a row with an encoded empty-term posting.
- **End-to-end:** invoke Stream `not.Execute` through the actual index-filter path and assert both field-aware result and absence of a live-universe substitution; restart and repeat.
- **Compatibility/resource:** retained oracle and native existing bytes cross-open; no new writer is required; no writes during read; cancellation/error cleanup is idempotent; sparse/zero-token fields do not trigger schema-wide scans or unbounded materialization.

- **Native-path guard:** the test installs a forbidden retired Writer/Reader/plugin-instantiation and Query/Search/collector spy and fails if `not.Execute` invokes it; it also requires a native-execution counter to be non-zero.
- **Paired microbenchmark (proposed, not run):** first require discovery with `set -o pipefail; go test ./pkg/query/logical/stream ./pkg/index/native ./pkg/index/internal/nativeice -list '^BenchmarkNativeMatchField' | grep -q '^BenchmarkNativeMatchField'`; then run `go test ./pkg/query/logical/stream ./pkg/index/native ./pkg/index/internal/nativeice -run '^$' -bench '^BenchmarkNativeMatchField/(native|oracle)$' -benchmem -count=5`. Compare five standard Go subbenchmark samples: target native median `ns/op <` oracle and median `B/op <=` oracle; this is initial microbenchmark evidence, not a fresh-process paired macro benchmark; report honestly if unmet.
## Scope and readiness

Caller: `pkg/query/logical/stream/index_filter.go:not.Execute`. Exclude pure Boolean NOT live-universe implementation, MatchTerms, MATCH, range, and sort. **Planned blocked on Q2 and NATIVE-OWNER for live Stream activation; revalidate the corrected field-presence seam before queueing.**

Dependencies: Q1 native read-only adapter ownership, Q2 shared series/time membership helper, and NATIVE-OWNER for live Stream activation; this Q3 workpackage cannot queue independently. Relevant design rows: NQ-T01, NQ-T02, NQ-T04, NQ-T05.

Decision: this bounded sub-operation remains separately testable, but queues and merges only as Q3 workpackage; do not smuggle pure NOT or generic Boolean execution into it.

### Range operation

Classification: **planned blocked sub-operation in this merged workpackage**

## Summary
Replace the bounded byte/numeric range posting operation used by Stream range filters. Switch only `rangeOp.Execute`; preserve endpoint semantics and timestamp postings.

## Boundary

The proposed native field-dictionary range seam evaluates the full existing byte-term and numeric-term range contract; `Searcher.Range` is the compatibility caller contract: inclusive/exclusive lower and upper endpoints and the caller's series scope. `RangeOpts.Valid` requires both endpoints; Stream open-ended operators must encode finite minimum/maximum sentinels rather than pass nil. `FieldKey.TimeRange` is a separate timestamp filter. The native adapter returns document/timestamp postings and typed errors. No legacy `Query`, `Search`, collector, Boolean planner, MATCH, sort, aggregation, or score.

## Requirements

- Preserve independent endpoint inclusivity (`QUERY-003`), byte and numeric encoded terms, and the finite sentinel encoding used for Stream open-ended operators. Apply `FieldKey.TimeRange` as a separate filter.
- Apply deletion masks before exposing postings; use bounded dictionary/range traversal and cancellation checks.
- Keep borrowed read-view ownership with caller; close native cursors/scratch once on all exits.

## Acceptance criteria

- **RED planned, not run:** `go test ./pkg/query/logical/stream ./pkg/index/native ./pkg/index/internal/nativeice -run 'TestNativeRange(Endpoints|Unbounded|Numeric|Timestamp|Deletion|Cancellation|CallerCutover)$'`. At the audited commit this operation still resolves through the legacy search path; the command is a planned RED contract, not reported run output. A required discovery guard before that run is `set -o pipefail; go test ./pkg/query/logical/stream ./pkg/index/native ./pkg/index/internal/nativeice -list '^TestNativeRange' | grep -q '^TestNativeRange'`; zero matching tests is a failure, not green.

- **Independent fixture:** numeric field `latency` has live values `10` at d1, `20` at d2, `30` at d3, deleted `40` at d4. `[10,30)` must yield `{d1,d2}`; `(10,30]` must yield `{d2,d3}`; the minimum sentinel through `20` inclusive must yield `{d1,d2}`; deleted d4 never appears. Byte field `name` has `aa`,`bb`,`cc` at d1,d2,d3; `[bb,cc]` must yield `{d2,d3}`. A timestamp fixture has d1 at ts=100, d2 at ts=200, d3 at ts=300; a separate `FieldKey.TimeRange` `(100,300]` selects `{d2,d3}`. Writer omission of timestamps <=0 and DEC-005 signed-time handling are prerequisites/out of scope; no writer change is hidden here. Expected IDs are declared in fixture metadata independently of native range traversal.
- **End-to-end:** execute Stream `rangeOp.Execute` with each endpoint form through the actual filter path, restart, and repeat from immutable bytes; assert timestamps alongside document IDs where requested.
- **Compatibility/resource:** retained oracle and native existing bytes cross-open; no new writer is required; no file mutation; malformed numeric/range offsets return typed corruption within configured bounds; cancellation during dictionary traversal closes state and does not retain candidate buffers.

- **Native-path guard:** the test installs a forbidden retired Writer/Reader/plugin-instantiation and Query/Search/collector spy and fails if `rangeOp.Execute` invokes it; it also requires a native-execution counter to be non-zero.
- **Paired microbenchmark (proposed, not run):** first require discovery with `set -o pipefail; go test ./pkg/query/logical/stream ./pkg/index/native ./pkg/index/internal/nativeice -list '^BenchmarkNativeRange' | grep -q '^BenchmarkNativeRange'`; then run `go test ./pkg/query/logical/stream ./pkg/index/native ./pkg/index/internal/nativeice -run '^$' -bench '^BenchmarkNativeRange/(native|oracle)$' -benchmem -count=5`. Compare five standard Go subbenchmark samples: target native median `ns/op <` oracle and median `B/op <=` oracle; this is initial microbenchmark evidence, not a fresh-process paired macro benchmark; report honestly if unmet.
## Scope and readiness

Caller: `pkg/query/logical/stream/index_filter.go:rangeOp.Execute`. Exclude Boolean composition, pure NOT, MATCH/analyzer, prefix/wildcard, and ordering. **Planned blocked on Q2 and NATIVE-OWNER for live Stream activation; revalidate context propagation and endpoint behavior before queueing.**

Dependencies: Q1 native read-only adapter ownership, Q2 shared series/time membership helper, and NATIVE-OWNER for live Stream activation; this Q3 workpackage cannot queue independently. Relevant design rows: NQ-T01, NQ-T02, NQ-T04, NQ-T05.

Decision: this bounded sub-operation remains separately testable, but queues and merges only as Q3 workpackage; no generic query subsystem.

## Combined readiness and dependencies

The ticket is planned blocked until Q1/Q2 are merged and revalidated and NATIVE-OWNER exists for live Stream activation. Nativeice committed-read-only evidence does not provide NRT ownership. Both Stream callers must switch in the same merge; direct operation tests do not satisfy production activation.

Decision: the two operations fit one bounded review/queue unit as explicitly requested; do not split them into separate issue drafts.
