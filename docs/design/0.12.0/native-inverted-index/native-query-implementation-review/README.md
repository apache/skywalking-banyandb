# Native query implementation review — first execution wave

Status: **historical planning record, Q1–Q3 bounded operations implemented**. Q1 StoredFields was committed in `044ea235`; Q2 SeriesIterator/MatchTerms ownership and native Stream compatibility, plus Q3 MatchField/Range acceptance, are implemented in this working tree. Broader query-engine scope, full production performance gates, and 1.2M-document evidence remain. The original audit source is `8a9f9af2da0b6d275267dd91002bafc661577278` (merged Apache-main `aa145bc34770811fa93e4c38b3641bbfec0b9b3c`); `3f2e866a6a83b9d404b00f61a2cdf300ab6de3c3` is historical evidence only.

## Proposed boundaries

This is a deliberately narrow first execution wave: three bounded execution tickets containing five independently observable operations, with each merged ticket switching its named real callers in the same merge. Q2 and Q3 are combined scopes, not tracking parents or silently resplit subissues.

1. Q1 `StoredFields` exact lookup through the existing read-only `banyand/internal/dump/index_resolver.go:NewIndexResolver` store construction and `IndexResolver.Resolve` caller.
2. Q2 merged workpackage: `SeriesIterator` plus `MatchTerms`, switching `bydbctl analyze series` and Stream `eq.Execute` together.
3. Q3 merged workpackage: corrected `MatchField` plus `Range`, switching Stream `not.Execute` and `rangeOp.Execute` together.

These are bounded workpackages, not a disguised replacement of `Query`, `Search`, or collectors. They return only the operation's existing IDs/postings/values. No workpackage may construct or delegate to legacy query/search/collector objects; native aggregation and scoring are out of scope.

Drafts:

- [Q1 — StoredFields exact lookup](./01-stored-fields.md)
- [Q2 — SeriesIterator + MatchTerms](./02-series-iterator-and-match-terms.md)
- [Q3 — MatchField + Range](./03-match-field-and-range.md)

## Native ownership and graph (historical boundary plus current implementation)

The rejected upstream getter/bridge plan is not part of these tickets. Native query must not rely on a retired-engine runtime, writer reader, or search/collector plugin. Q1 is a complete committed-generation read-only resolver transition (exact lookup plus bounded identifier visitation) over the planned pure native packages (`pkg/index/native` committed-reader interface and `pkg/index/internal/nativeice` implementation), mechanically moved from the current inverted package without retired imports on the native path (`OpenReadOnlyGeneration`, `Reader`, `Dictionary`/`TermPosting`/`VisitDocument` seams) and can activate the immutable dump resolver without an NRT owner.

The original queue called this capability `NATIVE-OWNER`. The current Q1/Q2
implementation provides the BanyanDB-owned native publication/root lease,
coherent persisted plus NRT views, stable segment coordinates, close and
cancellation ownership, and atomic generation/deletion publication. It uses
typed native seams without a retired-engine runtime, second cache, reflection,
or plugin fallback. The paragraphs below retain the original readiness
contract as historical provenance, not as a current blocker.

```
validated nativeice committed-generation/read-only seams
       └── Q1 StoredFields + PartSeriesMap + immutable IndexResolver activation
              └── Q2 SeriesIterator + MatchTerms + series/time helper
                     └── Q3 MatchField + Range
Q1 read-only seams feed the native owner; Q2 MatchTerms and Q3 field/range
callers are activated through the current Stream factory and adapter.
```

### NATIVE-OWNER readiness contract (historical acceptance checklist)

The original checklist required: (1) one native publication root pins persisted
and NRT segments plus generation deletes coherently; (2) old queries remain
on their pinned generation while later queries see the published generation;
(3) segment-local document numbers do not cross generations; (4) cancellation,
abandonment, corruption, and close release readers/cursors exactly once; (5)
no retired runtime, fallback, second cache, or reflection is reachable; and
(6) retained fixtures survive mixed-version restart/resource tests. Focused
Q1-Q3 tests provide evidence for these items; the full failure-injection and
large-scale gates remain outside this bounded PR.

Q1-Q3 are implemented bounded execution tickets, not tracking parents or
independently queueable leaves. Their focused caller matrices pass; the three
tickets do not implement the complete query engine or claim every repository
performance/failure-injection gate.

## Remaining scope not covered

Full native `Search`, Boolean algebra, `MATCH`/analyzer override, prefix/wildcard dictionary expansion, projection/sort/search-after, Property/Measure/Trace query cutovers beyond the named operations, writer/merge/expiry/GC/replication lifecycle, external segments, admin/migration/rebuild and dependency removal (NIDX-05), aggregation, scoring, and the 1.2M-document performance gate remain out of this wave. No tracking umbrella is created here.

## Shared implementation constraints

Fixtures and expected values are authored independently (hand-written corpus + retained compatibility oracle identified by immutable revision/content hash). Read existing nativeice committed fixtures and retained oracle fixtures; prove read-only bytes/mtime/manifest immutability. Writer compatibility is an optional existing-suite gate, not new writer work in these workpackages. The native executor borrows the caller-pinned view; it never closes/transfers it. Engine-owned cursors/scratch close exactly once on success, cancellation, typed corruption, and caller abandonment. Thread context through membership/decoding and discard bounded candidate/materialization buffers; reuse existing query/result budgets and `pkg/pool.Bounded`. Every operation adds a test-only forbidden retired Writer/Reader/plugin-instantiation and Query/Search/collector spy and fails if that path is invoked. Each workpackage proposes five standard Go native/oracle subbenchmark runs, targeting native median ns/op < oracle and median B/op <= oracle; this is initial microbenchmark evidence, not a fresh-process paired macro benchmark, and no result is claimed here. The 1.2M campaign remains deferred, not all performance evidence. Do not add a second cache or unbounded dictionary/posting materialization.

## Native package migration note

Before Q1 implementation queueing, the source audit must verify the selected minimal mechanical move from `pkg/index/inverted/internal/nativeice` to `pkg/index/internal/nativeice` and expose only the pure `pkg/index/native` committed-reader interface. Retired writer/reader/plugin imports may remain in isolated oracle tests only; they must not be reachable from native production code. This is a seam/readiness verification, not a fourth ticket or a new framework.

## Source questions retained from the original queue review

- `NewIndexResolver` originally constructed the live `SeriesStore` route; Q1 changed both resolution paths to the committed native owner/read-only seam while preserving cache and signature contracts. Direct and caller tests cover exact stored fields, bounded identifier visitation, and independent fixtures; NRT ownership is now covered by the current owner tests.
- `SeriesIterator` is dictionary metadata, not live query hits: with its segment pinned, a fully deleted term remains visible (five fixed fixture terms); a new compacted generation may drop it. Preserve this established diagnostic behavior.
- `Searcher.MatchTerms`, `MatchField`, and `Range` now use the native context-capable private seam; Q2/Q3 caller tests cover cancellation and bounded traversal.
- `RangeOpts` supports both byte and float term values and requires both endpoints; Stream open-ended ranges use finite sentinels. `FieldKey.TimeRange` is separate. Writer omission of timestamps <=0 and DEC-005 are outside this read-only wave.
- The source audit verified the native multi-segment committed-reader primitive, newest-complete-generation fallback, native ownership, NRT deletes, borrow lifetime, and mixed-version reopen behavior. Broader writer/replication/large-scale lifecycle work remains outside these three tickets.
