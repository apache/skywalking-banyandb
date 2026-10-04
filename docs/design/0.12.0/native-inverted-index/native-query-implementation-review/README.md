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

## Native ownership prerequisite and graph

The rejected upstream getter/bridge plan is not part of these tickets. Native query must not rely on a retired-engine runtime, writer reader, or search/collector plugin. Q1 is a complete committed-generation read-only resolver transition (exact lookup plus bounded identifier visitation) over the planned pure native packages (`pkg/index/native` committed-reader interface and `pkg/index/internal/nativeice` implementation), mechanically moved from the current inverted package without retired imports on the native path (`OpenReadOnlyGeneration`, `Reader`, `Dictionary`/`TermPosting`/`VisitDocument` seams) and can activate the immutable dump resolver without an NRT owner.

`NATIVE-OWNER` is an explicit prerequisite for live Stream membership: a BanyanDB-owned native publication/ownership capability must pin one coherent persisted + NRT segment/deletion view, keep segment coordinates stable, own close/cancellation/lifetime, and publish generation/deletion state atomically. It must expose a typed native seam without a retired-engine runtime, second cache, reflection, or plugin fallback. It is not implemented by these drafts; do not disguise writer/publisher/recovery/merge/GC as Q1.

```
validated nativeice committed-generation/read-only seams
       └── Q1 StoredFields + PartSeriesMap + immutable IndexResolver activation
              └── Q2 SeriesIterator + MatchTerms + series/time helper
                     └── Q3 MatchField + Range
NATIVE-OWNER blocks Q2 MatchTerms live activation and Q3 live Stream activation. Its readiness contract remains unfinished overall design work, not a fourth ticket or dead foundation.
```

### NATIVE-OWNER readiness contract (unfinished overall design work)

Before Q2/Q3 live portions can queue, an implementation must prove: (1) one native publication root pins persisted and NRT segments plus generation deletes coherently; (2) a query opened before a write/delete/merge remains on its pinned generation while a later query sees the published generation; (3) segment-local document numbers never cross generations; (4) cancellation, caller abandonment, corruption, and close release native readers/cursors exactly once; (5) no retired-engine runtime, fallback, second cache, or reflection path is reachable; and (6) retained native/older fixtures remain readable under mixed-version restart/resource-bound tests. These are acceptance gates for the unfinished owner capability, not a fourth issue or work smuggled into Q1.

Q1 is a planned execution ticket with no external/NRT blocker but is not automation-ready until its native seam/readiness audit is verified. Q2 and Q3 are planned blocked execution tickets until NATIVE-OWNER exists. The requested operation pairs are not tracking parents or independently queueable subissues. Their wider caller/test matrices and seam counts must be revalidated before automation. The three tickets do not implement the complete native owner or full query engine.

## Remaining scope not covered

Full native `Search`, Boolean algebra, `MATCH`/analyzer override, prefix/wildcard dictionary expansion, projection/sort/search-after, Property/Measure/Trace query cutovers beyond the named operations, writer/merge/expiry/GC/replication lifecycle, external segments, admin/migration/rebuild and dependency removal (NIDX-05), aggregation, scoring, and the 1.2M-document performance gate remain out of this wave. No tracking umbrella is created here.

## Shared implementation constraints

Fixtures and expected values are authored independently (hand-written corpus + retained compatibility oracle identified by immutable revision/content hash). Read existing nativeice committed fixtures and retained oracle fixtures; prove read-only bytes/mtime/manifest immutability. Writer compatibility is an optional existing-suite gate, not new writer work in these workpackages. The native executor borrows the caller-pinned view; it never closes/transfers it. Engine-owned cursors/scratch close exactly once on success, cancellation, typed corruption, and caller abandonment. Thread context through membership/decoding and discard bounded candidate/materialization buffers; reuse existing query/result budgets and `pkg/pool.Bounded`. Every operation adds a test-only forbidden retired Writer/Reader/plugin-instantiation and Query/Search/collector spy and fails if that path is invoked. Each workpackage proposes five standard Go native/oracle subbenchmark runs, targeting native median ns/op < oracle and median B/op <= oracle; this is initial microbenchmark evidence, not a fresh-process paired macro benchmark, and no result is claimed here. The 1.2M campaign remains deferred, not all performance evidence. Do not add a second cache or unbounded dictionary/posting materialization.

## Native package migration note

Before Q1 implementation queueing, the source audit must verify the selected minimal mechanical move from `pkg/index/inverted/internal/nativeice` to `pkg/index/internal/nativeice` and expose only the pure `pkg/index/native` committed-reader interface. Retired writer/reader/plugin imports may remain in isolated oracle tests only; they must not be reachable from native production code. This is a seam/readiness verification, not a fourth ticket or a new framework.

## Source questions to revalidate before queueing

- `NewIndexResolver` currently constructs the live `SeriesStore` route; `Resolve` caches by series ID while using `Background`, and `PartSeriesMap` uses `r.store.SeriesIterator` for missing-series metadata. Q1 changes both paths to a committed nativeice `ReadOnlyGeneration` owner: exact raw stored fields plus bounded identifier visitation over multi-segment fixtures, without `NewNativeStore`, resolver signature changes, cache-policy changes, or a general public iterator. The audited legacy `StoredFields` primitive advances `dmi.Next` once; the native replacement must stop its `VisitSelectedDocuments` callback after the first live matching physical document, while preserving every repeated value in that document. Duplicate logical IDs therefore have an explicit first-snapshot-order fixture. `PartSeriesMap` only needs a result-map identifier walk and no new global lexical-order guarantee. Direct context tests cover the committed read-only seam; NRT tests remain blocked on NATIVE-OWNER.
- `SeriesIterator` is dictionary metadata, not live query hits: with its segment pinned, a fully deleted term remains visible (five fixed fixture terms); a new compacted generation may drop it. Preserve this established diagnostic behavior.
- `Searcher.MatchTerms`, `MatchField`, and `Range` currently lack context in their public signatures and audited paths use retired search construction/`context.TODO`; Q2 must establish a native context-capable private seam, with Q3 consuming it once NATIVE-OWNER exists.
- `RangeOpts` supports both byte and float term values and requires both endpoints; Stream open-ended ranges use finite sentinels. `FieldKey.TimeRange` is separate. Writer omission of timestamps <=0 and DEC-005 are outside this read-only wave.
- The source audit must verify the existing native multi-segment committed-reader primitive and newest-complete-generation/`SnapshotID` fallback before Q1 queueing. No source audit here proves NATIVE-OWNER exists; revalidate native ownership, NRT deletes, borrow lifetime, and mixed-version behavior before queueing Q2/Q3 live portions. Pending writer/publisher/recovery/merge/GC lifecycle work remains outside these three tickets.
