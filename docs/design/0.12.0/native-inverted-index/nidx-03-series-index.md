<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to You under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain a
copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.
-->

# NIDX-03: Native per-segment series index

Status: design, for review. Base: `main` at `735e9ad2` (#1390 merged).
Tracker: apache/skywalking#14003, parent #13990.
Delivery: one change set, not split into leaves.

## 1. Goal

The per-segment series index (`seg-*/sidx`) of Measure, Stream, and Trace
runs on the native index only. The legacy series code is removed in the
same change:

- There is no engine flag and no switch between engines.
- No series-index code path, writer, reader, migration tool, or dump tool
  imports `pkg/index/inverted` or the retired library.
- Upgrades are whole-cluster: every node is stopped and restarted on the new
  binary. Mixed-version clusters and rolling upgrades are not supported, so
  there is no cross-version wire or replication compatibility to keep.
- Rollback is a file property, not a code path. The binary contains no
  legacy engine and no way to select one; rolling back means stopping the
  cluster and starting the previous release on the same data directories.

So the files must work in both directions, with no replay or conversion:

- **Forward:** the new binary opens `sidx` directories written by the
  previous release. They hold ICE v3 segments and snapshot v3 manifests,
  which the native reader has read since NIDX-01.
- **Back:** the previous release opens `sidx` directories written by the new
  binary and returns the same results. Native therefore writes exactly the
  layout the previous release reads (§2.2), including `_id` doc values, and
  introduces no new file kinds the previous release would trip over.

**In scope.** Everything that reads or writes a series index:

- Write: field-set-aware insert-if-absent, update, version and timestamp.
- Read: exact, prefix, and wildcard series identity; composed filters
  (term, range, MATCH, IN, NOT, HAVING); index-rule sort; projection.
- Index-mode Measure reads and writes; stats; file snapshot; cache reset.
- Crash-safe external segment receive and the lifecycle sender.
- Offline tools that touch `sidx`: the union-sidx builder, the index-mode
  Measure copy, and the `sidx` dump helpers.

- Property's unused legacy switch and its last `pkg/index/inverted` uses
  (§15).
- `_id` doc values for the Stream element index (§16).

**Out of scope.**

- The rest of the Stream element `idx` work and its migration tool
  (NIDX-04). That tool is then the last user of `pkg/index/inverted`, so the
  package stays until NIDX-04 removes it.

**Merge proof.**

- Real Measure, Stream, and Trace segment flows use native series insert,
  lookup, sort, replication, restart, snapshot, and raw segment receive.
- A directory written by the previous release opens and answers queries
  with the results pinned before the legacy code was removed.
- The previous release opens directories written by the new binary and
  returns the same results (§12, rollback test).
- `go list -deps` of the storage, measure, stream, and trace packages
  reaches no series code in `pkg/index/inverted`, enforced by a guard test.

## 2. The contract

### 2.1 Operations

These semantics are what callers rely on today. They are kept as product
behaviour, not for compatibility.

| Operation | Callers | Semantics |
|---|---|---|
| `Insert(docs)` | Measure metadata docs, Stream series docs, Trace liaison series docs | Insert-if-absent. Skip a doc if its `_id` is live in some segment **and** that segment's field set contains every field name of the doc. Otherwise replace it. This is how a series picks up fields from a newly added index rule. |
| `Update(docs)` | Index-mode Measure, Trace standalone | Full replace by `_id`, last writer wins, no version check. |
| `Search` | Measure query, Stream and Trace `segment.Lookup` | Series matchers AND the filter AND the time range. Unordered. Projection returns one value per key (last stored value wins), nil when absent. |
| `Search` with an index order | Measure query | Sort on the index-rule field's doc values. Returns `SortedValue`, timestamp, version, and projection. Callers heap-merge segments by `bytes.Compare(SortedValue)`. |
| `SearchWithoutSeries` | Index-mode Measure | As above, with no matchers. The filter is `_im_name` AND the criteria. |
| `EnableExternalSegments` | `*-series-sync` receivers | Raw segment stream. The **existing doc wins**: incoming duplicates of a live `_id` are dropped. |
| `Stats` | metrics, segment stats | Live docs and on-disk bytes. |
| `ResetCache` | rotation, for segments past their end time | Drop the insert-presence cache only. Never data. |
| `TakeFileSnapshot` | backup, lifecycle snapshot | A consistent copy of the committed generation. |

### 2.2 Series document

| Field | Content | Indexed | Stored | Doc values |
|---|---|---|---|---|
| `_id` | `EntityValues`, the marshalled `pbv1.Series` | yes | yes | yes |
| `FieldKey.Marshal()`, `Index=true` | raw bytes, or analyzed terms if an analyzer is set | yes | if `Store` | if `!NoSort` |
| `FieldKey.Marshal()`, `Index=false` | raw bytes | no | yes | no |
| `_timestamp` (only when > 0) | prefix-coded int64 ns; shift terms 0..60 step 4 | yes | yes | yes |
| `_version` (only when > 0) | `convert.Int64ToBytes` | no | yes | no |

- The series ID is `convert.Hash(_id)` and is never stored.
  `Document.DocID` is ignored.
- Index-mode helper fields `_im_name` and `_im_entity_tag_<tag>` are indexed,
  not stored, and have no doc values.
- `_id` doc values are not used by the new binary. They are written only
  because the previous release reads document identity through them; without
  them a rolled-back node reports identifiers as empty or 0. The Stream
  element index written by #1390 lacks them today (§16).

## 3. Layering

The native engine already provides the inverted index; the series index is
an application on top of it, the same way the Property store
(`banyand/property/db/native_property.go`) is. The work splits into two
layers, and anything that only the series index needs lives in the upper
one.

| Layer | Owns | Package |
|---|---|---|
| Engine | Segments, admission, persistence, compaction, term and range postings, scope and time filtering, sorting, projection, external segments, snapshots | `pkg/index/native` (exists) |
| Criteria filter | Translating `modelv1.Criteria` into engine calls over a candidate set: EQ, NE, IN, NOT_IN, HAVING, NOT_HAVING, ranges, MATCH, AND, OR | today inside Property; promoted to a shared package (§5) |
| Series index | Series document mapping, identity matching, `IndexDB` results, receive plumbing | `banyand/internal/storage` (`index.go`) |

### 3.1 What the engine already does for the series index

| Series need | Engine primitive (no change) |
|---|---|
| Upsert (`Update`) | `Owner.Batch` (default mode) |
| Exact identity, many series | `MatchTermsSet{Field: "_id", Mode: MatchAnyTerm}` |
| Time range | `QueryScope.TimeRange` on the first request |
| Conjunct filters | `MatchAllTermSets`, `FilterTermsSet`, `FilterRange` |
| Byte-order ranges on tag values | `MatchRange` / `FilterRange` |
| Index-rule sort | `SortHits{Field, Desc}`; rows without a value sort last |
| Projection, timestamp, version | `ProjectHit` |
| Snapshot, stats | `Owner.TakeFileSnapshot`, `Owner.Stats` |
| Raw segment receive | `Owner.EnableExternalSegments` |
| Async persistence | `OwnerOptions.PersistInterval` (#1390) |

### 3.2 What the engine lacks

Four gaps, and each is a general engine feature, not series logic:

1. **Pattern terms.** No `ReadView` call accepts a prefix or wildcard.
   `nativeice` already runs vellum automata over the dictionary
   (`NewDictionaryTermIterator`), but `native` always passes `nil`.
2. **Insert-if-absent admission.** `Batch` has upsert and insert-only, not
   "skip when already present". The check needs segment field sets and must
   run under the owner lock against the admission root, so it cannot be done
   safely above the engine.
3. **Keep-existing external deduplication.** The series index drops
   incoming documents whose `_id` is already live. The native receiver can
   only ignore duplicates or let the incoming copy win.
4. **Identifier doc values.** The encoder always writes `_id` without doc
   values. The previous release needs them to read identity back (§2.2).

## 4. Engine additions (`pkg/index/native`)

### 4.1 Pattern term requests

`TermSetRequest` gains `Prefix [][]byte` and `Wildcard [][]byte`, ORed with
`Terms`:

- Prefix iterates the dictionary range [p, successor(p)).
- Wildcard compiles to a `vellum/regexp` automaton passed to
  `NewDictionaryTermIterator`. Compilation follows the escaping the series
  index uses today: regexp metacharacters (including `|` and `\`) are
  escaped, `*` becomes `.*`, and `?` becomes `.`.

Expanded terms count against `MaxTerms`, like `MatchRange`.

### 4.2 `InsertIfAbsent`

`Batch.Mode` gains `InsertIfAbsent`. For each document the owner checks, on
the admission root, whether some segment has a live posting for its
identifier **and** contains every field name the document carries. Present
documents are skipped; the rest are upserted.

- The check reuses the identifier index for admitted segments and the
  exact term-set and bloom filters for others (#1390).
- Each segment's field-name set is computed once per handle.
- A batch whose documents are all present publishes nothing: no segment,
  no generation change, no persist. Only absent documents are encoded.
- An optional presence cache (`OwnerOptions.PresenceCacheBytes`) remembers
  "present with this field set" per identifier, positive answers only, and
  survives across batches. An upsert or delete of an identifier drops its
  entry; a prefer-incoming receive drops the masked identifiers; a merge
  that drops documents and `Owner.Reset` clear the cache.
  `Owner.ResetPresenceCache` clears it on demand.

### 4.3 Keep-existing external deduplication

`OwnerOptions.ExternalDedup` replaces the `DeduplicateExternal` flag with
`None` (today's default), `PreferIncoming` (today's `DeduplicateExternal`),
and `KeepExisting`. `KeepExisting` masks incoming documents whose identifier
is already live, in the incoming segment's deletion bitmap, and leaves
existing segments alone. Everything else about receive (staging, validation
before visibility, atomic introduction) is unchanged.

### 4.4 Identifier doc values

`OwnerOptions.IdentifierDocValues` makes the encoder and the merger write
`_id` as a doc-value column too, as the previous release's writer does. Merge
splices it like any other doc-value field. The series index turns it on;
Property's file rollback is already proven without it and stays as is.

## 5. Shared criteria filter

Property already turns `modelv1.Criteria` into engine calls over a candidate
set (`matchCriteria`, `matchCondition`, `intersectHits`, `unionHits`,
`subtractHits`). The series index needs the same thing, with different field
names. That code moves to `pkg/index/native/criteria` and takes a field
resolver:

```go
type FieldResolver interface {
	// Field returns the engine field and analyzer for a tag, or false when the
	// tag is not indexed for this caller.
	Field(tagName string) (field string, analyzer string, ok bool)
}

func Filter(ctx context.Context, view *native.ReadView, universe []native.QueryHit,
	criteria *modelv1.Criteria, fields FieldResolver) ([]native.QueryHit, error)
```

- Property passes its `_tag_<name>` resolver; its behaviour is unchanged and
  its existing tests guard that.
- Measure passes a resolver over the index rules: the 4-byte rule ID, or
  `_im_entity_tag_<tag>` for index-mode entity tags without a rule.
- Ranges use `FilterRange` with unbounded `MaxTerms`, MATCH uses
  `nativeanalysis`, and NE, NOT_IN, NOT_HAVING subtract from the universe,
  exactly as Property does today.

The query planner stops building a bluge query. It passes the criteria and
the resolver through `IndexSearchOpts`. The entity extraction that
`inverted.BuildQuery` does today (pushing conditions on entity tags into the
series matchers) moves unchanged into a planner helper, so the matchers and
the remaining criteria are the same as before.

## 6. Series index (`banyand/internal/storage/index.go`)

`seriesIndex` holds a `*native.Owner` and keeps its `IndexDB` surface
(`Insert`, `Update`, `Search`, `SearchWithoutSeries`,
`EnableExternalSegments`, `Stats`) and `Lookup`.

### 6.1 Writes

| `index.Document` | native `Document` |
|---|---|
| `EntityValues` | `Identifier` (`_id`) |
| `Timestamp > 0` | `Timestamp` (`_timestamp`) |
| `Version > 0` | stored-only `_version` (`Int64ToBytes`) |
| field, `Index=true` | `Field{Index: true, Store, Sort: !NoSort}`; analyzer terms from `nativeanalysis` |
| field, `Index=false` | `Field{Index: false, Store: true}` |

`Insert` is `Batch{Mode: InsertIfAbsent}`, `Update` is the default upsert.
Persistence follows `SeriesIndexFlushTimeoutSeconds`: asynchronous with
`PersistInterval` when it is positive, synchronous at zero.

### 6.2 Reads

`Search` is a short sequence of engine calls:

1. **Universe.**
   - With series: one `MatchTermsSet` on `_id` carrying the exact values,
     prefixes, and wildcard patterns from the matchers, with
     `Scope.TimeRange`.
   - Without series (index mode): `MatchTermsSet` on `_im_name`, with
     `Scope.TimeRange`.
2. **Filter.** `criteria.Filter(view, universe, criteria, resolver)`.
3. **Order.** With an index order, `SortHits` on the rule's field;
   otherwise the hit order.
4. **Results.** `ProjectHit` for `_timestamp`, `_version`, and the
   projection; series from the identifier; `SortedValue` from
   `ProjectSortValue` when sorted.

`SeriesData` gains per-row presence for timestamp and version, which also
fixes the index-mode misalignment (§11). `ResetCache` calls
`ResetPresenceCache`; it never touches data.

### 6.3 Open and lease

- `newSeriesIndex` opens `native.NewOwner` on `seg-*/sidx` with the TSDB
  root lease, `ExternalDedup: KeepExisting`, and the presence cache sized by
  `SeriesIndexCacheMaxBytes`.
- Stream already passes `RootLeaseFactory`; Measure and Trace gain it.
- On open, the series index removes the previous release's
  `external-segment-temp` directory and `bluge.pid`.

## 7. Replication

Nothing changes on the wire or on the sender. The lifecycle visitors keep
streaming the raw `sidx/*.seg` files, and the file format is unchanged, so
the native receiver accepts segments written by either release.

The only receiver change is the owner it feeds: `EnableExternalSegments`
returns the native owner's streamer, opened with `ExternalDedup:
KeepExisting` (§4.3) so a duplicate `_id` keeps the existing document, as
today.

Validation before visibility comes with the native receiver: a truncated or
corrupt file fails `OpenSegmentFile` and is never introduced, where the
legacy receiver copied it into `sidx` first.

## 8. Storage lifecycle

| Area | Change |
|---|---|
| Open | `native.NewOwner` recovers the newest complete snapshot: previous-release, native, empty, or new. |
| Close | `closeResourcesLocked` no longer panics on a series-index close error. It logs and marks the segment failed. |
| Reset | rotation calls `ResetCache()`. |
| Stats, open | `Owner.Stats()`. |
| Stats, closed | native read-only generation count, replacing `inverted.ReadOnlyDocCount`. |
| Snapshot, open | `Owner.TakeFileSnapshot`, which streams disk-backed segments (#1390). |
| Snapshot, closed | `snapshotClosed` hard-links only the newest committed snapshot and the segments it references. |
| GC | the owner deletes files its current root does not reference, after each persist. On first open this removes segments only older snapshots referenced. |
| Format | ICE v3, snapshot v3, and segment `metadata` are unchanged, and `fileformat.CurrentVersion` is unchanged, so the previous release accepts the segment directories. Native staging files (`.native-external-*`) are never part of a snapshot; the previous release ignores any that a crash leaves behind. |

## 9. Offline tools

| Tool | Today | After |
|---|---|---|
| Union-sidx builder (`banyand/internal/migration/unionsidx.go`) | reads with `inverted.ReadOnlyWalkDocuments`, writes with the retired library | reads with native read-only generations, writes a native owner through the series document mapping (`storage.EncodeSeriesDocument`, exported from §6.1) |
| Index-mode Measure copy (`banyand/measure/migration_indexmode_copy.go`) | `inverted.ReadOnlyWalkDocuments` + `inverted.NewStore` `UpdateSeriesBatch` | native read-only walk + native owner upsert through the same mapping |
| `sidx` dump (`banyand/cmd/dump/sidx.go`, `banyand/internal/dump/helpers.go`) | writable `inverted.NewStore` + `SeriesIterator` | `native.OpenReadOnlyGeneration(...).NewSeriesIterator` (read-only, no lock) |

## 10. Code removed

- `banyand/internal/storage/index.go`: the `inverted.NewStore` series store,
  `BuildQuery`/`SeriesSort` calls, and `ReadOnlyDocCount` use.
- `pkg/index/inverted/query.go`: `BuildQuery`, `BuildIndexModeQuery`,
  `buildIndexModeCriteria`, and the node types only they use. The Property
  builders stay.
- `pkg/index/inverted/inverted_series.go`: the `_id` prefix and wildcard
  matcher nodes and the `timeRange` handling in `store.BuildQuery`, which
  only the series index passed. Property's exact-matcher use stays.
- The union-sidx writer's direct use of the retired library.
- Series-index tests that drive the legacy store, replaced by §12.

## 11. Hazards fixed in the same change

| Hazard | Fix |
|---|---|
| `Timestamps` and `Versions` misalign with `SeriesList` on unsorted search; index-mode `copyTo` reads them by position and can panic when `Version == 0`. | Per-row presence in `SeriesData`, used by `copyTo`. |
| Sorting with an empty query panics. | The universe is always explicit (§6.2 step 1). |
| A series-index close error panics the node. | §8. |

## 12. Test plan

1. **Pinned previous-release behaviour.** Before deleting the legacy code,
   a one-off run of the current store produces checked-in `sidx` fixtures
   (exact, prefix, wildcard, every filter kind, index-mode Measure, sort)
   and their query results as literal expectations. The native store must
   open the fixtures and return the same results. The generator is not kept.
2. **Rollback test.** An e2e job writes Measure (normal and index mode),
   Stream, and Trace data with the new binary, including updates, external
   receive, and compaction, stops it, starts the pinned previous-release
   image on the same volume, and checks that every query returns the same
   result. A crash-cut variant kills the new binary mid-persist first.
3. **Reference-model property test.** A plain in-memory model of §2
   (a map from `_id` to document plus the insert-if-absent rule) runs
   against the series index under seeded random Insert, Update, external
   receive, Search, and sorted Search. Results must match. Fixed seed in CI,
   longer behind a flag.
4. **Criteria filter.** Property's existing tests run against the shared
   package unchanged; new table tests cover the Measure resolver and entity
   extraction for every condition kind.
5. **Engine additions.** Prefix and wildcard requests (including `*`, `?`,
   `|`, and `\` in values), `InsertIfAbsent` field-set rules, and each
   external dedup mode.
6. **Crash cuts:** after a batch and mid external receive; the reopened
   root is a complete generation.
7. **Receive:** raw segments written by the previous release and by native
   are both accepted; a duplicate `_id` keeps the existing document; a
   truncated file is rejected and nothing becomes visible.
8. **Dependency guard:** storage, measure, stream, and trace reach no series
   code in `pkg/index/inverted`.
9. **Integration and e2e:** the existing measure, stream, trace, lifecycle,
   backup, and dump suites.

## 13. Performance targets

Baselines are measured on `main` before the legacy code is removed.

| Path | Target |
|---|---|
| Measure write (`Insert` of existing series) | ≥ baseline throughput |
| Stream and Trace `Lookup`, exact | ≤ baseline p50 latency |
| Prefix and wildcard lookup | ≤ 1.2× baseline |
| Index-order `Search` | ≤ baseline |
| External receive | ≤ baseline wall time |

A new `banyand/internal/storage` benchmark over 1M series covers these, and
the slow-disk check uses the dm-delay method from #1390.

## 14. Implementation phases (one PR)

The phases are commits in one PR. Each phase leaves the tree building and its
tests passing. The §13 baselines are measured on `main` before phase 1.

**Phase 1: engine and criteria filter.** No caller changes yet.

- `pkg/index/native`: pattern term requests (§4.1), `InsertIfAbsent` with
  the presence cache (§4.2), `ExternalDedup` (§4.3), identifier doc values
  (§4.4).
- `pkg/index/native/criteria` (§5), moved out of Property. Property switches
  to it.
- Exit: engine and criteria tests (§12 items 4, 5); Property suites unchanged.

**Phase 2: series index cutover.**

- `banyand/internal/storage`: `seriesIndex` on `native.Owner`, write mapping,
  search as engine calls, `SeriesData` presence, lease, lifecycle (§6, §8);
  the external receiver on the native owner with `KeepExisting` (§7).
- Measure planner: criteria and resolver, entity extraction helper. Measure
  and Trace: `RootLeaseFactory`. Measure: `copyTo` fix.
- Offline tools on native (§9).
- Exit: previous-release fixtures, reference-model test, crash cuts, receive
  tests, and integration suites (§12 items 1, 3, 6, 7, 9).

**Phase 3: legacy removal and the rest of the scope.**

- Legacy series code removed (§10).
- Property legacy switch, branches, and helpers removed; dump tool on native
  (§15).
- Stream element index: `IdentifierDocValues` on (§16).
- Exit: dependency guard, rollback e2e with the pinned previous-release image,
  Property file-rollback rerun, §13 benchmarks against the baselines, CHANGES
  and docs (§12 items 2, 8).

## 15. Property: the same rule (in this PR)

Property's binary still carries a legacy switch that production never uses:
`IndexConfig.NativeWriter` is hard-coded `true`, and nothing outside tests
calls `SwitchIndexWriter`. Under the rule above it goes too:

- Remove `NativeWriter`, `SwitchIndexWriter`, `shard.store`, and the legacy
  branches in `shard.go`, `db.go`, and `repair_gossip.go`, together with
  `BuildPropertyQuery` and `BuildPropertyQueryFromEntity`.
- Move the metrics type and the `ReadOnlyWalkDocuments` /
  `ReadOnlySelectDocuments` helpers (already `nativeice`-backed) out of
  `pkg/index/inverted`.
- Move the property dump tool to the native read-only API.
- Keep the Property file rollback test (native-written shards opened by the
  previous release), which #1390 recorded at `588cc602`; rerun it at the
  final commit.

## 16. Stream element index `_id` doc values (in this PR)

The Stream element index went native in #1390 without `_id` doc values, so
the previous release reads its document IDs as 0 after a rollback. Turning
on `IdentifierDocValues` for the element index fixes new segments; segments
already written by #1390 stay affected until they expire. The rollback test
(§12) covers the element index too.
