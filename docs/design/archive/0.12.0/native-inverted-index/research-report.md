# BanyanDB-Native Inverted Index — Research Report

## Status

This report summarizes the parallel source, format, compatibility, and architecture research requested by
[`research-plan.md`](research-plan.md).

The source-level research is complete enough to recommend an architecture and implementation sequence. It is **not** a
claim that writer compatibility is already proven. Historical fixture collection, an independent codec prototype,
cross-reader tests, fuzzing, crash injection, and production-shaped benchmarks remain hard gates before production code
can replace Bluge.

## 1. Executive conclusion

BanyanDB can replace Bluge and ICE without recreating a general search engine. The production requirement is a narrower
immutable-segment engine with:

- exact/prefix/wildcard term lookup, byte/numeric/time ranges, boolean filtering, and analyzed MATCH;
- stored fields and doc values for projection and explicit sorting;
- immutable snapshots with per-segment deletion masks;
- update-by-ID, field-evolving series insertion, persistence callbacks, merge/GC, and read-only administration;
- Property merge-time expiration; and
- raw ICE-segment receive/introduction for Stream, Measure, and Trace migration.

The static source inventory found no current production caller for BM25 scoring, phrase queries, fuzzy queries,
highlights, facets, aggregations, boosts, explanations, geo search, or a general extension API. Fixtures must still prove
which frequency/norm/location structures a legacy reader requires from a valid writer.

The recommended design is a **direct immutable-segment builder** backed by a BanyanDB-owned query IR, snapshot manager,
query executor, persistence lifecycle, and bounded ICE v3 codec. It should retain only the low-level codecs whose
serialized bytes are already part of the format—most likely Roaring, Vellum, and S2—behind internal adapters. mmap may
remain as an implementation/lifetime dependency, but is not part of the serialized format. Bluge, ICE, and
`bluge_segment_api` should disappear from runtime and offline paths.

During the rolling rollback window the native writer must continue to emit:

- segment type `ice`, version 3, in existing `*.seg` grammar; and
- Bluge snapshot version 3 in `*.snp`.

The native reader should accept every historically supported BanyanDB directory identified by the fixture audit. File
bytes need not be identical when several encodings are valid; the required contract is bidirectional cross-reading and
identical live documents/query results.

The largest risk is not query execution. It is independently producing compatible ICE v3 and snapshot bytes, including
recovery, merge, external transfer, and historical compressor behavior.

## 2. Evidence and scale

The active modules are replacements rather than the declared upstream releases:

| Import path | Resolved implementation |
| --- | --- |
| `github.com/blugelabs/bluge` | `github.com/SkyAPM/bluge@42385daf66b8` |
| `github.com/blugelabs/ice` | `github.com/SkyAPM/ice@b5173603b0b3` |
| `github.com/blugelabs/bluge_segment_api` | `github.com/zinclabs/bluge_segment_api v1.0.0` |

The declarations and replacements are in `go.mod:17`, `go.mod:157-158`, and `go.mod:210-212`.

Measured production Go source, excluding tests:

| Area | Lines |
| --- | ---: |
| Resolved Bluge module | approximately 34,982 |
| Resolved ICE module | approximately 6,079 |
| BanyanDB `pkg/index/inverted` plus analyzer adapter | approximately 2,417 |
| Existing `pkg/index/inverted` tests | approximately 1,872 |

Sixteen production BanyanDB files directly import a Bluge package. Coupling therefore extends beyond
`pkg/index/inverted` into Property repair/merge, schema reads, migration/rebuild/verify, logical filters, analyzers, and
numeric encoding. Replacing only `inverted.NewStore` would leave the dependency and its storage types embedded in the
system.

## 3. Required BanyanDB behavior

### 3.1 Required online interfaces

The primary boundary is `pkg/index/index.go:792-887`.

| Operation | Required use |
| --- | --- |
| `Batch` | Stream element index writes and element-index rebuild. |
| `InsertSeriesBatch` | Per-segment series metadata insertion. |
| `UpdateSeriesBatch` | Property, series/index-mode updates, repair, and migration. |
| `EnableExternalSegments` | Raw segment reception for Stream, Measure, and Trace. |
| `BuildQuery`, `Search`, `SeriesSort` | Series filtering/sorting and Property queries. |
| `Match`, `MatchField`, `MatchTerms`, `Range` | Stream element filters returning document and timestamp postings. |
| `Iterator`, `Sort` | Sorted element/index scans. |
| `SeriesIterator`, `StoredFields` | Dump and administrative reconstruction. |
| `TakeFileSnapshot`, `Stats`, `Reset`, metrics | Backup/snapshot and lifecycle monitoring. |
| Read-only count | Closed/cold segment statistics and migration verification. |

`Delete` has no production caller in the current tree, although deletion semantics remain necessary internally for
updates and manifest tombstones.

### 3.2 Write semantics that must be specified

#### Element batches

`pkg/index/inverted/inverted.go:172-216` maps `Document.DocID` to `_id`, indexes byte or numeric fields, stores/sorts
according to field flags, adds `_series_id` from the first field, and stores a positive `_timestamp`.

Its current adapter does not forward `Batch.PersistentCallback`. This may be intentional because element writes do not
wait for persistence, or it may be an omission. The native API must decide explicitly.

#### Series insertion is not ordinary put-if-absent

`InsertSeriesBatch` passes the document identity and complete field-name set to the fork's `InsertIfAbsent` path
(`pkg/index/inverted/inverted_series.go:42-72`). An existing live ID suppresses the new document only when the existing
segment contains every requested field. If a requested field is absent, the new document replaces/evolves the series
document.

The replacement therefore needs a clearly named **field-set-aware ensure/upsert** behavior. A simple `map[id]exists`
check would break series schema evolution.

#### Update, deletion, and durability

- `UpdateSeriesBatch` creates a replacement document and a deletion for the prior `_id`.
- A delete-only batch adds deletion masks without a tombstone document.
- A batch should become atomically visible in one new in-memory snapshot; this needs a contract test rather than an
  assumption inherited from Bluge.
- Property may block on `PersistentCallback` (`banyand/property/db/shard.go:237-269`). For APIs that the contract decides
  support callbacks, it should fire exactly once after the generation is durably published, or once with a terminal
  failure during close/shutdown. Element `Batch` callback support remains an explicit decision.
- `BatchWaitSec` currently allows visibility before delayed persistence. Native code should expose this as a durability
  policy rather than an incidental persister sleep.

### 3.3 Required query subset

The needed query IR is small:

- `All` and `None`;
- exact `Term` and an explicit live-document/universe query;
- byte, numeric, and timestamp ranges with independent inclusive endpoints;
- prefix and wildcard identity matching;
- analyzed `Match` with query-analyzer override and AND/OR token behavior;
- boolean AND, OR/min-should-one, and NOT; and
- an internal series/time constraint.

Current criteria map EQ, NE, GT/GE/LT/LE, IN/NOT_IN, HAVING/NOT_HAVING, MATCH, and nested AND/OR in
`pkg/index/inverted/query.go`. Series identity matchers and time conjunction live in `inverted_series.go`.

Despite its name, current `MatchField` delegates to an empty range and constrains only `_series_id` plus optional time; it
does not require that the requested field exist. Stream NOT uses it as the universe. Native code should model the universe
directly and separately decide whether a true field-existence query is needed.

Queries use `AllMatches` or explicit sort; relevance scores are not consumed. Term frequency, norms, and token locations
may be omitted by the native writer only after old-reader fixtures prove that a valid minimal ICE representation works.
The native reader must still tolerate these sections in existing segments.

### 3.4 Analyzer and numeric compatibility

Required analyzers are keyword, simple, standard, and URL (`pkg/index/analyzer/analyzer.go`). MATCH can override the
index analyzer and choose AND or OR. Compatibility includes Unicode normalization/casing and exact token boundaries, not
only analyzer names.

Integer fields currently pass through Bluge's int64-to-float transform and numeric prefix coding. Timestamps use its
date/numeric representation. These encodings occur in persisted term dictionaries, so a native `termcodec` must reproduce
them from an independent specification and golden terms. Negative integers, extrema, negative zero, infinities/NaNs where
accepted, and inclusive range endpoints need boundary fixtures.

The current exact numeric `MatchTerms` path formats a decimal string although writes use numeric terms. That inconsistency
is a likely defect and must be characterized rather than made a new contract.

### 3.5 Stored fields, doc values, and ordering

Required storage modes are independent:

- indexed-only;
- stored-only;
- indexed and stored;
- sortable/doc-value; and
- repeated values.

Property relies on stored JSON source, delete time, SHA, identity, and timestamp. Index-mode Measure migration reconstructs
stored fields and regenerates non-stored `_im_name` and `_im_entity_tag_*` fields. Dump and repair need raw visitation.

`Search` currently collapses repeated stored values into one value, while `StoredFields` returns all values and hides
`_id`, `_series_id`, `_timestamp`, and `_version`. This distinction should be documented in the new interfaces.

Sorting is implemented by timestamp or index-rule field. Although measure planning can request `OrderByTypeSeries`, the
current `SeriesSort` rejects it; identity sorting is therefore a decision/gap, not established behavior. The new collector
must define deterministic tie breaks, missing values, repeated values, and cancellation. Offset-based repeated Top-N
searches in the existing adapter are not a good native pagination contract; a pinned snapshot plus search-after key is
safer.

### 3.6 Product-specific requirements

- **Series index:** one live document per serialized entity identity; field-set evolution; identity dictionary scans;
  projection, timestamp/version, and explicit sort.
- **Stream element index:** document identity plus mandatory series constraint; exact/range/MATCH/existence filters; two
  output postings containing document IDs and timestamps; explicit sorting; external segment reception.
- **Property:** replacement writes, durable callbacks, stored source, merge-time expiration, snapshot generation lookup,
  stable sorted repair scan, and repair hash fields.
- **Index-mode Measure:** full stored projections, time/version semantics, index rules/analyzers/sort values, and migration
  reconstruction.
- **Offline/admin:** read-only open/count, latest generation, query and match-all scan, stored/doc-value visitation,
  search-after, verify, rebuild, snapshot, dump, repair, and migration.

## 4. Direct coupling that must be removed

The native admin/offline API must replace these bypasses:

- `banyand/metadata/schema/reader/reader.go`: raw Bluge reader, extracted `BlugeQuery`, bounded query/stored-field scan;
- `banyand/internal/migration/unionsidx.go`: raw multi-index scan, rebuild, and dedup;
- `banyand/measure/migration_indexmode_copy.go`: raw match-all scan and reconstruction;
- `banyand/{measure,stream}/migration_verify.go`: raw count;
- `banyand/property/db/repair.go`: snapshot-generation listing and sorted/search-after scan;
- `banyand/property/db/shard.go`: `bluge_segment_api.Segment` merge callback;
- `pkg/index/analyzer/analyzer.go` and `pkg/query/logical/tag_filter.go`: Bluge analysis types; and
- `pkg/index/index.go`, Stream tag filtering, and internal series filtering: Bluge numeric transforms.

The correct replacement is a small BanyanDB admin interface—`OpenReadOnly`, `Count`, `LatestGeneration`,
`WalkDocuments`, `Verify`, `Rebuild`, and stored-field visitation—not a local clone of Bluge's public API.

## 5. Behaviors that should not be copied blindly

Static research identified several likely defects or underspecified behaviors:

1. `Batch.PersistentCallback` is ignored while series callbacks are honored.
2. Writes against a closing store can return `nil`, risking silent loss.
3. `SeriesIterator` closes its reader before returning a dictionary iterator.
4. Some sort iterators have nil closer ownership and can panic on `Close`.
5. Reader-acquisition failures can leak store closer counts.
6. Pure negative queries depend on implicit Bluge match-all behavior, especially for missing fields.
7. `NOT_HAVING` builds the correct executable subquery but attaches its own trace node instead of the child node.
8. Property query construction can index `Groups[0]` for an empty group list.
9. Exact numeric matching may not use the writer's numeric encoding.
10. Timestamps `<=0` are omitted without an explicit public restriction.
11. Missing/multivalued sort semantics and pagination stability are not defined.
12. `MatchField` does not test field presence; it currently acts as the series/time universe for stream NOT.
13. Measure planning can request identity ordering that the current series sorter rejects.
14. The external dedup rebuild path reconstructs simplified fields and can lose timestamp, doc-value, analyzer, and
    location fidelity; cross-query tests must determine the observable impact.

The conformance suite should first record current behavior, then maintainers should label each case **required**,
**intentionally fixed**, or **unsupported**. Compatibility means preserving BanyanDB's intended data semantics, not every
adapter bug.

## 6. ICE v3 and snapshot source observations

Sections 6.1–6.6 describe the grammar observed in the pinned resolved source. They are strong implementation hypotheses,
not yet the independent format specification required by research phases R3/R4. Byte-annotated fixtures and cross-reader
tests must verify every offset, optional section, compressor behavior, and allowable writer omission before production
codec work treats them as settled facts.

### 6.1 ICE v3 envelope

ICE defines type `ice` and version 3. Fixed-width values are big-endian; variable integers are unsigned LEB128. There is
no leading magic value.

```text
compressed stored-field chunks
stored chunk-offset table and trailer
per-document stored index
per-field postings/details + Vellum dictionary + optional doc values
doc-value location index
field records
fixed-width field-record offset index
60-byte footer
```

The footer, in file order, is:

```text
numDocs             uint64
storedIndexOffset   uint64
fieldsIndexOffset   uint64
docValueOffset      uint64
chunkMode           uint32
docTimeMin          uint64
docTimeMax          uint64
version             uint32 (=3)
crc32               uint32
```

`docTimeMin/Max` preserve signed `int64` bit patterns through a `uint64` cast.

### 6.2 Stored fields

Stored fields use 128-document compressed chunks. Each uncompressed document is:

```text
uvarint(metaLen) | uvarint(dataLen) | metadata | concatenated values
```

Metadata repeats `(fieldID, valueOffset, valueLength)` as uvarints, allowing repeated stored values. A fixed-width
per-document index points into the document's uncompressed chunk. Chunk end offsets and their byte length/count are
stored before the document index.

### 6.3 Dictionaries and postings

Field `_id` is ordered first; other fields are lexical. Terms are lexical byte strings. Each field contains:

- compressed frequency/norm and location streams when present;
- a posting record containing detail offsets and a serialized optimized Roaring bitmap;
- a serialized Vellum FST mapping term bytes to a posting offset or inline one-hit value; and
- optional doc values.

The one-hit representation stores a 31-bit norm and 31-bit document number in the FST value and is valid only for one
frequency-1 posting without locations. A native writer can initially disable this optimization if the general form is
valid and old-reader compatible; fixtures must prove that choice.

### 6.4 Doc values

Doc values use 1024-document chunks. A chunk contains delta-coded document/end-offset metadata plus compressed encoded
terms. Multiple terms use `0xff` separators with backslash escaping. Per-field start/end locations lead readers to the
field data.

### 6.5 Compression and checksums

The resolved ICE fork uses a package-global compressor and currently defaults to S2. The ICE footer does **not** record
the compressor. This is the highest historical compatibility risk: an old BanyanDB index written with Snappy, Zstd, or a
different fork can look like ICE v3 until a chunk is decompressed.

ICE writes a CRC over every byte before the terminal CRC field, including the preceding footer fields, but `ice.Load`
only parses/stores it and does not validate it. The native reader should validate it under a defined eager or first-access
policy. Historical corpus collection must precede an S2-only claim.

Vellum's serialized FST and Roaring portable serialization are part of the effective disk contract. Their dependency
versions must be pinned and cross-tested before upgrades.

### 6.6 Bluge snapshots

Files are `%012x.snp`; segments are `%012x.seg`. Readers accept generic hexadecimal IDs and sort them by numeric value.

```text
uvarint(snapshotVersion)
uvarint(segmentCount)
segment records
crc32BE
```

Each record contains type string, segment version, ID, version-dependent metadata, and a length-prefixed Roaring deletion
bitmap. Snapshot versions differ as follows:

| Version | Per-segment fixed metadata after ID |
| --- | --- |
| 1 | none |
| 2 | minimum and maximum timestamp |
| 3 | size, document count, minimum and maximum timestamp |

The current writer emits snapshot v3. The native writer should also emit snapshot v3 during the compatibility window;
the reader should support snapshot v1-v3 only where historical fixtures confirm their use. This does not imply support
for ICE segment versions 1-3: the resolved ICE footer parser accepts segment version 3 only, so the historical segment
version floor requires a separate audit.

Deletion state lives in `.snp`, not in `.seg`. A raw segment transfer alone is not a complete recoverable index snapshot.

## 7. Lifecycle and compatibility findings

### 7.1 Existing persistence is weaker than expected

The resolved filesystem directory writes directly to the final `.seg` or `.snp` pathname, fsyncs the file, and closes it.
It does not use temp+rename, and the persister does not call directory fsync. It also opens with `O_CREATE|O_RDWR` without
`O_TRUNC`, relying on monotonically increasing IDs; a retry or ID reuse can retain stale trailing bytes.

Startup skips malformed snapshots and leaves the newest loadable generation as root. A snapshot that references a
missing/invalid segment is rejected. If snapshots exist but none load, writable open fails.

The native implementation should improve publication without changing final names or grammar:

1. write a unique same-filesystem temp segment;
2. fsync, close, and validate it;
3. rename to final `.seg` and fsync the directory;
4. write/fsync/rename the complete `.snp` and fsync the directory;
5. only then complete durable callbacks; and
6. GC unreachable files after snapshot refs drain.

An old Bluge reader only sees accepted final names, so this stronger protocol should remain format-compatible, but it
must be tested at every crash cut point.

### 7.2 Snapshot MVCC and GC

Readers need immutable segment references and immutable deletion masks pinned by a snapshot ref. Property's merge callback
already clones a published deletion bitmap before adding expiry drops (`banyand/property/db/shard.go:468-509`); the native
API should make that ownership rule impossible to violate.

The current deletion policy unlinks a segment when no retained manifest references it; it does not separately wait for
active snapshot/mmap refs. Unix readers can continue through open mappings after unlink, while platform locking may make
removal fail and retry. Native GC must preserve reader safety explicitly and handle platform removal failures.

### 7.3 External raw segments are mandatory

Migration paths send raw `*.seg` bytes into `ExternalSegmentStreamer`. The receiver currently:

- writes an OS temp file;
- copies it to a final local `.seg`;
- loads it through the writer's configured segment plugin, which is ICE v3 in the current default configuration;
- optionally deduplicates by `_id`; and
- introduces it into a later `.snp`.

The stream itself carries no self-describing type/version envelope, so acceptance currently depends on the configured
plugin. The new receiver should validate length, hash, expected type/version, checksum, offsets, FST, and postings before
introduction; publish through the same single-writer snapshot path; and make retry/idempotency explicit. Dedup must
preserve all surviving stored/index/doc-value/analyzer/timestamp semantics and must serialize against concurrent identity
updates.

### 7.4 Rolling compatibility policy

Required directions:

| Producer/action | Consumer |
| --- | --- |
| Legacy Bluge/ICE directory | native open/query |
| Legacy directory | native append/update/delete/merge/restart |
| Native ICE v3 segment | legacy Bluge open/query/merge/restart |
| Native snapshot v3 | legacy Bluge open with identical live docs/deletes |
| Legacy/native raw segment | legacy/native external receiver |

A new segment type/version is not safe while rollback to old binaries is required. A future BanyanDB-native disk version
can be considered only after the legacy fallback is retired and an explicit migration/version negotiation is designed.

### 7.5 Lock, read-only open, and backup

Preserve `bluge.pid` as the exclusive writer lock during the compatibility window. Read-only count/admin opens must not
acquire it. `TakeFileSnapshot` must pin a committed snapshot, copy or hard-link exactly its referenced `.seg` and `.snp`
files, and exclude `bluge.pid`, external staging, failed parts, and transient files. Relevant integration points include
`pkg/index/inverted/inverted.go`, `banyand/internal/storage/segment.go`, and `banyand/internal/migration/fsutil.go`.

The native publication/backup protocol should sync the destination parent directory. Compatibility tests must cover file
modes, lock semantics, old-reader merge/restart after native temp+rename publication, and every supported filesystem and
operating system—not only Go architecture.

## 8. Recommended architecture

```text
pkg/index public contracts
    |
BanyanDB query IR + analyzers + numeric/date term codec
    |
native store / query executor / sort collector
    |
immutable snapshot + live masks
    |
immutable memory/file segment interface
    |
bounded ICE v3 codec
    |
single writer + persister + merge/GC + external receiver + directory
```

Suggested package boundaries:

```text
pkg/index/queryir
pkg/index/analyzer
pkg/index/termcodec
pkg/index/native/store
pkg/index/native/snapshot
pkg/index/native/execute
pkg/index/native/segment
pkg/index/native/persist
pkg/index/native/directory
pkg/index/native/external
pkg/index/native/admin
pkg/index/native/icev3
```

No ICE, Vellum, or Roaring types should escape the internal segment/codec boundary.

### 8.1 Chosen design: direct immutable segment builder

For each admitted batch:

1. normalize identity and fields;
2. independently analyze terms;
3. build sorted per-field terms, postings, stored fields, and doc values in bounded staging;
4. seal an immutable byte-backed segment;
5. publish it plus any replacement deletion masks in a new in-memory snapshot;
6. asynchronously persist compatible ICE bytes and snapshot v3; and
7. later stream-merge live content into a new segment.

This uses one queryable representation and avoids a permanent mutable-index implementation. It is estimated at roughly
13–17 KLoC of production Go plus extensive tests, fixtures, fuzzers, and tools.

### 8.2 Alternative: mutable document arena plus compiler

A mutable arena retains canonical documents until persistence compiles them into ICE. It simplifies early write/retry
logic but creates two query/storage representations, higher memory, replay cost, and approximately 15–20 KLoC. It is not
recommended unless the direct writer prototype proves too risky.

### 8.3 Concurrency

- One mutation/admission owner per directory serializes ID evolution, deletion overlays, external introduction, and merge
  publication.
- Readers atomically acquire a ref-counted immutable snapshot and never observe bitmap mutation.
- The persister may batch work but publishes generations in order.
- Mmap and decompression/FST/doc-value caches are bounded and pinned by segment refs.
- `Reset` clears only evictable caches.

### 8.4 Property merge policy

Replace `bluge_segment_api.Segment` with a native `SegmentView` exposing bounded count and stored-field visitation. A
`MergeDropFilter` adds expiry drops into a private drop set. Merge then streams only live documents and publishes a new
snapshot before ref-safe source GC.

## 9. Dependency decision

Provisional recommendation:

| Dependency | Decision |
| --- | --- |
| Bluge | Remove after fallback window. |
| ICE module | Remove after compatible codec lands. |
| `bluge_segment_api` | Remove; replace with private segment/admin interfaces. |
| Roaring | Retain and pin; bytes are embedded in `.seg`/`.snp`. |
| Vellum | Retain behind codec adapter unless an independent compatible FST is justified. |
| S2 | Retain for verified current ICE v3 compatibility. |
| Snappy/Zstd | Reader support only if historical corpus proves they occurred. |
| mmap helper | Retain or replace with a small owned abstraction. |

Retaining small maintained binary codecs is consistent with a BanyanDB-native engine. The goal is to own indexing,
queries, lifecycle, and format specification—not to reimplement every embedded compression or bitmap algorithm.

## 10. Compatibility and validation program

### 10.1 Golden corpus

Every fixture manifest should record producer commit, Go/OS/architecture, compressor, options, SHA-256, and expected
logical documents/results.

Required boundaries include:

- 0/1/127/128/129 stored documents;
- 1023/1024/1025 doc-value documents;
- varint width boundaries;
- every field mode and repeated values;
- arbitrary bytes, zero bytes, `0xff`, backslashes, empty/long terms, and Unicode;
- one-hit and general postings;
- sparse/dense/run Roaring representations;
- positive/zero/negative timestamps and numeric extrema;
- every analyzer and query operator;
- pre/post merge with sparse/all deletions; and
- snapshot v1/v2/v3, valid/corrupt latest generations, missing and orphan segments.

### 10.2 Differential and cross-version tests

- Run a backend-neutral state-machine suite against the existing and native engines.
- Compare IDs, projections, repeated values, sort order, limits, errors, and cancellation.
- Open native-writer output with the pinned legacy reader and request dictionary, stored, doc-value, query, merge, and
  restart operations.
- Open historical/legacy output with native code, then append/update/delete/merge and reopen with both implementations.
- Cross-test external raw segment receive with dedup enabled and disabled.
- Run mmap and non-mmap readers and each supported OS/architecture.

### 10.3 Fuzzing and corruption

Structure-aware fuzz every count, varint, offset, FST value, Roaring payload, compressed chunk, doc-value escape, footer,
manifest record, version, length, and CRC. Enforce limits for file size, field/term count, name/value length, bitmap size,
decompressed chunk size, compression ratio, wildcard expansion, and result collection. Malformed data must error without
panic, hang, or unbounded allocation.

### 10.4 Crash injection

Inject failures after every segment/temp write, sync, close, rename, directory sync, manifest operation, callback,
external receive/dedup/introduction step, and GC deletion. Include retry/ID-reuse cases that could expose stale trailing
bytes. Reopen and require a complete, self-consistent committed manifest—an old or new manifest may legitimately mix
retained old segments with new replacement segments, but it must never reference a partial or missing segment.

### 10.5 Benchmarks

Use production-shaped workloads for high-cardinality series lookup, high-rate Stream writes, Property upsert/order/expiry,
index-mode time/range/projection, every analyzer, concurrent snapshots/merge, external transfer/dedup, and cold startup.

Record throughput, p50/p95/p99 latency, allocations, resident/mapped/cache bytes, goroutines, disk size, segment count,
write amplification, merge time, startup/recovery time, and callback durability latency. Establish budgets from measured
current workloads rather than choosing arbitrary parity thresholds.

## 11. Four-ticket implementation sequence and rollback

Implementation is organized by live BanyanDB role, not by engine layer. A parser, writer, query IR, shadow executor,
fuzzer, or benchmark may be built on a ticket branch, but it cannot merge as a standalone delivery. Each ticket must
replace its named Bluge production path in the same main-branch merge.

### NIDX-01 — Property shard index

- **Production cutover:** `property/db.newShard` selects native for existing and new Property shards; Property repair uses
  the native reader.
- **Complete boundary:** ICE/snapshot read and compatible write; Property replace/upsert/delete; stored fields;
  exact/boolean/range query and sort; durable callback; recovery; merge-time expiry; GC; repair; and backup.
- **Deferred:** Series wildcard/dictionary behavior, Stream element postings/MATCH, external receive, and general
  migration.

### NIDX-02 — Per-segment series `sidx`

- **Production cutover:** `internal/storage.newSeriesIndex` selects native for Measure, Stream, and Trace, including raw
  segment replication.
- **Complete boundary:** Field-set-aware insert, update/version/timestamp, exact/prefix/wildcard identity, dictionary
  iteration, projection, index/time/series sort, index-mode Measure, snapshot/stats, and external receive/deduplication.
- **Deferred:** Stream element `idx`, general schema/union/copy/verify/dump/rebuild, and dependency removal.

### NIDX-03 — Stream element `idx`

- **Production cutover:** `stream.newElementIndex` selects the native writer, filters, posting collector, sort, snapshot,
  and receiver.
- **Complete boundary:** Element batch and visibility; keyword/simple/standard/URL analysis; numeric/date terms;
  exact/range/AND/OR/NOT/HAVING/IN/prefix/wildcard/MATCH; paired document/timestamp postings; explicit sort; and raw
  segment receive.
- **Deferred:** Scoring, phrase/fuzzy/geo/highlight/facet/explanation APIs, generic plugins, offline migration, and
  dependency deletion.

### NIDX-04 — Administration, migration, and removal

- **Production cutover:** Schema loading, union/rebuild, index-mode copy, migration verification, dump, and read-only
  count use native administration APIs.
- **Complete boundary:** Read-only open/count, latest generation, document walk, stored/doc-value visit, sorted
  search-after, verify/rebuild, every remaining caller rewire, and runtime Bluge/ICE/segment-API removal.
- **Deferred:** New disk version, opening-time migration, unused search features, and a runtime legacy fallback after
  retirement.

NIDX-01 → NIDX-02 → NIDX-03 → NIDX-04 is the merge order. Each ticket is mergeable only when its native constructor is
the default for the named role, every new production component is reachable from that constructor, the full role
contract passes, and the named rollback binary opens, queries, mutates, merges, restarts, recovers, and—where relevant—
receives native files on the same directory. Pre-merge shadow comparison is evidence, not a ticket.

Before NIDX-04 retires the fallback, rollback means draining the role's writer and selecting the retained legacy
constructor against the same files; no document replay or conversion is allowed. NIDX-04 may remove the runtime fallback
only after the declared compatibility window and two-binary matrix pass. Because CRC32 processing is prohibited, any
writing ticket is blocked unless the named rollback binary accepts the retained-but-uncomputed CRC32 field.

## 12. Effort and ownership estimate

The preliminary estimate is 25–35 engineer-weeks, or approximately 4–6 calendar months for a small parallel team:

| Workstream | Estimate |
| --- | --- |
| Format specification, fixtures, parser, fuzzing | 6–10 weeks, two engineers |
| Query IR, analyzers, term codec, old bridge | 4–6 weeks, one or two engineers |
| ICE/snapshot writer and crash-safe directory | 8–12 weeks, two engineers after format gate |
| Executor, snapshots, sorting, mmap/cache, metrics | 6–8 weeks, two engineers |
| Merge/GC, callbacks, Property expiration | 4–6 weeks, one or two engineers |
| External receiver, admin, migration replacement | 4–6 weeks, one or two engineers |
| Benchmarks, failure injection, soak, rollout | 6–10 weeks, parallel/ongoing |

The ranges are planning estimates, not commitments. The codec compatibility prototype should revise them before the
first production cutover is scheduled. These are workstreams inside NIDX-01 through NIDX-04, not additional mergeable
tickets.

## 13. Decisions and unresolved experiments

Maintainer decisions required before implementation:

1. Is rollback to old binaries on the same files required for the whole rollout? This report assumes yes.
2. Which current adapter defects should become intentional fixes?
3. What are the exact tie/missing/repeated sort semantics?
4. Should timestamps at or below Unix epoch be supported?
5. Should callbacks be accepted uniformly on element and series batches?
6. Which historical BanyanDB releases/directories define the compatibility floor?

Experiments that still gate production work:

- collect historical/production index directories and identify compressor/FST/Roaring/analyzer/numeric versions;
- generate and independently annotate single-feature ICE/snapshot fixtures;
- finish byte-level frequency/norm/location and adaptive chunk specifications;
- prove native-reader/legacy-writer and native-writer/legacy-reader compatibility;
- test Vellum serialization across resolved historical versions;
- test stronger temp+rename+directory-sync publication with legacy readers and supported filesystems;
- determine whether external dedup fidelity loss is observable today; and
- run the fuzz, crash, and performance programs above.

## 14. Final recommendation

Proceed through four production vertical tickets. Do not merge a backend-neutral boundary, parser, shadow reader, or test
writer by itself. Build those foundations on the NIDX-01 branch and merge them only when Property shards use the complete
native read/write/lifecycle path by default. Then extend the already-live engine to per-segment series indexes, the Stream
element index, and finally the remaining administration/migration callers and dependency removal.

Keep legacy output compatibility until the declared rollback window ends. This order makes every merge useful to
BanyanDB, prevents an unused second engine from accumulating on main, and still produces a small BanyanDB-owned engine
rather than a renamed copy of Bluge.
