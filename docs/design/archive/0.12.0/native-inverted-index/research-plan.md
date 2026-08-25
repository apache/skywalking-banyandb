# BanyanDB-Native Inverted Index — Research Plan

## Status

Draft for review. This document plans the research needed to replace Bluge and ICE with a small BanyanDB-owned
implementation. It does not select the final design and does not authorize production implementation.

## 1. Objective

Determine how BanyanDB can directly implement the inverted index used for:

- the segment-level series index shared by the time-series engines;
- the Stream element index;
- Property storage and search;
- index-mode Measure storage and search; and
- migration, repair, dump, snapshot, and external-segment workflows.

The result should be the smallest maintainable engine that satisfies BanyanDB's actual behavior, rather than a general
full-text search library. Existing BanyanDB index directories must remain usable. In particular, compatibility includes
both ICE v3 `*.seg` files and Bluge `*.snp` manifests; preserving only the public Go API is insufficient.

The research will end with an implementation proposal, an independently written file-format specification, executable
compatibility fixtures, a dependency decision, benchmarks, and a staged rollout plan.

## 2. Constraints and definitions

### 2.1 Required constraints

1. **No mechanical port or source copy.** Existing Bluge/ICE code may be studied to establish behavior, but the new
   design must be expressed in BanyanDB concepts and implemented from an independent specification and black-box tests.
   Every borrowed algorithm or retained dependency must have recorded provenance and a compatible license.
2. **On-disk interoperability.** The new reader must open existing directories. During any supported rolling downgrade,
   the existing Bluge/ICE reader must also open files written by the new writer. Byte-for-byte deterministic output is
   not required when several valid encodings exist; cross-reading and identical query results are required.
3. **Crash safety is part of the format.** File naming, temporary files, atomic rename, directory sync, lock behavior,
   manifest publication, garbage collection, and persisted callbacks are compatibility requirements, not incidental
   implementation details.
4. **Behavioral compatibility.** Insert, insert-if-absent, update, delete, analysis, range bounds, sorting, stored-field
   projection, snapshots, merge-time deletion, and external-segment introduction must retain their current semantics.
5. **Minimal scope.** Relevance scoring, highlights, facets, aggregations, arbitrary user fields, and a general public
   search API are out of scope unless production call-site evidence shows they are required.
6. **No silent migration.** Research and prototypes must use test directories. A production directory is never rewritten
   merely by opening it.

### 2.2 Meaning of file compatibility

Compatibility will be reported separately for each direction:

| Producer | Consumer | Required result |
| --- | --- | --- |
| Existing Bluge/ICE | native read-only codec | Must read all supported fixtures. |
| Existing Bluge/ICE | native writer | Must reopen, append/update, merge, and restart safely. |
| Native writer | existing Bluge/ICE | Must reopen and return identical live documents during the compatibility window. |
| Native writer before crash | native recovery | Must select the last committed snapshot and safely collect orphans. |

The research must identify the oldest BanyanDB-created segment and snapshot versions that are actually supported. ICE
v3 and Bluge snapshot versions 1–3 are the initial audit set, not an assumption that all combinations occur in practice.

## 3. Current baseline to freeze

Research must pin the exact code and never use floating upstream branches as the oracle:

- BanyanDB commit under test;
- `github.com/SkyAPM/bluge` commit `42385daf66b8`;
- `github.com/SkyAPM/ice` commit `b5173603b0b3`;
- `github.com/zinclabs/bluge_segment_api v1.0.0`;
- the resolved versions of Roaring, Vellum, S2, and analyzer-related dependencies; and
- Go version, operating system, and architecture used to generate fixtures.

Record these in a machine-readable fixture manifest. The current dependency declarations and replacements are in
`go.mod:17`, `go.mod:157-158`, and `go.mod:210-212`.

The known disk baseline is:

- numeric hexadecimal IDs with `.seg` and `.snp` suffixes;
- an exclusive `bluge.pid` writer lock;
- immutable ICE type `ice`, version 3 segments;
- a 60-byte ICE v3 footer with no leading magic value;
- Bluge manifests containing segment type/version/ID, size/count/time bounds, and a Roaring deletion bitmap; and
- S2-compressed chunks in the currently resolved ICE fork.

Each item above must be verified by fixtures before it becomes a statement in the final specification. The ICE footer
does not identify the compression algorithm, so compatibility with indexes produced by older BanyanDB releases is an
explicit research question.

## 4. Research questions

### 4.1 Product and API surface

- Which methods in `index.Store`, `index.SeriesStore`, and `index.Searcher` have production callers?
- Which behaviors differ among series, element, Property, and index-mode Measure indexes?
- Which direct `github.com/blugelabs/...` imports bypass `pkg/index/inverted`, and how should they be removed?
- Are scoring, token frequency, norms, and token locations used, or can the writer omit some of them while remaining a
  valid ICE v3 producer?
- Which stored fields and doc values are mandatory for projection, ordering, repair, and migration?
- What are the exact ordering and limit semantics, including ties, absent values, duplicate values, and cancellation?

### 4.2 Write and lifecycle semantics

- When is `Batch` visible, and when does `PersistentCallback` fire?
- What atomicity is promised for multi-document batches?
- What does insert-if-absent mean when the ID exists in an older segment, lacks one of the requested fields, or is
  deleted in the current snapshot? The current fork's series insertion includes field-set evolution and must not be
  simplified to a conventional map `PutIfAbsent` without evidence.
- How are update and delete represented before and after merge?
- How do unsafe batches and `BatchWaitSec` change durability?
- What are the merge-policy requirements versus tunable policy choices?
- How does Property's `PrepareMergeCallback` add expiration tombstones without racing readers?
- What invariants are required by external segment streaming and deduplication?

### 4.3 Query semantics

- Exact term, field existence, byte and numeric ranges, date ranges, prefix, wildcard, and boolean AND/OR/NOT.
- MATCH behavior for keyword, simple, standard, URL, analyzer override, and AND/OR operator modes.
- Numeric prefix-coded term compatibility and lexicographic boundary behavior.
- Series exact/prefix/wildcard matchers and dictionary iteration.
- Stored-field projection, repeated values, internal-field hiding, doc values, ascending/descending sort, and pagination.
- Series/time restriction and the paired document-ID/timestamp posting lists returned to TSDB filters.

### 4.4 File format and recovery

- Complete ICE v3 grammar, including every offset base, integer width, byte order, sentinel, escaping rule, chunk table,
  one-hit posting encoding, timestamp convention, CRC coverage, and malformed-input behavior.
- Complete Bluge snapshot v1/v2/v3 grammar and which version the native writer must emit.
- Valid variations in field order, term order, Roaring serialization, FST construction, compression, and chunk mode.
- mmap lifetime, reference counting, cache ownership, maximum allocation, and bounds checking.
- Snapshot selection after truncated files, checksum failure, missing segments, orphan segments, and interrupted rename.
- Rules for snapshot and segment garbage collection while readers hold old snapshots.

### 4.5 Dependency boundary

- Can maintained low-level libraries such as Roaring, Vellum, and S2 be retained without inheriting a search engine?
- Is Vellum's serialized FST format stable enough to be treated as part of the compatibility boundary?
- Should analyzers be implemented as small BanyanDB tokenizers, or sourced from a maintained library?
- Which Bluge numeric encodings are used in persistent terms and therefore require an independently specified codec?
- Can `bluge_segment_api` be eliminated completely, including the Property merge callback?

## 5. Research method and work packages

The work is intentionally spec-first. A phase may change later implementation estimates, but it must not change a
previously measured compatibility result without updating the fixture and rationale.

These R0–R7 work packages are research activities, not main-branch implementation tickets. Prototypes remain isolated
research artifacts. The only production implementation merges are the four live cutovers defined by R7; no research
package authorizes dormant production code on main.

### R0 — Freeze the oracle and define provenance

**Purpose.** Make the research reproducible and prevent an accidental source translation.

**Work.**

1. Record the versions listed in section 3 and archive their licenses.
2. Write a short provenance policy distinguishing facts obtained from public format documentation, independent byte
   inspection, black-box behavior, existing BanyanDB expectations, and retained third-party algorithms.
3. Define two roles where practical: format researchers produce the specification and fixtures; implementers work from
   those artifacts rather than translating functions from the old modules.
4. Create a fixture manifest containing generator version, seed, options, SHA-256, and expected logical documents.

**Deliverables.** `research-baseline.md`, `provenance.md`, and a fixture-manifest schema.

**Exit gate.** Every future fixture can be traced to an exact oracle and regenerated.

### R1 — Inventory the required BanyanDB contract

**Purpose.** Reduce the replacement to what BanyanDB uses.

**Work.**

1. Trace every production method of the interfaces in `pkg/index/index.go:792-887`.
2. Build a call-site matrix for:
   - `pkg/index/inverted`;
   - `banyand/internal/storage/index.go`;
   - `banyand/stream/index.go`;
   - `banyand/property/db/shard.go`;
   - index-mode Measure queries and migration; and
   - dump, repair, schema-reader, and migration packages.
3. Inventory direct imports of Bluge analysis, numeric, query, search, index, and segment APIs. Direct production imports
   currently exist outside the adapter, so merely rewriting `inverted.NewStore` will not remove the dependency.
4. Inventory the admin/offline surface needed to replace those imports: read-only open/count, latest generation, raw and
   sorted/search-after scans, stored-field reconstruction, rebuild, and merge-time stored-field visitation.
5. Trace `TakeFileSnapshot`, cold read-only count, hard-link/copy behavior, lock-file exclusion, and externally visible
   index metrics in addition to the online query path.
6. Convert existing tests into a behavior table. Mark each row **required**, **compatibility-only**, **replaceable**, or
   **unused**.
7. Measure representative cardinality, field count, terms/document, stored bytes/document, update rate, delete rate,
   segment count, and query shape from existing benchmarks or synthetic BanyanDB workloads.

**Deliverable.** `native-index-requirements.md`, containing a method/consumer/semantics/performance matrix and an explicit
negative-scope list.

**Exit gate.** Every proposed engine feature maps to a production caller or a disk-compatibility requirement.

### R2 — Build a black-box behavioral oracle

**Purpose.** Capture semantics without coupling the new tests to Bluge types.

**Work.**

1. Define a backend-neutral contract harness using only `pkg/index` values.
2. Run the harness against the existing implementation and serialize canonical results.
3. Cover empty indexes, repeated fields, binary values including zero bytes, Unicode analysis, numeric extremes, time
   boundary inclusivity, wildcard escaping, missing sort values, duplicate IDs, update/delete sequences, restart after
   every step, and context cancellation.
4. Add state-machine/property tests that generate batches and compare the engine with a simple in-memory reference model.
5. Specify callback timing, visibility, snapshot isolation, close/reset behavior, and error handling.
6. Separate desired BanyanDB semantics from accidental Bluge behavior. Any intentional difference requires a design
   decision and cannot enter the compatibility suite silently.

**Deliverable.** A backend-neutral conformance suite and `behavior-contract.md`.

**Exit gate.** The existing backend passes the suite, and ambiguous behaviors are listed as decisions rather than hidden
assumptions.

### R3 — Specify ICE and snapshot formats independently

**Purpose.** Turn the disk format into a reviewable BanyanDB specification.

**Work.**

1. Generate minimal single-feature fixtures: zero/one/many documents, each field mode, repeated values, postings with and
   without frequency/location data, one-hit postings, doc values, stored-field chunk boundaries, multiple fields,
   timestamps, deletions, and merged segments.
2. Generate boundary fixtures around document chunks 127/128/129, doc-value chunks 1023/1024/1025, adaptive posting
   chunks, varint width changes, empty and very long terms, and large offsets.
3. Annotate every byte range using an independent inspection tool. Validate offset arithmetic by navigating only from the
   footer and manifest.
4. Document:
   - stored-field chunks and their per-document index;
   - field dictionaries and Vellum FST outputs;
   - Roaring postings, frequency/norm, and location streams;
   - doc-value escaping, chunks, and indexes;
   - field records and field index;
   - the footer, CRC, timestamps, and version checks;
   - snapshot versions, deletion bitmaps, and file naming; and
   - persistence/rename/fsync/lock/recovery protocol.
5. Corrupt one element at a time to define required failures and allocation limits.
6. Collect directories produced by supported historical BanyanDB releases. Determine whether their compressor, segment
   version, snapshot version, numeric encoding, and analyzer output differ from the current fork.

**Deliverables.** `ice-v3-format.md`, `index-snapshot-format.md`, a read-only inspection command, and versioned golden
fixtures under test data.

**Exit gate.** A reviewer can implement a bounded parser from the specifications without consulting Bluge or ICE source.

### R4 — Prove bidirectional codec compatibility

**Purpose.** De-risk the format before building an engine around it.

**Work.**

1. Implement a throwaway, read-only prototype parser from the R3 specification. It must expose fields, dictionaries,
   postings, stored values, doc values, timestamps, and deletion masks, but no query planner.
2. Differentially compare its decoded logical representation with the existing reader for every fixture.
3. Fuzz all entry points with allocation and recursion limits; require errors rather than panics for malformed data.
4. Implement a throwaway writer only after the reader passes. Cross-open its output with the existing Bluge/ICE reader.
5. Test append/update/delete/merge/restart in both directions, not just isolated segment decoding.
6. Verify CRC handling. The current ICE loader records the footer CRC but does not by itself validate it; the proposal
   must decide whether the native reader always validates segments, validates on first mmap, or preserves current policy.
7. Test old-reader/new-writer interoperability on all architectures supported by BanyanDB.

**Deliverable.** `codec-compatibility-report.md` with a producer/consumer matrix, fixture hashes, failures, and performance
numbers. Prototype code is not automatically production code.

**Exit gate.** All mandatory cells in section 2.2 pass, or the report identifies a migration/versioning change requiring
explicit approval.

### R5 — Evaluate a minimal native engine architecture

**Purpose.** Select internal structures only after the required semantics and codec are known.

The starting hypothesis to validate is:

```text
pkg/index API
    |
native query IR + analyzers + numeric term codec
    |
snapshot reader ---------------- mutable batch/analyzer
    |                                      |
immutable segment readers          in-memory segment
    |                                      |
ICE v3 codec <---------------- persister / streaming merger
    |
directory + manifest + GC + external segment receiver
```

Candidate package boundaries, subject to the research result:

- a backend-neutral query IR replacing `queryNode`'s embedded `bluge.Query`;
- small analyzer and numeric-codec packages owned by BanyanDB;
- an `icev3` codec isolated from query and lifecycle code;
- immutable segment readers with dictionary/posting/stored/doc-value APIs;
- a snapshot that combines segment readers and deletion masks;
- a single-writer batch introducer and persistence loop;
- a streaming merger that accepts per-segment drop sets; and
- a filesystem directory responsible for locks, atomic publication, snapshots, and GC.

**Experiments.**

1. Compare two write paths: build an ICE segment directly from analyzed documents versus build a simpler mutable memory
   segment and encode only during persistence.
2. Compare eager bitmap unions with iterator-based conjunction/disjunction for BanyanDB query shapes.
3. Determine whether term frequencies, norms, and locations can be omitted while still producing valid compatible files.
4. Measure Vellum, Roaring, and S2 as retained low-level dependencies versus replacement cost. Preference is to retain
   small maintained codecs whose serialized forms are already embedded in ICE rather than reimplement them.
5. Prototype snapshot reference ownership, mmap close, cache eviction, concurrent reads, one writer, persistence batching,
   and merge cancellation under the race detector.
6. Exercise Property's merge-time expiry and external-segment reception as first-class flows; they must not be deferred
   until after the ordinary insert/search prototype.

**Improvements to assess without changing the format.**

- strict offset validation, bounded allocation, and complete checksum verification;
- bounded decompression/FST/doc-value caches with observable hit rate and memory use;
- streaming segment construction and merge to reduce peak memory;
- a BanyanDB-specific query planner without scores or unused collectors;
- explicit batch durability states and clearer callback errors;
- deterministic merge scheduling and backpressure;
- manifest recovery diagnostics and an offline verify/repair command; and
- fewer goroutines and simpler ownership than the general-purpose library.

**Deliverable.** An architecture decision record comparing at least two viable designs by code size, dependency count,
compatibility risk, memory, write amplification, query latency, and operational complexity.

**Exit gate.** The selected design covers every R1 requirement and has no dependency on Bluge, ICE, or
`bluge_segment_api` at its public or internal boundaries.

### R6 — Benchmark and failure research

**Purpose.** Avoid choosing a minimal design that is functionally correct but operationally unusable.

**Workloads.**

- series lookup with high entity cardinality;
- Stream element filtering with high write rate;
- Property upsert/delete/search/order-by and merge-time expiration;
- index-mode Measure projection and time-range search;
- MATCH queries for each analyzer;
- snapshot-heavy read concurrency while batches persist and segments merge;
- external segment transfer and deduplication; and
- startup/recovery with many segments and manifests.

Measure throughput, p50/p95/p99 latency, allocations, resident and mmap bytes, goroutine count, segment count, write
amplification, merge duration, startup time, and disk size. Use the current implementation as a baseline, not as an
absolute performance requirement. Establish budgets from BanyanDB workloads before accepting regressions.

Inject failures at write, fsync, rename, manifest publication, old-file deletion, external-stream completion, and merge
cancellation boundaries. Reopen after each failure and verify that either the old or new snapshot is complete.

**Deliverable.** `native-index-benchmark-and-recovery-report.md`.

**Exit gate.** The proposal includes agreed regression budgets and demonstrates bounded memory and valid recovery.

### R7 — Produce the four-ticket implementation and rollout proposal

**Purpose.** Convert research evidence into reviewable engineering work.

The implementation proposal must contain exactly four production vertical tickets. A codec, query IR, test writer,
shadow executor, fuzz harness, or benchmark may be work inside a ticket, but cannot be merged as a standalone ticket.
Every ticket must route a named live BanyanDB constructor or administrative caller to native code in the same merge.

#### NIDX-01 — Property shard index

- **Live replacement:** Property upsert, query, sort, delete/expiry, repair, and durable publication.
- **Inside:** Only the ICE/snapshot, query, writer, merge, recovery, and administration pieces exercised by Property.
- **Outside:** Series matching, Stream element postings, external receive, and general migration.

#### NIDX-02 — Per-segment series `sidx`

- **Live replacement:** Measure, Stream, and Trace series insert, update, lookup, sort, snapshot, and raw segment
  replication.
- **Inside:** Field-set-aware insert, identity dictionaries/matchers, projection/sort, index-mode Measure, and external
  receive.
- **Outside:** The Stream element `idx` and general offline migration commands.

#### NIDX-03 — Stream element `idx`

- **Live replacement:** Stream element batch, filter/posting execution, sort, snapshot, and raw segment replication.
- **Inside:** Only the required analyzer, numeric/date term, MATCH/filter, and paired document/timestamp posting surface.
- **Outside:** Scoring and every other unused search-product feature.

#### NIDX-04 — Administration, migration, and removal

- **Live replacement:** Schema reader, union/rebuild, index-mode copy, migration verification, dump, and read-only count.
- **Inside:** Rewire every remaining production/CLI caller, then remove runtime Bluge, ICE, and segment-API dependencies.
- **Outside:** The pinned legacy oracle, which remains only as an isolated compatibility test input.

Each ticket must be a reviewable main-branch merge unit with all of the following:

1. a production activation point that selects native by default for the named role;
2. an explicit in-scope and out-of-scope API/format boundary;
3. a call-graph check proving every new production component is reachable from that activation point;
4. backend-neutral role tests, relevant fuzz/crash/benchmark gates, and legacy-directory coverage;
5. native-reader/legacy-writer and native-writer/named-rollback-binary interoperability; and
6. a same-file rollback procedure until NIDX-04 passes the approved dependency-retirement gate.

Shadow comparisons and test-only writers remain pre-merge evidence, not deliverables. Because CRC32 processing is
prohibited, each writing ticket is blocked unless the named rollback binary accepts the retained-but-uncomputed CRC32
field. The implementation must not silently restore checksum calculation to make the gate pass.

**Deliverable.** A ticketed implementation plan with dependencies, test gates, observability, upgrade/downgrade policy,
and removal criteria.

**Exit gate.** Maintainers approve the requirements, format specifications, architecture decision, performance budgets,
and rollout policy independently.

## 6. Required conformance matrix

The research suite must cover at least the following rows across create, persist, reopen, and post-merge states:

| Area | Cases |
| --- | --- |
| Writes | insert, field-evolving insert-if-absent, update, delete, empty/multi-doc batch, persistence callback |
| Fields | indexed-only, stored-only, stored+indexed, sortable, repeated, missing, binary, numeric, timestamp/version |
| Analysis | keyword, simple, standard, URL, Unicode, analyzer override, MATCH AND/OR |
| Queries | exact, exists, range bounds, date range, prefix, wildcard, AND, OR, NOT, HAVING, IN |
| Results | stored projection, internal-field hiding, repeated values, sort asc/desc, missing sort, limit, cancellation |
| Series | exact/prefix/wildcard ID, dictionary iteration, series sort, paired doc/timestamp postings |
| Lifecycle | reopen, concurrent snapshots, tombstones, merge, reset cache, stats, read-only count, file snapshot |
| Special | Property expiry callback, external stream, dedup, read-only count, dump, repair, migration |
| Corruption | truncated footer/chunk/FST/bitmap, bad offset/version/CRC, missing segment, orphan segment |

## 7. Proposed research artifacts

Names may change during review, but ownership should remain clear:

```text
docs/design/archive/0.12.0/native-inverted-index/
  research-baseline.md
  provenance.md
  native-index-requirements.md
  behavior-contract.md
  ice-v3-format.md
  index-snapshot-format.md
  codec-compatibility-report.md
  architecture-decision.md
  native-index-benchmark-and-recovery-report.md
  implementation-plan.md

pkg/index/indextest/
  backend-neutral conformance harness

testdata/native-index/
  manifest.json
  generated ICE/snapshot directories and logical expectations
```

Large fixtures should live in an appropriate artifact store or be generated deterministically in CI rather than
inflating the repository.

## 8. Review gates and stop conditions

Research stops for maintainer review when any of these occurs:

- historical data uses an unidentified compressor or unsupported ICE version;
- valid native output cannot be opened by the old reader without copying substantial legacy implementation;
- a required production behavior depends on scoring, locations, or another unexpectedly broad Bluge subsystem;
- crash-safe compatibility requires changing the manifest format;
- retained low-level dependencies are themselves unmaintained or have unstable serialized formats;
- performance requires an on-disk change; or
- rolling downgrade cannot be supported safely.

At that point the alternatives are: extend the compatibility scope, use a read-old/write-new migration with an explicit
format version, vendor a narrowly audited compatibility codec, or revise the rolling-downgrade requirement. None should
be chosen implicitly.

## 9. Research completion criteria

Research is complete only when all of the following are available for review:

1. The required/unused feature matrix is backed by production call sites.
2. The existing implementation passes a backend-neutral behavioral contract.
3. ICE v3 and snapshot formats are independently specified with boundary and corruption fixtures.
4. Bidirectional old/new codec compatibility is demonstrated, including restart and merge.
5. Historical BanyanDB index compatibility is measured rather than assumed.
6. At least two native architectures are compared and one is recommended.
7. The recommended design has quantified performance, memory, recovery, and dependency trade-offs.
8. A staged implementation plan preserves rollback until the final dependency-removal milestone.

Only after these gates pass should production implementation begin.
