# Document Index: a first-class document model

Status: **design** — not implemented. Targets 0.13.0 unless it can be cut into 0.12.0.
Tracking issue: [apache/skywalking#13466](https://github.com/apache/skywalking/issues/13466).

## 1. Motivation

Today a measure has two completely different storage engines behind one schema and one pair of
RPCs. The switch is `Measure.index_mode` (`api/proto/banyandb/database/v1/schema.proto`, field 7):

- `index_mode = false` writes columnar parts (`banyand/measure/part.go`) and keeps a lightweight
  metadata document per series in the segment series index.
- `index_mode = true` writes no part at all. The whole data point is an upserted document in the
  segment series index (`sidx/`, native ICE v3).

The second mode is already a document store: a document is identified by its entity, every tag is
searchable, the newest version wins, and TTL/segments do the retention. But it is expressed as a
measure, so it carries concepts that do not apply and forces every layer to branch:

| Layer | What has to branch today |
| --- | --- |
| Schema | `Measure` carries `fields`, `interval`, `sharding_key`, `index_mode`, and TopN can be derived from it. None of these means anything for a document. |
| Write path | `processDataPoint` in `banyand/measure/write_standalone.go` splits into `handleIndexMode` / `appendDataPoints`; `dataPointsInTable` keeps two separate doc sets. |
| Read path | `banyand/measure/query.go` short-circuits to `buildIndexQueryResult`; the vectorized planner carries `UseIndexModeMap` and index-mode-only limit/criteria rules across `pkg/query/vectorized/measure/plan`. |
| Index layout | `pkg/index/index.go` invents `_im_name` and `_im_entity_tag_<tag>` fields only to make an index-mode measure's subject and entity tags queryable. |
| Migration / export | `banyand/measure/migration_indexmode_copy.go` and `banyand/measure/export_indexmode.go` are dedicated index-mode implementations next to the columnar ones. |
| OAP | `BanyanDBModelExtension.isIndexMode()` is checked in the installer, schema registry, metrics DAO, TTL query, and several query paths. |

`docs/concept/storage-and-format.md` already calls this out: *"one schema, one write RPC, one
query RPC — but a single boolean flag silently swaps the entire on-disk engine."* The flag is also
effectively immutable: flipping it orphans all previously written data.

A document store that is a first-class model removes the fork. Measures keep one engine and one set
of concepts; documents get an API that describes what they actually are.

## 2. Goals and non-goals

### Goals

1. A new catalog, **`DocumentIndex`**, whose resources are upserted documents keyed by a declared
   entity. It has no fields, no interval, no downsampling, no TopN, and no aggregation.
2. Reuse the existing native ICE v3 segment index (`sidx/`) and segment/TTL/stage machinery. This
   is a modelling and API separation, not a new storage engine.
3. Every document tag is stored and, unless declared otherwise, searchable by exact match. Entity
   tags are stored and indexed exactly once (no `_im_entity_tag_*` duplicates).
4. Query by criteria, projection, order, limit/offset and optional time range. This covers the
   metadata queries SkyWalking OAP performs today.
5. A migration path for existing `index_mode = true` measures and for SkyWalking OAP metadata
   models, with a fallback to the old mode while the cluster has not migrated.
6. Keep `Measure.index_mode` working for at least one release after `DocumentIndex` ships, so
   users are never forced to migrate data and code at the same time.

### Non-goals

- Replacing `Stream` (immutable logs/events) or `Trace`. Those models have different identity,
  dedup and sampling semantics.
- A general search engine. `DocumentIndex` gets the analyzers, term/range filters and sort
  ladders the native index already supports; no BM25 scoring, facets, highlights or joins.
- Cross-model queries or joins.
- Changing the SkyWalking query protocol or UI. The OAP GraphQL API continues to serve the same
  metadata fields.
- Removing `Measure.index_mode` in the same release. Removal is a separate breaking change after
  the migration tool has shipped and been used.

## 3. Current index-mode contract (what must be preserved)

The new model must not lose any observable behaviour that index-mode measures rely on today.
The contract is:

- **Identity**: a document is identified by its subject (schema name) plus the serialized entity
  values. The doc ID is the series ID (`pbv1.Series{Subject, EntityValues}.Marshal()` hashed).
- **Upsert**: writing the same identity again replaces the document. The read path returns the
  newest version; a document in a newer segment shadows the same identity in an older segment.
- **Tags**: every tag is stored. Searchable tags carry an inverted index; array tags store one
  value per element; numeric and timestamp tags keep their native encodings.
- **Time**: a document has a timestamp used for segment placement, time-range pruning and time
  ordering. It is not a time-series bucketing key.
- **Retention**: group TTL, segment intervals, hot/warm/cold stages and shard count behave exactly
  as they do for a measure or stream group.
- **Query**: filter by criteria, project tags, order by time or a sortable tag, limit/offset, and
  optionally restrict to a time range. There is no aggregation, group-by or TopN.

## 4. Proposed model

### 4.1 Catalog and schema

Add a catalog value (wire-additive; existing values do not move):

```proto
enum Catalog {
  CATALOG_UNSPECIFIED = 0;
  CATALOG_STREAM = 1;
  CATALOG_MEASURE = 2;
  CATALOG_PROPERTY = 3;
  CATALOG_TRACE = 4;
  CATALOG_DOCUMENT_INDEX = 5;
}
```

Add the schema message in `api/proto/banyandb/database/v1/schema.proto`:

```proto
// DocumentIndex stores upserted documents keyed by their entity.
// Unlike Measure, it has no fields, interval or TopN, and unlike Stream,
// a document with the same entity is replaced rather than appended.
message DocumentIndex {
  common.v1.Metadata metadata = 1;
  // tag_families are the document fields. Every tag is stored.
  repeated TagFamilySpec tag_families = 2;
  // entity is the document key. At least one tag is required.
  Entity entity = 3;
  // Per-tag index options. A tag with no entry is stored, exact-match
  // indexed with the keyword analyzer, and not sortable.
  repeated DocumentTagOptions tag_options = 4;
  google.protobuf.Timestamp updated_at = 5;
  google.protobuf.Timestamp created_at = 6;
}

message DocumentTagOptions {
  // name refers to a tag in tag_families.
  string name = 1;
  // storage_only disables the inverted index for this tag.
  bool storage_only = 2;
  // sortable enables the numeric/tag sort ladder for ORDER BY on this tag.
  bool sortable = 3;
  // analyzer is one of the native index analyzers (keyword, simple,
  // standard, url). Empty means keyword.
  string analyzer = 4;
}
```

Why options live in `DocumentIndex` instead of reusing `IndexRule`/`IndexRuleBinding`:

- A document model should be self-describing. A user who creates a `DocumentIndex` should not
  also have to create an out-of-band rule and binding before anything is filterable.
- Index-mode measures need `IndexRule` only because non-entity tags must be explicitly bound. A
  `DocumentIndex` can write every searchable tag as a tag-name-keyed field, which the native index
  already supports (`FieldKey{TagName: name, Analyzer: analyzer}` in
  `banyand/measure/write_standalone.go`).
- Migration maps the source `IndexRule` analyzer and `no_sort` bit into `DocumentTagOptions`, so
  no behaviour is lost.
- If composite indexes are needed later, they can be added as a separate optional field without
  making the basic model depend on them.

Open question: whether to keep the default "all tags indexed" or to require explicit options.
The recommendation above is explicit defaults with a small options list, because it mirrors the
current index-mode behaviour (OAP currently indexes every non-`storageOnly` column) and keeps the
common case short.

### 4.2 Groups

A group has exactly one catalog. `DocumentIndex` groups use the same resource options as measures
and streams: `shard_num`, `segment_interval`, `ttl`, `replicas`, hot/warm/cold stages and node
selectors. No new group-level option is needed.

Multiple `DocumentIndex` schemas may share a group, just as multiple measures share a group. They
share the group's shards and segments and are separated inside the segment index by a subject
field. This keeps shard/TTL counts low and makes migration a group-to-group copy.

A group's catalog is immutable in practice. Introducing `CATALOG_DOCUMENT_INDEX` therefore means a
new group for existing deployments; an old measure group is not re-catalogued in place.

### 4.3 Write API

Add `api/proto/banyandb/document/v1/write.proto` (package name to be settled, see open questions):

```proto
message Document {
  // timestamp places the document in a segment. Optional on a normal write:
  // the server uses the receive time. Import and migration set it explicitly.
  google.protobuf.Timestamp timestamp = 1;
  // tag_families match the schema's tag families by name and order.
  repeated model.v1.TagFamilyForWrite tag_families = 2;
  // version breaks ties for the same identity and timestamp. Optional.
  int64 version = 3;
}

message WriteRequest {
  // metadata is required on the first request of the stream.
  common.v1.Metadata metadata = 1;
  Document document = 2;
  uint64 message_id = 3;
}

message WriteResponse {
  uint64 message_id = 1;
  string status = 2;
  common.v1.Metadata metadata = 3;
  model.v1.Status status_code = 4;
  uint32 retry_after_ms = 5;
}

service DocumentService {
  rpc Write(stream WriteRequest) returns (stream WriteResponse);
}
```

Semantics:

- Entity values are read from the document's tags in the schema entity order. A missing entity tag
  is a row error, not a silent null key.
- The doc ID is the series ID derived from `(subject, entity values)`. The writer uses
  `IndexDB().Update`, preserving the existing upsert and shadowing behaviour.
- `timestamp` defaults to receive time. This is important for updates: an updated document must
  land in the hot segment and inherit the group's current TTL, not the original write time. Import
  and migration may set it to preserve retention.
- `version` defaults to 0. A higher version wins for the same doc ID and timestamp; across
  segments, the newer segment wins as it does today.
- Tags not declared by the schema are rejected. Duplicate tag names are rejected. A document with
  no entity value is rejected.
- The existing per-batch ack, drop counters and rate limiting apply. Document writes are not fed
  through the measure buffer/TopN pipeline.

### 4.4 Query API

Add `api/proto/banyandb/document/v1/query.proto`:

```proto
message Document {
  google.protobuf.Timestamp timestamp = 1;
  repeated model.v1.TagFamily tag_families = 2;
  uint64 sid = 3;
  int64 version = 4;
}

message QueryRequest {
  common.v1.Metadata metadata = 1;
  // time_range is optional. When absent, every live segment is searched.
  model.v1.TimeRange time_range = 2;
  model.v1.Criteria criteria = 3;
  // tag_projection is required; an empty projection returns all stored tags.
  repeated model.v1.TagFamily tag_projection = 4;
  model.v1.QueryOrder order_by = 5;
  uint32 limit = 6;
  uint32 offset = 7;
}

message QueryResponse {
  repeated Document documents = 1;
  common.v1.Trace trace = 2;
  map<string, model.v1.Status> group_statuses = 50;
}

service DocumentService {
  rpc Query(QueryRequest) returns (QueryResponse);
}
```

Semantics:

- Criteria uses the existing `modelv1.Criteria` tree and a resolver that maps tag names directly
  to tag-name-keyed index fields.
- Projection returns tags in the requested families. Entity tags are projected from their stored
  field; there is no separate entity blob to unpack.
- `order_by` supports time and sortable tags. A non-sortable tag order falls back to the same
  bounded scan the native index uses for non-sort fields, or returns an error if that is too
  expensive; the design recommends allowing it only over a bounded time range and limit.
- `limit`/`offset` are pushed down where the order is time or a sortable tag, reusing the existing
  native limit pushdown that index-mode measures already get.
- No aggregation, group-by, TopN or field projection. A client that needs counts can use a
  separate `count_only` flag later (open question).
- Distributed queries route by group and shard, then merge documents. Merge dedupes by doc ID:
  for the same ID, the row with the greater `(timestamp, version)` wins, and a row in a newer
  segment shadows an older one.

### 4.5 Storage layout

Reuse the per-segment index directory (`seg-*/sidx/`) and the native ICE v3 encoder. A document's
fields are:

| Field | Stored | Indexed | Notes |
| --- | --- | --- | --- |
| `_subject` | yes | yes | Scopes a schema inside a shared group index. Replaces `_im_name`. |
| `_timestamp` | yes | yes | Segment placement, time range and time order. |
| `_version` | yes | no | Tie-breaker. |
| `_id` | doc values | no | Existing native document identity. |
| each tag | yes | per `DocumentTagOptions` | Keyed by tag name, with analyzer and numeric marker as needed. |

Differences from today's index-mode measure layout:

- No `_im_name`; the generic subject field is used.
- No `_im_entity_tag_*` duplicates. Every tag is stored once and entity tags are indexed like any
  other searchable tag. This removes a whole class of duplicate-field bugs.
- No fields, so no field encodings, compression, or field projection.
- The same segment/TTL/stage lifecycle. An update lands in the hot segment; the old document
  remains in its older segment until that segment expires or is merged, and the read path shadows
  it.

This keeps existing native index format compatibility: the bytes are still ICE v3 segments with
the same encodings. The only new thing is a new field-name convention for documents in a new
group.

### 4.6 Query execution

The index-mode result builder is already almost a document executor. The design proposes:

1. Generalize `banyand/internal/storage.IndexSearchOpts.IndexModeSubject` to `Subject`, and
   `pkg/index.IndexModeName` to `SubjectField`, with no behaviour change.
2. Extract `buildIndexQueryResult` (`banyand/measure/query.go`) into a reusable
   `banyand/internal/document` executor. It already handles projection, criteria, time order,
   limit pushdown, shadowing and per-segment search.
3. Use that executor from the new `DocumentService`, and (as a follow-up) from measure's
   index-mode path until the mode is removed.

The measure vectorized planner does not need to learn about `DocumentIndex`; document queries have
their own small plan and do not go through measure aggregation/group-by planning. This removes
`UseIndexModeMap` from the measure planner once index mode is gone.

### 4.7 Schema registry and lifecycle

`DocumentIndex` needs the same registry surface as the other schemas:

- `DocumentIndexRegistryService` (`Create`, `Update`, `Delete`, `Get`, `List`, `Exist`) in
  `api/proto/banyandb/database/v1/rpc.proto`, plus HTTP gateway handlers in
  `banyand/liaison/http/server.go`.
- Schema cache/revision tracking in `pkg/schema/init.go`, schema snapshots, node schema status,
  group drop handling and the schema-server role.
- `banyand/liaison/grpc/registry.go`, `methodpolicy.go` and RBAC extractors need the new
  resource. The permission should mirror `MeasureRegistryService` (schema admin for writes,
  data read/write for data).
- A `banyand/document` service package registered in standalone, data and liaison setups
  (`pkg/cmdsetup/standalone.go`, `pkg/cmdsetup/data.go` and the liaison command), with its own
  `--document-*` flags for root path, flush timeout and memory protector, mirroring measure.
- Export/import and `bydbctl analyze` support in a later phase; the first release can reject
  `DocumentIndex` with an explicit "not supported yet" error rather than silently dropping data.

## 5. SkyWalking OAP integration

The 16 OAP models currently annotated with `@BanyanDB.IndexMode` are metadata documents:
`ServiceTraffic`, `InstanceTraffic`, `EndpointTraffic`, `ProcessTraffic`, `ServiceLabelRecord`,
`TagAutocompleteData`, `NetworkAddressAlias`, the two hierarchy relation models, the OTLP traffic
models, the Zipkin traffic models and `EBPFProfilingScheduleRecord`.

### 5.1 Annotation and model

Add `@BanyanDB.DocumentIndex` and deprecate `@BanyanDB.IndexMode` (keep it working for one
release). In `StorageModels`, set a new flag on `BanyanDBModelExtension`:

```java
if (aClass.isAnnotationPresent(BanyanDB.DocumentIndex.class)) {
    banyanDBModelExtension.setDocumentIndex(true);
}
```

The OAP `Model` keeps its existing `Metrics` shape: these entities are still produced by
`MetricsStreamProcessor`, still use the virtual `id` series ID, and still have no measure fields.
Only the BanyanDB storage kind changes. That avoids touching OAL, the GraphQL schema, or the UI.

### 5.2 Schema registration

`MetadataRegistry` gains `registerDocumentIndexModel`, using the same tag parsing as
`registerMeasureModel` but emitting `DocumentIndex`:

- Group: a new `documents` (name TBD) group with the same TTL/stage options as the existing
  `metadata` group.
- Entity: the virtual `id` tag, exactly as index mode does today.
- Tags: all columns except `time_bucket`; `storageOnly` maps to `DocumentTagOptions.storage_only`,
  `@BanyanDB.EnableSort` maps to `sortable`, `@BanyanDB.MatchQuery` maps to `analyzer`.
- No `IndexRule` and no `IndexRuleBinding` are created for document indexes.

`BanyanDBIndexInstaller` gets a `DocumentIndex` branch. Reconciliation is additive only: tags may
be added; tag types, entity and storage-only/sortable/analyzer options may not change in place.
This matches the existing measure/stream shape rules and avoids silently reinterpreting data.

`BanyanDBTTLStatusQuery` and `BanyanDBConfigDumpExtension` learn the new group.

### 5.3 Write and query DAOs

`BanyanDBMetricsDAO` currently switches on `isIndexMode()` for multi-get, insert and update. The
proposal is:

- Add `DocumentIndexWrite` / `DocumentIndexQuery` to `library-banyandb-client`, generated from the
  new protos.
- Add a `BanyanDBDocumentIndexMetricsDAO` that implements `IMetricsDAO` for document-index
  models. `BanyanDBStorageDAO` chooses it when `model.getBanyanDBModelExtension().isDocumentIndex()`.
- Refactor `BanyanDBMetadataQueryDAO` (and the few other DAOs that query these models) through a
  small internal interface that returns `MeasureQueryResponse`-shaped rows. The existing
  `Conditions`, projections and builders stay unchanged; only the client call underneath changes
  from `MeasureQuery` to `DocumentIndexQuery`.
- Remove the `isIndexMode()` branches from `BanyanDBMetricsDAO` once index mode is no longer used
  by OAP, but keep them while fallback support exists.

The DAO split is deliberately narrow: the metadata DAOs are already expressed in terms of tags,
criteria and projections, so no query semantics change.

### 5.4 Configuration and version gating

`bydb.yml` gains a `documents` group block and a feature flag:

```yaml
  documents:
    shardNum: ${SW_STORAGE_BANYANDB_DOCUMENTS_SHARD_NUM:2}
    segmentInterval: ${SW_STORAGE_BANYANDB_DOCUMENTS_SI_DAYS:15}
    ttl: ${SW_STORAGE_BANYANDB_DOCUMENTS_TTL_DAYS:15}
    replicas: ${SW_STORAGE_BANYANDB_DOCUMENTS_REPLICAS:0}
```

and a global switch (default off until the migration tool is proven):

```yaml
  documentIndexEnabled: ${SW_STORAGE_BANYANDB_DOCUMENT_INDEX_ENABLED:false}
```

The switch is resolved against `compatibleServerApiVersions`. If it is on but the connected
BanyanDB does not advertise the `DocumentIndex` API version, OAP logs a clear error at startup and
falls back to measure index mode (or refuses to start, depending on the final compatibility
policy). This prevents a partially upgraded cluster from writing to a group that no data node can
serve.

## 6. Compatibility and migration

### 6.1 BanyanDB

- Proto changes are additive: one enum value, new messages/services. Old clients ignore them.
- On-disk changes are additive: new documents live in a new group. Existing `sidx` directories are
  untouched, and the ICE v3 format does not change.
- `Measure.index_mode` stays. New index-mode schemas should be rejected (or warned about) once
  `DocumentIndex` ships, but old data remains readable and writable.
- A future release may remove `index_mode` after the deprecation window. That release must document
  that the migration tool is mandatory and that old index-mode data is unreadable without it.

### 6.2 Migration tool

Add `bydbctl document-index migrate` (name TBD), modelled on the existing
`banyand/measure/migration_indexmode_copy.go` and `migration_verify.go`:

1. **Plan**: list all groups with `CATALOG_MEASURE` and index-mode measures; list the target
   `DocumentIndex` schemas and groups; show document counts and estimated bytes. `--dry-run`
   stops here.
2. **Create target**: create the destination group and `DocumentIndex` schemas (or require them
   to exist). The target group uses the same shard/segment/TTL/stage settings as the source.
3. **Copy**: read source documents with the existing native reader (`WalkIndexModeRows`), map
   `_im_name` to the target subject, map `_im_entity_tag_*` and regular tag fields to tag-name
   fields, preserve `_timestamp` and `_version`, and write batches through the new write path.
   The copy is idempotent: a re-run replaces the same doc ID.
4. **Verify**: compare document counts per subject and per entity, and a digest of each document's
   tag values. Reuse the `EntryGroupReport`/`IndexModeValueMismatch` shape from the measure
   migration verify tooling. Fail non-zero on any mismatch.
5. **Checkpoint**: write a small progress file per (source group, target group) so a failed copy
   can resume without re-reading everything. The existing migration tool's lock-file pattern is
   reused.
6. **Cut over**: stop OAP writes, run the copy and verify, flip
   `SW_STORAGE_BANYANDB_DOCUMENT_INDEX_ENABLED`, restart OAP. Because OAP metadata is rebuilt from
   incoming telemetry, the documented alternative is to start empty and let agents repopulate; the
   migration tool is for users who cannot lose the current metadata window.

The tool does not touch the source group. A rollback is therefore a flag flip back to measure
index mode, with the source data still present.

### 6.3 OAP rollout

1. Ship BanyanDB `DocumentIndex` support with OAP still writing index-mode measures by default.
2. Ship the OAP reader for `DocumentIndex` behind the flag; test both paths against the same data.
3. Ship the migration tool and docs.
4. Flip the default after one release of field use.
5. Deprecate `@BanyanDB.IndexMode`; remove it and the measure index-mode code in a later major
   release.

## 7. CLI, UI and documentation

- **bydbctl**: `document-index create/update/get/list/delete` mirroring `measure.go`, a
  `document-index write` and `document-index query` for data (or a generic `data` command), group
  creation with `--catalog CATALOG_DOCUMENT_INDEX`, and export/import support in the phase that
  implements it.
- **Canopy / UI**: show `DocumentIndex` schemas alongside the other catalogs, with tag count,
  entity, TTL and group; no new graph or dashboard concept.
- **Docs**: add a "DocumentIndex" section to `docs/concept/data-model.md`, `storage-and-format.md`
  and `api-reference.md`; add a migration section to `docs/operation/upgrade.md`; update the OAP
  BanyanDB storage docs (`docs/en/setup/backend/storages/banyandb.md`,
  `docs/en/banyandb/ttl.md`, and the `bydb.yml` comments).

## 8. Testing

### BanyanDB unit tests

- Schema validation: entity required, duplicate tags, unknown options, storage-only + sortable
  conflict, analyzer names.
- Write: first insert, upsert by entity, higher version wins, missing entity rejected, array tags,
  all numeric/time/binary tag types.
- Query: exact term, range, NOT, AND/OR, MATCH with each analyzer, projection of entity and
  non-entity tags, order by time and by sortable tag, limit/offset, no time range, bounded time
  range.
- Shadowing: an older segment's document never overrides a newer segment's document; a delete or
  update in the hot segment is visible immediately.
- Storage: reopen, crash/restart, segment rotation, TTL deletion, hot→warm→cold migration.

### Integration tests

- Standalone and distributed: create group/schema, write from one node, query from the liaison and
  from every data node, restart a data node, add a tag via schema update.
- Sharding: the same entity always routes to the same shard; a multi-shard query dedupes correctly.
- Migration: seed an index-mode measure group, migrate to `DocumentIndex`, verify every document
  and digest, re-run the migration (idempotent), verify a rollback to the source still works.
- Mixed-version: an old client/node that does not know `CATALOG_DOCUMENT_INDEX` must fail clearly,
  not panic or silently route to the wrong catalog.
- OAP: run the existing BanyanDB metadata integration tests against a `DocumentIndex` group and
  the fallback measure group, and compare results.

### Performance gates

No regression against index-mode measure for the metadata workloads OAP runs:

- write throughput and peak RSS for N documents per batch;
- point lookup by entity, filtered list, and limited time-ordered query;
- 1M-document group reopen and query after restart;
- migration throughput (documents/second) and verification memory.

The storage engine is the same, so the target is parity, not an improvement. The expected wins are
lower write amplification (no synthetic duplicate entity fields) and a simpler query plan.

## 9. Alternatives considered

**Rename `index_mode` to `document_index` inside `Measure`.** Cheapest, but keeps fields,
interval, TopN and the schema fork. It does not achieve the separation the issue asks for.

**Use `Stream` with upsert.** Stream already writes documents to an inverted index and has index
rules, but its elements are immutable append-only rows with an `element_id`; adding upsert would
change Stream's identity and dedup contract for logs and events. It also still needs an element ID
and timestamp, and the subject/entity layout would remain stream-specific.

**Keep `DocumentIndex` inside the `MEASURE` catalog as a schema variant.** Avoids a new catalog,
but then the group catalog no longer tells data nodes which engine to use, and every measure
handler still has to detect the variant. That is the current coupling with a new name.

**Build a new document engine.** The native ICE v3 index already provides the exact primitives
(upsert, criteria, projections, sort ladders, limit pushdown, snapshots, external segments). A new
engine would add risk without changing the API.

## 10. Rollout phases

| Phase | Deliverable | Depends on |
| --- | --- | --- |
| 0 | Refactor index-mode-only constants and `IndexSearchOpts` to generic subject names; extract the result builder; no behaviour change, parity tests green | — |
| 1 | `Catalog` + `DocumentIndex` schema + registry service + group support + bydbctl schema commands | Phase 0 |
| 2 | Write/query protos and services, storage path, standalone/distributed query, TTL/stages | Phase 1 |
| 3 | OAP annotation, schema registration, client, DAOs, config, version gate | Phase 2 |
| 4 | Migration tool (`plan/copy/verify/checkpoint`), upgrade docs, E2E | Phase 3 |
| 5 | Export/import, Canopy schema view, `bydbctl analyze`, default flag flip, deprecate `Measure.index_mode` | Phase 4 |

Phases 1 and 2 can land as one PR if the reviewer prefers; phase 0 can land independently and is
useful even if the rest slips.

## 11. Open questions

1. **Proto package and catalog name**: `banyandb.document.v1` with
   `CATALOG_DOCUMENT_INDEX`, or `banyandb.documentindex.v1`? The recommendation is the shorter
   `document.v1` package and the explicit `CATALOG_DOCUMENT_INDEX` enum.
2. **Default indexing**: every tag indexed by default (recommended, matches OAP), or only tags with
   an explicit option? If the latter, users need a rule-like mechanism and the model stops being
   self-contained.
3. **Timestamp ownership**: server time by default (recommended) with import/migration able to set
   it, or require the client to send it? Server time makes updates land in the hot segment and
   keeps TTL semantics obvious.
4. **Count queries**: add `count_only` now or defer? The OAP metadata UI mostly needs lists, so it
   can be deferred, but a count avoids transferring documents for pagination.
5. **Composite indexes**: support them in `DocumentTagOptions` now (as a repeated tag-name list) or
   wait for a real use case? The recommendation is to wait; the single-tag index covers the
   metadata workloads.
6. **OAP group name**: `documents`, `metadata_documents`, or reuse `metadata` after a full
   migration? A new name is safer because a group's catalog is effectively immutable; the
   recommendation is `metadata_documents` and a documented migration from `metadata`.
7. **Fallback policy**: if the feature flag is on but the server is old, fall back silently with a
   warning (recommended for a smooth upgrade) or refuse to start (safer for a mixed cluster)? The
   recommendation is to refuse writes to a half-migrated group and fall back to the old group,
   never to mix the two in one query.
8. **Removal window for `Measure.index_mode`**: one major release of read support after the
   migration tool ships, or longer? The recommendation is at least two minor releases and a
   documented breaking-change note.
