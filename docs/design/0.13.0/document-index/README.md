# Document Index: a first-class document model

Status: **design** — not implemented. Targets 0.13.0.
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

A document store that is a first-class **schema kind**, hosted by the module that already owns the
group, removes the fork without adding a catalog, a module or a group. Measures keep one engine and
one set of concepts; documents get an API that describes what they actually are.

## 2. Goals and non-goals

### Goals

1. A new schema resource kind, **`DocumentIndex`**, whose resources are upserted documents keyed by
   a declared set of tags. It has no fields, no interval, no downsampling, no TopN, and no
   aggregation. It is declarable inside an existing measure group and is served by the measure
   module; no new catalog value, module, group, root path or flag set.
2. Reuse the existing native ICE v3 segment index (`sidx/`) and segment/TTL/stage machinery. This
   is a modelling and API separation, not a new storage engine.
3. Every document tag is stored and, unless declared otherwise, searchable by exact match. Entity
   tags are stored and indexed exactly once (no `_im_entity_tag_*` duplicates).
4. Query by criteria, projection, order, limit/offset and optional time range. This covers the
   metadata queries SkyWalking OAP performs today.
5. A migration path for existing `index_mode = true` measures and for SkyWalking OAP metadata
   models. Where the field layout is preserved the migration is a declaration and nothing moves;
   otherwise it is an optional in-place re-index. The old mode remains available as a fallback.
6. Keep `Measure.index_mode` working for at least one release after `DocumentIndex` ships, so
   users are never forced to migrate data and code at the same time.

### Non-goals

- A new `CATALOG_DOCUMENT_INDEX`, a `banyand/document` service package, a `documents` group, a
  second root path or a second flag set. The catalog is the module selector in this codebase, so a
  new value is a new module; §4.1 and §9 explain why that cost is not justified.
- Replacing `Stream` (immutable logs/events) or `Trace`. Those models have different identity,
  dedup and sampling semantics. A stream group is a valid future host for the same resource kind,
  but stream host semantics are out of scope here and the identity/upsert rule stays with the host.
- A general search engine. `DocumentIndex` gets the analyzers, term/range filters and sort
  ladders the native index already supports; no BM25 scoring, facets, highlights or joins.
- Cross-model queries or joins.
- Changing the SkyWalking query protocol or UI. The OAP GraphQL API continues to serve the same
  metadata fields.
- Removing `Measure.index_mode` in the same release. Removal is a separate breaking change after
  the migration path has shipped and been used.

## 3. Current index-mode contract (what the measure host must preserve)

This section is written from the measure host's point of view, because that is the host being
replaced. The new model must not lose any observable behaviour that index-mode measures rely on
today. Another host, such as a stream group, may define a different identity and upsert rule; that
rule belongs to the host, not to `DocumentIndex`.

- **Identity**: for a measure, a document is identified by its subject (schema name) plus the
  serialized entity values. The doc ID is the series ID
  (`pbv1.Series{Subject, EntityValues}.Marshal()` hashed). A stream host would use its element ID
  instead; the schema only declares which tags form the key.
- **Upsert**: writing the same identity again replaces the document. The write path calls the
  series index `Update`; there is no per-document version conflict resolution on the write path.
  The version is stored as an `_version` field (`banyand/internal/storage/index.go:56, 222`), and
  the tie-break happens when segment results are merged on read (`SeriesData.Versions` and
  `VersionSet` in `banyand/internal/storage/storage.go`, consumed in `banyand/measure/query.go`).
  A document in a newer segment shadows the same identity in an older segment because segments are
  searched newest first (`banyand/internal/storage/segment.go`).
- **Tags**: every tag is stored. Searchable tags carry an inverted index; array tags store one
  value per element; numeric and timestamp tags keep their native encodings.
- **Time**: a document has a timestamp used for segment placement, time-range pruning and time
  ordering. It is not a time-series bucketing key.
- **Retention**: group TTL, segment intervals, hot/warm/cold stages and shard count behave exactly
  as they do for a measure or stream group.
- **Query**: filter by criteria, project tags, order by time or a sortable tag, limit/offset, and
  optionally restrict to a time range. There is no aggregation, group-by or TopN.

## 4. Proposed model

### 4.1 A schema resource kind, not a catalog

The catalog in this repository is the module selector, not just a schema label. Kind maps to
catalog in `pkg/schema/init.go:94-101`, catalog maps to a module topic in
`banyand/metadata/schema/collector.go:127-136`, and catalog selects the setup backend in
`pkg/cmdsetup/data.go` and `pkg/cmdsetup/standalone.go`. Adding a catalog value therefore drags in
a new service package, a new root path and flag set, and a case in every catalog switch:

- backup snapshot/restore: `banyand/backup/snapshot/snapshot.go`, `banyand/backup/restore.go`
- transfer export/import: `banyand/transfer/export/planner.go`, `pkg/transfer/selector.go`
- migration classifier: `banyand/cmd/migration/copy.go`
- collect/drop topics: `banyand/metadata/schema/collector.go`

None of that is needed for a new storage shape inside an existing group. The kinds registry is
already module-agnostic: `KindIndexRule` in `banyand/metadata/schema/kind.go` is a rule resource
with no catalog of its own. `DocumentIndex` follows the same pattern.

Add the schema message in `api/proto/banyandb/database/v1/schema.proto`:

```proto
// DocumentIndex stores upserted documents keyed by a declared entity.
// Unlike Measure it has no fields, interval or TopN. The host module
// defines the document identity and how a repeated identity is resolved:
// a measure uses the series id and the newest version, a stream would use
// the element id and its own append rule.
message DocumentIndex {
  common.v1.Metadata metadata = 1;
  // tag_families are the document fields. Every tag is stored.
  repeated TagFamilySpec tag_families = 2;
  // entity lists the tags that form the document key. At least one tag
  // is required. The host derives the doc id from these tags: a measure
  // uses the series id, a stream would use the element id.
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
  // storage_only disables the inverted index for this tag. It cannot be
  // combined with sortable: the engine only sorts indexed fields.
  bool storage_only = 2;
  // sortable enables the numeric/tag sort ladder for ORDER BY on this tag.
  // It implies an inverted index.
  bool sortable = 3;
  // analyzer is one of the native index analyzers (keyword, simple,
  // standard, url). Empty means keyword.
  string analyzer = 4;
}
```

**Sortability is coupled to indexing by the engine.** `banyand/internal/storage/index.go:204-205`
derives `store := f.Store || !f.Index` and `Sort: f.Index && !f.NoSort`. A `storage_only` tag can
therefore never be sortable, and a sortable tag must also be indexed. This is a model constraint,
not only a validation test: schema creation must reject `storage_only` together with `sortable`,
and the documentation must say that `sortable` implies an inverted index.

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
current index-mode behaviour and keeps the common case short. For the OAP models, the options
mirror the existing `storageOnly`/`EnableSort`/`MatchQuery` annotations rather than introducing
new defaults (see §11).

### 4.2 Host group placement

A `DocumentIndex` is declared inside an existing group. The first release supports a
`CATALOG_MEASURE` group, because the measure module is the host that already serves index-mode
documents. A stream group can host the resource later; stream write and query semantics are a
separate piece of work. The schema message has no measure-specific field, so a new host needs no
schema shape change, only its own identity and upsert rule.

The group keeps its shard count, segment interval, TTL, replicas, hot/warm/cold stages and node
selectors. The host module keeps its root path, flush policy, memory protector and lifecycle. There
is no new group, no group re-cataloguing and no group-to-group copy.

Multiple `DocumentIndex` schemas may share a group, just as multiple measures share a group. They
share the group's shards and segments and are separated inside the segment index by a subject
field. A measure group can also host columnar measures, legacy index-mode measures and
`DocumentIndex` resources at the same time. That is not a new invariant: a measure group already
mixes columnar measures and index-mode documents today, switched by a schema boolean. The group
catalog does not select the storage engine; the resource kind and its schema do.

This placement is what removes the migration copy in §6: the data is already in the right group,
segments, TTL and stages, so migration is either a declaration or an optional in-place re-index.

### 4.3 Write API

Add `api/proto/banyandb/document/v1/write.proto` (package name to be settled, see open questions):

```proto
message Document {
  // timestamp places the document in a segment. Optional on a normal write:
  // the server uses the receive time. Import and re-index set it explicitly.
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

The proto package gives the messages and RPCs a home. The implementation is registered by the host
module (`banyand/measure` for a measure group) on the existing server. There is no second service
process, root path, flag set or setup registration; §4.7 covers the registry side.

Semantics:

- Entity values are read from the document's tags in the schema entity order. A missing entity tag
  is a row error, not a silent null key.
- The doc ID is derived by the host from `(subject, entity values)`. The measure host uses
  `pbv1.Series{Subject, EntityValues}.Marshal()` (`banyand/measure/write_standalone.go`), the same
  as index mode. A future stream host would use its element ID. The writer calls the host's
  `IndexDB().Update`, preserving the existing upsert and shadowing behaviour.
- `timestamp` defaults to receive time. This is important for updates: an updated document must
  land in the hot segment and inherit the group's current TTL, not the original write time. Import
  and re-index may set it to preserve retention.
- `version` defaults to 0. A higher version wins for the same doc ID and timestamp when results
  are merged on read; the write path does not compare versions. Across segments, the newer segment
  wins as it does today.
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
  field, so the projection has no separate entity blob to unpack. Identity is still derived from
  `(subject, entity values)`, and the series index keeps the serialized `pbv1.Series`; the entity
  does not leave the storage model.
- `order_by` supports time and sortable tags. Time order already exists. A sortable tag needs real
  work: the engine sorts on `_timestamp` or a rule-ID field (`orderFieldName` →
  `FieldKey{IndexRuleID}` in `banyand/internal/storage/index.go`), and a tag-name-keyed field has
  no rule ID. The native engine can sort by field name, so the executor needs a name-keyed sort
  path. This is assigned to phase 2, not left implicit.
- `order_by` on a tag that is not `sortable` returns `InvalidArgument`. There is no bounded-scan
  fallback: its cost is unbounded, the memory protector would turn it into an intermittent error
  anyway, and a predictable rejection is easier for clients to handle. `limit`/`offset` are pushed
  down when the order is time or a sortable tag, reusing the existing native limit pushdown.
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
| `_subject` | yes | yes | Scopes a schema inside a shared group index. Written by new `DocumentIndex` documents. |
| `_im_name` | yes | yes | The legacy subject field. Read alongside `_subject` while old documents remain in the group. |
| `_timestamp` | yes | yes | Segment placement, time range and time order. |
| `_version` | yes | no | Tie-breaker, resolved on read. |
| `_id` | doc values | no | Existing native document identity. |
| each tag | yes | per `DocumentTagOptions` | Keyed by tag name, with analyzer and numeric marker as needed. |

**The legacy subject name must stay readable.** `pkg/index.IndexModeName` is not measure-only:

- property writes and matches use it: `banyand/property/db/shard.go`,
  `banyand/property/db/native_property.go`
- the schema reader detects index-mode measures through it:
  `banyand/metadata/schema/reader/reader.go`

Phase 0 introduces a generic Go name (`SubjectField`) as an alias for the existing literal and
does **not** change the value. Renaming the value from `_im_name` to `_subject` would make every
existing property document unreadable, because the property read path matches on
`index.IndexModeName`. Phase 0's "no behaviour change" holds only with the literal unchanged, and
that is an explicit deliverable, not an implementation detail.

New `DocumentIndex` documents write `_subject`. Legacy index-mode documents keep `_im_name` in the
same `sidx/`. The shared reader scopes a subject with an OR over both field names until the legacy
resources are retired, otherwise legacy documents would silently become invisible in a mixed
group. The executor owns that compatibility rule in one place; it is removed when index mode is
removed.

Differences from today's index-mode measure layout:

- No `_im_entity_tag_*` duplicates. Every tag is stored once and entity tags are indexed like any
  other searchable tag. This removes a whole class of duplicate-field bugs.
- No fields, so no field encodings, compression, or field projection.
- The same segment/TTL/stage lifecycle. An update lands in the hot segment; the old document
  remains in its older segment until that segment expires or is merged, and the read path shadows
  it.

This keeps existing native index format compatibility: the bytes are still ICE v3 segments with
the same encodings. The only addition is the `_subject` field name, which the shared reader
understands next to `_im_name`.

### 4.6 Query execution

The index-mode result builder is already almost a document executor. The design proposes:

1. Generalize `banyand/internal/storage.IndexSearchOpts.IndexModeSubject` to `Subject`, and
   introduce `SubjectField` as an alias for `pkg/index.IndexModeName` with the literal value
   unchanged. No behaviour change in phase 0.
2. Extract `buildIndexQueryResult` (`banyand/measure/query.go`) into a reusable
   `banyand/internal/document` executor. It already handles projection, criteria, time order,
   limit pushdown, shadowing, per-segment search and the dual subject name of §4.5.
3. Use that executor from the host module's document write/query path, and (as a follow-up) from
   measure's index-mode path until the mode is removed. No `banyand/document` service package.

The measure vectorized planner does not need to learn about `DocumentIndex`; document queries have
their own small plan and do not go through measure aggregation/group-by planning. This removes
`UseIndexModeMap` from the measure planner once index mode is gone.

### 4.7 Schema registry and lifecycle

`DocumentIndex` needs the same registry surface as the other schemas, as a new kind:

- `KindDocumentIndex` in `banyand/metadata/schema/kind.go` (`KindSize` and `KindMask`), the checker
  map in `banyand/metadata/schema/checker.go`, the per-catalog init lists in
  `pkg/schema/init.go`, the module registry masks (`banyand/measure/metadata.go`,
  `banyand/stream/metadata.go`) and the property-backed catalog conversion in
  `banyand/metadata/schema/property/converter.go`.
- A loader for the new kind, catalog-filtered the way
  `banyand/metadata/schema/reader/loaders.go` filters bindings.
- `DocumentIndexRegistryService` (`Create`, `Update`, `Delete`, `Get`, `List`, `Exist`) in
  `api/proto/banyandb/database/v1/rpc.proto`, plus HTTP gateway handlers in
  `banyand/liaison/http/server.go`.
- Schema cache/revision tracking in `pkg/schema/init.go`, schema snapshots, node schema status,
  group drop handling and the schema-server role.
- `banyand/liaison/grpc/registry.go`, `methodpolicy.go` and RBAC extractors need the new
  resource. The permission should mirror `MeasureRegistryService` (schema admin for writes,
  data read/write for data).
- The write and query paths live in the host module (`banyand/measure`) or in the shared
  `banyand/internal/document` package. The host module already owns the group's root path, flush
  policy, memory protector and lifecycle, so there is no `banyand/document` service package, no
  `--document-*` flag set and no extra registration in `pkg/cmdsetup/standalone.go`,
  `pkg/cmdsetup/data.go` or the liaison command.
- Export/import and `bydbctl analyze` support in a later phase; the first release can reject
  `DocumentIndex` with an explicit "not supported yet" error rather than silently dropping data.

Because the resource lives inside a group, backup, transfer, group drop and TTL handling operate on
the group as they do today. The new kind only has to be understood by schema collection and by the
group's export/import, not by a new module topic or root path.

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

- Group: the existing `metadata` group. The annotation creates a `DocumentIndex` resource inside
  it; no `documents` or `metadata_documents` group is created. The group keeps its TTL, stages and
  shard settings, so the document resources inherit them for free.
- Entity: the virtual `id` tag, exactly as index mode does today.
- Tags: all columns except `time_bucket`; `storageOnly` maps to `DocumentTagOptions.storage_only`,
  `@BanyanDB.EnableSort` maps to `sortable`, `@BanyanDB.MatchQuery` maps to `analyzer`.
- No `IndexRule` and no `IndexRuleBinding` are created for document indexes.

`BanyanDBIndexInstaller` gets a `DocumentIndex` branch. Reconciliation is additive only: tags may
be added; tag types, entity and storage-only/sortable/analyzer options may not change in place.
This matches the existing measure/stream shape rules and avoids silently reinterpreting data.

`BanyanDBTTLStatusQuery` and `BanyanDBConfigDumpExtension` learn the new resource kind; they do not
learn a new group.

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

`bydb.yml` gains a feature flag only. There is no `documents` group block: the resources are
created in the existing `metadata` group.

```yaml
  documentIndexEnabled: ${SW_STORAGE_BANYANDB_DOCUMENT_INDEX_ENABLED:false}
```

The flag gates "use `DocumentIndex` instead of an index-mode measure", not "which group to write
to". It is resolved against `compatibleServerApiVersions`. If it is on but the connected BanyanDB
does not advertise the `DocumentIndex` API version, OAP logs a clear error at startup and falls
back to measure index mode (or refuses to start, depending on the final compatibility policy).
This prevents a partially upgraded cluster from writing documents that no data node can serve.

## 6. Compatibility and migration

### 6.1 BanyanDB

- Proto changes are additive: new messages and services, a new schema kind. No `Catalog` enum
  value is added, so existing enum numbering and switches do not change.
- On-disk changes are additive and co-located: `DocumentIndex` documents live in the same group
  and the same `sidx/` as the resources already there. The ICE v3 format does not change; the
  `_subject` field name is added next to the legacy `_im_name`, and the shared reader accepts both
  while legacy resources exist.
- `Measure.index_mode` stays. New index-mode schemas should be rejected (or warned about) once
  `DocumentIndex` ships, but old data remains readable and writable.
- A future release may remove `index_mode` after the deprecation window. That release must document
  that the re-index path is mandatory and that old index-mode data is unreadable without it.

### 6.2 Migration path

There is no group-to-group copy. The documents are already in the target group, with the target
shards, segments, TTL and stages, so migration is one of two cases:

1. **Field layout preserved.** If the legacy layout maps one-to-one (the subject is readable as
   `_im_name`, entity and tag fields map to tag-name fields, `_timestamp` and `_version` are kept),
   create the `DocumentIndex` resource and nothing moves. The resource keeps the source subject, so
   legacy documents stay addressable under the same name and no document is rewritten. The reader
   of §4.5 serves both layouts.
2. **In-place re-index, optional.** If a resource needs the new field layout, rewrite its documents
   in place with the new field names. This is an optional tool, not a prerequisite for the default
   path. Extend the existing `banyand/cmd/migration` tooling (`copy`, `verify`, `analyze`, and the
   dedicated index-mode path in `banyand/measure/migration_indexmode_copy.go`) rather than add a
   parallel `bydbctl document-index migrate` command. The existing tool already byte-copies or
   rebuilds `sidx`, regenerates the legacy name/entity fields from `_id`, and restores
   `Analyzer`/`NoSort` from the catalog's index rules.
   - The re-index is idempotent per document and resumable per `(segment, series)`.
   - Verification reuses the existing per-`(segment, series)` digest tooling and the
     `EntryGroupReport`/`IndexModeValueMismatch` shape.
   - A failed run leaves the source layout intact; re-running replaces the same doc IDs.

Cut over: create the `DocumentIndex` resources (or let OAP's installer create them), flip
`SW_STORAGE_BANYANDB_DOCUMENT_INDEX_ENABLED`, restart OAP. Because OAP metadata is rebuilt from
incoming telemetry, the documented alternative is to start empty and let agents repopulate; the
re-index tool is for users who cannot lose the current metadata window.

### 6.3 Rollback

Rollback is to stop creating the `DocumentIndex` resource and keep writing the index-mode measure.
The source data was never moved, so there is no copy to undo and no second group to clean up. Any
documents written while the new kind was enabled remain in the same group and are still readable
through the shared subject handling of §4.5.

### 6.4 OAP rollout

1. Ship BanyanDB `DocumentIndex` support with OAP still writing index-mode measures by default.
2. Ship the OAP reader for `DocumentIndex` behind the flag; test both paths against the same data.
3. Ship the optional re-index tool and docs.
4. Flip the default after one release of field use.
5. Deprecate `@BanyanDB.IndexMode`; remove it and the measure index-mode code in a later major
   release.

## 7. CLI, UI and documentation

- **bydbctl**: `document-index create/update/get/list/delete` mirroring `measure.go`, and a
  `document-index write` and `document-index query` for data (or a generic `data` command). Group
  creation keeps `--catalog CATALOG_MEASURE`; there is no `CATALOG_DOCUMENT_INDEX` to pass and no
  new group to create. Export/import support comes in the phase that implements it and must
  include the new schema kind in the existing group.
- **Canopy / UI**: show `DocumentIndex` resources alongside the other schemas inside a group, with
  tag count, entity, TTL and group; no new graph or dashboard concept.
- **Docs**: add a "DocumentIndex" section to `docs/concept/data-model.md`, `storage-and-format.md`
  and `api-reference.md`; add a migration section to `docs/operation/upgrade.md`; update the OAP
  BanyanDB storage docs (`docs/en/setup/backend/storages/banyandb.md`,
  `docs/en/banyandb/ttl.md`, and the `bydb.yml` comments).

## 8. Testing

### BanyanDB unit tests

- Schema validation: entity required, duplicate tags, unknown options, `storage_only` + `sortable`
  rejected, sortable implies indexed, analyzer names.
- Write: first insert, upsert by entity, higher version wins on read, missing entity rejected,
  array tags, all numeric/time/binary tag types.
- Query: exact term, range, NOT, AND/OR, MATCH with each analyzer, projection of entity and
  non-entity tags, order by time, order by a sortable tag through the name-keyed sort path, order
  by a non-sortable tag rejected with `InvalidArgument`, limit/offset, no time range, bounded time
  range.
- Mixed layout: legacy `_im_name` documents and new `_subject` documents in one `sidx/` are both
  visible to a subject-scoped query.
- Shadowing: an older segment's document never overrides a newer segment's document; a delete or
  update in the hot segment is visible immediately.
- Storage: reopen, crash/restart, segment rotation, TTL deletion, hot→warm→cold migration.

### Integration tests

- Standalone and distributed: create the resource in a measure group, write from one node, query
  from the liaison and from every data node, restart a data node, add a tag via schema update.
- Shared group: a columnar measure, a legacy index-mode measure and a `DocumentIndex` coexist in
  one group; each query sees only its own subject, and a group drop removes all of them.
- Sharding: the same entity always routes to the same shard; a multi-shard query dedupes correctly.
- Migration: declare a `DocumentIndex` over existing index-mode documents without moving data,
  verify every document and digest, run the optional in-place re-index (idempotent), and verify a
  rollback to a new index-mode measure still works while the old documents remain readable.
- Mixed-version: an old client/node that does not know `KindDocumentIndex` must fail clearly, not
  panic or silently drop the resource.
- OAP: run the existing BanyanDB metadata integration tests against a `DocumentIndex` resource in
  the `metadata` group and the fallback index-mode measure, and compare results.

### Performance gates

No regression against index-mode measure for the metadata workloads OAP runs:

- write throughput and peak RSS for N documents per batch;
- point lookup by entity, filtered list, and limited time-ordered query;
- 1M-document group reopen and query after restart;
- re-index throughput (documents/second) and verification memory.

Add one gate for the shared-group case: a measure group hosting both a columnar measure and a
`DocumentIndex` resource, measuring write amplification, segment index size and query latency with
both layouts in one `sidx/`.

The storage engine is the same, so the target is parity, not an improvement. The expected wins are
lower write amplification (no synthetic duplicate entity fields) and a simpler query plan.

## 9. Alternatives considered

**Keep `DocumentIndex` as a schema kind inside a host group (chosen).** One module, one group
lifecycle, one storage path, one root path, one flag set. The catalog already does not select the
engine: a measure group hosts columnar measures and index-mode documents today, switched by a
boolean on the schema, so mixing storage shapes in a group is an existing invariant. The subject
field already scopes resources inside a shared `sidx/`, so a new kind fits the same mechanism.
Backup, transfer, TTL, stages and group drop keep working unchanged, and migration becomes a
declaration or an optional in-place re-index instead of a copy. The cost is that the host module
carries the document write/query path, which is the same code index mode uses today.

**Add `CATALOG_DOCUMENT_INDEX` and a `banyand/document` module (rejected).** In this repository
the catalog is the module selector, not only a schema label. A new value pulls in a new service
package, a new root path and flag set, a new setup registration and a case in every catalog switch
(backup, transfer export/import, migration classifier, collect/drop topics), and it forces a new
group because a group's catalog is effectively immutable. That is a second module's worth of
surface for a storage shape the host module already serves.

**Rename `index_mode` to `document_index` inside `Measure`.** Cheapest, but keeps fields,
interval, TopN and the schema fork. It does not achieve the separation the issue asks for.

**Use `Stream` with upsert.** Stream already writes documents to an inverted index and has index
rules, but its elements are immutable append-only rows with an `element_id`; adding upsert would
change Stream's identity and dedup contract for logs and events. It also still needs an element ID
and timestamp, and the subject/entity layout would remain stream-specific. A stream group remains a
valid future host for `DocumentIndex`, with its own identity rule, but it is not this design.

**Build a new document engine.** The native ICE v3 index already provides the exact primitives
(upsert, criteria, projections, sort ladders, limit pushdown, snapshots, external segments). A new
engine would add risk without changing the API.

## 10. Rollout phases

| Phase | Deliverable | Depends on |
| --- | --- | --- |
| 0 | Extract the result builder; introduce `SubjectField` and `IndexSearchOpts.Subject` without changing the `_im_name` literal; parity tests green | — |
| 1 | `KindDocumentIndex` + `DocumentIndex` schema message + registry service + bydbctl schema commands; no catalog value, no group support | Phase 0 |
| 2 | Write/query protos and RPCs implemented in the host module, storage path, name-keyed sortable order, dual subject read, standalone/distributed query, TTL/stages | Phase 1 |
| 3 | OAP annotation, schema registration in the existing `metadata` group, client, DAOs, config, version gate | Phase 2 |
| 4 | Optional in-place re-index tool, upgrade docs, E2E, OAP cut-over | Phase 3 |
| 5 | Export/import, Canopy schema view, `bydbctl analyze`, default flag flip, deprecate `Measure.index_mode` | Phase 4 |

Phases 1 and 2 can land as one PR if the reviewer prefers; phase 0 can land independently and is
useful even if the rest slips.

## 11. Open questions

1. **Proto package name**: `banyandb.document.v1` or `banyandb.documentindex.v1`? The recommendation
   is the shorter `document.v1` package. The catalog value question is settled: there is none.
2. **Default indexing**: every tag indexed by default (recommended, matches OAP), or only tags with
   an explicit option? For the OAP models, mirror the existing
   `storageOnly`/`EnableSort`/`MatchQuery` annotations rather than introduce new defaults.
3. **Timestamp ownership**: server time by default (recommended) with import/re-index able to set
   it, or require the client to send it? Server time makes updates land in the hot segment and
   keeps TTL semantics obvious.
4. **Count queries**: add `count_only` now or defer? The OAP metadata UI mostly needs lists, so it
   can be deferred, but a count avoids transferring documents for pagination.
5. **Composite indexes**: support them in `DocumentTagOptions` now (as a repeated tag-name list) or
   wait for a real use case? The recommendation is to wait; the single-tag index covers the
   metadata workloads.
6. **Fallback policy**: if the feature flag is on but the server is old, fall back silently with a
   warning (recommended for a smooth upgrade) or refuse to start (safer for a mixed cluster)? The
   recommendation is to refuse writes to a half-migrated resource and fall back to the old mode,
   never to mix the two in one query.
7. **Removal window for `Measure.index_mode`**: one major release of read support after the
   re-index path ships, or longer? The recommendation is at least two minor releases and a
   documented breaking-change note.
