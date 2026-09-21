# Data Export and Import

Status: **design** — not implemented.

BanyanDB already supports snapshots, backup/restore, and lifecycle management, but it still lacks
an operations-oriented, cluster-level data import/export workflow. Backup moves a whole node's
files to remote storage; lifecycle moves data between stages inside one cluster. Neither lets an
operator take a named set of groups out of one cluster and put it into another.

This document specifies that workflow as two `bydbctl` subcommands, a pair of gRPC services, and
an on-disk artifact format. Two things shape most of the design. First, the client never touches
a data node directly — everything crosses a liaison, which relays frames without storing them.
Second, the same logical shard routinely exists on several nodes, so the artifact carries every
copy and the decision about which copies to push happens on the import side, where a wrong guess
costs a re-run rather than data.

---

## 1. Goals

- **Support all data types**: import/export for stream, measure, trace, and property, including
  groups managed by lifecycle.
- **Connect through liaison nodes**: the client talks to the liaison endpoint, which routes to the
  corresponding data nodes.
- **Support multiple formats**: native and CSV. Native is efficient; CSV is readable.
- **Support schemas**: metadata and schemas travel with the data.

## 2. Architecture

Two subcommands are added to `bydbctl`:

```shell
# Export
bydbctl data export --plan plan.yaml                  # Use --plan to specify the export configuration file.
bydbctl data export --plan plan.yaml [--output <dir>] # Override part of the export configuration.

# Import
bydbctl data import --plan plan.yaml                  # Use --plan to specify the import configuration file.
# Import and export can share the same configuration file.
bydbctl data import --plan plan.yaml [--input <dir>]  # Override part of the import configuration.
```

### 2.1 Plan file

All import and export operations are driven by a plan file holding the key configuration.

```yaml
connection: # Shared by both commands
  nodes: [liaison-1:17912, liaison-2:17912] # Liaison node list; the client polls these nodes
  nodesTLS: {enable: false, insecure: false, cert: ""}   # TLS for liaison node port 17912
  rateLimit: 0                        # max bytes of data import/export each liaison node

export:                               # Export only: scope and options
  output: ./backup-20260708           # Export directory; CLI --output overrides this. Error if both are missing
  format: csv                         # native | csv
  compress: gzip                      # none | gzip; gzip is CSV-only, native only accepts none
  maxFileSize: 1GiB                   # Shared by both formats: soft limit per file; 0 = unlimited
  includeSchema: true
  parallelism: max
  nodeRateLimit: 0                    # per-node byte rate limit, 0 = unlimited, e.g. "50MiB/s"
  nodeRowLimit: 0                     # CSV only: per-node row rate limit (rows/s), 0 = unlimited
  selectors:                          # Empty or omitted = all catalogs, groups, and time ranges
    - catalog: stream
      groups: [sw_record, sw_log]     # One selector can include multiple groups under the same catalog
    - catalog: measure
      groups: [sw_metric]
    - catalog: property
      groups: [sw_prop]

import:                               # Import only: import policy; scope is read from the artifact manifest
  input: ./backup-20260708            # Import directory; CLI --input overrides this. Error if both are missing
  includeSchema: true
  schemaConflictPolicy:          # compare schemas before import and apply the policy to each resource
    default: error               # error (default) | ignore | overwrite
    rules:                       # override the default policy for specific resources
                                 # match = <kind>/<group>[/<name>]
    - match: group/sw_metric     # Example: allow topology differences only for this group
      policy: ignore
  parallelism: max
  writeRateLimit: 0              # CSV only: row replay rate limit (rows/s); 0 = unlimited
  partRateLimit: 0               # Native only: total outbound byte rate limit, e.g. "50MiB/s"; 0 = unlimited
  checkpoint: ""                 # Empty = <artifact>/.import-progress.json
  workDir: /data/import-work     # Local scratch directory, used only to unpack .tar/.tar.gz artifacts
```

`workDir` is **not** a rebuild workspace. Neither import channel reconstructs parts locally: the native channel relays opaque bytes and the CSV channel parses rows into write requests. Unpacking an archived artifact is the only local scratch requirement.

## 3. Dry run

Dry run walks the real workflow without importing or exporting anything.

### 3.1 Export dry run

1. **Connection check** — connect to one liaison and call `GetCurrentNode` to confirm the `LIAISON` role, then `GetClusterState` for the other nodes' IDs, labels, versions, and timezones.
2. **Fetch schema** — call the Schema gRPC service and load schemas into memory.
3. **Plan request** — send one `Plan` request to that liaison, which filters all `DATA`-role nodes, forwards the request, and waits for their responses.
4. **Client aggregation** — aggregate and print what every node returned.

```text
NODE    CATALOG   GROUP       STAGE   SEGMENTS  SHARDS  PARTS  EST-ROWS   EST-SIZE(comp/raw)  SHARD-COVERAGE  TIME-RANGE
data-1  measure   sw_metric   hot     7         2       34     118.2M     18.3GiB / 61.2GiB   2/2 ok          06-30 ~ 07-07
data-1  stream    sw_record   hot     7         2       61     342.7M     96.1GiB / 288GiB    2/2 ok          06-30 ~ 07-07
data-2  measure   sw_metric   warm    28        4       52     1.4G       201.5GiB / 702GiB   3/4 WARN        05-30 ~ 06-30
data-2  property  sw_prop     -       -         1       -      12.3K      4.1MiB / 12.0MiB    1/1 ok          -
```

Shard coverage is reported as observed, never derived from the schema's `shardNum`. Shards and segments are created lazily, so comparing against an expected set produces large-scale false reports.

### 3.2 Import dry run

1. **Connection check** — connect to every node in `connection` and confirm reachability.
2. **Artifact check** — verify the artifact is complete.
3. **Schema comparison** — fetch the target schema and compare it with the artifact's.
4. **Data compatibility check** — compare the artifact against the versions the target nodes support.

```text
TARGET   data-1:17912 data-2:17912  (from --plan ./restore.yaml) | 0.10.1 | supported segment formats [1.4.0 1.5.0] | api 0.10 | tz Asia/Shanghai
         schema endpoints data-1:17916 data-2:17916  (auto-discovered, active 2/2)

SOURCE   ./backup-20260707  native  artifactVersion=1  exported at 2026-07-07T12:00:00Z | 0.9.0 | tz Asia/Shanghai

BYDBCTL  0.10.1 | segment format 1.5.0 | api 0.10

CATALOG   GROUP       STAGE  SEGMENTS  SHARDS(src→dst)  ROWS     SIZE(comp/raw)      SCHEMA    CHANNEL          TARGET-NODES   GATE   TIME-RANGE
stream    sw_record   hot     7        2→2              342.7M   96.1GiB / 288GiB    CONFLICT  ① part-push      data-1,data-2  BLOCK  06-30 ~ 07-07
measure   sw_metric   hot     7        2→4              118.2M   18.3GiB / 61.2GiB   CREATE    ② row replay     data-1,data-2  ok     06-30 ~ 07-07
measure   sw_metric   warm   28        4→4                1.4G   201.5GiB / 702GiB   OK        ① part-push      data-2         TTL!   05-30 ~ 06-30
measure   sw_traffic  hot     7        -                  0.4M   12.3MiB / 41.0MiB   CREATE    ② row replay     data-1,data-2  ok     06-30 ~ 07-07
trace     sw_trace    hot     7        2→4               12.1M   4.1GiB / 14.2GiB    CONFLICT  ② row replay     -              BLOCK  06-30 ~ 07-07
property  sw_prop     -       -        1→1               12.3K   4.1MiB / 12.0MiB    CREATE    property/repair  data-1,data-2  ok     -
TOTAL                        49                          1.87G   320.1GiB / 1.09TiB                                                  05-30 ~ 07-07
```

`TARGET-NODES` is resolved by the liaison — one `LocateAll` call per `(group, shard)` — and never computed on the client. The real `roundRobinSelector.Pick` indexes a globally sorted lookup table rather than the shard ID, so a client-side `nodes[(shardID + replicaID) % len(nodes)]` is wrong as soon as more than one group exists.

`measure/<group>/_top_n_result` is filtered out before comparison and reported on its own `SKIP` line. The target creates it automatically from its own hardcoded schema, so including it only produces conflicts the user can neither fix nor should fix.

## 4. Export

### 4.1 Client

1. **Connection check** — select an available liaison and call `GetClusterState` for the topology.
2. **Fetch schema** — call the Schema gRPC service and load schemas into memory.
3. **Plan request** — send one `Plan` request; the liaison fans it out to all `DATA`-role nodes.
4. **Aggregate** — group results by node to decide what to export from each.
5. **Execute export** — connect to the available liaisons and send export requests.
6. **Write manifest** — write the summary file to the export root.

The export side performs **no source selection**. Every source node's copy of a unit is carried away verbatim into its own `nodes/<node-dir>/` subtree, and all multi-source judgement happens on the import side (§6). This is deliberate: a wrong judgement on the export side loses bytes permanently, while a wrong judgement on the import side costs a re-run.

#### 4.1.1 Node IDs are not path-safe

A node ID is **not** a friendly name. `GenerateNode` builds it as `net.JoinHostPort(host, port)` (`api/common/id.go`), so a normal ID is `10.0.0.5:17912`, and an IPv6 one is `[::1]:17912`. Those contain `:`, `[` and `]`, which are illegal in a Windows path component and awkward everywhere. The artifact must therefore never use the raw ID as a directory name.

Two values are kept, and they have different jobs:

| | Value | Where it lives |
|---|---|---|
| **Raw ID** | `10.0.0.5:17912` | `manifest.source.nodes[].id`, and every `node_id` field on the wire. This is the identity |
| **Directory component** | `10-0-0-5-17912-3f9a1c2d` | `manifest.source.nodes[].dir`, and the `nodes/<node-dir>/` path. This is only a filename |

The directory component is derived from the raw ID by:

1. lowercase the raw ID;
2. replace every run of characters outside `[a-z0-9]` with a single `-`;
3. trim leading and trailing `-`; if nothing is left, use `node`;
4. truncate to 32 characters;
5. append `-` followed by the first 8 hex characters of `SHA-256` over the **raw** UTF-8 bytes.

**Reversal is by lookup in the manifest, not by decoding.** Step 2 and step 4 are lossy on purpose — the readable prefix exists so an operator can tell subtrees apart, and the manifest carries the exact ID for anything that needs it. Import resolves a subtree by matching `dir`, and uses the paired `id` wherever identity matters.

The hash suffix is what makes the mapping injective, and it covers the collision cases a plain sanitizer misses:

| Raw IDs | Sanitized prefix | Result |
|---|---|---|
| `10.0.0.5:17912`, `10.0.0.5:17913` | both `10-0-0-5-1791x` after truncation of a longer host | distinct, the suffixes differ |
| `data-A`, `data-a` | both `data-a` | distinct — macOS and Windows fold case, the suffix does not |
| `[::1]:17912` | `1-17912` | usable, where the raw ID is not |

Only `manifest.source.nodes[]` and the paths inside it spell a directory component out in full. Every other example in this document abbreviates it to `data-1`, `data-a` and so on, purely for readability.

The client still refuses the export if two nodes in one `Plan` produce the same **full** directory component, which after the hash means a genuine duplicate node ID. That remains a prerequisite: `ConnManager` indexes connections by ID and silently drops same-ID nodes, so a duplicate is already broken at the cluster level, not just in the artifact.

### 4.2 Native export

Exporting in native format:

1. **Create snapshot** for the required data types.
2. **Traverse segments** of every group.
3. **Transfer data** — read files per the gRPC protocol and write them into BNC (BanyanDB Native Container) files in the stream, one frame per 1 MiB. Rate limiting applies here.
4. **Complete export** — finish the gRPC stream.

The storage layout differs by data type and mode.

| Type | Source directory | Output |
|---|---|---|
| Normal group | `<group>/seg-20260907/metadata` | `metadata` (raw file, not packed into a container) |
| | `<group>/seg-20260907/sidx` | `sidx-001.bnc` (segment-level container, one per segment) |
| | `<group>/seg-20260907/shard-0` | `shard-0-001.bnc` (shard-level container, one per shard) |
| Index-mode measure | `<group>/seg-20260907/metadata` | `metadata` |
| | `<group>/seg-20260907/sidx` | `sidx-001.bnc` |
| Property | `property/data/<group>/shard-0` | `shard-0-001.bnc` |

Two different directories are called `sidx`, and they are not the same thing:

| Source path | Actual content | Container |
|---|---|---|
| `seg-XXX/sidx/` | **Series index** — a Bluge inverted directory, no parts | segment-level `sidx-NNN.bnc` |
| `shard-N/sidx/<name>/<016x>/` | **Trace secondary index parts** — real parts, with `manifest.json` | shard-level `shard-N-NNN.bnc` |

#### 4.2.1 BNC file format

```text
[file bytes...][footer protobuf][footer_len uint32][MAGIC "BNCA"]
```

The footer only records which byte range belongs to which file. The magic is `BNC` plus a single format-version letter, so a future incompatible layout becomes `BNCB` without needing a separate version field.

There are only three gRPC frame types:

```text
file_started{ relative_path, unit, seq }   → Client creates <path>.tmp
chunk{ content } × N                       → Client appends data
file_finished{ bytes, crc32, entry_count, row_counts }
                                           → Client validates → fsync → rename
```

### 4.3 CSV export

Exporting in CSV format:

1. **Create snapshot** for the required data types.
2. **Traverse segments** of every group.
3. **Iterate shards** — use the dump readers to read rows shard by shard, encode as CSV, gzip on the fly, and emit one chunk frame per 1 MiB of **compressed** bytes. Both rate limits apply here — see below.
4. **Complete export** — finish the gRPC stream.

The existing dump readers `banyand/internal/dump/{stream,measure,trace}` already support row-level export and can be reused. `property` has no dump reader, so `SeriesIterator` is used instead.

**Rate limiting has two knobs, and CSV honours both.** They throttle different resources, so neither subsumes the other and whichever binds first wins:

| Knob | Plan field | Protobuf field | Measured | native | CSV |
|---|---|---|---|---|---|
| Bytes | `export.nodeRateLimit` | `RateLimit.bytes_per_second` | On the wire, so **after** gzip for CSV | yes | yes |
| Rows | `export.nodeRowLimit` | `RateLimit.rows_per_second` | At the dump reader, before encoding | no — native moves opaque bytes and has no row concept | yes |

The byte knob protects the liaison and the network; the row knob protects the data node, whose cost for CSV is row iteration rather than transfer. A CSV export that sets only `nodeRateLimit` can still saturate a data node's CPU on a highly compressible group, which is why the row knob exists as well as, not instead of, the byte knob.

#### 4.3.1 Value encoding

| Type | Encoding |
|---|---|
| STRING | Written via `encoding/csv`; only values containing `\r\n` need extra encoding as `\B` + base64 |
| INT | Decimal |
| FLOAT (measure field) | `strconv.FormatFloat(v, 'f', -1, 64)`. Precision **must** be `-1`, or the round trip loses precision |
| DATA_BINARY / span payload | `base64.StdEncoding`, standard alphabet, with padding |
| STRING_ARRAY | JSON array string |
| INT_ARRAY | Semicolon-separated decimal int64 values; an empty array is an empty string |
| All timestamp columns | Decimal int64 UnixNano; trace-only for TIMESTAMP tags |
| null | `\N`; a literal `\N` is escaped as `\\N` |

> **NaN and ±Inf cannot currently be exported at all.** `FormatFloat` renders them as `NaN` / `+Inf` / `-Inf` and `ParseFloat` accepts those literals back, so the encoding is not the problem. The problem is upstream: the write path silently degrades such a column to block type 10, and the `dumpmeasure` reader rejects it outright. This has to be settled before the CSV channel ships — either declare it unsupported, or extend the dump reader.

`'f'` is chosen over `'g'` because `'g'` renders `1000000` as `1e+06`, and five of the six non-test call sites in the repository already use `'f', -1, 64`. Both round-trip bit-exactly. The cost of `'f'` is length: `1e300` becomes a 301-character field, so the export side must not assume an upper bound on field width.

#### 4.3.2 Columns

| Model | Fixed columns | Additional columns |
|---|---|---|
| stream | `_name`, `_element_id`, `_timestamp` | all `tagFamily.tag` columns |
| measure (normal) | `_name`, `_timestamp`, `_version` | all `tagFamily.tag` and all `_field.*` columns |
| measure (index-mode) | `_name`, `_timestamp`, `_version` | all `tagFamily.tag` columns, no `_field.*` |
| trace | `_name`, `trace_id`, `span_id`, `_span` | all tags, without the family level |
| property | `_name`, `_id`, `_mod_revision`, `_create_revision`, `_updated_at`, `_delete_time` | all tags |

**stream**

```csv
_name,_element_id,_timestamp,searchable.trace_id,searchable.service_id,searchable.state,data.data_binary
sw_service_log,73281937461,1751883300123000000,af1c...beef,c2VydmljZS1h,1,ZGF0YQ==
```

**measure** — `_version` is preserved to maintain deduplication semantics.

```csv
_name,_timestamp,_version,default.entity_id,default.scope,_field.total,_field.value,_field.percentile
service_cpm_minute,1751883300000000000,1751846401000000,c2VydmljZS1h,SERVICE,\N,87,\N
service_resp_time,1751883300000000000,1751846401000000,c2VydmljZS1h,SERVICE,1024,\N,W1BMQUNFSE9MREVSXQ==
```

`_field.percentile` has type `DATA_BINARY`. `FieldValue` has no array branch, so it must use base64 rather than a JSON array such as `"[50,75,90,95,99]"`.

**trace**

```csv
_name,trace_id,span_id,_span,service_id,start_time,duration
sw_segments,af1c...beef,1,CgZzZWdtZW50EgQ...,c2VydmljZS1h,1751846400123000000,150
```

**property**

```csv
_name,_id,_mod_revision,_create_revision,_updated_at,_delete_time,name,value
ui_template,ui-dashboard-1,1751846400000000000,1751846400000000000,1751846400000000000,0,dashboard,"{""layout"":""grid""}"
```

`_delete_time` is a **required** column — a non-zero value is a tombstone. `_create_revision` and `_updated_at` must also be carried, or version merging and gossip repair on the target cannot decide correctly.

### 4.4 Artifact file format

The top-level artifact structure is identical for both formats; only the leaf files differ.

| Path | Description |
|---|---|
| `manifest.json` | Written last |
| `schema/` | One JSON Lines file per kind, one protojson object per line |
| `nodes/<node-dir>/` | Organized by source node, keyed by the derived directory component (§4.1.1), never the raw node ID |
| `nodes/<node-dir>/<catalog>/<group>/` | Further organized by catalog and group |

Below that level:

| | Native | CSV |
|---|---|---|
| Normal group | `seg-<suffix>/metadata` (raw file)<br>`seg-<suffix>/sidx-001.bnc`<br>`seg-<suffix>/shard-<N>-001.bnc` | `seg-<suffix>/shard-<N>-00001.csv.gz` |
| Index-mode measure | `seg-<suffix>/metadata`<br>`seg-<suffix>/sidx-001.bnc` | `seg-<suffix>/segment-00001.csv.gz` |
| Property | `shard-<N>-001.bnc` (no `seg-` layer) | `shard-<N>-00001.csv.gz` (no `seg-` layer) |
| File splitting | `-001`, `-002`, …; each volume has a complete footer | `-00001`, `-00002`, …; each volume repeats the header |

### 4.5 manifest.json

`manifest.json` records the format, export time, source cluster, group information, and per-group results.

```json
{
  "artifactVersion": 1,
  "format": "native",
  "exportedAt": "2026-09-07T12:00:00Z",
  "source": {
    "banyandVersion": "0.11",
    "nodes": [{"id": "10.0.0.5:17912", "dir": "10-0-0-5-17912-3f9a1c2d",
               "timezone": "Asia/Shanghai"}]
  },
  "groups": [{
    "name": "sw_metric", "catalog": "MEASURE", "indexMode": false,
    "stages": [{"name": "hot", "shardNum": 2, "segmentInterval": "1d", "ttl": "7d", "replicas": 1}]
  }],
  "units": [{
    "catalog": "MEASURE", "group": "sw_metric", "stage": "hot", "segment": "20260907",
    "minTimestamp": 1757203200000000000, "maxTimestamp": 1757289599999999999,
    "segmentVersion": "1.5.0",
    "sources": [{
      "node": "10.0.0.5:17912",
      "segmentFiles": [
        {"path": "nodes/10-0-0-5-17912-3f9a1c2d/measure/sw_metric/seg-20260907/metadata",
         "bytes": 34, "crc32": "0x1f4a03bb"},
        {"path": "nodes/10-0-0-5-17912-3f9a1c2d/measure/sw_metric/seg-20260907/sidx-001.bnc",
         "bytes": 4194618, "crc32": "0x3a91c7e2", "entryCount": 3}
      ],
      "sidxCoversShards": [0, 1],
      "shards": [{
        "shard": 0,
        "minTimestamp": "2026-09-07 00:00", "maxTimestamp": "2026-09-07 23:59",
        "totalCount": 123456,
        "parts": [
          {"id": 1, "minTimestamp": 1757203200000000000,
                    "maxTimestamp": 1757224799999999999, "totalCount": 54321},
          {"id": 2, "minTimestamp": 1757224800000000000,
                    "maxTimestamp": 1757289599999999999, "totalCount": 69135}
        ],
        "rowCounts": {"service_cpm_minute": 123456},
        "estimatedCompressedBytes": 10737418,
        "estimatedUncompressedBytes": 41943040,
        "files": [
          {"path": "nodes/10-0-0-5-17912-3f9a1c2d/measure/sw_metric/seg-20260907/shard-0-001.bnc",
           "bytes": 402653184, "crc32": "0x9f2a1b34", "entryCount": 47},
          {"path": "nodes/10-0-0-5-17912-3f9a1c2d/measure/sw_metric/seg-20260907/shard-0-002.bnc",
           "bytes": 118374400, "crc32": "0x77e0aa15", "entryCount": 12}
        ]
      }]
    }]
  }],
  "schemaFiles": [{"path": "schema/group.jsonl", "bytes": 812, "crc32": "0x05f5e0fe"}],
  "warnings": []
}
```

`shards[].parts[]` is written **unconditionally**, for every shard of every source. The export side does not decide which shards need it: that array is the input to the import-side multi-source judgement (§6.2), and a shard that looks single-sourced today becomes multi-sourced as soon as a second artifact is merged in.

`sidxCoversShards` is the **union of the route-derived shard set and the shard directories actually present on disk**. Both halves are required:

- Using only the disk fails for index-mode measure, which has no shard directories at all even though the segment-level sidx is its only payload.
- Using only the routing table fails after topology drift: a node can route-own shard `[1]` while physically holding shard `[3]`, with an empty intersection.
- The union costs nothing. Over-reporting a shard opens one extra stream whose bytes are absorbed by receiver-side deduplication; under-reporting makes `Lookup(series)` miss and renders the whole segment unqueryable.

### 4.6 Protocol

A new gRPC export service carries the workflow. The data node generates the stream directly and the client writes it to local files. The same service backs dry run.

```protobuf
service ExportService {
  // Plan performs inventory collection.
  // Its behavior is determined by the request fields.
  rpc Plan(PlanRequest) returns (stream PlanResponse);

  // ProbeSession reports whether a session exists and whether it is still live.
  // Read-only: it never creates, renews, or deletes anything.
  rpc ProbeSession(ProbeSessionRequest) returns (ProbeSessionResponse);

  // ReleaseSession asks each node to delete the snapshot for the session.
  rpc ReleaseSession(ReleaseSessionRequest) returns (ReleaseSessionResponse);

  // One export operation may contain multiple Export calls, one per node.
  // The snapshot belongs to the session rather than to any individual call.
  rpc Export(stream ExportRequest) returns (stream ExportResponse);
}

enum Format {
  FORMAT_UNSPECIFIED = 0;
  FORMAT_NATIVE = 1;   // .bnc container containing raw engine file bytes
  FORMAT_CSV = 2;      // Logical rows
}

message Selector {
  common.v1.Catalog catalog = 1;
  repeated string groups = 2;
}

// SegmentUnit is the key of a physical unit.
message SegmentUnit {
  common.v1.Catalog catalog = 1;
  string group = 2;

  // Example: "20260907"; always empty for property.
  string segment_suffix = 3;

  repeated uint32 shard_ids = 4;
}

// UnitInventory is returned by Plan.
// It contains the unit key and statistics calculated by the server.
message UnitInventory {
  SegmentUnit unit = 1;

  // Version from the segment root metadata file.
  // Used as the version gate during import.
  string segment_version = 2;

  // Per-shard statistics.
  repeated ShardStat shards = 3;

  // Statistics for segment-level outputs: metadata + sidx/.
  SidxStat segment_level = 4;
}

message SidxStat {
  // On-disk bytes of metadata + sidx/.
  uint64 estimated_bytes = 1;

  // Number of live documents in segment-level sidx,
  // obtained from inverted.ReadOnlyDocCount.
  uint64 doc_count = 2;
}

message ShardStat {
  uint32 shard_id = 1;

  // Full time range of this shard in UnixNano.
  int64 min_timestamp = 2;
  int64 max_timestamp = 3;

  uint64 estimated_compressed_bytes = 4;
  uint64 estimated_uncompressed_bytes = 5;
  uint32 parts_count = 6;
  uint64 total_count = 7;
  repeated PartStat parts = 8;
}

message PartStat {
  uint64 id = 1;
  int64  min_timestamp = 2;
  int64  max_timestamp = 3;
  uint64 total_count = 4;
}

message ProbeSessionRequest {
  string session_id = 1;
}

message ProbeSessionResponse {
  repeated SessionLease leases = 1;
}

message SessionLease {
  string node_id = 1;
  string session_id = 2;
  int64 created_at = 3;
  int64 last_renewed_at = 4;
  int64 expires_at = 5;
}

message ReleaseSessionRequest {
  string session_id = 1;
}

message ReleaseSessionResponse {
  repeated string failed_nodes = 1;
}

message PlanRequest {
  // Empty = all catalogs and groups in the cluster.
  repeated Selector selectors = 1;

  // Select which view to read:
  // empty = online directories;
  // non-empty = snapshot of the given session, and renew its lease.
  string session_id = 2;

  // Only meaningful when session_id is empty.
  bool create_session = 3;
  uint32 lease_seconds = 4;
  bool renew_only = 5;

  // Take over a session whose holder is judged dead. Requires --preempt.
  bool preempt = 6;
}

message PlanResponse {
  string node_id = 1;
  string stage = 2;
  repeated UnitInventory units = 3;

  string session_id = 4;
  int64 expires_at = 5;

  repeated common.v1.Catalog failed_catalogs = 6;
  repeated string failed_reasons = 7;
  repeated string unreachable_nodes = 8;
  repeated string preempted_session_ids = 9;
}

message ExportRequest {
  oneof frame {
    Init init = 1;          // First frame only
    SegmentUnit unit = 2;   // One unit per subsequent frame
  }

  message Init {
    // Snapshot session used for export.
    string session_id = 1;

    // Target data node for this stream.
    string target_node = 2;

    Format format = 3;

    // Soft limit for a single output file in bytes.
    // 0 = unlimited.
    uint64 max_file_size = 4;

    Compress compress = 5;
    RateLimit rate_limit = 6;
  }
}

enum Compress {
  COMPRESS_UNSPECIFIED = 0;
  COMPRESS_NONE = 1;
  COMPRESS_GZIP = 2;
}

// Output rate limit for a single data node.
// 0 = unlimited.
message RateLimit {
  uint64 bytes_per_second = 1;   // Applies to both formats
  uint64 rows_per_second = 2;    // Applies only to FORMAT_CSV
}

// Frame order:
// ready
// → [unit_started,
//    (file_started, chunk × N, file_finished) × M,
//    unit_finished] × number of units
message ExportResponse {
  oneof content {
    Ready ready = 1;                // Response to Init, once per stream
    UnitStarted unit_started = 2;   // Once per unit
    FileStarted file_started = 3;
    DataChunk chunk = 4;
    FileFinished file_finished = 5;
    UnitFinished unit_finished = 6; // Once per unit;
                                    // ctl sends the next unit only after receiving this
  }
}

message Ready {
  Compress effective_compress = 1;
  RateLimit effective_rate_limit = 2;
}

message UnitStarted {
  // len(unit.shard_ids); 0 for index-mode measure.
  uint32 total_shards = 1;

  // Total number of files generated for this unit,
  // including split files and segment-level outputs.
  uint32 total_files = 2;

  // Native: sum of file sizes obtained from ReadDir.
  // CSV: 0 because it cannot be known in advance.
  uint64 estimated_total_bytes = 3;
}

message FileStarted {
  // The client prefixes nodes/<dir of target_node>/ when writing locally,
  // where the directory component is derived per section 4.1.1.
  //
  // Examples:
  // "stream/sw_record/seg-20260907/shard-0-001.bnc"
  // ".../sidx-001.bnc"
  // ".../metadata"
  string relative_path = 1;

  // Shard this file belongs to.
  uint32 shard_id = 2;

  // Segment-level output such as metadata or sidx-NNN.bnc.
  bool segment_level = 3;

  // Progress: shard index within the current unit.
  uint32 shard_index = 4;
  uint32 seq = 5;
}

message DataChunk {
  bytes content = 1;
}

message FileFinished {
  uint64 bytes = 1;
  uint32 crc32 = 2;
  uint32 entry_count = 3;
  map<string, uint64> row_counts = 4;
  repeated string warnings = 5;
}

message UnitFinished {
  repeated string warnings = 1;
}
```

### 4.7 Full workflow

#### 4.7.1 Phase 1 — Plan: create session and inventory

| # | Direction | Frame | Liaison behavior |
|---|---|---|---|
| 1 | ctl → liaison | `Plan{selectors=[…], create_session=true}` | Generate one `session_id` for the whole fan-out. A node already holding a different live session rejects the create and returns the incumbent's ID (§4.8.1) |
| 2 | liaison → data-1 / data-2 | One `Plan{…, session_id="xx…", create_session=true}` per node | — |
| 3 | Each data node | Create an `export-9f2a…` snapshot, write `.lease`, inventory from the snapshot | — |
| 4 | data-* → liaison → ctl | Stream `PlanResponse{units[…]}` frames | Forward each frame as it arrives |
| 5 | ctl | Write the `session_id` and complete unit inventory into `.export-progress.json` | — |
| 6 | ctl | Group entries by `node_id` to build the assignment table | — |

Only this phase creates snapshots; they are created once and reused for the whole export. The liaison stamps `node_id` on every frame from the upstream connection and does not trust a node's self-report.

#### 4.7.2 Phase 2 — Export: transfer unit by unit

Each data node uses one stream. The example shows `data-1`; `data-2` runs in parallel with the same structure.

| # | Direction | Frame | Liaison behavior |
|---|---|---|---|
| 1 | ctl → liaison | `Init{session_id="9f2a…", target_node="data-1", format=NATIVE, max_file_size=512MiB, rate_limit{…}}` | Read `target_node`, take the pooled connection, open an upstream bidirectional `Export` stream |
| 2 | liaison → data-1 | `Init` | Forward unchanged, without modifying any field |
| 3 | data-1 | Locate `export-9f2a…` by `session_id` and renew the lease | Forward errors unchanged |
| 4 | data-1 → liaison → ctl | `ready{}` | Forward |
| 5 | ctl → … → data-1 | `unit{catalog=STREAM, group="sw_record", segment_suffix="20260907", shard_ids=[0,1]}` | Forward. Only one unit at a time |
| 6 | data-1 | Build paths, stat the two shards and the segment directory, `ReadDir` the file list | — |
| 7 | data-1 → liaison → ctl | `unit_started{total_shards=2}` | Forward |
| 8 | data-1 → liaison → ctl | `file_started{path=".../seg-20260907/metadata"}` | Forward |
| 9 | data-1 → liaison → ctl | `chunk` → `file_finished{bytes=52, crc32=0x1f4a…}` | Forward |
| 10 | data-1 → liaison → ctl | `file_started{path=".../sidx-001.bnc"}` → `chunk × N` → `file_finished{}` | Forward |
| 11 | data-1 → liaison → ctl | `file_started{path=".../shard-0-001.bnc"}` → `chunk × N` → `file_finished{}` | Forward. First of 2 shards |
| 12 | data-1 → liaison → ctl | `file_started{path=".../shard-1-001.bnc"}` → `chunk × N` → `file_finished{}` | Forward. Second of 2 shards |
| 13 | data-1 → liaison → ctl | `unit_finished{warnings=[]}` | Forward |
| 14 | ctl | Record the completed unit in `done` of `.export-progress.json` | — |
| 15 | ctl → … → data-1 | `unit{…, segment_suffix="20260908", shard_ids=[0,1]}` | Forward and repeat steps 7–13 |
| 16 | ctl → liaison | `CloseSend` | — |
| 17 | data-1 → … → ctl | Response stream ends normally | — |

#### 4.7.3 Phase 3 — Finalization

| # | Direction | Frame | Liaison behavior |
|---|---|---|---|
| 1 | ctl → liaison | `ReleaseSession{session_id="9f2a…"}` | Fan out to all data nodes |
| 2 | Each data node | Delete `export-xx…`; if already absent, still return success for idempotency | Aggregate `failed_nodes` |

### 4.8 Session management

Each export uses one session, corresponding to one snapshot per data node. One export should own the whole cluster at a time — but that is an outcome the protocol converges to, **not an invariant any single component enforces**, and the difference matters for implementers.

#### 4.8.1 There is no coordinator, so uniqueness is per node

BanyanDB has no etcd, no lock service, and no CAS primitive anywhere in the tree; node discovery is file-, DNS- or flag-based. A session therefore cannot be claimed atomically cluster-wide before fan-out. The only real enforcement point is **each data node's own snapshot directory**.

That leaves a genuine race. Two clients calling `Plan(create_session=true)` through two different liaisons can each win on a different subset of data nodes, producing two live, partial sessions whose heartbeats keep either from looking dead. Left unhandled they would deadlock each other. The protocol resolves it without a coordinator:

1. **A node accepts a create only if it holds no live session.** If it already holds one under a different `session_id`, it rejects the create and returns the **incumbent's** ID rather than a bare error.
2. **Any rejection aborts the whole attempt.** A client that sees even one node reject must call `ReleaseSession` for its own ID on every node that did accept, then fail with a concurrent-export error naming the incumbent IDs it saw.
3. **Retry with jittered backoff.** Because the loser releases everything it took, the next attempt finds a clean subset. Two clients retrying in lockstep is the only livelock risk, and jitter bounds it.

Step 2 is the load-bearing one: a client that keeps a partial session after a partial win is what creates the deadlock. Releasing is cheap — the snapshots are hard links.

`ProbeSessionResponse` returns `leases` **per node** precisely so this state is observable. A probe that comes back with two distinct `session_id` values, or with leases on only some nodes, is reporting a partial session, not a healthy one. The client must surface that verbatim; `--preempt` in that state releases **every** ID it found before creating its own, rather than taking over one of them.

Beyond the race, a partial session left behind by a client that died mid-abort is reclaimed by the hourly expiry sweep (step 5 below) with no operator action.

#### 4.8.2 Taking over an existing session

Starting a new export does **not** silently remove an existing session. The client first calls `ProbeSession`, which is read-only, and decides from `last_renewed_at`:

- **Live holder** (renewed within the liveness threshold): refuse the new export and report who holds it.
- **Dead holder** (no renewal past the threshold): still refuse, but tell the user that `--preempt` takes it over. `--preempt` is a confirmation action, not a tuning knob.

Lifecycle:

1. **Create** — the liaison sends `Plan(create_session=true)` to all data nodes.
2. **Export** — export requests carry the `session_id` to read the matching snapshot.
3. **Heartbeat** — `bydbctl` sends `Plan(session_id, renew_only=true)` on a fixed interval.
4. **Release** — after the export completes, `bydbctl` calls `ReleaseSession(session_id)`.
5. **Cleanup** — each data node scans export snapshots hourly and removes expired sessions.

| Constant | Value | Why it is not configurable |
|---|---|---|
| Default lease | 5 days | Hard guard against an obviously wrong input |
| Maximum lease | 30 days | Same |
| Heartbeat interval | 5 min | A client-side timer, deliberately not derived from the lease — otherwise the liveness threshold becomes tunable, and that is part of the mutual-exclusion semantics |
| Liveness threshold | 15 min (3 heartbeats) | Same. Three heartbeats tolerate one lost heartbeat without declaring a live holder dead |

The body of a snapshot is hard links, so it copies zero bytes, and deletion is `unlink` only — a background merge that removes a source part during export leaves the snapshot's link fully readable, which is what makes lock-free reading safe. "Hard links, therefore free" holds only for cold segments, though: the open segment's series index and the whole property database are full byte copies.

### 4.9 Node gRPC extension

The node response must expose supported file versions and timezone information.

```protobuf
message Node { // api/proto/banyandb/database/v1/database.proto
  // ...                            // existing fields 1-9
  banyandb.cluster.v1.VersionInfo version = 10;
  string tz_name = 11;
}
```

## 5. Import

### 5.1 Workflow

1. **Connection check** — call `GetCurrentNode` on each node.
2. **Artifact check** — validate `manifest.json`, the artifact version, and every file's CRC32.
3. **Schema check** — compare the artifact's schemas with the target cluster's.
4. **Version compatibility** — for CSV, check the locally generated version against the data nodes; for native, check each shard's part version.
5. **Timezone check** — for native, the target node timezone must match the artifact's, or the import fails.
6. **TTL check** — skip data that has already expired.
7. **Topology check** — reject any unit whose source shard ID is greater than or equal to the target group's `shardNum`. `LocateAll` returns `<group>-<N> is a unknown shard` for such a shard, so the data is simply unroutable. This must be listed and blocked during dry run, not discovered halfway through a push. Topology drift frequently arrives together with a `shardNum` change, so the two conditions co-occur.
8. **Schema import** — create or overwrite per policy, then rerun checks 2–7.
9. **Data import** — per data type, as below.

Two routing failures must be distinguished: zero nodes returns `nil` plus `no nodes available`, while an out-of-range shard returns `... is a unknown shard`. Separately, `LocateAll` deduplicates its result set, so it can silently return fewer targets than the requested `copies` with no error, log, or warning. The import side must compare `len(targets)` against the requested `copies` itself; `--strict-replicas` cannot use `len(targets)` as its denominator.

### 5.2 Native fast push

Fast push is available only when all of the following hold:

- The artifact format is native.
- The target shard number **equals** the source shard number after stage mapping, and both the stage-level `shardNum` and the group-level `ResourceOpts.ShardNum` match. Comparing only the former lets a unit through the gate and then fails in the liaison's `Pick` with `unknown shard`; comparing only the latter gets it silently dropped by `loadShards`.
- The segment interval and timezone are exactly the same.
- The segment version is in the supported compatibility list.
- The schema is either created successfully or already consistent.

#### 5.2.1 Protocol

```protobuf
service ImportService {
  rpc ImportParts(stream ImportPartsRequest) returns (ImportPartsResponse);
}

message ImportPartsRequest {
  oneof frame {
    Init init = 1;
    Chunk chunk = 2;
    Completion completion = 3;
  }

  // One ImportParts call transfers one part.
  message Init {
    common.v1.Catalog catalog = 1;
    string group = 2;
    uint32 shard_id = 3;
    string stage = 4;
    SyncKind kind = 5;            // PART | SERIES_INDEX | ELEMENT_INDEX

    // Required when kind = PART.
    repeated PartInfo parts = 6;

    string segment_version = 7;
  }

  enum SyncKind {
    SYNC_KIND_UNSPECIFIED = 0;
    PART = 1;
    SERIES_INDEX = 2;
    ELEMENT_INDEX = 3;
  }

  // Copy of cluster.v1.PartInfo.
  message PartInfo {
    uint64 id = 1;
    string part_type = 2;         // "core" or trace secondary index name
    uint64 compressed_size_bytes = 3;
    uint64 uncompressed_size_bytes = 4;
    uint64 total_count = 5;
    uint64 blocks_count = 6;
    int64 min_timestamp = 7;
    int64 max_timestamp = 8;
    int64 min_key = 9;
    int64 max_key = 10;
  }

  message Chunk {
    uint32 chunk_index = 1;
    bytes chunk_data = 2;
    string chunk_checksum = 3;

    // Describes which file ranges are included in this chunk.
    // offset and size are relative to chunk_data.
    repeated FileSlice files = 4;
  }

  message FileSlice {
    // On-disk file name taken from the final path component
    // in the container footer, such as:
    // "primary.bin", "searchable.tf".
    string disk_name = 1;
    uint32 size = 2;
    // Which part this slice belongs to, indexing Init.parts.
    uint32 part_index = 3;
  }

  message Completion {
    uint64 total_bytes = 1;
    uint32 total_chunks = 2;
  }
}

message ImportPartsResponse {
  // The copies the client asked for, so the caller can detect a shortfall
  // caused by LocateAll deduplication.
  uint32 requested_copies = 1;
  repeated TargetResult targets = 2;

  message TargetResult {
    string node = 1;
    bool success = 2;
    string error = 3;
  }
}
```

Duplicate-delivery validation must be **split by `SyncKind`**. `(id, part_type)` uniqueness can only be enforced on `PART` streams: index streams carry fixed zero values with `id = 0` and an empty `part_type`, so applying the same rule there — or trying to tell them apart by `part_type` — triggers `logger.Panicf` on the receiver.

The upstream `id` is a **framing signal, not an identifier**. The receiver always allocates its own part ID with `atomic.AddUint64(&tsTable.curPartID, 1)`, and `fillFromSyncContext` copies six scalars but deliberately not the ID. Its only real use is the receiver's `createNewContext := session.partCtx == nil || session.partCtx.ID != partInfo.Id` check, which decides where one part ends and the next begins. Mixing two part IDs in one stream therefore causes a split plus an orphan deletion (`MustRMAll`), not a silent merge.

#### 5.2.2 Transfer flow

| # | bydbctl → liaison (import/v1) | liaison → data-* (cluster.v1) | Liaison behavior |
|---|---|---|---|
| ⓪ | Read the manifest and target schema/topology | — | — |
| ① | Read the footer of the selected `.bnc` files and split into streams by part | — | — |
| ② | `Init{catalog, group, shard_id, stage, kind, segment_version, parts{…}}` | — | Validate, resolve the target stage, call `LocateAll(group, shard, copies)`, open `SyncPart` streams |
| ③ | `Chunk{chunk_index=0}` | `SyncPartRequest{chunk_index=0, metadata=SyncMetadata{…}, …}` | Build `SyncMetadata`, fill `session_id` and `version_info` |
| ④ | `Chunk` frames | `SyncPartRequest` frames × N | Forward to all N target nodes |
| ⑤ | `Completion` | `SyncPartRequest{completion=SyncCompletion{…}}` | Forward |
| ⑥ | `ImportPartsResponse{requested_copies, targets[]}` | Final responses from all streams | Wait for all N results before returning |

The fan-out must be **concurrent with independent per-target reporting**: one failed target must not cut off the others, and every target reports its own result. Do not copy the fan-out shape of `banyand/stream/syncer.go: syncPartsToNodesHelper` — it is a sequential loop with a fail-fast `return`, the opposite of what is needed. `banyand/liaison/grpc/property.go: replaceProperty` is closer in spirit (concurrent publish, wait for all futures, partial-success verdict), but it goes through `bus.Publish` rather than a gRPC stream.

Liaison memory stays flat only under a **per-frame synchronous barrier** — send one frame to all N targets, wait for all N acks, then read the next. An asynchronous or buffered fan-out makes memory grow with the speed gap between the fastest and slowest target.

#### 5.2.3 Build the import plan

This step reads only `manifest.json` and the target cluster's topology; it opens no BNC files. It decides which segments qualify for fast push, and which source node to select for each segment when several hold copies (§6).

#### 5.2.4 Read and split BNC files

```text
nodes/data-1/measure/sw_metric/seg-20260907/
  metadata            ← Raw file, not a container
  sidx-001.bnc        ← Contains seg-20260907/sidx/**
  shard-0-001.bnc     ← Contains shard-0/**
  shard-1-001.bnc     ← Contains shard-1/**
```

| Artifact file | Container | Source data | Generated streams |
|---|---|---|---|
| `metadata` | No | `seg-XXX/metadata`, holding `{Version, EndTime}` | None — not sent. The receiver generates it locally when creating the segment |
| `sidx-NNN.bnc` | Yes | `seg-XXX/sidx/**`, a Bluge inverted directory | `SERIES_INDEX` stream, one per shard in `sidxCoversShards` |
| `shard-N-NNN.bnc` | Yes | `shard-N/**`, `<016x>.snp`; also `idx/`, and for trace `sidx/<index-name>/<016x>/` | `PART` and `ELEMENT_INDEX` streams |

Pushing runs in three rounds with a **global barrier** between them: ① segment-level series index, ② core parts, ③ element index. The barrier cannot be per-subtree, because the ordering dependency is per-shard and the same shard routinely appears under several source subtrees.

#### 5.2.5 Data transfer

Segment-level sidx files go first.

```text
Init{
  catalog=MEASURE,
  group="sw_metric",
  shard_id=0,
  stage="hot",
  kind=SERIES_INDEX,
  segment_version="1.5.0",
  parts=[{
    id=0,
    part_type="",                    ← fixed zero values for index streams
    compressed_size_bytes=0,
    uncompressed_size_bytes=0,
    total_count=0,
    blocks_count=0,
    min_timestamp=1757203200000000000,
    max_timestamp=1757289599999999999,
    min_key=0,
    max_key=0
  }]
}

Footer of sidx-001.bnc, relative to the segment sidx/ directory:

  000000000012.seg     offset=0          size=4_194_304
  000000000013.seg     offset=4_194_304  size=1_048_576

Chunk{
  idx=0,
  data=<1MiB>,
  files=[
    {disk_name="000000000012.seg", size=1048576}
  ]
}

Chunk{ idx=1, … }

... one frame per 1 MiB ...

Completion{
  total_bytes=…,
  total_chunks=…
}
```

The segment-level sidx is one file on disk covering every shard the node holds, but the downstream `SyncMetadata.shard_id` is a single value. The client therefore opens one stream per shard in `sidxCoversShards`, sending the same bytes several times to different shards. This is safe in volume because the receiving series-index store enables deduplication — but that is **discarding, not merging**, and the winner is the first arrival.

Shard-level data follows this pattern:

```text
Init{
  catalog=MEASURE,
  group="sw_metric",
  shard_id=0,
  stage="hot",
  kind=PART,
  segment_version="1.5.0",
  parts=[
    {
      id=1,
      part_type="core",
      compressed_size_bytes=47972566,
      uncompressed_size_bytes=198000000,
      total_count=123456,
      blocks_count=892,
      min_timestamp=1757203200000000000,
      max_timestamp=1757289599000000000,
      min_key=0,
      max_key=0
    }
  ]
}

Entries under prefix 0000000000000001/ in shard-0-001.bnc:

  metadata.json     size=214             ← not sent; scalar fields above are parsed from it
  meta.bin          size=1_048_576
  primary.bin       size=8_912_896
  timestamps.bin    size=4_194_304
  searchable.tf     size=33_554_432
  searchable.tfm    size=262_144
  searchable.tff    size=65_536

6 files are transferred, totaling 48,037,888 bytes.

Chunk{ idx=0, data=<1MiB>, files=[{meta.bin, 1048576}] }
Chunk{ idx=1, data=<1MiB>, files=[{primary.bin, 1048576}] }

... primary.bin spans multiple frames ...

Chunk{
  idx=9,
  data=<1MiB>,
  files=[
    {primary.bin, 524288},
    {timestamps.bin, 524288}
  ]
}

... cross-file chunk ...

Chunk{
  idx=45,
  data=<832KiB>,
  files=[
    {searchable.tf, 524288},
    {searchable.tfm, 262144},
    {searchable.tff, 65536}
  ]
}

Completion{
  total_bytes=48037888,
  total_chunks=46
}

→ ImportPartsResponse{
    requested_copies=2,
    targets=[
      {node:"data-a", success:true},
      {node:"data-b", success:true}
    ]
  }
```

Parts are sent in ascending part-ID order, which is also the byte order inside the container. That keeps reading sequential, lets the checkpoint degrade to a single `maxDonePartID` watermark, and keeps the relative ordering of source and target part IDs aligned.

**Trace cannot be split across sessions.** Its core part and its secondary index parts share one locally allocated part ID: `NewPartType` reuses the ID within the same `syncPartContext` (`if s.partID == 0` before allocating). Across two sessions they receive different IDs, the core and the sidx lose each other, and whichever side arrives alone is physically removed by `MustRMAll`.

#### 5.2.6 Liaison handling

On `Init` the liaison resolves the data topic (for example `MEASURE + PART = "measure-part-sync"`), resolves the stage and schema to find the target data nodes, and opens one `SyncPart` stream per target with `NewChunkedSyncRelayClient("data-a").SyncPart(ctx)`.

The relay context must derive from the downstream client stream's context. Deriving it from `context.Background()` leaves zombie upstream streams when the client cancels.

| Time | bydbctl → liaison | liaison → data-a / data-b | Response |
|---|---|---|---|
| t0 | `Chunk{idx=0, data=<1MiB>, crc="3a91c7e2", files=[{meta.bin, 1048576}]}` | `SyncPartRequest{session_id, chunk_index=0, chunk_data=<same 1MiB>, chunk_checksum="3a91c7e2", parts_info=[{id=1, part_type="core", files=[{name:"meta", offset:0, size:1048576}]}], metadata=SyncMetadata{…}, version_info}` | Each node returns `CHUNK_RECEIVED` |
| t1–t8 | `Chunk{idx=1..8, …, files=[{primary.bin, 1048576}]}` | Same structure with increasing `chunk_index`; `files=[{name:"primary", offset:0, size:1048576}]`; no repeated metadata | Each node returns `CHUNK_RECEIVED` |
| t9 | `Chunk{idx=9, …, files=[{primary.bin, 524288}, {timestamps.bin, 524288}]}` | Convert to absolute offsets: `files=[{name:"primary", offset:0, size:524288}, {name:"timestamps", offset:524288, size:524288}]` | Each node returns `CHUNK_RECEIVED` |
| t10–t44 | Full frames containing one file slice | Same structure with one `files` entry | Each node returns `CHUNK_RECEIVED` |
| t45 | `Chunk{idx=45, data=<832KiB>, files=[…3 entries…]}` | `files=[{name:"tf:searchable", offset:0, size:524288}, {name:"tfm:searchable", offset:524288, size:262144}, {name:"tff:searchable", offset:786432, size:65536}]` | Each node returns `CHUNK_RECEIVED` |
| t46 | `Completion{total_bytes=48037888, total_chunks=46}` | `SyncPartRequest{session_id, chunk_index=46, completion=SyncCompletion{total_bytes_sent=48037888, total_parts_sent=1, total_chunks=46}, version_info}`, no `chunk_data` or `parts_info` | Final response |

The liaison stores nothing: it converts one frame at a time and forwards it, which is why it stays memory-flat at line rate. The memory protector is not a backstop here — it only observes memory, and a relay saturating the NIC has a flat memory curve.

### 5.3 CSV data rewrite

For CSV, `bydbctl` reads the files directly, rebuilds the corresponding requests, and redistributes them to the target data nodes.

```protobuf
service ImportService {
  rpc ImportRows(stream ImportRowsRequest) returns (ImportRowsResponse);
}

message ImportRowsRequest {
  oneof frame {
    Init init = 1;
    RowBatch batch = 2;
  }

  message Init {
    common.v1.Catalog catalog = 1;
    string group = 2;
    string name = 3;              // Resource name
    string stage = 4;             // Target stage
  }

  message RowBatch {
    repeated StreamRow stream_rows = 1;
    repeated MeasureRow measure_rows = 2;
    repeated TraceRow trace_rows = 3;
    repeated PropertyRow property_rows = 4;
  }

  message StreamRow {
    stream.v1.ElementValue element = 1;
    uint64 raw_element_id = 2;
  }

  message MeasureRow {
    measure.v1.DataPointValue point = 1;
  }

  message TraceRow {
    trace.v1.SpanValue span = 1;
  }

  message PropertyRow {
    property.v1.Property property = 1;
    int64 delete_time = 2;
  }
}
```

A dedicated import RPC is required rather than reusing the public write API, because the public path regenerates fields the artifact already carries. In particular a measure `version` of `0` is silently replaced with the message ID on the liaison, destroying the deduplication semantics the CSV `_version` column exists to preserve.

| # | bydbctl → liaison | liaison → data-* | Liaison behavior |
|---|---|---|---|
| ⓪ | Read the manifest and target schema/topology | — | — |
| ① | Open `.csv.gz` files and build the column mapping from the schema | — | — |
| ② | `Init{catalog, group, name, stage}` | — | Locate the schema of the target resource |
| ③ | Send `RowBatch{…_rows}` in batches | `InternalWriteRequest` per row | Call `Locate(group, shard, replicaID)` and send internal write requests to each replica |
| ④ | Receive the batch response | — | Aggregate success and failure for the batch |

This channel is **not** idempotent on replay, which is why its checkpoint is recorded at record-cursor granularity (§7.2).

Property rows must be applied with `db.Repair(ctx, id, shardID, property, deleteTime)`. `Update` has no `delete_time` parameter at all — its underlying call hardcodes `0` — so a tombstone imported through `Update` comes back to life. `Delete` searches for an existing document first and is meaningless against a fresh database.

## 6. Multiple shard sources

### 6.1 When it happens

| Cause | Description |
|---|---|
| Replicas | When `replicas > 0`, the same shard naturally exists on multiple nodes |
| Topology drift | After the node count in a stage changes, the same shard number may remain on old nodes |
| Orphan shard | A node is no longer the shard's owner, but the data still remains on disk |

Topology drift has a wider trigger surface than it appears. The mapping is `nodes[(index + replicaID) % len(nodes)]`, where `index` is the position of the `(group, shardID)` pair in the cluster-wide sorted lookup table — **not** the shard ID. Creating a new group whose name sorts early shifts every subsequent entry, and when the shift is not a multiple of the node count, **100%** of the following `(group, shard)` pairs change owners.

All three causes look identical in the manifest — they are simply a second entry under `sources[]`. There is no `replicaId`, no creation time, and no cross-node comparable version. So the design does not classify the cause; it classifies the coverage.

### 6.2 Detection basis

For each part the manifest records `{minTimestamp, maxTimestamp, totalCount}`. **The coverage of one source is the union of the time ranges of all its parts.**

```text
data-1   [00:00,06:00] + [06:00,12:00] + [12:00,23:59]  → [00:00,23:59]  120,000 rows
data-2   [00:00,23:59]                                  → [00:00,23:59]  118,000 rows
```

These two are replicas of the same data despite holding different numbers of parts, because one has already been merged. Therefore **neither the part ID nor the part count can be used for comparison**:

- Part IDs come from each node's local counter. Three replicas receiving the same part were measured to assign it `1`, `4`, and `8`.
- Part counts drift with merge progress. During a merge sweep the Jaccard similarity of two nodes' part sets fell to 0.45 at 50% merged and below 0.3 at 70%.

The decision rests on **time coverage and row count**. The union is computed over closed intervals, and adjacent intervals must be merged as well as overlapping ones (`next.Min <= cur.Max + 1`); otherwise every pair of parts leaves a 1 ns phantom hole and real holes vanish into the noise.

Sources with no `parts[]` or `totalCount == 0` are dropped before the union is computed. Leaving them in pollutes the coverage with `[0,0]` and adds a phantom source that makes the unit look disjoint.

### 6.3 Rules

Sort sources by coverage duration, descending, then compare each against the already-selected ones.

| Condition | Meaning | Action |
|---|---|---|
| Time ranges do not overlap, or only partially overlap | The source holds time ranges nobody else covers | Push |
| Time range fully contained, row-count difference large (relative > 5% **and** absolute > 64) | Volume does not match; may be an orphan left by topology drift | Push + warning |
| Time range fully contained, row-count difference small | Treat as a replica | Skip |

Both thresholds must be exceeded to call the difference large. Using only 5% would make a 100-row shard differ significantly on 6 rows; using only 64 rows would make a 120,000-row shard differ significantly on 65 rows, about 0.05%.

"Fully contained" means contained by **one single** already-selected source. A source covered only by the *union* of several selected sources has no counterpart to compare row counts against, so it falls into the first row: push with a warning.

Example, with `data-1 [00:00,23:59]` / 120,000 rows already selected:

| Source | Range | Rows | Decision |
|---|---|---|---|
| `data-2 [00:00,23:59]` | Fully contained | 118,000, difference 1.67% | Skip; replica |
| `data-3 [08:00,10:00]` | Fully contained | 5,000, difference 95.8% | Push + warning; possible orphan |
| `data-4 [00:00,06:00] + [18:00,next day 04:00]` | Extends beyond 23:59 | — | Push; new time coverage |

**measure does not apply the row-count condition** — a fully contained source is skipped outright. Its background merge collapses multiple versions of the same `(seriesID, timestamp)`, so the legitimate row-count skew is `(K−1)/K` for a source pushed K times: 66.7% at K=3. Any threshold below 100% is broken by the entirely normal "pushed a few times, not yet compacted" case. The cost is that a measure orphan whose time range happens to be contained is skipped silently.

Every decision and its reason is printed in the dry-run `MULTI-SRC` detail, because this is the only place in the import that can decide to *send less data*. `--strict-coverage` promotes a multi-source unit to an error and writes nothing; `--all-sources` skips the judgement and pushes everything. `--all-sources` must disable both the shard-level and the segment-level judgement — the escape hatch is one switch, not two. Including the segment-level sidx changes the stream count by only 1.5–2% but raises bytes by roughly 24% at `replicas=1` and 35% at `replicas=2`.

### 6.4 Multiple-source push behavior

| Catalog | Import behavior | Query | Disk |
|---|---|---|---|
| measure | Each part from every selected source becomes its own stream; no merging | Correct; the latest version is selected | Grows with the number of copies, and is **not** merged back |
| stream | Same as measure, plus the complete element index from every source | Correct; deduplicated by element ID | **Permanently** grows with the number of copies |
| trace | Same as above, but each core part and its secondary indexes must be sent together | **Duplicate spans are visible** | Grows with the number of copies |
| property | The whole shard directory is sent and every record from every source is written | Correct; the union is preserved | No amplification |
| index-mode measure | Only segment-level indexes exist; deduplicate by version before writing once | Correct; the highest version is selected | No amplification |

- **trace is the only catalog whose query result is wrong**, and it does not self-heal. That is the whole reason §6.3 has to be accurate; for every other catalog a misjudgement is absorbed by the read or write path.
- **The element index cannot be reduced to a covering set.** Each source's `idx/` may be complementary rather than duplicated, and a dropped index entry never reappears. It was measured at exactly ×3.000 for three sources (4,000 → 12,000 documents, 765 KB → 2,295 KB), and Bluge's merge does not reclaim duplicates because the element index does not enable deduplication. Extrapolated to a 167-segment group with a 200 MB per-shard baseline, that is 65 GB at `replicas=1` and 98 GB at `replicas=2`. Provision `idx/` at `(replicas + 1)` times.
- **The segment-level series index is the opposite**: only the minimum covering set is pushed, because the receiver deduplicates and over-pushing costs no disk. But the winner is the first arrival, and the discarded copy's index tag values become unqueryable — `Lookup(entity)` still matches, because the Bluge document `_id` is the entity value itself, while a filter on a non-entity indexed tag returns nothing for the losing value.
- **measure's storage does not converge.** The production default merge policy requires a write-amplification score of `≥ max(maxParts/2, minMergeMultiplier) = 4`, and equally sized parts score exactly their own count — three equal parts score 3 and are never merged. Queries are correct immediately regardless, because the read path deduplicates by version before any merge runs.

## 7. Resume

Both directions must survive an interruption. Export is the simpler half: exported files are organized by shard, so it resumes at shard granularity.

Import is more involved.

| Format | Data type | Checkpoint level | Resume behavior | Re-importing already-successful data |
|---|---|---|---|---|
| native | stream | node → segment → shard → part → target node | Safe | Re-pushed parts keep their element IDs and both query paths deduplicate by them; storage grows |
| native | measure | Same as above | Safe | The read path keeps the larger version by `(seriesID, timestamp)`, so duplicates are invisible in queries; storage grows and does not merge back |
| native | trace | Same as above | Safe | No deduplication; duplicates are visible |
| native | series index | node → segment → shard | Safe | First write wins; later writes are discarded silently |
| native | element index | node → segment → shard | Safe | **No deduplication**; documents and disk grow linearly and are never reclaimed |
| native | property | node → group → shard | Safe | Idempotent |
| CSV | stream / trace | node → segment → shard → split file → record index | Continue from the next record index | stream behaves as native; trace has no deduplication |
| CSV | measure | Same as above | Same as above | The read path keeps the larger version by `(seriesID, timestamp)`, so duplicates are invisible in queries |
| CSV | index-mode measure | Same as above | Safe | Idempotent |
| CSV | property | Same as above | Safe | Idempotent |

The target side cannot deduplicate a re-imported part: the same payload imported twice produces two directories with different part IDs, and `partMetadata` carries no hash, checksum, or fingerprint field. Idempotency therefore rests entirely on the client checkpoint.

### 7.1 Export progress file

Stored by default in `<output>/.export-progress.json`. It holds the `session_id`, the export plan, and the current progress.

```json
{
  "kind": "export-progress",
  "artifactVersion": 1,
  "format": "native",

  "session": {
    "id": "9f2a…",
    "expiresAt": "2026-09-19T18:00:00Z"
  },

  "startedAt": "…",
  "updatedAt": "…",
  "outputDir": "./backup-20260914",

  "selectorsDigest": "…",
  "sourceNodesDigest": "…",

  "progress": {
    "totalUnits": 1440,
    "doneUnits": 863,
    "totalBytes": 903641530368,
    "doneBytes": 541200000000
  },

  "plan": [
    {
      "node": "data-1",
      "units": ["measure/sw_metric/20260907/0"]
    }
  ],

  "inflight": {
    "data-1": {"unit": "measure/sw_metric/20260907/0", "startedAt": "…"},
    "data-2": {"unit": "stream/sw_record/20260907/1", "startedAt": "…"}
  },

  "done": [
    {
      "unit": "measure/sw_metric/20260907/0",
      "node": "data-1",
      "files": [
        {
          "path": "nodes/data-1/measure/sw_metric/seg-20260907/shard-0-001.bnc",
          "bytes": 402653184,
          "crc32": "0x9f2a1b34",
          "entryCount": 47,
          "rowCounts": {"service_cpm_minute": 123456}
        }
      ]
    }
  ]
}
```

Because several nodes export in parallel, a dedicated goroutine persists progress so concurrent workers never conflict.

| Time | Action | Persist |
|---|---|---|
| Plan starts | Record the session | Write immediately on receipt |
| Worker starts a unit | Set `inflight[node] = {unit, startedAt}` | Write |
| Each `file_finished` | Validate bytes/crc32 → fsync → rename → keep the entry in worker memory | No |
| `unit_finished` | Remove `inflight[node]`, append to `done`, update `progress` | Write |
| All units complete | Delete the progress file **after** `manifest.json` is written | — |

That final ordering is a discipline, not a detail: write `manifest.json` first, then delete the progress file. Reversing it leaves a window in which the artifact has neither a manifest nor a resume record.

Resuming an export reads the progress file and checks it against the current plan, renews the session through `Plan`, deletes the unfinished files recorded in `inflight`, and continues.

### 7.2 Import progress file

Stored by default in `<input>/.import-progress.json`. It holds target cluster information and current progress.

```json
{
  "kind": "import-progress",
  "artifactVersion": 1,
  "format": "csv",

  "manifestDigest": "<digest of the raw manifest.json bytes>",
  "artifactFilesDigest": "<digest of the (path, bytes, crc32) tuples in manifest.files[]>",
  "targetCluster": "<digest of the target node-name set; informational only>",

  "stageMap": {"hot": "warm"},

  "checkpointEveryBatches": 64,

  "exportedAt": "…",
  "updatedAt": "…",

  "progress": {"totalUnits": 1440, "doneUnits": 863},

  "plan": [
    {
      "key": "data-1/measure/sw_metric/20260907/0",
      "channel": "native",
      "stage": "warm",
      "copies": 2,
      "targets": ["data-a", "data-b"]
    }
  ],

  "inflight": {
    "data-1": {
      "key": "data-1/measure/sw_metric/20260907/0",
      "kind": "PART",
      "partID": 7,
      "sidxNames": [],
      "nodes": ["data-a", "data-b"],
      "isRetry": false,
      "startedAt": "…",
      "probe": {
        "totalCount": 123456,
        "minTimestamp": 1757203200000000000,
        "maxTimestamp": 1757289599000000000
      }
    },
    "data-2": {
      "key": "data-2/stream/sw_record/20260907/1",
      "kind": "ROWS",
      "slice": "shard-1-00003.csv.gz",
      "fromRecord": 128000,
      "records": 64000,
      "startedAt": "…"
    }
  },

  "done": [
    {
      "key": "data-1/measure/sw_metric/20260907/0",
      "parts": {
        "1": {"nodes": ["data-a", "data-b"], "missing": []},
        "2": {"nodes": ["data-a"], "missing": ["data-b"]}
      },
      "blobs": {
        "SERIES_INDEX": {"nodes": ["data-a", "data-b"], "missing": []},
        "ELEMENT_INDEX": {"nodes": [], "missing": ["data-a", "data-b"]}
      },
      "slices": {
        "shard-0-00001.csv.gz": "EOF",
        "shard-0-00002.csv.gz": 192000
      }
    }
  ]
}
```

The plan key is prefixed with the source node. An earlier draft used a scalar `source` field, which cannot express the case where the same `(segment, shard)` must be pushed from two sources after drift — the two records' keys would collide.

Native import records progress per part, tracking successful and missing target nodes separately. Native indexes and property record the whole blob, with no part level. CSV records a cursor per split file, where `EOF` means the file is fully processed.

| Time | Action | Persist |
|---|---|---|
| Native: before opening a stream | Set `inflight[worker] = {unit, kind, partID, nodes, isRetry}` | Write |
| Native: after the response | Clear `inflight[worker]`; add successful nodes to `done[].nodes`, failed ones to `missing` | Write |
| CSV: before a checkpoint range | Set `inflight[worker] = {unit, slice, fromRecord, records}` | Write |
| CSV: after each successful batch in a range | Update the in-memory cursor only | No forced disk write |
| CSV: when a checkpoint range completes | Clear `inflight[worker]`, write the cursor to `done[].slices[slice]` | Write |
| When a unit completes | Increment `progress.doneUnits` | Write |
| When all imports complete | Leave the progress file unchanged | — |

Resuming an import reads the progress file and checks it against the current plan, re-verifies the artifact against the target cluster as in the normal workflow, finishes the entries recorded in `inflight`, and continues with the rest.
