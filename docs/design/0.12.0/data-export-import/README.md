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
  rateLimit: 0                        # Per-liaison byte rate limit (bytes/s), 0 = unlimited, e.g. "800MiB/s".

export:                               # Export only: scope and options
  output: ./backup-20260708           # Export directory; CLI --output overrides this. Error if both are missing
  format: csv                         # native | csv
  compress: gzip                      # none | gzip; default gzip for CSV, none for native (native accepts only none)
  maxFileSize: 1GiB                   # Shared by both formats: soft limit per output file, measured on bytes
                                      #   on disk after compression; 0 = unlimited
  includeSchema: true
  parallelism: max                    # Data nodes exported concurrently; max = the selected node count,
                                      #   clamped to min(N, nodes)
  nodeRateLimit: 0                    # per-node byte rate limit, 0 = unlimited, e.g. "50MiB/s"
  nodeRowRateLimit: 0                 # CSV only: per-node row rate limit (rows/s), 0 = unlimited
  checkpoint: ""                      # Empty = <output>/.export-progress.json
  selectors:                          # Empty or omitted = all catalogs and groups in the cluster
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
                                 # match = <kind>/<group>[/<name>]; kind is one of group, stream,
                                 #   measure, trace, property, indexRule, indexRuleBinding,
                                 #   topNAggregation (case-sensitive; group takes two segments only)
    - match: group/sw_metric     # Example: allow topology differences only for this group
      policy: ignore
  parallelism: max               # Source-node subtrees (nodes/<node-dir>/) processed concurrently;
                                 #   max = the artifact's source-node count, one unit in flight per worker
  writeRateLimit: 0              # Row rate limit (rows/s) shared by CSV row replay and property import;
                                 #   0 = unlimited
  partRateLimit: 0               # Native only: total outbound byte rate limit, e.g. "50MiB/s"; 0 = unlimited
  partChunkSize: 1MiB            # Native only: bytes per ImportParts frame; clamped to [64KiB, 4MiB]
  writeBatchSize: 1000           # CSV only: rows per ImportRows batch; clamped to [1, 10000]
  checkpointEveryBatches: 64     # CSV only: batches between progress-file fsyncs
  resumeDuplicationPolicy:       # ONLY on a resumed run: what to do with a unit the
    default: ""                  # previous run never got confirmation for.
                                 #   block | warn | push | skip
                                 #   empty = the per-catalog default
    rules:                       # match = <catalog>[/<group>]
    - match: trace               # Example: stop rather than risk duplicate spans
      policy: block
  checkpoint: ""                 # Empty = <artifact>/.import-progress.json
  verify: true                   # Reconcile counts after the import; CLI --verify overrides
  workDir: /data/import-work     # Local scratch directory, used only to unpack .tar/.tar.gz artifacts
```

**An import reproduces what the export captured**, so a policy exists only where that leaves a real choice — and it does so in exactly one place: a resumed run, and there only for payloads carrying no idempotency key. A native `kind=PART` replay is recognised by `import_op_id` and writes nothing; only the element index and the CSV stream / trace payloads can duplicate on re-push or lose on skip. Everything else is pushed as-is. Overlapping sources on a first run are reported as a WARN; `--strict-coverage` turns that into an error.

`workDir` is **not** a rebuild workspace. Neither import channel reconstructs parts locally: the native channel relays opaque bytes and the CSV channel parses rows into write requests. Unpacking an archived artifact is the only local scratch requirement.

## 3. Dry run

Dry run walks the real workflow without importing or exporting anything. It is a strict prefix of the real run with two deliberate exceptions: `Plan` goes out with `create_session=false`, so no snapshot is created; and schemas are loaded into memory but never written to `schema/`.

### 3.1 Export dry run

1. **Connection check** — connect to one liaison and call `GetCurrentNode` to confirm the `LIAISON` role, then `GetClusterState` for the other nodes' IDs, labels, versions, and timezones.
2. **Fetch schema** — call the Schema gRPC service and load schemas into memory.
3. **Plan request** — send one `Plan{create_session=false}` request to that liaison, which filters all `DATA`-role nodes, forwards the request, and relays each response frame as it arrives. A dry run creates **no session and no snapshot**, so it inventories the online catalog and its numbers have a shelf life; the real export inventories a frozen snapshot.
4. **Client aggregation** — aggregate and print what every node returned.

```text
NODES   The cluster reports 3 data nodes [data-1 data-2 data-3]; --nodes listed 2 → ⚠ data-3 is missing, its data will not be exported

NODE    CATALOG   GROUP       STAGE   SEGMENTS  SHARDS  PARTS  EST-ROWS   EST-SIZE(comp/raw)  TIME-RANGE
data-1  measure   sw_metric   hot     7         2       34     118.2M     18.3GiB / 61.2GiB   06-30 ~ 07-07
data-1  stream    sw_record   hot     7         2       61     342.7M     96.1GiB / 288GiB    06-30 ~ 07-07
data-2  measure   sw_metric   warm    28        4       52     1.4G       201.5GiB / 702GiB   05-30 ~ 06-30
data-2  property  sw_prop     -       -         1       -      12.3K      4.1MiB / 12.0MiB    -
```

The table lists only data that exists; it never derives "which shards ought to be here" from the schema's `shardNum`, because shards and segments are created lazily and comparing against an expected set produces large-scale false reports. The `NODES` line is therefore the only completeness check — it diffs the data-node set from `GetClusterState` against `connection.nodes`, which cannot produce a false positive. It is a WARN by default and an error under `--strict-coverage`.

### 3.2 Import dry run

Steps 1–7 of [the import workflow](#51-workflow) plus the channel decision, then exit — the same code path, stopping before step 8, the schema write, so **not one byte of the target changes**. Exit code `0` means the run would proceed and `2` that a gate would reject it, which makes it directly usable as a CI gate. `--input` is required here, unlike the export dry run, because the artifact has to be read.

1. **Connection check** — poll `connection.nodes` until one liaison answers, confirm the `LIAISON` role, then `GetClusterState` once for every target data node.
2. **Artifact check** — validate `manifest.json`, the artifact version, and every file's size and CRC32, then derive the multiple-source decisions from `manifest.units`.
3. **Schema comparison** — fetch the target schema and compare it with the artifact's. Read-only: because no schema is created, the later gates run against a *hypothetical* topology — existing groups use the target's stages, groups this run would create use the artifact's.
4. **Version gate** — the unit's segment version must be in the target's `compatible_file_format_version`.
5. **Timezone gate** (per unit) — compare IANA names, not current UTC offsets. A no-op for CSV.
6. **TTL gate** (per unit) — the effective TTL is cumulative: `group.ttl` plus every `stage.ttl` up to that stage, as `pub.ResolveStageResourceOpts` computes it. The test is whether the whole target segment has expired (`segmentEnd < now − effectiveTTL`), projected onto the target's segment grid and timezone — not the unit's `minTimestamp`. Stages with retention disabled are exempt, as is property.
7. **Topology gate** (per unit) — the target `segmentInterval` and **both** `shardNum` values — the mapped stage's and the group-level `ResourceOpts.ShardNum` — must equal the artifact's. Comparing only the stage-level one lets a unit through and then fails in the liaison's routing with `unknown shard`; comparing only the group-level one gets it silently dropped by `loadShards` on the next restart.
8. **Channel decision and guards** — pick native / CSV / property / rejected per unit, then apply the schema guards.

```text
TARGET   data-1:17912 data-2:17912  (from --plan ./restore.yaml) | 0.10.1 | supported segment formats [1.4.0 1.5.0] | api 0.10 | tz Asia/Shanghai
         liaison  liaison-1:17912  (ROLE_LIAISON, active)

SOURCE   ./backup-20260707  native  artifactVersion=1  exported at 2026-07-07T12:00:00Z | 0.9.0 | tz Asia/Shanghai

BYDBCTL  0.10.1 | segment format 1.5.0 | api 0.10

CATALOG   GROUP       STAGE  SEGMENTS  SHARDS(src→dst)  ROWS     SIZE(comp/raw)      SCHEMA    CHANNEL           GATE   TIME-RANGE
stream    sw_record   hot     7        2→2              342.7M   96.1GiB / 288GiB    CONFLICT  ① part-push       BLOCK  06-30 ~ 07-07
measure   sw_metric   hot     7        2→4              118.2M   18.3GiB / 61.2GiB   IGNORE    REJECT(topology)  BLOCK  06-30 ~ 07-07
measure   sw_metric   warm   28        4→4                1.4G   201.5GiB / 702GiB   OK        ① part-push       TTL!   05-30 ~ 06-30
measure   sw_traffic  hot     7        -                  0.4M   12.3MiB / 41.0MiB   CREATE    ① sidx only       ok     06-30 ~ 07-07
trace     sw_trace    hot     7        4→2               12.1M   4.1GiB / 14.2GiB    CONFLICT  REJECT(topology)  BLOCK  06-30 ~ 07-07
property  sw_prop     -       -        1→1               12.3K   4.1MiB / 12.0MiB    CREATE    property/repair   ok     -
TOTAL                        56                          1.87G   320.0GiB / 1.04TiB                                     05-30 ~ 07-07
         of which BLOCKED: 3 rows / 42 segments / 473.0M rows
```

There is **no `TARGET-NODES` column**: the liaison picks target nodes at execution time from the live routing table, and `LocateAll` is not on the dry run's read-only call list. Never compute them on the client as `nodes[(shardID + replicaID) % len(nodes)]` either — the real `roundRobinSelector.Pick` indexes a globally sorted lookup table rather than the shard ID, so that formula breaks as soon as there is more than one group.

All three verdict columns take closed value sets:

- `SCHEMA` — `CREATE` / `OK` / `MISSING` / `CONFLICT` / `IGNORE` / `OVERWRITE` / `INVALID` / `SKIP`. `SKIP` covers only resources filtered out before comparison and never affects `GATE`.
- `CHANNEL` — `① part-push` / `① sidx only` (index-mode measure) / `② row replay` / `property/repair` / `REJECT(<gate>)`. **`② row replay` can only appear for a CSV artifact**: a native artifact has no fallback across a topology change, so it renders `REJECT(topology)`.
- `GATE` — `ok` / `TTL!` / `BLOCK`. Any row whose `CHANNEL` is `REJECT(...)` is `BLOCK`, as is any row whose `SCHEMA` is `CONFLICT` or `INVALID`, whatever its channel; `TTL!` marks a row that clears every other gate and is stopped by the TTL gate alone, which `--force-ttl` overrides.

`TOTAL` counts **every** row, blocked ones included, and `SEGMENTS` / `ROWS` / `SIZE` all use that same basis; the blocked subtotal is reported on its own line.

`measure/<group>/_top_n_result` is filtered out before comparison and reported on its own `SKIP` line. The target creates it automatically from its own hardcoded schema, so including it only produces conflicts the user can neither fix nor should fix.

## 4. Export

### 4.1 Client

1. **Connection check** — select an available liaison and call `GetClusterState` for the topology.
2. **Fetch schema** — call the Schema gRPC service and load schemas into memory.
3. **Plan request** — send one `Plan` request; the liaison fans it out to all `DATA`-role nodes.
4. **Aggregate** — group results by node to decide what to export from each.
5. **Execute export** — connect to the available liaisons and send export requests.
6. **Write manifest** — write the summary file to the export root.

The export side performs **no source selection**. Every source node's copy of a unit is carried away verbatim into its own `nodes/<node-dir>/` subtree, and all multi-source judgement happens on the import side ([Multiple shard sources](#6-multiple-shard-sources)). This is deliberate: a wrong judgement on the export side loses bytes permanently, while a wrong judgement on the import side costs a re-run.

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
| `shard-N/sidx/<rule>/<016x>/` | **Trace secondary index parts** — real parts, one directory per index rule, each with `manifest.json` | shard-level `shard-N-NNN.bnc` |

#### 4.2.1 BNC file format

```text
[file bytes...][footer protobuf][footer_len uint32][MAGIC "BNCA"]
```

The footer only records which byte range belongs to which file. The magic is `BNC` plus a single format-version letter, so a future incompatible layout becomes `BNCB` without needing a separate version field.

There are only three gRPC frame types:

```text
file_started{ relative_path, shard_id, segment_level, shard_index, seq }
                                           → Client creates <path>.tmp
chunk{ content } × N                       → Client appends data
file_finished{ bytes, crc32, entry_count, row_counts }
                                           → Client validates → fsync → rename
```

### 4.3 CSV export

Exporting in CSV format:

1. **Create snapshot** for the required data types.
2. **Traverse segments** of every group.
3. **Iterate shards** — use the dump readers to read rows shard by shard, encode as CSV, gzip on the fly, and emit one chunk frame per 1 MiB of **compressed** bytes. Both rate limits apply here — see below.

**Two models never reach part iteration, and missing either produces a header-only file with exit code 0 rather than an error.** Index-mode measure has zero parts and no shard directories — the segment-level series index *is* the payload — so its rows come from `readIndexModeDocs`, one CSV row per document, with the three fixed columns taken from the reserved stored fields `_id` / `_timestamp` / `_version` (`_id` holds `EntityValues`, whose first element is the resource name). Property has no parts either.
4. **Complete export** — finish the gRPC stream.

The existing dump readers `banyand/internal/dump/{stream,measure,trace}` cover the part-iteration shape, but **two of their entry points open the series index in write mode and cannot be used against a snapshot as they stand**: `dump.NewIndexResolver` and `dump.LoadSegmentSeriesMap` both call `inverted.NewStore`, which ends in `bluge.OpenWriter` and creates a `bluge.pid` lock inside the directory it opens. The snapshot machinery deliberately keeps that file out — `includeInClosedSnapshot` excludes it, and the open-segment path copies the index through `reader.Backup` — so opening a snapshot's `sidx/` this way writes the lock back in and streams it into the artifact. CSV export therefore needs one new read-only construction in the dump package, backed by `pkg/index/inverted`'s `ReadOnlyWalkDocuments` / `ReadOnlyDocCount`, neither of which takes the exclusive lock.

**Trace is a real gap, not a call-site difference.** `dump/trace`'s `Row` has no `EntityValues` and its reader has no `SetIndexResolver` — only stream and measure do — so the `_name` column has no source in the part. Its `Row.SeriesID` does not help: it is a hash over the span's own tags, and the reader's own comment says it must not be used to look up on-disk series metadata. Trace's `_name` has to be recovered by matching the row's tag-name set against the embedded schemas, and when two `Trace` resources in one group have identical tag-name sets the signature is ambiguous — **reject CSV export for that group** rather than guess, and direct the operator to the native channel, which needs no `_name`.

`property` has no dump reader, and `SeriesIterator` is **not** the answer: it lives on the writable store, so it reintroduces the same lock. Property rows come from `inverted.ReadOnlyWalkDocuments` over each `property/data/<group>/shard-N` directory, restoring each `Property` from the `_source` stored field. Do not copy `banyand/cmd/dump/property.go`: it opens the shard with `NewStore` twice and accumulates the whole shard into one in-memory slice before emitting a row.

**Rate limiting has two knobs, and CSV honours both.** They throttle different resources, so neither subsumes the other and whichever binds first wins:

| Knob | Plan field | Protobuf field | Measured | native | CSV |
|---|---|---|---|---|---|
| Bytes | `export.nodeRateLimit` | `RateLimit.bytes_per_second` | On the wire, so **after** gzip for CSV | yes | yes |
| Rows | `export.nodeRowRateLimit` | `RateLimit.rows_per_second` | At the dump reader, before encoding | no — native moves opaque bytes and has no row concept | yes |

The byte knob protects the liaison and the network; the row knob protects the data node, whose cost for CSV is row iteration rather than transfer. A CSV export that sets only `nodeRateLimit` can still saturate a data node's CPU on a highly compressible group, which is why the row knob exists as well as, not instead of, the byte knob.

#### 4.3.1 Value encoding

| Type | Encoding |
|---|---|
| STRING | Written via `encoding/csv`; a value containing `\r` is downgraded to `\B` + base64 ([the escape algorithm](#432-escape-algorithm)) |
| INT | Decimal |
| FLOAT (measure field) | `strconv.FormatFloat(v, 'f', -1, 64)`. Precision **must** be `-1`, or the round trip loses precision |
| DATA_BINARY / span payload | `base64.StdEncoding`, standard alphabet, with padding |
| STRING_ARRAY | JSON array string |
| INT_ARRAY | Semicolon-separated decimal int64 values; an empty array is an empty string |
| All timestamp columns | Decimal int64 UnixNano; trace-only for TIMESTAMP tags |
| null | The `\N` sentinel. Literals are kept distinct by the leading-backslash rule in [the escape algorithm](#432-escape-algorithm) |

> **NaN and ±Inf cannot currently be exported at all.** `FormatFloat` renders them as `NaN` / `+Inf` / `-Inf` and `ParseFloat` accepts those literals back, so the encoding is not the problem. The problem is upstream: `encodeFloat64Column` cannot encode NaN/±Inf losslessly and falls back to `encodeDefault`, which emits a dictionary block (type 10) beneath an outer plain marker (type 9). The engine's own `decodeFloat64Column` delegates that case to `decodeDefault` and reads it back; the dump reader strips the outer 9 and hands the inner bytes straight to the decoder, so it fails. This has to be settled before the CSV channel ships — either declare it unsupported, or extend the dump reader.

`'f'` is chosen over `'g'` because `'g'` renders `1000000` as `1e+06`, and six of the nine non-test call sites in the repository already use `'f', -1, 64`. Both round-trip bit-exactly. The cost of `'f'` is length: `1e300` becomes a 301-character field, so the export side must not assume an upper bound on field width.

#### 4.3.2 Escape algorithm

Two sentinels share one cell: `\N` for null and `\B<base64>` for a value downgraded because it contains `\r`. They must therefore share **one** escaping rule, or the mapping stops being injective.

An earlier draft used two rules of different granularity — "a literal `\N` is written `\\N`" (whole-value equality) plus "a value starting with `\B` is written `\\B`" (prefix match) — and neither handled a value that already starts with a backslash. That collapses real values: the literal `\\N` and the literal `\N` both come out as `\\N`, and a CRLF-bearing string and the literal `\BDQo=` both come out as `\BDQo=`. The reader cannot tell them apart, so one of them is silently corrupted.

**The rule: a single leading-backslash escape, decided on the first character only.** No whole-value equality, no pairing, no recursion.

Writing, in this order:

1. value is null → emit `\N`;
2. value is a STRING containing `\r` → emit `\B` + `base64.StdEncoding(value)`;
3. value starts with a backslash → emit one extra leading backslash, then the value verbatim;
4. otherwise → emit the value verbatim.

Reading, in this order — the four branches are mutually exclusive:

1. the cell equals `\N` → null;
2. the cell starts with `\\` → drop **exactly one** leading backslash; the rest is the literal;
3. the cell starts with `\B` → base64-decode the remainder;
4. otherwise → the cell is the literal.

The three written prefixes `\N` / `\B…` / `\\…` are pairwise disjoint and the writer's image covers every reader branch, so the mapping is closed and injective.

Round-trip cases that must be in the test suite, each asserted byte-for-byte:

| Original value | On disk | Read back |
|---|---|---|
| null | `\N` | null |
| `\N` | `\\N` | `\N` |
| `\\N` | `\\\\N` | `\\N` |
| `\Bx` | `\\Bx` | `\Bx` |
| `\\Bx` | `\\\\Bx` | `\\Bx` |
| `a\r\nb` | `\BYQ0KYg==` | `a\r\nb` |
| `\BDQo=` (literal) | `\\BDQo=` | `\BDQo=` |

> **Why `\r` needs the downgrade at all**: `encoding/csv`'s reader converts every `\r\n` in its input to a plain `\n`, **regardless of whether it sits inside quotes**. CRLF is routine in log bodies and stack traces, and the CRC32 is computed over the compressed bytes on disk, so it cannot catch this kind of logical rewrite. The base64 alphabet contains neither CR nor LF, so the downgrade sidesteps it by construction.
>
> STRING_ARRAY does **not** participate in the downgrade — it is already a JSON array string, and JSON escapes CR itself.

#### 4.3.3 Columns

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
sw_service_log,73281937461,1783419300123000000,af1c...beef,c2VydmljZS1h,1,ZGF0YQ==
```

**measure** — `_version` is preserved to maintain deduplication semantics.

```csv
_name,_timestamp,_version,default.entity_id,default.scope,_field.total,_field.value,_field.percentile
service_cpm_minute,1783419300000000000,1783382401000000,c2VydmljZS1h,SERVICE,\N,87,\N
service_resp_time,1783419300000000000,1783382401000000,c2VydmljZS1h,SERVICE,1024,\N,W1BMQUNFSE9MREVSXQ==
```

`_field.percentile` has type `DATA_BINARY`. `FieldValue` has no array branch, so it must use base64 rather than a JSON array such as `"[50,75,90,95,99]"`.

**trace**

```csv
_name,trace_id,span_id,_span,service_id,start_time,duration
sw_segments,af1c...beef,1,CgZzZWdtZW50EgQ...,c2VydmljZS1h,1783382400123000000,150
```

**property**

```csv
_name,_id,_mod_revision,_create_revision,_updated_at,_delete_time,name,value
ui_template,ui-dashboard-1,1783382400000000000,1783382400000000000,1783382400000000000,0,dashboard,"{""layout"":""grid""}"
```

`_delete_time` is a **required** column — a non-zero value is a tombstone. `_create_revision` and `_updated_at` must also be carried, or version merging and gossip repair on the target cannot decide correctly.

### 4.4 Artifact file format

The top-level artifact structure is identical for both formats; only the leaf files differ.

| Path | Description |
|---|---|
| `manifest.json` | Written last |
| `schema/` | One JSON Lines file per kind, one protojson object per line |
| `nodes/<node-dir>/` | Organized by source node, keyed by the derived directory component ([Node IDs are not path-safe](#411-node-ids-are-not-path-safe)), never the raw node ID |
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
    "minTimestamp": 1788739200000000000, "maxTimestamp": 1788825599999999999,
    "segmentVersion": "1.5.0",
    "sources": [{
      "node": "10.0.0.5:17912",
      "segmentFiles": [
        {"path": "nodes/10-0-0-5-17912-3f9a1c2d/measure/sw_metric/seg-20260907/metadata",
         "bytes": 52, "crc32": "0x1f4a03bb"},
        {"path": "nodes/10-0-0-5-17912-3f9a1c2d/measure/sw_metric/seg-20260907/sidx-001.bnc",
         "bytes": 4194618, "crc32": "0x3a91c7e2", "entryCount": 3}
      ],
      "sidxCoversShards": [0, 1],
      "shards": [{
        "shard": 0,
        "minTimestamp": 1788739200000000000, "maxTimestamp": 1788825599999999999,
        "totalCount": 123456,
        "parts": [
          {"id": 1, "minTimestamp": 1788739200000000000,
                    "maxTimestamp": 1788760799999999999, "totalCount": 54321},
          {"id": 2, "minTimestamp": 1788760800000000000,
                    "maxTimestamp": 1788825599999999999, "totalCount": 69135}
        ],
        "rowCounts": {},
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

`shards[].parts[]` is written **unconditionally**, for every shard of every source. The export side does not decide which shards need it: that array is the input to the import-side multi-source judgement ([Detection basis](#62-detection-basis)), and a shard that looks single-sourced today becomes multi-sourced as soon as a second artifact is merged in.

`sidxCoversShards` is the **union of the route-derived shard set and the shard directories actually present on disk**. It is used **only to work out which nodes must receive this segment's series index** ([Data transfer](#525-data-transfer)); it never selects *which sources* to push — that follows the core-part selection ([Multiple-source push behavior](#64-multiple-source-push-behavior)). Both halves of the union are required:

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

// The client probes before it holds an ID of its own, so the request carries none:
// every node reports whatever it holds.
message ProbeSessionRequest {}

message ProbeSessionResponse {
  repeated SessionLease leases = 1;
  // Nodes that did not answer. A node holding no session contributes no lease,
  // so "clean" and "silent" must not look alike.
  repeated string unreachable_nodes = 2;
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
  // where the directory component is derived as "Node IDs are not path-safe" describes.
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
| 1 | ctl → liaison | `Plan{selectors=[…], create_session=true}` | Generate one `session_id` for the whole fan-out. A node already holding a different live session rejects the create and returns the incumbent's ID ([why session uniqueness is per node](#481-there-is-no-coordinator-so-uniqueness-is-per-node)) |
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

A non-empty `failed_nodes` is a **WARN, not a failure**: the artifact is already sealed and valid, so the run still exits 0. The client prints each node with the `bydbctl data release-session --id <id>` retry command, because until those snapshots go the nodes carry them for the rest of the lease — and on a hot node a snapshot costs roughly a second copy of the live data, not the free hard links the cold case suggests.

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

Beyond the race, a partial session left behind by a client that died mid-abort is reclaimed by the node-side expiry sweep (step 5 below) within one sweep period of the lease expiring, with no operator action. That is a backstop, not the unblocking path: the next export stops waiting after the 15-minute liveness threshold, not after the full lease.

#### 4.8.2 Taking over an existing session

Starting a new export does **not** silently remove an existing session. The client first calls `ProbeSession`, which is read-only, and decides from `last_renewed_at`:

- **Live holder** (renewed within the liveness threshold): refuse the new export and report who holds it.
- **Dead holder** (no renewal past the threshold): still refuse, but tell the user that `--preempt` takes it over. `--preempt` is a confirmation action, not a tuning knob.

Lifecycle:

1. **Create** — the liaison sends `Plan(create_session=true)` to all data nodes.
2. **Export** — export requests carry the `session_id` to read the matching snapshot.
3. **Heartbeat** — `bydbctl` sends `Plan(session_id, renew_only=true)` on a fixed interval.
4. **Release** — after the export completes and `manifest.json` is written, `bydbctl` calls `ReleaseSession(session_id)`. **On Ctrl-C or a fail-fast abort it deliberately does not**: the session is kept so the run can resume against the same frozen snapshot, and the client prints the session ID plus the `bydbctl data release-session --id <id>` command. There is deliberately no flag for "release on abort": it would make a graceful stop strictly worse than `kill -9`, because the resumed run would then hit `FAILED_PRECONDITION` and have to restart against a new point in time — and `release-session` already covers the case where you really are done.
5. **Cleanup** — each data node sweeps export snapshots on a one-minute timer and removes any whose lease has expired. The sweep also runs once at process start, so a session survives a data-node restart.

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
3. **Schema check** — compare the artifact's schemas with the target cluster's. Two resources compare equal unless they differ outside the ignore set `created_at`, `updated_at` and `Metadata.{id, create_revision, mod_revision}`; `created_at` always differs across clusters, so counting it would make every import a `CONFLICT`. `indexRule`'s `Metadata.id` is the exception — it is materialised into the inverted files, so the native channel compares it out of band and refuses a mismatch.
4. **Version compatibility** — for CSV, check the locally generated version against the data nodes; for native, check each shard's part version.
5. **Timezone check** — for native, the target node timezone must match the artifact's, or the import fails.
6. **TTL check** — skip data that has already expired.
7. **Topology check** — the target stage's `shardNum` and `segmentInterval` must equal the artifact's; a mismatch rejects the unit for native. The default stage is always written as `hot`: the server reports it as an empty string, and the client normalises that so the default tier has a stable name in the manifest and the gates. A unit is routed to the target stage **of the same name** — both sides' stage definitions are already known, from the artifact's `groups[].stages[]` and the target schema, so no mapping is configured — and a unit whose stage the target does not have falls back to `hot` and is re-checked against *that* stage — routing with the wrong stage's shard number writes shard directories that `loadShards` skips on restart, so the data is written, is queryable, and vanishes on the next restart. Equality also guarantees that no source shard ID reaches or exceeds the target's `shardNum`. `LocateAll` returns `<group>-<N> is a unknown shard` for such a shard, so the data is simply unroutable. This must be listed and blocked during dry run, not discovered halfway through a push. Topology drift frequently arrives together with a `shardNum` change, so the two conditions co-occur.
8. **Schema import** — create or overwrite per policy, then rerun checks 2–7.
9. **Data import** — per data type, as below.

Two routing failures must be distinguished: zero nodes returns `nil` plus `no nodes available`, while an out-of-range shard returns `... is a unknown shard`. Separately, `LocateAll` deduplicates its result set, so it can silently return fewer targets than the requested `copies` with no error, log, or warning. The import side must compare `len(targets)` against the requested `copies` itself; `--strict-replicas` cannot use `len(targets)` as its denominator. A shortfall is a WARN by default, recorded and filled in on the next resume; `--strict-replicas` promotes it to an error that aborts the run.

**Prerequisite: the liaison has no per-group stage resolution today, so multi-stage import cannot work until it is added.** `pub.ResolveStageResourceOpts` is called only by data nodes; every liaison-side `ResolveResourceOpts` returns the group's `ResourceOpts` unchanged, and `pkg/node/round_robin.go` builds its lookup table from `group.ResourceOpts.ShardNum` and `.Replicas` alone. The liaison's only notion of a stage is the `--data-node-selector` start-up flag, which pins the whole process to one tier. **Until that gap closes, importing into a group that has lifecycle stages is not supported at all — this is not "the routing may be off".** `banyand/backup/lifecycle/steps.go` already does the right thing and is the model: parse the target stage's `NodeSelector`, build a fresh selector over only the matching nodes, and route with that stage's shard number. Single-stage clusters are unaffected.

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
    SyncKind kind = 5;            // PART | SERIES_INDEX | ELEMENT_INDEX | PROPERTY_DATA

    // Required when kind = PART.
    repeated PartInfo parts = 6;

    string segment_version = 7;

    // Only set when re-pushing to replicas that failed earlier
    // ("When only some replicas succeed"). Empty = push to every target
    // that LocateAll resolves.
    // Non-empty = push only to these.
    //
    // The liaison MUST intersect this with the LocateAll result and never
    // treat it as an extension: a node outside that result is rejected with
    // INVALID_ARGUMENT. The receiver does not verify shard ownership, so
    // letting through a non-owner silently writes data to the wrong place.
    repeated string retry_nodes = 8;

    // Idempotency key. Meaningful only for kind=PART; index streams and CSV
    // leave it empty.
    //
    // Derived deterministically:
    //   sha256(manifestDigest || sourceNode || unitKey || sourcePartID)
    // It MUST NOT include part_type: the key is per stream, and a stream
    // carries exactly one partID — for trace, the whole bundle of core part
    // plus the secondary index parts sharing that ID. A PART stream without a
    // core part is forbidden, so part_type would be the constant "core".
    // It MUST NOT include the attempt number, a timestamp, or the target node:
    // a replay has to compute the same value, and each receiver only consults
    // its own set, so the target node adds nothing.
    string import_op_id = 9;
  }

  enum SyncKind {
    SYNC_KIND_UNSPECIFIED = 0;
    PART = 1;
    SERIES_INDEX = 2;
    ELEMENT_INDEX = 3;
    PROPERTY_DATA = 4;
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
  // The copies the liaison resolved for this Init (target stage replicas + 1),
  // not len(targets). The client cannot compute it — it does not know the
  // target stage's replica count — and needs it to detect a shortfall caused
  // by LocateAll deduplication.
  uint32 requested_copies = 1;
  repeated TargetResult targets = 2;

  message TargetResult {
    string node = 1;
    bool success = 2;
    string error = 3;
    // True when the receiver recognised import_op_id and wrote nothing.
    // --verify accounting needs it: a recognised replay legitimately adds zero
    // rows, so delta == 0 is an error only when no target reported
    // already_applied.
    bool already_applied = 4;
  }
}
```

Duplicate-delivery validation must be **split by `SyncKind`**. `(id, part_type)` uniqueness can only be enforced on `PART` streams: index streams carry fixed zero values with `id = 0` and an empty `part_type`, so applying the same rule there — or trying to tell them apart by `part_type` — triggers `logger.Panicf` on the receiver.

#### Idempotency: `import_op_id`

The receiver commits before it answers — `handleCompletion` calls `FinishSync()` and only then `sendResponse()` (`banyand/queue/sub/chunked_sync.go`, `(*server).handleCompletion`). A lost response therefore makes the client record a failure for a part that **did** land, and resume writes a second copy. Nothing on the receiver can recognise the repeat: `partMetadata` carries no hash, checksum or fingerprint, and its `ID` is `json:"-"` so it is not even persisted.

`import_op_id` closes that. The receiver checks the key against the set of op-ids held by this shard's live parts; on a hit it skips the whole transfer and answers `success=true, already_applied=true`. Otherwise it proceeds and persists the key into the part's own `metadata.json` via `fillFromSyncContext`, which already builds the metadata on the receive side rather than copying it from the sender.

**Why `metadata.json` and not a side log**: `metadata.json` lives inside the part directory, and "create the part directory + introduce it into `.snp`" *is* the atomic commit. The key therefore becomes visible exactly when the part does. Any separate log merely moves the gap somewhere else. The in-memory index costs nothing either — `initTSTable` already reads every live part's metadata at startup.

The one real cost is **merge propagation**: a merged part's metadata is rebuilt, so it must carry the union of its inputs' keys, aged out by a TTL. Skipping propagation degrades safely — a lost key means a replay behaves as it does today (a duplicate), never a wrong result.

**Scope is `kind=PART` only.** `SERIES_INDEX` has no part metadata, but re-pushing it is absorbed by deduplication anyway. `ELEMENT_INDEX` has no part metadata **and** re-pushing it is *not* harmless (×N). CSV has no parts at all. Those two stay with `resumeDuplicationPolicy` ([The ambiguous window](#73-the-ambiguous-window-inflight-and-missing)).

This is an engine change — a persisted-format addition plus merge propagation — so it is a prerequisite for the native channel rather than part of the client.

The upstream `id` is a **framing signal, not an identifier**. The receiver always allocates its own part ID with `atomic.AddUint64(&tsTable.curPartID, 1)`, and `fillFromSyncContext` copies six scalars but deliberately not the ID. Its only real use is the receiver's `createNewContext := session.partCtx == nil || session.partCtx.ID != partInfo.Id` check, which decides where one part ends and the next begins. Mixing two part IDs in one stream therefore causes a split plus an orphan deletion (`MustRMAll`), not a silent merge.

#### 5.2.2 Transfer flow

| # | bydbctl → liaison (import/v1) | liaison → data-* (cluster.v1) | Liaison behavior |
|---|---|---|---|
| ⓪ | Read the manifest and target schema/topology | — | — |
| ① | Read the footer of the selected `.bnc` files and split into streams by part | — | — |
| ② | `Init{catalog, group, shard_id, stage, kind, segment_version, parts{…}}` | — | Validate, resolve the target stage, call `LocateAll(group, shard, copies)`, open `SyncPart` streams |
| ③ | `Chunk{chunk_index=0}` | `SyncPartRequest{chunk_index=0, metadata=SyncMetadata{…}, …}` | Build `SyncMetadata`, fill `session_id`, and **assemble** `version_info`: `file_format_version` is copied from the client's `Init.segment_version` — it is the guard on the *source* data — while `api_version` and `compatible_file_format_version` are the liaison's own. A liaison that fills `file_format_version` with its own value turns that guard into a tautology |
| ④ | `Chunk` frames | `SyncPartRequest` frames × N | Forward to all N target nodes |
| ⑤ | `Completion` | `SyncPartRequest{completion=SyncCompletion{…}}` | Forward |
| ⑥ | `ImportPartsResponse{requested_copies, targets[]}` | Final responses from all streams | Wait for all N results before returning |

The fan-out must be **concurrent with independent per-target reporting**: one failed target must not cut off the others, and every target reports its own result. Do not copy the fan-out shape of `banyand/stream/syncer.go: syncPartsToNodesHelper` — it is a sequential loop with a fail-fast `return`, the opposite of what is needed. `banyand/liaison/grpc/property.go: replaceProperty` is closer in spirit (concurrent publish, wait for all futures, partial-success verdict), but it goes through `bus.Publish` rather than a gRPC stream.

Liaison memory stays flat only under a **per-frame synchronous barrier** — send one frame to all N targets, wait for all N acks, then read the next. An asynchronous or buffered fan-out makes memory grow with the speed gap between the fastest and slowest target.

#### 5.2.3 Build the import plan

This step reads only `manifest.json` and the target cluster's topology; it opens no BNC files. It decides which segments qualify for fast push, and which source node to select for each segment when several hold copies ([Multiple shard sources](#6-multiple-shard-sources)).

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
| `sidx-NNN.bnc` | Yes | `seg-XXX/sidx/**`, a Bluge inverted directory | `SERIES_INDEX` stream, **one per target node** ([Data transfer](#525-data-transfer)) — not one per shard |
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
    min_timestamp=1788739200000000000,
    max_timestamp=1788825599999999999,
    min_key=0,
    max_key=0
  }]
}

Footer of sidx-001.bnc, relative to the segment sidx/ directory:

  000000000012.seg     offset=0          size=4_194_304
  000000000013.seg     offset=4_194_304  size=1_048_576
  000000000013.snp     offset=5_242_880  size=1_024
  (a series index is a Bluge directory — only .seg and .snp, and no metadata.json)

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

**The series index is segment-scoped, not shard-scoped, so it is delivered once per target node.** `<segment>/sidx` is a single Bluge directory shared by every shard in that segment (`storage/segment.go` builds it from the segment location), and the receive side confirms it: `syncSeriesCallback.CreatePartHandler` — identical in stream, measure and trace — resolves the destination from `ctx.Group` and `ctx.MinTimestamp` only, calls `CreateSegmentIfNotExist`, and **never reads `ctx.ShardID`**. Compare the element index, whose handler does exactly the opposite: `syncElementIndexCallback.CreatePartHandler` calls `segment.CreateTSTableIfNotExist(common.ShardID(ctx.ShardID))`.

So `sidxCoversShards` has **one job only: resolving which nodes need this index.** Run `LocateAll(group, shard, copies)` for each shard in the list, take the **union of the node sets, deduplicated**, and open one `SERIES_INDEX` stream per node. `shard_id` on that stream is only a routing hint for the liaison; the receiver ignores it for placement.

> **An earlier draft said "one stream per shard in `sidxCoversShards`, sending the same bytes several times to different shards".** That was wrong on its premise. Because the receiver ignores `shard_id`, those extra streams re-introduce the identical external segment into the identical index. Deduplication absorbs them, so nothing breaks — it is pure waste. Measured on the real store: a source covering 4 shards sent 6,084 bytes under the per-shard rule versus 1,521 once per node, **4.0×**, with byte-identical query results.

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
      min_timestamp=1788739200000000000,
      max_timestamp=1788825599000000000,
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
  fv.bin            size=65_536

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
    {fv.bin, 65536}
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

Parts are sent in ascending part-ID order, which is also the byte order inside the container. That keeps reading sequential and keeps the relative ordering of source and target part IDs aligned. It does **not** reduce the checkpoint to a watermark: `done[].parts` stays a map keyed by part ID, because a part that reached only some of its replicas has to record its own `missing` set.

**Trace cannot be split across sessions.** Its core part and its secondary index parts share one locally allocated part ID: `NewPartType` reuses the ID within the same `syncPartContext` (`if s.partID == 0` before allocating), and the secondary part is written under that same ID. Across two sessions they receive different IDs and lose each other — and the two halves then fail *differently*, neither of them as an error:

- **A secondary part without its core is deleted.** `FinishSync` sees an empty core path and returns early. `Close()` then skips the core `MustRMAll` — that path is empty — but still calls `Close()` on every `sidx.SyncPartContext`, and *that* is what removes the secondary part directory. The bytes are gone before the stream answers `success=true`.
- **A core without its secondaries is kept, which is worse.** It is introduced normally with an empty secondary-part map. The spans stay queryable by time, but the ordered-by-key path over that index never returns them, silently and permanently.
- **A late secondary cannot repair it.** Reopening the tsTable passes the core parts' IDs as `AvailablePartIDs`, and any on-disk secondary part whose ID is not in that list is removed.

So a `PART` stream carrying any trace secondary part must also carry its core part, in the same session.

#### 5.2.6 Liaison handling

On `Init` the liaison resolves the data topic (for example `MEASURE + PART = "measure-part-sync"`), resolves the stage and schema to find the target data nodes, and opens one `SyncPart` stream per target. That needs a **new** `queue.Client` method: the existing `NewChunkedSyncClient` hands back a client whose only send path is `SyncStreamingParts`, which re-reads parts from disk. Add `NewChunkedSyncRelayClient(node)` alongside it, borrowing the pooled connection exactly as `NewNodeSchemaStatusClient` already does, then drive the raw `SyncPart(ctx)` stream.

The relay context must derive from the downstream client stream's context. Deriving it from `context.Background()` leaves zombie upstream streams when the client cancels.

| Time | bydbctl → liaison | liaison → data-a / data-b | Response |
|---|---|---|---|
| t0 | `Chunk{idx=0, data=<1MiB>, crc="3a91c7e2", files=[{meta.bin, 1048576}]}` | `SyncPartRequest{session_id, chunk_index=0, chunk_data=<same 1MiB>, chunk_checksum="3a91c7e2", parts_info=[{id=1, part_type="core", files=[{name:"meta", offset:0, size:1048576}]}], metadata=SyncMetadata{…}, version_info}` | Each node returns `CHUNK_RECEIVED` |
| t1–t8 | `Chunk{idx=1..8, …, files=[{primary.bin, 1048576}]}` | Same structure with increasing `chunk_index`; `files=[{name:"primary", offset:0, size:1048576}]`; no repeated metadata | Each node returns `CHUNK_RECEIVED` |
| t9 | `Chunk{idx=9, …, files=[{primary.bin, 524288}, {timestamps.bin, 524288}]}` | Convert to absolute offsets: `files=[{name:"primary", offset:0, size:524288}, {name:"timestamps", offset:524288, size:524288}]` | Each node returns `CHUNK_RECEIVED` |
| t10–t44 | Full frames containing one file slice | Same structure with one `files` entry | Each node returns `CHUNK_RECEIVED` |
| t45 | `Chunk{idx=45, data=<832KiB>, files=[…3 entries…]}` | `files=[{name:"tf:searchable", offset:0, size:524288}, {name:"tfm:searchable", offset:524288, size:262144}, {name:"fv", offset:786432, size:65536}]` | Each node returns `CHUNK_RECEIVED` |
| t46 | `Completion{total_bytes=48037888, total_chunks=46}` | `SyncPartRequest{session_id, chunk_index=46, completion=SyncCompletion{total_bytes_sent=48037888, total_parts_sent=1, total_chunks=46}, version_info}`, no `chunk_data` or `parts_info` | Final response |

The liaison stores nothing: it converts one frame at a time and forwards it, which is why it stays memory-flat at line rate. The memory protector is not a backstop here — it only observes memory, and a relay saturating the NIC has a flat memory curve.

#### 5.2.7 When only some replicas succeed

`ImportPartsResponse.targets` reports `{node, success, error}` per node, and the liaison waits for all N before returning. It does **not** retry the part itself — it holds one chunk at a time, and re-pushing a part means re-reading the container, which only the client can do.

The client records the part as **partially complete**, not done. `done[].parts[partID]` already carries its own `nodes` / `missing` pair ([the import progress file](#72-import-progress-file)), so recording the successful subset needs no new structure; a part with a non-empty `missing` does not count towards `progress.doneUnits`, and every outstanding entry is listed as a WARN at the end of the run.

**Re-pushing must target only the missing replicas, through `Init.retry_nodes`.** Re-sending the whole part is not equivalent: `data-a` would receive a second copy. Measure still converges by `(seriesID, timestamp, version)`, but **stream and trace are append-only, so the second copy is a permanent, visible duplicate**, and the element index re-introduces a whole segment. So the set actually pushed on resume is:

```
retry_nodes = inflight[worker].nodes  ∖  done[part].nodes
```

Pushing `inflight.nodes` verbatim re-delivers to replicas that already succeeded. The same rule governs a non-empty `missing` — see [The ambiguous window](#73-the-ambiguous-window-inflight-and-missing), which also explains why a `missing` entry is not by itself proof that the node lacks the data.

### 5.3 CSV data rewrite

For CSV, `bydbctl` reads the files directly, rebuilds the corresponding requests, and redistributes them to the target data nodes.

```protobuf
service ImportService {
  // Bidirectional: the CSV checkpoint is recorded per batch, so the
  // client needs an acknowledgement per batch, not one response at end of stream.
  rpc ImportRows(stream ImportRowsRequest) returns (stream ImportRowsResponse);
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
    // Monotonic within one stream, starting at 0. Echoed back in the
    // acknowledgement so the client can correlate without assuming
    // that responses arrive in request order.
    uint64 batch_seq = 5;

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

// One acknowledgement per RowBatch, so the client can advance its record
// cursor as batches land instead of only at end of stream.
message ImportRowsResponse {
  uint64 batch_seq = 1;          // Echoes RowBatch.batch_seq
  uint32 accepted_rows = 2;
  bool success = 3;
  string error = 4;
}
```

The RPC is **bidirectional**, not client-streaming. A client-streaming RPC delivers a single response after the client closes the stream, which cannot carry the per-batch acknowledgements that the CSV checkpoint depends on ([the import progress file](#72-import-progress-file)): the cursor may only advance over a batch the server has confirmed. Acknowledgements carry `batch_seq` so the client correlates them explicitly rather than assuming they arrive in request order.

A dedicated import RPC is required rather than reusing the public write API, because the public path regenerates fields the artifact already carries. In particular a measure `version` of `0` is silently replaced with the message ID on the liaison, destroying the deduplication semantics the CSV `_version` column exists to preserve.

| # | bydbctl → liaison | liaison → data-* | Liaison behavior |
|---|---|---|---|
| ⓪ | Read the manifest and target schema/topology | — | — |
| ① | Open `.csv.gz` files and build the column mapping from the schema | — | — |
| ② | `Init{catalog, group, name, stage}` | — | Locate the schema of the target resource |
| ③ | Send `RowBatch{…_rows}` in batches | `InternalWriteRequest` per row | Resolve `(EntityValues, ShardID)`, call `Locate(group, name, shard, 0)` and send one internal write request to **replica 0 only** — the write path replicates internally, so the liaison does not fan out. `property_rows` are the exception: they go to `TopicPropertyRepair` and fan out over all `copies` replicas |
| ④ | Receive the batch response | — | Aggregate success and failure for the batch |

This channel is **not** idempotent on replay, which is why its checkpoint is recorded at record-cursor granularity ([the import progress file](#72-import-progress-file)).

Property rows must be applied with `db.Repair(ctx, id, shardID, property, deleteTime)`. `Update` has no `delete_time` parameter at all — its underlying call hardcodes `0` — so a tombstone imported through `Update` comes back to life. `Delete` searches for an existing document first and is meaningless against a fresh database.

## 6. Multiple shard sources

### 6.1 When it happens

| Cause | Description |
|---|---|
| Replicas | When `replicas > 0`, the same shard naturally exists on multiple nodes |
| Topology drift | After the node count in a stage changes, the same shard number may remain on old nodes |
| Orphan shard | A node is no longer the shard's owner, but the data still remains on disk |
| Lifecycle migration window | Lifecycle migrates an expired segment to the next stage and only then deletes the source; on failure it returns and skips the delete. The same data sits in two stages at once, and **both go into the artifact**. This is deliberately not detected: the source cluster genuinely holds two copies at that moment, so reproducing it is correct. Pause lifecycle before exporting |

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

**measure does not apply the row-count condition** — a fully contained source is skipped outright. Its background merge collapses multiple versions of the same `(seriesID, timestamp)`, so the legitimate row-count skew is `(K−1)/K` for a source pushed K times: 66.7% at K=3. Any threshold below 100% is broken by the entirely normal "pushed a few times" case — and that skew is permanent rather than awaiting compaction, because equally sized parts are never merged under the production policy. The cost is that a measure orphan whose time range happens to be contained is skipped silently.

Every decision and its reason is printed in the dry-run `MULTI-SRC` detail, because this is the only place in the import that can decide to *send less data*. `--strict-coverage` promotes a multi-source unit to an error and writes nothing; `--all-sources` skips the judgement and pushes everything. `--all-sources` must disable both the shard-level and the segment-level judgement — the escape hatch is one switch, not two. It then adds, for every source whose core parts were *not* selected, one `SERIES_INDEX` stream per target node in that source's `LocateAll` union.

### 6.4 Multiple-source push behavior

| Catalog | Import behavior | Query | Disk |
|---|---|---|---|
| measure | Each part from every selected source becomes its own stream; no merging | Correct; the latest version is selected | Grows with the number of copies, and is **not** merged back |
| stream | Same as measure, plus the complete element index from every source | Correct; deduplicated by element ID | **Permanently** grows with the number of copies |
| trace | Same as above, but each core part and its secondary indexes must be sent together | ⚠️ Correct when the selected sources have disjoint coverage; **duplicate spans are visible** only in the overlapping cases below | Grows with the number of copies |
| property | The whole shard directory is sent and every record from every source is written | Correct when the `mod_revision`s differ. ⚠️ **Not** when they are equal and the delete states differ — under replication that is the ordinary convergence window, and two of six replay orders split one logical entity into two physical documents. `shard.repair` must be fixed so that on an equal revision the `deleteTime != 0` side wins and no tombstone is re-stamped | No amplification |
| index-mode measure | Only segment-level indexes exist. The import side must deduplicate **in memory by `_version` per docID across every source of a segment**, keep the maximum, then issue a single `UpdateSeriesBatch` — that primitive is last-write-wins and `InsertSeriesBatch` is keep-existing, so neither alone gives a reproducible winner across sources. The docID is the series' `EntityValues`, not the uint64 `series.ID` | Correct; the highest version is selected | No amplification |

- **trace is the only catalog whose query result can be wrong**, and it does not self-heal. But the exposure is narrower than "any multi-source unit", and every case is closed on the import side — the read path is not changed.

  **Selecting two sources does not by itself duplicate anything.** A second source is pushed because its coverage is *not* contained in the first, and disjoint time means disjoint spans — a span carries its own timestamp. Overlap only arises when a source is rescued by `count-spread` / `no-single-covering` (its coverage *is* contained) or when `--all-sources` skips the judgement. Both are pushed as-is and reported; `--strict-coverage` stops instead.

  > **Row-level deduplication was designed and rejected.** The key is cheap — `_element_id` for stream, `(trace_id, span_id)` for trace, both fixed columns needing no schema, measured at 12.1M keys / 92 MB / 2.2 s. It loses because **a content key cannot tell where a duplicate came from**: if the source already held the same span twice (a lifecycle migration window, say), deduplicating removes the source's copy too, and the target stops reproducing the source. So no channel deduplicates rows — native cannot decode them, and CSV will not.

- **The element index cannot be reduced to a covering set.** Each source's `idx/` may be complementary rather than duplicated, and a dropped index entry never reappears. It was measured at exactly ×3.000 for three sources (4,000 → 12,000 documents, 765 KB → 2,295 KB), and Bluge's merge does not reclaim duplicates because the element index does not enable deduplication. Extrapolated to a 167-segment group with a 200 MB per-shard baseline, that is 65 GB at `replicas=1` and 98 GB at `replicas=2`. Provision `idx/` at `(replicas + 1)` times.
- **The segment-level series index follows the core-part selection, source by source.** Push the series index of **every source whose core parts were selected**, and for index-mode measure — which has no core parts, the index *is* the payload — push every source's. Do not compute a separate minimum shard-covering set over `sidxCoversShards`.

  > **Why shard coverage is the wrong containment test.** Two sources can both advertise shard 0 and still hold *different series*: that is exactly what topology drift produces. Their core parts cover disjoint time ranges, so [the selection rules](#63-rules) rule 1 selects **both** — but a minimum shard-covering set sees `{0}` ⊇ `{0}` and keeps only one. The pruned source's series then have core data with **no series-index entry**, and `Lookup(series)` never finds them: the rows are on disk and undiscoverable.
  >
  > `sidxCoversShards` records shard membership, not series membership. The manifest carries no series-document inventory, so series containment cannot be proven from it, and an unproven containment must not be used to drop an index.
  >
  > **This costs nothing in the common case.** With pure replicas the redundant source is already skipped at the core-part step, so its index is skipped too — the same saving the covering set would have produced. Only the drift case pushes more, which is precisely the case where the extra push is load-bearing. On disk the redundant bytes are free anyway: the receiving store enables deduplication.

- **Only the series index is segment-scoped; every other index is per shard.** This is what decides whether "merging shards" is even a question for a given index:

  | Index | On-disk scope | Receiver keys placement on | Merge axis |
  |---|---|---|---|
  | Series index `seg-XXX/sidx/` | **segment** — one directory for all shards | `(group, segment)`; `ShardID` unused | across **sources** |
  | Element index `shard-N/idx/` (stream) | shard | `CreateTSTableIfNotExist(ShardID)` | across sources, within one shard |
  | Trace secondary index `shard-N/sidx/<rule>/<016x>/` | shard, and it is a real **part** | the core part's tsTable | follows its core part |
  | Property `property/data/<group>/shard-N` | shard | shard | `(entity, mod_revision)` via `Repair` |

  So for the series index there is nothing to merge *across shards* — one segment already has exactly one index covering all of them. The axis that does need merging is **across sources**, which is the rule above. For the other three the shard is part of the identity, and merging across shards would matter — but it never arises, because native fast push requires the source and target shard counts to be equal ([Native fast push](#52-native-fast-push)), and the CSV channel rebuilds every index from rows on the target.

- **What deduplication on that index does and does not do**: the winner is the first arrival, not the newest. The discarded copy's index tag values become unqueryable — `Lookup(entity)` still matches, because the Bluge document `_id` is the entity value itself, while a filter on a non-entity indexed tag returns nothing for the losing value.
- **measure's storage does not converge.** The production default merge policy requires a write-amplification score of `≥ max(maxParts/2, minMergeMultiplier) = 4`, and equally sized parts score exactly their own count — three equal parts score 3 and are never merged. Queries are correct immediately regardless, because the read path deduplicates by version before any merge runs.

#### 6.4.1 What a duplicate costs, per catalog

Duplication is not a trace problem; it is a problem whose *cost* differs per catalog. Those costs are what set the per-catalog defaults for [`resumeDuplicationPolicy`](#73-the-ambiguous-window-inflight-and-missing), and what an operator weighs when overriding them.

| Value | Behaviour | Fidelity preserved? |
|---|---|---|
| `block` | Do not import; list the affected units and stop. On a TTY this renders as a per-unit prompt, so there is no separate `ask` value | ✅ nothing written |
| `warn` | Import as-is, and list what was affected with the projected amplification | ✅ |
| `push` | Import as-is, silently | ✅ |
| `skip` | Leave the unit out without stopping the run. **Forces `--verify` on** — the one value that can lose data | ✅ drops whole units, never alters rows |

| Catalog | What a duplicate costs | Default |
|---|---|---|
| **trace** | Duplicate spans, visible to users, and nothing self-heals | `warn` |
| **stream** | Queries stay correct (the vectorized path deduplicates by element ID). Disk grows **permanently**: the element index was measured at exactly ×3.000 for three sources, extrapolating to 65 GB at `replicas=1` and 98 GB at `replicas=2` for a 167-segment group | `warn` |
| **measure** | Queries stay correct (the read path deduplicates by version *before* any merge). Disk grows, and under the production merge policy equally sized parts are never merged back | `warn` |
| **property** | Union by `(entity, mod_revision)`; no amplification | `push` |
| **index-mode measure** | docID overwrite; no amplification | `push` |


**Nothing defaults to `block`.** Every value preserves fidelity, so the choice among defaults is only about visibility:

- `warn` fails by leaving **duplicate rows**, which users see and report.
- `block` fails by leaving **units un-imported**, and on trace that is indistinguishable from "there was no traffic in that window". A gap in observability data is silent; a duplicate is not.

An import tool's job is to get the data in, so the defaults err toward completeness and tell you what it cost. The governing rule is **prefer a duplicate over a gap**. An operator who would rather stop sets `block` explicitly — per catalog, or per group when only one group's disk budget is tight.

There is deliberately no `overwrite` value anywhere. A part has no overwrite semantics and there is no API to delete a single part, so "replace the existing copy" cannot be built; the choice really is only reject, re-push, or skip. Row-level deduplication on the CSV channel was designed and rejected: a content key cannot tell a duplicate the source cluster already held from one this import created, so deduplicating it would stop the target reproducing the source.

## 7. Resume

Both directions must survive an interruption. Export is the simpler half: it checkpoints per **unit**, appending one to `done` only after every file of that unit has passed its CRC32 check and been renamed. Volume splitting does not change that granularity, because a part never spans volumes. Index-mode measure and the segment-level series index have no shard axis at all and resume at segment granularity.

Import is more involved.

"Safe" below means **safe to replay automatically**: re-delivering a unit that already landed is harmless. The rest need [The ambiguous window](#73-the-ambiguous-window-inflight-and-missing).

| Format | Data type | Checkpoint level | Automatic replay | Re-importing already-successful data |
|---|---|---|---|---|
| native | measure | node → segment → shard → part → target node | Safe | The read path keeps the larger version by `(seriesID, timestamp)`, so duplicates are invisible in queries; storage grows and does not merge back |
| native | series index | node → segment → target node | Safe | First write wins; later writes are discarded silently |
| native | property | node → group → shard | Safe | Idempotent |
| native | stream | node → segment → shard → part → target node | Safe — a replay is recognised by `import_op_id` and writes nothing | Without the key it would stay queryable (the vectorized path deduplicates by element ID in `pkg/query/vectorized/stream`'s `Distinct`) but storage would grow permanently |
| native | trace | Same as above | Safe — a replay is recognised by `import_op_id` and writes nothing | Without the key there is no deduplication anywhere and **duplicate spans become visible to users** |
| native | element index | node → segment → shard | **Needs `resumeDuplicationPolicy`** — `import_op_id` does not cover it | **No deduplication**; documents and disk grow linearly and are never reclaimed |
| CSV | measure | node → segment → shard → split file → record index | Safe | The read path keeps the larger version by `(seriesID, timestamp)`, so duplicates are invisible in queries |
| CSV | index-mode measure | Same as above | Safe | Idempotent |
| CSV | property | Same as above | Safe | Idempotent |
| CSV | stream / trace | Same as above | **Needs `resumeDuplicationPolicy`** | stream behaves as native; trace has no deduplication |

The target side cannot deduplicate a re-imported part *by content*: the same payload imported twice produces two directories with different part IDs, and `partMetadata` carries no hash, checksum, or fingerprint field. `import_op_id` closes that for `kind=PART` by persisting a client-supplied operation key in the part's own `metadata.json`. For every other payload — the element index and all CSV rows — idempotency still rests entirely on the client checkpoint.

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
| `Plan` first frame returns | Record `session` (`id` + `expiresAt`) | Write on receipt — a resumed run needs the ID, and `release-session` cannot release a session that was never written down |
| `Plan` last frame returns | Record the complete `plan` | Write — no `Export` may be sent until this lands, or a resume cannot tell "this unit had no data" from "this unit never came up" |
| Worker starts a unit | Set `inflight[node] = {unit, startedAt}` | Write |
| Each `file_finished` | Validate bytes/crc32 → fsync → rename → keep the entry in worker memory | No |
| `unit_finished` | Remove `inflight[node]`, append to `done`, update `progress` | Write |
| All units complete | Delete the progress file **after** `manifest.json` is written | — |

That final ordering is a discipline, not a detail: write `manifest.json` first, then delete the progress file. Reversing it leaves a window in which the artifact has neither a manifest nor a resume record.

Resuming an export reads the progress file, checks its `outputDir` / `selectorsDigest` / `sourceNodesDigest` bindings against this invocation, renews the session through `Plan`, and then deletes, **by path prefix and as whole directories**, everything under `nodes/<node-dir>/<catalog>/<group>/seg-<suffix>/` for each unit still in `inflight` — the prefix is derivable from the key alone. It must not delete "the files of the unfinished unit" by name: those names existed only in the crashed process's memory, and a re-run can emit fewer volumes than the previous attempt, leaving `-004`/`-005` behind to break the manifest-equals-disk check. Checking the bindings first is what makes deleting safe — a mistyped `--checkpoint` is rejected before anything is removed.

### 7.2 Import progress file

Stored by default in `<artifact>/.import-progress.json`. It holds target cluster information and current progress.

```json
{
  "kind": "import-progress",
  "artifactVersion": 1,
  "format": "csv",

  "manifestDigest": "<digest of the raw manifest.json bytes>",
  "artifactFilesDigest": "<digest of the (path, bytes, crc32) tuples of every file entry in the manifest: segmentFiles[], shards[].files[], schemaFiles[]>",
  "targetCluster": "<cluster identity reported by the liaison, plus a digest of the target node-name set>",


  "checkpointEveryBatches": 64,

  "exportedAt": "…",
  "updatedAt": "…",

  "verifyPreBaseline": {"measure/sw_metric/20260907/0": {"data-a": 12345, "data-b": 12345}},

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
        "minTimestamp": 1788739200000000000,
        "maxTimestamp": 1788825599000000000
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
| Native: before opening a stream | Set `inflight[worker] = {key, kind, partID, sidxNames, nodes, isRetry}` | Write |
| Native: after the response | Clear `inflight[worker]`; add successful nodes to `done[].parts[partID].nodes` (or `done[].blobs[kind].nodes`), failed ones to the sibling `missing` | Write |
| CSV: before a checkpoint range | Set `inflight[worker] = {key, slice, fromRecord, records}` | Write |
| CSV: after each successful batch | Overwrite `<checkpoint>.cursor` in place with `{key, slice, record}` — one `write()`, no fsync, main file untouched | Written, not synced |
| CSV: every `checkpointEveryBatches` batches, or when a slice is exhausted | Fold the cursor into `done[].slices[slice]`, clear `inflight[worker]`, fsync, then truncate the cursor file | Write |
| When a unit completes | Increment `progress.doneUnits` | Write |
| When all imports complete | Leave the progress file unchanged | — |

Resuming an import reads the progress file and checks it against the current plan, re-verifies the artifact against the target cluster as in the normal workflow, finishes the entries recorded in `inflight`, and continues with the rest.

### 7.3 The ambiguous window: `inflight` and `missing`

Both channels have a window in which work is **committed on the server but not yet recorded by the client**. The shape is the same on both: commit first, record second.

| Channel | Window | Worst case |
|---|---|---|
| native | after `targets[]` arrives, before the progress file is renamed | **one part** per worker, so at most `parallelism` parts |
| CSV | after a batch is sent, before the cursor is written | **one batch** (`import.writeBatchSize`, 1,000 rows by default); only a power loss or kernel panic widens this to `import.checkpointEveryBatches` (64) batches |

**Reversing the order does not help, and neither does checkpointing more often.** Recording before committing turns a failed delivery into a recorded success, which is silent data loss — strictly worse. And no checkpoint frequency closes the gap, because the gap is not an interval the client controls: it is the instant between the server's commit and the client's knowledge of it. Shrinking it changes how many units land in the window, never whether the window exists.

So the design does not claim that resume is automatically safe. `inflight` **bounds** the window to at most one unit per worker, and resume then splits by model:

| Channel / payload | On resume | Why |
|---|---|---|
| native, `kind=PART`, any model | **Re-push** — the receiver recognises `import_op_id` and writes nothing | Exactly-once by construction ([the ImportParts protocol](#521-protocol)) |
| native, `SERIES_INDEX` | **Re-push** | Deduplication absorbs it; no part metadata to carry a key, and none needed |
| native, property | **Re-push** | Idempotent via `Repair`, once the equal-revision / differing-delete-state case above is fixed |
| native, `ELEMENT_INDEX` | **Decided by `resumeDuplicationPolicy`** | No part metadata to carry the key, **and** re-pushing is not harmless: documents and disk grow ×N, never reclaimed |
| CSV, measure / index-mode / property | **Re-push** | Idempotent or version-converging |
| CSV, stream / trace | **Decided by `resumeDuplicationPolicy`** | Rows are replayed through the write path; there is no part and therefore no key. Re-pushing leaves a permanent duplicate for stream and a visible one for trace |

Their values, with per-catalog defaults taken from [what a duplicate costs](#641-what-a-duplicate-costs-per-catalog):

| Value | At resume time |
|---|---|
| `block` | Stop and list the ambiguous units; on a TTY, prompt per unit. Exits **7** — "halted for a human decision; the completed part is valid", kept distinct from 4 (verify found an ERROR-level discrepancy) and 6 (progress file unusable — *this run* wrote nothing, though earlier runs may have) |
| `warn` | Re-push and say so |
| `push` | Re-push silently |
| `skip` | Leave the unit out and **force `--verify` on**; if verify finds a gap, exit **4** and list every skipped unit |

There is no `ask` value: `block` renders as a prompt when there is a terminal to prompt on.

#### `missing` is ambiguous too, and must not be re-pushed unconditionally

A `missing` entry in `targets[]` is **not** proof that the node lacks the data. The receiver commits before it answers — `handleCompletion` calls `FinishSync` and *then* sends the response — so "committed, but the response never came back" (node OOM, liaison restart, dropped connection) is faithfully recorded as `missing`. Automatically re-pushing to that node writes a second copy onto a node that already has the data: a permanent visible duplicate for stream and trace, a whole re-introduced segment for the element index.

**`missing` therefore follows the same split as `inflight`**, and that split is by *payload kind*, not by model: every native `kind=PART` re-pushes directly and is recognised by `import_op_id`, as do the series index and property; only `ELEMENT_INDEX` and the CSV stream / trace payloads go through `resumeDuplicationPolicy`. Either way the re-push is targeted with `Init.retry_nodes` ([When only some replicas succeed](#527-when-only-some-replicas-succeed)) and never re-sends the whole part.

#### What this means for the guarantee

With `import_op_id` in place, the native `kind=PART` path is **exactly-once**: a replay is recognised and writes nothing. Everything else — the element index, and every CSV payload — remains **at-least-once with a bounded, enumerable ambiguous set**, because those payloads have no part metadata to carry a durable key.

Two things this does *not* buy, and they are the ones that dominate in practice:

- **It does not reduce multi-source duplication.** `--all-sources`, a coverage misjudgement, or overlap the source cluster already had all produce *genuinely different parts* with different keys. The receiver is right to accept them all. Stream's ×2.38 storage and the element index's ×3.000 were measured on exactly that case, so the key saves nothing there.
- **It does not make trace duplicates invisible.** It prevents one *cause* of them. Any duplicate arriving by another route is still shown to the user, because neither the trace merger nor the query path deduplicates by span ID outside the cross-node merge.
