# Data Export and Import

Status: the export dry run, the session RPCs (`Plan` create/read, `Sessions`) and the CSV export (`Export` RPC, `bydbctl data export`) are implemented (2026-10-10), and so is the CSV import (`ImportService`, `bydbctl data import`, 2026-10-19) — see [5.4 CSV import as implemented](#54-csv-import-as-implemented). The native export and the native import (`ImportService.ImportParts`, 2026-11-03) are implemented too — see [5.5 Native import as implemented](#55-native-import-as-implemented); export resume (2026-11-12) and import resume (2026-11-13) are implemented — see [7. Resume](#7-resume); what is still design is listed in [4.9 Remaining work](#49-remaining-work).

> **NIDX-03/04/05 note.** This document was written while the per-segment
> series index (`seg-*/sidx/`) was still backed by a third-party index
> library. NIDX-03 cut `seg-*/sidx/` over to the native ICE v3 engine
> (`pkg/index/native`) and removed the legacy series store entirely: there is
> no index-local lock file, no legacy writer, and no engine flag for it any
> more, so every claim below that describes `seg-*/sidx/` as "a legacy
> inverted directory" or routes its snapshot/export path through the retired
> `pkg/index/inverted` package's `ReadOnlyWalkDocuments` / `ReadOnlyDocCount`
> (now `pkg/index/native`'s own read-only API) needs re-validation against the
> native engine before this design is implemented. The Stream element index
> (`shard-N/idx/`) is a separate cutover from NIDX-03: PR #1390 already moved
> the live element-index write/query path onto the same native ICE v3 engine,
> so it is **not** "unaffected", and every claim below that describes
> `shard-N/idx/` itself as a legacy inverted directory needs the same
> re-validation as `seg-*/sidx/` above. NIDX-04 then cut the offline migration
> tool (`banyand/stream/migration_element_index.go`) that bridges pre-native
> element-index data onto the native writer too, and NIDX-05 removed the
> retired third-party index library (and its dependency packages) from the
> module entirely, so no claim below should assume that library's presence
> at runtime, on disk, or in `go.mod`.

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
  nodes: [liaison-1:17912, liaison-2:17912] # Liaison node list; every usable one shares the export streams
  nodesTLS: {enable: false, insecure: false, cert: ""}   # TLS for liaison node port 17912
  rateLimit: 0                        # Per-liaison byte rate limit (bytes/s), enforced by each liaison,
                                      #   0 = unlimited, e.g. "800MiB/s"

export:                               # Export only: scope and options
  output: ./backup-20260708           # Export directory; CLI --output overrides this. Error if both are missing
  format: csv                         # native | csv
  compress: gzip                      # none | gzip; default gzip for CSV, none for native (native accepts only none)
  maxFileSize: 1GiB                   # Shared by both formats: limit per output file in on-disk bytes (native: hard, >= 64KiB; CSV: soft)
                                      #   on disk after compression; 0 = unlimited
  includeSchema: true                 # false = no schema/ files and no schema revision check
  schemaOnly: false                   # true = write schema/ and manifest.json only: no session, no data
  csvAnnotations: false               # CSV only: write a "#datatype" line before every header
  parallelism: max                    # Data nodes exported concurrently; max = the selected node count,
                                      #   clamped to min(N, nodes)
  nodeRateLimit: 0                    # per-node byte rate limit, 0 = unlimited, e.g. "50MiB/s"
  nodeRowRateLimit: 0                 # CSV only: per-node row rate limit (rows/s), 0 = unlimited
  nodeIdleTimeout: 10m                # Fail a node stream that delivers nothing for this long; 0 = never
  sendRetryTimeout: 15m               # Window for reopening a refused export stream on the next liaison
  selectors:                          # Empty or omitted = all catalogs and groups in the cluster, but the
                                      #   groups the server creates for itself (_deletion_task,
                                      #   _property_gossip), which no export includes
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
  parallelism: max               # Work items imported concurrently; max = the union of target data
                                 #   nodes reported by Targets, capped at the number of work items
  writeRateLimit: 0              # CSV only: row rate limit (rows/s) shared by CSV row replay and property import;
                                 #   0 = unlimited
  nodeRateLimit: 0               # CSV only: byte rate each target data node admits from the imports,
                                 #   however many liaisons write to it, e.g. "50MiB/s"; 0 = unlimited
  nodeRowRateLimit: 0            # CSV only: row rate each target data node admits from the imports
                                 #   (rows/s); 0 = unlimited
  partRateLimit: 0               # Native only: total outbound byte rate limit, e.g. "50MiB/s"; 0 = unlimited
  partChunkSize: 1MiB            # Native only: bytes per ImportParts frame; must be within [64KiB, 4MiB],
                                 #   out of range is a usage error (exit 1)
  writeBatchSize: 1000           # CSV only: rows per ImportRows batch; 1-10000, out of range is a
                                 #   usage error (exit 1); see §5.4.7 for writeBatchBytes and the rest
  verify: true                   # Reconcile counts after the import; CLI --verify overrides
  workDir: /data/import-work     # Local scratch directory, used only to unpack .tar/.tar.gz artifacts
```

> **In the tree today** (CSV export step, 2026-10-10): the plan file is strict and accepts `connection.nodes`, `connection.nodesTLS`, `connection.rateLimit` and, under `export`, `output`, `format`, `compress`, `maxFileSize`, `includeSchema`, `schemaOnly`, `csvAnnotations`, `parallelism`, `nodeRateLimit`, `nodeRowRateLimit`, `nodeIdleTimeout`, `sendRetryTimeout` and `selectors`. Every option is declared once in `pkg/transfer/config` with struct tags naming its flag, plan path, `BYDBCTL_*` variable, default and grammar; one-shot switches (`--dry-run`, `--preempt`, `--resume`, `--strict-coverage`, `--output-format`) have a flag and a variable but no plan field. Under `import` (CSV import, 2026-10-19) it accepts `input`, `includeSchema`, `schemaOnly`, `schemaConflictPolicy.{default, rules}` (`rules` is plan-only), `parallelism`, `writeRateLimit`, `nodeRateLimit`, `nodeRowRateLimit`, `writeBatchSize`, `writeBatchBytes`, `writeInflightBatches`, `sendRetryTimeout`, `verify` and `verifyTimeout`; the native import (2026-11-03) adds `partRateLimit` and `partChunkSize`; export resume (2026-11-12) adds `export.checkpoint`; import resume (2026-11-13) adds `import.checkpoint`, `import.checkpointEveryBatches` and `import.resumeDuplicationPolicy`; `import.workDir` belongs to archives and is rejected until those land, as is any other key not listed here. `format: native` exports `.bnc` containers ([Native export](#42-native-export)). Explicit flags override the file, which overrides the `BYDBCTL_*` environment.

**An import reproduces what the export captured**, so there is no duplicate policy: everything is pushed as-is. Running an import again stores native stream and trace parts, element index segments and CSV stream / trace rows a second time ([What a duplicate costs, per catalog](#641-what-a-duplicate-costs-per-catalog)). Overlapping sources on a first run are reported as a WARN; `--strict-coverage` turns that into an error.

`workDir` is **not** a rebuild workspace. Neither import channel reconstructs parts locally: the native channel relays opaque bytes and the CSV channel parses rows into write requests. Unpacking an archived artifact is the only local scratch requirement.

## 3. Dry run

Dry run walks the real workflow without importing or exporting anything. It is a strict prefix of the real run with two deliberate exceptions: `Plan` goes out without a `session`, so no snapshot is created; and no schema beyond the group list is listed, so nothing is written to `schema/`.

### 3.1 Export dry run

1. **Connection check** — connect to one liaison and call `GetCurrentNode` to confirm the `LIAISON` role, then `GetClusterState` for the other nodes' IDs, labels, versions, and timezones.
2. **Fetch group list** — list every group through the liaison's group registry, so a selector that names an unknown group, or a group of another catalog, fails before anything is planned. The dry run loads no other schema.
3. **Plan request** — send one `Plan{}` request (no `session`) to that liaison, which filters all `DATA`-role nodes, forwards the request, and relays each response frame as it arrives. A dry run creates **no session and no snapshot**, so it inventories the online catalog and its numbers have a shelf life; the real export inventories a frozen snapshot.
4. **Client aggregation** — aggregate and print what every node returned.

```text
NODES     the cluster reports 4 data node(s) [data-1 data-2 data-3 data-4]; no inventory came back from [data-3] ⚠ (their data would not be exported)
MULTI-SRC 1 unit(s) have more than one source; all of them are exported, the importer decides which to use
          stream/sw_record/hot/20260706/shard-1  <- data-1(48.9M rows, 07-06 ~ 07-06) + data-4(48.9M rows, 07-06 ~ 07-06)

NODE    CATALOG   GROUP             STAGE  SEGMENTS  SHARDS  PARTS  EST-ROWS  EST-SIZE   TIME-RANGE
data-1  measure   sw_metricsMinute  hot    7         2       34     118.2M    18.3 GiB   06-30 ~ 07-07
data-1  stream    sw_record         hot    7         2       61     342.7M    96.1 GiB   06-30 ~ 07-07
data-2  measure   sw_metricsMinute  warm   28        4       52     1.4G      201.5 GiB  05-30 ~ 06-30
data-2  property  sw_prop           warm   -         1       -      12.3K     4.1 MiB    -
data-4  stream    sw_record         hot    7         2       58     339.1M    95.2 GiB   06-30 ~ 07-07

SNAPSHOT  each data node pins its own snapshot; the largest node (data-2) needs about 201.5 GiB; check df there first
```

The `NODES` line has two other forms: `NODES     N data node(s) [...], all answered`, and `NODES     standalone process [self]` for a standalone server. `MULTI-SRC` appears only when some unit is held by more than one node (replicated shards): it lists each key — `catalog/group/stage/segment/shard-N` — with every source's rows and time range. Property groups are not listed, nor are index-mode measure segment-level units: they carry no shard id, so the dry run cannot tell replicas of a segment from different shards of it. The `SNAPSHOT` line budgets the largest per-node compressed subtotal, because each node pins its own snapshot on its own disk.

The table lists only data that exists; it never derives "which shards ought to be here" from the schema's `shardNum`, because shards and segments are created lazily and comparing against an expected set produces large-scale false reports. The `NODES` line is therefore the only completeness check — it lists the nodes the liaison reported unreachable or not holding the session (`unreachable_nodes`), which cannot produce a false positive. It is a WARN by default and an error under `--strict-coverage`. A dry run inventories the live directories, and a session snapshot contains what was flushed to disk when it was taken: rows still in memory (up to one flush interval) are in neither.

### 3.2 Import dry run

> **CSV import as implemented (2026-10-19).** The CSV dry run runs steps 0–5 of [the implemented workflow](#541-workflow) — preflight, artifact check, read-only schema comparison, `ImportService.Targets`, the pure `decide()` and the rendered verdict table — and exits `0` (would proceed) or `2` (would be refused) without writing anything. Its table is byte-identical to the one the real import prints before it writes. Below the main table the CSV table prints SCHEMA, one line per group with its resource count and how many resources get each disposition; PROBLEMS, only when there are any, one sentence each: the groups of the missing resources together, every conflict with its differences and the `match:` rule that would accept it, the work items a route, TTL, coverage or row check refuses, and the warnings; and RESULT, the conclusion in one line. The main table's `STATUS` column is `ok`, `refused` or `expired`. Step 4 also runs the read-only row check (gate `ROWS`) over every row of the artifact, so the CSV dry run reads all the data, not only the manifest and schema. Of the gates below only **[1] schema**, **[4] TTL** and `ROWS` apply to CSV: the version and timezone gates are no-ops, and there is no topology gate because rows are re-routed by the target's own stage, shard number and replicas. The TTL gate follows item 6 below: it asks whether the target segment holding the unit's newest row ends before `now − cumulative TTL of the mapped target stage` (see [TTL gate](#545-gates)). The CSV table **has** a `TARGET-NODES` column, read from `Targets`, which the liaison computes with the same route the import uses. A group the import creates or overwrites has no route before its schema is written, so its `TARGET-NODES` is `after-schema`, explained by a `NOTE` line below the table; the re-check after the schema write routes it. A standalone target shows `-`. The native dry run as implemented ([5.5](#55-native-import-as-implemented)) prints the same table, with `TARGET-NODES` and `STATUS`; its `CHANNEL` is `part-push` (`property/parts` for a property group), and its refusals (`TOPOLOGY`, `TZ`, `VERSION`, `INDEX_RULE`) appear in `PROBLEMS`. The rest of this section, including the table below, its `CHANNEL`/`GATE` values and the paragraph about `LocateAll`, is the original native-aware design and is superseded where it differs.

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

CATALOG   GROUP       STAGE  SEGMENTS  SHARDS(src→dst)  ROWS     SIZE      SCHEMA    CHANNEL           GATE   TIME-RANGE
stream    sw_record   hot     7        2→2              342.7M   96.1GiB   CONFLICT  ① part-push       BLOCK  06-30 ~ 07-07
measure   sw_metric   hot     7        2→4              118.2M   18.3GiB   IGNORE    REJECT(topology)  BLOCK  06-30 ~ 07-07
measure   sw_metric   warm   28        4→4                1.4G   201.5GiB  OK        ① part-push       TTL!   05-30 ~ 06-30
measure   sw_traffic  hot     7        -                  0.4M   12.3MiB   CREATE    ① sidx only       ok     06-30 ~ 07-07
trace     sw_trace    hot     7        4→2               12.1M   4.1GiB    CONFLICT  REJECT(topology)  BLOCK  06-30 ~ 07-07
property  sw_prop     -       -        1→1               12.3K   4.1MiB    CREATE    property/repair   ok     -
TOTAL                        56                          1.87G   320.0GiB                                     05-30 ~ 07-07
         of which BLOCKED: 3 rows / 42 segments / 473.0M rows
```

There is **no `TARGET-NODES` column**: the liaison picks target nodes at execution time from the live routing table, and `LocateAll` is not on the dry run's read-only call list. Never compute them on the client as `nodes[(shardID + replicaID) % len(nodes)]` either — the real `roundRobinSelector.Pick` indexes a globally sorted lookup table rather than the shard ID, so that formula breaks as soon as there is more than one group.

All three verdict columns take closed value sets:

- `SCHEMA` — `CREATE` / `OK` / `MISSING` / `CONFLICT` / `IGNORE` / `OVERWRITE` / `INVALID`.
- `CHANNEL` — `① part-push` / `① sidx only` (index-mode measure) / `② row replay` / `property/repair` / `REJECT(<gate>)`. **`② row replay` can only appear for a CSV artifact**: a native artifact has no fallback across a topology change, so it renders `REJECT(topology)`.
- `GATE` — `ok` / `TTL!` / `BLOCK`. Any row whose `CHANNEL` is `REJECT(...)` is `BLOCK`, as is any row whose `SCHEMA` is `CONFLICT` or `INVALID`, whatever its channel; `TTL!` marks a row that clears every other gate and is stopped by the TTL gate alone, which `--force-ttl` overrides.

`TOTAL` counts **every** row, blocked ones included, and `SEGMENTS` / `ROWS` / `SIZE` all use that same basis; the blocked subtotal is reported on its own line.

`measure/<group>/_top_n_result` is never written and not compared as an ordinary resource: the target creates it automatically from its own hardcoded schema, so comparing it would only produce conflicts the user can neither fix nor should fix. Its **rows** are still imported (see [Top-N results](#546-top-n-results)), so its columns get a separate compatibility check: matching columns make it `OK`, counted with the identical resources and not listed, and an artifact `_top_n_result` whose tag or field names or types differ from the target's auto-created one is a `CONFLICT`.

## 4. Export

### 4.1 Client

`bydbctl data export` without `--dry-run` runs these steps (`pkg/transfer/exporter.RunExport`):

1. **Check the output directory** — it must not exist or must be empty; anything else is exit `1`, except a directory that holds the progress file of an interrupted export (`.export-progress.json`, or `--checkpoint`): that export is resumed against its session ([7.2](#72-export-resume)), so an export never mixes with what an unrelated run left behind. A `--checkpoint` in a subdirectory of the output directory is refused (exit `1`): that subdirectory would end up in the artifact.
2. **Connection check** — probe every address in `--nodes` (`GetCurrentNode`, `GetClusterState`). An unreachable candidate, or one that has not discovered any data node yet, is skipped with a `WARN`; refused credentials or an endpoint that is not a liaison fail at once (exit `2`). The first usable liaison is the primary: the schema, `Plan` and `Sessions` calls go through it, and the dry run and `release-session` use only it.
3. **Fetch and write the schema** — list the groups, validate the selectors, then list every resource of the selected groups through the liaison's registry services and write `schema/<kind>.jsonl` ([Schema files](#441-schema-files)). The mod revision of every stream, measure, trace and property is kept in memory for the CSV check in step 6. With `--include-schema=false` this step lists nothing beyond the groups and writes no `schema/` files, and step 6 skips the revision check. With `--schema-only` the client writes `manifest.json` (`schemaOnly: true`, no units) here and stops: no session, no snapshot, no data.
4. **Create the session** — one `Plan{selectors, create{preempt}}`. The session id from the first (`created`) frame is printed at once, before the inventory arrives. A node holding another session refuses with `ALREADY_EXISTS`; the client lists the sessions once, prints every occupant with its start time, last heartbeat, catalogs and nodes, and exits `2` ([Taking over an existing session](#482-taking-over-an-existing-session)). Transient failures (`INTERNAL`, `UNAVAILABLE`, `ABORTED`) are attempted up to three times with jittered back-off. A create whose summary still names unreachable nodes is released again and fails, because their data would be missing.
5. **Heartbeat** — `Sessions(ACTION_HEARTBEAT)` every 5 minutes in the background, checked against the data nodes that hold the session, sent as `required_nodes` (a node the cluster gained since, which never held it, does not count). A resumed export narrows the list as nodes finish ([7.2.2](#722-resume-flow)). A failed heartbeat is a `WARN` line and is retried on the next tick; `FAILED_PRECONDITION` (the session expired or its lease is unreadable) cancels every worker and exits `3`. A node stream refused with `FAILED_PRECONDITION` counts as a lost session only once a heartbeat to that node finds the session gone: a node answers it for other reasons too (a catalog CSV cannot encode, say), and then its own error stands.
6. **Export** — fold the Plan frames into one job per `node_id` and run up to `--parallelism` workers, one `Export` stream per node ([Full workflow](#47-full-workflow)). The nodes, sorted by id, are dealt round-robin over the usable liaisons, so node `i` streams through liaison `i mod L`; a stream that cannot be opened (`UNAVAILABLE`, `RESOURCE_EXHAUSTED`, or `NOT_FOUND` from a liaison that does not see the node) moves on to the next liaison within `--send-retry-timeout`. Each file lands as `<path>.tmp`, is checked against its `file_finished` (size and CRC32, and for CSV also the schema mod revisions the node encoded with; a native file binds no column to the schema, so its revisions are not checked), fsynced, renamed, and its directory fsynced. The first failing worker cancels the others.
7. **Write the manifest** — once every node finished, `manifest.json` is built from what was received and written atomically ([manifest.json](#45-manifestjson)); every warning is also printed as a `WARN` line.
8. **Release** — `Sessions(ACTION_RELEASE)`, then the progress file and its plan side file (`<progress file without .json>-plan.json`) are deleted; in this order a crash in between leaves the progress file, and the same command finishes the export again without the session. A failed release is a warning only: the artifact is complete and the lease reclaims the snapshot. The client then prints what was actually written — one `NODE CATALOG GROUP STAGE UNITS FILES ROWS SIZE TIME` row per node, catalog, group and stage (a property row shows its unit's stage, that of the unit's first source; `TIME` adds up the time its units took) — and one line with the units, files, bytes and warnings written and the time of the whole export. The snapshot is released at this point, so the dry run's `SNAPSHOT` budget line is not repeated.

Once the session exists, a failure keeps it and prints `bydbctl data release-session --id <id>`; the run exits `3`. Files already renamed stay in the output directory, a file in flight stays as `.tmp`, and no `manifest.json` is written, so a partial directory never passes for an artifact. Ctrl-C or `SIGTERM` is handled the same way: the export stops, the session is kept and the same `bydbctl data release-session --id <id>` line is printed; how the interrupted export is continued, and what a second Ctrl-C does, is in [7.2.2](#722-resume-flow). After a successful export, a failed release (as a whole or on some node) is a `WARN` with exit `0`, followed by the same `bydbctl data release-session --id <id>` line.

The export side performs **no source selection**. Every source node's copy of a unit is carried away into its own `nodes/<node-dir>/` subtree, and all multi-source judgement happens on the import side ([Multiple shard sources](#6-multiple-shard-sources)). This is deliberate: a wrong judgement on the export side loses bytes permanently, while a wrong judgement on the import side costs a re-run. Every source node is included, but a CSV copy is a reconstruction, not a byte copy: rows or values the format cannot represent under the current schema are left out with a warning ([CSV export](#43-csv-export)), and the export still succeeds. An export that exits `0` is therefore complete for migration only when `manifest.warnings` is empty, or when every warning in it is acceptable.

#### 4.1.1 Node IDs are not path-safe

A node ID is **not** a friendly name. `GenerateNode` builds it as `net.JoinHostPort(host, port)` (`api/common/id.go`), so a normal ID is `10.0.0.5:17912`, and an IPv6 one is `[::1]:17912`. Those contain `:`, `[` and `]`, which are illegal in a Windows path component and awkward everywhere. The artifact must therefore never use the raw ID as a directory name.

Two values are kept, and they have different jobs:

| | Value | Where it lives |
|---|---|---|
| **Raw ID** | `10.0.0.5:17912` | `manifest.source.nodes[].id`, and every `node_id` field on the wire. This is the identity |
| **Directory component** | `10-0-0-5-17912-8a05db98` | `manifest.source.nodes[].dir`, and the `nodes/<node-dir>/` path. This is only a filename |

The directory component is derived from the raw ID by:

1. lowercase the raw ID;
2. replace every run of characters outside `[a-z0-9]` with a single `-`;
3. truncate to 32 characters;
4. trim leading and trailing `-`; if nothing is left, use `node`;
5. append `-` followed by the first 8 hex characters of `SHA-256` over the **raw** UTF-8 bytes.

**Reversal is by lookup in the manifest, not by decoding.** Steps 1 to 3 are lossy on purpose — the bounded readable prefix exists so an operator can tell subtrees apart, and the manifest carries the exact ID for anything that needs it. Import resolves a subtree by matching `dir`, and uses the paired `id` wherever identity matters.

The hash suffix keeps IDs apart that the readable prefix alone would merge:

| Raw IDs | Directory component |
|---|---|
| `10.0.0.5:17912` | `10-0-0-5-17912-8a05db98` |
| `[::1]:17912` | `1-17912-…`, usable where the raw ID is not |
| `Data-1:17912`, `data-1:17912` | `data-1-17912-03d2b6d6`, `data-1-17912-98a53f33` — macOS and Windows fold case, the suffix does not |
| `a.b:17912`, `a-b:17912` | `a-b-17912-613e0e89`, `a-b-17912-b5233c34` |
| two ports on one host name longer than 32 characters | one prefix, distinct suffixes |

The client still refuses the export if two nodes in one `Plan` produce the same directory component, naming both IDs, so a 32-bit hash collision fails the export instead of mixing two nodes' files in one subtree.

### 4.2 Native export

> **As implemented (2026-11-03).** `bydbctl data export --format native` exports `.bnc` containers through the same session, `Plan` and `Export` RPCs as CSV; the data node's native producer (`banyand/transfer/export/native`) packs each unit straight from the session snapshot. A segment unit gives `<group>/seg-<suffix>/metadata` (the raw segment metadata file), `sidx-NNNNN.bnc` (the segment series index, when the snapshot holds one) and one `shard-<N>-NNNNN.bnc` per shard; a shard container holds the parts the newest snapshot file names (and their trace secondary index parts), that snapshot file, the files of the newest committed generation of the stream element index `idx/` and any other file in the shard directory, so the leftovers a closed segment can carry (unnamed parts, older snapshot files, older `idx/` generations) are not exported. A property unit gives one `shard-<N>-NNNNN.bnc` per shard. Compression is always `none` (`--compress gzip` with native is exit `1`); `--max-file-size` splits containers into volumes and must be 0 or at least 64 KiB. `FileFinished.entry_count` is filled; `FileFinished.row_counts` stays empty, so the manifest takes a shard's `totalCount` from the `Plan` statistics. With `--format native`, `--compress` other than `none`, `--node-row-rate-limit` and `--csv-annotations` are exit `1` when given as a flag or in `plan.yaml`, and are ignored when they come from `BYDBCTL_*` environment variables. `FileStarted.segment_level` marks the segment metadata file and the `sidx-NNNNN.bnc` volumes, and `FileStarted.seq` is the volume number within its container. The progress lines of a native export print no row counts. The table and the BNC format below are the design it follows.

Exporting in native format:

1. **Create snapshot** for the required data types.
2. **Traverse segments** of every group.
3. **Transfer data** — read files per the gRPC protocol and write them into BNC (BanyanDB Native Container) files in the stream, one frame per 1 MiB. Rate limiting applies here.
4. **Complete export** — finish the gRPC stream.

The storage layout differs by data type and mode.

| Type | Source directory | Output |
|---|---|---|
| Normal group | `<group>/seg-20260907/metadata` | `metadata` (raw file, not packed into a container) |
| | `<group>/seg-20260907/sidx` | `sidx-00001.bnc` (segment-level container, one per segment) |
| | `<group>/seg-20260907/shard-0` | `shard-0-00001.bnc` (shard-level container, one per shard) |
| Index-mode measure | `<group>/seg-20260907/metadata` | `metadata` |
| | `<group>/seg-20260907/sidx` | `sidx-00001.bnc` |
| Property | `property/data/<group>/shard-0` | `shard-0-00001.bnc` |

Two different directories are called `sidx`, and they are not the same thing:

| Source path | Actual content | Container |
|---|---|---|
| `seg-XXX/sidx/` | **Series index** — a native index directory (`%012x.seg` segments and a `%012x.snp` manifest), no parts | segment-level `sidx-NNNNN.bnc` |
| `shard-N/sidx/<rule>/<016x>/` | **Trace secondary index parts** — real parts, one directory per index rule, each with `manifest.json` | shard-level `shard-N-NNNNN.bnc` |

#### 4.2.1 BNC file format

The container library is `pkg/transfer/format/bnc`; its package documentation is the authoritative definition. All integers are little endian:

```text
[member 0][member 1]…[member n-1][footer][footer CRC-32 u32][footer length u32]["BNCA"]

footer:
  uvarint volume_seq     number of this volume in its container, from 1
  byte    last           1 when this is the last volume of the container, else 0
  uvarint count          number of members, at least 1
  repeat count times:
    uvarint path_len
    bytes   path         UTF-8, '/'-separated, relative to the container root
    u64     size         bytes of this member
    u32     crc32        IEEE CRC-32 of those bytes
    uvarint file_offset  where these bytes start in their file, 0 for a whole file
    uvarint file_size    size of the whole file, equal to size for a whole file
```

- **No offsets are stored.** A member starts where the one before it ends, so overlaps, gaps and out-of-range offsets cannot be expressed; the members must add up to the file size minus the footer and the 12-byte trailer. Every uvarint is minimally encoded, so a footer has exactly one encoding.
- **The magic carries the version.** It is `BNC` plus a single format-version letter, so a future incompatible layout becomes `BNCB` without needing a separate version field.
- **Opening reads the tail only.** Two reads (trailer, then footer) check the magic, the footer CRC-32, every bound before allocating, every path, and where each member sits in its file. Member bytes are checked against their CRC-32s when they are read or verified.
- **Limits:** footer ≤ 16 MiB, at most 2^20 members per volume, paths ≤ 4096 bytes with components ≤ 255 bytes. Zero-length files are allowed (CRC-32 0); no member is compressed, since column blocks are already zstd.

**Volumes.** `max_file_size` caps every volume file on disk, footer and trailer included; `0` means no limit, and any other value below 64 KiB is refused. Each volume is a complete container named `<prefix>-00001.bnc`, `-00002.bnc`, … (five digits, at most 99999). The writer fills each volume up to the limit regardless of part boundaries; **a file that crosses the end of a volume is split**: its first piece is the last member of that volume and the rest continues as the first member of the next one, so one file — and one part — can span several volumes. `file_offset`/`file_size` place every piece in its file, and each piece has its own CRC-32.

**Reading.** A reader opens all volumes of one container, in sequence order, as one volume set. The set requires volume numbers 1, 2, … with `last` set on the final volume only, and every unfinished file at the end of a volume to continue at exactly the next byte as the first member of the next one. A missing, reordered or repeated volume, a missing tail or a volume of another container is therefore rejected before any data is read. Files are then read whole across volumes; the read that returns the last byte of a corrupt piece fails, and closing the reader reports that failure again, because helpers that copy an exact length (`io.CopyN`, `io.ReadFull`) drop it.

**What the container cannot prove.** A container carries no identity — a valid container from another artifact, unit or node passes every check — and a prefix that ends at an inner boundary (a first member that is itself a container, or volumes whose last one says it is last) opens and verifies. A file the producer never packed leaves nothing to detect. The manifest is the final authority: the importer compares the size and CRC-32 of every volume with the manifest entry of that path before it trusts a container.

There are only three gRPC frame types:

```text
file_started{ relative_path, shard_id, segment_level, shard_index, seq }
                                           → Client creates <path>.tmp
chunk{ content, rows } × N                 → Client appends data and updates the progress
file_finished{ bytes, crc32, entry_count, row_counts, schema_mod_revisions }
                                           → Client validates → fsync → rename
```

### 4.3 CSV export

Each data node produces CSV straight from the session snapshot, read-only (`banyand/transfer/export`): parts are opened through the dump readers `banyand/internal/dump/{stream,measure,trace}` over a read-only file system, and the segment series index and the property store through `pkg/index/native`'s `OpenReadOnlyGeneration`, which takes no lock and writes nothing into the snapshot. Directories are listed with `os.ReadDir`. Per unit:

1. **Locate** the unit in the snapshot: `<root>/<group>/seg-<suffix>/shard-<N>` (property: `<root>/<group>/shard-<N>`). A missing segment or requested shard directory is `INTERNAL`: the snapshot is frozen, so that is a node-side fault. A file that is missing when it is read is taken to mean the session was preempted or released, and the stream fails with `FAILED_PRECONDITION`; the session is checked once more before `unit_finished`, so a release or takeover during the unit is caught even when no read failed. This covers detected read failures and session loss only, not snapshot damage in general: a snapshot-listed part directory that vanished before the unit was listed is skipped like a merge source, and a shard whose snapshot file vanished lists no parts. As a lightweight consistency check, `unit_started.total_parts` is counted the same way as the session `Plan`, which inventoried the same snapshot, and the client fails the export (exit `3`) when the two differ. A shard's live parts are the ones its newest readable `<epoch>.snp` names, as when the engine opens the table: a closed segment is linked into the snapshot as a whole, so it can still hold the source parts of a merge the engine had not yet removed, and those are skipped. The Plan inventory counts parts the same way.
2. **Build the column layout** of the group from the node's current schema ([Columns](#433-columns)). A group the layout cannot describe — a `TIMESTAMP` tag in a stream or measure, or two traces of one group with the same tag set — is refused with `FAILED_PRECONDITION`.
3. **Encode rows** — one CSV stream per shard (index-mode measure: one per segment), gzipped on the fly unless `compress` is `NONE`, sent as chunks of at most 1 MiB of on-disk bytes. A stream rolls to the next file after a complete row once the file's on-disk bytes reach `max_file_size` (`0` never rolls), and every file repeats the header. A row whose encoded size reaches the limit gets a file of its own, and the unit reports the number of such rows in one warning.
4. **Finish the unit** — `unit_finished` carries the unit's warnings. A unit without any row produces no file, and an empty file is never written. The warning `unit has no rows` is given only when the unit listed shards, whose parts then yielded no row; a unit without shards (a segment the engine created ahead of time) is silent.

How each model is read:

| Model | Rows come from | `_name` |
|---|---|---|
| stream | the parts of every requested shard | the series subject decoded from the row's `EntityValues`; the entity tags are filled from the same series. The segment series index stands in for parts without `smeta.bin` |
| measure | as stream; indexed tags, which the write path keeps only in the series index, are decoded from it | as stream |
| index-mode measure | the documents of the segment-level series index (`seg-<suffix>/sidx`), one row per document of an index-mode measure; the series entries of the group's regular measures in the same index are skipped | the document's subject |
| trace | the parts of every requested shard | matched by the row's tag-name set, without the trace-id and span-id tags and without tags no trace of the group declares, against the group's traces. A group with one trace takes it directly; a row whose set equals one trace's takes it; otherwise a row (from a part written before a tag was removed) goes to the only trace whose tags include all of its tags, and a row that no trace or several traces include is dropped and counted; two traces with the same tag set make the group fail (use native, once it exists). Matching by tag set cannot tell an old row from a row of another trace with the same tags |
| property | the live documents of every shard's store, decoded from `_source` and streamed one at a time; tombstones included | `Property.metadata.name` |

`_top_n_result` is exported like any other measure: its binary `value` field carries the engine-internal Top-N encoding, base64-encoded. A point stored in several versions is exported as **one row per point**, the versions merged the way a query or a part merge combines them (`MergeTopNResults` in `banyand/measure/export_topn.go`), with the highest version. The import writes those rows verbatim and never recomputes them. The warnings report dropped rows and why, stored tag and field values the current schema no longer declares or declares with another type, and property timestamps outside the int64 nanosecond range (counted per resource and tag), tags only a multi-tag index rule stores, a single row larger than the file size limit, and a unit whose shards yielded no row. A column that is null on every row is not reported: the source schemas commonly declare tags they never fill. None of them fails the export; all of them land in `manifest.warnings`, and each of them is data the artifact does not carry.

**The schema the rows were encoded with is checked.** Columns are bound by position and a group's layout is built from all of its resources, so every `file_finished` carries the `ModRevision` of every resource the node built the layout from, whether or not it wrote rows. The client compares that set with the revisions it saved in `schema/` (including `_top_n_result`); a missing or extra resource or any different revision fails the export (exit `3`, "schema … changed during export; export again"). With `--include-schema=false` there is nothing to compare against and the check is skipped.

**Rate limiting has three knobs; two are enforced by the data node, one by the liaison.** All three are set by the client and travel in `init`; neither server has a configuration of its own for them. They throttle different resources, so none subsumes another and whichever binds first wins:

| Knob | Flag / plan field | Protobuf field | Measured |
|---|---|---|---|
| Node bytes | `--node-rate-limit` / `export.nodeRateLimit` | `RateLimit.bytes_per_second` | Each chunk before it is sent, so **after** gzip |
| Node rows | `--node-row-rate-limit` / `export.nodeRowRateLimit` | `RateLimit.rows_per_second` | Each row the producer encodes. CSV only (a usage error with native): native moves opaque bytes and has no row concept |
| Liaison bytes | `--liaison-rate-limit` / `connection.rateLimit` | `Init.liaison_bytes_per_second` | Each chunk the liaison relays, waited for before it is forwarded to the client; below 1 MiB/s a chunk is forwarded in pieces of one second's worth, each waited for on its own. Nothing is prefetched, so the wait back-pressures the node |

The node knobs travel in `init.rate_limit` and apply to that one stream, that is, per data node. The liaison knob is **process-wide on each liaison**: every Export stream a liaison relays shares one bucket, and the value of the stream opened last wins — `0` included, which lifts the limit — until no Export stream is left, when the bucket goes back to unlimited. One export session at a time per cluster means every stream on a liaison normally carries the same value. With several liaisons in `--nodes` each enforces the value on its own share, so the total can reach `L ×` the limit. A node bucket's burst is one second's worth (at least one row), and a node byte rate below 1 MiB/s sends chunks of one second's worth, so a chunk always fits the burst; a node about to wait for its row bucket first sends the rows it holds, flushing gzip — right away when the wait is 100 ms or longer, so a low row rate delivers every row as it is written, otherwise once they are a second old — so a throttled stream still delivers a chunk at least about every second; the liaison bucket's burst is one second's worth too, at most one chunk (1 MiB), and a larger chunk is relayed in pieces of the burst's share of one relay (the burst divided by the relays in flight) carrying the chunk's `rows`, with its bytes in order, so every node stream still gets a frame about every second; `0` disables a knob entirely. The byte knobs protect the liaison and the network; the row knob protects the data node, whose cost for CSV is row iteration rather than transfer. A CSV export that sets only a byte limit can still saturate a data node's CPU on a highly compressible group, which is why the row knob exists as well as, not instead of, the byte knob.

Rates take `<quantity>[/<period>]`: the period is `s` (the default), `min` or `h` (`m` is refused as ambiguous); a byte quantity is bare bytes or a size unit (`50MiB/s`, `3GiB/min`), a row quantity a plain integer (`50000/s`). A negative rate, a byte unit on a row rate, and a non-zero rate below one unit per second are usage errors (exit `1`).

#### 4.3.1 Value encoding

| Type | Encoding |
|---|---|
| STRING (also `trace_id`, `span_id`, property `_id`) | Written via `encoding/csv`; a value containing `\r` is downgraded to `\B` + base64, and a value starting with a backslash gets one extra leading backslash ([the escape algorithm](#432-escape-algorithm)) |
| INT (also `_version`, `_mod_revision`, `_create_revision`) | Decimal |
| `_element_id` | The 16 lowercase hex digits of the stored uint64 element id's big-endian bytes, the form a stream query returns as `element_id` (`csvfmt.EncodeElementID`). An import parses it back with `csvfmt.DecodeElementID` and replays the row with that value as `raw_element_id`, so the target stores the same id |
| FLOAT (measure field) | `strconv.FormatFloat(v, 'f', -1, 64)`. Precision **must** be `-1`, or the round trip loses precision. NaN and infinities are written `NaN`, `+Inf`, `-Inf` |
| DATA_BINARY / `_span` payload | `base64.StdEncoding`, standard alphabet, with padding; an empty value is an empty cell, distinct from null |
| STRING_ARRAY | JSON array string (HTML characters not escaped; invalid UTF-8 becomes U+FFFD) |
| INT_ARRAY | Semicolon-separated decimal int64 values; an empty array is an empty string |
| All time columns (`_timestamp`, `_updated_at`, `_delete_time`) and TIMESTAMP tags | Decimal int64 UnixNano. TIMESTAMP tags exist only for trace and property; a stream or measure group declaring one is refused |
| null | The `\N` sentinel; a cell whose value the row does not carry is null. Literals are kept distinct by the leading-backslash rule in [the escape algorithm](#432-escape-algorithm) |

> **NaN and ±Inf are exported.** `encodeFloat64Column` cannot encode them losslessly and falls back to `encodeDefault`, which writes a dictionary or plain block beneath an outer plain marker. The measure dump reader now decodes measure columns the way the engine does (`readColumnValues`, mirroring `column.decodeColumnValues`), so such blocks read back and the values reach the CSV as `NaN` / `+Inf` / `-Inf`, which `ParseFloat` accepts.

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

The three written prefixes `\N` / `\B…` / `\\…` are pairwise disjoint and the reader accepts every cell the writer produces, so the mapping is injective.

Round-trip cases that must be in the test suite, each asserted byte-for-byte:

| Original value | On disk | Read back |
|---|---|---|
| null | `\N` | null |
| `\N` | `\\N` | `\N` |
| `\\N` | `\\\N` | `\\N` |
| `\Bx` | `\\Bx` | `\Bx` |
| `\\Bx` | `\\\Bx` | `\\Bx` |
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

A group has **one** layout, shared by every file of every unit and by all of its resources: the fixed columns, then the union of the resources' tag columns (`family.tag` for stream and measure, the bare tag name for trace and property), then, for measures, the union of the `_field.<name>` columns. Resources are taken in name order (`_top_n_result` included) and tags in declaration order; the first occurrence fixes a column's position, and a row leaves the columns of other resources null (`\N`). When two resources declare the same tag or field with different types, or its header would repeat another column's (a fixed column, a `_field.<name>` column, or family `a` tag `b.c` against family `a.b` tag `c`), each resource declaring it gets its own column headed `<resource>::<family.tag>` (or `<resource>::_field.<name>`), so every header is unique. Every measure group holds the auto-created `_top_n_result`, whose binary field is named `value`, so a group whose measures declare a numeric `value` field gets split headers such as `cpm::_field.value` and `_top_n_result::_field.value`. The layout is a pure function of the schema (`pkg/transfer/format/csvfmt`), so an importer rebuilds it from `schema/`.

With `--csv-annotations` (`init.csv_annotations`) every file starts with one extra line before the header: `#datatype`, then one token per column — `string`, `long`, `double`, `base64Binary`, `stringArray`, `longArray` or `timestamp`. That line has one field more than the header, so a reader must accept a variable number of fields per record.

**stream**

```csv
_name,_element_id,_timestamp,searchable.trace_id,searchable.service_id,searchable.state,data.data_binary
sw_service_log,000000110ff1a035,1783419300123000000,af1c...beef,c2VydmljZS1h,1,ZGF0YQ==
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
| Normal group | `seg-<suffix>/metadata` (raw file)<br>`seg-<suffix>/sidx-00001.bnc`<br>`seg-<suffix>/shard-<N>-00001.bnc` | `seg-<suffix>/shard-<N>-00001.csv.gz` |
| Index-mode measure | `seg-<suffix>/metadata`<br>`seg-<suffix>/sidx-00001.bnc` | `seg-<suffix>/segment-00001.csv.gz` |
| Property | `shard-<N>-00001.bnc` (no `seg-` layer) | `shard-<N>-00001.csv.gz` (no `seg-` layer) |
| File splitting | `-00001`, `-00002`, …; each volume is a complete container of at most `max_file_size` bytes, and a file crossing the end of a volume continues in the next | `-00001`, `-00002`, …; each volume repeats the header |

In CSV, `--compress none` drops the `.gz` suffix (`shard-0-00001.csv`). The `<catalog>` component is lowercase (`stream`, `measure`, `trace`, `property`), and the group name is escaped into a safe path component: every byte outside `[A-Za-z0-9._-]` becomes `%XX` (upper-case hex), and the names `.` and `..` are escaped in full. The client also rejects any `relative_path` from a node that is absolute, contains `..` or a backslash, or lies outside the unit's `<catalog>/<group>/` prefix, and refuses to overwrite an existing file. Directories are created `0700` and files `0600`.

#### 4.4.1 Schema files

`schema/` always holds eight JSON Lines files, one per kind, in this order: `group.jsonl`, `stream.jsonl`, `measure.jsonl`, `trace.jsonl`, `property.jsonl`, `indexRule.jsonl`, `indexRuleBinding.jsonl`, `topNAggregation.jsonl`. Each line is one resource as protojson with unpopulated fields emitted. Only the selected groups and their resources are listed, and only the kinds a group's catalog can hold (index rules and bindings for stream, measure and trace; Top-N aggregations for measure). A kind with nothing to list still gets an empty file, so the file set does not depend on the data. The files are listed with their size and CRC32 in `manifest.schemaFiles`.

`--include-schema=false` (or `export.includeSchema: false`) leaves the schema out entirely: no registry listing beyond the groups, no `schema/` files (`schemaFiles: []`), no mod revision check, and `groups[].indexMode` is recorded as `false` with a manifest warning that it is unknown. An import then reads the CSV columns with the target's schema, which must be the schema the rows were exported with (see [CSV import as implemented](#54-csv-import-as-implemented)). `--schema-only` together with `--include-schema=false` is a usage error. `--schema-only` writes `schema/` and a `manifest.json` with `"schemaOnly": true` and no units, without creating a session.

### 4.5 manifest.json

`manifest.json` records the format, export time, source cluster, group information, and per-unit results. It is written last, atomically (temporary file, fsync, rename, fsync of the directory), mode `0600`; until it exists the directory is not an artifact. A CSV export writes:

```json
{
  "artifactVersion": 1,
  "format": "csv",
  "compress": "gzip",
  "exportedAt": "2026-09-07T12:00:00Z",
  "source": {
    "nodes": [{"id": "10.0.0.5:17912", "dir": "10-0-0-5-17912-8a05db98",
               "timezone": "Asia/Shanghai"}]
  },
  "groups": [{
    "name": "sw_metric", "catalog": "MEASURE", "indexMode": false,
    "stages": [{"name": "hot", "shardNum": 2, "segmentInterval": "1d", "ttl": "7d", "replicas": 1}]
  }],
  "units": [{
    "catalog": "MEASURE", "group": "sw_metric", "stage": "hot", "segment": "20260907",
    "segmentVersion": "1.5.0",
    "minTimestamp": 1788739200000000000, "maxTimestamp": 1788825599999999999,
    "sources": [{
      "node": "10.0.0.5:17912",
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
        "rowCounts": {"service_cpm_minute": 100000, "service_resp_time": 23456},
        "estimatedCompressedBytes": 10737418,
        "estimatedUncompressedBytes": 41943040,
        "files": [
          {"path": "nodes/10-0-0-5-17912-8a05db98/measure/sw_metric/seg-20260907/shard-0-00001.csv.gz",
           "bytes": 536871012, "crc32": "0x9f2a1b34"},
          {"path": "nodes/10-0-0-5-17912-8a05db98/measure/sw_metric/seg-20260907/shard-0-00002.csv.gz",
           "bytes": 11834400, "crc32": "0x77e0aa15"}
        ]
      }]
    }]
  }],
  "schemaFiles": [{"path": "schema/group.jsonl", "bytes": 812, "crc32": "0x05f5e0fe"}],
  "warnings": []
}
```

- `stages` lists the group-level options first, named `hot` (the default tier), then every declared lifecycle stage; `stages[0]` is always the group's own opts, and one lifecycle stage may also be named `hot`. A stage records its own `shardNum`, `replicas` (`0` means no replicas) and `segmentInterval`; its `ttl` is the stage's own, not the cumulative effective TTL. `indexMode` is true when the group holds an index-mode measure.
- A segment unit is keyed by `(catalog, group, stage, segment)` and holds one source per node that exported it. A property unit's key degrades to `(catalog, group)`: every node's shards of the group are sources of one unit, whose `stage` is the stage of its first source (in node-id order). Property units have no `segment` and no time range; their shards carry `docCount` (the Plan's document count) and an empty `parts`. An index-mode measure unit has its rows in `segmentFiles` only and no shards; its time range is the one the series index records (`SidxStat.min_timestamp`/`max_timestamp`), and is zero only when the index is missing or unflushed.
- `compress` is `gzip` or `none`; an unspecified compression is the CSV default `gzip`.
- In a CSV artifact, a shard's `totalCount` and `rowCounts` count the rows actually written to its files, per resource, as the data node reported each file in `file_finished.row_counts`; the Plan's statistics only feed `parts[]` and the `estimated*` sizes. Files carry no row counts of their own: `--verify` and the multiple-source judgement only use the totals, and a file's integrity is its `bytes` and `crc32`.
- An index-mode measure's rows are segment-level: its files are listed in the source's `segmentFiles` rather than under a shard, and the source carries their `rowCounts` and `totalCount`.
- `schemaOnly: true` marks a `--schema-only` artifact, which has no units.
- `warnings` collects every unit warning, prefixed with the node and unit, plus the stage warnings of the dry run.

The times of a run (`startedAt`, `updatedAt` and `finishedAt` in the progress files and the command output) are RFC 3339 times in the local time zone of the machine running `bydbctl`, with their offset; the times of the data (`TIME-RANGE`, the segment suffixes) and the `exportedAt` of `manifest.json`, which is the start of the export, are UTC (`…Z`).

A native export writes the same layout with `"format": "native"` and `"compress": "none"`. A shard's `totalCount` is the Plan's aggregate `ShardStat.total_count` (split pieces already merged), its `parts` come from the Plan's part statistics and its `rowCounts` is empty. A segment source's `totalCount` is the Plan's `SidxStat.doc_count`, computed only when the group declares an index-mode measure: an upper bound of the index-mode rows, since it also counts the series entries of the group's regular measures. The unit's `minTimestamp`/`maxTimestamp` also cover `SidxStat.min_timestamp`/`max_timestamp`, the range the series index records for those rows (both formats), so an index-mode unit without shards still has a time range. A property shard's `totalCount` is its `doc_count`, which includes superseded versions and tombstones. Every container volume carries `entryCount` (required, above 0; the import refuses a volume without one), and the segment-level files sit in the source's `segmentFiles` (abridged):

```json
{
  "artifactVersion": 1, "format": "native", "compress": "none",
  "units": [{
    "catalog": "MEASURE", "group": "sw_metric", "stage": "hot", "segment": "20260907",
    "sources": [{
      "node": "10.0.0.5:17912",
      "segmentFiles": [
        {"path": "nodes/10-0-0-5-17912-8a05db98/measure/sw_metric/seg-20260907/metadata",
         "bytes": 24, "crc32": "0x1c2d3e4f"},
        {"path": "nodes/10-0-0-5-17912-8a05db98/measure/sw_metric/seg-20260907/sidx-00001.bnc",
         "bytes": 1048576, "crc32": "0x5a6b7c8d", "entryCount": 12}
      ],
      "shards": [{
        "shard": 0, "totalCount": 123456, "parts": [{"id": 1, "totalCount": 123456}],
        "rowCounts": {},
        "files": [
          {"path": "nodes/10-0-0-5-17912-8a05db98/measure/sw_metric/seg-20260907/shard-0-00001.bnc",
           "bytes": 536870912, "crc32": "0x9f2a1b34", "entryCount": 48}
        ]
      }]
    }]
  }]
}
```

A native export records a segment's raw segment-level files (`metadata`, `sidx-NNNNN.bnc`) in the source's `segmentFiles`, and every container volume carries `entryCount`, the footer entry count of that volume (a piece of a split file counts as one). `sidxCoversShards` is not recorded: the import derives series-index targets from the unit's shards, see [Native import as implemented](#55-native-import-as-implemented).

`shards[].parts[]` is written **unconditionally**, for every shard of every source. The export side does not decide which shards need it: that array is the input to the import-side multi-source judgement ([Detection basis](#62-detection-basis)), and a shard that looks single-sourced today becomes multi-sourced as soon as a second artifact is merged in.

> **As implemented:** `sidxCoversShards` below is the original design and is not written to the manifest; the import sends a segment's series index to the union of the nodes of the unit's shards, or to every node of the target stage when the unit has none ([Native import as implemented](#55-native-import-as-implemented)).

`sidxCoversShards` (native only) is the **union of the route-derived shard set and the shard directories actually present on disk**. It is used **only to work out which nodes must receive this segment's series index** ([Data transfer](#525-data-transfer)); it never selects *which sources* to push — that follows the core-part selection ([Multiple-source push behavior](#64-multiple-source-push-behavior)). Both halves of the union are required:

- Using only the disk fails for index-mode measure, which has no shard directories at all even though the segment-level sidx is its only payload.
- Using only the routing table fails after topology drift: a node can route-own shard `[1]` while physically holding shard `[3]`, with an empty intersection.
- The union costs nothing. Over-reporting a shard opens one extra stream whose bytes are absorbed by receiver-side deduplication; under-reporting makes `Lookup(series)` miss and renders the whole segment unqueryable.

### 4.6 Protocol

A new gRPC export service carries the workflow. The data node generates the stream directly and the client writes it to local files. The same service backs dry run.

> The listings below are illustrative. Field numbers, validation rules and comments are authoritative in `api/proto/banyandb/transfer/v1/export.proto`.

```protobuf
service ExportService {
  // Plan enumerates the per-segment units (per-group for property) a selector hits.
  // Its behavior is determined by the request fields.
  rpc Plan(PlanRequest) returns (stream PlanResponse);

  // Sessions runs one lifecycle action (list, heartbeat, release) on every data node
  // and streams one frame per node, stamped with node_id by the liaison.
  rpc Sessions(SessionsRequest) returns (stream SessionsResponse);

  // Export streams the files of one data node's units out of an export session
  // snapshot. The first request frame is init; every later frame carries exactly one
  // unit. The node produces units one at a time in request order, and the client may
  // send the next unit before unit_finished for the previous one. The liaison forwards the
  // stream to init.target_node unchanged. Export neither creates nor removes the snapshot.
  rpc Export(stream ExportRequest) returns (stream ExportResponse);
}

message Selector {
  common.v1.Catalog catalog = 1;
  repeated string groups = 2;
}

// SegmentUnit is the key of a physical unit of a stream, measure or trace group.
message SegmentUnit {
  common.v1.Catalog catalog = 1;  // never PROPERTY
  string group = 2;

  // Example: "20260907" (day) or "2026090712" (hour).
  string segment_suffix = 3;

  repeated uint32 shard_ids = 4;
}

// UnitInventory is one unit returned by Plan. Property groups have no segments, so they
// are inventoried per group instead of per segment.
message UnitInventory {
  oneof kind {
    SegmentInventory segment = 1;
    PropertyInventory property = 2;
  }
}

// SegmentInventory is a SegmentUnit with the statistics calculated by the server.
message SegmentInventory {
  SegmentUnit unit = 1;

  // Version from the segment root metadata file.
  // Used as the version gate during import.
  string segment_version = 2;

  // Per-shard statistics.
  repeated ShardStat shards = 3;

  // Segment-level series index (the sidx/ directory).
  SidxStat segment_level = 4;
}

// PropertyInventory is one property group on one node: its shards, each a document store
// without parts or timestamps.
message PropertyInventory {
  string group = 1;
  repeated PropertyShardStat shards = 2;
}

message PropertyShardStat {
  uint32 shard_id = 1;
  // On-disk size of the shard's document store: property has no compressed form, so this
  // is both the compressed and the uncompressed estimate.
  uint64 estimated_bytes = 2;
  // Documents in the store, including superseded versions and tombstones.
  uint64 doc_count = 3;
}

// SidxStat describes the segment-level series index.
message SidxStat {
  uint64 estimated_bytes = 1;
  // Documents of the series index, computed only for groups that hold an index-mode measure
  // (0 otherwise). Those documents are the index-mode rows plus the series entries of the
  // group's regular measures, so the value is an upper bound of the rows.
  uint64 doc_count = 2;
  // "_timestamp" range the index's segment footers record, read alongside doc_count; also a
  // superset (deleted documents and series entries count too). 0 when not computed or absent.
  int64 min_timestamp = 3;
  int64 max_timestamp = 4;
}

// ShardStat aggregates the parts of one shard. For trace, estimated_uncompressed_bytes
// counts the span payload only (tags are excluded), so it can be below
// estimated_compressed_bytes; estimated_compressed_bytes is the bytes the part writer
// recorded and is a lower bound of the on-disk size.
//
// A shard whose part list is too large for one frame arrives as several ShardStat pieces
// with the same shard_id and disjoint parts. Only the first piece carries the aggregates
// (fields 2-7); the others set only shard_id and parts.
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

// SessionsRequest runs one session lifecycle action on every data node; the liaison
// streams back one SessionsResponse per node.
message SessionsRequest {
  enum Action {
    ACTION_UNSPECIFIED = 0;
    // Read-only: every node reports the session it holds (the newest) or none. An unreachable node
    // fails the whole call, so "clean" and "silent" cannot look alike.
    ACTION_LIST = 1;
    // The heartbeat: renew the lease of session_id on every node.
    ACTION_HEARTBEAT = 2;
    // Delete the snapshot of session_id on every node; a node that does not hold it still
    // succeeds (answering `none`), so the action is idempotent.
    ACTION_RELEASE = 3;
  }
  Action action = 1;
  // Required by ACTION_HEARTBEAT and ACTION_RELEASE, ignored by ACTION_LIST.
  string session_id = 2;
  // ACTION_HEARTBEAT only: the nodes that must still hold a readable lease (the client's nodes
  // with units left); empty: every node. An expired or unreadable lease on another node is
  // that node's `error` frame instead of failing the call.
  repeated string required_nodes = 3;
}

// One frame per data node; node_id is stamped by the liaison. Exactly one outcome is set.
message SessionsResponse {
  string node_id = 1;
  oneof outcome {
    // ACTION_LIST: the session this node holds. One session id covers the whole cluster, so
    // a node reports at most one: the newest when leftovers of older sessions exist.
    SessionLease session = 2;
    // ACTION_LIST: the node holds no session. ACTION_RELEASE: the node did not hold
    // session_id, so there was nothing to delete; this is still a success.
    Ack none = 3;
    // ACTION_HEARTBEAT: the node renewed the lease. ACTION_RELEASE: the node held the session
    // and deleted it.
    Ack done = 4;
    // ACTION_HEARTBEAT and ACTION_RELEASE: the node did not complete the action; the frame is
    // still sent so the client can tell which nodes failed.
    // ACTION_HEARTBEAT: the node was unreachable, does not hold the session, or is a data
    // node without ExportService, or (only outside a non-empty required_nodes) its lease
    // expired or is unreadable. An expired or unreadable lease on any other node, or any other
    // node error, fails the whole call instead (FAILED_PRECONDITION for the lease).
    // ACTION_RELEASE: the node was unreachable, removing the session failed, or it is a data
    // node without ExportService. A node that does not hold the session answers `none`.
    string error = 5;
  }
}

// Ack is an outcome that carries no data.
message Ack {}

// Mirrors the .lease file plus the catalogs, which the node derives from the
// <export-snapshot-path>/<id> directories it finds. expires_at drives reclamation by the
// node's sweeper; last_heartbeat_at tells a refused operator when the holder was last seen.
// Times are Unix nanoseconds of the data node's clock; an unreadable .lease reports zero times.
message SessionLease {
  string session_id = 1;
  int64 started_at = 2;
  int64 expires_at = 3;
  int64 last_heartbeat_at = 4;
  repeated common.v1.Catalog catalogs = 5;
}

message PlanRequest {
  // Empty = all catalogs and groups in the cluster.
  repeated Selector selectors = 1;

  // Unset = inventory the online directories (a dry run).
  oneof session {
    CreateSession create = 2;  // snapshot first, then inventory that snapshot
    ReadSession read = 3;      // read the session's snapshot and renew its lease
  }
}

message CreateSession {
  // Empty at the liaison, which generates the id and forwards it; always set on a data node.
  string id = 1;
  // Remove every session the node holds, whatever its lease says, before creating this
  // one; the removed ids come back in preempted_session_ids. Without it any other session
  // on the node answers ALREADY_EXISTS naming the occupants. The client sets it for
  // --preempt.
  bool preempt = 2;
}

message ReadSession {
  string id = 1;  // required
  // The nodes that must still hold a readable lease (the client's nodes with units left);
  // empty: every node. An expired or unreadable lease on another node puts it in
  // unreachable_nodes instead of failing the call.
  repeated string required_nodes = 2;
}

// One frame of the Plan stream. Through the liaison the stream is: with CreateSession a
// `created` frame first; then `units` frames, each carrying units of exactly one
// (node, group); then exactly one `summary` frame. A data node's own stream follows the same order without `created`: it
// never sets node_id or answered_nodes, and its closing `summary`, carrying only
// preempted_session_ids, is sent only with CreateSession when anything was removed.
message PlanResponse {
  oneof frame {
    SessionCreated created = 1;
    UnitFrame units = 2;
    PlanSummary summary = 3;
  }
}

message SessionCreated {
  string session_id = 1;
}

message UnitFrame {
  string node_id = 1;  // stamped by the liaison
  // The node's resolved stage for this group; empty for the default tier (no stages, no
  // node labels, or no matching stage). Only groups the registry knows are listed.
  string stage = 2;
  repeated UnitInventory units = 3;
}

message PlanSummary {
  // Sessions removed while creating because preempt was set.
  repeated string preempted_session_ids = 1;
  // Live directories and ReadSession only: transport failure, liaison Plan max wait,
  // NotFound (no longer holds the session), FAILED_PRECONDITION on a node outside a non-empty
  // ReadSession.required_nodes, or a data node without ExportService.
  // CreateSession fails on any of these instead.
  repeated string unreachable_nodes = 2;
  // Nodes whose Plan stream completed; with unreachable_nodes it covers every planned node.
  repeated string answered_nodes = 3;
}

enum Format {
  FORMAT_UNSPECIFIED = 0;
  FORMAT_NATIVE = 1;  // .bnc containers, byte for byte; never compressed (ImportService.ImportParts imports it)
  FORMAT_CSV = 2;
}

enum Compress {
  COMPRESS_UNSPECIFIED = 0;  // refused: the client always names one
  COMPRESS_NONE = 1;
  COMPRESS_GZIP = 2;
}

// In transfer/v1/common.proto, shared by Export and ImportRows; 0 = unlimited. Enforced by
// the data node: per Export stream, and for ImportRows on every row the node is sent.
message RateLimit {
  uint64 bytes_per_second = 1;
  uint64 rows_per_second = 2;  // CSV only
}

// Property groups have no segments: the unit is the group's shards on one node.
message PropertyUnit {
  string group = 1;
  repeated uint32 shard_ids = 2;
}

message ExportRequest {
  message Init {
    string session_id = 1;    // 32 lowercase hex
    string target_node = 2;   // a node_id from the Plan frames; the liaison routes by it
    Format format = 3;        // required
    Compress compress = 4;    // required
    uint64 max_file_size = 5; // on-disk bytes, 0 = no split; CSV: soft (rolls after a row), NATIVE: hard cap per .bnc volume, 0 or >= 64 KiB
    RateLimit rate_limit = 6;
    bool csv_annotations = 7; // CSV only: a #datatype line before every header
    uint64 liaison_bytes_per_second = 8; // the relaying liaison's process-wide rate; last stream wins
  }
  oneof frame {               // required
    Init init = 1;            // first frame only
    SegmentUnit segment = 2;  // stream, measure, trace: SegmentInventory.unit, pieces of a split segment merged
    PropertyUnit property = 3;
  }
}

// Order: ready, then per unit: unit_started, (file_started, chunk*, file_finished)*, unit_finished.
message ExportResponse {
  oneof frame {
    Ready ready = 1;
    UnitStarted unit_started = 2;
    FileStarted file_started = 3;
    DataChunk chunk = 4;
    FileFinished file_finished = 5;
    UnitFinished unit_finished = 6;
  }
}

// The node located the session and renewed its lease; units may follow.
message Ready {}

message UnitStarted {
  uint32 total_shards = 1;           // 0 for segment-level-only units
  uint32 total_parts = 4;            // live parts of the unit's shards; 0 without parts
}

message FileStarted {
  string relative_path = 1;  // relative to nodes/<node-dir>/
  uint32 shard_id = 2;
  bool segment_level = 3;    // CSV index-mode measure rows; native the segment metadata file and sidx volumes
  uint32 shard_index = 4;    // index of shard_id in the unit's shard list
  uint32 seq = 5;            // from 1: CSV split number within the shard; native volume number within its container
}

message DataChunk {
  bytes content = 1;  // at most 1 MiB
  uint64 rows = 2;    // CSV only: rows written into the open file so far (progress only); native sends 0
}

message FileFinished {
  uint64 bytes = 1;                               // on-disk size
  uint32 crc32 = 2;                               // IEEE, over the on-disk bytes
  uint32 entry_count = 3;                         // native only; CSV sends 0
  map<string, uint64> row_counts = 4;             // CSV only: rows per resource in this file; native sends none
  map<string, int64> schema_mod_revisions = 5;    // CSV: schema revision of every resource in the layout
}

message UnitFinished {
  repeated string warnings = 1;
}
```

### 4.7 Full workflow

#### 4.7.1 Phase 1 — Plan: create session and inventory

| # | Direction | Frame | Liaison behavior |
|---|---|---|---|
| 1 | ctl → liaison | `Plan{selectors=[…], create{}}` | Generate one session id for the whole fan-out. A node already holding a different session rejects the create and returns the incumbent's ID ([why session uniqueness is per node](#481-there-is-no-coordinator-so-uniqueness-is-per-node)); any rejection or failure makes the liaison roll the new id back on the nodes that accepted it; nodes whose rollback failed are named in the error as leaked, and only those ids need a manual `bydbctl data release-session` |
| 2 | liaison → data-1 / data-2 | One `Plan{…, create{id="xx…"}}` per node | — |
| 3 | Each data node | Refuse if any other session directory exists (or remove them all when `preempt` is set), snapshot the selected groups of every selected catalog under `<export-snapshot-path>/<id>`, then commit the `.lease` in all of them; inventory from the snapshot | — |
| 4 | data-* → liaison → ctl | Stream `PlanResponse{units{node_id, stage, units[…]}}` frames. A data node bounds each frame to 4 MiB, so a large group spans several consecutive frames, and a segment too large for one frame is split into pieces with the same key and disjoint `shard_ids` (only the first piece carries `segment_level`). A single shard too large for one frame is split further into `ShardStat` pieces with the same `shard_id` and disjoint `parts`, in different segment pieces; only the first carries the aggregates. ctl merges the pieces back into one unit per (node, segment) as it collects the frames, concatenating each shard's parts and adding up its aggregates | Forward each frame as it arrives, stamping `node_id`, through one bounded queue drained by a single sender, so a client that stops reading cannot hold back a failed create's rollback; the sender also stops when the call ends, so a create rejected before any unit leaves nothing running; fold a node's `summary` (preempted ids) into the final one |
| 5 | ctl | Print the `session_id` (from the `created` frame) as soon as it arrives; record it in the progress file before anything else ([7.2.1](#721-progress-file)) | — |
| 6 | ctl | Group entries by `node_id` into one job per node, each with its own `nodes/<node-dir>/` subtree | — |

Only this phase creates snapshots; they are created once and reused for the whole export. The liaison stamps `node_id` on every frame from the upstream connection and does not trust a node's self-report.

#### 4.7.2 Phase 2 — Export: transfer unit by unit

Each data node uses one stream. The example shows `data-1`; the other nodes run in parallel with the same structure, up to `--parallelism` at a time.

| # | Direction | Frame | Liaison behavior |
|---|---|---|---|
| 1 | ctl → liaison | `Init{session_id="9f2a…", target_node="data-1", format=CSV, compress=GZIP, max_file_size=512MiB, rate_limit{…}, csv_annotations, liaison_bytes_per_second}` | Read `target_node`; a node that is not registered answers `NOT_FOUND`. Apply `liaison_bytes_per_second` to the process-wide bucket (last stream wins). Take the pooled connection and open an upstream bidirectional `Export` stream |
| 2 | liaison → data-1 | `Init` | Forward unchanged, without modifying any field. Both directions are pumped without prefetching, so back-pressure reaches the node |
| 3 | data-1 | Validate `Init` (`INVALID_ARGUMENT` if the first frame is not one); answer `UNIMPLEMENTED` for an unknown format, and `INVALID_ARGUMENT` for a native `Init` that asks for compression (anything but `COMPRESS_NONE`) or a non-zero `max_file_size` below the BNC minimum; locate `<export-snapshot-path>/9f2a…` by `session_id` and renew the lease (`FAILED_PRECONDITION` when it is gone or expired; `NOT_FOUND` is kept for a liaison that cannot see the node, which the client retries through another liaison) | Forward errors unchanged |
| 4 | data-1 → liaison → ctl | `ready{}` | Forward |
| 5 | ctl → … → data-1 | `segment{catalog=STREAM, group="sw_record", segment_suffix="20260907", shard_ids=[0,1]}` | Forward. The node produces one unit at a time; ctl keeps the next request queued |
| 6 | data-1 | Renew the lease again, then check that the segment and both shard directories exist in the snapshot | — |
| 7 | data-1 → liaison → ctl | `unit_started{total_shards=2, total_parts=5}` | Forward |
| 8 | data-1 → liaison → ctl | `file_started{relative_path="stream/sw_record/seg-20260907/shard-0-00001.csv.gz", shard_id=0, seq=1}` → `chunk × N` → `file_finished{bytes, crc32, row_counts, schema_mod_revisions}` | Forward; each `chunk` waits on the liaison bucket first, and one larger than its share of the bucket's burst goes out in pieces of that share. First of 2 shards; a shard larger than `max_file_size` continues in `shard-0-00002.csv.gz` |
| 9 | data-1 → liaison → ctl | `file_started{…/shard-1-00001.csv.gz}` → `chunk × N` → `file_finished{…}` | Forward. Second of 2 shards |
| 10 | data-1 → liaison → ctl | `unit_finished{warnings=[…]}` | Forward |
| 11 | ctl | Keep the unit's files and warnings in memory for the manifest. Throughout steps 7–10 ctl prints the progress from the frames: the unit and its part count from `unit_started`, the file and its shard from `file_started`, the bytes and the `rows` of every `chunk`, and a line with the time of every finished file and unit; on a terminal one live line per node is redrawn in place | — |
| 12 | ctl → … → data-1 | `segment{…, segment_suffix="20260908", shard_ids=[0,1]}` or `property{group="sw_prop", shard_ids=[0]}` | Forward and repeat steps 6–10. ctl sends the next unit before the current one's `unit_finished` arrives, so the node starts it without a round trip; the node still produces units one at a time in request order |
| 13 | ctl → liaison | `CloseSend` | Half-close upstream |
| 14 | data-1 → … → ctl | Response stream ends normally | — |

An index-mode measure unit sends one `file_started{relative_path=".../seg-20260907/segment-00001.csv.gz", segment_level=true}` instead of per-shard files; a property unit sends `property/<group>/shard-<N>-00001.csv.gz` files.

On the client every frame is checked against that order; anything else fails the node. Two timers guard a stream:

- **`--node-idle-timeout`** (default `10m`) fails a node that delivers nothing for that long; it bounds the gap between two frames, not the length of the export. Time between open retries does not count. Throttling does not trip it: a throttled node sends a chunk about every second, and a throttled liaison relays a chunk in pieces of one second's worth. `0` disables it.
- **`--send-retry-timeout`** (default `15m`) bounds how long the client keeps reopening a stream that the liaison or node refused with `RESOURCE_EXHAUSTED` or `UNAVAILABLE`, or that a liaison answered `NOT_FOUND` because it does not see the target node; every retry moves to the next liaison in `--nodes`, backing off from 1 s, doubling up to 30 s. Any other refusal fails at once, and so does a stream that breaks after it started transferring.

#### 4.7.3 Phase 3 — Finalization

| # | Direction | Frame | Liaison behavior |
|---|---|---|---|
| 1 | ctl | Write `manifest.json` atomically, once every node has finished | — |
| 2 | ctl → liaison | `Sessions{action=ACTION_RELEASE, session_id="9f2a…"}` | Fan out to all data nodes |
| 3 | Each data node | Delete `<export-snapshot-path>/xx…`; if already absent, still return success for idempotency | One frame per node; a node that failed carries `error` |

A frame with a non-empty `error`, or a failed release call, is a **WARN, not a failure**: the artifact is already sealed and valid, so the run still exits 0. The CLI prints one `WARN release of export session <id> failed on <node>: <reason>` line per failed node; run `bydbctl data release-session --id <id>` to retry, because until those snapshots go the nodes carry them for the rest of the lease — and on a hot node a snapshot costs roughly a second copy of the live data, not the free hard links the cold case suggests.

### 4.8 Session management

Each export uses one session, corresponding to one snapshot per data node. One export should own the whole cluster at a time — but that is an outcome the protocol converges to, **not an invariant any single component enforces**, and the difference matters for implementers.

On disk a session is one directory per catalog, `<export-snapshot-path>/<session-id>/` (`<catalog>/export-snapshots/<id>` under the catalog root by default), where `<session-id>` is exactly 32 lowercase hex characters (the sweeper treats no other directory name as a session), holding the snapshot of every selected group as `<group>/…` plus the `.lease` file (hard links for stream, measure and trace; a full copy of the property index for property, see below); property sessions use the same `<group>/shard-N` layout, without the extra `data/` level its backup snapshots keep. The directory is set per catalog with `--stream-export-snapshot-path`, `--measure-export-snapshot-path`, `--trace-export-snapshot-path` and `--property-export-snapshot-path`, and for stream, measure and trace must be on the same filesystem as the data path because their snapshots are hard links; the property directory instead needs as much free space as the property data. The export service refuses to start when one of these paths equals or lies inside any catalog's backup snapshot directory, equals, contains or lies inside any catalog's data path, or when two catalogs' paths are equal or nested. These paths are compared with their symlinks resolved (through the longest existing ancestor of a path not created yet), so an alias of a live data directory is refused too, and a path that cannot be resolved refuses to start. It is deliberately **not** the `snapshots/` directory the backup snapshots live in: the snapshot reclaimers (`DeleteStaleSnapshots`, `DeleteOldSnapshots`) only ever see that directory, so a multi-day export needs no special case in them, and the session sweeper (step 5 below) only ever sees sessions.

#### 4.8.1 There is no coordinator, so uniqueness is per node

BanyanDB has no etcd, no lock service, and no CAS primitive anywhere in the tree; node discovery is file-, DNS- or flag-based. A session therefore cannot be claimed atomically cluster-wide before fan-out. The only real enforcement point is **each data node's own export snapshot directory**.

That leaves a genuine race. Two clients calling `Plan{create{}}` through two different liaisons can each win on a different subset of data nodes, producing two partial sessions that block each other. Left unhandled they would deadlock each other. The protocol resolves it without a coordinator:

1. **A node accepts a create only if it holds no other session.** The rule is purely structural: if any other session directory exists on the node, whatever its `.lease` says, the node rejects the create and returns the **incumbent's** ID(s) rather than a bare error. The node takes no liveness decision — the sweeper reclaims expired sessions, the operator reclaims abandoned ones with `--preempt`.
2. **Any rejection aborts the whole attempt.** When even one node rejects, the liaison releases the new ID on every node that accepted it (`Sessions(ACTION_RELEASE)` semantics, over the nodes the create may have reached) and fails the call with a concurrent-export error naming the incumbent IDs. The client releases by hand only the ids the error reports as leaked.
3. **Retry with jittered backoff.** Because the loser releases everything it took, the next attempt finds a clean subset. Two clients retrying in lockstep is the only livelock risk, and jitter bounds it.

Step 2 is the load-bearing one: a partial session kept after a partial win is what creates the deadlock. Releasing is cheap — deleting a snapshot only unlinks files (hard links for stream, measure and trace; the property copy's own files).

`Sessions(ACTION_LIST)` streams one frame **per node** precisely so this state is observable. A list that comes back with two distinct `session_id` values, or with a session on only some nodes, is reporting a partial session, not a healthy one. The client surfaces that verbatim in the refusal message; `--preempt` in that state removes **every** session the node holds before creating its own, rather than taking over one of them.

A data node that joins the cluster after the session was created holds no snapshot for it. It answers `NOT_FOUND` to a `Plan{read{id}}`, which the liaison reports in `unreachable_nodes` rather than failing the call, and it answers its `ACTION_HEARTBEAT` frame with the `error` outcome, so the client sees it as a coverage gap either way. An expired session is different: until the sweeper removes it, the node answers `FAILED_PRECONDITION` and the call fails; once it is swept the node answers `NOT_FOUND` like any node without the session. Both calls take `required_nodes`, the nodes the client still has units to export from: an expired lease on a node outside a non-empty list is reported like a node without the session (`unreachable_nodes` for `Plan{read}`, an `error` frame for the heartbeat), so a node whose units are all exported cannot fail the export; an empty list requires every node.

Beyond the race, a partial session left behind by a client that died mid-abort is reclaimed by the node-side expiry sweep (step 5 below) within one sweep period of the lease expiring, with no operator action. Until then it blocks the next export, which is told whose session it is and when that session was last seen; the operator decides whether to `--preempt` it.

#### 4.8.2 Taking over an existing session

Starting a new export never silently removes another session, and no component classifies a session as alive or dead. A session is held until it is released, preempted, or expires (5 days after its last heartbeat) and gets swept:

- **Any holder**: the node answers `ALREADY_EXISTS`; the client then lists the sessions once and prints who holds it, where, and when its last heartbeat was, with the hint that `--preempt` takes it over. The last-heartbeat time is what tells the operator whether the holder is still running (heartbeats arrive every 5 minutes) or died hours ago. `--preempt` is a confirmation action, not a tuning knob. An operator at a terminal (stdin and stdout both terminals) is asked instead, `[y/N]`, and a yes is the same confirmation; a script or a pipe is never asked and gets the refusal.
- **`--preempt`**: the node removes every session it holds before creating the new one and reports the removed ids in `preempted_session_ids`, which the client prints as a WARN. Nothing else ever sets that field.

Earlier revisions (2026-10-07 to 2026-10-09) had the data node reclaim a holder whose last heartbeat was older than 15 minutes without `--preempt`. That classification was dropped on 2026-10-09: a heartbeat lost to a network partition must not cost a running export its snapshot, and the heartbeat time shown to the refused operator carries the same information without the node guessing.

Lifecycle:

1. **Create** — the liaison sends `Plan{create{id}}` to all data nodes.
2. **Export** — export requests carry the `session_id` to read the matching snapshot.
3. **Heartbeat** — `bydbctl` sends `Sessions{action=ACTION_HEARTBEAT, session_id}` on a fixed interval.
4. **Release** — after the export completes and `manifest.json` is written, `bydbctl` calls `Sessions{action=ACTION_RELEASE, session_id}`. **On a fail-fast abort it deliberately does not**: the session is kept, and the client prints the session ID plus the `bydbctl data release-session --id <id>` command; Ctrl-C and `SIGTERM` stop the export the same way and print the same command. Keeping the session is what will let a resumed run continue against the same frozen snapshot; running the same command again resumes it ([7.2](#72-export-resume)). There is deliberately no flag for "release on abort": it would make a graceful stop strictly worse than `kill -9`, because a released (or swept) session answers `NOT_FOUND` on every node, so the resumed run would see every node unreachable and have to restart against a new point in time — and `release-session` already covers the case where you really are done.
5. **Cleanup** — each data node sweeps export snapshots on a one-minute timer and removes any whose lease has expired. A session directory left without a readable lease (a crash mid-create or a failed rollback) is reclaimed once its newest directory is older than a 10-minute orphan grace; until then it still counts as an occupant, so a create without `preempt` answers `ALREADY_EXISTS`. The first sweep runs as soon as the export service starts serving, so after a data-node restart an expired session is reclaimed at once and a live one survives.

| Constant | Value | Why it is not configurable |
|---|---|---|
| Lease | 5 days | A data-node constant: every heartbeat resets the expiry to now + 5 days, so it only decides how long after the client disappears the snapshot is reclaimed. The request used to carry a lease with a 30-day cap; both were removed because the task length is irrelevant |
| Heartbeat interval | 5 min | A client-side timer, deliberately not derived from the lease. It keeps the lease fresh and stamps `last_heartbeat_at`, which a refused operator reads to judge whether the holder is still running |

The body of a snapshot is hard links, so it copies zero bytes, and deletion is `unlink` only — a background merge that removes a source part during export leaves the snapshot's link fully readable, which is what makes lock-free reading safe. "Hard links, therefore free" holds only for cold segments, though: the open segment's series index and the whole property database are full byte copies (the property snapshot is the index's own backup, written shard by shard, and also holds documents not yet flushed). The snapshot holds what was flushed to disk when it was taken; rows still in a memtable are not in it, so wait one flush interval after stopping writers for a complete export. Creating a session can be canceled only between groups and shards: the snapshot of a single group or shard is not interruptible. A large property copy can therefore outlast the liaison's per-frame wait for a node (5 minutes); the create then fails and is rolled back on every node.

The data-node gRPC port has no authentication of its own; the export RPCs are gated by RBAC only on the liaison. Deployments must keep the data-node port on an isolated network or behind mTLS, as they already must for the snapshot and segment-deletion RPCs on that port.

### 4.9 Remaining work

The CSV export step (2026-10-10) completes the export path for CSV: the `Export` RPC on the liaison (a directed relay, also in-process for a standalone server) and on the data node, the four CSV producers, the client session library (create with retries, refusal rendering, heartbeat, release), the receive loop, the schema files, the manifest and the `bydbctl data export` flags. What is still to come, each with where its design lives in this document:

- **Export resume** — landed (2026-11-12), see [7.2](#72-export-resume). `pointInTime: mixed`, `--seal-partial` and a `bydbctl data progress` command remain future work.
- **`--archive tar|tar.gz`** — packing the artifact into one file.
- **Native format** — landed with the native step (2026-11-03): `.bnc` containers, consumed only by `data import` (there is no offline extract command) ([Native export](#42-native-export), [Native import as implemented](#55-native-import-as-implemented)).
- **Import** — the CSV import (including property) and the cross-cluster e2e landed with the CSV import step ([CSV import as implemented](#54-csv-import-as-implemented)), the native import with the native step, and import resume (2026-11-13) as [7.3](#73-import-resume) describes.
- **Deferred design items** — a dedicated `bydbctl data sessions` listing command (declined for now: the refusal text already renders the occupant), probe-before-create, pinning the session's node membership, skipping zombie nodes under file discovery, and a stage manifest inside the snapshot.

The end-to-end cases `test/e2e-v2/cases/transfer/csv/` and `test/e2e-v2/cases/transfer/native/` run the same cases, once per format, against one hot and one warm data node behind a liaison: the dry run, `release-session`, the export of each catalog, and the import into a second, empty cluster with its refusals and round trip. Their environment and scripts are shared in `test/e2e-v2/cases/transfer/common/`.

### 4.10 Node gRPC extension

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

> The workflow below is the original, native-aware design. The CSV import as implemented runs the steps in [5.4.1](#541-workflow): its checks are the schema and TTL gates only, and rows are re-routed by the target's own topology, so the topology gate (item 7) does not apply to CSV.

1. **Connection check** — call `GetCurrentNode` on each node.
2. **Artifact check** — validate `manifest.json`, the artifact version, and every file's CRC32.
3. **Schema check** — compare the artifact's schemas with the target cluster's. Two resources compare equal unless they differ outside the ignore set `created_at`, `updated_at` and `Metadata.{id, create_revision, mod_revision}`; `created_at` always differs across clusters, so counting it would make every import a `CONFLICT`. `indexRule`'s `Metadata.id` is the exception — it is materialised into the inverted files, so the native channel compares it out of band and refuses a mismatch.
4. **Version compatibility** — for CSV, check the locally generated version against the data nodes; for native, check each shard's part version.
5. **Timezone check** — for native, the target node timezone must match the artifact's, or the import fails.
6. **TTL check** — skip data that has already expired.
7. **Topology check** — the target stage's `shardNum` and `segmentInterval` must equal the artifact's; a mismatch rejects the unit for native. The default stage is always written as `hot`: the server reports it as an empty string, and the client normalises that so the default tier has a stable name in the manifest and the gates. A unit is routed to the target stage **of the same name** — both sides' stage definitions are already known, from the artifact's `groups[].stages[]` and the target schema, so no mapping is configured — and a unit whose stage the target does not have falls back to `hot` and is re-checked against *that* stage — routing with the wrong stage's shard number writes shard directories that `loadShards` skips on restart, so the data is written, is queryable, and vanishes on the next restart. Equality also guarantees that no source shard ID reaches or exceeds the target's `shardNum`. `LocateAll` returns `<group>-<N> is a unknown shard` for such a shard, so the data is simply unroutable. This must be listed and blocked during dry run, not discovered halfway through a push. Topology drift frequently arrives together with a `shardNum` change, so the two conditions co-occur.
8. **Schema import** — create or overwrite per policy, then rerun checks 2–7.
9. **Data import** — per data type, as below.

Two routing failures must be distinguished: zero nodes returns `nil` plus `no nodes available`, while an out-of-range shard returns `... is a unknown shard`. Separately, `LocateAll` deduplicates its result set, so it can silently return fewer targets than the requested `copies` with no error, log, or warning. The import side must compare `len(targets)` against the requested `copies` itself; `--strict-replicas` cannot use `len(targets)` as its denominator. A shortfall is a WARN by default; `--strict-replicas` promotes it to an error that aborts the run.

**Stage routing.** `ImportService` places every `(group, stage)` with the cluster's own node selection: the hot stage with the data node selector the liaison's write queue syncs online writes with, any other stage with the round-robin selector a lifecycle migration uses for it; see [Stage-aware routing](#543-stage-aware-routing). Multi-stage CSV import is therefore supported. The paragraph above about `LocateAll` and `--strict-replicas` describes the native design; the CSV import writes to every available distinct node when a stage has fewer than `copies` nodes and reports the actual count.

### 5.2 Native fast push

Fast push is available only when all of the following hold:

- The artifact format is native.
- The target shard number **equals** the source shard number after stage mapping, and both the stage-level `shardNum` and the group-level `ResourceOpts.ShardNum` match. Comparing only the former lets a unit through the gate and then fails in the liaison's `Pick` with `unknown shard`; comparing only the latter gets it silently dropped by `loadShards`.
- The segment interval and timezone are exactly the same.
- The segment version is in the supported compatibility list.
- The schema is either created successfully or already consistent.

#### 5.2.1 Protocol

The messages are defined in `api/proto/banyandb/transfer/v1/import.proto` ([API reference](../../../api-reference.md)), which is authoritative. One `ImportParts` stream carries one payload, `ImportPartsRequest.Kind`: a part with its trace secondary index parts (`KIND_PART`), a segment's series index (`KIND_SERIES_INDEX`), a stream shard's element index (`KIND_ELEMENT_INDEX`) or a property shard (`KIND_PROPERTY`). The client names the core part with an empty `part_type`, which the liaison relays as `"core"` on the cluster wire; routing uses the stage route of [5.4.3](#543-stage-aware-routing). `requested_copies` is per shard (replicas + 1); for a series index, whose targets are the union of several shards' nodes, it is not comparable with `len(targets)`. The semantics as implemented are in [5.5](#55-native-import-as-implemented); the rest of this section records the design reasoning.

Duplicate-delivery validation must be **split by `ImportPartsRequest.Kind`**. `(id, part_type)` uniqueness can only be enforced on `PART` streams: index streams carry fixed zero values with `id = 0` and an empty `part_type`, so applying the same rule there — or trying to tell them apart by `part_type` — makes the receiver reject the stream with a `does not support part type` error.

The upstream `id` is a **framing signal, not an identifier**. The receiver always allocates its own part ID with `atomic.AddUint64(&tsTable.curPartID, 1)`, and `fillFromSyncContext` copies six scalars but deliberately not the ID. Its only real use is the receiver's `createNewContext := session.partCtx == nil || session.partCtx.ID != partInfo.Id` check, which decides where one part ends and the next begins. Mixing two part IDs in one stream therefore causes a split plus an orphan deletion (`MustRMAll`), not a silent merge.

#### 5.2.2 Transfer flow

| # | bydbctl → liaison (import/v1) | liaison → data-* (cluster.v1) | Liaison behavior |
|---|---|---|---|
| ⓪ | Read the manifest and target schema/topology | — | — |
| ① | Open **every volume** of each container as one volume set, group its files by part and split into streams by part. A part — even a single file — may span volumes, so streams are cut on part boundaries, never on volume boundaries | — | — |
| ② | `Init{catalog, group, shard_id, stage, kind, segment_version, parts{…}}` | — | Validate, route with the stage route of [5.4.3](#543-stage-aware-routing), open `SyncPart` streams |
| ③ | The first `Chunk` | `SyncPartRequest{chunk_index=0, metadata=SyncMetadata{…}, …}` | Build `SyncMetadata`, fill `session_id`, and **assemble** `version_info`: `file_format_version` is copied from the client's `Init.segment_version` — it is the guard on the *source* data — while `api_version` and `compatible_file_format_version` are the liaison's own. A liaison that fills `file_format_version` with its own value turns that guard into a tautology |
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
  sidx-00001.bnc        ← Contains seg-20260907/sidx/**
  shard-0-00001.bnc     ← Contains shard-0/**
  shard-1-00001.bnc     ← Contains shard-1/**
```

| Artifact file | Container | Source data | Generated streams |
|---|---|---|---|
| `metadata` | No | `seg-XXX/metadata`, holding `{Version, EndTime}` | None — not sent. The receiver generates it locally when creating the segment |
| `sidx-NNNNN.bnc` | Yes | `seg-XXX/sidx/**`, a native index directory | `SERIES_INDEX` stream, **one per target node** ([Data transfer](#525-data-transfer)) — not one per shard |
| `shard-N-NNNNN.bnc` | Yes | `shard-N/**`, `<016x>.snp`; also `idx/`, and for trace `sidx/<index-name>/<016x>/` | `PART` and `ELEMENT_INDEX` streams |

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

Footer of sidx-00001.bnc, relative to the segment sidx/ directory:

  000000000012.seg     offset=0          size=4_194_304
  000000000013.seg     offset=4_194_304  size=1_048_576
  000000000013.snp     offset=5_242_880  size=1_024
  (a series index is a native index directory — only .seg and .snp, and no metadata.json)

Chunk{
  data=<1MiB>,
  files=[
    {name="000000000012.seg", size=1048576}
  ]
}

Chunk{ … }

... one frame per 1 MiB ...

Completion{
  total_bytes=…,
  total_chunks=…
}
```

**The series index is segment-scoped, not shard-scoped, so it is delivered once per target node.** `<segment>/sidx` is a single native index directory shared by every shard in that segment (`storage/segment.go` builds it from the segment location), and the receive side confirms it: `syncSeriesCallback.CreatePartHandler` — identical in stream, measure and trace — resolves the destination from `ctx.Group` and `ctx.MinTimestamp` only, calls `CreateSegmentIfNotExist`, and **never reads `ctx.ShardID`**. Compare the element index, whose handler does exactly the opposite: `syncElementIndexCallback.CreatePartHandler` calls `segment.CreateTSTableIfNotExist(common.ShardID(ctx.ShardID))`.

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
      part_type="",              // the core part; the liaison sends "core" on the SyncPart wire
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

Entries under prefix 0000000000000001/ in shard-0-00001.bnc:

  metadata.json     size=214             ← not sent; scalar fields above are parsed from it
  meta.bin          size=1_048_576
  primary.bin       size=8_912_896
  timestamps.bin    size=4_194_304
  searchable.tf     size=33_554_432
  searchable.tfm    size=262_144
  fv.bin            size=65_536

6 files are transferred, totaling 48,037,888 bytes.

Chunk{ data=<1MiB>, files=[{name=meta.bin, size=1048576}] }
Chunk{ data=<1MiB>, files=[{name=primary.bin, size=1048576}] }

... primary.bin spans multiple frames ...

Chunk{
  data=<1MiB>,
  files=[
    {primary.bin, 524288},
    {timestamps.bin, 524288}
  ]
}

... cross-file chunk ...

Chunk{
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

Parts are sent in ascending part-ID order, which is also the byte order inside the container. That keeps reading sequential and keeps the relative ordering of source and target part IDs aligned.

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
| t0 | `Chunk{data=<1MiB>, files=[{name=meta.bin, size=1048576}]}` | `SyncPartRequest{session_id, chunk_index=0, chunk_data=<same 1MiB>, chunk_checksum="3a91c7e2", parts_info=[{id=1, part_type="core", files=[{name:"meta", offset:0, size:1048576}]}], metadata=SyncMetadata{…}, version_info}` | Each node returns `CHUNK_RECEIVED` |
| t1–t8 | `Chunk{…, files=[{name=primary.bin, size=1048576}]}` (8 chunks) | Same structure with increasing `chunk_index`; `files=[{name:"primary", offset:0, size:1048576}]`; no repeated metadata | Each node returns `CHUNK_RECEIVED` |
| t9 | `Chunk{…, files=[{primary.bin, 524288}, {timestamps.bin, 524288}]}` | Convert to absolute offsets: `files=[{name:"primary", offset:0, size:524288}, {name:"timestamps", offset:524288, size:524288}]` | Each node returns `CHUNK_RECEIVED` |
| t10–t44 | Full frames containing one file slice | Same structure with one `files` entry | Each node returns `CHUNK_RECEIVED` |
| t45 | `Chunk{data=<832KiB>, files=[…3 entries…]}` | `files=[{name:"tf:searchable", offset:0, size:524288}, {name:"tfm:searchable", offset:524288, size:262144}, {name:"fv", offset:786432, size:65536}]` | Each node returns `CHUNK_RECEIVED` |
| t46 | `Completion{total_bytes=48037888, total_chunks=46}` | `SyncPartRequest{session_id, chunk_index=46, completion=SyncCompletion{total_bytes_sent=48037888, total_parts_sent=1, total_chunks=46}, version_info}`, no `chunk_data` or `parts_info` | Final response |

The liaison stores nothing: it converts one frame at a time and forwards it, which is why it stays memory-flat at line rate. The memory protector is not a backstop here — it only observes memory, and a relay saturating the NIC has a flat memory curve.

#### 5.2.7 When only some replicas succeed

`ImportPartsResponse.targets` reports `{node, success, error}` per node, and the liaison waits for all N before returning. It does **not** retry the part itself — it holds one chunk at a time, and re-pushing a part means re-reading the container, which only the client can do.

The client counts the part as **partially complete**, not done, and retries only the failed targets.

**Re-pushing must target only the failed replicas, through `Init.retry_nodes`.** Re-sending the whole part is not equivalent: `data-a` would receive a second copy. Measure still converges by `(seriesID, timestamp, version)`, but **stream and trace are append-only, so the second copy is a permanent, visible duplicate**, and the element index re-introduces a whole segment. So the set actually pushed again is:

```
retry_nodes = targets of the payload  ∖  targets that answered success
```

Pushing to every target again re-delivers to replicas that already succeeded. A failed target is not by itself proof that the node lacks the data: the receiver commits before it answers, so a stream or trace part, or an element index, whose target failed after its completion was relayed (`completion_sent`) is not pushed again; see the Retries item of [5.5](#55-native-import-as-implemented).

### 5.3 CSV data rewrite

For CSV, `bydbctl` reads the files directly, rebuilds the corresponding requests, and redistributes them to the target data nodes.

> **Superseded by the implementation.** The protocol below is the original draft. The implemented `ImportService` (`api/proto/banyandb/transfer/v1/import.proto`) differs: one stream per `(catalog, group, stage)` with the resource name on every row, no `success`/`error` in the acknowledgement (a failed batch ends the stream), a read-only `Targets` RPC, and the liaison **fans every row out to all replicas itself** instead of writing replica 0. See [CSV import as implemented](#54-csv-import-as-implemented).

```protobuf
service ImportService {
  // Bidirectional: the client keeps a bounded window of unacknowledged batches,
  // so it needs an acknowledgement per batch, not one response at end of stream.
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
    uint64 raw_element_id = 2; // the _element_id cell, decoded by csvfmt.DecodeElementID
  }

  message MeasureRow {
    measure.v1.DataPointValue point = 1;
  }

  message TraceRow {
    // Tags and span of the row. The engine does not store a span's write version, which
    // must be positive, so the importer sends 1.
    trace.v1.WriteRequest span = 1;
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
  bool success = 3;
  string error = 4;
}
```

The RPC is **bidirectional**, not client-streaming. A client-streaming RPC delivers a single response after the client closes the stream, which cannot carry the per-batch acknowledgements the client needs: a batch counts as done only once the server has confirmed it, the client bounds the batches sent and not yet acknowledged ([5.4.7](#547-batches-parallelism-and-sources)), and a failed batch is named by its `batch_seq`. Acknowledgements carry `batch_seq` so the client correlates them explicitly rather than assuming they arrive in request order.

A dedicated import RPC is required rather than reusing the public write API, because the public path regenerates fields the artifact already carries. In particular a measure `version` of `0` is silently replaced with the message ID on the liaison, destroying the deduplication semantics the CSV `_version` column exists to preserve.

| # | bydbctl → liaison | liaison → data-* | Liaison behavior |
|---|---|---|---|
| ⓪ | Read the manifest and target schema/topology | — | — |
| ① | Open `.csv.gz` files and build the column mapping from the schema | — | — |
| ② | `Init{catalog, group, name, stage}` | — | Locate the schema of the target resource |
| ③ | Send `RowBatch{…_rows}` in batches | `InternalWriteRequest` per row | Resolve `(EntityValues, ShardID)`, call `Locate(group, name, shard, 0)` and send one internal write request to **replica 0 only** — the write path replicates internally, so the liaison does not fan out. `property_rows` are the exception: they go to `TopicPropertyRepair` and fan out over all `copies` replicas |
| ④ | Receive the batch response | — | Aggregate success and failure for the batch |

This channel is **not** idempotent on replay: re-sending a batch duplicates stream and trace rows.

Property rows must be applied with `db.Repair(ctx, id, shardID, property, deleteTime)`. `Update` has no `delete_time` parameter at all — its underlying call hardcodes `0` — so a tombstone imported through `Update` comes back to life. `Delete` searches for an existing document first and is meaningless against a fresh database.

### 5.4 CSV import as implemented

The CSV import landed on 2026-10-19 (`bydbctl data import`, `pkg/transfer/importer`, the liaison's `ImportService`). Where it differs from the design above, this section is authoritative. The native import ([5.5](#55-native-import-as-implemented)) and import resume ([7.3](#73-import-resume)) landed later.

#### 5.4.1 Workflow

```text
bydbctl data import --input DIR [--plan plan.yaml] [--dry-run] [-o table|yaml|json] ...

 0  preflight        reuse the export preflight (several liaisons, standalone fallback, ROLE_LIAISON)
 1  artifact check   manifest.json, format=csv|native, artifactVersion <= 1, non-empty schemaFiles,
                     every schema and unit file present with its size and CRC32 (under
                     --schema-only, data files by size only); the server's
                     internal groups an older export kept are left out with a warning
 2  schema compare   read-only, artifact schema/*.jsonl against the target registry
 3  Targets          ImportService.Targets for every (group, stage) involved, read-only
 4  decide()         [1] schema gate, [4] TTL gate, multiple-source selection, and the ROWS
                     row check that reads every row (skipped with --schema-only) -> ImportPlan
 5  render           verdict table, byte-identical for --dry-run and the real run; with a progress
                     file the resume summary comes first and the plan is compared with the recorded
                     one (exit 6); unless --schema-only, a file is created and removed next to the
                     progress file, so an unwritable location exits 1 before anything is written
 -- a dry run exits here: 0 = would proceed, 2 = would be refused --
 6  schema write     dependency order group -> stream/measure/trace/property -> indexRule
                     -> indexRuleBinding -> topNAggregation, through the registry Create / Update
 7  wait             SchemaBarrierService.AwaitSchemaApplied, including every group's auto-created
                     _top_n_result
 8  re-check         collect + Targets + decide again; a changed data plan is printed and the
                     run continues, or exits 5 with --abort-on-plan-change (schema written, no data);
                     a route failure of a created or overwritten group exits 3 here
 9  baseline        Plan counts of the imported groups, whenever the plan has work items: for
                     --verify, the count reconciliation of native stream and trace parts and a
                     later resume; retried with the transfer back-off (1 s doubling, up to 1 minute)
                     while it fails or misses a target node; recorded once with the plan (7.3.1)
                     and read back by a resume. A fresh import creates the progress file just before
                     (after the re-check; --restart sets the old one aside only then) and prints
                     `progress: <file>`. When --verify or a reconciliation
                     needs it, a baseline that did not count a target node of the plan exits 3
                     before any push; otherwise the failure is a WARN, the progress file records
                     noBaseline and the import goes on (a verify forced later exits 4)
10  push             worker pool, one ImportRows stream per (catalog, group, stage) per worker
11  verify           poll and reconcile; a mismatch exits 4
```

The column layout is rebuilt from **all** resources of the group in the artifact's `schema/`, so it matches the export bit for bit whatever `--include-schema` says for the import, and the import logs that the schema is read from those files. An artifact exported with `--include-schema=false` carries no schema: the import logs an `info` line and rebuilds the layout from the **target's** resources of the group instead, compares and writes no schema (as `--include-schema=false`; a group the target lacks is `MISSING`, and `--schema-only` is a usage error), and refuses a file whose header or `#datatype` line differs from that layout. The target's schema must therefore be the one the rows were exported with; a column whose type alone changed is caught only by the `#datatype` line or by a row that fails to parse.

#### 5.4.2 Protocol

`api/proto/banyandb/transfer/v1/import.proto`, served on the liaison gRPC port in cluster and standalone mode. Both methods require `PermissionClusterAdmin` at global scope.

```protobuf
service ImportService {
  rpc ImportRows(stream ImportRowsRequest) returns (stream ImportRowsResponse);
  rpc Targets(TargetsRequest) returns (TargetsResponse); // read-only
}

message ImportRowsRequest {
  message Init {
    common.v1.Catalog catalog = 1;          // not CATALOG_UNSPECIFIED
    string group = 2;
    string stage = 3;                       // empty = default stage; an undefined stage resolves as "hot"
                                            // (the explicit hot stage if defined, else the default); ignored for property
    uint64 liaison_bytes_per_second = 4;    // process-wide import bucket, last stream wins; 0 = unlimited
    RateLimit rate_limit = 5;               // per target data node, enforced by the node; last import wins
  }
  message StreamRow   { string name = 1; stream.v1.ElementValue element = 2; uint64 raw_element_id = 3; } // id > 0
  message MeasureRow  { string name = 1; measure.v1.DataPointValue point = 2; }  // point.version != 0
  message TraceRow    { string name = 1; repeated model.v1.TagValue tags = 2; bytes span = 3; } // 1..512 tags
  message PropertyRow { property.v1.Property property = 1; int64 delete_time = 2; } // group == Init.group
  message StreamRows   { repeated StreamRow rows = 1; }   // each list at most 10000
  message MeasureRows  { repeated MeasureRow rows = 1; }
  message TraceRows    { repeated TraceRow rows = 1; }
  message PropertyRows { repeated PropertyRow rows = 1; }
  message RowBatch {
    uint64 batch_seq = 1;                   // monotonic from 0
    oneof rows {                            // the catalog of Init
      StreamRows stream = 2;
      MeasureRows measure = 3;
      TraceRows trace = 4;
      PropertyRows property = 5;
    }
  }
  oneof frame { Init init = 1; RowBatch batch = 2; } // required; init first
}

message ImportRowsResponse { uint64 batch_seq = 1; }

message TargetsRequest  { string group = 1; string stage = 2; } // one (group, stage) per call keeps every response small
message TargetsResponse {
  message Shard { uint32 shard_id = 1; repeated string node_ids = 2; }
  string group = 1;
  string stage = 2;          // the stage actually used: "hot" after a fallback
  uint32 shard_num = 3;
  uint32 copies = 4;         // replicas + 1
  repeated Shard shards = 5;
  repeated string inactive_node_ids = 6;  // route nodes this liaison is not connected to
  bool group_deleting = 7;
}
```

- Rows carry tags and tag families **in the declaration order of the target schema** and no `TagFamilySpec` / `DataPointSpec`, like the lifecycle row replay. Every internal message carries its `Metadata`.
- An acknowledgement means every target data node accepted the batch at batch level. A data node only logs per-row failures, so `--verify` is the only check that rows landed.
- A failed batch ends the whole stream with a gRPC status whose message starts with `batch_seq=<n> node=<id>:`. Codes: disk full → `ResourceExhausted`, unknown schema → `NotFound`, unreachable node → `Unavailable`, anything else → `Internal`. A row the liaison refuses ends the stream with `InvalidArgument` and an ErrorInfo (reason `ROW_INVALID`) whose metadata holds `batch_seq` and `row`; its message starts with `batch_seq=<n>: row <i>:`. A batch naming a missing resource gets `NotFound` with reason `SCHEMA_NOT_FOUND` and `batch_seq` in the metadata. Only a frame refused by message validation, or an `Init` sent after the first frame, carries no `batch_seq`, and the client attributes it to the oldest unacknowledged batch.
- A trace data node that answers `STATUS_SERVER_BUSY` is retried by the liaison with backoff for that node, bounded by the liaison's write pressure timeout; if it is still busy the batch fails with `ResourceExhausted`.
- The liaison writes rows as `InternalWriteRequest`s wrapped in `transfer.v1.ImportWrite` frames straight to the data nodes' import topics, whose listener hands them to the catalog's write path, so the public write path's rewrites (a measure `version` of `0` replaced by a message ID) never apply; `version == 0` is refused with `InvalidArgument` instead. A row the write path cannot store is only logged for a regular write. The import listener puts a `common.WriteRecorder` in the context it calls the catalog's write listener with, and the write listener reports to it where it logs, its reply unchanged (without a recorder the calls do nothing): a row that failed **before it was stored** (a decode failure, its schema or TSDB not loaded on the node, a segment or table it could not create) by its index in the message (`common.RecordRowFailure`); an index write after the rows were stored is wrapped in `common.RetryWrite`, which under the recorder retries it with back-off, since every such write may be repeated (series and metadata documents are inserted if absent, index-mode and trace series documents upserted, a refused element-index batch admits nothing, a sidx instance is created once); what still fails is recorded with `common.RecordWriteFailure`. The node writes the rows not stored again, alone, with back-off, for at most `30s` shared with the index retries, so a retry stores no row twice. Rows still not written are answered as an error naming them (`k of n imported row(s) were not written after retries (rows …)`), and a failure after storing ends the batch at once, its message saying that importing the rows again would store stream and trace rows twice; either ends the import with `NODE_FAILED`.
- The import listener hands measure rows to the write path wrapped in `data.ImportedMeasureWrite`, so it skips Top-N for them (see [Top-N results](#546-top-n-results)).

#### 5.4.3 Stage-aware routing

The liaison computes a route snapshot on every `Init` and every `Targets` call with the node selection API the cluster already places shards with, so imported rows land where the cluster's own writes and migrations put them:

```text
route(catalog, group, stage):
  g := the target group (absent -> NotFound)
  property: shardNum/replicas from g.ResourceOpts, nodes from the property node registry; done
  want := lower(stage) or "hot"; a stage the group does not define -> "hot"
  "hot" (the default stage, or a stage explicitly named hot):
      shardNum/replicas from g.ResourceOpts (group level)
      nodes := the catalog's data node selector LocateAll -- the selector the liaison's write queue
               syncs online writes with, over the --data-node-selector nodes
  any other stage:
      shardNum/replicas from the stage
      nodes := a round-robin selector over the registered nodes matching the stage's node selector,
               laid out by every group -- the selector a lifecycle migration places the stage with
  targets(shard) := nodes.LocateAll(group, shard, replicas + 1)  // sorted, distinct
  a shard the selector cannot place -> FailedPrecondition
```

- The hot stage follows the online write placement exactly, the `--data-node-selector` included: a liaison without one places hot rows on every data node, as its online writes do.
- A stage other than hot follows the lifecycle migration's placement. The round-robin selector is laid out by the group-level shard number, so, as for a migration, a stage with more shards than the group is refused (`unknown shard`).
- The route borrows the selectors: it registers no handler on the shared tier-2 client, which would overwrite the online handlers. The data node selectors are the ones the write queues keep up to date; a stage's selector is built per call from the registered nodes and the listed groups.
- A node that is down fails the batch instead of silently rerouting it.
- **Replica fan-out**: stream, measure and trace rows go to every node of `targets(shard)` through one `BatchPublisher` per batch (`TopicStreamImport` / `TopicMeasureImport` / `TopicTraceImport`), each node's stream opening with an init frame. Property rows go to `TopicPropertyImport` the same way, one batch per node holding the revisions of an entity in batch order; the node's listener repairs them one at a time in that order and stops at the first failure. When a stage has fewer nodes than `copies`, every available distinct node is written, `Targets` reports the actual count, and the client warns.
- Shard ids: stream and measure via the entity / sharding-key locator with the **route's** shard number (the group's for hot, the stage's otherwise); trace via `TraceShardID(traceID, routeShardNum)` after checking the tag count, a non-empty string trace and span id and a non-null, valid timestamp tag; property via the entity and the group shard number.
- Standalone: `Targets` reports the local node, a non-hot stage falls back to `hot`, and rows go through the local queue.
- The liaison rate limit (`Init.liaison_bytes_per_second`) is a bucket of its own, independent of the export's, charged once per received batch before the fan-out.
- The node rate limit (`Init.rate_limit`) is one byte bucket and one row bucket **per target data node**, kept by the liaison and shared by every import stream it serves; the stream opened last sets the rates and they are lifted once no import stream is left. Before a batch is written to a node the liaison pays, in full, the internal write requests that node receives (`proto.Size` for bytes, one row per request), so every replica is paid on its own node; a property repair is paid per node the same way. A busy node's retry is not paid again: the node refused the batch before writing any of it. A bucket's burst is one second's worth (at most 1 MiB, at least one row) and a larger payment is paid in steps of the burst.

#### 5.4.4 Rows

| Catalog | Handling |
|---|---|
| stream | `_element_id` → `csvfmt.DecodeElementID` → `raw_element_id`, kept as is by the receiver (`0` means a corrupt artifact) |
| measure | `_version` must be present and non-zero; fields in target schema order |
| index-mode measure | as measure, no fields; when several sources of a segment are pushed, the client keeps the highest `_version` per `(name, entity)` within the unit, the later timestamp winning a tie |
| trace | tags in target schema order with `trace_id` / `span_id` as string tags in their positions; `_span` → bytes; a null timestamp tag means a corrupt artifact |
| property | `_updated_at` `\N` → unset; `_delete_time` `0` (or `\N`) = live, > 0 = tombstone, negative = corrupt. **Only each entity's latest state is kept**: a property work item is read once, source by source and file by file, and every row is sent as it is read, batched like the other catalogs, so memory does not depend on the item's size; nothing is indexed, sorted or folded on the client. The target's repair makes the result independent of the order: a revision older than the newest one the entity holds is refused, and a same-revision conflict resolves deterministically by the repair rule below, so every entity converges to its latest revision. Whether older revisions leave documents on the target is not guaranteed. The liaison keeps property batches, and the rows of one entity within a batch, sequential |

The **repair rule** (`banyand/property/db/shard.go`), now applied to every caller — gossip anti-entropy, liaison read repair, `RepairSchema` and the import — for an incoming row with the same `mod_revision` as the local latest: a local tombstone beats an incoming live row (nothing is written); an incoming tombstone replaces a local live row with the incoming `deleteTime`; between two tombstones the smaller `deleteTime` wins, so replicas converge whatever the order; identical rows are a no-op.

A resource name the target schema lacks, a header that does not match the layout, or a cell that does not parse or fails the liaison's row checks is a corrupt artifact. The row check (gate `ROWS`) reads every row before anything is written, in the dry run too, and refuses the import with exit `2`, naming the file and record number in the `[d] rows` details.

#### 5.4.5 Gates

**[1] Schema.** Resources compare equal unless they differ outside `created_at`, `updated_at` and `Metadata.{id, create_revision, mod_revision}`. Each difference is classified as identity, topology, shape or behavior, and resolved by `import.schemaConflictPolicy{default, rules[{match, policy}]}` (`match = <kind>/<group>[/<name>]`, first match wins; `--on-schema-conflict` sets only the default) to `error`, `ignore` or `overwrite`. Every `CONFLICT` prints a ready-to-paste `match:` line. Guards that no policy overrides:

- G1 — an identity difference is always `CONFLICT`.
- G4 — `overwrite` of a non-empty target group needs `--force`; "non-empty" is any count above zero in the target's `Plan`, failing closed when the inventory is incomplete. Rows not yet flushed are invisible to it.
- G5 — a measure `index_mode` or `interval` difference is always `CONFLICT`.
- G6 — an `overwrite` the registry's `Update` would refuse is `CONFLICT` up front, so no run writes half a schema: a changed stream or measure entity, a removed entity tag or its family, a changed existing measure field spec, a removed reserved trace tag, or enabling a sampler pipeline on a group with several traces.
- `ignore` with a type difference in a tag or field present on both sides is `CONFLICT`; extra or missing columns are dropped or sent as null and listed.

Artifact resources are re-validated (`INVALID`), and a missing dependency is `MISSING`. Any `CONFLICT`, `INVALID` or `MISSING` exits `2` with nothing written. `CREATE` uses the registry `Create` and `OVERWRITE` its `Update`, with `created_at`, `updated_at` and the metadata revisions cleared so the server stamps them. A resource someone else created between steps 2 and 6 makes `Create` fail with `AlreadyExists` (exit `3`). G0 (tombstones) is not implemented because the registry does not expose them; G2/G3 are native only.

**[4] TTL.** The effective TTL is the group TTL plus every stage TTL up to the mapped target stage (`CumulativeTTL`, checked against `pub.ResolveStageResourceOpts`); the default stage, including a fallback, has the group TTL only; property is exempt. A unit's newest time is the largest `maxTimestamp` of its selected sources, or, for index-mode segment files, the segment end parsed in the source node's timezone. Retention removes a whole segment, and queries hide it, only once the segment's end is before `now − effectiveTTL`; a row older than that in a segment ending later stays visible. So the gate projects the newest time onto the target stage's segment grid (`IntervalRule.Standard`: buckets of the stage's segment interval anchored at 1970-01-01 in the target's time zone: for native, the source node's, which the TZ gate makes equal to the target's; for CSV, that of the first target data node of the route that reports one, else the source node's) and refuses a unit (`STATUS` `expired`) unless `--force-ttl` only when that segment ends before `now − effectiveTTL`. A unit just inside the window may still land in a segment that expires soon after. Some units refused and the rest imported exits `0`; all refused exits `2`.

#### 5.4.6 Top-N results

`_top_n_result` is **exported and imported verbatim**, never recomputed. A standalone server would recompute it from the imported source measures, and the recomputed values, carrying newer versions, would overwrite the imported ones; the import listener therefore hands each measure row to the write path wrapped in `data.ImportedMeasureWrite`, an in-process marker that skips the Top-N flow (a cluster data node never computes Top-N on writes anyway). The imported `_top_n_result` therefore equals the artifact's exactly in both deployments. The schema itself is not created (the target auto-creates it with each measure group, and step 7 waits for it), but its columns must match the target's: a mismatch is a `CONFLICT` that refuses the whole import. The cost: a target whose `TopNAggregation` differs keeps the source's historical Top-N; new traffic is computed by the target as usual.

#### 5.4.7 Batches, parallelism and sources

- A batch is sent when it reaches `writeBatchSize` rows (1–10000, default 1000) or `writeBatchBytes` (64 KiB–64 MiB, default 4 MiB), whichever comes first; a single row above the byte limit travels alone with a warning. Out-of-range values are usage errors. A `ResourceExhausted` reply names the file and record and both possible causes (the liaison's `--max-recv-msg-size` or a full disk).
- Work items: one per shard of a segment unit; one per segment for index-mode segment files (decided by where the files are, not by `groups[].indexMode`); property grouped by source shard id. `--parallelism max` is the size of the **union of target nodes** returned by `Targets`, clamped to the number of work items; an explicit N is clamped the same way.
- Each worker opens one `ImportRows` stream per `(catalog, group, stage)` lazily and reuses it; worker `i` uses liaison `i % L`. Only the first send or receive of a stream is retried (within `--send-retry-timeout`: `Unavailable` and load-protection `ResourceExhausted` move to the next liaison; `SCHEMA_NOT_FOUND`, or a `NotFound` without a detail, waits on the same liaison; `NOT_WRITTEN` (the liaison refused before writing anything, such as on a failed schema read) moves to the next liaison even after a stream or trace batch was sent; `NODE_FAILED` (including a full disk), `MESSAGE_TOO_LARGE` and `ROW_INVALID` are never retried); a later error is terminal, because re-sending a whole batch duplicates stream and trace rows. A failed worker cancels the rest (exit `3`).
- After that first exchange (init plus `batch_seq` 0, the only batch that may be retried), a worker keeps up to `writeInflightBatches` (1–8, default 2) batches sent and not yet acknowledged on each stream: it builds and sends the next batch while the liaison works on the previous one, and takes the acknowledgements in `batch_seq` order. A batch is done only once acknowledged and a work item once all its batches are; a failed batch is named by the `batch_seq` the liaison reports. A worker holds at most `writeInflightBatches × writeBatchBytes` of rows.
- Rate limiting has four knobs, set by the client; whichever binds first wins:

  | Knob | Flag / plan field | Protobuf field | Enforced by, measured on |
  |---|---|---|---|
  | Client rows | `--write-rate-limit` / `import.writeRateLimit` | — | The client: one row bucket for the whole import, paid before each batch is sent |
  | Liaison bytes | `--liaison-rate-limit` / `connection.rateLimit` | `Init.liaison_bytes_per_second` | Each liaison, process-wide: the serialized size of every received batch, before the fan-out |
  | Node bytes | `--node-rate-limit` / `import.nodeRateLimit` | `Init.rate_limit.bytes_per_second` | Each target data node: the encoded rows it is sent, replicas included |
  | Node rows | `--node-row-rate-limit` / `import.nodeRowRateLimit` | `Init.rate_limit.rows_per_second` | Each target data node: the rows it is sent, replicas included |

  The node knobs reuse the export's `RateLimit` and flag names, and like the export the data node enforces them. The liaison writes import rows on their own topics, one per catalog (`stream-import`, `measure-import`, `trace-import`, `property-import`), each frame a `transfer.v1.ImportWrite`: every batch stream to a node opens with an init frame carrying `rate_limit`. The node's import listener charges the batch to one byte pace and one row pace that every import on the node shares, then hands the rows to the catalog's write path; only the import topics pay, so other writes are never limited. A pace is the time by which the node has paid out what it admitted, moved on by each batch's cost at the rate that batch names, so the import that wrote last sets the rate and a change carries no debt; one second's worth (at most 1 MiB) passes on an idle node without waiting. The rates hold on the node **whichever and however many liaisons** write to it; a process that runs several data nodes, as a test harness does, keeps one pace per node. A batch waits for its own share and for the batches admitted before it, but no more than its own share plus `10s`: a batch that would wait longer, or past its request's deadline, is answered `STATUS_RATE_LIMITED` before anything is charged or written. The liaison therefore waits `2 min` plus the batch's share (the slower of its bytes over the byte rate and its rows over the row rate) plus `10s` for an acknowledgement, and resends a batch refused by the rate limit to that node with backoff for as long as the stream lasts; unlike memory pressure, the rate limit is not bounded by the write pressure timeout, since it is expected to hold batches back. A standalone server admits the rows the same way in process. A data node older than the import topics refuses them, so a cluster must run one version for an import. The node knobs are ignored by `--dry-run`, as are the other two.
- The schema write prints one summary line, `schema: <n> resource(s) written and applied on every node`; the plan table above it already lists every resource. During the push the client prints, like the export, a line for every finished work item (`<catalog> <group>/<stage>  [done/total] <position>  <rows> rows  <time>`) and one for every finished `(catalog, group, target stage)` (`✓ … done: <n> work item(s), <rows> rows in <time>`); on a terminal it also redraws one live line per group in flight with the rows written (acknowledged by the data nodes), the rows left of the planned ones, the row rate and the elapsed time (`<catalog> <group>/<stage>  <n> rows written, <m> left  <r> rows/s  <time>`). The rows written move one acknowledged batch at a time, so a small `writeBatchSize` makes them move more often.
- Multiple sources follow [§6.3](#63-rules), keyed by `(group, stage, segment, shard)`: coverage from `parts[]`, counts from `shards[].totalCount`. Property and index-mode units push every source. `--all-sources` skips the selection; `--strict-coverage` refuses (exit `2`) a shard work item, or a CSV stream/trace segment-level item, that would push more than one source; property units, native series indexes (whose receiver keeps the first copy of each document) and measure segment items, which deduplicate or merge, are exempt.

#### 5.4.8 Verify

`--verify` (default on, `--verify-timeout` 2m, any positive value) counts with `ExportService.Plan` without a session, narrowed to the imported groups, polling every 5 s (the default flush interval) because only flushed parts are counted; on a terminal a live line (`verify  <elapsed> / <timeout>  waiting for the target to flush: <what has not reconciled yet>`) is redrawn after every poll and erased when the check ends; without a terminal every poll that does not end the check logs one `verify: waiting for the target to flush (...)` line instead. For each `(catalog, group, stage, node, shard)` it takes Δ = post − pre over the segments present after the import (segments removed by retention are excluded with a warning). Any target node in `unreachable_nodes` makes verify incomplete.

- stream / trace: for every shard with Δ > 0, exactly its target nodes have Δ > 0 and the same Δ, and the shards' Δ sum to the rows pushed.
- measure: once the sum is stable across two samples a flush interval apart, rows pushed > 0 ⇒ ΣΔ > 0, Σ_nodes Δ ≤ rows pushed × copies, and a shard with Δ > 0 has Δ > 0 on all its target nodes; a negative Δ (merge folding) is only a warning.
- index-mode measure: a target node that gained no segment document although rows were sent, and held none before, is a warning; nothing else is checked (one document per series and segment, and rows replayed onto held documents gain nothing).
- property: not counted, since the target keeps each entity's latest revision; a failed repair already fails the import.

Verify assumes no other writes reach the target during the import, does not cover index structures, and in standalone uses `copies = 1`, stage `hot`. Only a reconciliation difference or an incomplete coverage exits `4`. The push ends with `IMPORTED  <n> rows of <m> work item(s) in <time>`, and the report is one line, `VERIFY    ok in <time>` or `VERIFY    FAILED in <time> (exit 4)`, followed by one line per problem: a check that could not complete (`not checked: …`), a count mismatch, or a warning (`warning: …`). Work items refused by a gate are already listed under the plan's PROBLEMS, so verify does not repeat them.

#### 5.4.9 Exit codes

| Code | Meaning |
|---|---|
| `0` | success, warnings included (also: some units refused by the TTL gate) |
| `1` | usage error (also `--resume` with `--restart`, or `--resume` without a progress file), a progress file location that cannot be written, a `--checkpoint` that names a directory, ends in `-plan.json` or `-cursor.json` (the name of a side file) or, for an import, is a file of the artifact (`manifest.json` or a file it lists), another `bydbctl` holding the lock of the progress file, or a resume not confirmed (no `--resume` and no terminal, or not answered `y`) |
| `2` | refused before anything was written (preflight, schema gate, row check `ROWS` including a corrupt artifact, all units refused, `--strict-coverage`) |
| `3` | failure after writing started (schema write, wait, re-check route of a created or overwritten group, a baseline needed by `--verify` or a reconciliation missing a target node, push); once the progress file exists the message names it and says to run the same command again to resume. Ctrl-C at the `Resume?` question is `3` too, and so is a resume's check of the data the progress file records as imported when a node that may hold it does not answer (nothing written; run again once it is reachable) |
| `4` | `--verify` failed; the verify stays pending in the progress file, and the next run of the same command verifies again, also over a finished import, until one passes |
| `5` | the re-check found a different data plan and `--abort-on-plan-change` was given |
| `6` | resume impossible: the progress file is unusable, a binding differs, the target is not the recorded one or no longer holds what the progress file records as imported, or the plan differs from the recorded one; nothing was sent, and only a re-checked plan that differs comes after the schema write ([7.1](#71-detection-summary-and-confirmation), [7.3.2](#732-resume-flow)) |
| `7` | import halted by `resumeDuplicationPolicy: block` before anything was pushed ([7.3.5](#735-resumeduplicationpolicy)) |

Errors before step 6 map through `exitcode.For`; every error from step 6 on is `3`, except a verify failure (`4`), `--abort-on-plan-change` (`5`), a resume whose re-checked plan differs from the recorded one (`6`, the schema written; an unusable plan side file, which holds the baseline, is refused, `6`, when the progress file is loaded, before anything is written) and `resumeDuplicationPolicy: block` (`7`).

#### 5.4.10 Known limitations

- `_top_n_result` is transferred verbatim and never recomputed, so it keeps the source's Top-N caliber; an incompatible target `_top_n_result` refuses the import.
- An acknowledgement is a batch-level acceptance; per-row failures on a data node are only logged and surface in `--verify`.
- Stream and trace row replay is not idempotent. A rerun with the progress file ([7.3](#73-import-resume)) skips every acknowledged batch; only the batches in flight at the interruption follow `resumeDuplicationPolicy`. A rerun without it (deleted, or another `--checkpoint`) duplicates what had landed.
- G4's emptiness check cannot see unflushed rows; G0 is not implemented.
- No `.tar` / `.tar.gz` input; it arrives with the export's `--archive`.
- `--verify` does not cover index structures and needs a quiet target; content equality is proven by the integration and e2e tests, which re-export the target and compare logical rows with the source artifact (`pkg/transfer/rowdiff`). A property is compared by its entity `(group, _name, _id)` at its latest revision only (the largest `_mod_revision`, a tombstone winning over a live row of the same revision; `_delete_time` compared as live or tombstone); older revisions are ignored on both sides.
- The same data held by source nodes in different timezones, or caught mid lifecycle migration, forms different units the multiple-source selection cannot match, so it is pushed more than once (duplicate trace spans), never lost.
- Pre-existing and not fixed here: the lifecycle row replay writes replica 0 only, and builds its route table from the group-level shard number while computing shards with the stage's.

### 5.5 Native import as implemented

The native import (2026-11-03) is the CSV import's workflow ([5.4.1](#541-workflow)) with another push: the preflight, the artifact check (every file's size and CRC-32; each `.bnc` volume is verified once, member checksums included, and under `--schema-only` data files are checked by size, though each `.bnc` container's volume footers are still read: the volume set must be complete and every volume must hold the `entryCount` the manifest registers), the schema comparison and write, `Targets`, the source selection, the TTL gate, the re-check after the schema write and `--verify` are shared. What differs:

- **No row check.** The part files are relayed as they are, so the `ROWS` gate does not run.
- **Four more gates**, each a refusal of the whole import (exit `2`), with the gate and its reason in `PROBLEMS`:
  - `TOPOLOGY` — the shard number of the target stage (the routed one, or the predicted group's) and of the group, and the segment interval of the target stage, must equal the source's from `manifest.groups[].stages`. For a property group only the shard number counts.
  - `TZ` — the time zone of every target data node must equal the source node's (`manifest.source.nodes[].timezone`). A source or target node that reports no time zone is not refused.
  - `VERSION` — every target data node must read the unit's `segmentVersion` (its `compatible_file_format_version`, or its own version); a target node that reports no file format version is not refused, and property units carry no segment version and are not checked.
  TZ and VERSION apply to stream, measure and trace groups only, and, like the `TARGET-NODES` column, check only the nodes the liaison relays each payload to: a shard or property item the nodes of its shard, a segment item (series index) the union of the nodes of the shards it covers, or every node of the stage when it covers none; when that set is empty, every node of the stage; and when the stage has no route yet (a group the import creates) or no nodes, every data node of the target cluster. A CSV import checks every node of the stage.
  - `INDEX_RULE` — every index rule of the group that the target already has must carry the artifact's id: the native index files key the stored tag values by the rule id. An index rule the import creates keeps the artifact's id (the schema write passes it on the create instead of letting the registry assign one); an existing rule with another id refuses the import whatever `onSchemaConflict` says, because an overwrite keeps the target's id and `ignore` leaves it. Letting the import create the schema avoids this; a schema created beforehand by hand does not. An artifact exported without its schema skips this gate with a warning.
  The table's `CHANNEL` column says `part-push` (`property/parts` for a property group).
- **Payloads.** One `ImportParts` stream carries one payload: `KIND_PART` (a core part and, for trace, the secondary index parts sharing its id, so the two halves never land in different sessions), `KIND_SERIES_INDEX` (the `.seg` segments of a segment's series index), `KIND_ELEMENT_INDEX` (the `.seg` segments of a stream shard's `idx/`) or `KIND_PROPERTY` (a property shard's index directory as it is). The importer reads every part's `metadata.json` (or a secondary part's `manifest.json`) out of the container into `Init.parts`; neither is sent, nor `smeta.bin`, nor an index's `.snp` manifest — the receivers introduce each `.seg` as an external segment, as a lifecycle migration sends them. Each chunk lists its file slices by their on-disk names; the liaison maps them to the names the receivers expect (`primary.bin` → `primary`, `<tag>.tf` → `tf:<tag>`, …). `pkg/transfer/format/nativefmt` holds the disk-to-wire table, shared by the importer and the liaison, and the one table of the (catalog, kind) pairs a native import carries (which kinds each catalog takes, which are duplicable, which catalog has secondary parts), which the liaison's topic table is tested against. Each data-node receiver keeps its own dispatch; engine contract tests (`banyand/{stream,measure}` `TestNativeSyncFileNameMatchesReceiver`, `banyand/trace` `TestNativeSyncFileNameMatchesStreamingMapping`) pin the two together. An element index of a catalog that has none fails the import before it is sent.
- **Rounds.** All series index payloads go first, then the parts, the element indexes and the property shards, so no part is visible before the index naming its series. A segment's series index is pushed from every source whose core parts some shard item of the unit pushes; in a group that holds an index-mode measure it is pushed from every source, even when the unit has shard items, because the index also holds the index-mode rows, which no shard item carries. Its targets are the union of the nodes of the unit's shards, or every node of the target stage when the unit has none (index-mode measure: the manifest records no series membership).
- **Relay.** The liaison validates the payload, routes it with the same stage-aware route as `ImportRows` ([5.4.3](#543-stage-aware-routing)), opens one cluster `SyncPart` stream per target through `queue.Client.NewChunkedSyncRelayClient` (a standalone process feeds its own chunked sync handlers in-process), and forwards each chunk to every live target, waiting for all acknowledgements before it reads the next. A chunk carries only its bytes and file slices: the liaison computes each relayed chunk's CRC-32 itself, and refuses a chunk over 4 MiB (`nativefmt.MaxChunkSize`; a data node's `--max-recv-msg-size` must stay above that plus framing overhead, as the default 10 MiB does) or a `KIND_PART` payload in which a listed part received no file. A frame over the liaison's own receive limit is answered with the `MESSAGE_TOO_LARGE` detail. A target that answers nothing to one frame within twice the receivers' default memory wait (10 minutes; `--{stream,measure,trace}-lifecycle-receive-mem-wait-timeout` defaults to 5) is dropped, so a busy receiver's own answer reaches the client rather than a relay timeout. `file_format_version` is the client's `Init.segment_version` (the liaison's own version for a property payload, which carries none). Each payload holds an in-flight request on its group from open until close, so a group deletion waits for the native payloads in flight on it. A target that fails is dropped and reported; when all failed, the liaison answers at once. Every failed target carries a `failure_cause`: `UNREACHABLE` (its stream could not be opened, nothing was sent), `REFUSED` (it answered a frame with a `SyncStatus` other than the expected one, a definite outcome), `STREAM_BROKEN` (sending or receiving failed, or the stream ended with a gRPC error status) or `NO_ANSWER` (it answered nothing within that wait and was dropped), so the client knows that, with `completion_sent`, a `NO_ANSWER` target may still be committing the payload; `completion_sent` tells when the target failed, `failure_cause` why, and a client takes a cause it does not know as unspecified. The response carries `requested_copies` and one result per target. A target that failed after its completion was relayed, without answering it, has `completion_sent` set: it may have committed the payload; a `REFUSED` target never has it.
- **Retries.** A payload the liaison refuses before answering is sent again within `--send-retry-timeout`: through the next liaison when it is unavailable or busy, through the same one when a schema is not live yet. A frame the liaison refuses as too large (`MESSAGE_TOO_LARGE`) is final, not retried. A payload some targets failed is sent again to those targets only (`Init.retry_nodes`: the liaison narrows the route to those nodes and refuses, with INVALID_ARGUMENT, a retry node that is not in the route), up to three attempts, with the back-off doubling from 1 s to 30 s that stream reopening uses; still failing, it fails the import (exit `3`) with the work items done and not done. A stream or trace part, or an element index, that may already be committed (a failed target with `completion_sent`, or a stream that broke after the completion was sent, reported as UNAVAILABLE/ABORTED without a detail) is not pushed again blindly, through the same or another liaison, since a second copy would store it twice. For a stream or trace part on a failed target with `completion_sent`, the import reconciles the counts instead ([7.4](#74-count-reconciliation)): a part that landed is done; anything else, a count that did not grow included, fails the import (exit `3`) with the decision logged, and a resume decides it from the counts then. An element index, and a stream that broke after the completion (it names no target), still fail the import at once. `requested_copies` is per shard, so the copies warning is not given for a series index, whose targets span several shards.
- **Index deletion masks do not travel.** An index payload carries the `.seg` segments of the source's live generation without its `.snp` manifest, which also holds the masks that hide superseded or deleted documents. The importer sends the segments newest first, so the series index receiver, which keeps the first copy of a document, keeps the newest one; a document the source masked without a newer copy is live again on the target. Carrying the masks (re-encoding live-only segments on export, or a mask with each external segment) is a follow-up in `pkg/index/native`.
- **Same version on every node.** Empty part files are relayed as zero-length slices, which only a data node of this version writes; as the design requires, do not import during a rolling upgrade.
- **Resume** (2026-11-13): the progress file, the summary with its `AMBIGUOUS` section, `--resume`, `--checkpoint`, `--checkpoint-every-batches` and `--resume-duplication-policy` are described in [7.3](#73-import-resume) and [7.5](#75-options-and-cli).
- **Options.** `--part-rate-limit` (`import.partRateLimit`) caps the bytes per second the whole import sends; `--part-chunk-size` (`import.partChunkSize`, 64 KiB to 4 MiB, default 1 MiB) sizes the frames; a size out of that range is a usage error (exit `1`). `--write-rate-limit`, `--liaison-rate-limit` (`connection.rateLimit`), `--node-rate-limit` and `--node-row-rate-limit` do not apply to the part push: the liaison relays part bytes without a bucket of its own, so `--part-rate-limit` is the only native throttle (the import warns when another limit is set). Why a user sets them: a native import copies whole segments, often many GiB, into a cluster that is usually serving live traffic, and every byte the client sends is written by the liaison to each replica's disk, so an unthrottled import can saturate the target's network and disks and slow its queries and writes; `--part-rate-limit` bounds that cost, as `--write-rate-limit` does for CSV. `--part-chunk-size` trades memory for round trips: the liaison holds one frame per target until every target acknowledges it, so a larger chunk means fewer acknowledgements on a high-latency link and a smaller one less memory on the liaison and the receivers, and a finer-grained rate limit; the 4 MiB cap keeps a frame under a data node's default receive limit.
- **Progress.** The live progress of a shard item moves after each acknowledged part.
- **Verify** counts the rows of the parts that landed per (catalog, group, target stage); the target's `Plan` counts the same parts, so stream and trace grow by exactly the rows sent. The push ends with `IMPORTED  <m> work item(s) of native files, <n> rows in the imported parts, in <time>`; series index, element index and property payloads count no rows.

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

Every decision and its reason is printed in the import table's `DUPLICATES` section (`<key>: import from <node>; skip <node> (same data as <node>)`, with the reason when more than one copy is imported), because this is the only place in the import that can decide to *send less data*. `--strict-coverage` promotes a multi-source unit to an error and writes nothing; `--all-sources` skips the judgement and pushes everything. `--all-sources` must disable both the shard-level and the segment-level judgement — the escape hatch is one switch, not two. It then adds, for every source whose core parts were *not* selected, one `SERIES_INDEX` stream per target node in that source's `LocateAll` union.

### 6.4 Multiple-source push behavior

| Catalog | Import behavior | Query | Disk |
|---|---|---|---|
| measure | Each part from every selected source becomes its own stream; no merging | Correct; the latest version is selected | Grows with the number of copies, and is **not** merged back |
| stream | Same as measure, plus the complete element index from every source | Correct; deduplicated by element ID | **Permanently** grows with the number of copies |
| trace | Same as above, but each core part and its secondary indexes must be sent together | ⚠️ Correct when the selected sources have disjoint coverage; **duplicate spans are visible** only in the overlapping cases below | Grows with the number of copies |
| property | The whole shard directory is sent and every record from every source is written | Correct. On an equal `mod_revision` `shard.repair` now lets the tombstone win (a live row never resurrects it), keeps the smaller `deleteTime` between two tombstones and never re-stamps one with now (fixed with the CSV import). The "two physical documents" split no longer reproduces since the native store folds same-docID writes | No amplification |
| index-mode measure | Only segment-level indexes exist. The import side must deduplicate **in memory by `_version` per docID across every source of a segment**, keep the maximum, then issue a single `UpdateSeriesBatch` — that primitive is last-write-wins and `InsertSeriesBatch` is keep-existing, so neither alone gives a reproducible winner across sources. The docID is the series' `EntityValues`, not the uint64 `series.ID` | Correct; the highest version is selected | No amplification |

- **trace is the only catalog whose query result can be wrong**, and it does not self-heal. But the exposure is narrower than "any multi-source unit", and every case is closed on the import side — the read path is not changed.

  **Selecting two sources duplicates only where they overlap.** A second source is pushed because its coverage is *not* contained in the first, and a span carries its own timestamp, so only the time both sources cover can repeat. A source chosen to fill a coverage gap may still partly overlap the first one; that overlap is pushed twice, and the CSV dry run warns about it for stream and trace. Overlap also arises when a source is rescued by `count-spread` / `no-single-covering` (its coverage *is* contained) or when `--all-sources` skips the judgement. Both are pushed as-is and reported; `--strict-coverage` stops instead.

  > **Row-level deduplication was designed and rejected.** The key is cheap — `_element_id` for stream, `(trace_id, span_id)` for trace, both fixed columns needing no schema, measured at 12.1M keys / 92 MB / 2.2 s. It loses because **a content key cannot tell where a duplicate came from**: if the source already held the same span twice (a lifecycle migration window, say), deduplicating removes the source's copy too, and the target stops reproducing the source. So no channel deduplicates rows — native cannot decode them, and CSV will not.

- **The element index cannot be reduced to a covering set.** Each source's `idx/` may be complementary rather than duplicated, and a dropped index entry never reappears. It was measured at exactly ×3.000 for three sources (4,000 → 12,000 documents, 765 KB → 2,295 KB), and the legacy engine's merge does not reclaim duplicates because the element index does not enable deduplication. Extrapolated to a 167-segment group with a 200 MB per-shard baseline, that is 65 GB at `replicas=1` and 98 GB at `replicas=2`. Provision `idx/` at `(replicas + 1)` times.
- **The segment-level series index follows the core-part selection, source by source.** Push the series index of **every source whose core parts were selected**, and in a group that holds an index-mode measure — whose rows only the index carries — push every source's. Do not compute a separate minimum shard-covering set over `sidxCoversShards`.

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

- **What deduplication on that index does and does not do**: the winner is the first arrival, not the newest. The discarded copy's index tag values become unqueryable — `Lookup(entity)` still matches, because the legacy engine's document `_id` is the entity value itself, while a filter on a non-entity indexed tag returns nothing for the losing value.
- **measure's storage does not converge.** The production default merge policy requires a write-amplification score of `≥ max(maxParts/2, minMergeMultiplier) = 4`, and equally sized parts score exactly their own count — three equal parts score 3 and are never merged. Queries are correct immediately regardless, because the read path deduplicates by version before any merge runs.

#### 6.4.1 What a duplicate costs, per catalog

Duplication is not a trace problem; it is a problem whose *cost* differs per catalog. A duplicate comes from overlapping sources on a first run or from running an import again: nothing on the target recognises a re-imported part, because the same payload imported twice produces two directories with different part IDs and `partMetadata` carries no hash, checksum or fingerprint. The costs hold for both channels; CSV stream and trace rows behave as their native parts.

| Catalog / payload | What a duplicate costs |
|---|---|
| **trace** | Duplicate spans, visible to users; there is no deduplication anywhere and nothing self-heals |
| **stream** | Queries stay correct (the vectorized path deduplicates by element ID in `pkg/query/vectorized/stream`'s `Distinct`). Disk grows **permanently** |
| **measure** | Queries stay correct (the read path keeps the larger version by `(seriesID, timestamp)` *before* any merge). Disk grows, and under the production merge policy equally sized parts are never merged back |
| **property** | Union by `(entity, mod_revision)`; idempotent, no amplification |
| **index-mode measure** | docID overwrite; idempotent, no amplification |
| series index | First write wins; later writes are discarded silently |
| element index | **No deduplication**: documents and disk grow linearly and are never reclaimed — measured at exactly ×3.000 for three sources, extrapolating to 65 GB at `replicas=1` and 98 GB at `replicas=2` for a 167-segment group |

A first run has no duplicate policy. `resumeDuplicationPolicy` applies only on a resume, and only to a stream or trace payload whose outcome the resume cannot decide: one value for every such payload, default `warn`; measure and property payloads are always pushed again ([7.3.5](#735-resumeduplicationpolicy)).

There is deliberately no `overwrite` anywhere. A part has no overwrite semantics and there is no API to delete a single part, so "replace the existing copy" cannot be built. Row-level deduplication on the CSV channel was designed and rejected: a content key cannot tell a duplicate the source cluster already held from one this import created, so deduplicating it would stop the target reproducing the source.

## 7. Resume

Both directions survive an interruption. An export and an import keep a progress file next to the artifact (or at `--checkpoint`), and the same command run again continues from it. [7.1](#71-detection-summary-and-confirmation) is the flow both share (detection, the summary, the confirmation, the process lock and the exit codes), [7.2](#72-export-resume) the export, [7.3](#73-import-resume) the import, [7.4](#74-count-reconciliation) how an ambiguous stream or trace part is decided, [7.5](#75-options-and-cli) the options, [7.6](#76-limits) the limits and [7.7](#77-tests) the tests.

Export checkpoints per **unit**, appending one to `done` only after every file of that unit has passed its CRC32 check and been renamed. Volume splitting does not change that granularity: all volumes of a unit share one directory prefix, so the whole-directory cleanup on resume still applies. Import checkpoints per work item, and within it per native payload or CSV record: a part, and even one file, may span volumes, so the import side joins every volume of a unit before it groups parts, and checkpoints a part only after the whole logical part is acknowledged — never at the end of a physical volume. For trace, the core part and its secondary index parts travel in one `PART` session, or the receiver keeps a core part whose secondary index never arrives. Index-mode measure and the segment-level series index have no shard axis at all and resume at segment granularity.

### 7.1 Detection, summary and confirmation

Both commands share one flow, in the package `pkg/transfer/resume`:

1. **Detect.** The progress file is `<output>/.export-progress.json` (export) or
   `<input>/.import-progress.json` (import, [7.3.1](#731-progress-file); one artifact imported into
   several clusters needs a `--checkpoint` per cluster after the first), or the path given by
   `--checkpoint` / `export.checkpoint` / `import.checkpoint`. A run that finds no progress file is
   a fresh run, except that an import given `--resume` exits `1` (`--resume given but no progress
   file at <path>`). A `--schema-only` import uses no progress file: it reads none and takes no lock,
   and with `--resume` or `--restart` prints `NOTE a schema-only import uses no progress file:
   --resume and --restart are ignored, <file> is left as it is`.
2. **Check the bindings** ([7.2.1](#721-progress-file), [7.3.1](#731-progress-file)). A file that cannot
   be parsed, has another `kind` or `version`, or whose bindings differ from this invocation exits
   `6` and **deletes nothing**. The message names every differing binding. An import also compares
   its plan with the recorded one here ([7.3.2](#732-resume-flow) step 3), before the question. An
   import not at a terminal and without `--resume` is no exception: it reads the target as a run at a
   terminal does, but not the rows of the artifact (they are checked only by a run that goes on), prints the summary of step 3 (and what is left), and only then exits `1`
   (the refusal of step 4). That read-only stretch writes nothing to the target, writes, changes or
   removes no progress file (a leftover cursor side file is not folded into it; only the temporary
   files a crash left are removed, as at the start of every run), and takes the lock as every run does.
3. **Print the resume summary** to stdout, as everything but a failure (its error and the hints
   before it, which go to stderr):

   ```text
   RESUME   ./backup-20260914/.export-progress.json  (last updated 2026-11-12T17:14:03+09:00, 41m ago)
            session 9f2a0c… (export)
   DONE     2 group(s), 863 unit(s), 5212 file(s), 504.0 GiB
   CATALOG  GROUP      STAGE  UNITS  FILES  SIZE
   measure  sw_metric  hot    120    1212   18.3 GiB
   stream   sw_record  warm   743    4000   485.7 GiB

   LEFT     2 group(s), 577 unit(s), 625 shard(s), est. 337.6 GiB
   NODE          CATALOG  GROUP      STAGE  SEGMENTS  SHARDS  PARTS  EST-ROWS  EST-SIZE   TIME-RANGE
   data-1:17912  measure  sw_metric  hot    48        96      288    12.4M     7.1 GiB    09-01 ~ 09-07
   data-2:17912  stream   sw_record  warm   529       529     1587   31.0M     330.5 GiB  09-01 ~ 09-07
   ```

   The first table lists what is done, per catalog, group and stage. The second is the plan of **every unit not done** —
   also those that were in flight when the run stopped, which are exported again from their start — read from the plan side
   file, so the cluster is not contacted; it is the table `--dry-run` prints, without its `NODES` line unless a node is missing.
   `shard(s)` counts each shard of a group once, however many nodes hold a replica of it (a unit is counted by its
   catalog, group and shard id), and a resumed export prints no `SNAPSHOT` line (that of the dry run names the disk a
   new snapshot needs; a resume reads the one the first run pinned).
   Run times (when a file was written or an import finished), here and in the progress files, carry the offset of the local time zone of the machine running `bydbctl`; the data times (`TIME-RANGE`) are UTC, and so is the `exportedAt` of `manifest.json` (`…Z`).

   The import summary has the same lines, counts work items instead of units (column `ITEMS`), has no
   shard count in `LEFT`, which has the rows left instead (`LEFT     <g> group(s), <n> work item(s), <rows> rows, est. <size>`, the rows only when counted, and of a CSV item a cursor has acknowledged part of only the rows after it), and adds an `AMBIGUOUS` section: one line per payload whose landing is not known
   ([7.3.4](#734-the-ambiguous-set)), with what resume will do with it (`reconcile`, `re-push`, or
   the `resumeDuplicationPolicy` value and where it comes from). An export summary without a recorded plan
   says `LEFT unknown: no plan is recorded yet; it is read from the export session when the export
   resumes`. A finished import ([7.3.1](#731-progress-file)) is summarised as `FINISHED <file>
   (finished at <time>, <age> ago)`, the `DONE` line and the table of what is done; it has no `LEFT` or
   `AMBIGUOUS` lines and no table of what is left.
4. **Confirm** (`resume.ConfirmResume`). Every refusal ends with how to start over instead:
   export `to start over: bydbctl data release-session --id <id>, then delete what is in <output>
   and export again` (`exporter.startOverHint`; a progress file outside the output directory names
   itself and its plan side file too: `then delete <file>, <file without .json>-plan.json and what is in <output>
   and export again`); import `give --restart to import from scratch (<file> is kept as a .bak file);
   stream and trace data already imported is then stored twice`.
   - `--resume` (flag, `BYDBCTL_RESUME` for export, `BYDBCTL_IMPORT_RESUME` for import, no plan
     field) continues without asking. Without a progress file the variable starts a fresh import
     (the import prints `NOTE BYDBCTL_IMPORT_RESUME is set, but there is no progress file at <path>:
     a fresh import starts`), while the import's flag exits `1`; a resumed import names the source
     (`resume: <file> is continued without asking (--resume|BYDBCTL_IMPORT_RESUME)`). A finished import asks nothing ([7.3.1](#731-progress-file)).
   - Otherwise, when stdin is a terminal, the operator is asked `Resume? [y/N]` below the summary:
     on stdout when that is a terminal; when stdout is redirected and stderr is a terminal, on stderr,
     after the summary lines are written there as well (stdout keeps them too); with `-o json|yaml`
     on stderr, where the summary went (wired through the `Confirm` option of `exporter.Env` and its
     import twin; the export's takeover question of `--preempt` goes to the same stream). Anything but
     `y`/`yes` exits `1` with `not resumed; <file> and the files it records are left in place;
     <start over>`. Ctrl-C at the question exits `3` and leaves everything in place.
   - Otherwise (stdin is not a terminal, or no stream to ask on is) the run prints the summary and exits `1` with
     `a previous run left <file>; add --resume to continue it (needed when not at a terminal);
     <start over>`.
5. `--dry-run` with a progress file prints the summary and then the plan of what is left, and never
   deletes or writes anything. An import exits as its dry run does (`0`/`2`, or `1` when a file cannot be created next to the progress file), or `6` when its plan
   differs from the recorded one or the target check of [7.3.1](#731-progress-file) refuses (both
   layers run under `--dry-run` too); an export exits `0`, since what is left is inventoried by the
   session already. For export, `--output` or `--checkpoint` is accepted with `--dry-run` only when
   it points at an existing progress file, and only as a flag or in `plan.yaml`: `BYDBCTL_OUTPUT`
   and `BYDBCTL_CHECKPOINT` are defaults of real exports and alone never start a resume preview;
   with `--output` given as a flag or in `plan.yaml`, a progress file from `BYDBCTL_CHECKPOINT` is
   kept and previewed, since it is where that export records its progress. With
   `--checkpoint` and no `--output` the dry run reads the output directory the progress file
   records. It does not call `Plan{read}` (that renews the lease), so a lost session is only found
   by the real resume. An export dry run makes the checks a resume makes without the cluster —
   the bindings, the plan side file, the files of the finished units, the `schema/` files they
   were exported with, the data nodes with units left — and exits `6` as the resume would; it then
   prints `NOTE export session <id> was not contacted: …`. With `-o json|yaml` the plan of
   what is left carries `resumed: {path, done, total}` (`exporter.ResumedProgress`). It does not take the lock
   ([7.1](#71-detection-summary-and-confirmation), *process lock*), so it also reads an export that
   is running.

The exit codes of resume, in `pkg/transfer/exitcode`:

| Code | Meaning |
|---|---|
| `6` | Resume impossible: progress file or a side file unusable, a binding differs, the export session is gone or a data node does not answer for it, a file of a finished unit is missing or short, the exported `schema/` files or the live schema (step 4) changed while units are done, or the plan differs from the recorded one; for an import also a target that is not the recorded one or no longer holds what the progress file records as imported ([7.3.1](#731-progress-file), layers 1 and 2). No data was sent and the progress file is kept. Every export exit `6` comes before anything in the output directory is deleted. One import refusal comes late and has written the schema: a re-checked plan that differs ([7.3.2](#732-resume-flow) step 3); a plan side file (which holds the baseline) that is missing or has another digest is refused when the progress file is loaded, before the target is read, and so is a progress file that records a baseline or pushed rows but names no plan side file |
| `7` | Import halted by `resumeDuplicationPolicy: block`; what was imported before is valid |

**Process lock.** `resume.LockWith(<file>, log)` (`resume.Lock` warns on stderr) creates the
directory of `<file>.lock` and takes the file lock of `pkg/fs` `CreateLockFile` on it
(`flock(LOCK_EX|LOCK_NB)` on Linux and macOS, `LockFileEx` on Windows). The kernel releases that
lock when its holder ends, however it ends, so a lock file a killed run left behind is no lock:
the next run locks it at once. Holding the lock, the process writes its holder into the file, a
newline and one line of JSON (`resume.Holder`: pid, host, start time, the command up to its first
flag); the leading newline is the byte `LockFileEx` locks, which Windows keeps others from reading.
Before it tries the lock a process reads the holder the file names (`CreateLockFile` truncates the
file), so that:
- held, it exits `1` with `another bydbctl is using <file> (locked by another process;
  lock file <file>.lock, held by pid <n> on <host> since <time> (<command>)): wait until it ends,
  or stop it`, and writes the holder back into the file it truncated while that is still the same
  file and still empty;
- taken over a file that named a holder, it warns `WARN taking over a stale lock of <holder>:
  <file>.lock; the process that held it is gone`.

Only a lock another open file holds (`EWOULDBLOCK`, `ERROR_LOCK_VIOLATION` on Windows; `fs.ErrLockHeld`)
is "another bydbctl". Any other failure to open or lock the file (a read-only file system, an I/O
error, `no locks available`) exits `1` with `progress file <file> cannot be locked: <cause>`, so the
operator is not sent after a process that does not exist.

The unlock function removes the file while it still holds the lock and it is still the file
locked (`os.SameFile`), then releases it; on Windows, where an open file cannot be removed, it
releases first and removes the file unless another process has it open. A process that locked a
file no longer at the path (its holder removed it meanwhile) closes it and tries again. The caller
runs unlock on every way out, success, failure and interrupt. A lock file that is not a regular
file (a symlink, say) is refused, since `CreateLockFile` would truncate its target: `progress file
<file> cannot be locked: <file>.lock is not a regular file (...); remove it if no bydbctl uses
<file>`. The lock is only as good as the file system's `flock`: on NFS, Linux maps it to NFS byte-range
locks, which need the lock manager and exclude other hosts only when the mount does not keep locks
local (`nolock`, `local_lock`); where the file system has no file locks at all, every run fails
with exit `1`, `cannot be locked` naming the error (`no locks available`). The export takes
it for its whole run, fresh or resumed, before it checks the output directory, so the checks and
the cleanup never race another export of the same directory.

**Writing a progress file.** One persister goroutine per run owns the file; workers send it events
and never write it themselves. Every write of it is atomic: write a temporary file with a unique
name next to it (`.<base>.*.tmp`), fsync, rename, fsync the directory (`resume.SyncDir`, which
skips it on Windows and on a file system that cannot fsync a directory; `manifest.Write` fsyncs
the directory of `manifest.json` itself), mode `0600`. The JSON is indented by two spaces and ends in a newline.
Leftover temporary files are removed with the progress file (`resume.Remove`) and by the export's
cleanup. A change that is not worth a write of its own is queued lazily (`UpdateLazy`): it is
written with the next write, or when the persister closes. What never changes after it is first
written goes to a side file `<file>.<name>` written once (`resume.WriteSide`), whose digest the
progress file records and a resume checks (`resume.ReadSide`; a mismatch exits `6`). The digest of a list of fields (the
selectors, the source nodes, the options) hashes each field as its byte length, a colon and its bytes, so that no two lists
share a digest, whatever characters (a newline, say) the fields hold. A write
error fails the run (exit `3`): continuing without a record would make the next resume wrong. The one exception is the CSV import's cursor side file
([7.3.1](#731-progress-file)): the worker that sends a batch or gets an acknowledgement replaces it itself, under
the tracker's lock, without the persister and without fsync: a temporary file next to it, named as one
of the side file's temporary files, is renamed over it.

### 7.2 Export resume

#### 7.2.1 Progress file

```json
{
  "kind": "export-progress",
  "version": 2,
  "format": "native",
  "session": {
    "id": "9f2a0c…"
  },
  "startedAt": "…",
  "updatedAt": "…",
  "outputDir": "/abs/path/backup-20260914",
  "elapsedMs": 2462000,
  "bindings": {
    "selectors": [
      "catalog=measure,groups=sw_metric"
    ],
    "selectorsDigest": "sha256:…",
    "sourceNodes": [
      "data-1:17912",
      "data-2:17912"
    ],
    "sourceNodesDigest": "sha256:…",
    "options": {
      "compress": "none",
      "csvAnnotations": "false",
      "format": "native",
      "includeSchema": "true",
      "maxFileSize": "0"
    },
    "optionsDigest": "sha256:…"
  },
  "schemaFiles": [
    {
      "path": "schema/group.jsonl",
      "bytes": 812,
      "crc32": "0x05f5e0fe"
    }
  ],
  "planDigest": "sha256:…",
  "sourceTimezones": {
    "data-1:17912": "Asia/Shanghai",
    "data-2:17912": "Asia/Shanghai"
  },
  "groups": [
    {
      "name": "sw_metric",
      "catalog": "MEASURE",
      "stages": [
        "..."
      ]
    }
  ],
  "inflight": {
    "data-1:17912": {
      "unit": "measure/sw_metric/hot/20260907",
      "startedAt": "…"
    }
  },
  "done": [
    {
      "node": "data-1:17912",
      "unit": "measure/sw_metric/hot/20260907",
      "files": [
        {
          "path": "nodes/…/shard-0-00001.bnc",
          "bytes": 402653184,
          "crc32": "0x9f2a1b34",
          "entryCount": 47,
          "rowCounts": {},
          "shard": 0,
          "segmentLevel": false
        }
      ],
      "warnings": [],
      "elapsedMs": 81234
    }
  ]
}
```

- `version` is the format version of the progress file and of its plan side file (`progressVersion`).
  A resume reads a file of the same `kind` and `version` only: a change to the format of either
  raises the version by one, and the side file is written only after the version of the progress
  file was accepted, or by the run that starts the progress file, so the version of the progress
  file covers it. A file of another version, written by an older or a newer bydbctl, exits `6`
  before anything of it is read or deleted, with no summary and also under `--dry-run` and `-o json|yaml`,
  and the message names the file, both versions and the two ways on: resume with the bydbctl that
  wrote it, or give the export up (release its session, delete what is in the output directory and
  export again, or export into a new output directory). The session id is named when the file
  records one, else the general form `bydbctl data release-session --id <id>`.
- `groups` is the manifest's group list as the first run wrote it, so the manifest of a resumed
  export does not depend on the live group list. `sourceTimezones` are the time zones of the
  cluster's data nodes when the export started (node id → zone), which the manifest records even
  of a node that has left the cluster since; a progress file written before they were recorded
  lacks them, and the live zones are used then.
- `outputDir` (absolute) is a binding too.
- `selectorsDigest` covers the normalised selectors. `options` records the options that change
  the bytes of a file: `format`, `compress`, `maxFileSize`, `csvAnnotations`, `includeSchema`
  (`optionsDigest`, their digest, is recorded only). A resume compares them by value, all but
  `maxFileSize`: that one only decides where the files of a unit roll over, an interrupted unit is
  exported again from its start and the manifest does not record it, so a resume may change it.
  The units already exported keep their files, the units left get the new limit, a `WARN` names
  both values, and the progress file records the new one (`rebindMaxFileSize`). Rate limits,
  parallelism, timeouts and liaison addresses may change between runs too. A refusal says what
  differs field by field, e.g. `options differs (…):` followed by
  `compress: recorded gzip (default), now none`: sizes print in IEC units, and a value equal to
  the option's default is marked `(default)` (`resume.OptionDiffs`, `resume.SetDiffs`).
- `sourceNodes` (and `sourceNodesDigest`) are the data nodes of the cluster at the first run,
  recorded only. They are not compared as a set (`checkMembers`): every data node that still has
  units to export (every recorded source node while no plan is recorded) must still be a data node
  of the cluster, so a cluster that gained a node resumes; a node whose units are all done is not
  checked here. A refusal exits `6`: `data node(s) <nodes> still have units of this export to
  export but are not registered in the cluster now; nothing was deleted. If a node is restarting,
  run the same command again later; only if it is gone for good, give the export up: <start
  over>`. Whether the session's inventory is unchanged is the plan comparison of
  [7.2.2](#722-resume-flow) step 5.
- The plan is in the side file `<file without .json>-plan.json` (`{"frames": [<protojson UnitFrame as a JSON
  object>, ...], "answeredNodes": [...]}`, indented like the progress file; each frame is an object, not a
  string holding JSON), written once before any `Export` stream opens, its digest in
  `planDigest`; a resume reads it and checks the digest. It stores exactly what `fillManifest`
  needs from the Plan frames, so the manifest of a resumed export is built without the first
  run's memory, and the progress file the persister rewrites stays small.
- A done file keeps the `rowCounts` (CSV) its `FileFinished` reported, so the manifest of a
  resumed export carries the same counts as an uninterrupted one.
- `elapsedMs` is the time all runs took, up to the last write of the file; the summary of a resumed
  export prints it as the cumulative time.
- A unit is keyed by `catalog/group/stage/segment` (property: `property/group`) per node; the
  on-disk subtree of a (node, unit) is `nodes/<NodeDir>/<catalog>/<escGroup>[/seg-<suffix>]`.

| Moment | Record | Persist |
|---|---|---|
| `created` frame | `session.id` | yes — before anything else, so the id is never only on the terminal |
| last Plan frame | the side file `<file without .json>-plan.json`, then `planDigest` | yes — no `Export` stream opens before both writes |
| unit started | `inflight[node]` | lazily: written with the next write (another unit finished) or when the run stops; only the summary counts it, the cleanup does not need it |
| `file_finished` | (verified, fsynced and renamed by the receive loop) | no |
| `unit_finished` | append to `done`, clear `inflight[node]` | yes, asynchronously and at most once a second (`progressWriteInterval`: the file lists every file of every finished unit and grows with the export): a crash can lose the `done` entries of the last second; their files are then deleted by the cleanup and those few units exported again |
| all units done | write `manifest.json`, **then** release the session, then delete the progress file and its plan side file (`finishExport`): a crash in between leaves the progress file, and the same command finishes the export again without the session; the other order could leave a session nothing on disk names | — |

#### 7.2.2 Resume flow

`RunExport` (`pkg/transfer/exporter/export.go`) on a directory holding a progress file:

1. The lock of the progress file is taken ([7.1](#71-detection-summary-and-confirmation)), then
   `CheckOutputDir` accepts a non-empty directory when it holds the progress file (the CLI's
   `validateExport` makes the same exception). The lock file does not count. Neither does what an
   export killed before its first progress write leaves when the progress file is in the output
   directory — `schema/` and temporary files of the progress file, nothing else: a fresh export
   removes them under the lock and says so. Any other content is refused (exit `1`), naming what
   the directory holds and `rm -rf <dir>` to clear it. A progress file in a subdirectory of the
   output directory is refused (exit `1`): that subdirectory would end up in the artifact.
2. Bindings check (with the plan side file, which must match `planDigest`, or the resume exits `6`), summary
   ([7.1](#71-detection-summary-and-confirmation)). A progress file with `done[]` entries but no
   `planDigest` is unusable (exit `6`): the plan is written before any unit is exported, and
   without it the finished units would be exported again over their own files.
3. **Local checks** (`localChecks`), before the confirmation. They read only the output directory
   and the cluster's node list, so a refusal neither asks first, nor renews the lease, nor deletes
   anything. Every `done[]` file must exist with its size; a missing or short one exits `6` (the CRC
   was checked when it was written; it is not re-read). Neither `nodes/`, nor a directory below
   it on the way to a done file, nor the file may be a symlink (exit `6`, `<path> is a symlink,
   which a resume does not follow: move what it points to in its place; nothing was deleted`):
   the cleanup walks `nodes/` without following links, so the files behind one would drop out of
   the export. Anything else below `nodes/` that is neither a regular file nor a directory (a
   symlink deep in an unfinished unit, say) exits `6` here too, with the start-over hint, rather
   than in the cleanup after the confirmation and the lease renewal. Once a unit is done, the files under
   `schema/` must match `schemaFiles`; they are loaded with `schemasync.Load(dir)`, which rebuilds
   `schemasync.Result` (revisions, measures, files) from the JSON Lines. A mismatch or an unusable
   schema exits `6`, since a finished CSV unit was checked against the first schema. Unless every
   unit is done, the data nodes with units left must still be data nodes of the cluster
   (`checkMembers`, [7.2.1](#721-progress-file)). Then the confirmation
   ([7.1](#71-detection-summary-and-confirmation)).
4. **All units done.** An export whose units are all done only lacks its manifest: the cleanup of
   step 6 runs, `manifest.json` is written from the progress file, the session is released if it is
   still held, and the progress file is deleted. No `Plan{read}` is needed, so a session lost by
   now does not matter.
   **Live schema check** (`checkLiveSchema`). An export with finished units (and `includeSchema`)
   lists the live schema (`schemasync.Live`) and compares it with the `schema/` files the first run
   wrote (`schemasync.Result.Fingerprints`) by mod revision, which the registry sets on every create
   and every update that changes it. The content is not compared: a resource read back from a
   schema file lacks the fields this bydbctl does not know. A difference exits `6`, naming each
   resource, before `Plan{read}` (which renews the lease) and before anything is deleted.
   - Native: every group, stream, measure, trace, property, index rule, index rule binding and TopN
     aggregation of the recorded groups, since a native file carries no schema revision a node
     could check. A resource changed or removed refuses; one added does not
     (`schemasync.Restrict`): the units left come from the session snapshot frozen in the first
     run, which cannot hold it, and the exported schema still describes them. A `NOTE` names it.
   - CSV: the streams, measures, traces and properties (`schemasync.DataKinds`) of the recorded
     groups that still have units left (`schemasync.Select`). A node encodes a CSV file with every
     resource of these kinds in the group and reports their mod revisions, which are checked against
     the exported schema, so a change of one of them — changed, added or removed — would fail the
     first unit left only after the session was read and the unfinished files deleted. Index rules,
     bindings, TopN aggregations and finished groups are not in a CSV header and do not count.
   Without finished units the schema is written again (step 7); a group the progress file records
   that the cluster no longer has exits `6` first, naming it, before `Plan{read}`: the schema
   written again would lack it while the session's snapshot still holds its units.
5. **Renew and compare.** One `Plan{selectors, read{id}}` through the primary liaison, collected
   with `cluster.CollectPlanFrames`. It renews the lease on every node (`planner.go` `resolveRoots`).
   The read names the nodes that hold the session (as below) in `ReadSession.required_nodes`.
   - A `FAILED_PRECONDITION` / `NOT_FOUND` answer exits `6`: the snapshot is gone, and an export
     against a new point in time cannot be mixed with this one. The message gives the start-over
     command. The liaison fails the call with `FAILED_PRECONDITION` only for an expired or
     unreadable lease on a required node; on any other node (its units are all done, say, and it
     was partitioned past the lease, not swept yet) it reports that node in `unreachable_nodes`,
     so such a node does not refuse the resume. A liaison older than the field ignores it and
     fails the call for any node: the resume is then refused with `6`, which is safe. A resume therefore relies on a liaison of
     the same version that supports `required_nodes`: on an older liaison the lease of a node whose units are all done may have
     expired, and the resume fails.
   - Any node in `unreachable_nodes` that holds the session (a node of the recorded plan; without a
     plan, a recorded source node) exits `6` too (the liaison reports a node that no longer holds
     the session like one it cannot reach): the message names the nodes, says to run the same
     command again later if a node is restarting or unreachable, and to start over only if the
     session is lost. A node the cluster gained since never held the session and is reported
     unreachable too: it does not count.
   - The inventory must equal the recorded `plan` (same nodes, same units, same shards and part
     statistics). The server filters the frozen snapshot through the live group registry, so a
     group deleted since the first run shows up here; any difference exits `6`.
   - A progress file that records the session but no plan yet (a crash between `created` and the
     plan write) takes the inventory `Plan{read}` returns and records it.
   - Any other error of `Plan{read}` (`UNAVAILABLE`, …) is exit `3`: the resume can be retried.
   A refusal of the server on the resume path (`PERMISSION_DENIED` from the schema listing, say),
   which a fresh export reports as a preflight rejection (exit `2`, nothing exported), is exit `3`
   too (`resumedCode`): earlier runs exported units, and the same command can run again.
6. **Clean up.** Delete every file under `nodes/` that no `done[]` entry lists — `.tmp` files,
   finished volumes of unfinished units, the whole subtree of every `inflight` unit — then remove
   empty directories, a manifest temporary file and the temporary files of the progress file. Names are never derived from the crashed run's
   memory, so a re-run that emits fewer volumes leaves nothing behind.
7. **Schema.** While no unit is done, `schema/` is written again from the live schema, for the
   groups the first run recorded only (a group created since is not added); the new files and the
   groups are recorded. The CSV mod-revision check of every new file runs against the schema in
   use, so a schema changed between the runs fails the unit as it would in one run.
8. **Export what is left.** Start the heartbeat, which checks the answers of the data nodes that
   hold the session only (a node gained since does not count) and sends them as
   `required_nodes`, so an expired lease elsewhere is an ignored `error` frame rather than a lost
   session; a node leaves the list once its worker exported all its units. Only a resume shrinks
   the list: a fresh export sends every node of the plan throughout. An older liaison ignores the field and
   reports any expired lease as a lost session; build the jobs from `plan` minus `done`. A unit is
   always exported again **whole**: the node keeps no per-unit state, and with `nodeRowRateLimit`
   a CSV unit is not byte-reproducible (gzip flushes on wall-clock time), so no partial unit is
   ever spliced.
9. **Finish.** Build `manifest.json` from `done[]` plus the new units, write it, release the
   session, delete the progress file, print the usual summary. A resumed run says
   `RESUMED 863 unit(s) from the previous run(s)`, its total line ends `in <time> (this run)`, and
   a last line gives `cumulative time <time> over every run, the first started at <time>`. Its
   `exportedAt`, written in UTC (`…Z`), is the first run's `startedAt`, by design: the session's snapshot was taken then,
   so the data is that of an export started at that time. The manifest therefore equals an
   uninterrupted export's in every field but `exportedAt`, whose value is the first run's start
   rather than the start of the run that writes it. A fresh export writes its own `startedAt` too,
   so a manifest written again (a crash after it was written and before the progress file was
   deleted) is the same bytes, and an import begun from the first one still resumes.

A `--schema-only` export into a directory holding a progress file is a usage error (exit `1`), and
so is a `--checkpoint` that is a directory or the output directory itself, checked before anything
is contacted. `--preempt` (or `BYDBCTL_PREEMPT`) on a resume is ignored with a `NOTE`: a resumed
export creates no session, it reads its own again. A
fresh export creates the directory of the progress file and probes that a file can be created there
before the schema and the session (exit `1`). A session create that fails removes the progress file
together with `schema/`, so the directory is empty again; one interrupted after the create
announced the session id releases that session once, printing `stopping: releasing export session
<id>, waiting up to 30s; press Ctrl-C again to stop now`, and prints the `release-session` command
when that fails. The first Ctrl-C or SIGTERM of `data export` or `data import` stops it cleanly; a
second one is no longer caught and stops `bydbctl` at once (`connectLiaisons`).

A failure or interrupt once the session exists (resumed or not) keeps the session and the progress
file (exit `3`) and prints both ways on: run the same command again to resume (`--resume` when not
at a terminal), or release the session and delete the directory's content to give up. A session the
heartbeat found lost, and a CSV schema change detected after some units are done, cannot be resumed:
their message gives only the second way, the progress file included in what to delete.
A node stream refused with `FAILED_PRECONDITION` is taken for a lost session only once a heartbeat to that node
(`HeartbeatOnce`, `sessionGone`) finds the session gone: a node answers it for other reasons too (a catalog the format
cannot encode, a CSV layout clash), and then the stream's own error stands, with the hints to resume or release. A
heartbeat that fails otherwise settles nothing.

### 7.3 Import resume

#### 7.3.1 Progress file

```json
{
  "kind": "import-progress",
  "version": 2,
  "format": "native",
  "startedAt": "…",
  "updatedAt": "…",
  "finishedAt": "…",
  "bindings": {
    "manifestDigest": "sha256:<raw manifest.json bytes>",
    "liaisons": [
      "liaison-0",
      "liaison-1"
    ],
    "targetNodes": [
      "data-a",
      "data-b"
    ],
    "options": {
      "allSources": "false",
      "includeSchema": "false"
    }
  },
  "foldSeq": 12,
  "verifyPending": true,
  "baselineAt": "…",
  "pushed": [
    {
      "catalog": "measure",
      "group": "sw_metric",
      "stage": "hot",
      "kind": "shard",
      "rows": 1234567
    }
  ],
  "duplicates": [
    {
      "catalog": "stream",
      "group": "sw_record",
      "stage": "hot",
      "kind": "shard",
      "rows": 54321,
      "payloads": [
        "stream/sw_record/hot/20260907/shard-0 part 7 of data-1:17912 on data-b"
      ]
    },
    {
      "catalog": "trace",
      "group": "sw_trace",
      "stage": "hot",
      "kind": "shard",
      "rows": 4000,
      "payloads": [
        "trace/sw_trace/hot/20260907/shard-0 rows after nodes/…/shard-0-00003.csv.gz record 192000 up to record 196000 (4000 row(s) sent)"
      ]
    }
  ],
  "skippedPayloads": [
    "stream/sw_record/hot/20260908/shard-0 ELEMENT_INDEX of data-1:17912 on data-a"
  ],
  "planDigest": "sha256:<bytes of <checkpoint without .json>-plan.json>",
  "inflight": {
    "3": {
      "key": "stream/sw_record/hot/20260907/shard-0",
      "source": "data-1:17912",
      "kind": "PART",
      "partID": 7,
      "rows": 54321,
      "nodes": [
        "data-a",
        "data-b"
      ]
    }
  },
  "done": {
    "stream/sw_record/hot/20260906/shard-0": {
      "complete": true,
      "cursor": null,
      "landed": {
        "data-a": 98765,
        "data-b": 98765
      },
      "tainted": [
        "data-b"
      ]
    },
    "stream/sw_record/hot/20260907/shard-0": {
      "complete": false,
      "parts": {
        "data-1:17912/7": {
          "nodes": [
            "data-a"
          ],
          "missing": [],
          "completionSent": [
            "data-b"
          ],
          "causes": {
            "data-b": "no_answer"
          },
          "rows": 54321
        }
      },
      "blobs": {
        "data-1:17912/SERIES_INDEX": {
          "nodes": [
            "data-a",
            "data-b"
          ],
          "missing": []
        }
      },
      "cursor": null
    }
  }
}
```

`noBaseline: true` replaces `baselineAt` when the first run could not count the target and needed
no baseline (below). `version` is the format version of the progress file and of its side files, the plan and the cursor (`progressVersion`). An import resumes a file of the same `kind` and `version` only: a change to the format of any of the three raises the version by one, and a side file is written only after the version of the progress file was accepted, or by the run that starts the progress file, so the version of the progress file covers the side files. A file of another version, written by an older or a newer bydbctl, exits `6` before anything of it is read, with no summary and also under `--dry-run` and `-o json|yaml`; nothing is written or deleted. The message names the file, both versions and the ways on: resume with the bydbctl that wrote it; `--restart` imports from scratch, setting the progress file and its side files aside as `.bak` files, and stores the stream and trace data already imported into the target once more; `--checkpoint <another path>` runs the import separately.

- The file is `<input>/.import-progress.json` unless `--checkpoint` / `import.checkpoint` names
  one; its side files are named `<path without .json>-cursor.json` (CSV cursors)
  and `-plan.json` (the plan and the baseline) in the same way (`resume.SidePath`; a path that does not end in `.json` just gets
  `-<name>.json`), and the lock is `<path>.lock`. A progress file path (default or `--checkpoint`, export or import) that
  ends in `-plan.json` or `-cursor.json` is exit `1` (`resume.CheckSideCollision`): it would be the side file of another
  progress file, or of its own, and one would overwrite the other. An import's `--checkpoint` that names `manifest.json`
  or a file the manifest lists is exit `1` too, as `--restart` would set that file aside. The name does not depend on the target: another cluster needs its own `--checkpoint`,
  and the same cluster imported into again needs `--restart`. Every run over the file (fresh
  resume, finished, `--dry-run` too) checks the target in two layers, and a refusal of either exits
  `6`, writes nothing and ends with `to import this artifact into another cluster give --checkpoint
  <another path>; to import it again into this cluster give --restart`.
- **Layer 1, who the target is** (`diffBindings`, before the target is read). `liaisons` are the
  names of the target's liaisons: those the cluster state reports registered (route table
  `tire1`) and the one connected to, sorted (`cluster.Info.Liaisons`); `targetNodes` the names of
  its data nodes (`tire2`), sorted; a standalone server's own name for both. The check is not
  equality: the current liaisons must intersect the recorded ones (any liaison of the cluster may
  be used, liaisons may be added, removed or restarted), the current data nodes must intersect the
  recorded ones, also when every item is complete, and every target node of a work item of the
  recorded `plan` that is not complete must still be a data node of the target (the nodes of
  complete items only seed reconciliation). A file without `liaisons` (written before they were
  recorded) cannot be checked and is refused. The bindings are in
  clear, so that a refusal prints every difference per field (`manifestDigest differs: recorded
  …, now …`, `liaisons differs (…):` and `targetNodes differs (…):` with `recorded [...], now [...]:
  none of the recorded … is a … of the target` or `target node data-a of the recorded import is no
  longer a data node of the target`, `options differs (…):` with one `allSources: recorded false,
  now true` line per option). `options` holds `allSources` and `includeSchema`; a `schemaOnly` option in a progress file is ignored, since a schema-only import reads no progress file.
- **Layer 2, what the target holds** (`checkPresence`, after the target is read, before a resumed
  push and before `already imported`). Names do not tell a cluster apart from one deployed from the
  same Helm values, StatefulSet or compose file (node names default to the host name), or from the
  recorded one wiped and deployed again. So for the work items the file records as complete or as having data landed (a complete item, also one without rows, such as the series index of a regular measure; the done parts of an item; a CSV cursor past its first row): the schema comparison must not CREATE (or, for an artifact without a schema, miss) any resource of their
  groups (`the progress file <file> records data this import wrote, but the target lacks
  <resources>; the target looks reset or is another cluster`, naming the first five resources and counting the rest), and one session-less `Plan` of those groups must count rows wherever the file records rows as landed: for a native stream
  or trace item on every target node with landed rows, in its shard, in a segment overlapping the
  item's rows (a node the count no longer reaches, unreachable or gone, is checked per shard over
  the nodes that answered); for a property item, documents in its group; for any other shard item (CSV, native measure) in its shard, summing the replicas
  only to tell that the shard holds rows, when the shard number of its target stage is the
  source's (`shardNums`); otherwise, in some node and shard of its (catalog, group, target stage), with `NOTE the data check of <catalog> <group> stage <stage> is per (group, stage), not per shard: <why>`; for a complete native segment item with rows (an index-mode measure's rows, which live in the segment's series index and no part counts), in some node and shard or segment-level document of its (catalog, group, target stage), without that NOTE; a segment item without rows has nothing to count. Whether the counts match the rows is not this check's
  business. A target segment whose suffix does not parse as a time of the stage's interval exits
  `6` naming the catalog, group, stage, node and segment (`the target reports segment "<suffix>"
  of … on node <node>, whose suffix is not a time of the stage's <interval> segment interval`)
  instead of being matched loosely. A segment that ends before now − the cumulative TTL
  of the target stage does not count, and an item whose newest row is past it (the TTL gate's
  rule) is not checked. A trace group that samples spans in merges, which may have dropped them, is
  not refused: a `NOTE` names the place. A violation names up to five places (`the progress file
  <file> records data this import wrote that the target does not hold (<n> place(s)):`) and exits
  `6` when every node that may hold the rows answered; when one did not, it exits `3` (`could not
  check the data the progress file <file> records as imported: node(s) <nodes> did not answer the
  count, and no node that answered holds it ...; nothing was written; run again once the node(s)
  are reachable`). Only when both layers pass does a finished import print `already imported into
  this cluster at <time> (liaisons …, data nodes …)`.
- The import holds `<checkpoint>.lock` (the lock of `pkg/transfer/resume`, as the export) for
  the whole run, fresh, resumed or `--restart`, but not under `--dry-run` or `--schema-only`, and
  not when the directory of the path does not exist (the writability probe refuses that). Held by
  another process: exit `1`, the message of [7.1](#71-detection-summary-and-confirmation) (*process
  lock*) followed by `; to import this artifact into another cluster give --checkpoint <another
  path>`. It takes the lock with `resume.LockWith(<checkpoint>, <the import's log>)`, so the
  `WARN taking over a stale lock ...` of a lock file a killed run left behind goes to the import's log,
  and releases it on every way out (deferred: success, failure, interrupt, `--restart`).
- The key of a work item is `WorkItem.Key()` (`importer/workitems.go`); parts and blobs are keyed
  by `<source node>/<part id>` or `<source node>/<kind>` because one item can push several sources.
- The plan records what the first run decided, in the side file `<checkpoint without .json>-plan.json`
  (`{"plan": [{"key", "sources", "targetStage", "targetNodes"}], "baseline": {…}}`, the baseline
  once it is counted, below) whose digest the progress file keeps (`planDigest`); a missing plan
  side file or another digest exits `6`. It is written before the progress file that names it, and
  written again (`rewritePlan`) only to add the baseline once it is counted, to append the work
  items a gate refused then and accepts now (`appendPlan`), or to add to the baseline what it lacks:
  the progress file first records `planNextDigest`, the digest the new side file will have, then
  the side file is rewritten, then `planDigest` takes it and `planNextDigest` is cleared (and, for
  appended items, `finishedAt`), so a crash in between leaves a side file one of the two digests
  names; a load that adopts the side file `planNextDigest` names clears `finishedAt` too when the
  plan it holds has an item not done, since appended items are not pushed yet. Such an item has no progress, so pushing it stores nothing twice; an item
  missing from the recorded plan that has progress exits `6`. The baseline and `pushed` make `--verify` of the last
  run compare against the state before the **first** push. The baseline (the `Counts` the
  baseline Plan returned, with the nodes it counted) is written to the plan side file, next to the
  plan, before anything is pushed, and `baselineAt` records it; a baseline the side file holds
  while the progress file has no `baselineAt` (a crash in between, so before any push) is not used,
  and the next run counts the target again. A plan side file missing or with another digest exits
  `6`, and so does one without the baseline `baselineAt` records. A progress file that names no plan side file (`planDigest` empty) while it records a baseline or pushed rows
  was not written by a run, which writes the side file first: it exits `6` too, with the start-over hint. Items appended to the plan (a gate refused them then and accepts now) whose
  group no recorded item covers are counted before anything of them is pushed, and the side file
  is rewritten with their counts, under `planNextDigest` first like any rewrite of it: a crash
  between the two writes leaves a side file a load accepts. In the same
  way a resume first counts the target nodes of its plan the baseline did not count (a data node
  added since the first run, say) and records them with the baseline (`baseline: target node(s)
  <nodes>, which the baseline did not count, were counted before the push and recorded with it`),
  but only a node the progress file records nothing pushed to: a node it does may already hold rows
  of the import, so it is left uncounted, and the verify says so.
- A complete item keeps no `parts`, `blobs` or `cursor`: when it completes they are replaced by
  `landed` (rows of its parts landed per target node) and `tainted` (the nodes where a part was
  pushed again or left out), all the reconciliation seed needs.
- CSV items carry `cursor` instead of parts: `{"file": "<path>",
  "record": 192000, "rows": 381000, "sent": {"file": "<path>", "record": 196000, "rows": 385000},
  "sendLimit": 391000}`,
  the last record of the last acknowledged batch and the rows acknowledged so far, and in `sent`
  the last record of the last batch sent and the item's rows up to it. `sent` is written **before**
  the batch is sent: the item's first batch together with the start of the item in one synced
  write of the progress file, every later batch unsynced to the cursor side file. The rows in
  (cursor, `sent`] may have landed; the rows after `sent` were never sent. The files of the item
  before `file` (in the item's source and file order) are skipped without being opened, so no
  `"EOF"` marker is needed; a cursor without `file` marks an item started with nothing
  acknowledged yet. A finished item has `complete: true` and no cursor. A batch's `batch_seq`
  restarts per stream and is never persisted.
- `sendLimit` is how many rows of the item the run may have sent, as of the last synced write of the
  progress file. It is a row count because the rows ahead of the sender have no position yet. Every
  fold (and the write of an item's start) sets it to the last row sent or acknowledged plus
  `(checkpointEveryBatches + writeInflightBatches) × writeBatchSize` rows, which no batch sent before the next
  fold can pass, and never lowers it. A batch whose last row is past the limit that is on disk is
  not sent before the sender has raised it: `csvSending` folds, waits until the progress file is
  written (`flush`) and only then returns, so the extra synced write happens at most once per
  that many rows, and none in a steady run (the limit is raised by the folds). The write of the end of a
  run (`close`) clears it: what a run that ended leaves is exact. A cursor without `sendLimit` is read as having none.
- The cursor side file `<checkpoint without .json>-cursor.json` holds `{"cursors": {...}, "pushed": [...], "foldSeq": n}`:
  the cursor of every started CSV item and the cumulative `pushed`. Every fold (an item start,
  every `checkpointEveryBatches` acknowledged batches, an item's completion, the end of the run)
  writes the side file first with the next `foldSeq`, then the progress file with the same
  `foldSeq`. A resume merges the side file into the progress file and deletes it when it starts
  recording, before it resolves the ambiguous set, so no side file from before the resume is left
  to be read together with the resolved state. A resume uses the side file only when its `foldSeq`
  is not lower than the progress file's, so a stale side file left by a crash between the two
  writes is ignored. The side file is replaced as a whole, a temporary file renamed over it
  (`writeCursors`), so a process killed meanwhile leaves the old file or the new one, never a torn
  one. The fold of an item's completion (`itemComplete`) writes the side file
  with the item's cursor, which then covers all its rows, before the progress file records the
  item complete, so a crash in between completes the item on the resume instead of pushing its
  rows again. A side file that cannot be read or parsed exits `6` (`readCursors`): its cursors may
  be newer than the progress file's. One that a power loss or an OS crash lost (it is never synced:
  missing, empty or all NUL) or left older than the progress file is not used: the resume falls back to the
  cursors of the last fold. Whatever was sent or acknowledged after that fold is at most up to `sendLimit`, so for
  every unfinished item whose `sendLimit` is above its acknowledged `rows`, the rows after the
  cursor up to `sendLimit` (at most the rows of the item) are the ambiguous window of the item, as
  those after the cursor up to `sent` are with a side file: the same `AMBIGUOUS` line (`rows after
  <file> record <n> up to its recorded send limit (<k> row(s))`), the same duplication policy, and
  the summary says `the cursor side file is missing, empty or older than the progress file; the rows up to the
  recorded send limit are treated as possibly landed`. `skip` leaves out the window and continues
  after its last row, whose position is found by reading the rows after the cursor as the push
  reads them (`locateLimitWindows`). A run that ended clears `sendLimit`, and so does a resume that read the side file, when it folds the cursors into the progress file before it removes the side file; a side file removed after either makes nothing ambiguous, and the summary says nothing of a lost side file. The cursors of a lost side file keep their `sendLimit`, since their window is only known by it. A side file that is not lost is used as it is, the kill -9 case.
- `pushed` counts rows per (catalog, group, target stage, work item kind): CSV per acknowledged batch,
  native per part once it is stored on every target node (or skipped by the policy). `duplicates`
  records the rows a resume pushed again under `warn`/`push`, and the measure rows of a `re-push`,
  each with the payloads it comes from, so that a later `--verify` (also of a finished import run
  again) keeps the tolerance. Every resume appends the duplicates of the payloads it pushes again:
  once a CSV window is decided its `sent` mark is dropped, so the window a later resume finds holds
  only what the run before it sent again. A payload record's `causes` names, per node of
  `completionSent`, the `failure_cause` of the `ImportPartsResponse` target, in lower case
  (`unreachable`, `refused`, `stream_broken`, `no_answer`; a node whose answer named no cause, or
  one the client does not know, has none; a run writes only `stream_broken` and `no_answer`, as
  the liaison sets `completion_sent` with no other); a resume takes any other name, which a newer
  client may have written, as no cause (`FAILURE_CAUSE_UNSPECIFIED`), so that the counts decide the node, and
  does not refuse the file. A node with
  `no_answer` is one the liaison stopped waiting for (an answer with one is written at once,
  synced, not with the next write): it may still have been committing the part, so a resume does
  not take a count that did not grow there as not landed. A run never does, for any node that
  failed after the completion: its stream broke or the liaison stopped waiting for it, and the node
  may still be committing the part, so the run fails and a resume decides it. A target that
  answered a frame with a `SyncStatus` other than the expected one (`refused`) gave a definite answer:
  it never has `completion_sent`; it is recorded in `missing` and pushed again to that node (`retry_nodes`), not reconciled. A payload record's `skipped` lists the nodes the `skip`
  policy left out, and `repushed` the nodes a stream or trace part was pushed to again under
  `warn`/`push` although it may have landed there; the counts at both are off by an unknown number
  of rows, so a later reconciliation at that key is undecidable. `skippedPayloads` names the
  payloads `skip` left out that no `--verify` has passed yet: every run forces `--verify` while it
  is not empty, and a passing verify empties it.
- A payload whose send fails without any answer stays in `inflight`, even when it failed before its
  completion was sent: the record does not tell the two apart, and treating it as ambiguous is safe.
- `finishedAt` is set by the run that pushes the last work item. It counts as finished only when no
  work item of the plan is left (`finished`): an item a gate refused then and accepts now (with
  `--force-ttl`, say) is not in the recorded plan; the run lists it under `NEW` in the summary,
  appends it to the plan and pushes it without asking `Resume?` (`the import was finished at <t>;
  the <n> work item(s) a gate refused then and accepts now are imported`). An item of the recorded
  plan left (one a gate refused on the run that set `finishedAt` and accepts now) may have progress,
  payloads in flight among it: the run is then a resume like any other, which asks `Resume?` (or
  needs `--resume` when not at a terminal).
- `verifyPending` is set by that run when it is to verify (`--verify`, or payloads `skip` left out)
  and has a baseline, and cleared only by a verify that passes; a verify pending from an earlier
  run stays pending through a later push. A verify that fails (exit `4`) or is interrupted leaves
  it set, and every later run verifies again (`verifyFinished`) until one passes, unless
  `--verify=false`, which does not turn off the verify `skippedPayloads` force. The per-node counts
  cover also the groups of recorded items a gate refuses this time.

| Moment | Record | Persist |
|---|---|---|
| before the schema (also under `--dry-run`, not with `--schema-only`) | a file is created and removed next to the progress file: an unwritable location exits `1`, nothing written | — |
| before the plan is decided (not under `--dry-run` or `--schema-only`) | `<checkpoint>.lock` taken for the whole run; the `--resume` flag without a progress file exits `1`, `BYDBCTL_IMPORT_RESUME` without one starts a fresh import | — |
| `--restart` (not under `--schema-only`) | the progress file is not read; under `--dry-run` the plan of a fresh import is printed with `NOTE --restart: the plan below is that of a fresh import, <file> is not read; the real run keeps it as <file>.<timestamp>.bak` | — |
| schema written and applied (also when nothing needed writing), plan re-checked | with `--restart`, an existing progress file is set aside now (`restartProgress`): the side files first, to `<cursor side file>.<stamp>.bak` and `<plan side file>.<stamp>.bak` (each side file's own name plus `.<stamp>.bak`), the ones a version 1 file left (`<checkpoint>.plan`, `.cursor`, `.baseline` and `<checkpoint without .json>-baseline.json`) with them, and the progress file last, to `<checkpoint>.<stamp>.bak`; all with one stamp `<YYYYMMDDTHHMMSSZ>` (UTC), with `-<n>` appended when a `.bak` of that stamp exists; the progress file stays in place until its side files are kept, and once it is moved a crash leaves no progress file a rerun would resume; a run refused or failing earlier renames nothing. The temporary files of the side files are removed. A fresh import then writes the plan side file and creates the file with `planDigest` and `bindings`, and prints `progress: <file> (kept after the import; the same command resumes an interrupted one)`; a resumed one prints that line before the schema write | yes — before the baseline; a `--schema-only` import creates no progress file |
| verify baseline taken | the plan side file rewritten with it (`planNextDigest`, the side file, then `planDigest` and `baselineAt`), or `noBaseline` | yes — taken once, by the first run, by every import that pushes anything (not only with `--verify` or native stream/trace parts): a resume may force verify (`skip`) or reconcile. The Plan is retried with the transfer back-off (1 s, doubling, for up to a minute) while it fails or misses a target node. When `--verify` or the reconciliation of native stream/trace parts needs it, a baseline that did not count a target node of the plan exits `3` before any push and is not recorded: that node would stay uncounted for every later run. When neither needs it, the failure is `WARN <reason>; neither --verify nor a reconciliation needs it, so the import goes on, and a verify forced later has no baseline to compare against` and `noBaseline: true` is recorded |
| native: before a payload is sent | `inflight[worker]`, together with the answers recorded since the last write | yes |
| native: after its response | clear `inflight[worker]`; per target node into `nodes` or `missing` (+`completionSent`, `causes`); `pushed` | an answer with a `no_answer` cause is written at once, synced; any other has no write of its own: written with the next write (the next payload any worker sends, the item's completion, or the stop); until then the payload is in flight in the file, and a resume reconciles or pushes it again |
| CSV: an item's first batch, before it is sent | the item's cursor with `sent` | yes, with the next `foldSeq` |
| CSV: every later batch, before it is sent | `sent` into the cursor side file | written, not synced; synced first, with `sendLimit` raised, when the batch ends past the `sendLimit` on disk |
| CSV: each acknowledged batch | the cursor side file replaced (a temporary file renamed over it, no fsync): every worker's cursor | written, not synced |
| CSV: every `checkpointEveryBatches` (default 64) acknowledged batches across the import | the side file with the next `foldSeq`, then the cursors with `sendLimit` raised, `pushed` and `foldSeq` queued for the progress file; the worker does not wait for that write under the tracker's lock | yes, by the persister |
| item finished | `complete: true`; a native item's `parts` and `blobs` replaced by `landed` and `tainted` | CSV: yes, the worker waits for it; native: with the next synced write, like the answers of its payloads (a crash before leaves its last payload in flight, which a resume resolves) |
| import finished | `finishedAt`, and `verifyPending` when it is to verify; the cursors left are folded and the cursor side file is deleted; the file is **kept**: it records what this artifact put into this cluster, and is what makes a later rerun refuse to run blind | yes |

A failure (exit `3`) once the progress file exists names it: `the progress file <file> is kept: run
the same command again to resume`. On a worker's failure, Ctrl-C or SIGTERM the CSV workers stop
sending and wait up to 30 s for the acknowledgements of the batches in flight before closing their
streams, so those batches are recorded instead of left ambiguous; a worker with batches in flight
prints `waiting up to 30s for the acknowledgements of <n> batch(es) in flight, so that they are
recorded; press Ctrl-C again to stop now`. A native worker cancels a payload still in its init or
chunk frames at once, but waits up to the same 30 s for the answer of one whose completion was sent
(`waiting up to 30s for the answer of the <KIND> <group>/<stage> shard <n> in flight, so that it is
recorded; press Ctrl-C again to stop now`), so where it landed is recorded rather than left in
`inflight`; when no answer comes within those 30 s, the payload is recorded with `completionSent` and
`no_answer` for every node it was sent to, as the nodes may still commit it. A drain that a
worker's failure started says `press Ctrl-C twice to stop now`, the first Ctrl-C being the stop
already under way. A second Ctrl-C or SIGTERM stops `bydbctl` at once.

A finished import (`finishedAt` set and no work item left, `finished`) that is started again prints
the `FINISHED` summary ([7.1](#71-detection-summary-and-confirmation)) with no plan table (nothing is left),
asks no question, checks the target (both layers above), prints `already imported into this cluster
at <RFC3339 time> (liaisons <names>, data nodes <names>); nothing is pushed (--restart imports it
again)` and exits `0` without pushing, unless it verifies; a dry run prints
the same line and exits `0`. It verifies (`verifyFinished`) when the `--verify` flag is given on the
command line (`import.verify`, `BYDBCTL_IMPORT_VERIFY` and the default do not count: they set a
default for every run, not a request to verify a finished import again), when `verifyPending`
is set and verify is on (`verify: on, since no verify of this import has passed yet`), or when
`skippedPayloads` is not empty: `verify: against the baseline the first run recorded (the baseline
was taken at <t>); rows written into these groups since by anything else count as mismatches`. A
failure exits `4` and leaves `verifyPending` and `skippedPayloads` as they are; a pass clears
both (`--verify=false` turns off the verify `verifyPending` asks for, not the one `skippedPayloads`
forces). Otherwise, with verify on by default, it prints `NOTE verify skipped: the baseline was taken
at <t>, and whatever was written into these groups since would count as a mismatch; give --verify
explicitly to check against it anyway`. With `noBaseline` any such verify exits `4`: the first run
recorded no baseline (on a resumed run: `verify is not possible: the first run of this import
could not count the target before its push (it needed no baseline then), so there is nothing to
compare against`).

`--restart` (the flag only, like `--force`: no variable and no plan field; with the `--resume` flag
exit `1`, they exclude each other, while a resume from `BYDBCTL_IMPORT_RESUME` yields to it; ignored
by `--schema-only`) keeps an existing progress file and its side files as `.bak` files once the new
progress file is about to be created (table above) and runs a fresh import after `WARN --restart:
<path> is kept as <bak> and the import starts over; <what>` (`restartWarning`), `<what>` read
best-effort from the old progress file: `the stream and trace rows an earlier run already imported
are stored twice, in stream sw_stream, trace sw_trace` for the stream and trace groups of the plan it
records rows or payloads of (`parts and element indexes` instead of `rows` for a native artifact;
none: `nothing of this plan is stored twice: measures and properties overwrite what is there`); `it
records nothing pushed yet, so nothing is stored twice`; `it records the import of another
artifact, target or options, so nothing it records is stored twice by this one (it differs in:
…)` when its bindings differ; `it cannot be read (…), so what it records is unknown: …` (then the
plan's stream and trace groups `may be` stored twice). With `--dry-run` it reads no progress file
and previews the plan of a fresh import (table above), whatever import the file records.

A dry run with `-o json|yaml` over a progress file adds `resumed: {path, done, total, finishedAt}`
to the plan (`finishedAt` only when the progress file records one) and lists in `entries` the work items left. The table output
is only the table of what is left, printed after the summary: no `TARGET`, `SOURCE` or `RESULT` line, and no table at all when
nothing is left (a finished import prints the `already imported` line instead).

#### 7.3.2 Resume flow

`RunImport` (`pkg/transfer/importer/import.go`):

1. The steps before the schema write run as usual (preflight, artifact check, schema compare,
   `Targets`, decide, render). The schema written by the first run compares `OK`; the TTL gate is
   re-evaluated against `now`.
2. Bindings check, before the target is read (`openRun`). Then the summary, which a run not at a
   terminal and without `--resume` prints too before it exits `1` (step 4 of
   [7.1](#71-detection-summary-and-confirmation)); a finished import stops after the plan comparison of step 3 (exit `0`,
   [7.3.1](#731-progress-file)) without a plan table, verifying only as described there.
3. **Plan comparison.** The new plan must select the same sources for every item recorded in
   `plan`; an item with any progress must keep its `targetStage` and `targetNodes`; an item the
   recorded plan does not hold is a difference too. Otherwise exit `6`. An item that the TTL gate now
   refuses is listed as refused and its progress is kept. The comparison runs right after the summary,
   before the confirmation and also under `--dry-run` (an item whose route is still unknown skips the
   node check), and again on the re-checked plan after the schema write.
   Then the confirmation ([7.1](#71-detection-summary-and-confirmation)).
4. Schema write is skipped when the compare is all `OK`, as for a fresh import; the re-check runs.
5. **Ambiguous set** ([7.3.4](#734-the-ambiguous-set)): resolve it once, before the push, in this
   order: classify every ambiguous payload ([7.3.3](#733-what-is-skipped-and-what-is-pushed-again)),
   reconcile the stream and trace parts ([7.4](#74-count-reconciliation)), halt when the policy of any
   undecided payload is `block` (exit `7`), record the decisions and the policy outcomes in the
   progress file, then push.
6. Push what is left, skipping finished items, finished parts/blobs and CSV rows up to the cursor
   (up to `sent` for an item whose window `skip` left out).
7. `--verify` uses the recorded baseline and the cumulative `pushed`.

#### 7.3.3 What is skipped and what is pushed again

| Payload | Done | `missing` (no completion sent) | `inflight` or `completionSent` |
|---|---|---|---|
| native stream / trace `PART` | skip | re-push to those nodes (`retry_nodes`) | **reconcile** ([7.4](#74-count-reconciliation)) |
| native measure `PART` | skip | re-push (`retry_nodes`) | re-push (`retry_nodes`): versions converge |
| native `SERIES_INDEX` | skip | re-push | re-push: the receiver keeps the first copy of a document |
| native `PROPERTY` | skip | re-push | re-push: `Repair` is idempotent |
| native `ELEMENT_INDEX` | skip | re-push | `resumeDuplicationPolicy` |
| CSV measure / index-mode / property | rows ≤ cursor skipped | — | rows after the cursor re-pushed |
| CSV stream / trace | rows ≤ cursor skipped | — | rows in (cursor, `sent`]: `resumeDuplicationPolicy`; rows after `sent`: pushed (with the cursor side file lost, the window is (cursor, `sendLimit`]) |

A trace part is still sent with its secondary index parts in one `PART` payload.

`missing` names the nodes whose target failed before its completion was sent: the payload cannot have committed there, so it is pushed again to them. `completionSent` names the nodes that failed after it: the payload may have committed there, which is why those are ambiguous.

#### 7.3.4 The ambiguous set

The server commits before it answers, so a payload whose answer never reached the client may or
may not have landed. At most one payload per worker is in this state. For CSV the
window is the batches sent and not acknowledged, at most `writeInflightBatches` batches per worker;
they start right after the cursor and end at `sent`, so they are exactly the rows in (cursor,
`sent`] of an unfinished item. An item with nothing sent after its cursor is not ambiguous. The
window's name in the `AMBIGUOUS` list and in `WARN` lines is `<item> rows after <file> record <n>
up to <file> record <m> (<k> row(s) sent)`, the second `<file>` left out when it is the first, or
`<item> rows from its first row up to <file> record <m> (<k> row(s) sent)` when nothing was
acknowledged yet.

Both channels have the same shape of window, in which work is committed on the server but not yet recorded by the client: commit first, record second.

| Channel | Window | Worst case |
|---|---|---|
| native | after the payload is sent, before the progress file records its answer (with its next write) | one payload per worker, so at most `parallelism` payloads |
| CSV | after a batch is sent, before its acknowledgement is written to the cursor side file | the batches a worker keeps in flight (`--write-inflight-batches`, 2 by default, of `import.writeBatchSize` rows, 1,000 by default); with the side file lost by a power loss or a kernel panic (it is never synced), the rows after the cursor of the last fold up to its `sendLimit` ([7.3.1](#731-progress-file), [7.6](#76-limits)) |

Reversing the order does not help, and neither does checkpointing more often. Recording before committing turns a failed delivery into a recorded success, which is silent data loss. And no checkpoint frequency closes the window, because it is not an interval the client controls: it is the instant between the server's commit and the client's knowledge of it. Shrinking it changes how many payloads land in it, never whether it exists.

#### 7.3.5 `resumeDuplicationPolicy`

```yaml
import:
  resumeDuplicationPolicy: warn   # block | warn | push | skip
```

| Value | At resume |
|---|---|
| `block` | Push nothing, list the ambiguous payloads, exit `7` |
| `warn` | Push again and print a `WARN` per payload with what it may duplicate (a CSV item is pushed again from its cursor; the `WARN` and the verify tolerance use the window's rows) |
| `push` | Push again silently |
| `skip` | Leave the payload out (the rest of the item is still pushed) and force `--verify` on; a gap fails verify (exit `4`) and lists the skipped payloads. The skipped rows count as pushed, so verify expects them: it passes only when they did land. The skipped payloads are recorded (`skippedPayloads`) and keep forcing `--verify` on every later run until one passes. For a CSV item the ambiguous payload is the window (cursor, `sent`]: `skip` leaves out only the window, counts it as pushed (so `--verify` checks it) and continues the push after `sent`. `skip` becomes `block` wherever `--verify` cannot check what it would leave out (`skipUncheckable`): the parts and CSV rows of a trace group sampling spans in merges (`--verify` checks that group only partially), a native `ELEMENT_INDEX` (`--verify` does not count element index documents), and every payload of an import whose first run recorded no baseline. The payload's line names why (`<source>; skip is refused: <why>`), and the exit `7` asks for `--resume-duplication-policy warn|push (skip stays refused where a line says so)` |

The policy applies to stream and trace payloads only: measure and property payloads are always
pushed again (they converge). The default is `warn`. `--resume-duplication-policy`, like
`import.resumeDuplicationPolicy`, sets one policy for every stream and trace payload. A dry run uses it too: its
summary names the policy of every ambiguous payload. Because a skipped payload forces verification,
also in a later run whose policy is no longer `skip`, `--verify-timeout` is accepted with
`--verify=false` (only `--schema-only` refuses it) and must be positive whenever data is written. A reconciled
part's undecidable nodes take the policy of its item; the decided ones (landed / not landed) never do. The
policy applies only to what reconciliation cannot decide and to the payloads in the last column of
[7.3.3](#733-what-is-skipped-and-what-is-pushed-again) marked with it. `--verify` treats the rows of a
`warn`/`push` re-push as a known tolerance: a stream/trace Δ above the pushed rows by at most those
rows is a warning naming the payloads, not exit `4`. The same holds for the measure points of a
`re-push` (a CSV measure item's rows after its cursor, a native measure part): the new point
overwrites the one that may have landed, but both versions are counted until a merge, so a measure
whose counts exceed the rows sent × copies by at most those rows × copies is a warning, not exit
`4`. The stream/trace rule with a tolerance: every shard must grow on exactly its target nodes, by
at least one row on each, its copies differing by at most the tolerated rows; the shards together
must grow per copy by at least the rows sent, and that surplus plus the copies' differences must
stay within the tolerated rows.

A segment present in the baseline and gone at verify (retention) is left out of both sides with a
warning; a stream or trace (group, stage) that then grew by fewer rows than were sent is
`Incomplete` (`... a segment of it disappeared meanwhile (retention?) and may have held the rest`),
which fails verify (exit `4`) without claiming a mismatch.

#### 7.3.6 Checkpoint level and what a replay costs

"Safe" below means safe to replay automatically: re-delivering a unit that already landed is harmless. The rest need their ambiguous payloads decided ([7.3.4](#734-the-ambiguous-set)).

| Format | Data type | Checkpoint level | Automatic replay | Re-importing already-successful data |
|---|---|---|---|---|
| native | measure | node → segment → shard → part → target node | Safe | The read path keeps the larger version by `(seriesID, timestamp)`, so duplicates are invisible in queries; storage grows and does not merge back |
| native | series index | node → segment → target node | Safe | First write wins; later writes are discarded silently |
| native | property | node → group → shard | Safe | Idempotent |
| native | stream | node → segment → shard → part → target node | Through the progress file: a rerun skips what it records, and a part in flight is reconciled by counts; an undecidable one follows `resumeDuplicationPolicy`. `--restart` or another `--checkpoint` stores the parts again | It stays queryable (the vectorized path deduplicates by element ID in `pkg/query/vectorized/stream`'s `Distinct`) but storage grows permanently |
| native | trace | Same as above | Same as above | There is no deduplication anywhere and **duplicate spans become visible to users** |
| native | element index | node → segment → shard | **Needs `resumeDuplicationPolicy`** | **No deduplication**; documents and disk grow linearly and are never reclaimed |
| CSV | measure | node → segment → shard → split file → record index | Safe | The read path keeps the larger version by `(seriesID, timestamp)`, so duplicates are invisible in queries |
| CSV | index-mode measure | Same as above | Safe | Idempotent |
| CSV | property | Same as above | Safe | Idempotent |
| CSV | stream / trace | Same as above | **Needs `resumeDuplicationPolicy`** | stream behaves as native; trace has no deduplication |

The target cannot deduplicate a re-imported part *by content*: the same payload imported twice produces two directories with different part IDs, and `partMetadata` carries no hash, checksum or fingerprint field. What keeps a payload from being stored twice is therefore the progress file and, for a stream or trace part whose answer was lost, the count reconciliation of [7.4](#74-count-reconciliation). What a duplicate costs per catalog is in [6.4.1](#641-what-a-duplicate-costs-per-catalog).

#### 7.3.7 What resume guarantees

Every path is at-least-once, with a bounded, enumerable ambiguous set. A stream or trace part whose answer was lost is decided from the target's counts ([7.4](#74-count-reconciliation)); what reconciliation cannot decide, an element index and a CSV stream or trace window, follows `resumeDuplicationPolicy`; measure, series index and property payloads are pushed again and converge. Two things resume does not reduce, and they dominate in practice:

- **Multi-source duplication.** `--all-sources`, a coverage misjudgement, or overlap the source cluster already had produce *genuinely different parts*, which the receiver rightly accepts. Stream's ×2.38 storage and the element index's ×3.000 were measured on exactly that case.
- **Visible trace duplicates.** Any duplicate that reaches the target is shown to the user, because neither the trace merger nor the query path deduplicates by span ID outside the cross-node merge.

### 7.4 Count reconciliation

**Question.** Did stream/trace part `P` (rows `n`, source segment `s`, shard `h`) land on target
node `N`?

**Answer from counts.** The session-less `ExportService.Plan` (the `Snapshot` of
`importer/verify.go`) reports per `(catalog, group, stage, node, shard)` and segment the sum of
the `total_count` of the live parts. For the target key `k = (group, targetStage, N, h, s)`:

```text
expected(k) = baseline(k) + Σ rows of the parts recorded as landed on N for k
actual(k)   = Snapshot(k)
Δ           = actual(k) − expected(k)

Δ == 0       → P did not land on N: push it again to N only (retry_nodes)
Δ == n       → P landed on N: record N in done, do not push
otherwise    → undecidable: resumeDuplicationPolicy
node not counted by the baseline (unreachable, or not known to the liaison) → undecidable, no read
node unreachable or not asked in this read → undecidable
key tainted (an earlier part there was pushed again or left out by the policy) → undecidable, no read
key missing from the snapshot although the node answered:
  expected == 0 and the node's shard counts what is expected over all segments
               → actual = 0 (the inventory skips a shard or segment without parts) → not landed
  expected  > 0 → undecidable (retention or a foreign delete)
  the shard grew elsewhere → undecidable (the part may have landed in another segment covering its time)
several ambiguous parts at one key → decided only when none or all of them landed
Δ == 0 on a node the liaison stopped waiting for (causes: no_answer) → undecidable: it may commit the part later
Δ == 0 within a run (any node that failed after the completion) → undecidable: a resume decides it
```

The snapshot is read up to five times, one flush interval (5 s) apart. Each key is decided as soon as
two consecutive reads count it the same (for a missing key, its node's whole shard too), the way
`--verify` waits for a stable sum; a merge in flight is never sampled. A key still moving after the
last read, or every key left when the reads fail or the run is canceled, is undecidable. The rows
recorded as landed seed `expected`, for every item the progress file records, also one a gate
refuses this time. Each decision is logged as `reconcile <item> part <part id> of <source node> on
<node>: landed|not landed|undecidable (<expected>→<actual>, part <n> rows)`, an undecidable one
followed by `: <reason>`, within a run and at resume alike. Within a run, a reason starts with
`other parts of shard <n> were being pushed meanwhile (segment <s>), which moves its counts; ` when
work items of other segments of the same shard pushed parts while the counts were read (the
reconciler counts the items pushing per shard and segment).

**Why it is exact.**

- A native import keeps the source's segment grid and shard number (the `TOPOLOGY` and `TZ`
  gates), so the source `(segment, shard)` of a part is its target `(segment, shard)`.
- An imported part is a file part: `mustAddFilePart` introduces it before the receiver answers, so
  `Plan` counts it at once, with no flush.
- A stream or trace merge keeps every row, so `total_count` is invariant under merges — the same
  property the stream/trace rule of `--verify` (`evaluateRows`) already relies on.
- At most one part per target key is ambiguous at a time: the parts of an item are pushed one after
  another, and the push serialises items that map to the same target `(stage, segment, shard)` (two
  source stages falling back to `hot` can do that). If a resume still finds several ambiguous parts on
  one key (from different crashed workers), it decides them only when none or all of them landed;
  any other split is undecidable.

**Trace groups with a merge-time sampler.** A trace group whose native sampling pipeline drops
spans during merges (a MERGE-event filter, or a FINALIZE round) breaks the invariance above, so its
parts are never reconciled: an ambiguous part of such a group is undecidable (within a run: fail as
before; at resume: `resumeDuplicationPolicy`). `--verify`'s stream/trace rule has the same premise
and checks such a group only partially: no node outside a shard's target nodes grows, the target
nodes together grow when rows were sent, and the growth is at most (rows sent + rows pushed again)
× copies. The report says `  - trace <group>/<stage> checked partially: it samples spans in
merges, so only the routes, the growth on the target nodes and at most the rows sent × copies are
checked`, and a verify that passes with any such group prints `VERIFY    ok (N group(s) not fully
checked)` (plus ` in <elapsed>`).

**Premise: the imported groups of the target are quiet.** From the baseline until the import ends, resumes included, nothing else may write to or delete from the groups being imported on the target: no other import, no application writes, no data removed by hand. `--verify` states the same premise. The reconciler counts rows per key and cannot tell the part's rows from anyone else's, so its decision is right only while the part is the one thing that changed the count of its key. When the premise is broken:

- Most cases are undecidable (`reconcile.go`, `evaluateKey`): `Δ` is neither `0` nor `n`; the key is missing from the snapshot although the baseline or the progress file records rows there (a segment removed by retention or deleted by hand); the node's shard grew elsewhere; or the counts never read the same twice. At a resume the payload follows `resumeDuplicationPolicy`; within a run the import fails. `block` lists these payloads and exits `7` instead of pushing them again.
- Two cases are not recognised, because the counts then look like a clean outcome, and `block` does not stop them, since a decided part never reaches the policy:
  - The net change others made at the key equals the part's rows (`Δ == n`): a part that did not land is taken as landed and not pushed, so its rows are missing. A net change of zero (`Δ == 0`), such as others' writes and deletes cancelling, or others removing exactly as many rows as the part added, takes a part that landed as not landed: it is pushed again and stored twice.
  - Rows lost between the baseline and a later read in a segment that is still there (retention, lifecycle migration). A key that vanished with rows expected is undecidable, but fewer rows in a surviving segment is judged by `Δ` like any other change: normally negative, so undecidable, yet when the loss equals the rows of a landed part, `Δ` is `0` and the part is pushed again.

When the data lies close to the retention boundary of the target stage, avoid leaving the import interrupted for long, and resume with `--resume-duplication-policy block`, so that the parts the counts cannot decide stop the import for a human decision instead of being pushed again under `warn`.

**Also within one run.** A failed target with `completion_sent` on a duplicable payload fails the import at once (`native_push.go` `pushPayload`); for a stream or trace `PART` the push reconciles first: landed → done; anything else → fail at once. A count that did not grow
is not taken as not landed within the run: a target that fails after the completion lost its
stream to the node, or the liaison stopped waiting for it (the liaison sets `failure_cause` to
`FAILURE_CAUSE_STREAM_BROKEN` or `FAILURE_CAUSE_NO_ANSWER` on that target of the
`ImportPartsResponse`), and the node may still be committing
the part. It is undecidable, the import fails, and a resume decides it: the progress file records
the cause in `causes` (`no_answer`), and the resume's reconciliation does not take a node the liaison
stopped waiting for as not landed either, while a node whose stream broke, or whose answer named no
cause, is decided by the counts at resume. A target that answered with an error status
(`FAILURE_CAUSE_REFUSED`: it answered a frame with a `SyncStatus` other than the expected one;
a stream it ended with a gRPC error status is `FAILURE_CAUSE_STREAM_BROKEN` and is reconciled) gave a
definite outcome and never has `completion_sent`, so it is not reconciled: the payload is missing there
and pushed again to it (`retry_nodes`). A client takes a
cause it does not know as `FAILURE_CAUSE_UNSPECIFIED`. This needs a baseline even when `--verify` is off,
so a native import that pushes stream or trace parts always takes the `Snapshot` before the push,
and a baseline it cannot take is exit `3` with no data sent.
An element index fails the import at once.

Code: `pkg/transfer/importer/reconcile.go`, called from `pushPayload` and once from `RunImport`
before a resumed push.

### 7.5 Options and CLI

| Option | Flag | Plan field | Default |
|---|---|---|---|
| resume without asking | `--resume` (`BYDBCTL_RESUME` for export, `BYDBCTL_IMPORT_RESUME` for import) | — (switch) | off |
| import from scratch, keeping the progress file as `.bak` | `--restart` (no variable) | — (switch) | off |
| export progress file | `--checkpoint` (`BYDBCTL_CHECKPOINT`) | `export.checkpoint` | `<output>/.export-progress.json` |
| import progress file | `--checkpoint` (`BYDBCTL_IMPORT_CHECKPOINT`) | `import.checkpoint` | `<input>/.import-progress.json` (another cluster needs its own) |
| CSV fsync interval | `--checkpoint-every-batches` (`BYDBCTL_IMPORT_CHECKPOINT_EVERY_BATCHES`) | `import.checkpointEveryBatches` | `64` (1–10000) |
| duplication policy | `--resume-duplication-policy` (`BYDBCTL_IMPORT_RESUME_DUPLICATION_POLICY`) | `import.resumeDuplicationPolicy` | `warn` |

`import.workDir` stays rejected; it belongs to `--archive`. A checkpoint outside the output/input
directory must be given again on resume — the binding check refuses a progress file whose
`outputDir` / `manifestDigest` is another artifact's. An export checkpoint in a subdirectory of the
output directory is refused (exit `1`). An import or export `--checkpoint` that is a directory, or the input/output directory itself, is refused
(exit `1`) before anything is contacted. `--schema-only` ignores `--resume` and `--restart` on import
(with a `NOTE`); `--dry-run` ignores `--resume` and previews a fresh import with `--restart`, so under
either of them `--resume` and `--restart` may be given together (elsewhere the flags exclude each other,
exit `1`).

### 7.6 Limits

- `--restart`, or deleting a progress file, loses the record: a rerun of an import then stores
  stream and trace parts, element indexes and CSV stream/trace rows twice.
- The imported groups of the target must have no other write or delete from the baseline until the import ends, resumes included ([7.4](#74-count-reconciliation)). Reconciliation then degrades to the policy in most cases, but a net change by others equal to the part's rows is taken for a landed part, a net change of zero for a part that did not land, and rows lost in a segment that is still there (retention, lifecycle) can cancel the rows of a landed part; `block` does not stop these. Near the retention boundary of the target stage, avoid long interruptions and resume with `--resume-duplication-policy block`.
- Element indexes and CSV stream/trace rows in (cursor, `sent`] are at-least-once, decided by the
  policy.
- The CSV cursor side file is never synced: after a power loss or an OS crash that loses or empties it
  the rows after the cursors of the last fold, up to their `sendLimit` (the batches of at most
  `checkpointEveryBatches` acknowledgements across the import, plus the batches in flight), are ambiguous
  and follow the duplication policy. A side file of the same `foldSeq` that the crash rolled back to an
  older content cannot be told from a current one: its window is the one it records.
- A progress file resumes only with a bydbctl that reads its format version (`progressVersion`):
  the side files follow the version of the progress file, so an upgrade or a downgrade of bydbctl
  between the runs of one export or import is refused (exit `6`, nothing deleted) with the file, the
  two versions and the ways on. The run is finished with the bydbctl that began it; or it is started
  over, an import with `--restart` (the earlier files are kept as `.bak` files, and stream and trace
  data already imported is stored twice), an export by releasing its session and deleting what is in
  the output directory, or exporting into another one.
- An export session lost between runs cannot be resumed (exit `6`).
- All nodes of the target run one version; do not import during a rolling upgrade (as for the
  native import).

### 7.7 Tests

- **Export** (`pkg/transfer/exporter`, fake liaison): cancel after N units and resume — manifest
  and every file byte-equal to an uninterrupted run except `exportedAt`; binding mismatch → `6`
  with nothing deleted; session gone / unreachable node / changed inventory → `6`; leftover `.tmp`
  and extra volumes removed; no `--resume` without a terminal → `1`; confirm yes/no.
- **Reconciliation** (`pkg/transfer/importer`): Δ = 0, Δ = n, other, missing segment; within-run
  reconcile of a `completion_sent` stream part; serialisation of items sharing a target key.
  Engine tests in `banyand/stream` and `banyand/trace` pin the two premises: an imported part is
  counted by `Plan` right after `mustAddFilePart`, and `total_count` survives a merge.
- **Import** (fake liaison): interrupt CSV and native pushes and resume — stored counts equal one
  uninterrupted run; `ackGate` (applied but not acknowledged) under each policy; plan or binding
  mismatch → `6` with the per-field differences, a target that only gained nodes accepted;
  the target identity (liaisons or data nodes disjoint → `6`, overlapping, superset or subset
  accepted, a standalone of another name → `6`, a progress file without liaisons → `6`) and the
  data check (a group done items wrote into deleted → `6`, its rows gone → `6`, also before a
  resumed push and under `--dry-run` with the progress file unchanged; measures only need some rows; segments past the TTL ignored; native items checked per node, shard and segment; a complete item without rows still needs the schema of its group, and a complete index-mode segment item its rows);
  `--resume` without a progress file → `1`; a finished import run again → `0`, nothing pushed,
  verify only with an explicit `--verify`; `--restart` keeps the files as `.bak` and imports again;
  the lock held → `1`; the CSV `sent` window (only it is ambiguous, `skip` leaves out only it); a
  stale (lower `foldSeq`), empty, all-NUL or missing cursor side file ignored, its window taken from `sendLimit`; a missing or altered plan side file (which holds the baseline) → `6`;
  a failed baseline the import does not need → `WARN` and `noBaseline`; the partial verify of a
  sampling trace group.
- **Integration**: `test/integration/standalone/data_import` interrupt-and-resume round trips for
  both formats, compared with `pkg/transfer/rowdiff`. Rowdiff compares distinct rows, so it proves
  nothing is lost, not that nothing is stored twice; the specs check that separately: the rows the
  progress file counts as pushed for stream and trace exceed the artifact's by no more than the rows
  recorded as possible duplicates, and a resume pushes none of the work items the interrupted run
  finished. One spec keeps the default progress file path and checks its name, `.import-progress.json`. The distributed suite
  interrupts an import through one liaison of a two-liaison cluster and resumes it through the other: the progress file records
  both liaisons and the resume passes the target check. `bydbctl/internal/cmd` drives `bydbctl data export` and `data import`
  against standalone servers, for both formats: a finished import run again is `already imported`; the same artifact into a second
  server is refused (also as a dry run, the progress file unchanged) and imported with its own `--checkpoint`; the imported group
  deleted is refused as reset and `--restart` imports it again; the group's data deleted with its schema kept is refused by the
  count check.
- **E2E**: `test/e2e-v2/cases/transfer/resume-{csv,native}` run the whole flow on the shared
  compose environment — export interrupted by SIGINT and then `kill -9`, the summary without
  `--resume`, a binding mismatch, the resumed export, a lost session, an import interrupted the
  same two ways and resumed with `--verify`, and a CSV re-export of the target compared row by row
  with the source (nothing lost; the comparison is of distinct rows). Duplicates are checked from the
  progress files instead: the resumed export rewrites no file of a unit done before it and reports
  as resumed exactly the units done before; the resumed import reports as done exactly the work items
  complete before it, and its stream and trace rows pushed exceed the artifact's by no more than the
  rows recorded as possible duplicates. The native case also checks that a reconciliation decision
  was printed. After the round trip, `import-reset`
  deletes the imported groups on the destination with `bydbctl group delete` and runs the finished
  import again (exit `6`, "looks reset", refused by the data check, also as a dry run, the progress file
  unchanged); `import-restart` then imports afresh into the reset destination, keeping the progress
  file and its plan side file (which holds the baseline) as `.bak` files.
