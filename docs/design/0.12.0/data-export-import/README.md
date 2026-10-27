# Data Export and Import

Status: the export dry run, the session RPCs (`Plan` create/read, `Sessions`) and the CSV export (`Export` RPC, `bydbctl data export`) are implemented (2026-10-10), and so is the CSV import (`ImportService`, `bydbctl data import`, 2026-10-19) — see [5.4 CSV import as implemented](#54-csv-import-as-implemented); native export, native import, export and import resume are still **design** — see [4.9 Remaining work](#49-remaining-work).

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
  maxFileSize: 1GiB                   # Shared by both formats: soft limit per output file, measured on bytes
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
  checkpoint: ""                      # Empty = <output>/.export-progress.json
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
  writeRateLimit: 0              # Row rate limit (rows/s) shared by CSV row replay and property import;
                                 #   0 = unlimited
  nodeRateLimit: 0               # CSV only: byte rate each target data node admits from the imports,
                                 #   however many liaisons write to it, e.g. "50MiB/s"; 0 = unlimited
  nodeRowRateLimit: 0            # CSV only: row rate each target data node admits from the imports
                                 #   (rows/s); 0 = unlimited
  partRateLimit: 0               # Native only: total outbound byte rate limit, e.g. "50MiB/s"; 0 = unlimited
  partChunkSize: 1MiB            # Native only: bytes per ImportParts frame; clamped to [64KiB, 4MiB]
  writeBatchSize: 1000           # CSV only: rows per ImportRows batch; 1-10000, out of range is a
                                 #   usage error (exit 1); see §5.4.7 for writeBatchBytes and the rest
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

> **In the tree today** (CSV export step, 2026-10-10): the plan file is strict and accepts `connection.nodes`, `connection.nodesTLS`, `connection.rateLimit` and, under `export`, `output`, `format`, `compress`, `maxFileSize`, `includeSchema`, `schemaOnly`, `csvAnnotations`, `parallelism`, `nodeRateLimit`, `nodeRowRateLimit`, `nodeIdleTimeout`, `sendRetryTimeout` and `selectors`. Every option is declared once in `pkg/transfer/config` with struct tags naming its flag, plan path, `BYDBCTL_*` variable, default and grammar; one-shot switches (`--dry-run`, `--preempt`, `--strict-coverage`, `--output-format`) have a flag and a variable but no plan field. Under `import` (CSV import, 2026-10-19) it accepts `input`, `includeSchema`, `schemaOnly`, `schemaConflictPolicy.{default, rules}` (`rules` is plan-only), `parallelism`, `writeRateLimit`, `nodeRateLimit`, `nodeRowRateLimit`, `writeBatchSize`, `writeBatchBytes`, `writeInflightBatches`, `sendRetryTimeout`, `verify` and `verifyTimeout`; `export.checkpoint`, `import.checkpoint`, `import.workDir`, `import.partRateLimit`, `import.partChunkSize`, `import.checkpointEveryBatches` and `import.resumeDuplicationPolicy` belong to resume, archives and the native channel and are rejected until those land. `format: native` parses but is refused with exit `1` (not implemented). Explicit flags override the file, which overrides the `BYDBCTL_*` environment.

**An import reproduces what the export captured**, so a policy exists only where that leaves a real choice — and it does so in exactly one place: a resumed run, and there only for payloads carrying no idempotency key. A native `kind=PART` replay is recognised by `import_op_id` and writes nothing; only the element index and the CSV stream / trace payloads can duplicate on re-push or lose on skip. Everything else is pushed as-is. Overlapping sources on a first run are reported as a WARN; `--strict-coverage` turns that into an error.

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

NODE    CATALOG   GROUP             STAGE  SEGMENTS  SHARDS  PARTS  EST-ROWS  EST-SIZE(comp/raw)     TIME-RANGE
data-1  measure   sw_metricsMinute  hot    7         2       34     118.2M    18.3 GiB / 61.2 GiB    06-30 ~ 07-07
data-1  stream    sw_record         hot    7         2       61     342.7M    96.1 GiB / 288.0 GiB   06-30 ~ 07-07
data-2  measure   sw_metricsMinute  warm   28        4       52     1.4G      201.5 GiB / 702.0 GiB  05-30 ~ 06-30
data-2  property  sw_prop           warm   -         1       -      12.3K     4.1 MiB / 4.1 MiB      -
data-4  stream    sw_record         hot    7         2       58     339.1M    95.2 GiB / 285.3 GiB   06-30 ~ 07-07

SNAPSHOT  each data node pins its own snapshot; the largest node (data-2) needs about 201.5 GiB; check df there first
```

The `NODES` line has two other forms: `NODES     N data node(s) [...], all answered`, and `NODES     standalone process [self]` for a standalone server. `MULTI-SRC` appears only when some unit is held by more than one node (replicated shards): it lists each key — `catalog/group/stage/segment/shard-N` — with every source's rows and time range. Property groups are not listed, nor are index-mode measure segment-level units: they carry no shard id, so the dry run cannot tell replicas of a segment from different shards of it. The `SNAPSHOT` line budgets the largest per-node compressed subtotal, because each node pins its own snapshot on its own disk.

The table lists only data that exists; it never derives "which shards ought to be here" from the schema's `shardNum`, because shards and segments are created lazily and comparing against an expected set produces large-scale false reports. The `NODES` line is therefore the only completeness check — it lists the nodes the liaison reported unreachable or not holding the session (`unreachable_nodes`), which cannot produce a false positive. It is a WARN by default and an error under `--strict-coverage`. A dry run inventories the live directories, and a session snapshot contains what was flushed to disk when it was taken: rows still in memory (up to one flush interval) are in neither.

### 3.2 Import dry run

> **CSV import as implemented (2026-10-19).** The CSV dry run runs steps 0–5 of [the implemented workflow](#541-workflow) — preflight, artifact check, read-only schema comparison, `ImportService.Targets`, the pure `decide()` and the rendered verdict table — and exits `0` (would proceed) or `2` (would be refused) without writing anything. Its table is byte-identical to the one the real import prints before it writes. Below the main table the CSV table prints SCHEMA, one line per group with its resource count and how many resources get each disposition; PROBLEMS, only when there are any, one sentence each: the groups of the missing resources together, every conflict with its differences and the `match:` rule that would accept it, the work items a route, TTL, coverage or row check refuses, and the warnings; and RESULT, the conclusion in one line. The main table's `STATUS` column is `ok`, `refused` or `expired`. Step 4 also runs the read-only row check (gate `ROWS`) over every row of the artifact, so the CSV dry run reads all the data, not only the manifest and schema. Of the gates below only **[1] schema**, **[4] TTL** and `ROWS` apply to CSV: the version and timezone gates are no-ops, and there is no topology gate because rows are re-routed by the target's own stage, shard number and replicas. The CSV TTL gate is coarser than item 6 below: it compares the unit's newest row with `now − cumulative TTL of the mapped target stage` (see [TTL gate](#545-gates)). The CSV table **has** a `TARGET-NODES` column, read from `Targets`, which the liaison computes with the same route the import uses. A group the import creates or overwrites has no route before its schema is written, so its `TARGET-NODES` is `-`; the re-check after the schema write routes it. The paragraph below about `LocateAll` applies only to the native design. The rest of this section is the native-aware design.

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

- `SCHEMA` — `CREATE` / `OK` / `MISSING` / `CONFLICT` / `IGNORE` / `OVERWRITE` / `INVALID`.
- `CHANNEL` — `① part-push` / `① sidx only` (index-mode measure) / `② row replay` / `property/repair` / `REJECT(<gate>)`. **`② row replay` can only appear for a CSV artifact**: a native artifact has no fallback across a topology change, so it renders `REJECT(topology)`.
- `GATE` — `ok` / `TTL!` / `BLOCK`. Any row whose `CHANNEL` is `REJECT(...)` is `BLOCK`, as is any row whose `SCHEMA` is `CONFLICT` or `INVALID`, whatever its channel; `TTL!` marks a row that clears every other gate and is stopped by the TTL gate alone, which `--force-ttl` overrides.

`TOTAL` counts **every** row, blocked ones included, and `SEGMENTS` / `ROWS` / `SIZE` all use that same basis; the blocked subtotal is reported on its own line.

`measure/<group>/_top_n_result` is never written and not compared as an ordinary resource: the target creates it automatically from its own hardcoded schema, so comparing it would only produce conflicts the user can neither fix nor should fix. Its **rows** are still imported (see [Top-N results](#546-top-n-results)), so its columns get a separate compatibility check: matching columns make it `OK`, counted with the identical resources and not listed, and an artifact `_top_n_result` whose tag or field names or types differ from the target's auto-created one is a `CONFLICT`.

## 4. Export

### 4.1 Client

`bydbctl data export` without `--dry-run` runs these steps (`pkg/transfer/exporter.RunExport`):

1. **Check the output directory** — it must not exist or must be empty; anything else is exit `1`. There is no resume yet, so an export never mixes with what an earlier run left behind.
2. **Connection check** — probe every address in `--nodes` (`GetCurrentNode`, `GetClusterState`). An unreachable candidate, or one that has not discovered any data node yet, is skipped with a `WARN`; refused credentials or an endpoint that is not a liaison fail at once (exit `2`). The first usable liaison is the primary: the schema, `Plan` and `Sessions` calls go through it, and the dry run and `release-session` use only it.
3. **Fetch and write the schema** — list the groups, validate the selectors, then list every resource of the selected groups through the liaison's registry services and write `schema/<kind>.jsonl` ([Schema files](#441-schema-files)). The mod revision of every stream, measure, trace and property is kept in memory for the check in step 6. With `--include-schema=false` this step lists nothing beyond the groups and writes no `schema/` files, and step 6 skips the revision check. With `--schema-only` the client writes `manifest.json` (`schemaOnly: true`, no units) here and stops: no session, no snapshot, no data.
4. **Create the session** — one `Plan{selectors, create{preempt}}`. The session id from the first (`created`) frame is printed at once, before the inventory arrives. A node holding another session refuses with `ALREADY_EXISTS`; the client lists the sessions once, prints every occupant with its start time, last heartbeat, catalogs and nodes, and exits `2` ([Taking over an existing session](#482-taking-over-an-existing-session)). Transient failures (`INTERNAL`, `UNAVAILABLE`, `ABORTED`) are attempted up to three times with jittered back-off. A create whose summary still names unreachable nodes is released again and fails, because their data would be missing.
5. **Heartbeat** — `Sessions(ACTION_HEARTBEAT)` every 5 minutes in the background. A failed heartbeat is a `WARN` line and is retried on the next tick; `FAILED_PRECONDITION` (the session expired or its lease is unreadable) cancels every worker and exits `3`.
6. **Export** — fold the Plan frames into one job per `node_id` and run up to `--parallelism` workers, one `Export` stream per node ([Full workflow](#47-full-workflow)). The nodes, sorted by id, are dealt round-robin over the usable liaisons, so node `i` streams through liaison `i mod L`; a stream that cannot be opened (`UNAVAILABLE`, `RESOURCE_EXHAUSTED`, or `NOT_FOUND` from a liaison that does not see the node) moves on to the next liaison within `--send-retry-timeout`. Each file lands as `<path>.tmp`, is checked against its `file_finished` (size, CRC32, and the schema mod revisions the node encoded with), fsynced, renamed, and its directory fsynced. The first failing worker cancels the others.
7. **Write the manifest** — once every node finished, `manifest.json` is built from what was received and written atomically ([manifest.json](#45-manifestjson)); every warning is also printed as a `WARN` line.
8. **Release** — `Sessions(ACTION_RELEASE)`. A failed release is a warning only: the artifact is complete and the lease reclaims the snapshot. The client then prints what was actually written — one `NODE CATALOG GROUP STAGE UNITS FILES ROWS SIZE TIME` row per node, catalog, group and stage (a property row shows its unit's stage, that of the unit's first source; `TIME` adds up the time its units took) — and one line with the units, files, bytes and warnings written and the time of the whole export. The snapshot is released at this point, so the dry run's `SNAPSHOT` budget line is not repeated.

Once the session exists, a failure keeps it and prints `bydbctl data release-session --id <id>`; the run exits `3`. Files already renamed stay in the output directory, a file in flight stays as `.tmp`, and no `manifest.json` is written, so a partial directory never passes for an artifact. Ctrl-C or `SIGTERM` is handled the same way: the export stops, the session is kept and the same `bydbctl data release-session --id <id>` line is printed. After a successful export, a failed release (as a whole or on some node) is a `WARN` with exit `0`, followed by the same `bydbctl data release-session --id <id>` line.

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

> **Not implemented yet.** `FORMAT_NATIVE` is reserved in the protocol, together with the native-only fields (`UnitStarted.total_files`/`estimated_total_bytes`, `FileFinished.entry_count`; CSV sends 0). `bydbctl data export --format native` exits `1` before connecting, and a data node answers an `Export` stream whose `init.format` is native with `UNIMPLEMENTED`. The data node dispatches each unit to a per-format producer, so native slots in beside CSV. The rest of this section is the design.

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
| `seg-XXX/sidx/` | **Series index** — a legacy-engine inverted directory, no parts | segment-level `sidx-NNN.bnc` |
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
| Node rows | `--node-row-rate-limit` / `export.nodeRowRateLimit` | `RateLimit.rows_per_second` | Each row the producer encodes. CSV only: native moves opaque bytes and has no row concept |
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
| Normal group | `seg-<suffix>/metadata` (raw file)<br>`seg-<suffix>/sidx-001.bnc`<br>`seg-<suffix>/shard-<N>-001.bnc` | `seg-<suffix>/shard-<N>-00001.csv.gz` |
| Index-mode measure | `seg-<suffix>/metadata`<br>`seg-<suffix>/sidx-001.bnc` | `seg-<suffix>/segment-00001.csv.gz` |
| Property | `shard-<N>-001.bnc` (no `seg-` layer) | `shard-<N>-00001.csv.gz` (no `seg-` layer) |
| File splitting | `-001`, `-002`, …; each volume has a complete footer | `-00001`, `-00002`, …; each volume repeats the header |

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

- `stages` lists the group-level options first, named `hot` (the default tier), then every declared lifecycle stage. A stage records its own `shardNum`, `replicas` (`0` means no replicas) and `segmentInterval`; its `ttl` is the stage's own, not the cumulative effective TTL. `indexMode` is true when the group holds an index-mode measure.
- A segment unit is keyed by `(catalog, group, stage, segment)` and holds one source per node that exported it. A property unit's key degrades to `(catalog, group)`: every node's shards of the group are sources of one unit, whose `stage` is the stage of its first source (in node-id order). Property units have no `segment` and no time range; their shards carry `docCount` (the Plan's document count) and an empty `parts`. An index-mode measure unit has its rows in `segmentFiles` only, no shards, and a zero time range: the series index records no time span.
- `compress` is `gzip` or `none`; an unspecified compression is the CSV default `gzip`.
- A shard's `totalCount` and `rowCounts` count the rows actually written to its files, per resource, as the data node reported each file in `file_finished.row_counts`; the Plan's statistics only feed `parts[]` and the `estimated*` sizes. Files carry no row counts of their own: `--verify` and the multiple-source judgement only use the totals, and a file's integrity is its `bytes` and `crc32`.
- An index-mode measure's rows are segment-level: its files are listed in the source's `segmentFiles` rather than under a shard, and the source carries their `rowCounts` and `totalCount`.
- `schemaOnly: true` marks a `--schema-only` artifact, which has no units.
- `warnings` collects every unit warning, prefixed with the node and unit, plus the stage warnings of the dry run.

A native export will also record the raw segment files (`metadata`, `sidx-NNN.bnc`) in `segmentFiles`, `entryCount` on each container, and `sidxCoversShards`; these fields are not written by the code yet.

`shards[].parts[]` is written **unconditionally**, for every shard of every source. The export side does not decide which shards need it: that array is the input to the import-side multi-source judgement ([Detection basis](#62-detection-basis)), and a shard that looks single-sourced today becomes multi-sourced as soon as a second artifact is merged in.

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
  // unit, sent only after unit_finished of the previous one. The liaison forwards the
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
    // node without ExportService. An expired or unreadable lease, or any other node error,
    // fails the whole call instead (FAILED_PRECONDITION for the lease).
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
  // NotFound (no longer holds the session), or a data node without ExportService.
  // CreateSession fails on any of these instead.
  repeated string unreachable_nodes = 2;
  // Nodes whose Plan stream completed; with unreachable_nodes it covers every planned node.
  repeated string answered_nodes = 3;
}

enum Format {
  FORMAT_UNSPECIFIED = 0;
  FORMAT_NATIVE = 1;  // reserved: data nodes answer UNIMPLEMENTED
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
    uint64 max_file_size = 5; // soft limit in on-disk bytes; 0 = no split
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
  uint32 total_files = 2;            // native only; CSV sends 0
  uint64 estimated_total_bytes = 3;  // native only; CSV sends 0
  uint32 total_parts = 4;            // live parts of the unit's shards; 0 without parts
}

message FileStarted {
  string relative_path = 1;  // relative to nodes/<node-dir>/
  uint32 shard_id = 2;
  bool segment_level = 3;    // index-mode measure rows
  uint32 shard_index = 4;    // index of shard_id in the unit's shard list
  uint32 seq = 5;            // split sequence, from 1
}

message DataChunk {
  bytes content = 1;  // at most 1 MiB
  uint64 rows = 2;    // CSV only: rows written into the open file so far (progress only); native sends 0
}

message FileFinished {
  uint64 bytes = 1;                               // on-disk size
  uint32 crc32 = 2;                               // IEEE, over the on-disk bytes
  uint32 entry_count = 3;                         // native only; CSV sends 0
  map<string, uint64> row_counts = 4;             // rows per resource in this file
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
| 5 | ctl | Print the `session_id` (from the `created` frame) as soon as it arrives; keep the unit inventory in memory (there is no progress file until resume lands) | — |
| 6 | ctl | Group entries by `node_id` into one job per node, each with its own `nodes/<node-dir>/` subtree | — |

Only this phase creates snapshots; they are created once and reused for the whole export. The liaison stamps `node_id` on every frame from the upstream connection and does not trust a node's self-report.

#### 4.7.2 Phase 2 — Export: transfer unit by unit

Each data node uses one stream. The example shows `data-1`; the other nodes run in parallel with the same structure, up to `--parallelism` at a time.

| # | Direction | Frame | Liaison behavior |
|---|---|---|---|
| 1 | ctl → liaison | `Init{session_id="9f2a…", target_node="data-1", format=CSV, compress=GZIP, max_file_size=512MiB, rate_limit{…}, csv_annotations, liaison_bytes_per_second}` | Read `target_node`; a node that is not registered answers `NOT_FOUND`. Apply `liaison_bytes_per_second` to the process-wide bucket (last stream wins). Take the pooled connection and open an upstream bidirectional `Export` stream |
| 2 | liaison → data-1 | `Init` | Forward unchanged, without modifying any field. Both directions are pumped without prefetching, so back-pressure reaches the node |
| 3 | data-1 | Validate `Init` (`INVALID_ARGUMENT` if the first frame is not one); answer `UNIMPLEMENTED` for native; locate `<export-snapshot-path>/9f2a…` by `session_id` and renew the lease (`FAILED_PRECONDITION` when it is gone or expired; `NOT_FOUND` is kept for a liaison that cannot see the node, which the client retries through another liaison) | Forward errors unchanged |
| 4 | data-1 → liaison → ctl | `ready{}` | Forward |
| 5 | ctl → … → data-1 | `segment{catalog=STREAM, group="sw_record", segment_suffix="20260907", shard_ids=[0,1]}` | Forward. Only one unit at a time |
| 6 | data-1 | Renew the lease again, then check that the segment and both shard directories exist in the snapshot | — |
| 7 | data-1 → liaison → ctl | `unit_started{total_shards=2, total_parts=5}` | Forward |
| 8 | data-1 → liaison → ctl | `file_started{relative_path="stream/sw_record/seg-20260907/shard-0-00001.csv.gz", shard_id=0, seq=1}` → `chunk × N` → `file_finished{bytes, crc32, row_counts, schema_mod_revisions}` | Forward; each `chunk` waits on the liaison bucket first, and one larger than its share of the bucket's burst goes out in pieces of that share. First of 2 shards; a shard larger than `max_file_size` continues in `shard-0-00002.csv.gz` |
| 9 | data-1 → liaison → ctl | `file_started{…/shard-1-00001.csv.gz}` → `chunk × N` → `file_finished{…}` | Forward. Second of 2 shards |
| 10 | data-1 → liaison → ctl | `unit_finished{warnings=[…]}` | Forward |
| 11 | ctl | Keep the unit's files and warnings in memory for the manifest. Throughout steps 7–10 ctl prints the progress from the frames: the unit and its part count from `unit_started`, the file and its shard from `file_started`, the bytes and the `rows` of every `chunk`, and a line with the time of every finished file and unit; on a terminal one live line per node is redrawn in place | — |
| 12 | ctl → … → data-1 | `segment{…, segment_suffix="20260908", shard_ids=[0,1]}` or `property{group="sw_prop", shard_ids=[0]}` | Forward and repeat steps 6–10 |
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

A data node that joins the cluster after the session was created holds no snapshot for it. It answers `NOT_FOUND` to a `Plan{read{id}}`, which the liaison reports in `unreachable_nodes` rather than failing the call, and it answers its `ACTION_HEARTBEAT` frame with the `error` outcome, so the client sees it as a coverage gap either way. An expired session is different: until the sweeper removes it, the node answers `FAILED_PRECONDITION` and the call fails; once it is swept the node answers `NOT_FOUND` like any node without the session.

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
4. **Release** — after the export completes and `manifest.json` is written, `bydbctl` calls `Sessions{action=ACTION_RELEASE, session_id}`. **On a fail-fast abort it deliberately does not**: the session is kept, and the client prints the session ID plus the `bydbctl data release-session --id <id>` command; Ctrl-C and `SIGTERM` stop the export the same way and print the same command. Keeping the session is what will let a resumed run continue against the same frozen snapshot; resume itself is not implemented yet ([Remaining work](#49-remaining-work)), so today an interrupted export is re-run into a new, empty directory. There is deliberately no flag for "release on abort": it would make a graceful stop strictly worse than `kill -9`, because a released (or swept) session answers `NOT_FOUND` on every node, so the resumed run would see every node unreachable and have to restart against a new point in time — and `release-session` already covers the case where you really are done.
5. **Cleanup** — each data node sweeps export snapshots on a one-minute timer and removes any whose lease has expired. A session directory left without a readable lease (a crash mid-create or a failed rollback) is reclaimed once its newest directory is older than a 10-minute orphan grace; until then it still counts as an occupant, so a create without `preempt` answers `ALREADY_EXISTS`. The first sweep runs as soon as the export service starts serving, so after a data-node restart an expired session is reclaimed at once and a live one survives.

| Constant | Value | Why it is not configurable |
|---|---|---|
| Lease | 5 days | A data-node constant: every heartbeat resets the expiry to now + 5 days, so it only decides how long after the client disappears the snapshot is reclaimed. The request used to carry a lease with a 30-day cap; both were removed because the task length is irrelevant |
| Heartbeat interval | 5 min | A client-side timer, deliberately not derived from the lease. It keeps the lease fresh and stamps `last_heartbeat_at`, which a refused operator reads to judge whether the holder is still running |

The body of a snapshot is hard links, so it copies zero bytes, and deletion is `unlink` only — a background merge that removes a source part during export leaves the snapshot's link fully readable, which is what makes lock-free reading safe. "Hard links, therefore free" holds only for cold segments, though: the open segment's series index and the whole property database are full byte copies (the property snapshot is the index's own backup, written shard by shard, and also holds documents not yet flushed). The snapshot holds what was flushed to disk when it was taken; rows still in a memtable are not in it, so wait one flush interval after stopping writers for a complete export. Creating a session can be canceled only between groups and shards: the snapshot of a single group or shard is not interruptible. A large property copy can therefore outlast the liaison's per-frame wait for a node (5 minutes); the create then fails and is rolled back on every node.

The data-node gRPC port has no authentication of its own; the export RPCs are gated by RBAC only on the liaison. Deployments must keep the data-node port on an isolated network or behind mTLS, as they already must for the snapshot and segment-deletion RPCs on that port.

### 4.9 Remaining work

The CSV export step (2026-10-10) completes the export path for CSV: the `Export` RPC on the liaison (a directed relay, also in-process for a standalone server) and on the data node, the four CSV producers, the client session library (create with retries, refusal rendering, heartbeat, release), the receive loop, the schema files, the manifest and the `bydbctl data export` flags. What is still to come, each with where its design lives in this document:

- **Export resume** — `.export-progress.json` ([Export progress file](#71-export-progress-file)), continuing against the same session with `Plan(read{id})`, `pointInTime: mixed`, plan comparison, `export.checkpoint` / `--checkpoint`, `--seal-partial`, and a `bydbctl data progress` command. Today the output directory must be new or empty and an interrupted export starts over.
- **`--archive tar|tar.gz`** — packing the artifact into one file.
- **Native format** — `.bnc` containers, `data extract`; `FORMAT_NATIVE`, its proto fields and the per-format producer interface are already reserved ([Native export](#42-native-export)).
- **Import** — the CSV import (including property) and the cross-cluster e2e landed with the CSV import step ([CSV import as implemented](#54-csv-import-as-implemented)); native import and import resume remain design.
- **Deferred design items** — a dedicated `bydbctl data sessions` listing command (declined for now: the refusal text already renders the occupant), probe-before-create, pinning the session's node membership, skipping zombie nodes under file discovery, and a stage manifest inside the snapshot.

The end-to-end case `test/e2e-v2/cases/transfer/export/` runs the dry run, `release-session` and the CSV export against one hot and one warm data node behind a liaison.

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

Two routing failures must be distinguished: zero nodes returns `nil` plus `no nodes available`, while an out-of-range shard returns `... is a unknown shard`. Separately, `LocateAll` deduplicates its result set, so it can silently return fewer targets than the requested `copies` with no error, log, or warning. The import side must compare `len(targets)` against the requested `copies` itself; `--strict-replicas` cannot use `len(targets)` as its denominator. A shortfall is a WARN by default, recorded and filled in on the next resume; `--strict-replicas` promotes it to an error that aborts the run.

**Stage routing.** `ImportService` places every `(group, stage)` with the cluster's own node selection: the hot stage with the data node selector the liaison's write queue syncs online writes with, any other stage with the round-robin selector a lifecycle migration uses for it; see [Stage-aware routing](#543-stage-aware-routing). Multi-stage CSV import is therefore supported. The paragraph above about `LocateAll` and `--strict-replicas` describes the native design; the CSV import writes to every available distinct node when a stage has fewer than `copies` nodes and reports the actual count.

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
| `sidx-NNN.bnc` | Yes | `seg-XXX/sidx/**`, a legacy-engine inverted directory | `SERIES_INDEX` stream, **one per target node** ([Data transfer](#525-data-transfer)) — not one per shard |
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
  (a series index is a legacy-engine directory — only .seg and .snp, and no metadata.json)

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

**The series index is segment-scoped, not shard-scoped, so it is delivered once per target node.** `<segment>/sidx` is a single legacy-engine directory shared by every shard in that segment (`storage/segment.go` builds it from the segment location), and the receive side confirms it: `syncSeriesCallback.CreatePartHandler` — identical in stream, measure and trace — resolves the destination from `ctx.Group` and `ctx.MinTimestamp` only, calls `CreateSegmentIfNotExist`, and **never reads `ctx.ShardID`**. Compare the element index, whose handler does exactly the opposite: `syncElementIndexCallback.CreatePartHandler` calls `segment.CreateTSTableIfNotExist(common.ShardID(ctx.ShardID))`.

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

> **Superseded by the implementation.** The protocol below is the original draft. The implemented `ImportService` (`api/proto/banyandb/transfer/v1/import.proto`) differs: one stream per `(catalog, group, stage)` with the resource name on every row, no `success`/`error` in the acknowledgement (a failed batch ends the stream), a read-only `Targets` RPC, and the liaison **fans every row out to all replicas itself** instead of writing replica 0. See [CSV import as implemented](#54-csv-import-as-implemented).

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

### 5.4 CSV import as implemented

The CSV import landed on 2026-10-19 (`bydbctl data import`, `pkg/transfer/importer`, the liaison's `ImportService`). Where it differs from the design above, this section is authoritative. Native import and import resume are still future work.

#### 5.4.1 Workflow

```text
bydbctl data import --input DIR [--plan plan.yaml] [--dry-run] [-o table|yaml|json] ...

 0  preflight        reuse the export preflight (several liaisons, standalone fallback, ROLE_LIAISON)
 1  artifact check   manifest.json, format=csv, artifactVersion <= 1, non-empty schemaFiles,
                     every schema and unit file present with its size and CRC32; the server's
                     internal groups an older export kept are left out with a warning
 2  schema compare   read-only, artifact schema/*.jsonl against the target registry
 3  Targets          ImportService.Targets for every (group, stage) involved, read-only
 4  decide()         [1] schema gate, [4] TTL gate, multiple-source selection, and the ROWS
                     row check that reads every row (skipped with --schema-only) -> ImportPlan
 5  render           verdict table, byte-identical for --dry-run and the real run
 -- a dry run exits here: 0 = would proceed, 2 = would be refused --
 6  schema write     dependency order group -> stream/measure/trace/property -> indexRule
                     -> indexRuleBinding -> topNAggregation, through the registry Create / Update
 7  wait             SchemaBarrierService.AwaitSchemaApplied, including every group's auto-created
                     _top_n_result
 8  re-check         collect + Targets + decide again; a changed data plan is printed and the
                     run continues, or exits 5 with --abort-on-plan-change (schema written, no data);
                     a route failure of a created or overwritten group exits 3 here
 9  verify baseline  with --verify
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
  repeated string inactive_node_ids = 7;  // route nodes this liaison is not connected to
  bool group_deleting = 8;
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

**[4] TTL.** The effective TTL is the group TTL plus every stage TTL up to the mapped target stage (`CumulativeTTL`, checked against `pub.ResolveStageResourceOpts`); the default stage, including a fallback, has the group TTL only; property is exempt. A unit's newest time is the largest `maxTimestamp` of its selected sources, or, for index-mode segment files, the segment end parsed in the source node's timezone. A unit whose newest time is before `now − effectiveTTL` is refused (`STATUS` `expired`) unless `--force-ttl`. The gate compares each unit's newest row with now − cumulative TTL of the target stage; it does not project units onto the target segment grid, so a unit just inside the window may still land in a segment that expires soon after. Some units refused and the rest imported exits `0`; all refused exits `2`.

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

  The node knobs reuse the export's `RateLimit` and flag names, and like the export the data node enforces them. The liaison writes import rows on their own topics, one per catalog (`stream-import`, `measure-import`, `trace-import`, `property-import`), each frame a `transfer.v1.ImportWrite`: every batch stream to a node opens with an init frame carrying `rate_limit`. The node's import listener charges the batch to one byte pace and one row pace that every import on the node shares, then hands the rows to the catalog's write path; only the import topics pay, so other writes are never limited. A pace is the time by which the node has paid out what it admitted, moved on by each batch's cost at the rate that batch names, so the import that wrote last sets the rate and a change carries no debt; one second's worth (at most 1 MiB) passes on an idle node without waiting. The rates hold on the node **whichever and however many liaisons** write to it; a process that runs several data nodes, as a test harness does, keeps one pace per node. A batch waits for its own share and for the batches admitted before it, but no more than its own share plus `10s`: a batch that would wait longer, or past its request's deadline, is answered `STATUS_SERVER_BUSY` (marked `import rate limit:`) before anything is charged or written. The liaison therefore waits `2 min` plus the batch's share (the slower of its bytes over the byte rate and its rows over the row rate) plus `10s` for an acknowledgement, and resends a batch refused by the rate limit to that node with backoff for as long as the stream lasts; unlike memory pressure, the rate limit is not bounded by the write pressure timeout, since it is expected to hold batches back. A standalone server admits the rows the same way in process. A data node older than the import topics refuses them, so a cluster must run one version for an import. The node knobs are ignored by `--dry-run`, as are the other two.
- The schema write prints one summary line, `schema: <n> resource(s) written and applied on every node`; the plan table above it already lists every resource. During the push the client prints, like the export, a line for every finished work item (`<catalog> <group>/<stage>  [done/total] <position>  <rows> rows  <time>`) and one for every finished `(catalog, group, target stage)` (`✓ … done: <n> work item(s), <rows> rows in <time>`); on a terminal it also redraws one live line per group in flight with the rows written (acknowledged by the data nodes), the rows left of the planned ones, the row rate and the elapsed time (`<catalog> <group>/<stage>  <n> rows written, <m> left  <r> rows/s  <time>`). The rows written move one acknowledged batch at a time, so a small `writeBatchSize` makes them move more often.
- Multiple sources follow [§6.3](#63-rules), keyed by `(group, stage, segment, shard)`: coverage from `parts[]`, counts from `shards[].totalCount`. Property and index-mode units push every source. `--all-sources` skips the selection; `--strict-coverage` refuses (exit `2`) any shard work item that would push more than one source; index-mode segment items and property, which deduplicate or merge, are exempt.

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
| `1` | usage error |
| `2` | refused before anything was written (preflight, schema gate, row check `ROWS` including a corrupt artifact, all units refused, `--strict-coverage`) |
| `3` | failure after writing started (schema write, wait, re-check route of a created or overwritten group, push) |
| `4` | `--verify` failed |
| `5` | the re-check found a different data plan and `--abort-on-plan-change` was given |

Errors before step 6 map through the export's `ExitCodeFor`; every error from step 6 on is `3`.

#### 5.4.10 Known limitations

- `_top_n_result` is transferred verbatim and never recomputed, so it keeps the source's Top-N caliber; an incompatible target `_top_n_result` refuses the import.
- An acknowledgement is a batch-level acceptance; per-row failures on a data node are only logged and surface in `--verify`.
- Stream and trace row replay is not idempotent, and there is no resume: re-running after a failure duplicates what had landed.
- The TTL gate is coarse (newest row against cumulative TTL, no segment-grid projection).
- G4's emptiness check cannot see unflushed rows; G0 is not implemented.
- No `.tar` / `.tar.gz` input; it arrives with the export's `--archive`.
- `--verify` does not cover index structures and needs a quiet target; content equality is proven by the integration and e2e tests, which re-export the target and compare logical rows with the source artifact (`pkg/transfer/rowdiff`). A property is compared by its entity `(group, _name, _id)` at its latest revision only (the largest `_mod_revision`, a tombstone winning over a live row of the same revision; `_delete_time` compared as live or tombstone); older revisions are ignored on both sides.
- The same data held by source nodes in different timezones, or caught mid lifecycle migration, forms different units the multiple-source selection cannot match, so it is pushed more than once (duplicate trace spans), never lost.
- Pre-existing and not fixed here: the lifecycle row replay writes replica 0 only, and builds its route table from the group-level shard number while computing shards with the stage's.

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

- **What deduplication on that index does and does not do**: the winner is the first arrival, not the newest. The discarded copy's index tag values become unqueryable — `Lookup(entity)` still matches, because the legacy engine's document `_id` is the entity value itself, while a filter on a non-entity indexed tag returns nothing for the losing value.
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

> **Not implemented yet.** The CSV export step writes no progress file and cannot resume: the output directory must be new or empty, and an interrupted export is run again from scratch ([Remaining work](#49-remaining-work)). This section is the design.

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
    "id": "9f2a…"
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
| `Plan` `created` frame returns | Record `session.id` | Write on receipt — a resumed run needs the ID, and `release-session` cannot release a session that was never written down |
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
