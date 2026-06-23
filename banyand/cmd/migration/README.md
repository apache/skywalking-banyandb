# BanyanDB data migration (measure & stream)

A one-shot CLI that rewrites **measure** or **stream** data — from either a local backup snapshot or a set of live PVC mount paths — into one or more target data roots, with rows re-bucketed onto each target stage's `SegmentInterval` grid. The catalog of each group is auto-detected (authoritatively from the source's schema-property catalog via `banyand/metadata/schema/reader`; when a backup snapshot carries no catalog, the backup directory layout — `<node>/<date>/{measure,stream}/<group>/` — classifies the group instead) and each group is routed to its catalog's executor. One plan may mix normal measure, **index-mode measure** (e.g. `sw_metadata`), and stream groups freely. Trace / property catalogs are **out of scope**.

Index-mode specifics: an index-mode measure (`IndexMode: true`, e.g. SkyWalking's `sw_metadata` group) stores no columnar parts — every data point is one inverted document under the segment-level `seg/sidx/`, upserted by series per segment (one max-version doc per `(series, segment)`). The tool detects these groups from the catalog and routes them through a dedicated path: each source sidx is either **byte-copied** whole (when all its docs align to one not-yet-written target segment) or **rebuilt** doc-by-doc — stored tag fields are carried over verbatim and the index-only entity fields (`_im_name` / `_im_entity_tag_*`, never stored) are regenerated from each doc's `_id`, with `Analyzer`/`NoSort` restored from the catalog's index rules so the migrated sidx stays searchable. Re-gridding onto a coarser/realigned target collapses redundant same-series docs (max version wins), so the target doc count is `≤` source — every distinct series is preserved, never lost. The per-group internal `_top_n_result` measure (auto-created, normally empty) is ignored; an index-mode group that also holds a non-`_top_n_result` normal measure with actual shard parts is refused. A doc read back with `_timestamp == 0` (which never occurs for valid data, since the write path always stamps a checked non-zero timestamp) is treated as corrupt and aborts the copy with the offending sidx + sample series IDs rather than being routed to a guessed segment.

Stream specifics: stream has no field columns, no version, and the tool never de-duplicates rows, so source and target row counts are expected to match **exactly**. Each stream segment carries two index layers — the segment-level series index (`sidx/`, union-built and broadcast like measure) and the per-shard element index (`idx/`). On the fast path (a source segment maps to a single target segment) both are byte-copied; on the slow path (a source segment splits across target segments) the element index is rebuilt from rows + index rules. The slow path supports multi-stream groups (e.g. SkyWalking's `sw_records`, 21 streams): because a shard's element index mixes docs from every stream of the group, each row's owning stream is resolved per-row from its `seriesID` via the source segment's series index (`seriesID -> sidx EntityValues -> pbv1.Series.Subject`), and that stream's index locator is used to build the row's doc. The fast path has no such cost (it byte-copies).

For end-to-end operator runbooks (workstation flow + in-cluster live flow), see [`MIGRATION.md`](MIGRATION.md).

The CLI exposes three subcommands; all share a single required flag `--copy-config <path>` pointing at the YAML plan documented below:

| Subcommand | Purpose |
|---|---|
| `copy`    | Rewrite measure or stream data from the plan's source into each entry's target. Side-effecting. |
| `verify`  | Re-read the same plan and report per-(entry, group) source-vs-target row counts, segment grid alignment, and union-sidx doc counts. For index-mode groups it instead reconciles sidx doc counts, distinct doc-id (series) counts, per-`(segment, series)` full-field value digests, and per-segment 1:1 alignment. Read-only inspector. |
| `analyze` | For one (entry, group), walk every source part and dump within-part duplicate rows, per-part block boundaries, and an exact src↔tgt multiset diff. For index-mode groups it reports `(series, timestamp)` version-duplicate and value-conflict keys (explains why target doc count `≤` source). Diagnostic. |

Each source row is routed to a grid-aligned target segment using the `SegmentInterval` defined by the entry's `stage`. A per-group **union sidx** is built once from every source segment and broadcast (byte-copied) into every aligned target segment. The tool reads schemas and group `ResourceOpts` directly from the source's `schema-property` bluge catalog — no liaison or network access is required at run time.

---

## What it does

1. Loads the schemas of the plan's catalog from the source's schema-property bluge catalog — measure schemas (tag families + `IndexMode` bit + index rules) or stream schemas (tag families + index-rule bindings + entity layout) — plus each group's `ResourceOpts`.
2. Classifies each measure group as **normal** or **index-mode** (a group is index-mode when its only non-`_top_n_result` measures all carry `IndexMode: true`). Normal groups take the union-sidx path below; index-mode groups take the sidx rebuild/byte-copy path (see "Index-mode specifics" above) and are **excluded** from union-sidx broadcast. Stream has no `IndexMode`.
3. For each **normal** measure / stream group, walks every source segment under `<source>/<node>/.../measure/<group>/` (or `.../stream/<group>/`) and:
   - Builds one **union sidx** at `<staging_dir>/<group>/sidx/` by deduplicating every source sidx doc by SeriesID.
   - Discovers every `seg-*/shard-*/<partID>/` directory and records a `partTask` list.
4. For each `(entry, group)` pair:
   - Resolves the SegmentInterval for `entry.stage`.
   - For each source part: if all rows fall in one aligned target segment, takes the **fast path** (whole-part byte copy under a fresh target partID); otherwise the **slow path** row-level rewrite into per-target-segment buckets. Measure's slow path de-duplicates `(seriesID, timestamp)` within a chunk; stream's slow path never de-duplicates.
   - Writes `metadata` (segment version + endTime) and a `.snp` snapshot file per `(target segment, shard)`.
   - Broadcasts the group's union sidx into every aligned target segment that received any rows.
   - Stream only: finalizes the per-shard element index (`idx/`) — byte-copied when a source `(segment, shard)` fed exactly one fresh target segment, otherwise rebuilt row-by-row from the copied parts + the group's index rules (entity-tag values are resolved through the source segment's sidx).

Entries run sequentially. The first error aborts the run. The whole `staging_dir` is removed at the end (success or failure).

---

## Limitations

- Every command **requires** a `schema-property` catalog under at least one source node (typically a hot / schema-server node): group catalogs, segment alignment and stream index rebuilds all resolve from it. A source without the catalog fails fast.
- Index-mode measure groups are supported (sidx rebuild / byte-copy path). A group that mixes an index-mode measure with a non-`_top_n_result` normal measure that has actual shard parts is refused — split such a group before migrating.
- Source and target shard topology must match (no reshard semantics).
- The tool assumes nothing else is writing to the target paths.

---

## Configuration

A `CopyPlan` selects ONE source mode (`backup` or `live`) and a list of `entries` that fan-out the same source data into multiple target directories. Two ready-to-edit samples ship under [`example/`](example/), both covering measure + stream groups in one plan: [`plan-backup.yaml`](example/plan-backup.yaml) (workstation, reads a downloaded snapshot) and [`plan-live.yaml`](example/plan-live.yaml) (in-cluster, reads live PVC mounts; each (node, catalog) PVC root gets its own stages name and entry).

```yaml
# Optional. When unset, a fresh os.MkdirTemp("banyandb-migration-staging-*")
# is created under TMPDIR. When set, the directory must be absent OR an
# empty directory (otherwise the loader aborts). The whole staging tree
# is removed at the end of the run.
staging_dir: /tmp/banyandb-staging

# Required. Exactly one of source.backup or source.live must be set.
source:
  # --- mode A: backup snapshot --------------------------------------
  backup:
    # Local path holding the backup snapshot, laid out as
    #   <root>/<node>/<date>/measure/<group>/seg-*/shard-*/<partID>/
    #   <root>/<node>/<date>/stream/<group>/seg-*/shard-*/<partID>/
    #   <root>/<node>/<date>/schema-property/_schema/shard-*/
    root: /tmp/banyandb-backup
    # Pins the snapshot date inside <root>. Format YYYY-MM-DD.
    date: 2026-05-19
  # --- mode B: live PVC mounts (mutually exclusive with backup) -----
  # live:
  #   schema_property_path: /mnt/hot-0/schema-property/data/_schema
  #   stages:
  #     hot:
  #       - { node: hot-0, root: /mnt/hot-0/measure/data }
  #     warm: [...]
  #     cold: [...]

# Required. Non-empty, no duplicates. Each group is processed once per entry.
groups:
  - sw_metricsMinute
  - sw_metricsHour
  - sw_metricsDay

# Required. Non-empty. Each entry names one fan-out destination.
#  - stage:  one of the LifecycleStages configured on every group's
#            ResourceOpts (e.g. hot / warm / cold) — its SegmentInterval
#            determines the target grid for this entry.
#  - target: the data root (measure-data or stream-data) for this entry.
#            Must not pre-exist (or be empty) for any group dir written
#            under it.
#  - nodes:  the subset of source node identifiers (backup directory
#            names, or live `stages[stage][].node` keys) feeding this
#            entry's target.
# Targets must be globally unique across all entries.
entries:
  - { stage: hot,  target: /tmp/banyandb-copy/hot/measure-data,  nodes: [hot-0, hot-1]   }
  - { stage: warm, target: /tmp/banyandb-copy/warm/measure-data, nodes: [warn-0, warn-1] }
  - { stage: cold, target: /tmp/banyandb-copy/cold/measure-data, nodes: [cold-0]         }
```

For a **stream** plan the shape is identical — list stream groups (e.g. `sw_records`, `sw_recordsLog`, `sw_recordsBrowserErrorLog`) and point each entry's `target` at a stream-data root (e.g. `/tmp/banyandb-copy/hot/stream-data`). Measure and stream groups may also share one plan: each group is routed by its auto-detected catalog, and the catalogs run one after the other within a single invocation. Note a mixed plan writes both catalogs' group trees under the same `entries[*].target` — when serving that root, point `--measure-data-path` and `--stream-data-path` at it (each service only loads its own catalog's groups), or keep per-catalog plans with distinct targets. See [`MIGRATION.md`](MIGRATION.md) §3 for a full stream plan demo.

Validation errors are surfaced at startup, before any byte is written. The common ones: missing `source` (or setting both modes), missing/duplicate `groups`, missing/duplicate `entries[*].target`, bad `date` format, pre-existing non-empty `staging_dir`, an `entries[*].stage` that is not defined as a key in `source.live.stages`, or an `entries[*].nodes` entry that does not match any node declared under `source.live.stages[stage]`.

---

## Output layout

Each target directory is byte-compatible with what a BanyanDB data pod expects at `--measure-root-path` / `--stream-root-path`. The on-disk shape under one entry's target is:

```
<entry.target>/<group>/seg-YYYYMMDD[HH]/
                       ├── metadata                     # storage.SegmentMetadata: segment version + endTime
                       ├── shard-N/
                       │   ├── <16-hex-partID>/         # measure part files (one dir per copied part)
                       │   └── <16-hex-epoch>.snp       # part index snapshot
                       └── sidx/                        # broadcast union sidx (bluge index)
```

A stream target carries one extra layer — the per-shard element index:

```
<entry.target>/<group>/seg-YYYYMMDD[HH]/
                       ├── metadata
                       ├── shard-N/
                       │   ├── <16-hex-partID>/         # stream part files
                       │   ├── <16-hex-epoch>.snp       # part index snapshot
                       │   └── idx/                     # element index (bluge); byte-copied or rebuilt, see above
                       └── sidx/                        # broadcast union sidx (bluge index)
```

An **index-mode** measure target carries no `shard-N/` parts at all — every segment holds only the rebuilt/byte-copied series index:

```
<entry.target>/<group>/seg-YYYYMMDD[HH]/
                       ├── metadata                     # storage.SegmentMetadata: segment version + endTime
                       └── sidx/                        # index-mode docs (bluge); byte-copied or rebuilt per segment
```

Segment names use `seg-YYYYMMDD` for `DAY` units and `seg-YYYYMMDDHH` for `HOUR` units, matching `banyand/internal/storage.segmentController`. PartIDs are emitted as zero-padded 16-character lowercase hex, starting at `0000000000000001` and incrementing as `copy` writes parts — banyandb's runtime picks up the highest existing partID on startup so live writes resume at `max(N)+1`.

---

## Build & run

Build via the top-level Makefile — it produces every banyandb binary, including `banyand-migration`, under `banyand/build/bin/`:

```bash
cd <banyandb-repo-root>
make build
ls banyand/build/bin/banyand-migration
```

Run any of the three subcommands with the same `--copy-config` plan:

```bash
./banyand/build/bin/banyand-migration copy    --copy-config ./migration-plan.yaml
./banyand/build/bin/banyand-migration verify  --copy-config ./migration-plan.yaml
./banyand/build/bin/banyand-migration analyze --copy-config ./migration-plan.yaml \
    --entry-idx 3 --group sw_metricsHour --sample 5
```

To attach `pprof` to a long-running `copy`, pass `--pprof-addr` (persistent flag on the root command):

```bash
./banyand/build/bin/banyand-migration --pprof-addr 127.0.0.1:6060 copy --copy-config ./migration-plan.yaml &
go tool pprof -seconds=60 http://127.0.0.1:6060/debug/pprof/profile
```

The `copy` subcommand also auto-sets `GOMEMLIMIT` to 85 % of the pod cgroup memory limit on startup (see `applyGoMemLimit` in `banyand/cmd/migration/copy.go`) so a momentary heap spike during slow-path zstd flush does not trip the kernel OOMKiller.
