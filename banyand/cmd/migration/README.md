# BanyanDB measure migration

A one-shot CLI that rewrites **measure** data — from either a local backup snapshot or a set of live PVC mount paths — into one or more target measure-data roots, with rows re-bucketed onto each target stage's `SegmentInterval` grid. Stream / trace / property catalogs and `sw_metadata` are **out of scope**: the tool only touches measure data.

For end-to-end operator runbooks (workstation flow + in-cluster live flow), see [`MIGRATION.md`](MIGRATION.md).

The CLI exposes three subcommands; all share a single required flag `--copy-config <path>` pointing at the YAML plan documented below:

| Subcommand | Purpose |
|---|---|
| `copy`    | Rewrite measure data from the plan's source into each entry's target. Side-effecting. |
| `verify`  | Re-read the same plan and report per-(entry, group) source-vs-target row counts, segment grid alignment, and union-sidx doc counts. Read-only inspector. |
| `analyze` | For one (entry, group), walk every source part and dump within-part duplicate rows, per-part block boundaries, and an exact src↔tgt multiset diff. Diagnostic. |

Each source row is routed to a grid-aligned target segment using the `SegmentInterval` defined by the entry's `stage`. A per-group **union sidx** is built once from every source segment and broadcast (byte-copied) into every aligned target segment. The tool reads schemas and group `ResourceOpts` directly from the source's `schema-property` bluge catalog — no liaison or network access is required at run time.

---

## What it does

1. Loads measure schemas (tag families + `IndexMode` bit) and group `ResourceOpts` from the source's schema-property bluge catalog.
2. Rejects any requested group that contains an `IndexMode` measure (their field data lives inside sidx and broadcasting the union would corrupt cross-node dedup at query time).
3. For each group, walks every source segment under `<source>/<node>/.../measure/<group>/` and:
   - Builds one **union sidx** at `<staging_dir>/<group>/sidx/` by deduplicating every source sidx doc by SeriesID.
   - Discovers every `seg-*/shard-*/<partID>/` directory and records a `partTask` list.
4. For each `(entry, group)` pair:
   - Resolves the SegmentInterval for `entry.stage`.
   - For each source part: if all rows fall in one aligned target segment, takes the **fast path** (whole-part byte copy under a fresh target partID); otherwise the **slow path** row-level rewrite into per-target-segment buckets.
   - Writes `metadata` (segment version + endTime) and a `.snp` snapshot file per `(target segment, shard)`.
   - Broadcasts the group's union sidx into every aligned target segment that received any rows.

Entries run sequentially. The first error aborts the run. The whole `staging_dir` is removed at the end (success or failure).

---

## Limitations

- The source **must** include a `schema-property` catalog under at least one node (typically a hot / schema-server node). Sources lacking the catalog are rejected.
- All groups containing any `IndexMode` measure are refused up front.
- Source and target shard topology must match (no reshard semantics).
- The tool assumes nothing else is writing to the target paths.

---

## Configuration

A `CopyPlan` selects ONE source mode (`backup` or `live`) and a list of `entries` that fan-out the same source data into multiple target directories. Two ready-to-edit samples ship under [`example/`](example/): [`plan-backup.yaml`](example/plan-backup.yaml) (workstation, reads a downloaded snapshot) and [`plan-live.yaml`](example/plan-live.yaml) (in-cluster, reads live PVC mounts).

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
#  - target: the measure-data root for this entry. Must not pre-exist
#            (or be empty) for any group dir written under it.
#  - nodes:  the subset of source node identifiers (backup directory
#            names, or live `stages[stage][].node` keys) feeding this
#            entry's target.
# Targets must be globally unique across all entries.
entries:
  - { stage: hot,  target: /tmp/banyandb-copy/hot/measure-data,  nodes: [hot-0, hot-1]   }
  - { stage: warm, target: /tmp/banyandb-copy/warm/measure-data, nodes: [warn-0, warn-1] }
  - { stage: cold, target: /tmp/banyandb-copy/cold/measure-data, nodes: [cold-0]         }
```

Validation errors are surfaced at startup, before any byte is written. The common ones: missing `source` (or setting both modes), missing/duplicate `groups`, missing/duplicate `entries[*].target`, bad `date` format, pre-existing non-empty `staging_dir`, or an `entries[*].nodes` entry that does not match any node declared under `source.live.stages[stage]`.

---

## Output layout

Each target directory is byte-compatible with what a BanyanDB data pod expects at `--measure-root`. The on-disk shape under one entry's target is:

```
<entry.target>/<group>/seg-YYYYMMDD[HH]/
                       ├── metadata                     # storage.SegmentMetadata: segment version + endTime
                       ├── shard-N/
                       │   ├── <16-hex-partID>/         # measure part files (one dir per copied part)
                       │   └── <16-hex-partID>.snp      # part index snapshot
                       └── sidx/                        # broadcast union sidx (bluge index)
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
