# Migration manual

Operator-facing procedure to re-normalize **measure or stream** data on a BanyanDB deployment when the on-disk state has drifted from what the runtime expects. Common triggers:

- Historical segments left in a layout the current banyandb version no longer reads cleanly.
- A botched lifecycle event (failed merge / compaction / shard topology change) that left duplicated boundary rows or misaligned segments.
- A partial restore that landed measure / stream data outside the active stage's `SegmentInterval` grid.
- General "clean the historical measure / stream tree without losing rows" maintenance — re-bucket every source row onto a fresh, grid-aligned target tree that the BanyanDB runtime can pick up unchanged.

Two end-to-end procedures, depending on where the source data lives:

- [Part 1 — Local single-host flow](#part-1--local-single-host-flow): the source is a backup snapshot downloaded to a workstation, the target is a fresh data root (measure-data or stream-data) you ship into the cluster afterwards.
- [Part 2 — Kubernetes live in-place flow](#part-2--kubernetes-live-in-place-flow): the source is the live cluster's own data PVCs; a helper pod rewrites each PVC's measure / stream tree in place, the operator swaps the migrated groups onto every PVC, then the data StatefulSets restart on top of the clean tree.

For the tool's CLI surface, plan schema, and on-disk output layout, see [`README.md`](README.md).

> **Scope:** the tool migrates **measure** and **stream** data. The catalog of each group is auto-detected from the schema-property catalog (falling back to the backup directory layout for catalog detection only) and each group is routed to its catalog's executor — one plan may mix both catalogs; the runbook steps below are identical for both, substituting the stream data roots / `--stream-root-path` where a measure path is shown. Trace / property catalogs and `sw_metadata` are out of scope — they stay in place (Part 2) or remain on the source backup (Part 1). Stream notes (no dedup → exact row parity; the `idx/` element index; multi-stream slow-path resolution) are covered in [`README.md`](README.md).

---

# Part 1 — Local single-host flow

This part assumes a local backup snapshot is available, the snapshot contains a hot / schema-server node so the `schema-property` catalog is present, and the target measure-data root will be consumed by a BanyanDB process that is **not running** during the migration.

## 1. Build the migration binary

Build via the standard banyandb Makefile — it produces `build/bin/banyand-migration`:

```bash
cd <banyandb-repo-root>/banyand
make banyand-migration
ls build/bin/banyand-migration
```

Confirm the `copy` subcommand is wired in:

```bash
./build/bin/banyand-migration copy --help
```

## 2. Stage the backup snapshot locally

Download a backup snapshot for the date you intend to migrate (use whatever transport fits your environment: `gsutil cp`, `aws s3 cp`, `rclone`, etc.). After the download, the local layout should look like:

```
/tmp/banyandb-backup/
├── hot-0/2026-05-19/measure/<group>/seg-*/shard-*/<partID>/
├── hot-0/2026-05-19/stream/<group>/seg-*/shard-*/<partID>/    # stream parts; plus seg-*/sidx/ and shard-*/idx/
├── hot-0/2026-05-19/schema-property/_schema/shard-*/
├── hot-1/2026-05-19/...
├── warn-0/2026-05-19/measure/...
├── warn-1/2026-05-19/stream/...
└── cold-0/2026-05-19/measure/...
```

The plan loader will pick up `measure` / `stream` from every node directory (whichever catalog the plan's groups belong to) and `schema-property` from whichever hot / schema-server nodes carry it.

## 3. Write a migration plan

Copy [`example/plan-backup.yaml`](example/plan-backup.yaml) and adapt every path / date / group / target to your environment. The shipped sample is a **mixed measure + stream plan** using `/tmp/...` paths, so it works out of the box on a fresh workstation. The catalog of every group is auto-detected from the schema-property catalog and routed to its executor; both catalogs' group trees land under the same entry target. A single-catalog plan is the same file with only that catalog's groups listed.

```yaml
staging_dir: /tmp/banyandb-staging

source:
  backup:
    root: /tmp/banyandb-backup
    date: 2026-05-19

groups:
  # measure groups
  - sw_metricsMinute
  - sw_metricsHour
  - sw_metricsDay
  # stream groups
  - sw_records
  - sw_recordsLog
  - sw_recordsBrowserErrorLog

entries:
  - { stage: hot,  target: /tmp/banyandb-copy/hot/data,  nodes: [hot-0, hot-1]   }
  - { stage: warm, target: /tmp/banyandb-copy/warm/data, nodes: [warn-0, warn-1] }
  - { stage: cold, target: /tmp/banyandb-copy/cold/data, nodes: [cold-0]         }
```

> **Time zone matters for the target grid.** Target segment names are derived in the migration host's local time zone (matching banyandb's own local-day segment naming). Run the tool in the same time zone as the target cluster's data nodes; a mismatched zone (e.g. a UTC-grid backup migrated on a UTC+8 workstation) re-buckets every source segment across two local days — rows are all preserved, but parts straddle the new day boundary and take the slow path, and the output grid only matches nodes serving in that same zone.

Make sure each `entries[*].target` directory is absent or empty (the tool aborts otherwise), `staging_dir` (if set) is absent or empty, and `<source.backup.root>/<source.backup.date>` exists.

## 4. Run the migration

```bash
./build/bin/banyand-migration copy --copy-config /tmp/migration-plan.yaml
```

The tool prints timestamped progress per group / entry / part. A representative head of the live log:

```
[migration] GOMEMLIMIT set to 17.0GiB (85% of cgroup limit 20.0GiB) at 11:57:54
2026/05/20 11:57:55 [migration/measure] group sw_metricsMinute: building union sidx from 5 source dir(s) merged across all entries
2026/05/20 11:58:44 [migration/measure] group sw_metricsHour: building union sidx from 5 source dir(s) merged across all entries
2026/05/20 11:59:35 [migration/measure] group sw_metricsDay: building union sidx from 5 source dir(s) merged across all entries
2026/05/20 12:00:49 [migration/measure] entry [1/5] stage=hot nodes=[hot-0] target=/tmp/banyandb-copy/hot/measure-data
2026/05/20 12:00:49 [migration/measure] entry [1/5] stage=hot group sw_metricsMinute: 1 source dir(s), 34 parts, interval=day×1 — writing target (target="/tmp/banyandb-copy/hot/measure-data/sw_metricsMinute", union sidx="…/staging/groups/sw_metricsMinute/sidx")
2026/05/20 12:00:49 [migration/measure] entry [1/5] stage=hot group sw_metricsMinute: part 1/34 (2.9%) done seg-20260519/shard-0/0000000000002c4c rows=18907 targetParts=1
...
2026/05/20 12:00:50 [migration/measure] entry [1/5] stage=hot group sw_metricsMinute: done rows=7706398 segs=2 srcParts=34 targetParts=34
```

A stream plan logs under the `[migration/stream]` prefix with the same shape, plus a per-group element-index finalize step and a stream-specific `DONE` summary (fast-path = whole-part byte copies, slow-path = row-level rewrites; with matching time zones and unchanged `SegmentInterval`s most parts take the fast path):

```
2026/06/11 12:25:31 [migration/stream] loading stream schemas
2026/06/11 12:25:33 [migration/stream] group sw_records: building union sidx from 2 source dir(s) merged across all entries
2026/06/11 12:25:48 [migration/stream] entry [1/3] stage=hot nodes=[hot-0 hot-1] target=/tmp/banyandb-copy/hot/stream-data
2026/06/11 12:25:48 [migration/stream] entry [1/3] stage=hot group sw_records: 2 source dir(s), 80 parts, interval=day×1 — writing target (target="/tmp/banyandb-copy/hot/stream-data/sw_records", union sidx="/tmp/banyandb-staging/groups/sw_records/sidx")
2026/06/11 12:25:49 [migration/stream] entry [1/3] stage=hot group sw_records: part 1/80 (1.2%) done seg-20260610/shard-0/0000000000000fc4 rows=3502 targetParts=1
...
2026/06/11 12:27:23 [migration/stream] entry [1/3] stage=hot group sw_records: finalizing 4 aligned target segments (writing metadata + snp + union sidx)
2026/06/11 12:27:23 [migration/stream] entry [1/3] stage=hot group sw_records: done rows=253394 segs=4 srcParts=80 targetParts=85
...
DONE in 2m56.5s
   target segments   : 32
   source parts      : 562
   target mem-parts  : 580 (pre-merge; banyandb's merge loop will compact)
   rows copied       : 1899832
   bytes written     : 1623074219
   stream fast-path parts  : 512
   stream slow-path parts  : 32
   stream slow-path rows   : 641920
```

On error the tool aborts and removes the `staging_dir`. The partially-written target directories remain — clean them up manually before re-running.

To attach `pprof` mid-run, pass `--pprof-addr 127.0.0.1:6060` on the root command (before the `copy` subcommand) and then `go tool pprof -seconds=60 http://127.0.0.1:6060/debug/pprof/profile`.

## 5. Verify the output

Two complementary checks. The **file-level** one re-reads the same plan and inspects the on-disk parts; the **query-level** one exercises the data through a live BanyanDB process.

### 5.1 File-level verify (`migration verify`)

```bash
./build/bin/banyand-migration verify --copy-config /tmp/migration-plan.yaml
```

The subcommand walks every (entry, group) read-only: sums source rows by opening every `src/seg-*/shard-*/<partID>/`, enumerates `target/seg-*/`, opens each part and per-segment union sidx, and flags any target seg whose start time misaligns with the stage's `SegmentInterval` grid. It prints all the numbers — there is no exit code, this is an inspector. Sample tail:

```
== SUMMARY ==
  data coverage per (node, group):
    ✓  = src has rows AND tgt has rows  (normal copy success)
    S  = src has rows, tgt is empty     (copy lost this group — investigate)
    T  = tgt has rows, src is empty     (orphan target — investigate)
    -- = neither side has rows          (PVC does not hold this group; hash-sharding normal)
    ┌────────┬──────────────────┬────────────────┬───────────────┐
    │ node   │ sw_metricsMinute │ sw_metricsHour │ sw_metricsDay │
    ├────────┼──────────────────┼────────────────┼───────────────┤
    │ hot-0  │ ✓                │ --             │ ✓             │
    │ hot-1  │ ✓                │ ✓              │ --            │
    │ warn-0 │ ✓                │ --             │ ✓             │
    │ warn-1 │ ✓                │ ✓              │ --            │
    │ cold-0 │ ✓                │ ✓              │ ✓             │
    └────────┴──────────────────┴────────────────┴───────────────┘

  target segments                : 31
  target segments misaligned     : 0
  source rows total              : 588213170
  target rows total              : 588212817
```

For a stream plan the per-(entry, group) blocks are tagged `(stream)` and additionally report the per-shard element-index doc count (`idxDocs`). The acceptance bar also differs: measure tolerates small negative diffs (slow-path dedup of `(seriesID, timestamp)` duplicates, see below), while **stream never de-duplicates — source and target totals must match exactly; any non-zero diff is a bug**. The summary block says so explicitly:

```
== entry stage=hot target=/tmp/banyandb-copy/hot/stream-data group=sw_recordsLog (stream) ==
  src  : 384480 row(s) across 141 part(s) in 2 dir(s)
           /tmp/banyandb-backup/hot-0/2026-05-19/stream/sw_recordsLog
           /tmp/banyandb-backup/hot-1/2026-05-19/stream/sw_recordsLog
  tgt  : 384480 row(s) across 4 seg(s) under /tmp/banyandb-copy/hot/stream-data/sw_recordsLog
    seg-20260519           aligned shards=2 parts=41 rows=196491 sidxDocs=4729
      shard-0              rows=98246 parts=21 idxDocs=98246
      shard-1              rows=98245 parts=20 idxDocs=98245
...
== SUMMARY (stream) ==
  target segments                : 32
  target segments misaligned     : 0
  source rows total              : 1899832
  target rows total              : 1899832

  src == tgt  (stream invariant: no rows dropped)
```

Note `verify` is an inspector: it always exits 0 when it can read both trees, even when row counts mismatch — gate automation on the printed mismatch table, not the exit code.

### 5.2 Query-level verify (`verify-data.sh`)

Start a standalone BanyanDB process pointing at one entry's target — `--measure-data-path` for a measure target, `--stream-data-path` for a stream target (both flags accept a data root that directly contains `<group>/seg-*`):

```bash
./build/banyandb standalone \
    --measure-data-path=/tmp/banyandb-copy/hot/measure-data \
    --stream-data-path=/tmp/banyandb-copy/hot/stream-data \
    --grpc-port=17912 \
    --http-port=17913
```

Then in another shell:

```bash
bash banyand/cmd/migration/scripts/verify-data.sh
```

The script probes the most recent `DAYS` (default 15) UTC days through the liaison HTTP API. Measure probes: full day for `sw_metricsDay`, the `08:00..09:00` hour for `sw_metricsHour`, the `08:08..08:09` minute for `sw_metricsMinute`. Stream probes: the `08:00..09:00` hour for `sw_recordsLog/log` and `sw_recordsBrowserErrorLog/browser_error_log`. Any zero response is tagged `★MISSING`. Override `DAYS` to widen / narrow the window, or `LIAISON_HTTP` to point at a different liaison endpoint; set `CATALOGS=measure`, `CATALOGS=stream`, or leave the default `CATALOGS="measure stream"` to match what the plan migrated. Days older than a group's hot/warm/cold retention are expected to read `★MISSING` — that is the lifecycle window, not data loss.

## 6. Ship the copy into your cluster

The copy-mode output is a measure-data (or stream-data) root, byte-compatible with what a running BanyanDB pod expects. Common ways to bring it into a cluster: package the directory into a volume or image and start a pod with `--measure-root-path` / `--stream-root-path` pointing at it; or stop the BanyanDB process on the target node, swap the data directory, and restart. For an end-to-end in-place flow inside Kubernetes, see [Part 2](#part-2--kubernetes-live-in-place-flow).

## 7. Rollback

Copy mode never touches the source backup. To roll back, just delete the target data roots produced in Step 4:

```bash
rm -rf /tmp/banyandb-copy/{hot,warm,cold}/data
```

---

# Part 2 — Kubernetes live in-place flow

End-to-end procedure to migrate measure / stream data **in place** inside a live BanyanDB cluster running on Kubernetes. The steps below are written against the measure PVC paths; for stream groups substitute every `measure` path component with `stream` (see the note under Step 4). The runner pod mounts every data PVC, runs the migration into a `data-copy/` sibling on the same volume, and the operator then swaps `data/` ↔ `data-copy/` per PVC before resuming traffic.

The placeholder names used below (`skywalking-showcase` namespace, `demo` helm release, `hot-{0,1} / warn-{0,1} / cold-0` nodes) match the sample manifests under [`example/`](example/); adapt them to your real namespace, release, and topology.

## 1. Detach the liaison svc from its clients

The Service in front of the liaison pods (e.g. `demo-banyandb-grpc`) keeps routing every gRPC client to the live cluster until you repoint its selector to a label nothing in the namespace carries. This drops all in-flight + new connections immediately while leaving the Service object itself intact:

```bash
kubectl -n skywalking-showcase patch svc demo-banyandb-grpc \
    -p '{"spec":{"selector":{"app.kubernetes.io/component":"liaison-migration-mode"}}}'

kubectl -n skywalking-showcase get endpoints demo-banyandb-grpc
```

Endpoints should drop to 0. Liaison pods stay Running so the data pods keep their gossip / internal state.

## 2. Scale data StatefulSets to 0

The runner pod will re-mount each data PVC; RWO requires the original data pods to release them first.

```bash
for s in demo-banyandb-data-hot demo-banyandb-data-warm demo-banyandb-data-cold ; do
    kubectl -n skywalking-showcase scale sts $s --replicas=0
done
kubectl -n skywalking-showcase wait --for=delete pod -l app.kubernetes.io/component=data --timeout=5m
```

Note the original replica counts (e.g. hot=2, warm=2, cold=1) — Step 8 restores them.

## 3. Launch the migration-runner pod

Copy [`example/runner-pod.yaml`](example/runner-pod.yaml) and adapt it to your real cluster: PVC `claimName`s, mountPaths, resource limits, and the container `image` (use the same banyandb image your cluster runs — it already ships `banyand-migration` at `/banyand-migration`). The shipped sample mounts each measure PVC at `/mnt/<node>/measure`, each stream PVC at `/mnt/<node>/stream`, and each hot schema-property PVC at `/mnt/<node>/schema-property`, plus an `emptyDir` at `/work` for the plan files and per-group union sidx staging.

```bash
kubectl -n skywalking-showcase apply -f banyand/cmd/migration/example/runner-pod.yaml
kubectl -n skywalking-showcase wait --for=condition=ready pod/bydb-migration-runner --timeout=3m
```

## 4. Push the plan into the runner

Copy [`example/plan-live.yaml`](example/plan-live.yaml) and adapt the node names, paths, and target groups to your topology, then push it into the runner:

```bash
kubectl -n skywalking-showcase cp banyand/cmd/migration/example/plan-live.yaml \
    bydb-migration-runner:/work/plan.yaml
```

The shipped sample uses the **live** source mode and covers BOTH catalogs in one plan: in the in-place flow measure and stream live on different PVC roots, so each (node, catalog) root gets its own `stages` name and its own entry targeting `data-copy` on that volume. A group that doesn't exist under an entry's roots is skipped for that entry, which routes measure groups through the measure roots and stream groups through the stream roots automatically:

```yaml
source:
  live:
    schema_property_path: /mnt/hot-0/schema-property/data/_schema
    stages:
      hot:
        - { node: hot-0-measure, root: /mnt/hot-0/measure/data }
        - { node: hot-0-stream,  root: /mnt/hot-0/stream/data }
        - { node: hot-1-measure, root: /mnt/hot-1/measure/data }
        - { node: hot-1-stream,  root: /mnt/hot-1/stream/data }
      warm:
        - { node: warn-0-measure, root: /mnt/warn-0/measure/data }
        - { node: warn-0-stream,  root: /mnt/warn-0/stream/data }
        - { node: warn-1-measure, root: /mnt/warn-1/measure/data }
        - { node: warn-1-stream,  root: /mnt/warn-1/stream/data }
      cold:
        - { node: cold-0-measure, root: /mnt/cold-0/measure/data }
        - { node: cold-0-stream,  root: /mnt/cold-0/stream/data }

groups: [sw_metricsMinute, sw_metricsHour, sw_metricsDay, sw_records, sw_recordsLog, sw_recordsBrowserErrorLog]

entries:
  - { stage: hot,  target: /mnt/hot-0/measure/data-copy,  nodes: [hot-0-measure]  }
  - { stage: hot,  target: /mnt/hot-0/stream/data-copy,   nodes: [hot-0-stream]   }
  - { stage: hot,  target: /mnt/hot-1/measure/data-copy,  nodes: [hot-1-measure]  }
  - { stage: hot,  target: /mnt/hot-1/stream/data-copy,   nodes: [hot-1-stream]   }
  - { stage: warm, target: /mnt/warn-0/measure/data-copy, nodes: [warn-0-measure] }
  - { stage: warm, target: /mnt/warn-0/stream/data-copy,  nodes: [warn-0-stream]  }
  - { stage: warm, target: /mnt/warn-1/measure/data-copy, nodes: [warn-1-measure] }
  - { stage: warm, target: /mnt/warn-1/stream/data-copy,  nodes: [warn-1-stream]  }
  - { stage: cold, target: /mnt/cold-0/measure/data-copy, nodes: [cold-0-measure] }
  - { stage: cold, target: /mnt/cold-0/stream/data-copy,  nodes: [cold-0-stream]  }
```

## 5. Run the migration

```bash
kubectl -n skywalking-showcase exec bydb-migration-runner -- \
    /banyand-migration copy --copy-config /work/plan.yaml
```

Progress is printed in the same line shape as Part 1 — one line per part, plus a per-(entry, group) `done` line, and a final `DONE in …` summary with target seg / mem-part counts, rows copied, bytes written, and fast-/slow-path totals. The tool sets `GOMEMLIMIT` to 85 % of the pod's cgroup limit on startup, so the runner pod's memory limit directly controls peak heap usage.

## 6. Verify (optional but recommended)

Two checks again. Run at least the file-level one before you swap.

### 6.1 File-level verify (`migration verify`)

```bash
kubectl -n skywalking-showcase exec bydb-migration-runner -- \
    /banyand-migration verify --copy-config /work/plan.yaml
```

Output mirrors Part 1's `verify` — per (entry, group) src/tgt row counts, per-target-seg row + sidxDoc breakdown, the coverage table, and finally any mismatches. Excerpt from a real run:

```
== entry stage=hot target=/mnt/hot-0/measure/data-copy group=sw_metricsMinute ==
  src  : 7706398 row(s) across 34 part(s) in 1 dir(s)
           /mnt/hot-0/measure/data/sw_metricsMinute
  tgt  : 7706398 row(s) across 2 seg(s) under /mnt/hot-0/measure/data-copy/sw_metricsMinute
    seg-20260519           aligned shards=1 parts=19 rows=6325262 sidxDocs=1469980
    seg-20260520           aligned shards=1 parts=15 rows=1381136 sidxDocs=1469980
...
== SUMMARY ==
  target segments                : 31
  target segments misaligned     : 0
  source rows total              : 588213170
  target rows total              : 588212817

  row-count mismatches (tgt - src, negative = rows dropped by copy):
    stage  node    group             src        tgt        diff
    warm   warn-1  sw_metricsHour    2980213    2980121    -92
    cold   cold-0  sw_metricsMinute  448364487  448364341  -146
    cold   cold-0  sw_metricsHour    16360573   16360458   -115
    TOTAL                                                  -353
```

Small negative diffs (a few hundred rows out of hundreds of millions) are caused by slow-path `mustInitFromDataPoints` deduping `(seriesID, timestamp)` within each chunk flush — banyandb's merger can leave duplicated boundary rows in source parts, and only the latest version of each `(sid, ts)` survives in the target.

If `verify` reports a mismatch you want to drill into, run `analyze` on the offending (entry, group) to dump the exact within-part duplicate rows + the cross-part diff:

```bash
kubectl -n skywalking-showcase exec bydb-migration-runner -- \
    /banyand-migration analyze \
        --copy-config /work/plan.yaml \
        --entry-idx 3 --group sw_metricsHour --sample 5
```

It walks every src part for that (entry, group), reports total/unique/duplicate row counts, lists per-part block boundaries where consecutive blocks share the same `(sid, ts)` (the banyandb merger fingerprint), and runs an src↔tgt multiset diff that pinpoints the exact `(sid, ts, version)` rows missing from the target:

```
== analyze entry [4/5] stage=warm nodes=[warn-1] group=sw_metricsHour ==
  parts scanned                : 17
  total rows on disk           : 2980213
  distinct (sid, ts) (global)  : 2401206
  cross-part dup rows          : 579007  (NOT dropped by copy — slow path is per-part)
  (sid, ts) keys with >1 row   : 544037
  WITHIN-part dup rows         : 110  (← MATCH this against verify src-tgt diff)
  ...
== src-vs-target multiset diff ==
  src rows           : 2980213
  tgt rows           : 2980121
  missing rows total : 92  (← MATCHES verify src-tgt diff for this entry+group)
  missing rows (showing 5 / 74 keys):
    sid=3766501475917424574 ts=2026-05-06T15:00:00Z
      src    version=511194984324783  part=…/seg-20260506/shard-0/0000000000000013
      src    version=514535618046101  part=…/seg-20260506/shard-0/0000000000000013
      tgt    version=514535618046101  part=…/data-copy/sw_metricsHour/seg-20260430/shard-0/0000000000000034
      MISSING version=511194984324783  part=…/seg-20260506/shard-0/0000000000000013
```

### 6.2 Query-level verify (`verify-data.sh`)

Deferred to Step 9 — once the swap is done and queries are live again.

## 7. Swap migrated groups in place

For each PVC, move the migrated groups from `data-copy/` onto `data/`, keeping the originals under `data-backup/`. The list of migrated groups comes straight from your plan's `groups:` field — using the sample plan above, that is `sw_metricsMinute sw_metricsHour sw_metricsDay`:

```bash
kubectl -n skywalking-showcase exec bydb-migration-runner -- sh -c '
    set -eux
    MIGRATED_GROUPS="sw_metricsMinute sw_metricsHour sw_metricsDay"
    for n in hot-0 hot-1 warn-0 warn-1 cold-0 ; do
        base=/mnt/$n/measure
        mkdir -p "$base/data-backup"
        for g in $MIGRATED_GROUPS ; do
            [ -d "$base/data/$g" ]      && mv "$base/data/$g"      "$base/data-backup/$g"
            [ -d "$base/data-copy/$g" ] && mv "$base/data-copy/$g" "$base/data/$g"
        done
        rmdir "$base/data-copy" 2>/dev/null || true
    done
'
```

A missing source/copy dir on some PVC is normal — banyandb's hash sharding means each replica only carries a subset of groups. If you also migrated stream groups, repeat the same swap with `base=/mnt/$n/stream` and the stream group list.

Once the swap is confirmed (Step 9), tear the runner pod down so the data StatefulSets can re-claim their PVCs:

```bash
kubectl -n skywalking-showcase delete pod bydb-migration-runner \
    --grace-period=0 --force
kubectl -n skywalking-showcase wait --for=delete pod/bydb-migration-runner --timeout=2m
```

## 8. Scale data StatefulSets back up

Restore the original replica counts and wait until every data pod reports Ready:

```bash
kubectl -n skywalking-showcase scale sts demo-banyandb-data-hot  --replicas=2
kubectl -n skywalking-showcase scale sts demo-banyandb-data-warm --replicas=2
kubectl -n skywalking-showcase scale sts demo-banyandb-data-cold --replicas=1

kubectl -n skywalking-showcase wait --for=condition=ready \
    pod -l app.kubernetes.io/component=data --timeout=10m
```

After startup, each data pod's `segmentController` picks up the migrated `seg-*` directories and the merge loop will compact the per-source `partID` files into larger merged parts over time — no operator action required.

## 9. Port-forward + query-level verify

Port-forward to a liaison pod and run `verify-data.sh` against it:

```bash
kubectl -n skywalking-showcase port-forward \
    pod/demo-banyandb-liaison-0 17913:17913 > /tmp/pf.log 2>&1 &
until curl -sf -m 3 http://127.0.0.1:17913/api/healthz > /dev/null ; do sleep 2 ; done

bash banyand/cmd/migration/scripts/verify-data.sh
```

For each of the most recent `DAYS` (default 15) UTC days, the script issues a `stages=[hot,warm,cold]` query against each probe target (set `CATALOGS=measure` or `CATALOGS=stream` to restrict; default probes both):

| Catalog | Group / name | Window |
|---|---|---|
| measure | `sw_metricsDay` / `service_apdex_day`       | full day (`00:00:00 .. 23:59:59`) |
| measure | `sw_metricsHour` / `service_apdex_hour`     | the `08:00` hour (`08:00:00 .. 08:59:59`) |
| measure | `sw_metricsMinute` / `service_apdex_minute` | the `08:08` minute (`08:08:00 .. 08:08:59`) |
| stream  | `sw_recordsLog` / `log`                     | the `08:00` hour (`08:00:00 .. 08:59:59`) |
| stream  | `sw_recordsBrowserErrorLog` / `browser_error_log` | the `08:00` hour (`08:00:00 .. 08:59:59`) |

A trailing `★MISSING` marker is appended whenever the probe returns zero data points / elements for that day. Days beyond a group's retention are expected to be missing (record groups typically keep far fewer days than metrics); investigate any other `★MISSING` row before reattaching client traffic.

## 10. Reattach the liaison svc selector

Once Step 9 is green, point the Service back at the original liaison selector so external clients can reach the cluster again:

```bash
kubectl -n skywalking-showcase patch svc demo-banyandb-grpc -p '{
  "spec":{"selector":{
    "app.kubernetes.io/component":"liaison",
    "app.kubernetes.io/instance":"demo",
    "app.kubernetes.io/name":"banyandb"
  }}
}'

kubectl -n skywalking-showcase get endpoints demo-banyandb-grpc
```

## 11. Rollback

The migration never touches the live data dir until Step 7. To revert: detach client traffic again, scale data down, restore each PVC from `data-backup/`, scale back up, then reattach traffic.

```bash
kubectl -n skywalking-showcase patch svc demo-banyandb-grpc \
    -p '{"spec":{"selector":{"app.kubernetes.io/component":"liaison-migration-mode"}}}'

for s in demo-banyandb-data-hot demo-banyandb-data-warm demo-banyandb-data-cold ; do
    kubectl -n skywalking-showcase scale sts $s --replicas=0
done
kubectl -n skywalking-showcase wait --for=delete pod -l app.kubernetes.io/component=data --timeout=5m

kubectl -n skywalking-showcase apply -f banyand/cmd/migration/example/runner-pod.yaml
kubectl -n skywalking-showcase wait --for=condition=ready pod/bydb-migration-runner --timeout=3m

kubectl -n skywalking-showcase exec bydb-migration-runner -- sh -c '
    set -eux
    MIGRATED_GROUPS="sw_metricsMinute sw_metricsHour sw_metricsDay"
    for n in hot-0 hot-1 warn-0 warn-1 cold-0 ; do
        base=/mnt/$n/measure
        for g in $MIGRATED_GROUPS ; do
            [ -d "$base/data/$g" ]        && rm -rf "$base/data/$g"
            [ -d "$base/data-backup/$g" ] && mv "$base/data-backup/$g" "$base/data/$g"
        done
        rmdir "$base/data-backup" 2>/dev/null || true
    done'

kubectl -n skywalking-showcase delete pod bydb-migration-runner --grace-period=0 --force

kubectl -n skywalking-showcase scale sts demo-banyandb-data-hot  --replicas=2
kubectl -n skywalking-showcase scale sts demo-banyandb-data-warm --replicas=2
kubectl -n skywalking-showcase scale sts demo-banyandb-data-cold --replicas=1
kubectl -n skywalking-showcase wait --for=condition=ready \
    pod -l app.kubernetes.io/component=data --timeout=10m

kubectl -n skywalking-showcase patch svc demo-banyandb-grpc -p '{
  "spec":{"selector":{
    "app.kubernetes.io/component":"liaison",
    "app.kubernetes.io/instance":"demo",
    "app.kubernetes.io/name":"banyandb"
  }}
}'
```
