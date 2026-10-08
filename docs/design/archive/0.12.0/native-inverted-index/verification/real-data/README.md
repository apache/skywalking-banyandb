# PR1383 complete downloaded-real-data validation

> This repository copy preserves the complete verification narrative from the external run. The companion [fault-injection report](fault-report.md) is copied beside it. Raw logs, downloaded corpora, manifests, and scratch worktrees are intentionally not copied; referenced evidence remains available only at the original local paths identified below.

- **Run:** `fullverification-20261002T122838Z`
- **Durable root (external evidence, not copied):** `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z`
- **UTC execution date:** 2026-10-02
- **Tested production commit:** `f4a44d0d73229a43614dabcf26c510c813975408` (candidate `fix(index): accept legacy empty doc-value chunks`)
- **Captured source/PR base:** `737d12c9f110106124fec8fc1cd0075e57607469`

This report covers successful production flows against fresh disposable copies of both immutable downloaded snapshots. It does not mutate or reopen the live source. The independent fault-validation artifacts and final recovery closure are retained as external evidence under `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035`; the portable narrative is in the [companion fault-injection report](fault-report.md).

## Overall conclusion

**PASS for the bounded ten-step real-data matrix:** successful native production flows passed on both downloaded corpora; all five injected fault classes × two corpora passed their intended committed-snapshot/acknowledgement oracle; and all ten fault outputs passed native production writer recovery with one durable probe and close/reopen verification. This is strong bounded lifecycle evidence for this commit and corpus, not a blanket claim of production readiness or exhaustive distributed/host-filesystem fault coverage.

## Inputs and safety

| Dataset | Immutable source | Baseline query rows | Baseline physical docs | Pre/post source hash comparison |
|---|---|---:|---:|---|
| Property | `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/property-snapshot/data` (`sw_property/shard-0`) | 643 | 643 | PASS (identical external `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/inputs/property-before.sha256` and `property-after.sha256`) |
| Schema Property | `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/schema-property-snapshot/data` (`_schema/shard-0`) | 8,933 | 9,220 | PASS (identical external `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/inputs/schema-property-before.sha256` and `schema-property-after.sha256`) |

All six work copies were made below `work/` in this run: `property-native`, `property-legacy`, `schema-property-native`, `schema-property-legacy`, and native-produced rollback copies. No source snapshot files were opened for writing. The parent checkout and captured `nativefix` worktree remained clean; instrumentation was applied only to detached disposable worktree `instrumented-nativefix`.

## Commands and environment

Harness module: `harness/` (copied from the captured harness), with its module replace pointed to `instrumented-nativefix`. Protobuf sources were generated in that disposable worktree using `make -C api generate`; generated files were not added to the production checkout.

```text
export PR1383_NATIVE_MERGE_LOG=<run>/<dataset>-native-merge.ndjson
cd <run>/harness
/usr/bin/time -v go run main.go <run>/work/property-native sw_property
/usr/bin/time -v go run main.go <run>/work/schema-property-native _schema
/usr/bin/time -v go run main.go <run>/work/property-native-rollback sw_property legacy
/usr/bin/time -v go run main.go <run>/work/schema-property-native-rollback _schema legacy
/usr/bin/time -v go run main.go <run>/work/property-legacy sw_property legacy
/usr/bin/time -v go run main.go <run>/work/schema-property-legacy _schema legacy
```

Every command exited `0`; command stdout, stderr, status, and `/usr/bin/time -v` output are retained at the external run root `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z`. `property-native` took 12.15 s wall time; `schema-property-native` took 2:20.93 wall time (see `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/schema-native.stderr` for exact `/usr/bin/time` peak RSS).

## Execution matrix

| Step | Property expected → actual | Schema Property expected → actual | Result |
|---|---|---|---|
| Immutable input/hash guard | source unchanged | source unchanged | PASS |
| Legacy baseline open/query | 643 rows, 2 logical delete markers → 643/2; ID digest `232ce2c2b49e1641f6868f58fb695e163dc7367a12a3f494ad73b4302a4e7055` | 8,933 rows, 284 markers → 8,933/284; ID digest `6035016058e8bf775b9add9dafe7d6e5efefe95f129572c1ad5def160c093fab` | PASS |
| Native OpenDB baseline | 643/2 | 8,933/284 | PASS |
| 128 durable native writes | 771 rows, 128 probes | 9,061 rows, 128 probes | PASS |
| Native close/reopen | 771 rows, source/ID digests unchanged | 9,061 rows, source/ID digests unchanged | PASS |
| Exact indexed filter | 1 row | 1 row | PASS |
| Indexed numeric sort/doc-value path | 771 rows, non-decreasing | 9,061 rows, non-decreasing | PASS |
| Repeated stored array value | 3 values (`r1,r2,r1`) | 3 values (`r1,r2,r1`) | PASS |
| Update + production Delete | 2 history rows, 1 deletion marker | 2 history rows, 1 deletion marker | PASS |
| Native final close/reopen | same update/delete result and aggregate digests | same update/delete result and aggregate digests | PASS |
| Read-only visible-doc oracle | 772 | 9,062 | PASS |
| Same native-produced path opened by legacy reader | baseline 772 rows/3 markers; lifecycle completed | baseline 9,062 rows/285 markers; lifecycle completed | PASS (rollback open/lifecycle) |

Full JSON summaries are external `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/property-native.stdout`, `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/schema-native.stdout`, `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/property-legacy-rollback.stdout`, and `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/schema-legacy-rollback.stdout`. Query logical delete-marker counts are intentionally distinct from manifest physical deletion-mask counts.

The independent legacy-writer runs also completed with exit 0. Property remained stable at 769 rows after its legacy writes/reopen. The captured Schema Property legacy-writer run opened and wrote successfully but its query row count changed from 9,061 to 8,779 after the legacy reopen; this is recorded as a legacy baseline observation, not used to claim native-writer correctness. Legacy rollback of native-produced snapshots was stable.

## Direct native production Merge observability

Instrumentation patch (not production code): external `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/instrumentation.patch`, SHA-256 `c2cc5ad007cc63efdbc39d5be8761ae391cf0583f85ecb95e04f3a11045ef55e`; source file SHA-256 is recorded in external `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/instrumented-nativefix`. The seam wraps `nativeSegmentPluginMerge` and records start/finish events from the actual segment plugin `Merger.WriteTo`, including source segment document counts, deletion/drop counts, bytes, and errors. This is direct runtime evidence, not inference from multi-document files. `PR1383_NATIVE_MERGE_LOG` was set only for the native harness commands.

- **Property:** 4 direct merge start/finish pairs, all finish events successful. Representative direct invocation: inputs `[642,1]`, drops `[2,0]`, output `3,523,604` bytes. The final committed manifest `000000001cfd.snp` contains old segment ID `4274` (642 docs) and generated multi-document IDs `4407` (10 docs), `4408` (10 docs), alongside one-document records. It has 113 records and 772 physical documents; every referenced segment is present.
- **Schema Property:** 11 direct merge start/finish pairs, all finish events successful. Representative direct invocations: `[502,1,1,1]` → 713,677 bytes and `[2818,505]` with drops `[414,0]` → 3,807,420 bytes. Final committed manifest `000000002c21.snp` contains source IDs `6182` (5,900 docs, 155 masked), `9551` (2,818 docs, 132 masked), generated merged ID `10907` (505 docs), generated 10-document records, and one-document records. It has 62 records, 9,349 physical documents, 287 masked ordinals; every referenced segment is present.

Manifest parser helper is retained as external `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/instrumented-nativefix/pkg/index/inverted/manifest_dump_test.go`; decoded summaries are external `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/property-manifest.json` and `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/schema-manifest.json`. Raw direct events are external `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/property-native-merge.ndjson` and `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/schema-native-merge.ndjson`.

## Verification and limitations

- `go test ./pkg/index/inverted -run '^TestNativePluginLoadsLegacySparseDocValueChunk$' -count=1` passed in the instrumented detached worktree.
- A broad `go test ./banyand/property/db` compile was not counted as a product failure: the checkout lacks generated `schema.NewMockGroup` and `metadata.NewMockRepo` symbols in unrelated test files. The exact production harness (`go run`) compiled and ran both datasets successfully.
- The production parent checkout was not changed or committed. The detached instrumentation worktree is retained for reproducibility and has only the test-only instrumentation plus generated ignored protobuf outputs.
- The harness's deterministic probe names and values contain no source-private values; the report contains only aggregate digests and counts.

## Independent injected-failure artifacts

The independent fault agent completed a final callback-instrumented matrix at:

`/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/artifacts5`

and a focused actual-native-merge cancellation rerun at:

`/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/artifacts-native-merge-hook`.

The earlier external `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/artifacts3/` attempt is retained but superseded: it used a baseline-freeze oracle and incorrectly classified accepted writes during merge cancellation. The final oracle counts only durable acknowledgements made before the injected barrier and separately asserts that canceled merge output was not committed.

| Dataset / fault | Expected restart | Actual visible restart | Durable ACKs | Failure-stage evidence | Result |
|---|---:|---:|---:|---|---|
| Property / merge-cancel | 643 + 63 = 706 (focused); full matrix 643 + 64 = 707 | 706 focused; 707 full matrix | 63 focused / 64 full, 0 errors | Native hook: `segments=2`, canceled output `10f4` absent from committed files | PASS |
| Schema Property / merge-cancel | 8,933 + 63 = 8,996 focused; full matrix 8,933 + 64 = 8,997 | 8,996 focused; 8,997 full matrix | 63 focused / 64 full, 0 errors | Native hook: `segments=4`, canceled output `2ad8` absent from committed files | PASS |
| Property / segment-short | 643 | 643 | 0 | Partial/short segment unreferenced; no snapshot publication | PASS |
| Schema Property / segment-short | 8,933 | 8,933 | 0 | Partial/short segment unreferenced; no snapshot publication | PASS |
| Property / segment-error | 643 | 643 | 0 | Segment persistence error; no snapshot publication | PASS |
| Schema Property / segment-error | 8,933 | 8,933 | 0 | Segment persistence error; no snapshot publication | PASS |
| Property / snapshot-error | 643 | 643 | 0 | 2,235 snapshot persist attempts rejected; no new `.snp` | PASS |
| Schema Property / snapshot-error | 8,933 | 8,933 | 0 | 2,202 snapshot persist attempts rejected; no new `.snp` | PASS |
| Property / crash-after-segment | 643 | 643 | 0 | Child killed after finished segment barrier; prior snapshot readable | PASS |
| Schema Property / crash-after-segment | 8,933 | 8,933 | 0 | Child killed after finished segment barrier; prior snapshot readable | PASS |

Full final matrix command (exit 0, 107.973 s):

```text
PR1383_PROPERTY_SNAPSHOT=/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/property-snapshot \
PR1383_SCHEMA_SNAPSHOT=/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/schema-property-snapshot \
PR1383_FAULT_ARTIFACT_ROOT=/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/artifacts5 \
go test ./pkg/index/inverted -run TestRealDataFaultValidation -count=1 -timeout=8m
```

Focused native merge-hook command (exit 0, 22.116 s) used the same environment with `PR1383_FAULT_ARTIFACT_ROOT=.../artifacts-native-merge-hook` and `PR1383_ONLY_MODE=merge-cancel`. It directly observed the real native `nativeSegmentPluginMerge` invocation, close-channel cancellation, persistence error, and absence of the canceled output segment. Failure-stage raw logs, callback counts, event hashes, source hashes, and the test-only instrumentation diff were copied under the external `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/failure-validation/` directory.
The final fault-agent corpus aggregate hashes matched before/after: Property `8e83a58310653b54555807eda1c334f5b9886b0acaaf8ae61dfdf31eff6c1fa5`; Schema Property `da7c35f8be25f6951d01efee7047674f67394773a869b595655a6ada3a311b3a`. The main success-flow source manifests independently matched in external `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/inputs/*-before.sha256` and `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/inputs/*-after.sha256`.

Failure cases used the native read-only restart oracle (`go run doccount.go <case>/index`) and preserved the old committed snapshot where no durable write was acknowledged. The final agent also opened all ten outputs with `NewStore(NativeWriter=false)` and `Stats()`: all ten legacy readability checks passed. Legacy compatibility restart was separately exercised against both native-produced successful snapshots (`property-native-rollback` and `schema-property-native-rollback`) with the production legacy reader/writer path; those runs exited 0.

All ten final fault cases passed their intended oracle. No wrong IDs, missing manifest references, incorrectly acknowledged failed writes, or canceled merge outputs were observed. The short-write cases may leave partial unreferenced bytes; snapshot-error may leave newly written unreferenced segments; crash-after-segment may leave one complete unreferenced segment. Those bytes are not reachable from the committed snapshot, which remained readable.

### Production writer recovery closure (10/10)

The final agent performed a bounded recovery closure on fresh clones of every output under external `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/artifacts5/`. Each clone was opened with production `propertydb.OpenDB` using `NativeWriter=true` and `WaitForPersistence=true`; one unique recovery probe was durably written, the database was closed, reopened, and checked for the exact probe value plus the expected query count. Recovery artifacts are under external `/mnt/d/graphloom-runs/banyandb-14075/fullverification-20261002T122838Z/failure-validation/production-recovery3/<dataset>/<fault>/recovery.json` and the source run's external `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/production-recovery3/`.

| Corpus | Fault outputs | Property query count baseline → after → reopen | Native raw count baseline → after → reopen | Result |
|---|---|---|---|---|
| Property | all five | 643 → 644 → 644 | crash/segment-error/segment-short/snapshot-error: 643 → 644 → 644; merge-cancel: 707 → 708 → 708 | 5/5 PASS |
| Schema Property | all five | 8,933 → 8,934 → 8,934 | crash/segment-error/segment-short/snapshot-error: 8,933 → 8,934 → 8,934; merge-cancel: 8,997 → 8,998 → 8,998 | 5/5 PASS |

The merge-cancel raw count includes the 64 acknowledged direct synthetic index documents; the Property group query intentionally excludes those documents because they lack the database's internal group/source field shape. Query and raw-index counts are therefore distinct oracles, not a discrepancy. This closes the writer-restart gap: every orphan-segment/prior-snapshot state accepted a real production write, published it, and retained its exact value/count after close/reopen.

Recovery command:

```text
PR1383_RECOVERY_SOURCE_ROOT=.../artifacts5 \
PR1383_RECOVERY_ARTIFACT_ROOT=.../production-recovery3 \
go test ./pkg/index/inverted -run TestRealDataProductionWriterRecovery -count=1 -timeout=8m
```

All five fault classes × two datasets and all ten recovery closures passed. This is bounded real-data lifecycle evidence, not a proof of every distributed or host-filesystem failure mode.
