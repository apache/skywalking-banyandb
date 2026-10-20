# PR1383 real-data fault-injection validation

> This repository copy preserves the complete fault-validation narrative. Raw fault outputs, downloaded corpora, and instrumentation worktrees are intentionally not copied; all artifact references below are explicit external paths from the originating local run.

Captured 2026-10-02 UTC. All runs used detached scratch worktree `f4a44d0d73229a43614dabcf26c510c813975408` at external `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/worktree`; the PR worktree and immutable inputs were not modified.

## Production seam and deterministic faults

The scratch-only `StoreOpts.DirectoryFunc` seam wraps Bluge's real `FileSystemDirectory` while preserving BanyanDB's native `SegmentPlugin`, snapshot reader, and publication lifecycle. The fault wrapper was not a mock index or adapter-only test:

- `merge-cancel`: after 64 accepted one-document segment writes, the wrapper closes Bluge's real `closeCh` on the next segment persist. The native plugin's actual `nativeSegmentPluginMerge` callback is instrumented and observed, and its `WriteTo` returns cancellation. The finished segment is not published in a snapshot.
- `segment-short`: the real segment `WriterTo` is fed a short-write sink. A partial/zero-byte `.seg` may remain unreferenced; no snapshot is published.
- `segment-error`: the real segment `WriterTo` is fed an erroring sink. The segment persist fails; no snapshot is published.
- `snapshot-error`: after real segment persistence, snapshot `Persist` returns an injected error before publication. New segments remain unreferenced; the prior snapshot remains readable.
- `crash-after-segment`: after the real segment `Persist` returns successfully, the child writes a barrier and blocks. The parent kills only that child, deterministically between finished segment and snapshot commit; restart uses the native read-only reader.

An earlier artifact3 attempt incorrectly treated the captured baseline as mandatory even when writes were acknowledged; that superseded attempt is retained, not used for conclusions. The corrected oracle below is previous committed snapshot OR all acknowledged writes. Each case used a fresh copy of each corpus. The external directory `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/artifacts5/` is the callback-instrumented five-case matrix; the external directory `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/artifacts-native-merge-hook/` is the focused rerun proving actual native merge invocation/cancellation.

## Matrix outcome

Baseline visible counts are the captured immutable snapshots: Property 643 and SchemaProperty 8,933. `callback-success` counts are durable acknowledgements observed before the injected failure; absence of callbacks means no write was acknowledged before the disposable child exited (the writer's asynchronous retry was intentionally not converted into an acknowledgement).

| corpus / fault | expected restart | native visible | legacy open/stats | durable callbacks | segment persist events | snapshot persist events | native merge hook | result |
|---|---:|---:|---:|---:|---:|---:|---|
| Property / merge-cancel (focused) | 643 + 63 = 706 | 706 | PASS (706-class index) | 63 success, 0 error | 66 (call 65 canceled; retry call 66) | 74 | 1 (`segments=2`) | PASS |
| SchemaProperty / merge-cancel (focused) | 8,933 + 63 = 8,996 | 8,996 | PASS (8,996-class index) | 63 success, 0 error | 65 (call 65 canceled) | 77 | 1 (`segments=4`) | PASS |
| Property / segment-short | 643 | 643 | PASS (643-class index) | 0 / 0 | 2,254 | 0 | 0 | PASS |
| SchemaProperty / segment-short | 8,933 | 8,933 | PASS (8,933-class index) | 0 / 0 | 2,279 | 0 | 0 | PASS |
| Property / segment-error | 643 | 643 | PASS (643-class index) | 0 / 0 | 4,551 | 0 | 0 | PASS |
| SchemaProperty / segment-error | 8,933 | 8,933 | PASS (8,933-class index) | 0 / 0 | 2,292 | 0 | 0 | PASS |
| Property / snapshot-error | 643 | 643 | PASS (643-class index) | 0 / 0 | 65 | 2,235 (all rejected) | 0 | PASS |
| SchemaProperty / snapshot-error | 8,933 | 8,933 | PASS (8,933-class index) | 0 / 0 | 64 | 2,202 (all rejected) | 0 | PASS |
| Property / crash-after-segment | 643 | 643 | PASS (643-class index) | 0 / 0 | 1 (barrier after finish) | 0 | 0 | PASS |
| SchemaProperty / crash-after-segment | 8,933 | 8,933 | PASS (8,933-class index) | 0 / 0 | 1 (barrier after finish) | 0 | 0 | PASS |


Legacy readability was also exercised through `NewStore(NativeWriter=false)` plus `Stats()` on every case under external `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/artifacts5/`: all 10 corpus/fault copies passed. The focused external `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/artifacts-native-merge-hook/` copies passed the same legacy-open check for both corpora. This validates opening/readability, not a claim that injected failures were host filesystem permission or fsync failures.

The failed-write cases leave the old committed snapshot readable. Segment-short leaves a zero-byte/unreferenced segment in some copies; segment-error removes the failed segment. Snapshot-error leaves newly written unreferenced segments but no new `.snp`. Crash leaves one fully written unreferenced segment and no new `.snp`. The native reader never follows those unreferenced files.

Focused native merge event evidence:

- Property: `native-plugin-merge segments=2`, then `merge-write-cancel id=10f4 call=65`; no `10f4.seg` in the committed directory; visible count 706.
- SchemaProperty: `native-plugin-merge segments=4`, then `merge-write-cancel id=2ad8 call=65`; no `2ad8.seg` in the committed directory; visible count 8,996.

## Commands

Full callback-instrumented matrix (PASS, 107.973s):

```text
PR1383_PROPERTY_SNAPSHOT=/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/property-snapshot \
PR1383_SCHEMA_SNAPSHOT=/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/schema-property-snapshot \
PR1383_FAULT_ARTIFACT_ROOT=/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/artifacts5 \
go test ./pkg/index/inverted -run TestRealDataFaultValidation -count=1 -timeout=8m
```

Focused actual native merge cancellation (PASS, 22.116s): same command with `PR1383_FAULT_ARTIFACT_ROOT=.../artifacts-native-merge-hook PR1383_ONLY_MODE=merge-cancel`.

Read-only restart oracle command for every copy:

```text
(cd /mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/harness && go run doccount.go <case>/index)
```

Legacy readability command (all 10 matrix copies):

```text
PR1383_LEGACY_FAULT_ROOT=.../artifacts5 go test ./pkg/index/inverted -run TestLegacyReadFaultCopies -count=1 -timeout=3m
```

## Integrity / instrumentation

external `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/artifacts5/property/corpus-source-before.sha256` equals external `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/artifacts5/property/corpus-source-after.sha256`; the same holds for SchemaProperty (the per-index manifests are retained alongside them). Their aggregate file-manifest hashes are:

- Property: `8e83a58310653b54555807eda1c334f5b9886b0acaaf8ae61dfdf31eff6c1fa5` (before and after).
- SchemaProperty: `da7c35f8be25f6951d01feef7047674f67394773a869b595655a6ada3a311b3a` (before and after).

Instrumentation file hashes are retained in external `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/artifacts5/instrumentation-files-final.sha256`; event-log hashes are in external `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/artifacts5/event-log-sha256.txt`. The test-only patch is not committed or pushed.

Instrumentation checksum record (`instrumentation-files-final.sha256`):

- `pkg/index/inverted/inverted.go`: `90d4ac6a35d9035f02a86b1ac5ce4da0023577b3969b9f85efc561f2467e39e0`
- `pkg/index/inverted/native_plugin.go`: `c5677c75aa6b24245b5b124c89358f8b3462b621b7954f89dedccbf2d58cab00`
- `realdata_fault_validation_test.go`: `e427f0706fedc4ac255211c7b34a3a393bce4d11d8ec58c0db219e07497ab148`
- `legacy_fault_read_validation_test.go`: `898e1cbdc3cc5e8802add31501e13079913e6d2ed78a08f9c9991f0169095597`

Only the directory wrapper's short/error/snapshot failures are simulated I/O at the production `Directory.Persist` seam; they are not claims of host disk permission/fsync faults. Merge cancellation and crash publication use the real Bluge channel, native plugin merge, segment persist, snapshot lifecycle, and child-process barrier.

## Production writer recovery after each injected fault

On fresh clones of all 10 fault outputs under external `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/artifacts5/` (fault artifacts preserved), `propertydb.OpenDB` was reopened with `Index.NativeWriter=true` and `WaitForPersistence=true`. Each clone then received one unique recovery probe, waited for the production update to return, closed, reopened through `OpenDB`, and verified the exact probe ID/value plus query count `baseline+1`. The native raw reader count was also checked before/after/reopen.

| corpus | fault cases | Property query baseline → after → reopen | native raw baseline → after → reopen | result |
|---|---|---|---|---|
| Property | all five | 643 → 644 → 644 | crash/segment-error/segment-short/snapshot-error: 643 → 644 → 644; merge-cancel: 707 → 708 → 708 | 5/5 PASS |
| SchemaProperty | all five | 8,933 → 8,934 → 8,934 | crash/segment-error/segment-short/snapshot-error: 8,933 → 8,934 → 8,934; merge-cancel: 8,997 → 8,998 → 8,998 | 5/5 PASS |

The merge-cancel raw count includes the 64 acknowledged direct synthetic index documents, while the Property query intentionally excludes those documents because they do not carry the database's internal group/source field shape. These are kept as separate query/deletion and raw-index oracles.

Artifacts: external `/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002/fault-validation-20261002T123035/production-recovery3/<corpus>/<mode>/recovery.json`. Command:

```text
PR1383_RECOVERY_SOURCE_ROOT=.../artifacts5 \
PR1383_RECOVERY_ARTIFACT_ROOT=.../production-recovery3 \
go test ./pkg/index/inverted -run TestRealDataProductionWriterRecovery -count=1 -timeout=8m
```

This closes the restart gap: orphan segment states and prior snapshots were recovered by the actual production `propertydb.OpenDB` writer, a new durable production write was published, and close/reopen retained its exact value and count.

The production-recovery helper checksum is `5c1021f7efce0ce5cc8151fc507a38d124dcd18d0bbbc34e8b604e37321024e3` and is included in the final `instrumentation-files-final.sha256` record.
