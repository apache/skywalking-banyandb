# T17 million-row readiness corruption

## Cause and fix

The million-row fixture mismatch exposed a shared measure-storage compaction
bug. It was not a QL grammar, oracle, or integer codec defect.

When `mergeBlocks` splits an oversized merged block, it writes the first part
and retains the remaining rows in `pendingBlock`. Copying those rows copied byte
slice descriptors, not their backing bytes. The split then returned the shared
`BytesBlockDecoder` to its pool. A subsequent decode could overwrite the retained
field/tag bytes before the tail was written, persisting incorrect values.
Null-free numeric columns can have independent allocations, explaining why the
row ID and timestamps remained correct while a nullable field changed.

The split now detaches the retained tail into one compact allocation before
releasing the decoder. It copies fields and tags, preserves nil versus empty
values, and caps each cell's slice capacity at its length. The allocation covers
only retained payload bytes; complete source decode buffers are not held across
successive splits. Other merge paths and stored/wire encodings are unchanged.
The fix prevents subsequent merges from introducing this corruption; it does
not reconstruct values already corrupted in existing parts.

## Reproduction

The original fixture timestamp was pinned at `1792299660000000000`, with the
same 4-CPU, 8-GiB, zero-extra-swap Docker limits and 256-MiB query tracker.
The diagnostic replay reproduced the original hashes exactly:

- Host `host_00`, row window `[638976, 655360)`, row `643232`.
- Expected `key0=3279`; received `key0=1776`.
- Timestamp, row ID, `key1=2`, and `v=-23` matched.

A standalone replay of that window passed, as did a 655,360-row prefix.
Writing all one million rows and querying that window first also passed.
Running earlier host-00 readiness reads reproduced corruption at another row:
`393232`, expected `key0=2475`, received `32`. This narrowed the trigger to
background processing/state reuse rather than deterministic fixture arithmetic.
Isolated integer, byte-block and dictionary roundtrips passed.

`Test_mergeParts_splitPreservesValues` then reproduced the actual merge defect
without RPC or timing dependencies. Seven file parts contain 38,192 rows, nullable
integer fields, string fields and string tags. Before the fix, timestamp `8193`
contained tag `tag-00018193` instead of `tag-00008193`. The test checks every
payload, timestamp, version, row count and output block size. After the fix, five
repetitions passed. A direct ownership test poisons the old backing and checks
nil/empty preservation and isolation between adjacent cells.

Readiness failures now include host/window and a first differing tuple, while
ignoring storage SID/version and result order as before. Temporary minimization
tests were removed. Failed reports remain historical evidence rather than being
reclassified as accepted resource rejection.

## Evidence

- `.omx/t17-debug-1m.log`: original pinned failure with tuple diagnostics.
- `.omx/t17-debug-{prefix,full-window,host0}.log`: narrowed RPC experiments.
- `.omx/t17-merge-tail-red.log`: deterministic pre-fix failure.
- `.omx/t17-merge-tail-green.log`: repeated passing regression and ownership test.

## Fixed million-row probe

The original million-row scenario now passes complete fixture readiness. The
first fixed run then exposed a recovery-probe assumption: its time-only 1,024-row
query still opens large decoded blocks across all 16 hosts and was itself rejected
by the 256-MiB tracker. That failed run is preserved at
`.omx/t17-fixed-1m/t17-large-rpc-1000000.json`; it is not counted as recovery success.
All its recorded cleanup counters were already zero.

Recovery now uses the same single-host query before and after each owning query:
`host_00`, time `[base, base+1024ms)`, returning 64 independently verified rows.
The query must succeed in preflight, then succeed exactly after each large query,
without retries, restarts or memory-limit changes. This proves the cluster can
execute a previously admitted bounded query after rejection; it does not claim
the old all-host recovery query fits the budget.

The final fixed million-row run passed in 105.67 seconds:

- All one million input rows verified with the original fixture values.
- Raw full sort, raw top-k and all three composed bindings returned
  `ResourceExhausted` from native input materialization under the unchanged budget.
- Preflight and all five recovery queries returned the exact 64-row result.
- Every recorded cleanup counter was zero after rejection and recovery.

Evidence: `.omx/t17-fixed-recovery.log` and
`.omx/t17-fixed-recovery/t17-large-rpc-1000000.json`.
This closes the readiness correctness blocker and establishes bounded rejection
and recovery at 1m. It supplies no successful large-input throughput measurement.

## Verification

- Full `banyand/measure` suite: pass (73.326 seconds).
- Focused merge/ownership race tests: pass (2.573 seconds).
- Full querymetrics querybench suite after recovery changes: pass (25.812 seconds).
- Scoped querymetrics lint for measure/querybench: zero issues.
- `git diff --check`: pass.

Logs: `.omx/t17-merge-fix-measure.log`, `.omx/t17-merge-fix-race.log`,
`.omx/t17-merge-fix-querybench-final.log`, `.omx/t17-merge-fix-lint-final3.log`.

The same final binary also passed the 100k probe in 19.32 seconds. Readiness,
preflight and all five recovery queries passed, with zero cleanup counters.
Four owning requests were resource-rejected; composed binding 2 (the final
50k-row time range) succeeded with its exact one-row result. This differs from
the earlier run's five rejections and is recorded as current feasibility, not a
latency or throughput claim. Physical block layout affects decode admission;
the test does not require a particular query to reject if it can return exactly.
Evidence: `.omx/t17-fixed-recovery/t17-large-rpc-100000.json` and
`.omx/t17-fixed-recovery-100k.log`.

Final probe binary `.omx/t17-fixed-recovery.test` SHA-256:
`c0f611537e7fd3d1ad51d92b2cf9e223cdd04647c405267ae457c1e1dc3ee2aa`.
