<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to you under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain a
copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
-->

<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership.
-->

# Dictionary-fix verification

Date: 2026-10-03 UTC. This is a scratch-only verification of the two native
reader fixes; this verification agent did not edit, commit, or push the candidate
production checkout (the implementer's candidate changes remain uncommitted).

## Frozen source and controls

- Source: `<source-worktree>`, base revision
  `94160e78199e4f95a71a957663d686b4fe02bf9d` (uncommitted candidate).
- Final source manifest, including untracked files:
  `dictionary-fix-verification-20261003T080000Z/source-manifest-final.sha256`
  (manifest digest `f69e9bbe53f945dc510f6a3717097f1ce60c0a41111041f2dc580772be7f6db6`).
  The final production-file hashes include `nativeice.go`
  `da6f7d2757ad9692ea260efb63a3772e8da45c2d15b7e3ed6e46c8faac52637f`,
  `selection.go`
  `95a4f4f408a1d972c4701243d20242c101c4432cc7fbf02300a8d0988a23b8da`, and
  `native_plugin.go`
  `c9bb3026e961fef76444c60376685f3dfec5b1511ef368a405023295547c2a67`.
  The final untracked regression-test hash is recorded in the manifest; later
  test-only edits do not change the production binary.
- Binary: `470d9a990e315dc8b6a73a196ccff854dba937dd9b41e45fedbeac3734d0f996`.
  The last source update was a Close-documentation comment only; rebuilding
  produced the same binary hash, so the worker-4/16 measurements remain
  attributable to the final behavior.
- Each container used: `--cpus=4 --cpuset-cpus=0-3 --memory=8g
  --memory-swap=8g --pids-limit=256 --network=none`, `GOMAXPROCS=4`, one
  container at a time, fresh data directory, and the scratch image
  `banyandb-series-perf-scratch:latest`.

## Authoritative final rows

| Case | Mode | Result |
|---|---|---|
| 10K seed + 100 duplicate passes | native | PASS: visible count 10,000; 100 seed callbacks, 0 callback errors; 0 duplicate callback errors; exit 0. Physical bytes/files changed after merge (`physical_noop=false`), therefore this is a logical idempotency result, not a physical-no-op claim. |
| 50K long-value/skew fixture | native | PASS: common prefix 45,000; rare prefix 5,000; absent 0; many-tag prefix 1,000; full scan 50,000; checksum `b33e8f6df098ffb8`. This fixture has one roughly 1.3-KB long ID, not 20 independent tags. Exit 0. Query-window peak memory and OOM counters are omitted from the native baseline because the request implicitly scored. |
| 1.2M -> 2.4M growth, close/reopen | native | PASS: Stats 1,200,000 then 2,400,000; reopened full scan 2,400,000; checksum `450ac1eeb1110cc2`; exit 0. `memory.peak=5,239,091,200` bytes; `memory.events` all zero (`oom=0`, `oom_kill=0`). |

The retained legacy growth control also passed 2.4M/count/checksum in the same
scratch matrix (checksum `450ac1eeb1110cc2`). A bounded legacy rollback read of
the final native-produced 2.4M fixture also passed: full scan 2,400,000 with the
same checksum, exit 0, `OOMKilled=false`. The native result above is the final post-fix rerun; the
first native growth attempt before the final selection update is superseded.

## Query timing invalidation

The exact-query latency table formerly in this report is **invalidated and not
design-compliant**. The series-store requests used `NewAllMatches` with the
default empty score option, implicitly enabling similarity scoring. Its timing
rows and native/legacy performance claims are removed; the retained fixture
counts, checksums, memory events, and growth correctness rows above are not
query-performance claims. Raw JSON, profiles, and harnesses remain untouched
under `<external-scratch>/dictionary-fix-verification-20261003T080000Z/`
for historical provenance only.

## Compatibility and test gates

Historical/compatibility/rollback-focused package tests passed, as did the new
nativeice dictionary regression tests (including the 17.6-MB dictionary seam,
cached concurrent lookup, malformed posting offset, and read-limit tests).
Implementer's full package/race/lint report is retained separately. No
containers remain after the run. Native historical corpus readability and
legacy rollback compatibility are represented by the existing checked-in
historical-corpus and real-data rollback artifacts; this pass did not mutate
those fixtures.

Raw JSON, metadata, memory events, source manifests, and harness are retained
under `<external-scratch>/dictionary-fix-verification-20261003T080000Z/`.
