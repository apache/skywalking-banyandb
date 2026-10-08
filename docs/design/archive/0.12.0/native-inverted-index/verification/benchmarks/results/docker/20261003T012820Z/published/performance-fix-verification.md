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

# Native performance-fix verification

Run date: 2026-10-03 UTC. All runtime cases used Docker `--cpus=4
--cpuset-cpus=0-3 --memory=8g --memory-swap=8g --pids-limit=256
--network=none`, `GOMAXPROCS=4`, and fresh scratch data roots. The benchmark
binary was built from the candidate source snapshot; no production source was
changed by this verification.

## Candidate provenance

- Source base: `94160e78199e4f95a71a957663d686b4fe02bf9d`.
- Final combined source snapshot hash (tracked diff plus untracked tests): `6ca69f12895e5ac97861891cb9df54b4c337b2dbe2ee06f56aff73a2ecd8bce3`.
- Tracked diff hash: `4e856148c222c875017a0637fbd2f7eadeb0b7bb4841ea0912152d23bac88f61`.
- Frozen source: `<source-worktree>/` (source-hash manifest in `final-candidate-20261003T050000Z/`).
- Candidate binary SHA-256: `3307e5add66bd720f610ee7340e9e549dcc8144cd92e8690a2eb5255bb1281e1`.
- Harness: `<external-scratch>/verification-harness-20261003T020000Z/`; runner `run-verification-case.sh` in the same scratch directory.

## Small paired checks

The final candidate native fresh N=50,000 query completed with open 0.5 ms,
seed 3.609 s, exact-hit counts 1 for all three deterministic targets, exact
miss 0, full scan 50,000 checksum `605d58f5e0cf5324` in 10.3 ms, and close 0.3
ms. Fresh-process reuse and legacy same-copy checks were exercised on completed
fixtures. Duplicate N=10,000 × 100 passes
completed in both modes: visible count remained 10,000 and duplicate callbacks
were zero; `physical_noop` was false because compaction changed physical files
and bytes (therefore no physical no-op was claimed).
No comparative legacy throughput claim is made: the earlier 119.65 s legacy
measurement was not a paired run. All peak values above are cgroup bytes with
binary units stated explicitly.

The final source hash was recomputed after the race-fix build and still matches
`6ca69f12895e5ac97861891cb9df54b4c337b2dbe2ee06f56aff73a2ecd8bce3`; the build
mounted those current production files directly. No benchmark-owned Docker
containers remain running or exited.

## Full 1.2M capacity gate

The final candidate native fresh streaming query completed under the same
limits in 133 s wall time. Seed wrote 1,200,000 documents in 121.143 s; all
three exact hits returned 1, exact miss returned 0, full scan returned
1,200,000 with checksum `ec8f51b1ef121606` in 205 ms, and close took 1 ms.
Cgroup peak was `840,687,616` bytes (~802 MiB). A fresh-process reopen of
that exact fixture passed: open 62.41 ms, all hits/miss correct, full scan
checksum identical in 215 ms, close 1.36 ms.

The durable fixture was
`<external-scratch>/final-full-20261003T050100Z/data-native-query1p2m/query-native-0`.
Manifest validation found ICE v3, CRC `2f7d37f5`, 24 segment files plus one
snapshot, 24 referenced records, 1,200,000 encoded documents, zero deletion
bytes, and 55,527,306 data bytes.

## Legacy fixture native reopen

A fresh scratch copy of the committed 1.2M legacy fixture was reopened with the
final native candidate under the same Docker limits. Open took 451.36 ms, all
three exact-hit checks returned 1, exact miss returned 0, full scan returned
1,200,000 with historical checksum `ec8f51b1ef121606` in 196.17 ms, and close
took 1.26 ms. This validates native lazy read compatibility with the legacy
ICE-v3 fixture.

## Native-to-legacy rollback probe

The earlier zero result was invalid: the legacy harness opened its own empty
`query-legacy-0` directory beside the native `query-native-0` directory. It
was not a same-copy compatibility read. A corrected bounded qualification used
durably closed native output and aliased the legacy path to that exact native
directory (no data copy or nested path):

| Fixture | Native same-copy | Legacy same-copy | Manifest |
| --- | --- | --- | --- |
| N=10,000, `.../rollback-qualification-20261003T050000Z/data-native-query10k/query-native-0` | full scan 10,000, checksum `71732fddc5d2c474`; 3 hits; miss 0; open 1.81ms; close 0.21ms | full scan 10,000, same checksum; 3 hits; miss 0; open 3.71ms; close 0.35ms | ICE v3, 5 referenced records, 10,000 docs, 6 segment files + 1 snapshot |
| N=50,000, fresh copied qualification fixture | full scan 50,000, checksum `605d58f5e0cf5324`; 3 hits; miss 0 | full scan 50,000, same checksum; 3 hits; miss 0 | native and legacy read the same `query-native-0` directory |

Therefore native-to-legacy rollback reading passes for completed/published native
fixtures. **The final 1.2M fixture also passes the same rollback check:** legacy
mode opened an alias to the exact native directory
(same `/data/data-native-query1p2m/query-native-0`, no copy), with open 5.27 ms,
three hits, miss 0, full scan 1,200,000 checksum `ec8f51b1ef121606` in 839.6
ms, and close 1.01 ms. The 1.2M legacy fixture source/copy audit remains independent:
identical ICE data hashes (23 segments plus one snapshot), v3 snapshot and
manifest records totaling 1,200,000 encoded documents with zero deletion bytes;
the copied directory only had an extra zero-byte runtime `lock` file. Audit
files: `rollback-audit-20261003T040400Z/hash-parity-data.txt`,
`manifest-doccounts.txt`, and `native10k-manifest.txt` under the scratch
directory.

Raw JSON and durable scratch data remain outside the repository under
`<external-scratch>/`; this report stores
only summarized results.
