<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to You under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain a
copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.
-->
# Property native cutover verification

Date: 2026-10-04. Worktree base: `588cc6029cec7f386bd0d4e8488a7789e94c62eb`.
This report uses disposable copies only; the original rollback roots and captured
production corpus were not modified.

## Harness

Source: `/tmp/pr1390-property-harness-20261004`; module replacement points at the
current worktree. Build command:

```text
go build -o /tmp/q2-current-new-harness ./main.go
```

Current candidate binary SHA-256:
`0fd1b37259c86d53f0f49de46edc14ef3f4d66bc461e1b2add7e2503555aacbb`.
The pinned legacy binary used for the matrix is `/tmp/q2-base588cc602-harness`,
SHA-256 `da0733a009ced961c0ca5e32226346e0fa3042477fdb1f9157f95696f17ae1fa`.
The prior current native RED binary (`/tmp/q2-current-harness`, SHA-256
`71ea456a3a302cfec80419bac9bc1b34d3450b8b54b395c3b11cf70fb16b319f`) is not
labeled as legacy.

## Fresh-copy results

Each source was copied to a new temporary root and run with the candidate native
harness. Property rollback copy `.../q2-rollback-property-copy-1791156479`
passed (exit 0): filter 1, ordered 771, non-decreasing true, repeated values 3;
close/reopen and final update/delete/reopen were successful. Schema copy
`.../q2-rollback-schema-copy-1791156479` passed (exit 0): filter 1, ordered
8782, non-decreasing true, repeated values 3; close/reopen and final mutation
also succeeded. The schema count is not compared to the historical 8,933/9,061
figures: this harness performs expiry/tombstone compaction, so those figures are
not an invariant without a stable pre-compaction manifest.

## Frozen-binary matrix

On fresh property copy `/tmp/q2-matrix-1791156551`, the sequence was candidate
native, pinned legacy, then candidate native, each with the harness's writes and
reopen. All phases exited 0 and preserved ordering/filter checks, but visible
counts changed 771 → 772 → 773 (and deleted counts 2 → 3 → 4). Because the
harness opens with expiry enabled and does not emit an active-ID manifest, this
matrix does **not** prove cross-engine row preservation; it is recorded as
inconclusive rather than PASS. No arbitrary count change is accepted as expiry
without an explicit tombstone manifest.

## Source preservation

Recursive path/content manifests computed after all runs were unchanged for the
original rollback roots:

- `/tmp/q2-rollback-property`: `f9aed0945b00047922eb18ac2d4328f7f5ee6fa924a4a2e791941a949652baa2`
- `/tmp/q2-rollback-schema`: `8c37aa1c9adc2b18d5d402dfd10623fcf2e5420c9f1464584af1a43d5fe96940`

The original immutable captures under
`/mnt/d/graphloom-runs/banyandb-14075/pr1383-real-data-20261002` were never used
as writable paths.

Raw phase logs remain outside the repository at `/tmp/q2-native-property.log`,
`/tmp/q2-native-schema.log`, and `/tmp/q2-matrix-1791156551.*.json`; they are
not copied into this report to avoid retaining corpus data.

## Deterministic engine-handoff inventory (long expiry)

A temporary public-API inventory helper was compiled from the same helper source
against both engines. Candidate helper SHA-256 is
`6dfa83b31c73658d9e4e3132714616930ec10b31a1f2f4c0833f584fb25b4ab7`; the
helper compiled against the pinned legacy source is
`8f25f7e6064a867a57a0870326e92b8d09e33cec7121bc860ce083d8fdbcc01b`.
It sets expiry to 100 years and emits only sorted hashes of ID/source plus
(timestamp, deletion time).

On fresh copies of both rollback roots, legacy and candidate-native inventories
were byte-identical:

- Property inventory: `ba9d550a7774356c21c2e4f35e0c91210c0260a950a69348ae14e46363007623`
- Schema inventory: `f4dcb21af37e079a50e589c0021e4c1a4419ce2d3c301492d256df958d9db0ff`

The deterministic handoff then ran candidate-native mutation, legacy inventory,
legacy mutation, candidate-native inventory. Both endpoint inventories were
byte-identical: `36914eba5caa4a77f8a4798b8a93097b55a4f4554f6ee1bef41b9a1ddbf51220`.
This verifies cross-engine read/write/reopen identity for the controlled +2
insert, same-ID update, and one delete (with long expiry), without embedding
corpus data. The earlier short-expiry harness count changes are therefore
expiry/compaction-sensitive observations, not an accepted compatibility gate.

## Corrected deterministic delta run

The helper mutation is intentionally phase-specific (`native-probe` then
`legacy-probe`). It inserts two IDs at revisions 9000/9001, updates the first
while retaining revision 9000 (value 99), and deletes the second by its exact
fixed ID. Commands were:

```text
q2-inventory-base <copy> sw_property inventory legacy
q2-inventory-current <copy> sw_property mutate
q2-inventory-base <copy> sw_property inventory legacy
q2-inventory-base <copy> sw_property mutate legacy
q2-inventory-current <copy> sw_property inventory
```

On `/tmp/q2-det-1791156813`, rows/deleted rows were `771/2` baseline,
`773/3` after the native phase, and `775/4` after the legacy phase: exactly
+2 rows and +1 tombstone per phase. Canonical inventory hashes were
`ba9d550a...`, `dc8d5022...`, and `93f8a2eb...`, respectively. The updated
`native-probe-0`/`legacy-probe-0` source carries the controlled value 99.
The helper source was rebuilt against both current and pinned `588cc602` trees;
its current/base binary hashes are `5d19229c...` and `8d701923...`.
