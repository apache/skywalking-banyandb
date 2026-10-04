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

# Extended benchmark matrix (authoritative results)

Run date: 2026-10-03 UTC. This is a scratch-only `index.SeriesStore`
contract benchmark of the legacy and experimental native backends, not a
production-service benchmark. No production source was edited, committed, or
pushed. All benchmark-owned containers were removed before completion.

## Provenance and controls

- Source: `<source-worktree>`, base
  `94160e78199e4f95a71a957663d686b4fe02bf9d`. The tracked diff plus untracked
  native tests was compared to the requested final combined source hash
  `6ca69f12895e5ac97861891cb9df54b4c337b2dbe2ee06f56aff73a2ecd8bce3`.
  Current tracked/untracked source-freeze evidence is
  `<external-scratch>/extended-20261003T060000Z/source-current-freeze.txt`,
  SHA-256 `29dc7f084f20a55c2fe5e8e7bd0abd5128c52f2053d3234f42dbfa49805aa242`. Frozen audit notes: `<external-scratch>/extended-audit.md`.
- Harness: `<external-scratch>/extended-20261003T060000Z/harness/main.go`,
  SHA-256 `c115283a6b46d047188ca347126dc64c99eef09524a382e75a92b5f9dcf717da`.
  Binary SHA-256:
  `3bcd25619c9e80f8fc4283668dc18ab63150d62bc7f36d47da1d8f6618f707d6`.
- Runtime controls for every measurement: Docker `--cpus=4
  --cpuset-cpus=0-3 --memory=8g --memory-swap=8g --pids-limit=256
  --network=none`, `GOMAXPROCS=4`, fresh per-case data roots, and sequential
  heavy containers.
- Raw JSON, metadata, peaks, durable case directories, and exact runner are in
  `<external-scratch>/extended-20261003T060000Z/`.

## Authoritative P0 results

### Query timing invalidation

The query timing comparison from the mixed-write cases is withdrawn. The
series-store path used `NewAllMatches` with its default empty score option, so
similarity scoring was implicitly enabled. Those p50/p95/p99 rows were not a
no-score native membership workload and are not retained as a performance
baseline. Raw JSON and profiles remain untouched in the external scratch
archive. Build, deduplication, growth, scan, and correctness observations
below are retained where they do not depend on query scoring.

### Cold reopen/reinsert, N=1.2M

The store cache was enabled (`CacheMaxBytes=64 MiB`); this is not an OS page
cache flush claim. Each mode seeded 1,200,000 canonical IDs, closed, reopened,
reinserted existing IDs without waiting for a callback (physical no-op callbacks
may be omitted), closed again, and reopened for an independent exact/full-scan
oracle.

| mode | seed | reinsert | full scan | checksum | result |
|---|---:|---:|---:|---|---|
| legacy | 121.88 s | 10.78 s | 1,200,000 | `ec8f51b1ef121606` | pass |
| native | 118.37 s | 10.98 s | 1,200,000 | `ec8f51b1ef121606` | pass |

Artifacts: `p0cold12-{legacy,native}-cold-1200000-1.json`.

### Concurrent-launch mixed writes/queries, N=1.2M + 10K

The mixed query timing rows were invalidated above. The post-close/reopen
correctness oracle (new-ID exact hit and Stats count 1,210,000) remains useful
as a correctness observation, but its query timings are not a benchmark
baseline. Artifacts: `p0mix12c-{legacy,native}-mixed-1200000-{1,4,16}.json`.

### Sustained 1.2M -> 2.4M capacity pilot

One paired repetition used canonical documents, close as the durability barrier,
and a fresh reopen/full-scan oracle.

- **Legacy passed:** counts 1,200,000 then 2,400,000; reopen count 2,400,000;
  checksum `450ac1eeb1110cc2`; final disk bytes 207,661,370.
- **Native failed at capacity:** second-stage Stats stopped at 1,353,600 and
  returned `error computing doc numbers: nativeice: segment "memory" requested
  an oversized read: nativeice: corrupt index`; reopen scan returned count 0
  with the same corruption. This is the main blocking capacity result, not a
  pass claim.

Forensics identify `sidx/3ac4.seg` `_id` FST size 17,631,190 bytes (16.81 MiB),
within the configured 64 MiB dictionary limit but above the 16 MiB cached
`readBytes` helper limit. The call is
`pkg/index/inverted/internal/nativeice/nativeice.go:1457`, reached by dictionary
loading at `selection.go:264`, while computing doc numbers/Stats. No production
fix was attempted. Artifacts: `p0fullgrowth2-{legacy,native}-growth-1200000-1.json`.

## Authoritative P1/P2 results

### Cancellation

After ten iterator values, the corrected probe cancels and continues iterating
through the polling boundary. Both modes observed 1,000 values (990 after
cancellation) and `Close()` returned `context canceled`. This demonstrates
cancellation at the iterator polling boundary, not immediate interruption at
value ten. Artifacts: `p1cancel-{legacy,native}-scan-5000-1.json`.

### Updates/deletes/version/reopen

At N=50,000, one disjoint 5K range was updated to `Version=2` and another 5K
range deleted. After close/reopen, both modes verified deleted exact ID = 0,
updated exact ID = 1 with Version = 2, survivor = 1, and Stats count = 45,000.
Artifacts: `p1ver3-{legacy,native}-mutate-50000-1.json`.

### Bounded long values/skew/many tags

The legacy 50K fixture used 1,024-byte payloads, 90/10 common/rare skew, and 20
tag-shaped components per ID. Independent expected counts passed: common 45,000,
rare 5,000, absent 0, many-tag prefix 1,000, and full scan 50,000 with checksum
`b33e8f6df098ffb8`. Artifact: `p2fix-legacy-p2-50000-1.json`.

The native counterpart's query-window memory/exit-137 observation is withdrawn
from the native baseline because the request implicitly scored. Its raw
artifact remains untouched for provenance: `p2-native-p2-50000-1.json` and its
`.meta`. No native query-memory or OOM claim is made here.

## Explicitly unsupported or pending

- `SeriesMatcher` lists are OR in the current SeriesStore API. A true multitag
  AND benchmark is unsupported by this seam; no AND claim is made.
- Sorted scans were not added because this harness has no supported sorted-scan
  seam without inventing API behavior.
- The native 50K long-value stress result is not generalized to all tag queries;
  it is one bounded stress case and remains separate from the passing legacy
  result.
- Earlier small smoke artifacts and the earlier `p0mix12-*` insertion-before-
  query artifacts are retained for audit history only; they are superseded by
  the authoritative sections above and are not used for the final claims.
- The 1.2M -> 2.4M native capacity failure was preserved and not rerun.

All owned containers were stopped/removed (`docker ps -a --filter name=extended-`
returned no entries at completion).
