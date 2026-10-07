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

# Native series benchmark performance investigation

This is a bounded empirical diagnosis, not a fix. The independent static review is
[`code-audit.md`](code-audit.md). Runtime source was the read-only checkout
`<source-worktree>` at
`94160e78199e4f95a71a957663d686b4fe02bf9d`. The harness was copied to scratch and
instrumented there only; no production source was changed.

## Reproducible diagnostic loop

The loop is measurable but is not a deterministic failing assertion (the harness has
no threshold assertion):

```sh
# scratch binary was built from the pinned checkout; output is scratch-only
for mode in legacy native; do
  docker run --name nativeperf-seedonly-$mode-50000 \
    --cpus=4 --memory=8g --memory-swap=8g --pids-limit=256 --network=none \
    -e GOMAXPROCS=4 \
    -v <external-scratch>:/data \
    banyandb-series-perf-scratch /data/series-bench-drain \
    -data=/data/seedonly-$mode-50000 -mode=$mode -workload=duplicates \
    -seed=50000 -passes=0 -reps=1 \
    > <external-scratch>/seedonly-$mode-50000.json
done
```

The same limits were used for all bounded runs: Docker `--cpus=4
--memory=8g --memory-swap=8g --pids-limit=256 --network=none`, with
`GOMAXPROCS=4`. Raw JSON, profiles, and the scratch harness are under
`<external-scratch>/`. No 1.2M-series OOM or
10-minute reopen run was repeated.

## Phase-separated measurements

The instrumented scratch harness records seed timing/allocation, one optional
foreground duplicate pass, and `SeriesStore.Close()` timing as a durability/merge
draining barrier. It calls `runtime.GC()` before live-heap readings. Seed callbacks
wait for batch persistence callbacks, but do not guarantee that asynchronous merges
have drained; seed allocation therefore includes background work completing during
the seed window.

| N/mode | seed wall | seed cumulative allocation | live heap after GC | files observed after seed | close/drain wall | live heap after close+GC |
|---|---:|---:|---:|---:|---:|---:|
| 10k legacy | 0.652s | 223.5MB | 5.9MB | 12 | 0.35ms | 2.8MB |
| 10k native | 0.926s | **2.384GB** | 78.8MB | **71** | **230ms** | 2.8MB |
| 50k legacy | 3.621s | 824.3MB | 13.3MB | 16 | 0.55ms | 8.7MB |
| 50k native | 6.224s | **17.743GB** | **346.7MB** | **313** | **849ms** | 8.6MB |

Raw records: `seedonly-{legacy,native}-{10000,50000}.json` and
`drain-{legacy,native}-{10000,50000}.json` in the scratch directory.

The measurements support **both** problematic open-state retained expansion and
transient allocation/GC churn. Closing releases much of the open-state heap, but that
does not make the expansion harmless: native retained heap during the open seed phase
reaches 346.7MB at 50k, and the reopen profile retains 155.4MB after GC against about
5.3MB durable index bytes. No unbounded leak was established. Native durable bytes
remain close to legacy (5.30MB vs 4.93MB at 50k), while cumulative allocations differ
by 21.5x.

The observed file-count divergence (313 native vs 16 legacy at 50k; 71 vs 12 at 10k)
is a useful correlate of different writer behavior. These counts are not asserted to
be active segment counts, nor do they by themselves establish causality; the exact
merge scheduling and file lifecycle need further targeted instrumentation if a fix is
attempted.

## Open versus query path

A closed native 50k fixture was reopened with `-workload=query -large=50000
-reuse=true`. Native open took **3.282s**; full scan took 39.7ms and returned the
expected 50,000 documents with checksum `aed7ceacb31f0604`. The exact query calls
were not used as hit-performance evidence: the reused target bytes came from a
seed-mismatched fixture, so their zero returns are not an apples-to-apples hit test.

Native reopen profile artifacts:

- `prof-queryreuse2-native-50000.json`
- `prof-queryreuse2-native-50000/query-native-0/cpu-open.prof`
- `prof-queryreuse2-native-50000/query-native-0/heap-open.prof`

The CPU profile sampled 8.31s over 3.45s wall, dominated by GC (`runtime.scanobject`
26.2% flat, `gcDrain` 56.8% cumulative) and
`nativeSegmentPluginLoad` (39.4% cumulative). Post-GC heap was 155.38MB, with
`nativeSegmentPluginLoad` accounting for 97.7% cumulatively; direct contributors
included `nativePluginSegment.rebuild`, `readBytes`, and `selectedDocuments`.
This distinguishes expensive reopen/load from query iteration for this fixture; it is
not a universal claim about every query workload.

## Source correlation

Pinned source lines:

- `pkg/index/inverted/native_plugin.go:234-371` (`nativeSegmentPluginLoad`) reads the
  segment, decodes stored documents, walks every field/term via
  `Reader.TermDocuments` and `Reader.TermFrequencies`, copies terms/frequencies into
  document structures, then calls `nativePluginSegment.rebuild`.
- `native_plugin.go:165-231` (`rebuild`) creates per-document/per-field maps and copies
  stored values and postings into a second representation.
- `native_plugin.go:386-518` (`nativeSegmentMerger.WriteTo`) materializes complete
  merged generations, per-document maps/slices, and an encoded payload before writing.
- `inverted.go:399-453` registers the native segment plugin and opens the writer;
  audit evidence confirms the common fixture records route through the registered
  ICE v3 native `Load` path with the owner lock held.

The strongest shared-path diagnosis is repeated native segment construction/load and
merge materialization, producing large open-state representations and extreme
allocation/GC churn. The evidence does not isolate one exact OOM instruction or prove
that file count alone causes the defect.

## Limitations and cleanup

- A manually copied legacy reopen attempt faulted inside the legacy ICE reader; no new
  legacy reopen profile is claimed. Existing benchmark legacy results remain the
  comparison oracle.
- No fixed 1.2M-series rerun was performed, so this does not independently prove the
  exact Docker OOM threshold or allocation site at 1.2M.
- All owned containers were removed; `docker ps -a` showed no `nativeperf` containers
  after the runs. Scratch-only profiling harnesses and outputs remain for auditability.
