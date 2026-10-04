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

# Extended benchmark performance root causes

This is the bounded source-and-harness diagnosis for the retained capacity
defect in the
[extended benchmark report](./extended-benchmark-report.md). It is an analysis
of the current lazy-load WIP, not a production fix or a claim that any fix is
complete.

## Scope and provenance

- Source under test: `<source-worktree>`, base
  `94160e78199e4f95a71a957663d686b4fe02bf9d`, plus the current uncommitted
  native patch. The requested combined source hash is recorded as
  `6ca69f12895e5ac97861891cb9df54b4c337b2dbe2ee06f56aff73a2ecd8bce3`.
- Current-source evidence:
  `<external-scratch>/extended-20261003T060000Z/source-current-freeze.txt`
  (SHA-256 `29dc7f084f20a55c2fe5e8e7bd0abd5128c52f2053d3234f42dbfa49805aa242`).
- Diagnostic reports: `query-latency-diagnosis-20261003.md` and
  `extended-20261003T060000Z/memory-analysis.md` under
  `<external-scratch>/`.
- Measurements used Docker `--cpus=4 --cpuset-cpus=0-3 --memory=8g
  --memory-swap=8g --network=none --pids-limit=256`, with `GOMAXPROCS=4`,
  fresh data roots, and sequential heavy cases. No production source was
  changed, committed, or pushed.

## Summary of before/controlled evidence

The query timing and prefix-memory rows formerly summarized here were withdrawn
because their default-scored requests did not represent the native no-score
membership contract. Their raw artifacts remain in the external scratch archive
for provenance only. The capacity-bound evidence below is independent of query
scoring and remains retained.

## Defect 1: capacity read-bound mismatch

### Evidence and causal chain

The failing segment is `sidx/3ac4.seg`, `_id` FST payload 17,631,190 bytes.
The dictionary-selection guard allows up to 64 MiB
(`pkg/index/inverted/internal/nativeice/selection.go:36,261`), but the cached
dictionary path calls the generic helper at `selection.go:264`. That helper
rejects any request above `maxStoredChunkTableSize = 16 << 20`
(`pkg/index/inverted/internal/nativeice/nativeice.go:61,1457-1465`). The error
therefore labels a valid, in-bounds dictionary as a corrupt index while
computing Stats/doc numbers and during reopen.

### Confidence and impact

**Confirmed**, including the exact size/bound mismatch and call chain. This does
not imply byte corruption: the payload is below the dictionary's 64 MiB bound.
The impact is inability to compute document membership/statistics once a valid
dictionary crosses 16 MiB, followed by failed reopen/full-scan behavior.

### Fix direction and regression gates

Use a dictionary-local bounded read (or `readInto` after the dictionary-specific
64 MiB check) in `storedSegmentReader.dictionary`; retain the generic 16 MiB
bound for chunk-table and other callers. Do not loosen every `readBytes` call.
Add a fixture with a dictionary between 16 and 64 MiB and verify Stats,
exact lookup, close/reopen, and full scan; retain rejection tests above the
dictionary bound and at file-boundary violations.

## Query-performance sections withdrawn

The exact/mixed-query latency and wide-prefix memory sections that followed are
**invalidated and not design-compliant**. Their series-store requests used
`NewAllMatches` with the default empty score option, which implicitly enabled
similarity scoring. The associated timing tables, peak-memory tables, profiles,
and optimization claims are removed from this report and must not be used as a
native no-score baseline. Historical raw profiles and JSON remain untouched in
the external scratch archive:
`<external-scratch>/`.

A benchmark-local request wrapper now sets `SearcherOptions.Score` explicitly to
`"none"`; this cleanup did not run a replacement performance measurement or
change production code.
