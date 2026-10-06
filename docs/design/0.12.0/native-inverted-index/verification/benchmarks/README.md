<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to you under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain a
copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Native series-index benchmark plan

> This is a design and reproducibility plan. It records the production seam,
> two performance-sensitive series workloads, controls, correctness gates, and
> the reporting contract. It contains no benchmark result, does not execute a
> benchmark, and does not authorize a production-code change.

**Status:** proposed plan only. **Scope:** series-index writes and reads.
The old Property-document benchmark is intentionally not the primary study.
Offline SQL, unrelated field aggregation, codec-only microbenchmarks, and
synthetic stand-ins that bypass the series path are out of scope.

## 1. Question and comparison boundary

Compare the native and retained legacy inverted-index implementations on the
actual series-store operations used by the storage service (the real Luna
operation path), with the same input bytes, lifecycle, durability, cache cap,
and query contract. The headline questions are:

1. Can a repeated set of 10,000 identical series be rejected with near-zero
   additional backend work while preserving the production callback contract?
2. How do query and full-scan costs behave once the index contains 1,200,000
   unique series?

CPU work for lookup, locking, hashing, encoding, and deduplication is real work
and must be measured; “near zero” means near-zero *additional backend work*,
not zero duplicate-attempt latency. Compare native/legacy ratios rather than
inventing an absolute latency-adoption threshold.

The comparison is through the existing `index.SeriesStore` interface and its
real service owner/lifecycle. Do not call a private native plugin, bypass the
writer lock, or substitute a map for the index. The future harness may use a
small test-only adapter at this seam; it must not assert that the current
production series path is already native.

### Current source state and ownership seam

Source-backed anchors in this worktree are:

- `banyand/internal/storage/index.go:newSeriesIndex` creates the per-segment
  series store at `<segment>/sidx`. It sets the service's
  `BatchWaitSec`, `CacheMaxBytes`, `ExternalSegmentTempDir`, and
  `EnableDeduplication: true`, then currently calls `inverted.NewStore`.
- `banyand/internal/storage/index.go:seriesIndex.Insert` delegates to
  `SeriesStore.InsertSeriesBatch`; `Search` and `SearchWithoutSeries` build
  production matchers and delegate to `SeriesStore.Search`; and
  `pkg/index/inverted/inverted_series.go:SeriesIterator` is the dictionary
  scan used for a full series scan.
- `pkg/pb/v1/series.go:Marshal` encodes the subject and tag values through the
  production tag encoder and hashes the encoded buffer into `Series.ID`.
  Fixture generation must use this path, not an ad hoc textual ID.
- `pkg/index/inverted/inverted.go` exposes `NewNativeStore` and
  `NewNativeWriterOwner`. `NewNativeStore` rejects a nil, closed, or
  mismatched owner before opening a writer; the owner proves that the caller
  holds the root lock for the directory tree.
- The native Property path in this PR is selected in
  `banyand/property/db/db.go`/`shard.go`, where the database owns the root
  lock. `newSeriesIndex` currently has no owner argument and remains on
  `NewStore`. This plan therefore names a **future backend-selection seam**
  (factory/adapter plus owner propagation), not an existing series cutover.

The benchmark implementation must preserve that ownership invariant. If the
native-series backend is unavailable, record the precise seam and stop; do not
invent a command or report native numbers. The comparator should be a
test-harness adapter/factory receiving the owner-backed store through this seam,
never a direct call to private native plugin entry points.

## 2. Workload flow

```text
canonical Series.Marshal/tag encoding
              │ (same bytes, seed, order metadata)
              ▼
   ┌────────── production SeriesStore seam ──────────┐
   │ legacy NewStore │ native owner-backed adapter    │
   └──────────────┬───────────────┬──────────────────┘
                  │               │
        durable flush/settle   durable flush/settle
                  │               │
          correctness gates + physical/backend counters
                  │               │
       duplicate insert path       1.2M query + scan path
```

Each mode gets a fresh disposable directory and process. Seed, duplicate,
query, scan, and dataset-construction phases are distinct in metadata/timers;
a comparison is invalid if either side uses a fake store, different callback
wait/cache cap, or different encoded series document.

## 3. Canonical fixture and service controls

### 3.1 Series documents

Generate canonical series objects with deterministic subject/entity/tag
cardinality and fixed bytes. Use `Series.Marshal` (including the production
`MarshalTagValues` encoding) and retain the resulting `EntityValues` bytes as
the fixture oracle. Include string, numeric, array, empty/nil, and sparse tag
values accepted by the real series API. Keep generation, protobuf construction,
hashing, and directory setup outside timed sections; the write timer starts at
the real store call, not fixture generation.

The 10K fixture has exactly 10,000 distinct canonical encoded series. The large
fixture has exactly 1,200,000 distinct canonical encoded series (2M is an
optional, separately budgeted scale point, never an implicit requirement).
Record fixture seed, canonical bytes digest, counts, tag cardinalities, and
expected match counts. Do not use random values at run time or estimated
selectivities.

### 3.2 Shared controls

- Use the same production service defaults for batch wait, durability,
  persistence callback, merge policy, analyzer, and index configuration. A
  “persisted” callback must be awaited as the real caller awaits it; no timeout
  substitutes for completion.
- Primary write runs use one worker and batch size 100. Batch size 1 is a
  secondary single-call sensitivity; 1,000 is an optional throughput point.
  Do not create a worker-count × batch-size Cartesian matrix. A fixed worker
  count greater than one may be recorded as a secondary contention point.
- Pin the process to the same host CPUs and record Go version, commit, kernel,
  architecture, cgroup, filesystem, `GOMAXPROCS`, affinity, and memory. Use
  ten paired AB/BA process repetitions where practical, randomizing mode order
  with fresh directories. Full scans may be bounded to at least three paired
  repetitions when wall time dominates; record that count, never fake work.
- Keep `CacheMaxBytes` equal and use the same prewarm procedure. Warm-cache
  runs are primary for the 10K fixture; reopened/process-cold-cache runs are
  secondary. “Cold” means process/index reopen, not a global page-cache drop.
- Run a durable flush/settle barrier after seed and after each duplicate pass
  (or an equivalent documented batch barrier). Record background merge/flush
  state and measure a settled idle control so asynchronous no-op work is not
  mistaken for duplicate work.
- Build any future harness once and exclude compiler/startup time from operation
  timers. Record machine-readable metadata and raw observations; use benchstat
  for Go benchmark output. Proposed benchmark names are labels, not commands
  that exist today: `BenchmarkSeriesIndexDuplicateInsert`,
  `BenchmarkSeriesIndexQuery`, and `BenchmarkSeriesIndexScan`.

## 4. Primary workload A — repeated identical 10K series

### 4.1 Protocol

1. Open one fresh store in the selected mode with production deduplication
   enabled and the same real-service options. Record cache cap and whether the
   cache was prewarmed.
2. Insert the canonical 10,000 distinct series once (the **seed** phase), using
   batch size 100, one worker, and the normal persisted callback. Wait for
   durable acknowledgement and flush/merge settlement. Verify 10,000 logical
   series before timing duplicates.
3. Repeat the complete 10,000-series set for 100 passes (1,000,000 duplicate
   attempts). Each pass uses the same deterministic shuffle algorithm and a
   fixed per-pass seed; order and batch boundaries are identical across modes.
   Time each pass and the whole duplicate phase separately from seed.
4. Run an optional control with 10,000 genuinely new series before the repeat
   phase, and an optional 1% new-series mix in the repeat stream. This guards
   against a dedup path dropping new IDs; it is not a combinatorial matrix.
5. Close/reopen and repeat the duplicate phase for the secondary process-cold
   case. Keep warm-cache and reopened-cache results in separate tables.

Generation, shuffle, serialization, and directory copying are not timed.
Lookup, locking, hash/ID work, analysis, dedup, admission, persistence, and
callbacks are timed as production work. The duplicate callback must complete
once per submitted batch with production success/error semantics; no “all
duplicates” special case may skip callback completion.

### 4.2 Correctness and idempotency gates

After the settled seed, warm duplicates are expected to add no logical
documents or physical output: visible count remains 10,000; accepted-doc and
new-segment counts are zero; persisted bytes, manifest generations, segments,
merges, WAL/snapshot activity, and other duplicate output are zero or explained
  by the settled idle control. Subtract idle-control deltas from duplicate
  counters; do not mask them. This is a correctness expectation, not a latency
  target.

The reopened/cold case must report what is actually guaranteed. Do not
silently hard-fail a physical-rewrite observation if the current contract only
offers best-effort deduplication; classify it as **true idempotency** (no
logical or physical duplicate output) or **best effort** (logical result is
stable but backend rewrite/output occurred), with the source limitation named.
A run that adds a second live copy, loses a series, or fails a required callback
is a correctness failure regardless of speed.

### 4.3 Measurements

Report at minimum, split into seed and duplicate phases:

- attempts/s and nanoseconds per attempt; batch acknowledgement latency;
- user/system CPU, allocations/op and allocated bytes/op, peak RSS and Go heap;
- dedup/cache hits and misses, lookup counts, lock/contention counters when
  available, and ID/hash/analysis counters when available;
- accepted documents, rejected/deduplicated documents, writer new calls,
  persisted bytes, manifest generations, segment creations, and merges;
- visible/logical count, physical count, segment/file count, and post-settle
  directory bytes; never use physical count as a proxy for logical count;
- callback completion/error counts and settled-idle-control deltas.

The primary headline table contains duplicate attempts/s, allocations, and
backend-work counters for native versus legacy. Include seed costs separately;
do not hide expensive initial indexing behind the duplicate rate.

### 4.4 Source limitation to call out

`InsertSeriesBatch` constructs documents with `InsertIfAbsent`. The writer's
existing-ID path consults `_id` dictionaries, field names, and its bounded
cache. `StoreOpts.EnableDeduplication` also configures external-segment
deduplication, which is best-effort against a snapshot and can admit a
concurrent duplicate. Logical rejection, physical no-op output, and concurrent
idempotency are therefore separate assertions; “dedup enabled” alone proves
none of them.

## 5. Primary workload B — 1.2M-series query and scan

### 5.1 Dataset and query plans

Build and durably settle exactly 1,200,000 unique canonical series with the
same encoded bytes in both modes. Dataset construction cost is a separate
measurement and is not mixed into query latency. Open a serialized manifest
snapshot for reads and record each backend's natural physical segment count,
bytes, and compaction state; native and legacy physical layouts need not match
and are not equality gates.

Use deterministic query samples with expected counts computed from the
fixture oracle, not estimates. Include:

- exact encoded entity/series-ID hit and miss;
- supported multi-tag AND/entity matcher;
- numeric range only if the actual series index/query contract supports it;
- prefix/tag-filtered scan only if exposed by the actual API;
- sorted query/top-N only when the real `SeriesSort` path supports it, with a
  meaningful limit and explicit tie handling;
- full `SeriesIterator` scan consuming every ID and updating a checksum;
- ID-only projection versus needed stored tags/fields when the interface
  exposes both, otherwise record “not supported” rather than adding a fake API.

Cover exact expected selectivities of 0, 1, approximately 0.1%, 1%, and 10%.
For each selectivity, specify the sampled query IDs and expected result count
in metadata. Do not compare a native exact-only reader against a legacy full
query engine; both modes must execute the same `SeriesStore` operation.

### 5.2 Read protocol and measurements

Run warm-cache queries after one documented prewarm, then a separate
process-cold/reopen sequence. Capture time to first result/all results,
p50/p95/p99 latency, QPS, and result-count correctness. Use fixed AB/BA
samples and identical Go/runtime settings. Full scans must consume EOF; report
series/s, nanoseconds/series, wall time, CPU, RSS, allocations, and checksum.
A lazy iterator that is not consumed is not a scan result.

The query headline table contains p99 and full-scan throughput/RSS. Include
query-cache hit/miss and distribution controls; do not let an unequal query
cache hide backend differences. High-selectivity and full-scan points may have
fewer repetitions for a bounded run, but the exact sample count and resulting
uncertainty must be reported. Startup/reopen time is a separate result from
steady query time.

## 6. Correctness, persistence, and physical observability

Correctness is a prerequisite for performance results. For both workloads and
both modes, verify:

- every canonical encoded series round-trips to the expected subject/tags and
  hash-derived ID;
- seed and large-fixture logical counts (10K and 1.2M) and exact hit/miss/AND/
  range/prefix/sort expected results;
- duplicate phase does not lose or multiply live IDs and all callbacks complete;
- close/reopen persistence and durable acknowledgement behavior;
- full-scan checksum and consumed count, including an empty/miss query;
- visible/logical and physical counts separately, with files/segments,
  manifest generation, merge, and persisted-byte evidence.

Physical correctness is a core gate for dedup, but native and legacy segment
layout, compaction timing, and physical file counts are not equality gates in
workload B. Existing load/merge/ownership compatibility checks from the
surrounding native-index verification plan remain useful as an optional
appendix; they are not another primary workload or a reason to bypass the
series-store seam.

## 7. Report and limitations

Publish two headline tables (duplicate attempts/s + allocations + backend work;
query p99 + full-scan throughput/RSS), per-mode raw metadata, paired native /
legacy ratios, sample counts, confidence/benchstat output, and all correctness
counters. Report physical rewrites and background idle deltas explicitly. Do
not publish a native performance claim until the owner-backed series adapter,
production callback semantics, and both workload correctness gates exist.

The plan does not claim that the current PR has cut over production series to
the native backend. It does not claim zero duplicate CPU, zero compaction in
all future implementations, or strict physical-layout equality. It does not
benchmark distributed deployment, offline SQL, unrelated Property documents,
field aggregation, fault injection, or an invented backend API. No benchmark
has been executed under this plan.
