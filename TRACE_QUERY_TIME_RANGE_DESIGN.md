# Exact time filtering for ordered trace queries

Status: core fix implemented and validated locally, including the part-coverage
fast path and real-matcher benchmarks.

Upstream issue: [apache/skywalking#14058](https://github.com/apache/skywalking/issues/14058).

Investigated revision: `d8ce8281`. Context: HAPI session
`ff9632fa-3b97-45cd-8ab2-183b82e776b9` (PR CI Fail).

## Confirmed bug

An ordered trace search can return traces with no span inside its requested time
window when the ordering key is duration rather than the schema timestamp.

The existing `TestIssue14058LatencyOrderIgnoresTimeRange` reproduces this with an
empty 15-minute interval inside a populated daily segment:

| Experiment | Timestamp order | Duration order |
| --- | ---: | ---: |
| Current code | 0 traces | 8 traces (incorrect) |
| Add only SIDX MinTimestamp/MaxTimestamp | 0 traces | 8 traces (still incorrect) |
| Experimental stored-tag matcher plus internal timestamp projection | 0 traces | 0 traces (test passes) |

All three investigation experiments were run locally before implementation. Build overlays supplied the missing generated
mocks and replaced the missing UI bundle embed with an existing HTML asset; no
production source or existing reproduction file was edited. The second experiment
also overlaid the proposed two timestamp bounds. Logs are
`/tmp/repro-14058-design.log` and `/tmp/repro-14058-bounds-only.log`.

The passing prototype was also overlay-only; its log is
`/tmp/repro-14058-exact-prototype.log`. It validates the ordinary duration-index
approach against this reproduction, not the full design: composite entities,
internal exclusive bounds, and the regression matrix below are not implemented or
validated by that prototype. Overlays are under `/tmp/trace-time-range-design/`.

## Root cause

1. `pkg/query/logical/trace/trace_plan_tag_filter.go:75-78` translates the time
   window to key bounds only for timestamp ordering. This distinction is correct:
   duration keys must not be compared with epoch timestamps.
2. `banyand/trace/query.go:301-310` forwards key restrictions and existing filters,
   but adds neither exact time filtering nor timestamp part bounds.
3. `banyand/internal/sidx/query.go:208` and `part_wrapper.go:206-222` use timestamp
   bounds only for whole-part overlap tests. They do not enforce a row predicate.
4. The write paths currently use entire segment bounds for SIDX part metadata
   (`write_standalone.go:439-442`, `write_liaison.go:226-229`). Part pruning alone
   therefore does not even repair the supplied empty-window reproduction.

The missing invariant is **time eligibility independent of sort-key eligibility**.

## Contract and scope

For ordered searches, a candidate row must satisfy:

```text
existing series/key restrictions AND existing row predicate AND timestamp in TimeRange
```

The same row must satisfy the row predicate and time condition. Two different
spans, one satisfying time and another satisfying the predicate, are not enough.
Time qualification must happen before the row supplies a trace's representative
sort key, before deduplication, and before pagination.

Preserve public inclusive begin/end semantics. The public API validates
millisecond precision, while storage and ordering use nanoseconds; never convert
storage bounds with UnixMilli. Internal TimeRange inclusivity flags must also be
respected, including empty ranges and overflow-safe handling of exclusive bounds.

Preserve the existing distinction between **selecting a trace through a qualifying
span** and **clipping the spans returned for that trace**. Timestamp ordering today
selects IDs and then reconstructs their traces. This fix must not silently truncate
out-of-window sibling spans. Explicit trace-ID queries bypass SIDX; retain that
lookup behavior and document it with tests rather than accidentally changing it.

## Recommended implementation

### 1. Centralize restriction construction at the trace-to-SIDX boundary

Extract a small request-building helper used by `prepareSIDXStreaming`, shared by
legacy and vectorized execution. It receives the selected index, schema timestamp
name, TimeRange, existing key bounds/filter, projection, and resolved series.

Keep key and timestamp ranges separate. Set `MinTimestamp`/`MaxTimestamp` for
part classification. Trusted conservative part bounds can prove either exclusion
or complete coverage; overlap alone cannot prove that individual rows qualify.
Legacy parts without trustworthy timestamp bounds remain eligible for exact
filtering.

Determine where the timestamp resides from the actual selected index's tag list,
not from an index name such as `timestamp` or `duration`:

| Timestamp representation | Exact restriction |
| --- | --- |
| Last index tag: the ordering key | Intersect existing key bounds with the time interval. |
| Index prefix: a series entity | Filter resolved series by its timestamp entity value. |
| Ordinary stored SIDX tag | Apply the time matcher only to partially covered or unknown-range parts. |

An empty key intersection or empty post-filter series set must return an empty
result, not an invalid SIDX request or the fallback series ID `1`.

### 2. Classify each part before loading timestamp columns

For query interval Q and trusted part timestamp envelope P:

| Relationship | Action |
| --- | --- |
| Q and P are disjoint | Skip the part. |
| Q completely contains P | Scan without the implicit row time predicate. |
| Q intersects but does not completely contain P | Apply exact row time filtering. |
| P is missing, incomplete, or not trustworthy | Apply exact row time filtering; never infer coverage. |

For inclusive bounds, full coverage means `queryMin <= partMin` and
`partMax <= queryMax`. Respect exclusive query endpoints in internal callers.
The reverse containment (part contains query) does NOT allow bypassing the filter.

Make this decision per part, not once for the whole request. One query may fully
cover interior parts and only partially cover boundary parts. Use the part
metadata available at block-cursor construction before loading tag data
(`banyand/internal/sidx/query.go:582-590`, `sidx.go:542-559`). Classify against
immutable part metadata during block-cursor construction, not per row. Caching
request variants across a part's blocks is a potential follow-up if profiling
demonstrates a benefit.

Keep the implicit time predicate and its extra projection dependency separate
from the original user predicate/projection in the internal SIDX request. Prepare
immutable base and time-filtered variants. Covered parts use the base variant;
partial/unknown parts use the time-filtered variant. Do not mutate the shared
request or projection maps from concurrent block workers. Apply this consistently
to synchronous and streaming SIDX cursor loading where the new filter is used.

On the covered path, do not add or decode the timestamp column solely for the
implicit time check. Still load it if the user projection or another predicate
requires it. Retain every user predicate, including explicit timestamp criteria:
coverage eliminates only the implicit TimeRange predicate. Preserve existing
empty-projection semantics rather than assuming an empty tag map means no I/O.

Segment-wide bounds written today are conservative: a full-segment query can use
the bypass, but a narrow query must filter rows. Tighter actual-row part bounds
could expand the fast path later without being required for this fix.

Metadata correctness is a prerequisite for both skipping and bypassing. In
particular, merging a known-range part with an unknown-range part must preserve
an unknown envelope unless a trusted encompassing segment envelope is available.
Audit `sidx/merge.go:124-143`, which currently aggregates non-nil bounds, and test
mixed legacy/new metadata rather than assuming a non-nil aggregate covers all rows.

### 3. Add a small timestamp matcher for partially covered parts

Implement a trace-owned `model.TagFilterMatcher` decorator. Look up the schema's
timestamp tag by name, check its typed value against TimeRange, then delegate to
the original matcher if present. Retain the original decoder when wrapping one;
otherwise use the trace decoder. No user criteria is a supported case.

Compose as `time AND (original predicate)`, preserving all original OR grouping.
Use exact TimeRange membership; null or wrong-type values must not accidentally
match. Missing timestamps that are actually represented as keys/entities must
take their corresponding route above, not be mistaken for null stored tags.

For the time-filtered variant, clone the SIDX projection, deduplicate its names,
and append the timestamp tag.
Do not modify the caller's projection, logical result schema, or payload projection.
SIDX loads tags and constructs matcher inputs from projection names, so merely
adding a matcher is insufficient when the timestamp was not requested.

The existing `sidx.blockCursorBuilder.processWithFilter` calls the matcher before
`appendElement` marks a payload seen (`sidx.go:370-397,473-480`). Reuse this point.
Post-response filtering is too late: an invalid row may already have suppressed
another qualifying row of the same trace or supplied the wrong ordering key.

Keep this matcher allocation-light and immutable: precompute the timestamp name
and bounds once, with no per-row schema lookup or mutation of shared requests.
Do not introduce new matcher errors without addressing SIDX's existing behavior
of logging a matcher error and dropping that block rather than propagating it.

### 4. Resolve composite-index entities correctly

`processIndexRules` removes every index-rule tag from stored SIDX tags
(`write_standalone.go:315-330`), not just the ordering key. A timestamp in the index
prefix therefore needs series filtering, not a stored-tag matcher.

There is an existing layout hazard: `pkg/query/logical/trace/schema.go:27-32`
derives entities from the first rule, rather than necessarily the selected rule.
Do not blindly index `seriesToEntity` using another rule's positions. Derive the
query's entity layout from the selected rule before building entity constraints
and resolving series, and use that same layout for timestamp extraction. Keep
layout identity explicit in the internal handoff if necessary. Test a selected
rule that is not first. Missing or inconsistent layout metadata must produce a
clear planning/request error rather than an unfiltered fallback.

For the ordinary duration index this requires no write-path or storage-format
change: the timestamp is already stored as a tag. Composite timestamp entities
are recoverable through their series metadata; no reindex is inherently required.

## Adjacent correctness finding: descending deduplication

Source inspection found a separate ordering hazard. SIDX's block builder scans
ascending physical keys and deduplicates trace payloads before descending cursor
traversal is selected (`sidx.go:379-397,504-519`, `query.go:585-590`). Multiple
qualifying rows of the same trace in one block can therefore retain the minimum
duration even for DESC.

This is not the cause of the time leak, and the time fix must not be represented
as repairing it automatically. Add an independent regression. For complete
ordering correctness, retain the direction-correct qualifying representative per
payload, while preserving ascending cursor storage and aligned data/tag arrays;
alternatively defer deduplication to the ordered merge, with measured memory and
batching costs. Treat this as a separate, independently testable change rather
than expanding the timestamp matcher into a deduplication rewrite.

## Regression plan

Use a deterministic fixture that forces mixed timestamps into the same block and
part. Wait for data visibility explicitly; do not depend on sleeps or accidental
flush boundaries. Distinct traces with durations unrelated to timestamps should
provide the basic membership oracle.

1. Original empty interval within a populated segment; timestamp and duration
   ordering both return zero.
2. Before/begin/inside/end/after timestamps in one part. Public millisecond
   endpoints, plus internal nanosecond/exclusive-boundary unit tests.
3. Default timestamp order, explicitly named timestamp index, and differently
   named duration indexes; ASC and DESC; duration key restrictions still applied.
4. No criteria, AND/OR criteria, and an existing timestamp criterion. An outside
   row must never satisfy a user OR branch without satisfying the global window.
5. Projection excludes timestamp, includes it, or is empty. Assert no mutation or
   unrequested response fields; repeated requests must behave identically.
6. Same trace with an outside high-ranked row and an inside lower-ranked row.
   Outside rows cannot suppress inside rows or supply their ordering keys.
7. Several invalid rows before valid ones, small LIMIT, nonzero OFFSET, multiple
   batches/parts/shards. Assert exact IDs and keys, not just counts.
8. Timestamp as a composite entity; selected rule not first; no surviving series.
9. Parts lacking timestamp metadata; memory, flushed/reopened, and merged parts.
10. Legacy/vectorized execution and standalone/distributed ingestion/query parity.
11. Complete-trace sibling spans and explicit trace-ID lookup compatibility.
12. Independent same-trace/multiple-inside-duration test for the DESC dedup hazard.
13. Full part coverage bypasses the implicit matcher, while partial coverage and
    unknown bounds invoke it. Test a single request mixing covered, partial, and
    disjoint parts; all rows/results must match an always-filtered reference.
14. Covered parts retain user predicates and requested timestamp values. Instrument
    matcher calls and column loading to prove no extra implicit timestamp work
    occurs on the bypass path; verify shared projections remain unchanged.
15. Known/unknown metadata merged together cannot incorrectly prove disjointness
    or full coverage. Include exclusive-boundary containment cases.

Use request-builder unit tests for restriction/projection behavior, SIDX-level
tests for same-block witness handling, and integration tests for API results.
Assert the same unpaginated membership across sort modes; assert each mode's
ordering separately. Cover error/cancellation cleanup if builder signatures change.

## Required benchmark: part scans with and without timestamp filtering

`BenchmarkSIDXPartScanTimestampFilter` is implemented in
`banyand/trace/query_time_benchmark_test.go`, where it can invoke the actual
trace-owned timestamp matcher through public SIDX APIs without an import cycle.
It benchmarks part/block loading and filtering, not only the timestamp comparison.
This benchmark is part of implementation acceptance, not an optional follow-up.

### Apples-to-apples full-coverage comparison

Use the same immutable duration-keyed part, query bounds, key range, batch size,
ordering, and response projection for each variant. Choose a query that fully
covers the part so all variants must return identical IDs, keys, and row counts:

| Variant | Implicit timestamp predicate | Extra timestamp column work |
| --- | --- | --- |
| `NoTimeFilter` | Disabled in the benchmark baseline | None unless already requested |
| `ForcedTimeFilter` | Applied to every row despite full coverage | Load/decode timestamp for filtering |
| `AutoCoverageBypass` | Production part-coverage decision bypasses it | Same as the baseline |

Keep forced filtering and filter disabling confined to the benchmark harness;
do not introduce a public configuration switch. The forced variant must exercise
the real matcher/projection path, not a synthetic equivalent predicate.

Run two projection cases: timestamp omitted, and timestamp already required by
the base request. The first measures the combined column-loading, decoding, and
predicate cost; the second holds the required columns constant and measures the
incremental filter-path cost. Also cover no existing predicate versus a fixed
existing tag predicate, verifying that bypass preserves the latter.

### Partial-coverage and mixed-part workloads

Add partial-overlap cases with deterministic 0%, 10%, 50%, and 100% matching rows.
The 0% and 100% cases can use conservative part envelopes wider than the actual
row timestamps. Compare production automatic filtering against forced filtering;
both must produce identical results. Do not present an unfiltered partial scan
as a correct competing implementation: its output cardinality and semantics differ.

Include a multi-part case with fully covered interior parts, partially covered
boundary parts, and disjoint parts. Compare selective filtering against always
filtering all overlapping parts. Add unknown-bound parts to exercise the safe
fallback. Hold candidate parts and the dataset fixed across paired variants.

### Fixture, measurement, and reporting

- Use fixed-seed timestamps independent of duration keys and unique trace IDs for
  the primary benchmark, avoiding deduplication/selectivity confounders. Exercise
  ASC and DESC, small and large multi-block parts (for example 10K and 100K rows),
  and both in-memory and flushed/reopened parts.
- Construct, flush, and publish parts before timing. Keep the snapshot stable;
  exclude ingestion, fixture generation, and background merges from measured work.
- Drain each scan completely and release results/resources within each iteration.
  Do not use LIMIT or stop after the first result for the full-scan comparison.
- Validate result equivalence and fast-path matcher/column-loading counters in
  untimed checks. Avoid a per-row atomic instrumentation cost in timed scans.
- Report `ns/op`, `B/op`, `allocs/op`, and input rows/second. Record output counts
  separately so selective workloads cannot appear faster merely by returning fewer
  rows. If reporting bytes read/decoded, distinguish logical column bytes from
  physical disk I/O; do not label warmed filesystem-cache runs as cold-storage tests.
- Run repeated samples on the same machine with fixed Go version and GOMAXPROCS.
  Use statistical comparisons, not a brittle single-run timing assertion in CI.

Suggested command after implementation and normal test-artifact setup:

```sh
GOMAXPROCS=2 go test ./banyand/trace -run '^$' \
  -bench '^BenchmarkSIDXPartScanTimestampFilter$' -benchmem -count=10
```

Include the benchmark results in the implementation report. The coverage-bypass
case should remain close to the no-filter baseline, and its savings relative to
forced filtering must be measured rather than assumed. If metadata checks or
request/projection handling cause a material regression, investigate before
acceptance. Initial measurements and the implemented workload coverage are below.

## Delivery and validation

Implement and validate the exact-filter fix before optimizing timestamp metadata.
Add focused tests in `banyand/trace`, `pkg/query/logical/trace`, and
`banyand/internal/sidx`; replace/promote the ad-hoc integration reproduction with
maintained fixtures without weakening its assertion. Exercise the existing
standalone and distributed trace suites, then focused race tests and repository
format/lint checks. Compare narrow-window duration-query allocations and latency
with and without a user predicate; timestamp-key queries should retain their
key-range fast path without an unnecessary tag decode.

No public API, on-disk encoding, or configuration switch is proposed. Add a
release note explaining corrected non-timestamp ordered search results. A
bounds-only patch, response-side filtering, or time values substituted into
duration key bounds does not satisfy acceptance.

## Implementation and validation record

The implementation adds a separate implicit time-filter request variant, exact
timestamp-tag matching, timestamp-key intersection, selected-index timestamp-entity
filtering, and conservative propagation of unknown timestamp envelopes through
merge. Empty half-open intervals are handled explicitly. User predicates and
response projections remain separate from the extra timestamp dependency.

Maintained integration cases now live in `test/cases/trace/trace.go`, shared by
standalone/distributed and legacy/vectorized suites. The empty-gap regression
fails against baseline production code and passes with the fix. A narrow-window
case selects the qualifying trace while retaining its out-of-window sibling spans.
The original user reproduction file and `sortedMIterator` remain untouched.

Validation completed:

- Full SIDX and logical trace package tests, including race runs.
- Focused trace query, vectorized, timestamp-matcher/entity, and streaming tests,
  including a race run.
- Existing standalone and distributed trace integration suites.
- New shared integration regressions in both standalone and distributed modes.
- Original `TestIssue14058LatencyOrderIgnoresTimeRange` reproduction.
- Pinned golangci-lint v1.64.8 for SIDX, trace, logical trace, and shared trace cases;
  gofumpt and `git diff --check`.
- The mixed known/unknown merge regression independently fails against the original
  merge implementation and passes with conservative merged bounds.

Green integration runs used `/tmp/trace-time-range-design/overlay.json` only for
missing generated build artifacts/UI embedding, never to replace the query fix.
Generated mock artifacts were also materialized locally for lint's parser.

### Recorded benchmark samples

Command: `GOMAXPROCS=2 go test ./banyand/trace -run '^$'
-bench '^BenchmarkSIDXPartScanTimestampFilter$' -benchmem -benchtime=100ms -count=3`.
Linux/amd64, AMD EPYC 7B13, Go 1.25.13. These are warm in-process scans, not
cold-storage measurements. Medians from three short samples, no existing user
predicate, timestamp omitted from the response projection:

| Fixture | Variant | ms/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Memory, 10K rows, ASC | No time filter | 7.975 | 7,998,063 | 40,829 |
| Memory, 10K rows, ASC | Forced exact filter | 14.114 | 13,358,037 | 130,876 |
| Memory, 10K rows, ASC | Coverage bypass | 8.226 | 8,409,034 | 40,848 |
| Flushed/reopened, 100K rows, ASC | No time filter | 88.688 | 72,964,864 | 407,153 |
| Flushed/reopened, 100K rows, ASC | Forced exact filter | 133.400 | 124,216,872 | 1,307,356 |
| Flushed/reopened, 100K rows, ASC | Coverage bypass | 86.651 | 73,258,720 | 407,182 |

The measured bypass is close to the unfiltered baseline, with substantially less
work than forced filtering. Treat these samples as an initial comparison, not a
statistical performance guarantee. Full output:
`/tmp/trace-time-range-final-benchmark.log`.

The benchmark also covers projected timestamps, an existing service predicate,
10K flushed/reopened DESC scans, and partial-overlap selectivity of 0%, 10%, 50%,
and 100%, comparing forced and automatic exact filtering with identical IDs/keys.
Mixed-part performance workloads remain an extension; unknown-envelope correctness
is covered by tests. The independent descending same-trace deduplication issue
described above is not changed by this patch.
