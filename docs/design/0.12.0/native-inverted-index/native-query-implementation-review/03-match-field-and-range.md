# Q3: native MatchField and Range

> **Status (2026-10-04).** The bounded Q3 operations are implemented and their
> real Stream callers are covered by native integration tests. This document
> records the acceptance evidence and the remaining limits; it is no longer a
> blocked/design-only ticket.

Classification: **implemented bounded execution workpackage** (not a complete
query engine and not a pure-NOT/live-universe feature).

## Combined boundary

Q2's Stream factory activated the real `not.Execute` and `rangeOp.Execute`
callers; Q3 validates and completes those callers against the native adapter.
Both operations use the pinned native read view, native
dictionary/posting traversal, deletion masks, series scope, and timestamp
scope. They do not construct or delegate to a legacy `Query`, `Search`,
collector, writer, or plugin.

The operation boundary remains narrow:

- `MatchField` means indexed posting presence, not schema or stored-field
  presence. A zero-token field is absent; an encoded empty term is present.
- `MatchRange` evaluates bounded encoded byte/numeric terms with independent
  endpoint inclusivity.
- A NOT leaf subtracts matching IDs from the indexed-present field universe.
  Pure NOT over every live document, Boolean planner expansion, MATCH/analyzer
  expansion, prefix/wildcard, sort, aggregation, and scoring remain out of
  scope.

## Acceptance evidence

| Area | Public seam and evidence | Result |
| --- | --- | --- |
| Field presence | `TestNativeMatchFieldPresence` in `pkg/query/logical/stream/native_presence_integration_test.go` invokes `not.Execute` through `nativeadapter.Searcher`. | Passed |
| Presence semantics | Stored-only and zero-token fields are absent; empty indexed term is present; deleted and other-series postings are absent. | Passed |
| Field-aware NOT | Literal IDs are retained-minus-matched IDs, not the live-document universe. | Passed |
| Time scope | The presence test excludes the timestamp-600 row from a 50..150 query. | Passed |
| Pinned view/reopen | The test queries a pinned view across deletion, then a fresh view and a reopened durable owner. | Passed |
| Range caller | `TestNativeRangeExecuteEndpointsDeletionTimeAndPinnedView` and `TestNativeRangeExecuteCancellationAndClosedSearcher` in `pkg/query/logical/stream/native_range_integration_test.go`. | Passed in the focused Stream suite |
| Cancellation/resource bounds | `TestNativeQ3QueryResourceBoundaries` in `pkg/index/native/q3_query_resource_test.go` deterministically cancels during dictionary traversal, checks term/candidate limits, reuses the view after errors, and verifies idempotent close. | Passed |
| Native dependency guard | `TestAdapterHasNoRetiredTransitiveDependencies` checks the adapter's `go list -deps` closure for retired inverted/Bluge packages. | Passed |
| Native package regression | `go test ./pkg/index/native -count=1` and the focused race test pass. | Passed |
| Stream regression | `go test ./pkg/query/logical/stream -count=1` passes. | Passed |
| Paired operation microbenchmark | `pkg/index/nativeadapter/q3_query_benchmark_test.go`, five 100ms samples over the same copied closed corpus and public native/legacy seams. | Native MatchField 51.656µs/6,289 B/192 allocs vs oracle 216.644µs/366,727 B/446; native Range 51.601µs/7,822 B/201 vs oracle 206.261µs/388,359 B/473 |

Commands used for the bounded gates:

```text
go test ./pkg/query/logical/stream -run '^TestNativeMatchFieldPresence$' -count=1
go test -race ./pkg/query/logical/stream -run '^TestNativeMatchFieldPresence$' -count=1 -timeout=5m
go test ./pkg/query/logical/stream -count=1
go test ./pkg/index/native -run '^TestNativeQ3QueryResourceBoundaries$' -count=1
go test -race ./pkg/index/native -run '^TestNativeQ3QueryResourceBoundaries$' -count=1 -timeout=5m
go test ./pkg/index/native -count=1
```

The shared-timestamp regression found that independently subtracting timestamp
postings can remove a timestamp belonging to a retained ID. The final policy
keeps timestamps as a conservative pruning candidate set while ID postings
remain exact; this avoids false negatives without inventing an ID-to-timestamp
relation that the existing posting API does not carry.

## Resource and corruption limits

Native dictionary expansion uses the required `MaxTerms` budget. The optional
`MaxCandidates` budget bounds candidate materialization when configured (zero
means no candidate cap). Canceled traversal returns the caller's context
error, and a failed query can be followed by a successful query on the same
view. Closing a view is idempotent and a closed view rejects further work.

Malformed committed bytes are covered at the nativeice seam rather than by
fabricated test spies. Existing named tests include
`TestOpenStrictDoesNotFallBackFromNewestCorruptSnapshot`,
`TestOpenMissingReferencedSegmentIsCorrupt`,
`TestReaderRejectsOversizedOrTruncatedDictionary`, and
`TestTermDocumentsRejectsMalformedPostingOffset`, plus malformed timestamp
tests under `pkg/index/internal/nativeice`.
An end-to-end malformed-file injection through the Stream caller is not claimed
by this workpackage.

## Remaining gaps

- The Q3 tests prove the native public wrapper and real callers; they do not
  add forbidden legacy spies or claim a general query-engine dependency guard.
  The production path is source-audited to use `nativeadapter.Searcher`.
- Pure live-universe NOT, general Boolean composition, prefix/wildcard,
  ordering, aggregation, and scoring remain outside this Q3 acceptance. Q2's
  MATCH/analyzer compatibility is not expanded or re-certified by Q3.
- The paired results are operation-level evidence on a small five-document
  fixture, not a production throughput or 1.2M-document gate. The benchmark
  command is `go test ./pkg/index/nativeadapter -run '^$' -bench
  '^BenchmarkNative(MatchField|Range)/(native|oracle)$' -benchmem -count=5`.
  The six-operation controlled rerun (including StoredFields, SeriesIterator,
  MatchTerms, and PartSeriesMap) is archived at
  `verification/benchmarks/results/docker/20261004T2115Z/`.
- Full repository pre-push and unrelated benchmark artifacts remain outside
  this bounded acceptance update.
