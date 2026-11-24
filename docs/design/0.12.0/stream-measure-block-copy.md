# Copy unchanged full blocks during stream and measure merges

Status: reverted on 2026-11-23 after the [live-data evaluation](showcase-merge-replay.md).
Stream and measure use ordinary decoded merging again. The copy implementation,
lookahead, counters, and copy-specific tests were removed. The independent
measure timestamp-boundary correctness fix and the general merge benchmarks remain.
This document and its samples retain the evaluated design and historical results;
they do not describe an active optimization.

## Problem and scope

Stream and measure merge already keep array cells encoded, but they still decode
and encode the enclosing columns when an entire input block could remain
unchanged. Numeric compression must run again when rows are combined, reordered,
or selected by version. It need not run again when the original block and its
compression boundaries are preserved.

Add a direct-copy path for full, isolated blocks. Keep the existing storage
format, compression algorithms, sorting, version selection, and TopN behavior.
This work is separate from the trace secondary-index array optimization.

Small blocks must still be decoded and combined. A faster merge must not leave
more small blocks for later queries to read.

## Eligibility

A block can be copied only when all of the following hold:

- It meets the existing fullness threshold: at least 2 MiB of logical
  uncompressed data for stream; at least 2 MiB or 8,192 rows for measure.
- A measure block has no more than 8,192 rows. Oversized input must not bypass
  the normal split path merely because it is full.
- There is no pending decoded output. The ordinary merge may reach its existing
  flush boundary first; it must not flush a partial block just to enable copying.
- Its maximum timestamp is strictly below every remaining same-series input
  head's minimum, including its own source's next block. Equality uses normal
  merging. Different series do not conflict.
- Its minimum is above the largest timestamp already emitted for that series.
  Track that maximum across all emitted blocks, since their ranges can overlap.
- It needs no conflicting tag-type rename. The first implementation also leaves
  measure TopN on the normal path.

For example, full blocks covering `[1, 3]` and `[10, 12]` may be copied separately.
A third block covering `[2, 4]` prevents copying the first. Two 800 KiB stream
blocks must be opened and combined even if their time ranges are separate.

Input metadata guarantees ordered block minima, not disjoint ranges. The own-source
successor check is necessary for `[10, 100]` followed by `[50, 60]`. One-block
metadata lookahead must also work across primary-index pages and preserve the
current block and its source until the payload has been consumed.

## Implementation

Keep the decision at the existing merge reader/writer interface. The reader
checks fullness, conflicts, and global ordering using metadata. It then either
passes the block through the existing decoded merge or copies its encoded
payloads to the writer. Metadata lookahead is owned by each source iterator.

Copy timestamp/element-ID bytes for stream, timestamp/version bytes for measure,
and each tag or field column's encoded bytes. Preserve per-block codec metadata,
including starting values, encoding types, and internal timestamp split offsets.
Copy stream filter bytes when present and retain integer min/max summaries.

Rewrite destination payload offsets, family-metadata offsets and sizes, and the
outer primary index. Reuse the normal writer's accounting for row counts, time
ranges, logical sizes, block counts, and tag types. Copying payloads does not mean
copying old file addresses or whole part metadata.

Consume payloads through the same sequential readers used by the normal path;
otherwise a later decoded block would start at the wrong offset. Stream filters
can be read by position because decoded merging does not consume filter files.
Use bounded scratch space, check cancellation while copying, and preserve the
existing cleanup and publication sequence. Inputs remain immutable.

Private reader counters record copied blocks and payload bytes, excluding
rewritten metadata. They allow regression tests to prove the shortcut ran and
that ineligible inputs stayed on the normal path.

## Correctness verification

Tests exercise actual file-backed merging, reopen the output, and compare every
row with independently constructed expected values. Cover:

1. Full disjoint blocks and immutable copied payloads.
2. Blocks below, at, and above the byte/row threshold; small blocks still combine.
3. Copy, decode, and copy transitions within one source, with multiple families
   and primary-metadata page transitions.
4. Overlap from another input, a third input, the source's next block, and pending
   decoded output. Equal boundaries preserve stream records and measure versions.
5. Missing/null values, array and numeric codecs, type conflicts, and TopN fallback.
6. Correct stream filters and min/max metadata after reopening.
7. Cancellation and read/write failure during copying: no published partial
   output, unchanged input, and successful retry.

Measure's normal full-block flush condition must also use strict separation
(`previous.max < next.min`). Its former `<=` condition could flush a duplicate
boundary timestamp before selecting the newer version. A file-backed regression
checks this independently of the copy path.

Retain existing merge, split-value, reader, and shutdown tests. Run both packages
normally and with the race detector. Check that disabling key safety conditions
causes the relevant new tests to fail, rather than merely increasing coverage.

## Benchmark design

See [the live showcase replay](showcase-merge-replay.md) for the separate
comparison using captured production files, including workloads where few
blocks qualify for copying.

Add `BenchmarkMergeParts` to both packages. The same benchmark source must compile
against the pre-change base and the implementation. Build the base in an isolated
checkout; do not compare different fixtures or use a production runtime flag to
switch algorithms.

| Scenario | Input arrangement | Purpose |
| --- | --- | --- |
| Full disjoint | Full blocks with separate ranges, multiple parts and series | Measure the benefit of copying eligible payloads |
| Small disjoint | Sub-threshold blocks with separate ranges | Verify coalescing and measure fallback overhead |
| Overlapping | Full blocks whose ranges overlap | Check the cost when copying cannot help |
| Mixed | Full, small, and overlapping blocks | Exercise transitions under a mixed workload |

Use deterministic numeric, string, and array values. Generate and persist inputs
outside the timed region. Time the actual merge, including input reads, output
writes, flushing, and syncing. Exclude output verification and removal. Reopen
outputs and verify values, metadata, output size, and block count outside the
timer; verify that inputs remain unchanged.

Compare logical rows in canonical series/time order. Persisted blocks have
ordered starting timestamps but may have overlapping ranges; simply concatenating
their decoded rows is not necessarily a globally sorted result.

Report elapsed time per merge, allocated bytes and objects, input-byte throughput,
output size, and block count. Use copy counters in targeted tests to establish
which scenarios exercise copying. CPU profiles or process CPU and peak resident
memory are supplementary measurements only when captured separately; Go's
allocated bytes are not peak memory.

Run both binaries on the same machine with `GOMAXPROCS=2` and
`GOMEMLIMIT=1536MiB`, with at least five samples per scenario. Record exact
commands, source revisions, fixture parameters, and raw samples. Compare medians
and sample spread. Investigate repeatable regressions in fallback cases; do not
hide them behind a combined average or a best-case result.

These local repeated runs primarily measure cached reads. Label them accordingly.
Cold-storage and concurrent-query measurements need separate experiments; do not
claim that the result predicts production disk latency. Copying still reads and
writes the encoded bytes. It primarily saves decoding, encoding, and allocation.

## Results

Measured on 2026-11-23 against base
`959a4d76c659be31815ec69438048ddaa490ba1f`. The base was a detached checkout
with the identical benchmark files added and no production changes. The changed
variant contains the implementation described above. Both use Go 1.26.11 on an
AMD EPYC 7B13 host (32 logical CPUs available), with `GOMAXPROCS=2` and
`GOMEMLIMIT=1536MiB`. Fixtures and output are on local ext4 under `/tmp`.

Each table row summarizes five alternating base/change samples, with three
merges per sample. Input reads are cache-warmed by fixture creation and integrity
checks. Times include the real file-backed merge, output flushing and syncing;
fixture setup, verification, cleanup and a GC before each iteration are untimed.
Other task-owned tests and builds were stopped during measurement.

[Raw samples](stream-measure-block-copy-samples.md) include all 80 main samples
and the 20 isolated fallback follow-up samples. MB denotes decimal megabytes.
Allocated bytes and allocation counts are cumulative per merge, not peak memory.
Output-byte columns below are medians; small encoded-size variation is possible.

| Package | Scenario | Base ms min/median/max | Revised ms min/median/max | Median change | Base/Revised MB allocated | Base/Revised allocations | Base/Revised throughput MB/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| stream | full_disjoint | 64.68/69.74/75.19 | 26.11/28.81/29.04 | -58.7% | 16.35/0.20 | 45425.00/615.00 | 12.22/29.57 |
| stream | small_disjoint | 26.33/27.78/28.26 | 26.14/28.01/35.57 | +0.8% | 7.05/4.70 | 6215.00/6196.00 | 3.95/3.92 |
| stream | full_overlap | 76.61/83.42/88.80 | 77.30/84.14/85.31 | +0.9% | 48.64/43.39 | 45726.00/45714.00 | 10.18/10.09 |
| stream | mixed | 61.12/65.36/73.14 | 49.43/56.10/62.16 | -14.2% | 31.95/20.74 | 35849.00/24626.00 | 10.27/11.97 |
| measure | full_disjoint | 117.87/123.10/193.52 | 33.81/36.55/43.53 | -70.3% | 57.20/0.86 | 100501.00/605.00 | 21.01/70.75 |
| measure | small_disjoint | 36.75/41.86/43.61 | 35.36/38.43/45.73 | -8.2% | 15.02/13.52 | 13366.00/13363.00 | 7.86/8.56 |
| measure | full_overlap | 170.71/182.08/198.25 | 174.53/189.68/205.29 | +4.2% | 168.48/166.61 | 101381.00/101384.00 | 14.27/13.70 |
| measure | mixed | 125.58/139.73/155.80 | 107.34/110.38/116.02 | -21.0% | 97.10/84.11 | 79310.00/54366.00 | 14.50/18.36 |

| Package | Scenario | Input rows | Input files bytes | Input blocks | Base/Revised output bytes | Base/Revised output blocks |
| --- | --- | --- | --- | --- | --- | --- |
| stream | full_disjoint | 44076 | 851915 | 12 | 585737/851215 | 12/12 |
| stream | small_disjoint | 5508 | 109809 | 12 | 90089/90089 | 3/3 |
| stream | full_overlap | 44076 | 848905 | 12 | 684705/684705 | 6/6 |
| stream | mixed | 34434 | 671497 | 12 | 459258/525630 | 9/9 |
| measure | full_disjoint | 98304 | 2585975 | 12 | 2585196/2585196 | 12/12 |
| measure | small_disjoint | 12288 | 328904 | 12 | 361819/361819 | 3/3 |
| measure | full_overlap | 98304 | 2598282 | 12 | 2842114/2842114 | 12/12 |
| measure | mixed | 76800 | 2026432 | 12 | 2022199/2022199 | 12/12 |


Full disjoint blocks took 58.7% less time for stream and 70.3% less for measure.
Mixed inputs took 14.2% and 21.0% less time respectively. Small stream and fully
overlapping stream cases were within 1% by median in this run. The main measure
overlap run was 4.2% slower; that difference did not repeat in the isolated check
below. These samples do not establish an improvement for fallback-only inputs.

The separate fallback runs used five alternating samples of five merges each:

| Case | Base median ms | Changed median ms | Change |
| --- | --- | --- | --- |
| Stream small disjoint | 28.54 | 28.26 | -1.0% |
| Measure full overlap | 177.80 | 175.21 | -1.5% |

The timing ranges overlap in these fallback checks. Buffer-pool reuse also causes
allocation-byte variation; the raw samples retain it. No process CPU or peak RSS
measurement was made, and these local results do not predict cold-storage or
concurrent-query latency.

All main runs have identical input-file hashes and matching canonical output-row
hashes across base and change. Each timed merge is followed by reopening its
output and checking every value, row count, timestamp range and logical size.
Input files remain unchanged. Both variants combine the 12 small input blocks
into three output blocks, one per series.

Stream output grows for copied blocks because the copy preserves existing Bloom
filters and integer min/max metadata that the old decoded path discards. The full
fixture grows from 585,737 to 851,215 bytes, with 12 blocks in both cases. The mixed
fixture also retains more filters, with nine blocks in both cases. All measure
output sizes and block counts match the base for these fixtures.

### Fixtures and reproduction

The benchmarks are in
[stream/merge_benchmark_test.go](../../../banyand/stream/merge_benchmark_test.go)
and [measure/merge_benchmark_test.go](../../../banyand/measure/merge_benchmark_test.go).
Each fixture contains four parts and three series, with one block per series in
each part. Stream full blocks contain 3,673 rows, reaching the 2 MiB logical-size
threshold with integer tags, 512-byte strings and escaped string arrays. Indexed
filter metadata is present. Measure full blocks contain 8,192 rows, with string
and escaped-array tags plus nullable integer, exactly representable float and
string fields. Small blocks have one-eighth as many rows. Mixed inputs contain
one isolated full block, two overlapping full blocks and a small tail per series.

Compile the same benchmark source in the base and changed source trees, after
the repository's generated-code prerequisites are available. For example, run
these commands in each tree with distinct output binary names:

```sh
GOMAXPROCS=2 go test -p=2 -c ./banyand/stream -o /tmp/stream-merge.test
GOMAXPROCS=2 go test -p=2 -c ./banyand/measure -o /tmp/measure-merge.test
GOMAXPROCS=2 GOMEMLIMIT=1536MiB /tmp/stream-merge.test \
  -test.run='^$' -test.bench='^BenchmarkMergeParts$' -test.benchtime=3x -test.count=1
GOMAXPROCS=2 GOMEMLIMIT=1536MiB /tmp/measure-merge.test \
  -test.run='^$' -test.bench='^BenchmarkMergeParts$' -test.benchtime=3x -test.count=1
```

Alternate base and changed binaries over five rounds. Isolated follow-ups select
`BenchmarkMergeParts/small_disjoint` or `BenchmarkMergeParts/full_overlap` and
use `-test.benchtime=5x`.

The local evidence bundle is `/tmp/bydb-block-copy-benchmark`: `reproduce.sh`
runs the frozen binaries; `comparison.json` and logs preserve raw metrics and
hash checks. `provenance.json` records binary/source hashes and machine details.
The frozen source-patch SHA256 is
`6777ed440246f84f104535f8173b25cce22984d43f1e3093ccafdf5c130b02a7`.
Measure's unchanged source and binary were measured before the final stream-only
flush-transition adjustment; their original provenance is retained in `v1/`.
The base checkout is `/tmp/bydb-block-copy-baseline-e15b`. Its ignored generated
protobuf/mock Go files were copied from the working tree and archived in
`generated-go.tar.gz`, with filenames in `generated-files.txt`. To reconstruct
that checkout, create a detached worktree at the base commit, extract the archive
there, copy the two identical benchmark files, and run the compile commands.

### Regression-test evidence

The evaluated implementation had file-backed tests in
`banyand/stream/merge_copy_test.go` and `banyand/measure/merge_copy_test.go`.
Those tests were removed with the shortcut. The retained boundary regression is in
[measure/merger_boundary_test.go](../../../banyand/measure/merger_boundary_test.go).
The former adjacent `merge_copy_failure_test.go` files exercised cancellation, failed
reads/writes, cleanup, and retry through the real `mergeParts` entry point.
Both full-block tests first failed with zero copied blocks before implementation,
then passed with the expected two copied blocks and unchanged encoded payloads.

Temporary Go build overlays deliberately removed safety rules, leaving the
working source unchanged. The regression tests rejected all five mutations:

| Deliberate fault | Observed failure |
| --- | --- |
| Stream accepts a block below the fullness threshold | Expected zero copied blocks, got two |
| Measure copies small blocks | Expected zero copied blocks, got two |
| Stream retains a source payload offset | Reopened output rejects offset 0 where 474 is required |
| Stream ignores its source's overlapping successor | Output block row counts differ from the expected normal-merge layout |
| Measure flushes when timestamps are equal | Boundary row has version 1 instead of the expected version 2 |

Verification completed on 2026-11-23:

```sh
GOMAXPROCS=4 go test -p=2 ./banyand/stream ./banyand/measure -count=1 -timeout=20m
GOMAXPROCS=4 go test -race -p=2 ./banyand/stream ./banyand/measure -count=1 -timeout=20m
GOMAXPROCS=4 bin/golangci-lint run --config .golangci.yml --timeout=10m ./banyand/stream ./banyand/measure
bin/license-eye header check
git diff --check
```

Both full package suites passed normally and with the race detector. Lint reported
zero issues; license checking reported zero invalid files. An earlier concurrent
measure-only run encountered a migration fixture failure (one flushed segment
instead of two). Its focused rerun passed on both base and changed code; the final
full normal and race runs also passed. No migration code was changed.
