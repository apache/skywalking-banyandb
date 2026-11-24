# Reuse measure merge buffers and keep numeric columns typed

## Problem

Ordinary measure merging resets destination columns between merge steps, keeping
their row-buffer capacity. Initial field and tag appends then construct fresh
columns and overwrite those reusable slots. Repeated merging allocates and grows
new row lists even when the destination already has enough space.

Compressed numeric columns also take an unnecessary round trip: decode integers
or floats, allocate a byte slice for each cell, merge those byte-backed rows, then
convert them back into numbers for compression. The live-data diagnosis is in
[the showcase replay report](showcase-merge-replay.md).

This change improves the ordinary merge path. Whole-block copying remains
reverted. Small blocks still combine, and numeric compression runs again for the
selected output rows.

For example, after writing an 8,192-row block, the merger can empty its column
lists and fill the same storage for the next block. It no longer replaces those
lists with new ones. For a numeric column such as `[10, 20, 30]`, it decodes the
three numbers, merges the selected numbers, and compresses them again. It avoids
creating three separate byte slices merely to turn them back into numbers.

## Implementation

Reuse reset column-family and column slots when constructing merge output. Each
slot keeps its row-buffer capacity, but its old name, type, contents, and active
representation are reset before use. Missing earlier rows are padded with nulls.
Validate the full field layout before the fast append starts modifying columns,
so a later mismatch can fall back without appending leading fields twice.

Only the ordinary merge reader enables typed numeric decoding. Null-free integer
and float columns encoded with numeric compression remain in owned numeric
slices through append, version selection, block splitting, and output encoding.
The writer feeds those slices to the existing compression algorithms and keeps
the same persisted format. Ordinary sequential readers and TopN keep their
existing byte-backed behavior.

Plain/nullable numeric columns remain byte-backed. Appending a missing value or
mixing typed and byte-backed input converts the affected typed rows to bytes in
one contiguous allocation. It preserves nil, empty, and explicit null values;
it never substitutes zero for a missing number. Unsupported float compression
uses the existing plain encoding. Query readers retain their existing contract.

Typed append copies numeric values into destination-owned storage. Byte cells
retain the existing decoder ownership rules, including detaching a retained
tail before releasing the decoder. Reset and rollback clear discarded byte
references. Released merge block pointers and input iterators retain at most
twice the normal row limit per column buffer, avoiding oversized row buffers in
the shared pools.

## Verification

Regression tests cover buffer reuse through repeated reset and changing layouts,
typed-value ownership, typed/byte transitions, missing fields and tag families,
zero versus null, and full-block timestamp-boundary version selection. File-backed
tests reopen merged output through the old byte-backed reader and check values,
row limits, and small-block coalescing. Encoder tests compare persisted bytes
between typed and byte-backed input, including integer extremes and float
fallbacks. Existing TopN, query, merge, and split-value tests remain applicable.

The buffer-reuse and changed-layout regressions fail against the frozen baseline.
The file-backed fixture also passes the baseline, establishing that its expected
logical output does not depend on the new representation. Both the complete
measure package suite and its race-enabled suite pass. The final pool-retention
change additionally passes `go test -race ./banyand/measure -run '^TestReleaseMerge'`.

## Benchmark design

Freeze the working tree immediately before this change, including the retained
strict timestamp-boundary fix and excluding the reverted block-copy shortcut.
Use the same captured 18 measure parts for three variants:

1. Frozen baseline.
2. Buffer reuse with the original byte-backed merge reader.
3. Buffer reuse and typed numeric merging together.

The second variant uses a temporary build overlay, not a production runtime flag.
Rotate execution order over four rounds. Run one fresh process at a time, with
four fixed CPU cores, `GOMAXPROCS=4`, and `GOMEMLIMIT=3GiB`. Checksum-read every
input before each run, use a fresh output directory on the same filesystem, and
exclude concurrent builds, tests, and profiling from the timing experiment.

Time the real `tsTable.mergeParts`, including writing, syncing, and reopening the
output. Report elapsed and process CPU time, cumulative allocated bytes/objects,
GC activity, and output size/block count. Record file-sync time separately to
explain elapsed-time variation. Validate full output-file hashes outside timing;
if bytes differ, investigate and compare decoded logical rows before accepting
results. These warmed local replays measure the captured workload, not concurrent
production query performance.

## Captured-data results

Measured on 2026-11-24 with Go 1.26.11. The input has 18 parts,
58,209,290 rows, 104,974 blocks, and 1,068,910,520 file bytes. Each variant
has four unprofiled samples. The baseline is the frozen working tree before this
change, including the strict timestamp-boundary fix; it is not Git HEAD or the
earlier whole-block-copy experiment.

| Metric (median) | Baseline | Buffer reuse | Buffer reuse + typed numbers |
| --- | ---: | ---: | ---: |
| Merge elapsed time | 38.751 s | 33.261 s | 28.898 s |
| Process CPU time | 43.778 s | 34.252 s | 29.241 s |
| Total allocated during merge | 56,505.6 MiB | 16,507.5 MiB | 11,131.6 MiB |
| Allocated objects | 114.938 million | 114.395 million | 0.809 million |
| GC cycles | 1,199.5 | 296 | 179 |
| Total GC pause time | 145.5 ms | 28.6 ms | 16.1 ms |
| Peak process resident memory | 206.3 MiB | 213.0 MiB | 214.7 MiB |
| File sync time (included above) | 0.229 s | 0.320 s | 0.461 s |

Buffer reuse alone reduces elapsed time by 14.2% and total allocated bytes by
70.8%. Keeping numeric values typed gives a further 13.1% elapsed reduction
relative to buffer reuse alone and removes 99.3% of its allocated objects.
Together, the changes reduce elapsed time by 25.4%, CPU time by 33.2%, and
total allocated bytes by 80.3% relative to the baseline. Total allocation is
cumulative memory churn, not simultaneous memory use: peak resident memory
increases by 8.4 MiB (4.1%) in these samples.

| Round | Execution order | Baseline elapsed | Buffers elapsed | Both elapsed | Both reduction vs baseline |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | baseline, buffers, revised | 45.774 s | 37.818 s | 29.575 s | 35.4% |
| 2 | buffers, revised, baseline | 38.991 s | 33.399 s | 28.988 s | 25.7% |
| 3 | revised, baseline, buffers | 38.512 s | 33.124 s | 28.808 s | 25.2% |
| 4 | revised, buffers, baseline | 38.225 s | 32.008 s | 28.037 s | 26.7% |

The host is shared. Average host CPU activity varied from 16.3% to 42.4%, with
the first baseline and buffers runs encountering the highest activity. The
first-round timing improvement is therefore not the headline result. Rounds
2–4 still show a 25.2–26.7% elapsed reduction, and the allocation reductions
repeat in every round. File sync was slightly slower with both changes in every
round, so faster sync does not explain the improvement. This is evidence of
removed merge work on this workload; it is not a production latency guarantee.

Every output file from all 12 timings matches the frozen baseline and the
previously validated strict-boundary output byte for byte. Each output has
58,080,011 rows, 40,326 blocks, eight files, and 1,071,418,074 file bytes.
This also confirms identical compression output and small-block coalescing for
the captured data. The separate file-backed regressions cover nullable values,
changing layouts, ownership, and split boundaries that this capture may not
exercise.

### Profile confirmation

One separate baseline/revised profile pair also produces byte-identical files;
its timings are excluded from the table above. Sampled allocations attributed
to the fast/full field append paths fall from about 39,319 MiB to 10.4 MiB.
The numeric cell allocator, `pkg/convert.Uint64ToBytes`, accounts for 97.4% of
sampled baseline allocation objects and is absent from the revised allocation
profile. The whole-process allocation counters in the table are authoritative
totals; allocation profiles are sampled estimates used to locate the work.

Sampled CPU time in `runtime.memclrNoHeapPointers` falls from 4.07 s to 0.23 s,
and cumulative background GC CPU time falls from 4.45 s to 0.76 s. Block writing, which includes
the existing compression, still accounts for 18.79 s versus 19.95 s originally.
These observations support the intended mechanism: reuse storage and remove
per-cell conversions while continuing to repack the selected numeric rows.

### Reproduction and evidence

Local evidence is under `/mnt/d/bydb-measure-reuse/`. The
`harness-and-evidence.tar.gz` archive and its `.sha256` sidecar preserve the frozen
baseline/revised source, three binaries and their provenance, build overlays,
runner, input hashes, all 14 output validation records, raw timing results,
CPU/allocation profiles, profile text reports, and verification logs. Captured
input files and large merged output files remain outside the archive.

Archive SHA-256:
`eabfea3c98148f19b39ecfd03c0599af28cb3905c3971291b64ca80b7a5f669e`.

The original input manifest is
`/mnt/d/bydb-showcase-merge/measure/manifest.json`. On this workspace, the archived
runner's working copy is `/tmp/bydb-measure-reuse/run.py`; another four rounds can
use `python3 /tmp/bydb-measure-reuse/run.py --start 5 --rounds 4` to avoid
overwriting the existing samples. Run it without concurrent builds, tests, or
profiling. `summarize.py` calculates medians and within-round differences from
the validation records. The archive records the fixed 12-sample comparison above.

The inner timing harness reports `input_cache=as-is` and `digest_rows=0` because
the outer runner performs the full input checksum pre-read and complete output
file comparison. `*-invocation.json` records that preparation and host activity;
each run's `validation.json` records the file hashes and equality checks. All
eight output files match both references in all 14 runs, and input hashes remain
unchanged. Source provenance confirms that the benchmarked production code
matches the final implementation.
