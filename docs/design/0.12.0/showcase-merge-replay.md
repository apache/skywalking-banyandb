# Merge replay using captured showcase data

Status: the stream/measure whole-block copy optimization was reverted on 2026-11-23.
The separate measure timestamp-boundary fix, trace secondary-index improvement,
and general merge benchmarks remain. The comparisons below describe the preserved
experimental revisions, not the current stream/measure implementation.

The [follow-up diagnosis](#follow-up-explain-the-small-measure-gain-and-stream-slowdown)
reproduces the modest measure improvement and does not reproduce the earlier
stream slowdown. It identifies limited copy coverage, an overly conservative
history check, and existing allocation problems in ordinary merging. The
initial measurements are retained below, followed by the controlled comparison.

## Initial replay results

Median elapsed time, with all individual timings retained below:

| Merge | Base median (range), s | Revised median (range), s | Change |
| --- | ---: | ---: | ---: |
| Stream | 4.765 (4.661–5.199) | 5.038 (4.558–5.184) | +5.7% |
| Measure | 37.407 (37.234–37.779) | 35.082 (34.346–36.178) | -6.2% |
| Trace secondary index | 17.702 (17.700–17.919) | 11.180 (11.102–11.409) | -36.8% |

Median resource measurements; allocated bytes are cumulative allocation work,
not simultaneously resident memory:

| Merge | CPU seconds, base → revised | Allocated MiB, base → revised | Peak RSS MiB, base → revised | Output blocks, base → revised |
| --- | ---: | ---: | ---: | ---: |
| Stream | 5.132 → 5.039 | 3554.5 → 3476.8 | 234.2 → 244.7 | 331 → 331 |
| Measure | 42.391 → 39.778 | 56426.2 → 55207.1 | 221.3 → 210.1 | 40365 → 40326 |
| Trace secondary index | 19.290 → 11.521 | 19827.4 → 5512.1 | 325.5 → 320.6 | 1748 → 1748 |

The trace secondary-index change gives the clearest improvement: it avoids
unpacking and repacking array cells, reducing cumulative allocated bytes by
72.2%. Its output files are byte-for-byte identical across base and change in
all three pairs. Measure improves modestly. In these initial samples, stream does not demonstrate a speed
improvement: its elapsed-time median increases, its timing ranges overlap, and
its CPU median falls slightly. These measurements do not establish the cause of
that elapsed-time increase.

The whole-block shortcut covers only a small part of these captured inputs:

| Merge | Copied blocks / input blocks | Copied payload bytes | Share of input file bytes |
| --- | ---: | ---: | ---: |
| Stream | 15 / 645 | 10,280,433 | 3.8% |
| Measure | 474 / 104,974 | 67,008,828 | 6.3% |

Stream has 253 small input blocks and 342 same-source overlapping/equal
boundaries; these counts are not exclusive rejection categories. Measure has
101,279 small input blocks. Fullness alone does not permit copying: overlap,
pending partially filled output, and the other safety checks still apply.
Consequently, the large gains on fully disjoint synthetic inputs do not describe
this workload.

Metadata-only inventories of the completed outputs confirm that the shortcut
creates no additional small blocks in either captured workload:

| Merge | Small input blocks | Small output blocks, base | Small output blocks, revised |
| --- | ---: | ---: | ---: |
| Stream | 253 | 89 | 89 |
| Measure | 101,279 | 32,118 | 32,118 |

Every stream run has the same complete decoded-row multiset. Every trace-index
run matches both its original input rows and the other revision. Measure keeps
the same newest values, with the independently audited older-version retention
difference described below. All 1,021 captured source files still match their
original remote checksums after the experiment. No production source code was
changed during this replay task.

## Experiment

On 2026-11-23, download existing completed snapshots from the live GKE
`skywalking-showcase` cluster and run the real persisted-part merge locally.
Compare base commit `959a4d76c659be31815ec69438048ddaa490ba1f` with the pending
stream/measure full-block-copy and trace secondary-index encoded-array changes.
The baseline and revised binaries use identical replay and validation code.

The capture uses explicit kubectl context
`gke_skywalking-live-demo_asia-east2_skywalking-showcase`, namespace
`skywalking-showcase`, and the pods' `backup` container, which shares their data
volumes. No new snapshot or cluster mutation is needed. Stream each completed
snapshot's part directories with `kubectl exec ... -- tar -C SOURCE -cf - PARTS`.
Compare remote SHA-256 checksums before and after download with every local file.

| Input | Pod | Group / segment | Parts | File bytes | Input rows | Input blocks |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Stream | `demo-banyandb-data-warm-1` | `sw_recordsLog / seg-20261121` | 33 | 273,476,210 | 2,306,281 | 645 |
| Measure | `demo-banyandb-data-cold-0` | `sw_metricsMinute / seg-20261103` | 18 | 1,068,910,520 | 58,209,290 | 104,974 |
| Trace secondary index | `demo-banyandb-data-hot-0` | `sw_otlpTrace / seg-20261121`, `sidx/start_time` | 23 | 199,661,809 | 4,832,580 | 2,346 |

All inputs come from shard 0. Snapshot directories are respectively
`20261123060006-00000006`, `20261123060019-00000006`, and
`20261123060201-00000006`. Data-pod image revision is
`8d577edf418c55b81fb420b1c042e70f47f939a4-plugins`.
The measure files represent 15,569,567,425 logical uncompressed bytes.
Stream inverted-index files and trace payload files are excluded: these replays
exercise stream column merging and the trace secondary index, respectively.

Force one merge of all captured parts for each input. This exercises the actual
merge implementation on large persisted inputs, but does not replay the live
scheduler's part selection or concurrent workload.

## Measurement and validation

Use Go 1.26.11 on Linux amd64, local ext4 storage, `GOMAXPROCS=4`, and
`GOMEMLIMIT=3GiB`. The latter is a Go soft memory target, not a container memory
limit. The host has 32 logical CPUs and runs unrelated services, which remain
untouched. Serialize all replay processes and finish builds and downloads before
timing. Alternate base/change order over three pairs: base/change, change/base,
base/change. Stream receives three additional pairs because its initial elapsed
time increased while CPU time decreased; report all six pairs, including the
original samples. Each run starts a fresh process and writes a fresh output
directory. The logs preserve the actual order and the pause for the measure
version audit.

Read and checksum the input files before each run. These are warm local-cache
measurements; they do not establish cold-storage or cloud production latency.
Time `tsTable.mergeParts` for stream/measure and `state.Merge` for the secondary
index, including output flush/sync and metadata publication. Exclude input setup,
post-merge verification, external sorting, and report generation.

Record wall time, process CPU time, Go allocations, output bytes/blocks, and
process peak RSS sampled immediately after merging. RSS includes startup and
input opening; it is not a resettable merge-only heap measurement, and excludes
later validation memory.

Reopen every output and decode every row. Hash series IDs, timestamps or keys,
versions or element IDs, and all typed field/tag values. Normalize absent columns
and null padding while keeping present empty values distinct. Sort fixed-length
row hashes with bounded memory, preserving duplicates, and compare the complete
row multiset between revisions and repetitions. Measure may remove superseded
versions; the independent version audit below handles that difference. The trace
secondary-index harness also compares its output directly with all input rows.
Separate untimed runs collect stream/measure copied-block counters and must
produce the same decoded output as the measured runs.

## Measure version audit

The initial exact row-multiset comparison deliberately stopped on a difference:
base stores 58,080,044 rows and the revised merge stores 58,080,011. Comparing
only row counts would not establish that this difference is correct.

Both outputs contain overlapping blocks, so a scan in physical file order is
insufficient to select the newest version. Export fixed-width records containing
series ID, timestamp, version, and the already verified canonical row hash.
Externally sort those records with bounded memory, then select the greatest
version for each key independently of the production merge implementation.

Both outputs contain **58,076,680 distinct keys**, with identical newest-version
values and no conflicting payloads at an equal winning version. The ordered
winner-row SHA-256 is
`67906ce250a4e7965d9bd892e5328ce81735592926b430995ed2b5819a5a51dc`.
Base retains 3,364 strictly older rows; the revised merge retains 3,331.
There are 3,300 common older rows, 64 present only in the base output, and 31
present only in the revised output. Thus the change leaves **33 fewer older
rows overall**; it does not simply remove the same 33 records from an otherwise
identical physical layout. Neither implementation globally removes every older
version across overlapping output blocks.

Targeted decoding of 187 original input blocks confirms that all 95 differing
older-row hashes exist in the captured inputs, with no missing or altered
payloads. Later repetitions must match the audited raw row multiset for their
own revision exactly. This retains a strict check without incorrectly demanding
identical physical retention of superseded versions.

## Local evidence and reproduction

Captured files and replay outputs stay under `/mnt/d/bydb-showcase-merge`, with
access restricted by its parent directory. Raw observability data is not added
to the repository. `capture.json` and each input's `source.json` record exact
snapshot paths and part lists; `source-before.sha256` and `source-after.sha256`
record the remote checksums.

The working harness bundle is `/tmp/bydb-showcase-merge`. A durable copy is
`/mnt/d/bydb-showcase-merge/harness-and-evidence.tar.gz`, with a SHA-256 sidecar.
It includes the frozen binaries, audit tools, revised package sources, and
generated-code prerequisites; captured inputs and merged outputs remain outside
that archive in the data directory. The package folders
contain the Go test overlay source, overlay mappings, frozen baseline/revised
binaries, and build logs. `production-provenance.json` records the source hashes
and environment. The baseline worktree is `/tmp/bydb-block-copy-baseline-e15b`;
its generated protobuf/mock prerequisites are archived in
`/tmp/bydb-block-copy-benchmark/generated-go.tar.gz`.

The driver pre-reads inputs, runs one binary at a time, saves its invocation and
binary hash, and validates the output before starting the next run:

```sh
python3 /tmp/bydb-showcase-merge/replay.py stream --pairs 6
python3 /tmp/bydb-showcase-merge/replay.py measure --pairs 3 \
  --expected /tmp/bydb-showcase-merge/audited-reference.json
python3 /tmp/bydb-showcase-merge/replay.py sidx --pairs 3
python3 /tmp/bydb-showcase-merge/replay.py stream --diagnostic
python3 /tmp/bydb-showcase-merge/replay.py measure --diagnostic
```

Run directories must be new; use a fresh capture/output root for a repeated
experiment. Each output retains `report.json`, `validation.json`, the persisted
part, and decoded row hashes. Top-level run logs and invocation records preserve
failures as well as successes. The stream/measure semantic hash uses
`LC_ALL=C sort --parallel=1 -S 256M` followed by SHA-256; the trace harness uses a
128 MiB sort buffer. Neither removes duplicate rows.

The measure audit is separate from timing. Its source and build overlays are retained
in the measure harness folder; `oracle-comparison.json`,
`input-loser-validation.json`, and `loser-key-differences.json` record the
aggregate findings. Each first-pair run retains `sorted-version-oracle.json`.
The audit sorts 116-byte key/version/hash records with a 512 MiB buffer and two
sort workers per output, then streams them through a constant-memory version
selector. Builds and all audit work finish before timed runs resume.

## Raw timing samples

Diagnostics and version-audit work are excluded. Allocated bytes use MiB
(1,048,576 bytes); peak RSS is sampled through the end of merging.

```csv
run,wall_s,cpu_s,allocated_mib,allocations,rss_mib,output_bytes,output_blocks,output_rows
stream-baseline-1,4.661486,4.995313,3517.758247,7012856,251.910156,272594742,331,2306281
stream-baseline-2,4.702838,5.143552,3768.651802,7014297,235.980469,272594742,331,2306281
stream-baseline-3,5.198946,5.266693,3699.134224,7013825,227.648438,272594742,331,2306281
stream-baseline-4,4.771622,5.151195,3591.329514,7013234,249.847656,272594742,331,2306281
stream-baseline-5,4.902260,5.113993,3465.438332,7013188,232.378906,272594742,331,2306281
stream-baseline-6,4.758731,5.119823,3513.747604,7013458,221.054688,272594742,331,2306281
stream-revised-1,5.131120,5.055551,3413.792221,6626333,213.484375,272594742,331,2306281
stream-revised-2,4.944303,5.022205,3452.961456,6626428,245.738281,272594742,331,2306281
stream-revised-3,5.164689,5.113893,3511.217705,6626785,231.335938,272594742,331,2306281
stream-revised-4,4.558267,4.941057,3507.514877,6626964,243.570312,272594742,331,2306281
stream-revised-5,4.705735,4.997228,3500.721825,6626180,248.785156,272594742,331,2306281
stream-revised-6,5.184358,5.187098,3243.777367,6625548,265.656250,272594742,331,2306281
measure-baseline-1,37.234495,42.391354,56408.487000,114937476,207.589844,1071452450,40365,58080044
measure-baseline-2,37.779279,42.794415,56426.231926,114937510,221.265625,1071452450,40365,58080044
measure-baseline-3,37.406695,42.087103,56527.743286,114937702,230.945312,1071452450,40365,58080044
measure-revised-1,36.177905,40.621142,55183.567490,114925319,202.683594,1071418074,40326,58080011
measure-revised-2,34.345865,39.122710,55207.148666,114926535,210.136719,1071418074,40326,58080011
measure-revised-3,35.082361,39.778280,55244.135040,114926081,221.589844,1071418074,40326,58080011
sidx-baseline-1,17.919138,19.570838,19827.415474,71915926,326.691406,199083786,1748,4832580
sidx-baseline-2,17.699793,19.289834,19986.404167,71917791,324.589844,199083786,1748,4832580
sidx-baseline-3,17.702022,19.276246,19740.352097,71916091,325.480469,199083786,1748,4832580
sidx-revised-1,11.180473,11.521497,5491.288513,34483444,323.191406,199083786,1748,4832580
sidx-revised-2,11.102021,11.471488,5606.894440,34483486,318.750000,199083786,1748,4832580
sidx-revised-3,11.409246,11.634307,5512.091118,34483967,320.628906,199083786,1748,4832580
```

## Follow-up: explain the small measure gain and stream slowdown

The whole-block shortcut has limited coverage in these captures. It also has a
conservative history check that excludes additional measure candidates. The
original stream elapsed-time increase must be assessed separately from those
algorithmic limits: CPU fell slightly, three of six paired runs became faster,
and three became slower. The difference of medians (+5.7%) is not a stable
estimate of a regression. The mean paired difference was +0.115 seconds.

### Controlled comparison

Build four versions using temporary Go overlays, without changing production
sources:

1. Original base `959a4d76`.
2. Revised code with `tryCopyBlock` returning false before any eligibility work.
3. Revised code running all eligibility checks and lookahead, then returning
   false before copying.
4. Full revised code.

Versions 2–4 retain the strict timestamp-boundary correction. Comparing 2 and 3
isolates eligibility overhead; comparing 3 and 4 isolates copying after checks.
Comparing the original with version 2 also includes the other small supporting
changes, so it is not a pure equality-fix microbenchmark.

Use eight rounds per stream variant and four per measure variant: **48 timed
replays**. Each round rotates a seeded variant order; the second four stream
rounds reverse the rotations. Every process has `GOMAXPROCS=4`, a 3 GiB Go soft
memory target, and CPU affinity 8–11 (four distinct cores, not exclusively
reserved). Inputs are checksum-read before every process. Output is fresh and
uses the same local ext4 filesystem. Other services remain untouched.

All variants include identical low-frequency timers around actual file sync
calls. Close time includes buffered flushes and some of those syncs, so close
and sync durations must not be added. CPU profiles and eligibility counters
are collected in separate runs and excluded from the timing table.

| Variant | Stream elapsed median, s | Stream CPU median, s | Measure elapsed median, s | Measure CPU median, s |
| --- | ---: | ---: | ---: | ---: |
| Original | 4.856 | 5.203 | 38.230 | 43.125 |
| Revised, copying off | 4.837 | 5.112 | 38.591 | 43.435 |
| Revised, checks only | 4.822 | 5.221 | 38.577 | 43.179 |
| Revised, copying on | 4.749 | 5.052 | 35.772 | 40.338 |

Original → revised median changes are **-2.2% elapsed / -2.9% CPU for stream**,
and **-6.4% elapsed / -6.5% CPU for measure**. Revised stream is faster in seven
of eight paired elapsed-time comparisons and all eight CPU comparisons. Measure
is faster in all four comparisons. These are results for the captured inputs,
not a guarantee for other data or the production scheduler.

Checks-only versus copying-off has no clearly resolved CPU penalty: the mean
paired difference is +0.042 s for stream and -0.062 s for measure. Exploratory
95% paired-bootstrap intervals include zero (stream -0.083 to +0.152 s; measure
-0.407 to +0.186 s). Those small sample intervals are diagnostic evidence, not
universal performance bounds. Enabling copying after checks saves a mean
0.122 CPU seconds for stream and 2.977 CPU seconds for measure.

The earlier stream slowdown is not reproduced. Sync time across the quiet
stream runs ranges from **0.094 to 0.625 seconds**, larger than the useful saving.
For example, round 2 uses 5.089 → 4.980 CPU seconds but 0.319 → 0.438 sync
seconds. Elapsed time consequently moves from 4.767 to 4.795 seconds. A slower
elapsed result can therefore coexist with less computation. The original runs
had no sync breakdown, so their individual delays cannot be attributed
retrospectively with certainty.

A first attempted follow-up batch was stopped and excluded after detecting
99.8% aggregate host CPU utilization. An unrelated 48-worker CPU stress test
was running. Its seven completed samples and interrupted run remain preserved
as interference evidence, not algorithm results. The quiet batch starts after
that workload and all builds/diagnostics finish. It checks host occupancy before
each run and aborts on high sustained occupancy during a process. This episode
does not establish that the same stress test affected the original morning
measurements.

Every output from all four variants matches a previously semantically validated
output **byte-for-byte**: stream matches the original and revised references;
measure original matches its original reference and the other three variants
match the revised reference. Thus copying does not cause the measure row-layout
difference on this fixture. The difference persists with copying disabled,
which isolates it to the timestamp-boundary correction among the changes that
alter row selection. New replays compare complete persisted-file hashes after
timing; they do not repeat the expensive row oracle already established above.

### Measure: most rows still take the original path

The 474 copied blocks contain only **3,803 of 58,209,290 rows (0.00653%)**. Their
large nonnumeric values account for 67,008,828 payload bytes (6.29% of payload,
6.27% of all input files) and 1,531,749,166 logical bytes. Byte coverage and row
coverage are therefore very different.

Every one of the **113,281,124 numeric cell positions** belongs to the 101,279
small input blocks. None bypasses normal decoding or encoding. These counts
include null positions; they are not a count of nonnull values. No array cells
occur in this measure fixture, so the trace array-cell optimization does not
address this workload.

Normal `mergeTwoBlocks` handles **338,403,990 cumulative input rows** across
69,134 calls: 5.81 times the original row count. Accumulated pending rows are
processed repeatedly. This count is actual slice lengths, not an estimate from
cached block byte-size metadata. The timestamp equality correction adds only
54 immediate merge decisions involving 53,775 input rows; subsequent grouping
can still change the physical retention of older versions.

There is avoidable representation work in the numeric fallback:

- `banyand/measure/column.go:424` selects byte-backed cells for sequential reads.
- `column.go:474–476` builds `[][]byte` and converts each decoded integer to bytes.
- `column.go:283–305` converts those bytes back into integers before compression.

For example, a numeric column `[10, 11, 12]` becomes an integer slice, then three
byte-backed cells for merging, then an integer slice again. Repacking numeric
compression remains necessary when rows change. Those extra representations are
a separate optimization opportunity. Extending the existing typed numeric form
from query readers into merging would require proper null handling, version
selection, ownership, and encoding support; passing a different decode flag alone
is insufficient.

### Measure: the previous-output check is overly conservative

Exclusive first-stop reasons from the real merge:

| Reason | Input blocks |
| --- | ---: |
| Small | 101,279 |
| Overlaps the previously emitted range | 2,939 |
| Pending output considered partial | 116 |
| Pending overlapping output | 85 |
| Overlaps another input head | 79 |
| Overlaps its own successor | 2 |
| Copied | 474 |

The history check uses the greatest maximum timestamp written for the current
series, not merely the last block's maximum. A second diagnostic kept that guard
enabled but evaluated all later checks for its rejected candidates. **2,826** of
those 2,939 blocks passed every later check: **418,003,264 payload bytes**,
9,069,017,372 logical bytes, and 50,067 rows. None contains numeric cells.
The remaining 113 have other-head and/or own-successor overlap.

Consider already-written history `[1,100]`, a full candidate `[4,5]`, and all
remaining input heads starting after 5. The current guard rejects `[4,5]`.
Ordinary merging then loads it into an empty pending block and flushes it
unchanged on the very next iteration (or at EOF). It never revisits `[1,100]`.
This is why rejecting historical overlap is stronger than ordinary merging's
row-selection behavior.

The local equivalence argument depends on full, valid candidate metadata, no
schema conflict or TopN, and strict isolation from every other head and the own
source's successor. A future equal timestamp must still reject copying. The
history-specific unit test asserts the policy directly; it is not an end-to-end
counterexample to that equivalence. Changing the policy needs full merge tests
covering wide historical ranges, historical versions, missing columns, and
future equality. The diagnostic did **not** remove the guard, so the additional
418 MB is an opportunity count, not a measured speedup or a released fix.

### Stream: input overlap prevents most copying

| First-stop reason | Input blocks |
| --- | ---: |
| Small | 253 |
| Pending output needs merging | 181 |
| Previous emitted range | 170 |
| Own successor overlap | 24 |
| Other input head overlap | 2 |
| Copied | 15 |

Independent checks show **333 of 392 full blocks overlap their own successor**.
These overlap flags are not exclusive and must not be added to the table.
For example, blocks covering `10:00–10:10` and `10:05–10:15` need their rows
merged, even if both blocks came from one input file.

Only two otherwise isolated full blocks, containing 1,755,605 payload bytes,
are blocked solely by pending output. Relaxing small-block coalescing would gain
little on this capture. Another 16 blocks / 8,258,506 payload bytes fail only the
history check. The main stream limitation remains overlap, independently of
that conservative check.

The copied stream blocks cover 128,196 rows, 10,280,433 payload bytes (3.8%),
and 42,408,843 logical bytes (2.3%). No encoded filters occur in the capture;
filter preservation therefore adds no I/O here. Fallback reads and decodes
payloads once; successor lookahead reuses the decoded metadata. The added
eligibility checks and chunked copying are evaluated by the controlled
comparisons above.

### Profiles locate the remaining work

Separate CPU and allocation profiles use the same inputs and original/revised
binaries. Profiles are diagnostic samples, not additional timing observations.
CPU profiling includes the merge and brief measurement setup/teardown;
allocation profiles cover the process without decoded-row validation. Their
sampled allocation counts differ from exact `runtime.MemStats` counters,
including Go's treatment of tiny allocations.

In revised measure, cumulative CPU samples attribute 44.3% to ordinary block
writing, 23.0% to loading block data, and 14.4% to `mergeTwoBlocks`. The
`encodeDefault` subtree alone accounts for 37.9%. Cumulative call-tree costs
contain their callees and must not be added to their child functions.

Allocation profiles expose two concrete costs left by the shortcut:

| Revised measure allocation site | Share |
| --- | ---: |
| `fastFieldAppend` | 41.0% of allocated bytes |
| `fullFieldAppend`'s field initialization | 30.1% of allocated bytes |
| `convert.Uint64ToBytes`, reached through numeric decoding | 97.3% of sampled allocation objects |

The first two sites allocate approximately 39,482 MiB combined. This is
cumulative allocation traffic, not resident memory and not a predicted speedup.
Numeric conversion accounts for many small objects; field-list rebuilding
accounts for most bytes. Both remain on the ordinary path used by almost all
measure rows.

There is a confirmed existing buffer-reuse defect behind part of this traffic.
`column.reset` keeps the inner value-slice capacity, and `columnFamily.reset`
keeps the column array. But the first `fullFieldAppend` after reset constructs a
fresh `column` with a nil value slice and overwrites that reusable slot
(`banyand/measure/block.go:1345–1350`). The alternating pending and temporary
blocks repeat this cycle. The same code exists in the baseline.

For example, a column can retain capacity for 8,192 rows, then allocate a new
100-row buffer after reset instead of using that space; later appends grow and
copy it again. These lists hold byte-slice headers, not copies of every payload.
Not all profiled append allocation is avoidable: new columns, schema padding,
and genuine capacity growth still need storage. A buffer-reuse fix has not yet
been implemented or benchmarked here.

In revised stream, ordinary block writing accounts for 55.0% of CPU samples and
loading block data for 22.9%. Zstd encoding is prominent. `fastTagAppend` and
initial tag append together account for 39.8% of allocated bytes. With 333 full
blocks overlapping their own successor, the shortcut cannot bypass most of
that decode/merge/recompress work.

The observed stream elapsed-time reversal is therefore consistent with a small
real computational saving obscured by system wait variation. The measure
limitation is structural: a narrow whole-block shortcut, a conservative history
restriction, and expensive ordinary merging. No copied-output corruption or
large eligibility-check overhead was observed on these captures.

### Follow-up work suggested by the evidence

1. Test the history-check equivalence at the real merge seam, then consider
   removing that restriction while retaining fullness, future-overlap,
   own-successor, schema, and TopN checks. The 418 MB count makes this a concrete
   opportunity, but its resulting speedup has not been measured.
2. Improve ordinary field-list buffer reuse and numeric representation. Preserve
   null versus empty values, numeric repacking, version selection, ownership,
   cancellation, and bounded retained capacity. Measure both overlapping numeric
   inputs and disjoint large-payload inputs; these are different workloads.
3. Keep small-block coalescing. Relaxing it does not address the dominant stream
   limitation and would violate the intended output-size behavior.

### Follow-up artifacts and reproduction

Temporary harness source, overlays, frozen binaries, profile summaries, and
commands are in `/tmp/bydb-showcase-diagnosis`. The durable harness archive is
`/mnt/d/bydb-showcase-diagnosis/harness-and-evidence.tar.gz`, with a SHA-256
sidecar. The earlier capture/harness archive supplies capture references
and production source provenance; raw source parts remain under the original
capture directory and are not included in either harness archive.

Quiet outputs, invocations, all 48 timing samples, and four separate profiled
runs are under `/mnt/d/bydb-showcase-diagnosis/quiet`. Its `summary.json` retains
medians, ranges, paired differences, and exploratory bootstrap intervals;
`samples.json` retains every unprofiled observation. Each run retains complete
persisted-file hashes. `postflight.json` records 52 successful output-reference
comparisons and unchanged production sources/baseline tracked tree.

Eligibility evidence is under the original capture's `runs` directory:
`stream-eligibility-diagnostic`, `measure-eligibility-1`, and
`measure-eligibility-counterfactual-1`. The counterfactual diagnostic leaves the
history guard enabled and confirms the output files remain identical.
Pending logical-byte metadata can be stale after accumulating blocks; use the
reported row-work counts, not those raw pending-byte counters, to quantify
repeated merging.

To rebuild the frozen overlays and replay into a new output directory:

```sh
python3 /tmp/bydb-showcase-diagnosis/build.py
BYDB_DIAG_ROOT=/mnt/d/bydb-showcase-diagnosis/new-run \
  python3 /tmp/bydb-showcase-diagnosis/run.py stream --rounds 8
BYDB_DIAG_ROOT=/mnt/d/bydb-showcase-diagnosis/new-run \
  python3 /tmp/bydb-showcase-diagnosis/run.py measure --rounds 4
BYDB_DIAG_ROOT=/mnt/d/bydb-showcase-diagnosis/new-run \
  python3 /tmp/bydb-showcase-diagnosis/summarize.py
```

Keep builds, replays, profiling, and other heavy work sequential. Existing run
directories are rejected. The archived mappings use this machine's absolute
paths and require adjustment on another machine. No production implementation
changes were made during this follow-up analysis.

### Follow-up raw timing samples

Profiled and saturated-host runs are excluded. All times are seconds; allocations are cumulative MiB.

```csv
run,wall_s,cpu_s,sync_s,allocated_mib
stream-baseline-1,4.756254,5.089633,0.317605,3394.546
stream-baseline-2,4.767299,5.088981,0.319294,3381.374
stream-baseline-3,4.816750,5.167940,0.322043,3523.262
stream-baseline-4,4.752496,5.123671,0.329172,3708.657
stream-baseline-5,4.961032,5.308551,0.314967,3564.113
stream-baseline-6,4.894983,5.237466,0.314818,3358.132
stream-baseline-7,5.055327,5.341753,0.366453,3533.271
stream-baseline-8,5.035004,5.286696,0.430146,3631.607
stream-nocopy-1,4.590230,4.961781,0.231243,3184.820
stream-nocopy-2,4.883391,5.182858,0.321358,3408.534
stream-nocopy-3,4.901551,5.063917,0.471552,3438.193
stream-nocopy-4,4.727643,5.052379,0.318128,3624.498
stream-nocopy-5,4.693011,5.046355,0.309034,3393.299
stream-nocopy-6,4.790464,5.159747,0.316660,3618.100
stream-nocopy-7,4.999005,5.242224,0.317615,3425.018
stream-nocopy-8,5.331495,5.413213,0.624501,3613.457
stream-checks-1,4.586176,5.054846,0.144240,3534.186
stream-checks-2,4.537617,4.959645,0.134753,3399.913
stream-checks-3,4.890863,5.298507,0.313839,3511.972
stream-checks-4,4.762122,5.052398,0.306083,3588.673
stream-checks-5,4.827717,5.260996,0.309671,3706.381
stream-checks-6,4.852653,5.257424,0.307209,3688.880
stream-checks-7,4.835538,5.391551,0.137516,3419.532
stream-checks-8,4.817245,5.184114,0.154041,3564.710
stream-revised-1,4.677366,4.959952,0.320929,3354.893
stream-revised-2,4.795395,4.979682,0.438414,3600.788
stream-revised-3,4.505273,5.044255,0.094125,3555.740
stream-revised-4,4.745549,5.058864,0.317720,3438.480
stream-revised-5,4.794705,5.186433,0.317498,3406.891
stream-revised-6,4.752358,5.101076,0.314910,3448.887
stream-revised-7,4.646273,4.968143,0.314809,3521.750
stream-revised-8,4.777162,5.186631,0.172433,3677.305
measure-baseline-1,38.748669,43.799646,0.166671,56564.598
measure-baseline-2,37.813753,43.069154,0.057687,56433.840
measure-baseline-3,38.022577,43.095914,0.255512,56393.831
measure-baseline-4,38.437331,43.154216,0.312410,56368.502
measure-nocopy-1,38.679895,43.212192,0.401242,56475.473
measure-nocopy-2,39.013653,43.786474,0.201319,56323.658
measure-nocopy-3,38.501722,43.657122,0.072496,56485.050
measure-nocopy-4,38.298765,43.053387,0.330090,56382.888
measure-checks-1,38.492614,43.177984,0.326930,56461.935
measure-checks-2,38.934709,44.032110,0.186003,56473.482
measure-checks-3,38.661939,43.072299,0.520956,56449.686
measure-checks-4,38.293339,43.179304,0.184270,56433.028
measure-revised-1,35.680011,40.564529,0.351349,55294.163
measure-revised-2,36.065828,40.316899,0.548928,55190.837
measure-revised-3,35.489871,40.313411,0.346636,55277.563
measure-revised-4,35.863182,40.359813,0.438404,55319.578
```
