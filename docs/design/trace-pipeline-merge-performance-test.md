# Trace Pipeline Merge Performance Test

## Status

Proposed iterative benchmark design. The benchmark is local and opt-in because it requires the downloaded real-trace
shard and creates a 24-hour merge workload on local disk. Results from each phase are used to find and fix performance
problems before the next phase begins; this is not a single final benchmark run.

The first generated fixtures and their reports are invalid for performance conclusions because their timestamp mapper
used `publication - mergeGrace + 1ns` as a fallback. Although their absolute bounds covered nearly 24 hours, 1,146 of
3,219 writes had a maximum timestamp exactly one nanosecond inside the two-hour maturity frontier, and 52 writes had a
maximum timestamp later than publication. Those reports remain discarded.

The corrected two-times fixture has now been regenerated from the frozen 29 July shard backup. It contains 74,576
traces, 325,570 rows, and 3,219 writes, and the default SkyWalking sampler deletes 35.3733% of its complete traces. All
3,219 write maxima are no later than publication and strictly inside the hot side of the two-hour frontier when first
published. None uses the old `frontier + 1ns` fallback. Write maxima span 23.985 logical hours and publications span
23.993 logical hours, so the workload now represents the intended day rather than a frontier-concentrated timestamp
artifact.

### No-Pre-Roll Experiment

The historical pipeline-disabled iteration deliberately omitted pre-roll and used two independent production-shaped write
streams in one 24-hour window. It preserves observed liaison write shapes while assigning unique IDs to every trace
instance. The resulting fixture contains 74,576 traces, 325,570 rows, and 3,219 writes. Its untimed default SkyWalking
validation deletes 35.3733% of complete traces.

The original requirement for ten mature selections per repetition is replaced before the next run because selection
count had 23.6% variation while mature row and byte work varied by only 8.4% and 8.3%. The frozen compound maturity gate
requires, in every throughput repetition:

- mature input rows of at least 30% of fixture rows;
- mature core input bytes of at least 30% of raw fixture core bytes;
- at least three mature selections;
- at least 95% of mature row work in selections containing an input with merge depth one or greater; and
- no more than 15% coefficient of variation for mature rows and mature core bytes across five fresh processes.

A mature selection whose every input has depth one or greater remains visible as a non-blocking diagnostic. It is not a
performance-readiness gate because mixed selections still execute the plugin over earlier merge outputs and represent
the production picker trajectory. Complete logical ledgers for core, `latency`, and `start_time`, exact row counts, zero
sampler calls, the Docker resource envelope, and five repetitions remain blocking gates.

The first clean run after freezing these gates remains `HOLD`. Mature selections processed 25.3%-49.4% of fixture rows
and 24.9%-49.0% of raw core bytes. One repetition fell below both 30% volume floors, while cross-run row and byte
coefficients of variation were 22.0% and 22.2%, above the 15% limit. Selection diversity and merged-output exposure
passed: every run had 6-13 mature selections and 97.1%-99.9% exposure. Only one run contained a fully precompacted mature
selection. Do not lower the frozen gates based on this result; it demonstrates that the two-times, no-pre-roll topology
does not provide repeatable mature plugin work and is not yet a valid baseline for plugin comparisons.

### Corrected Serialized Pilot Outcome

The corrected empty-shard pilot serialized each publication with its following merge drain while keeping the controller
and data node in separate processes. It reconciled the core and both secondary-index ledgers exactly, published all 3,219
parts, completed 286 production-picker merges, and returned every publication barrier with zero queued, running, or
in-flight merge work. It therefore validates the fixture boundary, logical clock, cross-process barrier, and lossless
baseline path.

The original report incorrectly treated a merge as mature only when every selected part was mature. A mature merge is a
selection containing at least one part whose maximum row timestamp is at or before `logical_now - 2h`. A selection may
therefore be both mature and hot. Reconstructing every selected input from the immutable schedule and preceding merge
events gives six mature merges, all of which were mixed with at least one hot part; all 286 merges contained a hot part.
The six mature selections processed 224,788 mature input rows across their merge rounds.

This definition is deliberately independent of whether sampling executes. The event's mature/hot input-part and row
counters describe the selected data, while its sampling classification, bypass reason, and plugin-call count describe
what the pipeline did. Under the current whole-selection grace guard, a mixed merge is mature for workload coverage but
still bypasses sampling because it also contains a hot part.

The corrected report with per-input maturity counters confirms six mature-containing selections, so the pilot is below
the unchanged ten-mature-round gate but does not establish that the empty-shard topology produces no mature work. Do not
infer pre-roll necessity from the obsolete zero count. Decide the next fixture iteration from the corrected six-round
result. The mature-round gate remains ten and is not replaced by merge-depth, row-volume, or input-depth gates.

#### Why 280 Selections Are Pure Hot

Every one of the 286 pilot selections has a newest input maximum equal to its dispatch publication clock. Usually this is
the raw part just published; in recursive picker passes it is an output created earlier under the same fixed logical
clock. Consequently, every selection contains a hot input. A selection becomes mature-containing only when the same
size-compatible picker window also includes an older part whose maximum is at or before the two-hour frontier.

The first merge occurs at `00:08:03` after 19 publications and combines 14 raw parts. The first 24 merge rounds have no
mature input available in the active inventory. A mature part first exists at the `02:20:54` dispatch, but that merge
selects 11 newer raw parts and leaves the mature part behind. Across 256 later pure-hot rounds, mature parts are available
but not selected; the median such inventory has six mature parts and 118,329 mature rows.

This behavior follows the unmodified size-based policy rather than timestamp priority. The picker sorts by compressed
size, breaking equal-size ties by newer minimum timestamp, and searches size-balanced windows of 8-15 parts. In pure-hot
rounds with mature inventory, the median mature-part size is about 3.10 MB while the median selected-part size is about
1.4 KB, a median ratio of roughly 1,765:1. The older parts are predominantly large outputs from earlier compactions, so
including them with the new small writes would violate the policy's write-amplification balance. They remain parked until
enough comparable outputs accumulate.

Only six size windows bridge into mature data, at logical times `05:44:00`, `08:42:29`, `12:21:14`, `14:51:33`,
`17:51:50`, and `21:55:11`. All six are mixed selections. The largest mature rounds are the depth-five merges at
`12:21:14` (10 mature plus 5 hot inputs) and `21:55:11` (11 mature plus 4 hot inputs); the latter contains only previous
merge outputs and no raw part. It is still hot because four of those outputs carry data newer than the frontier.

### Full-Day Adaptive-Budget Validation

The first retain-all full-day replay confirmed that the serialized production MERGE trajectory alone cannot validate
adaptive batching: all 286 selections contained at least one hot part, so the whole-selection grace guard correctly made
zero plugin calls. Advancing the logical clock by two hours did not create another size-policy selection because the
ordinary merges had already reduced the shard to 20 parts. Treating that zero-call run as plugin coverage would be a test
defect.

The harness now has an explicit, opt-in cooldown FINALIZE round for plugin-enabled integration runs. It first completes
the unchanged 24-hour serialized MERGE replay and its day-boundary drain, advances the benchmark logical clock to
`day_end + 2h`, and then invokes one real production finalization round over the cooled active snapshot. The round uses
the production fragment guard, adaptive decision planner, trace assembly, plugin ABI, core writer, secondary-index
mergers, snapshot introduction, and finalization-generation commit. It does not alter the production picker or make
FINALIZE part of the pipeline-disabled baseline. The benchmark fails rather than silently succeeding if no finalization
round commits.

The 2-core, 4-GiB AlwaysKeep validation passed. The primary phase published 3,219 parts, preserved the exact ordered 286
MERGE selections from the pipeline-disabled run, and made zero plugin calls because every selection was hot. The cooldown
FINALIZE round selected all 20 mature core parts, processed 325,570 rows and 74,576 complete traces, invoked the plugin 10
times, retained every trace, and produced one core part plus one part for each secondary index. All three logical ledgers
matched; there were no dropped or oversized traces, lossless retries, or recording errors.

For the 82,584,217-byte core selection, metadata estimated 190,169,855 staging bytes. The 4-GiB cgroup produced a
268,435,456-byte staging hard limit and the adaptive planner chose a 31,694,976-byte decision limit with six planned
batches. Actual complete-trace charging totaled 306,951,501 bytes, resulting in nine byte-limit flushes and one final
flush. Peak staged bytes were 31,697,078, only 2,102 bytes above the decision target because the boundary trace is
indivisible, and 11.8% of the hard limit. Process cgroup peak was 277,794,816 bytes, 6.5% of the container limit. The
cooldown round took 14.44 seconds wall and 14.51 CPU seconds and allocated 487,021,344 bytes cumulatively; allocation
traffic is not live memory.

This is a framework and adaptive-budget validation, not the completed SkyWalking result. The actual SkyWalking
configuration and its independently frozen deletion ledger still need a full MERGE-plus-FINALIZE run. Repeated processes
are also required before drawing timing-regression conclusions; the single disabled and retain-all primary runs are used
only to prove identical selection trajectories and outputs.

### Revised Two-Track Test Strategy

The primary performance comparison uses one frozen, production-shaped mature merge rather than requiring the continuously
writing replay to produce a stable number of mature selections. Every framework and plugin variant clones the same
pre-dispatch shard snapshot, advances the test clock until every selected input is mature, and executes exactly one
production-picker merge. This controlled round is the pipeline-disabled baseline and the common comparison unit for the
minimal framework plugin, trace assembly, and native plugin variants.

The seed is not a synthetic part fabricated for the benchmark. Discover it by running the real two-times fixture through
the unmodified picker with the pipeline disabled and pausing immediately before a representative high-generation
selection. Freeze the complete core and secondary-index snapshot, selected input IDs, selection checksum, logical clock,
part metadata, and logical ledgers. Preserve the whole snapshot, not only the selected directories, because conjunction
parts outside the selected set can affect trace-fragment boundary protection.

The strongest current candidate is pilot sequence 263 at `21:55:11`. Its production selection contains 15 merge outputs,
no raw parts, input depths two through four, 147,126 rows, and 36,948,453 compressed core input bytes. Eleven inputs and
113,898 rows were already mature; four inputs and 33,228 rows were hot. For the controlled benchmark, keep the snapshot
and selection unchanged but set logical time to exactly `selected_max_timestamp + 2h`, making all 15 selected inputs
mature without changing picker ordering. A discovery rerun must capture this pre-dispatch state because the completed
pilot has already consumed those input parts.

The controlled discovery rerun has now captured that exact state after 2,941 publications and 262 completed merges. The
selection checksum is `45aa16460d7ca36fc0ddaa1bc1d2e73f45109dbaab86328012798d5011e60b7b`. The active snapshot contains 1,621 files and
81,894,144 bytes with tree checksum `24dd0abcadafec97d80fbf99e069777365dbfdfaef07c79a05117f8a8a4e3ddf`. Its manifest also freezes every active part's
in-memory merge depth because merge generation is not persisted in the production part format and would otherwise reset
when a seed clone reopens.

The first five pipeline-disabled controlled repetitions all selected the same 15 inputs, matured all 15 parts, merged
147,126 input rows to 147,126 output rows, completed exactly two secondary-index children, and reconciled all three
logical ledgers. Median core-round wall time is 7.061 seconds (2.59% CV), median attributed CPU is 7.176 seconds (2.66%
CV), median allocation is 215.5 MiB (2.19% CV), and median peak RSS is 135.2 MiB (1.99% CV). Compressed logical output
over core plus both indexes is 1.0009 times compressed input. These values establish the controlled pipeline-disabled
baseline; filesystem write-byte counters remain a noisier diagnostic at 6.66% CV.

The first minimal-framework comparison now loads the real metadata-only `alwayskeepsampler.so` through the production
SDK loader before timing. Its plugin checksum is
`0966c63513576d0f7d35176f1dec172bef760ed40dfaf475faa4b9f2d6baac8c`. Five fresh pipeline-disabled processes and five
fresh retain-all processes used the same frozen selection in alternating order. Every retain-all repetition made two
plugin calls, evaluated and retained exactly 33,353 trace IDs, dropped no trace, preserved all 147,126 span rows, merged
both secondary indexes, and reconciled all three logical ledgers. Compressed logical write amplification remained
identical at 1.0009 times.

This opening comparison exposes a framework memory issue. Relative to the disabled medians, retain-all allocated 56.2%
more bytes and made 38.0% more allocations; median peak Go heap increased 83.4% and median peak RSS increased 72.8%.
The exact-interval allocation profiles identify raw-trace staging as the first optimization target: `stageRawTrace`,
`blockMetadata.copyFrom`, `traceEvaluationStager.stage`, and `assembleStagedEvaluationBatch` account for the dominant
plugin-only allocation samples. The profiler is written immediately after the controlled merge, before output-ledger
verification, although its cumulative allocation view also contains the identical pre-merge input-ledger scan.

Do not interpret the retain-all median wall time (6.262 seconds) or CPU time (6.462 seconds) as a speedup over the
disabled medians (6.590 and 6.647 seconds). The disabled wall series has 9.87% CV and spans 5.937-7.608 seconds, while
the CPU profiles are dominated by variable filesystem syscalls. Physical process write-byte counters are likewise not
a semantic comparison: the two modes produce byte-identical logical core and index outputs even though those counters
differ. The timing conclusion remains `HOLD`; the stable allocation and memory deltas are actionable. Optimize staging,
then rerun both modes from the unchanged seed before adding the deterministic-drop matrix.

The controlled-round gates are:

1. the cloned opening snapshot and all three logical ledgers match the frozen seed;
2. the unmodified production picker chooses the exact frozen input-ID set and selection checksum;
3. every selected input maximum is at or before the two-hour frontier;
4. exactly one core merge and its two secondary-index child merges complete, with no recursive merge included in timing;
5. the pipeline-disabled baseline is lossless, while enabled variants invoke the expected plugin and reconcile against
   their independently generated output oracle; and
6. repeated processes use identical inputs and report wall time, CPU, allocation, peak memory, read/write I/O, and write
   amplification for that one round.

Plugin logic still has independent unit benchmarks over complete trace batches at the frozen 35% SkyWalking deletion
ratio. The controlled merge measures framework, reader/writer, trace assembly, fragment guard, and plugin integration
cost; the unit benchmark isolates the sampler algorithm itself.

The 24-hour serialized write-and-merge replay remains the final SkyWalking-settings integration test, not the primary
performance comparison. It validates real publication, repeated policy selection, hot bypass, secondary-index updates,
trace correctness, and end-to-end resource behavior. Its acceptance gate requires actual SkyWalking plugin calls. The
empty-shard replay therefore completes one explicit FINALIZE round after the two-hour cooldown; the controlled mature
MERGE round remains the identical-input framework comparison.

This two-track strategy replaces the unstable ten-mature-round requirement as the performance-comparison gate. Mature
selection counts in the full replay remain an integration diagnostic rather than a prerequisite for comparing plugin
costs.

### Full-Replay Integration Gates

Every full-replay integration iteration starts from the same frozen opening snapshot and serializes each publication with
the following merge drain while keeping the controller and data node in separate processes. The measured 24-hour
two-times workload remains unchanged.

Only four conditions block a report:

1. **Same test boundary:** fixture, empty opening snapshot, binary, resource envelope, one-shard setting, and repetition count
   match the frozen suite identity.
2. **Correct output:** core and both secondary-index logical ledgers reconcile with the variant's expected result, with
   exact row counts for lossless baseline and retain-all variants.
3. **SkyWalking exercised:** the configured SkyWalking plugin receives traces, returns decisions, and produces the
   expected independently verified deletions; a replay containing only grace bypasses is invalid.
4. **Sustainable execution:** every publication-to-idle barrier and the final drain complete within their timeout without
   OOM, unexpected sampling, leaked in-flight parts, or unfinished merge work.

Mature merge counts, mature rows and bytes, merge-depth composition, run-to-run CV, write amplification, CPU, memory, and
I/O remain full-replay report metrics rather than performance-comparison gates. Merge generation is not part of maturity
validity: it only describes a part's history.

The previous mature-round instability analysis was polluted by the defective timestamp mapper. A part positioned one
nanosecond inside the frontier became mature after any positive logical-clock advance, so asynchronous picker timing
changed its classification. The corrected mapper preserves each trace's internal deltas but selects the latest constant
offset for which every fragment timestamp is no later than its publication. Consequently, at least one fragment of each
trace is publication-aligned, all fragments are initially hot, and the two-hour grace is a real aging interval. The
corrected rerun yields six mature-containing selections, below the ten-round gate; it does not by itself choose between
a warmed opening state and another production-shaped fixture adjustment.

The first discovery replay at 1,400x completed with exact ledgers but used the defective fixture. Its five mature rounds
and merge-depth results are discarded rather than treated as a failed readiness gate.

## Objective

Measure and remove the cost added by the plugin framework, complete-trace assembly, and native trace plugins during real
file-part compaction. First establish a stable pipeline-disabled baseline from the revision under test. Then measure the
framework with the simplest plugin, benchmark plugin logic independently, and finally validate the complete SkyWalking
configuration. The legacy plugin implementation and historical pre-plugin revisions are outside the comparison boundary.

The test must exercise the production merge picker, dispatcher, worker lanes, core and secondary-index readers and
writers, trace assembly, plugin call, fragment guard, and merged-part introduction. It must not include trace ingestion,
seed decoding, fixture generation, plugin compilation, or plugin loading in the measured interval.

## Questions Answered

1. What is the correctness and performance baseline of the current ordinary merge path with the pipeline disabled?
2. How much wall time and CPU does the minimal plugin framework add to the merge loop?
3. How much additional allocation and peak memory comes from assembling complete traces?
4. Does the plugin change merge throughput, write amplification, or the merge-policy trajectory?
5. Does the two-hour merge grace prevent plugin evaluation for the hot tail while allowing evaluation for mature data?
6. Does the plugin see one assembled input per trace ID, including traces split between seed and boundary parts?
7. Is the logical output identical to the input when the benchmark plugin always retains traces?
8. After framework optimization, how expensive is each proposed plugin independently?
9. Can the final SkyWalking configuration sustain the downloaded shard's one-shard workload inside the resource limit?

## Boundary

### Included

- File-part selection by the default production merge policy.
- Fast- and slow-lane dispatch, queueing, and merge workers.
- Core trace-part reads and writes.
- Shard-local secondary-index merges associated with the selected core part IDs.
- Trace-ID grouping and complete-trace assembly.
- Native plugin invocation through the production registry and chain.
- The merge-grace maturity check and fragment-guard session.
- Snapshot introduction and deletion of merged input parts.
- All merge rounds caused by one serialized 24-hour logical replay.
- Exactly one trace shard; the fixture is never cloned into additional shard IDs to manufacture concurrency.

### Excluded

- Network ingestion and WAL work.
- Liaison raw-write conversion, part transfer, and data-node receipt.
- Query load, retention deletion, background finalization scheduling, handoff, and segment-level series-index work. The
  final plugin integration includes exactly one explicitly triggered cooldown FINALIZE round.
- Plugin compilation and initial `plugin.Open` cost.
- Fixture cloning and logical-output verification.
- Performance of a future query-time trace-latency field.

The shard-local secondary indexes are part of the primary end-to-end merge workload. A separate core-only diagnostic
may disable them to attribute CPU more precisely, but it must not replace the production-shaped primary result.

The untimed fixture builder uses the production data path only to create valid inputs. A liaison-side encoder converts
each frozen raw-write batch directly into a core part and its secondary-index parts. The production part-transfer handler
delivers them to an isolated data-node table, which persists each received part immediately. No accumulation or timed
flush policy is modeled. The resulting files become immutable merge inputs, and none of this setup is measured.

## Resource-Limited Container

Every measured run must execute in a fresh resource-limited Docker container. Running the benchmark directly on the
host is permitted only for development and cannot produce a reportable result.

Only the data-node container belongs to the measured cgroup. A separate fixed-version controller process or container
drives logical time and part publication, and its CPU, heap, and allocations are never mixed with data-node results. The
controller is pinned away from the data-node CPUs and reports its own scheduling lag and failures separately.

The canonical container profile is:

- two pinned logical CPUs with a two-CPU quota;
- `GOMAXPROCS=2`;
- 4 GiB memory limit;
- memory swap limit equal to the memory limit, preventing additional swap use;
- a fixed PID limit of 512;
- the same read-only seed mount and a new writable fixture volume for every run;
- the same image digest, benchmark binary, plugin binary, filesystem, and storage device for all variants.

The profile may be changed for an explicit resource-scaling matrix, but baseline and plugin variants in one comparison
must have identical limits. Every report records both the requested Docker limits and the CPU and memory limits detected
inside the container. A mismatch invalidates the run because merge concurrency and staging budgets are derived from the
detected cgroup resources.

The two-CPU limit is an upper resource envelope, not a utilization target. Low utilization from the single shard is a
valid result and must not be corrected by cloning the workload, increasing shard count, or forcing extra merges. This
design measures plugin impact on the downloaded shard; it does not establish whole-node saturation capacity.

Fixture generation uses the same image, binary, schema, encoders, and cgroup profile as the measured runs. Generate and
validate the immutable fixture once, then clone it for every measured variant. Part boundaries come from the frozen
liaison input batches, not from resource-derived memory limits or a flush timer.

The monitor captures cgroup v2 CPU usage and throttling, memory current/peak/events, I/O bytes and operations, PID
events, and CPU, memory, and I/O pressure. CPU throttling, OOM events, or nonzero swap use do not automatically invalidate
a stress run, but they must be highlighted and cannot be compared with an unthrottled run as if the environments were
equivalent.

Docker containers share the host page cache. Fresh containers alone do not create cold-cache runs. Cache policy must
therefore remain unchanged across variants, and reports must label the series as warm, cold under externally controlled
host cache, or uncontrolled. Results from different cache policies must not be combined.

## Real-Data Seed

The fixture separates write-batch, tail-fragment, and semantic seed roles. Each smallest source part is the persisted
result of one real liaison write batch. These parts provide empirical batch templates plus 254 real hot-tail traces and
observed cross-part fragment cases; they are not the sole semantic row population. Their initial manifest is:

- `0000000000002266`
- `000000000000226b`
- `0000000000002265`
- `0000000000002259`
- `0000000000002233`
- `000000000000222c`
- `0000000000002256`
- `0000000000002264`
- `0000000000002223`
- `0000000000002248`
- `000000000000223a`
- `00000000000021f1`

The twelve observed batches total 301,026 compressed bytes and 254 distinct trace IDs. Individually they contain 3-49
blocks, 14-262 rows, 6,171-121,692 uncompressed span bytes, and 4,542-54,450 compressed core bytes. Eighteen selected
trace IDs also occur outside this selection in the original shard. At the shard's logical end time, all twelve parts fall
inside the two-hour grace window and must merge without invoking a plugin.

The batch template uses only the observed post-encoding block count and row count. In data-node receive order, represented
by ascending part ID, the frozen template cycle is:

| Part ID | Blocks | Rows |
| --- | ---: | ---: |
| `00000000000021f1` | 45 | 262 |
| `0000000000002223` | 46 | 117 |
| `000000000000222c` | 13 | 31 |
| `0000000000002233` | 9 | 36 |
| `000000000000223a` | 49 | 188 |
| `0000000000002248` | 32 | 180 |
| `0000000000002256` | 26 | 164 |
| `0000000000002259` | 7 | 33 |
| `0000000000002264` | 32 | 137 |
| `0000000000002265` | 6 | 28 |
| `0000000000002266` | 4 | 14 |
| `000000000000226b` | 3 | 24 |

Fixture generation repeats this ordered cycle. The counts are batch-composition targets, not BanyanDB configuration.
Compressed and uncompressed byte fields remain reported outcomes and are not used as batching limits.

Batch boundaries use a deterministic closest-boundary rule over the chronologically ordered, indivisible physical
fragments. For template boundary `k`, let `(B_k, R_k)` be the cumulative target blocks and rows through that boundary and
let `(b, r)` be cumulative realized counts. Compare the boundary immediately before the next fragment with the boundary
immediately after it using:

`distance = abs(b - B_k) / templateBlocks_k + abs(r - R_k) / templateRows_k`

Choose the lower distance. A tie chooses the earlier boundary, except that a batch must consume at least one fragment to
make progress. Using cumulative targets prevents one batch's underfill or overshoot from drifting through later batches.
The generator never reorders, splits, or drops a fragment to improve the match.

When the measured input ends before the next template boundary, emit all remaining fragments as one `partial_tail`
batch. Do not pad it, carry it into another logical day, or redistribute it into earlier batches. It remains part of the
merge workload and all correctness and resource totals, but it is excluded from batch-template fit statistics. A fixture
has at most one measured partial-tail batch.

The twelve batches contain 1,214 rows, so one observed write contains an average of `1,214 / 12 = 101.1667` rows. The
generated day's actual row count determines the write rate:

`writesPerDay = ceil(generatedRows * 12 / 1,214)`

`writesPerMinute = writesPerDay / 1,440`

The downloaded shard contains 162,238 rows. At that reference volume, the mock writer introduces 1,604 parts per day,
or 1.1139 parts per logical minute, with one part due every 53.8653 logical seconds. The final hybrid fixture recalculates
the rate from its immutable row ledger rather than changing its trace population to force the reference number.

Create exactly `writesPerDay` evenly spaced logical publication slots across the half-open measured day. Repeat the
twelve `(blocksCount, totalCount)` templates across those slots; use the closest-boundary rule for the first
`writesPerDay - 1` boundaries and put every remaining fragment in the final batch. The final batch is marked
`partial_tail` only when it does not satisfy the next complete template. The writer introduces each already-generated
part at its slot while the production merge loop remains continuously active.

The semantic seed is the two-hour-mature production population from these six parts:

- `00000000000005ee`;
- `0000000000000b5b`;
- `0000000000001002`;
- `0000000000001191`;
- `000000000000165a`; and
- `0000000000001c36`.

They contain 31,844 physical blocks, 31,832 distinct trace IDs, 138,686 rows, and 34,856,465 compressed bytes. Their trace
IDs do not overlap the 254 structural-seed IDs. Thirty-one mature trace IDs also have 245 rows in
`00000000000021d4`; those rows are part of the semantic closure even though the unrelated rows in that hot part are not.

The measured payload contains all 31,832 mature traces, all 254 closed structural-seed traces, and one additional copy of
5,202 mature traces selected by an independent stable hash. Every copied trace receives a new ID and includes all of its
physical fragments. This produces exactly 37,288 distinct traces while retaining mature production variety and a small
amount of the real hot-tail fragmentation pattern.

Before fixture generation, re-read metadata and reject the source when any part ID, count, byte total, closure allowlist,
or source-manifest hash changes. The source remains read-only throughout the test.

The fixture closes those 18 traces by extracting only their missing rows from five outside source parts:

- `0000000000002218`: eight trace IDs and 63 rows;
- `000000000000222b`: six trace IDs and 28 rows;
- `0000000000002267`: one trace ID and six rows;
- `000000000000226a`: one trace ID and 15 rows; and
- `000000000000226c`: two trace IDs and 18 rows.

Every affected trace occurs in exactly one of these outside parts, for 130 extracted rows in total. Unrelated rows from
the five carrier parts are excluded. The mature closure similarly extracts only the 245 rows for its 31 allowlisted trace
IDs from `00000000000021d4`. Generated closure fragments retain separate boundary roles so the affected trace IDs remain
split across normal parts. These parts participate in the production snapshot, picker, merge, and fragment guard; they
are not hidden test-only guard inputs. Their generated bytes are reported separately because extraction changes their size
relative to the original carrier parts.

### Daily Volume Calibration

The primary 24-hour workload matches the complete downloaded shard rather than merely extrapolating one copy of the
small-part seed. The reference targets are 37,288 distinct traces, 162,238 rows, and 40,879,799 compressed core bytes.
The downloaded secondary-index trees occupy 2,798,752 bytes for `latency` and 2,404,977 bytes for `start_time`. Their
part-metadata compressed payload totals are 2,793,388 and 2,398,716 bytes respectively. Generated raw tree bytes are
reported independently, while density gates compare production-consolidated payload bytes to these like-for-like
source payload totals.

The mature population, structural seed, and 5,202-copy extension have an estimated source-equivalent core size of
40,853,750 bytes, within 0.1% of the downloaded daily target before production re-encoding. The 5,202 templates are the
lowest stable hashes over the complete mature trace IDs. This choice is independent of sampler class, error, latency,
tags, trace size, and performance results; freeze its allowlist and hash in the manifest.

Distribute the mature semantic population across the 24-hour write schedule while preserving each trace's intra-trace
timestamp deltas, duration, fragment gaps, relative row order, errors, and tags. Absolute timestamps may shift by one
constant per trace to place it in the measured day; this does not change the sampler's latency or error decisions. Every
raw part must still be hot when introduced under the two-hour grace. Divide chronological fragments into the calculated
number of liaison raw-write batches, convert every batch through the production part encoder, transfer it through the
production part-sync contract, and persist it in an isolated merge-disabled data-node table. There is no fixture flush
clock or flush threshold. The mature rows supply the dominant semantics; the smallest parts contribute real batch
templates, tail traces, and fragment cases. Repeat the twelve ordered `(blocksCount, totalCount)` targets while assigning
whole physical fragments; never split a fragment to hit a target. Batch boundaries come from those observed templates,
never from a synthetic byte budget that BanyanDB does not use.

Before freezing the fixture, run the actual default SkyWalking sampler over every complete generated trace after trace-ID
mapping. Its deletion ratio must be between 34.5% and 35.5%. This is a validity gate, not an optimization target: do not
change trace IDs, the 5,202-template allowlist, content, or plugin configuration after observing verdicts. Failure means
the seed does not represent the mature calibration and must be redesigned. The controlled DeterministicDrop plugin
remains configured at 35% and reports its realized per-selection ratios without a result-selected hash salt.

Encode the complete candidate through the production liaison encoder and data-node receiver. Require generated rows and
production-consolidated core bytes to be within 2% of the 162,238-row and 40,879,799-byte references. Require each
combined production-consolidated secondary-index bytes to be within 5% of their reference total and each individual
index to be within 10% of its reference. The per-index allowance covers key-distribution compression skew from the
independently hash-selected complete trace copies, while the tighter combined gate preserves total secondary-index I/O.
The consolidation is an untimed,
lossless production merge into the source shard's 26-part cardinality; it validates logical data density without
conflating it with the unavoidable framing cost of the approximately 1,604 raw input parts. Preserve and report the raw
fixture byte totals separately because those bytes, not the consolidated calibration outputs, are the merge workload.
Remove the temporary consolidation outputs and prove that the raw fixture remains queryable. Do not retune input
batches, timestamps, trace mapping, or content to satisfy these gates after observing output. The fixture may not add
padding, copy unrelated rows, or drop partial traces. If the gates cannot be met, the volume model must be redesigned
rather than accepting an unreported scale mismatch.

Daily trace, row, and byte volume remain the primary scale gates. The twelve write batches guide input grouping, but they
are a small tail sample rather than proof of the shard-wide production batch distribution. Full-loop results compare
plugin variants under one identical frozen topology; they cannot estimate production merge frequency or fan-in without
broader batch evidence. Reports must expose generated batch count, part-size histogram, publication gaps, and picker
trajectory.

The downloaded shard also contains shard-local secondary-index trees under `sidx/<index-name>/<part-id>`. These are
different from the segment-level `sidx/*.seg` and `sidx/*.snp` files, which are the series index. The fixture manifest
must pair every generated core part with every secondary-index part having the same part ID and record the source role,
index schema, and bytes separately.

The manifest should also record seed span count, block count, minimum and maximum timestamp, per-part compressed size,
and a checksum of the logical trace rows. These values make later benchmark runs comparable and make accidental fixture
changes visible.

### Default SkyWalking Ratio Calibration

The realistic DeterministicDrop point is derived from the actual SkyWalking sampler rather than an arbitrary 50%.
[SkyWalking PR 13965](https://github.com/apache/skywalking/pull/13965) leaves the pipeline disabled by default because it
deletes stored data. For calibration only, the pipeline was enabled while retaining its shipped `sw_trace` sampler
defaults: a 500 ms duration threshold, error retention enabled, no tag rules, and a 10% deterministic sample of the
remaining healthy traces. This distinction is important: the literal disabled configuration deletes nothing.

The calibration used the downloaded 24-hour shard with 26 core parts, covering 2026-07-29 00:00:00.587 through
23:59:56.171 UTC. Its source-manifest SHA-256 is
`7291ff8abedb1c6d31bb98356a89ed0bee4020d1ff0f602376b9028d0cf8c510`. The input contained 37,404 physical trace blocks
and 37,288 distinct trace IDs; 113 trace IDs occurred in more than one part. Because the files predate persisted block
timestamp bounds, the probe first performed a lossless production merge, asserted that no trace was dropped, and
asserted that the normalized output contained exactly one block for each of the 37,288 trace IDs. It then invoked the
actual native `sw-trace-sampler` through the merge path.

The source-manifest hash covers the sorted relative path and content hash of every file in the shard. The clean
mature-only replay opened only the six eligible part IDs listed below. Its eligible-input manifest SHA-256 was
`445e46f7d2a0d17976b8efb45b9479121e333a907ea6bfbd5a36b5e9f9bef2fa`, sampler binary SHA-256 was
`5768529e6087c00acc246bea4aceb4876dc9d3b954d6bb1a1a95d1c2fe550618`, and exact OAP-shaped configuration SHA-256 was
`6466931a246d608cd5994feec5daec6091f36e91e659437b01e62ce413ecb2ce`. The configuration passed `healthySampleRate` in
the quoted-string form produced by OAP's environment-placeholder loader.

The complete-shard verdict breakdown was:

- 29 traces retained by the duration rule;
- 22,789 additional traces retained by the error rule;
- 14,470 healthy, fast traces reaching the hash rule, of which 1,475 were retained;
- 24,293 traces retained in total; and
- 12,995 traces deleted, a 34.8504% deletion ratio.

With logical time fixed at 2026-07-29 23:59:56.171 UTC, the two-hour maturity frontier is 21:59:56.171 UTC. Exactly these
six downloaded source parts are mature by their maximum timestamps:

- `00000000000005ee`;
- `0000000000000b5b`;
- `0000000000001002`;
- `0000000000001191`;
- `000000000000165a`; and
- `0000000000001c36`.

The next large part, `00000000000021d4`, reaches 23:41:48.898 UTC, so any selection containing it is hot even though the
part begins at 18:07:40.302 UTC. A single hot part bypasses sampling for the whole merge.

After lossless normalization, the six mature parts contained 31,832 distinct trace IDs. The actual default sampler
retained 26 traces by duration, 19,377 additional traces by error, and 1,287 of the remaining 12,429 by the healthy hash.
It retained 20,690 and deleted 11,142, a 35.0025% deletion ratio. The complete-shard ratio of 34.8504% is consistent with
that result. The 35.0025% value is a production-data-calibrated sampler verdict and is suitable for shaping the Phase 2
DeterministicDrop workload. It is not an observed production merge deletion ratio.

The twelve smallest parts produced a 40.9449% deletion ratio when forced through the sampler, but that result is not
used for performance load shaping because a real two-hour-grace merge would bypass the plugin. They contribute only
their 254 closed traces and real tail-fragment cases. The mature population supplies the dominant semantic content of the
generated fixture, so the default SkyWalking plugin naturally sees the production-calibrated mix instead of a
synthetically reweighted sample. Their original part sizes do not control the generated topology.

The current production picker selects no parts from this frozen snapshot. For the six mature parts, the best admissible
size window has a merge multiplier of 3.92791341, below the default policy's required 7.5. Including all 26 parts still
produces only 5.52848485, also below 7.5. Therefore the mature-only 35.0025% result is a clean offline plugin verdict but
does not describe a merge that production dispatched at that instant. This picker result does not invalidate 35% as a
representative deletion-load input for the framework benchmark; it limits the claim that can be attached to it.

The benchmark distinguishes three ratios:

- `R_calibrated` is the 35.0025% actual-plugin verdict over the clean mature production-data population and controls the
  Phase 2 DeterministicDrop setting;
- `R_merge_verdict` is the trace-weighted plugin verdict over exact mature selections dispatched during replay; and
- `R_effective` is the ratio actually removed after grace, fragment-guard, bypass, retry, and publication handling.

`R_calibrated` is an external day-long production calibration. Never infer it from the small-part seed and never
reweight latency, error, tag, or span content to manufacture it. The hybrid seed combines mature semantic data with the
real small-part tail traces and must naturally pass the 35% actual-plugin validity gate. The fixed 35%
DeterministicDrop policy controls Phase 2 deletion load.

Measure the latter two from the serialized production loop. Run the pipeline-disabled loop first, capture every
selection for which all parts are beyond the two-hour frontier, and replay those selections offline through the actual
sampler without changing the baseline picker trajectory. Freeze the selection-manifest hash and report any difference
from `R_calibrated`; do not silently retune the Phase 2 matrix after seeing performance results.

The ratio calibration must be rerun when the source manifest, trace-ID mapping, merge schedule, merge policy, or
SkyWalking sampler configuration changes. Reports must show the eligible plugin verdict ratio separately from the
effective deletion ratio after merge grace and fragment-guard decisions.

## Building a 24-Hour Fixture

Fixture construction happens once before profiling and is never timed.

1. Decode the six mature semantic parts through the production trace block reader.
2. Decode `00000000000021d4`, extract exactly the 245 allowlisted rows needed to complete the 31 mature boundary trace
   IDs, and reject any missing or unrelated row.
3. Decode the twelve structural parts and the five structural boundary carriers. Extract exactly the 130 allowlisted
   rows needed to complete the 18 structural boundary trace IDs, excluding every unrelated carrier row.
4. Build two immutable complete-trace catalogs. Verify 31,832 mature IDs, 254 structural IDs, zero overlap between the
   catalogs, and a logical checksum for every trace and physical fragment.
5. Select the 5,202 mature templates with the lowest values under the frozen independent hash. Add one complete copy of
   each selected template. Map every measured trace instance to a unique fixed-width ID; every physical fragment of an
   instance receives the same mapped ID. Derive the fixed-width ID from an independent source-ID-only family prefix and
   an instance digest. This preserves the repeated producer-prefix compression present in real SkyWalking IDs without
   consulting trace content, sampler verdicts, index size, or benchmark output; copied instances share only the family
   prefix with their source and retain a unique instance suffix.
6. Calculate `writesPerDay` from the immutable generated-row ledger and the observed 101.1667 rows per write. Create that
   many evenly spaced slots in one measured logical day. For each complete trace, calculate the latest constant timestamp
   offset for which every fragment maximum is no later than its assigned publication. Preserve intra-trace deltas,
   fragment gaps, and relative row order. Reject the schedule unless every fragment is non-future and inside the two-hour
   grace when introduced. This makes trace maxima follow the 24-hour publication schedule instead of concentrating at the
   grace frontier. Generate a deterministic preceding pre-roll at the same part rate only after validating this mapping.
7. Divide physical fragments in deterministic event order into immutable liaison raw-write batches.
   Repeat the twelve observed `(blocksCount, totalCount)` templates in ascending source part-ID order. Assign only whole
   physical fragments. At every cumulative template boundary, compare stopping immediately before or after the next
   fragment with the frozen normalized-distance formula and choose the closer boundary. Do not reorder or split a
   fragment, reset cumulative error between batches, or introduce a byte-budget flush rule. Schedule each allowlisted
   boundary fragment in a later batch than its companion so the resulting parts remain separate and the production
   fragment guard can observe them. Use exactly the calculated number of publication slots and emit every final remainder
   in the last batch, marking it `partial_tail` when it is not a complete template match.
8. Convert each batch through the production liaison part encoder and send the resulting core, `latency`, and
   `start_time` parts through the production part-sync contract to an isolated data-node receiver. The receiver persists
   each part immediately; merge workers, plugins, and WAL are disabled. Capture the received files without rewriting or
   manually splitting them, then freeze their boundaries and logical publication times before sampling or profiling.
   The downloaded shard supplies the already-resolved secondary-index series IDs and post-rule SIDX tags for each row;
   fixture generation preserves those records and sends them through the same SIDX mem-part encoder used after liaison
   rule processing, rather than reconstructing a potentially different historical schema or dropping projected tags.
9. Persist a schedule manifest containing source catalog and role, source trace ID, copy ordinal, mapped trace ID, part
   ID, template ordinal, full or `partial_tail` status, target and realized block and row counts, cumulative targets and
   counts, before/after boundary distances and choice, logical publication slot, timestamp bounds, core and per-index
   bytes, trace counts, content checksums,
   source-manifest hash, closure-allowlist hashes, copy-allowlist hash, liaison-encoder and schema hashes, raw-write-batch
   template and schedule hashes, and received-topology hash.

The fixed-width trace-ID mapping prevents template copies from becoming one false logical trace. It also keeps key
lengths stable and prevents a changing string prefix from distorting compression more than necessary. The mapping and
copy allowlist are chosen before any sampler verdict and cannot be tuned to reach the ratio gate.

The complete fixture must satisfy these gates before a measured run:

- the configured publication window is exactly one half-open 24-hour logical day, every publication and mapped trace
  maximum falls inside it, mapped maxima span the publication day, and the timestamp mapping preserves every intra-trace
  delta and fragment gap; an earlier trace row may precede the publication-day boundary only by its preserved trace
  duration;
- the measured window contains exactly 37,288 distinct mapped trace IDs;
- the generated row ledger is within 2% of the 162,238-row source reference, and `writesPerDay` exactly equals
  `ceil(generatedRows * 12 / 1,214)`;
- the population ledger contains exactly 31,832 mature base instances, 254 structural instances, and 5,202 additional
  mature instances, with no duplicate mapped ID or overlap between the source catalogs;
- the mature source closure contains exactly 245 allowlisted rows for 31 trace IDs from `00000000000021d4`, and the
  structural source closure contains exactly 130 allowlisted rows for 18 trace IDs from the five declared carriers;
  neither catalog contains an unrelated carrier row;
- every base or copied instance is complete, includes all applicable closure rows, maps all physical fragments to the
  same ID, and preserves its source intra-trace timestamp deltas, fragment gaps, and relative row order;
- the actual default SkyWalking plugin deletes between 34.5% and 35.5% of all complete generated trace instances and
  its per-rule counts, configuration hash, binary hash, mapped-ID manifest hash, and verdict checksum are recorded;
- production-consolidated core bytes are within 2% of 40,879,799 bytes, combined consolidated secondary-index bytes are
  within 5% of their downloaded-shard total, and each index is within 10% of its own reference; raw fixture bytes and
  26-part calibration bytes are both reported;
- the pre-roll ends at a dispatch barrier immediately before the first mature production-picker selection;
- every merge completed before that barrier is hot under the two-hour grace, while the pending selection is entirely
  mature and satisfies the unmodified production merge policy;
- all part bounds enclose the timestamps decoded from their blocks;
- no mapped trace ID is reused in pre-roll or crosses a logical-day boundary;
- every generated instance of an affected structural or mature trace remains split across separate normal part roles;
- every raw-write batch was converted by the production liaison encoder and immediately persisted through the
  production data-node part receiver; no configurable accumulation or flush deadline affected the topology;
- every generated batch follows the frozen contract extracted from the twelve observed source batches, and the fixture
  contains no synthetic byte-budget or flush-threshold parameter;
- the template sequence repeats in ascending source part-ID order, no source physical fragment is split to improve a
  batch match, every boundary is reproducibly the closest chronological boundary under the frozen formula, and
  target-versus-realized block and row deviations are recorded;
- the measured schedule has at most one `partial_tail` batch, it is last, it contains every remaining fragment exactly
  once, and only this batch is excluded from template-fit statistics;
- the fixture contains exactly `writesPerDay` evenly spaced publication slots, each part is introduced once at its slot,
  every newly introduced raw part is inside the two-hour merge grace, and no fragment timestamp is later than its
  publication;
- no compaction, plugin evaluation, or WAL write occurred while creating source parts, and transfer time is outside the
  benchmark interval;
- the logical checksum before liaison conversion equals the checksum reopened from the received core and secondary-index
  parts;
- every benchmark variant uses the exact frozen part boundaries, publication schedule, and received-topology hash;
- all parts can be reopened by the production file-part reader;
- every generated core part has the expected secondary-index parts with the same part ID;
- trace lookups for every generated ID return the expected row count; secondary-index full scans return the exact
  query-semantic projection of the rewritten row multiset (including native per-block data deduplication), and keyed
  lookups cover every distinct encoded trace ID with only valid ledger rows and never exceed the corresponding physical
  row multiplicity (the keyed implementation deduplicates data within each scanner batch, not globally across batches);
- the default merge policy can select at least one set of parts;
- total input rows and the logical checksum match the schedule manifest.

## Logical Clock and Serialized Publication

The test must not wait 24 wall-clock hours. Add a test-only clock seam used by the merge maturity check; production still
uses the real clock. The logical clock starts at the pre-roll's first timestamp and advances directly to the next
publication only after the preceding merge-idle barrier completes. Before introducing a part scheduled at `P`, the
controller sets the data node's logical current time to exactly `P`; logical time remains fixed at `P` until every merge
triggered by that publication has drained.

Only if the corrected empty-shard serialized pilot fails the mature-round gate for a demonstrated startup-state reason,
evaluate pre-roll as a separate redesign; it is not part of the current run. Determine any proposed pre-roll length in an
untimed discovery pass. Publish the preceding hybrid schedule through the real production
merge loop with the pipeline disabled, advance the logical clock, and inspect each proposed dispatch. Stop when the picker
first proposes a selection for which every part maximum timestamp is at or before `logical_now - 2h`. A benchmark-only
dispatch barrier must pause before that merge starts. Fail fixture generation if no such selection appears within 24
logical hours.

Rebuild the pre-roll from the immutable schedule to the discovered barrier and verify its opening inventory, logical
checksum, picker-selection fingerprint, and logical duration against the discovery manifest. Every merge executed during
pre-roll must be hot, making pipeline-disabled behavior equivalent to an enabled pipeline's grace bypass. Freeze and
clone this warmed state for every variant. Timing begins when the barrier is released, so the first controlled merge is
both production-dispatchable and mature. Record opening core and secondary-index inventories separately from the 24-hour
measured input.

All file parts are generated before measurement. During a run, the driver publishes scheduled parts by atomic rename and
snapshot introduction, sets the logical clock to that part's publication, notifies the production merge loop, and waits
for verified merge-idle before continuing. Merge-output notifications may cause recursive production picker passes, but
the controller cannot introduce the next write or advance logical time until all of them finish. There is no concurrent
publisher mode in a reportable run.

The driver is an external controller, not a goroutine in the measured data-node process. Before timing, every immutable
part is staged on the same filesystem as its destination so publication is an atomic rename rather than a timed byte
copy. The controller performs the rename and calls a test-only introduction/clock endpoint. The data node still performs
the real part open, snapshot introduction, picker notification, and merge work; those operations remain measured. The
controller cannot rewrite part contents or invoke the merger directly.

The merge-idle barrier is epoch-aware: it may return only after the dispatcher has observed the snapshot epoch created by
the publication, found no selectable parts for that epoch, and both worker lanes have no queued, running, or in-flight
work. Checking only empty queue counters is insufficient because the dispatcher might not yet have consumed the
publication notification. Fixed sleeps are not acceptable.

The controller and data node remain different processes. The external controller owns file publication and logical-time
advancement; the resource-limited data-node process owns snapshot introduction, production picking, core and secondary-
index merging, and plugin execution. Controller CPU and memory remain outside measured data-node resources.

The merge grace is exactly two logical hours. At a `12:10` publication, the maturity frontier is exactly `10:10`; a part
whose maximum timestamp is at or before `10:10` is mature. A merge containing that part is a mature merge even when it
also contains the newly written `12:10` part. Such a selection is both mature and hot. Under the current whole-selection
grace guard, the hot part causes sampling to be bypassed; the maturity classification and sampling decision are separate
reported dimensions. The driver records mature and hot input-part counts, their row counts, the frontier, and the
sampling decision before dispatch.

After the last scheduled publication and its merge-idle barrier, advance only to the measured day boundary and drain one
final picker epoch. This completes the primary 24-hour phase without prematurely maturing the tail. Record a phase
boundary and resource-counter snapshot.

The cooldown phase then advances logical time by two hours, triggers the merge loop without creating a synthetic data
part, and drains to merge-idle again. This trigger requires a test-only notifier seam. Cooldown work is reported
separately because it represents deferred tail processing rather than continuous 24-hour writing. The sum of primary and
cooldown work is a secondary end-to-end result used for complete-output accounting.

This schedule deliberately includes two states:

- rolling hot-tail compaction, in which newly published parts bypass sampling;
- mature compaction of parts produced by earlier hot merges, in which the plugin processes complete traces.

The approximately 1,604 raw parts at the reference row volume must create numerous policy-driven merges while writing
continues. Require at least ten grace-bypassed core merges and ten mature plugin-executed core merges during the 24-hour
publication interval, before the artificial cooldown. Every plugin-executed input part must have merge depth of at least
one, proving that raw write parts compacted while hot and sampling ran only on their later merged outputs. A raw part
sampled only after the final cooldown does not satisfy this gate. If natural policy selection cannot meet it, redesign
the fixture rather than forcing a selection.

## Per-Merge Sampling Classification

The benchmark monitor must classify every core file merge independently. Inferring sampling from a difference between
global counters is insufficient because sampled and unsampled merges can occur in the same logical replay.

Each merge receives a benchmark-local sequence number used only in the JSON event stream, not as a metrics label. At
dispatch, the monitor records selected part count and bytes, part time envelope, logical time, maturity frontier, lane,
input merge-depth range, and the initial pipeline decision. Raw writer parts have depth zero; every merge output has one
plus the maximum input depth. At completion, the monitor records plugin calls, evaluated traces, output bytes, secondary
index child merges, duration, CPU, allocation, memory, and I/O deltas.

Every event has a low-cardinality `sampling` classification:

- `executed`: at least one plugin `Decide` call evaluated at least one complete trace;
- `enabled_no_evaluation`: a filter was installed, but no trace reached `Decide`, for example because all traces were
  oversized-bypassed;
- `not_executed`: no filter was installed or the merge took a lossless retry path.

`not_executed` and `enabled_no_evaluation` also carry exactly one reason:

- `pipeline_disabled`;
- `no_sampler`;
- `event_disabled`;
- `merge_grace`;
- `fragment_gap_contract`;
- `guard_unavailable`;
- `all_traces_oversized`;
- `lossless_retry`;
- `empty_input`;
- `other`.

The binary report groups `executed` as sampling and the other two classifications as no sampling, while preserving the
reason breakdown. A merge is never marked `executed` merely because a sampler was registered; actual `Decide` calls and
evaluated-trace counts are required.

Secondary-index merges inherit the sequence number and sampling classification of their parent core merge. They are
reported as child work rather than independent sampling decisions because the plugin runs only against the core trace
merge.

The monitor emits both a JSONL event file for per-merge analysis and aggregate counters/histograms labeled only by
sampling classification, reason, and lane. Part IDs and merge sequence numbers must not become Prometheus labels.

### Attribution Run

Per-merge CPU, memory, and I/O cannot be attributed reliably when sampled and unsampled merges overlap. Therefore the
mandatory attribution run limits active merge concurrency to one while retaining the four-CPU container. The monitor
captures process resource counters immediately before and after each merge and samples memory during the interval. With
query, flush, finalization, and ingestion work disabled, the deltas belong to that merge and its secondary-index child
work. The driver also waits for merge-idle before publishing the next scheduled batch.

The pipeline-disabled and AlwaysKeep attribution runs must produce the same ordered selection fingerprint, including
input part lineage, fan-in, lane, and logical output contents. Wall time may differ, but closed-loop publication prevents
that latency from changing which parts are available to the picker. Any trajectory divergence is a framework issue and
invalidates attribution until explained and fixed. DeterministicDrop is exempt because deleting output intentionally
changes later policy inputs.

### Serialized Full-Loop Resource Run

A second full-loop run uses production-derived merge concurrency for the four-CPU container. It reports aggregate daily
resource use. Per-merge wall time, selection, sampling classification, and bytes remain valid, but CPU and peak-memory
attribution by sampling class is advisory when merge workers overlap within one drain epoch. The report must not present
overlapping cgroup deltas as additive per-class CPU totals. The controller publishes the next part only after the current
merge loop reaches epoch-aware idle. Report logical input volume per data-node CPU second and active merge wall time; do not
interpret serialized end-to-end wall time as production ingestion capacity.

Only the downloaded shard is active. Report effective CPU utilization and available headroom, but do not interpret
unused CPUs as evidence that the data node could sustain a proportional number of additional shards.

All variants start from the same opening state and serialized publication schedule. Plugin latency cannot change which
raw parts coexist at a picker epoch. Pipeline-disabled and AlwaysKeep must therefore have identical selection
fingerprints. A dropping plugin may diverge because its output sizes and logical contents intentionally differ; report
the first divergence rather than forcing the baseline plan.

## Benchmark Variants

Each variant starts from an independent clone of the same immutable generated fixture and uses the same logical clock
and publication schedule.

### 1. Baseline: Ordinary Merge

- Run only the revision under test.
- Disable the native trace pipeline and register no sampler.
- Do not load a plugin or perform filtering, projection, or complete-trace staging.
- Use the same frozen opening state, schedule-manifest checksum, binary, Docker image digest, Go toolchain, resource
  limits, filesystem, storage device, cache policy, merge policy, and merge concurrency as the plugin variants.

This pipeline-disabled run is the sole baseline for the iteration. Establish its correctness, merge trajectory,
repeatability, and performance envelope before plugin-enabled measurements begin. Record its commit and artifact hashes;
after a production change, rerun and freeze a new baseline rather than comparing against the legacy plugin or an
incompatible historical file format.

### 2. Minimal AlwaysKeep Plugin

- Native trace pipeline enabled.
- Two-hour merge grace.
- Fragment-gap declaration set to a valid value no greater than the merge grace, so the guarded path can run.
- A real native plugin is loaded before timing.
- The plugin projects no optional columns and retains every assembled trace.
- Apart from constructing the required verdict mask and maintaining test counters, it performs no application logic.

This variant measures the minimum framework cost: trace grouping, staging, SDK batch construction, native ABI dispatch,
verdict handling, guard setup, core rewriting, and ordinary secondary-index merging. Its output must be logically
identical to the pipeline-disabled baseline.

### 3. Minimal DeterministicDrop Plugin

- Use the same engine configuration and empty projection as AlwaysKeep.
- Decide from a stable hash of `TraceID` only, using a fixed configured drop percentage.
- Record the configured and observed drop ratios.
- Perform no tag decoding, span decoding, logging, telemetry, or other application logic in the timed path.

This variant exercises dropped-ID collection, core trace removal, secondary-index pruning, and atomic publication without
mixing projection or business-logic costs into the framework measurement. The hash function and threshold must remain
identical across all iterations.

Use the following drop matrix:

- 1% to expose fixed framework cost when output size is nearly unchanged;
- 35% as `R_calibrated`, the clean mature production-data sampler ratio; and
- 99% to stress dropped-ID tracking, deletion, and secondary-index pruning while minimizing output writes.

Run all three points in the controlled mature-selection comparison and short validation fixture. Run 35% for the
mandatory full 24-hour Phase 2 DeterministicDrop workload. The 1% and 99% points need a full 24-hour run only when the
shorter results reveal a nonlinear cost or correctness anomaly. Phase 4 separately reports `R_merge_verdict` and
`R_effective` from the actual SkyWalking plugin; those observed ratios must not be relabeled as the controlled 35% case.

### 4. Final SkyWalking Plugin Configuration

After framework and plugin-unit optimization, run the actual SkyWalking plugins and settings, including their real
projections and sampling rules. Dropped output reduces write I/O, so compare this result using retained bytes and work
normalized by input traces, spans, and bytes rather than interpreting a lower elapsed time as lower framework overhead.

The full-loop harness selects this variant with `FULL_PIPELINE=skywalking`. It builds the production
`sw-trace-sampler`, loads its JSON configuration from `SKYWALKING_PLUGIN_CONFIG`, and passes those exact bytes to the
native plugin constructor. The default harness configuration is the calibrated 500 ms duration threshold, error
retention, and 10% deterministic healthy sampling policy. Every report records independent SHA-256 identities for the
plugin binary and configuration so a result cannot silently mix policies. Custom configuration files are allowed, but
they must remain inside the read-only repository mount and their observed verdict ratios must be reported as observed
SkyWalking ratios rather than as the controlled 35% DeterministicDrop case.

## Iterative Execution Phases

### Phase 1: Protect the Ordinary Merge Path

Run the current revision with the pipeline disabled. Validate output correctness and profile selection trajectory, wall
time, CPU, allocations, memory, I/O, and amplification. Fix flaws observable on this ordinary path and repeat until at
least five runs establish a stable baseline envelope. The legacy plugin implementation is scheduled for removal and is
not a benchmark comparator.

### Phase 2: Measure the Plugin Framework

Run both minimal native plugins with empty projections. AlwaysKeep establishes the minimum complete-trace assembly and
ABI overhead while preserving all data. DeterministicDrop adds only stable trace-ID hash selection and exercises core
deletion and both shard-local secondary-index pruning paths. Profile both full merge paths, fix framework bottlenecks and
correctness flaws, and repeat the baseline and both framework cases after every change. Phase 2 finishes only when all
known framework issues have been resolved or explicitly accepted with quantified impact. Use 35% for the full-workload
DeterministicDrop run and retain the 1% and 99% controlled-round sensitivity points.

### Phase 3: Benchmark Plugins Independently

Run unit benchmarks for each plugin outside storage merging. Use representative assembled batches captured from the real
fixture, including trace-count and spans-per-trace distributions. Measure projection, decision logic, allocations,
latency calculation, and any output construction independently so plugin-local costs are not confused with storage I/O.
Fix plugin-local performance problems and repeat until stable.

### Phase 4: SkyWalking Integration

Configure the actual SkyWalking pipeline, projections, thresholds, merge grace, fragment-gap contract, core indexes, and
secondary indexes, with both MERGE and FINALIZE enabled. Run the serialized 24-hour publication schedule with
production-derived merge concurrency inside the same Docker resource envelope, then trigger exactly one FINALIZE round
at `day_end + 2h`. Report the primary MERGE phase and cooldown FINALIZE phase independently. This is the final one-shard
integration result, not a whole-data-node capacity claim.

Every optimization iteration preserves the fixture and environment checksums and reruns all earlier phases affected by
the change. A final integration improvement is not accepted if it regresses the pipeline-disabled baseline.

## Two Measurement Layers

### Controlled First-Round Comparison

Capture a representative hot selection during pre-roll and the mature selection held at the opening dispatch barrier.
Replay each exact selection under every applicable variant. The hot selection proves that merely enabling the pipeline
does not invoke the plugin inside grace. The mature selection provides the identical-input plugin-cost comparison. This
reports kernel-level overhead but does not claim to represent a complete day's compaction.

### Full Merge-Loop Comparison

Run the actual merge loop for the entire serialized logical schedule, the day-boundary drain, and the separately
reported cooldown drain. Do not force a precomputed selection plan. Output sizes may cause later picker decisions to
diverge between baseline and plugin variants; that is a real system effect and must be recorded rather than hidden.

Both layers are required. The controlled round answers how much one merge costs. The full loop answers how the feature
changes total compaction work over a day.

## Timed Region

Start timing only after the generated fixture clone is ready, the table and snapshot are open, the plugin is compiled
and loaded, the registry is configured, and profiles are armed. The primary timer starts when the opening dispatch
barrier is released and stops after the day-boundary merge-idle barrier. The cooldown timer starts immediately before the
two-hour logical advance and stops after the second merge-idle barrier. Every merged introduction must be durable at its
phase boundary.

Both timed phases include their publication or trigger notification, policy or finalization selection, queueing, core
and secondary-index merge I/O, plugin execution, fragment guard work, and merged-part introduction. They exclude fixture
generation, directory cloning, plugin loading, result scanning, and checksum verification. Report primary and cooldown
profiles and metrics independently; report their sum only as the secondary end-to-end total.

For the controlled first-round layer, timing starts immediately before dispatch and ends after durable introduction.

## Metrics

### Primary

- primary serialized-replay wall-clock seconds, active merge seconds, and publication-to-idle barrier latency;
- cooldown wall-clock seconds, reported separately;
- process CPU seconds and average effective cores;
- compressed input MiB per second;
- traces and spans per second;
- allocated bytes and allocation count;
- peak Go heap and process RSS;
- bytes read and written by the process;
- merge write amplification: merged output bytes divided by bytes of selected inputs;
- total daily compaction amplification: all merge output bytes divided by generated fixture bytes.

Report every resource metric separately for the primary and cooldown phases. Report core and secondary-index bytes
independently in addition to their combined totals.

### Merge Shape

- merge count by lane;
- selected part count and bytes per merge;
- output part count and bytes per merge;
- selection fingerprint in dispatch order;
- queue latency and active merge duration;
- maximum concurrent merges;
- final part count and size histogram;
- merge count, selected bytes, output bytes, and duration grouped by sampling classification and reason;
- merge queue depth within each epoch, unmerged-part inventory, oldest-part age, publication-to-idle time, and cooldown
  drain time for the serialized full-loop run.

### Pipeline

- traces evaluated, retained, dropped, and oversized-bypassed;
- traces retained by duration, error, tag, and healthy-hash rules, plus traces reaching each rule;
- plugin calls and traces per call;
- grace-bypassed merge and trace counts;
- metadata-estimated staging bytes, resource-derived staging hard limit, adaptive decision-batch limit, planned batch
  count, and effective trace-count limit;
- actual batch bytes, traces, flush reason, peak staged bytes, and peak concurrent staged bytes;
- fragment-guard candidate parts, Bloom probes, deferrals, and budget exhaustion;
- plugin errors, panics, timeouts, and malformed verdicts;
- sampled and unsampled merge counts, including every unsampled reason.

The benchmark plugins must export final call, evaluated-trace, and decision checksums so the monitor can reconcile their
work with engine counters and the compiler cannot eliminate benchmark-only decision calculations.

## Profiles

Collect separate primary and cooldown profiles for each variant rather than combining the phases:

- CPU profiles for the primary interval and cooldown drain;
- heap-in-use profiles at primary peak and cooldown peak;
- allocation profile;
- block and mutex profiles;
- runtime execution trace for one representative run;
- operating-system I/O and peak-RSS samples.

Use one measured operation per process. Run at least five processes per variant, alternating variant order. Report median
and p95, not only the fastest run. Record commit, Go version, kernel, filesystem, storage device, `GOMAXPROCS`, cgroup CPU
and memory limits, merge concurrency, and whether the filesystem clone used reflink or a full copy.

Warm-cache and cold-cache results must not be mixed. The default report should use a fresh fixture clone and unchanged
host cache policy for every process. An additional warm-cache series is useful for CPU attribution.

## Correctness Gates

The performance result is rejected unless all gates pass:

1. Baseline and always-retain plugin runs end with the same trace, span, and tag counts as the generated input.
2. Their order-independent logical row checksums equal the input checksum. Physical output bytes need not match.
3. Every input part selected for a merge is removed exactly once and every output part is introduced exactly once.
   The same condition applies to every matching secondary-index part.
4. No trace ID is emitted in more than one logical block inside a merged output part.
5. The plugin receives exactly one batch entry per trace ID for each merge in which that trace participates.
6. The plugin's `MinTS` and `MaxTS` equal bounds independently reconstructed from all selected fragments of the trace.
7. No plugin call occurs for a hot selection. The controlled all-mature MERGE round must execute the plugin. In the
   full-day integration run, the cooldown FINALIZE round must execute the plugin over at least one cooled part and report
   only mature inputs; a replay containing only grace bypasses is invalid.
8. There are no plugin failures, timeouts, malformed verdicts, oversized-trace bypasses, or guard-budget failures unless
   a separate fault scenario intentionally requests them.
9. The merge loop reaches the defined idle state without leaked in-flight parts or references.
10. Secondary-index queries over the final output return the same logical rows as the generated fixture for the
    always-retain variants.
11. For DeterministicDrop, the final core and every secondary index exclude exactly the confirmed-drop ledger and retain
    every other expected row. The plugin uses the fixed 35% threshold without a result-selected salt; its observed
    full-fixture decision count and checksum are frozen before profiling. Every selected ID not confirmed for deletion
    must reconcile to no evaluation, grace bypass, or fragment-guard retain.
12. The source downloaded shard and immutable generated fixture remain unchanged.
13. The monitor event count equals the number of completed core file merges, every event has one sampling
    classification, and aggregate event totals reconcile with engine pipeline counters.
14. Every publication occurs only after the preceding epoch-aware merge-idle barrier. Logical time remains fixed from
    publication until the dispatcher has observed that publication's snapshot epoch and all recursively eligible merge
    work has drained.
15. The primary phase reaches merge-idle at the logical day boundary before the clock advances. Cooldown has independent
    counters and profiles, and primary plus cooldown reconciles with end-to-end part, row, byte, and merge totals.
16. Every scheduled part moves exactly once from the staging directory through an atomic same-filesystem rename. Its
    checksum is unchanged, the external controller never appears in data-node profiles or cgroup counters, and all
    controller lag and errors are reported separately.
17. Pipeline-disabled and AlwaysKeep runs have identical ordered primary-phase MERGE selection fingerprints and logical
    outputs. The plugin-only FINALIZE event is excluded from this fingerprint comparison. Dropping variants may diverge,
    but their first point and subsequent trajectories are recorded.

## Comparison and Acceptance

First report absolute values and run-to-run variance for the pipeline-disabled revision under test. Only after that
baseline is stable should every plugin variant report absolute values and percentage change from it for wall time, CPU,
allocation, peak RSS, read bytes, write bytes, throughput, and amplification. Normalize full-loop results by generated
input bytes, traces, and spans because the picker trajectory may differ.

Do not establish a pass/fail regression threshold from the first run. First collect at least five stable baseline and
plugin samples and calculate run-to-run variance. A later CI or release gate can use a threshold greater than both the
observed variance and an agreed product budget.

Reports should explicitly separate:

- the current pipeline-disabled baseline and its repeatability;
- trace assembly and plugin overhead from the controlled first-round comparison;
- total system impact from the full merge-loop comparison;
- AlwaysKeep framework overhead from DeterministicDrop pruning work and real plugin logic;
- hot grace-bypass work from mature plugin-evaluated work.

## Implementation Plan

1. Freeze the revision-under-test commit, benchmark binary, pipeline-disabled baseline contract, and artifact hashes.
2. Add the merge-maturity clock seam plus explicit merge-idle and trigger seams without changing production defaults.
3. Add the mature and structural source validators, both closure allowlists, shard-local secondary-index validation, and
   immutable source and observed-write-batch manifests.
4. Add the hybrid semantic catalog, independent 5,202-template copy selector, timestamp mapper, liaison part encoder and
   isolated data-node receive fixture builder, whole-fragment batch-template packer, and trace, sampler-ratio, core-byte,
   and index-byte fixture gates.
5. Add the minimal AlwaysKeep and trace-ID-hash DeterministicDrop native benchmark plugins.
6. Add the serialized external publisher, epoch-aware merge-idle barrier, mature-selection dispatch barrier, discovery
   pass, and full merge-loop harness.
7. Add logical checksumming and selection/event recording.
8. Replay exact mature baseline selections offline through the actual sampler, freeze the selection manifest, and report
   `R_merge_verdict` alongside the fixed 35% `R_calibrated` framework case.
9. Add per-merge sampling classification and resource-delta monitoring.
10. Add the pinned, resource-limited data-node Docker runner, separately pinned external controller, and data-node-only
    cgroup v2 collector.
11. Add pipeline-disabled controlled-round, single-worker attribution, serialized production-concurrency full-loop, and
    plugin-only unit benchmark entry points.
12. Add per-variant profiling commands and a machine-readable JSON report.
13. Run a short pre-roll and mature-selection smoke fixture first, then generate and validate the full 24-hour fixture.
14. Run and correct the current pipeline-disabled baseline before enabling the plugin.
15. Iterate through framework profiling and fixes, plugin unit optimization, and final SkyWalking integration in order.

## Known Interpretation Risk

Publishing a 24-hour backlog and then merging it all at once makes old small parts eligible for the plugin, which is not
the same as steady ingestion. Therefore the primary full-loop run advances through every scheduled publication and
drains its picker epoch before moving logical time. A backlog stress mode may still be useful, but it must be labeled
separately and must not be used to claim normal two-hour-grace behavior.
