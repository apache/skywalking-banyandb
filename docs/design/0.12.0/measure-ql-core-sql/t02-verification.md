# T02 shared batch execution verification

Status: the necessary review fixes are implemented and functionally verified.
Performance acceptance is incomplete: v12 passes 16 of 18 small-query checks,
while the QL raw and aggregate p50 intervals cross the original limit. The large
matrix was held under the stopping rule. A fixed CPU-placement diagnostic did
not consistently reduce variation. The v10 result below is a historical checkpoint,
not proof for the current source. See the v12 outcome and the later direct
comparison at the end of this file. The direct comparison found no material
v10/v12 latency difference; the user stopped the fresh acceptance matrix after
ten complete pairs. No complete current-source acceptance verdict is claimed.

Candidate v10 clears all 36 p50, p95 and
allocated-byte checks across the complete small/large matrices: fifty containers
and 300,000 measured requests, using the original limits and fresh PR-compatible
reference runs. The largest 95% interval upper bounds are 1.070804 for p50
(limit 1.10), 1.087325 for p95 (limit 1.15), and 1.019249 for allocated bytes
(limit 1.10). No performance check remains inconclusive.

The native compatibility tables pass 147 cases in each topology; all four protected RPC
cases, focused unit/race suites and scoped lint pass. Candidate cleanup counters
return to baseline, actual cgroup limits are verified, and no memory-limit/OOM
events occur. See the [complete v10 result](#v10-complete-matrix-outcome) and
`.omx/bench-reports/qlb-t02-resolution-v10/` for the final proof. Earlier failures
and inconclusive outcomes below are retained as historical checkpoints.
The prerequisite review fixes are committed as `b0a4dc14`; the T02 shared batch
implementation is committed as `c10d3ac2`. This does not change the performance
acceptance outcome recorded above.

## Execution boundary

Native protobuf queries and legacy measure QL use the existing vectorized native
operators. `DistributedPlan.ExecuteBatches` exposes their output before the final
protobuf iterator. QL consumes that output through `ql.NativeScan`; native callers
use the same result's final iterator. Standalone execution exposes the equivalent
boundary, including native multigroup merging and hidden-column projection.

The shared liaison entry retains authorization, group acquisition, revision gates,
time-range preparation, metrics, tracing and status handling. Batch results have
an explicit schema and idempotent Close. Batches are borrowed until the next pull
or Close. Retained QL views snapshot descriptors and values, including repeated
columns. The raw-frame emitter independently retains its first borrowed batch
before asking for the next one.

No protobuf source, generated API contract, persisted format or frame encoding
format changes. Native fanout still receives complete frames eagerly; the new
result handle does not make that path streaming.

T02 supplies execution infrastructure; the new raw measure grammar is delivered
by [T03](t03-verification.md). Its source shorthand, aliases, timestamp projection,
`WHERE time`, `now()` and fixed `INTERVAL` syntax, pagination behavior and current
execution limits are documented in the
[BydbQL user reference](../../../interacting/bydbql.md#55-raw-measure-query-extensions).
T05 adds transparent compiled-plan reuse without adding query syntax.

## Resource contract and limits

One configured execution tracker is shared by liaison ingress, merge operators,
final protobuf materialization and response encoding. A Go-only context holder
lets the processor initialize the tracker with its configured limit while the
caller accounts final encoding against the same tracker. Response reservations
remain held through the existing transport scope. Data-node frame coalescing,
conversion, encoding and transport copies are reserved before allocation, and the
encoded output stays charged after iterator Close until the node finishes sending.
Direct frame-emitter callers retain their reservation until iterator Close.
Execution sources retain their reservations through owner pins; Close releases
execution resources.

Storage decode reservations connect to the same local query tracker. Selected
index allocations use a context scope that survives source closure while any
output owner remains. Reservations precede selected buffer allocation or reuse;
oversized pooled capacities are discarded before attaching them to the query.
Existing node-wide storage protection remains active.

The existing parent admission lease is monotonic. Returning execution buffers
does not refund spent parent charges. Transport completion releases the parent
reservation. The benchmark reports tracker-originated parent charges explicitly;
it does not present them as a complete count of admission overhead.

Native receive guards reserve all pending streams before opening them. Measure
fanout uses a cached connection with static 64 KiB receive windows, retaining the
publisher's TLS, credential reload and node lifecycle. This bounds transport
buffering without reserving the ordinary connection's possible 16 MiB adaptive
window. Other queue traffic retains its existing connection policy.

The guard divides a conservative message allowance among pending streams. With
at most 32 MiB remaining, messages are capped at 32 KiB with 256 KiB workspace per
stream; larger allowances use 2 MiB workspace to cover gRPC's larger buffer-pool
tiers and partition the remaining allowance. A skewed response can exceed its
individual cap even when sequential receipt would fit. This is not an adaptive
streaming memory policy. Small-query success under the existing 8 MiB fallback
admission allowance is part of native compatibility verification. The explicit
16/256 MiB table requires small-query success and oversized-query rejection or
the independently verified complete result.

The benchmark embeds liaison and data nodes in one process. Its process peak
reservation counter sums independent node trackers, including conservative
allowances; it is neither a single-node peak nor a measurement of actual heap
residency. The configured 16/256 MiB limit applies independently at each executing
node. A Go context and its accounting objects do not cross the cluster wire.

## Correctness evidence

The dedicated query cases cover raw/aggregate equivalence, hidden/empty
projections, multigroup results and replica deduplication in both topologies.
The protected endpoint table covers denied groups, already canceled clients,
revision gates, planning failure and recovery, completed execution traces, budget
rejection and post-transport cleanup. Resource observations wait for transport
completion outside measured query latency.

Intermediate verification exposed and fixed two ownership bugs: standalone
pipeline outputs leaked owner pins, and the raw frame emitter retained a borrowed
batch across a pull without retaining its owners. The latter corrupted native
series identity when many series shared one timestamp. The expected row counts
remain independent of the implementation.

Recorded RED evidence includes the standalone multigroup test leaving eight
owner pins (`qlb-t02-e2e.log:6409`), native compatibility teardown leaving 434
cursor references (`qlb-t02-native-compat-r3.log:53806`), and a distributed tiny
multigroup query exhausting the fallback admission allowance
(`qlb-t02-distributed-final2.log:7984`). The latter used:

```sh
go test ./test/integration/distributed/query -run TestPropertyQuery \
  -ginkgo.label-filter=QLB-T02 -count=1
```

All have subsequent passing integration coverage below. There is no recorded
preimplementation RED execution for the initial seam contract. The final frame
egress gap was identified by ownership inspection; its new regression tests were
first executed with the fix. Neither is presented as an executed RED test.

Verified checks (Go 1.26.9; host tests pinned to CPUs `10-15,26-31`):

| Check | Result | Log under `/mnt/d/` |
| --- | --- | --- |
| Native golden and T02 query tables, both topologies | 147/147 standalone; 147/147 distributed | `qlb-t02-native-egress-final.log` |
| Protected RPC table, 16/256 MiB in both topologies | 4/4 after frame-egress fix | `qlb-t02-gate-egress-final.log` |
| Full vectorized/measure race suite, including frame-egress lifetime | Pass | `qlb-t02-frame-egress-full.log` |
| Full dquery, liaison/grpc, vectorized/measure, frame and plan packages | Pass | `qlb-t02-egress-packages-final.log` |
| Allocation, selected-index, QL view and transport-ownership race tests | Eight packages pass | `qlb-t02-owned-focused-race.log` |
| Benchmark harness unit checks | Pass | `qlb-t02-harness-final3.log` |
| Full storage measure/query suites | Pass | `qlb-t02-pool-integration.log` |

The final native/T02 table and protected endpoint commands were:

```sh
go test ./test/integration/standalone/query ./test/integration/distributed/query \
  -v -count=1 -timeout=20m -ginkgo.focus='vec independent verification' \
  -ginkgo.fail-fast -ginkgo.fail-on-empty
go test -v ./test/integration/standalone/other/rbac_data \
  -run TestRBACDataWorkflow -ginkgo.focus 'T02 gate-error-cancel-budget' \
  -ginkgo.no-color -count=1
go test ./banyand/dquery ./banyand/liaison/grpc \
  ./pkg/query/vectorized/measure/... -count=1
go test -race ./pkg/query/vectorized/measure -count=1
```

The final replica workspace adjustment is covered by an exact reservation
regression and the full integration tables: decoded frame residency is charged
per frame, while the merger's output capacity is charged once per group.
Affected package lint reports zero issues. Protobuf sources and generated API
files have no diff.

Large storage semantic fixtures now include their retained final protobuf output
in the explicit test allowance. The mixed fixture retains a dedicated 64 MiB
rejection-and-cleanup check; its successful result checks use 256 MiB. The OAP100
synthetic fixture requires a larger, test-only 1 GiB allowance. At 256 MiB it
rejected a charge with 267,455,136 bytes already reserved and 8,935,312 additional
bytes requested. Production limits and expected query results are unchanged.
The full storage package rerun passes, including the retained-response rejection
check. Size-classed metadata and column pools preserve the same acquisition
capacity limits while preventing small result batches from discarding reusable
native scan slabs. A focused alternating 16,384/16-row regression allocates zero
objects after warmup and checks that growth returns the original pool checkout.
The full native and T02 integration rerun after this performance fix passes.
The later cached-lease lookup optimization passes the full vectorized race suite;
it preserves cancellation, failed-charge rollback and monotonic parent charging.
Copies of the final logs are retained in
`.omx/bench-reports/qlb-t02-logs/verification/`.

## Benchmark protocol

The `qlb_t02` matrix has six real RPC variants: native/QL raw queries, native/QL
aggregation, and native/QL wide hidden-column projection. Each runs 20 warmups and
1,000 measured requests with one worker, at 1,024 and 100,000 input rows. The wide
fixture has sixteen integer fields. Results are checked against independently
constructed values before timing and at sampled points after profiling.

Setup waits for all sixteen service partitions using narrow raw projections.
This verifies the whole written dataset without requiring a 100,000-row protobuf
response merely to establish readiness. These waits and result hashing are
outside timing. Each cardinality has a fresh cluster and process. The scenario
deadline is forty minutes plus its per-query timeout; the per-query timeout is
unchanged.

The reference checkout is `b0a4dc14`, whose production code is identical to PR
#1412's `012115c56652691d4163f69d0460dfa9b064191f`. It receives the same benchmark
harness and test schema. Reference-only counters that did not exist on that
revision are explicitly unavailable.

Five repetitions run in separate Docker containers, each limited to four logical
CPUs and 8 GiB, with swap disabled beyond that memory limit. The five containers
are co-scheduled on disjoint physical-core pairs and their SMT siblings:
`0,1,16,17`; `2,3,18,19`; `4,5,20,21`; `6,7,22,23`; `8,9,24,25`.
Reference and candidate use identical assignments. Host verification is pinned
to the remaining cores. This is a co-scheduled comparison, not an unloaded-host
latency claim. Reports retain all five observations and their variation.

The fixed dataset time base is `1792037072818714860` nanoseconds. Commands and raw
logs are retained in `.omx/bench-reports/qlb-t02-logs/`, including
`run-comparison.sh`. Reports and CPU/heap profiles are retained under
`.omx/bench-reports/qlb-t02-{reference,candidate-v4}-r{1..5}/`.

Two incomplete reference attempts are preserved separately: one exposed a
missing wide-fixture index binding; the other exceeded the old scenario deadline.
Neither contributes to the five-run comparison. No sample count or timed query
was reduced to address the timeout.

All five reference repetitions completed: twelve variant/cardinality records and
12,000 measured successful RPCs per repetition. The initial candidate completed
the 1,024-row matrix in all five repetitions, exposing median allocation ratios
of 2.671x for native aggregation and 2.652x for QL aggregation. Heap differences
identified capacity eviction in the shared metadata and column pools. Those runs
were stopped during the larger matrix once the bounded size-class reuse fix was
verified. Their completed measurements, profiles and source archive remain in
`qlb-t02-candidate-r{1..5}` and the logs directory; they are not acceptance runs.

The second candidate fixed allocations but retained a 1.127x median small-query
aggregation p50 ratio. Its CPU profile attributed repeated context lookups to
tracker lease charging. That lookup now occurs once per tracker; unadmitted
trackers avoid the unused charge hook. These superseded runs are preserved under
`qlb-t02-candidate-v2-r{1..5}` and are not acceptance runs.

Five third-candidate repetitions completed under
`qlb-t02-candidate-v3-r{1..5}`, each with all twelve variants and 12,000
measured successful RPCs. All median ratios satisfy the proposed numerical gates. `candidate-v3-production-sha256.json` and
`candidate-v3-production.tar.gz` preserve their production source.
`compare.py` (with `QLB_CANDIDATE=candidate-v3`) validates checksums, sample counts, profiles and
resource deltas, then reports median ratios and an exact paired bootstrap
interval over the five CPU-set repetitions. The interval does not eliminate
shared-host noise or turn five observations into a large independent sample.

The third candidate's small-data median ratios satisfy the proposed gates, but
the native raw control's initial 95% interval crosses the p50/p95 thresholds
slightly. A predeclared confirmation completed all six 1,024-row variants in five
baseline/candidate pairs, alternating revisions on the same `10,11,26,27` CPU
set. It uses the same schema, time base, warmups, samples, limits and profiles;
the larger runs continued on their disjoint CPU sets. Its reports are retained
under `qlb-t02-{reference,candidate-v3}-confirm-r{1..5}`. The original observations
remain part of the record; the confirmation does not replace them.

The third candidate and its confirmation both met all median thresholds. A
stratified paired bootstrap retaining all ten small-data observations still put
the native raw p50 ratio's upper bound at 1.101, marginally above 1.10. Those
results are recorded as inconclusive rather than accepted. All large-data
intervals met their thresholds. Third-candidate raw reports, profiles and
comparisons remain available, including `qlb-t02-comparison-v3.*`,
`qlb-t02-confirmation.*` and `qlb-t02-small-combined.json`.

A final ownership check found that data-node encoded frames outlived their input
reservations between iterator Close and transport completion. The fourth
candidate adds a separate preallocation bound for that interval, with tests for
success, cancellation, low-budget rejection and direct-iterator cleanup. It
changes only the frame emitter and iterator relative to the third candidate.
Five complete replacement candidate runs completed under
`qlb-t02-candidate-v4-r{1..5}`; their source is preserved in
`candidate-v4-production-sha256.json` and `candidate-v4-production.tar.gz`.

## Final candidate results

All five final candidate repetitions completed all twelve variants, giving 60,000
measured successful requests and 120 CPU/heap profiles. The reference has the same
coverage. Every result matches its independent expected row count and checksum.
All production hashes still match the fourth-candidate snapshot.

The ratios below divide the candidate median by the reference median. Brackets
show the exact paired bootstrap 95% interval. A numerical median within budget
is not accepted when its interval crosses the limit. These five co-scheduled
observations do not remove shared-host or run-to-run uncertainty.

| Input rows | Variant | p50 ratio [95% interval] | p95 ratio [95% interval] | Allocated bytes ratio [95% interval] |
| ---: | --- | --- | --- | --- |
| 1024 | native_raw | 1.069 [1.059, 1.108] (inconclusive) | 1.073 [1.048, 1.107] (pass) | 1.006 [1.001, 1.012] (pass) |
| 1024 | ql_raw | 1.083 [1.062, 1.122] (inconclusive) | 1.095 [1.031, 1.174] (inconclusive) | 1.008 [1.002, 1.012] (pass) |
| 1024 | native_aggregate | 1.078 [1.043, 1.093] (pass) | 1.042 [1.021, 1.092] (pass) | 0.974 [0.972, 0.986] (pass) |
| 1024 | ql_aggregate | 1.094 [1.061, 1.114] (inconclusive) | 1.093 [0.988, 1.114] (pass) | 0.972 [0.969, 0.978] (pass) |
| 1024 | native_wide_hidden | 0.680 [0.670, 0.684] (pass) | 0.728 [0.705, 0.754] (pass) | 0.655 [0.655, 0.656] (pass) |
| 1024 | ql_wide_hidden | 0.691 [0.683, 0.694] (pass) | 0.726 [0.694, 0.751] (pass) | 0.657 [0.656, 0.657] (pass) |
| 100000 | native_raw | 1.071 [1.055, 1.095] (pass) | 1.039 [1.035, 1.044] (pass) | 1.005 [1.001, 1.017] (pass) |
| 100000 | ql_raw | 1.056 [0.983, 1.071] (pass) | 0.995 [0.970, 1.016] (pass) | 1.004 [1.001, 1.015] (pass) |
| 100000 | native_aggregate | 1.014 [0.995, 1.040] (pass) | 1.011 [1.003, 1.019] (pass) | 0.996 [0.989, 1.022] (pass) |
| 100000 | ql_aggregate | 0.971 [0.927, 1.009] (pass) | 0.972 [0.953, 0.981] (pass) | 0.996 [0.988, 1.020] (pass) |
| 100000 | native_wide_hidden | 0.393 [0.388, 0.398] (pass) | 0.485 [0.481, 0.498] (pass) | 0.485 [0.483, 0.485] (pass) |
| 100000 | ql_wide_hidden | 0.396 [0.393, 0.402] (pass) | 0.485 [0.479, 0.500] (pass) | 0.484 [0.484, 0.484] (pass) |

All median ratios in the original co-scheduled matrix meet G1's numerical limits,
but the marked small-data intervals cross their thresholds. That comparison was
recorded as inconclusive. The sequential rerun below gives the latest decision. Completed
third-candidate and confirmation results are preserved as prior evidence, not
pooled into the fourth candidate's acceptance comparison.

The final candidate's resource observations below take the maximum across both
native/QL paths and all five repetitions. Peak reservation sums independent node
trackers in the benchmark process; it is not per-node heap residency. Parent
charges are cumulative tracker-originated charges per request and are not
refunded when buffers are released.

| Dataset rows | Workload | Process peak reserved MiB | Tracker parent charges MiB/request | Source rows/request | Received frame rows/request | Result rows/request |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1,024 | raw | 32.78 | 29.30 | 128 | 128 | 64 |
| 1,024 | aggregate | 36.63 | 28.13 | 2,048 | 32 | 16 |
| 1,024 | wide_hidden | 38.20 | 31.82 | 128 | 128 | 64 |
| 100,000 | raw | 112.20 | 67.98 | 12,600 | 12,600 | 6,300 |
| 100,000 | aggregate | 516.80 | 28.13 | 200,000 | 32 | 16 |
| 100,000 | wide_hidden | 231.13 | 122.17 | 12,600 | 12,600 | 6,300 |

Every variant records two native leaf calls per request, zero intermediate
DataPoints, exactly the expected final DataPoints, and zero post-close live-result,
reservation and owner-pin deltas after 1,000 requests. Source rows count batches
emitted by storage; physical rows scanned, total parent-admission overhead and
planning/cache counters are explicitly unavailable. No planning speedup is claimed.

G2's exercised 16/256 MiB RPC cases pass with the expected complete result or
ResourceExhausted, including cleanup after error/cancellation. The final frame
egress lifetime also passes focused and race tests. G3's work and cleanup counters
pass across the complete benchmark matrix. The remaining acceptance limitation is
the unstable G1 latency comparison; no subsequent ticket has been started.

Reproduce the final artifact validation and comparison from the retained logs:

```sh
QLB_CANDIDATE=candidate-v4 python3 .omx/bench-reports/qlb-t02-logs/compare.py
```

Raw reports, profiles, source snapshots and failed/superseded attempts remain
under `.omx/bench-reports/`; they are local artifacts, not tracked source files.


## Sequential Docker rerun of the small-query comparison

At the user's request, all six 1,024-row variants were rerun in five fresh
baseline/candidate pairs. Only one benchmark container runs at a time, with no
concurrent host verification jobs. Each has a four-logical-CPU quota on
`10,11,26,27` (two physical cores and their SMT siblings), an 8 GiB memory limit,
no additional swap, and the same Go 1.26.9 image. Pair order is baseline/candidate
for odd pairs and candidate/baseline for even pairs. Unrelated host services are
unchanged; CPU pinning does not provide exclusive ownership of the host CPUs.

Production remains the frozen fourth candidate. Dataset time base, all six
variants, 20 warmups, 1,000 timed requests per variant, one worker, profiles and
256 MiB local query limit are unchanged. Baseline counters remain explicitly
unavailable. The timed harness and data are identical; differing harness lines
are comments and unexercised invalid-configuration error messages.

`qlb-t02-logs/serial-rerun-protocol.json` records comparison settings, source hashes,
image ID and run order. `run-serial.sh` and `run-serial-inner.sh` preserve commands.
Each report's `resources/` directory captures actual cgroup CPU quota, allowed
CPUs, memory/swap limits, CPU statistics and memory/OOM events before and after
execution. These container statistics include compilation and setup, not only
the measured query intervals. Docker HostConfig records are retained alongside
the raw logs.

Artifacts use `qlb-t02-{reference,candidate-v4}-serial-r{1..5}`. All ten containers
completed, yielding 30,000 measured requests per revision and 120 CPU/heap
profiles in total. Every variant has the correct row count/checksum; the final
candidate has zero intermediate DataPoints and zero cleanup deltas. All 796
frozen production hashes remain unchanged.

`qlb-t02-serial-resources.json` verifies non-overlap and resource enforcement for
all ten containers. No CPU quota throttling, memory-limit events or OOM events
were observed. The maximum cgroup memory peak was 652.51 MiB, including compilation
and setup, against the 8 GiB limit.

The comparison retains all five serial pairs and uses the same exact paired
bootstrap as the original analysis. These runs are compared with each other;
they are not substituted into or pooled with the original co-scheduled samples.

| Input rows | Variant | p50 ratio [95% interval] | p95 ratio [95% interval] | Allocated bytes ratio [95% interval] |
| ---: | --- | --- | --- | --- |
| 1024 | native_raw | 1.071 [1.032, 1.092] (pass) | 1.097 [1.026, 1.125] (pass) | 1.003 [0.999, 1.005] (pass) |
| 1024 | ql_raw | 1.056 [1.037, 1.096] (pass) | 1.071 [0.955, 1.096] (pass) | 1.003 [0.999, 1.008] (pass) |
| 1024 | native_aggregate | 1.062 [1.028, 1.100] (inconclusive) | 1.001 [0.968, 1.077] (pass) | 0.973 [0.967, 0.977] (pass) |
| 1024 | ql_aggregate | 1.067 [1.033, 1.072] (pass) | 1.105 [0.889, 1.190] (inconclusive) | 0.973 [0.967, 0.981] (pass) |
| 1024 | native_wide_hidden | 0.658 [0.645, 0.668] (pass) | 0.730 [0.713, 0.735] (pass) | 0.655 [0.654, 0.656] (pass) |
| 1024 | ql_wide_hidden | 0.663 [0.649, 0.673] (pass) | 0.718 [0.702, 0.740] (pass) | 0.657 [0.656, 0.657] (pass) |

The raw-query p50/p95 and wide-query comparisons now satisfy their interval
limits. Two aggregate metrics remain inconclusive:

- Native aggregate p50: baseline median 1.837 ms, candidate median 1.951 ms;
  ratio 1.062058, interval [1.027927, 1.100497]. The upper bound is a 10.0497%
  increase against the 10% limit; the three-decimal table rounds it to 1.100.
- QL aggregate p95: baseline median 3.407 ms, candidate median 3.766 ms;
  ratio 1.105371, interval [0.889471, 1.189913]. The upper bound is an 18.9913%
  increase against the 15% limit.

Every observed median is within its proposed gate, and all allocation intervals
pass. **G1 remains inconclusive and T02 remains unaccepted** because the two
aggregate intervals still cross their limits. The sequential Docker rerun is
complete; no production code was changed to obtain these results.

Reproduce validation and analysis using the retained scripts:

```sh
python3 .omx/bench-reports/qlb-t02-logs/validate-serial.py
QLB_REFERENCE=reference-serial QLB_CANDIDATE=candidate-v4-serial \
  QLB_CARDS=1024 QLB_COMPARISON=qlb-t02-serial-comparison \
  QLB_PROTOCOL='five sequential pairs;4CPU/8GiB;pinned CPUs;alternating order' \
  python3 .omx/bench-reports/qlb-t02-logs/compare.py
```

## Investigation of the remaining aggregate comparisons

This investigation explains the inconclusive results; it does not replace or
change the acceptance samples. Production still matches the frozen fourth
candidate. The deterministic replay command is:

```sh
python3 .omx/bench-reports/qlb-t02-investigation/replay-gates.py
```

It exits 1 (RED), reproducing native aggregate p50's interval
[1.027927, 1.100497] and QL aggregate p95's interval [0.889471, 1.189913].
The minimized replay uses only these two measurements from the captured real-RPC
runs. Live feedback remains the unchanged Docker RPC harness; the subsequent
controlled probes use its actual query functions through a test-only Go overlay.

Four initial hypotheses were tested: five-pair sample sensitivity, GC tail
behavior, recurring accounting/ownership cost, and profiling perturbation.
Inspection also revealed a latency-reporting precision limit.

### Native aggregate p50: sample sensitivity and precision

Pair 4 sets the upper bootstrap endpoint: candidate 1.993 ms / baseline 1.811 ms
= 1.100496963. Diagnostic removal of that pair brings the upper endpoint to
1.064429; removing any other single pair leaves it at 1.100497. Exclusions are
used only to identify sensitivity, never to pass the gate. With five observations,
resampling one pair three or more times has probability 5.792%; a percentile
bootstrap tail can therefore be determined by one pair.

The benchmark's `percentile` helper truncates durations to whole microseconds
before reporting milliseconds, and the bootstrap consumes those reported values.
The borderline pair is only 0.9 microseconds above 1.10 times its reported
baseline. Its possible ratio before truncation is [1.099890, 1.101049], crossing
the limit. For example, unrounded medians 1.993000/1.811900 give 1.099950, while
their reported values give 1.100497. The original raw durations were not retained,
so their true endpoint cannot be recovered. This is evidence of insufficient
precision at the boundary, not evidence that the original test passed.

The observed 6.2% median increase remains meaningful; the 0.0497 percentage-point
endpoint excess cannot be interpreted confidently from microsecond-truncated
inputs and five pairs. `quantization.json` and `qlb-t02-gate-diagnosis.json`
preserve the calculation. Future acceptance runs should retain nanosecond samples
and round only for display, using more independent repetitions for a stable tail.

### QL aggregate p95: GC-related tail behavior

Pair 3 sets the upper endpoint (4.223/3.549 = 1.189913). Removing it diagnostically
reduces the endpoint to 1.129146; removing any other pair leaves 1.189913. Native
and QL use the same execution operators, but their separate timing windows capture
different runtime/GC phases.

Three controlled conditions each ran three baseline/candidate pairs in sequential
4-CPU/8-GiB containers: normal GC with profiles, GC disabled with profiles, and
normal GC without profiles. Each ran 20 warmups and 1,000 measured requests for
each of the two aggregate variants. Conditions and pair order rotate by repetition.
The overlay forces one collection before each condition's warmups, applies the GC
setting, and retains per-query nanosecond durations, timestamps and GC history.
These normalization and scope changes make the probes diagnostic-only; their
three-pair results cannot replace the six-variant acceptance matrix.

QL aggregate median-of-three p95 values:

| Diagnostic condition | Baseline p95 ms | Candidate p95 ms | Candidate/baseline | GC cycles baseline/candidate |
| --- | ---: | ---: | ---: | ---: |
| control | 3.646080 | 3.744160 | 1.0269 | 63/59 |
| gc-off | 2.345070 | 2.523690 | 1.0762 | 0/0 |
| profile-off | 3.547390 | 3.797490 | 1.0705 | 68/64 |

Disabling GC reduces candidate QL p95 from 3.744160 to 2.523690 ms (32.6%) and
baseline p95 from 3.646080 to 2.345070 ms (35.7%). GC-off timed windows contain zero
collections. With GC enabled, roughly 59 candidate queries out of 1,000 contain a
GC-cycle completion, including 11 of the slowest 50 queries in the median probe.
That association is enriched relative to the whole sample, but most tail queries
do not contain a cycle completion. Total recorded stop-the-world pause time is
only about 9–10 ms per 1,000 queries; long pauses alone are not established as the
cause. Concurrent marking/assists, scheduling and allocation/pool behavior remain
part of the GC-related effect.

The tail persists with profiling disabled (candidate p95 3.797490 ms), so CPU
profiling is not necessary to produce it. These probes do not prove zero profiler
perturbation. GC-off also changes heap growth and `sync.Pool` retention; it is a
diagnostic intervention on the whole embedded client/liaison/data-node process,
not a proposed production setting or a localization to one node.

### Persistent CPU cost and allocation evidence

Across the five original CPU-profile pairs, total sampled process CPU/query
increases 2.59% for native aggregate and 1.45% for QL aggregate. Candidate-only sampled
cumulative means are 38/68 microseconds per query in `ReserveAllocation` and 28/20
microseconds in `ReserveAllocationOnce` (native/QL respectively). These helpers
perform cancellation/context checks, locking, budget reservation and reusable-buffer
lookup. Inclusive costs contain callees; nested memory-tracker frames must not be
added to them. Ownership-release samples are too sparse for strong attribution.

This identifies recurring bookkeeping as an optimization target, but sampled
process CPU cannot explain exact RPC p95 or be added directly to wall latency.
The candidate allocates about 2.7% fewer bytes per aggregate query and performs
fewer collections in the acceptance runs, so there is no shared increase in GC
frequency. GC worker CPU decreases for native and rises slightly for QL.

Heap `alloc_space` profiles are process-lifetime snapshots: each later variant
also contains earlier variants' allocations. They cannot be divided by 1,000 to
claim case-specific allocation cost. The acceptance allocation numbers use the
correct before/after `TotalAlloc` deltas; those comparisons remain valid.

### Evidence and outcome

All 18 diagnostic containers completed 36,000 requests with independent expected
rows/checksums. Query counters show zero intermediate DataPoints and zero live
result/reservation/owner-pin deltas. Enforced CPU, memory and swap limits and
non-overlap are validated; all 796 production hashes remain unchanged.

Retained evidence under `.omx/bench-reports/qlb-t02-investigation/` includes
`diagnostic-protocol.json`, the test-only overlay, `run-diagnostics.sh`, raw
per-query and GC records, all profiles, `probe-results.json`,
`aggregate-profile-findings.md` and extraction/analysis scripts. Temporary
instrumentation stays in this explicitly marked, ignored diagnostic directory.

The native endpoint is dominated by one pair and finer than the reported timing
precision. The QL tail is sensitive to GC-related runtime behavior, with only five
independent acceptance pairs. Recurring accounting adds a modest CPU cost. G1
remains inconclusive; no sample is excluded or reclassified as a pass.

The next implementation target is reducing repeated allocation-scope context
lookups and reservation locking while preserving preallocation checks,
cancellation and ownership. A subsequent acceptance run should retain full timing
precision and use enough independent repetitions to resolve the remaining tail.

## Tracked pool evaluation (2026-10-15)

The existing column and MeasureBatch reuse already uses the repository's
`pkg/pool.Register` helpers. Those checkouts participate in the native pool
reference counts and optional stack tracking. This evaluation tested whether
another tracked pool would help the remaining aggregate allocations.

Same-process heap snapshot differences, rather than cumulative snapshots, locate
38–50 MiB of `fstNode` constructor allocations per aggregate interval. The interval
contains 20 warmups and 1,000 measured queries, so these sampled totals identify a
target rather than exact bytes per timed query. Nodes are reused within an FST
iterator, but their small records are allocated again for the next iterator.

Candidate v5 tested `pkg/pool.RegisterBounded[*fstNode]` with a 4 MiB limit on
retained node shells. Transition slices were dropped on return, preserving the
pre-pool buffer sizing. The limit covered shell sizes, excluding pool bookkeeping.
Close, empty results, decode failures, constructor panics and repeated Close all
returned the checked-out records. The exact excluded prototype and its tests are
preserved in `.omx/bench-reports/qlb-t02-pooling/prototype-v5/`. Its archive
`candidate-v5-production.tar.gz` and 796-file manifest
`candidate-v5-production-sha256.json` are in the adjacent `qlb-t02-logs/` directory.
Only `pkg/index/internal/nativeice/paged_fst.go` differed from production v4.

The focused iterator benchmark demonstrates why allocation savings alone cannot
justify the change. These are medians of five host benchmark runs, separate from
the RPC comparison:

| Rows walked | Before ns/op | Pooled ns/op | Before B/op | Pooled B/op | Allocations before/after |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 6,980 | 6,100 | 6,386 | 4,081 | 145/121 |
| 32 | 16,195 | 16,673 | 6,659 | 4,353 | 151/127 |
| 512 | 147,420 | 169,430 | 6,659 | 4,354 | 151/127 |

### Real-query comparison

Five repetitions each ran three versions: the PR production baseline, pre-pool v4,
and pooled v5. All 15 containers ran sequentially on CPUs 10,11,26,27, with a
4-CPU quota, 8 GiB memory and no swap. Each ran the six existing variants over
1,024 input rows, 20 warmups and 1,000 measured requests per variant, one query
worker, profiles enabled, and the existing 256 MiB query budget. Version order
rotated by repetition. The seven shared harness files matched the baseline;
test-only instrumentation observed the new pool before and after timed windows.
The original microsecond reporting precision was preserved for comparability.

All 90,000 requests passed independent row/checksum validation. Each v5 variant
reported zero post-close FST node checkouts, intermediate DataPoints, live
results, reserved bytes and owner pins. Actual cgroup limits and non-overlap were
validated for every container, with no memory-limit or OOM events. All 796 v5
production hashes remained unchanged throughout the comparison.

The direct pooled/pre-pool ratios below use the median of all five runs. Brackets
are exact paired-bootstrap 95% intervals using all five repetitions; no run is
excluded. Allocation bytes and CPU describe the embedded client, liaison and both
data nodes together.

| Aggregate path | p50 ratio [95% interval] | p95 ratio [95% interval] | Allocation bytes ratio [95% interval] | Process CPU ratio | GC cycles before/after |
| --- | --- | --- | --- | ---: | ---: |
| Native | 1.003 [0.975, 1.060] | 1.002 [0.988, 1.053] | 0.949 [0.947, 0.951] | 0.991 | 57/54 |
| QL | 1.036 [0.995, 1.087] | 1.054 [0.993, 1.157] | 0.949 [0.944, 0.951] | 1.010 | 59/56 |

Native aggregate allocated bytes fall from 907,848 to 861,540 per query; QL falls
from 909,843 to 863,215. The roughly 5.1% savings and fewer GC cycles are consistent,
but there is no demonstrated latency improvement. Native latency is nearly
unchanged and QL point estimates rise. The QL intervals include parity, so this
does not establish a definite latency regression either.

Against the PR baseline, v5 also leaves four latency intervals inconclusive:
QL raw p50, native aggregate p50, and QL aggregate p50/p95. Every point estimate
meets its existing limit, but those intervals cross the corresponding limits.
The contemporaneous pre-pool/PR comparisons are retained too; they do not replace
or reclassify the original T02 acceptance runs.

CPU profiles from the first two repetitions show 30–70 ms of node-pool work per
1,000 aggregate requests, about 0.5–1.2% of sampled process CPU, including mutex
work. This is measurable synchronization cost, not attribution of the whole wall
latency change. Extra traversal stores were not isolated as a cause. The compiler
also shows that pre-pool `push` already exceeded the inlining budget, so v5 did
not lose previously enabled inlining.

### Verification and decision

The prototype passed the complete nativeice suite, its race run, scoped lint,
147 native query cases in each topology, and all four protected RPC cases,
including the 16/256 MiB budget checks. Added lifecycle tests cover early/full
walks, concurrent reuse across dictionaries, stale-state clearing, malformed
input, panic cleanup, idempotent Close, retention limits and native pool reference
balance. The initial Close test failed before implementation. Raw logs and the
five before/after microbenchmarks are retained under `qlb-t02-pooling/verification/`.

The extra node pool is excluded: it saves allocation bytes but does not improve
the latency problem that motivated the experiment. The planned large-input v5
acceptance runs were therefore not started. Production was restored exactly to
the frozen v4 manifest, verified across all 796 files; no protocol or format was
changed. Existing native column/batch pooling remains. Two test corrections remain:
the differential iterator helper now closes both iterators, and the posting-bitmap
cancellation test triggers after a processed batch rather than depending on the
number of context checks. After restoration, the complete nativeice suite and
benchmark-harness unit tests pass again, scoped lint reports zero issues, and
`git diff --check` passes. These final restoration logs are retained with the
prototype's verification logs.

Evidence under `.omx/bench-reports/qlb-t02-pooling/` includes
`pool-verification-protocol.json`, `pool-small-resources.json`,
`pool-process-resources.json`, `allocation-findings.md`, `cpu-analysis/`, the
prototype, runner/validation scripts and `restored-v4-production.json`. The adjacent
`qlb-t02-pool-impact.*`, `qlb-t02-pool-small-compatibility.*` and
`qlb-t02-pool-prepool-compatibility.*` files retain all six variants' comparisons.
Original T02 G1 remains inconclusive; this experiment does not accept T02 or
start the next ticket.

## Latency-gate resolution with candidate v6

Candidate v6 changes only `pkg/query/allocation_scope.go` relative to the frozen
v4 production manifest. Repeated reservations for the same reusable buffer use a
cached key and high-water size under the existing mutex. Interleaved keys still
use the full reservation map, and only successful growth updates either record.
Cancellation remains checked on every call. Closed scopes reject even no-growth
reservations, and Close clears the cache as well as releasing the exact reserved
total. No speculative credits, budget-limit changes, new pools or protocol
changes are involved.

A new closed-scope regression failed before this change. Unit and race checks
cover interleaved growth, shrink, failed reservations, cancellation, deadlines,
comparable keys, panic-safe unlocking and concurrent Close. Five focused benchmark
runs show the cache's tradeoff: repeated keys improve from 41.99 to 30.38 ns/op,
while four interleaved keys change from 42.26 to 46.63 ns/op. Both allocate zero
bytes. This is local evidence, not a claim of improved RPC latency.

The affected query/index/vectorized execution packages pass, as do the selected
allocation race checks. Both topologies pass all 147 native/T02 query cases, and
the protected endpoint table passes all four cases including 16/256 MiB budgets.
Scoped query and benchmark-harness lint reports zero issues.

The shared benchmark harness now computes percentiles from full nanosecond
durations and retains the original request-order samples in an optional
`raw_latencies_ns` field. Existing latency fields retain their names, types and
millisecond units; old JSON remains readable. Precision and serialization tests
pass for both the candidate and reference harnesses. Query workloads, timing
windows, GC settings, profiling and correctness checks are unchanged.

The new experiment fixes its sample counts before timing: twenty sequential
baseline/candidate pairs at 1,024 rows and five pairs at 100,000 rows. Every
container uses a 4-CPU quota, 8 GiB memory, no swap, twenty warmups and 1,000 timed
requests per variant. The small matrix alternates version order on CPUs
10,11,26,27. Large pairs run in five disjoint CPU lanes, sequentially within each
pair and alternating order by lane. The two matrices do not overlap. Both versions
use the same eight shared harness files and exact Docker image ID; reference-only
execution counters remain unavailable.

One shared image was replaced by another job before the first container could
start. No timing samples existed. That setup failure is retained, and the verified
Go 1.26.9 replacement is pinned by image ID for the entire comparison. The other
job completed before the small matrix started.

The estimator remains median(candidate runs) / median(reference runs). Its 95%
interval resamples whole paired runs: all 5^5 draws for the large matrix and
200,000 draws with fixed seed 1412 for the twenty small pairs. Individual request
samples verify each reported percentile; they are not treated as independent
benchmark repetitions. The p50/allocated-byte limits remain 1.10 and the p95 limit
remains 1.15. All observations are retained, with no exclusion or stopping when a
gate first passes.

The frozen protocol, source manifests/archive, runner, strict analyzer and
verification logs are under `.omx/bench-reports/qlb-t02-resolution/`. Reports use
`qlb-t02-{reference,candidate}-v6-{small,large}-rN`. The strict analyzer rejects
missing reports, wrong samples, percentile/raw-data disagreement, incorrect
results, missing profiles, inactive batch paths or nonzero cleanup deltas before
evaluating latency. Resource validation checks actual cgroup limits, version
order, non-overlap within the small matrix and each large pair, unchanged source
hashes and identical shared harness files.

All twenty small pairs completed: 240,000 measured requests passed correctness
checks, with zero candidate cleanup deltas. Full-precision results clear the raw
query gates and all p95 and allocated-byte gates. Aggregate p50 remains
inconclusive: native has ratio 1.050740 with 95% interval [1.024193, 1.102081],
and QL has ratio 1.082475 with interval [1.042301, 1.120264]. Both intervals cross
the unchanged 1.10 limit. The large v6 matrix was not started. The complete small
protocol and result are preserved under `v6-small-complete/`; this outcome does
not accept T02.

## Inline iterator buffers: candidate v7

The twenty-pair CPU profiles locate most additional stack-growth samples below
FST node decoding and its small slice allocations. They do not establish that
this explains the whole latency difference. Candidate v7 keeps v6's scope cache
and gives iterator nodes inline backing storage for eight keys and one
destination/output pair. Larger nodes grow normally, and the existing iterator
spare-node reuse remains. Lookup nodes retain their previous layout. This adds
no pool, synchronization or repeated-path bookkeeping, and does not change
index encoding, cancellation or resource-accounting semantics.

Five focused benchmark runs show median allocation counts falling from 145 to
76 for a one-row walk and from 151 to 82 for longer walks. Median times improve
18.5% for one row and 7.1% for 32 rows; the 1.5% full-walk change may be noise.
These host measurements are separate from query acceptance. Tests cover single,
mixed and 256-way transitions, full traversal and spare-node reuse, reopen and
independent iterators. The complete nativeice suite, its race run and scoped lint
pass. Both query topologies again pass all 147 native/T02 cases; all four
protected RPC cases and the affected execution suites pass.

Before a new acceptance comparison, a fixed five-pair diagnostic compares v7
with an overlay restoring the exact v6 FST implementation. It exercises native
and QL aggregate queries over 1,024 rows, with the same 4-CPU, 8-GiB, no-swap
limits, twenty warmups and 1,000 timed requests per variant. All ten containers
run sequentially with alternating version order. This diagnostic is not an
acceptance matrix. Its protocol, source manifests, overlays, scripts and
verification logs are under `.omx/bench-reports/qlb-t02-resolution-v7/`.

All five diagnostic pairs completed, covering 20,000 timed requests with matching
results and zero cleanup deltas. The direct v7/v6 ratios are:

| Aggregate path | p50 ratio [95% interval] | p95 ratio [95% interval] | Allocation count ratio | Allocated-byte ratio | Process CPU ratio |
| --- | --- | --- | ---: | ---: | ---: |
| Native | 0.996175 [0.938601, 1.031358] | 0.956421 [0.897913, 1.028532] | 0.959460 | 0.999777 | 1.003656 |
| QL | 0.997970 [0.981371, 1.040985] | 0.962753 [0.887925, 1.025364] | 0.959707 | 0.999888 | 0.998195 |

These are exact paired-bootstrap intervals over all five whole-run pairs. Fewer
allocations do not produce a demonstrated p50, CPU or allocated-byte improvement.
The prototype is excluded, and no full v7 acceptance matrix was started. Its
implementation and test are retained under `excluded-prototype/`; the production
FST file was restored exactly to v6. This diagnostic does not clear G1.

## Dispatch preparation: candidate v8

Candidate v8 changes only `plan/dispatch.go` relative to v6. A private
`prepareBatchDispatch` helper performs the existing validation, criteria/entity
resolution, analysis and storage-option construction, then returns before the
storage query starts. `DispatchBatches` keeps the same tracker, storage call,
operators, error wrapping, counters and ownership/Close paths. No execution
operators or public request/result contracts change.

Go 1.26.9 objdump shows the active dispatch frame falling from 1,568 to 1,000
bytes. Preparation returns by value without new heap escape sites. Its own
1,120-byte frame is present during preparation and returns before storage;
therefore the smaller storage-phase frame alone does not prove fewer stack
copies or faster queries. Normal and race plan suites and scoped lint pass.
The affected execution suites, 147 native/T02 cases per topology and all four
protected RPC cases pass again.

A separate fixed five-pair diagnostic compares v8 against the exact v6 dispatch
file through a Go overlay, using the same two aggregate variants and resource
limits as the v7 diagnostic. The candidate's 796 production files and all overlay
inputs are frozen before timing. Artifacts are retained under
`.omx/bench-reports/qlb-t02-resolution-v8/`; this diagnostic is not T02 acceptance.

The complete diagnostic passes its result, sample, cleanup, cgroup and frozen
source checks for all 20,000 requests. Its v8/v6 ratios are:

| Aggregate path | p50 ratio [95% interval] | p95 ratio [95% interval] | Allocated-byte ratio | Process CPU ratio |
| --- | --- | --- | ---: | ---: |
| Native | 0.975320 [0.937179, 1.016300] | 0.969122 [0.923712, 0.994996] | 1.000216 | 0.988785 |
| QL | 0.962578 [0.955246, 1.009735] | 0.986542 [0.942204, 1.014089] | 0.999236 | 0.963964 |

The p50 intervals still include parity, so this pilot does not establish a
definite latency improvement. Lower p50 and CPU point estimates in both paths,
the native p95 interval and the reduced dispatch frame justify retaining the
split for a complete acceptance comparison. No fallback decoder change is
included.

The v8 acceptance protocol was frozen before timing: all twenty small pairs
first, then all five large pairs only if every small gate passes. It uses fresh
reference runs, all six existing variants, identical shared harness files,
full-precision samples and the original limits. All planned observations within
each started matrix are retained. No intermediate pass ends a matrix. The
protocol and runner hashes are in `qlb-t02-resolution-v8/protocol.json`.

### V8 small-data acceptance result

All twenty small pairs completed. All eighteen p50, p95 and allocated-byte
gates pass, using all 240,000 timed requests. The values below are candidate /
reference; brackets are whole-run paired-bootstrap 95% intervals. Displayed
values are rounded to six decimals; the verdict uses full-precision JSON.

| Variant | p50 ratio [95% interval] | p95 ratio [95% interval] | Allocated-byte ratio [95% interval] |
| --- | --- | --- | --- |
| native_raw | 1.050957 [1.035754, 1.057052] | 1.030830 [1.014472, 1.070336] | 1.005139 [1.004414, 1.006941] |
| ql_raw | 1.043455 [1.035048, 1.056728] | 1.041830 [1.015133, 1.060878] | 1.006078 [1.004753, 1.007974] |
| native_aggregate | 1.029037 [1.015505, 1.038176] | 1.017177 [0.991967, 1.050833] | 0.973749 [0.969948, 0.977268] |
| ql_aggregate | 1.050530 [1.021849, 1.068632] | 0.983427 [0.949890, 1.034982] | 0.975893 [0.972399, 0.978031] |
| native_wide_hidden | 0.660503 [0.653138, 0.665634] | 0.719868 [0.711861, 0.731828] | 0.654973 [0.654725, 0.655335] |
| ql_wide_hidden | 0.661765 [0.657765, 0.666253] | 0.723813 [0.714285, 0.735167] | 0.656742 [0.656228, 0.657132] |

The worst p50 upper bound is 1.068632, below 1.10; the worst p95 upper
bound is 1.070336, below 1.15. The previously inconclusive native aggregate p50
and QL aggregate p50/p95 checks now pass. The largest allocated-byte upper
bound is 1.007974, below 1.10. All candidate activation counters have their
expected values; intermediate DataPoints and post-close live-result, reserved-byte
and owner-pin deltas are zero for every variant. Completed-container cgroup
records show the enforced limits and no memory-limit or OOM events.

The five-pair, 100,000-row matrix started after this complete small-data verdict.
The small-only report is preserved under `small-complete/` before the combined
analyzer replaces its top-level output. T02 acceptance still requires the large
matrix and complete resource/source validation.

The complete pilot's twenty CPU profiles support a real reduction in stack
work. Mean sampled `runtime.newstack` CPU falls from 170 to 116 microseconds per
query for native aggregation and from 166 to 112 for QL. FST-node-associated
stack-growth samples fall to zero; new preparation samples cost 22/24 microseconds
and are already included in the lower totals. These are whole-process CPU costs
across the client, liaison and two data nodes, not wall-clock latency savings.
Sampling, compiler layout and nonstack work limit causal attribution. Exact
extraction, stack traces and findings are retained under
`qlb-t02-resolution-v8/profile-analysis/`.

### V8 complete-matrix outcome

All fifty containers completed, covering 300,000 timed requests. Correctness,
activation, cleanup, actual cgroup limits, no-swap settings, source freeze and
shared-harness checks pass. All eighteen small-data gates and sixteen large-data
gates pass. Two large-data p50 gates fail; T02 remains unaccepted.

| 100,000-row variant | p50 ratio [95% interval] | p95 ratio [95% interval] | Allocated-byte ratio [95% interval] |
| --- | --- | --- | --- |
| native_raw | 1.126508 [1.114489, 1.139085] (fail) | 1.061055 [1.049718, 1.069597] (pass) | 1.005070 [1.003231, 1.016201] (pass) |
| ql_raw | 1.130638 [1.115296, 1.130864] (fail) | 1.065540 [1.054946, 1.071841] (pass) | 1.006143 [1.002669, 1.015799] (pass) |
| native_aggregate | 1.029249 [1.009063, 1.037298] (pass) | 1.006723 [0.976291, 1.034450] (pass) | 0.998073 [0.994716, 1.018827] (pass) |
| ql_aggregate | 1.005088 [0.992595, 1.033184] (pass) | 1.006668 [0.983153, 1.043470] (pass) | 0.997262 [0.993777, 1.019176] (pass) |
| native_wide_hidden | 0.400705 [0.397292, 0.407452] (pass) | 0.491097 [0.488159, 0.495268] (pass) | 0.484348 [0.484032, 0.484816] (pass) |
| ql_wide_hidden | 0.399713 [0.395443, 0.402721] (pass) | 0.501316 [0.489374, 0.502529] (pass) | 0.484717 [0.484231, 0.485008] (pass) |

The raw responses contain 6,300 rows. All twenty large-raw CPU profiles identify
a new full-response `proto.Size` traversal in `ReserveResponseEncoding`, followed
by gRPC sizing the same response. The accounting traversal adds about 3 ms of
whole-process CPU per query; total process CPU rises about 7.1 ms. Stack growth
adds only 4–36 microseconds, so this does not justify removing the dispatch split.
This diagnosis does not guarantee that removing the duplicate walk alone clears
both gates. Findings are retained under `large-raw-analysis/`.

The complete failed comparison and resource validation are preserved under
`acceptance-complete/`; no runs are excluded or reclassified. A subsequent
candidate must retain encoding admission before allocation and transport-lifetime
reservations while avoiding the repeated response walk.

## Final-response encoding bounds: candidate v9

Candidate v9 extends the final batch serializer's existing memory preflight with
a checked wire-size upper bound. The query scope records the existing pointer
slabs and accepts this bound only after complete serialization, matching row
counts and every final DataPoint identity. Unsupported values, reordered or
duplicate selections, stale coverage and direct legacy responses retain exact
`proto.Size` fallback. Response metadata and unknown fields are sized separately;
the possible QL response wrapper has an eleven-byte allowance. Encoding and
transport copies remain reserved before encoding and held until RPC completion.

Unit tests, race checks and scoped lint pass, including typed-nil responses,
selection ordering and duplicates, repeated constant payloads, overflow,
cancellation, concurrent coverage replacement and transport cleanup. The four
protected real-RPC cases also pass. Native compatibility verification rejects
this candidate: the existing standalone "all of index mode" query returns
ResourceExhausted under the unchanged default admission allowance. No v9 timing
run was started. The frozen 796-file source archive, diagnostic protocol and
runners are retained under `.omx/bench-reports/qlb-t02-resolution-v9/`;
`/mnt/d/qlb-t02-v9-native-e2e.log` records the failure. The conservative estimate
must preserve successful queries that fit using exact sizing before this
optimization can proceed to performance verification.

## Budget-aware encoding bounds: candidate v10

Candidate v10 retains v9's checked bound and identity coverage. Before charging,
it checks whether the bound fits both the live tracker and the parent lease's
remaining cumulative allowance. If either lacks headroom, it computes the exact
protobuf size instead. These checks are advisory; the existing atomic reserve
still makes the admission decision. No speculative parent charge or failed
reservation retry occurs. Concurrent allocations can still cause the final
reservation to fail. The query limit and the two-copy encoding allowance stay
unchanged.

Tests cover exact-fit success and one-byte-short rejection at both limits,
assert that the parent receives only the exact charge, and preserve its
monotonic charge after local release. Normal tests, race checks and scoped lint
pass; the protected real-RPC table passes all four cases. The native tables pass
all 147 cases in each topology, including the standalone index-mode query that
rejected v9. The unchanged v8 dispatch split and repeated-reader
scope cache remain included; the excluded pool and inline-FST experiments do not.

The source archive and both protocols are frozen under
`.omx/bench-reports/qlb-t02-resolution-v10/`. A fixed five-pair diagnostic first
compares the two 100,000-row raw-query variants against fresh reference runs.
The complete fresh twenty-pair small and five-pair large matrices run only
after compatibility and this diagnostic pass. The limits, full-precision
whole-run bootstrap, query workload and container resource limits are unchanged.
The diagnostic cannot substitute for full acceptance, and all runs are retained.

All five diagnostic pairs complete successfully: 20,000 measured requests,
correct results, expected activation counters, zero intermediate DataPoints and
zero post-close live-result, reserved-byte and owner-pin deltas. Actual cgroup
limits, no-swap configuration, source freeze, tool hashes and shared harness
checks pass for all ten containers. The candidate/reference results are:

| Large raw path | p50 ratio [95% interval] | p95 ratio [95% interval] | Allocated-byte ratio [95% interval] | Process CPU ratio |
| --- | --- | --- | --- | ---: |
| Native | 1.060075 [1.055990, 1.064845] | 1.024720 [1.017476, 1.036059] | 1.016147 [1.006446, 1.017983] | 1.054004 |
| QL | 1.059024 [1.054126, 1.062902] | 1.025988 [1.018571, 1.032813] | 1.017475 [1.007161, 1.021433] | 1.054886 |

These exact paired-bootstrap intervals use all five pairs. All six diagnostic
gates pass against the original limits. The raw-query regression is smaller
than v8's failed complete comparison, but the diagnostic does not cover small
queries, aggregates or wide projections. Full acceptance remains required.

All twenty diagnostic CPU profiles confirm that the duplicate full-response
size walk is negligible. Mean cumulative `ReserveResponseEncoding` CPU is
0.052 ms/query for native and 0.058 ms/query for QL, with every run at or below
0.10 ms. V8's corresponding means were 3.074 and 2.980 ms. Total `proto.Size`
CPU is now near the reference's single encoding traversal. These are sampled
whole-process CPU costs, not wall-clock savings; cumulative frames overlap and
must not be added. Total process CPU still exceeds reference by about 5.3–5.6%.
Exact extraction and all observations are under `profile-analysis/`. Profiling
finished before the complete small-data matrix started.

### V10 small-data acceptance result

All twenty pairs complete with 240,000 measured requests. All eighteen latency
and allocated-byte gates pass. Every result, activation and cleanup counter
passes the strict analyzer. Resource verification confirms all forty containers
used 4 CPUs, 8 GiB and no swap, with no memory-limit/OOM events; the source, shared
harness and frozen tools remain unchanged. Ratios are candidate/reference:

| Variant | p50 ratio [95% interval] | p95 ratio [95% interval] | Allocated-byte ratio [95% interval] |
| --- | --- | --- | --- |
| native_raw | 1.008231 [0.997553, 1.024137] | 1.030072 [0.989395, 1.050237] | 1.006191 [1.003094, 1.008611] |
| ql_raw | 1.034163 [1.023701, 1.046712] | 1.021565 [0.998163, 1.031797] | 1.006269 [1.002996, 1.008632] |
| native_aggregate | 1.029509 [1.018710, 1.042016] | 1.015523 [0.994672, 1.039984] | 0.976233 [0.972279, 0.978969] |
| ql_aggregate | 1.027132 [1.003618, 1.041998] | 1.061470 [1.024723, 1.087325] | 0.976186 [0.972228, 0.978088] |
| native_wide_hidden | 0.633916 [0.626430, 0.640478] | 0.688537 [0.682668, 0.697422] | 0.655000 [0.654691, 0.655192] |
| ql_wide_hidden | 0.637704 [0.633402, 0.647346] | 0.690180 [0.674944, 0.708988] | 0.656683 [0.656475, 0.656915] |

The largest p50 upper bound is 1.046712 and the largest p95 upper bound is
1.087325, below the unchanged 1.10/1.15 limits. The largest allocated-byte upper
bound is 1.008632. All twenty pairs are retained. The complete small-only result
and its protocol are preserved in `small-complete/`. The five-pair large-data
matrix started after this complete verdict, followed by the combined verification
below.

### V10 complete-matrix outcome

All fifty containers complete successfully, covering 300,000 measured requests.
The complete strict analysis passes all 36 p50, p95 and allocated-byte gates.
Correctness, expected batch activation, zero intermediate DataPoints, and zero
post-close live-result/reserved-byte/owner-pin deltas pass for every candidate
variant run. Baseline batch counters remain unavailable. All actual cgroup
limits, CPU assignments, no-swap settings, version order, source/tool hashes and
shared harness files pass validation; no memory-limit or OOM event occurs.

| 100,000-row variant | p50 ratio [95% interval] | p95 ratio [95% interval] | Allocated-byte ratio [95% interval] |
| --- | --- | --- | --- |
| native_raw | 1.063205 [1.051328, 1.070804] | 1.031997 [1.013520, 1.034853] | 1.008960 [1.005780, 1.017057] |
| ql_raw | 1.062531 [1.053024, 1.065725] | 1.023527 [1.019857, 1.027837] | 1.007303 [1.005075, 1.018714] |
| native_aggregate | 0.998754 [0.985006, 1.020411] | 1.009515 [0.985185, 1.029706] | 0.994964 [0.993945, 1.019249] |
| ql_aggregate | 1.012113 [0.992364, 1.021448] | 1.010527 [0.986472, 1.023978] | 0.996471 [0.992411, 1.019017] |
| native_wide_hidden | 0.365823 [0.365526, 0.371139] | 0.471594 [0.467489, 0.477589] | 0.484981 [0.484563, 0.485354] |
| ql_wide_hidden | 0.366645 [0.366645, 0.375288] | 0.472695 [0.463066, 0.480832] | 0.484631 [0.484086, 0.485413] |

All eighteen large-data gates pass, including both raw-query p50 checks that
failed v8. Across both matrices, the largest p50 upper bound is 1.070804, the
largest p95 upper bound is 1.087325, and the largest allocated-byte upper bound
is 1.019249. Every observation is retained; no limits were relaxed and no
failed matrix was rerun unchanged. The full comparison,
resource proof, outcome and frozen scripts are preserved under
`acceptance-complete/`. The separate diagnostic adds 20,000 requests and is not
counted toward the 300,000-request acceptance matrix.

The retained implementation combines the allocation-scope repeated-reader cache,
the smaller active dispatch frame, and checked response-size bounds. The latter
keeps exact sizing near memory limits and for uncovered response shapes. It
retains existing pointer slabs until finalization, adding some residency; this
is included in the serializer preflight. Bounds are conservative, so queries
near the allowance may still pay the exact-size walk. No FST node pool, inline
FST-buffer prototype, protobuf contract change or production GC tuning is included.

Native compatibility passes all 147 cases per topology, and the four protected
RPC cases verify denial, cancellation, revision/planning errors, execution traces,
the 16/256 MiB budget table and transport cleanup. Focused normal/race suites
and scoped lint pass. The isolated gRPC race retry resolves the test-runner port
collision; its initial failed log remains retained. T02 meets G1/G2/G3 for the
prescribed workload. T02 remains uncommitted; T03 has not started.

## Opus review fixes and rejected v11 checkpoint

The review is preserved in
`.omx/reviews/t02-claude-opus-20261015T151749Z/t02-opus-review.md`.
Necessary fixes retain the existing per-query limits, finite shared pool,
monotonic parent charges, execution operators and protobuf wire format.

- MEASURE admission initially commits its fixed overhead and grows the shared
  pool commitment atomically with successful charges. The old whole-assignment
  policy remains for other catalogs. A regression at the common liaison caller
  holds 32 transport scopes open: the old code refuses query nine; the new code
  admits all 32 and releases the commitments when the transports close. Growth
  still refuses pool pressure or the hard per-query ceiling without spending a
  failed charge. Protector usage is sampled at admission; growth checks its
  current availability snapshot without sampling the runtime on every allocation.
- Guarded native fanout no longer uses a fixed 32 KiB message cutoff or the
  former 32 MiB workspace-policy discontinuity. An actual public query returning
  a 64 KiB string reproduces the old `65690 vs. 32768` receive refusal. The
  guard reserves fixed stream/parser costs first, then partitions a fixed
  fraction of the remaining allowance for bounded message copies. Reserving
  fixed costs first also permits eight default-budget streams whose windows
  fit, instead of accidentally rejecting them through the fractional policy.
- A measure-only receive codec delegates request encoding to the registered
  protobuf codec and uses the same protobuf decoder for responses. Fragmented
  receive payloads use the repository's `native-query-codec-flatten` buffer pool,
  bounded by the prepaid message cap and released inside decoding. Single-buffer
  messages need no flatten copy. This avoids a speculative 1 MiB default gRPC
  flatten tier for every small stream. Unknown fields, malformed payloads, wire
  compatibility, oversize refusal and pool loan balance have focused coverage.
- The connection window is 16 MiB; each stream retains its 64 KiB static window.
  The connection credit does not add independent payload residency: guarded
  streams prepay their own window, message-read flow-control delta and codec
  workspace. A bounded delayed-gRPC experiment exercises eight concurrent
  64 KiB responses at a configured 1 ms RTT; this diagnostic is separate from
  the full real-query acceptance matrices.
- Operation and global memory-observation counters compile only with
  `querymetrics`. Production no-op implementations leave actual tracker/lease
  enforcement and native pool tracking active. Integration CI and the benchmark
  orchestrator enable the tag; required T02 cases explicitly reject missing
  instrumentation. Both build modes have unit/race and lint coverage.
- Distributed QL tracing completes after final serialization and pipeline Close.
  The final iterator records `rounds` and `size`; an early `QueryTrace()` cannot
  snapshot an unfinished result. Regressions exercise the real distributed
  processor through the QL consumer, pagination, cancellation, serialization
  failure and repeated Close. Protected real RPCs require a completed final
  iterator whose end precedes the distributed parent end.
- Generic integer pool trimming affected actual ingestion encoders. Alternating
  8,192/64-row public integer and byte encoders added about 66 KiB and two
  allocations per pair. Bounded borrowing is confined to decoder paths; the
  original encoder reuse behavior returns to zero bytes and zero allocations
  in three repeated microbenchmarks. Measure column scratch uses the same
  separation between encoder reuse and bounded decoder borrowing.

V11 was rejected before acceptance. Its first tier-aware guard prepaid half the
remaining allowance for each fanout. The two-group `write_mixed` compatibility
case exhausted the default monotonic lease: a final 16,118-byte charge could not
fit after 8,377,422 bytes had been spent against 8,388,608 bytes. Standalone passed
147 cases; distributed stopped after 58 successes and that failure. All six
protected/small-budget RPC cases passed. The candidate-only large diagnostic was
stopped, and no acceptance container was run. The frozen source, partial profiles,
logs and outcome remain in `.omx/bench-reports/qlb-t02-resolution-v11/`.
The bounded codec and revised fanout partition fix the actual cause rather than
raising a memory limit or refunding parent charges.

### Cumulative-charge calibration

A temporary real standalone RPC probe seeded one numeric entity across hourly
segments, with an 8 MiB vectorized allowance and unchanged default admission.
Native and QL returned the same result/status. These are cumulative tracker-to-lease
charges, excluding the separately charged fixed admission overhead.

| Rows | Approximate hourly segments | Result, both callers | Lease charges | Peak live reservations | Live after Close |
| ---: | ---: | --- | ---: | ---: | ---: |
| 1024 | 26 | Correct COUNT = 1024 | 5,278,528 B | 5,210,448 B | 0 B |
| 4096 | 103 | ResourceExhausted | 7,297,008 B | 6,257,008 B | 0 B |

This confirms that a monotonic lease can exhaust before the refundable live
tracker. G2 explicitly permits this outcome; it is calibration evidence, not a
wrong result, leak or permission to refund spent charges. The probe source,
overlay and raw logs are retained with the v12 calibration artifacts. Generic
maintainability suggestions without a demonstrated contract or performance issue
are outside these fixes.


### V12 functional verification and incomplete performance acceptance

The final source is frozen as 802 production Go files in
`.omx/bench-reports/qlb-t02-resolution-v12/candidate-v12-production.tar.gz`.
The reference's 786 production files still match the PR #1412-compatible
comparison checkout. The eight shared harness files match byte for byte.

Both native compatibility tables pass all 147 cases on the final source. All six
protected/small-budget real RPC cases pass, including the new 8 MiB large-frame
and concurrent narrow-read cases and completed distributed QL traces. Focused
unit and race checks pass for admission, receive lifecycle/codecs, both metrics
build modes, tracing, encoding and column scratch. The liaison's full unit/race
suites pass. Scoped lint reports zero issues in normal and `querymetrics` builds.

The full prescribed 20 small-query pairs completed once: 40 fresh containers and
240,000 measured RPCs, with every run retained. All correctness and G3 checks
pass, including zero post-close native flatten-pool references. All allocated-byte
and p95 gates pass; four p50 gates pass. Two p50 checks remain inconclusive:

| Variant | Candidate/reference p50 | Paired whole-run 95% interval | Limit | Result |
| --- | ---: | --- | ---: | --- |
| QL raw | 1.063751 | [0.983900, 1.126625] | 1.10 | Inconclusive |
| QL aggregate | 1.034759 | [0.972406, 1.115575] | 1.10 | Inconclusive |

Neither interval establishes a failure or a pass. The five large pairs were not
started, as the frozen protocol requires every small gate to pass first. The
comparison files, exact intervals and nanosecond samples remain in the v12
artifacts; `small-complete/` preserves the initial analysis. No run was excluded,
no threshold changed, and no unchanged acceptance run was repeated until a pass.

The bounded profile audit found no demonstrated optimization target sufficient
to explain this variation. Counter extraction preserves the same non-padding
machine instructions, calls and stack frames in its reservation/serialization
hot paths. Incremental pool growth beyond the existing admission-time sample
costs about 1–2 microseconds per query in the aggregate profiles. Codec CPU is
about 5–9 microseconds per query including existing protobuf decoding. Candidate
stack-growth CPU is below the contemporary reference for both QL variants.
These sampled CPU observations do not establish a wall-latency improvement.
The audits are retained in `profile-codec-trace/`, the copied metric audit files,
and `.omx/bench-reports/qlb-t02-review-admission-analysis/`.

One separately prescribed five-pair diagnostic used unchanged v12 source with
four distinct physical cores (`8,9,10,11`) instead of two SMT cores
(`10,11,26,27`). The CPU quota remained 4, memory remained 8 GiB, and swap remained
zero. It completed all ten containers and 60,000 measured RPCs; it cannot replace
acceptance pairs. Variation did not fall consistently: QL raw baseline/candidate
p50 coefficients of variation were about 2.6%/12.6%, and QL aggregate about
16.4%/12.6%. Its protocol and every run remain under
`.omx/bench-reports/qlb-t02-resolution-v13-isolation/`.
This diagnostic does not justify another full run on the same environment.

Resource validation confirms enforced quota, CPU placement, Go 1.26.9, 8 GiB
memory, zero swap and no memory-limit/OOM events for all 40 v12 containers and all
ten placement-diagnostic containers. Production source hashes and shared harness
hashes still match both protocol snapshots. These are 40 acceptance-small runs
plus ten diagnostic runs, not a completed small/large acceptance matrix.

The separate bounded network diagnostic on final source completed five
alternating pairs, ten measured batches per variant, with eight parallel 64 KiB
responses and a configured 1 ms RTT. Median batch time was 28.601391 ms with the
64 KiB connection window and 7.118763 ms with the 16 MiB connection window. Both
used 64 KiB stream windows; reservations stayed below the asserted 3 MiB bound
per 8 MiB query allowance and returned to zero. This demonstrates the transport
credit issue under that workload, not a 75% improvement in complete query latency.

At that checkpoint, T02 remained unaccepted pending a reliable latency verdict.
All necessary code fixes were retained as uncommitted work;
protobuf definitions, persisted formats and the accepted monotonic-charge policy
are unchanged.

### Direct v10/v12 comparison and user-requested stop

The later diagnosis compared the saved v10 and v12 candidate executables
directly in five alternating ABBA/BAAB blocks. Every run used a fresh cluster
and query process, the original six workloads, 20 warmups and 1,000 timed
queries per workload. The container retained the original four-CPU quota,
CPU set `10,11,26,27`, 8 GiB memory and zero swap. Compilation was outside
measurement. All twenty query runs and 120,000 measured RPCs were retained.

| Query | v12/v10 p50 | Whole-block bootstrap 95% interval | v12/v10 CPU |
| --- | ---: | --- | ---: |
| QL raw | 1.000194 | [0.973315, 1.024934] | 1.001057 |
| QL aggregate | 1.005820 | [0.986578, 1.032041] | 1.003524 |

The confidence intervals enumerate all 3,125 paired whole-block resamples.
This diagnostic found no material latency difference between the two candidates;
it does not replace the original reference-versus-candidate acceptance test.
The earlier v12 campaign also slowed the unchanged reference and showed much
larger run variation. Its recordings do not establish a particular host or CPU
mechanism as the cause. CPU quota throttling was negligible.

An isolated trace-cleanup experiment reduced `BatchResult.Close`'s stack frame
from 160 to 40 bytes, but showed no consistent lifecycle latency improvement
and identical allocations. The experimental patch was not applied. No production
code changed during this latency diagnosis.

The first direct query run passed, but its untimed report merge rejected
inherited single-shot settings. Only the merge was resumed; query timing was
retained. That run lacks an after-resource snapshot and after-canary, explicitly
recorded as unavailable. Subsequent runs have both snapshots.

After the direct comparison showed substantially lower variation, one fresh
20-pair small acceptance matrix began with the original statistical method and
limits. The user requested a stop after ten complete pairs. Reference run 11
was interrupted, all evidence was retained, and no acceptance verdict was
computed from the incomplete matrix. The large matrix remains outstanding.
T02 is not recorded as fully accepted.

The diagnostic sources, exact reports, resource snapshots, unmodified-production
hashes and stop record are under `.omx/bench-reports/qlb-t02-debug/`, including
`findings.md`, `abba-analysis.json`, `close-probe/findings.md` and
`small-gates/outcome.json`.
