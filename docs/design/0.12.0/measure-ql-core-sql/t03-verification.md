# T03 typed raw measure queries

Implementation base: `c10d3ac2` (T02), itself based on PR #1412. The T03
measurements were collected from the T03 working tree before committing it.
Verification uses Go 1.26.9. Functional,
resource and P(T03) verification is complete. An additional G1 compatibility
comparison was stopped with partial observations and no gate verdict. This record
does not claim complete Catalog B performance acceptance.

## Execution contract

`Transformer.CompileMeasure` resolves a prepared measure statement against cloned
schema snapshots and produces an immutable `MeasureTemplate`. `Bind` creates a
fresh native request and typed raw plan for each set of parameters and execution
clock. Legacy measure queries use this template/instance seam and retain their
native request, pagination and output behavior. Other resource types retain their
existing transformer and dispatch.

Extended raw queries support both source spellings, aliases, family-qualified
tags, explicit tag/field disambiguation, timestamp projections and aliases, and
WHERE time comparisons with timestamp/string parameters, `now()` and fixed
`INTERVAL`. Ambiguous references and duplicate names within an output container
are rejected before scanning. The raw timestamp remains `DataPoint.timestamp`;
timestamp aliases also appear as timestamp TagValues in `__ql`. A conflicting
selected user column is rejected. Distinct field and tag containers may reuse an
output name.

```sql
SELECT host AS node, v::FIELD AS reading, time AS observed_at
FROM sw_metric.ql_t03
WHERE time >= ? AND time < ?
LIMIT ? OFFSET ?
```

The liaison authorizes the lowered native request and uses the existing measure
admission, tracing, query-budget and batch-dispatch infrastructure. Data nodes
execute the same native operators as protobuf requests. `NativeScan` lends their
columnar batches to the new raw operator. Projection lends source columns with
independent output descriptors; a disjoint time residual creates a selection
vector, and final OFFSET/LIMIT follows that residual. The existing serializer
constructs DataPoints only at final egress. Timestamp alias cells are constructed
at this boundary and included in response accounting.

Absent LIMIT on an extended query has no semantic row cap, subject to query
budgets. Native offset is zero and its limit is unrestricted; the liaison applies
final pagination. Contradictory time bounds and explicit extended LIMIT 0 return
empty results without publishing a native scan. Timestamp-only queries use a
hidden native projection anchor that never becomes visible output.

One execution clock is captured after compilation and supplied to every bound
time expression. No parameter values or clock values are retained in the
template. Compiled-template caching is T05; T03 does not claim cache-hit savings.
Extended aggregates, mixed time/tag OR and general output-column sorting are
explicitly rejected pending their owning tickets. Legacy aggregation remains on
its existing native lowering path.

### Native time compatibility

The original design incorrectly called native TimeRange half-open. The existing
analyzer and dispatcher use `timestamp.NewInclusiveTimeRange`. T03 keeps internal
intervals half-open on the stored millisecond lattice and lowers `[begin, end)`
to native inclusive `[begin, end - 1ms]`. Legacy TIME semantics are preserved.
Sub-millisecond comparisons use exact floor/ceiling rules; non-aligned equality
is empty. Native-domain checks happen before narrowing or arithmetic.

The representable stored endpoints are `1677-09-21T00:12:43.146Z` and
`2262-04-11T23:47:16.854Z`. The internal exclusive endpoint one millisecond past
the maximum is safe in millisecond arithmetic; it is never converted to an
overflowing nanosecond timestamp. Pure-time OR scans one native envelope and
retains the full disjoint residual. Intersections with legacy TIME can become
empty locally.

## Correctness evidence

RED evidence is retained in `.omx/t03-planner-red.log`,
`.omx/t03-raw-red.log`, `.omx/t03-benchmark-red.log` and
`.omx/t03-rpc-red-v2.log`. These exposed the absent typed-template/operator seams,
the unregistered benchmark scenario, and the real RPC's failure to execute the
new source/time contract. Subsequent E2E failures exposed the need for a complete
time envelope and a hidden source column for timestamp-only projections.
`.omx/t03-raw-metadata-red.log` records the later query-budget accounting defect
in raw projection descriptors, fixed by reserving metadata before allocation.

The generator adds 142 QL-only T03 cases with independent expectations over a
129-row fixture. It covers nullable int/float values, multiple tag families,
repeated sources, output collisions, both source spellings, all six time
comparators, nanosecond boundary offsets, domain extremes, fixed intervals,
changed parameters/clocks, OR residuals, legacy TIME intersections, trace,
omitted limits, zero limits and results larger than 100 rows. Fixture setup and
visibility checks use QL queries. No native query supplies an expected result.

Both final topology runs pass all 149 selected cases: 142 T03 and seven T01/T02
compatibility cases, with no selected case skipped. Ginkgo's JSON reports also
contain suite setup/cleanup nodes and cases excluded by the label filter.

```sh
go test -tags=querymetrics ./test/integration/standalone/query \
  -ginkgo.label-filter='QLB-T01 || QLB-T02 || QLB-T03' \
  -ginkgo.json-report="$PWD/.omx/t03-standalone-final.json"
go test -tags=querymetrics ./test/integration/distributed/query \
  -ginkgo.label-filter='QLB-T01 || QLB-T02 || QLB-T03' \
  -ginkgo.json-report="$PWD/.omx/t03-distributed-final.json"
```

Focused planner/operator tests exercise concurrent template binding, schema
snapshot isolation, physical selections, multi-batch pagination, cancellation,
final value ownership and budget rejection before allocation. Metadata, selection
and timestamp-egress reservations release on constructor failure, Init failure,
Close and cancellation. Production builds omit test counters and clock overrides.

The pre-existing 151 generated regression cases and their expectations remain
active. A fresh run in each topology returns 142 passing cases and exactly the
same nine previously identified failures concerning indexed string-array decoding
and empty entity IN. There are no new failures in this corpus. The nine failures
are outside T03's fixed liaison-only boundary; T03 does not remove them or claim
the entire integration suite is green. Their final reports are
`.omx/t03-legacy-{standalone,distributed}.{log,json}`.

Additional checks pass:

```sh
go test -race -tags=querymetrics ./pkg/bydbql ./pkg/query/ql \
  ./banyand/liaison/grpc ./test/cases/measure/qlgen \
  ./test/cases/measure/cmd/generate ./test/integration/distributed/querybench
go test ./pkg/bydbql ./pkg/query/ql ./test/cases/measure/qlgen \
  ./test/cases/measure/cmd/generate ./test/integration/distributed/querybench
make check-ql-test-cases
make lint
make license-check
golangci-lint fmt -c .golangci-format.yml --diff \
  ./pkg/bydbql/... ./pkg/query/ql/... ./banyand/liaison/grpc/... \
  ./test/cases/measure/qlgen/... ./test/cases/measure/cmd/generate/... \
  ./test/integration/distributed/querybench/...
git diff --check
```

The formatter emits an empty diff. Logs are `.omx/t03-final-{race,normal}.log`,
`.omx/t03-generated-check-final.log`, `.omx/t03-make-lint.log`,
`.omx/t03-license-check.log` and `.omx/t03-format-final.diff`.

## Benchmark protocol

P(T03) runs the real distributed QL RPC in five isolated Docker repetitions,
with 4 CPUs, 8 GiB, two data nodes, 1,024 and 100,000 written rows, one query
worker, 20 warmups and 1,000 timed requests per variant. Thirteen variants cover
plain/aliased/shorthand raw queries, 16 repeated projections, 10% and 50% exact
ranges, changed bound values, equivalent legacy TIME, disjoint OR, its complete
envelope reference, contradiction and LIMIT 0. The OR reference adds time points
outside this fixture; it is equivalent on this fixed fixture, not on arbitrary
future data.

```sh
for repetition in 1 2 3 4 5; do
  DQB_ENGINE=measure DQB_MODES=vec DQB_SCENARIOS=qlb_t03 \
    DQB_CARDINALITIES=1024,100000 DQB_QUERY_WORKERS=1 \
    DQB_QUERY_ITERATIONS=1000 DQB_WARMUP_ITERATIONS=20 DQB_PROFILE=1 \
    DQB_REPORT_DIR="/work/.omx/bench-reports/qlb-t03-v1/r${repetition}" \
    test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
done
```

Each response's row count is checked during timing; independently calculated
contents are checked before timing and at sampled points afterwards. Reports
retain raw latencies, p50/p95/p99, throughput, process CPU/heap profiles,
allocations, native source/frame counts, clock/operator counters, reservation
peaks, cumulative tracker lease charges and post-close resource observations.
`native_source_rows` counts rows emitted by the native source, not physical
storage rows examined. Total parent lease charges, physical scanned rows and
planning/cache metrics remain explicitly unavailable.

One nonempty query starts one liaison batch execution and one storage leaf per
data node (two leaf calls in this topology). Empty/LIMIT 0 starts neither.
Exact WHERE and equivalent legacy TIME must retain equal native source/frame
counts; narrower ranges and OR envelopes must reduce source work. Typed aliases
must copy zero value bytes and every completed request must release live results,
owner pins, flatten-pool references and reservations. Structural checks do not
prove lower latency; timing conclusions require the retained measurements.

### P(T03) results

All five repetitions passed both cardinalities and report merge: 130 verified
variant records, 130,000 timed RPCs and 260 nonempty CPU/heap profiles. All
activation, clock, projection, pruning and cleanup assertions passed. Native
source/frame counts match for exact WHERE and equivalent legacy TIME. Every
empty/zero-limit variant performs zero native work. All source/build manifests,
raw latencies, logs and profiles are retained under
`.omx/bench-reports/qlb-t03-v1/`; `analysis.json` summarizes all 26 variant shapes.

These are ratios of the five-run medians. Brackets are exact paired-bootstrap
95% intervals over the five repetitions, not independent confidence intervals
over the 1,000 queries within a repetition. The shared host and fixed within-run
variant order remain limitations.

| RPC comparison | 1,024 rows, p50 ratio | 100,000 rows, p50 ratio |
| --- | --- | --- |
| Alias / plain, same projection width | 1.038 [0.990, 1.075] | 1.013 [1.006, 1.023] |
| Exact 10% range / legacy TIME | 1.019 [0.928, 1.050] | 1.010 [0.990, 1.032] |
| Exact 50% range / legacy TIME | 0.980 [0.922, 1.064] | 1.005 [1.001, 1.010] |
| Pruned OR / complete-envelope reference | 0.944 [0.943, 1.025] | 0.688 [0.681, 0.698] |

Aliases add approximately 1.3% large-query median latency. The large OR case
reduces median latency by approximately 31%; its small-query timing interval
crosses 1.0, so the small case has no demonstrated latency improvement. These
new-query comparisons do not substitute for G1's unchanged legacy/native controls.

| Large-dataset variant | p50 / p95 / p99 (ms) | Allocated MiB/query | Native source rows/query | Returned rows |
| --- | --- | --- | --- | --- |
| Plain legacy | 29.145 / 39.684 / 43.130 | 19.02 | 12,600 | 6,300 |
| Alias | 29.528 / 39.857 / 42.957 | 19.06 | 12,600 | 6,300 |
| 16 repeated projections | 110.285 / 136.908 / 146.653 | 60.65 | 12,600 | 6,300 |
| Exact 10% range | 9.665 / 13.933 / 15.339 | 5.19 | 1,260 | 630 |
| Exact 50% range | 24.346 / 33.166 / 36.195 | 14.77 | 6,300 | 3,150 |
| Pruned OR | 10.439 / 14.570 / 15.775 | 7.95 | 6,426 | 126 |
| Complete-envelope OR reference | 15.184 / 18.515 / 19.921 | 12.78 | 12,600 | 126 |
| Contradictory time | 0.261 / 0.367 / 0.490 | 0.022 | 0 | 0 |
| LIMIT 0 | 0.247 / 0.351 / 0.456 | 0.021 | 0 | 0 |

Allocation figures include the client, liaison, data nodes and background work
in the benchmark process. Repeating projections copies zero intermediate value
bytes, but the final response still contains all named values and incurs its
serialization/allocation costs. No planning CPU or compiled-cache benefit is
claimed.

### G2 resource probes

The separate `TestQLBT03ResourceBounds` runs in the same 4 CPU/8 GiB container
with 2,048 aliases over 64 rows. Both configured budgets passed five requests:

| Per-tracker limit | Correct successes | ResourceExhausted | Maximum process-global reserved bytes | Live reservations after every request |
| --- | --- | --- | --- | --- |
| 16 MiB | 0 | 5 | 9,182,084 | 0 |
| 256 MiB | 5 | 0 | 140,910,354 | 0 |

At 16 MiB, no final DataPoints are constructed and no partial response is
returned. At 256 MiB, every complete result matches the independent oracle.
Both limits finish with zero live result handles, owner pins and native
flatten-pool references. Peak reservations count the process-global sum over one
liaison and two data nodes; they are bounded against the sum of three configured
limits. Per-tracker peaks and parent lease observations are explicitly unavailable.
The source-level preallocation checks and focused budget-failure tests complement
this aggregate observation; the aggregate peak alone cannot prove each tracker's
limit. Cumulative tracker lease charges are recorded separately from live bytes.

Artifacts are
`.omx/bench-reports/qlb-t03-resources-v1/qlb_t03_resources_{16,256}mib.json`
and the corresponding logs. The exact Docker invocation is retained at
`.omx/bench-reports/qlb-t03-v1/run-resources.sh`; it uses the instrumented binary
from the first P(T03) run, so its production code is identical to that matrix.

### G1 compatibility

The additional comparison against the immediate T02 prerequisite used unchanged
`qlb_t01` native and legacy QL controls. It was stopped after the user challenged
the verification scope. The requested new-feature benchmark had already completed;
continuing the broad compatibility matrix was excessive for this implementation
task. The original proposed G1 criterion is not relaxed or marked passed. T02's
previously stopped acceptance investigation is not reopened by this implementation.

The initial attempt, retained at `.omx/bench-reports/qlb-t03-g1-v1/`, failed in
the reference revision's setup. Its old T01 visibility query selected the entire
100,000-row table and exceeded T02's enforced 256 MiB budget before the measured
service-filtered workload could start. That attempt was stopped after reproducing
the error. `waitForQLBT01Visibility` now verifies the same service-filtered legacy
QL result as the measured workload, using the independent fixture oracle. This
setup-only change is identical in reference and candidate; timed queries,
parameters, expected results, samples and thresholds are unchanged.

The replacement comparison is retained separately under
`.omx/bench-reports/qlb-t03-g1-v2/`. Its source manifest records the identical
setup overlay. All production build inputs and T03 workload functions remain
identical to P(T03). Reference production code is the detached `c10d3ac2`
checkout at `/mnt/d/qlb-t03-reference-c10d3ac2`; the only tracked overlay is the
T01 benchmark setup file. Ignored protobuf/mock Go artifacts were copied
byte-identically to permit its build, with a separate generated-file manifest.
No protobuf source or generated contract was changed.

Both revisions use the same pinned fixture time, Go/image, schema, 4 CPU/8 GiB
limits, cardinalities, single worker and query parameters. Odd pairs run reference
then candidate; even pairs reverse that order. Each variant retains the existing
T01 warmup loop (21 validation/warmup requests with the configured value 20),
followed by 1,000 timed requests. The exact script is
`.omx/bench-reports/qlb-t03-v1/run-g1.sh`.

Two complete paired repetitions and the third reference repetition were retained;
the third candidate completed 1,024 rows and was interrupted during its
100,000-row process. These are partial observations, not a median-of-five comparison or a compatibility gate
verdict. No additional runs are required to finish the T03 implementation report.
Both completed pairs passed their independent result checks at both cardinalities.
All nine previously identified generated-suite failures remain visible, and the
focused T01/T02 integration cases passed in both topologies.
