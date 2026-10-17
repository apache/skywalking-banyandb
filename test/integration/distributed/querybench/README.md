# Distributed query benchmark

This package benchmarks vectorized distributed query on a local in-process BanyanDB distributed cluster. The harness is gated to run only inside a resource-limited Docker container, and every `(mode, scenario, cardinality, variant)` combo runs in its own Go process so heap and CPU profiles describe exactly that combo -- no cross-scenario leakage in the cumulative allocation snapshot.

`DQB_ENGINE=measure` is the default and preserves the original measure benchmark behavior. `DQB_ENGINE=trace` enables the distributed trace benchmark.

Measure scenarios:

- `scan_all`: mirrors `test/cases/measure/data/input/all.yaml` for `service_cpm_minute`.
- `top_with_filter`: mirrors `test/cases/measure/data/input/top_with_filter.yaml` (`id != svc3`, group by `id`, `MEAN(value)`, top 2 desc).

Trace scenarios:

- `trace_by_id`: trace ID point/bulk lookup using `trace_id IN (...)`, with `limit` pinned to the trace ID batch size.
- `trace_tag_filter`: ordered tag filter using `service_id = svc-0`, `order_by timestamp DESC`, and a pinned trace-count limit.

Default measure cardinalities: `1024,10000,100000,1000000,2000000`.
Default trace cardinalities: `1000,10000,100000,1000000,2000000`.

## How the harness is shaped

`run-docker.sh` starts one resource-limited container and invokes `orchestrate.sh` inside it. The inner orchestrator:

1. Builds the test binary once into `${DQB_REPORT_DIR}/tmp/dqb.test`.
2. Clears `${DQB_REPORT_DIR}/shards/` and `${DQB_REPORT_DIR}/profiles/` so the merged report reflects only this run.
3. Iterates the selected matrix and invokes the binary once per combo with `DQB_ENGINE`, `DQB_MODE`, `DQB_SCENARIO`, `DQB_CARDINALITY`, and any trace variant dimensions set. Each invocation:
   - Boots a fresh cluster.
   - Writes the data set at the configured cardinality.
   - Runs warmup + timed queries for one scenario.
   - Captures CPU and heap profiles bracketed around the timed phase.
   - Writes a shard JSON under `${DQB_REPORT_DIR}/shards/` and exits. Measure keeps the original `<mode>_<scenario>_<cardinality>.json` filenames; trace shard names include the full variant key.
4. Invokes the binary one last time with `DQB_MERGE=1`. The merge pass reads every shard and writes `distributed-querybench.json` + `distributed-querybench.md` with the unified results.

## Run profiles

Smoke (single small cardinality, fast iteration):

```bash
DQB_CARDINALITIES=1024 \
DQB_QUERY_ITERATIONS=10 \
DQB_PROFILE=1 \
test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
```

Standard sweep through 1M rows:

```bash
DQB_CARDINALITIES=1024,10000,100000,1000000 \
DQB_QUERY_ITERATIONS=50 \
test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
```

Stress (>1M):

```bash
DQB_CARDINALITIES=2000000 \
DQB_QUERY_ITERATIONS=20 \
test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
```

Trace smoke:

```bash
DQB_ENGINE=trace \
DQB_MATRIX=A \
DQB_CARDINALITIES=1000 \
DQB_SCENARIOS=trace_by_id,trace_tag_filter \
DQB_SPANS_PER_TRACE=20 \
DQB_QUERY_ITERATIONS=10 \
DQB_TRACE_BY_ID_ITERATIONS=20 \
DQB_PROFILE=1 \
test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
```

Trace standard sweep through 1M spans:

```bash
DQB_ENGINE=trace \
DQB_MATRIX=A \
DQB_CARDINALITIES=1000,10000,100000,1000000 \
test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
```

Trace stress (>1M spans):

```bash
DQB_ENGINE=trace \
DQB_MATRIX=A \
DQB_CARDINALITIES=2000000 \
DQB_QUERY_ITERATIONS=20 \
test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
```

Trace deep dive:

```bash
DQB_ENGINE=trace \
DQB_MATRIX=B \
test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
```

`DQB_SCENARIOS` selects the scenario set. `DQB_QUERY_WORKERS`, `DQB_WARMUP_ITERATIONS`, and `DQB_WRITERS` map to the corresponding knobs. Trace-specific knobs include `DQB_SPANS_PER_TRACE`, `DQB_SPAN_DIST=uniform|heavytail`, `DQB_FILTER_SELECTIVITY`, `DQB_TRACE_ID_BATCH`, `DQB_SHARD_NUM`, `DQB_DATA_NODES`, `DQB_SPAN_BYTES`, and `DQB_QUERY_MEMORY_MIB`.

`DQB_QUERY_ITERATIONS` (default 50) sets the timed-loop iteration count for most scenarios. The `trace_by_id` scenario is a sub-10ms point lookup, so at 50 iterations its p95/p99 are dominated by a few GC pauses and read as noise; it instead uses `DQB_TRACE_BY_ID_ITERATIONS` (default 1000) to report a stable tail. Lower it for a quick smoke (e.g. `DQB_TRACE_BY_ID_ITERATIONS=20`).

Trace `DQB_MATRIX` modes:

- `A` (default): primary volume sweep over `DQB_CARDINALITIES`, using the fixed realistic shape `DQB_SPANS_PER_TRACE=20`.
- `B`: fixed-volume deep dive at about 1M target spans, varying one axis at a time: trace size, heavy-tail distribution, selectivity, trace ID batch size, storage shard count, and data-node fan-out.
- `both`: runs Matrix A and then Matrix B sequentially inside the same Docker invocation.

Trace cardinality means target total spans. Uniform trace count is derived as `round(total_spans / DQB_SPANS_PER_TRACE)`. Heavy-tail uses 95% traces at about 10 spans and 5% at about 500 spans, so the mean is about 34.5 spans/trace. `service_id=svc-0` selectivity is assigned per trace so tag-filter result sizes are deterministic and reviewable.

## Output layout

Reports default to `.omx/bench-reports/distributed-query/`:

- `distributed-querybench.json` — full per-result detail plus the environment and config view.
- `distributed-querybench.md` — the results summary table.
- `shards/*.json` — one per single-shot invocation, the raw evidence the merge pass aggregates.
- `profiles/<scenario>/<cardinality>/.../<mode>/{cpu,heap}.pprof` — when `DQB_PROFILE=1`. Trace profiles include the variant key in the path. Each scenario/cardinality/mode runs in a fresh process. QLB T01 runs four RPC variants sequentially in that process, so later variant profiles and heap snapshots include earlier cache and heap residency.

## Direct invocation contract

The test binary refuses to run when `RUN_DISTRIBUTED_QUERY_BENCH=1` is set without `DQB_IN_CONTAINER=1` -- host VM execution is not a supported benchmark path. Direct `go test` calls that bypass the orchestrator must set either:

- `DQB_MODE=vec`, `DQB_SCENARIO=scan_all|top_with_filter`, and `DQB_CARDINALITY=<int>` for a single-shot run, or
- `DQB_MERGE=1` for a merge run.

Anything else is a hard configuration error so the orchestrator contract stays the only well-formed entry point. Host-runnable unit/config tests do not set `RUN_DISTRIBUTED_QUERY_BENCH=1` and do not boot a cluster.

## In-process cluster note

`startBenchCluster` runs the two data nodes and the liaison inside the same Go process. CPU, RSS, and allocation metrics in each shard are process-level deltas covering all three nodes plus the client. The report's `environment.resource_note` records this so consumers know the metrics are not per-node.

### QLB T01 baseline

`DQB_SCENARIOS=qlb_t01` runs `native`, `native_rebound`, `legacy_ql`, and
`legacy_ql_rebound` through the real measure/BydbQL RPC clients. Set
`DQB_ENGINE=measure DQB_QUERY_WORKERS=1 DQB_QUERY_ITERATIONS=1000`.
The four variants share one cluster and process and execute sequentially; prior
variants can affect cache and heap residency in later measurements. This is an
explicit exception to process isolation per variant. The QL variants share query text and change the bound service ID from svc0 to
svc1. Warmup and sampled timed responses are checked against independently specified fixture values,
including timestamps, projection names and types; only storage SID/version are
ignored. Setup, visibility and warmups precede measurement. Each variant receives
its own CPU/heap profiles and process allocation record. The report lists counters
not yet exposed by production as unavailable. Timed responses receive a cheap row-count check; sampled responses are retained
and checked exactly after profiles and allocation snapshots stop. Exact checksum
validation is excluded from latency, throughput, CPU and allocation intervals.
Process heap observations include retained sample responses.

Run five isolated repetitions with cardinalities `1024,100000`, 20 warmups,
profiling enabled, and unique report directories `qlb-t01-r1` through `r5`.
The merge rejects missing variants or anything other than 1,000 samples per
variant. Existing `scan_all` and `top_with_filter` controls remain available.

### T03 typed raw QL and time pruning

`qlb_t03` sends every query through `BydbQLService.Query`, including fixture
readiness. It uses the standard deterministic measure fixture and checks results
against the write formula, without capturing results or issuing native queries.
The service-0 partition contains 64 rows at cardinality 1,024 and 6,300 rows at
cardinality 100,000.

Each isolated cardinality process runs these 13 required variants:

| Variants | Purpose |
|---|---|
| `ql_plain_legacy`, `ql_alias`, `ql_shorthand` | Compare equal-width ID/value projections, aliases, and the shorthand source. |
| `ql_repeated_projection` | Project one value column under 16 independent names. |
| `ql_time_exact_10pct`, `ql_time_exact_10pct_rebound`, `ql_time_legacy_10pct` | Compare exact WHERE time pruning with legacy TIME; rebind the same WHERE text to the middle of the fixture. |
| `ql_time_exact_50pct`, `ql_time_legacy_50pct` | Repeat the time comparison at a wider selectivity. |
| `ql_time_or`, `ql_time_or_fullscan_reference` | Compare an OR envelope with the same result over a complete source scan. |
| `ql_empty`, `ql_limit_zero` | Verify that contradictory bounds and LIMIT 0 dispatch no native query. |

Time windows are half-open. Legacy TIME uses an inclusive end one millisecond
before the corresponding SQL end. The OR selects the first point and the middle
point of each series. Its complete-scan reference adds two timestamp-equality
arms outside the fixture, widening only the native envelope. The independent
oracle proves that these arms contribute no rows.

A run requires exactly 20 checked warmups and 1,000 timed queries per variant,
with one worker. Row counts are checked for every query; full value, type,
projection-name and timestamp hashes are checked for every warmup and sampled
responses after timing. Reports retain all 1,000 latency samples, percentiles,
process CPU/allocation measurements, profiles when enabled, and operator/native
counters. Missing variants, hashes, samples or required counters fail validation.

Run five independent container invocations, each with both cardinalities:

```sh
for repetition in 1 2 3 4 5; do
  DQB_SCENARIOS=qlb_t03 DQB_CARDINALITIES=1024,100000 \
    DQB_QUERY_WORKERS=1 DQB_QUERY_ITERATIONS=1000 DQB_WARMUP_ITERATIONS=20 \
    DQB_REPORT_DIR="/work/.omx/bench-reports/t03-r${repetition}" \
    test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
done
```

Each nonempty query must activate one liaison QL batch execution and two storage
leaf calls, one per data node. Empty/LIMIT 0 must activate neither. Every request
captures the execution clock once. Typed projections copy zero value bytes;
OR residuals must process the complete envelope before final pagination. Exact
WHERE and equivalent legacy TIME must observe equal native source/frame counts;
the 10% and 50% cases must reduce source rows, and OR pruning must reduce work
relative to its full-scan reference. Owners, native flatten-pool references,
result handles and live reservations must return to their initial values.

`native_source_rows` counts batches emitted by the storage source before native
operators. It is not a count of physical storage rows examined; that metric is
explicitly unavailable. These structural checks and correctness checks do not
turn noisy latency comparisons into pass/fail claims. Compare the five reports
and profiles for performance conclusions. The separate `TestQLBT03ResourceBounds`
probe uses 2,048 repeated aliases and accepts either a complete, independently
verified response or `ResourceExhausted`. Run its instrumented test binary inside
the same limited container at `DQB_QUERY_MEMORY_MIB=16` and `256`. It writes
`qlb_t03_resources_<limit>mib.json`, checks cleanup after each request, and bounds
the process-global reservation peak by the sum of the three node budgets. It
reports per-tracker peaks and parent lease charges as unavailable. A resource
error in the successful-query benchmark remains an error and cannot produce a
passing benchmark shard.


### T05 compiled-plan cache comparison

The T05 benchmark has four variants: legacy_simple_cache_off,
legacy_simple_cache_on, typed_wide_cache_off and typed_wide_cache_on.
Each cache setting starts a fresh process and distributed cluster, writing the same 1,024
rows. The simple query projects ID and value. The typed query projects 16 value
aliases and contains eight parameterized time comparisons. Both alternate two
binding sets while retaining their query text; each binding returns 32 independently
specified rows. Cache size is the only configuration change: zero versus 4,000
entries, with the default 64 MiB byte cap.

This deliberately narrows the original T05 performance matrix to the requested
cache-on/cache-off comparison. It does not add larger cardinalities, working sets,
invalidation/churn workloads or comparisons with previous tickets. Cold-miss,
invalidation and capacity correctness belong to focused tests.

Exactly 20 checked warmups precede 1,000 timed RPCs with eight workers per variant.
The warm phase must compile once with caching, then record 1,000 hits and zero
compiles during timing. Disabling caching must compile every request and retain
zero plans. Timed responses receive row-count checks, with full fixture hashes
verified outside timing for retained samples of both binding sets. Latencies,
process CPU/allocations, profiles and cache/resource counters are recorded. The
client, liaison and two data nodes share one process, so process totals are not
planning-only measurements. The two query shapes share their child process and
cluster. The parent starts no cluster and validates distinct child process IDs
for cache-off and cache-on. Child reports and logs are retained under the
report directory's children/cache_off and children/cache_on directories.

The initial T05 runs used sequential clusters in one process. Their cache-on
phase retained the closed first cluster's schema registry graphs through an
existing process-global test roster, so their RPC latency/CPU comparison was
confounded. Those artifacts remain available as the initial runs. Isolating the
two settings fixes the harness without changing production schema lifecycle or
adding benchmark workloads. The planning benchmark remains unchanged.

Run five repetitions in independent 4-CPU, 8-GiB containers with unique report
directories:

```sh
for repetition in 1 2 3 4 5; do
  DQB_ENGINE=measure DQB_SCENARIOS=qlb_t05 DQB_CARDINALITIES=1024 \
    DQB_QUERY_WORKERS=8 DQB_QUERY_ITERATIONS=1000 DQB_WARMUP_ITERATIONS=20 \
    DQB_PROFILE=1 DQB_REPORT_DIR="/work/.omx/bench-reports/t05-isolated-r${repetition}" \
    test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
done
```

The focused planning benchmark compares the same two shapes and cache settings,
including fresh binding and native-request construction while excluding parse and
execution. Run with the same Go version and resource limits:

```sh
go test ./banyand/liaison/grpc -run '^$' \
  -bench '^BenchmarkQLBPlanReuse$/^T05$' -benchmem -benchtime=2s -count=5
```

Report the median planning ns/op and B/op ratios for typed-wide cache-on versus
cache-off; the T05 design target is at most 0.80 for each. Report simple-query
planning and RPC results separately. A planning improvement alone does not prove
an RPC latency improvement.

### T07 local SQL aggregation

`qlb_t07` measures four owning query shapes over the same nullable INT64/DOUBLE
fixture: scalar SUM, COUNT/SUM/MIN/MAX with 1,024 nullable string groups, AVG with
one group, and SUM/COUNT/AVG with N integer groups. NULL and empty string keys
remain different groups. Duplicate series/timestamps are written every 10,000
rows; the local aggregate must receive exactly N deduplicated input rows. The
floating fixture uses binary fractions so the scalar oracle has exact sums.

The N-group query has a final LIMIT 1 to measure complete grouping without a
million-row protobuf response. Every returned RID is checked against its own
independently calculated SUM/COUNT/AVG, including null-only groups. The selected
group is unordered and may differ between requests; its checked sample checksum
is retained in the report. Instrumented input/group counts must still equal N,
so LIMIT cannot hide an incomplete scan or aggregation. Generated E2E tests cover
complete multi-group output separately.

The frozen successful-query schedule uses one worker and four variants only:

| Input rows | Checked warmups per variant | Timed RPCs per variant |
| --- | ---: | ---: |
| 1,024 | 5 | 100 |
| 32,768 | 3 | 20 |

This capability-specific schedule replaces the old shared 1,000-call boilerplate
for T07. It keeps the benchmark focused on rows/groups, null handling and shared
state. It does not repeat cache settings or the previous tickets' comparison
matrices. Five independent repetitions retain all latency samples. Small sample
counts limit tail estimates: p95/p99 at twenty requests mostly describe the
slowest observations and are not evidence of a stable latency distribution. New SQL has
no old-version latency baseline.

The existing orchestrator runs each cardinality in a fresh process with one
cluster. T07 validates separate process identities across cardinalities, avoiding
retention of a previous closed cluster. It selects the schedule above regardless
of the generic iteration defaults. Schema creation, ingestion, visibility checks,
scalar reference construction and warmup remain outside timing. Timed queries
check row counts; warmups and retained timed samples check complete values,
types, aliases and nulls. Profiles and CPU/allocation totals include the client,
liaison and two data nodes.

Run five independent 4-CPU, 8-GiB containers. The successful N-group experiment
may use a 1-GiB per-query budget; this is an experiment setting, not a production
default change. The protector's admission limit also applies and can be lower
than this vectorized tracker setting. Separate probes below target the
16/256-MiB resource contract; a failure during readiness is a blocked probe,
not a passing aggregate resource result.

```sh
for repetition in 1 2 3 4 5; do
  DQB_ENGINE=measure DQB_SCENARIOS=qlb_t07 \
    DQB_CARDINALITIES=1024,32768 DQB_QUERY_WORKERS=1 \
    DQB_QUERY_MEMORY_MIB=1024 DQB_PROFILE=1 \
    DQB_REPORT_DIR="/work/.omx/bench-reports/t07-bounded-r${repetition}" \
    test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
done
```

The querymetrics report separates cumulative aggregate key bytes/capacity and
fixed-width state bytes/capacity from process-global query reservation peaks.
Divide cumulative capacities by `aggregate_executions` for per-query averages.
It requires one logical QL raw scan, two storage leaf calls (one per data node),
exactly N local input rows, and the actual group cardinality. Fixed-width state
bytes per group must remain constant as input rows increase. Float SUM, COUNT
and AVG share two state lanes. The report checks that live owners, native flatten
buffers, result handles and reservations return to baseline after every variant.
Native retained-frame bytes and total parent lease charges remain explicitly
unavailable; process-global peak reservations are not native residency alone.

`TestQLBT07ResourceBounds` uses the same N-group query over 100,000 or
1,000,000 rows and five requests. Build the querymetrics test binary and run this
test in separate limited containers with `RUN_DISTRIBUTED_QUERY_BENCH=1`, `DQB_IN_CONTAINER=1`,
`DQB_ENGINE=measure`, each `DQB_CARDINALITY=100000` and `1000000`, and
each `DQB_QUERY_MEMORY_MIB=16` and `256`, with a unique `DQB_REPORT_DIR` for all
four combinations. It verifies fixture visibility with bounded raw QL chunks
that prune one host per query. Each time window spans sixteen times the per-host
row limit, keeping setup query counts close to input rows divided by chunk size.
The independent RID oracle checks every fixture row exactly once. This avoids
retaining decoder buffers for all hosts in a low-budget readiness request;
the measured aggregate still receives the complete fixture. After the probes,
one host-pruned chunk verifies that a bounded query still succeeds. The probe
then accepts only complete correct responses or `ResourceExhausted`, with no
partial results or leaked resources. It writes
`qlb_t07_resources_<rows>rows_<limit>mib.json`.
The process-global reservation peak is bounded by three independent node budgets.
A resource rejection in the successful-query benchmark remains a failure.
The 100,000-row, 16-MiB run cannot currently complete native readiness: even
one host, one field and 1,024 returned rows exceed the native working allowance.
It therefore produces no aggregate resource/recovery verdict. The verification
report records this separately from measured aggregate outcomes.


The original successful-query matrix also required 100,000 and 1,000,000 input
rows. That requirement is **not fulfilled** by the two-cardinality matrix above.
The initial 100,000-row run deterministically failed before local aggregation:
after all raw QL chunks had verified fixture visibility, the inherited native
frame reservation requested 157,505,864 bytes with 51,609,596 bytes already charged
against a 201,326,592-byte protector lease. The first native replica frame alone
crossed that allowance, and the second frame needed another reservation.
`AggregateExecutionStats` remained zero: this was not a local HashAggregate cost
or throughput measurement.

The native reservation includes 128 bytes per cell and per row, thirteen copies
of encoded frame bytes, and merge workspace. A larger vectorized tracker setting
does not enlarge the protector lease. T07 preserves this inherited accounting and
admission policy. The successful RPC matrix therefore reports 1,024/32,768 rows,
while 100,000/1,000,000 rows remain strict resource-outcome experiments. Rejections
at those larger sizes are reported as rejections, with no SQL throughput claim.
The original failed run and the bounded one-query diagnostic artifacts remain
available. This is a concrete limitation of the current one-scan native boundary,
not an acceptance pass for the originally specified large successful workloads.

### T08 native aggregate branches and keyed merge

`qlb_t08` compares the automatic native aggregate path against forced local T07
execution of **identical QL**, using the T07 nullable fixture and its versioned
writes. It runs only these four owning shapes, grouped by the 16 entity hosts
and explicitly restricted to those non-null hosts with `host IN (...)`:

| Shape | Selected fields | Native branches |
| --- | --- | --- |
| `float_avg` | `AVG(f)` | SUM(f), COUNT(f) |
| `float_shared` | `SUM(f), COUNT(f), AVG(f), AVG(f)` with distinct aliases | The same two branches |
| `int_extrema` | `MIN(v), MAX(v), COUNT(v)` | MIN(v), MAX(v), COUNT(v) |
| `int_avg_fallback` | `AVG(v)` | One local raw scan; integer overflow semantics prevent native lowering |

Each shape has `pushdown` (native enabled) and `local` (forced local) variants.
The fallback control intentionally uses T07 in both modes. The three eligible
shapes also have an `individual` control: the same native SUM/COUNT or
MIN/MAX/COUNT requests executed serially through the existing measure Query
RPC. Its latency and allocations describe the **whole sequence of two or three
RPCs**, not a single SQL request. The control reports 32/48 response rows per
iteration; QL reports 16. It checks each native result against fixture arithmetic,
including native COUNT(f)'s float64 result type, then verifies host-keyed AVG
finalization outside timing. Native queries are benchmark controls, never the
expected-result oracle for QL.

The frozen protocol uses 1,024 and 32,768 logical input rows, one query worker,
100 timed queries/5 warmups at 1,024 rows and 20 timed queries/3 warmups at 32,768
rows. Every cardinality starts a fresh process and cluster. Before timing,
host-pruned QL raw queries verify the complete visible fixture and every warmup
checks values and types against independent scalar arithmetic. Timed calls check
row counts, retain roughly ten samples, and validate their full contents after
timing. Run five independent 4 CPU/8 GiB repetitions with CPU and heap profiles:

```sh
for repetition in 1 2 3 4 5; do
  DQB_ENGINE=measure DQB_SCENARIOS=qlb_t08 \
    DQB_CARDINALITIES=1024,32768 DQB_QUERY_WORKERS=1 \
    DQB_QUERY_MEMORY_MIB=256 DQB_PROFILE=1 \
    DQB_REPORT_DIR="/work/.omx/bench-reports/t08-r${repetition}" \
    test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
done
```

Each cardinality produces 11 variants. Reports retain per-query latency samples,
process CPU/GC/allocation counters, profiles and the following work/ownership
evidence:

- Eligible AVG uses two unique native branches; selecting SUM, COUNT and a repeated
  AVG adds none. The integer AVG control uses one complete local raw scan.
- A native branch makes one logical distributed execution (two leaf calls in this
  fixture). `ql_batch_executions` counts native branch executions; one logical QL
  request is evidenced by one `merge_executions` or `aggregate_executions`.
- Native merge input is 16 finalized group rows per branch. Merge visits are
  bounded by input group rows plus one visit per output group. State lanes,
  used/capacity bytes, reservations and releases are reported separately for the
  local aggregate and keyed merge. Merge state bytes include branch-presence flags.
- Intermediate DataPoints are zero. Final DataPoints match the selected mode's
  16 SQL rows or 32/48 individual-native rows. Results, tracker reservations, owner
  pins and native flatten pool references return to their starting counts.
- Physical storage rows, retained native frame bytes and total parent lease
  charges are explicitly unavailable; process memory peaks and lease tracker
  counters are not relabeled as those quantities.

The report rejects missing shapes/modes/cardinalities, incomplete timed samples,
extra AVG branches, unexpected fallback, nonzero intermediate rows and leaks.
Pushdown is an execution optimization, so latency comparisons are recorded
without assuming that repeated native scans outperform one raw scan. This adds
no cache, concurrency, earlier-ticket or spill benchmark matrix. Separate native
requests have no common snapshot guarantee while writes continue; timed fixtures
are fixed after visibility checks complete.

### T10 typed expression projection

`qlb_t10` measures five SELECT projections over the T07 fixture's nullable integer
field. Each query returns all logical input rows; no LIMIT hides expression work.
The raw control retains the same response column name and row shape.

| Variant | Projection |
| --- | --- |
| `raw_control` | `v::FIELD AS result` |
| `constant` | `CAST(? AS DOUBLE) AS result`, binding the string `"1.25"` |
| `arithmetic` | `v + 2 AS result` |
| `parameter_cast` | `v + CAST(? AS DOUBLE) AS result`, binding `"1.25"` |
| `masked_case_coalesce` | `COALESCE(CASE WHEN v = 0 THEN 0 ELSE 8 / v END, -1) AS result` |

The frozen protocol has 1,024 and 32,768 input/output rows, one query worker and
a 256 MiB query tracker. The smaller input uses 100 timed calls and five warmups;
the larger uses 20 timed calls and three warmups. Every cardinality starts a fresh
process and cluster. QL raw visibility checks run before timing. Each warmup and
six retained timed responses are checked against the independent row formula,
including exact types, nulls and unique millisecond timestamps. A commutative
row-keyed checksum is derived independently from fixture arithmetic. Hashing and
full value checks run outside latency and allocation snapshots; timed calls
only check row counts. Profiles cover the RPC phase.

```sh
for repetition in 1 2 3 4 5; do
  DQB_ENGINE=measure DQB_SCENARIOS=qlb_t10 \
    DQB_CARDINALITIES=1024,32768 DQB_QUERY_WORKERS=1 \
    DQB_QUERY_MEMORY_MIB=256 DQB_PROFILE=1 \
    DQB_REPORT_DIR="/work/.omx/bench-reports/t10-r${repetition}" \
    test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
done
```

Reports preserve every latency sample, process CPU/GC/allocation counters and
CPU/heap profile paths. They require one QL raw execution and two distributed
leaf calls per RPC, no aggregate/merge execution, no intermediate DataPoints,
and complete expression input/output rows. Constant kernels must allocate zero
full-length value columns. Expression batches, vector/scalar evaluations, arm
rows, buffer bytes, allocations and reservation/release counters are recorded.
Results, query memory, owner pins and native flatten pool references must return
to their starting counts after completion. Physical storage reads, retained
native frame bytes and total parent lease charges remain explicitly unavailable.

The report rejects incomplete variants/cardinalities, changed iteration or memory
settings, missing counters, inconsistent fixture checksums and leaked owners or
reservations. RPC comparisons describe the added cost of projection; independent
scalar-versus-kernel speed and masked branch savings belong to K(T10). This scope
adds no cache, concurrency, aggregation or previous-ticket benchmark matrix.

### T12 field filtering before projection and aggregation

`qlb_t12` measures only field filtering and its downstream work. Its 16 variants
combine nominal selectivity 0/1/50/100%, raw projection/scalar SUM+COUNT, and
one/two hidden predicate fields. The dedicated versioned fixture adds non-null
`score = row % 100` to the existing nullable integer/float rules. Narrow queries
filter `score < ?`; wider queries also test `COALESCE(f, 0) > -1000`, which is
true for every fixture value. Predicates stay absent from visible results.
Actual passing counts are independently computed, including partial 100-row
cycles. The one-percent cohort has only null `v` values, exercising SQL
SUM=NULL/COUNT=0 even when rows passed the filter.

Successful measurements use 1,024/16,384 input rows, one worker, 256 MiB, and
100 timed/5 warmup or 20 timed/3 warmup calls, respectively. There is no LIMIT:
every required input row must reach the filter, and local aggregates must see
exactly its TRUE rows. One logical raw scan makes two distributed leaf calls;
native aggregation, merge and intermediate DataPoints are forbidden. Reports
reject missing variants/metrics, changed protocol, incorrect checksums, a nonzero
structural copy invariant, incomplete scans and leaked reservations/owners/pool refs.

The initial 32,768-row smoke fails at full raw output: final response-encoding
reservation crosses the existing 192-MiB cumulative parent admission allowance.
A fresh-query diagnostic confirms complete filtering, no retained memory after
failure, and successful lower-output recovery. The 16,384-row successful size
is chosen before protocol freeze; the larger failure remains resource evidence.

Each cardinality starts a fresh cluster process. All warmups and six retained
timed responses are fully checked outside timing using fixture arithmetic and
timestamp keys. Timed calls check result count. CPU/heap profiles, allocation
snapshots and every latency sample are retained. Reported filter copied bytes are
structurally zero because the filter borrows the input column objects. Unit tests
and K's pre-timing identity checks verify this property; the RPC counter alone
does not measure copies. Boundary decoding scratch is separately charged to query
memory. Physical storage reads, native retained frame bytes
and total parent lease charges remain explicitly unavailable.

```sh
for repetition in 1 2 3 4 5; do
  DQB_ENGINE=measure DQB_SCENARIOS=qlb_t12 \
    DQB_CARDINALITIES=1024,16384 DQB_QUERY_WORKERS=1 \
    DQB_QUERY_MEMORY_MIB=256 DQB_PROFILE=1 \
    DQB_REPORT_DIR="/work/.omx/bench-reports/t12-r${repetition}" \
    test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
done
```

K(T12), `BenchmarkFilterT12`, separately measures the complete public filter
lifecycle over 1/1,024/65,536 physical rows, the four thresholds, and dense or
selected nullable inputs (24 cases). It reports examined-row throughput,
passed-row throughput, copied bytes, selections and reservations. A zero-percent
filter still examines input and reaches EOF; no sentinel row substitutes for it.
There is no invented old-version baseline for new field-filter syntax.
The owning resource probes cover larger inputs without changing native memory
protection or expanding previous-ticket benchmark matrices.

### T14 filtering completed groups and collection membership

`qlb_t14` measures HAVING after complete native merge or local aggregation. Both
routes produce 16 groups from the versioned nullable T07 float fixture. Each route
has six variants: aggregation without HAVING, nominal 0/1/50/100% passing groups,
and a hidden-AVG 50% predicate. Repeated SELECT/HAVING AVG calls share two state
lanes and, on the native route, exactly two SUM/COUNT branches. All queries consume
the complete input. Reports check that HAVING sees completed groups and that raw
WHERE counters stay separate. With only 16 groups, the nominal 1% case keeps at
least one group; threshold ties can keep more. Actual selected counts are recorded.

At 1,024 input rows, eight additional variants compare CONTAINS and legacy infix
HAVING over unindexed arrays of length 0/1/16/256. A separate fixed 1,024-row
fixture has 256 candidate rows per width. Empty arrays match no rows; half the
other candidates match, producing eight groups. An existing entity-tag predicate
selects the width; typed membership and a field residual run before aggregation,
followed by post-aggregate HAVING. This is 20 variants at 1,024 and 12 at 32,768
rows, or 32 per repetition. No other ticket's timing matrix is rerun.

Use five fresh 4-CPU, 8-GiB containers with no additional swap, one worker and a
256-MiB query budget. The schedules are 100 timed/5 warmup and 20 timed/3 warmup
calls. Each cardinality starts a fresh cluster process. All warmups and six retained
timed responses are fully compared with independent fixture arithmetic outside
timing; every timed call checks its result count. Every timed call must hit the
compiled-plan cache. CPU/heap profiles, raw latencies, reservations and owner/pool
cleanup accompany each variant.

Typed array membership also validates current index bindings/rules before native
dispatch on every call, including cache hits. Indexed arrays reject because native
projection cannot guarantee their complete values. Scalar/no-membership variants
skip these metadata reads; membership RPC CPU/latency includes them. CONTAINS is
contextual and measure-only, preserving existing identifier names.

For the same six independently checked timed responses, `checked_response_bytes`
sums `proto.Size` of each actual full BydbQL response envelope, with
`min_response_bytes` and `max_response_bytes` recording its range. Divide the total
by `checked_timed_samples` for average output bytes. Sizing runs after latency,
process snapshots and profiling stop, so it does not add serialization work to the
timed loop. Reports reject missing byte evidence, envelopes smaller than two bytes,
inconsistent extrema/totals and overflow. No additional query variants are added.

```sh
for repetition in 1 2 3 4 5; do
  DQB_ENGINE=measure DQB_SCENARIOS=qlb_t14 \
    DQB_CARDINALITIES=1024,32768 DQB_QUERY_WORKERS=1 \
    DQB_QUERY_MEMORY_MIB=256 DQB_PROFILE=1 \
    DQB_REPORT_DIR="/work/.omx/bench-reports/t14-r${repetition}" \
    test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
done
```

K(T14), `BenchmarkHavingT14`, measures the public completed-group batch lifecycle
with and without HAVING at 1/1,024/65,536 groups and the same four thresholds;
four additional 1,024-row kernels vary membership array length. Run five one-second
samples of each of its 19 variants. Row identities and borrowed column identity
are checked before timing. All/zero-match cases avoid a retained selection; partial
matches retain uint16 row indices. The copy counter is a structural invariant,
not measured copy telemetry. Array-element count describes input width, not the
number of comparisons actually executed.

RPC CPU/allocation metrics cover the client, liaison and data nodes in one process;
shared-kernel profiles cannot attribute CPU solely to HAVING. Process reservation
peaks include earlier variants. Per-operator peak retained bytes, physical storage
reads, native retained frame bytes and total parent lease charges are unavailable.
The hidden variant returns only the grouping tag, whereas its baseline also
returns two AVG fields; their latency difference includes this output-width change.
The existing indexed-array native projection limitation is documented in
[T14 verification](../../../../docs/design/0.12.0/measure-ql-core-sql/t14-verification.md).


### T15 visible-tuple DISTINCT

`qlb_t15` is a real distributed QL RPC benchmark. It compares `SELECT DISTINCT a, b`
with an explicit `SELECT a, b ... GROUP BY a, b` reference over the same written
rows. Both use the T07 group table with zero aggregate state lanes; this measures
DISTINCT overhead/equivalence, not a claimed independent hash-table algorithm.
Correctness comes from independently enumerated fixture tuples rather than either
query result. Entity and raw row identity are hidden and cannot affect equality.

The frozen ordinary matrix is 1,024 and 8,192 input rows per dataset, one query
worker, 256 MiB per tracker, and profiles enabled. It has exactly fourteen variants:
`distinct_p001_narrow`, `group_by_p001_narrow`, `distinct_p001_wide`,
`group_by_p001_wide`, the corresponding `p050` and `p100` names, plus
`distinct_empty` and `group_by_empty`. Unique counts are
`max(1, floor(input_rows * percent / 100))`; 1% therefore means ten or 81 unique
tuples. Narrow tuples contain twelve UTF-8 bytes; wide tuples add 256 ASCII
bytes to the second component. Adjacent tuples deliberately collide under plain
string concatenation. Empty variants select an absent entity from the same schema.
There is no cache mode matrix. A LIMIT above input cardinality prevents an implicit
response cap from silently truncating high-cardinality output.

Every original row in all six datasets is verified through QL in entity-pruned
windows of at most 1,024 rows before any workload query. Warmups and full fixture
checks are outside timing. Six sampled timed responses are checked afterward, and
their complete BydbQL protobuf envelope sizes are measured afterward as well.
Reports retain raw latency samples, p50/p95/p99, throughput, process-wide inclusive
CPU/heap profiles (heap profiles support alloc_space/alloc_objects), allocation
deltas, group/key capacities and bytes, zero state lanes, native leaf calls, lease
charges and verified post-close cleanup. Unobservable metrics are explicitly
unavailable. Neither prerequisite errors nor failed queries count as throughput.

Run five isolated repetitions with separate report directories:

```sh
DQB_ENGINE=measure DQB_MODES=vec DQB_SCENARIOS=qlb_t15 \
DQB_CARDINALITIES=1024,8192 DQB_QUERY_WORKERS=1 DQB_QUERY_MEMORY_MIB=256 \
DQB_PROFILE=1 DQB_REPORT_DIR=/work/.omx/bench-reports/qlb-t15-r1 \
test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
```

The orchestrator freezes 100 timed calls/5 warmups for 1,024 rows and 20/3 for
8,192 rows. Larger inputs are separate bounded probes: build with
`-tags=querymetrics`, then invoke `TestQLBT15Resources` in a fresh container/process
for each `DQB_T15_RESOURCE_ROWS=100000|1000000` and
`DQB_T15_RESOURCE_BUDGET_MIB=16|256`. Also set `RUN_DISTRIBUTED_QUERY_BENCH=1`,
`DQB_IN_CONTAINER=1`, `DQB_ENGINE=measure`, and `DQB_REPORT_DIR` to an artifact
directory. The resource probe writes only the 100% unique narrow dataset, verifies
all input through bounded QL reads, then attempts DISTINCT and GROUP BY five times
each and checks recovery/cleanup. It records readiness blockers distinctly from
owning-query outcomes. Shared tracker errors do not establish whether a native
decoder or the group table rejected the allocation; that attribution is explicitly
unavailable alongside the measured native/group counters and original error.

The initially attempted 16,384-row matrix failed its first wide DISTINCT warmup
with the native `query memory pool exhausted` error. An untimed fresh single-dataset
replay reproduced that error after full bounded input verification and cleanup,
before any DISTINCT execution or retained key state. The same replay at 8,192 rows
returned the independently expected wide tuples. The ordinary matrix therefore uses
8,192 uniformly across all fourteen variants; 16,384 is not reported as successful
throughput. The configured 256 MiB tracker does not override the separate finite
protector admission policy, and aggregate process counters do not identify the
specific allowance that rejected a charge.

### T16 computed and ordinal grouping

`qlb_t16` has six real distributed QL RPC controls: `simple_low`,
`computed_explicit_low`, `computed_ordinal_low`, and the corresponding `_high`
variants. Low cardinality has eight groups; high cardinality has half as many
groups as input rows. The fixed inputs are 1,024 and 32,768 rows per dataset,
one worker, 256 MiB per query tracker, and profiles enabled. The schedule is
100 timed calls/5 warmups and 20/3 respectively. Each low/high dataset is written
once, including repeated series/timestamps for version deduplication, and reused
by its three query forms.

The performance comparison covers current T16 simple/computed paths. Historical
G1/T02 conclusions are outside this benchmark's claims. Full public QL/native
E2E suites provide current correctness compatibility evidence.

The simple control groups a stored INT field. Computed controls use
`CASE WHEN raw::FIELD >= 0 THEN raw::FIELD + 1 ELSE 0 END`; the ordinal control
spells the same grouping as `GROUP BY 1`. Each query reads two INT fields and
returns the same `key`, `key_again`, and nullable `SUM(v)` fields. Duplicate key
projections check that SELECT and GROUP BY share one evaluation. Results come
from independent fixture arithmetic, with the same complete response schema and
hash across equivalent forms. All input rows are verified through bounded,
entity-pruned QL requests before warmup; sampled timed responses are checked
fully after the measurement window.

Reports require one distributed raw scan, one local aggregate, and one actual
computed-key root invocation per active batch. Eligible row applications are
counted at those real invocations; cached scalar kernel evaluations are separate.
The fixed CASE fixture independently predicts exactly `3 * rows - zero_keys`
vector-kernel rows, zero scalar evaluations, `rows` selected arm values and
`zero_keys` ELSE values. Simple controls have zero expression work. Explicit and
ordinal forms must have identical physical work counts.
Key bytes, aggregate state lanes/bytes, native source/frame rows, reservations,
lease charges, raw latencies, allocation deltas and post-close cleanup are
retained. Native source/frame counters include both data-node replicas; logical
aggregate input rows count the deduplicated rows. Storage physical scans, native
retained frame bytes and per-tracker peaks remain explicitly unavailable.

Freeze the source after E2E verification and build the querymetrics test binary
once with the same Go version used by the container. Set `DQB_TEST_BINARY` to its
mounted path to reuse it across five isolated repetitions:

```sh
go test -tags=querymetrics -c -o .omx/t16-bench-final.test \
  ./test/integration/distributed/querybench

DQB_ENGINE=measure DQB_MODES=vec DQB_SCENARIOS=qlb_t16 \
DQB_CARDINALITIES=1024,32768 DQB_QUERY_WORKERS=1 DQB_QUERY_MEMORY_MIB=256 \
DQB_PROFILE=1 DQB_TEST_BINARY=/work/.omx/t16-bench-final.test \
DQB_REPORT_DIR=/work/.omx/bench-reports/qlb-t16-r1 \
test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
```

Use separate `r1` through `r5` directories. The Docker memory-swap limit defaults
to its memory limit, providing no extra swap. `TestQLBT16Admission`, with
`DQB_T16_ADMISSION_ROWS=1024|32768`, checks the full control matrix without timing
before freezing the successful schedule. The 32,768-row admission check passed
all six controls at 256 MiB.

Larger inputs use `TestQLBT16Resources` in a fresh container/process for each
`DQB_T16_RESOURCE_ROWS=100000|1000000` and
`DQB_T16_RESOURCE_BUDGET_MIB=16|256`, with the usual container/run/engine/report
settings. It writes the high-cardinality dataset, verifies bounded raw input,
then attempts the owning computed-key request five times and verifies cleanup
and recovery. A correct full result or ResourceExhausted without a partial result
is accepted as a resource observation. Readiness blockers are recorded separately;
a prerequisite rejection supplies no throughput evidence. If the unchanged native
prerequisite blocks 100,000 rows at 16 MiB, retain that blocker rather than repeat
the same impossible prerequisite at one million rows.

`BenchmarkGroupExpressions` in `pkg/query/ql` measures key evaluation plus grouping
for the same low/high shapes and a small 2-second TIME_BUCKET control. It uses the
public aggregate/expression plans, checks independent values before and after
timing, and reports allocations and observed key/state work. Use a querymetrics
build to observe unique key evaluation counts.

## QLB-T17 ordering and finite-page controls

`qlb_t17` asks whether compact top-k reduces sort CPU and retained state for the
same complete query and returned page. Its five RPC variants are `fullsort_slice`,
`topk`, and one aggregate/filter/HAVING/order query with workers 1, 8, and 32.
The pair sends byte-identical SQL and parameters through `BydbQLService.Query`;
a scoped, instrumented server context hook forces full sort for the reference.
Both return `LIMIT 10 OFFSET 5`, with three keys and hidden sort inputs. The
composed query changes string-to-double thresholds and the time range on the same
cached SQL text, then sorts completed groups before `LIMIT 1 OFFSET 1`.

The requested schedule is 1,024 and 32,768 raw rows, 256 MiB query trackers,
100/20 sort-pair samples and 5/3 warmups respectively. Every composed variant has
96 samples, evenly covering three bindings. Sort-pair workers stay at one.
`TestQLBT17Admission`, selected with `DQB_T17_ADMISSION_ROWS=1024` or `32768`, checks
this complete protocol without collecting performance evidence. Readiness first
checks every fixture tuple using entity/time windows of at most 1,024 rows. A
large native-frame admission failure remains a blocker, not a successful sort
sample; the successful algorithm controls at 100,000 rows belong to K(T17).

Build the querymetrics test binary before freezing the source. Reuse that exact
binary in five fresh 4-CPU/8-GiB Docker containers with `--memory-swap 8g`, retaining
each report directory. The existing orchestrator accepts `DQB_TEST_BINARY` and
performs no compilation when it is set. For example, with a prebuilt binary:

```sh
DQB_ENGINE=measure DQB_MODES=vec DQB_SCENARIOS=qlb_t17 \
DQB_CARDINALITIES=1024,32768 DQB_QUERY_WORKERS=1 DQB_QUERY_MEMORY_MIB=256 \
DQB_PROFILE=1 DQB_TEST_BINARY=/work/.omx/t17-final-querybench.test \
DQB_REPORT_DIR=/work/.omx/bench-reports/qlb-t17-r1 \
test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g --memory-swap 8g
```

The orchestrator fixes the per-cardinality sample schedule. Use unique `r1` through
`r5` directories and stop other builds/tests during measured phases. Each variant
retains raw latencies, p50/p95/p99, throughput, process CPU/heap profiles,
allocations, full response-envelope bytes, ordered expected checksums, actual
operator row/root/kernel/comparator counters, candidate capacities and reservation
release. Sampled complete responses are checked after profiling; every timed
response checks its row count. Process-wide peak reservations sum the liaison and
two data-node trackers. Sort peak/capacity fields sum per-query completion
snapshots; divide by sort executions. Native frame residency, physical storage
reads, total parent charges, separate aggregate peaks and compile-time savings
remain explicitly unavailable. Warm measured requests must hit the compiled
plan with zero additional compiles; changed-bind exact results check fresh binding.

`BenchmarkSortT17` in `pkg/query/ql` compares a standard-library tuple baseline,
full sort plus identical slicing, and top-k through the public operator lifecycle.
The nine orthogonal shapes cover 1/3/8 keys, ordered/reverse/tied/random input,
16/1,024-byte payloads, k=10/100/1,000 and an offset of 90,000. The 100,000-row
random k=10 and k=100 controls own the median top-k/full-sort ratio gate of 0.80.
Run five independent samples from one prebuilt kernel binary under the same
container limits. Correctness and measured work checks run outside its timer;
per-operation timing includes construction, key binding, sorting, output and Close.
An additional fixture poisons 32 one-MiB source batches after copying exactly one
16-byte surviving candidate per batch, checking payload capacity, released source
owners, tracker reservations and native flatten-pool baseline. This is separate
from native frame residency.

The 32-worker admission gate can be retained explicitly with
`DQB_T17_BLOCKED_32_REPORT=/work/path/to/the/retained/report.json`. This opt-in
requires a readable, untimed 1,024-row/256-MiB artifact with the exact native
broadcast `ResourceExhausted` error and successful cleanup. The harness records
its SHA-256, retains `composed_workers_32` as blocked with zero samples and no
latency/profile/allocation values, and labels the enclosing scenario
`partial_known_32_blocked`. The default still requires all five successful
variants. A partial report cannot satisfy the T17 or parent acceptance gate.
The independent forced-full-sort 32-worker control is another untimed ownership
diagnostic, selected by `DQB_T17_CONCURRENCY_CONTROL=1`.

`run-t17-frozen.sh` supplies separate `rpc` and `kernel` modes. Each runs five
fresh Docker containers at the fixed limits, reuses one prebuilt executable,
refuses to replace report directories, and checks source/binary/evidence hashes
before and after each repetition. It does not compile or pull/build an image.
Capture its source fingerprint immediately before building each binary, verify
that fingerprint again after compilation, and save it as
`<binary>.source.sha256`. `DQB_T17_BINARY` selects that binary and
`DQB_T17_REPORT_ROOT` selects a new report root. The `fingerprint` mode prints the
source hash without starting measurements. Do not start either measured mode
until the coordinated source freeze. Kernel CPU/heap profiles cover the complete
benchmark invocation, including untimed fixture setup and validation; the Go
benchmark ns/op and allocation counters exclude that setup. Its summarizer
requires all 27 controls in all five repetitions and preserves failed ratio
gates in `kernel-summary.json` before returning failure.
