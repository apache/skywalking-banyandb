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
