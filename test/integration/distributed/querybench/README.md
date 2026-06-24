# Distributed query benchmark

This package benchmarks row vs vectorized distributed query on a local in-process BanyanDB distributed cluster. The harness is gated to run only inside a resource-limited Docker container, and every `(mode, scenario, cardinality, variant)` combo runs in its own Go process so heap and CPU profiles describe exactly that combo -- no cross-scenario leakage in the cumulative allocation snapshot.

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

1. Builds the test binary once into `/tmp/dqb.test`.
2. Clears `${DQB_REPORT_DIR}/shards/` and `${DQB_REPORT_DIR}/profiles/` so the merged report reflects only this run.
3. Iterates the selected matrix and invokes the binary once per combo with `DQB_ENGINE`, `DQB_MODE`, `DQB_SCENARIO`, `DQB_CARDINALITY`, and any trace variant dimensions set. Each invocation:
   - Boots a fresh cluster (`row` mode plain, `vec` mode with the selected engine's vectorized flag enabled on every node).
   - Writes the data set at the configured cardinality.
   - Runs warmup + timed queries for one scenario.
   - Captures CPU and heap profiles bracketed around the timed phase.
   - Writes a shard JSON under `${DQB_REPORT_DIR}/shards/` and exits. Measure keeps the original `<mode>_<scenario>_<cardinality>.json` filenames; trace shard names include the full variant key.
4. Invokes the binary one last time with `DQB_MERGE=1`. The merge pass reads every shard, computes vec/row correctness, and writes `distributed-querybench.json` + `distributed-querybench.md` with the unified results and the vec/row ratio table.

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

`DQB_SCENARIOS` selects the scenario set. `DQB_QUERY_WORKERS`, `DQB_WARMUP_ITERATIONS`, `DQB_WRITERS`, and `DQB_SMALL_EXACT_ROWS` map to the corresponding knobs. Trace-specific knobs include `DQB_SPANS_PER_TRACE`, `DQB_SPAN_DIST=uniform|heavytail`, `DQB_FILTER_SELECTIVITY`, `DQB_TRACE_ID_BATCH`, `DQB_SHARD_NUM`, `DQB_DATA_NODES`, `DQB_SPAN_BYTES`, and `DQB_QUERY_MEMORY_MIB`.

Trace `DQB_MATRIX` modes:

- `A` (default): primary volume sweep over `DQB_CARDINALITIES`, using the fixed realistic shape `DQB_SPANS_PER_TRACE=20`.
- `B`: fixed-volume deep dive at about 1M target spans, varying one axis at a time: trace size, heavy-tail distribution, selectivity, trace ID batch size, storage shard count, and data-node fan-out.
- `both`: runs Matrix A and then Matrix B sequentially inside the same Docker invocation.

Trace cardinality means target total spans. Uniform trace count is derived as `round(total_spans / DQB_SPANS_PER_TRACE)`. Heavy-tail uses 95% traces at about 10 spans and 5% at about 500 spans, so the mean is about 34.5 spans/trace. `service_id=svc-0` selectivity is assigned per trace so tag-filter result sizes are deterministic and reviewable.

## Output layout

Reports default to `.omx/bench-reports/distributed-query/`:

- `distributed-querybench.json` — full per-result detail plus the environment and config view.
- `distributed-querybench.md` — per-mode summary table and the `Vec/Row Ratios` table (values < 1.00x mean vec is faster or lighter).
- `shards/*.json` — one per single-shot invocation, the raw evidence the merge pass aggregates.
- `profiles/<scenario>/<cardinality>/.../<mode>/{cpu,heap}.pprof` — when `DQB_PROFILE=1`. Trace profiles include the variant key in the path. Each pprof file was captured inside a fresh process so the heap snapshot is not polluted by other scenarios or modes.

## Direct invocation contract

The test binary refuses to run when `RUN_DISTRIBUTED_QUERY_BENCH=1` is set without `DQB_IN_CONTAINER=1` -- host VM execution is not a supported benchmark path. Direct `go test` calls that bypass the orchestrator must set either:

- `DQB_MODE=row|vec`, `DQB_SCENARIO=scan_all|top_with_filter`, and `DQB_CARDINALITY=<int>` for a single-shot run, or
- `DQB_MERGE=1` for a merge run.

Anything else is a hard configuration error so the orchestrator contract stays the only well-formed entry point. Host-runnable unit/config tests do not set `RUN_DISTRIBUTED_QUERY_BENCH=1` and do not boot a cluster.

## In-process cluster note

`startBenchCluster` runs the two data nodes and the liaison inside the same Go process. CPU, RSS, and allocation metrics in each shard are process-level deltas covering all three nodes plus the client. The report's `environment.resource_note` records this so consumers know the metrics are not per-node.
