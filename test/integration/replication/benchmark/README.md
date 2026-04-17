# Replication Benchmark (Kind)

This package benchmarks BanyanDB replication performance across RF=1/2/3 using a Kind-based Kubernetes cluster and the measure module.

## Prerequisites

- Linux with Docker
- `kind`, `kubectl`, and `helm` in PATH
- Go toolchain (same as repo)

> Run `make generate` first. Generated protobuf Go files are part of the normal contributor workflow.

## Chart Source

By default the benchmark installs the official OCI chart:

```
oci://registry-1.docker.io/apache/skywalking-banyandb-helm
```

with chart version `0.5.3`. OCI pulls require Helm registry login:

```
helm registry login registry-1.docker.io
```

Recommended maintainer path (faster, repeatable) is a local checkout of the official chart repo:

```
export BANYANDB_BENCH_CHART=/path/to/skywalking-banyandb-helm
```

You can override the chart version when using OCI:

```
export BANYANDB_BENCH_CHART_VERSION=0.5.3
```

## Running

From repo root:

```
export BANYANDB_BENCH_CHART=/path/to/skywalking-banyandb-helm  # optional
./test/integration/replication/benchmark/run.sh
```

The suite creates a Kind cluster once, builds & loads a local BanyanDB image, then runs RF=1/2/3 sequentially.

## Environment Variables

- `BANYANDB_BENCH_CHART` - chart ref/path (default: OCI ref)
- `BANYANDB_BENCH_CHART_VERSION` - chart version when using OCI (default: 0.5.3)
- `BANYANDB_BENCH_REPORT_DIR` - JSON report output dir (default: parent of repo)
- `BENCH_WRITERS` - number of write workers (default: 4)
- `BENCH_ENTITIES` - number of entities (default: 200)
- `BENCH_POINTS_PER_ENTITY` - points per entity (default: 30)
- `BENCH_QUERY_WORKERS` - query worker count (default: 4)
- `BENCH_QUERY_ITERATIONS` - query iterations (default: 200)
- `BENCH_METRICS_INTERVAL` - Prometheus scrape interval (default: 5s)

## Outputs

- Human-readable Ginkgo summary in test output
- Machine-readable JSON report written to `BANYANDB_BENCH_REPORT_DIR` (or repo parent dir by default)

## Cleanup

The suite deletes the RF namespace and Helm release after each run and deletes the Kind cluster after the suite.

## Local Smoke Test

Use small values and a local chart:

```
export BANYANDB_BENCH_CHART=/path/to/skywalking-banyandb-helm
export BENCH_ENTITIES=20
export BENCH_POINTS_PER_ENTITY=5
export BENCH_QUERY_ITERATIONS=20
./test/integration/replication/benchmark/run.sh
```
