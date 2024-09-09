# Observability

This document outlines the observability features of BanyanDB, which include metrics, profiling, and tracing. These features help monitor and understand the performance, behavior, and overall health of BanyanDB.

## Logging

BanyanDB uses the [zerolo](https://github.com/rs/zerolog) library for logging. The log level can be set using the `log-level` flag. The supported log levels are `debug`, `info`, `warn`, `error`, and `fatal`. The default log level is `info`.

`logging-env` is used to set the logging environment. The default value is `prod`. The logging environment can be set to `dev` for development or `prod` for production. The logging environment affects the log format and output. In the `dev` environment, logs are output in a human-readable format, while in the `prod` environment, logs are output in JSON format.

`logging-modules` and `logging-levels` are used to set the log level for specific modules. The `logging-modules` flag is a comma-separated list of module names, and the `logging-levels` flag is a comma-separated list of log levels corresponding to the module names. The log level for a specific module can be set using these flags. Available modules are `storage`, `distributed-query`, `liaison-grpc`, `liaison-http`, `measure`, `stream`, `metadata`, `etcd-client`, `etcd-server`, `schema-registry`, `metrics`, `pprof-service`, `query`, `server-queue-sub`, `server-queue-pub`. For example, to set the log level for the `storage` module to `debug`, you can use the following flags:

```sh
--logging-modules=storage --logging-levels=debug
```

### Slow Query Logging

BanyanDB supports slow query logging. The `slow-query` flag is used to set the slow query threshold. If a query takes longer than the threshold, it will be logged as a slow query. The default value is `0`, which means no slow query logging. This flag is only used for the data and standalone servers.

The `dst-slow-query` flag is used to set the distributed slow query threshold. This flag is only used for the liaison server. The default value is `0`, which means no distributed slow query logging.

When query tracing is enabled, the slow query log won't be generated.

## Metrics

BanyanDB has built-in support for metrics collection. Currently, there are two supported metrics provider: `prometheus` and `native`. These can be enabled through `observability-modes` flag, allowing you to activate one or both of them.

### Prometheus

Prometheus is auto enabled at run time, if no flag is passed or if `promethus` is set in `observability-modes` flag.

When the Prometheus metrics provider is enabled, the metrics server listens on port `2121`. This allows Prometheus to scrape metrics data from BanyanDB for monitoring and analysis.

### Self-observability

If the `observability-modes` flag is set to `native`, the self-observability metrics provider will be enabled. The some of metrics will be displayed in the dashboard of [banyandb-ui](http://localhost:17913/) 

![dashboard](https://skywalking.apache.org/doc-graph/banyandb/v0.7.0/dashboard.png)

#### Metrics storage

In self-observability, the metrics data is stored in BanyanDB within the ` _monitoring` internal group. Each metric will be created as a new `measure` within this group.

You can use BanyanDB-UI or bydbctl to retrieve the data.

#### Write Flow

When starting any node, the `_monitoring` internal group will be created, and the metrics will be created as measures within this group. All metric values will be collected and written together at a configurable fixed interval. For a data node, it will write metric values to its own shard using a local pipeline. For a liaison node, it will use nodeSelector to select a data node to write its metric data.

![self-observability-write](https://skywalking.apache.org/doc-graph/banyandb/v0.7.0/self-observability-write.png)

#### Read Flow

The read flow is the same as reading data from `measure`, with each metric being a new measure.

## Profiling

Banyand, the server of BanyanDB, supports profiling automatically. The profiling data is collected by the `pprof` package and can be accessed through the `/debug/pprof` endpoint. The port of the profiling server is `2122` by default.

## Query Tracing

BanyanDB supports query tracing, which allows you to trace the execution of a query. The tracing data includes the query plan, execution time, and other useful information. You can enable query tracing by setting the `QueryRequest.trace` field to `true` when sending a query request.

The below command could query data in the last 30 minutes with `trace` enabled:

```shell
bydbctl measure query --start -30m -f - <<EOF
name: "service_cpm_minute"
groups: ["measure-minute"]
tagProjection:
  tagFamilies:
    - name: "storage-only"
      tags: ["entity_id"]
fieldProjection:
  names: ["total", "value"]
trace: true
EOF
```

The result will include the tracing data in the response. The duration time unit is in nano seconds.
