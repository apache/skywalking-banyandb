# Observability

This document outlines the observability features of BanyanDB, which include metrics, profiling, and tracing. These features help monitor and understand the performance, behavior, and overall health of BanyanDB.

## Metrics

BanyanDB has built-in support for metrics collection through the use of build tags. The metrics provider can be enabled by specifying the build tag during the compilation process.

Currently, there is only one supported metrics provider: `Prometheus`. To use Prometheus as the metrics client, include the `prometheus` build tag when building BanyanDB:

`BUILD_TAGS=prometheus make -C banyand banyand-server`

If no build tag is specified, the metrics server will not be started, and no metrics will be collected:

`make -C banyand banyand-server`

When the Prometheus metrics provider is enabled, the metrics server listens on port `2121`. This allows Prometheus to scrape metrics data from BanyanDB for monitoring and analysis.

The Docker image is tagged as "prometheus" to facilitate cloud-native operations and simplify deployment on Kubernetes. This allows users to directly deploy the Docker image onto their Kubernetes cluster without having to rebuild it with the "prometheus" tag.

## Profiling

Banyand, the server of BanyanDB, supports profiling automatically. The profiling data is collected by the `pprof` package and can be accessed through the `/debug/pprof` endpoint. The port of the profiling server is `2122` by default.

## Tracing

TODO: Add details about the tracing support in BanyanDB, such as how to enable tracing, available tracing tools, and how to analyze tracing data.
