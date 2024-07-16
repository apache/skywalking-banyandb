# Observability

This document outlines the observability features of BanyanDB, which include metrics, profiling, and tracing. These features help monitor and understand the performance, behavior, and overall health of BanyanDB.

## Metrics

BanyanDB has built-in support for metrics collection. Currently, there is only one supported metrics provider: `Prometheus`. It is auto enabled at run time through `observability-modes` flag. 

When the Prometheus metrics provider is enabled, the metrics server listens on port `2121`. This allows Prometheus to scrape metrics data from BanyanDB for monitoring and analysis.

The Docker image is tagged as "prometheus" to facilitate cloud-native operations and simplify deployment on Kubernetes. This allows users to directly deploy the Docker image onto their Kubernetes cluster without having to rebuild it with the "prometheus" tag.

## Profiling

Banyand, the server of BanyanDB, supports profiling automatically. The profiling data is collected by the `pprof` package and can be accessed through the `/debug/pprof` endpoint. The port of the profiling server is `2122` by default.

## Tracing
