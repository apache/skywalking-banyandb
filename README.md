# BanyanDB

![](https://github.com/apache/skywalking-banyandb/workflows/Build/badge.svg?branch=main)

![](./assets/banyandb_small.jpg)

BanyanDB, as an observability database, aims to ingest, analyze and store Metrics, Tracing and Logging data.
It's designed to handle observability data generated by observability platform and APM system, like [Apache SkyWalking](https://github.com/apache/skywalking) etc.

## Introduction

BanyanDB, as an observability database, aims to ingest, analyze and store Metrics, Tracing, and Logging data. It's designed to handle observability data generated by Apache SkyWalking. Before BanyanDB emerges, the Databases that SkyWalking adopted are not ideal for the APM data model, especially for saving tracing and logging data. Consequently, There’s room to improve the performance and resource usage based on the nature of SkyWalking data patterns.

The database research community usually uses [RUM conjecture](http://daslab.seas.harvard.edu/rum-conjecture/) to describe how a database access data. BanyanDB combines several access methods to build a comprehensive APM database to balance read cost, update cost, and memory overhead.

## Documents

[Documents](https://skywalking.apache.org/docs/skywalking-banyandb/latest/readme/)

## RoadMap

### Client manager

- [x] gRPC server
- [ ] HTTP server (v0.2.0)

### Distributed manager

- [ ] Sharding
- [ ] Replication and consistency model
- [ ] Load balance
- [ ] Distributed query optimizer
- [ ] Node discovery
- [ ] Data queue

### Data processor

- [x] Schema management
- [x] Time-series abstract layer
- [x] Stream data processor
- [x] Measure data processor
- [x] Property data processor
- [ ] TopNAggregation processor (v0.2.0)
- [x] Index processor
- [ ] TTL (v0.2.0)
- [ ] Cold data processor (v0.2.0)
- [ ] WAL

### Query processor

- [x] Stream query processor
- [x] Measure query processor
- [x] Index reader
- [ ] Streaming pipeline processor(OR and nested querying) (v0.2.0)
- [ ] Parallel executor
- [ ] Cost-based optimizer

### Verification

- [x] E2E with OAP and simulated data
- [ ] E2E with showcases, agents and OAP (v0.2.0)
- [ ] Space utilization rate (v0.2.0)
- [ ] Leading and trailing zero (v0.2.0)
- [ ] Stability (v0.2.0)
- [ ] Crash recovery
- [ ] Performance

### Tools

- [ ] Command-line (v0.2.0)
- [ ] Webapp (v0.2.0)

## Contributing

For developers who want to contribute to this project, see [Contribution Guide](CONTRIBUTING.md)

## License

[Apache 2.0 License.](/LICENSE)
