# Design Documents

Design documents are grouped by the milestone in which they were introduced. The current
milestone sits directly under this directory and holds live working documents; new design work
must be added there when it is introduced. When a milestone ships, its directory moves into
[`archive/`](archive/README.md).

## Current milestone — 0.12.0

- [Measure QL Catalog B: core SQL implementation plan](0.12.0/measure-ql-core-sql/README.md)
- [Tag aggregation and time bucketing in the measure query engine](0.12.0/tag-aggregation/README.md)
- [Data export and import](0.12.0/data-export-import/README.md)
- [Docker-canonical build and license generation](0.12.0/docker-canonical-build/README.md)
- [Measure batch v2: reference-counted, columnar data unit](0.12.0/measure-batch-v2/README.md)
- [Measure scan path v2: streaming series windows and a time-ordered merge](0.12.0/measure-scan-path-v2/README.md)
- [Proto-free BydbQL measure planning with a reusable plan cache](0.12.0/bydbql-plan-cache/README.md)
- [Measure QL: remaining gaps and order of work](0.12.0/measure-ql-gaps/README.md)
- [Aggregating measures and materialized views](0.12.0/measure-aggregation-mv/README.md)
- [Streaming API: progressive queries and acknowledged write streams](0.12.0/streaming-api/README.md)

The native inverted-index replacement design shipped and its legacy engine
dependency has since been fully removed; see
[`archive/0.12.0/native-inverted-index/README.md`](archive/0.12.0/native-inverted-index/README.md)
(archived ahead of the rest of 0.12.0, which has not shipped yet).

## Shipped milestones

See [`archive/`](archive/README.md) for 0.11.0 and earlier.
