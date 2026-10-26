# 0.12.0 Design Documents

- [Trace sampling before lifecycle migration](lifecycle-trace-sampling.md)
- [Measure QL Catalog B: core SQL implementation plan](measure-ql-core-sql/README.md)
  - [PR 1 scope and isolated verification](measure-ql-core-sql/pr1-verification.md)
  - [PR 2 aggregation scope and isolated verification](measure-ql-core-sql/pr2-verification.md)
  - [Compiled measure QL plan reuse](measure-ql-core-sql/plan-reuse.md)
  - [Implementation tickets: per-ticket end-to-end tests and benchmarks](measure-ql-core-sql/implementation-tickets.md)
- [Tag aggregation and time bucketing in the measure query engine](tag-aggregation/README.md)
- [Self-stored logs](self-stored-logs/README.md)
- [Time-range pruning in the native inverted index](native-index-time-pruning/README.md)
- [Measure batch v2: reference-counted, columnar data unit](measure-batch-v2/README.md)
- [Measure scan path v2: streaming series windows and a time-ordered merge](measure-scan-path-v2/README.md)
- [Proto-free BydbQL measure planning with a reusable plan cache](bydbql-plan-cache/README.md)
- [Measure QL: remaining gaps and order of work](measure-ql-gaps/README.md)
- [Aggregating measures and materialized views](measure-aggregation-mv/README.md)
- [Streaming API: progressive queries and acknowledged write streams](streaming-api/README.md)

The [native inverted-index replacement](../archive/0.12.0/native-inverted-index/README.md)
design shipped and has moved to the archive ahead of the rest of this milestone.
