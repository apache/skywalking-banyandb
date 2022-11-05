# Changes by Version

Release Notes.

## 0.2.0

### Features

- Command line tool: bydbctl.
- Retention controller.
- Full-text searching.
- TopN aggregation.
- Add RESTFul style APIs based on gRPC gateway.
- Add "exists" endpoints to the schema registry.
- Support tag-based CRUD of the property.
- Support index-only tags.
- Support logical operator(and & or) for the query.

### Bugs

- "metadata" syncing pipeline complains about an "unknown group".
- "having" semantic inconsistency.
- "tsdb" leaked goroutines.

### Chores

- "tsdb" structure optimization.
  - Merge the primary index into the LSM-based index
  - Remove term metadata.
- Memory parameters optimization.
- Bump go to 1.19.

For more details by referring to [milestone 0.2.0](https://github.com/apache/skywalking/issues?q=is%3Aissue+milestone%3A%22BanyanDB+-+0.2.0%22)

## 0.1.0

### Features

- BanyanD is the server of BanyanDB
  - TSDB module. It provides the primary time series database with a key-value data module.
  - Stream module. It implements the stream data model's writing.
  - Measure module. It implements the measure data model's writing.
  - Metadata module. It implements resource registering and property CRUD.
  - Query module. It handles the querying requests of stream and measure.
  - Liaison module. It's the gateway to other modules and provides access endpoints to clients.
- gRPC based APIs
- Document
  - API reference
  - Installation instrument
  - Basic concepts
- Testing
  - UT
  - E2E with Java Client and OAP
