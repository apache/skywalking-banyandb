# Stream vs Trace Performance Test

This directory contains the performance comparison test between BanyanDB's Stream and Trace models for storing SkyWalking segment data (spans).

## Overview

The test compares the performance characteristics of two different data models:
- **Stream Model**: Entity-based structure with individual tag indexes
- **Trace Model**: Trace-specific structure with composite indexes

## Test Structure

### Schema Files
- `testdata/schema/` - Contains all schema definitions (groups, schemas, index rules, bindings)
- `schema_loader.go` - Loads schemas into BanyanDB during test setup
- `stream_vs_trace_suite_test.go` - Main test suite using Ginkgo
- `stream_client.go` - Stream model client for operations
- `trace_client.go` - Trace model client for operations

### Test Setup

The test uses `setup.ClosableStandaloneWithSchemaLoaders` to:
1. Start a standalone BanyanDB instance
2. Load both stream and trace schemas
3. Create necessary groups, index rules, and bindings
4. Verify schema creation

### Schema Differences

#### Stream Model (`stream_performance_test` group)
- **Entity**: `serviceId + serviceInstanceId`
- **Catalog**: `CATALOG_STREAM`
- **Indexes**: Individual indexes for each tag (except entity tags and dataBinary)
- **Data Binary**: Stored in separate tag family

#### Trace Model (`trace_performance_test` group)
- **Catalog**: `CATALOG_TRACE`
- **Indexes**: Two composite indexes:
  - `serviceId + serviceInstanceId + startTime`
  - `serviceId + serviceInstanceId + latency`
- **Data Binary**: Stored as regular tag

## Running the Test

```bash
# Run the performance test
cd test/stress/stream-vs-trace
go test -v -timeout 30m

# Run with specific Ginkgo labels
go test -v -timeout 30m --ginkgo.label-filter="performance"
```

## Test Phases

1. **Schema Setup**: Creates groups, schemas, and index rules
2. **Schema Verification**: Verifies both models are properly created
3. **Performance Testing**: (TODO) Implements actual performance benchmarks
4. **Cleanup**: Cleans up test resources

## Expected Results

The test will verify that:
- Both stream and trace schemas are created successfully
- All index rules and bindings are applied correctly
- Both models are ready for performance testing

## Next Steps

This is the foundation for the full performance comparison test. The actual performance benchmarks (write throughput, query latency, storage efficiency) will be implemented in subsequent phases.

## Dependencies

- Ginkgo v2 for test framework
- BanyanDB test setup utilities
- gRPC clients for stream and trace operations
