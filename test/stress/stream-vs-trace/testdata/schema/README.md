# Schema Files for Stream vs Trace Performance Test

This directory contains the schema definitions for the performance comparison test between BanyanDB's Stream and Trace models.

## Files

### Group Definitions
- `group.json` - Defines two separate groups:
  - `stream_performance_test` with CATALOG_STREAM
  - `trace_performance_test` with CATALOG_TRACE

### Stream Model (stream_performance_test group)
- `stream_schema.json` - Stream schema definition with entity-based structure
- `stream_index_rules.json` - Individual index rules for each tag (except entity tags and dataBinary)
- `stream_index_rule_bindings.json` - Binds index rules to the stream schema

### Trace Model (trace_performance_test group)
- `trace_schema.json` - Trace schema definition with trace-specific structure
- `trace_index_rules.json` - Two composite index rules (time-based and latency-based)
- `trace_index_rule_bindings.json` - Binds index rules to the trace schema

## Schema Differences

### Stream Model
- Uses entity-based structure with `serviceId` and `serviceInstanceId` as entities
- `dataBinary` is stored in a separate tag family
- Individual index rules for each tag (except entity tags and dataBinary)
- No time-based index (startTime is not indexed)

### Trace Model
- Uses trace-specific structure with `traceId` as trace ID tag and `startTime` as timestamp tag
- `dataBinary` is stored as a regular tag
- Two composite index rules:
  - `serviceId + serviceInstanceId + startTime`
  - `serviceId + serviceInstanceId + latency`

## Usage

These schema files can be used to:
1. Create the necessary schemas in BanyanDB for the performance test
2. Validate schema definitions before running tests
3. Reference the exact structure used in the performance comparison

## Data Structure

Both schemas are designed to store SkyWalking segment data (spans) with the following fields:
- `serviceId`, `serviceInstanceId` - Service identification
- `traceId`, `spanId`, `parentSpanId` - Trace and span identification
- `startTime`, `latency` - Temporal information
- `operationName`, `component` - Semantic information
- `isError` - Error status
- `dataBinary` - Serialized span data
