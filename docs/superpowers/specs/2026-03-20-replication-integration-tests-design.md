# Replication Integration Tests Design

## Overview

Add comprehensive replication integration tests for BanyanDB that verify:
1. **Deduplication**: Consistent unique results from multiple replicas
2. **Accessibility**: Queries continue working after a node failure
3. **Recovery**: Failed nodes recover and resync data via handoff queue

## Background

GitHub issue [#13229](https://github.com/apache/skywalking/issues/13229) requests integration tests for BanyanDB replication resilience. The existing replication test only covers `service_traffic` measure with `index_mode: true` in a replicated group.

## Scope

This design adds replication tests for:
- **Stream** - replicated stream schemas
- **Measure (normal mode)** - measures without `index_mode: true`
- **Trace** - replicated trace schemas
- **TopN** - replicated TopN schemas (runs on top of measures)

## Design Principles

1. **No test case copying** - Existing test cases in `test/cases/measure/data/`, `test/cases/stream/data/`, etc. are reused
2. **Same schema names** - Measures/streams/traces keep their original names
3. **Separate schema location** - Replicated schemas in a new directory `pkg/test/replicated/`
4. **Isolation** - Replication test suite loads only from `pkg/test/replicated/` schemas

## Schema Location

```
pkg/test/replicated/
├── measure/
│   └── testdata/
│       ├── groups/
│       │   ├── sw_metric_replicated.json      # replicas: 2
│       │   ├── index_mode_replicated.json     # replicas: 2
│       │   └── exception_replicated.json      # replicas: 2
│       └── measures/
│           ├── service_traffic.json           # index_mode: true
│           ├── service_cpm_minute.json        # normal mode
│           ├── service_instance_traffic.json
│           └── ... (all existing measures, same names)
├── stream/
│   └── testdata/
│       ├── group.json                         # replicas: 2
│       └── streams/
│           ├── sw.json
│           ├── duplicated.json
│           └── ... (all existing streams)
├── trace/
│   └── testdata/
│       └── groups/
│           ├── test-trace-group.json          # replicas: 2
│           └── zipkin-trace-group.json        # replicas: 2
└── topn/
    └── testdata/
        └── topnaggregations/
            └── ... (existing TopN schemas)
```

## Schema Design

### Measure Groups

| Original Group | Replicated Group | Replicas |
|----------------|------------------|----------|
| sw_metric | sw_metric | 2 |
| index_mode | index_mode | 2 |
| exception | exception | 2 |

### Stream Groups

| Original Group | Replicated Group | Replicas |
|----------------|------------------|----------|
| default | default | 2 |
| default-spec | default-spec | 2 |
| default-spec2 | default-spec2 | 2 |

### Trace Groups

| Original Group | Replicated Group | Replicas |
|----------------|------------------|----------|
| test-trace-group | test-trace-group | 2 |
| zipkinTrace | zipkinTrace | 2 |
| test-trace-updated | test-trace-updated | 2 |
| test-trace-spec | test-trace-spec | 2 |
| test-trace-spec2 | test-trace-spec2 | 2 |

## Test Files to Create

### Schema Files (pkg/test/replicated/)

#### Measure Schemas
- `pkg/test/replicated/measure/testdata/groups/sw_metric.json` (copy from `pkg/test/measure/testdata/groups/sw_metric.json` with `replicas: 2`)
- `pkg/test/replicated/measure/testdata/groups/index_mode.json` (copy with `replicas: 2`)
- `pkg/test/replicated/measure/testdata/groups/exception.json` (copy with `replicas: 2`)
- `pkg/test/replicated/measure/testdata/measures/*.json` (all measure schemas)

#### Stream Schemas
- `pkg/test/replicated/stream/testdata/group.json` (copy with `replicas: 2`)
- `pkg/test/replicated/stream/testdata/group_with_stages.json` (copy with `replicas: 2`)
- `pkg/test/replicated/stream/testdata/streams/*.json` (all stream schemas)

#### Trace Schemas
- `pkg/test/replicated/trace/testdata/groups/test-trace-group.json` (copy with `replicas: 2`)
- `pkg/test/replicated/trace/testdata/groups/test-trace-updated.json` (copy with `replicas: 2`)
- `pkg/test/replicated/trace/testdata/groups/zipkin-trace-group.json` (copy with `replicas: 2`)
- `pkg/test/replicated/trace/testdata/groups/test-trace-spec.json` (copy with `replicas: 2`)
- `pkg/test/replicated/trace/testdata/groups/test-trace-spec2.json` (copy with `replicas: 2`)

#### TopN Schemas
- `pkg/test/replicated/topn/testdata/topnaggregations/*.json` (all TopN schemas)

### Go Files (pkg/test/replicated/)

#### etcd.go Preload Functions
- `pkg/test/replicated/measure/etcd.go` - `PreloadSchema(ctx, registry)` for replicated measures
- `pkg/test/replicated/stream/etcd.go` - `PreloadSchema(ctx, registry)` for replicated streams
- `pkg/test/replicated/trace/etcd.go` - `PreloadSchema(ctx, registry)` for replicated traces

### Integration Test Files (test/integration/replication/)

#### New Test Files
- `test/integration/replication/measure_normal_replication_test.go` - Normal mode measure replication tests
- `test/integration/replication/stream_replication_test.go` - Stream replication tests
- `test/integration/replication/trace_replication_test.go` - Trace replication tests

#### Modified Files
- `test/integration/replication/replication_suite_test.go` - Add preloading of replicated schemas
- `test/cases/init.go` - Add data initialization for replicated schemas

## Extended Test Pattern

For each data type (measure, stream, trace), the test verifies:

### 1. Deduplication Verification
```go
// Query the same data multiple times
for i := 0; i < 3; i++ {
    resp := query()
// Verify results are consistent (deduplication works across replicas)
}
```

### 2. Accessibility Verification
```go
// Stop one data node
closersToStop[0]()

// Wait for cluster stabilization
gm.Eventually(func() int {
    nodes, _ := helpers.ListKeys(etcdEndpoint, nodePath)
    return len(nodes)
}, flags.EventuallyTimeout).Should(gm.Equal(3))

// Verify queries still work
verifyQueryResults(conn, now)
```

### 3. Recovery Verification
```go
// Restart the stopped node
restartedNode := startDataNode(config)

// Wait for handoff queue to drain
gm.Eventually(func() int {
    return countPendingParts(liaisonAddr)
}, flags.EventuallyTimeout).Should(gm.Equal(0))

// Verify data is fully recovered
verifyQueryResults(conn, now)
```

## Implementation Order

1. **Measure (normal mode)** - Create `pkg/test/replicated/measure/` and add measure replication tests
2. **Stream** - Create `pkg/test/replicated/stream/` and add stream replication tests
3. **Trace** - Create `pkg/test/replicated/trace/` and add trace replication tests
4. **TopN** - Create `pkg/test/replicated/topn/` and add TopN replication tests

## Data Initialization

The `test/cases/init.go` file will be extended to initialize data for replicated schemas:

```go
// replicated measure
casesmeasuredata.Write(conn, "service_traffic", "index_mode", "service_traffic_data.json", now, interval)
casesmeasuredata.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", now, interval)
// ... (all measures written to their replicated groups)

// replicated stream
casesstreamdata.Write(conn, "sw", now, interval)
// ... (all streams written to their replicated groups)

// replicated trace
casestrace.WriteToGroup(conn, "sw", "test-trace-group", "sw", now, interval)
// ... (all traces written to their replicated groups)
```

## Acceptance Criteria

1. All existing test cases in `test/cases/measure/data/`, `test/cases/stream/data/`, `test/cases/trace/data/` can run against replicated schemas without modification
2. Replication tests verify deduplication (consistent results from replicas)
3. Replication tests verify accessibility (queries work after node failure)
4. Replication tests verify recovery (handoff queue drains after node restart)
5. Test infrastructure is isolated: replication tests use only `pkg/test/replicated/` schemas
