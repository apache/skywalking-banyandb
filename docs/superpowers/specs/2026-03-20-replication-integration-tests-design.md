# Replication Integration Tests Design

## Overview

Add comprehensive replication integration tests for BanyanDB that verify:
1. **Deduplication**: Consistent unique results from multiple replicas
2. **Accessibility**: Queries continue working after a node failure
3. **Recovery**: Failed nodes recover and resync data via handoff queue

## Background

GitHub issue [#13229](https://github.com/apache/skywalking/issues/13229) requests integration tests for BanyanDB replication resilience. The existing replication test only covers `service_traffic` measure with `index_mode: true` in a replicated group and uses embedded etcd for both node discovery and schema storage.

## Key Architectural Changes

### Current State (Must Change)
- Uses embedded etcd for node discovery and schema storage
- `test/integration/replication/replication_suite_test.go` starts embedded etcd server
- Schema loading via `test_measure.PreloadSchema`, `test_stream.PreloadSchema`, etc.

### Target State
- **Property-based schema repo** (`ModeProperty`) - schemas stored in data nodes' property schema servers
- **DNS-based node discovery** (`ModeFile`) - nodes discover each other via `discovery.yaml` file
- **No embedded etcd** - no `embeddedetcd.NewServer()` call

## Design Principles

1. **No test case copying** - Existing test cases in `test/cases/measure/data/`, `test/cases/stream/data/`, etc. are reused
2. **Same schema names** - Measures/streams/traces keep their original names
3. **Separate schema location** - Replicated schemas in a new directory `pkg/test/replicated/`
4. **Isolation** - Replication test suite loads only from `pkg/test/replicated/` schemas
5. **No etcd** - Use property-based schema storage and file-based node discovery

## Infrastructure Changes

### Modified: `test/integration/replication/replication_suite_test.go`

#### Before (etcd-based)
```go
// Start embedded etcd server
server, err := embeddedetcd.NewServer(...)
schemaRegistry, err := schema.NewEtcdSchemaRegistry(...)
test_stream.PreloadSchema(ctx, schemaRegistry)
test_measure.PreloadSchema(ctx, schemaRegistry)
test_trace.PreloadSchema(ctx, schemaRegistry)
clusterConfig = setup.EtcdClusterConfig(ep)
```

#### After (property + file-based)
```go
// Create discovery file writer for DNS-based node discovery
tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
clusterConfig := setup.PropertyClusterConfig(dfWriter)

// Start data nodes and liaison node
// Data nodes register themselves via dfWriter.AddNode()
// Liaison node discovers data nodes from the discovery.yaml file

// Load schemas via property-based registry
setup.PreloadSchemaViaProperty(clusterConfig,
    test_replicated_measure.PreloadSchema,
    test_replicated_stream.PreloadSchema,
    test_replicated_trace.PreloadSchema,
)
```

### Key Setup Functions

| Function | Purpose |
|----------|---------|
| `setup.NewDiscoveryFileWriter(tmpDir)` | Creates a `discovery.yaml` file for node discovery |
| `setup.PropertyClusterConfig(dfWriter)` | Configures cluster with `ModeFile` (node discovery) and `ModeProperty` (schema registry) |
| `setup.PreloadSchemaViaProperty(config, loaders...)` | Loads schemas via property-based schema registry |

## Schema Location

```
pkg/test/replicated/
├── measure/
│   └── testdata/
│       ├── groups/
│       │   ├── sw_metric.json      # replicas: 2
│       │   ├── index_mode.json     # replicas: 2
│       │   └── exception.json      # replicas: 2
│       └── measures/
│           ├── service_traffic.json           # index_mode: true
│           ├── service_cpm_minute.json        # normal mode
│           ├── service_instance_traffic.json
│           └── ... (all existing measures, same names)
├── stream/
│   └── testdata/
│       ├── group.json                         # replicas: 2
│       ├── group_with_stages.json            # replicas: 2
│       └── streams/
│           ├── sw.json
│           ├── duplicated.json
│           └── ... (all existing streams)
├── trace/
│   └── testdata/
│       └── groups/
│           ├── test-trace-group.json          # replicas: 2
│           ├── test-trace-updated.json        # replicas: 2
│           ├── zipkin-trace-group.json        # replicas: 2
│           ├── test-trace-spec.json           # replicas: 2
│           └── test-trace-spec2.json          # replicas: 2
└── topn/
    └── testdata/
        └── topnaggregations/
            └── ... (existing TopN schemas)
```

## Schema Design

### Measure Groups

| Original Group | Replicas (Original) | Replicas (New) |
|----------------|---------------------|----------------|
| sw_metric | 1 | 2 |
| index_mode | - | 2 |
| exception | 0 | 2 |

### Stream Groups

| Original Group | Replicas (Original) | Replicas (New) |
|----------------|---------------------|----------------|
| default | 0 | 2 |
| default-spec | 0 | 2 |
| default-spec2 | 0 | 2 |

### Trace Groups

| Original Group | Replicas (Original) | Replicas (New) |
|----------------|---------------------|----------------|
| test-trace-group | 0 | 2 |
| zipkinTrace | 0 | 2 |
| test-trace-updated | 0 | 2 |
| test-trace-spec | 0 | 2 |
| test-trace-spec2 | 0 | 2 |

## Implementation Steps

### Step 1: Modify Current Replication Suite (Must Do First)

**File:** `test/integration/replication/replication_suite_test.go`

1. Remove embedded etcd server startup
2. Remove `schema.NewEtcdSchemaRegistry()` call
3. Add `setup.NewDiscoveryFileWriter()` for DNS-based node discovery
4. Use `setup.PropertyClusterConfig()` instead of `setup.EtcdClusterConfig()`
5. Use `setup.PreloadSchemaViaProperty()` instead of direct schema preloading
6. Remove `embeddedetcd` import

**Node Discovery Flow:**
- Each data node calls `config.NodeDiscovery.FileWriter.AddNode(nodeName, grpcAddr)` to register itself in `discovery.yaml`
- The liaison node reads `discovery.yaml` to discover data nodes

### Step 2: Create Replicated Schema Directory

**Location:** `pkg/test/replicated/`

Structure mirrors `pkg/test/measure/`, `pkg/test/stream/`, `pkg/test/trace/`

### Step 3: Create Replicated Schemas

For each measure/stream/trace schema:
1. Copy from original location (`pkg/test/measure/testdata/`, etc.)
2. Modify group to have `replicas: 2`
3. Keep measure/stream/trace names unchanged

### Step 4: Create Replicated Preload Functions

**Files:**
- `pkg/test/replicated/measure/etcd.go` - PreloadSchema for replicated measures
- `pkg/test/replicated/stream/etcd.go` - PreloadSchema for replicated streams
- `pkg/test/replicated/trace/etcd.go` - PreloadSchema for replicated traces

### Step 5: Add New Replication Tests

**Files:**
- `test/integration/replication/measure_normal_replication_test.go` - Normal mode measure tests
- `test/integration/replication/stream_replication_test.go` - Stream tests
- `test/integration/replication/trace_replication_test.go` - Trace tests

## Extended Test Pattern

For each data type (measure, stream, trace), the test verifies:

### 1. Deduplication Verification
```go
// Query the same data multiple times
var firstResult *measurev1.QueryResponse
for i := 0; i < 3; i++ {
    resp := query()
    if i == 0 {
        firstResult = resp
    } else {
        // Verify consistent results (deduplication works across replicas)
        Expect(cmp.Equal(resp, firstResult, ...)).To(BeTrue())
    }
}
```

### 2. Accessibility Verification
```go
// Stop one data node
closersToStop[0]()

// Wait for cluster stabilization via file-based discovery
gm.Eventually(func() int {
    // Check number of active nodes in discovery file
    return len(dfWriter.ListNodes())
}, flags.EventuallyTimeout).Should(gm.Equal(3))

// Verify queries still work
verifyQueryResults(conn, now)
```

### 3. Recovery Verification
```go
// Restart the stopped node
restartedNode := setup.DataNode(clusterConfig, "--node-labels", "role=data")

// Wait for handoff queue to drain
gm.Eventually(func() int {
    return countPendingParts(liaisonAddr)
}, flags.EventuallyTimeout).Should(gm.Equal(0))

// Verify data is fully recovered
verifyQueryResults(conn, now)
```

## Acceptance Criteria

1. **No etcd** - Replication tests do not start embedded etcd server
2. **Property-based schema** - Schemas loaded via `ModeProperty` registry
3. **File-based discovery** - Nodes discover each other via `discovery.yaml`
4. **No test case copying** - Existing test cases reused
5. **Same schema names** - Replicated schemas use identical names to originals
6. **Deduplication verified** - Consistent results from replicas
7. **Accessibility verified** - Queries work after node failure
8. **Recovery verified** - Handoff queue drains after node restart
