# Replication Integration Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add comprehensive replication integration tests that verify deduplication, accessibility, and recovery using property-based schema storage and file-based node discovery (no embedded etcd).

**Architecture:** Convert the existing etcd-based replication suite to use property-based schema repo (`ModeProperty`) and DNS-based node discovery (`ModeFile`). Create a new `pkg/test/replicated/` directory containing schemas with `replicas: 2` for measures, streams, and traces.

**Tech Stack:** Go, Ginkgo/Gomega testing framework, BanyanDB test infrastructure (`pkg/test/setup/`)

---

## File Structure

### Modified Files
- `test/integration/replication/replication_suite_test.go` - Convert from etcd to property+file-based

### New Files to Create

**Replicated Measure Schemas (`pkg/test/replicated/measure/`):**
- `testdata/groups/index_mode.json` (replicas: 2)
- `testdata/groups/sw_metric.json` (replicas: 2)
- `testdata/groups/exception.json` (replicas: 2)
- `testdata/measures/service_traffic.json` (index_mode: true, in index_mode group)
- `testdata/measures/service_cpm_minute.json` (normal mode, in sw_metric group)
- `testdata/measures/service_instance_traffic.json` (and other measures)
- `etcd.go` - PreloadSchema function

**Replicated Stream Schemas (`pkg/test/replicated/stream/`):**
- `testdata/group.json` (replicas: 2)
- `testdata/group_with_stages.json` (replicas: 2)
- `testdata/streams/sw.json` (and other streams)
- `etcd.go` - PreloadSchema function

**Replicated Trace Schemas (`pkg/test/replicated/trace/`):**
- `testdata/groups/test-trace-group.json` (replicas: 2)
- `testdata/groups/test-trace-updated.json` (replicas: 2)
- `testdata/groups/zipkin-trace-group.json` (replicas: 2)
- `testdata/groups/test-trace-spec.json` (replicas: 2)
- `testdata/groups/test-trace-spec2.json` (replicas: 2)
- `etcd.go` - PreloadSchema function

**New Test Files:**
- `test/integration/replication/measure_normal_replication_test.go`
- `test/integration/replication/stream_replication_test.go`
- `test/integration/replication/trace_replication_test.go`

---

## Phase 1: Convert Current Replication Suite to Property+File-Based

### Task 1: Modify replication_suite_test.go to use property-based infrastructure

**File:** `test/integration/replication/replication_suite_test.go`

**Changes:**
- Remove `embeddedetcd` import and server startup
- Remove `schema.NewEtcdSchemaRegistry()` call
- Add `setup.NewDiscoveryFileWriter()` and `setup.PropertyClusterConfig()`
- Use `setup.PreloadSchemaViaProperty()` for schema loading
- Update node discovery to use file-based approach

**Reference:** See `test/integration/distributed/cluster_state/property/suite_test.go` for pattern

```go
// BEFORE (lines 78-111)
By("Starting etcd server")
ports, err := test.AllocateFreePorts(2)
dir, spaceDef, err := test.NewSpace()
Expect(err).NotTo(HaveOccurred())
ep := fmt.Sprintf("http://127.0.0.1:%d", ports[0])
etcdEndpoint = ep

server, err := embeddedetcd.NewServer(...)
<-server.ReadyNotify()

schemaRegistry, err := schema.NewEtcdSchemaRegistry(...)
ctx := context.Background()
test_stream.PreloadSchema(ctx, schemaRegistry)
test_measure.PreloadSchema(ctx, schemaRegistry)
test_trace.PreloadSchema(ctx, schemaRegistry)
test_property.PreloadSchema(ctx, schemaRegistry)

clusterConfig = setup.EtcdClusterConfig(ep)

// AFTER
By("Creating discovery file writer for DNS-based node discovery")
tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
Expect(tmpErr).NotTo(HaveOccurred())
dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
clusterConfig = setup.PropertyClusterConfig(dfWriter)

By("Starting 3 data nodes for replication test")
dataNodeClosers = make([]func(), 0, 3)
for i := 0; i < 3; i++ {
    closeDataNode := setup.DataNode(clusterConfig, "--node-labels", "role=data")
    dataNodeClosers = append(dataNodeClosers, closeDataNode)
}

By("Starting liaison node")
liaisonAddr, closerLiaisonNode := setup.LiaisonNode(clusterConfig, "--data-node-selector", "role=data")

// Load schemas via property-based registry
setup.PreloadSchemaViaProperty(clusterConfig,
    test_replicated_measure.PreloadSchema,
    test_replicated_stream.PreloadSchema,
    test_replicated_trace.PreloadSchema,
)
```

- [ ] **Step 1: Read the full current replication_suite_test.go**

Run: `cat test/integration/replication/replication_suite_test.go`

- [ ] **Step 2: Read property-based cluster_state suite for reference**

Run: `cat test/integration/distributed/cluster_state/property/suite_test.go`

- [ ] **Step 3: Create backup and modify replication_suite_test.go**

Remove lines 33-36 (embeddedetcd imports), lines 78-94 (etcd server), lines 96-109 (schema loading), update clusterConfig line 111

- [ ] **Step 4: Add new imports and setup code**

Add `setup.NewDiscoveryFileWriter` and `setup.PropertyClusterConfig` initialization after line 76

- [ ] **Step 5: Replace PreloadSchema calls**

Replace direct schema preloading with `setup.PreloadSchemaViaProperty()`

- [ ] **Step 6: Update cleanup function**

Remove `server.Close()` and `server.StopNotify()`, add `tmpDirCleanup()`

- [ ] **Step 7: Run existing replication test to verify it works**

Run: `go test -v ./test/integration/replication/... -run TestReplication`
Expected: Existing replication test passes with property-based setup

- [ ] **Step 8: Commit**

```bash
git add test/integration/replication/replication_suite_test.go
git commit -m "refactor(replication): convert from etcd to property+file-based discovery"
```

---

## Phase 2: Create Replicated Schema Directory Structure

### Task 2: Create pkg/test/replicated/measure/ directory

**Files to create:**
- `pkg/test/replicated/measure/testdata/groups/index_mode.json`
- `pkg/test/replicated/measure/testdata/groups/sw_metric.json`
- `pkg/test/replicated/measure/testdata/groups/exception.json`
- `pkg/test/replicated/measure/testdata/measures/service_traffic.json`
- `pkg/test/replicated/measure/testdata/measures/service_cpm_minute.json`
- `pkg/test/replicated/measure/testdata/measures/*.json` (all other measures)
- `pkg/test/replicated/measure/etcd.go`

**Pattern:** Copy from `pkg/test/measure/` and change `replicas: 2` in group JSON

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p pkg/test/replicated/measure/testdata/{groups,measures,index_rules,index_rule_bindings}
```

- [ ] **Step 2: Create index_mode.json group with replicas: 2**

Create from `pkg/test/measure/testdata/groups/index_mode.json` source

- [ ] **Step 3: Create sw_metric.json group with replicas: 2**

Create from `pkg/test/measure/testdata/groups/sw_metric.json` source

- [ ] **Step 4: Create exception.json group with replicas: 2**

Create from `pkg/test/measure/testdata/groups/exception.json` source

- [ ] **Step 5: Copy measure schema files**

```bash
cp pkg/test/measure/testdata/measures/*.json pkg/test/replicated/measure/testdata/measures/
```

- [ ] **Step 6: Copy index_rules and index_rule_bindings**

```bash
cp -r pkg/test/measure/testdata/index_rules pkg/test/replicated/measure/testdata/
cp -r pkg/test/measure/testdata/index_rule_bindings pkg/test/replicated/measure/testdata/
```

- [ ] **Step 7: Create etcd.go preload function**

Create `pkg/test/replicated/measure/etcd.go` following pattern from `pkg/test/measure/etcd.go`

- [ ] **Step 8: Commit**

```bash
git add pkg/test/replicated/measure/
git commit -m "feat(replicated): add replicated measure schemas"
```

### Task 3: Create pkg/test/replicated/stream/ directory

**Files to create:**
- `pkg/test/replicated/stream/testdata/group.json`
- `pkg/test/replicated/stream/testdata/group_with_stages.json`
- `pkg/test/replicated/stream/testdata/streams/*.json`
- `pkg/test/replicated/stream/etcd.go`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p pkg/test/replicated/stream/testdata/{streams,index_rules,index_rule_bindings}
```

- [ ] **Step 2: Create group.json with replicas: 2**

Copy from `pkg/test/stream/testdata/group.json`

- [ ] **Step 3: Copy stream schema files**

```bash
cp pkg/test/stream/testdata/streams/*.json pkg/test/replicated/stream/testdata/streams/
```

- [ ] **Step 4: Copy index_rules and index_rule_bindings**

```bash
cp -r pkg/test/stream/testdata/index_rules pkg/test/replicated/stream/testdata/
cp -r pkg/test/stream/testdata/index_rule_bindings pkg/test/replicated/stream/testdata/
```

- [ ] **Step 5: Create etcd.go preload function**

Create `pkg/test/replicated/stream/etcd.go`

- [ ] **Step 6: Commit**

```bash
git add pkg/test/replicated/stream/
git commit -m "feat(replicated): add replicated stream schemas"
```

### Task 4: Create pkg/test/replicated/trace/ directory

**Files to create:**
- `pkg/test/replicated/trace/testdata/groups/*.json`
- `pkg/test/replicated/trace/etcd.go`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p pkg/test/replicated/trace/testdata/groups
```

- [ ] **Step 2: Copy and modify trace group files**

```bash
cp pkg/test/trace/testdata/groups/*.json pkg/test/replicated/trace/testdata/groups/
```

- [ ] **Step 3: Create etcd.go preload function**

Create `pkg/test/replicated/trace/etcd.go`

- [ ] **Step 4: Commit**

```bash
git add pkg/test/replicated/trace/
git commit -m "feat(replicated): add replicated trace schemas"
```

---

## Phase 3: Add New Replication Tests

### Task 5: Add measure normal mode replication test

**File:** `test/integration/replication/measure_normal_replication_test.go`

**Tests to add:**
1. Deduplication - query multiple times, verify consistent results
2. Accessibility - stop 1 node, verify queries still work
3. Recovery - restart node, verify handoff queue drains

**Pattern:** Follow existing `replication_test.go` structure

```go
var _ = g.Describe("Measure Normal Mode Replication", func() {
    // Test deduplication
    g.It("should return consistent results from replicas", func() { ... })

    // Test accessibility after node failure
    g.It("should survive single node failure", func() { ... })

    // Test recovery
    g.It("should recover data after node restart", func() { ... })
})
```

- [ ] **Step 1: Create measure_normal_replication_test.go**

Create with Ginkgo test structure following replication_test.go pattern

- [ ] **Step 2: Run test to verify it compiles and runs**

Run: `go test -v ./test/integration/replication/... -run "Measure.*Replication"`

- [ ] **Step 3: Commit**

```bash
git add test/integration/replication/measure_normal_replication_test.go
git commit -m "test(replication): add measure normal mode replication tests"
```

### Task 6: Add stream replication test

**File:** `test/integration/replication/stream_replication_test.go`

- [ ] **Step 1: Create stream_replication_test.go**

Create with similar structure to measure test

- [ ] **Step 2: Run test to verify it compiles and runs**

Run: `go test -v ./test/integration/replication/... -run "Stream.*Replication"`

- [ ] **Step 3: Commit**

```bash
git add test/integration/replication/stream_replication_test.go
git commit -m "test(replication): add stream replication tests"
```

### Task 7: Add trace replication test

**File:** `test/integration/replication/trace_replication_test.go`

- [ ] **Step 1: Create trace_replication_test.go**

Create with similar structure

- [ ] **Step 2: Run test to verify it compiles and runs**

Run: `go test -v ./test/integration/replication/... -run "Trace.*Replication"`

- [ ] **Step 3: Commit**

```bash
git add test/integration/replication/trace_replication_test.go
git commit -m "test(replication): add trace replication tests"
```

---

## Verification

After all tasks complete, run the full replication test suite:

```bash
go test -v ./test/integration/replication/... -run TestReplication
```

Expected: All replication tests pass (deduplication, accessibility, recovery for measure, stream, trace)

---

## Dependencies

- @superpowers:systematic-debugging - if tests fail
- @superpowers:verification-before-completion - before claiming success
