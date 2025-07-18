

# Plan for Creating a New "trace" Package under `/banyand`

Based on the analysis of the existing `stream` and `measure` packages, here's a comprehensive plan to create a new `trace` package following the established patterns:

## 1. Package Structure

```
banyand/trace/
├── svc_liaison.go          # Liaison service implementation
├── svc_standalone.go       # Standalone service implementation  
├── metadata.go             # Schema repository and suppliers
├── trace.go                # Core trace interface and implementation
└── [other supporting files]
```

## 2. Core Interfaces and Types

### 2.1 Service Interface
```go
// Service allows inspecting the trace data.
type Service interface {
    run.PreRunner
    run.Config
    run.Service
    Query
}

// Query allows retrieving traces.
type Query interface {
    LoadGroup(name string) (schema.Group, bool)
    Trace(metadata *commonv1.Metadata) (Trace, error)
    GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange
}

// Trace allows inspecting trace details.
type Trace interface {
    GetSchema() *databasev1.Trace
    GetIndexRules() []*databasev1.IndexRule
    Query(ctx context.Context, opts model.TraceQueryOptions) (model.TraceQueryResult, error)
}
```

### 2.2 Configuration Options
```go
type option struct {
    mergePolicy              *mergePolicy
    protector                protector.Memory
    tire2Client              queue.Client
    seriesCacheMaxSize       run.Bytes
    flushTimeout             time.Duration
    elementIndexFlushTimeout time.Duration
}
```

## 3. Service Implementations

### 3.1 Liaison Service (`svc_liaison.go`)

**Key Features:**
- Implements `Service` interface
- Role: `databasev1.Role_ROLE_LIAISON`
- Uses `queueSupplier` for resource management
- No pipeline listeners (as requested)
- Handles distributed trace operations

**Structure:**
```go
type liaison struct {
    pm                  protector.Memory
    metadata            metadata.Repo
    pipeline            queue.Server
    omr                 observability.MetricsRegistry
    lfs                 fs.FileSystem
    dataNodeSelector    node.Selector
    l                   *logger.Logger
    schemaRepo          schemaRepo
    dataPath            string
    root                string
    option              option
    maxDiskUsagePercent int
}
```

**Key Methods:**
- `Trace(metadata *commonv1.Metadata) (Trace, error)`
- `LoadGroup(name string) (resourceSchema.Group, bool)`
- `GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange`
- `FlagSet() *run.FlagSet`
- `Validate() error`
- `Name() string` (returns "trace")
- `Role() databasev1.Role` (returns `ROLE_LIAISON`)
- `PreRun(ctx context.Context) error`
- `Serve() run.StopNotify`
- `GracefulStop()`

### 3.2 Standalone Service (`svc_standalone.go`)

**Key Features:**
- Implements `Service` interface
- Role: `databasev1.Role_ROLE_DATA`
- Uses `supplier` for resource management
- No pipeline listeners (as requested)
- Handles local trace operations

**Structure:**
```go
type standalone struct {
    pm                  protector.Memory
    pipeline            queue.Server
    localPipeline       queue.Queue
    omr                 observability.MetricsRegistry
    lfs                 fs.FileSystem
    metadata            metadata.Repo
    l                   *logger.Logger
    schemaRepo          schemaRepo
    snapshotDir         string
    root                string
    dataPath            string
    option              option
    maxDiskUsagePercent int
    maxFileSnapshotNum  int
}
```

**Key Methods:**
- `Trace(metadata *commonv1.Metadata) (Trace, error)`
- `LoadGroup(name string) (resourceSchema.Group, bool)`
- `GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange`
- `FlagSet() *run.FlagSet`
- `Validate() error`
- `Name() string` (returns "trace")
- `Role() databasev1.Role` (returns `ROLE_DATA`)
- `PreRun(ctx context.Context) error`
- `Serve() run.StopNotify`
- `GracefulStop()`

## 4. Schema Repository and Suppliers (`metadata.go`)

### 4.1 Schema Repository
```go
type schemaRepo struct {
    resourceSchema.Repository
    l        *logger.Logger
    metadata metadata.Repo
    path     string
}
```

### 4.2 Supplier (for Standalone)
```go
type supplier struct {
    metadata   metadata.Repo
    omr        observability.MetricsRegistry
    pm         protector.Memory
    l          *logger.Logger
    schemaRepo *schemaRepo
    nodeLabels map[string]string
    path       string
    option     option
}
```

**Required Methods:**
- `OpenResource(spec resourceSchema.Resource) (resourceSchema.IndexListener, error)`
- `ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error)`
- **Note: `OpenDB` method should NOT be implemented (as requested)**

### 4.3 Queue Supplier (for Liaison)
```go
type queueSupplier struct {
    metadata               metadata.Repo
    omr                    observability.MetricsRegistry
    pm                     protector.Memory
    traceDataNodeRegistry  grpc.NodeRegistry
    l                      *logger.Logger
    schemaRepo             *schemaRepo
    path                   string
    option                 option
}
```

**Required Methods:**
- `OpenResource(spec resourceSchema.Resource) (resourceSchema.IndexListener, error)`
- `ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error)`
- **Note: `OpenDB` method should NOT be implemented (as requested)**

## 5. Core Trace Implementation (`trace.go`)

```go
type trace struct {
    pm         protector.Memory
    indexSchema atomic.Value
    tsdb        atomic.Value
    l           *logger.Logger
    schema      *databasev1.Trace
    schemaRepo  *schemaRepo
    name        string
    group       string
}

type traceSpec struct {
    schema *databasev1.Trace
}
```

## 6. Configuration and Flags

### 6.1 Liaison Service Flags
- `--trace-root-path` (default: "/tmp")
- `--trace-data-path` (optional)
- `--trace-flush-timeout` (default: 1 second)
- `--trace-max-disk-usage-percent` (default: 95)

### 6.2 Standalone Service Flags
- `--trace-root-path` (default: "/tmp")
- `--trace-data-path` (optional)
- `--trace-flush-timeout` (default: 1 second)
- `--trace-max-fan-out-size` (merge policy)
- `--trace-series-cache-max-size` (default: 32MB)
- `--trace-max-disk-usage-percent` (default: 95)
- `--trace-max-file-snapshot-num` (default: 2)

## 7. Error Handling

Define common errors:
```go
var (
    errEmptyRootPath = errors.New("root path is empty")
    ErrTraceNotExist = errors.New("trace doesn't exist")
)
```

## 8. Integration Points

### 8.1 Metadata Integration
- Use `metadata.TraceRegistry()` for trace schema operations
- Implement proper schema watching and updates

### 8.2 Storage Integration
- Use `storage.TSDB` for time-series data storage
- Implement proper segment management and TTL handling

### 8.3 Metrics Integration
- Implement trace-specific metrics
- Use observability framework for monitoring

## 9. Implementation Order

1. **Core Interfaces** (`trace.go`)
2. **Schema Repository** (`metadata.go`)
3. **Standalone Service** (`svc_standalone.go`)
4. **Liaison Service** (`svc_liaison.go`)
5. **Supporting Files** (metrics, query, write handlers, etc.)

## 10. Key Differences from Stream/Measure

1. **No Pipeline Listeners**: Both services should not implement any pipeline listeners as requested
2. **No OpenDB Implementation**: Both suppliers should implement all methods except `OpenDB`
3. **Trace-Specific Schema**: Use `databasev1.Trace` instead of `databasev1.Stream` or `databasev1.Measure`
4. **Trace-Specific Queries**: Implement trace-specific query patterns and result types

This plan follows the established patterns from the stream and measure packages while adapting them specifically for trace data handling, ensuring consistency with the existing codebase architecture.