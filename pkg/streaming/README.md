# SIDX Streaming Pipeline Framework Design

## Overview
- Introduce a reusable streaming pipeline package that encapsulates concurrency, channel wiring, and lifecycle management for SIDX streaming queries.
- Provide strongly typed channels via Go generics so operators exchange domain objects without `interface{}` casts.
- Ensure the framework alone owns the creation and management of goroutines, while SIDX operators focus on pure data transformations.
- Extend the framework to additional query paths such as the trace query orchestrator in `banyand/trace/query.go`.

## Goals
- **Maintainability**: Replace ad-hoc goroutine orchestration in `banyand/internal/sidx` and domain-specific pipelines (like `banyand/trace/streaming_pipeline.go`) with a cohesive framework that is easy to reason about and extend. The trace query already validates this pattern; the framework extracts and generalizes those lessons.
- **Type Safety**: Enforce compile-time type checking for data moving between stages, eliminating runtime type assertions and `interface{}` casts. The trace pipeline's `traceBatch` and `scanBatch` demonstrate this approach.
- **Backpressure Awareness**: Allow the pipeline to absorb fluctuations and signal cancellation cleanly through channel-based flow control. Slow downstream consumers naturally throttle upstream producers without explicit coordination.
- **Observability**: Integrate hooks for tracing, metrics, and logging without polluting operator logic. The trace pipeline's hierarchical span creation (`sidx-stream` → `block-scan`) serves as the template for framework-level telemetry.
- **Code Reuse**: Enable SIDX, trace, measure, and stream queries to share common operators (heap merge, deduplication, batching) instead of reimplementing similar patterns across services.

## Package Layout
- `pkg/streaming/pipeline.go`: public entry points to compose and execute pipelines (to be implemented).
- `pkg/streaming/channel.go`: typed channel abstractions with backpressure metrics and closure notifications.
- `pkg/streaming/operator.go`: operator contracts (`Operator[In, Out]`, `Emitter[T]`), lifecycle states, and internal coordination helpers.
- `pkg/streaming/scheduler.go`: goroutine management, channel wiring, and shutdown coordination.
- `pkg/streaming/error.go`: error aggregation utilities for multi-source error channels.
- `pkg/streaming/observability.go`: telemetry context, span lifecycle, and metrics integration.
- `pkg/streaming/docs_test.go`: living examples to validate the design once implemented.
- `pkg/streaming/operators/`: built-in operators that can be reused:
  - `heap_merge.go`: generic heap-based merge for ordered streams (extracted from `sidxStreamRunner`).
  - `batch_accumulator.go`: batch formation with configurable flush policies.
  - `filter.go`: predicate-based filtering operator.
  - `fan_out.go`: parallel execution across multiple workers.
  - `deduplicator.go`: removes duplicates based on key extraction function.

## Core Concepts
- **Pipeline**: a runtime container that connects a sequence (or graph) of operators, manages goroutines, and relays context cancellation and errors.
- **Operator**: a generic processing unit parametrized by input/output types with a small lifecycle (`Init`, `Process`, `Close`).
- **TypedChannel[T]**: wraps native Go channels and exposes send/receive operations plus metadata (buffer size, statistics, closure notifications).
- **Scheduler**: internal component that launches operators as goroutines, links their channels, and supervises termination/failure scenarios.

## Pipeline Construction
- Expose builder-style APIs where SIDX provides:
  - Operator instances for each stage.
  - Wiring instructions (e.g., linear chain, branching, fan-in).
  - Execution knobs such as buffer sizes, bounded parallelism, and retry semantics.
- The builder validates type compatibility and returns a compiled pipeline ready for execution.
- Execution returns typed output readers and a unified error channel for terminal faults.

### Example: Simple Linear Pipeline
```go
// SIDX constructs a query pipeline
pipeline := streaming.NewPipeline(ctx, obs).
    Source(partitionLoader).                     // Produces: []PartDescriptor
    Then(NewPartScannerOp(), BufferSize(32)).    // Consumes: []PartDescriptor, Produces: []blockScanResult
    Then(NewHeapMergeOp(), BufferSize(64)).      // Consumes: []blockScanResult, Produces: []MergedBatch
    Then(NewBatchAssemblerOp()).                 // Consumes: []MergedBatch, Produces: []QueryResponse
    Build()

// Execute and consume results
results, errs := pipeline.Run()
for result := range results {
    // stream to client
}
if err := <-errs; err != nil {
    // handle pipeline failure
}
```
Type safety is enforced at compile time: connecting operators with incompatible input/output types fails with a type error.

### Example: Multi-Shard Fan-In with Deduplication (Trace Query Pattern)
```go
// Trace query compiles a streaming pipeline that merges SIDX shards
pipeline := streaming.NewPipeline(ctx, obs).
    Sources(sidxShardSources).                         // Multiple sources: []sidx.QueryResponse
    Then(NewHeapMergeOp(KeyExtractor, Ascending)).     // Fan-in merge by key: []KeyValuePair
    Then(NewDeduplicatorOp(TraceIDExtractor)).         // Remove duplicates: []TraceID
    Then(NewBatchAccumulatorOp(maxBatchSize)).         // Batch formation: []TraceBatch
    Then(NewBlockScannerOp(parts, quotaLimit)).        // Block scanning: []BlockCursor
    Build()

results, errs := pipeline.Run()
for cursor := range results {
    if cursor.Error != nil {
        return cursor.Error
    }
    // process block cursor
}
if err := <-errs; err != nil {
    log.Errorf("pipeline failed: %v", err)
}
```
This pattern mirrors the current trace query implementation in `banyand/trace/streaming_pipeline.go`, with the framework handling all goroutine coordination, error aggregation, and resource cleanup.

## Operator Contract
Operators transform data without managing concurrency. The framework guarantees:
- `Init` is called exactly once before processing begins.
- `Process` is invoked sequentially for each upstream item (or batch, if configured).
- `Close` fires when upstream completes, allowing state flush.

### Interface Definition
```go
type Operator[In, Out any] interface {
    // Init prepares the operator with context and typed emitter.
    // Returns error only for unrecoverable setup failures.
    Init(ctx context.Context, emit Emitter[Out]) error
    
    // Process handles one input item, emitting zero or more outputs.
    // Recoverable errors (e.g., transient read failures) should be retried internally.
    // Unrecoverable errors (e.g., schema mismatch) terminate the pipeline.
    Process(ctx context.Context, item In) error
    
    // Close flushes buffered state and releases resources.
    // Called with upstream error context (nil on clean completion).
    Close(upstreamErr error) error
}
```

### Execution Guarantees
- No concurrent `Process` calls on the same operator instance (single-threaded per operator).
- Operators never create goroutines or touch channels directly.
- Context cancellation interrupts `Process` and triggers graceful shutdown via `Close`.

### Emitter Interface
```go
type Emitter[T any] interface {
    // Emit sends a value downstream. Blocks on backpressure.
    // Returns error if context cancelled or downstream closed.
    Emit(ctx context.Context, value T) error
    
    // EmitBatch sends multiple values efficiently.
    EmitBatch(ctx context.Context, values []T) error
}
```

### Batching Semantics
Operators can process individual items or batches:
- **Item-by-item**: `Process(ctx, item In)` called once per upstream value.
- **Batch mode**: operator declares batch preference; framework accumulates items and invokes `Process(ctx, batch []In)`.
- SIDX examples: `PartScannerOperator` emits batches of blocks; `HeapMergeOperator` processes batches for efficiency.

## Channel Semantics
- **Generic Types**: `TypedChannel[T]` exposes `Send(T)`, `Recv() (T, bool)`, and `Close()` while keeping the underlying `chan T` private.
- **Backpressure**: upstream `Send` calls block when downstream buffers fill, allowing natural backpressure; timeouts can be configured per operator.
- **Sideband Signals**: control paths (stop, drain, flush) travel on separate lightweight channels so data flow remains uncluttered.

## Asynchronous Execution Handling
- The scheduler owns all goroutines, creating one per operator (or per shard when parallelism is requested).
- Context cancellation cascades through the scheduler, which closes channels and waits for shutdown with bounded deadlines.
- Operator code is invoked synchronously within the scheduler-managed goroutines, keeping `go func(){}` usage internal to the framework.

## Error Propagation

### Error Classification
- **Recoverable Errors**: handled internally by operators with retry logic:
  - Transient disk I/O failures (retry with backoff)
  - Temporary network timeouts when reading remote blocks
  - Resource contention (e.g., lock acquisition delays)
- **Unrecoverable Errors**: terminate the pipeline immediately:
  - Context cancellation (client disconnect, timeout)
  - Schema validation failures (corrupted metadata)
  - Out-of-memory conditions or resource exhaustion
  - Programming errors (nil pointer, index out of bounds)

### Propagation Behavior
When an operator returns an unrecoverable error, the scheduler:
1. Cancels the pipeline context, interrupting all operators.
2. Closes downstream channels with error annotations.
3. Drains upstream channels to prevent goroutine leaks.
4. Reports the root cause on a dedicated error stream.

### Partial Failure Handling
For fan-out scenarios (e.g., scanning 10 partitions in parallel):
- Single partition failure is treated as unrecoverable by default (fail-fast).
- Future: operators can opt into degraded mode, emitting partial results with warnings.
- EOF from upstream is benign; operators receive `Close(nil)` to finalize cleanly.

## Observability

### Instrumentation Hooks
The framework provides non-intrusive telemetry:
- **Tracing**: Each operator execution creates a child span of the query span, recording:
  - Processing duration per operator
  - Item counts (input/output)
  - Buffer utilization at channel boundaries
- **Metrics**: Automatic counters for:
  - Items processed/emitted per operator
  - Backpressure stalls (time blocked on full channels)
  - Error rates by operator type
- **Logging**: Structured context flows through the pipeline with operator IDs and stage names.

### Integration with BanyanDB Telemetry
```go
type ObservabilityContext struct {
    Span   *query.Span         // Parent trace span
    Logger *logger.Logger      // Structured logger
    Meter  *meter.Provider     // Metrics registry
}
```

Operators receive this context during `Init` but remain decoupled from telemetry implementation details. The framework handles:
- Span lifecycle (start, annotate, finish)
- Metric publishing and aggregation
- Log correlation across concurrent operators

The trace query pipeline demonstrates this in practice: `streamSIDXTraceBatches` creates a parent `sidx-stream` span that tracks batches emitted, trace IDs produced, and deduplication statistics, while `scanTraceIDsInline` creates child `block-scan` spans that record blocks scanned and bytes processed. This hierarchical tracing allows operators to remain focused on data transformation while the framework captures detailed execution metrics.

## Integration with `banyand/internal/sidx`
- SIDX constructs pipelines by:
  - Selecting domain-specific operators (e.g., part scanners, heap mergers, filter evaluators).
  - Configuring typed channels for `blockScanResult`, `QueryResponse`, and other domain entities.
  - Delegating execution to the framework, receiving a streaming results reader and error channel.
- Existing goroutine-based logic in `query.go` transitions to operator implementations, while the orchestration shifts into the new package.

## Prototype Usage Flow (SIDX)
- **1. Define Operators**
  - Implement `PartScannerOperator` that consumes partition descriptors and emits `blockScanResultBatch`.
  - Implement `HeapMergeOperator` that accepts scan batches, maintains a cursor heap, and streams ordered batches downstream.
  - Implement `BatchAssemblerOperator` that turns merged items into `QueryResponse` batches ready for the client.
- **SIDX Operator Checklist**
  - `PartitionSelectorOperator`: accepts `QueryRequest`, resolves relevant partitions and seeds the pipeline.
  - `PartScannerOperator`: reads part metadata, loads blocks, produces `blockScanResultBatch`.
  - `FilterOperator`: evaluates filter expressions against blocks or elements, emitting only matches.
  - `HeapMergeOperator`: merges sorted batches across partitions using the SIDX cursor heap logic.
  - `LimiterOperator`: enforces `MaxBatchSize` and other request-level limits before final assembly.
  - `BatchAssemblerOperator`: packages elements into `QueryResponse` chunks and propagates streaming stats.
  - `TelemetrySinkOperator`: optional stage that records metrics/tracing without altering payloads.
- **2. Configure Pipeline**
  - Create a pipeline builder with execution context, tracing span, and buffer hints derived from `QueryRequest`.
  - Register operators in order: partitions → scanner → heap merge → batch assembler.
  - Specify channel capacities: small buffer between scanner and merge (per partition), larger buffer before the client-facing stage.
- **3. Run & Consume**
  - Invoke `pipeline.Run()` supplying the initial input set (selected partitions, series IDs).
  - Receive streaming results through the returned typed reader while monitoring the error stream.
  - On cancellation (client disconnect or timeout), propagate context cancellation; the framework handles shutdown and channel closure.
- **4. Lifecycle Management**
  - When `Run` completes, the framework finalizes telemetry, aggregates operator stats, and returns them for logging.
  - Operators remain stateless between runs; reuse comes from rebuilding pipelines with fresh inputs.

## Trace Query Integration
- `banyand/trace/query.go` already implements a domain-specific streaming pipeline through helpers in `banyand/trace/streaming_pipeline.go`. This implementation serves as a **proof of concept** and validation of the design patterns that will become the generic `pkg/streaming` framework. Both the SIDX and trace services share the same building blocks (typed batches, heap-merging, error propagation) and demonstrate these patterns before their generalization.
- When a trace request specifies ordering or relies on SIDX indexes, the query path compiles a streaming pipeline that:
  1. Resolves series/entities and attaches table snapshots.
  2. Selects the appropriate trace batch source:
     - `staticTraceBatchSource` feeds batches when explicit trace IDs are provided.
     - `streamSIDXTraceBatches` launches `StreamingQuery` goroutines across SIDX shards, merges them via `sidxStreamRunner`, and yields deduplicated trace ID batches in key order. Deduplication is required because the same trace ID may appear in multiple SIDX shards (different time partitions or series), and the trace query must return each unique trace ID exactly once.
  3. Hands each batch to `startBlockScanStage`, which scans blocks inline and exposes a typed cursor channel to downstream consumers. The scan stage implements per-query memory quotas by checking available bytes before adding blocks to results, demonstrating the resource management pattern that will inform the framework's quota design.
- The trace pipeline keeps the same lifecycle surface as the generic framework:
  - A per-query context with a short timeout guards the run.
  - Batches carry sequence numbers, key maps, and propagated errors so the consumer can correlate scan results back to SIDX ordering guarantees. Sequence numbers ensure correct ordering when batches may be emitted from multiple concurrent SIDX shards.
  - Error fan-in is handled centrally (`forwardSIDXError`, `drainErrorEvents`) using dedicated error channels and `sync.WaitGroup` coordination. Each SIDX shard's error channel is monitored by a separate goroutine, with errors aggregated into a shared `sidxStreamError` channel. This pattern prevents goroutine leaks by ensuring all error forwarding completes before pipeline shutdown and provides proper span annotation with failure metadata.
- Observability hooks mirror the framework contract: `query.GetTracer` starts a `sidx-stream` span with child `block-scan` spans, tags include order index metadata, emitted batch counts, dedup stats, and eventual errors. This makes the trace pipeline a concrete proving ground for the reusable telemetry interface.
- As the `pkg/streaming` package lands concrete builders, the trace path already has clear operator boundaries that map 1:1 to future components:
  - **Source**: trace ID discovery (static IDs or SIDX runners).
  - **Transform**: heap merge and duplicate suppression.
  - **Sink**: block scanning and cursor streaming.
  This alignment ensures the trace service can adopt the shared pipeline with minimal refactoring once the generic interfaces stabilize.

### Validated Patterns from Trace Pipeline
The trace query implementation has validated several key patterns that will transfer directly to the generic framework:
- **Multi-shard heap merging**: `sidxStreamRunner` maintains a min/max heap across multiple SIDX query streams, proving the viability of heap-based merge operators for ordered results.
- **Typed batch channels**: `traceBatch` and `scanBatch` demonstrate strongly-typed data flow without `interface{}` casts, validating the generic channel design.
- **Inline scanning with backpressure**: `scanTraceIDsInline` streams results through channels while respecting memory quotas, showing how operators can provide natural backpressure.
- **Graceful error propagation**: Separate error channels with coordinated shutdown prevent goroutine leaks even when errors occur mid-stream.
- **Context-driven cancellation**: Pipeline stages respect context cancellation and propagate it cleanly through all goroutines.
- **Sequence numbering**: Batch sequence numbers maintain ordering guarantees across concurrent producers.
- **Resource quotas**: Dynamic quota checking in the scan stage demonstrates how operators can enforce resource limits without terminating prematurely when partial results are acceptable.

## Performance and Resource Management

### Memory Efficiency
- **Pooling**: Reuse buffers and domain objects (e.g., `blockScanResult`) across pipeline runs.
- **Bounded Channels**: Configure buffer sizes based on workload; default to small buffers (16-64 items) to limit memory footprint.
- **Zero-Copy**: Where possible, operators pass pointers to avoid allocation/copying overhead.

### Goroutine Management
- **Fixed Overhead**: One goroutine per operator instance plus scheduler goroutines (predictable resource usage).
- **Graceful Shutdown**: Bounded deadlines (e.g., 5s) for operator `Close` to prevent hung goroutines.
- **No Goroutine Leaks**: Framework ensures all goroutines terminate even on panic or cancellation.

### Backpressure Tuning
- **Flow Control**: Slow operators naturally throttle upstream via blocked `Emit` calls.
- **Timeout Policies**: Per-operator send/receive timeouts prevent indefinite blocking.
- **Circuit Breaking**: Optional: drop old items or shed load when buffers remain full beyond thresholds.

### Resource Quotas
The trace query pipeline demonstrates quota enforcement in practice:
- `scanTraceIDsInline` checks `AvailableBytes()` before adding each block to results.
- When quota is exceeded with partial results already emitted, the scan completes successfully with what was gathered.
- When quota is exceeded with no results, an error is returned to the caller.
- This graceful degradation pattern allows queries to return partial results under memory pressure.

Framework design considerations:
- Enforce per-query memory limits by tracking allocations across operators.
- Support both hard limits (terminate immediately) and soft limits (allow current batch to complete).
- Expose quota headroom metrics so operators can make informed decisions about prefetching and batching.
- Terminate pipelines exceeding hard quotas with resource exhaustion errors that include diagnostic context.

## Extensibility Considerations
- Support fan-out/fan-in patterns for merging index partitions.
- Allow pluggable buffering strategies (fixed vs. dynamic) via operator configuration.
- Provide testing harnesses that simulate channel interactions deterministically for unit and integration tests.
- Enable operator chaining and composition (e.g., wrapping filter + transform into a composite operator).

## Common Pitfalls and Anti-Patterns

### Lessons from Trace Pipeline Implementation
The trace query streaming pipeline revealed several pitfalls that the framework must help developers avoid:

#### 1. Goroutine Leak from Unconsumed Channels
**Problem**: If a downstream consumer stops reading early (error or cancellation), upstream goroutines writing to channels can block indefinitely.
**Solution**: The framework must ensure all goroutines terminate when:
- Context is cancelled
- Downstream closes its input channel
- An unrecoverable error occurs
The trace pipeline's `Release()` method drains all channels explicitly; the framework should handle this automatically.

#### 2. Shared Reference Bug in Pooled Objects
**Problem**: Pooled iterators that hold references to mutable slices can cause data corruption when the same iterator is reused across multiple invocations (see memory ID 10066530 about `tstIter.reset()` bug).
**Solution**: Framework should encourage immutable or copy-on-write patterns for pooled objects, or provide clear ownership semantics.

#### 3. Error Channel Management Complexity
**Problem**: Managing multiple error channels (one per SIDX shard) requires careful coordination with `sync.WaitGroup` to prevent premature closure.
**Solution**: Framework provides error aggregation utilities that handle the coordination pattern automatically, ensuring all error sources are drained before closing the aggregated error channel.

#### 4. Context Cancellation Propagation
**Problem**: Must check both the pipeline context (for user cancellation) and operation-specific contexts (for internal timeouts).
**Solution**: Framework maintains a single cancellation context that cascades through all operators, with clear semantics for graceful vs. immediate shutdown.

#### 5. Span Lifecycle and Error Attribution
**Problem**: Spans must be finished even when errors occur, and errors should be attributed to the correct span in the hierarchy.
**Solution**: Framework wraps operator execution to guarantee span completion and captures errors for automatic annotation.

#### 6. Batch Size vs. Buffer Size Confusion
**Problem**: Mixing up logical batch sizes (items emitted together) with channel buffer sizes (backpressure threshold) leads to unexpected blocking or memory usage.
**Solution**: Framework keeps these concepts distinct: batch sizes are operator concerns, buffer sizes are wiring concerns configured separately.

#### 7. Quota Enforcement Timing
**Problem**: Checking quotas after allocating resources leads to temporary quota violations; checking before can cause underutilization.
**Solution**: Framework exposes quota headroom before each operator `Process` call, allowing informed decisions about whether to accept the next item.


## Testing Strategy

### Unit Testing Operators
Operators are pure functions, making them trivial to test:
```go
func TestFilterOperator_Process(t *testing.T) {
    op := NewFilterOperator(expr)
    emit := &mockEmitter[Result]{}
    op.Init(ctx, emit)
    
    err := op.Process(ctx, inputItem)
    assert.NoError(t, err)
    assert.Equal(t, expectedOutput, emit.emittedItems)
}
```

### Integration Testing Pipelines
The framework provides test harnesses:
- **MockSource**: Inject controlled input sequences (including error cases).
- **CollectorSink**: Capture pipeline outputs for assertion.
- **Deterministic Scheduling**: Disable concurrent execution to test logic without race conditions.

### Chaos Testing
Validate robustness under adverse conditions:
- Random context cancellation during execution.
- Simulated slow operators (inject delays in `Process`).
- Channel buffer exhaustion and backpressure scenarios.
- Operator panics and recovery behavior.

### Benchmarking
- Measure throughput (items/sec) for common SIDX workloads.
- Profile memory allocations and goroutine counts.
- Compare against current ad-hoc implementation to validate performance parity.

## Next Steps

### Phase 1: Extract Generic Abstractions (Current)
- ✅ **Proof of Concept Complete**: The trace query pipeline in `banyand/trace/streaming_pipeline.go` validates the design with a production workload.
- Detail public API sketches and finalize naming conventions based on trace pipeline learnings.
- Extract common patterns from `streaming_pipeline.go` into reusable components in `pkg/streaming/`:
  - Generic heap merge operator parametrized by key extraction and comparison functions.
  - Typed channel wrappers with backpressure metrics and closure notifications.
  - Error aggregation utilities that coordinate multiple error channels.
  - Observability context and span lifecycle helpers.

### Phase 2: Implement Core Framework
- Implement core framework components:
  - `Pipeline` builder with compile-time type checking between operators.
  - `Scheduler` that manages operator goroutines and channel wiring.
  - `TypedChannel[T]` abstractions with buffering strategies.
  - `Operator[In, Out]` interface and execution guarantees.
- Write integration tests covering:
  - Error propagation across multi-stage pipelines.
  - Context cancellation and graceful shutdown.
  - Backpressure scenarios with slow downstream consumers.
  - Goroutine leak prevention under failure conditions.

### Phase 3: Migrate Existing Code
- Refactor trace query to use generic framework:
  - Replace `sidxStreamRunner` with framework's heap merge operator.
  - Migrate `traceBatch` channels to `TypedChannel[TraceBatch]`.
  - Adopt framework's error aggregation instead of custom `forwardSIDXError`.
  - Validate performance parity with current implementation.
- Migrate one SIDX query path (e.g., `StreamingQuery` in `banyand/internal/sidx/query.go`) as a second proof of concept.
- Continue hardening shared operators so both SIDX and trace query benefit from consistent behavior.

### Phase 4: Expand and Optimize
- Extend framework with additional operator types:
  - Fan-out operators for parallel partition scanning.
  - Batch accumulation operators with configurable flush policies.
  - Filter and projection operators for common query patterns.
- Benchmark and tune:
  - Profile memory allocations and optimize hot paths.
  - Measure goroutine overhead and validate bounded resource usage.
  - Compare against current ad-hoc implementations to ensure no regression.
- Document migration guide for converting existing goroutine-based code to the framework.

### Phase 5: Production Hardening
- Adopt framework across all SIDX query operators.
- Add chaos testing: inject random failures, delays, and cancellations.
- Establish SLOs for pipeline performance and resource usage.
- Create debugging tools for inspecting pipeline state and diagnosing stalls.
