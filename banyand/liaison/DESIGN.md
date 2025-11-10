# Design for Dynamic gRPC Heap Size Control and Load Shedding

This document outlines the design for implementing dynamic gRPC heap size control and load shedding in the BanyanDB liaison component to prevent Out-Of-Memory (OOM) errors under high-throughput write traffic.

## 1. Motivation

As identified in performance testing and detailed in issue [#13485](https://github.com/apache/skywalking/issues/13485), the liaison gRPC server is susceptible to OOM errors when client ingestion rates exceed the server's processing capacity. This leads to unbounded growth of internal gRPC buffers, consuming all available heap memory and causing server crashes.

The current memory protector (`banyand/protector`) provides per-operation quota management but does not prevent accepting new connections when the system is already under memory pressure. This can lead to:

- Queueing of requests that cannot be processed due to memory constraints
- Increased latency and resource consumption
- Potential OOM crashes when multiple concurrent requests arrive during high memory usage

To enhance server stability and resilience, we need to introduce two key mechanisms:

1. **Load Shedding**: Proactively reject new connections when the system is under high memory pressure to prevent resource exhaustion.
2. **Dynamic Buffer Sizing**: Intelligently configure gRPC's network buffers to apply backpressure before the heap is exhausted.

## 2. Proposed Solution

The solution is divided into two parts: implementing a load shedding mechanism using a gRPC interceptor that leverages the existing `protector` service, and dynamically configuring gRPC buffer sizes based on available system memory.

### 2.1. Load Shedding via Protector State

We will introduce a gRPC Stream Server Interceptor that checks the system's memory state before accepting a new stream.

#### 2.1.1. Service Interface Definition

First, we need to define a `Service` interface that provides discrete memory states for load shedding decisions:

```go
// Service provides memory state information for load shedding decisions
type Service interface {
    // State returns the current memory pressure state
    State() State
}

// State represents memory pressure levels for load shedding
type State int

const (
    // StateLow indicates normal memory usage, accept all requests
    StateLow State = iota
    // StateHigh indicates high memory pressure, reject new requests
    StateHigh
)
```

The `State()` method will translate continuous memory metrics into discrete states:
- `StateLow`: When `AvailableBytes() > lowWatermark` (default: 20% of limit)
- `StateHigh`: When `AvailableBytes() <= lowWatermark`

#### 2.1.2. Memory Protector Extension

Extend the existing `protector.Memory` interface to implement the `Service` interface:

```go
// memory implements both Memory and Service interfaces
func (m *memory) State() State {
    available := m.AvailableBytes()
    if available <= 0 {
        return StateHigh
    }

    // Use 20% of memory limit as the threshold for high pressure
    // This provides a buffer zone to prevent rapid state oscillations
    threshold := int64(m.GetLimit() / 5)
    if available <= threshold {
        return StateHigh
    }

    return StateLow
}
```

#### 2.1.3. Dependency Injection

The `protector.Service` will be injected into the `liaison/grpc` server. This will be done by modifying the `NewServer` function signature and updating callers in `banyand/cmd/server/main.go`.

#### 2.1.4. Interceptor Logic

A new stream interceptor, `protectorLoadSheddingInterceptor`, will be implemented. For each incoming stream, this interceptor will:

1. Query the system's memory state by calling `protector.State()`.
2. If the state is `protector.StateHigh`, it will log a warning with memory metrics and reject the stream with a `codes.ResourceExhausted` gRPC status. This provides immediate backpressure to the client.
3. If the state is `protector.StateLow`, the stream will be processed normally.

The interceptor will be added to the chain of stream server interceptors in the `Serve()` method of `banyand/liaison/grpc/server.go`.

```go
// protectorLoadSheddingInterceptor rejects streams when memory pressure is high
func (s *server) protectorLoadSheddingInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
    if s.protector.State() == protector.StateHigh {
        available := s.protector.AvailableBytes()
        limit := s.protector.GetLimit()
        s.log.Warn().
            Int64("available_bytes", available).
            Uint64("limit", limit).
            Str("service", info.FullMethod).
            Msg("rejecting new stream due to high memory pressure")
        return status.Errorf(codes.ResourceExhausted, "server is under memory pressure, please retry later")
    }
    return handler(srv, ss)
}
```

### 2.2. Dynamic gRPC Buffer Sizing

We will dynamically calculate gRPC's HTTP/2 flow control window sizes at server startup based on available system memory.

#### 2.2.1. Configuration

A new configuration flag will be introduced in the `liaison-grpc` FlagSet:

* `--grpc-buffer-memory-ratio`: A float64 determining the fraction of available system memory to be allocated for gRPC's connection-level buffers. Valid range: `(0.0, 1.0]`. The default value will be `0.10` (10%).

Configuration validation will ensure:
- Value must be greater than 0.0 and less than or equal to 1.0
- Warn if the ratio exceeds 0.5 (50%) as this may starve other system components

#### 2.2.2. Dynamic Calculation Logic

In the `Serve()` method of the gRPC server, before initializing the gRPC server, we will:

1. Check if static buffer size flags are explicitly set via environment variables or command-line flags. If so, they take precedence and dynamic calculation is skipped.
2. Query the total available system memory via `protector.GetLimit()`.
3. Calculate the buffer sizes using the following heuristic:
   * `totalBufferSize = min(availableMemory * grpc-buffer-memory-ratio, maxReasonableBufferSize)`
   * `InitialConnWindowSize = totalBufferSize * 2 / 3` (connection-level flow control)
   * `InitialWindowSize = totalBufferSize * 1 / 3` (stream-level flow control)

Where `maxReasonableBufferSize` prevents allocating unreasonably large buffers (default: 1GB).

#### 2.2.3. Applying Server Options

The calculated `InitialWindowSize` and `InitialConnWindowSize` values will be passed as server options to `grpc.NewServer()`. If calculation fails, the server will fall back to gRPC defaults with a warning log.

## 3. Implementation Details

### 3.1. Files to be Modified

#### `banyand/protector/protector.go`
- Add the `Service` interface and `State` type definitions
- Implement the `State()` method on the `memory` struct
- Add configuration for the low watermark threshold (default: 20%)

#### `banyand/liaison/grpc/server.go`
- Add a `protector.Service` field to the `server` struct
- Update `NewServer` to accept `protector.Service` and store it
- Implement the `protectorLoadSheddingInterceptor` method
- Add the new flag `--grpc-buffer-memory-ratio` with validation
- In the `Serve` method:
  - Add the `protectorLoadSheddingInterceptor` to the stream interceptor chain
  - Implement the logic for dynamic buffer size calculation
  - Pass the calculated buffer sizes as `grpc.ServerOption`s when creating the new gRPC server

#### `banyand/cmd/server/main.go`
- The `protector` service needs to be passed to the gRPC liaison server during its creation
- Update the server initialization sequence to ensure the protector is available

### 3.2. Error Handling and Edge Cases

- **Protector Unavailable**: If protector is nil, skip load shedding (fail open)
- **Memory Query Failures**: Log warnings but don't fail server startup
- **Buffer Calculation Errors**: Fall back to gRPC defaults with warning
- **Invalid Configuration**: Validate ratios at startup and reject invalid values
- **Memory State Transitions**: Include hysteresis to prevent rapid state oscillations

### 3.3. Metrics and Monitoring

Add the following metrics to the liaison gRPC scope:
- `memory_load_shedding_rejections_total`: Counter of rejected streams due to memory pressure
- `grpc_buffer_size_bytes`: Gauge of calculated buffer sizes
- `memory_state`: Gauge indicating current memory state (0=Low, 1=High)

### 3.4. Testing Strategy

- **Unit Tests**: Test state transitions with mocked memory values
- **Integration Tests**: Verify interceptor behavior under memory pressure
- **Load Tests**: Validate load shedding prevents OOM under high concurrency
- **Configuration Tests**: Ensure proper validation and fallback behavior

### 3.5. Backwards Compatibility

- Default buffer ratio (10%) provides conservative behavior
- Load shedding is additive - existing quota management remains intact
- Static buffer size flags maintain precedence over dynamic calculation
- Graceful degradation when protector is unavailable

### 3.6. Performance Considerations

- State checks are lightweight (memory reads only)
- Interceptor overhead is minimal (< 1μs per request)
- Buffer calculation performed once at startup
- Memory state caching to avoid frequent protector queries

### 3.7. Recovery Mechanism

Once in `StateHigh`, the system will remain in load shedding mode until memory pressure drops below the threshold plus a hysteresis buffer (additional 5% of limit). This prevents rapid oscillations between states when memory usage is near the threshold.

## 4. Implementation Workflow

This section provides a step-by-step implementation guide with specific test cases for each step. Each step should be implemented and tested incrementally to ensure correctness and prevent regressions.

### 4.1. Step 1: Define Service Interface and State Types

**Files to modify**: `banyand/protector/protector.go`

**Implementation**:
- Add the `Service` interface definition
- Add the `State` type and constants
- Ensure the interface is properly documented

**Test Cases**:
```go
// TestServiceInterfaceCompliance verifies that memory implements Service interface
func TestServiceInterfaceCompliance(t *testing.T) {
    var _ Service = (*memory)(nil)
}

// TestStateConstants verifies state constants are properly defined
func TestStateConstants(t *testing.T) {
    assert.Equal(t, State(0), StateLow)
    assert.Equal(t, State(1), StateHigh)
}
```

### 4.2. Step 2: Implement State() Method on Memory Struct

**Files to modify**: `banyand/protector/protector.go`

**Implementation**:
- Add the `State()` method to the `memory` struct
- Implement the threshold logic (20% of limit by default)
- Handle edge cases (available <= 0, limit == 0)

**Test Cases**:
```go
// TestMemoryStateLow verifies StateLow when memory is plentiful
func TestMemoryStateLow(t *testing.T) {
    m := &memory{limit: atomic.Uint64{}, usage: 0}
    m.limit.Store(1000) // 1KB limit

    // 80% available (800 bytes) > 20% threshold (200 bytes)
    m.usage = 200
    assert.Equal(t, StateLow, m.State())
}

// TestMemoryStateHigh verifies StateHigh when memory is scarce
func TestMemoryStateHigh(t *testing.T) {
    m := &memory{limit: atomic.Uint64{}, usage: 0}
    m.limit.Store(1000) // 1KB limit

    // 15% available (150 bytes) <= 20% threshold (200 bytes)
    m.usage = 850
    assert.Equal(t, StateHigh, m.State())
}

// TestMemoryStateHighEdgeCase verifies StateHigh when no memory available
func TestMemoryStateHighEdgeCase(t *testing.T) {
    m := &memory{limit: atomic.Uint64{}, usage: 0}
    m.limit.Store(1000)
    m.usage = 1000 // Exactly at limit
    assert.Equal(t, StateHigh, m.State())
}

// TestMemoryStateNoLimit verifies behavior when no limit is set
func TestMemoryStateNoLimit(t *testing.T) {
    m := &memory{limit: atomic.Uint64{}, usage: 0}
    // No limit set (0)
    assert.Equal(t, StateLow, m.State()) // Fail open
}
```

### 4.3. Step 3: Add Buffer Memory Ratio Configuration

**Files to modify**: `banyand/liaison/grpc/server.go`

**Implementation**:
- Add `grpcBufferMemoryRatio` field to server struct
- Add flag definition in `FlagSet()` method
- Add validation in `Validate()` method

**Test Cases**:
```go
// TestGrpcBufferMemoryRatioFlagDefault verifies default value
func TestGrpcBufferMemoryRatioFlagDefault(t *testing.T) {
    s := &server{}
    fs := s.FlagSet()
    flag := fs.Lookup("grpc-buffer-memory-ratio")
    assert.NotNil(t, flag)
    assert.Equal(t, "0.1", flag.DefValue)
}

// TestGrpcBufferMemoryRatioValidation verifies validation logic
func TestGrpcBufferMemoryRatioValidation(t *testing.T) {
    tests := []struct {
        name      string
        ratio     float64
        expectErr bool
    }{
        {"valid_ratio_0_1", 0.1, false},
        {"valid_ratio_0_5", 0.5, false},
        {"valid_ratio_1_0", 1.0, false},
        {"invalid_ratio_zero", 0.0, true},
        {"invalid_ratio_negative", -0.1, true},
        {"invalid_ratio_too_high", 1.1, true},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            s := &server{grpcBufferMemoryRatio: tt.ratio}
            err := s.Validate()
            if tt.expectErr {
                assert.Error(t, err)
            } else {
                assert.NoError(t, err)
            }
        })
    }
}
```

### 4.4. Step 4: Update Server Struct and NewServer Function

**Files to modify**: `banyand/liaison/grpc/server.go`

**Implementation**:
- Add `protector.Service` field to server struct
- Update `NewServer` function signature to accept protector.Service
- Store protector in server struct

**Test Cases**:
```go
// TestNewServerWithProtector verifies protector injection
func TestNewServerWithProtector(t *testing.T) {
    // Create a mock protector
    protector := &mockProtector{}

    // Create server with protector
    server := NewServer(context.Background(), nil, nil, nil, nil, NodeRegistries{}, nil, protector)

    // Verify protector is stored
    assert.NotNil(t, server.protector)
    assert.Equal(t, protector, server.protector)
}

// TestNewServerWithoutProtector verifies nil protector handling
func TestNewServerWithoutProtector(t *testing.T) {
    // Server creation should not fail with nil protector (fail open)
    server := NewServer(context.Background(), nil, nil, nil, nil, NodeRegistries{}, nil, nil)
    assert.Nil(t, server.protector)
}

// mockProtector implements Service interface for testing
type mockProtector struct{}

func (m *mockProtector) State() State { return StateLow }
```

### 4.5. Step 5: Implement Load Shedding Interceptor

**Files to modify**: `banyand/liaison/grpc/server.go`

**Implementation**:
- Add `protectorLoadSheddingInterceptor` method
- Implement state checking and rejection logic
- Add proper logging with memory metrics

**Test Cases**:
```go
// TestProtectorLoadSheddingInterceptorLowState verifies normal operation in low state
func TestProtectorLoadSheddingInterceptorLowState(t *testing.T) {
    protector := &mockProtector{state: StateLow}
    server := &server{protector: protector}

    called := false
    handler := func(srv interface{}, stream grpc.ServerStream) error {
        called = true
        return nil
    }

    err := server.protectorLoadSheddingInterceptor(nil, nil, &grpc.StreamServerInfo{}, grpc.StreamHandler(handler))

    assert.NoError(t, err)
    assert.True(t, called)
}

// TestProtectorLoadSheddingInterceptorHighState verifies rejection in high state
func TestProtectorLoadSheddingInterceptorHighState(t *testing.T) {
    protector := &mockProtector{state: StateHigh}
    server := &server{protector: protector}

    handler := func(srv interface{}, stream grpc.ServerStream) error {
        t.Fatal("handler should not be called")
        return nil
    }

    err := server.protectorLoadSheddingInterceptor(nil, nil, &grpc.StreamServerInfo{FullMethod: "test"}, grpc.StreamHandler(handler))

    assert.Error(t, err)
    st, ok := status.FromError(err)
    assert.True(t, ok)
    assert.Equal(t, codes.ResourceExhausted, st.Code())
}

// TestProtectorLoadSheddingInterceptorNilProtector verifies fail-open behavior
func TestProtectorLoadSheddingInterceptorNilProtector(t *testing.T) {
    server := &server{protector: nil}

    called := false
    handler := func(srv interface{}, stream grpc.ServerStream) error {
        called = true
        return nil
    }

    err := server.protectorLoadSheddingInterceptor(nil, nil, &grpc.StreamServerInfo{}, grpc.StreamHandler(handler))

    assert.NoError(t, err)
    assert.True(t, called)
}

// Enhanced mock protector for testing
type mockProtector struct {
    state State
}

func (m *mockProtector) State() State { return m.state }
```

### 4.6. Step 6: Implement Dynamic Buffer Sizing Logic

**Files to modify**: `banyand/liaison/grpc/server.go`

**Implementation**:
- Add buffer calculation logic in `Serve()` method
- Check for static buffer flags precedence
- Implement fallback to gRPC defaults on error
- Add `maxReasonableBufferSize` constant

**Test Cases**:
```go
// TestCalculateGrpcBufferSizes verifies buffer size calculations
func TestCalculateGrpcBufferSizes(t *testing.T) {
    tests := []struct {
        name           string
        memoryLimit    uint64
        ratio          float64
        expectedConn   int32
        expectedStream int32
    }{
        {"normal_case", 1000, 0.1, 66667, 33333}, // 100MB * 0.1 = 10MB, conn=6.67MB, stream=3.33MB
        {"max_reasonable", 10000000000, 0.5, 333333333, 166666666}, // Capped at 1GB total
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            protector := &mockProtectorWithLimit{limit: tt.memoryLimit}
            server := &server{protector: protector, grpcBufferMemoryRatio: tt.ratio}

            connSize, streamSize := server.calculateGrpcBufferSizes()

            // Allow some tolerance for integer division
            assert.InDelta(t, tt.expectedConn, connSize, 1000)
            assert.InDelta(t, tt.expectedStream, streamSize, 1000)
        })
    }
}

// TestCalculateGrpcBufferSizesFallback verifies fallback when protector unavailable
func TestCalculateGrpcBufferSizesFallback(t *testing.T) {
    server := &server{protector: nil, grpcBufferMemoryRatio: 0.1}

    connSize, streamSize := server.calculateGrpcBufferSizes()

    // Should return default values (0) when protector is nil
    assert.Equal(t, int32(0), connSize)
    assert.Equal(t, int32(0), streamSize)
}

// mockProtectorWithLimit extends mock protector for buffer calculations
type mockProtectorWithLimit struct {
    limit uint64
}

func (m *mockProtectorWithLimit) State() State { return StateLow }
func (m *mockProtectorWithLimit) GetLimit() uint64 { return m.limit }
```

### 4.7. Step 7: Add Interceptor to Server Chain

**Files to modify**: `banyand/liaison/grpc/server.go`

**Implementation**:
- Add `protectorLoadSheddingInterceptor` to stream interceptor chain in `Serve()` method
- Ensure proper ordering (load shedding should be early in the chain)

**Test Cases**:
```go
// TestServeInterceptorChain verifies interceptor is added to chain
func TestServeInterceptorChain(t *testing.T) {
    // This is an integration test that verifies the interceptor chain
    // We need to capture the server options passed to grpc.NewServer

    // Note: This test may require mocking grpc.NewServer or using a test server
    // The exact implementation depends on how we structure the Serve method
    t.Skip("Integration test - requires server mocking framework")
}
```

### 4.8. Step 8: Update main.go to Pass Protector

**Files to modify**: `banyand/cmd/server/main.go`

**Implementation**:
- Locate protector service creation/initialization
- Pass protector to gRPC liaison server constructor
- Ensure proper dependency ordering

**Test Cases**:
```go
// TestMainServerInitialization verifies protector is passed to gRPC server
func TestMainServerInitialization(t *testing.T) {
    // This test verifies that the main function properly initializes
    // the gRPC server with a protector service

    // Mock the protector creation and verify it's passed to NewServer
    t.Skip("Integration test - requires main function refactoring for testability")
}
```

### 4.9. Step 9: Add Metrics

**Files to modify**: `banyand/liaison/grpc/server.go`, `banyand/liaison/grpc/metrics.go`

**Implementation**:
- Add new metrics to liaison gRPC scope
- Update metrics initialization
- Add metric updates in interceptor and buffer calculation

**Test Cases**:
```go
// TestLoadSheddingMetrics verifies rejection metrics are updated
func TestLoadSheddingMetrics(t *testing.T) {
    metrics := newTestMetrics()
    protector := &mockProtector{state: StateHigh}
    server := &server{protector: protector, metrics: metrics}

    initialCount := metrics.loadSheddingRejections.Count()

    handler := func(srv interface{}, stream grpc.ServerStream) error {
        return nil // Won't be called
    }

    _ = server.protectorLoadSheddingInterceptor(nil, nil, &grpc.StreamServerInfo{}, grpc.StreamHandler(handler))

    assert.Equal(t, initialCount+1, metrics.loadSheddingRejections.Count())
}

// TestBufferSizeMetrics verifies buffer size metrics are set
func TestBufferSizeMetrics(t *testing.T) {
    metrics := newTestMetrics()
    server := &server{metrics: metrics}

    server.updateBufferSizeMetrics(1000000, 500000)

    // Verify metrics are updated (exact verification depends on metrics implementation)
    t.Skip("Depends on metrics interface")
}
```

### 4.10. Integration Testing

**Test Cases**:
```go
// TestLoadSheddingIntegration performs end-to-end load shedding test
func TestLoadSheddingIntegration(t *testing.T) {
    // Set up a test gRPC server with protector
    // Configure protector to return StateHigh
    // Attempt gRPC calls and verify they are rejected
    // Switch to StateLow and verify calls succeed

    t.Skip("Complex integration test - requires full server setup")
}

// TestDynamicBufferSizingIntegration verifies buffer sizes are applied
func TestDynamicBufferSizingIntegration(t *testing.T) {
    // Start server with specific memory ratio
    // Verify that gRPC server options contain expected buffer sizes
    // This may require server introspection or mocking

    t.Skip("Complex integration test - requires server introspection")
}

// TestLoadTestUnderMemoryPressure verifies OOM prevention
func TestLoadTestUnderMemoryPressure(t *testing.T) {
    // Simulate high memory pressure scenario
    // Generate concurrent requests
    // Verify load shedding prevents server crash
    // Measure request rejection rate

    t.Skip("Load test - requires significant test infrastructure")
}
```

## 5. Rollout and Monitoring Strategy

### 5.1. Gradual Rollout
1. Deploy with conservative buffer ratio (0.05) initially
2. Monitor load shedding metrics for false positives
3. Gradually increase ratio based on observed behavior
4. Monitor system performance and memory usage

### 5.2. Key Metrics to Monitor
- `memory_load_shedding_rejections_total` - Should be low under normal conditions
- `grpc_buffer_size_bytes` - Should be reasonable (not too high or low)
- `memory_state` - Should not oscillate rapidly
- System memory usage and GC pressure

### 5.3. Alerting
- Alert on high load shedding rates (>1% of requests)
- Alert on buffer sizes exceeding reasonable limits
- Alert on memory state oscillation (rapid transitions)

### 5.4. Rollback Plan
- Static buffer size flags override dynamic calculation
- Can disable load shedding by passing nil protector
- Conservative defaults ensure safe fallback behavior
