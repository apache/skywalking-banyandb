# First Occurrence Data Collection (FODC) Development Design

## Table of Contents
1. [Overview](#overview)
2. [Component Design](#component-design)
3. [Data Flow](#data-flow)
4. [API Design](#api-design)
5. [Testing Strategy](#testing-strategy)
6. [Performance Considerations](#performance-considerations)
7. [Appendix](#appendix)

## Overview

The First Occurrence Data Collection (FODC) infrastructure consists of two main components working together to ensure metrics data survivability in BanyanDB:

**Watchdog**: Periodically polls metrics from the BanyanDB container and forwards them to the Flight Recorder for buffering.

**Flight Recorder**: Buffers metrics data using fixed-size circular buffers (RingBuffer) with in-memory storage, ensuring data persists even when the main BanyanDB process crashes.

Together, these components capture and preserve metrics data to ensure that critical observability data is not lost during process crashes.

### Responsibilities

**Watchdog Component**
- Polls metrics from BanyanDB at configurable intervals
- Parses Prometheus text format metrics efficiently
- Forwards collected metrics to Flight Recorder for buffering
- Handles connection failures and retries gracefully
- Monitors BanyanDB process health

**Flight Recorder Component**
- Maintains a fixed-size circular buffer (RingBuffer) per metric
- Stores metrics in-memory to ensure fast access and persistence across process crashes
- Manages buffer capacity and handles overflow scenarios using circular overwrite behavior
- Ensures data integrity and prevents data loss during crashes

### Component Interaction Flow

```
BanyanDB Metrics Endpoint
         │
         │ (HTTP GET /metrics)
         ▼
    Watchdog Component
         │
         │ (Poll at interval)
         │
         │ Parse Prometheus Format
         │
         │ Forward Metrics
         ▼
    Flight Recorder Component
         │
         │ Write to RingBuffer
         │
         │ (Per-metric buffers)
         ▼
    In-Memory Storage
```

## Component Design

### 1. Watchdog Component

**Purpose**: Periodically polls metrics from BanyanDB and forwards them to Flight Recorder

#### Core Responsibilities

- **Metrics Polling**: Polls metrics from BanyanDB metrics endpoint at configurable intervals
- **Metrics Parsing**: Uses metrics package to parse Prometheus text format efficiently
- **Error Handling**: Implements exponential backoff for transient failures
- **Health Monitoring**: Tracks BanyanDB process health and reports status

#### Core Types

**`Watchdog` Interface**
```go
type Watchdog interface {
    // Start begins polling metrics
    Start(ctx context.Context) error
    
    // Stop stops polling metrics
    Stop(ctx context.Context) error
}
```

**`PollMetrics` Function**
```go
// PollMetrics retrieves and parses metrics from the metrics endpoint
func (w *watchdog) PollMetrics(ctx context.Context) ([]metrics.Metric, error)
```

#### Key Functions

**`Start(ctx context.Context) error`**
- Initializes the watchdog component
- Starts polling loop with configurable interval
- Sets up HTTP client with connection reuse
- Begins periodic metrics collection

**`Stop(ctx context.Context) error`**
- Gracefully stops the polling loop
- Closes HTTP connections
- Ensures in-flight requests complete

**`PollMetrics(ctx context.Context) ([]metrics.Metric, error)`**
- Fetches raw metrics text from endpoint
- Uses metrics package to parse Prometheus text format
- Returns parsed metrics or error
- Implements retry logic with exponential backoff

#### Configuration Flags

**`--poll-interval`**
- **Type**: `duration`
- **Default**: `15s`
- **Description**: Interval at which the Watchdog polls metrics from the BanyanDB container

**`--metrics-endpoint`**
- **Type**: `string`
- **Default**: `http://localhost:2121/metrics`
- **Description**: URL of the BanyanDB metrics endpoint to poll from

### 2. Flight Recorder Component

**Purpose**: Buffers metrics data using fixed-size circular buffers with in-memory storage

#### Core Responsibilities

- **Metrics Buffering**: Maintains a fixed-size RingBuffer per metric
- **Data Persistence**: Ensures metrics survive process crashes
- **Overflow Handling**: Implements circular overwrite behavior when buffers are full

#### Core Types

**`FlightRecorder` Interface**
```go
type FlightRecorder interface {
    // Write adds metrics data to the buffer
    Write(metrics []MetricEntry) error
    
    // Start initializes the flight recorder
    Start(ctx context.Context) error
    
    // Stop stops the flight recorder
    Stop(ctx context.Context) error
}
```

**`MetricEntry`**
```go
type MetricEntry struct {
   Name        string
   Description string
   Labels      map[string]string
   Value       float64
}
```

**`RingBuffer`**
```go
type RingBuffer struct {
    next   int        // Next write position in the circular buffer
    values []float64  // Fixed-size buffer for metric values
    n      uint64     // Total number of values written (wraps around)
    ch     chan float64  // Channel for receiving values
    bindN  uint64     // Binding number for synchronization
}
```

#### Key Functions

**`Write(metrics []MetricEntry) error`**
- Adds metrics data to appropriate RingBuffer
- Creates new RingBuffer if metric doesn't exist
- Handles circular overwrite when buffer is full
- Updates overflow counters

**`Start(ctx context.Context) error`**
- Initializes the flight recorder
- Prepares buffer storage

**`Stop(ctx context.Context) error`**
- Gracefully stops the flight recorder
- Ensures all writes complete

### 3. Metrics Package (`fodc/internal/metrics`)

**Purpose**: Parsing Prometheus text format metrics into structured Go types

#### Core Types

**`Metric`**
```go
type Metric struct {
   Name        string            // Metric name
   Labels      map[string]string // Metric labels as key-value pairs
   Value       float64           // Metric value
   Description string            // HELP text description if available
}
```

#### Key Functions

**`Parse(text string) ([]Metric, error)`**
- Parses Prometheus text format metrics
- Handles HELP and TYPE lines
- Parses metric lines with labels and values
- Handles comments and empty lines
- Returns structured Metric objects or error

#### Implementation Requirements

- **Prometheus Format Support**: Parse standard Prometheus text format including HELP lines, TYPE lines, metric lines with labels
- **Label Parsing**: Parse metric labels in format `metric_name{label1="value1",label2="value2"} value`
- **Error Handling**: Return descriptive errors for malformed input
- **Performance**: Optimize for parsing large metrics outputs efficiently
- **Testing**: Include comprehensive tests for various Prometheus format edge cases

## Data Flow

### Metrics Collection Flow

```
1. Watchdog Polling Timer Triggers
   ↓
2. HTTP GET Request to Metrics Endpoint
   (http://localhost:2121/metrics)
   ↓
3. Read Response Body (Prometheus Text Format)
   ↓
4. Parse Metrics Using metrics.Parse()
   - Parse HELP lines for descriptions
   - Parse TYPE lines for metric types
   - Parse metric lines with labels and values
   ↓
5. Convert to MetricEntry Structures
   ↓
6. Forward to Flight Recorder
   ↓
7. Flight Recorder.Write() Called
   ↓
8. For Each Metric:
   a. Look up or create RingBuffer
   b. Write value to RingBuffer
   c. Handle circular overwrite if buffer full
   d. Update timestamps and counters
   ↓
9. Metrics Buffered in Memory
```

## API Design

### Flight Recorder Interface

```go
// FlightRecorder buffers metrics data in a circular buffer
type FlightRecorder interface {
    // Write adds metrics data to the buffer
    Write(metrics []MetricEntry) error
    
    // Start initializes the flight recorder
    Start(ctx context.Context) error
    
    // Stop stops the flight recorder
    Stop(ctx context.Context) error
}
```

### Watchdog Interface

```go
// Watchdog polls metrics from BanyanDB container
type Watchdog interface {
    // Start begins polling metrics
    Start(ctx context.Context) error
    
    // Stop stops polling metrics
    Stop(ctx context.Context) error
}
```

### Metrics Package API

```go
// Parse parses Prometheus text format metrics and returns a slice of Metric objects
func Parse(text string) ([]Metric, error)

// Metric represents a parsed Prometheus metric
type Metric struct {
    Name        string
    Labels      map[string]string
    Value       float64
    Description string
    Type        MetricType
}
```

## Testing Strategy

### Unit Testing

**Metrics Package**
- Test `Parse()` with various Prometheus formats
- Test HELP and TYPE line parsing
- Test label parsing (quoted values, special characters)
- Test edge cases (empty labels, invalid formats, comments)
- Test performance with large metrics outputs

**Flight Recorder Package**
- Test RingBuffer write and read operations
- Test circular overwrite behavior
- Test buffer status aggregation
- Test concurrent writes
- Test error handling

**Watchdog Package**
- Test polling interval accuracy
- Test HTTP client connection reuse
- Test exponential backoff retry logic
- Test error handling for connection failures
- Test metrics forwarding to Flight Recorder

### Integration Testing

**End-to-End Flow**
- Test metrics collection → parsing → buffering
- Test crash recovery scenario
- Test buffer overflow handling
- Test concurrent metric writes

### E2E Testing

**Test Case 1: Basic Metrics Buffering**
- Start BanyanDB
- Generate metrics by performing operations
- Wait for Watchdog to poll metrics
- Verify metrics are buffered correctly through internal checks

**Test Case 2: Buffer Overflow Handling**
- Start BanyanDB
- Generate large number of metric values (exceeding buffer size)
- Verify circular overwrite behavior
- Verify newest metrics are preserved
- Verify oldest metrics are overwritten correctly

**Test Case 3: Crash Recovery**
- Start BanyanDB
- Generate metrics and wait for buffering
- Forcefully terminate BanyanDB process
- Restart BanyanDB
- Verify metrics are still available through internal checks

**Test Case 4: Crash Recovery Verification**
- Start BanyanDB
- Generate metrics
- Verify metrics buffering through internal checks
- Verify data persistence across restarts

## Performance Considerations

### Memory Management

**Ring Buffers**
- Fixed-size buffers prevent unbounded growth
- O(1) memory per metric regardless of time window
- Automatic old data eviction via circular overwrite
- Memory usage: ~100 bytes per buffer entry (configurable)

**Metric Storage**
- Map-based indexing for O(1) lookups
- One RingBuffer per unique metric
- Memory scales linearly with metric cardinality

**Metrics Parsing**
- Stream processing (no full file load)
- Early termination on errors
- Efficient string operations
- Minimal allocations

### Scalability Limits

**Current Design**
- Suitable for hundreds to thousands of metrics
- Memory: ~100 bytes per metric entry (configurable buffer size)
- CPU: Minimal overhead for parsing and buffering

**Potential Bottlenecks**
- High metric cardinality (>10,000 metrics)
- Very high polling frequency (<1s)
- Large metrics response size (>10MB)
- Concurrent API queries during high write load

### Optimization Strategies

**Connection Reuse**
- Reuse HTTP client connections for metrics polling
- Implement connection pooling
- Set appropriate timeouts

**Concurrent Access**
- Optimize for concurrent reads and writes
- Use appropriate synchronization primitives
- Minimize lock contention

## Appendix

### Code Organization

**Package Structure**
```
fodc/
  internal/
    metrics/      - Prometheus metrics parsing
    watchdog/     - Watchdog component
    flightrecorder/ - Flight Recorder component
banyand/
  observability/ - Integration with observability service
  liaison/
    http/        - HTTP API endpoints
```

### Integration Points

**Existing Components**
- **Observability Service**: Integrates with `banyand/observability` package
- **HTTP Server**: Uses existing HTTP server infrastructure from `banyand/liaison/http`
- **Flag Management**: Uses existing flag system from `pkg/run`
- **Logging**: Uses existing logging infrastructure from `pkg/logger`

**Dependencies**
- Standard library: `net/http`, `context`, `time`, `fmt`, etc.
- Minimal external dependencies
- Prefer standard library when possible

### Prometheus Text Format Reference

**Format**: `metric_name{label1="value1", label2="value2"} value timestamp`

**Example**:
```
# HELP banyandb_stream_tst_inverted_index_total_doc_count Total document count
# TYPE banyandb_stream_tst_inverted_index_total_doc_count gauge
banyandb_stream_tst_inverted_index_total_doc_count{index="test"} 12345
cpu_usage{host="server1"} 75.5
```

**Histogram Format**:
```
# HELP http_request_duration_seconds Request duration histogram
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.1"} 100
http_request_duration_seconds_bucket{le="0.5"} 200
http_request_duration_seconds_bucket{le="+Inf"} 300
http_request_duration_seconds_count 300
http_request_duration_seconds_sum 45.2
```

### E2E Test Configuration

E2E tests are configured in `test/e2e-v2/cases/flight-recorder/e2e.yaml` with Docker Compose setup in `test/e2e-v2/cases/flight-recorder/docker-compose.yml`.
