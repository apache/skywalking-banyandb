# Watchdog And Flight Recorder Development Design

## Table of Contents
1. [Overview](#overview)
2. [Component Design](#component-design)
3. [Data Flow](#data-flow)
4. [Testing Strategy](#testing-strategy)
5. [Appendix](#appendix)

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
- Maintains a fixed-size circular buffer (RingBuffer) per metric in the `metrics` map
- Uses a shared `timestamps` RingBuffer to store timestamps for each polling cycle
- Stores metric descriptions in the `descriptions` map, keyed by metric name+labels
- Stores metrics in-memory to ensure fast access and persistence across process crashes
- Manages buffer capacity and handles overflow scenarios using circular overwrite behavior
- Tracks write positions using `next`, `count`, and `n` fields for circular buffer management
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

**`Watchdog`**
```go
type Watchdog struct {
	client       *http.Client
	url          string
	interval     time.Duration
}
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

**`pollMetrics(ctx context.Context) ([]metrics.RawMetric, error)`**
- Fetches raw metrics text from endpoint
- Uses metrics package to parse Prometheus text format
- Returns parsed metrics or error
- Implements retry logic with exponential backoff

#### Configuration Flags

**`--poll-metrics-interval`**
- **Type**: `duration`
- **Default**: `10s`
- **Description**: Interval at which the Watchdog polls metrics from the BanyanDB container

**`--metrics-endpoint`**
- **Type**: `string`
- **Default**: `http://localhost:2121/metrics`
- **Description**: URL of the BanyanDB metrics endpoint to poll from

**`--max-metrics-memory-usage-percentage`**
- **Type**: `int`
- **Default**: `10`
- **Description**: Maximum percentage of available memory (based on cgroup memory limit) that can be used for storing metrics in the Flight Recorder. The memory limit is obtained from the container's cgroup configuration (see `pkg/cgroups/memory.go`). When metrics memory usage exceeds this percentage, the Flight Recorder will stop accepting new metrics or evict older data. Valid range: 0-100.

### 2. Flight Recorder Component

**Purpose**: Buffers metrics data using fixed-size circular buffers with in-memory storage

#### Core Responsibilities

- **Metrics Buffering**: Maintains a fixed-size RingBuffer per metric
- **Data Persistence**: Ensures metrics survive process crashes
- **Overflow Handling**: Implements circular overwrite behavior when buffers are full

#### Core Types

**`RingBuffer[T]`** (Generic Ring Buffer)
```go
type RingBuffer[T any] struct {
   next   int         // Next write position in the circular buffer
   values []T         // Fixed-size buffer for values of type T
}

// NewRingBuffer creates a new RingBuffer.
func NewRingBuffer[T any]() *RingBuffer[T]

// Add adds a value to the ring buffer.
func (rb *RingBuffer[T]) Add(v T)
```
- Stores values of any type T in a circular buffer
- Implements circular overwrite behavior when buffer is full
- Provides type-safe operations for both float64 and int64 values

**`MetricRingBuffer`**
```go
type MetricRingBuffer = RingBuffer[float64]

// NewMetricRingBuffer creates a new MetricRingBuffer.
func NewMetricRingBuffer() *MetricRingBuffer
```

**`TimestampRingBuffer`**
```go
type TimestampRingBuffer = RingBuffer[int64]

// NewTimestampRingBuffer creates a new TimestampRingBuffer.
func NewTimestampRingBuffer() *TimestampRingBuffer
```
- Type alias for RingBuffer[int64] for storing timestamps
- Stores timestamps in a circular buffer
- Implements circular overwrite behavior when buffer is full

**`FlightRecorder`**
```go
type DataSource struct {
   metrics        map[string]*MetricRingBuffer // Map from metric name+labels to RingBuffer storing metric values
   timestamps     TimestampRingBuffer          // RingBuffer storing timestamps for each polling cycle
   descriptions   map[string]string            // Map from metric name to HELP content descriptions
   Capacity       int                          // Number of writable metrics length
   TotalWritten   uint64                       // Total number of values written (wraps around)
}
type FlightRecorder struct {
	DataSources   []*DataSource
	CapacitySize  int                           // memory limit
}
```

#### Key Functions

**`RingBuffer[T].Add(v T, capacity int)`**
- Generic method that adds a value of type T to the ring buffer
- Implements circular overwrite behavior when buffers are full, compare the capacity with  current MetricRingBuffer capacity, and manage the buffer with FIFO based on the next write position
- Updates the next write position
- Works for both `RingBuffer[float64]` and `RingBuffer[int64]`

**`MetricRingBuffer.Update(v float64, capacity int)`**
- Adds a metric value to the metric ring buffer
- Updates the embedded RingBuffer[float64]

**`TimestampRingBuffer.Update(v int64, capacity int)`**
- Adds a timestamp value to the timestamp ring buffer
- Uses the generic RingBuffer[int64].Add() method

**`DataSource.Update(m *metric.RawMetric)`**
- Sorts labels for consistent key generation
- Creates MetricKey string from metric name and labels (e.g., "http_requests{method=\"get\"}")
- Update Datasource.MetricBuffers with MetricKey. Please don't add any values
- Get the total count of each RingBuffer based on the capacity of the Datasource.MetricBuffers and the memory limitation
- Create or update the RingBuffer’s total count. Remove data using FIFO strategy if its original size exceeds the new total count
- Uses MetricRingBuffer.Update() method to add values

**`DataSource.ComputeCapacitySize() int`**
- Computes the total number of values currently stored in the DataSource
- Returns the sum of all values stored across all metric RingBuffers in the `metrics` map plus the number of timestamps in `timestamps` RingBuffer
- Each metric RingBuffer's Capacity() method provides the count of metric values stored
- The `timestamps` RingBuffer's Capacity() method provides the count of timestamps stored
- Returns 0 if DataSource is nil or if no buffers are initialized
```
Total Memory = Metrics Map Overhead  
             + Metadata Map Overhead 
             + String Storage 
             + Float64 Values (all RingBuffers)
             + Timestamp RingBuffer values
             + RingBuffer internal structures (next field, values slice headers)
             + Descriptions Map Overhead
```

**`FlightRecorder.NewFlightRecorder(capacitySize int) *FlightRecorder`**
- Creates a new FlightRecorder
- Returns initialized FlightRecorder instance

**`FlightRecorder.AddDataSource(name string, capacitySize int) *DataSource`**
- Creates a new DataSource with the given capacity size
- Initializes the `metrics` map, `timestamps` RingBuffer, and `descriptions` map
- Initializes circular buffer tracking fields (`next`, `count`, `n`)
- Appends the new DataSource to the DataSources array
- Returns initialized DataSource instance

### 3. Metrics Package (`fodc/internal/metrics`)

**Purpose**: Parsing Prometheus text format metrics into structured Go types

#### Core Types

**`Label`**
```go
type Label struct {
	Name  string
	Value string
}
```

**`RawMetric`**
```go
type RawMetric struct {
   Name        string      // Metric name
   Labels      []Label     // Metric labels as key-value pairs
   Value       float64     // Metric value
   Desc        string      // HELP text description if available
}
```

**`MetricKey`**
```go
type MetricKey struct {
   Name   string
   Labels []Label
}

func (mk MetricKey) String() string
```
- Used for uniquely identifying metrics in FlightRecorder
- Labels are sorted for consistent key generation
- String() method generates a canonical representation

#### Key Functions

**`Parse(text string) ([]RawMetric, error)`**
- Parses Prometheus text format metrics
- Handles HELP and TYPE lines
- Parses metric lines with labels and values
- Handles comments and empty lines
- Returns structured RawMetric objects or error

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
   - Parse metric lines with labels and values
   ↓
5. Convert to RawMetric Structures
   ↓
6. Forward to Flight Recorder
   ↓
7. Flight Recorder.Update() Called
   ↓
8. For Each Metric:
   a. Create MetricKey from metric name and sorted labels (e.g., "http_requests{method=\"get\"}")
   b. Ensure the RingBuffer[float64] exists in DataSource.metrics for the MetricKey
     - If it doesn't exist, create a new RingBuffer[float64] (do not add values yet)
   c. Calculate the target capacity for each RingBuffer based on:
     - The capacity of DataSource.metrics map
     - Available memory limitations
   d. Adjust RingBuffer capacity if needed:
     - If the RingBuffer's current size exceeds the new target capacity, remove oldest data using FIFO (First-In-First-Out) strategy
     - Update the RingBuffer's capacity to match the target capacity
   e. Add the metric value to the RingBuffer in DataSource.metrics
   f. Store the metric description in DataSource.descriptions map using the MetricKey
   g. Add timestamp to DataSource.timestamps RingBuffer (once per polling cycle, not per metric)
   ↓
9. Metrics Buffered in Memory via FlightRecorder
```

## Testing Strategy

### Unit Testing

**Metrics Package**
- Test `Parse()` with various Prometheus formats
- Test HELP line parsing
- Test label parsing (quoted values, special characters)
- Test edge cases (empty labels, invalid formats, comments)
- Test performance with large metrics outputs

**Flight Recorder Package**
- Test RingBuffer[T].Add() write operations for both float64 and int64 types
- Test MetricRingBuffer.Update()
- Test TimestampRingBuffer.Update() operations
- Test FlightRecorder.Update() with new and existing metrics
- Test circular overwrite behavior
- Test histogram storage and retrieval
- Test concurrent writes
- Test error handling
- Test generic RingBuffer[T] type safety

**Watchdog Package**
- Test polling interval accuracy
- Test HTTP client connection reuse
- Test exponential backoff retry logic
- Test error handling for connection failures
- Test metrics forwarding to Flight Recorder

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

**Format**: `metric_name{label1="value1", label2="value2"} value`

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
**Summary Format**:
```
# HELP http_request_duration_seconds Request duration summary
# TYPE http_request_duration_seconds summary
http_request_duration_seconds{quantile="0.5"} 0.052
http_request_duration_seconds{quantile="0.9"} 0.120
http_request_duration_seconds{quantile="0.99"} 0.250
http_request_duration_seconds_sum 45.2
http_request_duration_seconds_count 300
```

**Counter Format**:
```
# HELP http_requests_total Total number of HTTP requests
# TYPE http_requests_total counter
http_requests_total{method="GET",status="200"} 1234
http_requests_total{method="POST",status="200"} 567
http_requests_total{method="GET",status="404"} 89
http_requests_total{method="POST",status="500"} 12
```
