# Watchdog Development Design

## Overview

The Watchdog is a critical component of First Occurrence Data Collection infrastructure, responsible for periodically polling metrics from the BanyanDB container. It works together with the Flight Recorder to ensure metrics data survivability even when the main BanyanDB process crashes.

## Components

The Watchdog component is responsible for periodically polling metrics from the BanyanDB container.

### Responsibilities

- Polls metrics from the BanyanDB container at configurable intervals
- Retrieves metrics data from the BanyanDB metrics endpoint (typically `http://localhost:2121/metrics`)
- Forwards collected metrics to the Flight Recorder for buffering
- Handles connection failures and retries gracefully
- Monitors the health of the BanyanDB process

## API

### Internal API

```go
// Watchdog polls metrics from BanyanDB container
type Watchdog interface {
    // Start begins polling metrics
    Start(ctx context.Context) error
    
    // Stop stops polling metrics
    Stop(ctx context.Context) error
    
    // GetStatus returns the current status
    GetStatus() WatchdogStatus
}

// WatchdogStatus represents the status of the watchdog
type WatchdogStatus struct {
    Active          bool
    LastPollTime    time.Time
    LastPollError   error
    PollCount       uint64
    ErrorCount      uint64
}
```

## E2E

### Test Case: Watchdog Polling

**Objective**: Verify that Watchdog correctly polls metrics at configured intervals.

**Steps**:
1. Start BanyanDB with Flight Recorder enabled and a specific poll interval
2. Monitor Watchdog activity logs
3. Verify that polling occurs at the configured interval
4. Verify that metrics are collected correctly
5. Test error handling when metrics endpoint is temporarily unavailable

**Expected Result**: Watchdog polls metrics at the correct interval and handles errors gracefully.

## Details

### Implementation Details

The Watchdog should:

- **Polling Strategy**: Use a ticker-based approach with configurable intervals
- **Error Handling**: Implement exponential backoff for transient failures
- **Metrics Parsing**: Parse Prometheus text format metrics efficiently
- **Connection Management**: Reuse HTTP client connections for efficiency
- **Health Monitoring**: Track BanyanDB process health and report status

### Flags

#### `--flight-recorder-poll-interval`

- **Type**: `duration`
- **Default**: `15s`
- **Description**: Interval at which the Watchdog polls metrics from the BanyanDB container.

**Example**:
```bash
banyandb standalone --flight-recorder-enabled --flight-recorder-poll-interval=10s
```

#### `--flight-recorder-metrics-endpoint`

- **Type**: `string`
- **Default**: `http://localhost:2121/metrics`
- **Description**: URL of the BanyanDB metrics endpoint to poll from.

**Example**:
```bash
banyandb standalone --flight-recorder-enabled --flight-recorder-metrics-endpoint=http://localhost:2121/metrics
```

### Integration Points

- **Flight Recorder**: Forwards collected metrics to the Flight Recorder for buffering
- **Observability Service**: Integrates with existing `banyand/observability` package
- **Flag Management**: Uses the existing flag system from `pkg/run`
- **Logging**: Uses the existing logging infrastructure from `pkg/logger`

### Performance Considerations

- **Polling Frequency**: Balance between polling frequency and system load
- **Connection Reuse**: Reuse HTTP client connections for efficiency
- **Error Handling**: Implement exponential backoff to avoid overwhelming the system during transient failures
- **Metrics Parsing**: Parse Prometheus text format metrics efficiently to minimize CPU usage

### Security Considerations

- **Endpoint Access**: Ensure proper access control for metrics endpoint
- **Connection Security**: Consider using HTTPS for metrics endpoint if sensitive data is exposed
- **Rate Limiting**: Implement rate limiting to prevent abuse of the metrics endpoint
