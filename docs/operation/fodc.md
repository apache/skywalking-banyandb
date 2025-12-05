# FODC

FODC is a monitoring tool for BanyanDB containers that polls Prometheus metrics and records them in a flight recorder for analysis.

**FODC can run as a Sidecar** alongside each BanyanDB instance, automatically discovering and monitoring the BanyanDB instance in the same pod/container group. This sidecar pattern ensures that diagnostic data collection is coordinated and always available.

## Features

- 🔍 **Metrics Polling**: Continuously polls Prometheus metrics from BanyanDB container
- 🎯 **Flight Recorder**: Buffers metrics data in a circular buffer using memory-mapped files, ensuring data survives crashes
- 🚀 **Sidecar Mode**: Automatic service discovery and coordination when running alongside BanyanDB instances
- 🏥 **Health Endpoints**: Built-in health endpoints for Kubernetes liveness/readiness probes

## Installation

```bash
cd fodc
make fodc-cli fodc-view
```

The binaries will be built at:
- `build/bin/dev/fodc-cli` - Main monitoring tool
- `build/bin/dev/fodc-view` - Flight recorder viewer tool

## Usage

### Basic Usage

```bash
./build/bin/dev/fodc-cli
```

### With Custom Configuration

```bash
./build/bin/dev/fodc-cli \
  --metrics-url=http://localhost:2121/metrics \
  --poll-interval=5s \
  --health-url=http://localhost:17913/api/healthz
```

### Sidecar Mode

FODC can run in **sidecar mode** with automatic service discovery:

```bash
./build/bin/dev/fodc-cli --sidecar
```

In sidecar mode, FODC will:
- Automatically discover BanyanDB endpoints using environment variables or defaults
- Start a health endpoint server (default port: 17914)
- Provide Kubernetes-compatible health endpoints (`/healthz`, `/ready`, `/live`)
- Share the same network namespace as BanyanDB (in Kubernetes/Docker)

**Environment Variables for Sidecar Mode:**
- `BANYANDB_HOST`: BanyanDB hostname (default: `localhost`)
- `BANYANDB_METRICS_PORT`: Metrics port (default: `2121`)
- `BANYANDB_HTTP_PORT`: HTTP API port (default: `17913`)
- `POD_NAME`: Kubernetes pod name (auto-injected)
- `POD_NAMESPACE`: Kubernetes pod namespace (auto-injected)
- `POD_IP`: Kubernetes pod IP (auto-injected)

**Note:** The FODC health endpoint port is controlled by the `--health-port` flag (default: `17914`), not an environment variable.

### Command Line Flags

- `--sidecar`: Run in sidecar mode with auto-discovery (default: `false`)
- `--metrics-url`: Prometheus metrics endpoint URL (auto-discovered in sidecar mode)
- `--poll-interval`: Interval for polling metrics (default: `5s`)
- `--health-url`: Health check endpoint URL (auto-discovered in sidecar mode)
- `--flight-recorder-path`: Path to flight recorder memory-mapped file (default: `/tmp/fodc-flight-recorder.bin`)
- `--flight-recorder-buffer`: Number of snapshots to buffer in flight recorder (default: `1000`)
- `--flight-recorder-rotation`: Interval to automatically clear/rotate flight recorder (default: `0` = disabled). Examples: `24h`, `1h30m`, `30m`
- `--health-port`: Port for sidecar health endpoint (default: `17914`)

## Sidecar Deployment

### Docker Compose

See `examples/docker-compose-sidecar.yml` for a complete example of FODC running as a sidecar alongside BanyanDB:

```bash
cd fodc/examples
docker-compose -f docker-compose-sidecar.yml up
```

The sidecar will automatically discover BanyanDB on `localhost` (same network namespace) and start monitoring.

### Kubernetes

See `examples/kubernetes-sidecar.yaml` for a complete Kubernetes deployment example:

```bash
kubectl apply -f examples/kubernetes-sidecar.yaml
```

The deployment includes:
- BanyanDB container as the main application
- FODC sidecar container in the same pod
- Shared volumes for flight recorder data
- Health probes for both containers
- Service discovery via environment variables

**Key Features:**
- Both containers share the same pod network namespace (use `localhost` to communicate)
- Shared volumes allow FODC to persist flight recorder data
- Kubernetes environment variables (`POD_NAME`, `POD_IP`, etc.) are automatically injected
- Health endpoints enable proper liveness/readiness probe configuration

## Running in Docker

To run FODC inside a Docker container monitoring BanyanDB:

```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o fodc ./cmd/fodc

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/fodc /usr/local/bin/fodc
ENTRYPOINT ["fodc"]
```

Or use the provided Dockerfile:

```bash
cd fodc
make docker-build
```

## Integration with BanyanDB

FODC is designed to work with BanyanDB containers. Ensure that:

1. BanyanDB metrics endpoint is exposed (port 2121)
2. Health check endpoint is accessible (port 17913)

### Sidecar Pattern Benefits

When running as a sidecar, FODC provides several advantages:

1. **Automatic Discovery**: No manual configuration needed - FODC discovers BanyanDB automatically
2. **Shared Network**: Uses the same network namespace, enabling efficient localhost communication
3. **Coordinated Lifecycle**: Sidecar starts/stops with the BanyanDB instance
4. **Shared Storage**: Can share volumes for flight recorder data
5. **Health Monitoring**: Provides health endpoints for Kubernetes/Docker orchestration
6. **Per-Instance Monitoring**: Each BanyanDB instance has its own dedicated FODC sidecar

## Flight Recorder

The Flight Recorder is a critical component that ensures metrics data survives crashes:

- **Memory-Mapped Storage**: Uses memory-mapped files for efficient persistence
- **Circular Buffer**: Implements a circular buffer to store the most recent N snapshots
- **Crash Recovery**: Automatically recovers data on startup if the process crashed
- **Independent Memory Space**: Operates in its own memory space, separate from the main process

The flight recorder buffers metrics snapshots in a memory-mapped file. When FODC starts, it automatically attempts to recover any previously recorded data, allowing you to analyze metrics from before a crash.

### Flight Recorder Rotation/Clearing

The flight recorder uses a circular buffer, so old data is automatically overwritten when the buffer is full. However, you can also:

**1. Automatic Rotation (Periodic Clearing):**

Enable automatic clearing at regular intervals:

```bash
./build/bin/dev/fodc-cli --flight-recorder-rotation=24h
```

This will clear the flight recorder every 24 hours. Other examples:
- `--flight-recorder-rotation=1h` - Clear every hour
- `--flight-recorder-rotation=30m` - Clear every 30 minutes
- `--flight-recorder-rotation=1h30m` - Clear every 1.5 hours

**2. Manual Clearing:**

Use the viewer tool to manually clear the flight recorder:

```bash
./build/bin/dev/fodc-view --path=/tmp/fodc-flight-recorder.bin --clear
```

**Note:** The circular buffer automatically overwrites old data when full, so rotation is mainly useful for:
- Resetting the total count statistic
- Starting fresh after incidents
- Managing disk space if you want to archive data before clearing

### Viewing Flight Recorder Data

You can view the contents of a flight recorder file using the `fodc-view` command:

#### View all snapshots (pretty format):
```bash
./build/bin/dev/fodc-view --path=/tmp/fodc-flight-recorder.bin
```

#### View as JSON:
```bash
./build/bin/dev/fodc-view --path=/tmp/fodc-flight-recorder.bin --format=json
```

#### Save JSON to file (recommended for large datasets):
```bash
./build/bin/dev/fodc-view --path=/tmp/fodc-flight-recorder.bin --format=json --output=snapshots.json
```

#### Stream JSON output (JSONL format, one snapshot per line - best for large datasets):
```bash
./build/bin/dev/fodc-view --path=/tmp/fodc-flight-recorder.bin --format=json --stream --output=snapshots.jsonl
```

#### View only statistics:
```bash
./build/bin/dev/fodc-view --path=/tmp/fodc-flight-recorder.bin --format=stats
```

#### View recent snapshots:
```bash
./build/bin/dev/fodc-view --path=/tmp/fodc-flight-recorder.bin --recent --recent-n=10
```

#### Limit number of snapshots:
```bash
./build/bin/dev/fodc-view --path=/tmp/fodc-flight-recorder.bin --limit=5
```

**Command Options:**
- `--path`: Path to flight recorder file (default: `/tmp/fodc-flight-recorder.bin`)
- `--format`: Output format: `pretty`, `json`, or `stats` (default: `pretty`)
- `--limit`: Limit number of snapshots to display (0 = all)
- `--recent`: Show only recent snapshots
- `--recent-n`: Number of recent snapshots to show when using `--recent` (default: 10)
- `--output`: Output file path (default: stdout). **Recommended for large JSON outputs to avoid truncation**
- `--stream`: Stream JSON output in JSONL format (one snapshot per line). **Best for large datasets** as it's more memory-efficient and avoids truncation issues
- `--verbose`: Show timing and performance information
- `--clear`: Clear all snapshots from the flight recorder

### Recovering Data Programmatically

You can also programmatically recover data from a flight recorder file:

```go
import "github.com/apache/skywalking-banyandb/fodc/internal/flightrecorder"

snapshots, err := flightrecorder.Recover("/tmp/fodc-flight-recorder.bin")
if err != nil {
    log.Fatal(err)
}
// Process recovered snapshots
```

## Health Endpoints (Sidecar Mode)

When running in sidecar mode, FODC exposes health endpoints for monitoring and orchestration:

- **`GET /health`** or **`GET /healthz`**: Full health status with JSON response
  - Returns 200 OK if healthy, 503 Service Unavailable if BanyanDB is not connected
  - Includes metrics collection status, BanyanDB connection status, and uptime
  
- **`GET /ready`**: Readiness probe endpoint
  - Returns 200 OK when FODC is ready to monitor
  - Returns 503 when BanyanDB is not connected
  
- **`GET /live`**: Liveness probe endpoint
  - Returns 200 OK when FODC is alive
  - Returns 503 when FODC has stopped

**Example Health Response:**
```json
{
  "status": "running",
  "timestamp": "2024-01-01T12:00:00Z",
  "version": "0.1.0",
  "banyandb": {
    "connected": true,
    "last_check": "2024-01-01T12:00:00Z"
  },
  "metrics": {
    "total_snapshots": 1000,
    "last_snapshot_time": "2024-01-01T12:00:00Z",
    "errors": 0
  },
  "uptime": "1h30m0s",
  "metadata": {
    "mode": "sidecar",
    "banyandb_host": "localhost",
    "pod_name": "banyandb-0"
  }
}
```
