# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache SkyWalking BanyanDB is a distributed observability database written in Go. It's designed to ingest, analyze, and store Metrics, Tracing, and Logging data, particularly optimized for Apache SkyWalking APM system.

## Build and Development Commands

### Prerequisites
- Go 1.24
- Node 20.12
- Git >= 2.30
- GNU make
- Linux, macOS, or Windows + WSL2

### Core Commands

```bash
# Check prerequisites
make check-req

# Generate necessary build files (must run before building)
make generate

# Build all projects (binaries will be in <project>/build/bin/)
make build

# Run unit tests
make test

# Run tests with race detector
make test-race

# Run tests with coverage
make test-coverage

# Run tests in CI mode
make test-ci

# Lint code (run before submitting PRs)
make lint

# Format code (fixes obvious style issues)
make format

# Clean all artifacts
make clean

# Build release artifacts
make release
```

### Project-specific builds

```bash
# Build specific components
cd banyand && make build  # Server component
cd bydbctl && make build  # CLI tool
cd ui && make build        # Web UI
```

## Architecture and Code Structure

### Main Components

1. **banyand** - The main server daemon
   - `/banyand/` - Server implementation
   - `/banyand/measure/` - Measure data processing
   - `/banyand/stream/` - Stream data processing
   - `/banyand/metadata/` - Metadata management
   - `/banyand/liaison/` - gRPC and HTTP server interfaces
   - `/banyand/queue/` - Internal queue implementation
   - `/banyand/internal/storage/` - Storage layer with TSDB implementation

2. **bydbctl** - Command-line interface tool
   - `/bydbctl/` - CLI implementation for managing BanyanDB

3. **API and Protocol Buffers**
   - `/api/proto/` - Protocol buffer definitions
   - `/api/data/` - Data model implementations

4. **Core Packages**
   - `/pkg/index/` - Indexing system with inverted index
   - `/pkg/encoding/` - Time-series encoding algorithms
   - `/pkg/query/` - Query engine and logical plan execution
   - `/pkg/fs/` - File system abstractions (local, S3, Azure, GCS)
   - `/pkg/bus/` - Event bus for internal communication
   - `/pkg/flow/` - Stream processing framework

### Storage Architecture

BanyanDB uses a custom Time-Series Database (TSDB) implementation:
- **Measure**: For metrics data with pre-aggregation support
- **Stream**: For tracing and logging data with tag-based indexing
- **Property**: For metadata and configuration storage

Data is organized in:
- **Groups**: Logical data collections
- **Shards**: Horizontal partitioning units
- **Segments**: Time-based data chunks
- **Blocks**: Compressed data units within segments

## Code Style and Standards

This project follows strict Go coding standards. Key requirements are defined in `AI_CODING_GUIDELINES.md`:

### Critical Rules
1. **Import Organization** (using gci):
   - Standard library imports
   - Default imports
   - Project imports (github.com/apache/skywalking-banyandb/)

2. **Import Aliases** for protobuf packages:
   - `commonv1` for common/v1
   - `databasev1` for database/v1
   - `measurev1` for measure/v1
   - `streamv1` for stream/v1
   - `modelv1` for model/v1
   - `propertyv1` for property/v1
   - `clusterv1` for cluster/v1

3. **Variable Shadowing Prevention**:
   - Never shadow variables in nested scopes
   - Use descriptive names (e.g., `processErr` instead of `err` in nested scope)

4. **Error Handling**:
   - Always wrap errors with context using `fmt.Errorf` and `%w`
   - Check errors immediately after function calls

5. **Formatting**:
   - Use gofumpt (stricter than gofmt)
   - Maximum line length: 170 characters

## Testing Strategy

- Unit tests: Located alongside source files as `*_test.go`
- Integration tests: In `/test/integration/`
- E2E tests: In `/test/e2e-v2/`
- Stress tests: In `/test/stress/`

Run specific test suites:
```bash
# Unit tests only
make test

# Integration tests
cd test/integration && go test ./...

# Run with custom timeout values
make test-ci TEST_CI_OPTS="--timeout=30m"
```

## Key Development Patterns

1. **Service Registration**: Services register with the run.Group pattern for lifecycle management
2. **Schema Management**: Uses etcd for distributed schema synchronization
3. **Query Processing**: Implements logical and physical query plans with distributed execution
4. **Index Management**: Uses inverted indexes with posting lists (Roaring Bitmaps)
5. **Time-series Encoding**: Custom encoding for efficient storage (XOR, Delta, Dictionary)
6. **Event-driven Architecture**: Internal bus for component communication

## Common Development Tasks

### Adding a New API Endpoint
1. Define protobuf in `/api/proto/banyandb/`
2. Run `make generate` to generate Go code
3. Implement handler in `/banyand/liaison/grpc/`
4. Add tests alongside implementation

### Modifying Storage Layer
1. Core storage logic in `/banyand/internal/storage/`
2. Measure-specific: `/banyand/measure/`
3. Stream-specific: `/banyand/stream/`
4. Remember to update both write and query paths

### Working with Indexes
1. Index definitions in `/pkg/index/`
2. Inverted index implementation in `/pkg/index/inverted/`
3. Query planning uses indexes in `/pkg/query/logical/`

## Important Files to Know

- `/go.mod` - Go module dependencies
- `/revive.toml` - Linter configuration
- `/scripts/build/` - Build system scripts
- `/banyand/internal/storage/rotation.go` - Data rotation logic
- `/pkg/query/logical/measure/measure_plan.go` - Measure query planning
- `/pkg/query/logical/stream/stream_plan.go` - Stream query planning

## eBPF Sidecar Agent Implementation

The eBPF sidecar agent provides kernel-level observability for BanyanDB operations, offering insights into system calls, memory management, and I/O patterns. This agent runs as an independent service alongside BanyanDB, minimizing overhead while providing deep system metrics.

### Architecture Overview

The eBPF sidecar is designed as a standalone service with:
- **Independent Process**: Runs as a separate container/process alongside BanyanDB
- **API Endpoints**: gRPC and HTTP APIs for metrics retrieval and health checks  
- **Lifecycle Management**: Proper startup, shutdown, and configuration management
- **Metrics Collection**: Periodic collection and aggregation of kernel metrics
- **Export Capabilities**: Push metrics to BanyanDB or pull via Prometheus format

Location: `/ebpf-sidecar/` (standalone component at root level)

### Sidecar Agent Design

The agent follows a modular architecture:

```
/ebpf-sidecar/
├── Makefile                      # Build commands
├── Dockerfile                    # Container image
├── cmd/
│   └── sidecar/
│       └── main.go              # Entry point
├── internal/
│   ├── config/                  # Configuration management
│   │   └── config.go
│   ├── server/                  # gRPC/HTTP servers
│   │   ├── grpc.go
│   │   └── http.go
│   ├── collector/               # Metrics collection
│   │   ├── collector.go
│   │   └── scheduler.go
│   └── ebpf/                    # eBPF programs
│       ├── programs/            # eBPF C source
│       │   └── fadvise.c
│       ├── generated/           # bpf2go output
│       └── loader.go            # Program loader
├── api/                          # API definitions
│   └── proto/
│       └── metrics.proto
└── pkg/
    ├── metrics/                  # Metrics types
    └── export/                   # Export formats
        ├── prometheus.go
        └── banyandb.go
```

### Current Monitoring Capabilities

1. **System Call Monitoring**
   - fadvise operations (cache management)
   - File I/O patterns
   - Memory allocation tracking

2. **Memory Management**
   - Page cache statistics
   - Memory reclamation events
   - Direct/indirect reclaim tracking

3. **Extensible Framework**
   - Plugin architecture for new eBPF programs
   - Dynamic loading of monitoring modules
   - Configuration-driven feature enablement

### Implementation Process for New eBPF Features

#### 1. Prerequisites for eBPF Development

```bash
# Install required tools
apt-get install -y clang llvm libbpf-dev linux-headers-$(uname -r)

# Install Go eBPF tools
go install github.com/cilium/ebpf/cmd/bpf2go@latest
```

#### 2. Writing New eBPF Programs

Create your eBPF C program in `/ebpf-sidecar/internal/ebpf/programs/`:

```c
// Example structure for new monitoring feature
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 10240);
    __type(key, u32);
    __type(value, struct your_stats);
} stats_map SEC(".maps");

SEC("tracepoint/category/event")
int trace_your_event(struct trace_event_raw_your_event *ctx) {
    // Implementation
    return 0;
}
```

#### 3. Generating Go Bindings

Add generation directive in your Go loader file (`/ebpf-sidecar/internal/ebpf/loader.go`):

```go
//go:generate go run github.com/cilium/ebpf/cmd/bpf2go -cc clang -cflags "-O2 -Wall" -target amd64 -type your_stats_t bpf programs/your_program.c
//go:generate go run github.com/cilium/ebpf/cmd/bpf2go -cc clang -cflags "-O2 -Wall" -target arm64 -type your_stats_t bpf programs/your_program.c
```

Run generation:
```bash
cd /ebpf-sidecar/internal/ebpf
go generate ./...
```

#### 4. Integration Pattern

Create monitoring modules in `/ebpf-sidecar/internal/collector/`:

```go
// Define stats structures matching eBPF maps
type YourStats struct {
    // Fields matching the eBPF structure
}

// Initialization function
func RunYourMonitor() (func(), error) {
    objs := BpfObjects{}
    if err := LoadBpfObjects(&objs, nil); err != nil {
        return nil, fmt.Errorf("loading BPF objects: %w", err)
    }
    
    // Attach to tracepoints/kprobes
    tp, err := link.Tracepoint("category", "event", objs.YourProgram, nil)
    // ...
    
    cleanup := func() {
        tp.Close()
        objs.Close()
    }
    return cleanup, nil
}

// Stats retrieval function
func GetYourStats(objs *BpfObjects) ([]YourStats, error) {
    // Iterate over maps and collect data
}
```

### Service Configuration

The sidecar agent is configured via environment variables or config file:

```yaml
# /ebpf-sidecar/configs/config.yaml
server:
  grpc:
    port: 9090
    tls:
      enabled: false
  http:
    port: 8080
    metrics_path: /metrics

collector:
  interval: 10s
  modules:
    - fadvise
    - memory
    - io

export:
  type: prometheus  # or "banyandb"
  banyandb:
    endpoint: localhost:17912
    group: "ebpf-metrics"
```

### Sidecar Agent Deployment

#### 1. Building the Sidecar Container

```dockerfile
# Dockerfile for eBPF sidecar
FROM golang:1.24 AS builder
WORKDIR /app
COPY . .
RUN make generate && make build

FROM ubuntu:22.04
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /app/build/bin/ebpf-sidecar /usr/local/bin/
COPY --from=builder /app/configs/config.yaml /etc/ebpf-sidecar/
ENTRYPOINT ["/usr/local/bin/ebpf-sidecar"]
```

#### 2. Kubernetes Deployment

```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: banyandb
    image: apache/skywalking-banyandb:latest
    # ... banyandb config
  
  - name: ebpf-monitor
    image: banyandb-ebpf-monitor:latest
    securityContext:
      privileged: true  # Required for eBPF
    volumeMounts:
    - name: sys
      mountPath: /sys
    - name: headers
      mountPath: /lib/modules
      readOnly: true
  
  volumes:
  - name: sys
    hostPath:
      path: /sys
  - name: headers
    hostPath:
      path: /lib/modules
```

### API Specifications

The sidecar exposes both gRPC and HTTP APIs:

#### gRPC API
```protobuf
service EBPFMetrics {
  rpc GetMetrics(GetMetricsRequest) returns (GetMetricsResponse);
  rpc StreamMetrics(StreamMetricsRequest) returns (stream MetricData);
  rpc GetHealth(HealthRequest) returns (HealthResponse);
}
```

#### HTTP Endpoints
- `GET /metrics` - Prometheus format metrics
- `GET /health` - Health check endpoint
- `GET /api/v1/stats` - JSON format statistics
- `POST /api/v1/config` - Dynamic configuration

### Testing eBPF Programs

#### 1. Unit Testing

Create tests in `/ebpf-sidecar/internal/collector/`:

```go
func TestYourMonitor(t *testing.T) {
    // Skip if not running with privileges
    if os.Getuid() != 0 {
        t.Skip("Test requires root privileges")
    }
    
    cleanup, err := RunYourMonitor()
    require.NoError(t, err)
    defer cleanup()
    
    // Trigger monitored events
    // Verify stats collection
}
```

#### 2. Benchmark Testing

```go
func BenchmarkYourMonitor(b *testing.B) {
    cleanup, _ := RunYourMonitor()
    defer cleanup()
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        // Measure overhead
    }
}
```

### Metrics Export Formats

The sidecar supports multiple export formats:

#### Prometheus Format
```go
# HELP ebpf_fadvise_calls_total Total number of fadvise calls
# TYPE ebpf_fadvise_calls_total counter
ebpf_fadvise_calls_total{pid="1234",advice="dontneed"} 42

# HELP ebpf_memory_reclaim_pages Pages reclaimed
# TYPE ebpf_memory_reclaim_pages gauge
ebpf_memory_reclaim_pages{type="direct"} 1024
```

#### BanyanDB Native Format
```go
// Direct write to BanyanDB using measure API
client.WriteMeasure(&measurev1.WriteRequest{
    Metadata: &measurev1.Metadata{
        Name: "ebpf_metrics",
        Group: "system",
    },
    DataPoints: dataPoints,
})
```

### Contributing New eBPF Features

When adding new eBPF monitoring capabilities:

1. **Identify the monitoring target**: System call, kernel function, or tracepoint
2. **Check kernel compatibility**: Ensure the probe point exists across target kernels
3. **Design efficient maps**: Use appropriate map types for your data structure
4. **Minimize overhead**: Keep eBPF programs small and fast
5. **Document thoroughly**: Include usage examples and performance impact
6. **Test across architectures**: Ensure x86_64 and ARM64 compatibility
7. **Consider security**: Run with minimal required privileges

### Performance Considerations

- eBPF programs have strict instruction limits (typically 1M instructions)
- Map sizes affect memory usage - size appropriately
- Use per-CPU maps for high-frequency events
- Batch map operations to reduce syscall overhead
- Monitor the monitor - track eBPF program overhead

### Troubleshooting

Common issues and solutions:

1. **Permission denied**: Ensure CAP_BPF and CAP_PERFMON capabilities
2. **Verifier rejection**: Simplify eBPF program logic, check loop bounds
3. **Missing kernel headers**: Install linux-headers package for your kernel
4. **Architecture mismatch**: Regenerate bindings for target architecture

### Development Workflow

1. **Create the sidecar directory structure**:
```bash
mkdir -p /ebpf-sidecar/{cmd/sidecar,internal/{config,server,collector,ebpf/programs},api/proto,pkg/{metrics,export}}
```

2. **Initialize Go module**:
```bash
cd /ebpf-sidecar
go mod init github.com/apache/skywalking-banyandb/ebpf-sidecar
```

3. **Add to root Makefile**:
```makefile
PROJECTS := banyand bydbctl ui ebpf-sidecar
```

4. **Create sidecar Makefile**:
```makefile
include ../scripts/build/common.mk

.PHONY: generate
generate:
	cd internal/ebpf && go generate ./...

.PHONY: build
build: generate
	$(GO) build -o build/bin/ebpf-sidecar ./cmd/sidecar
```

### Future Roadmap

Planned enhancements for the eBPF sidecar:
- Automatic probe discovery based on kernel version
- Dynamic configuration without restart
- Plugin system for custom eBPF programs
- Web UI for real-time metrics visualization
- Integration with BanyanDB's query engine for historical analysis
- ML-based anomaly detection from kernel metrics
- Custom probe SDK for application-specific monitoring