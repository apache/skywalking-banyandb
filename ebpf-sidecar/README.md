# eBPF Sidecar Agent

The eBPF Sidecar Agent provides kernel-level observability for BanyanDB operations, offering insights into system calls, memory management, and I/O patterns.

## Current Status

### ✅ Completed Features
- **Core Architecture**: Standalone sidecar service with modular design
- **eBPF Programs**: Comprehensive I/O monitoring (`iomonitor.c`)
  - fadvise system call tracking (TID-based to avoid race conditions)
  - Page cache hit/miss rate monitoring
  - Memory reclaim and LRU statistics
- **Metrics Collection**: Unified `iomonitor_module.go` with memory management
- **Prometheus Export**: Full implementation with proper formatting
- **BanyanDB Native Export**: Direct metrics push to BanyanDB with batching
- **gRPC API**: Full service implementation with protobuf definitions in `/api/proto/banyandb/ebpf/v1/`
- **Docker Support**: Multi-stage Dockerfiles (Ubuntu and Alpine based)
- **Docker Compose**: Complete stack for local testing with BanyanDB, Prometheus, and Grafana
- **Memory Management**: Clear-after-read strategy for production use
- **Integration Tests**: Located in `/test/integration/ebpf_sidecar/`
- **Build System**: Makefile with automatic dependency installation

### 📊 Available Metrics
- `ebpf_fadvise_calls_total` - Total fadvise() system calls
- `ebpf_fadvise_success_rate_percent` - Success rate of fadvise calls
- `ebpf_cache_miss_rate_percent` - Page cache miss rate (key metric)
- `ebpf_cache_hit_rate_percent` - Page cache hit rate
- `ebpf_memory_reclaim_efficiency_percent` - Memory reclaim efficiency
- `ebpf_memory_direct_reclaim_processes` - Processes in direct reclaim

## Architecture

```
┌─────────────────────────────────────────┐
│            User Space                    │
│  ┌─────────────────────────────────┐    │
│  │     eBPF Sidecar Agent          │    │
│  │  ┌──────────┐  ┌──────────┐    │    │
│  │  │Collector │  │ Server   │    │    │
│  │  │          │  │ (gRPC/   │    │    │
│  │  │          │  │  HTTP)   │    │    │
│  │  └────┬─────┘  └──────────┘    │    │
│  └───────┼─────────────────────────┘    │
└──────────┼───────────────────────────────┘
           │
┌──────────┼───────────────────────────────┐
│          ▼        Kernel Space           │
│  ┌─────────────────────────────────┐    │
│  │      eBPF Programs              │    │
│  │  ┌──────────┐  ┌──────────┐    │    │
│  │  │ fadvise  │  │ memory   │    │    │
│  │  │ monitor  │  │ monitor  │    │    │
│  │  └──────────┘  └──────────┘    │    │
│  └─────────────────────────────────┘    │
└──────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Go 1.24+
- Linux kernel 4.14+ with eBPF support
- Root privileges (for eBPF program loading)
- clang and llvm (for compiling eBPF programs)

### Building

```bash
# Build the sidecar
cd ebpf-sidecar
go build ./cmd/sidecar

# Or from root directory
cd /path/to/skywalking-banyandb
go build ./ebpf-sidecar/cmd/sidecar

# Run integration tests (requires root)
sudo go test -tags=integration -v ./test/integration/ebpf_sidecar/

# Run specific test
sudo go test -tags=integration -run TestIOMonitorIntegration -v ./test/integration/ebpf_sidecar/
```

### Running

```bash
# Run with default configuration
sudo ./sidecar

# Run with custom configuration
sudo ./sidecar --config configs/config.yaml

# Check version
./sidecar version

# Check metrics endpoint (while running)
curl http://localhost:8080/metrics
curl http://localhost:8080/health
curl http://localhost:8080/api/v1/stats
```

## Configuration

The sidecar can be configured via YAML file or environment variables.

### Export Options

The sidecar supports two export modes:

1. **Prometheus Export** (default): Metrics are exposed via HTTP endpoint for scraping
2. **BanyanDB Native Export**: Metrics are pushed directly to BanyanDB

#### Prometheus Mode:
```yaml
export:
  type: prometheus
```

#### BanyanDB Mode:
```yaml
export:
  type: banyandb
  banyandb:
    endpoint: localhost:17912
    group: ebpf-metrics
    timeout: 30s
```

### Full Configuration Example:

```yaml
server:
  grpc:
    port: 9090
  http:
    port: 8080
    metrics_path: /metrics

collector:
  interval: 10s
  modules:
    - iomonitor  # Unified module for all I/O monitoring

export:
  type: banyandb  # Switch to "prometheus" for Prometheus scraping
  banyandb:
    endpoint: localhost:17912
    group: ebpf-metrics
    timeout: 30s
```

Environment variables use the prefix `EBPF_SIDECAR_` (e.g., `EBPF_SIDECAR_SERVER_GRPC_PORT=9090`).

## API Endpoints

### HTTP Endpoints

- `GET /metrics` - Prometheus format metrics
- `GET /health` - Health check
- `GET /api/v1/stats` - JSON statistics

### gRPC Services

The gRPC API is defined in `/api/proto/banyandb/ebpf/v1/` and provides:

- `EBPFMetricsService.GetMetrics` - Get current metrics from all or specific modules
- `EBPFMetricsService.StreamMetrics` - Stream real-time metrics updates
- `EBPFMetricsService.GetIOStats` - Get detailed I/O statistics
- `EBPFMetricsService.GetModuleStatus` - Check status of eBPF modules
- `EBPFMetricsService.ConfigureModule` - Enable/disable modules (coming soon)
- `EBPFMetricsService.GetHealth` - Health check

## Deployment

### Docker

```bash
# Build Docker image (Ubuntu-based)
docker build -f ebpf-sidecar/Dockerfile -t banyandb/ebpf-sidecar:latest .

# Build lightweight Alpine image
docker build -f ebpf-sidecar/Dockerfile.alpine -t banyandb/ebpf-sidecar:alpine .

# Run container with Prometheus export
docker run -d \
  --name ebpf-sidecar \
  --privileged \
  --pid host \
  -v /sys:/sys:ro \
  -v /proc:/proc:ro \
  -v /lib/modules:/lib/modules:ro \
  -p 8080:8080 \
  -p 9090:9090 \
  banyandb/ebpf-sidecar:latest

# Run with BanyanDB export
docker run -d \
  --name ebpf-sidecar \
  --privileged \
  --pid host \
  -e EBPF_SIDECAR_EXPORT_TYPE=banyandb \
  -e EBPF_SIDECAR_EXPORT_BANYANDB_ENDPOINT=banyandb:17912 \
  -p 8080:8080 \
  -p 9090:9090 \
  banyandb/ebpf-sidecar:latest
```

### Docker Compose

```bash
# Start complete stack (BanyanDB + eBPF Sidecar + Prometheus + Grafana)
cd ebpf-sidecar
docker-compose up -d

# View logs
docker-compose logs -f ebpf-sidecar-prometheus
docker-compose logs -f ebpf-sidecar-banyandb

# Access services
# - eBPF Metrics (Prometheus): http://localhost:8080/metrics
# - eBPF Metrics (BanyanDB): http://localhost:8081/metrics
# - Prometheus UI: http://localhost:9092
# - Grafana: http://localhost:3000 (admin/admin)
# - BanyanDB gRPC: localhost:17912

# Stop stack
docker-compose down
```

### Kubernetes

Deploy as a sidecar container with BanyanDB:

```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: banyandb
    image: apache/skywalking-banyandb:latest
    
  - name: ebpf-sidecar
    image: skywalking-banyandb/ebpf-sidecar:latest
    securityContext:
      privileged: true
    volumeMounts:
    - name: sys
      mountPath: /sys
    - name: modules
      mountPath: /lib/modules
      readOnly: true
      
  volumes:
  - name: sys
    hostPath:
      path: /sys
  - name: modules
    hostPath:
      path: /lib/modules
```

## Development

### Adding New eBPF Programs

1. Create eBPF C program in `internal/ebpf/programs/`
2. Add Go bindings generation in `internal/ebpf/loader.go`
3. Implement collector module in `internal/collector/modules/`
4. Register module in collector configuration

### Project Structure

```
ebpf-sidecar/
├── cmd/sidecar/          # Main entry point
├── internal/
│   ├── config/          # Configuration management
│   ├── server/          # gRPC/HTTP servers  
│   ├── collector/       # Metrics collection
│   │   ├── collector.go
│   │   └── iomonitor_module.go  # Unified I/O monitoring
│   ├── ebpf/           # eBPF programs and loaders
│   │   ├── programs/   # C source files
│   │   │   └── iomonitor.c
│   │   ├── generated/  # Auto-generated Go bindings
│   │   └── loader.go   # Program lifecycle management
│   ├── metrics/        # Metric types and storage
│   └── export/         # Export formats
│       └── prometheus.go
├── configs/            # Configuration files
└── test/integration/ebpf_sidecar/  # Integration tests
```

## Troubleshooting

### Common Issues

1. **Permission Denied**
   - Ensure running with root privileges or appropriate capabilities (CAP_BPF, CAP_PERFMON)

2. **eBPF Verifier Errors**
   - Check kernel version compatibility
   - Simplify eBPF program logic
   - Verify loop bounds

3. **Missing Kernel Headers**
   - Install `linux-headers-$(uname -r)` package

## License

Apache License 2.0