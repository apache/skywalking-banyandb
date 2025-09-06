# Apache SkyWalking BanyanDB eBPF Sidecar Agent

## Overview

The eBPF Sidecar Agent is a standalone observability service that provides kernel-level monitoring for Apache SkyWalking BanyanDB. It leverages eBPF (extended Berkeley Packet Filter) technology to collect system metrics with minimal overhead, offering deep insights into I/O patterns, memory management, and system call behavior.

## Features

### Core Capabilities
- **System Call Monitoring**: Track and analyze critical system calls including `fadvise64` for cache management
- **Page Cache Analytics**: Monitor cache hit/miss rates to optimize database performance
- **Memory Pressure Detection**: Track memory reclaim operations and efficiency
- **Multi-Architecture Support**: Compatible with x86_64 and ARM64 architectures
- **Flexible Export Options**: Support for both Prometheus scraping and native BanyanDB push

### Metrics Provided

| Metric | Description | Type |
|--------|-------------|------|
| `ebpf_fadvise_calls_total` | Total number of fadvise() system calls | Counter |
| `ebpf_fadvise_success_rate_percent` | Success rate of fadvise operations | Gauge |
| `ebpf_cache_hit_rate_percent` | Page cache hit rate | Gauge |
| `ebpf_cache_miss_rate_percent` | Page cache miss rate (critical for performance) | Gauge |
| `ebpf_memory_reclaim_efficiency_percent` | Memory reclaim operation efficiency | Gauge |
| `ebpf_memory_direct_reclaim_processes` | Number of processes in direct memory reclaim | Gauge |

## Architecture

The sidecar operates as an independent service alongside BanyanDB instances:

```
┌─────────────────────────────────────────────┐
│                BanyanDB Pod                  │
│                                              │
│  ┌──────────────┐      ┌──────────────────┐ │
│  │   BanyanDB   │      │  eBPF Sidecar    │ │
│  │    Server    │      │                  │ │
│  │              │      │  ┌────────────┐  │ │
│  │              │      │  │ Collector  │  │ │
│  │              │◄─────┤  ├────────────┤  │ │
│  │              │      │  │ gRPC/HTTP │  │ │
│  └──────────────┘      │  │  Server    │  │ │
│                        │  └─────┬──────┘  │ │
│                        └────────┼──────────┘ │
│                                 │            │
│                          ┌──────▼──────┐     │
│                          │ eBPF Kernel │     │
│                          │  Programs   │     │
│                          └─────────────┘     │
└─────────────────────────────────────────────┘
```

## Requirements

### System Requirements
- Linux kernel 4.14+ with eBPF support
- Root privileges or CAP_BPF and CAP_PERFMON capabilities
- Go 1.24+ (for building from source)

### Build Requirements
- clang and LLVM (for compiling eBPF programs)
- bpftool (for generating vmlinux.h)
- Linux kernel headers

## Installation

### Building from Source

```bash
# Clone the repository
git clone https://github.com/apache/skywalking-banyandb.git
cd skywalking-banyandb/ebpf-sidecar

# Install dependencies (automatic detection)
make install-deps

# Build the sidecar
make build

# Binary will be available at build/bin/ebpf-sidecar
```

### Docker Images

```bash
# Build Ubuntu-based image
docker build -f Dockerfile -t skywalking-banyandb/ebpf-sidecar:latest .

# Build Alpine-based image (smaller size)
docker build -f Dockerfile.alpine -t skywalking-banyandb/ebpf-sidecar:alpine .
```

## Configuration

The sidecar supports configuration via YAML files or environment variables.

### Basic Configuration

```yaml
# configs/config.yaml
server:
  grpc:
    port: 9090
  http:
    port: 8080
    metrics_path: /metrics

collector:
  interval: 10s
  modules:
    - iomonitor

export:
  type: prometheus  # or "banyandb"
```

### Export Configurations

#### Prometheus Mode (Default)
```yaml
export:
  type: prometheus
```

Metrics available at `http://localhost:8080/metrics`

#### BanyanDB Native Mode
```yaml
export:
  type: banyandb
  banyandb:
    endpoint: localhost:17912
    group: ebpf-metrics
    timeout: 30s
```

### Environment Variables

All configuration options can be set via environment variables with the prefix `EBPF_SIDECAR_`:

```bash
export EBPF_SIDECAR_SERVER_GRPC_PORT=9090
export EBPF_SIDECAR_EXPORT_TYPE=prometheus
```

## Deployment

### Standalone Mode

```bash
# Run with default configuration
sudo ./build/bin/ebpf-sidecar

# Run with custom configuration
sudo ./build/bin/ebpf-sidecar --config configs/config.yaml
```

### Docker Deployment

```bash
docker run -d \
  --name ebpf-sidecar \
  --privileged \
  --pid host \
  -v /sys:/sys:ro \
  -v /proc:/proc:ro \
  -v /lib/modules:/lib/modules:ro \
  -p 8080:8080 \
  -p 9090:9090 \
  skywalking-banyandb/ebpf-sidecar:latest
```

### Docker Compose

A complete stack with BanyanDB, Prometheus, and Grafana is provided:

```bash
cd ebpf-sidecar
docker-compose up -d

# Access services:
# - eBPF Metrics: http://localhost:8080/metrics
# - Prometheus: http://localhost:9092
# - Grafana: http://localhost:3000 (admin/admin)
```

### Kubernetes Deployment

Deploy as a sidecar container alongside BanyanDB:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: banyandb
spec:
  template:
    spec:
      containers:
      - name: banyandb
        image: apache/skywalking-banyandb:latest
        ports:
        - containerPort: 17912
        
      - name: ebpf-sidecar
        image: skywalking-banyandb/ebpf-sidecar:latest
        securityContext:
          privileged: true
          capabilities:
            add:
            - BPF
            - PERFMON
            - SYS_ADMIN
        ports:
        - containerPort: 8080
          name: metrics
        - containerPort: 9090
          name: grpc
        volumeMounts:
        - name: sys
          mountPath: /sys
          readOnly: true
        - name: proc
          mountPath: /proc
          readOnly: true
        - name: modules
          mountPath: /lib/modules
          readOnly: true
        env:
        - name: EBPF_SIDECAR_EXPORT_TYPE
          value: "prometheus"
          
      volumes:
      - name: sys
        hostPath:
          path: /sys
      - name: proc
        hostPath:
          path: /proc
      - name: modules
        hostPath:
          path: /lib/modules
```

## API Reference

### HTTP Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/metrics` | GET | Prometheus-format metrics |
| `/health` | GET | Health check endpoint |
| `/api/v1/stats` | GET | JSON-formatted statistics |

### gRPC Services

The gRPC API is defined in `/api/proto/banyandb/ebpf/v1/`:

```protobuf
service EBPFMetricsService {
  rpc GetMetrics(GetMetricsRequest) returns (GetMetricsResponse);
  rpc StreamMetrics(StreamMetricsRequest) returns (stream MetricData);
  rpc GetIOStats(GetIOStatsRequest) returns (GetIOStatsResponse);
  rpc GetModuleStatus(GetModuleStatusRequest) returns (GetModuleStatusResponse);
  rpc GetHealth(HealthRequest) returns (HealthResponse);
}
```

## Development

### Project Structure

```
ebpf-sidecar/
├── cmd/sidecar/          # Application entry point
├── internal/
│   ├── config/          # Configuration management
│   ├── server/          # gRPC and HTTP servers
│   ├── collector/       # Metrics collection logic
│   ├── ebpf/           # eBPF programs and loaders
│   │   ├── programs/   # eBPF C source code
│   │   └── generated/  # Auto-generated Go bindings
│   ├── metrics/        # Metric definitions
│   └── export/         # Export format implementations
├── configs/            # Configuration examples
├── Dockerfile          # Container images
└── Makefile           # Build automation
```

### Adding New eBPF Programs

1. Add eBPF C code to `internal/ebpf/programs/`
2. Update the Makefile to generate Go bindings
3. Implement collection logic in `internal/collector/`
4. Add metrics to the export formats

### Testing

```bash
# Run unit tests
make test

# Run integration tests (requires root)
sudo make test-integration

# Run specific test suites
cd ../test/integration/ebpf_sidecar
sudo go test -v ./...
```

## Troubleshooting

### Common Issues

#### Permission Denied
- Ensure the process has root privileges or appropriate capabilities (CAP_BPF, CAP_PERFMON)
- For containers, use `--privileged` flag or add specific capabilities

#### eBPF Program Load Failures
- Verify kernel version supports required eBPF features
- Check kernel configuration has CONFIG_BPF enabled
- Ensure kernel headers are installed

#### Missing Dependencies
- Run `make install-deps` to automatically install required tools
- For containers, dependencies are handled in the Dockerfile

#### High Memory Usage
- The default configuration uses "clear-after-read" strategy for Prometheus compatibility
- Adjust collection interval if needed
- Monitor eBPF map sizes via logs

## Performance Considerations

- eBPF programs have minimal overhead (typically < 1% CPU)
- Memory usage scales with the number of monitored processes
- Default configuration is optimized for production use
- Collection interval can be adjusted based on requirements

## Contributing

Please refer to the [Apache SkyWalking Contributing Guide](https://github.com/apache/skywalking/blob/master/CONTRIBUTING.md) for guidelines on contributing to this project.

## License

Apache License 2.0