# eBPF Sidecar Agent

The eBPF Sidecar Agent provides kernel-level observability for BanyanDB operations, offering insights into system calls, memory management, and I/O patterns.

## Features

- **System Call Monitoring**: Track file advisory operations, I/O patterns
- **Memory Management**: Monitor page cache, memory reclamation events
- **Extensible Framework**: Plugin architecture for custom eBPF programs
- **Multiple Export Formats**: Prometheus metrics, BanyanDB native format
- **Low Overhead**: Efficient kernel-space data collection

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
make build

# Run tests
make test

# Run eBPF tests (requires root)
sudo make test-ebpf
```

### Running

```bash
# Run with default configuration
sudo ./build/bin/ebpf-sidecar

# Run with custom configuration
sudo ./build/bin/ebpf-sidecar --config configs/config.yaml

# Check version
./build/bin/ebpf-sidecar version
```

## Configuration

The sidecar can be configured via YAML file or environment variables:

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
    - fadvise
    - memory

export:
  type: prometheus  # or "banyandb"
```

Environment variables use the prefix `EBPF_SIDECAR_` (e.g., `EBPF_SIDECAR_SERVER_GRPC_PORT=9090`).

## API Endpoints

### HTTP Endpoints

- `GET /metrics` - Prometheus format metrics
- `GET /health` - Health check
- `GET /api/v1/stats` - JSON statistics

### gRPC Services

- `EBPFMetrics.GetMetrics` - Get current metrics
- `EBPFMetrics.StreamMetrics` - Stream metrics updates
- `EBPFMetrics.GetHealth` - Health check

## Deployment

### Docker

```bash
# Build Docker image
make docker

# Run container
docker run -d \
  --name ebpf-sidecar \
  --privileged \
  -v /sys:/sys \
  -v /lib/modules:/lib/modules:ro \
  -p 8080:8080 \
  -p 9090:9090 \
  skywalking-banyandb/ebpf-sidecar:latest
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
├── cmd/sidecar/           # Main entry point
├── internal/
│   ├── config/           # Configuration management
│   ├── server/           # gRPC/HTTP servers
│   ├── collector/        # Metrics collection
│   └── ebpf/            # eBPF programs and loaders
├── api/proto/           # API definitions
├── pkg/
│   ├── metrics/         # Metric types
│   └── export/          # Export formats
└── configs/             # Configuration files
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