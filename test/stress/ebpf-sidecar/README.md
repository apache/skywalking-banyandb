# eBPF Sidecar Stress Testing

This directory contains tools for testing the eBPF sidecar's ability to capture kernel-level I/O metrics.

## Prerequisites

- Docker and Docker Compose
- Linux host with eBPF support (kernel 4.15+)
- Root privileges or Docker permissions
- Built eBPF sidecar binary

## Quick Start

### 1. Build the sidecar
```bash
cd ../../../ebpf-sidecar
make build
```

### 2. Run the test environment
```bash
make up
```

This starts:
- BanyanDB instance (port 17912)
- eBPF sidecar (privileged container)
- Load generator container

### 3. Generate test load
```bash
make generate-load
```

This creates various I/O patterns to trigger eBPF events:
- Sequential file reads
- Random access patterns  
- Cache operations
- Memory pressure scenarios

### 4. Check results
```bash
# View captured metrics
make show-metrics

# Query BanyanDB for stored data
make query-db

# Export metrics to file
make export-metrics
```

### 5. Clean up
```bash
make down
make clean
```

## Test Scenarios

### Basic I/O Test
Tests basic file operations and fadvise calls:
```bash
make test-basic
```

### Cache Miss Test
Forces cache misses and monitors cache behavior:
```bash
make test-cache
```

### Memory Pressure Test
Creates memory pressure to trigger reclaim events:
```bash
make test-memory
```

### Full Stress Test
Runs all scenarios with high concurrency:
```bash
make test-stress
```

## Configuration

Edit `env` file to configure:
- `BYDB_ENDPOINT`: BanyanDB endpoint
- `EXPORT_MODE`: `banyandb`, `file`, or `prometheus`
- `COLLECTION_INTERVAL`: Metrics collection interval
- `LOAD_DURATION`: How long to generate load
- `LOAD_CONCURRENCY`: Number of concurrent operations

## Output Files

- `/tmp/ebpf-sidecar-test/metrics.json`: Collected metrics
- `/tmp/ebpf-sidecar-test/events.log`: Raw eBPF events
- `/tmp/ebpf-sidecar-test/summary.txt`: Test summary

## Uploading Results

To upload test results to cloud storage:

```bash
# Google Cloud Storage
gsutil cp /tmp/ebpf-sidecar-test/metrics.json gs://your-bucket/ebpf-tests/

# AWS S3
aws s3 cp /tmp/ebpf-sidecar-test/metrics.json s3://your-bucket/ebpf-tests/

# Azure Blob Storage
az storage blob upload --file /tmp/ebpf-sidecar-test/metrics.json \
  --container-name ebpf-tests --name metrics.json
```

## Troubleshooting

### Permission Denied
Ensure Docker has privileged mode enabled and you're running on a Linux host.

### No eBPF Events Captured
- Check kernel version: `uname -r` (needs 4.15+)
- Verify BTF support: `ls /sys/kernel/btf/vmlinux`
- Check sidecar logs: `docker logs ebpf-sidecar`

### BanyanDB Connection Failed
- Verify BanyanDB is running: `docker ps | grep banyandb`
- Check network connectivity: `curl http://localhost:17912/api/healthz`

## Development

To add new test scenarios:
1. Add script to `scripts/` directory
2. Update `docker-compose.yaml` if needed
3. Add Make target in `Makefile`
4. Document the scenario here