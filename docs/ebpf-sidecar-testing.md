# eBPF Sidecar Testing Guide

## Overview

The eBPF sidecar requires special testing approaches due to its kernel-level operations. This guide covers both local development testing and CI integration strategies.

## Testing Strategies

### 1. Local Development Testing

For local development, use the stress test environment in `test/stress/ebpf-sidecar/`:

```bash
# Build and start the test environment
cd test/stress/ebpf-sidecar
make up

# Generate I/O load to trigger eBPF events
make generate-load

# View captured metrics
make show-metrics

# Clean up
make down
```

This environment provides:
- Real eBPF program execution (requires Linux host)
- Various I/O patterns to trigger different eBPF hooks
- Metrics collection and validation
- Integration with BanyanDB

### 2. CI Testing Approach

#### Phase 1: Mock Mode Tests (Current)
The current CI uses placeholder tests that don't require eBPF:
- Located in `test/integration/ebpf_sidecar/`
- Tests compile and pass without root privileges
- Actual eBPF tests are skipped

#### Phase 2: API Testing (To Implement)
Add tests for non-eBPF components:
```go
// Test HTTP/gRPC APIs
// Test configuration loading
// Test metrics formatting
// Test export functionality
```

These require implementing `--mock-mode` flag in the sidecar.

#### Phase 3: Privileged CI Tests (Future)
For real eBPF testing in CI:

```yaml
# .github/workflows/ebpf-test.yml
name: eBPF Integration Tests
on: [push, pull_request]

jobs:
  ebpf-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Run eBPF tests in privileged container
        run: |
          docker run --privileged \
            -v $PWD:/workspace \
            -w /workspace \
            ubuntu:22.04 \
            bash -c "
              apt-get update
              apt-get install -y golang make clang llvm libbpf-dev
              cd ebpf-sidecar
              make test-ebpf
            "
```

## Test Categories

### Unit Tests
Located in `ebpf-sidecar/internal/*/`:
- Test individual components
- Mock eBPF operations
- No special privileges required

### Integration Tests
Located in `test/integration/ebpf_sidecar/`:
- Currently contains placeholder tests for CI
- Real eBPF tests would require Linux + root privileges
- Future tests will use build tag: `//go:build integration && linux`

### Stress Tests
Located in `test/stress/ebpf-sidecar/`:
- Extended load testing
- Performance validation
- Real-world scenario simulation

## Running Tests

### Local Testing Commands

```bash
# Unit tests (no root required)
cd ebpf-sidecar
go test ./...

# Integration tests from root directory (requires root for real tests)
cd /path/to/skywalking-banyandb
sudo go test -tags=integration,linux ./test/integration/ebpf_sidecar/

# Or using ginkgo
bin/ginkgo ./test/integration/ebpf_sidecar/

# Stress tests
cd test/stress/ebpf-sidecar
make test-stress
```

### CI Testing Commands

```bash
# Current CI (with placeholders)
ginkgo ./test/integration/ebpf_sidecar

# Future CI with mock mode
./ebpf-sidecar/build/bin/ebpf-sidecar --mock-mode &
go test ./test/integration/ebpf_sidecar/

# Future CI with privileged container
docker run --privileged ... make test-ebpf
```

## Verification Methods

### 1. Direct Metric Validation
```bash
# Check if fadvise calls are captured
cat /tmp/ebpf-sidecar-test/metrics.json | jq '.fadvise_calls'

# Verify cache metrics
cat /tmp/ebpf-sidecar-test/metrics.json | jq '.cache_stats'
```

### 2. BanyanDB Query
```bash
# Query stored metrics
bydbctl measure query \
  --name ebpf_metrics \
  --group system \
  --begin -10m
```

### 3. Prometheus Metrics
```bash
# Check Prometheus format
curl http://localhost:8080/metrics | grep ebpf_
```

## Troubleshooting

### Common Issues

1. **"Operation not permitted"**
   - Solution: Run with `sudo` or in privileged container
   
2. **"Cannot find BTF"**
   - Solution: Install kernel headers: `apt install linux-headers-$(uname -r)`
   
3. **"No metrics captured"**
   - Check kernel version: `uname -r` (needs 4.15+)
   - Verify eBPF support: `ls /sys/kernel/btf/`
   - Check logs: `docker logs ebpf-sidecar`

### Debug Commands

```bash
# Check if eBPF programs are loaded
sudo bpftool prog list

# View eBPF maps
sudo bpftool map list

# Monitor eBPF events
sudo cat /sys/kernel/debug/tracing/trace_pipe

# Check sidecar logs
docker logs -f ebpf-sidecar
```

## Future Improvements

1. **Mock Mode Implementation**
   - Add `--mock-mode` flag to bypass eBPF initialization
   - Generate synthetic metrics for testing
   - Enable API testing without privileges

2. **GitHub Actions Integration**
   - Use Ubuntu runners with privileged containers
   - Matrix testing across kernel versions
   - Automated performance regression testing

3. **Test Coverage**
   - Add more eBPF hook points
   - Test error scenarios
   - Benchmark performance impact

4. **Cloud Upload Integration**
   - Automatically upload test results to GCS/S3
   - Generate performance trend reports
   - Compare metrics across versions