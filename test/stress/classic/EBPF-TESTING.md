# eBPF Sidecar Testing Guide

This guide explains how to test the eBPF sidecar with the classic stress test environment.

## Architecture

The eBPF sidecar runs **inside the same container** as BanyanDB:

```
┌─────────────────────────────────────┐
│   BanyanDB Container                │
│                                     │
│   ┌──────────────┐                 │
│   │   BanyanDB   │ ← monitored     │
│   │   (PID 1)    │                 │
│   └──────────────┘                 │
│          ↑                          │
│          │ attach                  │
│          │                          │
│   ┌──────────────┐                 │
│   │ eBPF Sidecar │                 │
│   │   (PID 2)    │                 │
│   └──────────────┘                 │
│                                     │
└─────────────────────────────────────┘
```

## Port Configuration

To avoid conflicts with other services:

| Service | Port | Purpose |
|---------|------|---------|
| BanyanDB | 17912 | gRPC API |
| BanyanDB | 17913 | HTTP API |
| UI | 8080 | Web Interface |
| Prometheus | 9090 | Metrics |
| **eBPF Sidecar** | **18080** | **HTTP Metrics** |
| **eBPF Sidecar** | **19090** | **gRPC Export** |

## Quick Start

### 1. Build eBPF Sidecar

```bash
cd ../../ebpf-sidecar
make build
cd -
```

This creates `ebpf-sidecar/build/bin/ebpf-sidecar` which will be mounted into the container.

### 2. Build BanyanDB

```bash
make build-server
```

### 3. Start the Environment

```bash
make dev-up
```

This will:
- Start BanyanDB
- Automatically start eBPF sidecar alongside it
- eBPF sidecar will attach to BanyanDB process and monitor I/O

### 4. Verify eBPF Attachment

```bash
./verify-ebpf.sh
```

Expected output:
```
=== Verifying eBPF Sidecar Attachment ===

Found BanyanDB container: classic-banyandb-1

1. Checking if BanyanDB container is running...
✓ BanyanDB container is running

2. Checking if eBPF sidecar binary is present...
✓ eBPF sidecar binary is present in container

3. Checking container logs for eBPF activity...
✓ eBPF sidecar appears to be running

4. Checking if eBPF sidecar process is running...
✓ eBPF sidecar process is running

5. Checking output directory...
✓ Output directory exists

6. Checking metrics endpoint...
✓ Metrics endpoint is accessible at http://localhost:18080/metrics
```

## Monitoring I/O Operations

### View Live Logs

```bash
# All logs (BanyanDB + eBPF)
docker logs -f classic-banyandb-1

# Only eBPF logs
docker logs -f classic-banyandb-1 2>&1 | grep '\[eBPF\]'
```

### Check Metrics

```bash
# All metrics
curl http://localhost:18080/metrics

# Only I/O metrics
curl http://localhost:18080/metrics | grep ebpf_io

# Specific metrics
curl http://localhost:18080/metrics | grep -E "(ebpf_io_read|ebpf_io_write)"
```

### Check Processes Inside Container

```bash
docker exec classic-banyandb-1 ps aux
```

Expected output:
```
USER       PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND
root         1  0.5  0.1 123456  7890 ?        Ssl  01:00   0:05 /banyand standalone
root         2  0.2  0.1  98765  5432 ?        Sl   01:00   0:02 /usr/local/bin/ebpf-sidecar ...
```

## Troubleshooting

### eBPF Sidecar Not Starting

1. **Check if binary exists:**
   ```bash
   ls -lh ../../ebpf-sidecar/build/bin/ebpf-sidecar
   ```
   If not found, run: `cd ../../ebpf-sidecar && make build`

2. **Check container logs:**
   ```bash
   docker logs classic-banyandb-1
   ```
   Look for error messages from eBPF sidecar.

3. **Check if eBPF is supported:**
   ```bash
   docker exec classic-banyandb-1 ls -l /sys/kernel/debug
   ```
   Should show debugfs mounted.

### Port Conflicts

If you see port binding errors:

1. Check what's using the ports:
   ```bash
   sudo lsof -i :18080
   sudo lsof -i :19090
   ```

2. Stop conflicting services or change ports in `docker-compose.yaml`:
   ```yaml
   environment:
     HTTP_PORT: "28080"  # Change to different port
     GRPC_PORT: "29090"
   ports:
     - 28080:28080  # Update port mapping
     - 29090:29090
   ```

### No I/O Metrics

If metrics endpoint is accessible but shows no I/O data:

1. **Generate some I/O activity:**
   The stress test should generate traffic automatically, but you can also:
   ```bash
   # Trigger some writes
   docker exec classic-banyandb-1 /bydbctl health --addr=http://localhost:17913
   ```

2. **Check if eBPF programs are loaded:**
   ```bash
   docker exec classic-banyandb-1 cat /proc/$(pidof banyand)/maps | grep bpf
   ```

3. **Check eBPF logs for errors:**
   ```bash
   docker logs classic-banyandb-1 2>&1 | grep -i error
   ```

### Permission Denied

If you see permission errors:

1. **Check if container has required capabilities:**
   ```bash
   docker inspect classic-banyandb-1 | grep -A 20 CapAdd
   ```
   Should include: SYS_ADMIN, SYS_RESOURCE, NET_ADMIN, PERFMON, BPF

2. **Verify privileged mode:**
   ```bash
   docker inspect classic-banyandb-1 | grep Privileged
   ```
   Should show: `"Privileged": true`

## Advanced Configuration

### Change Export Mode

Edit `docker-compose.yaml`:

```yaml
environment:
  EXPORT_MODE: "file"  # or "grpc", "prometheus"
  OUTPUT_DIR: "/ebpf-output"
```

### Adjust Collection Interval

```yaml
environment:
  EXPORT_INTERVAL: "5s"  # Collect every 5 seconds
```

### Change Log Level

```yaml
environment:
  LOG_LEVEL: "debug"  # or "info", "warn", "error"
```

## Cleanup

```bash
# Stop all containers
make down

# Clean up output directory
rm -rf /tmp/ebpf-sidecar-classic/
rm -rf /tmp/banyandb-stress-agent/
```

## What to Look For

When eBPF sidecar is working correctly, you should see:

1. **In logs:**
   - `[eBPF] Starting eBPF sidecar...`
   - `[eBPF] Attached to process PID: <pid>`
   - `[eBPF] eBPF programs loaded successfully`
   - `[eBPF] Monitoring I/O operations...`

2. **In metrics:**
   - `ebpf_io_read_bytes_total` - Total bytes read
   - `ebpf_io_write_bytes_total` - Total bytes written
   - `ebpf_io_read_ops_total` - Total read operations
   - `ebpf_io_write_ops_total` - Total write operations

3. **In processes:**
   - Both `banyand` and `ebpf-sidecar` processes running
   - eBPF sidecar should have a different PID than BanyanDB

## Next Steps

Once you verify eBPF sidecar is working:

1. Run the full stress test to generate load
2. Monitor I/O metrics over time
3. Compare with BanyanDB's internal metrics
4. Analyze I/O patterns and performance
