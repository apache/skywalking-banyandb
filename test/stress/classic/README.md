# Stress Test - Classic

This is a stress test for the classic version of the application. It is designed to test the application's ability to handle a large number of requests.

## Build BanyanD and Bydbctl

Before running the stress test, you need to build the BanyanD and Bydbctl binaries. You can do this by running the following command:

```bash
make build-server
```

## Build eBPF Sidecar (Optional)

If you want to test with eBPF sidecar to monitor I/O operations, build it first:

```bash
cd ../../ebpf-sidecar
make build
cd -
```

The eBPF sidecar will run inside the same container as BanyanDB and monitor its file I/O operations in real-time.

**Port Configuration:**
- BanyanDB uses standard ports: 17912 (gRPC), 17913 (HTTP)
- eBPF sidecar uses: 18080 (HTTP metrics), 19090 (gRPC)
- This avoids conflicts with UI (8080) and Prometheus (9090)

## Running the Stress Test in Development Mode

To run the stress test in development mode, you can use the following command:

```bash
make dev-up
```

The eBPF sidecar will automatically start alongside BanyanDB inside the same container. Check logs with:

```bash
# Find the container name
docker ps | grep banyandb

# View all logs (BanyanDB + eBPF)
docker logs -f classic-banyandb-1

# View only eBPF logs
docker logs -f classic-banyandb-1 2>&1 | grep '\[eBPF\]'
```

## Running the Stress Test in Production Mode

To run the stress test in production mode, you can use the following command:

```bash
make up
```

## Verifying eBPF Sidecar Attachment

Run the verification script to check if eBPF sidecar is properly attached:

```bash
./verify-ebpf.sh
```

Or manually verify:

1. Check if eBPF process is running:
   ```bash
   docker exec classic-banyandb-1 ps aux | grep ebpf-sidecar
   ```

2. Check eBPF logs:
   ```bash
   docker logs classic-banyandb-1 2>&1 | grep '\[eBPF\]'
   ```

3. Check output files (if using file export mode):
   ```bash
   ls -lh /tmp/ebpf-sidecar-classic/
   ```

4. Monitor metrics endpoint:
   ```bash
   curl http://localhost:18080/metrics
   ```

5. Check for I/O monitoring data:
   ```bash
   curl http://localhost:18080/metrics | grep ebpf_io
   ```
