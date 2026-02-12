# FODC KTM Integration Test (Kind)

This test boots a Kind cluster, runs BanyanDB with a FODC sidecar that has KTM enabled, and verifies KTM metrics on the FODC `/metrics` endpoint.

## Prerequisites
- `kind`, `kubectl`, and `docker` installed.
- eBPF build dependencies for the FODC agent: `clang`, `llvm-strip`, `bpftool`, and a readable `/sys/kernel/btf/vmlinux` on the host.

## Steps
1. Create the Kind cluster.

```bash
kind create cluster --name banyand-ktm --config kind.yaml
```

2. Build images.

```bash
make -C banyand release
make -C banyand docker
make -C fodc/agent release
make -C fodc/agent docker
```

3. Load images into Kind.

```bash
kind load docker-image apache/skywalking-banyandb:latest --name banyand-ktm
kind load docker-image apache/skywalking-banyandb-fodc-agent:latest --name banyand-ktm
```

4. Deploy the test pod.

```bash
kubectl apply -f pod.yaml
```

If your CI runner uses an older kernel and KTM fails to load eBPF programs, use the fallback manifest that includes `CAP_SYS_ADMIN`:

```bash
kubectl apply -f pod-sysadmin.yaml
```

5. Start the write-load generator (background) to produce I/O on BanyanDB so KTM metrics become non-zero.

```bash
./write-load.sh &
```

6. Verify KTM metrics from the FODC agent.

```bash
./check.sh
```

The check script waits until `ktm_status == 2`, all required metric names are present, **and** at least one histogram sum (`ktm_sys_read_latency_seconds_sum` or `ktm_sys_pread_latency_seconds_sum`) is > 0, proving that eBPF tracepoints are actually measuring I/O latency.

If you want a CI fallback from minimal permissions to the sysadmin manifest, you can use the following pattern:

```bash
set -euo pipefail

kubectl apply -f pod.yaml
./write-load.sh &
LOAD_PID=$!
set +e
./check.sh
status=$?
set -e

if [ "$status" -eq 2 ]; then
  kill "$LOAD_PID" 2>/dev/null || true
  kubectl delete -f pod.yaml
  kubectl apply -f pod-sysadmin.yaml
  ./write-load.sh &
  LOAD_PID=$!
  ./check.sh
else
  kill "$LOAD_PID" 2>/dev/null || true
  exit "$status"
fi
kill "$LOAD_PID" 2>/dev/null || true
```

## Cleanup
```bash
kubectl delete -f pod.yaml
kubectl delete -f pod-sysadmin.yaml
kind delete cluster --name banyand-ktm
```

## Troubleshooting
- If `ktm_status` is `0`, the KTM module did not start. Check that the FODC container has `CAP_BPF`, `CAP_PERFMON` (or `CAP_SYS_ADMIN` on older kernels),
  and `CAP_SYS_RESOURCE`, and that the host kernel exposes `/sys/kernel/btf/vmlinux`.
- If KTM fails to attach tracepoints, ensure tracefs is mounted into the container. The pod manifests mount `/sys/kernel/tracing`, `/sys/kernel/debug`, and `/sys/fs/cgroup` from the host.
- Inspect logs for startup errors.

```bash
kubectl logs banyand-fodc-ktm -c fodc-agent
```
