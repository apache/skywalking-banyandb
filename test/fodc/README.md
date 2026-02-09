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
make -C banyand docker
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

5. Verify KTM metrics from the FODC agent.

```bash
./check.sh
```

If you want a CI fallback from minimal permissions to the sysadmin manifest, you can use the following pattern:

```bash
set -euo pipefail

kubectl apply -f pod.yaml
set +e
./check.sh
status=$?
set -e

if [ "$status" -eq 2 ]; then
  kubectl delete -f pod.yaml
  kubectl apply -f pod-sysadmin.yaml
  ./check.sh
else
  exit "$status"
fi
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
- If KTM fails to attach tracepoints, ensure tracefs is visible in the container (for example, `/sys/kernel/tracing`).
- Inspect logs for startup errors.

```bash
kubectl logs banyand-fodc-ktm -c fodc-agent
```
