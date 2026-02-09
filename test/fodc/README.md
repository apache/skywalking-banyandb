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

5. Verify KTM metrics from the FODC agent.

```bash
./check.sh
```

## Cleanup
```bash
kubectl delete -f pod.yaml
kind delete cluster --name banyand-ktm
```

## Troubleshooting
- If `ktm_status` is `0`, the KTM module did not start. Check that the FODC container is privileged and the host kernel exposes `/sys/kernel/btf/vmlinux`.
- Inspect logs for startup errors.

```bash
kubectl logs banyand-fodc-ktm -c fodc-agent
```
