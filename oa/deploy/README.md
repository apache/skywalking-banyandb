# eBPF Sidecar Deployment

This directory contains deployment configurations and guides for running the eBPF Sidecar with Apache SkyWalking BanyanDB.

## 📁 Directory Structure

```
deploy/
├── README.md                    # This file
├── QUICKSTART.md               # 5-minute quick start guide
├── DEPLOYMENT.md               # Comprehensive deployment guide
├── Makefile                    # Deployment automation
└── kubernetes/
    ├── banyandb-with-sidecar.yaml      # Sidecar pattern (recommended)
    └── ebpf-sidecar-daemonset.yaml     # DaemonSet pattern (node-level)
```

## 🚀 Quick Start

Choose your deployment method:

### Local Development (Docker Compose)
```bash
cd ../
docker-compose up -d
```
See [QUICKSTART.md](QUICKSTART.md#-option-1-docker-compose-easiest---local-development) for details.

### Kubernetes (Production)
```bash
make deploy-sidecar
```
See [QUICKSTART.md](QUICKSTART.md#%EF%B8%8F-option-2-kubernetes-production-ready) for details.

## 📖 Documentation

- **[QUICKSTART.md](QUICKSTART.md)** - Get started in 5 minutes
- **[DEPLOYMENT.md](DEPLOYMENT.md)** - Comprehensive deployment guide covering:
  - Deployment strategies (Sidecar vs DaemonSet vs Standalone)
  - Kubernetes deployment options
  - Docker Compose setup
  - Helm Chart integration
  - Security considerations
  - Troubleshooting

## 🎯 Deployment Patterns

### 1. Sidecar Pattern (Recommended)
Deploy eBPF sidecar alongside BanyanDB in the same Pod.

**Use case:** Production environments, per-instance monitoring

**Deploy:**
```bash
kubectl apply -f kubernetes/banyandb-with-sidecar.yaml
# or
make deploy-sidecar
```

**Features:**
- Shared PID namespace for accurate monitoring
- Tight lifecycle coupling
- Easy resource management
- Automatic scaling

### 2. DaemonSet Pattern
Deploy one eBPF sidecar per node.

**Use case:** Large clusters, node-level monitoring

**Deploy:**
```bash
kubectl apply -f kubernetes/ebpf-sidecar-daemonset.yaml
# or
make deploy-daemonset
```

**Features:**
- Node-level monitoring
- Lower per-node overhead
- Can monitor multiple BanyanDB instances

### 3. Docker Compose
Local development and testing.

**Deploy:**
```bash
cd ../
docker-compose up -d
# or
make deploy-local
```

**Includes:**
- BanyanDB
- eBPF Sidecar
- Prometheus
- Grafana

## 🛠️ Makefile Targets

```bash
# Deployment
make deploy-sidecar          # Deploy with sidecar pattern
make deploy-daemonset        # Deploy as DaemonSet
make deploy-local            # Start Docker Compose

# Management
make check-deployment        # Check deployment status
make logs-sidecar           # View sidecar logs
make port-forward-metrics   # Port forward metrics endpoint

# Testing
make test-metrics           # Test metrics endpoint

# Cleanup
make undeploy-sidecar       # Remove sidecar deployment
make undeploy-daemonset     # Remove DaemonSet
make clean                  # Remove all deployments

# Debugging
make debug-shell            # Open shell in sidecar
make debug-kernel           # Check kernel version
```

Run `make help` for full list of targets.

## 🔒 Security Requirements

The eBPF sidecar requires:

- **Privileged mode** or specific Linux capabilities:
  - `CAP_BPF` (kernel 5.8+)
  - `CAP_PERFMON` (kernel 5.8+)
  - `CAP_SYS_ADMIN`
  
- **Host access:**
  - `/sys` (read-only)
  - `/proc` (read-only)
  - `/lib/modules` (read-only)

- **Kernel requirements:**
  - Linux kernel 4.14+
  - eBPF support enabled

See [DEPLOYMENT.md#security-considerations](DEPLOYMENT.md#security-considerations) for details.

## 📊 Metrics

The sidecar exposes metrics at `/metrics` endpoint:

### Key Metrics

```promql
# Cache performance
ebpf_cache_misses_total
ebpf_cache_read_attempts_total
ebpf_page_cache_adds_total

# Fadvise operations
ebpf_fadvise_calls_total
ebpf_fadvise_success_total
ebpf_fadvise_advice_total{advice="..."}

# Memory reclaim
ebpf_memory_lru_pages_scanned_total
ebpf_memory_lru_pages_reclaimed_total
ebpf_memory_direct_reclaim_processes
```

### Example Queries

```promql
# Cache miss rate
rate(ebpf_cache_misses_total[5m]) / rate(ebpf_cache_read_attempts_total[5m])

# Fadvise success rate
rate(ebpf_fadvise_success_total[5m]) / rate(ebpf_fadvise_calls_total[5m])
```

## 🔍 Troubleshooting

### Quick Checks

```bash
# Check if sidecar is running
kubectl get pods -n skywalking -l app=banyandb

# View sidecar logs
kubectl logs -n skywalking banyandb-0 -c ebpf-sidecar

# Test metrics endpoint
kubectl port-forward -n skywalking banyandb-0 18080:18080
curl http://localhost:18080/metrics | grep ebpf_
```

### Common Issues

| Issue | Solution |
|-------|----------|
| Permission denied | Ensure `privileged: true` or proper capabilities |
| No metrics | Generate I/O activity, check `shareProcessNamespace` |
| eBPF load failure | Check kernel version (must be 4.14+) |
| High memory | Adjust cleanup strategy, increase limits |

See [DEPLOYMENT.md#troubleshooting](DEPLOYMENT.md#troubleshooting) for detailed troubleshooting.

## 🎓 Examples

### Kubernetes Sidecar with Prometheus

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: banyandb
spec:
  template:
    spec:
      shareProcessNamespace: true  # Important!
      containers:
      - name: banyandb
        image: apache/skywalking-banyandb:latest
      - name: ebpf-sidecar
        image: banyandb/ebpf-sidecar:latest
        securityContext:
          privileged: true
```

### Docker Compose

```yaml
services:
  banyandb:
    image: apache/skywalking-banyandb:latest
  
  ebpf-sidecar:
    image: banyandb/ebpf-sidecar:latest
    privileged: true
    pid: host
    volumes:
      - /sys:/sys:ro
      - /proc:/proc:ro
```

## 🔗 Related Resources

- [eBPF Sidecar README](../README.md)
- [Configuration Guide](../configs/README.md)
- [BanyanDB Documentation](https://skywalking.apache.org/docs/skywalking-banyandb/latest/readme/)
- [BanyanDB Helm Chart](https://github.com/apache/skywalking-banyandb-helm)

## 📝 Notes

### Sidecar vs DaemonSet

**Choose Sidecar when:**
- You want per-instance metrics
- BanyanDB instances are isolated
- You need tight lifecycle coupling

**Choose DaemonSet when:**
- You want node-level metrics
- Multiple BanyanDB instances per node
- You want to minimize overhead

### Production Recommendations

1. **Use Sidecar pattern** for production deployments
2. **Enable Prometheus scraping** with ServiceMonitor
3. **Set resource limits** to prevent resource exhaustion
4. **Configure alerts** for high cache miss rates
5. **Use specific capabilities** instead of privileged mode (when possible)
6. **Test in staging** before production deployment

## 🤝 Contributing

To add new deployment configurations:

1. Add YAML files to `kubernetes/` directory
2. Update this README
3. Add Makefile targets if needed
4. Update DEPLOYMENT.md with usage instructions

## 📄 License

Apache License 2.0
