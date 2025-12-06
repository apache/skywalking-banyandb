# eBPF Sidecar Quick Start Guide

Get started with eBPF Sidecar in 5 minutes!

## Choose Your Deployment Method

### 🐳 Option 1: Docker Compose (Easiest - Local Development)

**Prerequisites:** Docker and Docker Compose installed

```bash
# Navigate to ebpf-sidecar directory
cd ebpf-sidecar

# Start all services
docker-compose up -d

# Check status
docker-compose ps

# View metrics
curl http://localhost:8080/metrics | grep ebpf_cache

# Access Grafana
open http://localhost:3000  # admin/admin
```

**What you get:**
- BanyanDB (standalone mode)
- eBPF Sidecar (Prometheus export)
- Prometheus (scraping metrics)
- Grafana (visualization)

---

### ☸️ Option 2: Kubernetes (Production-Ready)

**Prerequisites:** kubectl configured, Kubernetes cluster with eBPF support

#### Quick Deploy (Sidecar Pattern)

```bash
# Navigate to deploy directory
cd ebpf-sidecar/deploy

# Deploy everything
make deploy-sidecar

# Check status
make check-deployment

# Port forward to access metrics
make port-forward-metrics

# In another terminal, test metrics
make test-metrics
```

#### Alternative: DaemonSet Pattern

```bash
# Deploy as DaemonSet (one per node)
make deploy-daemonset

# Check status
kubectl get daemonset -n skywalking
kubectl get pods -n skywalking -l app=ebpf-sidecar
```

---

### 🎯 Option 3: Manual Kubernetes Deployment

```bash
# Create namespace
kubectl create namespace skywalking

# Deploy BanyanDB with eBPF sidecar
kubectl apply -f deploy/kubernetes/banyandb-with-sidecar.yaml

# Wait for pods to be ready
kubectl wait --for=condition=ready pod -l app=banyandb -n skywalking --timeout=300s

# Check pods
kubectl get pods -n skywalking

# Port forward metrics
kubectl port-forward -n skywalking banyandb-0 18080:18080

# View metrics
curl http://localhost:18080/metrics
```

---

## Verify It's Working

### 1. Check eBPF Sidecar is Running

**Docker Compose:**
```bash
docker-compose logs ebpf-sidecar-prometheus | tail -20
```

**Kubernetes:**
```bash
kubectl logs -n skywalking banyandb-0 -c ebpf-sidecar --tail=20
```

Expected output:
```
INFO    Starting I/O monitor module
INFO    I/O monitor module started successfully
INFO    eBPF map sizes  {"fadvise_entries": 0, "cache_entries": 0}
```

### 2. Check Metrics are Being Collected

```bash
# Docker Compose
curl http://localhost:8080/metrics | grep ebpf_cache_misses_total

# Kubernetes (after port-forward)
curl http://localhost:18080/metrics | grep ebpf_cache_misses_total
```

Expected output:
```
# HELP ebpf_cache_misses_total ebpf_cache_misses_total metric
# TYPE ebpf_cache_misses_total counter
ebpf_cache_misses_total 49
```

### 3. Generate Some Load

To see metrics change, generate some I/O activity:

**Docker Compose:**
```bash
# Exec into BanyanDB container
docker exec -it banyandb /bin/sh

# Create some I/O activity
dd if=/dev/zero of=/tmp/test.dat bs=1M count=100
cat /tmp/test.dat > /dev/null
rm /tmp/test.dat
```

**Kubernetes:**
```bash
# Exec into BanyanDB container
kubectl exec -it -n skywalking banyandb-0 -c banyandb -- /bin/sh

# Create some I/O activity
dd if=/dev/zero of=/tmp/test.dat bs=1M count=100
cat /tmp/test.dat > /dev/null
rm /tmp/test.dat
```

Then check metrics again - numbers should increase!

---

## View Metrics in Prometheus

### Docker Compose

1. Open Prometheus: http://localhost:9092
2. Go to "Status" → "Targets" - verify eBPF sidecar is UP
3. Go to "Graph" and try these queries:

```promql
# Cache miss rate
rate(ebpf_cache_misses_total[5m]) / rate(ebpf_cache_read_attempts_total[5m])

# Total cache misses in last hour
increase(ebpf_cache_misses_total[1h])

# Fadvise calls per second
rate(ebpf_fadvise_calls_total[5m])
```

### Kubernetes with Prometheus Operator

If you have Prometheus Operator installed:

```bash
# Create ServiceMonitor
cat <<EOF | kubectl apply -f -
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: ebpf-sidecar
  namespace: skywalking
spec:
  selector:
    matchLabels:
      component: ebpf-sidecar
  endpoints:
  - port: metrics
    interval: 10s
    path: /metrics
EOF
```

---

## View Metrics in Grafana

### Docker Compose

1. Open Grafana: http://localhost:3000
2. Login: admin/admin
3. Go to "Dashboards" → "Browse"
4. Import dashboard from `configs/grafana/dashboards/ebpf-sidecar.json`

### Kubernetes

```bash
# Install Grafana (if not already installed)
helm repo add grafana https://grafana.github.io/helm-charts
helm install grafana grafana/grafana -n skywalking

# Get admin password
kubectl get secret -n skywalking grafana -o jsonpath="{.data.admin-password}" | base64 --decode

# Port forward
kubectl port-forward -n skywalking svc/grafana 3000:80

# Open http://localhost:3000
```

---

## Common Queries

### Cache Performance

```promql
# Cache miss rate (%)
100 * rate(ebpf_cache_misses_total[5m]) / rate(ebpf_cache_read_attempts_total[5m])

# Cache hit rate (%)
100 * (1 - rate(ebpf_cache_misses_total[5m]) / rate(ebpf_cache_read_attempts_total[5m]))

# Pages added to cache per second
rate(ebpf_page_cache_adds_total[5m])
```

### Fadvise Operations

```promql
# Fadvise calls per second
rate(ebpf_fadvise_calls_total[5m])

# Fadvise success rate (%)
100 * rate(ebpf_fadvise_success_total[5m]) / rate(ebpf_fadvise_calls_total[5m])

# Fadvise by advice type
rate(ebpf_fadvise_advice_total[5m])
```

### Memory Reclaim

```promql
# Memory reclaim efficiency (%)
100 * rate(ebpf_memory_lru_pages_reclaimed_total[5m]) / rate(ebpf_memory_lru_pages_scanned_total[5m])

# Processes in direct reclaim
ebpf_memory_direct_reclaim_processes
```

---

## Troubleshooting

### Issue: No metrics showing up

**Check 1:** Is the sidecar running?
```bash
# Docker
docker-compose ps ebpf-sidecar-prometheus

# Kubernetes
kubectl get pods -n skywalking -l app=banyandb
```

**Check 2:** Are eBPF programs loaded?
```bash
# Docker
docker-compose logs ebpf-sidecar-prometheus | grep "eBPF programs"

# Kubernetes
kubectl logs -n skywalking banyandb-0 -c ebpf-sidecar | grep "eBPF programs"
```

**Check 3:** Is there I/O activity?
- Generate some load (see "Generate Some Load" section above)
- Check the debug cumulative counter: `ebpf_cache_misses_cumulative_debug`

### Issue: Permission denied errors

**Docker Compose:**
- Ensure `privileged: true` is set in docker-compose.yml
- Check: `docker inspect ebpf-sidecar-prometheus | grep Privileged`

**Kubernetes:**
- Ensure `securityContext.privileged: true` is set
- Check: `kubectl get pod banyandb-0 -n skywalking -o yaml | grep privileged`

### Issue: Metrics are all zero

This is normal if:
1. BanyanDB hasn't performed any I/O yet
2. The monitored processes haven't started
3. You're using `shareProcessNamespace` but BanyanDB PID isn't visible

**Solution:** Generate some I/O activity (see above)

---

## Next Steps

1. **Configure Alerts:** Set up Prometheus alerts for cache miss rates
2. **Create Dashboards:** Build custom Grafana dashboards
3. **Tune Collection:** Adjust collection interval in config
4. **Production Deploy:** Follow the full [Deployment Guide](DEPLOYMENT.md)
5. **Integrate with Helm:** Add to BanyanDB Helm chart

---

## Quick Reference

### Docker Compose Commands

```bash
# Start
docker-compose up -d

# Stop
docker-compose down

# View logs
docker-compose logs -f ebpf-sidecar-prometheus

# Restart sidecar
docker-compose restart ebpf-sidecar-prometheus

# View metrics
curl http://localhost:8080/metrics
```

### Kubernetes Commands

```bash
# Deploy
make deploy-sidecar

# Check status
make check-deployment

# View logs
make logs-sidecar

# Port forward
make port-forward-metrics

# Undeploy
make undeploy-sidecar
```

### Useful Endpoints

| Service | Docker Compose | Kubernetes (after port-forward) |
|---------|----------------|--------------------------------|
| eBPF Metrics | http://localhost:8080/metrics | http://localhost:18080/metrics |
| eBPF Health | http://localhost:8080/health | http://localhost:18080/health |
| BanyanDB HTTP | http://localhost:17913 | http://localhost:17913 |
| BanyanDB gRPC | localhost:17912 | localhost:17912 |
| Prometheus | http://localhost:9092 | - |
| Grafana | http://localhost:3000 | http://localhost:3000 |

---

## Getting Help

- **Documentation:** [Full Deployment Guide](DEPLOYMENT.md)
- **Issues:** Check [Troubleshooting](DEPLOYMENT.md#troubleshooting) section
- **Community:** [Apache SkyWalking Community](https://skywalking.apache.org/community/)
