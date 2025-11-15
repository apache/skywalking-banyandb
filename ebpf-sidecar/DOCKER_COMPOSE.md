# Docker Compose Deployment Options

This document explains the different Docker Compose configurations for eBPF Sidecar.

## 📦 Available Configurations

### 1. **Minimal Sidecar (Recommended for Testing)**

**File:** `docker-compose.sidecar.yml`

**What it includes:**
- ✅ BanyanDB
- ✅ eBPF Sidecar (true sidecar pattern)

**Use case:** Testing the sidecar functionality, minimal setup

```bash
# Start minimal sidecar
docker-compose -f docker-compose.sidecar.yml up -d

# View logs
docker-compose -f docker-compose.sidecar.yml logs -f

# Check metrics
curl http://localhost:18080/metrics

# Stop
docker-compose -f docker-compose.sidecar.yml down
```

**Key features:**
- Uses `pid: "service:banyandb"` to share PID namespace
- Uses `network_mode: "service:banyandb"` to share network
- Only 2 containers running
- Metrics available at `http://localhost:18080/metrics`

---

### 2. **Full Development Stack (Default)**

**File:** `docker-compose.yml`

**What it includes:**
- ✅ BanyanDB
- ✅ eBPF Sidecar (true sidecar pattern)
- ⚙️ Prometheus (optional, with `--profile monitoring`)
- ⚙️ Grafana (optional, with `--profile monitoring`)

**Use case:** Full development environment with monitoring

```bash
# Start only BanyanDB + eBPF Sidecar
docker-compose up -d

# Start with full monitoring stack
docker-compose --profile monitoring up -d

# View logs
docker-compose logs -f ebpf-sidecar

# Check metrics
curl http://localhost:18080/metrics

# Access Prometheus (if monitoring profile enabled)
open http://localhost:9090

# Access Grafana (if monitoring profile enabled)
open http://localhost:3000  # admin/admin

# Stop
docker-compose down
```

---

## 🔑 Key Differences from Old Setup

### Old Setup (Multiple Independent Containers)
```yaml
ebpf-sidecar-prometheus:
  pid: host              # Monitors ALL host processes
  network_mode: host     # Uses host network
  ports:
    - "8080:8080"
```
❌ Not a true sidecar
❌ Monitors everything on the host
❌ Multiple sidecar containers

### New Setup (True Sidecar Pattern)
```yaml
ebpf-sidecar:
  pid: "service:banyandb"        # Only sees BanyanDB processes
  network_mode: "service:banyandb"  # Shares network with BanyanDB
```
✅ True sidecar pattern
✅ Only monitors BanyanDB
✅ Single sidecar container
✅ Mimics Kubernetes sidecar behavior

---

## 📊 Accessing Metrics

### Minimal Sidecar
```bash
# Metrics endpoint (shared network with BanyanDB)
curl http://localhost:18080/metrics

# Health check
curl http://localhost:18080/health

# BanyanDB API
curl http://localhost:17913/api/healthz
```

### With Monitoring Stack
```bash
# eBPF Metrics
curl http://localhost:18080/metrics

# Prometheus UI
open http://localhost:9090

# Grafana
open http://localhost:3000
```

---

## 🧪 Testing the Sidecar

### 1. Start the Sidecar
```bash
docker-compose -f docker-compose.sidecar.yml up -d
```

### 2. Verify Containers are Running
```bash
docker-compose -f docker-compose.sidecar.yml ps
```

Expected output:
```
NAME            IMAGE                                    STATUS
banyandb        apache/skywalking-banyandb:latest        Up (healthy)
ebpf-sidecar    banyandb/ebpf-sidecar:latest            Up
```

### 3. Check eBPF Sidecar Logs
```bash
docker-compose -f docker-compose.sidecar.yml logs ebpf-sidecar
```

Expected output:
```
INFO    Starting eBPF sidecar
INFO    Loading eBPF programs
INFO    I/O monitor module started successfully
```

### 4. Verify Metrics are Being Collected
```bash
curl http://localhost:18080/metrics | grep ebpf_cache
```

Expected output:
```
ebpf_cache_misses_total 0
ebpf_cache_read_attempts_total 0
ebpf_cache_misses_cumulative_debug 0
```

### 5. Generate Some I/O Activity
```bash
# Exec into BanyanDB container
docker exec -it banyandb /bin/sh

# Create some I/O
dd if=/dev/zero of=/tmp/test.dat bs=1M count=100
cat /tmp/test.dat > /dev/null
rm /tmp/test.dat
exit
```

### 6. Check Metrics Again
```bash
curl http://localhost:18080/metrics | grep ebpf_cache_misses_total
```

You should see non-zero values now!

---

## 🔍 Debugging

### Check if Sidecar Can See BanyanDB Processes

```bash
# Exec into sidecar
docker exec -it ebpf-sidecar /bin/sh

# List processes (should see BanyanDB)
ps aux | grep banyandb

# Check eBPF maps
ls -la /sys/fs/bpf/
```

### Check Shared PID Namespace

```bash
# From host
docker inspect banyandb | grep -A 5 "Pid"
docker inspect ebpf-sidecar | grep -A 5 "Pid"

# They should share the same PID namespace
```

### View Real-time Logs

```bash
# Both containers
docker-compose -f docker-compose.sidecar.yml logs -f

# Only sidecar
docker-compose -f docker-compose.sidecar.yml logs -f ebpf-sidecar

# Only BanyanDB
docker-compose -f docker-compose.sidecar.yml logs -f banyandb
```

---

## 🚀 Quick Commands

```bash
# Minimal sidecar
alias sidecar-up='docker-compose -f docker-compose.sidecar.yml up -d'
alias sidecar-down='docker-compose -f docker-compose.sidecar.yml down'
alias sidecar-logs='docker-compose -f docker-compose.sidecar.yml logs -f'
alias sidecar-metrics='curl http://localhost:18080/metrics'

# Full stack
alias dev-up='docker-compose --profile monitoring up -d'
alias dev-down='docker-compose down'
alias dev-logs='docker-compose logs -f'

# Usage
sidecar-up
sidecar-logs
sidecar-metrics
sidecar-down
```

---

## 📝 Configuration Files

### Prometheus Config (`configs/prometheus.yml`)

Update the scrape config to use the correct endpoint:

```yaml
scrape_configs:
  - job_name: 'ebpf-sidecar'
    static_configs:
      - targets: ['banyandb:18080']  # Note: using banyandb hostname
        labels:
          instance: 'banyandb-sidecar'
```

### eBPF Sidecar Config (`configs/config-prometheus.yaml`)

```yaml
server:
  grpc:
    port: 19090
  http:
    port: 18080
    metrics_path: /metrics

collector:
  interval: 10s
  modules:
    - iomonitor

export:
  type: prometheus
```

---

## 🆚 Comparison Table

| Feature | Minimal Sidecar | Full Stack |
|---------|----------------|------------|
| Containers | 2 | 2-4 |
| BanyanDB | ✅ | ✅ |
| eBPF Sidecar | ✅ | ✅ |
| Prometheus | ❌ | ⚙️ (optional) |
| Grafana | ❌ | ⚙️ (optional) |
| Startup Time | ~10s | ~30s |
| Memory Usage | ~1GB | ~2GB |
| Use Case | Testing | Development |

---

## 🎯 Best Practices

1. **Use minimal sidecar for testing:**
   ```bash
   docker-compose -f docker-compose.sidecar.yml up -d
   ```

2. **Use full stack for development:**
   ```bash
   docker-compose --profile monitoring up -d
   ```

3. **Always check logs first:**
   ```bash
   docker-compose logs -f ebpf-sidecar
   ```

4. **Verify shared PID namespace:**
   ```bash
   docker exec ebpf-sidecar ps aux | grep banyandb
   ```

5. **Clean up properly:**
   ```bash
   docker-compose down -v  # Remove volumes too
   ```

---

## 🐛 Common Issues

### Issue: Sidecar can't see BanyanDB processes

**Cause:** PID namespace not shared

**Solution:**
- Ensure `pid: "service:banyandb"` is set
- Check Docker version (requires Docker 19.03+)
- Verify with: `docker exec ebpf-sidecar ps aux`

### Issue: Metrics endpoint not accessible

**Cause:** Network namespace not shared

**Solution:**
- Ensure `network_mode: "service:banyandb"` is set
- Access metrics via BanyanDB's ports: `http://localhost:18080/metrics`

### Issue: Permission denied loading eBPF

**Cause:** Container not privileged

**Solution:**
- Ensure `privileged: true` is set
- Check with: `docker inspect ebpf-sidecar | grep Privileged`

---

## 📚 Next Steps

- [Kubernetes Deployment](deploy/DEPLOYMENT.md)
- [Configuration Guide](configs/README.md)
- [Metrics Reference](README.md#metrics-provided)
- [Troubleshooting Guide](deploy/DEPLOYMENT.md#troubleshooting)
