# eBPF Sidecar Deployment Guide

This guide covers different deployment strategies for the eBPF Sidecar with BanyanDB.

## Table of Contents
- [Deployment Strategies](#deployment-strategies)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Docker Compose Deployment](#docker-compose-deployment)
- [Helm Chart Integration](#helm-chart-integration)
- [Security Considerations](#security-considerations)
- [Troubleshooting](#troubleshooting)

---

## Deployment Strategies

### 1. **Sidecar Pattern (Recommended for Production)**
Deploy eBPF sidecar alongside BanyanDB in the same Pod.

**Pros:**
- Tight coupling with BanyanDB lifecycle
- Shared PID namespace for accurate monitoring
- Easy resource management
- Automatic scaling with BanyanDB

**Cons:**
- Requires privileged mode
- Higher resource usage per pod

**Use case:** Production environments where you want per-instance monitoring

### 2. **DaemonSet Pattern**
Deploy eBPF sidecar as a DaemonSet on each node.

**Pros:**
- Node-level monitoring
- Single sidecar per node (lower overhead)
- Can monitor multiple BanyanDB instances

**Cons:**
- Less granular per-instance metrics
- Requires hostPID and hostNetwork

**Use case:** Large clusters with many BanyanDB instances per node

### 3. **Standalone Deployment**
Deploy eBPF sidecar as a separate service.

**Pros:**
- Independent lifecycle
- Easier to debug
- Can be updated independently

**Cons:**
- More complex networking
- Harder to correlate metrics

**Use case:** Development and testing

---

## Kubernetes Deployment

### Prerequisites

1. **Kubernetes cluster** with kernel 4.14+ and eBPF support
2. **kubectl** configured to access your cluster
3. **Privileged containers** enabled in your cluster
4. **Prometheus** (optional, for metrics collection)

### Option A: Sidecar Pattern

This deploys BanyanDB with eBPF sidecar in the same Pod.

```bash
# Create namespace
kubectl create namespace skywalking

# Deploy BanyanDB with sidecar
kubectl apply -f deploy/kubernetes/banyandb-with-sidecar.yaml

# Verify deployment
kubectl get pods -n skywalking
kubectl logs -n skywalking banyandb-0 -c ebpf-sidecar

# Check metrics
kubectl port-forward -n skywalking banyandb-0 18080:18080
curl http://localhost:18080/metrics
```

**Key Features:**
- `shareProcessNamespace: true` - Allows sidecar to see BanyanDB processes
- `privileged: true` - Required for eBPF operations
- Separate services for BanyanDB and eBPF metrics

### Option B: DaemonSet Pattern

This deploys one eBPF sidecar per node.

```bash
# Deploy as DaemonSet
kubectl apply -f deploy/kubernetes/ebpf-sidecar-daemonset.yaml

# Verify deployment
kubectl get daemonset -n skywalking
kubectl get pods -n skywalking -l app=ebpf-sidecar

# Check metrics from specific node
NODE_NAME=$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')
POD_NAME=$(kubectl get pods -n skywalking -l app=ebpf-sidecar --field-selector spec.nodeName=$NODE_NAME -o jsonpath='{.items[0].metadata.name}')
kubectl port-forward -n skywalking $POD_NAME 18080:18080
curl http://localhost:18080/metrics
```

**Key Features:**
- `hostPID: true` - Monitor all processes on the node
- `hostNetwork: true` - Use host networking
- Tolerations for running on all nodes

### Prometheus Integration

Add ServiceMonitor for automatic Prometheus scraping:

```yaml
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
```

---

## Docker Compose Deployment

### Local Development Setup

```bash
cd ebpf-sidecar

# Start all services (BanyanDB + eBPF Sidecar + Prometheus + Grafana)
docker-compose up -d

# Check services
docker-compose ps

# View logs
docker-compose logs -f ebpf-sidecar-prometheus

# Access services:
# - BanyanDB: http://localhost:17913
# - eBPF Metrics: http://localhost:8080/metrics
# - Prometheus: http://localhost:9092
# - Grafana: http://localhost:3000 (admin/admin)
```

### Production-like Setup

For a more production-like setup, use separate compose files:

```bash
# Start BanyanDB only
docker-compose -f docker-compose.banyandb.yaml up -d

# Start eBPF sidecar with Prometheus export
docker-compose -f docker-compose.sidecar-prometheus.yaml up -d

# Or with BanyanDB export
docker-compose -f docker-compose.sidecar-banyandb.yaml up -d
```

---

## Helm Chart Integration

### Adding eBPF Sidecar to Existing Helm Chart

If you're using the official BanyanDB Helm chart, you can add the sidecar by creating a `values.yaml`:

```yaml
# values-with-ebpf.yaml
standalone:
  enabled: true
  
  # Add sidecar container
  sidecars:
    - name: ebpf-sidecar
      image: banyandb/ebpf-sidecar:latest
      ports:
        - containerPort: 18080
          name: metrics
        - containerPort: 19090
          name: grpc
      env:
        - name: EBPF_SIDECAR_EXPORT_TYPE
          value: "prometheus"
        - name: EBPF_SIDECAR_COLLECTOR_INTERVAL
          value: "10s"
      volumeMounts:
        - name: sys
          mountPath: /sys
          readOnly: true
        - name: proc
          mountPath: /proc
          readOnly: true
        - name: modules
          mountPath: /lib/modules
          readOnly: true
      securityContext:
        privileged: true
        capabilities:
          add:
            - SYS_ADMIN
            - BPF
            - PERFMON
      resources:
        requests:
          memory: "128Mi"
          cpu: "100m"
        limits:
          memory: "512Mi"
          cpu: "500m"
  
  # Add required volumes
  extraVolumes:
    - name: sys
      hostPath:
        path: /sys
    - name: proc
      hostPath:
        path: /proc
    - name: modules
      hostPath:
        path: /lib/modules

# Enable shared PID namespace
shareProcessNamespace: true

# Add service for eBPF metrics
services:
  ebpf:
    type: ClusterIP
    ports:
      - port: 18080
        name: metrics
      - port: 19090
        name: grpc
    annotations:
      prometheus.io/scrape: "true"
      prometheus.io/port: "18080"
      prometheus.io/path: "/metrics"
```

Deploy with:

```bash
helm install banyandb \
  oci://registry-1.docker.io/apache/skywalking-banyandb-helm \
  --version 0.3.0 \
  --set image.tag=0.7.0 \
  -f values-with-ebpf.yaml \
  -n sw
```

---

## Security Considerations

### Required Capabilities

The eBPF sidecar requires the following Linux capabilities:

- `CAP_SYS_ADMIN` - Load eBPF programs
- `CAP_BPF` - eBPF operations (kernel 5.8+)
- `CAP_PERFMON` - Performance monitoring (kernel 5.8+)
- `CAP_NET_ADMIN` - Network tracing (if needed)
- `CAP_SYS_RESOURCE` - Resource limits

### Security Best Practices

1. **Use specific capabilities instead of privileged mode** (when possible):
   ```yaml
   securityContext:
     capabilities:
       add:
         - BPF
         - PERFMON
         - SYS_ADMIN
   ```

2. **Restrict to specific nodes** using node selectors:
   ```yaml
   nodeSelector:
     ebpf-enabled: "true"
   ```

3. **Use Pod Security Standards**:
   ```yaml
   apiVersion: v1
   kind: Namespace
   metadata:
     name: skywalking
     labels:
       pod-security.kubernetes.io/enforce: privileged
       pod-security.kubernetes.io/audit: privileged
       pod-security.kubernetes.io/warn: privileged
   ```

4. **Network Policies** to restrict sidecar access:
   ```yaml
   apiVersion: networking.k8s.io/v1
   kind: NetworkPolicy
   metadata:
     name: ebpf-sidecar-policy
   spec:
     podSelector:
       matchLabels:
         app: banyandb
     policyTypes:
     - Ingress
     ingress:
     - from:
       - podSelector:
           matchLabels:
             app: prometheus
       ports:
       - protocol: TCP
         port: 18080
   ```

### RBAC Configuration

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ebpf-sidecar
  namespace: skywalking

---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ebpf-sidecar
rules:
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["get", "list"]
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list"]

---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ebpf-sidecar
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ebpf-sidecar
subjects:
- kind: ServiceAccount
  name: ebpf-sidecar
  namespace: skywalking
```

---

## Troubleshooting

### Common Issues

#### 1. eBPF Program Load Failure

**Symptoms:**
```
Failed to load eBPF programs: permission denied
```

**Solutions:**
- Ensure container is running with `privileged: true` or proper capabilities
- Check kernel version: `uname -r` (must be 4.14+)
- Verify eBPF is enabled: `cat /proc/sys/kernel/unprivileged_bpf_disabled`

#### 2. No Metrics Collected

**Symptoms:**
```
ebpf_cache_misses_total 0
```

**Solutions:**
- Verify `shareProcessNamespace: true` is set (for sidecar pattern)
- Check if BanyanDB is actually performing I/O operations
- Verify eBPF programs are attached: `kubectl logs -c ebpf-sidecar`
- Check tracepoints exist: `ls /sys/kernel/debug/tracing/events/filemap/`

#### 3. High Memory Usage

**Symptoms:**
- eBPF sidecar OOMKilled
- Map sizes growing unbounded

**Solutions:**
- Adjust cleanup strategy in config:
  ```yaml
  collector:
    cleanup_strategy: clear_after_read
    cleanup_interval: 30s
  ```
- Increase memory limits
- Check for PID leaks: `kubectl logs -c ebpf-sidecar | grep "tracked_pids"`

#### 4. Prometheus Not Scraping

**Symptoms:**
- Metrics not appearing in Prometheus

**Solutions:**
- Verify service annotations:
  ```bash
  kubectl get svc ebpf-sidecar-metrics -n skywalking -o yaml
  ```
- Check Prometheus targets: http://prometheus:9090/targets
- Verify network policies allow Prometheus access
- Test metrics endpoint manually:
  ```bash
  kubectl port-forward svc/ebpf-sidecar-metrics 18080:18080 -n skywalking
  curl http://localhost:18080/metrics
  ```

### Debug Commands

```bash
# Check eBPF sidecar logs
kubectl logs -n skywalking -l app=banyandb -c ebpf-sidecar --tail=100

# Exec into sidecar container
kubectl exec -it -n skywalking banyandb-0 -c ebpf-sidecar -- /bin/bash

# Check eBPF maps
kubectl exec -it -n skywalking banyandb-0 -c ebpf-sidecar -- \
  bpftool map list

# View metrics
kubectl port-forward -n skywalking banyandb-0 18080:18080
curl http://localhost:18080/metrics | grep ebpf_cache

# Check kernel version and eBPF support
kubectl exec -it -n skywalking banyandb-0 -c ebpf-sidecar -- \
  uname -r
```

### Performance Tuning

```yaml
# Adjust collection interval
env:
- name: EBPF_SIDECAR_COLLECTOR_INTERVAL
  value: "30s"  # Reduce frequency for lower overhead

# Adjust resource limits
resources:
  requests:
    memory: "256Mi"
    cpu: "200m"
  limits:
    memory: "1Gi"
    cpu: "1000m"
```

---

## Monitoring the Monitor

### eBPF Sidecar Self-Monitoring

The sidecar exposes its own metrics:

```promql
# Sidecar health
up{job="ebpf-sidecar"}

# Collection errors
ebpf_collector_errors_total

# Map sizes (for memory monitoring)
ebpf_map_entries{map="cache_stats_map"}
```

### Alerting Rules

```yaml
groups:
- name: ebpf-sidecar
  rules:
  - alert: EBPFSidecarDown
    expr: up{job="ebpf-sidecar"} == 0
    for: 5m
    annotations:
      summary: "eBPF sidecar is down"
  
  - alert: EBPFHighMemory
    expr: container_memory_usage_bytes{container="ebpf-sidecar"} > 500000000
    for: 10m
    annotations:
      summary: "eBPF sidecar using high memory"
  
  - alert: EBPFNoMetrics
    expr: rate(ebpf_cache_misses_total[5m]) == 0
    for: 15m
    annotations:
      summary: "eBPF sidecar not collecting metrics"
```

---

## Next Steps

- [Configure Grafana Dashboards](../configs/grafana/README.md)
- [Set up Prometheus Alerts](../configs/prometheus/README.md)
- [Tune eBPF Programs](../internal/ebpf/programs/README.md)
- [Integrate with BanyanDB Helm Chart](https://github.com/apache/skywalking-banyandb-helm)
