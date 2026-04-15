# Node Discovery

## Introduction

Node discovery enables BanyanDB nodes to locate and communicate with each other in a distributed cluster. The discovery registry is consulted in three scenarios:

1. **Schema Registry Bootstrap**: Every node (data and liaison) runs a property schema client that must reach an embedded schema server on a data node to synchronize metadata. The discovery registry is the only source of schema server endpoints, so a misconfigured discovery mode prevents the cluster from ever forming.
2. **Request Routing**: Liaison nodes look up data nodes through the registry to send query and write requests.
3. **Data Migration**: The lifecycle agent lists all data nodes from the registry and filters them by `node_selector` labels to pick the warm/cold targets of a migration.

BanyanDB supports three discovery mechanisms to accommodate different deployment environments:

- **None** (default): No external discovery; suitable for standalone mode only.
- **DNS-based Discovery**: Cloud-native solution leveraging Kubernetes service discovery infrastructure.
- **File-based Discovery**: Static configuration file approach for simple deployments and testing environments.

### Which Commands Accept These Flags

The `--node-discovery-*` flag set is registered in the metadata client that every long-lived BanyanDB process embeds. All four of the following commands accept the same flags and must be given a consistent configuration so that they observe the same cluster topology:

- `banyand data`
- `banyand liaison`
- `banyand standalone` (only `none` is meaningful here)
- `lifecycle` (the lifecycle agent — see the [lifecycle documentation](lifecycle.md) for how it uses the discovery registry to pick migration targets)

This document provides guidance on configuring and operating all discovery modes.

## DNS-Based Discovery

### Overview

DNS-based discovery provides a cloud-native alternative leveraging Kubernetes' built-in service discovery infrastructure.

### How it Works with Kubernetes

1. Create a Kubernetes headless service.
2. Kubernetes automatically creates DNS SRV records: `_<port-name>._<proto>.<service-name>.<namespace>.svc.cluster.local`.
3. BanyanDB queries these SRV records to discover pod IPs and ports.
4. Connects to each endpoint via gRPC to fetch node metadata.

> Note: No external dependencies beyond DNS infrastructure.

### Configuration Flags

```shell
# Mode selection
--node-discovery-mode=dns                    # Enable DNS mode

# DNS SRV addresses (comma-separated)
--node-discovery-dns-srv-addresses=_grpc._tcp.banyandb-data.default.svc.cluster.local

# Query intervals
--node-discovery-dns-fetch-init-interval=5s  # Query interval during init (default: 5s)
--node-discovery-dns-fetch-init-duration=5m  # Initialization phase duration (default: 5m)
--node-discovery-dns-fetch-interval=15s      # Query interval after init (default: 15s)

# gRPC settings
--node-discovery-grpc-timeout=5s             # Timeout for metadata fetch (default: 5s)

# TLS configuration
--node-discovery-dns-tls=true                # Enable TLS for DNS discovery (default: false)
--node-discovery-dns-ca-certs=/path/to/ca.crt,/path/to/another.crt # Ordered CA bundle matching SRV addresses
```

`node-discovery-dns-fetch-init-interval` and `node-discovery-dns-fetch-init-duration` define the aggressive polling strategy during bootstrap before falling back to the steady-state
`node-discovery-dns-fetch-interval`. All metadata fetches share the same `node-discovery-grpc-timeout`, which is also reused by the file discovery mode. When TLS is enabled, the CA cert
list must match the SRV address order, ensuring each role (e.g., data, liaison) can be verified with its own trust bundle.

### Configuration Examples

**Single Zone Discovery:**

```shell
banyand data \
  --node-discovery-mode=dns \
  --node-discovery-dns-srv-addresses=_grpc._tcp.banyandb-data.default.svc.cluster.local \
  --node-discovery-dns-fetch-interval=10s \
  --node-discovery-grpc-timeout=5s
```

**Multi-Zone Discovery:**

```shell
banyand data \
  --node-discovery-mode=dns \
  --node-discovery-dns-srv-addresses=_grpc._tcp.data.zone1.svc.local,_grpc._tcp.data.zone2.svc.local \
  --node-discovery-dns-fetch-interval=10s
```

### Discovery Process

The DNS discovery operates continuously in the background:

1. **Query DNS SRV Records** (every 5s during init, then every 15s)
   - Queries each configured SRV address using `net.DefaultResolver.LookupSRV()`
   - Deduplicates addresses across multiple SRV queries

2. **Fallback on DNS Failure**
   - Uses cache from previous successful query
   - Allows cluster to survive temporary DNS outages

3. **Update Node Cache**
   - For each new address: connects via gRPC, fetches node metadata, notifies handlers.
   - For removed addresses: deletes from cache, notifies handlers.

### Kubernetes DNS TTL

Kubernetes DNS implements Time-To-Live (TTL) caching which affects discovery responsiveness:

- **Default TTL**: 30 seconds (CoreDNS default since Kubernetes 1.15)
- **Implications**: Pod IP changes may not be visible immediately. BanyanDB polls every 15 seconds, potentially using cached results for 2-3 cycles

**Customizing TTL:**

To reduce discovery latency, edit the CoreDNS ConfigMap:

```shell
kubectl edit -n kube-system configmap/coredns

# Modify cache plugin TTL:
# cache 5
```

### TLS Configuration

DNS-based discovery supports per-SRV-address TLS configuration for different roles.

**Configuration Rules:**

1. Number of CA certificate paths must match number of SRV addresses.
2. `caCertPaths[i]` is used for connections to addresses from `srvAddresses[i]`.
3. Multiple SRV addresses can share the same certificate file.
4. If multiple SRV addresses resolve to the same IP:port, certificate from first SRV address is used.

**TLS Examples:**

```shell
banyand data \
  --node-discovery-mode=dns \
  --node-discovery-dns-tls=true \
  --node-discovery-dns-srv-addresses=_grpc._tcp.data.namespace.svc.local,_grpc._tcp.liaison.namespace.svc.local \
  --node-discovery-dns-ca-certs=/etc/banyandb/certs/data-ca.crt,/etc/banyandb/certs/liaison-ca.crt
```

**Certificate Auto-Reload:**

BanyanDB monitors certificate files using fsnotify and automatically reloads them when changed:

- Hot reload without process restart
- 500ms debouncing for rapid file modifications
- SHA-256 validation to detect actual content changes

This enables zero-downtime certificate rotation.

### Kubernetes Deployment

#### Headless Service

Create a headless service for automatic DNS SRV record generation:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: banyandb-data
  namespace: default
  labels:
    app: banyandb
    component: data
spec:
  clusterIP: None  # Headless service
  selector:
    app: banyandb
    component: data
  ports:
  - name: grpc
    port: 17912
    protocol: TCP
    targetPort: grpc
```

This creates DNS SRV record: `_grpc._tcp.banyandb-data.default.svc.cluster.local`

## File-Based Discovery

### Overview

File-based discovery provides a simple static configuration approach where nodes are defined in a YAML file. This mode is ideal for testing environments, small deployments, or scenarios where dynamic service discovery infrastructure is unavailable.

The service periodically reloads the configuration file and automatically updates the node registry when changes are detected.

### How it Works
1. Read node configurations from a YAML file on startup
2. Attempt to connect to each node via gRPC to fetch full metadata
3. Successfully connected nodes are added to the cache
4. Nodes that fail to connect are skipped and will be attempted again on the next periodic file reload
5. Reload the file at the `node-discovery-file-fetch-interval` cadence as a backup to fsnotify-based reloads, reprocessing every entry (including nodes that previously failed)
6. Notify registered handlers when nodes are added or removed

### Configuration Flags

```shell
# Mode selection
--node-discovery-mode=file                    # Enable file mode

# File path (required for file mode)
--node-discovery-file-path=/path/to/nodes.yaml

# gRPC settings
--node-discovery-grpc-timeout=5s             # Timeout for metadata fetch (default: 5s)

# Interval settings
--node-discovery-file-fetch-interval=5m               # Polling interval to reprocess the discovery file (default: 5m)
--node-discovery-file-retry-initial-interval=1s       # Initial retry delay for failed node metadata fetches (default: 1s)
--node-discovery-file-retry-max-interval=2m           # Upper bound for retry delay backoff (default: 2m)
--node-discovery-file-retry-multiplier=2.0            # Multiplicative factor applied between retries (default: 2.0)
```

`node-discovery-file-fetch-interval` controls the periodic full reload that acts as a safety net even if filesystem events are missed.  
`node-discovery-file-retry-*` flags configure the exponential backoff used when a node cannot be reached over gRPC. Failed nodes are retried starting from the initial interval,
multiplied by the configured factor until the max interval is reached.

### YAML Configuration Format

```yaml
nodes:
  - name: liaison-0
    grpc_address: 192.168.1.10:18912
  - name: data-hot-0
    grpc_address: 192.168.1.20:17912
    tls_enabled: true
    ca_cert_path: /etc/banyandb/certs/ca.crt
  - name: data-cold-0
    grpc_address: 192.168.1.30:17912
```

**Configuration Fields:**

- **name** (required): Identifier for the node
- **grpc_address** (required): gRPC endpoint in `host:port` format
- **tls_enabled** (optional): Enable TLS for gRPC connection (default: false)
- **ca_cert_path** (optional): Path to CA certificate file (required when TLS is enabled)

### Configuration Examples

**Basic Configuration:**

```shell
banyand data \
  --node-discovery-mode=file \
  --node-discovery-file-path=/etc/banyandb/nodes.yaml
```

**With Custom Polling and Retry Settings:**

```shell
banyand data \
  --node-discovery-mode=file \
  --node-discovery-file-path=/etc/banyandb/nodes.yaml \
  --node-discovery-file-fetch-interval=20m \
  --node-discovery-file-retry-initial-interval=5s \
  --node-discovery-file-retry-max-interval=1m \
  --node-discovery-file-retry-multiplier=1.5
```

### Node Lifecycle

#### Initial/Interval Load

When the service starts:
1. Reads the YAML configuration file
2. Validates required fields (`grpc_address`)
3. Attempts gRPC connection to each node
4. Successfully connected nodes → added to cache

### Error Handling

**Startup Errors:**
- Missing or invalid file path → service fails to start
- Invalid YAML format → service fails to start
- Missing required fields → service fails to start

**Runtime Errors:**
- gRPC connection failure → node skipped; retried on next file reload
- File read error → keep existing cache, log error
- File deleted → keep existing cache, log error

## None Mode

### Overview

`--node-discovery-mode=none` disables external node discovery entirely. The discovery registry is wired to a stub that returns no peers, so no remote schema server, request routing target, or lifecycle migration target can be located. This is the default, and it is the only valid mode for `banyand standalone`.

### When to Use

- `banyand standalone` — single-process deployments that bundle the liaison and data roles together. The standalone process talks to its own in-process schema server, so no discovery is necessary.
- Smoke tests and unit tests where a cluster is not needed.

### Caveats

- **Not valid for clusters.** Running any of `banyand data`, `banyand liaison`, or `lifecycle` with `--node-discovery-mode=none` will leave the schema client with no server to talk to, liaison routing tables empty, and lifecycle migrations as no-ops. Use `dns` or `file` for every clustered deployment.
- None mode has no additional flags; there is nothing else to configure.

## Choosing a Discovery Mode

### DNS Mode - Best For

- Kubernetes-native deployments
- Simplified operations (no external dependencies beyond DNS)
- Cloud-native architecture alignment
- StatefulSets with stable network identities
- Rapid deployment without external dependencies

### File Mode - Best For

- Development and testing environments
- Small static clusters (< 10 nodes)
- Air-gapped deployments without service discovery infrastructure
- Proof-of-concept and demo setups
- Environments where node membership is manually managed
- Scenarios requiring predictable and auditable node configuration

### None Mode - Best For

- `banyand standalone` (the only supported cluster topology for this mode)
- Local smoke tests and unit tests where no remote peers are involved