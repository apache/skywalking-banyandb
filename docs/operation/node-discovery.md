# Node Discovery

## Introduction

Node discovery enables BanyanDB nodes to locate and communicate with each other in a distributed cluster. The service discovery mechanism is primarily applied in two scenarios:

1. **Request Routing**: When liaison nodes need to send requests to data nodes for query/write execution.
2. **Data Migration**: When the lifecycle service queries the node list to perform shard migration and rebalancing.

BanyanDB supports three discovery mechanisms to accommodate different deployment environments:

- **Etcd-based Discovery**: Traditional distributed consensus approach suitable for VM deployments and multi-cloud scenarios.
- **DNS-based Discovery**: Cloud-native solution leveraging Kubernetes service discovery infrastructure.
- **File-based Discovery**: Static configuration file approach for simple deployments and testing environments.

This document provides guidance on configuring and operating all discovery modes.

## Etcd-Based Discovery

### Overview

Etcd-based discovery uses a distributed key-value store to maintain cluster membership. Each node registers itself in etcd with a time-limited lease and continuously renews the lease through heartbeat mechanisms.

### Configuration Flags

The following flags control etcd-based node discovery:

```shell
# Basic configuration
--node-discovery-mode=etcd                    # Enable etcd mode (default)
--namespace=banyandb                          # Namespace in etcd (default: banyandb)
--etcd-endpoints=http://localhost:2379        # Comma-separated etcd endpoints

# Authentication
--etcd-username=myuser                        # Etcd username (optional)
--etcd-password=mypass                        # Etcd password (optional)

# TLS configuration
--etcd-tls-ca-file=/path/to/ca.crt           # Trusted CA certificate
--etcd-tls-cert-file=/path/to/client.crt     # Client certificate
--etcd-tls-key-file=/path/to/client.key      # Client private key

# Timeouts and intervals
--node-registry-timeout=2m                    # Node registration timeout (default: 2m)
--etcd-full-sync-interval=30m                 # Full state sync interval (default: 30m)
```

### Configuration Examples

**Data Node with TLS:**

```shell
banyand data \
  --node-discovery-mode=etcd \
  --etcd-endpoints=https://etcd-1:2379,https://etcd-2:2379,https://etcd-3:2379 \
  --etcd-tls-ca-file=/etc/banyandb/certs/etcd-ca.crt \
  --etcd-tls-cert-file=/etc/banyandb/certs/etcd-client.crt \
  --etcd-tls-key-file=/etc/banyandb/certs/etcd-client.key \
  --namespace=production-banyandb
```

**Data Node with Basic Authentication:**

```shell
banyand data \
  --node-discovery-mode=etcd \
  --etcd-endpoints=http://etcd-1:2379,http://etcd-2:2379 \
  --etcd-username=banyandb-client \
  --etcd-password=${ETCD_PASSWORD} \
  --namespace=production-banyandb
```

### Etcd Key Structure

BanyanDB stores node registrations using the following pattern:

```
/{namespace}/nodes/{node-id}

Example: /banyandb/nodes/data-node-1:17912
```

**Key Components:**

- **Namespace**: Configurable via `--namespace` flag (default: `banyandb`). Allows multiple BanyanDB clusters to coexist in the same etcd instance
- **Node ID**: Format `{hostname}:{port}` or `{ip}:{port}`, uniquely identifying each node

The value stored is a Protocol Buffer serialized `databasev1.Node` message containing node metadata, addresses, roles, and labels.

### Node Lifecycle

#### Registration

When a node starts, it performs the following sequence:

1. **Lease Creation**: Creates a 5-second TTL lease with etcd, automatically renewed through keep-alive heartbeat.
2. **Key-Value Registration**: Uses etcd transaction to ensure the key doesn't already exist, preventing registration conflicts.
3. **Keep-Alive Loop**: Background goroutine continuously sends heartbeat with automatic reconnection and exponential backoff on network failures.

#### Health Monitoring

Node health is implicitly monitored through the lease mechanism:

- Nodes with expired leases (>5 seconds without keep-alive) are automatically removed by etcd.
- Presence in etcd indicates the node is alive.
- Dead nodes disappear within 5 seconds of failure.

#### Event Watching

BanyanDB implements real-time cluster membership tracking:

- Watches etcd prefix `/nodes/` for PUT (add/update) and DELETE events.
- Revision-based streaming resumes from last known revision after reconnection.
- Periodic full sync every 30 minutes (randomized) to detect missed events.

#### Deregistration

**Graceful Shutdown:**

1. Close signal triggers lease revocation.
2. Etcd automatically deletes the node's key.
3. Watch streams notify all other nodes immediately.

**Crash Recovery:**

- Lease expires after 5 seconds without renewal.
- Etcd automatically removes the key.
- No manual intervention required for cleanup.

## DNS-Based Discovery

### Overview

DNS-based discovery provides a cloud-native alternative leveraging Kubernetes' built-in service discovery infrastructure. This eliminates the operational complexity of managing a separate etcd cluster.

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
--node-discovery-dns-ca-certs=/path/to/ca.crt # CA certificates (comma-separated)
```

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
banyand query \
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
5. Reload the file at a configured interval (FetchInterval), reprocessing all nodes (including previously failed ones, excluding all successful ones)
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
--node-discovery-file-retry-interval=20s     # Interval to poll the discovery file and retry failed nodes (default: 20s)
```

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

**With Custom Retry Interval:**

```shell
banyand liaison \
  --node-discovery-mode=file \
  --node-discovery-file-path=/etc/banyandb/nodes.yaml \
  --node-discovery-file-retry-interval=30s
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

## Choosing a Discovery Mode

### Etcd Mode - Best For

- Traditional VM/bare-metal deployments
- Multi-cloud deployments requiring global service discovery
- Environments where etcd is already deployed
- Need for strong consistency guarantees

### DNS Mode - Best For

- Kubernetes-native deployments
- Simplified operations (no external etcd management)
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