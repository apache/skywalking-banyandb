# Configuration

BanyanD is the BanyanDB server. There are two ways to configure BanyanD: using a bootstrap flag or using environment variables. The environment variable name has a prefix `BYDB_` followed by the flag name in uppercase. For example, the flag `--port` can be set using the environment variable `BYDB_PORT`.

> BanyanDB supports both absolute and relative paths for directory and file configurations (such as data directories, TLS certificates, keys, etc.). Relative paths are resolved against the current working directory where the BanyanD process is started.

## Commands

### Bootstrap commands

There are three bootstrap commands: `data`, `liaison`, and `standalone`. You could use them to boot different roles of BanyanD.

- `data`: Run as the data server. It stores the data and processes the data requests.
- `liaison`: Run as the liaison server. It is responsible for the communication between the data servers and clients.
- `standalone`: Run as the standalone server. It combines the data and liaison server for development and testing.

### Other commands

- `completion`: Generate the autocompletion script for the specified shell.
- `help`: Help about any command.

## Flags

Below are the available flags for configuring BanyanDB:

### Service Discovery

BanyanDB supports three node-discovery modes: `none` (default, standalone only), `dns` (DNS SRV based), and `file` (static YAML list). For conceptual details, TLS configuration, cluster bring-up, and operational guidance, see the [node discovery documentation](node-discovery.md).

- `--node-discovery-mode string`: Node discovery mode: `none` (default, standalone), `dns`, or `file`.
- `--node-registry-timeout duration`: Timeout for the node registry (default: 2m).
- `--node-discovery-grpc-timeout duration`: Timeout for gRPC calls to fetch node metadata from discovered nodes.

#### DNS Mode Flags

- `--node-discovery-dns-srv-addresses strings`: Comma-separated DNS SRV addresses to query (e.g., `_grpc._tcp.banyandb.default.svc.cluster.local`).
- `--node-discovery-dns-fetch-init-interval duration`: Query interval during the initialization phase (default: 5s).
- `--node-discovery-dns-fetch-init-duration duration`: How long the initialization phase lasts before falling back to the steady-state interval (default: 5m).
- `--node-discovery-dns-fetch-interval duration`: Query interval after the initialization phase (default: 15s).
- `--node-discovery-dns-tls`: Enable TLS for DNS discovery gRPC connections.
- `--node-discovery-dns-ca-certs strings`: Comma-separated CA certificate files, one per SRV address in the same order, used to verify DNS-discovered nodes.

#### File Mode Flags

- `--node-discovery-file-path string`: Path to the static YAML file that lists cluster nodes (required when the mode is `file`).
- `--node-discovery-file-fetch-interval duration`: Polling interval to reread the discovery file as a fallback to fsnotify-based reloads (default: 5m).
- `--node-discovery-file-retry-initial-interval duration`: Initial retry delay for nodes whose metadata fetch failed (default: 1s).
- `--node-discovery-file-retry-max-interval duration`: Upper bound for the retry backoff (default: 2m).
- `--node-discovery-file-retry-multiplier float`: Multiplicative factor applied between retries (default: 2.0).

#### Node Host Registration

Each node advertises itself with a host part resolved according to `--node-host-provider`:

`node-host-provider`: the node host provider, can be "hostname", "ip" or "flag", default is hostname.

If the `node-host-provider` is "flag", you can use `node-host` to configure the node host:

```sh
./banyand liaison --node-host=foo.bar.com --node-host-provider=flag
```

If the `node-host-provider` is "hostname", BanyanDB will use the hostname of the server as the node host. The hostname is parsed from the go library `os.Hostname()`.

If the `node-host-provider` is "ip", BanyanDB will use the IP address of the server as the node host. The IP address is parsed from the go library `net.Interfaces()`. BanyanDB will use the first non-loopback IPv4 address as the node host.

The official Helm chart uses the `node-host-provider` as "ip" as the default value.

### Liaison & Network

BanyanDB uses gRPC for communication between the servers. The following flags are used to configure the network settings.

- `--grpc-host string`: The host BanyanDB listens on.
- `--grpc-port uint32`: The port BanyanDB listens on (default: 17912).
- `--http-grpc-addr string`: HTTP server redirects gRPC requests to this address (default: "localhost:17912").
- `--http-host string`: Listen host for HTTP.
- `--http-port uint32`: Listen port for HTTP (default: 17913).
- `--max-recv-msg-size bytes`: The size of the maximum receiving message (default: 10.00MiB).

The following flags are used to configure access logs for the data ingestion:

- `--access-log-root-path string`: Access log root path.
- `--enable-ingestion-access-log`: Enable ingestion access log.
- `--access-log-sampled`: if true, requests may be dropped when the channel is full; if false, requests are never dropped

The following flags are used to configure the timeout of data sending from liaison to data servers:

- `--stream-write-timeout duration`: Stream write timeout (default: 1m).
- `--measure-write-timeout duration`: Measure write timeout (default: 1m).
- `--trace-write-timeout duration`: Trace write timeout (default: 1m).

### TLS

If you want to enable TLS for the communication between the client and liaison/standalone, you can use the following flags:

- `--tls`: gRPC connection uses TLS if true, else plain TCP.
- `--http-tls`: http connection uses TLS if true, else plain HTTP.
- `--key-file string`: The TLS key file.
- `--cert-file string`: The TLS certificate file.
- `--http-grpc-cert-file string`: The gRPC TLS certificate file if the gRPC server enables TLS. It should be the same as the `cert-file`.
- `--http-key-file string`: The TLS key file of the HTTP server.
- `--http-cert-file string`: The TLS certificate file of the HTTP server.

#### Internal queue TLS (Liaison ↔ Data)

Enable TLS on the internal gRPC queue that the Liaison uses to push data into every Data‑Node:

- `--internal-tls`: enable TLS on the queue client inside Liaison; if false the queue uses plain TCP.
- `--internal-ca-cert <path>`: PEM‑encoded CA (or bundle) that the queue client uses to verify Data‑Node server certificates.

#### Server certificates

Each Liaison/Data process still advertises its certificate with the public flags shown above (`--tls`, `--cert-file`, `--key-file`).
The same certificate/key pair can be reused for both external traffic and the internal queue.

### Data & Storage

The following flags are used to configure the measure storage engine:

- `--measure-flush-timeout duration`: The memory data timeout of measure (default: 5s).
- `--measure-root-path string`: The root path of the measure database (default: "/tmp").
- `--measure-data-path string`: The data directory path of measure. If not set, `<measure-root-path>/measure/data` is used.
- `--measure-max-fan-out-size bytes`: the upper bound of a single file size after merge of measure (default 8.00EiB)

The following flags are used to configure the stream storage engine:

- `--stream-flush-timeout duration`: The memory data timeout of stream (default: 1s).
- `--stream-root-path string`: The root path of the stream database (default: "/tmp").
- `--stream-data-path string`: The data directory path of stream. If not set, `<stream-root-path>/stream/data` is used.
- `--stream-max-fan-out-size bytes`: the upper bound of a single file size after merge of stream (default 8.00EiB)
- `--element-index-flush-timeout duration`: The element index timeout of stream (default: 1s).

The following flags are used to configure the trace storage engine:

- `--trace-flush-timeout duration`: The memory data timeout of trace (default: 1s).
- `--trace-root-path string`: The root path of the database (default: "/tmp").
- `--trace-max-fan-out-size bytes`: the upper bound of a single file size after merge of trace (default 8.00EiB)

The following flags configure the remaining per-catalog storage roots:

- `--property-root-path string`: The root path of the property database (default: "/tmp").
- `--trace-root-path string`: The root path of the trace database (default: "/tmp").

The following flags are used to configure the memory protector:

- `--allowed-bytes bytes`: Allowed bytes of memory usage. If the memory usage exceeds this value, the query services will stop. Setting a large value may evict data from the OS page cache, causing high disk I/O. (default 0B)
- `--allowed-percent int`: Allowed percentage of total memory usage. If usage exceeds this value, the query services will stop. This takes effect only if `allowed-bytes` is 0. If usage is too high, it may cause OS page cache eviction. (default 75)

### Observability

- `--observability-listener-addr string`: Listen address for observability (default: ":2121").
- `--observability-modes strings`: Modes for observability (default: [prometheus]).
- `--pprof-listener-addr string`: Listen address for pprof (default: ":6060").
- `--dst-slow-query duration`: distributed slow query threshold, 0 means no slow query log. This is only used for the liaison server (default: 0).
- `--slow-query duration`: slow query threshold, 0 means no slow query log. This is only used for the data and standalone server (default: 0).

### Schema Registry

BanyanDB stores cluster metadata in a property-based schema registry. Data nodes that carry the meta role host the schema server; every node (data and liaison) runs a schema client that connects to the schema server and keeps its local cache in sync.

- `--schema-registry-mode string`: Schema registry mode (default: "property"). Only `property` is supported.

#### Schema Server (data node)

These flags are only effective on the `data` command and configure the property-based schema server embedded in the data node. They have no effect on liaison nodes.

- `--has-meta-role`: Whether this data node runs the embedded schema server (default: true). Set to false to deploy a pure data node that only serves time-series data and reads schemas from other data nodes. When false, every other flag in this subsection is inert on that node.
- `--schema-server-root-path string`: Root storage path for the schema property data (default: "/tmp").
- `--schema-server-grpc-host string`: Host the schema server listens on.
- `--schema-server-grpc-port uint32`: Port the schema server listens on (default: 17916).
- `--schema-server-flush-timeout duration`: Memory flush interval (default: 5s).
- `--schema-server-expire-delete-timeout duration`: Soft-delete expiration for deleted schemas (default: 168h / 7d).
- `--schema-server-tls`: Enable TLS on the schema server.
- `--schema-server-cert-file string`: The TLS certificate file of the schema server.
- `--schema-server-key-file string`: The TLS key file of the schema server.
- `--schema-server-max-recv-msg-size bytes`: Max gRPC receive message size for the schema server.
- `--schema-server-max-file-snapshot-num int`: Maximum number of file snapshots retained (default: 10).
- `--schema-server-min-file-snapshot-age duration`: Minimum age before a file snapshot is eligible for deletion (default: 1h).

##### Schema repair (gossip)

The following flags tune the gossip-based repair protocol that reconciles schema state between data nodes.

- `--schema-property-repair-trigger-cron string`: Cron expression that triggers a repair gossip round (default: `@every 10m`).
- `--schema-server-repair-tree-slot-count int`: Repair merkle tree slot count (default: 32).
- `--schema-server-repair-build-tree-cron string`: Cron for periodic repair tree rebuilding (default: `@every 1h`).
- `--schema-server-repair-quick-build-tree-time duration`: Minimum delay between on-demand quick repair tree rebuilds (default: 10m).

#### Schema Client (all nodes)

These flags configure how every node talks to the schema server.

- `--schema-property-client-sync-interval duration`: Polling interval for property-based schema sync (default: 30s).
- `--schema-property-client-health-check-interval duration`: Interval for periodic connection health checks to schema servers. Set to 0 to use the default interval (10s), or to a negative value to disable.
- `--schema-property-client-max-recv-msg-size bytes`: Max gRPC receive message size for property schema client.
- `--schema-property-client-tls`: Enable TLS for property schema client connections.
- `--schema-property-client-ca-cert string`: CA certificate file to verify the property schema server.

### Other

- `-n, --name string`: Name of this service.
- `-h, --help`: Help for standalone.
- `--show-rungroup-units`: Show rungroup units.
- `-v, --version`: Version for standalone.

### Global Flags

- `--logging-env string`: The logging environment (default: "prod").
- `--logging-level string`: The root level of logging (default: "info").
- `--logging-levels strings`: The level logging of logging.
- `--logging-modules strings`: The specific module for logging.
- `--node-host string`: The node host of the server, only used when `node-host-provider` is "flag".
- `--node-host-provider nodeIDProvider`: The node host provider, can be hostname, IP, or flag (default: Hostname).

## Example Command

```sh
banyand standalone --logging-env=dev --logging-level=debug --grpc-port=18913
```

If you want to use environment variables to configure BanyanD, you can set the environment variables like this:

```sh
export BYDB_LOGGING_ENV=dev
export BYDB_LOGGING_LEVEL=debug
export BYDB_GRPC_PORT=18913
```

Then you can run BanyanD without any flags:

```sh
banyand standalone
```
