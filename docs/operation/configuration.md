# Configuration

BanyanD is the BanyanDB server. There are two ways to configure BanyanD: using a bootstrap flag or using environment variables. The environment variable name has a prefix `BYDB_` followed by the flag name in uppercase. For example, the flag `--port` can be set using the environment variable `BYDB_PORT`.

## Commands

### Bootstrap commands

There are three bootstrap commands: `data`, `liaison`, and `standalone`. You could use them to boot different roles of BanyanD.

- `data`: Run as the data server. It stores the data and processes the data requests.
- `liaison`: Run as the liaison server. It is responsible for the communication between the data servers and clients.
- `standalone`: Run as the standalone server. It combines the data, liaison server and embed etcd server for development and testing. 

### Other commands

- `completion`: Generate the autocompletion script for the specified shell.
- `help`: Help about any command.

## Flags

Below are the available flags for configuring BanyanDB:

### Service Discovery

BanyanDB Liaison reads the endpoints of the data servers from the etcd server. The following flags are used to configure:

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

BanyanDB uses etcd for service discovery and configuration. The following flags are used to configure the etcd settings. These flags are only used when running as a liaison or data server. Standalone server embeds etcd server and does not need these flags.

- `--etcd-listen-client-url strings`: A URL to listen on for client traffic (default: [http://localhost:2379]).
- `--etcd-listen-peer-url strings`: A URL to listen on for peer traffic (default: [http://localhost:2380]).

The following flags are used to configure the timeout of data sending from liaison to data servers:

- `--stream-write-timeout duration`: Stream write timeout (default: 15s).
- `--measure-write-timeout duration`: Measure write timeout (default: 15s).

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

If the node is running as a data server, you can configure the health check server port:

`--health-port uint32`: the port of banyand health check listens (default 17913)

The following flags are used to configure the measure storage engine:

- `--measure-flush-timeout duration`: The memory data timeout of measure (default: 5s).
- `--measure-root-path string`: The root path of the database (default: "/tmp").
- `--measure-max-fan-out-size bytes`: the upper bound of a single file size after merge of measure (default 8.00EiB)

The following flags are used to configure the stream storage engine:

- `--stream-flush-timeout duration`: The memory data timeout of stream (default: 1s).
- `--stream-root-path string`: The root path of the database (default: "/tmp").
- `--stream-max-fan-out-size bytes`: the upper bound of a single file size after merge of stream (default 8.00EiB)
- `--element-index-flush-timeout duration`: The element index timeout of stream (default: 1s).

The following flags are used to configure the embedded etcd storage engine which is only used when running as a standalone server:

- `--metadata-root-path string`: The root path of metadata (default: "/tmp").
- `--etcd-auto-compaction-mode string`: The mode to compact the storage (default: "periodic").
- `--etcd-auto-compaction-retention string`: The retention period of the storage (default: "1h").
- `--etcd-defrag-cron string`: The scheduled task to free up disk space (default: "@daily").
- `--quota-backend-bytes bytes`: Quota for backend storage (default: 2.00GiB).

The following flags are used to configure the memory protector:

- `--allowed-bytes bytes`: Allowed bytes of memory usage. If the memory usage exceeds this value, the query services will stop. Setting a large value may evict data from the OS page cache, causing high disk I/O. (default 0B)  
- `--allowed-percent int`: Allowed percentage of total memory usage. If usage exceeds this value, the query services will stop. This takes effect only if `allowed-bytes` is 0. If usage is too high, it may cause OS page cache eviction. (default 75)

### Observability

- `--observability-listener-addr string`: Listen address for observability (default: ":2121").
- `--observability-modes strings`: Modes for observability (default: [prometheus]).
- `--pprof-listener-addr string`: Listen address for pprof (default: ":6060").
- `--dst-slow-query duration`: distributed slow query threshold, 0 means no slow query log. This is only used for the liaison server (default: 0).
- `--slow-query duration`: slow query threshold, 0 means no slow query log. This is only used for the data and standalone server (default: 0).

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
