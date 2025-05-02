# Security

Security is a critical aspect of any application. In this section, we will discuss the security features of the BanyanDB.

## Authentication

BanyanDB supports TLS for secure communication between servers. The following flags are used to configure TLS:

- `--tls`: gRPC Connection uses TLS if true, else plain TCP.
- `--http-tls`: HTTP Connection uses TLS if true, else plain HTTP.
- `--key-file string`: The TLS key file for gRPC.
- `--cert-file string`: The TLS certificate file for gRPC.
- `--http-grpc-cert-file string`: The gRPC TLS certificate file if the gRPC server enables TLS. It should be the same as the `cert-file`. It is used for gRPC over HTTP.
- `--http-key-file string`: The TLS key file of the HTTP server.
- `--http-cert-file string`: The TLS certificate file of the HTTP server.

For example, to enable TLS for gRPC communication, you can use the following flags:

```shell
banyand liaison --tls=true --key-file=server.key --cert-file=server.crt --http-grpc-cert-file=server.crt --http-tls=true --http-key-file=server.key --http-cert-file=server.crt
```

If you only want to secure the gRPC connection, you can leave `--http-tls=false`.

```shell
banyand liaison --tls=true --key-file=server.key --cert-file=server.crt --http-grpc-cert-file=server.crt 
```

Also, you can enable TLS for HTTP connection only.

```shell
banyand liaison --http-tls=true --http-key-file=server.key --http-cert-file=server.crt
```

The key and certificate files can be reloaded automatically when they are updated. You can update the files or recreate the files, and the server will automatically reload them.

### Internal TLS (Liaison ↔ Data Nodes)

BanyanDB supports enabling TLS for the internal gRPC queue between liaison and data nodes. This secures the communication channel used for data ingestion and internal operations.

The following flags are used to configure internal TLS:

- `--internal-tls`: Enable TLS on the internal queue client inside Liaison; if false, the queue uses plain TCP.
- `--internal-ca-cert <path>`: PEM‑encoded CA (or bundle) that the queue client uses to verify Data‑Node server certificates.

Each Liaison/Data process still advertises its certificate with the public flags (`--tls`, `--cert-file`, `--key-file`). The same certificate/key pair can be reused for both external traffic and the internal queue.

**Example: Enable internal TLS between liaison and data nodes**

```shell
banyand liaison --internal-tls=true --internal-ca-cert=ca.crt --tls=true --cert-file=server.crt --key-file=server.key
banyand data --tls=true --cert-file=server.crt --key-file=server.key
```

> Note: The `--internal-ca-cert` should point to the CA certificate used to sign the data node's server certificate.

## Authorization

BanyanDB does not have built-in authorization mechanisms. However, you can use external tools like [Envoy](https://www.envoyproxy.io/) or [Istio](https://istio.io/) to manage access control and authorization.

## Data Encryption

BanyanDB does not provide data encryption at rest. If you require data encryption, you can use disk-level encryption or other encryption mechanisms provided by the underlying storage system.
