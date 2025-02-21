# Security

Security is a critical aspect of any application. In this section, we will discuss the security features of the BanyanDB.

## Authentication

### Username/Password Authentication

BanyanDB supports username/password authentication for gRPC and HTTP server. The following flags are used to configure
authentication:

- `--auth-config-file string`: Path to the authentication config file (YAML format).
- `--enable-health-auth bool`: Enable authentication for health service.It only takes effect when you provide a `grpc-health-port` different from the `grpc-port`.

For example, you can create an authentication configuration file like this:

```yaml
users:
  - username: admin
    password: password
```

Then, use the auth-config-file flag to enable the authentication:

```shell
banyand liaison --auth-config-file=/path/to/config.yaml
```

### TLS

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

If you only want to security gRPC connection, you can leave `--http-tls=false`.

```shell
banyand liaison --tls=true --key-file=server.key --cert-file=server.crt --http-grpc-cert-file=server.crt 
```

Also, you can enable TLS for HTTP connection only.

```shell
banyand liaison --http-tls=true --http-key-file=server.key --http-cert-file=server.crt
```

> Note: BanyanDB does not support TLS between liaison and data nodes.

## Authorization

BanyanDB does not have built-in authorization mechanisms. However, you can use external tools like [Envoy](https://www.envoyproxy.io/) or [Istio](https://istio.io/) to manage access control and authorization.

## Data Encryption

BanyanDB does not provide data encryption at rest. If you require data encryption, you can use disk-level encryption or other encryption mechanisms provided by the underlying storage system.
