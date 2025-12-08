# Security

Security is a critical aspect of any application. In this section, we will discuss the security features of the BanyanDB.

## Authentication

BanyanDB supports username and password-based authentication for both gRPC and HTTP endpoints. This guide explains how to configure and use this feature for both the BanyanDB server and the `bydbctl` command-line tool.

### Basic Authentication

To enable authentication on the BanyanDB server, you need to create a **YAML configuration file** that defines your users and their passwords.

#### Create the Configuration File

The configuration file should contain a list of users. For security, it's highly recommended to use strong, unique passwords instead of the examples provided below.

```yaml
users:
  - username: admin
    password: StrongPassword123
  - username: dev_user
    password: AnotherStrongPassword456
```

#### Set File Permissions

To protect your credentials, the configuration file **must** have read/write permissions only for the owner. Set the correct permissions using the following command:

```shell
chmod 600 /path/to/auth_config.yaml
```

This command ensures that only the file's owner can read or modify the contents.

#### Start the BanyanDB Server

Finally, start the BanyanDB server with the `--auth-config-file` flag, pointing to the file you just created:

```shell
banyand liaison --auth-config-file=/path/to/auth_config.yaml
```

### Authenticating with `bydbctl`

When the BanyanDB server has authentication enabled, you must provide a username and password with your `bydbctl` commands. There are two ways to do this.

#### Option 1: Use Command-Line Flags

You can provide the authentication details directly in your `bydbctl` command using the `-u` and `-p` flags.

```shell
bydbctl group get -g group1 -a http://localhost:17913 -u admin -p StrongPassword123
```

> **Note:** The `addr`, `group`, `username`, and `password` parameters will be automatically saved to a file named `.bydbctl.yaml` in your home directory if it doesn't already exist. This file will also have the permissions `0600` for security.

#### Option 2: Use a `bydbctl` Configuration File

You can create a separate configuration file for `bydbctl` to store your connection and authentication details.

#### Create the Configuration File

Create a YAML file containing your connection details. For example:

```yaml
addr: http://localhost:17913
group: group1
username: admin
password: StrongPassword123
```

#### Set File Permissions

Just like with the server's authentication file, the `bydbctl` configuration file should have secure permissions:

```shell
chmod 600 /path/to/bydbctl_config.yaml
```

#### Run `bydbctl` with the Configuration File

You can then run `bydbctl` by specifying the path to your configuration file with the `--config` flag.

```shell
bydbctl --config /path/to/bydbctl_config.yaml group list
```

### External TLS (Client ↔ Server)

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

- `--data-client-tls`: Enable TLS on the internal queue client inside Liaison; if false, the queue uses plain TCP.
- `--data-client-ca-cert`: PEM‑encoded CA (or bundle) that the queue client uses to verify Data‑Node server certificates.

Each Liaison/Data process still advertises its certificate with the public flags (`--tls`, `--cert-file`, `--key-file`). The same certificate/key pair can be reused for both external traffic and the internal queue.

**Example: Enable internal TLS between liaison and data nodes**

```shell
banyand liaison --data-client-tls=true --data-client-ca-cert=ca.crt --tls=true --cert-file=server.crt --key-file=server.key
banyand data --tls=true --cert-file=server.crt --key-file=server.key
```

> Note: 
> - The `--data-client-ca-cert` should point to the CA certificate used to sign the data node's server certificate.
> - Data nodes act as servers and do not need a CA certificate to connect to liaison nodes (liaison nodes connect to data nodes, not vice versa).
> - The flag names use the prefix "data" because liaison nodes connect to data nodes. The actual flag names are `--data-client-tls` and `--data-client-ca-cert`.

**Dynamic Certificate Reloading**

All certificates used for internal TLS can be reloaded automatically when they are updated:

- **Liaison nodes**:
  - CA certificate file (`--data-client-ca-cert`): Can be updated, and the server will automatically reload it and reconnect all clients to data nodes with the new certificate.
  - Server certificate files (`--cert-file`, `--key-file`): Can be updated, and the server will automatically reload them. These certificates are used for both external client connections and can be reused for internal queue communication.
- **Data nodes**: The server certificate files (`--cert-file`, `--key-file`) can be updated, and the server will automatically reload them without requiring a restart.

You can update the files or recreate the files, and the servers will automatically reload them.

## Authorization

BanyanDB does not have built-in authorization mechanisms. However, you can use external tools like [Envoy](https://www.envoyproxy.io/) or [Istio](https://istio.io/) to manage access control and authorization.

## Data Encryption

BanyanDB does not provide data encryption at rest. If you require data encryption, you can use disk-level encryption or other encryption mechanisms provided by the underlying storage system.
