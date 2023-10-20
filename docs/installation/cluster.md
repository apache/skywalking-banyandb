# Cluster Installation

## Setup Meta Nodes

Meta nodes are a etcd cluster which is required for the metadata module to provide the metadata service and nodes discovery service for the whole cluster.

The etcd cluster can be setup by the [etcd installation guide](https://etcd.io/docs/v3.5/install/)

## Role-base Banyand Cluster

There is an example: The etcd cluster is spread across three nodes with the addresses `10.0.0.1:2379`, `10.0.0.2:2379`, and `10.0.0.3:2379`.

Data nodes and liaison nodes are running as independent processes by

```shell
$ ./banyand-server storage --etcd-endpoints=http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379 <flags>
$ ./banyand-server storage --etcd-endpoints=http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379 <flags>
$ ./banyand-server storage --etcd-endpoints=http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379 <flags>
$ ./banyand-server liaison --etcd-endpoints=http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379 <flags>
```

## Node Discovery

The node discovery is based on the etcd cluster. The etcd cluster is required for the metadata module to provide the metadata service and nodes discovery service for the whole cluster.

The host is registered to the etcd cluster by the `banyand-server` automatically based on `node-host-provider` :

- `node-host-provider=hostname` : Default. The OS's hostname is registered as the host part in the address.
- `node-host-provider=ip` : The OS's the first non-loopback active IP address(IPv4) is registered as the host part in the address.
- `node-host-provider=flag` : `node-host` is registered as the host part in the address.

## Etcd Authentication

`etcd` supports through tls certificates and RBAC-based authentication for both clients to server communication. This section tends to help users set up authentication for BanyanDB.

### Authentication with username/password

The etcd user can be setup by the [etcd authentication guide](https://etcd.io/docs/v3.5/op-guide/authentication/)

The username/password is configured in the following command:

- `etcd-username`: The username for etcd client authentication.
- `etcd-password`: The password for etcd client authentication.

***Note: recommended using environment variables to set username/password for higher security.***

```shell
$ ./banyand-server storage --etcd-endpoints=your-endpoints --etcd-username=your-username --etcd-password=your-password <flags>
$ ./banyand-server liaison --etcd-endpoints=your-endpoints --etcd-username=your-username --etcd-password=your-password <flags>
```

### Transport security with HTTPS

The etcd trusted certificate file can be setup by the [etcd transport security model](https://etcd.io/docs/v3.5/op-guide/security/#example-1-client-to-server-transport-security-with-https)

- `etcd-tls-ca-file`: The path of the trusted certificate file.

```shell
$ ./banyand-server storage --etcd-endpoints=your-https-endpoints --etcd-tls-ca-file=youf-file-path <flags>
$ ./banyand-server liaison --etcd-endpoints=your-https-endpoints --etcd-tls-ca-file=youf-file-path <flags>
```

### Authentication with HTTPS client certificates

The etcd client certificates can be setup by the [etcd transport security model](https://etcd.io/docs/v3.5/op-guide/security/#example-2-client-to-server-authentication-with-https-client-certificates)

- `etcd-tls-ca-file`: The path of the trusted certificate file.
- `etcd-tls-cert-file`: Certificate used for SSL/TLS connections to etcd. When this option is set, advertise-client-urls can use the HTTPS schema.
- `etcd-tls-key-file`: Key for the certificate. Must be unencrypted.

```shell
$ ./banyand-server storage --etcd-endpoints=your-https-endpoints --etcd-tls-ca-file=youf-file-path --etcd-tls-cert-file=youf-file-path --etcd-tls-key-file=youf-file-path <flags>
$ ./banyand-server liaison --etcd-endpoints=your-https-endpoints --etcd-tls-ca-file=youf-file-path --etcd-tls-cert-file=youf-file-path --etcd-tls-key-file=youf-file-path <flags>
```