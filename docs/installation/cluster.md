# Cluster Installation<TBD>

## Setup Meta Nodes

Meta nodes are a etcd cluster which is required for the metadata module to provide the metadata service and nodes discovery service for the whole cluster.

The etcd cluster can be setup by the [etcd installation guide](https://etcd.io/docs/v3.5/install/)

## Role-base Banyand Cluster

There is an example: The etcd cluster is spread across three nodes with the addresses `10.0.0.1:2379`, `10.0.0.2:2379`, and `10.0.0.3:2379`.

Data nodes, query nodes and liaison nodes are running as independent processes by

```shell
$ ./banyand-server storage --mode data --etcd-endpoints=http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379 <flags>
$ ./banyand-server storage --mode data --etcd-endpoints=http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379 <flags>
$ ./banyand-server storage --mode data --etcd-endpoints=http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379 <flags>
$ ./banyand-server storage --mode query --etcd-endpoints=http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379 <flags>
$ ./banyand-server storage --mode query --etcd-endpoints=http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379 <flags>
$ ./banyand-server liaison --etcd-endpoints=http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379 <flags>
```

The data node, query node and liaison node would be listening on the `<ports>` if no errors occurred.

## Mix-mode Data Nodes

If you want to use a `mix` mode instead of separate query and data nodes, you can run the banyand-server as processes by

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
