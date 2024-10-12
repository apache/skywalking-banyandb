# Troubleshooting Installation Issues

If you encounter issues during the installation of BanyanDB, follow these troubleshooting steps to resolve common problems.

## Version Identification

Before troubleshooting, ensure you are using the correct version of BanyanDB. The installation instructions are specific to each version, so it's essential to verify the version you are installing.

```sh
banyand-server -v

version vx.y.z
```

It's recommended to use the latest stable version of BanyanDB to benefit from the latest features and bug fixes. Keep all servers in the cluster at the same version to avoid compatibility issues.

## Permission Denied

If you encounter a "Permission Denied" error during installation, check the file permissions and ownership of the installation directory. Ensure that the user running the installation has the necessary permissions to read, write, and execute files in the installation directory.

```sh
ls -l /path/to/installation/directory
```

If you deployed BanyanDB to OpenShift or Kubernetes, ensure that the service account has the required permissions to access the installation directory and run the BanyanDB server. The Docker image we published on Docker Hub is running as a **root** user, so you may need to adjust the permissions accordingly:

```sh
## check the service account of the pod of data node
kubectl get -n <namespace> pod <pod-name> -o=jsonpath='{.spec.serviceAccountName}'
```

Assuming the service account is **banyand**, you can grant the necessary permissions to the service account:

```sh
oc adm policy add-scc-to-user anyuid -z banyand -n <namespace>
```

## Liaison and Data Node Keeps in Pending State

If the liaison and data nodes remain in a pending state after installation, check the logs for any error messages that may indicate the cause of the issue. The logs can provide valuable information to troubleshoot the problem.

```sh
kubectl logs -n <namespace> <pod-name>
```

If you see `the schema registry init timeout, retrying...`, that means the schema registry(etcd) is not ready yet. You can check the status of the etcd cluster.

## Liaison and Data Node Keeps Restarting

If the liaison and data nodes keep restarting after installation, review the logs to identify the root cause of the issue:

```sh
kubectl logs -n <namespace> <pod-name>
```

Common reasons for nodes restarting include insufficient resources, configuration errors, or network connectivity issues. Ensure that the nodes have enough resources to run BanyanDB and that the configuration settings are correct.

## Liaison and Data Node Connection Issues

If the liaison and data nodes are unable to connect to each other, verify the network configuration and connectivity between the nodes. Ensure that the nodes can communicate with each other over the network and that there are no firewall rules blocking the connections.

Check registered endpoints of the data nodes in the etcd cluster:

```sh
etcdctl get --prefix /banyandb/nodes
```

`banyandb` is the namespace of the BanyanDB cluster. It can be changed by the flag `namespace`. You should ensure this namespace is consistent across all nodes.

If the addresses are incorrect or the nodes are not registered, check the configuration setting [service discovery](../configuration.md#service-discovery)

## Failed to Connect to Liaison Node

If the client application fails to connect to the liaison node, verify the network configuration and connectivity between the client and the liaison node. Ensure that the client can reach the liaison node over the network and that there are no firewall rules blocking the connection.

The SkyWalking OAP is using gRPC to communicate with the liaison node. Ensure that the gRPC port is open and accessible from the client application. If you are using WebUI(liaison) or bydbctl to connect to the liaison node, ensure that the correct HTTP port is used for the connection. The default HTTP port is `17913`. Refer to the [network](../configuration.md#liaison--network) for more details.
