# Upgrade Procedure

This document describes how to upgrade your existing installation of the BanyanDB database.

## Server and File Versioning

BanyanDB uses a separate version for the server and the file format. The server version is the version of the BanyanDB server binary, and the file version is the version of the data files stored on the disk.

You can use "banyand liaison/data/standalone -v" to check the server version. The file version is stored in "metadata" file laid in each ["segment"](../concept/tsdb.md#segment) folder.

The server will check the file version when it starts. If the file version is not compatible with the supported version list in the server, the server will refuse to start. Please check the [CHANGELOG.md](https://github.com/apache/skywalking-banyandb/tree/master/CHANGES.md) to ensure that the new version is compatible with the existing data files.

BanyanDB upgrade procedure is a rolling upgrade. You can upgrade the BanyanDB cluster without downtime. But you need to follow the instructions carefully to avoid any data loss:

- Perform the upgrades consecutively. You cannot skip versions.
- You must keep the all nodes with the same version. Do not mix versions in the same cluster.

All node roles (liaison and data) can be upgraded using the same procedure.

## Upgrade a Single Node

- Stop the server by sending a `SIGINT` or `SIGTERM` signal to the process.
- Wait for the server to stop gracefully.
- Replace the binary with the new version.
- Start the server with the same configuration.

## Rolling Strategy to Upgrade a Cluster

Before upgrading the cluster, you should check CHANGELOG.md](https://github.com/apache/skywalking-banyandb/tree/master/CHANGES.md) to ensure that there are no breaking changes in the new version.

- Upgrade the nodes one by one.
- Wait for the node to join the cluster and become healthy before upgrading the next node.
- Upgrade "data" nodes first, then "liaison" nodes.
- After upgrading all nodes, the cluster will be running the new version.

To ensure this strategy works, you should have a minimum of one node for each role of node in the cluster. For example, if you have a 2-node cluster, you should have at least one "liaison" and one "data" node.

The cluster should also have enough capacity to handle the load when one node is down for upgrade.

## Minimum Downtime Strategy to Upgrade a Cluster

If the new version has breaking changes, you can use the following strategy to upgrade the cluster with minimum downtime:

- Stop "liaison" nodes gracefully in a parallel manner.
- Stop "data" gracefully.
- Start the new version of "data" nodes.
- Start the new version of "liaison" nodes.

The data ingestion and retrieval will be stopped during the upgrade process. The downtime will be the sum of the time taken to stop and start the nodes. All these steps should be running in parallel to minimize the downtime.

If you don't have enough resource to perform a rolling upgrade or you have a large cluster with many nodes, you can use the minimum downtime strategy.

## Rollback

If you encounter any issues during the upgrade process, you can rollback to the previous version by following the same procedure as the upgrade process. If file format is backward compatible, you can rollback to the previous version without any data loss. Please check the [CHANGELOG.md](https://github.com/apache/skywalking-banyandb/tree/master/CHANGES.md) to ensure that old version is compatible with the new data files.
