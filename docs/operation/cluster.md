# Cluster Maintenance

## Introduction
Properly maintaining and scaling a cluster is crucial for ensuring its reliable and efficient operation. This document provides guidance on setting up a cluster, planning its capacity, and scaling it to meet evolving requirements.

## Cluster Setup
Before deploying or maintaining a cluster, it is recommended to familiarize oneself with the basic clustering concepts by reviewing the [clustering documentation](../concept/clustering.md).

To set up a cluster, one can refer to the [cluster installation guide](../installation/cluster.md), which describes the process in detail. A minimal cluster should consist of the following nodes:

- 3 etcd nodes
- 2 liaison nodes
- 2 data nodes

This configuration is recommended for high availability, ensuring that the cluster can continue operating even if a single node becomes temporarily unavailable, as the remaining nodes can handle the increased workload.

It is generally preferable to deploy multiple smaller data nodes rather than a few larger ones, as this approach reduces the workload increase on the remaining data nodes when some nodes become temporarily unavailable.

To balance the write and query traffic to the liaison nodes, the use of an gRPC load balancer is recommended. The gRPC port defaults to `17912`, but the gRPC host and port can be altered using the `grpc-host` and `grpc-port` configuration options.

For those seeking to set up a cluster in a Kubernetes environment, a [dedicated guide](../installation/kubernetes.md) is available to assist with the process.

## Capacity Planning
Each node role can be provisioned with the most suitable hardware resources. The cluster's capacity scales linearly with the available resources. The required amounts of CPU and RAM per node role depend highly on the workload, such as the number of time series, query types, and write/query QPS. It is recommended to set up a test cluster mirroring the production workload and iteratively scale the per-node resources and the number of nodes per role until the cluster becomes stable. Additionally, the use of observability tools is advised, as they can help identify bottlenecks in the cluster setup.

The necessary storage space can be estimated based on the disk space usage observed during a test run. For example, if the storage space usage is 10GB after a day-long test run on a production workload, then the cluster should have at least 10GB*7=70GB of disk space for a group with `ttl=7day`.

To ensure the cluster's resilience and responsiveness, it is recommended to maintain the following spare resource levels:

- 50% of free RAM across all the nodes to reduce the probability of OOM (out of memory) crashes and slowdowns during temporary spikes in workload.
- 50% of spare CPU across all the nodes to reduce the probability of slowdowns during temporary spikes in workload.
- At least 20% of free storage space at the directories pointed by `measure-root-path` and `stream-root-path`.

## Scalability
The cluster's performance and capacity can be scaled in two ways: vertical scalability and horizontal scalability.

### Vertical Scalability
Vertical scalability refers to adding more resources (CPU, RAM, disk I/O, disk space, network bandwidth) to existing nodes in the cluster.

Increasing the CPU and RAM of existing liaison nodes can improve the performance for heavy queries that process a large number of time series with many data points.

Increasing the CPU and RAM of existing data nodes can increase the number of time series the cluster can handle. However, it is generally preferred to add more data nodes rather than increasing the resources of existing data nodes, as a higher number of data nodes increases cluster stability and improves query performance over time series.

Increasing the disk I/O and disk space of existing etcd nodes can improve the performance for heavy metadata queries that process a large number of metadata entries.

### Horizontal Scalability
Horizontal scalability refers to adding more nodes to the cluster.

Increasing the number of liaison nodes can increase the maximum possible data ingestion speed, as the ingested data can be split among a larger number of liaison nodes. It can also increase the maximum possible query rate, as the incoming concurrent requests can be split among a larger number of liaison nodes.

Increasing the number of data nodes can increase the number of time series the cluster can handle. This can also improve query performance, as each data node contains a lower number of time series when the number of data nodes increases.

The new added data nodes can be automatically discovered by the existing liaison nodes. It is recommended to add data nodes one by one to avoid overloading the liaison nodes with the new data nodes' metadata.

The cluster's availability is also improved by increasing the number of data nodes, as active data nodes need to handle a lower additional workload when some data nodes become unavailable. For example, if one node out of 2 nodes is unavailable, then 50% of the load is re-distributed across the remaining node, resulting in a 100% per-node workload increase. If one node out of 10 nodes is unavailable, then 10% of the load is re-distributed across the 9 remaining nodes, resulting in only an 11% per-node workload increase.

Increasing the number of etcd nodes can increase the cluster's metadata capacity and improve the cluster's metadata query performance. It can also improve the cluster's metadata availability, as the metadata is replicated across all the etcd nodes. However, the cluster size should be odd to avoid split-brain situations.

The steps of adding more data nodes:

1. Boot up the new data node. They will register themselves to the etcd cluster. The liaison nodes will discover the new data node automatically.
2. If the shards are not balanced, the new data node will receive the shards from the existing data nodes. The shards are balanced automatically.
3. Or if the shards are too few to balance, more shards should be created by increasing `shard_num` of the `group`. Seeing the [CRUD Groups](../interacting/bydbctl/schema/group.md) for more details.
4. The new data node will start to ingest data and serve queries.

## Availability

The BanyanDB cluster remains available for data ingestion and data querying even if some of its components are temporarily unavailable.

### Liaison Node Failure

In the event of a liaison node failure, the cluster remains available when the gRPC load balancer can stop sending requests to the failed liaison node and start sending requests to the remaining liaison nodes. The failed liaison node is replaced by the remaining liaison nodes, and the cluster continues to ingest data and serve queries. However, if the remaining liaison nodes are overloaded, the cluster might face performance degradation.

It is recommended to monitor the cluster's performance and add more liaison nodes in case of performance degradation. A workload management platform, such as Kubernetes, can be used to automatically scale the liaison nodes based on the cluster's performance metrics.

### Data Node Failure

If a data node fails, the cluster remains available. The failed data node is replaced by the remaining data nodes, and the cluster continues to ingest new data and serve queries. If the remaining data nodes are overloaded, the cluster might face performance degradation.

The liaison nodes automatically discover the failed data node through the etcd cluster. They will perform a health check on the failed data node. If the failed data node is not healthy, the liaison nodes will stop sending requests to the failed data node and start sending requests to the remaining data nodes. Otherwise, the liaison nodes will continue sending requests to the failed data node in case of a temporary failure between the etcd cluster and the data node.

Liaison nodes continue serving queries if at least one data node is available. However, the responses might lose some data points that are stored in the failed data node. The lost data points are automatically recovered when the failed data node is back online.

The client might face a "grpc: the client connection is closing" error temporarily when the liaison nodes are switching the requests from the failed data node to the remaining data nodes. The client should retry the request in case of this error.

A workload management platform, such as Kubernetes, can be used to automatically scale the data nodes based on the cluster's performance metrics. But the shard number of the group should be increased manually. A proper practice is to set a expected maximum shard number for the group when creating the group. The shard number should match the maximum number of data nodes that the group can have.

### etcd Node Failure

If an etcd node fails, the cluster can still ingest new data and serve queries of `Stream` and `Measure`. `Property` operations are not available during the etcd node failure.

When the etcd node is back online, the cluster automatically recovers without any manual intervention. If the etcd cluster lost the data, the client should rerun the metadata initialization process to recover the metadata.

You might see some etcd-related errors in the logs of the liaison nodes and data nodes. These errors are automatically recovered when the failed etcd node is back online.
