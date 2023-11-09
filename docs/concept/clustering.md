# BanyanDB Clustering

BanyanDB Clustering introduces a robust and scalable architecture that comprises "Liaison Nodes", "Data Nodes", and "Meta Nodes". This structure allows for effectively distributing and managing time-series data within the system.

## 1. Architectural Overview

A BanyanDB installation includes three distinct types of nodes: Data Nodes, Meta Nodes, and Liaison Nodes.

### 1.1 Data Nodes

Data Nodes hold all the raw time series data, metadata, and indexed data. They handle the storage and management of data, including streams and measures, tag keys and values, as well as field keys and values.

Data Nodes also handle the local query execution. When a query is made, it is directed to a Liaison, which then interacts with Data Nodes to execute the distributed query and return results.

In addition to persistent raw data, Data Nodes also handle TopN aggregation calculation or other computational tasks.

### 1.2 Meta Nodes

Meta Nodes is implemented by etcd. They are responsible for maintaining high-level metadata of the cluster, which includes:

- All nodes in the cluster
- All database schemas

### 1.3 Liaison Nodes

Liaison Nodes serve as gateways, routing traffic to Data Nodes. In addition to routing, they also provide authentication, TTL, and other security services to ensure secure and effective communication without the cluster.

Liaison Nodes are also responsible for handling computational tasks associated with distributed querying the database. They build query tasks and search for data from Data Nodes.

### 1.4 Standalone Mode

BanyanDB integrates multiple roles into a single process in the standalone mode, making it simpler and faster to deploy. This mode is especially useful for scenarios with a limited number of data points or for testing and development purposes.

In this mode, the single process performs the roles of the Liaison Node, Data Node, and Meta Node. It receives requests, maintains metadata, processes queries, and handles data, all within a unified setup.

## 2. Communication within a Cluster

All nodes within a BanyanDB cluster communicate with other nodes according to their roles:

- Meta Nodes share high-level metadata about the cluster.
- Data Nodes store and manage the raw time series data and communicate with Meta Nodes.
- Liaison Nodes distribute incoming data to the appropriate Data Nodes. They also handle distributed query execution and communicate with Meta Nodes.

### Nodes Discovery

All nodes in the cluster are discovered by the Meta Nodes. When a node starts up, it registers itself with the Meta Nodes. The Meta Nodes then share this information with the Liaison Nodes which use it to route requests to the appropriate nodes.

## 3. **Data Organization**

Different nodes in BanyanDB are responsible for different parts of the database, while Query and Liaison Nodes manage the routing and processing of queries.

### 3.1 Meta Nodes

Meta Nodes store all high-level metadata that describes the cluster. This data is kept in an etcd-backed database on disk, including information about the shard allocation of each Data Node. This information is used by the Liaison Nodes to route data to the appropriate Data Nodes, based on the sharding key of the data.

By storing shard allocation information, Meta Nodes help ensure that data is routed efficiently and accurately across the cluster. This information is constantly updated as the cluster changes, allowing for dynamic allocation of resources and efficient use of available capacity.

### 3.2 Data Nodes

Data Nodes store all raw time series data, metadata, and indexed data. On disk, the data is organized by `<group>/shard-<shard_id>/<segment_id>/`. The segment is designed to support retention policy.

### 3.3 Liaison Nodes

Liaison Nodes do not store data but manage the routing of incoming requests to the appropriate Query or Data Nodes. They also provide authentication, TTL, and other security services.

They also handle the computational tasks associated with data queries, interacting directly with Data Nodes to execute queries and return results.

## 4. **Determining Optimal Node Counts**

When creating a BanyanDB cluster, choosing the appropriate number of each node type to configure and connect is crucial. The number of Meta Nodes should always be odd, for instance, “3”. The number of Data Nodes scales based on your storage and query needs. The number of Liaison Nodes depends on the expected query load and routing complexity.

If the write and read load is from different sources, it is recommended to separate the Liaison Nodes for write and read. For instance, if the write load is from metrics, trace or log collectors and the read load is from a web application, it is recommended to separate the Liaison Nodes for write and read.

This separation allows for more efficient routing of requests and better performance. It also allows for scaling out of the cluster based on the specific needs of each type of request. For instance, if the write load is high, you can scale out the write Liaison Nodes to handle the increased load.

The BanyanDB architecture allows for efficient clustering, scaling, and high availability, making it a robust choice for time series data management.

## 5. Writes in a Cluster

In BanyanDB, writing data in a cluster is designed to take advantage of the robust capabilities of underlying storage systems, such as Google Compute Persistent Disk or Amazon S3(TBD). These platforms ensure high levels of data durability, making them an optimal choice for storing raw time series data.

### 5.1 Data Replication

Unlike some other systems, BanyanDB does not support application-level replication, which can consume significant disk space. Instead, it delegates the task of replication to these underlying storage systems. This approach simplifies the BanyanDB architecture and reduces the complexity of managing replication at the application level. This approach also results in significant data savings.

The comparison between using a storage system and application-level replication boils down to several key factors: reliability, scalability, and complexity.

**Reliability**: A storage system provides built-in data durability by automatically storing data across multiple systems. It's designed to deliver 99.999999999% durability, ensuring data is reliably stored and available when needed. While replication can increase data availability, it's dependent on the application's implementation. Any bugs or issues in the replication logic can lead to data loss or inconsistencies.

**Scalability**: A storage system is highly scalable by design and can store and retrieve any amount of data from anywhere. As your data grows, the system grows with you. You don't need to worry about outgrowing your storage capacity. Scaling application-level replication can be challenging. As data grows, so does the need for more disk space and compute resources, potentially leading to increased costs and management complexity.

**Complexity**: With the storage system handling replication, the complexity is abstracted away from the user. The user need not concern themselves with the details of how replication is handled. Managing replication at the application level can be complex. It requires careful configuration, monitoring, and potentially significant engineering effort to maintain.

Futhermore, the storage system might be cheaper. For instance, S3 can be more cost-effective because it eliminates the need for additional resources required for application-level replication.  Application-level replication also requires ongoing maintenance, potentially increasing operational costs.

### 5.2 Data Sharding

Data distribution across the cluster is determined based on the `shard_num` setting for a group and the specified `entity` in each resource, be it a stream or measure. The resource’s `name` with its `entity` is the sharding key, guiding data distribution to the appropriate Data Node during write operations.

Liaison Nodes retrieve shard mapping information from Meta Nodes to achieve efficient data routing. This information is used to route data to the appropriate Data Nodes based on the sharding key of the data.

This sharding strategy ensures the write load is evenly distributed across the cluster, enhancing write performance and overall system efficiency. BanyanDB uses a hash algorithm for sharding. The hash function maps the sharding key (resource name and entity) to a node in the cluster. Each shard is assigned to the node returned by the hash function.

### 5.3 Data Write Path

Here's a text-based diagram illustrating the data write path in BanyanDB:

```
User
 |
 | API Request (Write)
 |
 v
------------------------------------
|          Liaison Node             |   <--- Stateless Node, Routes Request
| (Identifies relevant Data Nodes   |
|  and dispatches write request)    |
------------------------------------
       |                            
       v                            
-----------------  -----------------  -----------------
|  Data Node 1  |  |  Data Node 2  |  |  Data Node 3  |
|  (Shard 1)    |  |  (Shard 2)    |  |  (Shard 3)    |
-----------------  -----------------  -----------------

```

1. A user makes an API request to the Liaison Node. This request is a write request, containing the data to be written to the database.
2. The Liaison Node, which is stateless, identifies the relevant Data Nodes that will store the data based on the entity specified in the request.
3. The write request is executed across the identified Data Nodes. Each Data Node writes the data to its shard.

This architecture allows BanyanDB to execute write requests efficiently across a distributed system, leveraging the stateless nature and routing/writing capabilities of the Liaison Node, and the distributed storage of Data Nodes.

## 6. Queries in a Cluster

BanyanDB utilizes a distributed architecture that allows for efficient query processing. When a query is made, it is directed to a Liaison Node.

### 6.1 Query Routing

Liaison Nodes do not use shard mapping information from Meta Nodes to execute distributed queries. Instead, they access all Data Nodes to retrieve the necessary data for queries. As the query load is lower, it is practical for liaison nodes to access all data nodes for this purpose. It may increase network traffic, but simplifies scaling out of the cluster.

Compared to the write load, the query load is relatively low. For instance, in a time series database, the write load is typically 100x higher than the query load. This is because the write load is driven by the number of devices sending data to the database, while the query load is driven by the number of users accessing the data.

This strategy enables scaling out of the cluster. When the cluster scales out, the liaison node can access all data nodes without any mapping info changes. It eliminates the need to backup previous shard mapping information, reducing complexity of scaling out.

### 6.2 Query Execution

Parallel execution significantly enhances the efficiency of data retrieval and reduces the overall query processing time. It allows for faster response times as the workload of the query is shared across multiple shards, each working on their part of the problem simultaneously. This feature makes BanyanDB particularly effective for large-scale data analysis tasks.

In summary, BanyanDB's approach to querying leverages its unique distributed architecture, enabling high-performance data retrieval across multiple shards in parallel.

### 6.3 Query Path

```
User
 |
 | API Request (Query)
 |
 v
------------------------------------
|          Liaison Node            |   <--- Stateless Node, Distributes Query
|  (Access all Data nodes to       |
|  execute distributed queries)    |
------------------------------------
       |              |              |
       v              v              v
-----------------  -----------------  -----------------
|  Data Node 1  |  |  Data Node 2  |  |  Data Node 3  |
|  (Shard 1)    |  |  (Shard 2)    |  |  (Shard 3)    |
-----------------  -----------------  -----------------

```

1. A user makes an API request to the Liaison Node. This request may be a query for specific data.
2. The Liaison Node builds a distributed query to select all data nodes.
3. The query is executed in parallel across all Data Nodes. Each Data Node execute a local query plan to process the data stored in its shard concurrently with the others.
4. The results from each shard are then returned to the Liaison Node, which consolidates them into a single response to the user.

This architecture allows BanyanDB to execute queries efficiently across a distributed system, leveraging the distributed query capabilities of the Liaison Node and the parallel processing of Data Nodes.
