# Property Repair Benchmark Test

## Purpose

This performance evaluation is designed to test the execution efficiency of the backend’s automated Property repair feature. 
It primarily covers the following key features:
1. The cluster is configured with a maximum of three nodes, one group, one shard, and two replicas.
2. Each shard contains **100,000** records, with each record approximately **2KB** in size.

## Requirements

- Docker and Docker Compose
- Go 1.21+

### Building the BanyanDB Docker Image

Please make sure you have the latest version of the BanyanDB codebase, and building the Docker image is essential before running the tests.

```bash
export TARGET_OS=linux
export PLATFORMS=linux/arm64 # please replace to your platform
make clean && make generate && make release && make docker.build
```

## Monitoring

The performance evaluation is primarily conducted by observing logs and monitoring metrics in Prometheus. 

The logs provide clear markers for the start and end times of the backend repair process.
In Prometheus, by visiting `http://localhost:9090`, you can view system performance metrics for each machine in the cluster.

### CPU Usage Monitoring

Use this PromQL query to monitor CPU usage during property repair:

```promql
avg by (instance) (
  rate(process_cpu_seconds_total[1m]) * 100
)
```

## Case 1: Fully Data Property Repair

In the first test case, a brand-new, empty node will be started, 
and **100,000** records will be synchronized to it in a single batch. 
This test is designed to measure the node’s CPU usage and the total time consumed during the process.

### Step 1: Start the Cluster and Load Data

In the first step, a single liaison node and two data nodes will be started, 
with the number of copies in the group set to one. Then, **100,000** records will be written in parallel.

```bash
cd test/property_repair/full_data
# Start the cluster with 2 data nodes
docker-compose -f docker-compose-2nodes.yml up -d
# Load initial data into the cluster
go test . -v -tags initial_load -timeout 2h -count=1
```

### Step 2: Add a New Node and Change Copies Count

In the second step, a new data node will be added to the cluster,
and the number of copies in the group will be changed to two.

```bash
# Add a new data node to the cluster and restart the Prometheus service to monitor the new node
docker-compose -f docker-compose-3nodes.yml up data-node-3 -d --force-recreate prometheus
go test . -v -tags update_copies -count=1
```

Then, wait for the propagation to complete in the cluster.

### Result

After waiting for the Property Repair process to complete, the following information was recorded:
1. **Duration**: The total estimated time taken was approximately **36 minutes**.
2. **CPU Consumption**: The estimated CPU usage on the new node was about **1.4 CPU cores**.

The detailed CPU usage rate is shown in the figure below.

![CPU Usage](full_data/cpu-usage.png)

### Cleanup

```bash
docker-compose -f docker-compose-3nodes.yml down
```

## Case 2: Half-Data Property Repair

In the second test case, three nodes are started, with the group’s number of copies initially set to two. 
First, **50,000** records are written to all three nodes.
Next, the group’s copies' setting is changed to one, and the remaining **50,000** records are written to only two fixed nodes. 
At this point, the third node’s dataset is missing half of the data compared to the other nodes.
Finally, the group’s copies' setting is changed back to two, allowing the backend Property Repair process to perform the data synchronization automatically.

### Start and Load Data, Scale the Copies

This test case can be completed in a single step, with the script executing the operations exactly as described above.

```bash
cd test/property_repair/half_data
docker-compose -f docker-compose-3nodes.yml up -d
go test . -v -tags load_and_scale -timeout 2h -count=1
```

Then, wait for the propagation to complete in the cluster.

### Result

After waiting for the Property Repair process to complete, the following information was recorded:
1. **Duration**: The total estimated time taken was approximately **30 minutes**.
2. **CPU Consumption**: The estimated CPU usage on the new node was about **1.1 CPU cores**.

The detailed CPU usage rate is shown in the figure below.

![CPU Usage](half_data/cpu-usage.png)

### Cleanup

```bash
docker-compose -f docker-compose-3nodes.yml down
```

## Case 3: All Nodes Data are the Same

In the third test case, which represents the most common scenario, all nodes contain identical data.

### Start and Load Data

This test case can also be completed in a single step, with the script executing the operations exactly as described above.

```bash
cd test/property_repair/same_data
docker-compose -f docker-compose-3nodes.yml up -d
go test . -v -tags initial_load -timeout 2h -count=1
```

Then, wait for the propagation to complete in the cluster.

### Result

After waiting for the Property Repair process to complete, the following information was recorded:
1. **Duration**: Almost Less than **1 minute**.
2. **CPU Consumption**: The estimated CPU usage in almost has no impact.

### Cleanup

```bash
docker-compose -f docker-compose-3nodes.yml down
```