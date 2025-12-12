# BanyanDB Architecture Deep Dive

## Table of Contents
1. [Overview](#overview)
2. [What is a Node?](#what-is-a-node)
3. [Node Types](#node-types)
4. [Endpoints](#endpoints)
5. [Liaison Nodes](#liaison-nodes)
6. [Data Nodes](#data-nodes)
7. [Meta Nodes](#meta-nodes)
8. [Communication Flow](#communication-flow)
9. [Data Organization](#data-organization)
10. [Query Processing](#query-processing)
11. [Write Processing](#write-processing)

---

## Overview

BanyanDB is a distributed time-series database designed for observability data (Metrics, Tracing, Logging). It uses a three-tier architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Applications                      │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │      Liaison Nodes             │  ← Stateless Gateway
        │  (HTTP/gRPC Endpoints)         │
        └───────────────┬───────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌───────────────┐              ┌───────────────┐
│  Data Node 1  │              │  Data Node N  │
│  (Storage)    │              │  (Storage)    │
└───────┬───────┘              └───────┬───────┘
        │                               │
        └───────────────┬───────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │      Meta Nodes (etcd)        │
        │   (Metadata & Discovery)      │
        └───────────────────────────────┘
```

---

## What is a Node?

A **Node** in BanyanDB is a **running instance** of the `banyand` server process. Each node represents a single server in the cluster that can perform one or more roles.

### Node Identity

**Location**: `api/common/id.go`, `api/proto/banyandb/database/v1/database.proto`

A node is uniquely identified by:

1. **Node ID**: A unique identifier in the format `host:port`
   - Generated from hostname/IP + gRPC port
   - Example: `"10.0.0.1:17912"` or `"node-1.example.com:17912"`
   - Stored in `node.Metadata.Name`

2. **Addresses**:
   - **gRPC Address**: Where the node accepts gRPC connections (e.g., `"10.0.0.1:17912"`)
   - **HTTP Address**: Where the node accepts HTTP connections (e.g., `"10.0.0.1:17913"`) - optional
   - **Property Gossip gRPC Address**: For property repair gossip protocol (Data nodes only)

3. **Roles**: One or more roles the node performs:
   - `ROLE_DATA`: Stores data
   - `ROLE_LIAISON`: Routes requests
   - `ROLE_META`: Manages metadata (implemented by etcd, not banyand process)

4. **Labels**: Key-value pairs for node selection and filtering
   - Example: `{"region": "us-east", "zone": "a", "instance-type": "large"}`

### Node Structure

**Protobuf Definition** (`database.proto`):
```protobuf
message Node {
  common.v1.Metadata metadata = 1;        // Contains name (NodeID)
  repeated Role roles = 2;                // [ROLE_DATA, ROLE_LIAISON]
  string grpc_address = 3;                // "host:port"
  string http_address = 4;                // "host:port" (optional)
  google.protobuf.Timestamp created_at = 5;
  map<string, string> labels = 6;        // {"key": "value"}
  string property_repair_gossip_grpc_address = 7;
}
```

**Go Structure** (`api/common/id.go`):
```go
type Node struct {
    NodeID                      string            // "host:port"
    GrpcAddress                 string            // "host:port"
    HTTPAddress                 string            // "host:port" (optional)
    PropertyGossipGrpcAddress   string            // "host:port" (Data nodes)
    Labels                      map[string]string // {"key": "value"}
}
```

### Node Lifecycle

1. **Startup**:
   - Node process starts (`banyand data` or `banyand liaison`)
   - Generates Node ID based on host provider:
     - `hostname`: Uses OS hostname
     - `ip`: Uses first non-loopback IP
     - `flag`: Uses `--node-host` flag value
   - Registers itself with Meta Nodes (etcd)

2. **Registration**:
   - Node information stored in etcd: `/nodes/{nodeID}`
   - Other nodes watch for new nodes
   - Liaison nodes update routing tables

3. **Operation**:
   - Node performs its role(s)
   - Responds to health checks
   - Processes requests

4. **Shutdown**:
   - Node deregisters from etcd
   - Other nodes detect removal
   - Routing tables updated

### Node Discovery

**Location**: `banyand/metadata/client.go`, `banyand/queue/pub/client.go`

- **Registration**: When a node starts, it calls `RegisterNode()` to store its info in etcd
- **Discovery**: Other nodes watch `/nodes/` prefix in etcd
- **Updates**: On node add/update/delete, watchers receive notifications
- **Routing**: Liaison nodes use discovered nodes for request routing

### Node Selection

**Location**: `pkg/node/round_robin.go`

Nodes can be filtered and selected using:

1. **Role-based**: Select nodes with specific roles (`ROLE_DATA`, `ROLE_LIAISON`)
2. **Label-based**: Filter by label selectors (e.g., `region=us-east`)
3. **Shard-based**: Select node for specific shard using round-robin

**Example**:
```go
// Select Data nodes for a specific shard
nodeID, err := selector.Pick("measure-minute", "service_cpm", shardID, replicaID)
// Returns: "10.0.0.1:17912"
```

### Process Instance Explained

**What is a Process Instance?**

A **process instance** is a single running execution of a program. When you start a program, the operating system creates a process instance with its own memory space, file handles, and process ID (PID).

**Example: Running Multiple Process Instances**

Let's say you have a server machine `server-1.example.com`:

```bash
# Terminal 1: Start first Data Node process instance
$ banyand data --grpc-port=17912 --etcd-endpoints=http://etcd:2379
# This creates Process Instance #1 (PID: 12345)
# Node ID: "server-1.example.com:17912"
# Role: ROLE_DATA

# Terminal 2: Start second Data Node process instance (on same machine)
$ banyand data --grpc-port=17913 --etcd-endpoints=http://etcd:2379
# This creates Process Instance #2 (PID: 12346)
# Node ID: "server-1.example.com:17913"
# Role: ROLE_DATA

# Terminal 3: Start Liaison Node process instance
$ banyand liaison --grpc-port=17920 --http-port=17921 --etcd-endpoints=http://etcd:2379
# This creates Process Instance #3 (PID: 12347)
# Node ID: "server-1.example.com:17920"
# Role: ROLE_LIAISON
```

**Result**: You now have **3 process instances** running on the same physical machine:
- Process Instance #1: Data Node (port 17912)
- Process Instance #2: Data Node (port 17913)
- Process Instance #3: Liaison Node (port 17920)

Each process instance:
- Has its own **Process ID (PID)**: 12345, 12346, 12347
- Uses its own **memory space** (they don't share memory)
- Has its own **network ports**: 17912, 17913, 17920
- Registers as a **separate logical node** in the cluster

**Real-World Example: Docker Containers**

```bash
# Container 1: Data Node
$ docker run -p 17912:17912 banyandb/banyand data --grpc-port=17912
# Process Instance #1 (inside container, PID: 1)
# Node ID: "container-1:17912"

# Container 2: Data Node  
$ docker run -p 17913:17912 banyandb/banyand data --grpc-port=17912
# Process Instance #2 (inside container, PID: 1)
# Node ID: "container-2:17912"

# Container 3: Liaison Node
$ docker run -p 17920:17912 -p 17921:17913 banyandb/banyand liaison
# Process Instance #3 (inside container, PID: 1)
# Node ID: "container-3:17912"
```

**Key Points**:

1. **One Process Instance = One Logical Node**
   - Each time you run `banyand`, you create a new process instance
   - Each process instance registers as one logical node in the cluster

2. **Multiple Process Instances on Same Machine**
   - You can run multiple `banyand` processes on the same physical server
   - They must use different ports
   - Each becomes a separate logical node

3. **Process Instance Lifecycle**
   ```
   Start Command → OS Creates Process → Process Registers as Node → Operates → Shutdown → Process Terminates
   ```

4. **Process vs Thread**
   - **Process**: Separate execution with own memory (what we're talking about)
   - **Thread**: Shares memory within a process (BanyanDB uses goroutines, which are like threads)

**Example: Checking Process Instances**

```bash
# List all banyand process instances
$ ps aux | grep banyand
user  12345  ...  banyand data --grpc-port=17912    # Process Instance #1
user  12346  ...  banyand data --grpc-port=17913    # Process Instance #2
user  12347  ...  banyand liaison --grpc-port=17920 # Process Instance #3

# Each has different PID and potentially different ports
```

### Physical vs Logical

- **Physical Node**: A single machine/server (hardware)
- **Process Instance**: A running `banyand` program (one execution)
- **Logical Node**: A registered entity in the cluster (one process instance = one logical node)
- **Multiple Process Instances**: Can run multiple `banyand` processes on same physical machine, each becomes a separate logical node

### Standalone Mode

In standalone mode, a single `banyand` process acts as:
- One Liaison Node
- One Data Node  
- One Meta Node (embedded etcd)

But it's still registered as **one logical node** with multiple roles.

---

## Node Types

### 1. Meta Nodes
- **Implementation**: etcd cluster
- **Responsibilities**:
  - Store high-level cluster metadata
  - Node discovery and registration
  - Schema management (Groups, Streams, Measures, Index Rules)
  - Shard allocation information
- **Characteristics**:
  - Should always be odd number (e.g., 3, 5)
  - Provides distributed consensus
  - All nodes register with Meta Nodes on startup

### 2. Liaison Nodes
- **Role**: Stateless gateway/router
- **Responsibilities**:
  - Receive client requests (HTTP/gRPC)
  - Route writes to appropriate Data Nodes
  - Coordinate distributed queries
  - Authentication & authorization
  - TTL enforcement
  - Load balancing
- **Characteristics**:
  - Stateless (can scale horizontally)
  - Can be separated for read/write workloads
  - No data storage

### 3. Data Nodes
- **Role**: Data storage and local query execution
- **Responsibilities**:
  - Store raw time-series data
  - Store metadata and indexes
  - Execute local queries
  - Handle TopN aggregations
  - Data persistence and retention
- **Characteristics**:
  - Stateful (stores data)
  - Scales based on storage needs
  - Organized by shards

---

## Endpoints

### HTTP Endpoints

**Location**: `banyand/liaison/http/server.go`

The HTTP server acts as a gRPC gateway, translating HTTP REST requests to gRPC calls:

- **Port**: Default `17913` (configurable via `--http-port`)
- **Base Path**: `/api`
- **Protocol**: HTTP/1.1 with optional TLS

**Registered Services** (via gRPC Gateway):
- `/api/v1/stream/data` - Stream query/write
- `/api/v1/measure/data` - Measure query/write
- `/api/v1/trace/data` - Trace query/write
- `/api/v1/bydbql/query` - BydbQL query language
- `/api/v1/group` - Group registry
- `/api/v1/stream` - Stream registry
- `/api/v1/measure` - Measure registry
- `/api/v1/index-rule` - Index rule registry
- `/api/v1/property` - Property service
- `/api/healthz` - Health check

**Key Features**:
- Authentication middleware (configurable)
- TLS support with certificate reloading
- Health check endpoint
- gRPC gateway integration

### gRPC Endpoints

**Location**: `banyand/liaison/grpc/server.go`

The gRPC server handles direct gRPC client connections:

- **Port**: Default `17912` (configurable via `--grpc-port`)
- **Protocol**: gRPC with optional TLS

**Registered Services**:
```go
- StreamService (streamv1)
- MeasureService (measurev1)
- TraceService (tracev1)
- BydbQLService (bydbqlv1)
- GroupRegistryService (databasev1)
- StreamRegistryService (databasev1)
- MeasureRegistryService (databasev1)
- IndexRuleRegistryService (databasev1)
- IndexRuleBindingRegistryService (databasev1)
- TopNAggregationRegistryService (databasev1)
- PropertyService (propertyv1)
- PropertyRegistryService (databasev1)
- TraceRegistryService (databasev1)
- SnapshotService (databasev1)
- HealthService (grpc_health_v1)
```

**Key Features**:
- Request validation (grpc-validator)
- Panic recovery
- Authentication interceptors
- Memory pressure protection (load shedding)
- Dynamic buffer sizing based on available memory
- Access logging (optional)

**Interceptors Chain**:
1. Authentication (if enabled)
2. Validator
3. Memory protector (load shedding)
4. Recovery (panic handling)

---

## Liaison Nodes

### Architecture

**Location**: `pkg/cmdsetup/liaison.go`

A Liaison node consists of:

1. **Metadata Client** (`metadata.NewClient`)
   - Connects to etcd (Meta Nodes)
   - Watches schema changes
   - Discovers Data Nodes

2. **Queue Clients**:
   - **Tier1 Client** (`pub.New(metaSvc, ROLE_LIAISON)`)
     - Communicates with other Liaison nodes
     - Used for load balancing writes across Liaison nodes
   - **Tier2 Client** (`pub.New(metaSvc, ROLE_DATA)`)
     - Communicates with Data nodes
     - Routes writes/queries to Data nodes

3. **Node Selectors**:
   - Round-robin selectors for each data type:
     - `measureDataNodeSel` - Selects Data nodes for measure writes
     - `streamDataNodeSel` - Selects Data nodes for stream writes
     - `traceDataNodeSel` - Selects Data nodes for trace writes
   - Uses shard-based routing (see [Node Selection](#node-selection))

4. **Service Components**:
   - **Stream Service** (`stream.NewLiaison`)
   - **Measure Service** (`measure.NewLiaison`)
   - **Trace Service** (`trace.NewLiaison`)
   - **Distributed Query Service** (`dquery.NewService`)
   - **Property Service** (via Data nodes)

5. **Servers**:
   - gRPC Server (`liaison/grpc/server.go`)
   - HTTP Server (`liaison/http/server.go`)

### Node Selection

**Location**: `pkg/node/round_robin.go`

Liaison nodes use **Round-Robin Selectors** to route requests to Data nodes:

1. **Shard Calculation**:
   - Shard ID = hash(resource_name + entity_values) % shard_num
   - Example: `hash("service_cpm:frontend") % 5` → Shard 2

2. **Node Assignment**:
   - Shards are sorted by Group name and Shard ID
   - Assigned to Data nodes in round-robin fashion
   - Example with 3 Data nodes:
     ```
     Group "measure-minute" Shard 0 → Data Node 1
     Group "measure-minute" Shard 1 → Data Node 2
     Group "measure-minute" Shard 2 → Data Node 3
     Group "measure-minute" Shard 3 → Data Node 1 (round-robin)
     ```

3. **Lookup Table**:
   - Maintains mapping: `(group, shardID) → Data Node`
   - Updated when groups are added/modified
   - Sorted for efficient binary search

4. **Node Discovery**:
   - Watches Meta Nodes for node additions/removals
   - Updates node list automatically
   - Filters nodes by label selector (optional)

### Write Flow (Liaison)

1. **Client Request** → Liaison gRPC/HTTP endpoint
2. **Authentication** → Auth interceptor validates request
3. **Shard Calculation** → Calculate shard ID from entity
4. **Node Selection** → Round-robin selector picks Data node
5. **Queue Publishing** → Publish to Data node via Tier2 client
6. **Response** → Return to client

**Code Flow**:
```
liaison/grpc/stream.go:Write()
  → stream/svc_liaison.go:Write()
    → stream/write_liaison.go:handleWrite()
      → node.Selector.Pick(group, name, shardID, replicaID)
      → queue.Client.Publish(topic, message)
```

### Query Flow (Liaison)

1. **Client Request** → Liaison endpoint
2. **Query Planning** → Build distributed query plan
3. **Node Selection** → Select ALL Data nodes (for queries)
4. **Parallel Execution** → Query all Data nodes concurrently
5. **Result Aggregation** → Merge results from all nodes
6. **Response** → Return aggregated results

**Key Difference**: Queries access ALL Data nodes (not shard-specific), while writes are shard-specific.

---

## Data Nodes

### Architecture

**Location**: `pkg/cmdsetup/data.go`

A Data node consists of:

1. **Metadata Client** (`metadata.NewClient`)
   - Connects to etcd
   - Registers itself on startup
   - Watches schema changes

2. **Queue Server** (`sub.NewServer`)
   - Receives messages from Liaison nodes
   - Handles write/query requests
   - Port: Auto-assigned (stored in etcd)

3. **Storage Services**:
   - **Stream Service** (`stream.NewService`)
     - Stores stream data
     - Root path: `--stream-root-path`
   - **Measure Service** (`measure.NewDataSVC`)
     - Stores measure data
     - Root path: `--measure-root-path`
   - **Trace Service** (`trace.NewService`)
     - Stores trace data
     - Root path: `--trace-root-path`
   - **Property Service** (`property.NewService`)
     - Stores properties
     - Uses gossip protocol for repair

4. **Query Processor** (`query.NewService`)
   - Executes local queries
   - Processes TopN aggregations

### Node Registration

**Location**: `banyand/metadata/client.go`

When a Data node starts:

1. **Generate Node Info**:
   ```go
   node := &databasev1.Node{
       Metadata: &commonv1.Metadata{Name: nodeID},
       GrpcAddress: "host:port",
       HttpAddress: "host:port",  // optional
       Roles: []databasev1.Role{ROLE_DATA},
       Labels: map[string]string{...},
   }
   ```

2. **Register with Meta Nodes**:
   - Calls `schemaRegistry.RegisterNode()`
   - Stores in etcd: `/nodes/{nodeID}`
   - Retries on failure

3. **Watch Schema Changes**:
   - Subscribes to Group, Stream, Measure changes
   - Updates local schema cache

### Data Organization

**Location**: `docs/concept/clustering.md`

Data is organized on disk as:
```
<root-path>/
  <group>/
    shard-<shard_id>/
      <segment_id>/
        - data files
        - index files
        - metadata files
```

**Example**:
```
/var/banyandb/
  measure-minute/
    shard-0/
      segment-20240101/
        - primary.data
        - index.idx
    shard-1/
      segment-20240101/
        ...
```

**Segments**:
- Time-based partitions
- Support retention policies
- Can be deleted when expired

### Write Processing (Data Node)

1. **Receive Message** → Queue server receives write request
2. **Deserialize** → Unmarshal protobuf message
3. **Schema Validation** → Validate against schema
4. **Shard Selection** → Get/create shard for shardID
5. **Write to Queue** → Add to write queue
6. **Flush** → Periodically flush to disk
7. **Index Update** → Update indexes

**Code Flow**:
```
queue/sub/sub.go:Send()
  → measure/svc_data.go:Write()
    → measure/write_data.go:handleWrite()
      → wqueue.Shard.Add()
        → Flush to disk
```

### Query Processing (Data Node)

1. **Receive Query** → Queue server receives query request
2. **Parse Query** → Parse query conditions
3. **Index Lookup** → Use indexes to find matching data
4. **Data Retrieval** → Read data from storage
5. **Filter & Aggregate** → Apply filters and aggregations
6. **Return Results** → Send results back to Liaison

---

## Meta Nodes

### Implementation

- **Technology**: etcd (distributed key-value store)
- **Purpose**: Cluster coordination and metadata storage

### Stored Metadata

1. **Nodes** (`/nodes/{nodeID}`):
   - Node information (addresses, roles, labels)
   - Used for discovery

2. **Groups** (`/groups/{groupName}`):
   - Group definitions
   - Shard configuration
   - Resource options

3. **Streams** (`/streams/{group}/{streamName}`):
   - Stream schemas
   - Tag families
   - Entity definitions

4. **Measures** (`/measures/{group}/{measureName}`):
   - Measure schemas
   - Field definitions
   - Aggregation functions

5. **Index Rules** (`/index-rules/{group}/{ruleName}`):
   - Index definitions
   - Tag indexing rules

6. **Shard Allocation**:
   - Which Data node owns which shard
   - Used by Liaison for routing

### Node Discovery

**Process**:
1. Node starts → Registers with etcd
2. Liaison nodes watch `/nodes/` prefix
3. On node add/update → Update routing tables
4. On node delete → Remove from routing tables

**Resilience**:
- If Data node can't reach Meta nodes → Removed from etcd
- Liaison nodes keep Data nodes in routing until unreachable
- Allows continued operation during network partitions

---

## Communication Flow

### Write Request Flow

```
Client
  │
  │ HTTP/gRPC Request
  ▼
Liaison Node (Endpoint)
  │
  │ 1. Authenticate
  │ 2. Calculate Shard ID
  │ 3. Select Data Node
  ▼
Queue Client (Tier2)
  │
  │ gRPC Stream
  ▼
Data Node (Queue Server)
  │
  │ 4. Validate Schema
  │ 5. Write to Queue
  │ 6. Flush to Disk
  ▼
Storage (Local Filesystem)
```

### Query Request Flow

```
Client
  │
  │ HTTP/gRPC Query Request
  ▼
Liaison Node (Endpoint)
  │
  │ 1. Build Query Plan
  │ 2. Select ALL Data Nodes
  ▼
Queue Client (Tier2)
  │
  │ Parallel gRPC Queries
  ├──────────┬──────────┐
  ▼          ▼          ▼
Data Node 1  Data Node 2  Data Node 3
  │          │          │
  │ 3. Execute Local Query
  │ 4. Return Results
  ▼          ▼          ▼
  └──────────┴──────────┘
         │
         │ Aggregate Results
         ▼
Liaison Node
  │
  │ Return Aggregated Results
  ▼
Client
```

### Inter-Node Communication

**Queue System** (`banyand/queue/`):

1. **Publisher** (`queue/pub/`):
   - Maintains gRPC connections to target nodes
   - Implements circuit breaker pattern
   - Health checking
   - Retry logic

2. **Subscriber** (`queue/sub/`):
   - Listens on gRPC port
   - Receives messages
   - Routes to appropriate handlers
   - Returns responses

**Topics**:
- `TopicStreamWrite` - Stream write requests
- `TopicMeasureWrite` - Measure write requests
- `TopicTraceWrite` - Trace write requests
- `TopicStreamQuery` - Stream query requests
- `TopicMeasureQuery` - Measure query requests
- `TopicTraceQuery` - Trace query requests
- `TopicStreamPartSync` - Stream partition sync
- `TopicMeasurePartSync` - Measure partition sync

---

## Data Organization

### Sharding Strategy

**Shard Calculation**:
```go
shardID = hash(resource_name + ":" + entity_values) % shard_num
```

**Example**:
- Group: `measure-minute`, Shard Num: 5
- Measure: `service_cpm`, Entity: `service_id="frontend"`
- Sharding Key: `"service_cpm:frontend"`
- Shard ID: `hash("service_cpm:frontend") % 5` → 2

**Shard Distribution**:
- Shards sorted by (Group, ShardID)
- Assigned to Data nodes round-robin
- Ensures even distribution

### Storage Layout

```
<root-path>/
  <group-name>/
    shard-<shard_id>/
      <segment_id>/          # Time-based segment
        primary.data          # Raw data
        index.idx            # Index data
        metadata.json        # Segment metadata
```

**Segments**:
- Time-based partitions (e.g., daily)
- Support retention policies
- Can be deleted when expired
- Format: `segment-YYYYMMDD` or timestamp-based

---

## Query Processing

### Distributed Query Execution

**Location**: `banyand/dquery/`

1. **Query Planning**:
   - Parse query conditions
   - Determine time range
   - Identify affected groups/shard

2. **Node Selection**:
   - For queries: Select ALL Data nodes
   - For writes: Select specific Data node by shard

3. **Parallel Execution**:
   - Send query to all Data nodes concurrently
   - Each Data node executes local query
   - Returns partial results

4. **Result Aggregation**:
   - Merge results from all nodes
   - Apply final aggregations
   - Sort and limit results

**Why Query All Nodes?**:
- Query load is typically 100x lower than write load
- Simpler scaling (no shard mapping needed)
- Better for time-range queries (data may span shards)

### Local Query Execution

**Location**: `banyand/query/`

1. **Index Lookup**:
   - Use indexes to find matching series
   - Filter by tag values

2. **Data Retrieval**:
   - Read data blocks from storage
   - Decompress data

3. **Filtering**:
   - Apply query conditions
   - Filter by time range
   - Filter by tag/field values

4. **Aggregation**:
   - Group by tags
   - Calculate aggregations (sum, avg, max, min)
   - TopN calculations

---

## Write Processing

### Write Path

1. **Client** → Sends write request to Liaison
2. **Liaison**:
   - Validates request
   - Calculates shard ID
   - Selects target Data node
   - Publishes to queue

3. **Data Node**:
   - Receives message
   - Validates schema
   - Adds to write queue
   - Batches writes
   - Flushes to disk

4. **Storage**:
   - Writes to segment files
   - Updates indexes
   - Updates metadata

### Write Batching

- Writes are batched for efficiency
- Configurable batch size
- Time-based flushing
- Async write queue

### Consistency

- No application-level replication
- Relies on storage system durability (e.g., EBS, S3)
- Storage system handles replication
- Simpler architecture, better performance

---

## Key Design Decisions

### 1. Stateless Liaison Nodes
- **Benefit**: Easy horizontal scaling
- **Trade-off**: No local caching (but can add)

### 2. Query All Nodes Strategy
- **Benefit**: Simple scaling, no shard mapping
- **Trade-off**: More network traffic (acceptable for low query load)

### 3. Storage-Level Replication
- **Benefit**: Simpler code, better performance
- **Trade-off**: Relies on storage system

### 4. Round-Robin Shard Assignment
- **Benefit**: Even distribution, simple logic
- **Trade-off**: Rebalancing requires data movement

### 5. Time-Based Segments
- **Benefit**: Easy retention, efficient queries
- **Trade-off**: May need to query multiple segments

---

## Configuration

### Liaison Node Flags

```bash
# gRPC Server
--grpc-port=17912
--grpc-host=""
--tls=false
--cert-file=""
--key-file=""
--auth-config-file=""
--max-recv-msg-size=16MB

# HTTP Server
--http-port=17913
--http-host=""
--http-tls=false
--http-cert-file=""
--http-key-file=""

# Node Selection
--data-node-selector="key1=value1,key2=value2"

# Metadata
--etcd-endpoints="http://localhost:2379"
```

### Data Node Flags

```bash
# Storage Paths
--stream-root-path="/var/banyandb/stream"
--measure-root-path="/var/banyandb/measure"
--trace-root-path="/var/banyandb/trace"
--property-root-path="/var/banyandb/property"

# Node Configuration
--node-host-provider="hostname"  # hostname|ip|flag
--node-host=""
--grpc-port=<auto>
--property-repair-gossip-grpc-port=<auto>

# Metadata
--etcd-endpoints="http://localhost:2379"
```

---

## Failover & High Availability

### Data Node Failure

1. **Detection**:
   - Liaison nodes detect via health checks
   - Meta nodes detect via heartbeat

2. **Recovery**:
   - Liaison routes to other Data nodes with same shard
   - Queries continue (access all nodes)
   - Writes may fail (need to retry)

3. **Restoration**:
   - When Data node recovers, it re-registers
   - System doesn't automatically replay data
   - Data remains on other nodes (if replicated at storage level)

### Liaison Node Failure

1. **Multiple Liaison Nodes**:
   - Load balancer routes to healthy nodes
   - Stateless design allows easy replacement

2. **No Data Loss**:
   - Liaison nodes don't store data
   - Client can retry on different Liaison node

---

## Performance Considerations

### Write Performance

- **Batching**: Writes are batched for efficiency
- **Async Queue**: Non-blocking write queue
- **Parallel Writes**: Can write to multiple shards concurrently
- **Storage I/O**: Optimized for sequential writes

### Query Performance

- **Indexes**: Tag-based indexes for fast lookups
- **Parallel Execution**: Queries run in parallel across nodes
- **Caching**: Schema caching, index caching
- **Segment Pruning**: Skip segments outside time range

### Scalability

- **Horizontal Scaling**: Add more Data/Liaison nodes
- **Shard Scaling**: Increase shard count per group
- **Load Balancing**: Multiple Liaison nodes for load distribution

---

## Summary

BanyanDB uses a three-tier architecture:

1. **Liaison Nodes**: Stateless gateways that route requests
2. **Data Nodes**: Stateful storage nodes that persist data
3. **Meta Nodes**: etcd cluster for coordination

**Key Features**:
- Shard-based routing for writes
- All-node queries for reads
- Round-robin node selection
- Storage-level replication
- Time-based segments

**Communication**:
- HTTP/gRPC endpoints on Liaison nodes
- gRPC queue system between nodes
- etcd for metadata and discovery

This architecture provides:
- High availability
- Horizontal scalability
- Simple operations
- Good performance for time-series workloads
