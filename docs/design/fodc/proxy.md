# FODC Proxy Development Design

## Overview

The FODC Proxy is the central control plane and data aggregator for the First Occurrence Data Collection (FODC) infrastructure. It acts as a unified gateway that aggregates observability data from multiple FODC Agents (each co-located with a BanyanDB node) and exposes ecosystem-friendly interfaces to external systems such as Prometheus and other observability platforms.

The Proxy provides:

1. **Agent Management**: Registration, health monitoring, and lifecycle management of FODC Agents
2. **Metrics Aggregation**: Collects and aggregates metrics from all agents with enriched metadata
3. **Cluster Topology**: Maintains an up-to-date view of cluster topology, roles, and node states
4. **Configuration Collection**: Aggregates and exposes node configurations for consistency verification

### Responsibilities

**FODC Proxy Core Responsibilities**
- Accept bi-directional gRPC connections from FODC Agents
- Register and track agent lifecycle (online/offline, heartbeat monitoring)
- Aggregate metrics from all agents with node metadata enrichment
- Maintain cluster topology view based on agent registrations
- Collect and expose node configurations for audit and consistency checks
- Expose unified REST/Prometheus-style interfaces for external consumption
- Provide proxy-level metrics (health, agent count, RPC latency, etc.)

## Component Design

### 1. Proxy Components

#### 1.1 Agent Registry Component

**Purpose**: Manages the lifecycle and state of all connected FODC Agents

##### Core Responsibilities

- **Agent Registration**: Accepts agent registration requests via gRPC
- **Health Monitoring**: Tracks agent heartbeat and connection status
- **State Management**: Maintains agent state (online/offline, last heartbeat time)
- **Topology Building**: Aggregates agent registrations into cluster topology view
- **Connection Management**: Handles connection failures, reconnections, and cleanup

##### Core Types

**`AgentInfo`**
```go
type AgentInfo struct {
	NodeID            string                    // Unique node identifier
	NodeRole          databasev1.Role          // Node role (liaison, datanode-hot, etc.)
	PrimaryAddress    Address                  // Primary agent gRPC address
	SecondaryAddresses map[string]Address      // Secondary addresses with names (e.g., "metrics": Address, "gossip": Address)
	Labels            map[string]string        // Node labels/metadata
	RegisteredAt      time.Time                // Registration timestamp
	LastHeartbeat     time.Time               // Last heartbeat timestamp
	Status            AgentStatus               // Current agent status
}

type Address struct {
	IP   string
	Port int
}

type AgentStatus string

const (
	AgentStatusOnline  AgentStatus = "online"
	AgentStatusOffline AgentStatus = "unconnected"
)
```

**`AgentRegistry`**
```go
type AgentRegistry struct {
	agents    map[AgentKey]*AgentInfo      // Map from agent key (IP+port+role+labels) to agent info
	mu        sync.RWMutex                 // Protects agents map
	logger    *logger.Logger
	heartbeatTimeout time.Duration        // Timeout for considering agent offline
}

type AgentKey struct {
	IP     string                 // Primary IP address
	Port   int                    // Primary port
	Role   databasev1.Role       // Node role
	Labels map[string]string      // Node labels (used for key matching)
}
```

##### Key Functions

**`RegisterAgent(ctx context.Context, info *AgentInfo) error`**
- Registers a new agent or updates existing agent information
- Creates AgentKey from primary IP + port + role + labels
- Uses AgentKey as the map key (not nodeID)
- Validates primary address and role
- Updates topology view
- Returns error if registration fails

**`UnregisterAgent(key AgentKey) error`**
- Removes agent from registry using AgentKey
- Cleans up associated resources
- Updates topology view
- Called in the following scenarios:
  - When agent's registration stream closes (connection lost)
  - When agent's all streams are closed and connection is terminated
  - When agent has been offline longer than `--agent-cleanup-timeout` (detected by heartbeat health check)
  - During graceful shutdown or manual agent removal
  - When agent explicitly requests unregistration via stream

**`UpdateHeartbeat(key AgentKey) error`**
- Updates last heartbeat timestamp for agent using AgentKey
- Marks agent as online if it was offline
- Returns error if agent not found

**`GetAgent(ip string, port int, role databasev1.Role, labels map[string]string) (*AgentInfo, error)`**
- Retrieves agent information by primary IP + port + role + labels
- Creates AgentKey from the provided parameters
- Looks up agent in registry using AgentKey
- Returns error if agent not found

**`ListAgents() []*AgentInfo`**
- Returns list of all registered agents
- Thread-safe read operation

**`ListAgentsByRole(role databasev1.Role) []*AgentInfo`**
- Returns agents filtered by role
- Useful for role-specific operations

**`CheckAgentHealth() error`**
- Periodically checks agent health based on heartbeat timeout
- Marks agents as offline if heartbeat timeout exceeded
- Continuously runs heartbeat health checks regardless of cleanup timeout
- Unregisters agents that have been offline longer than `--agent-cleanup-timeout`
- Agents that cannot maintain connection will be removed after the cleanup timeout period
- Returns aggregated health status

##### Configuration Flags

**`--agent-heartbeat-timeout`**
- **Type**: `duration`
- **Default**: `30s`
- **Description**: Timeout for considering an agent offline if no heartbeat received

**`--max-agents`**
- **Type**: `int`
- **Default**: `1000`
- **Description**: Maximum number of agents that can be registered

**`--agent-cleanup-timeout`**
- **Type**: `duration`
- **Default**: `5m`
- **Minimum**: Must be greater than `--agent-heartbeat-timeout`
- **Description**: Timeout for automatically unregistering agents that have been offline. Agents that cannot maintain connection will be removed after being offline longer than this timeout. The heartbeat health check continues running regardless of this timeout. This timeout must be greater than `--agent-heartbeat-timeout` to allow for proper health checking.

#### 1.2 gRPC Server Component

**Purpose**: Handles bi-directional gRPC communication with FODC Agents

##### Core Responsibilities

- **Agent Connection Handling**: Accepts and manages gRPC connections from agents
- **Streaming Support**: Supports bi-directional streaming for metrics
- **Protocol Implementation**: Implements FODC gRPC service protocol
- **Connection Lifecycle**: Manages connection establishment, maintenance, and cleanup

##### Core Types

**`FODCService`** (gRPC Service Implementation)
```go
type FODCService struct {
	registry        *AgentRegistry
	metricsAggregator *MetricsAggregator
	logger          *logger.Logger
}

// Example gRPC service methods (to be defined in proto)
// RegisterAgent(stream RegisterRequest) (stream RegisterResponse) error
// StreamMetrics(stream MetricsMessage) (stream MetricsRequest) error
```

**`AgentConnection`**
```go
type AgentConnection struct {
	Key         AgentKey              // Agent key (IP+port+role+labels) for registry lookup
	NodeID      string                // Node ID (for reference, not used as key)
	Stream      grpc.ServerStream
	Context     context.Context
	Cancel      context.CancelFunc
	LastActivity time.Time
}
```

##### Key Functions

**`RegisterAgent(stream FODCService_RegisterAgentServer) error`**
- Handles bi-directional agent registration stream
- Receives registration requests from agent (includes primary address, role, labels)
- Creates AgentKey from primary IP + port + role + labels
- Validates registration information
- Registers agent with AgentRegistry using AgentKey (not nodeID)
- Sends registration responses with assigned session ID
- Maintains stream for heartbeat and re-registration

**`StreamMetrics(stream FODCService_StreamMetricsServer) error`**
- Handles bi-directional metrics streaming
- Sends metrics requests from Proxy to agent (on-demand collection)
- Receives metrics from agent at Proxy in response to requests
- Proxy initiates by sending a metrics request via MetricsRequest when an external client queries metrics
- Agent responds with MetricsMessage containing the collected metrics
- Manages stream lifecycle

##### Connection Lifecycle Management

**Stream Closure Handling**
- When a stream closes (due to network error, agent shutdown, or timeout), the gRPC server should:
  1. Detect stream closure via context cancellation or stream error
  2. Extract agent key (primary IP + port + role + labels) from the connection
  3. Check if this is the last active stream for the agent
  4. If all streams are closed, call `AgentRegistry.UnregisterAgent(agentKey)` to clean up
  5. Update topology to reflect agent offline status

**Graceful vs. Ungraceful Disconnection**
- **Graceful**: Agent sends explicit disconnect message before closing stream → immediate unregistration
- **Ungraceful**: Stream closes unexpectedly → unregistration happens after detecting all streams closed
- **Heartbeat Timeout**: Agent marked offline by `CheckAgentHealth()` → unregistration occurs after being offline longer than `--agent-cleanup-timeout`. Heartbeat health checks continue running regardless.

##### Configuration Flags

**`--grpc-listen-addr`**
- **Type**: `string`
- **Default**: `:17900`
- **Description**: gRPC server address where the Proxy listens for agent connections

**`--grpc-max-msg-size`**
- **Type**: `int`
- **Default**: `4194304` (4MB)
- **Description**: Maximum message size for gRPC messages

#### 1.3 Metrics Aggregator Component

**Purpose**: Aggregates and enriches metrics from all agents

##### Core Responsibilities

- **On-Demand Metrics Collection**: Requests metrics from agents via gRPC streams when external clients query metrics
- **Metrics Request Coordination**: Coordinates metrics requests to multiple agents concurrently
- **Metadata Enrichment**: Adds node metadata (role, ID, labels) to metrics
- **Normalization**: Normalizes metric formats and labels across agents
- **Time Window Filtering**: Filters metrics by time window when requested by external clients (agents filter based on Flight Recorder data)

##### Core Types

**`AggregatedMetric`**
```go
type AggregatedMetric struct {
	Name        string                 // Metric name
	Labels      map[string]string      // Metric labels (including node metadata)
	Value       float64                // Metric value
	Timestamp   time.Time              // Metric timestamp
	NodeID      string                 // Source node ID
	NodeRole    databasev1.Role       // Source node role
	Description string                 // Metric description/HELP text
}
```

**`MetricsAggregator`**
```go
type MetricsAggregator struct {
	registry      *AgentRegistry
	grpcService   *FODCService        // For requesting metrics from agents
	mu            sync.RWMutex
	logger        *logger.Logger
}

type MetricsFilter struct {
	NodeIDs []string              // Filter by specific node IDs (empty = all nodes)
	Role    databasev1.Role       // Filter by node role (optional)
	StartTime *time.Time          // Start time for time window (optional)
	EndTime   *time.Time          // End time for time window (optional)
}
```

##### Key Functions

**`CollectMetricsFromAgents(ctx context.Context, filter *MetricsFilter) ([]*AggregatedMetric, error)`**
- Requests metrics from all agents (or filtered agents) when external client queries
- Sends metrics request via StreamMetrics() to each agent with time window filter (if specified)
- Agents retrieve metrics from Flight Recorder (in-memory storage) filtered by time window and respond
- Waits for MetricsMessage responses from agents (with timeout)
- Enriches metrics with node metadata from registry
- Combines metrics from all agents into a single list
- Returns aggregated metrics (not stored, only returned)
- Returns error if collection fails

**`GetLatestMetrics(ctx context.Context) ([]*AggregatedMetric, error)`**
- Triggers on-demand collection from all agents
- Calls CollectMetricsFromAgents() with no time filter to get current metrics
- Returns latest metrics from all agents
- Used for `/metrics-windows` endpoint without time parameters
- Returns error if collection fails

**`GetMetricsWindow(ctx context.Context, startTime, endTime time.Time, filter *MetricsFilter) ([]*AggregatedMetric, error)`**
- Triggers on-demand collection from all agents
- Calls CollectMetricsFromAgents() with time window filter
- Agents filter metrics from Flight Recorder by the specified time range
- Returns metrics within specified time range
- Used for `/metrics-windows` endpoint with time parameters
- Returns error if collection fails

##### Configuration Flags

*Note: No configuration flags needed for MetricsAggregator since metrics are collected on-demand and not stored.*

#### 1.4 HTTP/REST API Server Component

**Purpose**: Exposes REST and Prometheus-style endpoints for external consumption

##### Core Responsibilities

- **REST API**: Implements REST endpoints for cluster topology and configuration
- **Prometheus Integration**: Exposes Prometheus-compatible metrics endpoints
- **Request Handling**: Handles HTTP requests and routes to appropriate handlers
- **Response Formatting**: Formats responses in appropriate formats (JSON, Prometheus text)

##### API Endpoints

**`GET /metrics`**
- Returns latest metrics from all agents (on-demand collection, not stored in Proxy)
- Includes node metadata (role, ID, labels)
- Format: Prometheus text format
- Query parameters:
  - `node_id` (optional): Filter by node ID
  - `role` (optional): Filter by role (liaison, datanode-hot, etc.)
- Used by: Prometheus scrapers, monitoring systems
- Note: Metrics are collected on-demand from agents' Flight Recorder. No metrics are stored in the Proxy.

**`GET /metrics-windows`**
- Returns metrics from all agents (on-demand collection, not stored in Proxy)
- Includes node metadata (role, ID, labels)
- Format: Prometheus text format
- Query parameters:
  - `start_time`: Start time for time window (optional) - filters metrics by start time (agents filter from Flight Recorder)
  - `end_time`: End time for time window (optional) - filters metrics by end time (agents filter from Flight Recorder)
  - `node_id`: Filter by node ID (optional)
  - `role`: Filter by node role (optional)

**`GET /cluster`**
- Returns cluster topology and node status
- Format: JSON
- Response includes:
  - List of all registered nodes
  - Node identity (ID, name, address)
  - Node role
  - Node labels
  - Agent status (online/offline, last heartbeat)
  - Runtime metrics (optional)
- Note: Node information is obtained from agent registration data stored in AgentRegistry

**`GET /cluster/config`**
- Returns node configurations
- Format: JSON
- Query parameters:
  - `node_id`: Filter by node ID (optional)
  - `role`: Filter by node role (optional)

**`GET /health`**
- Health check endpoint
- Format: JSON
- Returns proxy health status

##### Core Types

**`APIServer`**
```go
type APIServer struct {
	metricsAggregator   *MetricsAggregator
	server              *http.Server
	logger              *logger.Logger
}
```

##### Key Functions

**`Start(listenAddr string) error`**
- Starts HTTP server
- Registers all API endpoints
- Returns error if start fails

**`Stop() error`**
- Gracefully stops HTTP server
- Waits for in-flight requests
- Returns error if stop fails

##### Configuration Flags

**`--http-listen-addr`**
- **Type**: `string`
- **Default**: `:17901`
- **Description**: HTTP server listen address

**`--http-read-timeout`**
- **Type**: `duration`
- **Default**: `10s`
- **Description**: HTTP read timeout

**`--http-write-timeout`**
- **Type**: `duration`
- **Default**: `10s`
- **Description**: HTTP write timeout

### 2. Agent Components

**Purpose**: Components that run within FODC Agents to communicate with the Proxy

#### 2.1 Proxy Client Component

**Purpose**: Manages connection and communication with the FODC Proxy

##### Core Responsibilities

- **Connection Management**: Establishes and maintains gRPC connection to Proxy
- **Registration**: Registers agent with Proxy on startup
- **Heartbeat Management**: Sends periodic heartbeats to maintain connection
- **Stream Management**: Manages bi-directional streams for metrics
- **Reconnection Logic**: Handles connection failures and automatic reconnection

##### Core Types

**`ProxyClient`**
```go
type ProxyClient struct {
	proxyAddr      string
	nodeID         string
	nodeRole       databasev1.Role
	labels         map[string]string
	conn           *grpc.ClientConn
	client         fodcv1.FODCServiceClient
	sessionID      string
	heartbeatTicker *time.Ticker
	mu             sync.RWMutex
	logger         *logger.Logger
}

type MetricsRequestFilter struct {
	StartTime *time.Time  // Start time for time window (optional)
	EndTime   *time.Time  // End time for time window (optional)
}
```

**Note**: The ProxyClient integrates with Flight Recorder, which continuously collects and stores metrics in-memory. When the Proxy requests metrics, the agent retrieves them from Flight Recorder rather than collecting them on-demand.

##### Key Functions

**`Connect(ctx context.Context) error`**
- Establishes gRPC connection to Proxy
- Returns error if connection fails

**`StartRegistrationStream(ctx context.Context) error`**
- Establishes bi-directional registration stream with Proxy
- Sends initial RegisterRequest with node ID, role, and labels
- Receives RegisterResponse with session ID and heartbeat interval
- Maintains stream for periodic heartbeat and re-registration
- Returns error if stream establishment fails

**`StartMetricsStream(ctx context.Context) error`**
- Establishes bi-directional metrics stream with Proxy
- Maintains stream for on-demand metrics requests
- Listens for metrics request commands from Proxy
- When request received, retrieves metrics from Flight Recorder and sends response
- Returns error if stream fails

**`RetrieveAndSendMetrics(ctx context.Context, filter *MetricsRequestFilter) error`**
- Retrieves metrics from Flight Recorder when requested by Proxy
- Queries Flight Recorder (in-memory storage) for metrics
- Flight Recorder contains continuously collected metrics from:
  * BanyanDB /metrics endpoint scraping
  * KTM/OS telemetry collection
  * Other observability sources
- Filters metrics by time window if specified in request
- Packages metrics in MetricsMessage
- Sends metrics to Proxy via StreamMetrics() stream
- Includes node ID, session ID, and timestamp
- Returns error if retrieval or send fails

**`SendHeartbeat(ctx context.Context) error`**
- Sends heartbeat to Proxy
- Updates connection status
- Returns error if heartbeat fails

**`Disconnect() error`**
- Closes connection to Proxy
- Cleans up resources
- Returns error if disconnect fails

##### Configuration Flags

**`--proxy-addr`**
- **Type**: `string`
- **Default**: `localhost:17900`
- **Description**: FODC Proxy gRPC address

**`--node-id`**
- **Type**: `string`
- **Default**: (required, no default)
- **Description**: Unique identifier for this BanyanDB node. Must be unique across the cluster. Typically matches the node's identity in the BanyanDB cluster.

**`--node-role`**
- **Type**: `string`
- **Default**: (required, no default)
- **Description**: Role of this BanyanDB node. Valid values: `liaison`, `datanode-hot`, `datanode-warm`, `datanode-cold`, etc. Must match the node's actual role in the cluster.

**`--node-labels`**
- **Type**: `string` (comma-separated key=value pairs)
- **Default**: (optional)
- **Description**: Labels/metadata for this node. Format: `key1=value1,key2=value2`. Examples: `zone=us-west-1,env=production`. Used for filtering and grouping nodes in the Proxy.

**`--heartbeat-interval`**
- **Type**: `duration`
- **Default**: `10s`
- **Description**: Interval for sending heartbeats to Proxy. Note: The Proxy may override this value in RegisterResponse.

**`--reconnect-interval`**
- **Type**: `duration`
- **Default**: `5s`
- **Description**: Interval for reconnection attempts when connection to Proxy is lost

## API Design

### gRPC API

#### Service Definition

```protobuf
syntax = "proto3";

package banyandb.fodc.v1;

import "google/protobuf/timestamp.proto";

service FODCService {
  // Bi-directional stream for agent registration and heartbeat
  rpc RegisterAgent(stream RegisterRequest) returns (stream RegisterResponse);
  
  // Bi-directional stream for metrics
  rpc StreamMetrics(stream MetricsMessage) returns (stream MetricsRequest);
}

message RegisterRequest {
  string node_id = 1;
  string node_role = 2;
  map<string, string> labels = 3;
  Address primary_address = 4;
  map<string, Address> secondary_addresses = 5;
}

message Address {
  string ip = 1;
  int32 port = 2;
}

message RegisterResponse {
  string session_id = 1;
  bool success = 2;
  string message = 3;
  int64 heartbeat_interval_seconds = 4;
}

message MetricsMessage {
  string node_id = 1;
  string session_id = 2;
  repeated Metric metrics = 3;
  google.protobuf.Timestamp timestamp = 4;
}

message Metric {
  string name = 1;
  map<string, string> labels = 2;
  double value = 3;
  string description = 4;
}

message MetricsRequest {
  google.protobuf.Timestamp start_time = 1;  // Optional start time for time window
  google.protobuf.Timestamp end_time = 2;    // Optional end time for time window
}
```

### REST API

#### Endpoints

**`GET /metrics`**
- **Description**: Returns latest metrics from all agents (on-demand collection, not stored in Proxy)
- **Query Parameters**:
  - `node_id` (optional): Filter by node ID
  - `role` (optional): Filter by role (liaison, datanode-hot, etc.)
- **Response**: Prometheus text format with node metadata labels
- **Note**: Metrics are collected on-demand from agents' Flight Recorder. No metrics are stored in the Proxy.
- **Example**:
```
# HELP banyandb_stream_tst_inverted_index_total_doc_count Total document count
# TYPE banyandb_stream_tst_inverted_index_total_doc_count gauge
banyandb_stream_tst_inverted_index_total_doc_count{index="test",node_id="node1",node_role="datanode-hot"} 12345
```

**`GET /metrics-windows`**
- **Description**: Returns metrics from all agents (on-demand collection, not stored in Proxy)
- **Query Parameters**:
  - `start_time` (optional): RFC3339 timestamp - filters metrics by start time (agents filter from Flight Recorder)
  - `end_time` (optional): RFC3339 timestamp - filters metrics by end time (agents filter from Flight Recorder)
  - `node_id` (optional): Filter by node ID
  - `role` (optional): Filter by role (liaison, datanode-hot, etc.)
- **Response**: Prometheus text format with node metadata labels
- **Note**: Metrics are collected on-demand from agents' Flight Recorder. No metrics are stored in the Proxy.
- **Example**:
```
# HELP banyandb_stream_tst_inverted_index_total_doc_count Total document count
# TYPE banyandb_stream_tst_inverted_index_total_doc_count gauge
banyandb_stream_tst_inverted_index_total_doc_count{index="test",node_id="node1",node_role="datanode-hot"} 12345
```

**`GET /cluster`**
- **Description**: Returns cluster topology and node information
- **Response**: JSON
- **Example**:
```json
{
  "nodes": [
    {
      "node_id": "node1",
      "node_role": "liaison",
      "primary_address": {
        "ip": "192.168.1.1",
        "port": 17900
      },
      "secondary_addresses": {
        "metrics": {
          "ip": "192.168.1.1",
          "port": 17901
        },
        "admin": {
          "ip": "192.168.1.1",
          "port": 17902
        }
      },
      "labels": {
        "zone": "us-west-1"
      },
      "status": "online",
      "last_heartbeat": "2024-01-01T12:00:00Z",
      "runtime_metrics": {
        "load": 0.5,
        "health": "healthy"
      }
    }
  ],
  "updated_at": "2024-01-01T12:00:00Z"
}
```

**`GET /cluster/config`**
- **Description**: Returns node configurations
- **Query Parameters**:
  - `node_id` (optional): Filter by node ID
  - `role` (optional): Filter by role
- **Response**: JSON
- **Example**:
```json
{
  "configurations": [
    {
      "node_id": "node1",
      "node_role": "liaison",
      "startup_params": {
        "--listen-addr": ":17900",
        "--data-path": "/data"
      },
      "runtime_config": {
        "max_connections": 1000
      },
      "collected_at": "2024-01-01T12:00:00Z"
    }
  ]
}
```

**`GET /health`**
- **Description**: Health check endpoint
- **Response**: JSON
- **Example**:
```json
{
  "status": "healthy",
  "agents_online": 5,
  "agents_total": 5,
  "uptime_seconds": 3600
}
```

## Data Flow

### Agent Registration Flow

```
1. FODC Agent Starts
   - Reads node information from configuration flags:
     * --node-id: Unique node identifier
     * --node-role: Node role (liaison, datanode-hot, etc.)
     * --node-labels: Optional node labels/metadata
   ↓
2. Agent Connects to Proxy via gRPC
   - Uses --proxy-addr flag to determine Proxy address
   ↓
3. Agent Establishes RegisterAgent() Bi-directional Stream
   - Opens stream to Proxy
   ↓
4. Agent Sends RegisterRequest
   - Sends node ID, role, labels from configuration flags
   ↓
5. Proxy Validates Registration
   - Checks node ID uniqueness
   - Validates role
   ↓
6. Proxy Registers Agent
   - Creates AgentInfo in AgentRegistry
   - Updates ClusterTopology
   ↓
7. Proxy Sends RegisterResponse via Stream
   - Session ID for subsequent requests
   - Heartbeat interval
   ↓
8. Agent Maintains Registration Stream
   - Sends periodic heartbeats via RegisterRequest
   - Receives RegisterResponse updates
   - Stream remains open for re-registration if needed
   ↓
9. Agent Establishes Metrics Stream
   - StreamMetrics() for metrics collection
```

### Metrics Collection Flow

```
1. External Client Requests Metrics
   - GET /metrics (latest metrics from all agents)
   - GET /metrics-windows (agent metrics with time window)
   ↓
2. APIServer Receives Request
   - Routes to metrics handler
   - Extracts query parameters (start_time, end_time, node_id, role)
   ↓
3. Proxy Requests Metrics from All Agents
   - For each registered agent, sends metrics request via StreamMetrics()
   - For `/metrics`: requests latest metrics (no time window filter)
   - For `/metrics-windows`: includes time window filter (start_time, end_time) if specified
   - Uses bi-directional stream to request current metrics
   - May filter by node_id or role if specified in query
   ↓
4. Agents Retrieve Metrics from Flight Recorder
   - Each agent receives metrics request via StreamMetrics() with time window filter (if specified)
   - Agent retrieves metrics from Flight Recorder (in-memory storage)
   - Flight Recorder continuously collects and stores metrics from:
     * BanyanDB /metrics endpoint scraping
     * KTM/OS telemetry collection
   - Agent filters metrics by time window if specified in MetricsRequest
   - Packages filtered metrics in MetricsMessage
   ↓
5. Agents Send Metrics to Proxy
   - Each agent sends MetricsMessage via StreamMetrics()
   - Includes node ID, timestamp, and collected metrics
   ↓
6. Proxy Receives Metrics from All Agents
   - Collects MetricsMessage from each agent
   - Waits for responses from all requested agents (with timeout)
   ↓
7. Proxy Enriches Metrics
   - Adds node metadata (role, ID, labels) to each metric
   - Normalizes metric format across agents
   ↓
8. MetricsAggregator Aggregates Metrics
   - Combines metrics from all agents into a single list
   - Filters by time window if specified (agents already filtered by time window from Flight Recorder)
   - Associates metrics with node ID and role
   - Does not store metrics (on-demand collection only)
   ↓
9. Proxy Formats and Returns Response
   - Converts aggregated metrics to Prometheus text format
   - Includes node metadata labels
   - Returns HTTP 200 response to external client
```

### Metrics Query Flow

**Note**: This flow is integrated with Metrics Collection Flow. When an external request comes in, the Proxy requests metrics from all agents. Agents retrieve metrics from Flight Recorder (in-memory storage) which continuously collects metrics.

**For `/metrics` endpoint (Latest metrics from all agents)**:
```
1. External Client Requests Latest Metrics
   - GET /metrics?node_id=X&role=Y (optional filters)
   ↓
2. Triggers Metrics Collection Flow
   - Proxy requests latest metrics from all agents (or filtered agents)
   - No time window filter (returns latest metrics)
   - See "Metrics Collection Flow" above for detailed steps
   ↓
3. Response Sent to Client
   - HTTP 200 with Prometheus text format
   - Includes aggregated latest metrics from all agents with node metadata
```

**For `/metrics-windows` endpoint (Agent metrics)**:
```
1. External Client Requests Agent Metrics
   - GET /metrics-windows?start_time=X&end_time=Y&node_id=Z&role=W
   ↓
2. Triggers Metrics Collection Flow
   - Proxy requests metrics from all agents (or filtered agents)
   - See "Metrics Collection Flow" above for detailed steps
   ↓
3. Response Sent to Client
   - HTTP 200 with Prometheus text format
   - Includes aggregated metrics from all agents with node metadata
```

## Testing Strategy

### Unit Testing

**Agent Registry Package**
- Test `RegisterAgent()` with valid and invalid inputs
- Test `UnregisterAgent()` cleanup behavior
- Test `UpdateHeartbeat()` timestamp updates
- Test `GetAgent()` and `ListAgents()` retrieval
- Test `ListAgentsByRole()` filtering
- Test `CheckAgentHealth()` timeout detection
- Test concurrent registration/unregistration
- Test error handling for duplicate registrations

**gRPC Service Package**
- Test `RegisterAgent()` bi-directional streaming
- Test `StreamMetrics()` bi-directional streaming
- Test connection lifecycle management
- Test error handling for invalid requests
- Test stream reconnection behavior
- Test concurrent stream handling

**Metrics Aggregator Package**
- Test `CollectMetricsFromAgents()` on-demand collection
- Test `GetLatestMetrics()` without time filter
- Test `GetMetricsWindow()` with time filter
- Test metrics enrichment with node metadata
- Test concurrent metrics collection from multiple agents
- Test time window filtering (agents filter from Flight Recorder)
- Test node and role filtering

**HTTP/REST API Package**
- Test all REST endpoints (GET/POST handlers)
- Test request validation
- Test response formatting (JSON, Prometheus text)
- Test query parameter parsing
- Test error handling and status codes
- Test concurrent request handling
- Test rate limiting (if implemented)

### Integration Testing

**Test Case 1: Agent Registration and Metrics Flow**
- Start Proxy server
- Start mock Agent
- Agent registers with Proxy
- Query `/metrics-windows` endpoint (triggers metrics request)
- Verify Proxy requests metrics from agent via StreamMetrics()
- Verify agent retrieves metrics from Flight Recorder and sends response
- Verify metrics appear in `/metrics-windows` endpoint response
- Verify agent appears in `/cluster` endpoint
- Stop agent and verify offline status

**Test Case 2: Metrics Time Window**
- Start Proxy and multiple Agents
- Query `/metrics-windows` with time range (triggers metrics request)
- Verify Proxy requests metrics from all agents
- Verify agents retrieve metrics from Flight Recorder and send responses
- Verify metrics are filtered by time window if specified
- Test edge cases (empty window, full window)

**Test Case 3: Agent Reconnection**
- Start Proxy and Agent
- Agent registers with Proxy
- Query `/metrics-windows` endpoint (triggers metrics collection)
- Simulate agent disconnection
- Verify agent marked as offline
- Query `/metrics-windows` endpoint again
- Verify Proxy handles missing agent gracefully
- Agent reconnects and re-registers
- Verify agent marked as online
- Query `/metrics-windows` endpoint again
- Verify metrics collection resumes successfully

**Test Case 4: Multiple Agents and Roles**
- Start Proxy
- Start multiple Agents with different roles
- Verify topology shows all nodes correctly
- Verify metrics aggregation includes all nodes
- Test role-based filtering in APIs

### E2E Testing

**Test Case 1: Full FODC Proxy Workflow**
- Deploy Proxy in test environment
- Deploy multiple Agents (liaison, datanode-hot, datanode-warm)
- Agents register with Proxy
- External Prometheus scraper queries `/metrics-windows` endpoint
- Verify Proxy requests metrics from all agents
- Verify agents retrieve metrics from Flight Recorder and respond
- Verify metrics include node metadata
- Verify cluster topology API

**Test Case 2: High Availability and Scalability**
- Deploy Proxy with multiple agent connections (100+)
- Verify Proxy handles concurrent connections
- Verify on-demand metrics collection performance
- Verify Proxy does not store metrics (memory efficient)
- Test agent reconnection under load

**Test Case 3: Failure Scenarios**
- Test Proxy behavior when agents disconnect
- Test Proxy behavior with invalid agent data
- Test Proxy behavior with network partitions
- Test Proxy recovery after failures
- Verify data consistency

**Test Case 4: Prometheus Integration**
- Deploy Proxy with Prometheus
- Configure Prometheus to scrape `/metrics` and `/metrics-windows`
- Verify metrics appear in Prometheus
- Verify metric labels and metadata
- Test Prometheus query performance
