### Handoff Queue for BanyanDB Trace Model - Design Document

#### 1. Overview

This document outlines the design for a **Handoff Queue** mechanism in the BanyanDB trace model, to be implemented on the liaison nodes. The primary purpose of this feature is to handle temporary unavailability of data nodes. When a liaison node attempts to send a data part to a data node that is offline, instead of discarding it, the data part will be placed in a persistent handoff queue. A background process will monitor the status of offline nodes. Once a node comes back online, the queued data for that node will be replayed.

This mechanism builds upon the existing `wqueue` (write queue), which is a `tsTable` instance used for ingesting trace data. The handoff queue will act as a temporary, persistent buffer for data parts that cannot be delivered to their destination data nodes immediately.

#### 2. Motivation

In a distributed system like BanyanDB, nodes can become temporarily unavailable due to network issues, restarts, or failures. Without a handoff mechanism, data intended for an offline node would be dropped, leading to data loss. The handoff queue ensures data durability and eventual consistency by safely storing and later delivering this data.

#### 3. Architecture

The handoff queue will be a new component within the liaison node. It will interact with the existing `wqueue` and the data-sending logic.

The main components of this new feature are:

1.  **Handoff Controller**: A central component managing the handoff process. It will be responsible for:
    *   Receiving a list of *online* nodes from the `syncer` after a sync operation.
    *   Comparing this with the full list of data nodes to determine which nodes were offline.
    *   Persisting the data part payload once into a shared content store and appending a per-node reference for each offline node.
    *   Coordinating with the Node Status Monitor to trigger replays when nodes come back online.
2.  **Shared Content Store + Per-node Reference Queues**: A persistent, on-disk shared store holds each part payload exactly once, keyed by a stable `partId` or content hash. For each data node, a lightweight reference queue stores only references to `partId`s that still require delivery to that node. This layout ensures that even if multiple nodes are offline, the payload is not duplicated. The locations are configurable, for example: shared store at `<root-path>/handoff/store/` and per-node references at `<root-path>/handoff/refs/<data-node-address>/`.
3.  **Node Status Monitor**: A background service on the liaison node that periodically checks the health of all configured data nodes. It will maintain the status of each data node (online/offline) and notify the Handoff Controller when a node's status changes.

#### 4. Data Flow

*   **Normal Flow**:
    1.  The liaison node receives trace data and writes it to the `wqueue`.
    2.  As `wqueue` flushes data, it creates parts.
    3.  The part is passed to the `syncer`, which sends it to all available data nodes.
    4.  The `syncer` sends a `syncIntroduction` to the `introducer`.
    5.  The `introducer` processes the introduction and removes the part from the `wqueue`.

*   **Node Offline Flow**:
    1.  The liaison node receives trace data and writes it to the `wqueue`.
    2.  The `wqueue` flushes a part, which is then handled by the `syncer`.
    3.  The `syncer` attempts to send the part to all configured data nodes, keeping track of which sends are successful.
    4.  The `syncer` reports the list of *online* nodes (to which it successfully sent the data) to the `HandoffController`.
    5.  The `HandoffController` compares the list of online nodes with the full list of all data nodes to calculate the set of *offline* nodes.
    6.  The `HandoffController` writes the part payload once to the shared content store (idempotently, if not already present) and appends a reference to that `partId` in each identified offline node's reference queue.
    7.  The `syncer` sends a `syncIntroduction` to the `introducer`, signifying that the sync operation is complete from its perspective.
    8.  The `introducer` receives the `syncIntroduction` and removes the part from the `wqueue`. The part payload is now durably stored in the shared content store with per-node references for all offline nodes.

*   **Node Recovery Flow**:
    1.  The Node Status Monitor detects that a previously offline data node is now online.
    2.  It notifies the Handoff Controller.
    3.  The Handoff Controller starts a replay process for that node's handoff queue.
    4.  Parts are read from the handoff queue and sent sequentially to the recovered data node.
    5.  Once a part is successfully sent, it is removed from the handoff queue.

#### 5. Detailed Design

##### 5.1. Handoff Queue Implementation

*   A single shared `tsTable` (or equivalent) will store part payloads keyed by a stable `partId` or content hash. Each payload is persisted exactly once regardless of how many nodes are offline.
*   For each data node defined in `--data-node-list`, a lightweight reference queue is maintained to store pending `partId` references for that node. This can be implemented as a small `tsTable` or append-only log containing metadata such as `partId`, size, and enqueue timestamp.
*   The directory structure will be: shared store under `<root>/handoff/store/` and per-node reference queues under `<root>/handoff/refs/<node_id>/`, where `node_id` is a unique identifier for the data node.
*   Insertion into the shared store is idempotent. If a payload with the same `partId` already exists, only new per-node references are appended.

##### 5.2. Node Status Detection

*   A new service, `NodeStatusMonitor`, will be added to the liaison node.
*   On startup, it will parse the `--data-node-list` flag to get the list of data nodes to monitor.
*   It will leverage the existing publisher client in `banyand/queue/pub` to manage connections and health status of data nodes.
*   For each data node address, a connection will be established. The `pub` client implementation already includes a mechanism to monitor the gRPC connection state. A node is considered "UP" if its gRPC connection is in the `READY` state, and "DOWN" otherwise.
*   This approach reuses the existing health-checking and connection management logic, avoiding the need for a separate gRPC health check mechanism.
*   The `NodeStatusMonitor` will get the health status from the `pub` client.
*   When a node's status changes from UP to DOWN or DOWN to UP, the monitor will use a channel or callback to notify the Handoff Controller.

##### 5.2.1. Implementation using `banyand/queue/pub`

The `NodeStatusMonitor` will use the client from the `banyand/queue/pub` package to discover and track the health of data nodes. This client provides robust connection management, automatic retries, and health checking.

The process is as follows:

1.  **Add a `HealthyNodes` method to `queue.Client`**:
    An exported method, `HealthyNodes() []string`, will be added to the `queue.Client` interface. This method will return a list of the addresses of all data nodes that are currently healthy and connected. The implementation in the `pub` package will return the keys of its `active` map.

2.  **Instantiate a `pub` Client**:
    The `NodeStatusMonitor` will create an instance of the `pub` client using `pub.NewWithoutMetadata()`.

3.  **Feed the Node List to the `pub` Client**:
    On startup, the `NodeStatusMonitor` will parse the `--data-node-list`. For each node address, it will:
    a.  Create a `schema.Metadata` object containing a `databasev1.Node` spec. The node's name and gRPC address will be populated from the list.
    b.  Call the `OnAddOrUpdate` method on the `pub` client instance itself (the `pub` struct implements `schema.EventHandler` for its own use). This will trigger the client's internal logic to establish a connection and start health checking the node.

4.  **Polling for Healthy Nodes and Detecting Status Changes**:
    The `NodeStatusMonitor` will maintain an internal map or set of nodes that were healthy during the last check. It will start a background goroutine that periodically does the following:
    a.  Calls the `HealthyNodes()` method on the `pub` client to get the current list of healthy nodes.
    b.  Compares this new list with its internal state (the list from the previous check).
    c.  **Detecting nodes that come back online**: If a node is present in the new list but was not in the previous list, it is considered to have come back online. The `NodeStatusMonitor` will then notify the `HandoffController` about this "DOWN to UP" transition.
    d.  **Detecting nodes that go offline**: If a node was in the previous list but is absent from the new list, it is considered to have gone offline. The `NodeStatusMonitor` will notify the `HandoffController` about this "UP to DOWN" transition.
    e.  After processing the changes, it updates its internal state with the new list of healthy nodes, ready for the next check.

This approach provides a clear and simple API for the `NodeStatusMonitor` to get the information it needs, while still leveraging the sophisticated connection management capabilities of the `pub` client.

##### 5.3. Integration with `wqueue` and data sending logic

The `wqueue`'s flushing process will now integrate with the `syncer` and `HandoffController` to manage the lifecycle of a data part. The responsibility for deleting the part from the `wqueue` is delegated to the `introducer` component, ensuring a clean separation of concerns.

**Dispatching Flushed Parts:**

*   When `wqueue` flushes a part, it passes the part to the `syncer`.
*   The `syncer` is responsible for sending the part to all configured data nodes. It tracks which nodes the send was successful for.
*   The `syncer` then communicates the list of *online* (successful) nodes to the `HandoffController`.
*   The `HandoffController` takes this list, compares it against the full list of data nodes to identify the unreachable ones, then persists the part payload once to the shared content store and appends a `partId` reference to each unreachable node's reference queue.
*   Crucially, after the `syncer` has completed its work (sending to online nodes and notifying the controller), it sends a `syncIntroduction` message to the `introducer` (`introducer.go`).
*   The `introducer` processes this message and removes the part from the `wqueue`'s snapshot. This deletion is decoupled from the sync and handoff operations, relying on the `tsTable`'s internal state management logic.

**Sending Data from the Handoff Queue (Replay Logic):**

When a node comes back online, the `HandoffController` initiates a replay process. This process is responsible for sending the queued data to the recovered node. The design of this sender will be heavily influenced by `banyand/trace/syncer.go`.

1.  **Node Availability Check**: Before sending, the replay process will confirm the node's availability by checking against the list of healthy nodes provided by the `NodeStatusMonitor`. This ensures we don't attempt to send to a node that has become unavailable again.

2.  **Collecting Parts**: The replay process will read `partId`s from the node's reference queue and, for each, fetch the corresponding payload from the shared content store. This is analogous to the `collectPartsToSync` function in the `syncer`, but references are decoupled from payload bytes.

3.  **Data Serialization**: The collected parts, including core trace data, metadata, and indexes, will be prepared for transmission as `queue.StreamingPartData`. This standardized format allows for efficient streaming.

4.  **Data Transmission**:
    *   A `ChunkedSyncClient` (from `banyand/queue`) will be created for the target node.
    *   The `SyncStreamingParts` method of the client will be used to send the data parts in a streaming fashion. This is efficient for large data volumes and resilient to transient network issues. This mirrors the behavior of `syncStreamingPartsToNode` in the `syncer`.
    *   The entire batch of parts for a given replay cycle will be sent to the single recovered node.

5.  **State Management**: After the data is successfully sent and acknowledged by the data node, the corresponding `partId` reference will be removed from that node's reference queue. If no other node references remain for the `partId`, the payload is removed from the shared content store (reference-counted or via periodic GC), preventing orphaned data.

6.  **Flow Control**: To prevent overwhelming a newly recovered node, the replay mechanism should include a configurable rate limit or a simple backoff strategy in case of failures.

##### 5.3.1. Replay and live send interactions

**Concurrency control**

*   Live sends and replays may operate concurrently. To avoid duplicate work and races, the Handoff Controller maintains a per-node in-flight ledger of `partId`s across both paths (live and replay).
*   Only one replay worker is active per node at a time. It atomically claims the next reference from that node's reference queue to ensure exclusive processing and predictable progress.
*   Before sending, the replay worker checks the in-flight ledger. If the `partId` is currently being sent by the live path to the same node, the replay worker skips it for now and moves on, preventing concurrent duplicate sends.
*   Completion handling is unified: when either live or replay delivery succeeds for `(node, partId)`, the node's reference is removed. If this was the last node referencing the payload, the shared content store entry is eligible for deletion (subject to reference counting/GC).
*   The receiver enforces idempotency keyed by `partId`. If a duplicate arrives due to races or retries, it is acknowledged without double-applying side effects.
*   To simplify correctness, the system disallows multiple concurrent replay streams to the same node. A lightweight per-node mutex or worker ownership election ensures single-writer semantics for that node's replay.

**Fairness across nodes**

*   A global replay scheduler iterates over nodes with pending references using round-robin selection to prevent starvation when multiple nodes are backlogged.
*   Per scheduling quantum, each selected node processes up to a small, fixed batch of references before yielding to the next node. This avoids draining one node completely while others make no progress.
*   To further improve fairness, nodes may be ordered by the age of their oldest pending reference (oldest-first) when ties occur, ensuring long-waiting data is prioritized.
*   The fairness policy applies only to replay workers; live sends remain opportunistic and are not throttled by the replay scheduler.

##### 5.4. Data Volume Control

To prevent the handoff storage from growing indefinitely and consuming excessive disk space, data volume control will limit both the shared content store and per-node reference queues. This ensures that the liaison node remains stable even if a data node is offline for an extended period.

1.  **Maximum Sizes**: Configurable limits control the maximum total size of the shared content store and the per-node reference queues. The per-node cap is controlled by `--handoff-max-size-mb` (per-node refs). A new flag `--handoff-store-max-size-mb` limits the global shared store size.

2.  **Size Monitoring**: Before appending references or writing a new payload, the `HandoffController` will check the current on-disk size of the shared store and the target node's reference queue.

3.  **Eviction Policy**: If adding a payload or reference would exceed limits, references may be evicted per-node according to policy, and payloads may be removed only when they have no remaining references. Policies should be configurable (e.g., drop-oldest FIFO vs. backpressure) and applied without duplicating bytes in the shared store.

#### 6. Configuration Changes

New command-line flags will be introduced for the liaison node server:

*   `--data-node-list`: A comma-separated list of data node addresses (e.g., `banyandb-data-0.banyandb-data-headless:17912,banyandb-data-1.banyandb-data-headless:17912`).
*   `--handoff-max-size-mb`: The maximum size in megabytes (MB) that a single node's reference queue can occupy on disk. Defaults to a reasonable value, e.g., 1024 MB.
*   `--handoff-store-max-size-mb`: The maximum size in megabytes (MB) that the shared content store can occupy on disk. Defaults to a reasonable value, e.g., 10240 MB.

The root path for the handoff queue storage will also be configurable.

#### 7. Helm Chart Changes

The BanyanDB Helm chart will be updated to support the `--data-node-list` and `--handoff-max-size-mb` flags. The `--data-node-list` will be populated automatically based on the number of replicas for the data node `StatefulSet`.

#### 8. Documentation

The official BanyanDB documentation will be updated to include:

*   A description of the handoff queue feature and its benefits.
*   Configuration details for the new `--data-node-list` and `--handoff-max-size-mb` flags.
*   Operational guidance on how to monitor the handoff queues and what to expect during a data node outage.

#### 9. Future Considerations

*   **Dynamic Node Discovery**: The current design relies on a static list of data nodes. In the future, this could be improved by integrating with a service discovery mechanism (e.g., Kubernetes services, etcd) to automatically discover data nodes. This would remove the need to restart liaison nodes when the data node cluster scales.
*   **Metrics and Observability**: Exposing metrics about the handoff queues (e.g., queue size, replay rate, number of offline nodes) would be valuable for monitoring and troubleshooting.
