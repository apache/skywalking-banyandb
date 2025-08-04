# Property Background Repair Strategy

In Property, the `replicas` configuration allows Property data to be stored across multiple data nodes, providing high availability. 
However, during data updates, some nodes may become temporarily unavailable, which can lead to version inconsistencies of the same data across different machines.

This documentation explains how the system automatically synchronizes Property data across all data nodes in the background, 
ensuring that all replicas eventually converge to the same version.

The process can be roughly divided into the following two steps:
1. **Build Merkel Tree**: Automatically build anti-entropy Merkle Trees in the background for efficient comparison in subsequent operations.
2. **Recovery Data through gossip**: Use the gossip protocol among data nodes to detect and repair data inconsistencies.

## Build Merkel Tree

The Merkel Tree is a data structure used to efficiently compare node information across two different data nodes, allowing 
the data nodes to quickly determine whether their data is consistent.

### Structure
In the context of property data, each shard within a group builds its own Merkle Tree, Since the Property data is finite.
The tree consists of the following three types of nodes:
1. **Leaf Node**: Store the summary information of each Property data:
   1. **Entity**: The identifier of the Property data, composed of `group_name` + `property_name` + `id`.
   2. **SHA Value**: The SHA512 hash value of the Property source data(`sha512(source_json_bytes+property_is_deleted)`), used for fast equality comparison to check the consistency of the same entity data
2. **Slot Node**: Each tree contains a fixed number(such as `32`) of slot nodes. When the Property data is added into the tree, it is placed in a slot node based on its hash value of Entity(`hash(entity) % slot_count`). 
   The slot node contains the SHA value of the Property data and the number of Property data in that slot.
3. **Root Node**: The root node of the Merkle Tree, which contains the SHA value of the entire tree and the number of slot nodes.

Therefore, the Merkle Tree has above three structure levels:
* The top level is a single root node.
* The middle level is a fixed number of slot nodes.
* The bottom level is a variable number of leaf nodes.

### Timing and Building Process

There are two types of triggers for Merkle Tree construction:
1. **Fixed Interval**: By default, the system automatically triggers the construction every hour (this interval is configurable).
2. **On Update**: When an update in the shard is detected, the system schedules a delayed build after a short wait period (default 10 minutes).

The construction process follows these steps:
1. **Check for Updates**: The system compares the snapshot ID(`XXXXX.snp`) of the previously built tree with the current snapshot ID of the shard. 
   If they differ, it indicates that data has changed, and the process continues. If they match, the tree construction is skipped.
2. **Snapshot the Shard**: A snapshot of the shard data is taken to avoid blocking ongoing business operations during data traversal.
3. **Build the Tree**: Using the streaming method, the system scans all data in the snapshot and builds a Merkle Tree for each group individually.
4. **Save the Snapshot ID**: The snapshot ID used in this build is saved, so it can be used for efficient change detection during the next scheduled run.

## Gossip Protocol

In BanyanDB's typical data communication model, data exchange is primarily handled by **liaison** nodes, which interact with **data nodes** through **broadcasting or publishing** mechanisms.
However, in the context of Property data repair, involving liaison nodes would introduce unnecessary network overhead. 
Therefore, the system adopts the gossip protocol, allowing data nodes to communicate directly with each other to perform data repair in a more efficient and decentralized manner.

Unlike typical gossip-based message dissemination, where messages are spread randomly, the number of **data nodes** in BanyanDB is fixed and relatively small.
To ensure greater stability, the random peer selection is replaced with a deterministic strategy, where each node communicates only with its next node in the sequence, and the node list is ordered. 
Additionally, to minimize network traffic, only one peer node is involved in each round of communication.

BanyanDB already has a built-in [cluster discovery and data transmission](./clustering.md) mechanism. Therefore, the implementation of gossip protocol can be built as an extension on top of the existing cluster protocol.
Since each node already exposes a gRPC port, there is no need to introduce any additional ports, simplifying the deployment and integration.

### Propagation Message

When initiating gossip message propagation, the sender node must include both the list of participating nodes and the message content. 
The gossip protocol then proceeds through the following steps:

1: **Build the Context**: The sender node builds a context object that is attached to each message and includes the following parameters:
   1. **Node List**: A list of participating nodes used to determine the next node in the sequence.
   2. **Maximum Count**: The maximum number of message transmissions, calculated as `(node_count) * 2 - 3`. The rationale behind this formula will be explained in an upcoming example.
   3. **Origin Node ID**: The ID of the node that initiated the gossip message propagation, used to return the final result. 
   4. **Original Message ID**: A unique identifier for the original message, allowing the origin node to track which a gossip message propagation process has completed.
2. **Send to the First Node**: If the first node in the list is the sender itself, the process begins locally. Otherwise, the message is sent to the first node in the list.
3. **Receive Gossip Message**: Upon receiving the message, the node identifies the next node in the sequence and prepares for peer-to-peer interaction.
4. **Protocol-Level Handling**: The Property repair logic runs its own protocol between the current node and the next node, ensuring their Property data is synchronized. (Details of the two-node sync protocol will be covered in a later section.)
5. **Handle Result**: If the sync is successful, the process keeps continuing. If it fails, the flow jumps to step 7.
6. **Forward to the Next Node**: The maximum count in the context is decremented by one to indicate a completed round. 
   * If the maximum count reaches zero, the process is considered complete, and it proceeds to step 7.
   * Otherwise, the message is forwarded to the next node, repeating steps 3–6.
7. **Send Result to Origin Node**: The current node sends the result—either success or failure—to the origin node specified within the context.
8. **Origin Node Receives the Result**: The initiating sender node receives the final outcome of the gossip protocol and can proceed with any post-processing logic.

### Maximum Count Formula

Since no synchronization is needed in **a single-node** setup, the following formula applies only when there are two or more nodes participating in the repair process.

**Maximum Gossip Count = (number of nodes) × 2 − 3**

This ensures that the updated data has sufficient opportunity to fully propagate across all nodes, even in the worst-case version distribution.

#### Example Scenarios

##### Case 1: 2 Nodes

| Step    | Action                               | A Version | B Version |
|---------|--------------------------------------|-----------|-----------|
| Initial | —                                    | v1        | v2        |
| A → B   | A receives v2 from B → **A updated** | v2        | v2        |

* **Max Gossip Count**: `2 × 2 − 3 = 1`
* **Final State**: All nodes hold **v2**

##### Case 2: 5 Nodes

| Step    | Action                                        | A Version | B Version | C Version | D Version | E Version |
|---------|-----------------------------------------------|-----------|-----------|-----------|-----------|-----------|
| Initial | —                                             | v1        | v2        | v3        | v4        | v5        |
| A → B   | A receives v2 from B → **A updated**          | v2        | v2        | v3        | v4        | v5        |
| B → C   | B receives v3 from C → **B updated**          | v2        | v3        | v3        | v4        | v5        |
| C → D   | C receives v4 from D → **C updated**          | v2        | v3        | v4        | v4        | v5        |
| D → E   | D receives v5 from E → **D updated**          | v2        | v3        | v4        | v5        | v5        |
| E → A   | A receives v5 from E → **A updated to final** | v5        | v3        | v4        | v5        | v5        |
| A → B   | B receives v5 from A → **B updated to final** | v5        | v5        | v4        | v5        | v5        |
| B → C   | C receives v5 from B → **C updated to final** | v5        | v5        | v5        | v5        | v5        |

* **Max Gossip Count**: `5 × 2 − 3 = 7`
* Final State: All nodes hold **v5**

### Example of Message Propagation

To illustrate the gossip message propagation process, consider a scenario with three nodes with the version: A(version 2), B(version 1), C(version 3), and the sender is node B. The sequence of operations is as follows:

The process starts from **Node B** (the initiator).

     1.  **Initiation (at Node B):** Node B builds the gossip context. The propagation message is sent to the first node in the list, **Node A**.
     2.  **Step 1 (at Node A):**
         * Node A receives the message.
         * A identifies its successor in the list: **Node B**.
         * A synchronizes with B. Since A(v2) is newer than B(v1), B's data is updated.
         * **State:** A(v2), B(v2), C(v3).
         * A forwards the gossip message to B.
     3.  **Step 2 (at Node B):**
         * Node B receives the forwarded message.
         * B identifies its successor: **Node C**.
         * B synchronizes with C. Since C(v3) is newer than B(v2), B's data is updated.
         * **State:** A(v2), B(v3), C(v3).
         * B forwards the gossip message to C.
     4.  **Step 3 (at Node C):**
         * Node C receives the forwarded message.
         * C identifies its successor: **Node A**.
         * C synchronizes with A. Since C(v3) is newer than A(v2), A's data is updated.
         * **State:** A(v3), B(v3), C(v3).
         * C forwards the gossip message back to A (or recognizes the process is complete).
     5.  **Completion:** The maximum propagation count, `(3) * 2 - 3 = 3`,  is reached. The last node in the chain (Node C) sends the final result back to the origin node (Node B). All nodes have now converged to the latest version (v3).

### Tracing

Tracing becomes critically important in a gossip-based protocol, as issues can arise at any stage of the communication and synchronization process.
New trace spans are created in the following scenarios:

1. **Initiating Node**: The initiating node records the full trace of the entire request, capturing all steps from message dispatch to the final result collection.
2. **Receiving Sync Requests**: When a node receives a sync request and begins communication with another peer node, it creates a new trace span for that interaction to track the request lifecycle independently.
3. **Business Logic Execution**: During the actual business processing (e.g., data comparison, update, and transmission), custom trace data and metadata can be attached to the active trace context to provide deeper insights into logic-specific behaviors.

After each synchronization cycle, the receiving node will send its trace data back to the initiating node, allowing the initiator to aggregate and correlate all spans for end-to-end visibility and debugging.

## Property Repair

Based on the Merkel tree and Gossip concept, the system can proceed with the Property Repair process.
This process is scheduled to run on each data node daily at 2:00 AM(it's configurable as `property-background-repair-cron` flag), and follows these steps:
1. **Select a Group and Select a Shard**: The node retrieves a list of Property groups where the number of **replicas is greater than or equal to 2**, and randomly selects one group for repair, then randomly selects a shard within that group.
2. **Query Node List**: Then determines the list of nodes that hold replicas for the selected group and sends the gossip propagation message to those nodes to synchronize the Property data for that group.

### Property Synchronize between Two Nodes

When two nodes engage in Property data synchronization, they follow a specific protocol to ensure data consistency.
Let’s refer to the current node as A and the target node as B. The process is as follows:

1. **(A)Establish Connection**: Node A initiates a **bidirectional streaming connection** with node B to enable real-time, two-way data transfer.
2. **(A)Iterate Over Shards**: Node A retrieves the list of all Property-related shards for the selected group and processes them one by one.
3. **(A)Send Merkle Tree Root**: For each shard, node A reads its Merkle Tree and sends root SHA to node B.
This allows B to quickly identify which slots may contain differences.
4. **(B)Receive Merkle Tree Root**: Node B receives the root SHA from node A and sent the root SHA value is same or not.
5. **(A)Send Slot Summary**: If the root SHA matches, node A finished the synchronization for that shard and moves to the next one.
   If the root SHA differs, node A sends a summary of the slots, including the **slot index** and **SHA value** for each slot that has differences.
6. **(B)Verify Merkle Tree Summary and Respond**: Node B compares the received summary against its own Merkle Tree for the same shard and group,
identifies mismatched slots, and sends back all relevant leaf node details, including the **slot index**, **entity**, and **SHA value**.
7. **(A)Compare Leaf Data**: Node A processes the received leaf data and takes the following actions: 
   * For missing entities (present on B but not on A), A requests the full Property data from B.
   * For entities present on A, but not on B, A sends the full Property data to B.
   * For SHA mismatches, A sends its full Property data to B for validation.
8. **(B)Validate Actual Data**: Node B handles the data as follows: 
   * For missing entities, B returns the latest version of the data.
   * For entities present on A, but not on B, B updates its local copy with the data from A.
   * For SHA mismatches, B uses the "last-write-win" strategy. It compares the version numbers. If B’s version is newer, it returns the Property data to A. If A’s version is newer, B updates its local copy and does not return any data. If the versions are the same, it selects the data from the smaller index of the node list; in this case, it would be from A.
9. **(A)Update Local Data**: Node A receives updated Property data from B and applies the changes to its local store.

This concludes the A-to-B synchronization cycle for a given group in the Property repair process.

### Error Handling

During the synchronization process, the system will terminate or skip processing under the following scenarios:

1. **Merkle Tree Not Built**: 
   1. If node A(client) has not yet built the Merkel Tree for the target group, the gossip protocol is immediately terminated.
   2. If node B(server) has not yet built the Merkle Tree for the target group, the client would skip the current server and try to find another next to continue the gossip protocol.
2. **Duplicate Sync Request**: If a new gossip sync request for the same group is received by either node while an existing synchronization is in progress, the new request is terminated to avoid conflicts.
3. **Target Node Connection Failure Handling**: When the current node fails to connect to the target node, the following fallback logic applies:
   1. **Target Not in Connection Pool**: Skip the target, decrement the max round count by 1, and attempt the next available node. Trace information is recorded.
   2. **Connection Timeout**: Treat as unreachable, skip the node, decrement the round count, and record the trace.
   3. **Network Errors or Response Failures**: On any communication error during synchronization, skip the node, decrement the round count, and log the trace.
   4. **No Available Nodes Remaining**: If no further nodes can be contacted, terminate the gossip protocol.
4. **Property Repair Failure**: If an error occurs while applying Property data updates (e.g., a write failure), the error is recorded in the trace. The synchronization for that specific property is skipped, but the overall gossip process continues with the next item.
5. **Unhandled Exceptions**: For any other unexpected exceptions or failures, the gossip protocol is immediately terminated to maintain system consistency and avoid cascading errors.

## Background Repair vs. Query Repair

When querying Property data, the liaison node will still query the latest available data from existing data nodes, 
and if discrepancies are detected, it will repair the outdated Property data on the corresponding nodes.

When using background repair, there are several key differences compared to on-demand repair during queries:
1. **Query Efficiency**: On-demand repair during queries can significantly increase latency and CPU load, especially under high query volume. 
Each request may trigger comparison and data repair logic, which impacts performance. In contrast, background repair minimizes this overhead, 
ensuring query paths remain lightweight and responsive.
2. **Improved Availability**: During a query, if the node holding the latest version happens to be unavailable, 
the liaison node may fall back to returning data from other nodes, potentially missing the most recent update.
Background repair helps mitigate this by proactively synchronizing all nodes to the latest version, 
ensuring consistent data availability even if some nodes are temporarily offline.