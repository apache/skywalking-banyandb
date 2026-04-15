# Distributed Measure Aggregation

In a cluster, a measure query may touch many shards spread across **Data Nodes**. When the query includes an **aggregation** (`agg` in the API), BanyanDB splits the work into two logical phases so results stay correct while limiting how much raw data moves over the network.

This page describes that design at a high level. For roles of liaisons and data nodes in general, see [Clustering](clustering.md). For how to issue measure queries from clients, see [Query Measures](../interacting/bydbctl/query/measure.md).

## Map on Data Nodes, Reduce on the Liaison

**Map phase (Data Nodes)**  
Each data node scans its local shards, applies the same filters and time range as the user query, and runs the aggregation over the matching **raw** field values. Instead of sending every raw point upstream, the node sends a compact **intermediate** representation of the aggregation for each logical result row (for example one row per group when `group_by` is used).

**Reduce phase (Liaison Node)**  
The liaison collects those intermediate values from all participating nodes, **combines** them with the same aggregation function, and produces the final value the client expects. Combining partial sums into a total sum, or taking the max of partial maxima, are typical examples.

So: data nodes do the heavy lifting near storage; the liaison merges shard-level (and replica-deduplicated) partials into one answer.

## Why Two Phases

- **Correctness across shards**: A group or time bucket may span multiple shards on different nodes. Each node can only aggregate what it sees locally; the liaison merges those local summaries into a global result.
- **Efficient `MEAN`**: The global mean is not the mean of local means. The map phase tracks **sum and count** (conceptually); the reduce phase adds sums and counts from all nodes, then divides. That requires an intermediate form richer than a single number for the map output.
- **Less data on the wire**: For aggregated queries, nodes send partial aggregates instead of full raw series, which scales better as data volume grows.

## Supported Aggregation Functions

The same aggregation functions you use on a single node are supported in distributed mode: **SUM**, **COUNT**, **MAX**, **MIN**, and **MEAN**. The map and reduce steps are implemented so each function composes safely across shards (for example COUNT uses count-like partials that are summed at the liaison, analogous to SUM).

## Replicas and Deduplication

The same shard may be read from more than one replica for availability. Before the reduce step, the liaison **deduplicates** map results that represent the same shard (and the same group key when `group_by` is used), so replica responses are not counted twice. Operators do not configure this; it is part of query execution.

## What This Means for Operations

- **CPU**: Data nodes spend more CPU on aggregated queries because aggregation runs where the data lives. The liaison mainly merges partials, which is comparatively light for typical workloads.
- **Network**: Aggregated queries generally move less payload than fetching all raw points and aggregating only on the liaison.
- **Scaling**: Adding data nodes spreads map work across the cluster for shard-local portions of the query; the liaison still performs one reduce pass over the combined partial set, so liaison capacity remains relevant for very large fan-out.

## Standalone Mode

In standalone deployment, a single process plays both roles. The same aggregation logic applies locally without cross-node traffic; there is no separate “wire format” step from an operator’s perspective.

## Related Material

- [Clustering](clustering.md) — liaison vs data node responsibilities and routing  
- [Query Measures](../interacting/bydbctl/query/measure.md) — measure query examples  
- [API reference](../api-reference.md) — measure query and internal `agg_return_partial` (used between liaison and data nodes for partial aggregates)