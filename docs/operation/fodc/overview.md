# First Occurrence Data Collection (FODC)

First Occurrence Data Collection (FODC) is an observability and diagnostics subsystem for BanyanDB.  
It continuously collects runtime parameters, performance indicators, node states, and configuration
data from DB nodes, and also supports **on-demand** performance profiling and memory snapshots.

FODC has two primary goals:

1. Ensure the stable operation of DB nodes across all lifecycle stages (bootstrap, steady state, scaling, failure, etc.).
2. Provide trustworthy, structured data that supports capacity planning, performance analysis, and troubleshooting.

FODC adopts a **Proxy + Agent** deployment model and exposes a unified, ecosystem-friendly data interface to external systems
(such as Prometheus and other observability platforms).

---

## Overview

FODC provides multiple categories of data:

1. **Metric collection and short-term caching** (small time window)
2. **Node topology and status**, including runtime parameters and role states
3. **Node configuration collection**
4. **On-demand performance profiling and memory snapshots**

To accomplish this, FODC is deployed as:

- A **central Proxy** service
- Multiple **Agents**, typically co-located with BanyanDB nodes (sidecar pattern)

Agents connect to the Proxy via gRPC and register themselves. The Proxy then:

- Aggregates and normalizes data from all agents
- Exposes unified REST/Prometheus-style interfaces
- Issues **on-demand diagnostic commands** (profiling, snapshots, config capture, etc.) to one or more agents

---

## Architecture

### Deployment Model

FODC uses a **one-to-one mapping** between an Agent and a BanyanDB node:

- Each **liaison** node has one FODC Agent
- Each **data node** (hot / warm / cold) has one FODC Agent
- In manual deployment modes, the same 1:1 relationship must be preserved

Agents are typically deployed as **sidecars** in the same pod or host as the corresponding BanyanDB node.

### Proxy–Agent Relationship

The Proxy acts as the **control plane and data aggregator**, while Agents act as the **data plane** local to each DB node.

Key characteristics:

- Agents connect to the Proxy using **gRPC** and a configured **Proxy domain name**.
- The connection is **bi-directional**:
  - Agents stream node metrics, status, and configuration to the Proxy.
  - The Proxy sends on-demand diagnostic commands back to the Agents.

#### ASCII Architecture Diagram

```text
                 +-----------------------------------------+
                 |              FODC Proxy                 |
                 |-----------------------------------------|
                 |  - Agent registry                       |
                 |  - Cluster topology view                |
External         |  - Aggregated metrics (/metrics         |
Clients &  <---- |    and /metrics-windows)                |
Ecosystem        |  - Lifecycle view (/cluster/lifecycle)  |
                 |  - On-demand control APIs               |
                 +-----------------^-----------------------+
                                   |
                          gRPC bi-directional streams
                                   |
        -----------------------------------------------------------------
        |                               |                               |
        v                               v                               v
+------------------+          +------------------+            +------------------+
|   FODC Agent     |          |   FODC Agent     |            |   FODC Agent     |
|  (sidecar with   |          |  (sidecar with   |            |  (sidecar with   |
|  liaison node)   |          | data node - hot) |            | data node - warm)|
|------------------|          |------------------|            |------------------|
| - Scrape local   |          | - Scrape local   |            | - Scrape local   |
|   Prometheus     |          |   Prometheus     |            |   Prometheus     |
|   metrics        |          |   metrics        |            |   metrics        |
| - Collect OS &   |          | - KTM / OS obs   |            | - KTM / OS obs   |
|   KTM telemetry  |          |   metrics        |            |   metrics        |
| - Execute on-    |          | - Execute on-    |            | - Execute on-    |
|   demand profile |          |   demand profile |            |   demand profile |
|   & heap dump    |          |   & heap dump    |            |   & heap dump    |
+--------^---------+          +--------^---------+            +--------^---------+
         |                               |                               |
         |                               |                               |
  +------+--------+               +------+--------+               +------+--------+
  | BanyanDB      |               | BanyanDB      |               | BanyanDB      |
  | liaison node  |               | data node     |               | data node     |
  | (process)     |               | (hot tier)    |               | (warm tier)   |
  +---------------+               +---------------+               +---------------+
```

> Additional data node tiers (e.g. `datanode-cold`) follow the same **BanyanDB node ↔ FODC Agent** 1:1 pattern.

---

## Metric Collection and Prometheus Integration

### Data Sources

Each FODC Agent collects metrics from:

1. **The local BanyanDB node**
    - Via its Prometheus `/metrics` HTTP endpoint
    - Includes DB performance, internal queues, I/O stats, query latency, etc.
2. **Kernel & OS-level telemetry**
    - Through an integrated **Kernel Telemetry Module (KTM)** powered by eBPF
    - Examples: OS page cache statistics, system I/O latency, CPU scheduling behavior

### In-Memory Sliding Window Cache

Agents maintain a **sliding window** of recent metric samples in memory:

- A **wake-up queue** is used to buffer the last **N** collections.
- The time window is **auto-tuned** at startup based on:
    - Sample interval
    - Number of metrics
    - Available memory constraints
- Target memory usage is kept low (around **30 MB** per Agent) while still supporting:
    - Short-term trend analysis
    - Correlation during incident triage (e.g. spikes around first occurrence)

This design allows FODC to provide recent time-series context **without depending on an external TSDB**.

### Agent Metric Exposure

Each FODC Agent exposes a **Prometheus-compatible** endpoint:

- `GET /metrics`
    - Returns the **latest** scraped metrics and local telemetry
    - Can be scraped directly by:
        - The FODC Proxy
        - External observability systems (if desired and authorized)

### Proxy Metric Exposure

The FODC Proxy aggregates the metrics of **all** registered agents and re-exposes them through two endpoints. Both accept optional `role` and `pod_name` query parameters to scope the result to a subset of nodes:

- `GET /metrics`
    - Returns the **aggregated, latest** per-node metrics from every agent in **Prometheus text exposition format** (`Content-Type: text/plain; version=0.0.4`).
    - On each request the proxy collects the current sample from the agents on demand, then concatenates them — so scraping the proxy once yields every node's series. This is the **single scrape target** for a Prometheus-based setup; per-node identity is carried in the `pod_name` and `container_name` labels (see [Observability › Metrics Providers](../observability/providers.md)).
    - This endpoint exposes the aggregated node metrics, **not** a proxy-only view; the proxy's own health and agent counts are served separately by `GET /health` (status, online/total agents, uptime).
- `GET /metrics-windows`
    - Returns metrics as **JSON** — an array of time series, each with `name`, `description`, `labels`, `agent_id`, `pod_name`, and a time-sorted `data` array of `{timestamp, value}` points.
    - When **both** `start_time` and `end_time` (RFC3339) are supplied, it returns the samples held in the agents' in-memory sliding window for that range; otherwise it falls back to the latest sample (the same data as `/metrics`, in JSON form).
    - Each series carries whatever labels the underlying metric has (including `node_role` and `container_name`); richer cluster metadata such as node IDs, membership, and roles is served by `GET /cluster/topology`. This endpoint is intended for short-term trend queries and incident triage, **not** as a Prometheus scrape target.

This makes FODC a **drop-in component** for Prometheus-based ecosystems, while preserving richer semantic context about each node.

---

## Cluster Topology, Roles, and Runtime State

The FODC Proxy maintains an up-to-date view of cluster topology based on **Agent registration**:

1. On startup, each Agent:
    - Connects to the Proxy via gRPC
    - Registers its:
        - Node ID and role
        - Basic runtime attributes and capabilities
2. The Proxy aggregates these registrations into a **logical cluster hierarchy structure**.

### Topology & Status API

The Proxy exposes a cluster discovery endpoint:

- `GET /cluster/topology`
    - Triggers a topology collection across all registered agents and returns the merged snapshot as JSON: `{ "nodes": [...], "calls": [...] }`.
    - Each `nodes` entry carries:
        - Node identity — `metadata.name` and `grpc_address`
        - `labels` — e.g. `pod_name` and `type` (`hot` / `warm` / `cold`)
        - `roles` — role-name strings such as `ROLE_META`, `ROLE_DATA`, `ROLE_LIAISON` (extensible for future roles)
        - Agent `status` (online/offline) and `last_heartbeat`
    - `calls` describes the node-to-node call graph reported by the agents (route-table membership, not data flow).

See [Proxy APIs and CLI Flags](./apis.md) for the full response schema. To turn this node inventory into a directed, weighted topology of the actual data flow — joining it with the queue metrics for per-edge throughput, latency, and errors — see [Cluster Topology Rendering](./topology.md).

This simplifies integration with:

- Cluster dashboards
- Automated operations (e.g. scheduled checks before resharding / scaling)
- Higher-level diagnostics tooling that needs a consistent cluster graph

---

## Cluster Lifecycle and Group Information

Beyond topology, the Proxy aggregates per-group lifecycle data and per-pod lifecycle reports from the agents.

### Lifecycle API

- `GET /cluster/lifecycle`
    - Triggers lifecycle-data collection from all agents that support the lifecycle stream and returns JSON with two sections:
        - `groups` — group lifecycle information (group name, catalog type, resource options such as shard count / segment interval / TTL, and data info), collected from the first agent that provides it (typically the liaison node).
        - `lifecycle_statuses` — per-pod lifecycle reports, i.e. the JSON report files read from each agent's lifecycle report directory.
    - Agents that do not support the lifecycle stream are silently skipped.

This allows:

- Verifying group / resource-option consistency across the cluster
- Reviewing lifecycle (rotation / TTL / migration) reports per node
- Correlating group settings with observed behavior during incident triage

### Crash Diagnostics API

- `GET /diagnostics`
    - Aggregates crash diagnostic records (structured panic records and on-disk crash artifacts) from all connected agents, with optional `role` and `pod_name` filters.

See [Proxy APIs and CLI Flags](./apis.md) for the full request/response schemas of both endpoints.

---

## On-Demand Performance Profiling and Memory Snapshots

On-demand diagnostics are the **first non read-only capability** exposed by FODC.
They enable deep performance analysis while carefully controlling overhead.

### Design Principles

- **Opt-in and controlled**  
  Diagnostic actions are only triggered through explicit API calls to the Proxy.
- **Local execution, remote control**  
  Agents perform the heavy work (profiling, snapshots) on the local node; the Proxy only orchestrates.
- **Low default footprint**  
  By default, Agents run in **low CPU / low memory** mode and do not perform expensive diagnostics.
- **Burst resource usage when needed**  
  Extra CPU/memory budget is mainly consumed **only during active profiling or snapshot sessions**.

### Typical On-Demand Actions

Exact APIs may vary by implementation, but commonly include:

- **CPU profiling**
    - Short-term CPU usage profiling (e.g. pprof)
    - Useful for identifying hot code paths under load
- **Heap / memory snapshots**
    - Captures heap allocation state for leak or fragmentation analysis
- **I/O / lock contention profiling**
    - Optional profiling of DB internal lock contention or I/O stalls
- **Configuration snapshot on demand**
    - Force a re-capture of configuration and runtime flags at a specific point in time
- **Apply RBAC or other authorization controls on Proxy APIs**

---

## Summary

FODC provides:

- **Unified, structured observability** for BanyanDB clusters (metrics, topology, configuration)
- **Prometheus-friendly** interfaces for easy ecosystem integration
- **On-demand deep diagnostics** (profiling, memory snapshots) orchestrated centrally but executed locally
- A lightweight, extensible **Proxy + Agent** architecture that respects resource constraints

This makes FODC a foundational component for reliable operation, performance analysis, and automated troubleshooting of BanyanDB deployments.