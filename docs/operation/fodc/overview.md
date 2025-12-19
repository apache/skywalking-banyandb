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
Ecosystem        |  - Config view (/cluster/config)        |
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

The FODC Proxy provides aggregated / enriched metric endpoints:

- `GET /metrics`
    - Proxy’s own metrics (health, number of agents, RPC latency, etc.)
- `GET /metrics-windows`
    - Returns metrics **within the maintained time window** for all known agents
    - Includes **additional node metadata**, such as:
        - Node role (`liaison`, `datanode-hot`, `datanode-warm`, `datanode-cold`, etc.)
        - Node IDs and cluster membership
        - Location / shard information (if available)

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

The Proxy exposes a unified cluster discovery endpoint:

- `GET /cluster`
    - Returns the list of all registered nodes
    - Includes:
        - Node identity (ID, name, address)
        - Role:
            - `liaison`
            - `datanode-hot`
            - `datanode-warm`
            - `datanode-cold`
            - (extensible for future roles)
        - Agent status (online/offline, last heartbeat time)
        - Key runtime indicators (optional: load, health, etc.)

This simplifies integration with:

- Cluster dashboards
- Automated operations (e.g. scheduled checks before resharding / scaling)
- Higher-level diagnostics tooling that needs a consistent cluster graph

---

## Node Configuration Collection

FODC also collects and exposes static and dynamic configuration for each node.

### What Is Collected

Typical configuration categories include:

- **Startup parameters**
    - Command-line flags
    - Environment variables (where permitted)
- **Affected configurations**
    - Configurations used by the database node in the runtime.

### Configuration API

The FODC Proxy aggregates configuration from all Agents and exposes it via:

- `GET /cluster/config`
    - Returns configuration for all nodes in the cluster
- Potential filters (implementation-dependent):
    - `GET /cluster/config?node_id=<id>`
    - `GET /cluster/config?role=datanode-hot`

This allows:

- Quickly verifying configuration consistency across nodes and tiers
- Comparing pre- and post-incident configuration states
- Supporting automated configuration audits and drift detection

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