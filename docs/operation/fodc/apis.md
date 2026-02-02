
# FODC Setup: Proxy APIs and CLI Flags

This guide documents the FODC proxy HTTP APIs and the CLI flags for the FODC agent and proxy binaries.
It complements the high-level overview in `docs/operation/fodc/overview.md`.

## Proxy HTTP APIs

The proxy exposes HTTP endpoints on `--http-listen-addr` (default `:17913`).
Assume the base URL is `http://<proxy-host>:17913` in the examples below.

### GET /metrics

Aggregates the latest metrics from all connected agents and renders them in Prometheus text format.

**Query parameters**

- `role` (optional): Filters metrics whose `node_role` label matches the specified role.
- `pod_name` (optional): Filters metrics whose `pod_name` label matches the specified pod.

**Response**

- Content-Type: `text/plain; version=0.0.4; charset=utf-8`
- Prometheus exposition format.
- Labels include agent identity labels such as `node_role`, `pod_name`, and `container_name` when provided by the agent.

**Example**

```text
GET http://localhost:17913/metrics
GET http://localhost:17913/metrics?role=ROLE_DATA
GET http://localhost:17913/metrics?pod_name=banyandb-data-0
```

### GET /metrics-windows

Returns metrics within a time window as JSON time series. If the time window is omitted,
the proxy returns the latest metrics, similar to `/metrics`, but formatted as JSON.

**Query parameters**

- `start_time` (optional): RFC3339 timestamp for the beginning of the time window.
- `end_time` (optional): RFC3339 timestamp for the end of the time window.
- `role` (optional): Filters metrics whose `node_role` label matches the specified role.
- `pod_name` (optional): Filters metrics whose `pod_name` label matches the specified pod.

**Response**

- Content-Type: `application/json`
- Body shape (array of time series):

```json
[
	{
		"name": "banyandb_query_latency_ms",
		"description": "Average query latency",
		"labels": {
			"node_role": "ROLE_DATA",
			"pod_name": "banyandb-data-cold-0",
			"container_name": "banyandb"
		},
		"agent_id": "9b3f44a8-acde-4f7c-a9f9-0b4b4581fd12",
		"pod_name": "banyandb-data-cold-0",
		"data": [
			{
				"timestamp": "2026-02-02T09:59:00Z",
				"value": 12.4
			},
			{
				"timestamp": "2026-02-02T10:00:00Z",
				"value": 10.1
			}
		]
	}
]
```

**Example**

```text
GET http://localhost:17913/metrics-windows
GET http://localhost:17913/metrics-windows?start_time=2026-02-02T09:55:00Z&end_time=2026-02-02T10:00:00Z
GET http://localhost:17913/metrics-windows?role=ROLE_DATA&pod_name=banyandb-data-0
```

### GET /cluster/topology

Requests cluster topology snapshots from all agents and returns the aggregated topology as JSON.
The proxy triggers a topology collection across all registered agents and merges the responses.

**Response**

- Content-Type: `application/json`
- Body shape:

```json
{
	"nodes": [
		{
      "metadata": {
        "name": "demo-banyandb-data-hot-0.demo-banyandb-data-hot-headless.skywalking-showcase:17912"
      },
      "grpc_address": "demo-banyandb-data-hot-0.demo-banyandb-data-hot-headless.skywalking-showcase:17912",
      "created_at": {
        "seconds": 1769671048,
        "nanos": 362947026
      },
      "labels": {
        "pod_name": "demo-banyandb-data-hot-0",
        "type": "hot"
      },
      "property_repair_gossip_grpc_address": "demo-banyandb-data-hot-0.demo-banyandb-data-hot-headless.skywalking-showcase:17932",
      "status": "online",
      "last_heartbeat": "2026-02-02T17:11:35.027349+08:00",
      "roles": [
        "ROLE_META",
        "ROLE_DATA"
      ]
    },
    {
      "metadata": {
        "name": "demo-banyandb-data-hot-0.demo-banyandb-data-hot-headless.skywalking-showcase:17912"
      },
      "grpc_address": "demo-banyandb-data-hot-0.demo-banyandb-data-hot-headless.skywalking-showcase:17912",
      "created_at": {
        "seconds": 1769671048,
        "nanos": 362947026
      },
      "labels": {
        "pod_name": "demo-banyandb-data-hot-0",
        "type": "hot"
      },
      "property_repair_gossip_grpc_address": "demo-banyandb-data-hot-0.demo-banyandb-data-hot-headless.skywalking-showcase:17932",
      "status": "online",
      "last_heartbeat": "2026-02-02T17:11:35.027349+08:00",
      "roles": [
        "ROLE_META",
        "ROLE_DATA"
      ]
    }
	],
	"calls": [
		{
      "id": "demo-banyandb-data-hot-0.demo-banyandb-data-hot-headless.skywalking-showcase:17912-demo-banyandb-data-hot-1.demo-banyandb-data-hot-headless.skywalking-showcase:17912",
      "target": "demo-banyandb-data-hot-1.demo-banyandb-data-hot-headless.skywalking-showcase:17912",
      "source": "demo-banyandb-data-hot-0.demo-banyandb-data-hot-headless.skywalking-showcase:17912"
    }
	]
}
```

**Field notes**

- `nodes` entries are derived from `banyandb.database.v1.Node` plus:
	- `roles`: role names converted to strings.
	- `status` / `last_heartbeat`: online status from the agent registry.
- `calls` entries describe the node-to-node call graph reported by agents.

## FODC Agent CLI Flags

The agent binary is `fodc` (see `fodc/agent/cmd/agent`).

| Flag | Default | Description |
| --- | --- | --- |
| `--poll-metrics-interval` | `10s` | Interval for scraping local BanyanDB metrics endpoints. |
| `--poll-metrics-ports` | `2121` | Ports to scrape for `/metrics` (repeatable or comma-separated). |
| `--max-metrics-memory-usage-percentage` | `10` | Maximum percentage of cgroup memory used for in-memory metric cache. |
| `--prometheus-listen-addr` | `:9090` | Address for the agent's Prometheus endpoint. |
| `--proxy-addr` | `localhost:17900` | Proxy gRPC address for agent registration and streaming. |
| `--pod-name` | _empty_ | Pod name used for agent identity; required for proxy registration. |
| `--container-names` | _empty_ | Container names mapped one-to-one with `--poll-metrics-ports`. |
| `--heartbeat-interval` | `10s` | Heartbeat interval to the proxy; proxy may override on registration. |
| `--reconnect-interval` | `5s` | Backoff between reconnection attempts to the proxy. |
| `--cluster-state-ports` | _empty_ | gRPC ports for BanyanDB cluster state polling; enables topology collection. |
| `--cluster-state-poll-interval` | `30s` | Interval for polling cluster state from BanyanDB nodes. |

**Behavior notes**

- The proxy client starts only when `--proxy-addr`, `--pod-name`, and a node role are available.
	A node role is derived from cluster state polling, so set `--cluster-state-ports` for full registration.
- `--container-names` must match the number of entries in `--poll-metrics-ports`.

## FODC Proxy CLI Flags

The proxy binary is `fodc-proxy` (see `fodc/proxy/cmd/proxy`).

| Flag | Default | Description |
| --- | --- | --- |
| `--grpc-listen-addr` | `:17912` | gRPC address for agent connections. |
| `--http-listen-addr` | `:17913` | HTTP address for REST/Prometheus endpoints. |
| `--agent-heartbeat-timeout` | `30s` | Mark agents offline if no heartbeat is received within this duration. |
| `--agent-cleanup-timeout` | `5m` | Unregister agents that remain offline beyond this duration. |
| `--max-agents` | `1000` | Maximum number of agents that can register. |
| `--grpc-max-msg-size` | `4194304` | Maximum gRPC message size in bytes. |
| `--http-read-timeout` | `10s` | HTTP server read timeout. |
| `--http-write-timeout` | `10s` | HTTP server write timeout. |
| `--heartbeat-interval` | `10s` | Default heartbeat interval communicated to agents on registration. |

