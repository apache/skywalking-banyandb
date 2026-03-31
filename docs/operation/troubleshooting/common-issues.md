# Common Issues

This document covers known issues that users may encounter during normal operation of BanyanDB. These issues are typically transient and resolve automatically.

## Group Not Found During Startup

### Symptom

After starting or restarting BanyanDB, you may see error logs like the following.

On **liaison** nodes:

```
{"level":"error","module":"SERVER-QUEUE-PUB-DATA","error":"failed to receive response for chunk 0: rpc error: code = Unknown desc = failed to create part handler: group zipkinTrace not found","time":"2026-03-18T05:17:05Z","message":"chunk send failed, aborting sync session"}
{"level":"warn","module":"TRACE","error":"failed to sync streaming part: failed to stream parts: failed to send chunk 0: failed to receive response for chunk 0: rpc error: code = Unknown desc = failed to create part handler: group zipkinTrace not found","node":"banyandb-data-hot-1.banyandb-data-hot-headless.banyandb:17912","partID":30059,"partType":"core","time":"2026-03-18T05:17:05Z","message":"failed to send part during replay"}
```

On **data** nodes:

```
{"level":"error","module":"TRACE","error":"group zipkinTrace not found","group":"zipkinTrace","time":"2026-03-18T05:17:25Z","message":"failed to load TSDB for group"}
{"level":"error","module":"SERVER-QUEUE-SUB","error":"failed to create part handler: group zipkinTrace not found","session_id":"sync-1773811045574321157","time":"2026-03-18T05:17:25Z","message":"failed to process chunk"}
```

The group name in the error (e.g., `zipkinTrace`) may vary depending on your configuration. These errors appear in the logs during the initial startup phase and affect both query and write operations temporarily.

### Applicable Scenario

This issue is most commonly seen when `schema-registry-mode` is set to `property`, which is the **default** mode. However, it can also occur in `etcd` mode when the node is under heavy CPU pressure.

BanyanDB supports two schema registry modes:

- **`property`** (default): Schema metadata is synchronized between nodes via an internal property-based protocol. This mode does not require an external etcd cluster. The startup delay is more pronounced in this mode due to the push-pull synchronization mechanism.
- **`etcd`**: Schema metadata is stored and distributed through etcd. This mode generally provides faster schema availability upon startup, but under high CPU pressure, nodes may still experience delays in loading the schema from etcd.

### Cause

In `property` mode, schema metadata is distributed through a hybrid push-pull mechanism:

- **Active push (watch)**: When a schema change occurs, the schema server broadcasts the event to all connected clients in real time via a gRPC watch stream.
- **Periodic sync (pull)**: Each node also polls for schema updates at a configurable interval (default: 30 seconds, `--schema-property-client-sync-interval`) as a fallback to ensure eventual consistency.

During startup, there is an inherent delay before the node receives the full schema. Two common factors contribute to this delay:

1. **Schema propagation latency**: Both the active push and periodic sync may have delays during startup, so the node may need to wait up to one full sync cycle (default: 30 seconds, configurable via `--schema-property-client-sync-interval`) before the group metadata becomes available.
2. **CPU resource constraints**: If the node is running in a resource-constrained environment (e.g., containers with limited CPU), both the watch stream establishment and the periodic sync process may take longer to complete.

### Resolution

This is a **transient issue** that resolves automatically once the schema sync completes. No manual intervention is required in most cases.

- **Wait for sync**: The error should disappear within one or two sync intervals (default: 30-60 seconds) after startup.
- **Check resource allocation**: If the error persists for an extended period, verify that the nodes have sufficient CPU and memory resources.
- **Adjust sync interval**: If faster startup convergence is needed, you can reduce the sync interval with `--schema-property-client-sync-interval` (e.g., `10s`), though this increases the background sync overhead.
- **Switch to etcd mode**: For environments where immediate schema availability at startup is critical, consider using `--schema-registry-mode=etcd` with an external etcd cluster.
