# Lifecycle Management in BanyanDB

BanyanDB uses a hot-warm-cold data tiering strategy to optimize storage costs and query performance. The lifecycle management feature automatically migrates data between storage tiers based on configured policies.

## Overview

A dedicated lifecycle agent (an independent Go process) is responsible for:

- Identifying data eligible for migration based on lifecycle stages.
- Extracting data from the source node.
- Migrating data to designated target nodes.
- Removing the original data after a successful migration.

This process ensures data flows correctly through hot, warm, and cold stages as defined in your group configurations.

> **Prerequisite: node discovery must be configured.** The lifecycle agent resolves the target nodes of every stage by listing all data nodes through the cluster's node registry and filtering them by the stage's `node_selector`. In the default `--node-discovery-mode=none` setting the registry is empty, so the agent cannot find any destination to migrate data to and the run will be a no-op. Configure `dns` or `file` mode on the lifecycle agent using the same source of truth as the rest of the cluster; see the [node discovery documentation](node-discovery.md) for the full list of `--node-discovery-*` flags.

## Data Migration Process

The lifecycle agent performs the following steps:

1. **Identify Groups for Migration:**
   - Connect to the metadata service and list all groups.
   - Filter groups configured with multiple lifecycle stages.
   - Skip groups already processed (using progress tracking).

2. **Take Snapshots:**
   - Request a snapshot from the source data node.
   - Retrieve the stream, measure and trace data directory paths.

3. **Setup Query Services:**
   - Create read-only services for streams, measures and traces using the snapshot paths.

4. **Process Each Group:**
   - Determine the current stage based on node labels.
   - Identify target nodes for the next lifecycle stage.
   - For each stream, measure or trace:
     - Query data from the current stage.
     - Transform data into write requests.
     - Send write requests to target nodes.
   - Delete the source data after a successful migration.

5. **Progress Tracking:**
   - Track migration progress in a persistent progress file.
   - Enable crash recovery by resuming from the last saved state.
   - Clean up the progress file once the migration is complete.

## Configuration

Lifecycle stages are defined within the group configuration using the `LifecycleStage` structure.

| Field              | Description                                                            |
| ------------------ | ---------------------------------------------------------------------- |
| `name`             | Stage name (e.g., "hot", "warm", "cold")                               |
| `shard_num`        | Number of shards allocated for this stage                              |
| `segment_interval` | Time interval for data segmentation (uses `IntervalRule`)              |
| `ttl`              | Time-to-live before data moves to the next stage (uses `IntervalRule`) |
| `node_selector`    | Label selector to identify target nodes for this stage                 |
| `close`            | Indicates whether to close segments that are no longer live            |

### Example Configuration

```yaml
metadata:
  name: example-group
catalog: CATALOG_MEASURE
resource_opts:
  shard_num: 4
  segment_interval:
    unit: UNIT_DAY
    num: 1
  ttl:
    unit: UNIT_DAY
    num: 7
  stages:
    - name: warm
      shard_num: 2
      segment_interval:
        unit: UNIT_DAY
        num: 7
      ttl:
        unit: UNIT_DAY
        num: 30
      node_selector: "type=warm"
      close: true
    - name: cold
      shard_num: 1
      segment_interval:
        unit: UNIT_DAY
        num: 30
      ttl:
        unit: UNIT_DAY
        num: 365
      node_selector: "type=cold"
      close: true
```

In this example:

- Data starts in the default stage with 4 shards and daily segments.
- After 7 days, data moves to the "warm" stage with 2 shards and weekly segments.
- After 30 days in the warm stage, data transitions to the "cold" stage with 1 shard and monthly segments.
- Data is purged after 365 days in the cold stage.

## Command-Line Usage

The lifecycle command offers options to customize data migration:

```bash
lifecycle \
  --grpc-addr 127.0.0.1:17912 \
  --stream-root-path /path/to/stream \
  --measure-root-path /path/to/measure \
  --trace-root-path /path/to/trace \
  --node-labels type=hot \
  --progress-file /path/to/progress.json \
  --schedule @daily \
  --node-discovery-mode file \
  --node-discovery-file-path /etc/banyandb/nodes.yaml
```

### Command-Line Parameters

| Parameter             | Description                                                                   | Default Value                  |
| --------------------- | ----------------------------------------------------------------------------- | ------------------------------ |
| `--node-labels`       | Labels of the current node (e.g., `type=hot,region=us-west`)                  | `nil`                          |
| `--grpc-addr`         | gRPC address of the source data node to snapshot and read from                | `127.0.0.1:17912`              |
| `--enable-tls`        | Enable TLS for gRPC connection                                                | `false`                        |
| `--insecure`          | Skip server certificate verification                                          | `false`                        |
| `--cert`              | Path to the gRPC server certificate                                           | `""`                           |
| `--stream-root-path`  | Root directory for stream catalog snapshots                                   | `/tmp`                         |
| `--measure-root-path` | Root directory for measure catalog snapshots                                  | `/tmp`                         |
| `--trace-root-path`   | Root directory for trace catalog snapshots                                    | `/tmp`                         |
| `--progress-file`     | File path used for progress tracking and crash recovery                       | `/tmp/lifecycle-progress.json` |
| `--schedule`          | Schedule for periodic backup (e.g., @yearly, @monthly, @weekly, @daily, etc.) | `""`                           |

## Automatic Behavior on Warm and Cold Nodes

When a data node matches a lifecycle stage (i.e., its labels match a stage's `node_selector`), two behaviors are automatically configured:

- **Rotation is disabled**: The rotation task (which pre-creates segments on hot nodes) is not started on warm or cold nodes. Segments are created on demand by the lifecycle migration process, ensuring correct segment boundaries for multi-day intervals.
- **Retention is disabled on non-last stages**: TTL-based segment deletion is disabled on all stages except the last one. Data removal on intermediate stages is managed by the lifecycle migration process, not by the built-in retention mechanism.

If a node's labels do not match any stage, both rotation and retention are disabled as a safety measure.

The lifecycle agent also accepts the full suite of `--node-discovery-*` flags so it can enumerate every candidate target node in the cluster. At a minimum you need `--node-discovery-mode` and either `--node-discovery-dns-srv-addresses` (DNS mode) or `--node-discovery-file-path` (file mode). The set of nodes visible to the lifecycle agent must include every warm/cold data node that any stage's `node_selector` could match — use the same discovery configuration that the liaison nodes use. See the [node discovery documentation](node-discovery.md) for the complete reference.

## Best Practices

1. **Node Labeling:**
   - Use clear, consistent labels that reflect node capabilities and roles.
2. **Sizing Considerations:**
   - Allocate more shards to hot nodes for higher write performance.
   - Optimize storage by reducing shard count in warm and cold stages.
3. **TTL Planning:**
   - Set appropriate TTLs based on data access patterns.
   - Consider access frequency when defining stage transitions.
4. **Migration Scheduling:**
   - Run migrations during off-peak periods.
   - Monitor system resource usage during migrations.
5. **Progress Tracking:**
   - Use a persistent location for the progress file to aid in recovery.

## Example Workflow

1. **Setup nodes with appropriate labels:**

   Every node (liaison, hot, warm, cold, and the lifecycle agent itself) must point at the same node-discovery source so that the lifecycle agent and liaison nodes see the identical topology. The example below uses file mode with a shared `/etc/banyandb/nodes.yaml`; see the [node discovery documentation](node-discovery.md) for DNS mode.

   ```bash
   # Liaison node
   banyand liaison \
     --node-discovery-mode=file \
     --node-discovery-file-path=/etc/banyandb/nodes.yaml

   # Hot data node
   banyand data \
     --node-labels type=hot \
     --grpc-port 17912 \
     --node-discovery-mode=file \
     --node-discovery-file-path=/etc/banyandb/nodes.yaml

   # Warm data node
   banyand data \
     --node-labels type=warm \
     --grpc-port 18912 \
     --node-discovery-mode=file \
     --node-discovery-file-path=/etc/banyandb/nodes.yaml
   ```

2. **Create a group with lifecycle stages.**

`bydbctl` connects the `liaison` node to create a group with lifecycle stages:

```bash
   bydbctl group create -f - <<EOF
metadata:
   name: example-group
catalog: CATALOG_MEASURE
resource_opts:
   shard_num: 4
   segment_interval:
      unit: UNIT_DAY
      num: 1
   ttl:
      unit: UNIT_DAY
      num: 7
   stages:
      - name: warm
      shard_num: 2
      segment_interval:
         unit: UNIT_DAY
         num: 7
      ttl:
         unit: UNIT_DAY
         num: 30
      node_selector: "type=warm"
      close: true
EOF
```

3. **Run the lifecycle agent:**

`lifecycle` connects to the `hot` node (as its snapshot source via `--grpc-addr`) and uses node discovery to locate the warm/cold target nodes:

   ```bash
   lifecycle \
     --node-labels type=hot \
     --grpc-addr 127.0.0.1:17912 \
     --stream-root-path /data/stream \
     --measure-root-path /data/measure \
     --trace-root-path /data/trace \
     --progress-file /data/progress.json \
     --node-discovery-mode=file \
     --node-discovery-file-path=/etc/banyandb/nodes.yaml
   ```

4. **Verify migration:**
   - Check logs for progress details.
   - Query both hot and warm nodes to ensure data availability.

This process automatically migrates data according to the defined lifecycle stages, optimizing your storage usage while maintaining data availability.
