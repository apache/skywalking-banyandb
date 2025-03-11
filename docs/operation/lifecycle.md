# Lifecycle Management in BanyanDB

BanyanDB uses a hot-warm-cold data tiering strategy to optimize storage costs and query performance. The lifecycle management feature automatically migrates data between storage tiers based on configured policies.

## Overview

A dedicated lifecycle agent (an independent Go process) is responsible for:

- Identifying data eligible for migration based on lifecycle stages.
- Extracting data from the source node.
- Migrating data to designated target nodes.
- Removing the original data after a successful migration.

This process ensures data flows correctly through hot, warm, and cold stages as defined in your group configurations.

## Data Migration Process

The lifecycle agent performs the following steps:

1. **Identify Groups for Migration:**
   - Connect to the metadata service and list all groups.
   - Filter groups configured with multiple lifecycle stages.
   - Skip groups already processed (using progress tracking).

2. **Take Snapshots:**
   - Request a snapshot from the source data node.
   - Retrieve the stream and measure data directory paths.

3. **Setup Query Services:**
   - Create read-only services for streams and measures using the snapshot paths.

4. **Process Each Group:**
   - Determine the current stage based on node labels.
   - Identify target nodes for the next lifecycle stage.
   - For each stream or measure:
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

| Field         | Description                                                          |
| ------------- | -------------------------------------------------------------------- |
| `name`        | Stage name (e.g., "hot", "warm", "cold")                             |
| `shard_num`   | Number of shards allocated for this stage                            |
| `segment_interval` | Time interval for data segmentation (uses `IntervalRule`)       |
| `ttl`         | Time-to-live before data moves to the next stage (uses `IntervalRule`)|
| `node_selector` | Label selector to identify target nodes for this stage             |
| `close`       | Indicates whether to close segments that are no longer live          |

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
  --etcd-endpoints <etcd-endpoints> \
  --stream-root-path /path/to/stream \
  --measure-root-path /path/to/measure \
  --node-labels type=hot \
  --progress-file /path/to/progress.json \
  --schedule @daily
```

### Command-Line Parameters

| Parameter           | Description                                                               | Default Value                   |
| ------------------- | ------------------------------------------------------------------------- | ------------------------------- |
| `--node-labels`     | Labels of the current node (e.g., `type=hot,region=us-west`)                | `nil`                           |
| `--grpc-addr`       | gRPC address of the data node                                             | `127.0.0.1:17912`               |
| `--enable-tls`      | Enable TLS for gRPC connection                                            | `false`                         |
| `--insecure`        | Skip server certificate verification                                      | `false`                         |
| `--cert`            | Path to the gRPC server certificate                                       | `""`                            |
| `--stream-root-path`| Root directory for stream catalog snapshots                               | `/tmp`                          |
| `--measure-root-path`| Root directory for measure catalog snapshots                              | `/tmp`                          |
| `--progress-file`   | File path used for progress tracking and crash recovery                   | `/tmp/lifecycle-progress.json`  |
| `--etcd-endpoints`  | Endpoints for etcd connections                                             | `""`                            |
| `--schedule`        | Schedule for periodic backup (e.g., @yearly, @monthly, @weekly, @daily, etc.) | `""`                            |

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

   ```bash
   # Liaison node
   banyand liaison --etcd-endpoints <etcd-endpoints>

   # Hot node
   banyand data --node-labels type=hot --etcd-endpoints <etcd-endpoints> --grpc-port 17912
   
   # Warm node
   banyand data --node-labels type=warm --etcd-endpoints <etcd-endpoints> --grpc-port 18912
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

1. **Run the lifecycle agent:**

`lifecycle` connects to the `hot` node to manage data migration:

   ```bash
   lifecycle \
     --etcd-endpoints <etcd-endpoints> \
     --node-labels type=hot \
     --grpc-addr 127.0.0.1:17912 \
     --stream-root-path /data/stream \
     --measure-root-path /data/measure \
     --progress-file /data/progress.json
   ```

2. **Verify migration:**
   - Check logs for progress details.
   - Query both hot and warm nodes to ensure data availability.

This process automatically migrates data according to the defined lifecycle stages, optimizing your storage usage while maintaining data availability.
