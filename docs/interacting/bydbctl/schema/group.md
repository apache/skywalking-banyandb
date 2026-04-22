# CRUD [Groups](../../../concept/data-model.md#groups)

CRUD operations create, read, update and delete groups.

The group represents a collection of a class of resources. Each resource has a name unique to a group.

[bydbctl](../bydbctl.md) is the command line tool in examples.

## Create operation

Create operation adds a new group to the database's metadata registry repository. If the group does not currently exist, create operation will create the schema.

### Examples of creating

```shell
bydbctl group create -f - <<EOF
metadata:
  name: sw_metric
catalog: CATALOG_MEASURE
resource_opts:
  shard_num: 2
  segment_interval:
    unit: UNIT_DAY
    num: 1
  ttl:
    unit: UNIT_DAY
    num: 7
EOF
```

The group creates two shards to store group data points. Every day, it would create a segment.

The data in this group will keep 7 days.

## Get operation

Get operation gets a group's schema.

### Examples of getting

```shell
bydbctl group get -g sw_metric
```

## Update operation

Update operation updates a group's schema.

### Examples of updating

If we want to change the `ttl` of the data in this group to be 1 day, use the command:

```shell
bydbctl group update -f - <<EOF
metadata:
  name: sw_metric
catalog: CATALOG_MEASURE
resource_opts:
  shard_num: 2
  segment_interval:
    unit: UNIT_DAY
    num: 1
  ttl:
    unit: UNIT_DAY
    num: 1
EOF
```

You can't change the unit of `segment_interval`. If you want to change the unit, you should delete the group and create a new one.

## Delete operation

Delete operation removes a group's data and, unless `--data-only` is set, its associated schema resources. For non-dry-run deletes, BanyanDB creates a background task so the request can return quickly while data files and schema resources are removed in order.

### Delete modes

- Default delete only succeeds when the group is empty.
- `--force` allows deleting a non-empty group.
- `--dry-run` previews the schema resources that would be deleted and does not create a deletion task.
- `--data-only` removes only the data files and keeps the group metadata and schema resources.

### Examples of deleting

Delete an empty group:

```shell
bydbctl group delete -g sw_metric
```

Preview what would be deleted from a non-empty group:

```shell
bydbctl group delete -g sw_metric --dry-run
```

Force delete a non-empty group:

```shell
bydbctl group delete -g sw_metric --force
```

Delete only the data files while keeping the schema:

```shell
bydbctl group delete -g sw_metric --force --data-only
```

### Deletion task status

For non-dry-run deletes, BanyanDB creates a background deletion task. The task typically moves through these phases:

- `PHASE_PENDING`: the task is created and waiting to start.
- `PHASE_IN_PROGRESS`: data files and schema resources are being removed.
- `PHASE_COMPLETED`: deletion finished successfully.
- `PHASE_FAILED`: deletion stopped with an error message.

`bydbctl` currently exposes the delete request itself, but not a dedicated subcommand for polling the task status. Because `bydbctl` uses BanyanDB's HTTP endpoints, you can query the task status with any HTTP client:

```shell
curl http://127.0.0.1:17913/api/v1/group/task/sw_metric
```

Example response:

```json
{
  "task": {
    "currentPhase": "PHASE_IN_PROGRESS",
    "totalCounts": {
      "stream": 1,
      "index_rule_binding": 1
    },
    "deletedCounts": {
      "index_rule_binding": 1
    },
    "totalDataSizeBytes": "1048576",
    "deletedDataSizeBytes": "524288",
    "message": "deleting streams",
    "createdAt": "2026-04-21T10:30:00Z",
    "updatedAt": "2026-04-21T10:31:15Z"
  }
}
```

The task endpoint is especially useful after `--force` or `--data-only`, because those operations may continue after the initial delete request returns.

## List operation

The list operation shows all groups' schema.

### Examples

```shell
bydbctl group list
```

## API Reference

- [Group Registration Operations](../../../api-reference.md#banyandb-database-v1-GroupRegistryService)
- [GroupDeletionTask](../../../api-reference.md#banyandb-database-v1-GroupDeletionTask)
