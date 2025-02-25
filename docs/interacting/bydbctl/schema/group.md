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

The group creates two shards to store group data points. Every day, it would create a segment that will generate a block every 2 hours.

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

Delete operation deletes a group's schema.

### Examples of deleting

```shell
bydbctl group delete -g sw_metric
```

## List operation

The list operation shows all groups' schema.

### Examples

```shell
bydbctl group list
```

## API Reference
[Group Registration Operations](../../../api-reference.md#groupregistryservice)
