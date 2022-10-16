# CRUD Groups

CRUD operations create, read, update and delete groups.
Group is an internal object for Group management

## Create operation

Create operation adds a new group to the database's metadata registry repository. If the group does not currently exist, create operation will create the schema.

### Examples

`bydbctl` is the command line tool to create a group in this example.


```shell
$ bydbctl group create -f - <<EOF
metadata:
  name: sw_metric
catalog: CATALOG_MEASURE
resource_opts:
  shard_num: 2
  block_interval:
    unit: UNIT_HOUR
    num: 2
  segment_interval:
    unit: UNIT_DAY
    num: 1
  ttl:
    unit: UNIT_DAY
    num: 7
EOF
```

The group creates two shards to store group data points. Every one day, it would create a
segment which will generate a block every 2 hours.

The data in this group will keep 7 days.
```

## Read operation

Read(Get) operation get a group's schema.


### Examples
`bydbctl` is the command line tool to read a group in this example.
```shell
$ bydbctl get -g sw_metric
```

## Update operation
Update operation update a group's schema.

### Examples

`bydbctl` is the command line tool to update a group in this example.
If we want to change the ttl of the data in this group to be 1 day, use the command:
```shell
$ bydbctl group create -f - <<EOF
metadata:
  name: sw_metric
catalog: CATALOG_MEASURE
resource_opts:
  shard_num: 2
  block_interval:
    unit: UNIT_HOUR
    num: 2
  segment_interval:
    unit: UNIT_DAY
    num: 1
  ttl:
    unit: UNIT_DAY
    num: 1
EOF

```

## Delete operation
Delete operation delete a group's schema.
### Examples
`bydbctl` is the command line tool to delete a group in this example.
```shell
$ bydbctl delete -g sw_metric

```
## List operation
List operation list group's schema.
### Examples
`bydbctl` is the command line tool to list all the group in this example.
```shell
$ bydbctl group list
```

## API Reference
[GroupService v1](../../api-reference.md#groupservice)
