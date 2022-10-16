# CRUD indexRuleBindings

CRUD operations create, read, update and delete indexRuleBinding binding.

IndexRuleBinding is a bridge to connect severalIndexRules to a subject
This binding is valid between begin_at_nanoseconds and expire_at_nanoseconds, that provides flexible strategies
to control how to generate time series indices.

## Create operation

Create operation adds a new indexRuleBinding to the database's metadata registry repository. If the indexRuleBinding does not currently exist, create operation will create the schema.

### Examples

`bydbctl` is the command line tool to create a indexRuleBinding in this example.

A indexRuleBinding belongs to a unique group. We should create such a group with a catalog `CATALOG_STREAM`
before creating a indexRuleBinding.

```shell
$ bydbctl group create -f - <<EOF
metadata:
  name: default
catalog: CATALOG_STREAM
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

The group creates two shards to store indexRuleBinding data points. Every one day, it would create a
segment which will generate a block every 2 hours.

The data in this group will keep 7 days.

Then, below command will create a new indexRuleBinding:

```shell
$ bydbctl indexRuleBinding create -f - <<EOF
metadata:
  name: sw
  group: default
tagFamilies:
  - name: searchable
    tags: 
      - name: trace_id
        type: TAG_TYPE_STRING
EOF
```

## Read operation

Read(Get) operation get an indexRuleBinding's schema.


### Examples
`bydbctl` is the command line tool to get a indexRuleBinding in this example.
```shell
$ bydbctl indexRuleBinding get -g default -n sw
```

## Update operation
Update operation update an indexRuleBinding's schema.

### Examples

`bydbctl` is the command line tool to update an indexRuleBinding in this example.
```shell
$ bydbctl indexRuleBinding create -f - <<EOF
metadata:
  name: sw
  group: default
tagFamilies:
  - name: searchable
    tags: 
      - name: trace_id
        type: TAG_TYPE_STRING
EOF

```

## Delete operation
Delete operation delete an indexRuleBinding's schema.
### Examples
`bydbctl` is the command line tool to delete a indexRuleBinding in this example.
```shell
$ bydbctl indexRuleBinding delete -g default -n sw
```

## List operation
List operation list all indexRuleBinding's schema of a group.
### Examples
`bydbctl` is the command line tool to list all the indexRuleBindings in a group in this example.
```shell
$ bydbctl indexRuleBinding list -g default
```
## API Reference

[indexRuleBindingService v1](../../api-reference.md#IndexRuleBindingRegistryService)