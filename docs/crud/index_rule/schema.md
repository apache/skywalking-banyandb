# CRUD IndexRules

CRUD operations create, read, update and delete indexRule binding.

IndexRule defines how to generate indices based on tags and the index type.
IndexRule should bind to a subject through an IndexRuleBinding to generate proper indices.

## Create operation

Create operation adds a new indexRule to the database's metadata registry repository. If the indexRule does not currently exist, create operation will create the schema.

### Examples

`bydbctl` is the command line tool to create a indexRule in this example.

A indexRule belongs to a unique group. We should create such a group with a catalog `CATALOG_STREAM`
before creating a indexRule.

```shell
$ bydbctl group create -f - <<EOF
metadata:
  name: sw_metric
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

The group creates two shards to store indexRule data points. Every one day, it would create a
segment which will generate a block every 2 hours.

The data in this group will keep 7 days.

Then, below command will create a new indexRule:

```shell
$ bydbctl indexRule create -f - <<EOF
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

Read(Get) operation get a indexRule's schema.


### Examples
`bydbctl` is the command line tool to create a indexRule in this example.
```shell
$ bydbctl get -g default -n sw
```

## Update operation
Update operation update a indexRule's schema.

### Examples

`bydbctl` is the command line tool to update a indexRule in this example.
```shell
$ bydbctl indexRule create -f - <<EOF
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
Delete operation delete a indexRule's schema.
### Examples
`bydbctl` is the command line tool to delete an indexRule in this example.
```shell
$ bydbctl indexRuleBind delete -g default -n sw
```

## List operation
List operation list all indexRule's schema of a group.
### Examples
`bydbctl` is the command line tool to list all the indexRules in a group in this example.
```shell
$ bydbctl indexRule list -g default
```
## API Reference

[indexRuleService v1](../../api-reference.md#IndexRuleRegistryService)