# CRUD [IndexRules](../../../concept/data-model.md#indexrule--indexrulebinding)

CRUD operations create, read, update and delete index rules.

IndexRule defines how to generate indices based on tags and the index type.
IndexRule should bind to a subject(stream or measure) through an IndexRuleBinding to generate proper indices.

[bydbctl](../bydbctl.md) is the command line tool in examples.

## Create operation

Create operation adds a new index rule to the database's metadata registry repository. If the index rule does not currently exist, create operation will create the schema.

### Examples of creating

An index rule belongs to its subjects' group. We should create such a group if there is no such group.

The command supposes that the index rule will bind to streams. So it creates a `CATALOG_STREAM` group here.

```shell
bydbctl group create -f - <<EOF
metadata:
  name: sw_stream
catalog: CATALOG_STREAM
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

The group creates two shards to store indexRule data points. Every day, it would create a
segment that will generate a block every 2 hours.

The data in this group will keep 7 days.

Then, the next command will create a new index rule:

```shell
bydbctl indexRule create -f - <<EOF
metadata:
  name: trace_id
  group: sw_stream
tags:
- trace_id
type: TYPE_INVERTED
EOF
```

This YAML creates an index rule which uses the tag `trace_id` to generate a `TYPE_INVERTED` index.

The `analyzer` field is optional. If it is not set, the default value is an empty string.
We can set it to `url` to specify the analyzer. More analyzers can refer to the [API Reference](../../../api-reference.md#indexruleanalyzer).
```shell
bydbctl indexRule create -f - <<EOF
metadata:
  name: trace_id
  group: sw_stream
tags:
- trace_id
type: TYPE_INVERTED
analyzer: url
EOF
```

## Get operation

Get(Read) operation gets an index rule's schema.

### Examples of getting

```shell
bydbctl indexRule get -g sw_stream -n trace_id
```

## Update operation

Update operation updates an index rule's schema.

### Examples of updating

This example changes the type from `TREE` to `INVERTED`.

```shell
bydbctl indexRule update -f - <<EOF
metadata:
  name: trace_id
  group: sw_stream
tags:
- trace_id
type: TYPE_INVERTED
EOF

```

## Delete operation

Delete operation deletes an index rule's schema.

### Examples of deleting

```shell
bydbctl indexRule delete -g sw_stream -n trace_id
```

## List operation

List operation list all index rules' schema in a group.

### Examples of listing

```shell
bydbctl indexRule list -g sw_stream
```

## API Reference

[IndexRule Registration Operations](../../../api-reference.md#indexruleregistryservice)
