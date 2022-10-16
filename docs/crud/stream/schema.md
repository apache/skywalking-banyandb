# CRUD Streams

CRUD operations create, read, update and delete streams.

Stream intends to store streaming data, for example, traces or logs.
## Create operation

Create operation adds a new stream to the database's metadata registry repository. If the stream does not currently exist, create operation will create the schema.

### Examples

`bydbctl` is the command line tool to create a stream in this example.

A stream belongs to a unique group. We should create such a group with a catalog `CATALOG_STREAM`
before creating a stream.

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

The group creates two shards to store stream data points. Every one day, it would create a
segment which will generate a block every 2 hours.

The data in this group will keep 7 days.

Then, below command will create a new stream:

```shell
$ bydbctl stream create -f - <<EOF
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

Read(Get) operation get a stream's schema.


### Examples
`bydbctl` is the command line tool to create a stream in this example.
```shell
$ bydbctl get -g default -n sw
```

## Update operation
Update operation update a stream's schema.

### Examples

`bydbctl` is the command line tool to update a stream in this example.
```shell
$ bydbctl stream create -f - <<EOF
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
Delete operation delete a stream's schema.
### Examples
`bydbctl` is the command line tool to delete a stream in this example.
```shell
$ bydbctl delete -g default -n sw
```

## List operation
List operation list all streams' schema in a group.
### Examples
`bydbctl` is the command line tool to list all the streams in a group in this example.
```shell
$ bydbctl stream list -g default
```
## API Reference

[StreamService v1](../../api-reference.md#streamservice)
