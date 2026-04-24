# CRUD [Streams](../../../concept/data-model.md#streams)

CRUD operations create, read, update and delete streams.

[bydbctl](../bydbctl.md) is the command line tool in examples.

Stream intends to store streaming data, for example, traces or logs.

## Create operation

Create operation adds a new stream to the database's metadata registry repository. If the stream does not currently exist, create operation will create the schema.

### Examples of creating

A stream belongs to a unique group. We should create such a group with a catalog `CATALOG_STREAM`
before creating a stream.

```shell
bydbctl group create -f - <<EOF
metadata:
  name: default
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

The group creates two shards to store stream data points. Every one day, it would create a segment.

The data in this group will keep 7 days.

Then, below command will create a new stream:

```shell
bydbctl stream create -f - <<EOF
metadata:
  name: sw
  group: default
tagFamilies:
  - name: searchable
    tags:
      - name: trace_id
        type: TAG_TYPE_STRING
entity:
  tagNames:
    - stream_id
EOF
```

## Get operation

Get(Read) operation get a stream's schema.

### Examples of getting

```shell
bydbctl stream get -g default -n sw
```

## Update operation

Update operation update a stream's schema.

### Examples of updating

`bydbctl` is the command line tool to update a stream in this example.

```shell
bydbctl stream update -f - <<EOF
metadata:
  name: sw
  group: default
tagFamilies:
  - name: searchable
    tags:
      - name: trace_id
        type: TAG_TYPE_STRING
      - name: trace_name
        type: TAG_TYPE_STRING
entity:
  tagNames:
    - stream_id
EOF

```

You only can append new tags to a stream. You can't change the existing tags. The order of tags is immutable, you can't change the order of tags. You can't insert a new tag in the middle or front of the existing tags.

## Delete operation

Delete operation delete a stream's schema.

### Examples of deleting

`bydbctl` is the command line tool to delete a stream in this example.

```shell
bydbctl stream delete -g default -n sw
```

## List operation

List operation list all streams' schema in a group.

### Examples of listing

```shell
bydbctl stream list -g default
```

## Server-Generated Element ID

When writing stream data, the `element_id` field in the write request is optional. If omitted, the server generates a unique element ID automatically. This simplifies client code when stable element IDs are not required.

Note that this generated `element_id` is server-side only. The StreamService `WriteResponse` does not return the generated ID, so clients cannot rely on retrieving it later for correlation, retries, or idempotent writes. If a client needs a stable or externally known element ID, it must set `element_id` explicitly in the write request.

## Query Deduplication

For details about stream query deduplication and element identity, see [Element Identity and Deduplication](../query/stream.md#element-identity-and-deduplication).

## API Reference

[Stream Registration Operations](../../../api-reference.md#streamregistryservice)
