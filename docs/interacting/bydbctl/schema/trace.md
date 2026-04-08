# CRUD [Traces](../../../concept/data-model.md#traces)

CRUD operations create, read, update and delete traces.

[bydbctl](../bydbctl.md) is the command line tool in examples.

Trace is designed for distributed tracing data. It groups spans by trace ID for efficient trace-level queries.

## Create operation

Create operation adds a new trace to the database's metadata registry repository. If the trace does not currently exist, create operation will create the schema.

### Examples of creating

A trace belongs to a unique group. We should create such a group with a catalog `CATALOG_TRACE`
before creating a trace.

```shell
bydbctl group create -f - <<EOF
metadata:
  name: sw_trace
catalog: CATALOG_TRACE
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

The group creates two shards to store trace data. Every one day, it would create a segment.

The data in this group will keep 7 days.

Then, below command will create a new trace:

```shell
bydbctl trace create -f - <<EOF
metadata:
  name: sw
  group: sw_trace
tags:
  - name: trace_id
    type: TAG_TYPE_STRING
  - name: state
    type: TAG_TYPE_INT
  - name: service_id
    type: TAG_TYPE_STRING
  - name: service_instance_id
    type: TAG_TYPE_STRING
  - name: endpoint_id
    type: TAG_TYPE_STRING
  - name: duration
    type: TAG_TYPE_INT
  - name: span_id
    type: TAG_TYPE_STRING
  - name: timestamp
    type: TAG_TYPE_TIMESTAMP
trace_id_tag_name: trace_id
span_id_tag_name: span_id
timestamp_tag_name: timestamp
EOF
```

The `trace_id_tag_name`, `span_id_tag_name` and `timestamp_tag_name` fields are required. They specify which tags hold the trace ID, span ID and timestamp respectively. The database engine uses `trace_id_tag_name` to group spans into traces.

## Get operation

Get(Read) operation gets a trace's schema.

### Examples of getting

```shell
bydbctl trace get -g sw_trace -n sw
```

## Update operation

Update operation updates a trace's schema.

### Examples of updating

`bydbctl` is the command line tool to update a trace in this example.

```shell
bydbctl trace update -f - <<EOF
metadata:
  name: sw
  group: sw_trace
tags:
  - name: trace_id
    type: TAG_TYPE_STRING
  - name: state
    type: TAG_TYPE_INT
  - name: service_id
    type: TAG_TYPE_STRING
  - name: service_instance_id
    type: TAG_TYPE_STRING
  - name: endpoint_id
    type: TAG_TYPE_STRING
  - name: duration
    type: TAG_TYPE_INT
  - name: span_id
    type: TAG_TYPE_STRING
  - name: timestamp
    type: TAG_TYPE_TIMESTAMP
  - name: service_name
    type: TAG_TYPE_STRING
trace_id_tag_name: trace_id
span_id_tag_name: span_id
timestamp_tag_name: timestamp
EOF
```

You can only append new tags to a trace. You can't change or remove existing tags. The order of tags is immutable.

## Delete operation

Delete operation deletes a trace's schema.

### Examples of deleting

`bydbctl` is the command line tool to delete a trace in this example.

```shell
bydbctl trace delete -g sw_trace -n sw
```

## List operation

List operation list all traces in a group.

### Examples of listing

```shell
bydbctl trace list -g sw_trace
```

## API Reference

[TraceRegistryService v1](../../../api-reference.md#traceregistryservice)

