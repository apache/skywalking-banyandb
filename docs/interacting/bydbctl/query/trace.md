# Query [Traces](../../../concept/data-model.md#traces)

Query operation queries the data in a trace.

[bydbctl](../bydbctl.md) is the command line tool in examples.

The input contains two parts:

- Request: a YAML-based text which is defined by the [API](#api-reference)
- Time Range: YAML and CLI's flags both support it.

## Time Range

The query specification contains `time_range` field. The request should set absolute times to it.
`bydbctl` also provides `start` and `end` flags to support passing absolute and relative times.

"start" and "end" specify a time range during which the query is performed, they can be an absolute time like ["2006-01-02T15:04:05Z07:00"](https://www.rfc-editor.org/rfc/rfc3339),
or relative time (to the current time) like "-30m", or "30m".
They are both optional and their default values follow the rules below:

- when "start" and "end" are both absent, "start = now - 30 minutes" and "end = now",
  namely past 30 minutes;
- when "start" is absent and "end" is present, this command calculates "start" (minus 30 units),
  e.g. "end = 2022-11-09T12:34:00Z", so "start = end - 30 minutes = 2022-11-09T12:04:00Z";
- when "start" is present and "end" is absent, this command calculates "end" (plus 30 units),
  e.g. "start = 2022-11-09T12:04:00Z", so "end = start + 30 minutes = 2022-11-09T12:34:00Z".

## Understand the schema you are querying

Before querying the data, you need to know the trace name and the tags defined in the trace. You can use the `bydbctl trace get` command to get the trace schema.
For example, if you want to get the schema of a trace named `sw` in the group `sw_trace`, you can use the below command:

```shell
bydbctl trace get -g sw_trace -n sw
```

```yaml
trace:
  metadata:
    createRevision: "100"
    group: sw_trace
    id: 0
    modRevision: "100"
    name: sw
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
  traceIdTagName: trace_id
  spanIdTagName: span_id
  timestampTagName: timestamp
  updatedAt: null
```

## Examples

The following examples use the above schema to show how to query data in a trace and cover some common use cases:

### Query between specific time range

To retrieve traces named `sw` between `2022-10-15T22:32:48Z` and `2022-10-15T23:32:48Z`, the response will contain matching spans grouped by trace ID. These traces also choose a tag projection `trace_id`.

```shell
bydbctl trace query -f - <<EOF
groups: ["sw_trace"]
name: "sw"
tag_projection: ["trace_id"]
timeRange:
  begin: 2022-10-15T22:32:48+08:00
  end: 2022-10-15T23:32:48+08:00
EOF
```

### Query using relative time duration

The below command could query data in the last 30 minutes using relative time duration:

```shell
bydbctl trace query --start -30m -f - <<EOF
groups: ["sw_trace"]
name: "sw"
tag_projection: ["trace_id"]
EOF
```

### Query with filter by trace ID

The below command could query data filtered by a specific trace ID:

```shell
bydbctl trace query -f - <<EOF
name: "sw"
groups: ["sw_trace"]
tag_projection: ["trace_id"]
criteria:
  condition:
    name: "trace_id"
    op: "BINARY_OP_EQ"
    value:
      str:
        value: "trace_001"
EOF
```

### Query with filter

The below command could query data with multiple filter conditions using logical AND:

```shell
bydbctl trace query -f - <<EOF
name: "sw"
groups: ["sw_trace"]
criteria:
  le:
    op: "LOGICAL_OP_AND"
    left:
      condition:
        name: "service_instance_id"
        op: "BINARY_OP_EQ"
        value:
          str:
            value: "webapp_instance_1"
    right:
      condition:
        name: "endpoint_id"
        op: "BINARY_OP_EQ"
        value:
          str:
            value: "/home_endpoint"
orderBy:
  indexRuleName: "timestamp"
  sort: SORT_ASC
EOF
```

More filter operations can be found in [here](filter-operation.md).

### Query ordered by index

The below command could query data ordered by duration in descending [order](../../../api-reference.md#sort):

```shell
bydbctl trace query -f - <<EOF
name: "sw"
groups: ["sw_trace"]
orderBy:
  indexRuleName: "duration"
  sort: "SORT_DESC"
EOF
```

### Query with limit

The below command could query ordered data and return the first two results:

```shell
bydbctl trace query -f - <<EOF
name: "sw"
groups: ["sw_trace"]
orderBy:
  indexRuleName: "timestamp"
  sort: "SORT_DESC"
limit: 2
EOF
```

### Query from Multiple Groups

When querying data from multiple groups, you can combine traces that share the same trace name. Note the following requirements:

- Tags with the same name across different groups must share the same type.
- The orderBy indexRuleName must be identical across groups.
- Any tags used in filter criteria must exist in all groups with the same name and type.

```shell
bydbctl trace query -f - <<EOF
name: "sw"
groups: ["sw_trace", "another_trace_group"]
tag_projection: ["trace_id", "service_id"]
criteria:
  condition:
    name: "trace_id"
    op: "BINARY_OP_IN"
    value:
      str_array:
        value: ["trace_001", "trace_002"]
EOF
```

### More examples can be found in [here](https://github.com/apache/skywalking-banyandb/tree/main/test/cases/trace/data/input).

## API Reference

[TraceService v1](../../../api-reference.md#traceservice)

