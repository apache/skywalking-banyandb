# Query [Streams](../../../concept/data-model.md#streams)

Query operation queries the data in a stream.

[bydbctl](../bydbctl.md) is the command line tool in examples.

The input contains two parts:

* Request: a YAML-based text which is defined by the [API](#api-reference)
* Time Range: YAML and CLI's flags both support it.

## Time Range

The query specification contains `time_range` field. The request should set absolute times to it.
`bydbctl` also provides `start` and `end` flags to support passing absolute and relative times.

"start" and "end" specify a time range during which the query is performed, they can be an absolute time like ["2006-01-02T15:04:05Z07:00"](https://www.rfc-editor.org/rfc/rfc3339),
or relative time (to the current time) like "-30m", or "30m".
They are both optional and their default values follow the rules below:

* when "start" and "end" are both absent, "start = now - 30 minutes" and "end = now",
  namely past 30 minutes;
* when "start" is absent and "end" is present, this command calculates "start" (minus 30 units),
  e.g. "end = 2022-11-09T12:34:00Z", so "start = end - 30 minutes = 2022-11-09T12:04:00Z";
* when "start" is present and "end" is absent, this command calculates "end" (plus 30 units),
  e.g. "start = 2022-11-09T12:04:00Z", so "end = start + 30 minutes = 2022-11-09T12:34:00Z".


## Understand the schema you are querying
Before querying the data, you need to know the stream name and the tag families and fields in the stream. You can use the `bydbctl stream get` command to get the stream schema.
for example, if you want to get the schema of a stream named `segment` in the group `stream-segment`, you can use the below command:
```shell
bydbctl stream get -g stream-segment -n segment
```
```shell
stream:
  entity:
    tagNames:
    - service_id
    - service_instance_id
    - is_error
  metadata:
    createRevision: "100"
    group: stream-segment
    id: 0
    modRevision: "100"
    name: segment
  tagFamilies:
  - name: storage-only
    tags:
    - indexedOnly: false
      name: service_id
      type: TAG_TYPE_STRING
    - indexedOnly: false
      name: service_instance_id
      type: TAG_TYPE_STRING
    - indexedOnly: false
      name: start_time
      type: TAG_TYPE_INT
    - indexedOnly: false
      name: is_error
      type: TAG_TYPE_INT
    - indexedOnly: false
      name: data_binary
      type: TAG_TYPE_DATA_BINARY
  - name: searchable
    tags:
    - indexedOnly: false
      name: segment_id
      type: TAG_TYPE_STRING
    - indexedOnly: false
      name: trace_id
      type: TAG_TYPE_STRING
    - indexedOnly: false
      name: endpoint_id
      type: TAG_TYPE_STRING
    - indexedOnly: false
      name: latency
      type: TAG_TYPE_INT
    - indexedOnly: true
      name: tags
      type: TAG_TYPE_STRING_ARRAY
  updatedAt: null
```

## Examples
The following examples use above schema to show how to query data in a stream and cover some common use cases:

### Query between specific time range
To retrieve elements in a stream named `sw` between `2022-10-15T22:32:48Z` and `2022-10-15T23:32:48Z` could use the below command. These elements also choose a tag `trace_id` which lives in a family named `searchable`.

```shell
bydbctl stream query -f - <<EOF
groups: ["stream-segment"]
name: "segment"
projection:
  tagFamilies:
    - name: "searchable"
      tags: ["trace_id"]
timeRange:
  begin: 2022-10-15T22:32:48+08:00
  end: 2022-10-15T23:32:48+08:00
EOF
```

### Query using relative time duration
The below command could query data in the last 30 minutes using relative time duration :

```shell
bydbctl stream query --start -30m -f - <<EOF
groups: ["stream-segment"]
name: "segment"
projection:
  tagFamilies:
    - name: "searchable"
      tags: ["trace_id"]
EOF
```

### Query with filter
The below command could query data with a filter where the service_id is `bW9ja19iX3NlcnZpY2U=.1`:

```shell
bydbctl stream query -f - <<EOF
name: "segment"
groups: ["stream-segment"]
projection:
  tagFamilies:
    - name: "searchable"
      tags: ["service_id", "trace_id", "latency"]
criteria:
  condition:
    name: "service_id"
    op: "BINARY_OP_EQ"
    value:
      str:
        value: "bW9ja19iX3NlcnZpY2U=.1"
EOF
```

More filter operations can be found in [here](filter-operation.md).

### Query ordered by time-series
The below command could query data order by time-series in descending [order](../../../api-reference.md#sort) :

```shell
bydbctl stream query -f - <<EOF
name: "segment"
groups: ["stream-segment"]
projection:
  tagFamilies:
    - name: "searchable"
      tags: ["trace_id", "latency"]
    - name: "storage-only"
      tags: ["start_time", "data_binary"]
orderBy:
  sort: "SORT_DESC"
EOF
```

### Query ordered by index (searchable tag)
The below command could query data order by index in descending [order](../../../api-reference.md#sort) :

```shell
bydbctl stream query -f - <<EOF
name: "segment"
groups: ["stream-segment"]
projection:
  tagFamilies:
    - name: "searchable"
      tags: ["trace_id", "latency"]
    - name: "storage-only"
      tags: ["start_time", "data_binary"]
orderBy:
  indexRuleName: "latency"
  sort: "SORT_DESC"
EOF
```

### Query limit result
The below command could query ordered data and return the first two results:

```shell
bydbctl stream query -f - <<EOF
name: "segment"
groups: ["stream-segment"]
projection:
  tagFamilies:
    - name: "searchable"
      tags: ["trace_id", "latency"]
    - name: "storage-only"
      tags: ["start_time", "data_binary"]
orderBy:
  indexRuleName: "latency"
  sort: "SORT_DESC"
limit: 2
offset: 0
EOF
```

### More examples can be found in [here](https://github.com/apache/skywalking-banyandb/tree/main/test/cases/stream/data/input).

## API Reference

[StreamService v1](../../../api-reference.md#streamservice)
