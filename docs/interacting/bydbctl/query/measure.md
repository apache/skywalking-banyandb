# Query [Measures](../../../concept/data-model.md#measures)

Query operation queries the data in a measure.

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
Before querying the data, you need to know the measure name and the tag families and fields in the measure. You can use the `bydbctl measure get` command to get the measure schema.
If you want to get the schema of a measure named `service_cpm_minute` in the group `measure-minute`, you can use the below command:
```shell
bydbctl measure get -g measure-minute -n service_cpm_minute
```
```shell
measure:
  entity:
    tagNames:
    - entity_id
  fields:
  - compressionMethod: COMPRESSION_METHOD_ZSTD
    encodingMethod: ENCODING_METHOD_GORILLA
    fieldType: FIELD_TYPE_INT
    name: value
  - compressionMethod: COMPRESSION_METHOD_ZSTD
    encodingMethod: ENCODING_METHOD_GORILLA
    fieldType: FIELD_TYPE_INT
    name: total
  interval: 1m
  metadata:
    createRevision: "206"
    group: measure-minute
    id: 0
    modRevision: "206"
    name: service_cpm_minute
  tagFamilies:
  - name: storage-only
    tags:
    - name: entity_id
      type: TAG_TYPE_STRING
  updatedAt: null
```

## Examples
The following examples use above schema to show how to query data in a measure and cover some common use cases:

### Query between specific time range
To retrieve a series of data points between `2022-10-15T22:32:48Z` and `2022-10-15T23:32:48Z` could use the below command. These data points contain tags: `id` and `entity_id` that belong to a family `default`. They also choose fields: `total` and `value`.

```shell
bydbctl measure query -f - <<EOF
name: "service_cpm_minute"
groups: ["measure-minute"]
tagProjection:
  tagFamilies:
    - name: "storage-only"
      tags: ["entity_id"]
fieldProjection:
  names: ["total", "value"]
timeRange:
  begin: 2022-10-15T22:32:48Z
  end: 2022-10-15T23:32:48Z
EOF
```

### Query using relative time duration
The below command could query data in the last 30 minutes using relative time duration :

```shell
bydbctl measure query --start -30m -f - <<EOF
name: "service_cpm_minute"
groups: ["measure-minute"]
tagProjection:
  tagFamilies:
    - name: "storage-only"
      tags: ["entity_id"]
fieldProjection:
  names: ["total", "value"]
EOF
```

### Query with filter
The below command could query data with a filter where the entity_id is `bW9ja19iX3NlcnZpY2U=.1`:

```shell
bydbctl measure query -f - <<EOF
name: "service_cpm_minute"
groups: ["measure-minute"]
tagProjection:
  tagFamilies:
    - name: "storage-only"
      tags: ["entity_id"]
fieldProjection:
  names: ["total", "value"]
criteria:
  condition:
    name: "entity_id"
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
bydbctl measure query -f - <<EOF
name: "service_cpm_minute"
groups: ["measure-minute"]
tagProjection:
  tagFamilies:
    - name: "storage-only"
      tags: ["entity_id"]
fieldProjection:
  names: ["total", "value"]
orderBy:
  sort: "SORT_DESC"
EOF
```

### Query limit result
The below command could query ordered data and return the first two results:

```shell
bydbctl measure query -f - <<EOF
name: "service_cpm_minute"
groups: ["measure-minute"]
tagProjection:
  tagFamilies:
    - name: "storage-only"
      tags: ["entity_id"]
fieldProjection:
  names: ["total", "value"]
orderBy:
  sort: "SORT_DESC"
limit: 2
offset: 0
EOF
```

### Aggregation Query Max
The below command could query data with aggregate by entity_id and get `MAX` value:

```shell
bydbctl measure query -f - <<EOF
name: "service_cpm_minute"
groups: ["measure-minute"]
tagProjection:
  tagFamilies:
    - name: "storage-only"
      tags: ["entity_id"]
fieldProjection:
  names: ["total", "value"]
groupBy:
  tagProjection:
    tagFamilies:
    - name: "storage-only"
      tags: ["entity_id"]
  fieldName: "value"
agg:
  function: "AGGREGATION_FUNCTION_MAX"
  fieldName: "value"
EOF
```

### Aggregation Query TopN
The below command could query data with aggregate by entity_id and get `AVG` top 3 value:

```shell
bydbctl measure query -f - <<EOF
name: "service_cpm_minute"
groups: ["measure-minute"]
tagProjection:
  tagFamilies:
    - name: "storage-only"
      tags: ["entity_id"]
fieldProjection:
  names: ["value"]
groupBy:
  tagProjection:
    tagFamilies:
    - name: "storage-only"
      tags: ["entity_id"]
  fieldName: "value"
agg:
  function: "AGGREGATION_FUNCTION_MEAN"
  fieldName: "value"
top:
  number: 3
  fieldName: "value"
  fieldValueSort: "SORT_DESC"
EOF
```

### Query from Multiple Groups

When specifying multiple groups, use an array of group names and ensure that:

* Entity tag names must be identical across groups.
* Tags with the same name across different groups must share the same type.
* The orderBy tag and indexRuleName must be identical across groups.
* Any tags used in filter criteria must exist in all groups with the same name and type.
* All tags used in groupBy have identical names and types.
* All fields referenced in the aggregation function have identical names and types.
* All fields specified in top have identical names and types.

```shell
bydbctl measure query -f - <<EOF
name: "service_cpm_minute"
groups: ["measure-minute", "another-group"]
tagProjection:
  tagFamilies:
    - name: "storage-only"
      tags: ["entity_id"]
fieldProjection:
  names: ["total", "value"]
groupBy:
  tagProjection:
    tagFamilies:
      - name: "storage-only"
        tags: ["entity_id"]
  fieldName: "value"
agg:
  function: "AGGREGATION_FUNCTION_MAX"
  fieldName: "value"
top:
  number: 3
  fieldName: "value"
  fieldValueSort: "SORT_DESC"
EOF
```

### More examples can be found in [here](https://github.com/apache/skywalking-banyandb/tree/main/test/cases/measure/data/input).

## API Reference

[MeasureService v1](../../../api-reference.md#measureservice)
