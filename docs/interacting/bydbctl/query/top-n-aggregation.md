# Query [TopNAggregation](../../../concept/data-model.md#topnaggregation)

Query operation queries the data in a top-n-aggregation.

[bydbctl](../bydbctl.md) is the command line tool in examples.

The input contains two parts:

- Request: a YAML-based text which is defined by the [API](#api-reference)
- Time Range: YAML and CLI's flags both support it.

## Time Range

The query specification contains `time_range` field. The request should set absolute times to it. `bydbctl` also provides `start` and `end` flags to support passing absolute and relative times.

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

Before querying the data, you need to know the top-n-aggregation name, source measure and other basic information in the top-n-aggregation. You can use the `bydbctl topn get` command to get the top-n-aggregation schema.
If you want to get the schema of a top-n-aggregation named `service_instance_cpm_minute_top_bottom_100` in the group `sw_metric`, you can use the below command:

```shell
bydbctl topn get -g sw_metric -n service_instance_cpm_minute_top_bottom_100
```

```shell
topNAggregation:
  countersNumber: 1000
  criteria: null
  fieldName: value
  fieldValueSort: SORT_UNSPECIFIED
  groupByTagNames:
  - service_id
  lruSize: 10
  metadata:
    createRevision: "578"
    group: sw_metric
    id: 0
    modRevision: "578"
    name: service_instance_cpm_minute_top_bottom_100
  sourceMeasure:
    createRevision: "0"
    group: sw_metric
    id: 0
    modRevision: "0"
    name: service_instance_cpm_minute
  updatedAt: null
```

## Examples

The following examples use above schema to show how to query data in a top-n-aggregation and cover some common use cases:

### Query between specific time range

To retrieve a series of items between `2021-09-01T23:10:00Z` and `2021-09-01T23:35:00Z` could use the below command. These items also choose a tag `service_id` which lives in a family named `default`. The fieldValueSort is an enumeration type, with `0` for SORT_UNSPECIFIED, `1` for SORT_DESC, and `2` for SORT_ASC.

```shell
bydbctl topn query -f - <<EOF
name: "service_instance_cpm_minute_top_bottom_100"
groups: ["sw_metric"]
tagProjection:
  tagFamilies:
    - name: "default"
      tags: ["service_id"]
timeRange:
  begin: 2021-09-01T23:10:00+08:00
  end: 2021-09-01T23:35:00+08:00
topN: 3
agg: 2
fieldValueSort: 1
EOF
```

### Query using relative time duration

The below command could query data in the last 30 minutes using relative time duration :

```shell
bydbctl topn query --start -30m -f - <<EOF
name: "service_instance_cpm_minute_top_bottom_100"
groups: ["sw_metric"]
tagProjection:
  tagFamilies:
    - name: "default"
      tags: ["service_id"]
topN: 3
agg: 2
fieldValueSort: 1
EOF
```

### Query with filter

The below command could query data with a filter where the service_id is `svc_1`:

```shell
bydbctl topn query -f - <<EOF
name: "service_instance_cpm_minute_top_bottom_100"
groups: ["sw_metric"]
tagProjection:
  tagFamilies:
    - name: "default"
      tags: ["service_id"]
topN: 3
agg: 2
fieldValueSort: 1
criteria:
  condition:
    name: "service_id"
    op: "BINARY_OP_EQ"
    value:
      str:
        value: "svc_1"
EOF
```

More filter operations can be found in [here](filter-operation.md).

### Query ordered by time-series

The below command could query data order by time-series in descending [order](../../../api-reference.md#sort) :

```shell
bydbctl topn query -f - <<EOF
name: "service_instance_cpm_minute_top_bottom_100"
groups: ["sw_metric"]
tagProjection:
  tagFamilies:
    - name: "default"
      tags: ["service_id"]
topN: 3
agg: 2
fieldValueSort: 1
orderBy:
  sort: "SORT_DESC"
EOF
```

### More examples can be found in [here](https://github.com/apache/skywalking-banyandb/tree/main/test/cases/topn/data/input).

## API Reference

[MeasureService v1](../../../api-reference.md#measureservice)
