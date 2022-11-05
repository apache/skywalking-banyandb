# Query Measures

Query operation queries the data in a measure.

[`bydbctl`](../../clients.md#command-line) is the command line tool in examples.

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

## Examples

To retrieve a series of data points between `2022-10-15T22:32:48Z` and `2022-10-15T23:32:48Z` could use the below command. These data points contain tags: `id` and `entity_id` that belong to a family `default`. They also choose fields: `total` and `value`.

```shell
$ bydbctl measure query -f - <<EOF
metadata:
  name: "service_cpm_minute"
  group: "sw_metric"
tagProjection:
  tagFamilies:
  - name: "default"
    tags: ["id", "entity_id"]
fieldProjection:
  names: ["total", "value"]
timeRange:
  begin: 2022-10-15T22:32:48Z
  end: 2022-10-15T23:32:48Z
EOF
```

The below command could query data in the last 30 minutes using relative time duration :

```shell
$ bydbctl measure query --start -30m -f - <<EOF
metadata:
  name: "service_cpm_minute"
  group: "sw_metric"
tagProjection:
  tagFamilies:
  - name: "default"
    tags: ["id", "entity_id"]
fieldProjection:
  names: ["total", "value"]
EOF
```

## API Reference

[MeasureService v1](../../api-reference.md#measureservice)
