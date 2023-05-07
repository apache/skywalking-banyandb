# Query Streams

Query operation queries the data in a stream.

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

To retrieve elements in a stream named `sw` between `2022-10-15T22:32:48Z` and `2022-10-15T23:32:48Z` could use the below command. These elements also choose a tag `trace_id` which lives in a family named `searchable`.

```shell
$ bydbctl stream query -f - <<EOF
metadata:
  group: "default"
  name: "sw"
projection:
  tagFamilies:
  - name: "searchable"
    tags: ["trace_id"]
timeRange:
  begin: 2022-10-15T22:32:48+08:00
  end: 2022-10-15T23:32:48+08:00
EOF
```

The below command could query data in the last 30 minutes using relative time duration :

```shell
$ bydbctl stream query --start -30m -f - <<EOF
metadata:
  group: "default"
  name: "sw"
projection:
  tagFamilies:
  - name: "searchable"
    tags: ["trace_id"]
EOF
```

## API Reference

[StreamService v1](../../api-reference.md#streamservice)
