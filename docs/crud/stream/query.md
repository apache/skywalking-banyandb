# Query Streams

Query operation query the data of a stream.

timeRange is a range query with begin/end time of entities in the timeunit of milliseconds.In the context of stream, it represents the range of the `startTime` for spans/segments, while in the context of Log, it means the range of the timestamp(s) for logs. It is always recommended to specify time range for performance reason<br>
You can pass either absolute time or relative time to start and end option:<br>
* if --start and --end are both absent, then: start := now - 30min; end := now 
* if --start is given, --end is absent, then: end := now + 30 units, where unit is the precision of `start`, (hours, minutes, etc.)
* if --start is absent, --end is given, then: start := end - 30 units, where unit is the precision of `end`, (hours, minutes, etc.)

the time unit of timeRange must be one of units listed below:<br>
w for week, d for day, h for hour, m for minute, s for second, ms for millisecond.

projection can be used to select the key names of the element in the response.
## Examples
`bydbctl` is the command line tool to query the data of a stream in this example.
```shell
$ bydbctl stream query -f - <<EOF
metadata:
  name: sw
  group: default
timeRange:
  begin: 2022-10-15T22:32:48+08:00
  end: 2022-10-15T23:32:48+08:00
projection:
  tagFamilies:
    - name: searchable
      tags:
        - trace_id`
```
below command could query the stream data using relative time duration :
```shell
$ bydbctl stream query -f - <<EOF
metadata:
  name: sw
  group: default
timeRange:
  begin: 30m
  end: 0m
projection:
  tagFamilies:
    - name: searchable
      tags:
        - trace_id`
```

## API Reference

[StreamService v1](../../api-reference.md#streamservice)
