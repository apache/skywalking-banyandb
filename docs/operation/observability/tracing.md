# Query Tracing

BanyanDB supports query tracing, which allows you to trace the execution of a query. The tracing data includes the query plan, execution time, and other useful information. You can enable query tracing by setting the `QueryRequest.trace` field to `true` when sending a query request.

The below command could query data in the last 30 minutes with `trace` enabled:

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
trace: true
EOF
```

The result will include the tracing data in the response. The duration time unit is in nano seconds.
