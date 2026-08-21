# BydbQL Examples

## Measure

```bydbql
SELECT * FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10
```

```bydbql
SELECT endpoint, AVG(latency) FROM MEASURE service_latency IN production TIME > '-30m' GROUP BY endpoint LIMIT 10
```

## SHOW TOP

```bydbql
SHOW TOP 10 FROM MEASURE service_latency IN production TIME > '-30m' AGGREGATE BY SUM ORDER BY DESC
```

## Stream / Trace / Property

```bydbql
SELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT 10
```

```bydbql
SELECT * FROM TRACE zipkin_span IN default TIME > '-30m' ORDER BY timestamp_millis DESC LIMIT 30
```

```bydbql
SELECT * FROM TRACE sw_trace IN default TIME > '-30m' WHERE trace_id = 'abc123' LIMIT 30
```

```bydbql
SELECT * FROM PROPERTY server_metadata IN default LIMIT 10
```
