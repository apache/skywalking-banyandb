# BydbQL Syntax Reference

Use this reference to generate one read-only BanyanDB Query Language statement from natural language. Keep dynamic names such as groups, resource names, tags, fields, and sortable fields aligned with live BanyanDB schema discovered through MCP.

## Core Rules

- Statement forms: `SELECT ... FROM ...` or `SHOW TOP ... FROM MEASURE ...`.
- Resource types: `STREAM`, `MEASURE`, `TRACE`, `PROPERTY`.
- `FROM <RESOURCE_TYPE> <resource_name> IN <group>[, <group>...]` is required.
- Parentheses around group lists are optional: `IN default, staging` and `IN (default, staging)` are both valid.
- `TIME` is strongly recommended for `STREAM`, `MEASURE`, `TRACE`, and `SHOW TOP` queries and is normally expected at execution, but the grammar treats it as optional, so the parser (and `validate_bydbql`) will not flag its absence. `PROPERTY` queries do not use `TIME`.
- Keywords are case-insensitive. Identifiers are case-sensitive and should match schema names exactly.
- Identifiers may be simple (`service_id`), dotted (`http.method`), or quoted when needed (`"count"`). String filter values should use single quotes.
- Clause order for `SELECT`: `SELECT` -> `FROM` -> `TIME` -> `WHERE` -> `GROUP BY` -> `ORDER BY` -> `WITH QUERY_TRACE` -> `LIMIT` -> `OFFSET`.
- Clause order for `SHOW TOP`: `SHOW TOP` -> `FROM` -> `TIME` -> `WHERE` -> `AGGREGATE BY` -> `ORDER BY` -> `WITH QUERY_TRACE`.

## SELECT Form

```sql
SELECT <projection>
FROM STREAM|MEASURE|TRACE|PROPERTY <name>
IN <group>[, <group>...]
[ON <stage>[, <stage>...] STAGES]
[TIME <time_condition>]
[WHERE <criteria>]
[GROUP BY <tag>[, <tag>...]]
[ORDER BY <field_or_TIME> [ASC|DESC] | ORDER BY ASC|DESC]
[WITH QUERY_TRACE]
[LIMIT <n>]
[OFFSET <n>]
```

Projection forms:

- `*`: all projected columns supported by the resource.
- `()`: empty projection, useful for trace queries when only matching traces are needed.
- `tag_or_field`: schema-inferred tag or field.
- `identifier::TAG` or `identifier::FIELD`: disambiguate a measure key that can be both a tag and a field.
- `SUM(field)`, `MEAN(field)`, `AVG(field)`, `COUNT(field)`, `MAX(field)`, `MIN(field)`: measure aggregations.
- `TOP <n> <order_field> [ASC|DESC], <other_columns...>`: SELECT-form top query for measures/streams when supported by the transformer.

## SHOW TOP Form

```sql
SHOW TOP <n>
FROM MEASURE <name>
IN <group>[, <group>...]
[ON <stage>[, <stage>...] STAGES]
TIME <time_condition>
[WHERE <topn_criteria>]
[AGGREGATE BY SUM|MEAN|AVG|COUNT|MAX|MIN]
[ORDER BY ASC|DESC]
[WITH QUERY_TRACE]
```

Use `SHOW TOP` for measure ranking requests such as "top 10", "highest", "lowest", "best", or "worst" over a measure. `SHOW TOP` order direction has no field name; use `ORDER BY DESC` for highest/top and `ORDER BY ASC` for lowest/bottom.

## TIME Clause

Supported forms:

```sql
TIME = '<timestamp>'
TIME > '<timestamp>'
TIME < '<timestamp>'
TIME >= '<timestamp>'
TIME <= '<timestamp>'
TIME BETWEEN '<begin>' AND '<end>'
```

Timestamp values:

- Absolute RFC3339: `'2023-01-01T00:00:00Z'`, `'2023-01-01T15:30:45+08:00'`.
- Relative: `'-30m'`, `'-2h'`, `'-1d'`, `'-1w'`, `'now'`.
- Units: `m` minutes, `h` hours, `d` days, `w` weeks.

Natural-language mapping:

- "last 30 minutes", "past hour", "recent 1 day" -> `TIME > '-30m'`, `TIME > '-1h'`, `TIME > '-1d'`.
- "between X and Y" -> `TIME BETWEEN 'X' AND 'Y'`.
- Time windows are not limits. Do not turn "last 3 days" into `LIMIT 3`.

## WHERE Criteria

General `WHERE` supports:

```sql
<identifier> = <value>
<identifier> != <value>
<identifier> > <value>
<identifier> < <value>
<identifier> >= <value>
<identifier> <= <value>
<identifier> IN (<value>[, <value>...])
<identifier> NOT IN (<value>[, <value>...])
<identifier> HAVING <value_or_list>
<identifier> NOT HAVING <value_or_list>
<identifier> MATCH(...)
```

Values:

- String: `'frontend'`, `'/api/users'`.
- Integer: `200`, `500`.
- Null: `NULL`.
- Lists: `('frontend', 'backend')`, `(200, 500)`.

Logical composition:

- Parentheses have highest precedence.
- `AND` binds tighter than `OR`.
- Use parentheses for mixed `AND`/`OR` whenever natural language is ambiguous.

Examples:

```sql
WHERE service_id = 'webapp' AND status_code >= 500
WHERE service_id IN ('frontend', 'backend')
WHERE (status = 'error' OR status = 'warn') AND service = 'checkout'
WHERE tags HAVING ('region', 'prod')
```

## MATCH Full-Text Search

Use `MATCH` for text search in stream, measure, and trace tags/fields when indexed for text analysis. Do not use `MATCH` for `SHOW TOP` or `PROPERTY`.

Forms:

```sql
WHERE message MATCH('error')
WHERE http_url MATCH('/api/users', 'url')
WHERE message MATCH(('error', 'warning'), 'standard', 'OR')
WHERE operation_name MATCH(('GET', 'POST'), 'keyword')
```

Parameters:

- First parameter is one value or a parenthesized list.
- Optional analyzer: `'standard'`, `'simple'`, `'keyword'`, `'url'`.
- Optional operator for multiple values: `'AND'` or `'OR'`.

Use `"keyword"` style matching for exact terms, `"standard"` for free text, and `"url"` for URL/path fields.

## Streams

Streams query raw time-series elements such as logs and events.

```sql
SELECT <projection>
FROM STREAM <stream_name> IN <group>[, <group>...]
[ON <stage>[, <stage>...] STAGES]
TIME <time_condition>
[WHERE <criteria>]
[ORDER BY <field_or_TIME> [ASC|DESC] | ORDER BY ASC|DESC]
[WITH QUERY_TRACE]
[LIMIT <n>]
[OFFSET <n>]
```

Examples:

```sql
SELECT *
FROM STREAM logs IN default
TIME > '-30m'
WHERE message MATCH('error')
LIMIT 100
```

```sql
SELECT trace_id, service_id
FROM STREAM sw IN group1, group2
TIME BETWEEN '-2h' AND 'now'
WHERE service_id = 'webapp'
ORDER BY start_time DESC
LIMIT 100
```

## Measures

Measures query numerical time-series metrics. They support tag/field projection, aggregation, and grouping.

```sql
SELECT <tag_or_field_or_aggregation>[, ...]
FROM MEASURE <measure_name> IN <group>[, <group>...]
[ON <stage>[, <stage>...] STAGES]
TIME <time_condition>
[WHERE <criteria>]
[GROUP BY <tag>[, <tag>...]]
[ORDER BY <field_or_aggregation> [ASC|DESC]]
[WITH QUERY_TRACE]
[LIMIT <n>]
[OFFSET <n>]
```

Aggregation functions:

- `SUM(field)`
- `MEAN(field)` or `AVG(field)`
- `COUNT(field)`
- `MAX(field)`
- `MIN(field)`

Measure-specific guidance:

- Use `GROUP BY` when the user asks for "by service", "per instance", "grouped by region", etc.
- Use `identifier::TAG` or `identifier::FIELD` when a key name is ambiguous.
- If the user asks for a raw metric value and does not ask for aggregation, use `SELECT *` or specific fields.
- If the user asks for average, sum, max, min, or count, use an aggregation projection and add `GROUP BY` only when a grouping dimension is requested.

Examples:

```sql
SELECT service, MEAN(response_time)
FROM MEASURE http_metrics IN metricsMinute
TIME > '-30m'
WHERE region = 'us-west'
GROUP BY service
```

```sql
SELECT status::TAG, status::FIELD
FROM MEASURE http_requests IN default
TIME > '-30m'
WHERE path = '/api/v1/users'
```

## Top-N

Use `SHOW TOP` for optimized measure ranking.

```sql
SHOW TOP <n>
FROM MEASURE <measure_name> IN <group>[, <group>...]
[ON <stage>[, <stage>...] STAGES]
TIME <time_condition>
[WHERE <simple_conditions>]
[AGGREGATE BY SUM|MEAN|AVG|COUNT|MAX|MIN]
[ORDER BY ASC|DESC]
[WITH QUERY_TRACE]
```

Examples:

```sql
SHOW TOP 10
FROM MEASURE service_latency IN production
TIME > '-30m'
WHERE http_method = 'GET'
AGGREGATE BY MEAN
ORDER BY DESC
```

```sql
SHOW TOP 5
FROM MEASURE endpoint_errors IN production
TIME BETWEEN '-24h' AND 'now'
WHERE status_code = '500'
AGGREGATE BY SUM
ORDER BY DESC
```

Top-N generation rules:

- "top N", "highest N", "largest N" -> `SHOW TOP N ... ORDER BY DESC`.
- "bottom N", "lowest N", "smallest N" -> `SHOW TOP N ... ORDER BY ASC`.
- Do not add `LIMIT` to `SHOW TOP`; the `TOP <n>` value is the result count.

## Properties

Properties query metadata/key-value records. They require group/resource names but do not require `TIME`.

```sql
SELECT <projection>
FROM PROPERTY <property_name> IN <group>[, <group>...]
[WHERE <criteria>]
[ORDER BY <field> [ASC|DESC] | ORDER BY ASC|DESC]
[WITH QUERY_TRACE]
[LIMIT <n>]
[OFFSET <n>]
```

Property criteria commonly use `ID`:

```sql
SELECT *
FROM PROPERTY server_metadata IN datacenter-1
WHERE ID = 'server-1a2b3c'
```

```sql
SELECT ip, region
FROM PROPERTY server_metadata IN datacenter-1
WHERE ID IN ('server-1', 'server-2')
LIMIT 10
```

## Traces

Traces query trace/span data. Use `SELECT ()` when the user wants trace records without projecting tags.

```sql
SELECT <projection>|()
FROM TRACE <trace_name> IN <group>[, <group>...]
[ON <stage>[, <stage>...] STAGES]
TIME <time_condition>
[WHERE <criteria>]
[ORDER BY <field_or_TIME> [ASC|DESC] | ORDER BY ASC|DESC]
[WITH QUERY_TRACE]
[LIMIT <n>]
[OFFSET <n>]
```

Examples:

```sql
SELECT ()
FROM TRACE sw_trace IN default
TIME > '-30m'
WHERE status = 'error'
LIMIT 50
```

```sql
SELECT trace_id, operation_name
FROM TRACE zipkin_span IN default
TIME > '-30m'
WHERE operation_name MATCH(('GET', 'POST'), 'keyword')
ORDER BY timestamp_millis DESC
LIMIT 30
```

## Lifecycle Stages

Use lifecycle stage filtering when the user asks to query warm/cold/warn stages or explicitly names stages.

```sql
FROM STREAM sw IN default ON warn, cold STAGES
FROM MEASURE service_cpm IN metricsMinute ON (warn, cold) STAGES
FROM TRACE sw_trace IN default ON warn STAGES
SHOW TOP 10 FROM MEASURE service_latency IN production ON warn, cold STAGES TIME > '-30m'
```

## Query Trace

Append `WITH QUERY_TRACE` when the user asks for query execution debug/tracing details.

```sql
SELECT *
FROM STREAM logs IN default
TIME > '-30m'
WITH QUERY_TRACE
```

## ORDER BY and LIMIT

- Use `ORDER BY` only when the user asks for sorted, latest, earliest, highest, lowest, top, bottom, first, or last results.
- For "latest N" or "last N records", use a timestamp-like indexed field when known, for example `ORDER BY timestamp_millis DESC LIMIT N`.
- If no sortable field is known, prefer discovering schema/index information or omit `ORDER BY` rather than inventing a field.
- `LIMIT` is for result count, not time range.
- `OFFSET` is for pagination.
- For `SHOW TOP`, do not use `LIMIT`.

## Natural-Language Mapping Checklist

Before finalizing a query:

1. Select the resource form: logs/events -> `STREAM`; metrics/statistics -> `MEASURE`; trace records/spans -> `TRACE`; metadata/config/key-value -> `PROPERTY`; ranking over metrics -> `SHOW TOP`.
2. Determine group and resource name. If missing or ambiguous, call `list_groups_schemas`.
3. Add `TIME` for stream/measure/trace/topn queries.
4. Add filters from user constraints as `WHERE`.
5. Add aggregation/grouping only when requested by words like average, sum, count, max, min, per, by, grouped by.
6. Add ordering and limit only when result count or sort direction is requested.
7. Validate with `validate_bydbql` before execution.
