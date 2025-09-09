# BanyanDB Query Language (BydbQL)

## 1. Introduction

### 1.1. Purpose

This document outlines the design of the BanyanDB Data Query Language (BydbQL), a unified query language with SQL-like syntax for all of BanyanDB's data models. The primary goal of BydbQL is to provide users with a familiar, intuitive, and powerful interface to interact with their data, abstracting the underlying protobuf-based APIs for streams, measures, properties, and Top-N queries.

This document specifies the language's syntax, its semantics, and the precise mapping from BydbQL statements to the various `Request` messages.

### 1.2. Scope

BydbQL supports querying across BanyanDB's primary data models:
*   **Streams**: For raw, time-series elements like logs and traces.
*   **Measures**: For aggregated numerical time-series data (metrics).
*   **Properties**: For metadata and key-value information.

It also provides a specialized syntax for optimized **Top-N** queries against measures.

## 2. Core Concepts

### 2.1. Parser Architecture

BydbQL queries will be processed by a classic three-stage compiler front-end architecture, which ensures modularity and maintainability.

```
BydbQL Query String
        ↓
      Lexer
        ↓
     Parser
        ↓
Abstract Syntax Tree (AST)
        ↓
   Transformer
        ↓
  Protobuf Request
```
*   **Lexer**: Breaks the query string into a sequence of tokens.
*   **Parser**: Builds an Abstract Syntax Tree (AST) from the tokens, validating the query's syntax.
*   **Transformer**: Traverses the AST, performs semantic analysis using a schema, and transforms the AST into the appropriate target protobuf `Request` message.

### 2.2. Distinguishing Query Types

BydbQL distinguishes the target data model either explicitly through keywords in the `FROM` clause or implicitly through the **execution context**. This allows the parser to apply the correct grammar and transformation rules for the query.

*   **Explicit**: `FROM STREAM <name>`, `FROM MEASURE <name>`, `FROM PROPERTY <name>`
*   **Implicit**: The query is executed against a resource-specific endpoint (e.g., `/v1/streams/{stream_name}/query`), where the type and name of the resource are known.

Specialized queries, like Top-N, use a distinct top-level command and typically require an explicit `FROM` clause:

*   **Top-N**: `SHOW TOPN ... FROM MEASURE <measure_name>`

### 2.3. Optional `FROM` Clause

In BydbQL, the `FROM` clause is **optional** for `SELECT` queries. When it is omitted, the target resource (the specific stream, measure, or property) **must** be supplied by the execution context.

The simplest possible BydbQL query is `SELECT *`. When executed within the context of a stream named `sw`, this is equivalent to `SELECT * FROM STREAM sw`.

When the `FROM` clause is present, it overrides any context provided by the environment. This is useful for clients that connect to a generic query endpoint and need to specify the target resource directly within the query text.

### 2.4. Case Sensitivity

BydbQL follows SQL-like conventions for case sensitivity:

*   **Reserved words are case-insensitive**: Keywords like `SELECT`, `FROM`, `WHERE`, `ORDER BY`, `TIME`, `BETWEEN`, `AND`, etc. can be written in any case combination.
*   **Identifiers are case-sensitive**: Names of streams, measures, properties, tags, and fields preserve their case and must be referenced exactly as defined.

#### Examples

All of these queries are equivalent:
```sql
SELECT * FROM STREAM sw WHERE service_id = 'webapp';

select * from stream sw where service_id = 'webapp';

Select * From Stream sw Where service_id = 'webapp';
```

But these refer to different identifiers:
```sql
-- Different tag names (case-sensitive)
SELECT ServiceName FROM STREAM sw;  -- refers to tag "ServiceName"
SELECT servicename FROM STREAM sw;  -- refers to tag "servicename"

-- Different stream names (case-sensitive)
FROM STREAM MyStream    -- refers to stream "MyStream"
FROM STREAM mystream    -- refers to stream "mystream"
```

**Best Practice**: Use uppercase for reserved words and consistent casing for identifiers to maintain readability.

## 2.5. Timestamp Formats

BydbQL supports flexible timestamp specifications in TIME clauses, accommodating both absolute and relative time formats:

### 2.5.1. Absolute Time Format

Absolute timestamps use the RFC3339 standard format:
```
"2006-01-02T15:04:05Z07:00"
```

Examples:
* `"2023-01-01T00:00:00Z"` - January 1, 2023, 00:00:00 UTC
* `"2023-01-01T15:30:45+08:00"` - January 1, 2023, 15:30:45 UTC+8
* `"2023-12-31T23:59:59Z"` - December 31, 2023, 23:59:59 UTC

### 2.5.2. Relative Time Format

Relative timestamps are duration strings that are evaluated relative to the current time:
```
[-][duration]
```

Supported duration units:
* `m` - minutes
* `h` - hours  
* `d` - days
* `w` - weeks

Examples:
* `"-30m"` - 30 minutes ago
* `"2h"` - 2 hours from now
* `"-1d"` - 1 day ago
* `"-1w"` - 1 week ago
* `"now"` - current time

### 2.5.3. Usage in TIME Clauses

Both absolute and relative formats can be used interchangeably in TIME conditions:

```sql
-- Absolute time examples
TIME = '2023-01-01T00:00:00Z'
TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'
TIME > '2023-01-01T08:00:00Z'

-- Relative time examples  
TIME > '-30m'
TIME BETWEEN '-1h' AND 'now'
TIME < '-1d'
```

The `parseTime` function automatically determines whether a timestamp is absolute (RFC3339) or relative (duration string) and converts it appropriately.

## 4. BydbQL for Streams

BydbQL for streams is designed for querying and retrieving raw time-series elements. The syntax maps to the `banyandb.stream.v1.QueryRequest` message.

### 4.1. Grammar

```
query           ::= SELECT projection [from_stream_clause] [TIME time_condition] [WHERE criteria] [ORDER BY order_expression] [LIMIT integer] [OFFSET integer]
from_stream_clause ::= "FROM STREAM" identifier ["IN" "(" group_list ")"]
projection      ::= "*" | column_list
column_list     ::= identifier ("," identifier)*
group_list      ::= identifier ("," identifier)*
criteria        ::= condition (("AND" | "OR") condition)*
condition       ::= identifier binary_op (value | value_list)
time_condition  ::= "=" timestamp | ">" timestamp | "<" timestamp | ">=" timestamp | "<=" timestamp | "BETWEEN" timestamp "AND" timestamp
binary_op       ::= "=" | "!=" | ">" | "<" | ">=" | "<=" | "IN" | "NOT IN" | "HAVING" | "NOT HAVING" | "MATCH"
order_expression::= identifier ["ASC" | "DESC"]
value           ::= string_literal | integer_literal | "NULL"
value_list      ::= "(" value ("," value)* ")"
timestamp       ::= string_literal | integer_literal
	/* timestamp supports both absolute and relative time formats:
	   - Absolute: RFC3339 format like "2006-01-02T15:04:05Z07:00"
	   - Relative: duration strings like "-30m", "2h", "1d" (relative to current time) */
identifier      ::= [a-zA-Z_][a-zA-Z0-9_]*
string_literal  ::= "'" [^']* "'" | "\"" [^\"]* "\""
integer_literal ::= [0-9]+
```

### 4.2. Mapping to `stream.v1.QueryRequest`

*   **`FROM STREAM name IN (groups)`**: Maps to the `name` and `groups` fields. If the clause is omitted, these values are taken from the execution context.
*   **`SELECT tags`**: Maps to `projection`. Requires a stream schema to resolve tags to their families.
*   **`TIME = '2023-01-01T00:00:00Z'`**: Maps to `time_range` with `begin` and `end` set to the same timestamp.
*   **`TIME > '2023-01-01T00:00:00Z'`**: Maps to `time_range` with `begin` set to the timestamp.
*   **`TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'`**: Maps to `time_range` with `begin` and `end` set to the respective timestamps.
*   **`TIME > '-30m'`**: Maps to `time_range` with `begin` set to 30 minutes ago.
*   **`TIME BETWEEN '-1h' AND 'now'`**: Maps to `time_range` from 1 hour ago to current time.
*   **`WHERE conditions`**: Maps to `criteria`.
*   **`ORDER BY field`**: Maps to `order_by`.
*   **`LIMIT`/`OFFSET`**: Maps to `limit` and `offset`.

### 4.3. Examples

```sql
-- Simplest query (context must provide the stream name, e.g., 'sw')
SELECT *;

-- Basic selection with filtering and ordering
SELECT trace_id, service_id
FROM STREAM sw IN (default, updated)
WHERE service_id = 'webapp' AND state = 1
ORDER BY start_time DESC
LIMIT 100;

-- Project all tags from a stream (FROM is explicit)
SELECT * 
FROM STREAM sw 
WHERE state = 0 
LIMIT 10;

-- Use more complex conditions with IN and OR
SELECT trace_id, duration
WHERE service_id IN ('webapp', 'api-gateway') OR http.method = 'POST';

-- Query with time range using TIME clause
SELECT trace_id, service_id, start_time
FROM STREAM sw
TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'
WHERE service_id = 'webapp'
ORDER BY start_time DESC;

-- Query data after a specific timestamp
SELECT *
FROM STREAM sw
TIME > '2023-01-01T12:00:00Z'
WHERE state = 1
LIMIT 100;

-- Query data at an exact timestamp
SELECT trace_id, duration
FROM STREAM sw
TIME = '2023-01-01T15:30:00Z';

-- Query with relative time - last 30 minutes
SELECT *
FROM STREAM sw
TIME > '-30m'
WHERE state = 1
LIMIT 100;

-- Query with relative time range - last 2 hours
SELECT trace_id, service_id, start_time
FROM STREAM sw
TIME BETWEEN '-2h' AND 'now'
WHERE service_id = 'webapp'
ORDER BY start_time DESC;

-- Query data within the last hour using absolute time
SELECT *
FROM STREAM sw
TIME >= '2023-01-01T13:00:00Z'
WHERE status = 'error';

-- Query data older than 1 day ago
SELECT trace_id, duration
FROM STREAM sw
TIME < '-1d';
```

## 5. BydbQL for Measures

BydbQL for measures is tailored for analytical queries on aggregated numerical data. It supports aggregation, grouping, and mixed selection of tags and fields, mapping to the `banyandb.measure.v1.QueryRequest` message.

### 5.1. Grammar

```
measure_query     ::= SELECT projection [from_measure_clause] [TIME time_condition] [WHERE criteria] [GROUP BY group_list] [ORDER BY order_expression] [LIMIT integer] [OFFSET integer]
from_measure_clause ::= "FROM MEASURE" identifier ["IN" "(" group_list ")"]
projection        ::= "*" | (column_list | agg_function "(" identifier ")" | "TOP" integer projection)
column_list       ::= identifier ("," identifier)* ["::tag" | "::field"]
agg_function      ::= "SUM" | "MEAN" | "COUNT" | "MAX" | "MIN"
group_list        ::= identifier ("," identifier)*
criteria          ::= condition (("AND" | "OR") condition)*
condition         ::= identifier binary_op (value | value_list)
time_condition    ::= "=" timestamp | ">" timestamp | "<" timestamp | ">=" timestamp | "<=" timestamp | "BETWEEN" timestamp "AND" timestamp
binary_op         ::= "=" | "!=" | ">" | "<" | ">=" | "<=" | "IN" | "NOT IN" | "HAVING" | "NOT HAVING" | "MATCH"
order_expression  ::= identifier ["ASC" | "DESC"]
value             ::= string_literal | integer_literal | "NULL"
value_list        ::= "(" value ("," value)* ")"
timestamp         ::= string_literal | integer_literal
	/* timestamp supports both absolute and relative time formats:
	   - Absolute: RFC3339 format like "2006-01-02T15:04:05Z07:00"
	   - Relative: duration strings like "-30m", "2h", "1d" (relative to current time) */
identifier        ::= [a-zA-Z_][a-zA-Z0-9_]*
string_literal    ::= "'" [^']* "'" | "\"" [^\"]* "\""
integer_literal   ::= [0-9]+
```

### 5.2. BydbQL Extensions for `SELECT`

The `SELECT` clause for measures is highly flexible, allowing for the selection of tags, fields, and aggregations in a single, flat list.

*   `SELECT <field_key>, <tag_key>`: Returns specific fields and tags. The parser will infer the type of each identifier from the measure's schema.
*   `SELECT <identifier>::field, <identifier>::tag`: If a field and a tag share the same name, the `::field` or `::tag` syntax **must** be used to disambiguate the identifier's type.
*   The clause also supports aggregation functions (`SUM`, `MEAN`, `COUNT`, `MAX`, `MIN`) and a `TOP N` clause for ranked results.

### 5.3. Mapping to `measure.v1.QueryRequest`

*   **`FROM MEASURE name IN (groups)`**: Maps to the `name` and `groups` fields. If the clause is omitted, these values are taken from the execution context.
*   **`SELECT <tag1>, <field1>, <field2>`**: The transformer inspects each identifier. Those identified as tags (either by schema lookup or `::tag`) are added to `tag_projection`. Those identified as fields (by schema lookup or `::field`) are added to `field_projection`.
*   **`SELECT SUM(field)`**: Maps to `agg`.
*   **`TIME = '2023-01-01T00:00:00Z'`**: Maps to `time_range` with `begin` and `end` set to the same timestamp.
*   **`TIME > '2023-01-01T00:00:00Z'`**: Maps to `time_range` with `begin` set to the timestamp.
*   **`TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'`**: Maps to `time_range` with `begin` and `end` set to the respective timestamps.
*   **`TIME > '-30m'`**: Maps to `time_range` with `begin` set to 30 minutes ago.
*   **`TIME BETWEEN '-1h' AND 'now'`**: Maps to `time_range` from 1 hour ago to current time.
*   **`GROUP BY <tag1>, <tag2>`**: The `GROUP BY` clause takes a simple list of tags and maps to `group_by.tag_projection`.
*   **`SELECT TOP N ...`**: Maps to the `top` message.

### 5.4. Examples

```sql
-- Simplest query (context must provide measure name)
SELECT *;

-- Select a specific tag and a specific field
SELECT
    instance,
    latency
WHERE region = 'us-west-1'
LIMIT 10;

-- Select multiple tags and fields, with an aggregation
SELECT
    region,
    SUM(latency)
FROM MEASURE service_cpm
GROUP BY region;

-- Disambiguate a key named 'status' that exists as both a tag and a field
SELECT
    status::tag,
    status::field
FROM MEASURE http_requests
WHERE path = '/api/v1/users';

-- Find the top 10 instances with the highest CPU usage for a specific service
SELECT TOP 10
    instance,
    cpu_usage
FROM MEASURE instance_metrics
WHERE service = 'auth-service'
ORDER BY cpu_usage DESC;

-- Select aggregated latency from multiple groups
SELECT
    region,
    SUM(latency)
FROM MEASURE service_cpm IN (us-west, us-east, eu-central)
GROUP BY region;

-- Query measures with time range using TIME clause
SELECT
    service,
    AVG(response_time)
FROM MEASURE http_metrics
TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'
WHERE region = 'us-west'
GROUP BY service;

-- Query measures after a specific time
SELECT
    instance,
    MAX(cpu_usage)
FROM MEASURE system_metrics
TIME > '2023-01-01T08:00:00Z'
WHERE datacenter = 'dc-1'
GROUP BY instance
ORDER BY MAX(cpu_usage) DESC;

-- Query measures at exact timestamp
SELECT
    endpoint,
    SUM(request_count)
FROM MEASURE api_metrics
TIME = '2023-01-01T10:00:00Z'
WHERE method = 'POST'
GROUP BY endpoint;

-- Query measures with relative time - last 30 minutes
SELECT
    service,
    AVG(response_time)
FROM MEASURE http_metrics
TIME > '-30m'
WHERE region = 'us-west'
GROUP BY service;

-- Query measures with relative time range - last 2 hours
SELECT
    instance,
    MAX(cpu_usage)
FROM MEASURE system_metrics
TIME BETWEEN '-2h' AND 'now'
WHERE datacenter = 'dc-1'
GROUP BY instance
ORDER BY MAX(cpu_usage) DESC;

-- Query data older than 1 day ago
SELECT
    endpoint,
    SUM(error_count)
FROM MEASURE api_errors
TIME < '-1d'
WHERE status_code = '500'
GROUP BY endpoint;
```

## 6. BydbQL for Top-N

Top-N queries use a specialized, command-like syntax for clarity and to reflect the optimized nature of the underlying `banyandb.measure.v1.TopNRequest`. The `FROM` clause is mandatory.

### 6.1. Grammar

```
topn_query         ::= SHOW TOP integer from_measure_clause [TIME time_condition] [WHERE topn_criteria] [AGGREGATE BY agg_function] [ORDER BY value ["ASC"|"DESC"]]
from_measure_clause ::= "FROM MEASURE" identifier ["IN" "(" group_list ")"]
topn_criteria      ::= condition (("AND" | "OR") condition)*
condition          ::= identifier binary_op (value | value_list)
time_condition     ::= "=" timestamp | ">" timestamp | "<" timestamp | ">=" timestamp | "<=" timestamp | "BETWEEN" timestamp "AND" timestamp
binary_op          ::= "=" | "!=" | ">" | "<" | ">=" | "<=" | "IN" | "NOT IN"
agg_function       ::= "SUM" | "MEAN" | "COUNT" | "MAX" | "MIN"
group_list         ::= identifier ("," identifier)*
value              ::= string_literal | integer_literal | "NULL"
value_list         ::= "(" value ("," value)* ")"
timestamp          ::= string_literal | integer_literal
	/* timestamp supports both absolute and relative time formats:
	   - Absolute: RFC3339 format like "2006-01-02T15:04:05Z07:00"
	   - Relative: duration strings like "-30m", "2h", "1d" (relative to current time) */
identifier         ::= [a-zA-Z_][a-zA-Z0-9_]*
string_literal     ::= "'" [^']* "'" | "\"" [^\"]* "\""
integer_literal    ::= [0-9]+
```

### 6.2. Mapping to `measure.v1.TopNRequest`

*   **`SHOW TOP N`**: Maps to `top_n`.
*   **`FROM MEASURE name IN (groups)`**: Maps to the `name` and `groups` fields.
*   **`TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'`**: Maps to `time_range` with `begin` and `end` set to the respective timestamps.
*   **`TIME > '-30m'`**: Maps to `time_range` with `begin` set to 30 minutes ago.
*   **`TIME BETWEEN '-1h' AND 'now'`**: Maps to `time_range` from 1 hour ago to current time.
*   **`WHERE tag = 'value'`**: Maps to `conditions`. Only simple equality is supported.
*   **`AGGREGATE BY FUNC`**: Maps to `agg`.
*   **`ORDER BY value DESC`**: Maps to `field_value_sort`.

### 6.3. Examples

```sql
-- Get the Top 10 services with the highest latency
SHOW TOP 10
FROM MEASURE service_latency
WHERE http_method = 'GET' AND version = 'v1.2.0'
ORDER BY value DESC;

-- Get the Bottom 5 services with the fewest errors
SHOW TOP 5 
FROM MEASURE service_errors_total
ORDER BY value ASC;

-- Get the Top 3 pods with the most restarts in total over the time range
SHOW TOP 3
FROM MEASURE pod_restarts
WHERE namespace = 'production'
AGGREGATE BY SUM;

-- Get the Top 5 services with the highest error rate across multiple groups
SHOW TOP 5
FROM MEASURE service_errors IN (production, staging)
ORDER BY value DESC;

-- Get the Top 10 services with highest latency in the last hour
SHOW TOP 10
FROM MEASURE service_latency
TIME > '2023-01-01T13:00:00Z'
ORDER BY value DESC;

-- Get the Top 5 endpoints with most errors in a specific time range
SHOW TOP 5
FROM MEASURE endpoint_errors
TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'
WHERE status_code = '500'
ORDER BY value DESC;

-- Get the Top 10 services with highest latency in the last hour
SHOW TOP 10
FROM MEASURE service_latency
TIME > '-1h'
ORDER BY value DESC;

-- Get the Top 5 endpoints with most errors in the last 24 hours
SHOW TOP 5
FROM MEASURE endpoint_errors
TIME BETWEEN '-24h' AND 'now'
WHERE status_code = '500'
ORDER BY value DESC;

-- Get the Top 3 services with lowest response time in the last 30 minutes
SHOW TOP 3
FROM MEASURE service_response_time
TIME > '-30m'
ORDER BY value ASC;
```

## 7. BydbQL for Properties

BydbQL for properties is designed for simple key-value lookups and metadata filtering. It maps to the `banyandb.property.v1.QueryRequest` message.

### 7.1. Grammar

```
property_query      ::= SELECT projection [from_property_clause] [WHERE criteria] [LIMIT integer]
from_property_clause ::= "FROM PROPERTY" identifier ["IN" "(" group_list ")"]
projection          ::= "*" | column_list
column_list         ::= identifier ("," identifier)*
group_list          ::= identifier ("," identifier)*
criteria            ::= condition (("AND" | "OR") condition)*
condition           ::= identifier binary_op (value | value_list) | "ID" binary_op (value | value_list)
binary_op           ::= "=" | "!=" | ">" | "<" | ">=" | "<=" | "IN" | "NOT IN"
value               ::= string_literal | integer_literal | "NULL"
value_list          ::= "(" value ("," value)* ")"
identifier          ::= [a-zA-Z_][a-zA-Z0-9_]*
string_literal      ::= "'" [^']* "'" | "\"" [^\"]* "\""
integer_literal     ::= [0-9]+
```

### 7.2. Mapping to `property.v1.QueryRequest`

*   **`FROM PROPERTY name IN (groups)`**: Maps to the `name` and `groups` fields. If the clause is omitted, these values are taken from the execution context.
*   **`SELECT tags`**: Maps to `tag_projection`.
*   **`WHERE ID IN (...)`**: Maps to `ids`.
*   **`WHERE tag = 'value'`**: Maps to `criteria`.
*   **`LIMIT n`**: Maps to `limit`.

### 7.3. Examples

```sql
-- Simplest query (context must provide property name)
SELECT *;

-- Find properties by filtering on their tags
SELECT ip, owner
FROM PROPERTY server_metadata
WHERE datacenter = 'dc-101' AND in_service = 'true'
LIMIT 50;

-- Retrieve a specific property by its unique ID
SELECT * 
WHERE ID = 'server-1a2b3c';

-- Retrieve a set of properties by their unique IDs
SELECT ip, region
FROM PROPERTY server_metadata
WHERE ID IN ('server-1a2b3c', 'server-4d5e6f');

-- Find properties from multiple groups
SELECT ip, owner
FROM PROPERTY server_metadata IN (datacenter-1, datacenter-2, datacenter-3)
WHERE in_service = 'true'
LIMIT 100;
```

## 8. Summary of BydbQL Capabilities

| Feature             | Streams                               | Measures                              | Top-N                               | Properties                          |
| :------------------ | :------------------------------------ | :------------------------------------ | :---------------------------------- | :---------------------------------- |
| **Primary Command** | `SELECT ... [FROM STREAM]`            | `SELECT ... [FROM MEASURE]`           | `SHOW TOPN ... FROM MEASURE`        | `SELECT ... [FROM PROPERTY]`        |
| **Projection**      | Tags by family                        | Tags & Fields                         | Implicit (entity, value)            | Flat list of tags                   |
| **Aggregation**     | No                                    | Yes (`SUM`, `MEAN`, etc.)             | Yes (optional)                      | No                                  |
| **Grouping**        | No                                    | Yes (`GROUP BY`)                      | No                                  | No                                  |
| **Filtering**       | Full `WHERE` clause + `TIME` clause   | Full `WHERE` clause + `TIME` clause   | Simple equality `WHERE` + `TIME` clause | `WHERE` by ID or tags               |
| **Ordering**        | Yes (`ORDER BY`)                      | Yes (`ORDER BY`)                      | Yes (`ORDER BY value`)              | No                                  |
| **Pagination**      | Yes (`LIMIT`/`OFFSET`)                | Yes (`LIMIT`/`OFFSET`)                | No                                  | `LIMIT` only                        |
