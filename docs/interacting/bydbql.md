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
*   **Traces**: For distributed tracing data with spans.

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
```

*   **Lexer**: Breaks the query string into a sequence of tokens.
*   **Parser**: Builds an Abstract Syntax Tree (AST) from the tokens, validating the query's syntax.
*   **Transformer**: Traverses the AST, performs semantic analysis using a schema, and transforms the AST into the appropriate target protobuf `Stream/Measure/Property/Traces/TopN Request` message.

### 2.2. Distinguishing Query Types

BydbQL distinguishes the target data model explicitly through keywords in the `FROM` clause. This allows the parser to apply the correct grammar and transformation rules for the query.

*   **Streams**: `FROM STREAM <name> IN <groups>` or `FROM STREAM <name> IN (<groups>)`
*   **Measures**: `FROM MEASURE <name> IN <groups>` or `FROM MEASURE <name> IN (<groups>)`
*   **Properties**: `FROM PROPERTY <name> IN <groups>` or `FROM PROPERTY <name> IN (<groups>)`
*   **Traces**: `FROM TRACE <name> IN <groups>` or `FROM TRACE <name> IN (<groups>)`

Specialized queries, like Top-N, use a distinct top-level command:

*   **Top-N**: `SHOW TOP <n> FROM MEASURE <name> IN <groups>` or `SHOW TOP <n> FROM MEASURE <name> IN (<groups>)`

### 2.3. Required Clauses

In BydbQL, the following clauses are **required** for all queries:

*   **`FROM` clause**: Specifies the data model type, resource name, and group list
*   **`IN groups` clause**: Specifies one or more groups to query from. Parentheses around the group list are optional.
*   **`TIME` clause**: Specifies the time range for the query (required for Streams, Measures, Traces, and Top-N queries; not applicable to Property queries)

### 2.4. Case Sensitivity

BydbQL follows SQL-like conventions for case sensitivity:

*   **Reserved words are case-insensitive**: Keywords like `SELECT`, `FROM`, `WHERE`, `ORDER BY`, `TIME`, `BETWEEN`, `AND`, etc. can be written in any case combination.
*   **Identifiers are case-sensitive**: Names of streams, measures, properties, tags, and fields preserve their case and must be referenced exactly as defined.

#### Examples

All of these queries are equivalent:
```sql
SELECT * FROM STREAM sw in group1 WHERE service_id = 'webapp';

select * from stream sw in group1 where service_id = 'webapp';

Select * From Stream sw in group1 Where service_id = 'webapp';
```

But these refer to different identifiers:
```sql
-- Different tag names (case-sensitive)
SELECT ServiceName FROM STREAM sw in group1;  -- refers to tag "ServiceName"
SELECT servicename FROM STREAM sw in group1;  -- refers to tag "servicename"

-- Different stream names (case-sensitive)
FROM STREAM MyStream in group1   -- refers to stream "MyStream"
FROM STREAM mystream in group1   -- refers to stream "mystream"
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

## 3. WHERE Statement

The WHERE clause in BydbQL provides powerful filtering capabilities with support for various operators, logical expressions, and full-text search. It supports binary tree structures with proper operator precedence and parentheses grouping.

### Key Features

*   **Binary Tree Structure**: WHERE conditions are organized as a binary expression tree supporting complex nested logic
*   **Operator Precedence**: Parentheses `()` > `AND` > `OR`
*   **Multiple Operators**: Comparison (`=`, `!=`, `>`, `<`, `>=`, `<=`), set operations (`IN`, `NOT IN`, `HAVING`, `NOT HAVING`), and full-text search (`MATCH`)
*   **Type Support**: String, integer, and NULL values
*   **Complex Expressions**: Support for nested parentheses and mixed AND/OR logic

### 3.1. MATCH Operator

The `MATCH` operator provides full-text search capabilities for string fields and tags in BydbQL. It uses text analyzers to tokenize and match text content, making it ideal for searching log messages, trace attributes, and other textual data.

#### 3.1.1. Syntax

The MATCH operator uses a function-call syntax with optional parameters:

```
MATCH(value)
MATCH(value, analyzer)
MATCH(value, analyzer, operator)
MATCH((value1, value2, ...), analyzer, operator)
```

**Parameters:**

*   **value(s)** (required): The search term(s) to match. Can be:
    *   Single value: `MATCH('error')`
    *   Multiple values (array): `MATCH(('error', 'warning'))` - wrapped in parentheses
    *   Supports both string and integer types

*   **analyzer** (optional): The text analyzer to use for tokenizing and matching. Common analyzers include:
    *   `"standard"` - Standard text analysis with lowercase and word tokenization
    *   `"simple"` - Simple lowercase analysis
    *   `"keyword"` - No tokenization, exact matching (case-insensitive)
    *   `"url"` - URL-specific tokenization
    *   Default: Uses the analyzer configured for the field/tag in the schema

*   **operator** (optional): Logical operator for multiple values. Valid values:
    *   `"AND"` - All values must match (default for multiple values)
    *   `"OR"` - At least one value must match

#### 3.1.2. Supported Data Types

The MATCH operator is available in:

*   **Streams**: For full-text search in stream tags
*   **Measures**: For full-text search in measure tags
*   **Traces**: For full-text search in trace tags (e.g., searching span attributes)

Note: MATCH is not supported in Top-N and Property queries.

#### 3.1.3. Examples

#### Basic MATCH queries

```sql
-- Simple text search
SELECT trace_id, message
FROM STREAM logs in group1
TIME > '-30m'
WHERE message MATCH('error');

-- Search with specific analyzer
SELECT trace_id, http_url
FROM TRACE sw_trace in group1
TIME > '-30m'
WHERE http_url MATCH('/api/users', 'url');

-- Search with analyzer and operator
SELECT service_id, log_message
FROM STREAM application_logs in group1
TIME > '-30m'
WHERE log_message MATCH('error', 'standard', 'OR');
```

#### Multiple value searches

```sql
-- Search for multiple terms (default AND logic)
SELECT trace_id, operation_name
FROM TRACE sw_trace in group1
TIME > '-30m'
WHERE operation_name MATCH(('GET', 'POST'), 'keyword');

-- Search for any of multiple terms (OR logic)
SELECT service_id, message
FROM STREAM logs in group1
TIME > '-30m'
WHERE message MATCH(('error', 'warning', 'critical'), 'standard', 'OR');

-- Multiple values with explicit AND
SELECT trace_id, tags
FROM TRACE sw_trace in group1
TIME > '-30m'
WHERE tags MATCH(('payment', 'success'), 'standard', 'AND');
```

#### Combined with other conditions

```sql
-- MATCH with other WHERE conditions
SELECT trace_id, service_id, message
FROM STREAM logs in group1
TIME > '-30m'
WHERE service_id = 'payment-service'
  AND message MATCH(('error', 'timeout'), 'standard', 'OR')
ORDER BY timestamp DESC
LIMIT 100;

-- Complex query with MATCH and time range
SELECT trace_id, operation_name, http_status
FROM TRACE sw_trace in group1
TIME > '-30m'
TIME BETWEEN '-1h' AND 'now'
WHERE service_id IN ('api-gateway', 'auth-service')
  AND http_status > 400
  AND operation_name MATCH('api', 'keyword')
ORDER BY start_time DESC;
```

#### 3.1.4. Performance Considerations

*   **Indexing**: MATCH queries leverage full-text indexes. Ensure your tags/fields are properly indexed for optimal performance.
*   **Analyzer Selection**: Choose the appropriate analyzer for your use case:
    *   Use `"keyword"` for exact matching
    *   Use `"standard"` for general text search
    *   Use `"url"` for URL-specific patterns
*   **Multiple Values**: When using multiple values, `"OR"` logic may scan more results than `"AND"` logic.
*   **Combination with Filters**: Combine MATCH with other WHERE conditions to narrow down results and improve query performance.

#### 3.1.5. Notes

*   The MATCH operator is case-insensitive by default (behavior depends on the analyzer).
*   Multiple values must be wrapped in parentheses: `MATCH(('val1', 'val2'))`.
*   The analyzer and operator parameters are optional; when omitted, schema defaults are used.
*   For single-value searches, the operator parameter is ignored.

## 4. BydbQL for Streams

BydbQL for streams is designed for querying and retrieving raw time-series elements. The syntax maps to the `banyandb.stream.v1.QueryRequest` message.

### 4.1. Grammar

```
query           ::= SELECT projection from_stream_clause TIME time_condition [WHERE criteria] [ORDER BY order_expression] [LIMIT integer] [OFFSET integer] [WITH QUERY_TRACE]
from_stream_clause ::= "FROM STREAM" identifier "IN" ["("] group_list [")"]
projection      ::= "*" | column_list
column_list     ::= identifier ("," identifier)*
group_list      ::= identifier ("," identifier)+
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

**Note**: The `TIME` clause and `IN groups` clause are **required** for all stream queries. At least one group must be specified. Parentheses around the group list are optional.

### 4.2. Mapping to `stream.v1.QueryRequest`

*   **`FROM STREAM name IN groups`** or **`FROM STREAM name IN (groups)`**: Maps to the `name` and `groups` fields. Both are required.
*   **`SELECT tags`**: Maps to `projection`. Requires a stream schema to resolve tags to their families.
*   **`TIME` clause (required)**: Maps to `time_range`:
    *   **`TIME = '2023-01-01T00:00:00Z'`**: Sets `begin` and `end` to the same timestamp.
    *   **`TIME > '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp.
    *   **`TIME < '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp.
    *   **`TIME >= '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp (inclusive).
    *   **`TIME <= '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp (inclusive).
    *   **`TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'`**: Sets `begin` and `end` to the respective timestamps.
    *   **`TIME > '-30m'`**: Sets `begin` to 30 minutes ago.
    *   **`TIME BETWEEN '-1h' AND 'now'`**: Sets `begin` to 1 hour ago and `end` to current time.
*   **`WHERE conditions`**: Maps to `criteria`.
*   **`ORDER BY field`**: Maps to `order_by`.
*   **`LIMIT`/`OFFSET`**: Maps to `limit` and `offset`.
*   **`WITH QUERY_TRACE`**: Maps to the `trace` field to enable distributed tracing of query execution.

### 4.3. Examples

```sql
-- Basic selection with filtering and ordering
SELECT trace_id, service_id
FROM STREAM sw IN group1, group2
TIME > '-30m'
WHERE service_id = 'webapp' AND state = 1
ORDER BY start_time DESC
LIMIT 100;

-- Project all tags from a stream
SELECT *
FROM STREAM sw IN group1, group2
TIME > '-30m'
WHERE state = 0
LIMIT 10;

-- Use more complex conditions with IN and OR
SELECT trace_id, duration
FROM STREAM sw IN group1, group2
TIME > '-30m'
WHERE service_id IN ('webapp', 'api-gateway') OR http.method = 'POST';

-- Query with time range using TIME clause
SELECT trace_id, service_id, start_time
FROM STREAM sw IN group1, group2
TIME > '-30m'
TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'
WHERE service_id = 'webapp'
ORDER BY start_time DESC;

-- Query data after a specific timestamp
SELECT *
FROM STREAM sw IN group1, group2
TIME > '2023-01-01T12:00:00Z'
WHERE state = 1
LIMIT 100;

-- Query data at an exact timestamp
SELECT trace_id, duration
FROM STREAM sw IN group1, group2
TIME = '2023-01-01T15:30:00Z';

-- Query with relative time - last 30 minutes
SELECT *
FROM STREAM sw IN group1, group2
TIME > '-30m'
WHERE state = 1
LIMIT 100;

-- Query with relative time range - last 2 hours
SELECT trace_id, service_id, start_time
FROM STREAM sw IN group1, group2
TIME BETWEEN '-2h' AND 'now'
WHERE service_id = 'webapp'
ORDER BY start_time DESC;

-- Query data within the last hour using absolute time
SELECT *
FROM STREAM sw IN group1, group2
TIME >= '2023-01-01T13:00:00Z'
WHERE status = 'error';

-- Query data older than 1 day ago
SELECT trace_id, duration
FROM STREAM sw IN group1, group2
TIME < '-1d';

-- Query with distributed tracing enabled
SELECT trace_id, service_id, start_time
FROM STREAM sw IN group1, group2
TIME BETWEEN '-2h' AND 'now'
WHERE service_id = 'webapp'
WITH QUERY_TRACE;
```

## 5. BydbQL for Measures

BydbQL for measures is tailored for analytical queries on aggregated numerical data. It supports aggregation, grouping, and mixed selection of tags and fields, mapping to the `banyandb.measure.v1.QueryRequest` message.

### 5.1. Grammar

```
measure_query     ::= SELECT projection from_measure_clause TIME time_condition [WHERE criteria] [GROUP BY column_list] [ORDER BY order_expression] [LIMIT integer] [OFFSET integer] [WITH QUERY_TRACE]
from_measure_clause ::= "FROM MEASURE" identifier "IN" ["("] group_list [")"]
projection        ::= "*" | (column_list | agg_function "(" identifier ")" | top_clause)
top_clause        ::= "TOP" integer identifier ["ASC" | "DESC"] ["," column_list]
column_list       ::= identifier ("," identifier)* ["::tag" | "::field"]
agg_function      ::= "SUM" | "MEAN" | "COUNT" | "MAX" | "MIN"
group_list        ::= identifier ("," identifier)+
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

**Note**: The `TIME` clause and `IN groups` clause are **required** for all measure queries. At least one group must be specified. Parentheses around the group list are optional.

### 5.2. BydbQL Extensions for `SELECT`

The `SELECT` clause for measures is highly flexible, allowing for the selection of tags, fields, and aggregations in a single, flat list.

*   `SELECT <field_key>, <tag_key>`: Returns specific fields and tags. The parser will infer the type of each identifier from the measure's schema.
*   `SELECT <identifier>::field, <identifier>::tag`: If a field and a tag share the same name, the `::field` or `::tag` syntax **must** be used to disambiguate the identifier's type.
*   The clause also supports aggregation functions (`SUM`, `MEAN`, `COUNT`, `MAX`, `MIN`) and a `TOP N` clause for ranked results.

### 5.3. Mapping to `measure.v1.QueryRequest`

*   **`FROM MEASURE name IN groups`** or **`FROM MEASURE name IN (groups)`**: Maps to the `name` and `groups` fields. Both are required.
*   **`SELECT <tag1>, <field1>, <field2>`**: The transformer inspects each identifier. Those identified as tags (either by schema lookup or `::tag`) are added to `tag_projection`. Those identified as fields (by schema lookup or `::field`) are added to `field_projection`.
*   **`SELECT SUM(field)`**: Maps to `agg`.
*   **`TIME` clause (required)**: Maps to `time_range`:
    *   **`TIME = '2023-01-01T00:00:00Z'`**: Sets `begin` and `end` to the same timestamp.
    *   **`TIME > '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp.
    *   **`TIME < '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp.
    *   **`TIME >= '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp (inclusive).
    *   **`TIME <= '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp (inclusive).
    *   **`TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'`**: Sets `begin` and `end` to the respective timestamps.
    *   **`TIME > '-30m'`**: Sets `begin` to 30 minutes ago.
    *   **`TIME BETWEEN '-1h' AND 'now'`**: Sets `begin` to 1 hour ago and `end` to current time.
*   **`GROUP BY <tag1>, <tag2>`**: The `GROUP BY` clause takes a simple list of tags and maps to `group_by.tag_projection`.
    *   **Note**: When the query contains an aggregate function (e.g., `SUM`, `AVG`, `COUNT`, `MAX`, `MIN`) with `GROUP BY`, the `GROUP BY` clause **must include at least one field**. This ensures proper aggregation behavior in measure queries.
*   **`SELECT TOP N ...`**: Maps to the `top` message.
*   **`WITH QUERY_TRACE`**: Maps to the `trace` field to enable distributed tracing of query execution.

### 5.4. Examples

```sql
-- Select a specific tag and a specific field
SELECT
    instance,
    latency
FROM MEASURE service_cpm IN us-west
TIME > '-30m'
WHERE region = 'us-west-1'
LIMIT 10;

-- Select multiple tags and fields, with an aggregation
SELECT
    region,
    SUM(latency)
FROM MEASURE service_cpm IN us-west
TIME > '-30m'
GROUP BY region;

-- Disambiguate a key named 'status' that exists as both a tag and a field
SELECT
    status::tag,
    status::field
FROM MEASURE http_requests IN us-west
TIME > '-30m'
WHERE path = '/api/v1/users';

-- Find the top 10 instances with the highest CPU usage for a specific service
SELECT TOP 10 instance ASC,
    cpu_usage
FROM MEASURE instance_metrics IN us-west
TIME > '-30m'
WHERE service = 'auth-service'
ORDER BY cpu_usage DESC;

-- Select aggregated latency from multiple groups
SELECT
    region,
    SUM(latency)
FROM MEASURE service_cpm IN us-west, us-east, eu-central
TIME > '-30m'
GROUP BY region;

-- Query measures with time range using TIME clause
SELECT
    service,
    AVG(response_time)
FROM MEASURE http_metrics IN us-west
TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'
WHERE region = 'us-west'
GROUP BY service;

-- Query measures after a specific time
SELECT
    instance,
    MAX(cpu_usage)
FROM MEASURE system_metrics IN us-west
TIME > '2023-01-01T08:00:00Z'
WHERE datacenter = 'dc-1'
GROUP BY instance
ORDER BY MAX(cpu_usage) DESC;

-- Query measures at exact timestamp
SELECT
    endpoint,
    SUM(request_count)
FROM MEASURE api_metrics IN us-west
TIME = '2023-01-01T10:00:00Z'
WHERE method = 'POST'
GROUP BY endpoint;

-- Query measures with relative time - last 30 minutes
SELECT
    service,
    AVG(response_time)
FROM MEASURE http_metrics IN us-west
TIME > '-30m'
WHERE region = 'us-west'
GROUP BY service;

-- Query measures with relative time range - last 2 hours
SELECT
    instance,
    MAX(cpu_usage)
FROM MEASURE system_metrics IN us-west
TIME BETWEEN '-2h' AND 'now'
WHERE datacenter = 'dc-1'
GROUP BY instance
ORDER BY MAX(cpu_usage) DESC;

-- Query data older than 1 day ago
SELECT
    endpoint,
    SUM(error_count)
FROM MEASURE api_errors IN us-west
TIME < '-1d'
WHERE status_code = '500'
GROUP BY endpoint;

-- Query with distributed tracing enabled
SELECT
    service,
    AVG(response_time)
FROM MEASURE http_metrics IN us-west
TIME > '-30m'
WHERE region = 'us-west'
GROUP BY service
WITH QUERY_TRACE;
```

## 6. BydbQL for Top-N

Top-N queries use a specialized, command-like syntax for clarity and to reflect the optimized nature of the underlying `banyandb.measure.v1.TopNRequest`. The `FROM` clause is mandatory.

### 6.1. Grammar

```
topn_query         ::= SHOW TOP integer from_measure_clause TIME time_condition [WHERE topn_criteria] [AGGREGATE BY agg_function] [ORDER BY value ["ASC"|"DESC"]] [WITH QUERY_TRACE]
from_measure_clause ::= "FROM MEASURE" identifier "IN" ["("] group_list [")"]
topn_criteria      ::= condition (("AND" | "OR") condition)*
condition          ::= identifier binary_op (value | value_list)
time_condition     ::= "=" timestamp | ">" timestamp | "<" timestamp | ">=" timestamp | "<=" timestamp | "BETWEEN" timestamp "AND" timestamp
binary_op          ::= "=" | "!=" | ">" | "<" | ">=" | "<=" | "IN" | "NOT IN"
agg_function       ::= "SUM" | "MEAN" | "COUNT" | "MAX" | "MIN"
group_list         ::= identifier ("," identifier)+
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

**Note**: The `TIME` clause and `IN groups` clause are **required** for all Top-N queries. At least one group must be specified. Parentheses around the group list are optional.

### 6.2. Mapping to `measure.v1.TopNRequest`

*   **`SHOW TOP N`**: Maps to `top_n`.
*   **`FROM MEASURE name IN groups`** or **`FROM MEASURE name IN (groups)`**: Maps to the `name` and `groups` fields. Both are required.
*   **`TIME` clause (required)**: Maps to `time_range`:
    *   **`TIME = '2023-01-01T00:00:00Z'`**: Sets `begin` and `end` to the same timestamp.
    *   **`TIME > '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp.
    *   **`TIME < '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp.
    *   **`TIME >= '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp (inclusive).
    *   **`TIME <= '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp (inclusive).
    *   **`TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'`**: Sets `begin` and `end` to the respective timestamps.
    *   **`TIME > '-30m'`**: Sets `begin` to 30 minutes ago.
    *   **`TIME BETWEEN '-1h' AND 'now'`**: Sets `begin` to 1 hour ago and `end` to current time.
*   **`WHERE tag = 'value'`**: Maps to `conditions`. Only simple equality is supported.
*   **`AGGREGATE BY FUNC`**: Maps to `agg`.
*   **`ORDER BY value DESC`**: Maps to `field_value_sort`.
*   **`WITH QUERY_TRACE`**: Maps to the `trace` field to enable distributed tracing of query execution.

### 6.3. Examples

```sql
-- Get the Top 10 services with the highest latency
SHOW TOP 10
FROM MEASURE service_latency IN production
TIME > '-30m'
WHERE http_method = 'GET' AND version = 'v1.2.0'
ORDER BY value DESC;

-- Get the Bottom 5 services with the fewest errors
SHOW TOP 5
FROM MEASURE service_errors_total IN production
TIME > '-30m'
ORDER BY value ASC;

-- Get the Top 3 pods with the most restarts in total over the time range
SHOW TOP 3
FROM MEASURE pod_restarts IN production
TIME > '-30m'
WHERE namespace = 'production'
AGGREGATE BY SUM;

-- Get the Top 5 services with the highest error rate across multiple groups
SHOW TOP 5
FROM MEASURE service_errors IN production, staging
TIME > '-30m'
ORDER BY value DESC;

-- Get the Top 10 services with highest latency in the last hour
SHOW TOP 10
FROM MEASURE service_latency IN production
TIME > '2023-01-01T13:00:00Z'
ORDER BY value DESC;

-- Get the Top 5 endpoints with most errors in a specific time range
SHOW TOP 5
FROM MEASURE endpoint_errors IN production
TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'
WHERE status_code = '500'
ORDER BY value DESC;

-- Get the Top 10 services with highest latency in the last hour
SHOW TOP 10
FROM MEASURE service_latency IN production
TIME > '-1h'
ORDER BY value DESC;

-- Get the Top 5 endpoints with most errors in the last 24 hours
SHOW TOP 5
FROM MEASURE endpoint_errors IN production
TIME BETWEEN '-24h' AND 'now'
WHERE status_code = '500'
ORDER BY value DESC;

-- Get the Top 3 services with lowest response time in the last 30 minutes
SHOW TOP 3
FROM MEASURE service_response_time IN production
TIME > '-30m'
ORDER BY value ASC;

-- Top-N query with distributed tracing enabled
SHOW TOP 10
FROM MEASURE service_latency IN production
TIME > '-1h'
WHERE http_method = 'GET'
ORDER BY value DESC
WITH QUERY_TRACE;
```

## 7. BydbQL for Properties

BydbQL for properties is designed for simple key-value lookups and metadata filtering. It maps to the `banyandb.property.v1.QueryRequest` message.

### 7.1. Grammar

```
property_query      ::= SELECT projection from_property_clause [WHERE criteria] [LIMIT integer] [WITH QUERY_TRACE]
from_property_clause ::= "FROM PROPERTY" identifier "IN" ["("] group_list [")"]
projection          ::= "*" | column_list
column_list         ::= identifier ("," identifier)*
group_list          ::= identifier ("," identifier)+
criteria            ::= condition (("AND" | "OR") condition)*
condition           ::= identifier binary_op (value | value_list) | "ID" binary_op (value | value_list)
binary_op           ::= "=" | "!=" | ">" | "<" | ">=" | "<=" | "IN" | "NOT IN"
value               ::= string_literal | integer_literal | "NULL"
value_list          ::= "(" value ("," value)* ")"
identifier          ::= [a-zA-Z_][a-zA-Z0-9_]*
string_literal      ::= "'" [^']* "'" | "\"" [^\"]* "\""
integer_literal     ::= [0-9]+
```

**Note**: The `IN groups` clause is **required** for all property queries. At least one group must be specified. Parentheses around the group list are optional. Property queries do **not** require a `TIME` clause.

### 7.2. Mapping to `property.v1.QueryRequest`

*   **`FROM PROPERTY name IN groups`** or **`FROM PROPERTY name IN (groups)`**: Maps to the `name` and `groups` fields. Both are required.
*   **`SELECT tags`**: Maps to `tag_projection`.
*   **`WHERE ID IN (...)`**: Maps to `ids`.
*   **`WHERE tag = 'value'`**: Maps to `criteria`.
*   **`LIMIT n`**: Maps to `limit`.
*   **`WITH QUERY_TRACE`**: Maps to the `trace` field to enable distributed tracing of query execution.

### 7.3. Examples

```sql
-- Find properties by filtering on their tags
SELECT ip, owner
FROM PROPERTY server_metadata IN datacenter-1
WHERE datacenter = 'dc-101' AND in_service = 'true'
LIMIT 50;

-- Retrieve a specific property by its unique ID
SELECT *
FROM PROPERTY server_metadata IN datacenter-1
WHERE ID = 'server-1a2b3c';

-- Retrieve a set of properties by their unique IDs
SELECT ip, region
FROM PROPERTY server_metadata IN datacenter-1
WHERE ID IN ('server-1a2b3c', 'server-4d5e6f');

-- Find properties from multiple groups
SELECT ip, owner
FROM PROPERTY server_metadata IN datacenter-1, datacenter-2, datacenter-3
WHERE in_service = 'true'
LIMIT 100;

-- Property query with distributed tracing enabled
SELECT ip, region, owner
FROM PROPERTY server_metadata IN datacenter-1
WHERE datacenter = 'dc-101' AND in_service = 'true'
LIMIT 50
WITH QUERY_TRACE;
```

## 8. BydbQL for Traces

BydbQL for traces is designed for querying and retrieving trace data with spans. The syntax maps to the `banyandb.trace.v1.QueryRequest` message and is optimized for trace-specific operations.

### 8.1. Grammar

```
trace_query           ::= SELECT projection from_trace_clause TIME time_condition [WHERE criteria] [ORDER BY order_expression] [LIMIT integer] [OFFSET integer] [WITH QUERY_TRACE]
from_trace_clause     ::= "FROM TRACE" identifier "IN" ["("] group_list [")"]
projection            ::= "*" | column_list | "()"
column_list           ::= identifier ("," identifier)*
group_list            ::= identifier ("," identifier)+
criteria              ::= condition (("AND" | "OR") condition)*
condition             ::= identifier binary_op (value | value_list)
time_condition        ::= "=" timestamp | ">" timestamp | "<" timestamp | ">=" timestamp | "<=" timestamp | "BETWEEN" timestamp "AND" timestamp
binary_op             ::= "=" | "!=" | ">" | "<" | ">=" | "<=" | "IN" | "NOT IN" | "HAVING" | "NOT HAVING" | "MATCH"
order_expression      ::= identifier ["ASC" | "DESC"]
value                 ::= string_literal | integer_literal | "NULL"
value_list            ::= "(" value ("," value)* ")"
timestamp             ::= string_literal | integer_literal
	/* timestamp supports both absolute and relative time formats:
	   - Absolute: RFC3339 format like "2006-01-02T15:04:05Z07:00"
	   - Relative: duration strings like "-30m", "2h", "1d" (relative to current time) */
identifier            ::= [a-zA-Z_][a-zA-Z0-9_]*
string_literal        ::= "'" [^']* "'" | "\"" [^\"]* "\""
integer_literal       ::= [0-9]+
```

**Note**: The `TIME` clause and `IN groups` clause are **required** for all trace queries. At least one group must be specified. Parentheses around the group list are optional.

### 8.2. Trace Model Characteristics

The Trace model in BanyanDB is specifically designed for storing and querying trace data with the following key characteristics:

*   **Trace Resource**: A logical namespace within a group that defines the structure for trace data
*   **Tags**: Indexed tags that support filtering and querying (defined by `TraceTagSpec`)
*   **Trace ID Tag**: A specific tag that stores the trace ID for trace correlation
*   **Timestamp Tag**: A specific tag that stores the timestamp for time-based queries
*   **Span Data**: Raw span data stored as binary for efficient storage and retrieval

### 8.2.1. Empty Projection Support

BydbQL for traces supports an empty projection syntax `SELECT ()` that allows queries to return only raw span data without any tag information. This is useful for:

*   **Performance Optimization**: When you only need the raw span data and don't want to pay the cost of loading and returning tag values
*   **Data Processing**: When you plan to process the raw span data externally and don't need the indexed tag values
*   **Storage Efficiency**: Reducing network transfer and memory usage by excluding tag data

**Syntax**: `SELECT ()` - Returns only raw span data, no tag information
**Behavior**: The query will still apply filtering and ordering based on the WHERE and ORDER BY clauses, but the result will contain only the binary span data without any tag projections.

### 8.3. Mapping to `trace.v1.QueryRequest`

*   **`FROM TRACE name IN groups`** or **`FROM TRACE name IN (groups)`**: Maps to the `name` and `groups` fields. Both are required.
*   **`SELECT tags`**: Maps to `tag_projection`. Requires a trace schema to resolve tags to their specifications.
*   **`SELECT ()`**: Maps to an empty `tag_projection` array, indicating no tag data should be returned (only raw span data).
*   **`TIME` clause (required)**: Maps to `time_range`:
    *   **`TIME = '2023-01-01T00:00:00Z'`**: Sets `begin` and `end` to the same timestamp.
    *   **`TIME > '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp.
    *   **`TIME < '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp.
    *   **`TIME >= '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp (inclusive).
    *   **`TIME <= '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp (inclusive).
    *   **`TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'`**: Sets `begin` and `end` to the respective timestamps.
    *   **`TIME > '-30m'`**: Sets `begin` to 30 minutes ago.
    *   **`TIME BETWEEN '-1h' AND 'now'`**: Sets `begin` to 1 hour ago and `end` to current time.
*   **`WHERE conditions`**: Maps to `criteria` for filtering spans based on tag values.
*   **`ORDER BY field`**: Maps to `order_by` for sorting results.
*   **`LIMIT`/`OFFSET`**: Maps to `limit` and `offset` for pagination.
*   **`WITH QUERY_TRACE`**: Maps to the `trace` field to enable distributed tracing of query execution.

### 8.3.1. Naming Convention Clarification

To avoid confusion between different uses of the word "trace" in BanyanDB:

*   **Trace Model**: Refers to the data model for storing trace data (spans, tags, etc.) - used in `FROM TRACE` clauses
*   **Query Tracing**: Refers to distributed tracing of query execution for observability - enabled with `WITH QUERY_TRACE`

The `WITH QUERY_TRACE` clause enables distributed tracing of the query execution itself, which is separate from querying trace data. When enabled, the query response will include execution trace information in the `trace_query_result` field.

### 8.4. Examples

```sql
-- Basic selection with filtering and ordering
SELECT trace_id, service_id, operation_name
FROM TRACE sw_trace IN group1, group2
TIME > '-30m'
WHERE service_id = 'webapp' AND status = 'success'
ORDER BY start_time DESC
LIMIT 100;

-- Project all tags from a trace
SELECT *
FROM TRACE sw_trace IN group1
TIME > '-30m'
WHERE status = 'error'
LIMIT 10;

-- Query with no tag projection - returns only raw span data
SELECT ()
FROM TRACE sw_trace IN group1
TIME > '-30m'
WHERE service_id = 'webapp'
LIMIT 100;

-- Use more complex conditions with IN and OR
SELECT trace_id, duration, operation_name
FROM TRACE sw_trace IN group1
TIME > '-30m'
WHERE service_id IN ('webapp', 'api-gateway') OR http.method = 'POST';

-- Query with time range using TIME clause
SELECT trace_id, service_id, start_time, operation_name
FROM TRACE sw_trace IN group1
TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'
WHERE service_id = 'webapp'
ORDER BY start_time DESC;

-- Query data after a specific timestamp
SELECT *
FROM TRACE sw_trace IN group1
TIME > '2023-01-01T12:00:00Z'
WHERE status = 'success'
LIMIT 100;

-- Query data at an exact timestamp
SELECT trace_id, duration, operation_name
FROM TRACE sw_trace IN group1
TIME = '2023-01-01T15:30:00Z';

-- Query with relative time - last 30 minutes
SELECT *
FROM TRACE sw_trace IN group1
TIME > '-30m'
WHERE status = 'error'
LIMIT 100;

-- Query with relative time range - last 2 hours
SELECT trace_id, service_id, start_time, operation_name
FROM TRACE sw_trace IN group1
TIME BETWEEN '-2h' AND 'now'
WHERE service_id = 'webapp'
ORDER BY start_time DESC;

-- Query traces by specific trace ID
SELECT *
FROM TRACE sw_trace IN group1
TIME > '-30m'
WHERE trace_id = '1a2b3c4d5e6f7890';

-- Query traces with specific operation names
SELECT trace_id, service_id, duration
FROM TRACE sw_trace IN group1
TIME > '-30m'
WHERE operation_name IN ('GET /api/users', 'POST /api/orders')
ORDER BY duration DESC;

-- Query traces with error status in the last hour
SELECT trace_id, service_id, error_message
FROM TRACE sw_trace IN group1
TIME > '-1h'
WHERE status = 'error'
ORDER BY start_time DESC
LIMIT 50;

-- Query traces older than 1 day ago
SELECT trace_id, duration, operation_name
FROM TRACE sw_trace IN group1
TIME < '-1d'
WHERE service_id = 'legacy-service';

-- Query with no projection for raw span data only
SELECT ()
FROM TRACE sw_trace IN group1
TIME > '-1h'
WHERE status = 'error'
LIMIT 50;

-- Query with distributed tracing enabled for observability
SELECT trace_id, service_id, operation_name
FROM TRACE sw_trace IN group1
TIME > '-30m'
WHERE service_id = 'webapp'
WITH QUERY_TRACE;

-- Query with both empty projection and query tracing
SELECT ()
FROM TRACE sw_trace IN group1
TIME > '-30m'
WHERE status = 'error'
WITH QUERY_TRACE
LIMIT 100;
```

## 9. Summary of BydbQL Capabilities

| Feature             | Streams                                         | Measures                                        | Top-N                                           | Properties                                      | Traces                                          |
|:--------------------|:------------------------------------------------|:------------------------------------------------|:------------------------------------------------|:------------------------------------------------|:------------------------------------------------|
| **Primary Command** | `SELECT ... FROM STREAM ... IN ...`             | `SELECT ... FROM MEASURE ... IN ...`            | `SHOW TOP ... FROM MEASURE ... IN ...`          | `SELECT ... FROM PROPERTY ... IN ...`           | `SELECT ... FROM TRACE ... IN ...`              |
| **Groups Clause**   | **Required** `IN groups` (parentheses optional) | **Required** `IN groups` (parentheses optional) | **Required** `IN groups` (parentheses optional) | **Required** `IN groups` (parentheses optional) | **Required** `IN groups` (parentheses optional) |
| **Time Clause**     | **Required** `TIME ...`                         | **Required** `TIME ...`                         | **Required** `TIME ...`                         | Not applicable                                  | **Required** `TIME ...`                         |
| **Projection**      | Tags by family                                  | Tags & Fields                                   | Implicit (entity, value)                        | Flat list of tags                               | Tags by specification                           |
| **Aggregation**     | No                                              | Yes (`SUM`, `MEAN`, etc.)                       | Yes (optional)                                  | No                                              | No                                              |
| **Grouping**        | No                                              | Yes (`GROUP BY`)                                | No                                              | No                                              | No                                              |
| **Filtering**       | Full `WHERE` clause                             | Full `WHERE` clause                             | Simple equality `WHERE`                         | `WHERE` by ID or tags                           | Full `WHERE` clause                             |
| **Ordering**        | Yes (`ORDER BY`)                                | Yes (`ORDER BY`)                                | Yes (`ORDER BY value`)                          | No                                              | Yes (`ORDER BY`)                                |
| **Pagination**      | Yes (`LIMIT`/`OFFSET`)                          | Yes (`LIMIT`/`OFFSET`)                          | No                                              | `LIMIT` only                                    | Yes (`LIMIT`/`OFFSET`)                          |
