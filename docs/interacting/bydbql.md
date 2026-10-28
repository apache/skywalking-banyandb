# BanyanDB Query Language (BydbQL)

## 1. Introduction

### 1.1. Purpose

This document outlines the design of the BanyanDB Data Query Language (BydbQL), a unified query language with SQL-like syntax for all of BanyanDB's data models. The primary goal of BydbQL is to provide users with a familiar, intuitive, and powerful interface to interact with their data, abstracting the underlying protobuf-based APIs for streams, measures, traces, properties, and Top-N queries.

This document specifies the language's syntax, its semantics, and the precise mapping from BydbQL statements to the various `Request` messages.

### 1.2. Scope

BydbQL supports querying across BanyanDB's primary data models:

- **Streams**: For raw, time-series elements like logs and traces.
- **Measures**: For aggregated numerical time-series data (metrics).
- **Properties**: For metadata and key-value information.
- **Traces**: For distributed tracing data with spans.

It also provides a specialized syntax for optimized **Top-N** queries against measures.

## 2. Core Concepts

### 2.1. Parser Architecture

BydbQL queries pass through lexical analysis, parsing, parameter binding, and schema-aware planning.

```
BydbQL Query String
        ↓
      Lexer
        ↓
     Parser
        ↓
Abstract Syntax Tree (AST)
        ↓
 Parameter Binding
        ↓
   Transformer
```

- **Lexer**: Breaks the query string into a sequence of tokens.
- **Parser**: Builds an Abstract Syntax Tree (AST) from the tokens, validating the query's syntax.
- **Parameter Binding**: Binds `?` placeholders to the request's `params` (see section 2.6) without changing the query text; a query holding unbound placeholders is rejected.
- **Transformer**: Resolves columns against the schema and produces the appropriate native request. Extended measure queries also use a QL plan for projection, time filtering, local aggregation, and pagination before encoding the existing measure response.

### 2.2. Distinguishing Query Types

BydbQL names the target data model in the `FROM` clause. Measure SELECT queries also support the `group.measure` shorthand.

- **Streams**: `FROM STREAM <name> IN <groups>` or `FROM STREAM <name> IN (<groups>)`
- **Measures**: `FROM MEASURE <name> IN <groups>`, `FROM MEASURE <name> IN (<groups>)`, or `FROM <group>.<name>`
- **Properties**: `FROM PROPERTY <name> IN <groups>` or `FROM PROPERTY <name> IN (<groups>)`
- **Traces**: `FROM TRACE <name> IN <groups>` or `FROM TRACE <name> IN (<groups>)`

Specialized queries, like Top-N, use a distinct top-level command:

- **Top-N**: `SHOW TOP <n> FROM MEASURE <name> IN <groups>` or `SHOW TOP <n> FROM MEASURE <name> IN (<groups>)`

### 2.3. Required Clauses

Every query identifies a source and its groups:

- **`FROM` clause**: Specifies the data model type, resource name, and group list
- **`IN groups` clause**: Specifies one or more groups for an explicit resource source. Parentheses around the group list are optional. Measure shorthand embeds its single group in `FROM group.measure`.

Measure SELECT queries can specify time bounds in `TIME`, in `WHERE time ...`, or in both clauses. The clauses intersect when combined. See [raw measure query extensions](#55-raw-measure-query-extensions). The other resource forms retain their existing `TIME` syntax; properties have no `TIME` clause.

### 2.4. Case Sensitivity

BydbQL follows SQL-like conventions for case sensitivity:

- **Reserved words are case-insensitive**: Keywords like `SELECT`, `FROM`, `WHERE`, `ORDER BY`, `TIME`, `BETWEEN`, `AND`, etc. can be written in any case combination.
- **Identifiers are case-sensitive**: Names of streams, measures, traces, properties, tags, and fields preserve their case and must be referenced exactly as defined.

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

#### Identifier Character Set

An identifier starts with a letter or underscore and may continue with letters, digits, underscores, hyphens (`-`), and asterisks (`*`):

```
identifier ::= [a-zA-Z_][a-zA-Z0-9_*-]*
```

Hyphens and asterisks are allowed only after the first character. This matters because the schema layer accepts such names at creation time — a TopN rule named `endpoint_avg-cluster-excludes-*`, for instance — and a resource that can be created must remain referenceable from a query:

```sql
SHOW TOP 10 FROM MEASURE endpoint_avg-cluster-excludes-* IN default;
```

Confining `*` to non-initial positions is what keeps the star projection unambiguous: a bare `*` cannot begin an identifier, so `SELECT *` is always a projection.

**Caveat**: an identifier that begins with a reserved word followed by a non-word character is rejected by the lexer — `IN group-a` fails because `group` matches the keyword rule first. Avoid naming resources with a reserved-word prefix.

## 2.5. Timestamp Formats

BydbQL supports flexible timestamp specifications in TIME clauses, accommodating both absolute and relative time formats:

### 2.5.1. Absolute Time Format

Absolute timestamps use the RFC3339 standard format:

```
"2006-01-02T15:04:05Z07:00"
```

Examples:

- `"2023-01-01T00:00:00Z"` - January 1, 2023, 00:00:00 UTC
- `"2023-01-01T15:30:45+08:00"` - January 1, 2023, 15:30:45 UTC+8
- `"2023-12-31T23:59:59Z"` - December 31, 2023, 23:59:59 UTC

### 2.5.2. Relative Time Format

Relative timestamps are duration strings that are evaluated relative to the current time:

```
[-][duration]
```

Supported duration units:

- `m` - minutes
- `h` - hours
- `d` - days
- `w` - weeks

Examples:

- `"-30m"` - 30 minutes ago
- `"2h"` - 2 hours from now
- `"-1d"` - 1 day ago
- `"-1w"` - 1 week ago
- `"now"` - current time

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

## 2.6. Parameter Binding

BydbQL supports positional parameter binding with the `?` placeholder, so untrusted values never need to be concatenated into the query string. This eliminates QL injection: a bound value is always treated as a plain value and can never alter the query structure.

```sql
SELECT trace_id, message
FROM STREAM logs IN group1
TIME > ?
WHERE message MATCH(?)
```

Parameters are supplied alongside the query and are bound to the placeholders in their order of appearance. Each parameter is a `model.v1.TagValue`. Over HTTP:

```json
POST /api/v1/bydbql/query
{
  "query": "SELECT trace_id, message FROM STREAM logs IN group1 TIME > ? WHERE message MATCH(?)",
  "params": [
    {"str": {"value": "-30m"}},
    {"str": {"value": "error"}}
  ]
}
```

### 2.6.1. Supported Positions

Placeholders are accepted in *value* positions only. The complete list, with the parameter types each position accepts:

| Position | Example | Accepted parameter types |
|---|---|---|
| `TIME` comparison value | `TIME > ?` | `str` (RFC3339, relative like `-30m`, or `now`), `timestamp` |
| `TIME BETWEEN` boundaries | `TIME BETWEEN ? AND ?` | same as above; each boundary binds independently |
| Measure `WHERE time` comparison | `WHERE time >= ? AND time < ?` | `str` (RFC3339/RFC3339Nano or `now`), `timestamp`; relative strings such as `-30m` are not accepted here |
| Scalar comparison value (`=`, `!=`, `>`, `>=`, `<`, `<=`) | `WHERE service_id = ?` | `str`, `int`, `null` |
| `IN` / `NOT IN` value list | `IN (?, ?)`, `IN (?)`, `IN ('a', ?)` | `str`, `int`, `null`, `str_array`, `int_array` |
| `MATCH` value list | `MATCH(?)`, `MATCH(?, 'analyzer')` | `str`, `int`, `null`, `str_array`, `int_array` |
| `HAVING` / `NOT HAVING` values | `HAVING ?`, `HAVING (?)` | `str`, `int`, `null`, `str_array`, `int_array` |
| `LIMIT` count | `LIMIT ?` | `int` only |
| `OFFSET` count | `OFFSET ?` | `int` only |
| `SHOW TOP N` count | `SHOW TOP ? FROM MEASURE ...` | `int` only |
| `SELECT TOP N` projection count | `SELECT TOP ? field ...` | `int` only |

Bound values are kept out of the query text, so the server can group requests by template. One consequence worth knowing: if a query is slow enough to be logged, the server may record a sample of its bound values. By default it records only a digest of each string, never the string itself, and the operator can turn that off entirely or turn it up to verbatim — see [BydbQL Slow Queries](../operation/observability/logging.md#bydbql-slow-queries).

Array expansion: an array parameter bound inside a value list expands in place — `WHERE service_id IN (?)` with a three-entry `str_array` behaves exactly like listing the three strings literally, and literals can be mixed around it (`IN ('a', ?, 'b')`). The container keeps the form a literal query would have: a single-value `HAVING ?` stays a single value unless the array holds multiple entries, while a parenthesized `HAVING (?)` stays a list regardless of the entry count.

Repeated parameterized measure queries can reuse a compiled plan when their query text and schema dependencies remain valid. Each request still binds its own values, evaluates a fresh execution clock, and checks authorization. Reusing a plan does not reuse query results or freeze `now()`.

### 2.6.2. Unsupported Positions

Placeholders in any position that determines the query *structure* rather than a compared *value* are rejected at parse time with a syntax error. Bound parameters therefore can never change the shape of a query, which is what makes binding injection-proof.

| Category | Positions | Example (all syntax errors) |
|---|---|---|
| Resource identifiers | resource name, group name, stage name | `FROM STREAM ?`, `IN ?`, `ON ? STAGES` |
| Column identifiers | projection columns, condition left-hand side, `ORDER BY` / `GROUP BY` columns, aggregate function arguments | `SELECT ?`, `WHERE ? = 'a'`, `ORDER BY ?`, `SUM(?)` |
| Function options | `MATCH` analyzer and operator options | `MATCH('x', ?)` |
| Structure | operators, `ASC`/`DESC`, keywords, doubled placeholders | `= ??` |

Note that unlike some SQL implementations where `ORDER BY ?` silently degrades to a no-op constant sort, BydbQL rejects it outright.

### 2.6.3. Type Rules Summary

- `binary_data` parameters are rejected in every position (BydbQL has no binary literal).
- `timestamp` parameters are accepted in `TIME` positions and measure `WHERE time` comparisons. Their protobuf timestamp values are validated; a missing or invalid timestamp is rejected at bind time, and measure planning also checks the stored timestamp domain.
- `int` and `null` parameters are rejected in time positions: use `str` or `timestamp` instead.
- `int` parameters in count positions must fit the wire type of the position — `[0, 4294967295]` for `LIMIT`/`OFFSET` and `[0, 2147483647]` for the `TOP N` counts; out-of-range values are rejected at bind time instead of silently wrapping, and literal counts are held to the same bounds.
- Empty arrays, parameters without a value, and `nil` entries are rejected everywhere.
- A `?` inside a quoted string literal is part of the string, not a placeholder: `WHERE msg = 'why?'` consumes no parameter.
- After binding passes, values are checked against the tag schema by the same rules as literals: a `str` that cannot parse as an integer fails against an `INT` tag, while an `int` bound to a `STRING` tag is converted, exactly as if written literally.

### 2.6.4. Errors

The request fails with `InvalidArgument` when the number of parameters does not match the number of placeholders, or when a parameter type is not accepted at its placeholder's position. Error messages include the 1-based position of the offending parameter (the first `?` is parameter #1).

## 3. WHERE Statement

The WHERE clause in BydbQL provides powerful filtering capabilities with support for various operators, logical expressions, and full-text search. It supports binary tree structures with proper operator precedence and parentheses grouping.

### Key Features

- **Binary Tree Structure**: WHERE conditions are organized as a binary expression tree supporting complex nested logic
- **Operator Precedence**: Parentheses `()` > `AND` > `OR`
- **Multiple Operators**: Comparison (`=`, `!=`, `>`, `<`, `>=`, `<=`), set operations (`IN`, `NOT IN`, `HAVING`, `NOT HAVING`), and full-text search (`MATCH`)
- **Type Support**: String, integer, and NULL values
- **Complex Expressions**: Support for nested parentheses and mixed AND/OR logic

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

- **value(s)** (required): The search term(s) to match. Can be:
  - Single value: `MATCH('error')`
  - Multiple values (array): `MATCH(('error', 'warning'))` - wrapped in parentheses
  - Supports both string and integer types

- **analyzer** (optional): The text analyzer to use for tokenizing and matching. Common analyzers include:
  - `"standard"` - Standard text analysis with lowercase and word tokenization
  - `"simple"` - Simple lowercase analysis
  - `"keyword"` - No tokenization, exact matching (case-insensitive)
  - `"url"` - URL-specific tokenization
  - Default: Uses the analyzer configured for the field/tag in the schema

- **operator** (optional): Logical operator for multiple values. Valid values:
  - `"AND"` - All values must match (default for multiple values)
  - `"OR"` - At least one value must match

#### 3.1.2. Supported Data Types

The MATCH operator is available in:

- **Streams**: For full-text search in stream tags
- **Measures**: For full-text search in measure tags
- **Traces**: For full-text search in trace tags (e.g., searching span attributes)

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
TIME BETWEEN '-1h' AND 'now'
WHERE service_id IN ('api-gateway', 'auth-service')
  AND http_status > 400
  AND operation_name MATCH('api', 'keyword')
ORDER BY start_time DESC;
```

#### 3.1.4. Performance Considerations

- **Indexing**: MATCH queries leverage full-text indexes. Ensure your tags/fields are properly indexed for optimal performance.
- **Analyzer Selection**: Choose the appropriate analyzer for your use case:
  - Use `"keyword"` for exact matching
  - Use `"standard"` for general text search
  - Use `"url"` for URL-specific patterns
- **Multiple Values**: When using multiple values, `"OR"` logic may scan more results than `"AND"` logic.
- **Combination with Filters**: Combine MATCH with other WHERE conditions to narrow down results and improve query performance.

#### 3.1.5. Notes

- The MATCH operator is case-insensitive by default (behavior depends on the analyzer).
- Multiple values must be wrapped in parentheses: `MATCH(('val1', 'val2'))`.
- The analyzer and operator parameters are optional; when omitted, schema defaults are used.
- For single-value searches, the operator parameter is ignored.

## 4. BydbQL for Streams

BydbQL for streams is designed for querying and retrieving raw time-series elements. The syntax maps to the `banyandb.stream.v1.QueryRequest` message.

### 4.1. Grammar

```
query           ::= SELECT projection from_stream_clause TIME time_condition [WHERE criteria] [ORDER BY order_expression] [WITH QUERY_TRACE] [LIMIT integer] [OFFSET integer]
from_stream_clause ::= "FROM STREAM" identifier "IN" ["("] group_list [")"] [ON ["("] stage_list [")"] STAGES]
projection      ::= "*" | column_list
column_list     ::= identifier ("," identifier)*
group_list      ::= identifier ("," identifier)+
stage_list      ::= identifier ("," identifier)+
criteria        ::= condition (("AND" | "OR") condition)*
condition       ::= identifier binary_op (value | value_list)
time_condition  ::= "=" timestamp | ">" timestamp | "<" timestamp | ">=" timestamp | "<=" timestamp | "BETWEEN" timestamp "AND" timestamp
binary_op       ::= "=" | "!=" | ">" | "<" | ">=" | "<=" | "IN" | "NOT IN" | "HAVING" | "NOT HAVING" | "MATCH"
order_expression::= [identifier] ["ASC" | "DESC"]
value           ::= string_literal | integer_literal | "NULL"
value_list      ::= "(" value ("," value)* ")"
timestamp       ::= string_literal | integer_literal
	/* timestamp supports both absolute and relative time formats:
	   - Absolute: RFC3339 format like "2006-01-02T15:04:05Z07:00"
	   - Relative: duration strings like "-30m", "2h", "1d" (relative to current time) */
identifier      ::= [a-zA-Z_][a-zA-Z0-9_*-]*
string_literal  ::= "'" [^']* "'" | "\"" [^\"]* "\""
integer_literal ::= [0-9]+
```

**Note**: The `TIME` clause and `IN groups` clause are **required** for all stream queries. At least one group must be specified. Parentheses around the group list are optional.

### 4.2. Mapping to `stream.v1.QueryRequest`

- **`FROM STREAM name IN groups`** or **`FROM STREAM name IN (groups)`**: Maps to the `name` and `groups` fields. Both are required.
- **`SELECT tags`**: Maps to `projection`. Requires a stream schema to resolve tags to their families.
- **`TIME` clause (required)**: Maps to `time_range`:
  - **`TIME = '2023-01-01T00:00:00Z'`**: Sets `begin` and `end` to the same timestamp.
  - **`TIME > '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp.
  - **`TIME < '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp.
  - **`TIME >= '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp (inclusive).
  - **`TIME <= '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp (inclusive).
  - **`TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'`**: Sets `begin` and `end` to the respective timestamps.
  - **`TIME > '-30m'`**: Sets `begin` to 30 minutes ago.
  - **`TIME BETWEEN '-1h' AND 'now'`**: Sets `begin` to 1 hour ago and `end` to current time.
- **`WHERE conditions`**: Maps to `criteria`.
- **`ORDER BY` clause**: Maps to `order_by`. Supports the following forms:
  - **`ORDER BY field`**: Maps to `order_by` with ascending sort by default.
  - **`ORDER BY field DESC` / `ORDER BY field ASC`**: Adds an explicit sort direction while targeting the specified field.
  - **`ORDER BY TIME DESC` / `ORDER BY TIME ASC`**: Shorthand that relies on the timestamps.
- **`LIMIT`/`OFFSET`**: Maps to `limit` and `offset`.
- **`WITH QUERY_TRACE`**: Maps to the `trace` field to enable distributed tracing of query execution.

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

-- Query with lifecycle stages
SELECT trace_id, service_id
FROM STREAM sw IN group1, group2 ON warn, cold STAGES
TIME > '-30m'
WHERE service_id = 'webapp'
LIMIT 100;
```

## 5. BydbQL for Measures

BydbQL for measures supports native aggregation and grouping, plus raw queries with column aliases and time predicates in `WHERE`. It uses the existing measure query and response messages. The raw extensions and their current limits are described in [section 5.5](#55-raw-measure-query-extensions).

### 5.1. Grammar

```
measure_query     ::= SELECT projection from_measure_clause [TIME time_condition] [WHERE criteria] [GROUP BY group_list] [ORDER BY order_expression] [WITH QUERY_TRACE] [LIMIT count] [OFFSET count]
from_measure_clause ::= ("FROM MEASURE" identifier "IN" ["("] group_list_id [")"] | "FROM" identifier "." identifier) [ON ["("] stage_list [")"] STAGES]
projection        ::= "*" | column_list | top_clause
top_clause        ::= "TOP" count identifier ["ASC" | "DESC"] ["," column_list]
column_list       ::= column ("," column)*
column            ::= ("time" | identifier_path | agg_function "(" ["DISTINCT"] identifier_path ")") ["::TAG" | "::FIELD"] ["AS" identifier_path]
stage_list        ::= identifier ("," identifier)*
agg_function      ::= "SUM" | "MEAN" | "AVG" | "COUNT" | "MAX" | "MIN"
	/* "DISTINCT" is only valid with COUNT: COUNT(DISTINCT identifier) */
group_list        ::= group_column ("," group_column)*
group_column      ::= identifier | time_bucket_function
time_bucket_function ::= "TIME_BUCKET" "(" [string_literal] ")"
	/* at most one TIME_BUCKET(...) per GROUP BY clause; empty width defers
	   to the measure's own interval */
group_list_id     ::= identifier ("," identifier)*
criteria          ::= condition (("AND" | "OR") condition)*
condition         ::= identifier_path binary_op (value | value_list) | time_predicate | "(" criteria ")"
time_predicate    ::= "time" comparison_op time_value [interval_adjustment]
comparison_op     ::= "=" | "!=" | ">" | ">=" | "<" | "<="
time_value        ::= string_literal | "?" | "now" "(" ")"
interval_adjustment ::= ("+" | "-") "INTERVAL" string_literal
time_condition    ::= "=" timestamp | ">" timestamp | "<" timestamp | ">=" timestamp | "<=" timestamp | "BETWEEN" timestamp "AND" timestamp
binary_op         ::= "=" | "!=" | ">" | "<" | ">=" | "<=" | "IN" | "NOT IN" | "HAVING" | "NOT HAVING" | "MATCH"
order_expression  ::= ("time" | identifier_path) ["ASC" | "DESC"] | "ASC" | "DESC"
value             ::= string_literal | integer_literal | "NULL" | "?"
value_list        ::= "(" value ("," value)* ")"
timestamp         ::= string_literal | integer_literal | "?"
	/* timestamp supports both absolute and relative time formats:
	   - Absolute: RFC3339 format like "2006-01-02T15:04:05Z07:00"
	   - Relative: duration strings like "-30m", "2h", "1d" (relative to current time) */
identifier        ::= [a-zA-Z_][a-zA-Z0-9_*-]*
identifier_path   ::= identifier ("." identifier)* | quoted_identifier
quoted_identifier ::= string_literal /* used as a column name */
string_literal    ::= "'" [^']* "'" | "\"" [^\"]* "\""
integer_literal   ::= [0-9]+
count             ::= integer_literal | "?"
```

**Source and time bounds**: The explicit source form requires at least one `IN` group; parentheses around the list are optional. The shorthand names one group and one measure. `TIME` is optional for measure SELECT; raw queries may use `WHERE time` instead. Syntax accepted by the parser is subject to the execution limits in section 5.5, including the restrictions on combining raw extensions with aggregation.

### 5.2. BydbQL Extensions for `SELECT`

The `SELECT` clause for measures is highly flexible, allowing for the selection of tags, fields, and aggregations in a single, flat list.

- `SELECT <field_key>, <tag_key>`: Returns specific fields and tags. The parser will infer the type of each identifier from the measure's schema.
- `SELECT <identifier>::field, <identifier>::tag`: If a field and a tag share the same name, the `::field` or `::tag` syntax **must** be used to disambiguate the identifier's type.
- `SELECT <column> AS <name>`: Renames a raw output column. Family-qualified tags such as `default.host` select a particular tag family; aliases retain the original tag family or field container.
- `SELECT time, time AS observed_at`: Selects the built-in timestamp. Its response mapping is described in section 5.5.
- Legacy queries support one native aggregation function (`SUM`, `MEAN`/`AVG`, `COUNT`, `MAX`, `MIN`) per query and a `TOP N` clause for ranked results. Extended measure queries support multiple aggregate outputs and aliases, as described in section 5.8.
- An aggregate function's column resolves against both tags and fields, not just fields — `SUM(latency)` aggregates a field, `COUNT(DISTINCT user_id)` aggregates a tag. `DISTINCT` is only accepted inside `COUNT`; `SUM(DISTINCT x)` and similar are rejected as a parse error.

### 5.3. Mapping to `measure.v1.QueryRequest`

- **`FROM MEASURE name IN groups`** or **`FROM MEASURE name IN (groups)`**: Maps to the `name` and `groups` fields. Both are required.
- **`FROM group.name`**: Uses the same measure name and a single group, with the raw-query behavior described in section 5.5.
- **`SELECT <tag1>, <field1>, <field2>`**: The transformer inspects each identifier. Those identified as tags (either by schema lookup or `::tag`) are added to `tag_projection`. Those identified as fields (by schema lookup or `::field`) are added to `field_projection`.
- **`SELECT SUM(field)`**: Maps to `agg`, setting `agg.field_name`.
- **`SELECT COUNT(DISTINCT tag)`**: Maps to `agg` with `function = AGGREGATION_FUNCTION_COUNT_DISTINCT`, setting `agg.tag_name` and `agg.tag_family` instead of `agg.field_name`. The output column is named after the tag (`tag`), not after the function. A `COUNT(DISTINCT field)` over an INT or FLOAT field is also accepted, and sets `agg.field_name` as any other field aggregation does.
- **`TIME` clause (optional for measure SELECT)**: Maps to `time_range`; bounds from `WHERE time` intersect this range:
  - **`TIME = '2023-01-01T00:00:00Z'`**: Sets `begin` and `end` to the same timestamp.
  - **`TIME > '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp.
  - **`TIME < '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp.
  - **`TIME >= '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp (inclusive).
  - **`TIME <= '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp (inclusive).
  - **`TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'`**: Sets `begin` and `end` to the respective timestamps.
  - **`TIME > '-30m'`**: Sets `begin` to 30 minutes ago.
  - **`TIME BETWEEN '-1h' AND 'now'`**: Sets `begin` to 1 hour ago and `end` to current time.
- **`GROUP BY <tag1>, <tag2>`**: The `GROUP BY` clause takes a list of tags and maps to `group_by.tag_projection`. Each grouped tag must also appear in the query's own `SELECT` projection.
- **`GROUP BY TIME_BUCKET(width), <tag>`**: Maps to `group_by.time_bucket`, setting `group_by.time_bucket.width`. An empty `width` (`TIME_BUCKET()`) defers to the measure's own `interval`; a duration string (e.g. `'5m'`) uses the same units as `Measure.interval` (`ns`, `us`/`µs`, `ms`, `s`, `m`, `h`, plus the custom `d` suffix — see `pkg/timestamp.ParseDuration`). At most one `TIME_BUCKET(...)` is allowed per `GROUP BY` clause. Results are pre-ordered by bucket ascending, so a bucketed query needs no `ORDER BY` for that purpose — a non-time `ORDER BY DESC` is rejected on a bucketed query.
- **`SELECT TOP N ...`**: Maps to the `top` message. `TOP` can rank by a tag-targeted aggregation's own output column (e.g. `TOP 10 user_id DESC` when `COUNT(DISTINCT user_id)` is projected), not only by a field.
- **`WITH QUERY_TRACE`**: Maps to the `trace` field to enable distributed tracing of query execution.

**Note on `LIMIT` and `TIME_BUCKET`**: Legacy measure queries default to 100 when `LIMIT` is omitted. A bucketed `GROUP BY` returns one row per (bucket, tag-group), so a wide time window with a narrow bucket width can produce far more than 100 rows; without an explicit `LIMIT` sized to the expected row count, the response is silently truncated to the first 100 rows in bucket-ascending order rather than returning an error. Always set `LIMIT` explicitly for a bucketed query. Extended raw queries have different omission and zero-limit behavior, described in section 5.5.

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
GROUP BY instance;

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
GROUP BY instance;

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

-- Query with lifecycle stages
SELECT
    region,
    SUM(latency)
FROM MEASURE service_cpm IN us-west ON (warn, cold) STAGES
TIME > '-30m'
GROUP BY region;

-- Count distinct users per region
SELECT
    region,
    COUNT(DISTINCT user_id)
FROM MEASURE service_cpm IN us-west
TIME > '-30m'
GROUP BY region;

-- Bucket a metric into 5-minute windows, per region
SELECT
    region,
    SUM(latency)
FROM MEASURE service_cpm IN us-west
TIME > '-2h'
GROUP BY TIME_BUCKET('5m'), region
LIMIT 500;
```

### 5.5. Raw measure query extensions

The following measure SELECT extensions are implemented for 0.12.0. They return the existing `measure_result` response through the same Query API.

#### Sources, projections, and aliases

`FROM metrics.service_cpm` names the measure `service_cpm` in the group `metrics`. The explicit spelling `FROM MEASURE service_cpm IN metrics` remains available, including its multi-group form.

```sql
SELECT default.host AS node, value::FIELD AS reading, time AS observed_at
FROM metrics.service_cpm
WHERE time >= '2026-01-01T00:00:00Z'
  AND time < '2026-01-01T00:05:00Z'
ORDER BY observed_at DESC
LIMIT 100
```

Column resolution uses the measure schema:

- Use `family.tag` to distinguish tags with the same name in different families.
- Use `::TAG` or `::FIELD` when a tag and a field share a name. These select the column kind; they do not cast its value.
- A raw column can appear more than once with different aliases. Aliases rename output without changing its value type or nullness.
- A tag alias stays in its original tag family; a field alias stays in the field list. Duplicate output names in the same container are rejected. Different tag families, or tag and field containers, may use the same output name.

Unquoted `time` is the stored data-point timestamp. Every returned DataPoint retains its `timestamp`; selecting `time` alone adds no separate tag. `time AS observed_at` also returns a timestamp TagValue named `observed_at` in the `__ql` tag family. Selecting a user column with the same `__ql` output name is rejected. Use family qualification or an explicit column kind to select a user column named `time`.

#### Time predicates and binding

`WHERE time` supports `=`, `!=`, `>`, `>=`, `<`, and `<=`. Its right-hand side accepts an RFC3339/RFC3339Nano string, a string or timestamp parameter, or `now()`, optionally followed by `+ INTERVAL 'duration'` or `- INTERVAL 'duration'`.

```sql
SELECT host AS node, value::FIELD AS reading
FROM MEASURE service_cpm IN metrics
TIME BETWEEN '2026-01-01T00:00:00Z' AND '2026-01-01T00:10:00Z'
WHERE time >= ? AND time < ? AND host = ?
LIMIT ? OFFSET ?
```

For this query, supply five parameters in order: begin timestamp, end timestamp, host string, integer limit, and integer offset. For example, begin at `2026-01-01T00:05:00Z` and end at `2026-01-01T00:06:00Z` select that minute within the explicit `TIME` range. `TIME` and `WHERE time` bounds always intersect; neither overrides the other.

```sql
SELECT host AS node, value::FIELD AS reading
FROM metrics.service_cpm
WHERE time >= now() - INTERVAL '30m' AND time < now()
LIMIT 100
```

All `now()` references in one execution use one captured clock. A later request captures a fresh clock even when it reuses the same compiled plan. Quoted `'now'` is also accepted as a time comparison value. Relative strings such as `'-30m'` remain valid in the legacy `TIME` clause; in `WHERE time`, write `now() - INTERVAL '30m'` instead.

`INTERVAL` accepts fixed-duration units `ns`, `us`, `µs`, `ms`, `s`, `m`, `h`, `d`, and `w`, including fractional or compound durations such as `'1.5s'` and `'1h30m'`. A day means 24 hours and a week means seven days. Calendar months and years are not supported. Bounds outside the stored timestamp domain are rejected. Stored timestamps have millisecond resolution; comparisons preserve their strict or inclusive meaning at fractional-millisecond boundaries, and equality to a non-millisecond-aligned timestamp returns no rows.

Parenthesized time predicates support `AND` and `OR`. A disjoint time predicate keeps its full filter before pagination:

```sql
SELECT host AS node, time AS observed_at
FROM metrics.service_cpm
WHERE (time = '2026-01-01T00:00:00Z' OR time = '2026-01-01T00:10:00Z')
  AND host = 'frontend'
LIMIT 2 OFFSET 1
```

#### Pagination and current limits

A raw query uses the extended behavior when it uses source shorthand, an `AS` alias, the built-in time projection, or a time predicate in `WHERE`:

- Omitted `LIMIT` has no implicit 100-row cap; query memory limits still apply.
- Explicit `LIMIT 0` returns no rows and performs no native scan. Contradictory time bounds also return no rows without scanning.
- `OFFSET` and `LIMIT` apply after the complete time filter, including disjoint ranges. Legacy queries without these raw extensions keep their existing pagination defaults.
- Timestamp ordering, including an unambiguous timestamp alias such as `ORDER BY observed_at DESC`, is supported. Native index-rule ordering remains available. General sorting by tag/field output columns, multiple sort keys, and explicit NULL ordering are not implemented yet.

The current raw extensions cannot be combined with `SELECT TOP`. Aggregate projections and named scalar grouping use the extended aggregation behavior in section 5.8. Legacy native aggregation and grouping retain their existing syntax and behavior. Post-aggregation HAVING, field-value WHERE filters, arithmetic/casts, CASE/COALESCE, SELECT DISTINCT, and computed grouping are planned capabilities. An `OR` that mixes time and tag/field predicates is also rejected; combining a pure-time subtree with a tag predicate using `AND`, as above, is supported.

Aliases, built-in time projections, `now()`, timestamp `INTERVAL` arithmetic, and source shorthand in this section apply to measure SELECT. They do not extend stream, trace, property, or SHOW TOP syntax.

### 5.6. Legacy scalar distinct-count pagination

An ungrouped QL `COUNT(DISTINCT column)` consumes the complete native input before
returning its single count row. A positive `LIMIT` cannot reduce that input;
`LIMIT 0` or a positive `OFFSET` suppresses the row. With no matching input, the
count is zero. Grouped queries keep their existing native behavior and restrictions.

### 5.7. Compiled-plan cache controls

The liaison caches immutable compiled templates for parameterized measure QL,
separately from its prepared-statement/parse cache. Identical query text can reuse
a template with different parameters. Every execution rebinds values and one fresh
clock, checks schema dependencies and current authorization, and creates private
requests/operators. Literal queries bypass compiled reuse. Results and permission
decisions are not cached.

| Liaison flag | Default | Meaning of zero |
|---|---:|---|
| `--bydbql-plan-cache-size` | 4,000 retained templates | Disable compiled-plan reuse |
| `--bydbql-plan-cache-max-bytes` | 67,108,864 bytes (64 MiB) | Use the default byte limit |
| `--bydbql-plan-max-compiles` | 4 concurrent compilations | Use the default concurrency limit |
| `--bydbql-plan-max-in-flight` | 64 distinct shared compilations | Use the default in-flight limit |

Negative values are rejected. Count and byte limits include evicted templates
still held by active executions. Byte accounting estimates retained plan memory;
it is not an exact allocator heap measurement. Oversized plans or unstable local
schema views execute uncached under existing compilation/query resource limits.
Disabling compiled reuse preserves QL functionality and the separate prepared
cache. In particular, setting compiled-plan maximum bytes to zero does not remove
its byte bound.

Cache activity is visible through `bydbql_plan_cache_total{result}`,
`bydbql_plan_compiles_total`, `bydbql_plan_cache_count`,
`bydbql_plan_cache_bytes{kind}`, `bydbql_plan_work{kind}` and
`bydbql_plan_duration_seconds{stage}`. Cache outcomes also appear in existing QL
logs. These metrics distinguish plan reuse from prepared-cache activity.

Plan reuse reduces repeated planning work; end-to-end latency/CPU effects depend
on execution costs. The [current PR 1 measurements](../design/0.12.0/measure-ql-core-sql/pr1-verification.md)
show lower planning time and allocations but no established RPC latency gain.

### 5.8. Extended measure aggregation

Measure queries support multiple `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, and `MEAN` outputs over named columns. Aggregation uses this extended behavior when the query has multiple aggregate calls, source shorthand, an alias, or a `WHERE time` predicate. Existing single-aggregate queries using `FROM MEASURE ... IN ...` without these extensions keep their native behavior, including legacy `MEAN`/`AVG` results and count-distinct restrictions.

```sql
SELECT host AS node, COUNT(value) AS samples, SUM(value) AS total,
       MIN(value) AS minimum, MAX(value) AS maximum, AVG(value) AS average
FROM metrics.service_cpm
WHERE time >= ? AND time < ?
GROUP BY host
```

All aggregate calls use the complete, deduplicated input after the time and tag filters. Repeated calls share work while retaining their separate output aliases. Aggregates appear as named fields in the existing response; without an alias, their names are `COUNT(value)`, `SUM(value)`, and so on. Output field names must be unique. Synthesized aggregate rows omit raw-row timestamps, SIDs, and versions.

The extended aggregate rules are:

- `COUNT(column)` counts non-null values and returns an exact int64, including for floating-point inputs. It accepts scalar tags and fields.
- `SUM`, `MIN`, and `MAX` accept numeric columns and preserve int64 or float64 values. Integer SUM overflow fails the query.
- `AVG` and `MEAN` return float64 sum divided by non-null count. Fractional and sub-one averages are preserved; shard populations contribute their values and counts before finalization.
- Scalar empty input returns one row: COUNT is zero, and the other aggregates are null. Grouped empty input returns no rows. All-null groups also have COUNT zero and null SUM/MIN/MAX/AVG.
- `GROUP BY` accepts named scalar tag and field columns, including keys omitted from SELECT. Every selected non-aggregate column must be a grouping key. Aliases apply to output and cannot replace source names in GROUP BY. Null, zero, and empty-string keys remain distinct.
- Omitted LIMIT returns all groups, with no implicit 100-group cap. OFFSET and LIMIT apply after complete aggregation. LIMIT 0 returns no rows without scanning; contradictory time bounds still produce the scalar empty-input row unless pagination removes it.

```sql
SELECT COUNT(value) AS samples, AVG(value) AS average
FROM MEASURE service_cpm IN metrics
WHERE time >= ? AND time < ?
```

Extended aggregate queries currently reject COUNT(DISTINCT), TOP, TIME_BUCKET, array grouping, and ORDER BY. The existing legacy native forms of these supported operations remain available. Group-only queries without an aggregate, expressions as aggregate arguments or grouping keys, and HAVING are not implemented yet.

#### Native aggregate execution

The planner can execute eligible calls through the existing native aggregate operators and combine their batch results by the complete grouping tuple. For example, `SUM(f)`, `COUNT(f)`, and repeated `AVG(f)` outputs over a floating-point field share two native requests: SUM and COUNT. QL computes AVG from their sum and non-null count; it does not use the legacy native MEAN result. Integer MIN, MAX, and COUNT use their matching native requests, sharing the auxiliary COUNT needed to distinguish all-null groups.

This optimization currently requires one measure group in normal storage mode, one exact time interval, and a single provable lifecycle stage. Grouping keys must be entity tags from one tag family, constrained to non-null values by equality or IN predicates. Each remaining entity tag must be bound to one value, so every SQL group identifies one full entity. A scalar aggregate requires one bound full entity too. This also bounds COUNT by the number of possible stored millisecond timestamps, preserving its exact int64 result even when a native floating-point COUNT is returned.

Other supported queries use one complete raw scan and local SQL aggregation. These include nullable or non-entity keys, disjoint time ranges, multiple measure groups or stages, tag aggregates, floating-point MIN/MAX, and integer SUM/AVG spanning more than one possible timestamp. The fallback preserves checked integer overflow and SQL null/average semantics. Predicate conjunctions over entity IN lists are normalized before either execution path; expansion is limited to 512 entity tuples.

Native branches run sequentially under the parent query's deadline and memory allowance. Final LIMIT/OFFSET applies after all branches merge. A branch failure or exhausted allowance fails the query without partial results. Separate native requests do not provide a shared snapshot during concurrent writes. Memory limits remain enforced; disk spilling is not implemented.

## 6. BydbQL for Top-N

Top-N queries use a specialized, command-like syntax for clarity and to reflect the optimized nature of the underlying `banyandb.measure.v1.TopNRequest`. The `FROM` clause is mandatory.

### 6.1. Grammar

```
topn_query         ::= SHOW TOP integer from_measure_clause TIME time_condition [WHERE topn_criteria] [AGGREGATE BY agg_function] [ORDER BY ["ASC"|"DESC"]] [WITH QUERY_TRACE]
from_measure_clause ::= "FROM MEASURE" identifier "IN" ["("] group_list [")"] [ON ["("] stage_list [")"] STAGES]
topn_criteria      ::= condition (("AND" | "OR") condition)*
condition          ::= identifier binary_op (value | value_list)
stage_list         ::= identifier ("," identifier)+
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
identifier         ::= [a-zA-Z_][a-zA-Z0-9_*-]*
string_literal     ::= "'" [^']* "'" | "\"" [^\"]* "\""
integer_literal    ::= [0-9]+
```

**Note**: The `TIME` clause and `IN groups` clause are **required** for all Top-N queries. At least one group must be specified. Parentheses around the group list are optional.

### 6.2. Mapping to `measure.v1.TopNRequest`

- **`SHOW TOP N`**: Maps to `top_n`.
- **`FROM MEASURE name IN groups`** or **`FROM MEASURE name IN (groups)`**: Maps to the `name` and `groups` fields. Both are required.
- **`TIME` clause (required)**: Maps to `time_range`:
  - **`TIME = '2023-01-01T00:00:00Z'`**: Sets `begin` and `end` to the same timestamp.
  - **`TIME > '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp.
  - **`TIME < '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp.
  - **`TIME >= '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp (inclusive).
  - **`TIME <= '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp (inclusive).
  - **`TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'`**: Sets `begin` and `end` to the respective timestamps.
  - **`TIME > '-30m'`**: Sets `begin` to 30 minutes ago.
  - **`TIME BETWEEN '-1h' AND 'now'`**: Sets `begin` to 1 hour ago and `end` to current time.
- **`WHERE tag = 'value'`**: Maps to `conditions`. Only simple equality is supported.
- **`AGGREGATE BY FUNC`**: Maps to `agg`.
- **`ORDER BY DESC`**: Maps to `field_value_sort`.
- **`WITH QUERY_TRACE`**: Maps to the `trace` field to enable distributed tracing of query execution.

### 6.3. Examples

```sql
-- Get the Top 10 services with the highest latency
SHOW TOP 10
FROM MEASURE service_latency IN production
TIME > '-30m'
WHERE http_method = 'GET' AND version = 'v1.2.0'
ORDER BY DESC;

-- Get the Bottom 5 services with the fewest errors
SHOW TOP 5
FROM MEASURE service_errors_total IN production
TIME > '-30m'
ORDER BY ASC;

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
ORDER BY DESC;

-- Get the Top 10 services with highest latency in the last hour
SHOW TOP 10
FROM MEASURE service_latency IN production
TIME > '2023-01-01T13:00:00Z'
ORDER BY DESC;

-- Get the Top 5 endpoints with most errors in a specific time range
SHOW TOP 5
FROM MEASURE endpoint_errors IN production
TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'
WHERE status_code = '500'
ORDER BY DESC;

-- Get the Top 10 services with highest latency in the last hour
SHOW TOP 10
FROM MEASURE service_latency IN production
TIME > '-1h'
ORDER BY DESC;

-- Get the Top 5 endpoints with most errors in the last 24 hours
SHOW TOP 5
FROM MEASURE endpoint_errors IN production
TIME BETWEEN '-24h' AND 'now'
WHERE status_code = '500'
ORDER BY DESC;

-- Get the Top 3 services with lowest response time in the last 30 minutes
SHOW TOP 3
FROM MEASURE service_response_time IN production
TIME > '-30m'
ORDER BY ASC;

-- Top-N query with distributed tracing enabled
SHOW TOP 10
FROM MEASURE service_latency IN production
TIME > '-1h'
WHERE http_method = 'GET'
ORDER BY DESC
WITH QUERY_TRACE;

-- Top-N query with lifecycle stages
SHOW TOP 10
FROM MEASURE service_latency IN production ON warn, cold STAGES
TIME > '-30m'
ORDER BY DESC;
```

## 7. BydbQL for Properties

BydbQL for properties is designed for simple key-value lookups and metadata filtering. It maps to the `banyandb.property.v1.QueryRequest` message.

> **Note**: As of BanyanDB v0.10.0, Property queries support `ORDER BY` for sorted results.

### 7.1. Grammar

```
property_query      ::= SELECT projection from_property_clause [WHERE criteria] [ORDER BY identifier ["ASC"|"DESC"]] [WITH QUERY_TRACE] [LIMIT integer]
from_property_clause ::= "FROM PROPERTY" identifier "IN" ["("] group_list [")"]
projection          ::= "*" | column_list
column_list         ::= identifier ("," identifier)*
group_list          ::= identifier ("," identifier)+
criteria            ::= condition (("AND" | "OR") condition)*
condition           ::= identifier binary_op (value | value_list) | "ID" binary_op (value | value_list)
binary_op           ::= "=" | "!=" | ">" | "<" | ">=" | "<=" | "IN" | "NOT IN"
value               ::= string_literal | integer_literal | "NULL"
value_list          ::= "(" value ("," value)* ")"
identifier          ::= [a-zA-Z_][a-zA-Z0-9_*-]*
string_literal      ::= "'" [^']* "'" | "\"" [^\"]* "\""
integer_literal     ::= [0-9]+
```

**Note**: The `IN groups` clause is **required** for all property queries. At least one group must be specified. Parentheses around the group list are optional. Property queries do **not** require a `TIME` clause.

### 7.2. Mapping to `property.v1.QueryRequest`

- **`FROM PROPERTY name IN groups`** or **`FROM PROPERTY name IN (groups)`**: Maps to the `name` and `groups` fields. Both are required.
- **`SELECT tags`**: Maps to `tag_projection`.
- **`WHERE ID IN (...)`**: Maps to `ids`.
- **`WHERE tag = 'value'`**: Maps to `criteria`.
- **`ORDER BY tag [ASC|DESC]`**: Maps to `order_by`.
- **`LIMIT n`**: Maps to `limit`.
- **`WITH QUERY_TRACE`**: Maps to the `trace` field to enable distributed tracing of query execution.

### 7.3. Examples

```sql
-- Find properties by filtering on their tags
SELECT ip, owner
FROM PROPERTY server_metadata IN datacenter-1
WHERE datacenter = 'dc-101' AND in_service = 'true'
LIMIT 50;

-- Find properties by filtering on their tags and order by
SELECT ip, owner
FROM PROPERTY server_metadata IN datacenter-1
WHERE datacenter = 'dc-101' AND in_service = 'true'
ORDER BY ip
LIMIT 50;

-- Find properties and sort by an integer tag
SELECT ip, owner, priority
FROM PROPERTY server_metadata IN datacenter-1
WHERE in_service = 'true'
ORDER BY priority ASC
LIMIT 20;

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
WITH QUERY_TRACE
LIMIT 50;
```

## 8. BydbQL for Traces

BydbQL for traces is designed for querying and retrieving trace data with spans. The syntax maps to the `banyandb.trace.v1.QueryRequest` message and is optimized for trace-specific operations.

### 8.1. Grammar

```
trace_query           ::= SELECT projection from_trace_clause TIME time_condition [WHERE criteria] [ORDER BY order_expression] [WITH QUERY_TRACE] [LIMIT integer] [OFFSET integer]
from_trace_clause     ::= "FROM TRACE" identifier "IN" ["("] group_list [")"] [ON ["("] stage_list [")"] STAGES]
projection            ::= "*" | column_list | "()"
column_list           ::= identifier ("," identifier)*
group_list            ::= identifier ("," identifier)+
stage_list            ::= identifier ("," identifier)+
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
identifier            ::= [a-zA-Z_][a-zA-Z0-9_*-]*
string_literal        ::= "'" [^']* "'" | "\"" [^\"]* "\""
integer_literal       ::= [0-9]+
```

**Note**: The `TIME` clause and `IN groups` clause are **required** for all trace queries. At least one group must be specified. Parentheses around the group list are optional.

### 8.2. Trace Model Characteristics

The Trace model in BanyanDB is specifically designed for storing and querying trace data with the following key characteristics:

- **Trace Resource**: A logical namespace within a group that defines the structure for trace data
- **Tags**: Indexed tags that support filtering and querying (defined by `TraceTagSpec`)
- **Trace ID Tag**: A specific tag that stores the trace ID for trace correlation
- **Timestamp Tag**: A specific tag that stores the timestamp for time-based queries
- **Span Data**: Raw span data stored as binary for efficient storage and retrieval

### 8.2.1. Empty Projection Support

BydbQL for traces supports an empty projection syntax `SELECT ()` that allows queries to return only raw span data without any tag information. This is useful for:

- **Performance Optimization**: When you only need the raw span data and don't want to pay the cost of loading and returning tag values
- **Data Processing**: When you plan to process the raw span data externally and don't need the indexed tag values
- **Storage Efficiency**: Reducing network transfer and memory usage by excluding tag data

**Syntax**: `SELECT ()` - Returns only raw span data, no tag information
**Behavior**: The query will still apply filtering and ordering based on the WHERE and ORDER BY clauses, but the result will contain only the binary span data without any tag projections.

### 8.3. Mapping to `trace.v1.QueryRequest`

- **`FROM TRACE name IN groups`** or **`FROM TRACE name IN (groups)`**: Maps to the `name` and `groups` fields. Both are required.
- **`SELECT tags`**: Maps to `tag_projection`. Requires a trace schema to resolve tags to their specifications.
- **`SELECT ()`**: Maps to an empty `tag_projection` array, indicating no tag data should be returned (only raw span data).
- **`TIME` clause (required)**: Maps to `time_range`:
  - **`TIME = '2023-01-01T00:00:00Z'`**: Sets `begin` and `end` to the same timestamp.
  - **`TIME > '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp.
  - **`TIME < '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp.
  - **`TIME >= '2023-01-01T00:00:00Z'`**: Sets `begin` to the timestamp (inclusive).
  - **`TIME <= '2023-01-01T00:00:00Z'`**: Sets `end` to the timestamp (inclusive).
  - **`TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'`**: Sets `begin` and `end` to the respective timestamps.
  - **`TIME > '-30m'`**: Sets `begin` to 30 minutes ago.
  - **`TIME BETWEEN '-1h' AND 'now'`**: Sets `begin` to 1 hour ago and `end` to current time.
- **`WHERE conditions`**: Maps to `criteria` for filtering spans based on tag values.
- **`ORDER BY field`**: Maps to `order_by` for sorting results.
- **`LIMIT`/`OFFSET`**: Maps to `limit` and `offset` for pagination.
- **`WITH QUERY_TRACE`**: Maps to the `trace` field to enable distributed tracing of query execution.

### 8.3.1. Naming Convention Clarification

To avoid confusion between different uses of the word "trace" in BanyanDB:

- **Trace Model**: Refers to the data model for storing trace data (spans, tags, etc.) - used in `FROM TRACE` clauses
- **Query Tracing**: Refers to distributed tracing of query execution for observability - enabled with `WITH QUERY_TRACE`

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

-- Query with lifecycle stages
SELECT trace_id, service_id
FROM TRACE sw_trace IN group1 ON (warn, cold) STAGES
TIME > '-30m'
WHERE status = 'error'
LIMIT 100;
```

## 9. Summary of BydbQL Capabilities

| Feature             | Streams                                         | Measures                                        | Top-N                                           | Properties                                      | Traces                                          |
| :------------------ | :---------------------------------------------- | :---------------------------------------------- | :---------------------------------------------- | :---------------------------------------------- | :---------------------------------------------- |
| **Primary Command** | `SELECT ... FROM STREAM ... IN ...`             | `SELECT ... FROM MEASURE ... IN ...` or `FROM group.measure` | `SHOW TOP ... FROM MEASURE ... IN ...`          | `SELECT ... FROM PROPERTY ... IN ...`           | `SELECT ... FROM TRACE ... IN ...`              |
| **Groups Clause**   | **Required** `IN groups` (parentheses optional) | `IN groups` or single group in shorthand | **Required** `IN groups` (parentheses optional) | **Required** `IN groups` (parentheses optional) | **Required** `IN groups` (parentheses optional) |
| **Time Clause**     | **Required** `TIME ...`                         | Optional `TIME` and/or raw `WHERE time` | **Required** `TIME ...`                         | Not applicable                                  | **Required** `TIME ...`                         |
| **Projection**      | Tags by family                                  | Tags, fields, raw aliases, and timestamp projection | Implicit (entity, value)                        | Flat list of tags                               | Tags by specification                           |
| **Aggregation**     | No                                              | Yes (`SUM`, `MEAN`, etc.)                       | Yes (optional)                                  | No                                              | No                                              |
| **Grouping**        | No                                              | Yes (`GROUP BY`)                                | No                                              | No                                              | No                                              |
| **Filtering**       | Full `WHERE` clause                             | Full `WHERE` clause                             | Simple equality `WHERE`                         | `WHERE` by ID or tags                           | Full `WHERE` clause                             |
| **Ordering**        | Yes (`ORDER BY`)                                | Yes (`ORDER BY`)                                | Yes (`ORDER BY ASC/DESC`)                       | Yes (`ORDER BY tag`)                            | Yes (`ORDER BY`)                                |
| **Pagination**      | Yes (`LIMIT`/`OFFSET`)                          | Yes (`LIMIT`/`OFFSET`)                          | No                                              | `LIMIT` only                                    | Yes (`LIMIT`/`OFFSET`)                          |
