# BanyanDB Query Language (BDSL)

## 1. Introduction

### 1.1. Purpose

This document outlines the design of the BanyanDB Data Query Language (BDSL), a unified query language with SQL-like syntax for all of BanyanDB's data models. The primary goal of BDSL is to provide users with a familiar, intuitive, and powerful interface to interact with their data, abstracting the underlying protobuf-based APIs for streams, measures, properties, and Top-N queries.

This document specifies the language's syntax, its semantics, and the precise mapping from BDSL statements to the various `Request` messages.

### 1.2. Scope

BDSL supports querying across BanyanDB's primary data models:
*   **Streams**: For raw, time-series elements like logs and traces.
*   **Measures**: For aggregated numerical time-series data (metrics).
*   **Properties**: For metadata and key-value information.

It also provides a specialized syntax for optimized **Top-N** queries against measures.

## 2. Core Concepts

### 2.1. Parser Architecture

BDSL queries will be processed by a classic three-stage compiler front-end architecture, which ensures modularity and maintainability.

```mermaid
graph TD
    A[BDSL Query String] --> B{Lexer};
    B --> C{Parser};
    C --> D[Abstract Syntax Tree (AST)];
    D --> E{Transformer};
    E --> F[Protobuf Request];

    subgraph Parsing Pipeline
        B;
        C;
        D;
        E;
    end
```
*   **Lexer**: Breaks the query string into a sequence of tokens.
*   **Parser**: Builds an Abstract Syntax Tree (AST) from the tokens, validating the query's syntax.
*   **Transformer**: Traverses the AST, performs semantic analysis using a schema, and transforms the AST into the appropriate target protobuf `Request` message.

### 2.2. Distinguishing Query Types

BDSL distinguishes the target data model either explicitly through keywords in the `FROM` clause or implicitly through the **execution context**. This allows the parser to apply the correct grammar and transformation rules for the query.

*   **Explicit**: `FROM STREAM <name>`, `FROM MEASURE <name>`, `FROM PROPERTY <name>`
*   **Implicit**: The query is executed against a resource-specific endpoint (e.g., `/v1/streams/{stream_name}/query`), where the type and name of the resource are known.

Specialized queries, like Top-N, use a distinct top-level command and typically require an explicit `FROM` clause:

*   **Top-N**: `SHOW TOPN ... FROM MEASURE <measure_name>`

### 2.3. Optional `FROM` Clause

In BDSL, the `FROM` clause is **optional** for `SELECT` queries. When it is omitted, the target resource (the specific stream, measure, or property) **must** be supplied by the execution context.

The simplest possible BDSL query is `SELECT *`. When executed within the context of a stream named `sw`, this is equivalent to `SELECT * FROM STREAM sw`.

When the `FROM` clause is present, it overrides any context provided by the environment. This is useful for clients that connect to a generic query endpoint and need to specify the target resource directly within the query text.

## 3. BDSL for Streams

BDSL for streams is designed for querying and retrieving raw time-series elements. The syntax maps to the `banyandb.stream.v1.QueryRequest` message.

### 3.1. Grammar

```
query           ::= SELECT projection [from_stream_clause] [WHERE criteria] [ORDER BY order_expression] [LIMIT integer] [OFFSET integer]
from_stream_clause ::= "FROM STREAM" identifier ["IN" "(" group_list ")"]
projection      ::= "*" | column_list
column_list     ::= identifier ("," identifier)*
group_list      ::= identifier ("," identifier)*
criteria        ::= condition (("AND" | "OR") condition)*
condition       ::= identifier binary_op (value | value_list)
binary_op       ::= "=" | "!=" | ">" | "<" | ">=" | "<=" | "IN" | "NOT IN" | "HAVING" | "NOT HAVING" | "MATCH"
order_expression::= identifier ["ASC" | "DESC"]
value           ::= string_literal | integer_literal | "NULL"
value_list      ::= "(" value ("," value)* ")"
```

### 3.2. Mapping to `stream.v1.QueryRequest`

*   **`FROM STREAM name IN (groups)`**: Maps to the `name` and `groups` fields. If the clause is omitted, these values are taken from the execution context.
*   **`SELECT tags`**: Maps to `projection`. Requires a stream schema to resolve tags to their families.
*   **`WHERE conditions`**: Maps to `criteria`.
*   **`ORDER BY field`**: Maps to `order_by`.
*   **`LIMIT`/`OFFSET`**: Maps to `limit` and `offset`.

### 3.3. Examples

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
```

## 4. BDSL for Measures

BDSL for measures is tailored for analytical queries on aggregated numerical data. It supports aggregation, grouping, and mixed selection of tags and fields, mapping to the `banyandb.measure.v1.QueryRequest` message.

### 4.1. BDSL Extensions for `SELECT`

The `SELECT` clause for measures is highly flexible, allowing for the selection of tags, fields, and aggregations in a single, flat list.

*   `SELECT <field_key>, <tag_key>`: Returns specific fields and tags. The parser will infer the type of each identifier from the measure's schema.
*   `SELECT <identifier>::field, <identifier>::tag`: If a field and a tag share the same name, the `::field` or `::tag` syntax **must** be used to disambiguate the identifier's type.
*   The clause also supports aggregation functions (`SUM`, `MEAN`, `COUNT`, `MAX`, `MIN`) and a `TOP N` clause for ranked results.

### 4.2. Mapping to `measure.v1.QueryRequest`

*   **`FROM MEASURE name`**: Maps to the `name` field. If omitted, this value is taken from the execution context.
*   **`SELECT <tag1>, <field1>, <field2>`**: The transformer inspects each identifier. Those identified as tags (either by schema lookup or `::tag`) are added to `tag_projection`. Those identified as fields (by schema lookup or `::field`) are added to `field_projection`.
*   **`SELECT SUM(field)`**: Maps to `agg`.
*   **`GROUP BY <tag1>, <tag2>`**: The `GROUP BY` clause takes a simple list of tags and maps to `group_by.tag_projection`.
*   **`SELECT TOP N ...`**: Maps to the `top` message.

### 4.3. Examples

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
```

## 5. BDSL for Top-N

Top-N queries use a specialized, command-like syntax for clarity and to reflect the optimized nature of the underlying `banyandb.measure.v1.TopNRequest`. The `FROM` clause is mandatory.

### 5.1. Grammar

```
topn_query ::= SHOW TOP integer from_measure_clause [WHERE topn_criteria] [AGGREGATE BY agg_function] [ORDER BY value [ASC|DESC]]
from_measure_clause ::= "FROM MEASURE" identifier
```

### 5.2. Mapping to `measure.v1.TopNRequest`

*   **`SHOW TOP N`**: Maps to `top_n`.
*   **`WHERE tag = 'value'`**: Maps to `conditions`. Only simple equality is supported.
*   **`AGGREGATE BY FUNC`**: Maps to `agg`.
*   **`ORDER BY value DESC`**: Maps to `field_value_sort`.

### 5.3. Examples

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
```

## 6. BDSL for Properties

BDSL for properties is designed for simple key-value lookups and metadata filtering. It maps to the `banyandb.property.v1.QueryRequest` message.

### 6.1. Grammar

```
property_query ::= SELECT projection [from_property_clause] [WHERE criteria] [LIMIT integer]
from_property_clause ::= "FROM PROPERTY" identifier
projection ::= "*" | column_list
```

### 6.2. Mapping to `property.v1.QueryRequest`

*   **`FROM PROPERTY name`**: Maps to the `name` field. If omitted, this value is taken from the execution context.
*   **`SELECT tags`**: Maps to `tag_projection`.
*   **`WHERE ID IN (...)`**: Maps to `ids`.
*   **`WHERE tag = 'value'`**: Maps to `criteria`.
*   **`LIMIT n`**: Maps to `limit`.

### 6.3. Examples

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
```

## 7. Summary of BDSL Capabilities

| Feature             | Streams                               | Measures                              | Top-N                               | Properties                          |
| :------------------ | :------------------------------------ | :------------------------------------ | :---------------------------------- | :---------------------------------- |
| **Primary Command** | `SELECT ... [FROM STREAM]`            | `SELECT ... [FROM MEASURE]`           | `SHOW TOPN ... FROM MEASURE`        | `SELECT ... [FROM PROPERTY]`        |
| **Projection**      | Tags by family                        | Tags & Fields                         | Implicit (entity, value)            | Flat list of tags                   |
| **Aggregation**     | No                                    | Yes (`SUM`, `MEAN`, etc.)             | Yes (optional)                      | No                                  |
| **Grouping**        | No                                    | Yes (`GROUP BY`)                      | No                                  | No                                  |
| **Filtering**       | Full `WHERE` clause                   | Full `WHERE` clause                   | Simple equality `WHERE`             | `WHERE` by ID or tags               |
| **Ordering**        | Yes (`ORDER BY`)                      | Yes (`ORDER BY`)                      | Yes (`ORDER BY value`)              | No                                  |
| **Pagination**      | Yes (`LIMIT`/`OFFSET`)                | Yes (`LIMIT`/`OFFSET`)                | No                                  | `LIMIT` only                        |
