---
name: bydbql
description: Generate, validate, and optionally execute read-only BanyanDB BydbQL for STREAM, MEASURE, TRACE, and PROPERTY resources. Use when the user asks to query BanyanDB, translate natural language to BydbQL, inspect BanyanDB schema or data, validate BydbQL, or fetch raw BanyanDB records.
---

# BanyanDB BydbQL

Use this skill when the user asks to query BanyanDB, generate BydbQL, translate natural language to BydbQL, inspect BanyanDB data, validate BydbQL, or run read-only BanyanDB queries.

## Workflow

1. Read `references/safety.md` before generating, validating, or executing BydbQL.
2. Read `references/syntax.md` when query syntax is needed.
3. Read `references/examples.md` when mapping natural language to BydbQL.
4. Identify the BanyanDB resource type: `STREAM`, `MEASURE`, `TRACE`, or `PROPERTY`.
5. If the group, resource name, or resource type is missing or ambiguous, use `list_groups_schemas`.
6. Generate exactly one read-only BydbQL statement.
7. Call `validate_bydbql` before execution.
8. Execute with `list_resources_bydbql` only when the user asks to run, query, fetch, show, or inspect raw BanyanDB data.
9. If `validate_bydbql` returns `valid=false`, fix the query and validate again before any execution.
10. If execution fails due to a missing group, resource, or schema, use `list_groups_schemas` to discover the correct names and retry only when the correction is clear.

## Tool Use

Use the existing BanyanDB MCP tools:

- `list_groups_schemas`: discover groups and resource names for streams, measures, traces, and properties.
- `validate_bydbql`: run read-only safety checks and parse-only BydbQL syntax validation.
- `list_resources_bydbql`: execute a validated BydbQL statement when data retrieval is requested.

The Codex host model should perform natural language to BydbQL translation using these references and live schema discovery. Use `get_generate_bydbql_prompt` only when its live schema prompt text would materially help.

## Output

For generation-only BydbQL requests, return the single BydbQL statement and state that it was not executed.

For BydbQL execution requests, show the BydbQL statement, then summarize the returned data or error concisely.
