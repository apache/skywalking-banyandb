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
6. If the request involves sorting or ranking (`ORDER BY`, `SHOW TOP`, or wording such as "latest", "top", "highest", "lowest", "first", "last"), or you have already fetched schema, call `get_generate_bydbql_prompt` and follow its index-rule guidance. This is required: `get_generate_bydbql_prompt` is the only component that injects the indexed-field list and enforces `ORDER BY` field substitution/omission. `validate_bydbql` is parse-only, so `ORDER BY <non-indexed-field>` validates `true` but fails at execution.
7. Generate exactly one read-only BydbQL statement. When it contains `ORDER BY`, only sort by a field from the indexed-field list returned by `get_generate_bydbql_prompt`; substitute the closest indexed field, or omit `ORDER BY` when no indexed field matches.
8. Call `validate_bydbql` before execution.
9. Execute with `list_resources_bydbql` only when the user asks to run, query, fetch, show, or inspect raw BanyanDB data.
10. If `validate_bydbql` returns `valid=false`, fix the query and validate again before any execution.
11. If execution fails due to a missing group, resource, or schema, use `list_groups_schemas` to discover the correct names and retry only when the correction is clear.

## Tool Use

Use the existing BanyanDB MCP tools:

- `list_groups_schemas`: discover groups and resource names for streams, measures, traces, and properties.
- `get_generate_bydbql_prompt`: return the generation prompt text, including the live indexed-field list and the `ORDER BY` index-rule substitution/omission rules. It is the only source of index-rule `ORDER BY` safety.
- `validate_bydbql`: run read-only safety checks and parse-only BydbQL syntax validation. This does not verify group, resource, tag, field, or index-rule existence.
- `list_resources_bydbql`: execute a validated BydbQL statement when data retrieval is requested.

The host model performs natural language to BydbQL translation using these references and live schema discovery. Promote `get_generate_bydbql_prompt` to a required step whenever ranking or sorting is involved (or once schema has been fetched), because it carries the only index-rule `ORDER BY` check; `validate_bydbql` is parse-only and cannot catch a sort on a non-indexed field.

## Output

For generation-only BydbQL requests, return the single BydbQL statement and state that it was not executed.

For BydbQL execution requests, show the BydbQL statement, then summarize the returned data or error concisely.
