# BydbQL Syntax (condensed)

## Core

- Forms: `SELECT ... FROM ...` or `SHOW TOP N FROM MEASURE ...`
- Plan resource types: `STREAM`, `MEASURE`, `TRACE`, `PROPERTY`, and `TOPN`
- `FROM <TYPE> <name> IN <group>[, <group>...]` is required
- `TIME` is required for `STREAM`, `MEASURE`, `TRACE`, and `SHOW TOP`; `PROPERTY` does not use `TIME`
- Clause order for SELECT: `SELECT` -> `FROM` -> `TIME` -> `WHERE` -> `GROUP BY` -> `ORDER BY` -> `LIMIT`
- Clause order for SHOW TOP: `SHOW TOP` -> `FROM` -> `TIME` -> `WHERE` -> `AGGREGATE BY` -> `ORDER BY`

## TIME vs LIMIT

- "last 3 days", "past hour" -> `TIME > '-3d'`, `TIME > '-1h'` (time range, no LIMIT)
- "last 30 spans" -> `ORDER BY <index_rule> DESC LIMIT 30` (data points)
- When a number appears with a time unit (minutes, hours, days) -> TIME only
- When a number appears before/after a resource name as a count -> LIMIT (usually needs ORDER BY)

## TOPN vs SELECT

- Use a discovered `TOPN` schema when the goal asks for that registered ranking; the registered TopN aggregation name is rendered after `FROM MEASURE`
- `SHOW TOP` uses `ORDER BY DESC` or `ORDER BY ASC` without a field name
- Do not use LIMIT in SHOW TOP queries
- Never substitute a normal `MEASURE` schema for a missing `TOPN` schema
- TOPN filters support equality on registered `groupByTagNames` only

## ORDER BY

- SELECT plans use an exact `sortable_indexes.rule_name`, or `TIME`
- If no exact sortable rule supports the request, omit ORDER BY or ask one clarification; never substitute a close field
- TOPN queries: `ORDER BY DESC` or `ORDER BY ASC` only (no field name)
