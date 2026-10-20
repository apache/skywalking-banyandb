# Measure QL: Remaining Gaps and Order of Work

Status: **design** — not implemented.

This document lists what the measure query language still lacks compared with InfluxDB 3.0 SQL,
and the order in which to close those gaps.

Two things are deliberately absent from the list:

- **Time bucketing, tag aggregation and `COUNT(DISTINCT tag)`** are already supported on `main`
  (#1365, #1366): `GROUP BY TIME_BUCKET(...)`, aggregates over a tag, and `COUNT(DISTINCT ...)`.
- **Window functions and gap filling** (`OVER`, `lag`, `rank`, `date_bin_gapfill`, `locf`,
  `interpolate`) are out of scope.

SQL data writes (`INSERT`/`UPDATE`/`DELETE`) are not listed either; ingestion stays on the write
APIs. Schema definition (DDL) **is** in scope (group S), even though InfluxDB 3.0 manages schema
outside SQL: every BanyanDB schema object already has a registry service, so DDL is mostly a
language over existing RPCs.

## Remaining gaps

| # | Missing piece | What it needs |
|---|---|---|
| **B. Core SQL semantics** |||
| B1 | Time as a normal predicate: `WHERE time >= now() - INTERVAL '1h'` | Grammar only |
| B2 | `AS` aliases | Grammar, plus renaming on the liaison |
| B3 | More than one aggregate per query | The request carries only one aggregate; the liaison must merge several requests |
| B4 | Real `HAVING` (filter after aggregation). The current `HAVING` means "tag array contains" and needs a new name | Operator on the liaison |
| B5 | `ORDER BY` any column, alias or aggregate; several sort keys; `NULLS FIRST/LAST` | Sort on the liaison (today ordering requires an index rule) |
| B6 | `DISTINCT` | Operator on the liaison |
| B7 | Expressions: arithmetic, `CASE`, `COALESCE`, `CAST`/`::type` | Expression evaluator on the liaison |
| B8 | Filtering on **fields**, and float literals | Filter on the liaison first; later push it down to data nodes |
| B9 | `GROUP BY` an expression or column number (`GROUP BY 1`) | Grammar and planner |
| **C. Functions** |||
| C1 | Scalar functions: math, string, regex, date/time (`date_trunc`, `extract`, `tz`) | Function registry |
| C2 | Statistical and approximate aggregates (`stddev`, `percentile`, `approx_distinct`) | Engine or liaison |
| C3 | Selectors returning a value with its timestamp (`selector_max(v, time)`) | Liaison, plus struct-valued results |
| **D. Combining queries** |||
| D1 | Subqueries and CTEs (`WITH`) | Relational planner |
| D2 | `UNION` / `INTERSECT` / `EXCEPT` | Relational planner |
| D3 | `JOIN` across measures | Relational planner, plus hash join |
| **S. Schema definition (DDL)** |||
| S1 | `SHOW GROUPS`, `SHOW MEASURES IN g`, `DESCRIBE MEASURE g.m` (and the same for every kind) | Grammar, mapped to the registry `Get`/`List` RPCs |
| S2 | `CREATE GROUP` / `ALTER GROUP` / `DROP GROUP`, with catalog, shard count, segment interval, TTL and lifecycle stages | Grammar, mapped to `GroupRegistryService` |
| S3 | `CREATE MEASURE` / `ALTER MEASURE` / `DROP MEASURE`: tag families, fields (type, encoding, compression), `ENTITY`, `SHARDING KEY`, `INTERVAL`, index mode | Grammar, mapped to `MeasureRegistryService` |
| S4 | The same for `STREAM`, `TRACE` and `PROPERTY` | Grammar, mapped to their registry services |
| S5 | `CREATE INDEX ... ON MEASURE g.m (tag) USING INVERTED \| SKIPPING \| TREE`, with analyzer and `no_sort`; `DROP INDEX` | One statement creates both the index rule and its binding |
| S6 | `CREATE TOPN` / `DROP TOPN`: source measure, field, sort, group-by tags, filter, counter and LRU sizes | Grammar, mapped to `TopNAggregationRegistryService` |
| S7 | Shared behavior: `IF [NOT] EXISTS`; `DROP GROUP ... DRY RUN`, which lists what would be deleted; return the new schema revision; optionally wait until the change has reached every node | Grammar, plus the existing dry-run and revision-barrier support |

## Design first: the foundation

Apart from B1 and B2, every remaining gap needs operators that run on the liaison, so the
foundation comes before any feature work:

- **Language surface.** A SQL dialect with `FROM group.measure`, a `time` column, `?` parameters,
  and results returned as a table (columns and rows).
- **Plan layer.** Parse SQL into a tree of logical operators. Whatever the engine can already do
  goes down to the measure scan — today's `QueryRequest`, which now includes time buckets, tag
  aggregation and count-distinct. Everything else runs as operators on the liaison. The plan is
  compiled once, cached and reused, as described in the
  [plan-cache design](../bydbql-plan-cache/README.md).

## Next move: B1–B3

These prove the foundation works end to end:

1. **B1 and B2** (time predicate, aliases). They need no new operator, so they test the new grammar
   and the planner's pushdown on their own.
2. **B3** (several aggregates). This is the first query the planner must split into more than one
   scan and merge on the liaison, so it exercises the liaison-operator path.

## After that

3. **B4, B5, B6, B7, B9** — post-aggregation operators and expressions. Build **C1** alongside B7,
   since both need the same function and expression evaluation.
4. **B8** — field filters and float literals. Filter on the liaison first, then push the filter down
   to data nodes, which is an engine change.
5. **C2, C3** — added as they are needed.
6. **D** — D1, then D2, then D3. The liaison needs memory limits before these land, because joins
   and set operations hold state.

## DDL track (S): in parallel with the query work

DDL does not need the plan layer. Each statement maps to one registry call (or two, for
`CREATE INDEX`), so it can proceed alongside the query work:

1. **S1** first. It is read-only, so it needs only `schema:read` and no new safety work.
2. **Per-statement authorization and safety.** Do this before any statement that changes schema.
   Today the BydbQL `Query` RPC is authorized once, as `data:read`
   (`banyand/liaison/grpc/methodpolicy.go:275`). DDL needs `schema:write`, so permission must be
   decided per statement after parsing, or DDL gets its own RPC. Read-only clients must also keep
   rejecting DDL: the bydbctl/MCP guard `IsReadOnly`
   (`bydbctl/internal/tui/bydbql/read_only.go`) currently does this by keyword matching and should
   decide from the parsed statement instead.
3. **S2, then S3.** `ALTER` is a read-modify-write through the registry's `Update` RPC. It must
   reject the write if the schema changed in the meantime (compare revisions), and it stays subject
   to the registry's existing compatibility checks — for example, on changing a tag's type.
4. **S5, S6, S4.**
5. **S7** — build `IF [NOT] EXISTS` and the returned revision in from the start; add dry-run and
   wait-for-propagation last.
