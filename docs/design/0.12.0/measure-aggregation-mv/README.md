# Aggregating Measures and Materialized Views

Status: **design** — not implemented. Outline only; the detailed design comes later.

This document outlines how to bring ClickHouse-style pre-aggregation to BanyanDB: measures that
combine rows as they merge, and materialized views that feed them from other measures at write
time. It covers the DDL and the query language, and it builds on the new QL described in
[Measure QL: remaining gaps and order of work](../measure-ql-gaps/README.md).

**How dependencies are marked.** Every part lists what it needs:

- **QL: B3, S3, ...** — items from the gaps document. They must land first.
- **Engine** — storage or query-engine work that is not part of the QL.
- **Not in the gaps list** — language work this design needs that the gaps document does not cover.

## 1. What we take from ClickHouse

| ClickHouse | BanyanDB equivalent | Adopted |
|---|---|---|
| AggregatingMergeTree | **Aggregating measure**: rows with the same key are combined when parts merge | Yes |
| SimpleAggregateFunction (`sum`, `min`, `max`) | **Simple aggregate field**: the stored value is already the result | Yes |
| AggregateFunction (`avg`, `uniq`, quantiles) | **State field**: the stored value is an intermediate state | Yes |
| `-State` / `-Merge` / `-MergeState` combinators | Functions that write a state, read a state, and combine states | Yes |
| Incremental materialized view (`TO` target) | **Materialized view** from a source measure into an aggregating measure | Yes |
| Null engine as the source | A source measure whose raw rows are not kept | Later |
| Projections (chosen automatically by the optimizer) | — | No |
| `ROLLUP` / `CUBE` / `GROUPING SETS` | — | No |
| `FINAL` | — | No: reads always merge states |
| Refreshable views, joins inside a view, Summing/Collapsing engines | — | No |

## 2. Starting point in BanyanDB

- **Merges deduplicate today.** When rows collide during a merge, the latest version wins. The
  aggregating measure adds a second merge behavior: combine the rows instead of keeping one.
- **TopN is already a materialized view**, but a hard-coded one: a write-path streaming processor
  maintains a hidden result measure. The generic materialized view generalizes that processor.
  TopN can later be redefined as one.
- **Partial aggregation already exists.** Data nodes return partial aggregates and the liaison
  finishes them. Reading state fields reuses this.

## 3. DDL

### 3.1 Aggregating measure

`CREATE MEASURE` gains a way to declare the measure as aggregating, and each field declares how it
combines: a simple aggregate or a state. The key is the entity plus the time bucket.

- **QL:** S3 (`CREATE`/`ALTER MEASURE`), S7 (`IF NOT EXISTS`, returned revision), S1 (`DESCRIBE`
  shows how each field combines).
- **Engine:** combining rows at merge time; storing state values; state types and their encoding.

### 3.2 Materialized view

A new schema object: `CREATE MATERIALIZED VIEW name TO target AS SELECT ... FROM source GROUP BY ...`,
plus `DROP`, `SHOW` and `DESCRIBE`. The view runs on the write path against each incoming batch. It
does not fill in history when created; history needs an explicit backfill.

- **QL:** the view body is a SELECT in the new QL, restricted to what can run on each incoming
  batch: B1 (time as a predicate), B2 (aliases), B3 (several aggregates), time buckets and
  count-distinct (already on `main`). No subqueries or joins (D), and no `ORDER BY` or `LIMIT`.
- **QL:** S1, S3 (the target must already exist), S7.
- **Not in the gaps list:** a new schema kind with its own registry service, and a backfill
  statement (`INSERT INTO target SELECT ...` or a dedicated `BACKFILL` command). The gaps document
  excludes `INSERT`, so this needs a decision.
- **Engine:** a generic write-path view processor, generalized from the TopN processor.

## 4. Query language

- **Simple aggregate fields** are read with ordinary aggregates (`sum`, `max`, ...) plus
  `GROUP BY`. **QL:** B3.
- **State fields** are read with merge functions, and a query never sees a raw, unmerged state.
  **QL:** B7 (function-call syntax), C2 (the aggregate families that need states: approximate
  distinct, quantiles). **Engine:** merging states across parts and nodes at read time, reusing
  the existing partial-aggregation path.
- **Combining states without finishing them** is needed only for multi-tier rollups, where hourly
  states are combined into daily states. **QL:** C2. **Not in the gaps list:** the combinator itself.
- **Views are not used automatically.** A query reads the view's target by name. Rewriting queries
  onto a view transparently is out of scope.

## 5. Order of work

| Phase | Deliverable | Depends on |
|---|---|---|
| 0 | New QL foundation, plus S1, S3, S7, B2, B3 | Gaps document |
| 1 | Aggregating measure with simple aggregate fields (`sum`, `min`, `max`, `count`) | Phase 0 · Engine: combine on merge |
| 2 | Materialized view into an aggregating measure, simple fields only | Phase 1 · B1 · new view schema kind · Engine: view processor |
| 3 | State fields: `avg`, count-distinct, then approximate distinct and quantiles | Phase 1 · B7, C2 · Engine: state storage and read-time merge |
| 4 | Multi-tier rollups, backfill, TopN redefined as a view | Phases 2 and 3 · backfill statement · state combinator |

## 6. Open questions

Recommended answers, pending review:

1. **Backfill:** a dedicated `BACKFILL <view> TIME ...` command rather than `INSERT INTO ... SELECT`.
   It names exactly one operation, and a general `INSERT ... SELECT` invites requests for
   everything SQL allows there.
2. **Sources whose raw rows are not stored** (like ClickHouse's Null engine): not in the first
   release. They are a storage feature, not a QL one, and can be added later without changing the
   view DDL.
3. **Moving TopN onto views:** keep both at first. Retire the existing TopN aggregation only once
   views reproduce its results and cost.
