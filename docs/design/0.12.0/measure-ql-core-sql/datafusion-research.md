# DataFusion research for Measure QL Catalog B

Research date: 2026-10-14. DataFusion source is pinned to
[`07077ff83ffe1d44f61ff7557a3b2d8b927220b3`](https://github.com/apache/datafusion/commit/07077ff83ffe1d44f61ff7557a3b2d8b927220b3),
resolved from upstream `main`. This is an architectural reference, not a new
runtime dependency. Recommendations below are BanyanDB design judgments; upstream
facts have their owning source linked next to them.

## Recommendation and boundaries

Adopt DataFusion's separation of expressions, relational plans, physical operators
and columnar execution for B1–B9. Implement that subset in Go on PR #1412, reusing
BanyanDB's existing vectorized batches and liaison execution. Keep existing
protobufs, RPCs, request/response encoding and data-node execution unchanged.
DataFusion itself is a Rust engine whose in-memory representation is Arrow.
Its scope includes substantially more SQL and execution machinery than Catalog B
needs. [DataFusion README](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/README.md)

The useful result is a small Go relational engine over existing measure queries,
not an attempted port of DataFusion. Defer joins, subqueries, windows, exchanges,
distributed scheduling, a cost-based optimizer, general function plugins and
spill infrastructure. B3 retains the prescribed split-native-request and keyed
merge path; residual field filters and computed grouping require a separate local
aggregation path.

## What to transfer

| DataFusion mechanism | Catalog B adaptation | Upstream source |
|---|---|---|
| Relational `LogicalPlan` nodes separate from `Expr` trees | Small `Scan`, `Filter`, `Aggregate`, `Project`, `Distinct`, `Sort`, `Limit`, `Empty` vocabulary; aliases are output metadata, not storage names | [LogicalPlan](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/expr/src/logical_plan/plan.rs), [Expr](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/expr/src/expr.rs) |
| Analyzer distinct from semantics-preserving optimizer | Resolve scope, aliases, ordinals and aggregate legality; determine types/nullability and insert explicit coercions before physical lowering | [Analyzer](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/optimizer/src/analyzer/mod.rs), [type coercion](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/optimizer/src/analyzer/type_coercion.rs) |
| `PhysicalExpr::evaluate(batch)` returns `ColumnarValue` | Compile typed column-index access and batch kernels once; preserve scalar literals, bound parameters and captured `now` without expanding them into every row | [PhysicalExpr](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/physical-expr-common/src/physical_expr.rs), [ColumnarValue](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/expr-common/src/columnar_value.rs) |
| Schema-bearing record-batch streams | A fixed output schema exists even for empty input; compose existing Go pull/fusible/breaker contracts | [RecordBatchStream](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/execution/src/stream.rs) |
| Per-expression filter capability | Make pushdown decisions inspectable and preserve residual conditions | [TableProvider](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/session/src/table.rs) |
| Dense group IDs and columnar accumulator state | Share group lookup across all local aggregates and use typed state arrays | [GroupsAccumulator](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/expr-common/src/groups_accumulator.rs) |
| DISTINCT lowered to grouping | Reuse group-key encoding/equality for visible output tuples | [ReplaceDistinctWithAggregate](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/optimizer/src/replace_distinct_aggregate.rs) |
| Memory reservations and bounded top-k | Reserve retained state before growth; choose top-k when a finite final limit is available | [MemoryReservation](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/execution/src/memory_pool/mod.rs), [SortExec](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/physical-plan/src/sorts/sort.rs) |

### Expressions and planning

DataFusion's analyzer establishes executable meaning, while its optimizer must
preserve that meaning. For BanyanDB, keep PR #1412's prepare/bind and schema-source
boundaries, then use one typed expression tree for WHERE, HAVING, SELECT, grouping
and sorting. Parameter values and one execution-time `now` remain per execution;
schema-bound expressions must not mutate cached parse templates.
[Analyzer contract](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/optimizer/src/analyzer/mod.rs)

Implement only necessary normalization: expand stars, resolve aliases/ordinals,
collect unique aggregate calls, insert casts, prune unused columns, classify
predicates, choose native versus local aggregation, and fuse final sort/limit.
Use stable internal slot IDs rather than display-name lookup. Arithmetic and
comparisons require explicit BanyanDB numeric/null rules; borrowing the architecture
does not adopt every DataFusion coercion or function.

The scalar/array split directly helps B1 and B7: `latency > CAST(? AS DOUBLE)` needs
one scalar conversion and an array comparison. CASE and COALESCE must evaluate
only selected or still-unresolved rows. DataFusion's CASE implementation uses
filtered remainder batches and branch results; this provides a concrete model
for preserving short-circuit errors while processing columns.
[ColumnarValue](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/expr-common/src/columnar_value.rs),
[CASE evaluation](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/physical-expr/src/expressions/case.rs)

### Pushdown is a correctness contract

DataFusion classifies each offered filter as `Exact`, `Inexact` or `Unsupported`.
An inexact filter may reduce scan work but requires evaluation above the scan;
its provider documentation explicitly forbids pushing a limit below that residual
work. Projection must also preserve columns needed for residual evaluation.
[Provider filter/limit contract](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/session/src/table.rs)

For B, exact native tag/time predicates can be removed from liaison WHERE only
after equivalence is established. A safe time envelope is inexact when it covers
an original disjunction or leaves boundary checks. Field predicates remain local.
Do not push one arm of an OR as a mandatory native condition. Neither aggregate
pushdown nor final LIMIT is legal below a residual WHERE. Keep complete native
inputs for grouping, DISTINCT, merge and ordering, including more than 100 rows.

For example, `WHERE service = 'api' AND latency > 2.5` permits the service tag
predicate on the native request, but requires raw latency values and a liaison
filter before aggregation. A native SUM over all service rows cannot be repaired
by filtering its result.

### Aggregation, DISTINCT and ordering

DataFusion assigns each group a dense integer and passes batch values plus these
indices to grouped accumulators. State is stored per group; AVG can expose SUM
and COUNT state separately. [Grouped accumulator contract](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/expr-common/src/groups_accumulator.rs)

Use this for the B8/B9 local path: evaluate computed keys once, map tuples to group
IDs once, and update all required aggregates against the same filtered batch.
Retain explicit null/empty state. B3's native path instead aligns finalized branch
results by complete grouping tuple and bucket timestamp; it must not reinterpret
arbitrary native results as mergeable partial states. QL AVG uses verified
SUM/COUNT inputs and float finalization. Independent native requests still lack a
shared storage snapshot.

DataFusion rewrites `SELECT DISTINCT a, b` to an aggregate grouped on `a, b` with
no aggregate expressions. Reuse that idea for B6 without including hidden sort
columns in equality. Share key encoding and null equality with B9, while retaining
different visible schemas. [DISTINCT rewrite](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/optimizer/src/replace_distinct_aggregate.rs)

DataFusion distinguishes general sorting and top-k when a fetch bound is known.
B5 can use one typed lexicographic comparator for both paths, including per-key
direction/null placement. Retain `OFFSET + LIMIT` candidates after checked
addition, then apply offset. Evaluate keys once per batch; ensure top-k does not
pin entire source batches just to retain a few values. Full sorting remains
budgeted; DataFusion's spilling and dynamic-filter machinery are separate scope.
[SortExec](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/physical-plan/src/sorts/sort.rs),
[TopK](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/physical-plan/src/topk/mod.rs)

## Arrow style versus Arrow interchange

Arrow specifies homogeneous arrays, schema, length, null state and physical
buffers. Its validity bit is one for a non-null value; variable-width binary and
UTF-8 normally use offsets and a data buffer.
[Arrow format](https://arrow.apache.org/docs/format/Columnar.html)

BanyanDB already has a useful columnar carrier: `vectorized.RecordBatch`, typed
columns, selection vectors, constant columns and explicit owners. Its bitmap
uses one for null, strings use Go slices, and selection/constant representations
are project-specific. It is Arrow style, not an Arrow SDK record batch or proof
of IPC/C Data Interface compatibility.
[Batch](../../../../pkg/query/vectorized/batch.go),
[column/null representation](../../../../pkg/query/vectorized/column.go),
[typed slices](../../../../pkg/query/vectorized/typed_column.go),
[constant columns](../../../../pkg/query/vectorized/const_column.go)

Reuse that carrier for B with a QL schema/slot layer. Adapt existing native results
once at the liaison boundary; do not create protobuf cells between every operator.
Honor `ActiveLen`, distinguish nil from empty selections, and retain owners or copy
values before an input batch is released. Internal boolean/timestamp semantics
need explicit representation even where existing physical column types are reused.
The final adapter still emits the existing `measure_result`.

Arrow Go is an alternative if future interoperability is required: it supplies
schema-bearing `RecordBatch`, typed arrays, reference counting and compute APIs.
Adopting it would require conversion, allocator integration and kernel-semantic
validation. For B, a second batch representation brings little benefit over the
existing carrier. [Arrow Go RecordBatch](https://pkg.go.dev/github.com/apache/arrow-go/v18/arrow#RecordBatch),
[compute](https://pkg.go.dev/github.com/apache/arrow-go/v18/arrow/compute)

Embedding DataFusion is a larger alternative. Its FFI explicitly targets Rust
libraries and recommends a Rust wrapper for other languages; it is not an
off-the-shelf Go engine API. A Go integration would need cgo/build distribution,
cross-runtime cancellation, ownership and error translation, plus a bridge for
BanyanDB's dialect and native requests. Arrow interoperability alone does not
solve those requirements. [DataFusion FFI design](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/ffi/README.md)

## Lifecycle, verification and implementation priority

DataFusion requires execution streams to stop work after errors, release resources
when dropped and remain responsive to cancellation. Map this onto existing Go
operator `Close`, context checks and query ownership; do not introduce a Rust-like
scheduler. [ExecutionPlan lifecycle](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/physical-plan/src/execution_plan.rs),
[existing Go operators](../../../../pkg/query/vectorized/operator.go)

Its `MemoryReservation` supports fallible growth and releases reserved bytes on
drop. Reuse the principle through existing BanyanDB admission/tracking: charge
retained native frames/batches, computed columns, groups, distinct keys, retained sort
values and final output. Fail explicitly when reservations cannot grow; never
substitute silent truncation. [Memory reservations](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/datafusion/execution/src/memory_pool/mod.rs)

DataFusion prefers SQL result tests for expressible SQL behavior. Borrow that
test organization and selected independently authored edge cases; retain native
query equivalence as B3's oracle rather than treating all DataFusion semantics as
authoritative for BanyanDB. Add plan assertions for residual filter placement,
hidden columns and limit legality. Vary batch sizes/selections, reorder aggregate
branches, and verify nulls, short-circuit errors, cancellation and owner release.
[DataFusion testing guide](https://github.com/apache/datafusion/blob/07077ff83ffe1d44f61ff7557a3b2d8b927220b3/docs/source/contributor-guide/testing.md)

Build the typed plan and batch adapter alongside B1–B2, then B3 keyed merge. Add
the shared batch expression evaluator for B7, use it for B4/B5/B6, and finish the
raw-input local aggregate path needed by B8/B9. This makes every adopted mechanism
serve a Catalog B acceptance case instead of building a general SQL framework.
