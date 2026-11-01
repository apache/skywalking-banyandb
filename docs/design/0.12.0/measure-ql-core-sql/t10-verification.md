# T10 typed SELECT expressions and masked conditionals

This is a historical ticket-stage verification record. Current isolated PR 3
checks are recorded separately in the [PR 3 verification record](pr3-verification.md).

Implementation base: `6ed1591c` (T08), on PR #1412. Verification uses Go 1.26.9.
The implementation is confined to QL parsing, binding, liaison execution and
verification. It changes no protobuf, native request/operator, data-node code,
frame codec, persisted format or memory-protector policy. Spilling remains deferred.
The frozen protocol is `.omx/t10-frozen-protocol.json`; source hashes are in
`.omx/t10-source-manifest.sha256`.

## Execution contract

Raw measure SELECT now accepts arithmetic, remainder, unary signs, float literals,
explicit casts, comparisons, SQL boolean/null operations, IN, searched/simple CASE
and COALESCE. The [language guide](../../../interacting/bydbql.md#59-typed-select-expressions)
records the syntax, numeric rules, timestamp nanosecond unit, cast pairs,
parameter ordering and existing response mapping. New expression syntax applies
only to measure SELECT. Field WHERE/HAVING, aggregation expressions, computed
grouping and computed-output ordering remain later tickets.

The parser first retains accepted legacy syntax, including hyphen/star column
names and ::TAG/::FIELD. A projection-only parser handles new expressions and
leaves source, time, predicates, stages and dispatch with the existing grammar.
Ordinary quoted column resolution remains compatible. Boolean literal promotion
is measure-specific, preserving stream/trace/property columns named TRUE/FALSE.

Compiled cache entries retain immutable schema-resolved recipes with symbolic
parameters. Every bind produces fresh typed expression trees. Runtime construction
resolves source offsets and typed kernels once; active-row loops do not interpret
the AST or perform schema-name lookups. Numeric/string/bytes constants use the
existing ConstColumn. Boolean scratch stays local to QL without a new native
column type. Native protobuf passthrough cells are decoded only at the expression
boundary and only for eligible rows. Owned scratch and borrowed input slices
remain separate, including when batch representations alternate.

The pipeline is native raw scan, complete time residual, expression projection,
final OFFSET/LIMIT, then the existing response adapter. It preserves physical
row selection, timestamp/SID/version metadata and source ownership while hiding
input-only columns. CASE/COALESCE and AND/OR pass masks to children. Unreachable
invalid casts/divisions are not evaluated, although all branches are type-checked.
Native batches already fit the 65,536-row physical index domain; an oversized
external batch is rejected before any uint16 index conversion.

Integer overflow, division/remainder by zero and invalid casts return
InvalidArgument. Floating comparisons match T07's NaN/signed-zero ordering.
Timestamps remain signed int64 UTC Unix nanoseconds until final timestamp
TagValue conversion. Direct boolean or untyped-null output requires a supported
cast. Computed arrays use existing TagValues; other computed scalar values use
existing FieldValues. There is no intermediate DataPoint representation.

## Resource bounds

Expression metadata, cloned literal payloads, compiled nodes, value/null buffers,
row masks and generated string payloads reserve the shared query tracker before
allocation. Buffer growth accounts for old/new overlap. Buffers are reused by
the operator; no new memory pool is introduced. Cancellation, arithmetic errors,
budget rejection and idempotent Close release expression reservations and close
the source exactly once. Existing native/final-response accounting remains in use.

Public WrapExpressions and WrapRaw tests use 65,536 rows at actual 16-MiB and
256-MiB budgets. Two arithmetic outputs fit 16 MiB; 32 independent outputs reject
partial buffer growth at 16 MiB and validate all 2,097,152 values at 256 MiB.
Successful Close, rejection and cancellation after a successful batch return
tracked live bytes to zero. A separate small-budget test proves large cloned
literal payloads are reserved before copying. These tests do not replace or relax
the native aggregation memory blockers recorded for T07.

Constant arrays have an explicit compatibility cost. They remain scalar during
CASE/COALESCE evaluation, but the final expression projection allocates row
headers before final pagination because the unchanged native serializer has no
array ConstColumn. Payloads are shared rather than copied per row. A test checks
65,536 headers over one immutable 8,192-int payload, one counted full column,
charged headers/payload and less than 4 MiB of live expression memory. Thus the
zero-full-column scalar-constant benchmark gate does not establish zero header
allocation for array output. The counters expose this exception.

## Generated real-RPC coverage

The generator adds one versioned six-row fixture, 25 scenarios with three
different bound windows each, 21 rejection statements and one QL-only readiness
query. Expected values use an independent scalar fixture evaluator, including
math/big overflow checks, rather than the production expression engine or a
native Query RPC. Successful calls run cold/warm with changed bindings and with
compiled caching bypassed. Earlier generated case contracts remain unchanged.

The 46 new specs cover arithmetic precedence/remainder/division, decimal/exponent
floats, parameter casts, SQL three-valued logic, null/empty IN, arrays, timestamps
and typed null output, default names/aliases, legacy hyphen names, nested masks,
unreachable error branches, disjoint time residuals, pagination and LIMIT 0.
Failures check the RPC status, source/resource cleanup and successful recovery.
Custom projection grammar files are included in the generator's grammar audit.
Star-containing names retain parser compatibility tests; native FieldSpec DDL
does not accept a star, so the real fixture does not relax that existing rule.

Both final Docker topology runs pass **292 selected specs**, including **46 T10**
and 246 prior-ticket specs. Reports are `.omx/t10-standalone-final.json` and
`.omx/t10-distributed-final.json`. Suite lifecycle/report nodes are excluded from
these counts. Both containers enforce 4 CPUs, 8 GiB and equal memory/swap caps.

## Focused benchmark protocol

K(T10), `BenchmarkExpressions`, compares compiled evaluation with an independent
allocation-free recursive row interpreter. Both reuse result buffers and construct
typed result descriptors/null bitmaps. Setup, compilation, complete result checks
and structural assertions are outside timing. Constants can remain scalar in
the compiled path; the row interpreter evaluates each row. No padding or
artificial work is added to the reference.

The fixed 54 variants are nine shapes, vector/scalar implementations and physical
sizes 1/1,024/65,536. Shapes cover scalar constants, dense/selected arithmetic,
string-parameter casts with selected nulls, COALESCE, CASE first-arm hit rates
0/50/100%, and nested CASE/COALESCE with selected nulls. Selected variants use
half the physical rows; their metric divides by active rows. Five one-second
samples run in one 4-CPU/8-GiB Docker container. The speed gate compares medians
at physical sizes at least 1,024; one-row results disclose setup/dispatch cost.
Constants must allocate no full value column, and 100% first-arm hits must
execute zero later-arm rows.

P(T10), `qlb_t10`, measures only the owning projection feature. Five variants are
raw column control, constant double, integer arithmetic, string-parameter double
cast plus arithmetic, and masked CASE/COALESCE. It reuses the existing nullable,
versioned fixture over 16 entity hosts at 1,024 and 32,768 rows. Every variant
evaluates and returns all rows. The raw control measures expression overhead;
it is not a semantically equivalent old-version implementation of new syntax.

The fixed schedule is 100 timed/5 warmup queries at 1,024 rows and 20 timed/3
warmups at 32,768, one worker and a 256-MiB query budget. Five isolated containers
start a fresh process/cluster for each cardinality, with CPU/heap profiles per
variant. Warmups and six retained timed samples receive full independent
timestamp-keyed value/null/checksum checks outside timing. Every timed call
checks result count. Raw latency samples, allocated bytes, process CPU and
work/ownership counters are retained. All warm requests must hit the plan cache.

Each measured query requires one QL batch execution, two distributed leaf calls,
complete expression input/output counts and no aggregate/merge or intermediate
DataPoints. Constants require zero full value columns. Post-close query/native
reservations, result handles, owner pins and flatten-pool references must restore
baseline. Logical source/frame rows are not physical storage-read counts.
Total parent lease charges and native retained frame bytes remain unavailable;
the report labels them explicitly. No previous ticket benchmark matrix or
retrospective T02 acceptance is rerun or inferred.

K(T10) completes all 54 variants with five samples each. All 18 comparisons at
physical sizes at least 1,024 pass the median ns/active-row gate. Raw timings
are `.omx/t10-kernel-docker-final.log`; complete medians, allocations and sample
values are `.omx/t10-kernel-median.json`. The container records a 400,000/100,000
CPU quota and an 8,589,934,592-byte memory cap. No other test/benchmark ran during
these measurements. Production, benchmark and generated-contract source hashes
remain identical to the frozen manifest.

| Expression | 1,024 vector ns/active row | 1,024 scalar | 65,536 vector | 65,536 scalar |
| --- | ---: | ---: | ---: | ---: |
| Constant double | 0.09165 | 51.47 | 0.00152 | 51.35 |
| Dense arithmetic | 11.92 | 55.47 | 11.85 | 55.51 |
| Selected arithmetic | 13.10 | 56.42 | 12.92 | 56.93 |
| Selected parameter cast with nulls | 19.69 | 89.20 | 19.40 | 89.44 |
| COALESCE with nulls | 15.66 | 35.15 | 15.45 | 35.13 |
| CASE first-arm hit 0% | 30.13 | 128.70 | 29.52 | 129.00 |
| CASE first-arm hit 50% | 28.37 | 105.80 | 28.33 | 107.10 |
| CASE first-arm hit 100% | 16.97 | 84.13 | 16.54 | 85.26 |
| Nested selected CASE/COALESCE with nulls | 43.73 | 144.30 | 42.77 | 145.00 |

Nonconstant compiled expressions are about 2.2–5.2 times faster than this row
interpreter at the gated sizes. A constant's per-row number divides one scalar
operation by the active-row count; it does not describe a sub-nanosecond row
loop. Constant output allocates one 80-byte descriptor per pull, with no full
value column. At 65,536 rows, the CASE later-arm counts are exactly 65,536,
32,768 and zero for the 0/50/100% first-arm cohorts. The 100% cohort takes
1.084 ms/batch versus 1.935 ms at 0%, while preserving all output checks.

One-row nonconstant arithmetic/cast/CASE shapes are 1.25–1.44 times slower than
the row reference: roughly 158–231 ns rather than 110–177 ns. Fixed operator and
kernel dispatch outweigh vector work at that size. COALESCE and constants are
faster even at one row. The performance claim is confined to the specified
kernel batches/reference, rather than a universal small-query or RPC speedup.

Most reused-buffer pulls allocate one 80/96-byte descriptor. The selected nullable
parameter cohort allocates about 25.3 KiB and 13 objects per 65,536-physical-row
batch in both implementations, including final typed-column null bitmap growth.
The reference and compiled path pay the same result-container cost; this is not
reported as zero-allocation vector execution.

P(T10) completes five isolated repetitions at each cardinality: **50 measured
variants**, with all independent value checks, work/resource assertions and
**100 nonempty CPU/heap profiles** passing. Raw samples/reports are under
`.omx/bench-reports/t10-r1` through `t10-r5`; `.omx/t10-rpc-summary.json` retains
per-run measurements, counter values and medians. Each cardinality has a fresh
cluster process. No other test or benchmark ran alongside these timings.

The table reports medians of five per-run results. MB is decimal allocated bytes
per query, including client, liaison and two data nodes in their shared process.

| Input/output rows | Projection | p50 ms | p95 ms | Allocated MB/query | Process CPU ms/query |
| --- | --- | ---: | ---: | ---: | ---: |
| 1,024 | Raw control | 5.058 | 7.746 | 2.286 | 10.6 |
| 1,024 | Constant double | 5.009 | 7.787 | 2.327 | 10.3 |
| 1,024 | Integer arithmetic | 5.132 | 7.167 | 2.273 | 10.8 |
| 1,024 | Parameter cast + arithmetic | 5.165 | 7.202 | 2.296 | 10.6 |
| 1,024 | Masked CASE/COALESCE | 5.224 | 7.698 | 2.371 | 11.1 |
| 32,768 | Raw control | 93.537 | 105.572 | 67.690 | 165.5 |
| 32,768 | Constant double | 93.910 | 103.432 | 71.715 | 162.0 |
| 32,768 | Integer arithmetic | 93.643 | 103.139 | 67.658 | 165.5 |
| 32,768 | Parameter cast + arithmetic | 94.091 | 105.420 | 67.820 | 164.5 |
| 32,768 | Masked CASE/COALESCE | 94.482 | 105.280 | 68.604 | 164.0 |

The median paired p50 ratios against raw control range from 0.990 to 1.022 at
1,024 rows and 1.001 to 1.011 at 32,768. Across individual runs, non-control ratios
range from 0.945 to 1.089 and 0.968 to 1.059, respectively. These measurements
show modest expression overhead relative to complete scan/response work; they do
not establish a universal RPC speedup or an old-version compatibility verdict.
Twenty large-query samples per run also do not establish a precise tail-latency
acceptance interval. Raw p99 and individual latency samples remain in the reports.

All variants record two leaf calls and 2N logical native source rows per query,
deduplicated into N output rows. Expression input/output counts are exactly N,
plan-cache hits equal timed calls and timed compiles are zero. Scalar constants
allocate zero full value columns. Expression reservations equal releases, and
all observed post-close live reservation/result/owner/pool deltas are zero.
Recorded expression buffer allocation per query is about 10.5 KB for arithmetic,
21.3 KB for parameter arithmetic and 34.8 KB for CASE/COALESCE at both input sizes,
showing reuse across native batches. This excludes native/final-response buffers.

The representative 32,768-row CASE CPU profile has GC scanning, protobuf
decoding/serialization and allocation among its largest entries
(`.omx/t10-rpc-cpu-top.log`), consistent with the small total-RPC differences.
CPU profiles cover their measured interval; heap profiles are capture points
with cumulative allocation history within the cardinality's process. Per-query
allocation figures come from before/after runtime snapshots, not cumulative heap
profile totals. No claim of zero allocations or pooling benefits is made.

## Validation evidence

- Public expression Compile/Bind RED: `.omx/t10-parser-red.log`.
- Pipeline ordering/timestamp RED and GREEN: `.omx/t10-projection-red.log`,
  `.omx/t10-projection-timestamp-red.log`, `.omx/t10-projection-green.log`.
- Borrowed-source and non-measure parsing regressions:
  `.omx/t10-source-readonly-red.log`, `.omx/t10-nonmeasure-literal-red.log`,
  `.omx/t10-nonmeasure-literal-green.log`.
- Full normal planner/operator/liaison/generator/querybench packages:
  `.omx/t10-normal-final.log`.
- Full instrumented planner/operator/liaison race suites:
  `.omx/t10-race-final.log`; generator and focused querybench race suites:
  `.omx/t10-generator-final-race.log`, `.omx/t10-rpc-bench-race.log`.
- Additional actual-budget/cancellation/array resource race and lint:
  `.omx/t10-expression-resource-race.log`, `.omx/t10-expression-resource-lint.log`.
- Existing CLI/MCP parser consumers: `.omx/t10-parser-consumers-final.log`.
- Repository `make lint`: `.omx/t10-make-lint.log`.
- Deterministic generated-case check and license target:
  `.omx/t10-generated-license-final.log`. The license tool reports zero scanned
  files in this worktree; changed Go headers were also checked directly.

The post-commit license/format command is `make check`, with output retained in
`.omx/t10-make-check.log`. It includes index renormalization and a clean-tree
requirement, so it runs after committing the implementation and documentation.
