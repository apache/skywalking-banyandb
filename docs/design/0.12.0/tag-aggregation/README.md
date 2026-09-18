# Tag Aggregation and Time Bucketing in the Measure Query Engine

Status: **design** — not implemented.

This document specifies three additions to BanyanDB's existing measure query engine:

1. aggregation over **tags**, not just fields;
2. an exact **`COUNT_DISTINCT`** function;
3. **time-bucket grouping** — `GROUP BY <bucket>, <tags>` producing a series, with the bucket
   width taken from the request or, failing that, from the measure's `interval`.

They are separable features that share one API surface and one operator, and a dashboard needs
all three: (1) and (2) answer "how many distinct things", (3) answers "over time".

(3) is driven by a separate requirement — converting analytical `GROUP BY <bucket>, <tags>` SQL
onto BanyanDB (§1.1) — not by the cardinality work. That conversion also needs two things this
document scopes out, multiple aggregates per request and result aliases; §3 records the gap.

It is a companion to the [native inverted-index design](../native-inverted-index/README.md): the
scoping work for that package asked whether the native index should grow its own local
aggregation, and concluded it should not. Aggregation stays in the query engine, which already
reduces locally on data nodes and combines at the liaison; this design extends that machinery
rather than duplicating it.

---

## 1. Motivation

Dashboards built on SkyWalking-shaped measures routinely need cardinality answers where the
interesting dimension is a **tag**, not a field. Take a per-api-key request-rate measure: the
user is the `service_id` tag, the api-key is the `entity_id` tag, and the only field is a scalar
request count. Three questions follow immediately.

| Case | Question | SQL shape |
|---|---|---|
| **Scalar** | "how many distinct api-keys did one user use last week?" | `COUNT(DISTINCT entity_id) WHERE service_id = …` over a multi-bucket window |
| **Ranked** | "show every user, ranked by key count, page 3" | `GROUP BY service_id`, `ORDER BY COUNT(DISTINCT entity_id) DESC`, `LIMIT`/`OFFSET` |
| **Series** | "plot distinct api-keys per 5 minutes for the last hour" | `GROUP BY <time bucket>, service_id ORDER BY <time bucket> ASC` |

The ranked case is what a dashboard row needs; the series case is what a dashboard *chart* needs.
Every realistic dashboard window spans many buckets, so the scalar case is never asked over a
single bucket either.

The series case is the shape analytical SQL expresses with a bucketing function — PostgreSQL and
DataFusion spell it `DATE_BIN(INTERVAL <width>, time, <origin>)`, others `time_bucket` or
`GROUP BY FLOOR(time/width)`. The semantics this design adopts are that function's, with the
origin fixed at the Unix epoch: each row is assigned to `floor(timestamp / width) * width`, and
rows sharing a bucket and a tag key form one group.

None of the three is expressible today.

- `measurev1.QueryRequest.Aggregation` names only a **field**. `plan/analyzer.go:301
  validateAggField` walks `measureSchema.GetFields()` and rejects anything else.
- `modelv1.AggregationFunction` is `{MEAN, MAX, MIN, COUNT, SUM}` — there is no `COUNT_DISTINCT`.
- **There is no time bucketing anywhere in the measure query API.** `QueryRequest.GroupBy` is
  `{tag_projection, field_name}` — tags only; the timestamp is not addressable as a group key,
  and no interval/step/bin concept exists in `query.proto`. Worse, aggregation deliberately
  *discards* time: `buildAggOutputLayout` emits `[shard_id?] + tags + agg columns` with no
  `RoleTimestamp`, and `plan/build_test.go:221` pins it — *"aggregation output must drop
  timestamp (D2)"*. The row path agrees (`measure_plan_aggregation.go:286,342` build
  `DataPoint{TagFamilies: …}` and never set `Timestamp`). So `agg` collapses the entire
  `time_range` into one row per tag group, always.

The workarounds all fail:

1. **`COUNT(*)` over one bucket.** This leans on storage usually holding one row per
   `(entity, bucket)` — and `Measure.interval` is a **declared cadence, not an enforced
   invariant**. Writers may emit off-cadence: on a 1-minute measure, timestamps of `09:01:00`
   *and* `09:01:34` are both legal and both land in the 09:01 bucket. So the count can be wrong
   even inside a single bucket, and it is certainly wrong once the window spans two — measured at
   21 for 3 keys over 7 buckets. No error, just a plausible wrong number. This is the strongest
   argument for a real `COUNT_DISTINCT`: the shortcut it replaces has no invariant behind it,
   only a convention writers are free to break.
2. **`groupBy(entity_id)` and count rows client-side.** Correct at any window, but it ships one
   row per distinct value to the *client*, and it does not compose with `top` / `offset` /
   `limit`, so the ranked case cannot be rendered.
3. **One query per bucket.** Correct at any window — each individual query is the workaround-1
   shape, exact precisely because its window is one bucket wide — and it is the only workaround
   that still composes with ranking. The cost is a fan-out: a 1-hour window at 1-minute
   granularity is 60 round-trips, a day is 1440, and the caller carries the bucket arithmetic,
   the partial-failure handling and the reassembly.

### 1.1 The driving requirement for time bucketing

The series case is not a speculative extension of the cardinality work. It comes from a separate
requirement: converting existing analytical queries onto BanyanDB, where the canonical shape is

> group by **a time bucket plus two or more tag dimensions**, select a `COUNT(DISTINCT <tag>)`
> over a further tag that is **not** carried as an output column, alias the result, and select
> additional aggregates alongside it in the same row.

Read as a checklist, that shape is the fastest way to see what the design must cover:

| Element of the shape | Requirement | Where |
|---|---|---|
| group by a time bucket | time-bucket group key, emitted as the row timestamp | §5.1, §7.2 |
| group by several tags as well | **multiple** tag group keys alongside the bucket | §5.1 |
| `COUNT(DISTINCT <tag>)` | `COUNT_DISTINCT` over a tag | §5.1, §7.4 |
| the distinct target is not an output column | target stripped from the output, not emitted as a first-seen tag | §5.2 |
| the result is aliased | a result alias | **out of scope** — §3 |
| further aggregates in the same row | more than one aggregate per request | **out of scope** — §3 |

The last two rows are deliberately out of scope (§3). This design delivers the `COUNT_DISTINCT`
half of the shape; a caller converting such a query issues the distinct count as its own request
and names the result column client-side. That is a smaller step than the full conversion, and
saying so plainly is better than implying the checklist is fully served.

Two further notes on mapping this query onto a measure:

- **All group tags must live in one tag family.** `distributedGroupByTagNames`
  (`distributed.go:1131`) reads `families[0]` only, so the tag dimensions grouped alongside the
  bucket must share a family. This is a pre-existing v1 limitation, not one this design adds, but
  it constrains the schema the conversion targets.
- **The effective routing key determines whether the distinct pushes down** (§7.4) — the
  measure's `sharding_key` when set, otherwise its entity. The condition is
  `routingTags ⊆ groupByTags ∪ {target}`, so a routing key drawn from the grouped tags plus the
  distinct target pushes down; a routing tag outside that set gets the query rejected. A
  conversion gets to *choose* its routing key, so this is a modelling instruction rather than a
  constraint discovered too late.
- **The query must resolve to a single stage** (§7.4). Multi-stage `COUNT_DISTINCT` is rejected,
  and an empty `stages` falls back to `DefaultStages` — so this is a constraint on the target
  group's lifecycle configuration, not just on the query text.

The gap is narrow and the engine already has every primitive needed to close it: grouping over
tag columns, a Map/Reduce split across data nodes and the liaison, a per-query memory budget,
top-N/pagination over an aggregate output column, and — for the series case — an
epoch-anchored bucketing formula already in the tree (`getWindowStart` in
`pkg/flow/streaming/sliding_window.go:302`, used by the TopN pre-aggregation path).

**Time bucketing and tag aggregation are orthogonal**, and the series case needs both. They would
be near-interchangeable if a measure held exactly one row per `(entity, interval)` — then
counting rows in a bucket would count distinct entities. It does not: the interval is a declared
cadence, so two rows for one entity can share a bucket. `COUNT` grouped by bucket therefore
answers "how many data points", `COUNT_DISTINCT` answers "how many distinct things", and they
diverge whenever a writer emits off-cadence or a query bucket is coarser than the interval.
Shipping either feature alone leaves half the dashboard unservable.

---

## 2. Requirements

- `agg` may target a **tag** as well as a field.
- A new `COUNT_DISTINCT` function, **exact** (not sketch-based).
- `group_by` may key on a **time bucket**, producing one row per `(bucket, tag group)` and a
  timestamp on each result row. The width comes from the request or from the measure's
  `interval` — see §5.3.
- Bucket grouping must **stream node-locally**: on the part path, live state is one bucket rather
  than the whole window, and a node emits a bucket as it closes instead of withholding until its
  scan ends (§7.2). This is a requirement rather than an optimisation — it is what makes a
  day-long query at 1-minute buckets viable at all. It is explicitly **not** an end-to-end
  pipelining promise: the liaison collects node frames before reducing them, so the *query*
  remains request/response even where the *operator* streams. Index mode does not stream at all
  (§7.2).
- All of the above compose with each other and with the existing `top`, `offset` and `limit`, in
  standalone **and** distributed deployments.
- A high-cardinality target must fail loudly against the per-query budget, never OOM a node.
- `COUNT_DISTINCT` **pushes down to the data nodes like every other function**, with a scalar
  partial. A request that cannot be pushed down is **rejected at analyze time** — there is no
  second, costlier execution path (§7.4).
- **`index_mode` measures are in scope** for both grouping and aggregation, on the same terms as
  part-backed measures (§7.4, §7.7).
- Reachable from BydbQL, not only from clients that hand-build a `QueryRequest`.
- Default behaviour is unchanged: a request with no `time_bucket` still collapses the whole
  `time_range`, exactly as today.

## 3. Non-goals

- **Approximate distinct (HyperLogLog).** Constant wire cost is attractive, but it needs a new
  partial wire type and an API contract about error bounds. The cardinality of the motivating
  case (api-keys per user) does not justify either. Revisit only if a deployment actually hits
  the budget.

> **Decided:** the two entries below are out of scope. `COUNT_DISTINCT` is the piece of §1.1 this
> design delivers; the remaining elements of that query shape are deferred deliberately, not left
> open. A caller converting such a query issues the distinct count as its own request and names
> the result column client-side.

- **Multiple aggregates in one request.** `QueryRequest.agg` stays singular and BydbQL keeps
  erroring on a second aggregate in `SELECT`. The operator is already plural — `AggSpec` is a
  slice and `BatchAggregation` loops over `a.aggs` — so if this is ever picked up the work is API
  and planning rather than execution, and it can be added as `repeated Aggregation` under a new
  field number without breaking the singular form.
- **Result aliases (`AS`).** No BydbQL `AS` production and no `Aggregation.output_name`. The
  result column keeps the naming rule in §5.2: it is named after the aggregation target.
- **Row-path parity.** See §8.
- **`COUNT_DISTINCT` in `TopNAggregation` pre-aggregation rules.** Rejected, not implemented.
- **Per-bucket `top`.** With `time_bucket` set, `top`/`offset`/`limit` stay **global** over the
  whole result set — one heap, exactly today's semantics. "Top 10 devices *in each* bucket" is
  the TopN RPC's job and already exists there: `TopNResponse.lists` is "a series topN lists
  ranked by timestamp". Redefining `top` per bucket would fork that semantics across two APIs.
  See §10 for the consequence this has for `limit`.
- **Bucket gap-filling.** Buckets with no data produce no row. Zero-filling a sparse series is a
  presentation concern and needs the client to know the expected bucket set anyway.
- **Arbitrary bucket origins.** Buckets are anchored at the Unix epoch, matching the `DATE_BIN`
  call above with `TIMESTAMP '1970-01-01 00:00:00'`. A configurable origin is not in scope.

**Current limitation, not a design decision: `group_by.time_bucket` requires `agg`.** This
document never states whether a *raw* bucketed `GroupBy` (`time_bucket` set, no `agg`) is
supported — it is written throughout as if `agg` is always present. Implementing §7.2 found a
concrete reason it cannot be, as things stand: the no-`agg` shape reuses `BatchAggregation`'s
empty-`AggSpec` output layout, which — unlike `BatchGroupByFirst`, the row-preserving raw-GroupBy
operator — does not carry the full input schema forward (it drops other fields, the series id,
and the version), and the distributed row-merge path has no bucket-aware counterpart either. The
analyzer rejects this combination explicitly (`plan.Analyze: time_bucket requires Agg`) rather
than shipping a silently-truncated result. Lifting this is legitimate future work, but it needs
schema-preservation and distributed-merge work of its own; it is not a small extension of the
`agg` case.

---

## 4. Background: how measure aggregation works today

The vectorized engine is the default (`vectorized/measure/config.go DefaultConfig` → `Enabled:
true`); the row engine in `pkg/query/logical/measure` is explicitly marked *"Do not extend with
new features"* (`measure_analyzer.go:62-68`).

**Standalone.** `plan.Analyze` builds a `GroupByAgg` node which `BuildOperators`
(`vectorized/measure/plan.go:61`) turns into one `BatchAggregation`. Group keys are encoded by
`appendKeyComponent` (`groupby.go:287`) — a shared helper that handles int64, float64, string,
bytes, `*modelv1.TagValue` and `*modelv1.FieldValue`. Each group holds one `aggSlot` per
aggregate, backed by `pkg/query/aggregation`.

**Distributed.** The liaison sets `InternalQueryRequest.AggReturnPartial`
(`plan/distributed.go:474`), which the data node turns into `AggModeMap`
(`plan/dispatch.go:120-123`). The node emits typed-column *partials* — a `Partial{Value, Count}`
per group — serialized by `vectorized/measure/frame`. The liaison runs `AggModeReduce`
(`reduce.go:107 ReducePartialBatches`), deduplicating replicas on `(shard_id, group_key)` before
combining.

Two structural facts this design leans on:

- **Tag columns cross the wire fine.** The frame codec carries string and bytes columns with
  their family name intact (`frame/encode.go:65-69`, `decode.go:105-107`). The raw-GroupBy
  distributed path (`distributed.go:913 applyBatchGroupByFirstToRows`) already ships tag columns
  from data nodes and dedupes them liaison-side. Only array columns have no wire type
  (`frame/frame.go:185-202` → `ErrUnsupportedColumnType`).
- **A tag is only a *native typed* column when something keys off it.** `BuildBatchSchema`
  (`integration.go:60`) consults `buildNativeTagSet` (`:139`), which today contains only GroupBy
  keys. Every other tag stays a `ColumnTypeTagValue` passthrough. This matters because
  `aggregation.go:464 fold` hard-casts to `TypedColumn[int64]`/`[float64]` with no fallback.

---

## 5. API

### 5.1 Proto

One enum value in `api/proto/banyandb/model/v1/common.proto`:

```proto
enum AggregationFunction {
  …
  AGGREGATION_FUNCTION_SUM = 5;
  // COUNT_DISTINCT counts the distinct non-null values of the target.
  // It is exact and bounded by the per-query memory budget.
  AGGREGATION_FUNCTION_COUNT_DISTINCT = 6;
}
```

One field on `QueryRequest.Aggregation` in `api/proto/banyandb/measure/v1/query.proto`:

```proto
message Aggregation {
  model.v1.AggregationFunction function = 1;
  // field_name must be one of files indicated by the field_projection
  string field_name = 2;
  // tag_name aggregates over a tag instead of a field. Exactly one of
  // field_name and tag_name must be set.
  string tag_name = 3;
}
```

One field on `QueryRequest.GroupBy`, in the same file:

```proto
message GroupBy {
  // tag_projection must be a subset of the tag_projection of QueryRequest
  model.v1.TagProjection tag_projection = 1;
  // field_name must be one of fields indicated by field_projection
  string field_name = 2;
  message TimeBucket {
    // width is a duration string using the same units as Measure.interval
    // ("ns", "us", "ms", "s", "m", "h", "d"). Empty means "use the measure's
    // interval"; if the measure has no interval either, the request is
    // rejected.
    string width = 1;
  }
  // time_bucket adds the data point's timestamp, floored to a bucket boundary,
  // as the leading group key, and re-emits it as the result row's timestamp.
  // Buckets are anchored at the Unix epoch: bucket_start = ts - ts % width,
  // matching DATE_BIN with a Unix-epoch origin. Unset means no time bucketing
  // — the whole time_range collapses to one row per tag group, as today.
  TimeBucket time_bucket = 3;
}
```

All additions are wire-compatible; existing clients are unaffected, and an unset `time_bucket`
preserves today's behaviour exactly.

**The aggregation target is qualified by family.** An earlier draft used a bare `tag_name`,
reasoning that the engine keys tag specs by bare name
(`pkg/query/logical/schema.go:92-99`) and so names must be unique. That is an implementation
assumption, not a schema invariant — and the two are not the same thing. Tag-family validation
(`api/validate/validate.go:329-347`) does not reject a name repeated across families;
`pkg/pb/v1/metadata.go:27-35` makes the same unenforced assumption; and
`pkg/query/vectorized/schema_test.go:81-94` explicitly exercises identical names in different
families. `RegisterTag` silently overwrites one family's spec with another's.

So a bare name can be ambiguous on a valid schema. Two ways out were available: retrofit a global
uniqueness rule, or qualify at the new seam. **Qualifying is the correct one** — a uniqueness
rule would invalidate existing schemas that are legal today, to serve a feature they do not use.
`GroupBy` already qualifies by family; the aggregation target now matches it.

### 5.2 Result naming

The result column **inherits the target's name**, extending the existing field rule
(`plan.go:109-116`, pinned by `plan_test.go
TestBuildOperators_AggOutputName_InheritsInputFieldName`). So
`COUNT_DISTINCT(entity_id) GROUP BY service_id` returns tag `service_id` plus field `entity_id`,
and the ranked case orders with `top.field_name = "entity_id"` — no new API surface for ordering.

When the analyzer injects the target tag into the tag projection itself (the caller did not ask
for it), that tag is **stripped from the output**. Otherwise every result row would also carry a
meaningless first-seen copy of the value being counted. If the caller *does* project the tag
explicitly, they get both a tag and a field of that name; the two live in separate namespaces
(`BatchSchema.tagByPath`, keyed by `(family, name)`, vs `fieldByName`) so nothing collides.

The bucket has no such naming question: it is carried on `DataPoint.timestamp`, a field the
message already has, set to the **bucket start**. There is no synthetic `time_bucket` tag or
field — the wire shape of a bucketed result is the wire shape of any other data point.

### 5.3 Resolving the bucket width

The width comes from the request, or failing that from the measure. Either source is enough;
only the absence of both is an error.

| `time_bucket` | `Measure.interval` | Outcome |
|---|---|---|
| unset | — | No bucketing. Today's behaviour, unchanged. |
| set, `width` given | — | Bucket width = `width`. |
| set, `width` empty | set | Bucket width = the measure's interval. |
| set, `width` empty | empty | **Reject** — *"time_bucket needs a width: measure %q declares no interval"*. |
| set, `width` unparseable / zero / negative | — | Reject. |

Both sources are parsed with `timestamp.ParseDuration` (`pkg/timestamp/duration.go:31`), the same
parser the write path uses on the schema field (`banyand/measure/measure.go:118`), so the two
agree on units by construction.

**There is deliberately no "must be a multiple of the interval" rule.** An earlier draft required
one, on the reasoning that a narrower or non-aligned bucket would partition the data
arbitrarily. That reasoning assumed `Measure.interval` describes where data points actually land
— and it does not. The field declares a *cadence writers are asked to follow*, not an invariant
storage enforces: on a 1-minute measure, timestamps of `09:01:00` and `09:01:34` are both legal.

Once points may fall anywhere, every width is equally meaningful — binning arbitrary timestamps
is exactly what `DATE_BIN` does — and a multiple-of rule would reject useful queries to protect
an alignment that was never guaranteed. What survives from that reasoning is a cost observation
rather than a constraint: bucket count is `time_range / width`, so a very small width against a
long window produces many groups. That is bounded by the memory budget (§7.6) and by `limit`
(§10), which is where cost belongs.

The same correction removes an assumption that ran deeper than this section — see §1 on why
`COUNT(*)` is unreliable even within a single bucket, and §11 on the test that assumption would
have produced.

### 5.4 BydbQL

```sql
SELECT service_id, COUNT(DISTINCT entity_id) FROM MEASURE … GROUP BY service_id
SELECT host, COUNT(host) FROM MEASURE … GROUP BY TIME_BUCKET('5m'), host
```

`DISTINCT` is accepted inside `COUNT` only; `SUM(DISTINCT …)` is a parse error. The aggregate
column resolves against tags as well as fields — `transformer.go:846 convertAggregation`
currently hard-errors when the column is not in `allFields`.

`TIME_BUCKET(<duration>)` is a pseudo-column accepted only in `GROUP BY`, at most once, and it
maps to `GroupBy.time_bucket`. The argument may be omitted — `TIME_BUCKET()` requests bucketing
at the measure's own interval, the `width`-empty case in §5.3. It is deliberately not selectable
in the projection: the bucket arrives on the data point's timestamp, not as a column. Results are
ordered by bucket ascending (§7.5), so an explicit `ORDER BY` on the bucket is unnecessary.

---

## 6. Semantics

Validated at analyze time; anything outside the matrix is a hard error naming the target and its
type.

| Target type | SUM / MIN / MAX / MEAN | COUNT | COUNT_DISTINCT |
|---|---|---|---|
| Field INT / FLOAT | ✅ today | ✅ today | ✅ new |
| Tag `TAG_TYPE_INT` | ✅ new | ✅ new | ✅ new |
| Tag `TAG_TYPE_STRING` | ❌ reject | ✅ new | ✅ new |
| Tag `TAG_TYPE_DATA_BINARY` | ❌ reject | ✅ new | ✅ new |
| Tag `*_ARRAY`, `TAG_TYPE_TIMESTAMP` | ❌ reject | ❌ reject | ❌ reject |

- Null values are excluded, matching the current field behaviour: they do not increment `COUNT`
  and do not enter the distinct set.
- `MEAN` over an INT tag yields float64, matching the existing field rule.
- **Array and timestamp tags must be rejected explicitly, not left to fall through.**
  `tagTypeToColumnType` (`integration.go:162`) already errors on `TIMESTAMP`, but array tags map
  to `ColumnTypeInt64Array`/`ColumnTypeStrArray`, and `appendKeyComponent`
  (`groupby.go:287-303`) *silently returns the buffer unchanged* for those — every row would
  collapse into one group and produce a plausible wrong count, the exact failure mode this
  design exists to remove. A `keyComponentSupported(ColumnType) bool` predicate should sit next
  to `appendKeyComponent` so the analyzer's rejection list and the operator's assumption cannot
  drift apart.

Time bucketing is orthogonal to this matrix: it adds a group key, not an aggregate, so it
composes with every cell above. Its own validation rules are in §5.3. Two interactions are worth
stating explicitly:

- **A bucket key does not change what an aggregate means** — it only narrows the row set each
  aggregate sees. `COUNT_DISTINCT` per bucket counts values distinct *within* that bucket; a
  value present in two buckets counts once in each. That is the intended series semantics, and it
  is deliberately not the same number as the unbucketed total.
- **`TAG_TYPE_TIMESTAMP` tags remain rejected** as aggregation targets. The bucket key comes from
  the data point's own timestamp, which is not a tag, so the two never meet.

---

## 7. Execution design

### 7.1 Binding the target to a column

`model.MeasureAgg` gains `TagName`, `TagFamily` and `HideTag` (the last set when the analyzer
injected the projection). Four existing seams become target-aware:

| Seam | Change |
|---|---|
| `plan/analyzer.go:209 translateAgg` | resolve tag **or** field; enforce the §6 matrix |
| `plan/analyzer.go:264 ensureAggFieldProjected` | mirror `ensureGroupByProjected` (`:226`) and inject the tag, setting `HideTag` |
| `integration.go:139 buildNativeTagSet` | include the agg tag, so its column is native typed |
| `plan.go:155 lookupFieldColumnIndex` | add a tag sibling resolving on `(RoleTag, TagFamily, Name)`, like `lookupGroupByKeyIndices` (`:135`) |

The `buildNativeTagSet` change is load-bearing: without it the target column is a
`ColumnTypeTagValue` passthrough and `fold` panics on its type assertion. This is the same
native-column contract the analyzer already documents at `analyzer.go:73-79`.

Everything downstream of `AggSpec.InputCol` is index-based, so **SUM/MIN/MAX/MEAN/COUNT over an
INT tag need no execution changes at all** — the partial is a `RoleField` int64/float64 column
named after the target, exactly what `bindAggReduceSpecs` (`reduce.go:219`) already binds. Only
two small operator fixes are needed:

- `aggregation.go:464 fold` — `COUNT` must not read the value for non-numeric columns (it
  currently parses, then calls `In(v)`). A null-check-only branch makes `COUNT(string_tag)` work.
- `aggregation.go:252 collectTagIndices` — accept an exclusion index so a `HideTag` target is
  not emitted as a first-seen tag beside its own result field.

### 7.2 Time-bucket grouping

The implementation is one small map operator plus one conditional in the output layout. The key
insight is that **the timestamp is already an `int64` column** in the input schema
(`RoleTimestamp`, produced by `BuildBatchSchema`'s fixed prefix at `integration.go:64-69`), and
`appendKeyComponent` already encodes `int64` key components. So the bucket does not need a new
key encoder, a derived column, or a change to `computeKey`:

> **Floor the timestamp column in place, then group by it like any other column.**

A `BatchTimeBucket` map operator sits immediately before the aggregation and rewrites the
timestamp column to its bucket start:

```go
// bucketStart mirrors pkg/flow/streaming/sliding_window.go:302 getWindowStart,
// the formula the TopN pre-aggregation path already uses to assign tumbling
// windows. It is exactly DATE_BIN with a Unix-epoch origin.
func bucketStart(ts, width int64) int64 { return ts - ts%width }
```

#### The bucket group must NOT be a hash map over all buckets

The obvious implementation — add the bucket to `keyIndices` and let `BatchAggregation`'s
`map[string]*aggGroup` hold every `(bucket, tags…)` combination until the scan ends — is the
wrong shape for a time-series database. It holds `buckets × tagGroups` live groups, blocks until
the last row is read, and then needs a sort to put the series back in time order. All three costs
are avoidable, because **the input is already sorted by time**.

`applyMeasureQueryOrdering` (`banyand/measure/query.go:286`) sets the default explicitly:

```go
func applyMeasureQueryOrdering(mqo model.MeasureQueryOptions, result *queryResult) {
	if mqo.Order == nil {
		result.ascTS = true
		result.orderByTS = true      // ← globally timestamp-ascending
		return
	}
	…
	result.orderByTS = mqo.Order.Type == index.OrderByTypeTime
}
```

and `queryResult.Less` (`:913`) then makes the merge heap compare **timestamp first**, falling
back to seriesID only to break ties. An aggregation request carries no `order_by` — the
distributed analyzer explicitly skips OrderBy resolution when `Agg != nil`
(`distributed.go:248`) — so `Order == nil`, and rows arrive in globally ascending timestamp
order with no change to the scan at all. (The non-default branch, `orderByTS == false`, sorts by
`sidToIndex` first and is series-major; that is the case the guard below exists for.)

Sorted input means a bucket, once left behind, can never receive another row. So:

> **Stream the buckets.** Keep groups for the *current* bucket only; when a row's bucket advances,
> flush that bucket's groups downstream and reset the map.

```
rows (time-ascending) ──▶ [bucket B0 groups] flush ──▶ [B1 groups] flush ──▶ [B2 …]
                           ^ map holds ONE bucket at a time
```

Three properties fall out, and they are the reason to prefer this over the map:

| | Hash map over all buckets | Streaming |
|---|---|---|
| Live groups | `buckets × tagGroups` | `tagGroups` (one bucket) |
| Emission | blocking — nothing until the scan ends | node-local pipelining — bucket *n* leaves the operator while *n+1* fills |
| Output order | needs a sort pass | already bucket-ascending |

The 1440× memory multiplier a 24-hour/1-minute query would otherwise carry simply does not exist,
and `BatchAggregation`'s `BreakerOperator` contract (`vectorized/operator.go:63` — "buffers all
input via Consume, then produces output via NextBatch") relaxes to a pipelined operator for the
bucketed case. This also mirrors how the codebase already aggregates over time on the write path:
`pkg/flow/streaming` assigns tumbling windows and emits them as they close, using the same
`getWindowStart` formula.

**"Relaxes to a pipelined operator" is not a small tweak to `BreakerOperator` — it needed a new
operator shape.** `BreakerOperator` is driven by `breakerStage`, which fully drains upstream via
repeated `Consume` calls before it ever calls the breaker's own `NextBatch` — by contract, not by
a fixable oversight. Wrapping `BatchTimeBucket` as a `BreakerOperator` would therefore have pulled
every upstream batch, closing every bucket the whole scan will ever produce, before emitting the
first one: exactly the `buckets × tagGroups` cost this section exists to avoid, just moved from
the group map into an output queue. (This is precisely the failure mode §11's testing strategy
calls out under "Pipelining": *"A `BreakerOperator`-shaped implementation passes every correctness
test and fails this one."*)

The actual fix: `BatchTimeBucket` is a genuine `vectorized.PullOperator` that owns and pulls from
its upstream directly inside its own `NextBatch`, rather than being driven by `breakerStage`. It
is spliced into the pipeline via a new `PipelineBuilder.Transform(fn func(upstream PullOperator)
PullOperator)` method (`pkg/query/vectorized/pipeline.go`) — it closes whatever has been built so
far into one concrete `PullOperator`, hands it to `fn`, and makes `fn`'s result the new base for
whatever follows. `Transform` is now general pipeline infrastructure, available to any future
operator with the same requirement: pull upstream lazily, batch by batch, rather than draining it
before serving anything. (COUNT_DISTINCT, §7.3–§7.4, does not need it — it reuses
`BatchAggregation`'s existing `Consume`/`NextBatch` contract unchanged.)

A related pitfall surfaced during review even after the `PullOperator` rewrite: `BatchTimeBucket`
originally still drained a closed bucket's aggregator in one synchronous loop before returning
anything. That is harmless for the streaming path (one bucket's groups are bounded by construction
either way), but index mode's non-streaming aggregator can represent the *entire* scan — so
draining it in one shot re-introduced the same "materialize everything before returning the first
batch" shape this whole design exists to avoid, just at the output-queue layer instead of the
group-map layer. The fix (`drainMapModeAggregator`) pulls that terminal aggregator one page at a
time, interleaved with normal output commits, the same discipline the upstream pull loop already
follows. Any future operator that holds one aggregator across an entire unordered scan — a
candidate shape for other index-mode fallbacks — should drain it the same way.

**Index mode does not get this guarantee, and must not stream.** `query.go:140-147` returns
through the index-mode branch *before* the ordering setup above ever runs; `buildIndexQueryResult`
passes `mqo.Order` through unchanged (`query.go:506-560`), and with a nil order
`storage/index.go:350-374` takes the unsorted filter path and returns the underlying index search
order. Timestamps being present in `SeriesData` is not an ordering guarantee. A legal index-mode
input can therefore arrive in buckets B, A, B — which would reopen a flushed bucket, or trip the
monotonicity guard on a query that is perfectly valid.

> **Index-mode bucketed aggregation uses the non-streaming map** — groups keyed on
> `(bucket, tags…)` held until the scan ends — rather than the streaming operator. Index mode
> stays fully in scope (§2); it simply takes the safe path. Streaming is a part-path optimisation,
> and the plan must choose between them on `measureSchema.GetIndexMode()`, not on a runtime guess.

The alternative — forcing a verified timestamp sort ahead of the operator for index mode — buys
back streaming at the cost of a blocking sort, which is what streaming existed to avoid. The map
is the honest trade.

**The guard is not optional.** Streaming is correct *only* while the input is time-ordered. If
`orderByTS` were ever false on the part path, a bucket would be reopened after being flushed and
the query would emit duplicate rows for the same bucket — a silently wrong series, the exact
failure class this design exists to remove. Two defences, both required:

1. **Analyzer** — reject (or refuse to stream) a bucketed request whose `order_by` resolves to
   anything other than time ordering, since that flips `orderByTS` to false.
2. **Operator** — assert monotonicity. A row whose bucket is *less* than the current bucket is an
   invariant violation; fail loudly rather than emit a second row for a closed bucket. This is one
   comparison per row and it converts a silent corruption into a loud error.

Everything else downstream is unchanged: `keyIndices` gains the timestamp column index,
`computeKey` encodes it, `newGroup` copies it with the existing `copyOneValue`, and every row in
a group shares the bucket by construction.

Three further consequences to handle:

- **Reverse D2 conditionally.** `buildAggOutputLayout` (`aggregation.go:659`) must re-emit the
  `RoleTimestamp` column when — and only when — a bucket key is present. Today it never does, and
  `plan/build_test.go:221` asserts that; the assertion stays correct for the unbucketed case and
  gains a bucketed sibling. The serializer then populates `DataPoint.timestamp` from it, which is
  why §5.2 needs no new column name.
- **Negative timestamps.** Go's `%` truncates toward zero, so `bucketStart` skews by one bucket
  for `ts < 0`. Measure timestamps are Unix milliseconds and the write path rejects
  non-positive ones, so this is unreachable — but the helper should floor correctly anyway
  rather than depend on a caller's invariant.
- **Bucket ≠ segment.** The bucket width is unrelated to `ResourceOpts.segment_interval`, which
  is an `IntervalRule` restricted to HOUR/DAY and governs physical segmentation, not query
  grouping. A bucket may span segments and a segment may contain many buckets; nothing in the
  scan needs to know.

**Distributed.** The bucket is a pure function of a value already on the row, so each data node
floors independently and no coordination is needed. Node partials become per
`(bucket, tag group)`; the liaison reduce must key on the bucket too. Two seams:
`distributedGroupByTagNames` (`distributed.go:1131`) returns tag names only and needs to carry
the bucket flag, and `resolveKeyIndices` (`reduce.go:169`) resolves `RoleTag` columns only and
needs to resolve the `RoleTimestamp` column as the leading key.

The streaming property survives the fan-out, because **every node emits buckets in ascending
order**, so the liaison holds N sorted streams rather than N unordered heaps. That makes the
reduce a k-way merge on the bucket key: a bucket closes as soon as every node has moved past it,
and the liaison's live state is one bucket's groups rather than the whole window's. The same
`buckets × tagGroups` blow-up is avoided on both sides.

Node-side streaming is mandatory (it is where the scan-sized state lives). Liaison-side merging
is a refinement worth separating, and the distinction matters for what the design is allowed to
claim: the vec distributed path broadcasts and **collects every response before `executeAgg`**
(`distributed.go:471-476`, `:638`), so the encoded payload is already fully resident. Merging
saves the decoded group state, not the buffering.

That is why §2's requirement is worded as *node-local* streaming. Calling the whole query
pipelined while the liaison buffers all frames would be false. Genuine end-to-end pipelining
would need ordered partial streams, a liaison k-way merge, and cancellation/backpressure plumbing
— a separate piece of work, not a property this design delivers. A correct first cut
hash-aggregates on `(bucket, tags…)` at the liaison, bounded by what the nodes chose to send.

### 7.3 COUNT_DISTINCT on a single node

A new `AggCountDistinct` function and a new `aggSlot` variant holding a set of encoded values,
keyed by `appendKeyComponent` — the same encoder the group key already uses, which covers every
target type the matrix admits.

The set itself belongs in `pkg/query/aggregation` (a new `distinct.go`), keeping aggregation
semantics in the package that owns them; `aggregation_test.go
TestBatchAggregation_DelegatesToAggregationPackage` pins that convention.

```go
// Distinct accumulates the distinct values of one group.
type Distinct interface {
    In(key []byte) (added bool) // added is true on first sight; it drives the memory charge
    Val() int64
    Reset()
}
```

`NewMap`/`NewReduce` (`aggregation.go:62`, `:82`) should keep returning `errUnknownFunc` for
`COUNT_DISTINCT`. That is not an oversight but the mechanism by which a `COUNT_DISTINCT` TopN
rule is rejected: `banyand/measure/topn_post_processor.go:285` calls `NewMap` and propagates the
error rather than mis-aggregating.
### 7.4 COUNT_DISTINCT across data nodes

Every other function pushes down cleanly: the node reduces to a scalar `Partial` and the liaison
combines. `COUNT_DISTINCT` appears to break that, because per-node counts cannot in general be
summed — the same value seen on two nodes would count twice.

**But it can be pushed down whenever shard placement partitions the value space, and that is
decidable statically.** So `COUNT_DISTINCT` pushes down like everything else, and a request that
cannot be pushed down is **rejected at analyze time** rather than served by a second, costlier
mechanism.

**The rejection is uniform across deployment shapes.** A standalone server has no sharding, so it
could serve every `COUNT_DISTINCT` correctly — and deliberately does not. The condition is
evaluated in the analyzer, which runs identically in both modes, so a query either works
everywhere or nowhere. The alternative would let a query pass in a single-node development
environment and fail on a cluster, which is a worse failure than never having supported it: the
rule *the target must be the entity* is only teachable if it holds unconditionally.

#### The decomposability condition

Shard placement is deterministic and value-derived, but the values it derives from are **the
effective routing key**, which is not always the entity. `Locate` computes an entity-based shard
and then *overrides* it with the sharding-key router when one is configured
(`pkg/partition/route.go:51-65`); the liaison installs both routes
(`discovery.go:93-111`, sharding-key locator at `:428-452`), and the write path picks between
them at `liaison/grpc/measure.go:176-181`.

> **routingTags** = the measure's `sharding_key` when configured, otherwise its entity tags.

This is a supported schema shape, not a rejected input: `CheckShardingKeySubset`
(`api/validate/validate.go:228-255`) returns nil for a single entity tag, and a subset failure is
logged as a *warning* during create/update (`metadata/schema/property/client.go:779-809`). The
repo's own fixtures carry one — `service_instance_float_metric.json` has entity
`[service_id, entity_id]` with sharding key `[service_id]`.

Every row sharing a set of routing tag values lands on exactly one shard. If fixing the group key
and the distinct value pins *every* routing tag, that `(group, value)` pair can exist on only one
shard, the per-shard distinct sets for a group are **disjoint**, and disjoint cardinalities add:

> **routingTags ⊆ groupByTags ∪ {target}** ⟹ per-shard counts are summable.

**This is a sufficient condition, not a necessary one.** Fixed predicates in the query, or a
proven functional dependency between tags, can equally pin a routing tag that is not grouped. The
design does not model either, so some decomposable queries are rejected. That is a deliberate
product gate — see *Policy, not necessity* below — and it should not be described as a
mathematical boundary.

**Why stating this over entity tags is wrong**, concretely: entity `{e}`, sharding key `{s}`, and
`COUNT_DISTINCT(e)` with no grouping on `s`. An entity-based rule accepts it because the target
*is* the entity. But the same `e` occurring under two `s` values routes to two shards, each
reporting a local count of 1, summing to 2 for one distinct entity.

When the condition holds, each node computes its own exact distinct count per `(bucket, group)`
and emits **one int64** in the existing `Partial{Value}` shape; the liaison reduces with **SUM**.
No new wire type, no new partial shape, and the whole `AggModeMap` → `AggModeReduce` machinery is
reused verbatim: `COUNT_DISTINCT` is *"local distinct, global sum"*.

#### Multi-stage requests are rejected

`(stage, shard, group)` identity decides whether two partials are *replicas*. It does **not**
make stage-local distinct counts additive, and an earlier draft wrongly assumed it did.

Shards partition the *value* space; stages partition *time* — and a distinct value crosses time.
Hot holds group `g` with target set `{e1}`, warm holds `g` with `{e1}`: the true whole-range
answer is 1, and summing stage-local cardinalities gives 2. No migration overlap or duplicate
timestamp is needed; the same target legitimately occurs at different times in different stages.

**Bucketing does not rescue it, and the reason is worth recording.** It is tempting to argue that
because stages do not overlap in time, a bucket falls inside one stage. But that non-overlap is
not visible to the analyzer: `parseNodeSelector` (`dquery.go:161-189`) only builds *selectors* —
it produces no stage time windows — and segment selection is by overlapping physical segment
range and retention (`storage/segment.go:612-649`, `tsdb.go:277-299`), not by a query-time
lifecycle boundary. Bucket width alone is not proof of single-stage ownership.

> **Therefore: reject every resolved multi-stage `COUNT_DISTINCT`, bucketed or not.** Resolve the
> requested stages — including the `DefaultStages` fallback, which may list several — and reject
> if more than one survives. Never silently drop a stage.

Lifting this later requires all of: a per-output-bucket read-ownership invariant that is
*enforced and analyzer-visible*, stable effective routing, correct per-shard partials, and
replica dedup. Short of that, the alternative is a value-bearing union across stages with its own
memory budget — the fallback this design rejects on cost grounds elsewhere.

A related caveat with the same shape: `ResourceOpts.ShardNum` can be changed by `UpdateGroup`
(`metadata/schema/property/client.go:976-989`). A pure node rebalance that preserves logical
shard ids is harmless, but a modulus change creates historical routing domains, and a single
*current* shard count does not prove which modulus the older selected parts were written under.
Treat unchanged `shard_num` over the queried range as an assumption to establish, not a given.

#### The rule, and what it means in practice

For the common SkyWalking shape the condition collapses to something easy to predict. Those
measures declare both entity and sharding key as the single `entity_id` tag — visible in this
repo's fixtures, e.g. `pkg/test/measure/testdata/measures/endpoint_resp_time_minute.json`:

```json
"entity":       { "tag_names": ["entity_id"] },
"sharding_key": { "tag_names": ["entity_id"] },
"interval": "1m"
```

With `routingTags = {entity_id}`, `COUNT_DISTINCT(entity_id)` satisfies
`routingTags ⊆ groupBy ∪ {target}` for any grouping, and a distinct over a non-routing tag such
as `service_id` does not.

**The shorthand "distinct over the entity is always supported" is wrong** and was removed: it
holds only when the routing key *is* the entity. A measure with a narrower sharding key —
entity `[service_id, entity_id]`, sharding key `[service_id]`, as in
`service_instance_float_metric.json` — behaves differently in both directions. There,
`COUNT_DISTINCT(entity_id)` grouped by `service_id` is accepted because `{service_id}` is
covered, while an ungrouped `COUNT_DISTINCT(entity_id)` is rejected. **Always evaluate the
condition against `routingTags`, never against a remembered rule about entities.**

Applied to the motivating workload, whose measures route on `entity_id`:

| Case | Shape | Outcome |
|---|---|---|
| Distinct api-keys for one user | `COUNT_DISTINCT(entity_id) WHERE service_id = …` | ✅ pushed down |
| Distinct api-keys per user | `GROUP BY service_id, COUNT_DISTINCT(entity_id)` | ✅ pushed down |
| The same, per time bucket, single stage | `+ TIME_BUCKET(w)` | ✅ pushed down |
| Any of the above resolving to **more than one stage** | — | ❌ rejected |
| Distinct *models* per user, as modelled today | model is inside a serialized blob, not a tag | ❌ not expressible at any layer |
| Distinct models, with the model promoted to an entity | structurally identical to the api-key case | ✅ pushed down |
| "How many distinct users exist?" | `COUNT_DISTINCT(service_id)` — not a routing tag | ❌ rejected |

Every case this workload expresses is pushed down, **provided it resolves to a single stage** —
which is now a live constraint on the deployment, not a footnote, since an empty `stages` falls
back to `DefaultStages` and that may list several.

#### Why rejection rather than a fallback

A fallback path — nodes shipping their distinct *values* for the liaison to union — is always
correct and needs no condition check. It was considered and rejected (§9), because:

- **Its cost is unbounded and user-controlled.** Wire volume and liaison memory scale with
  cardinality, so one `COUNT_DISTINCT` over a high-cardinality tag funnels every value into a
  single process. "Slow and correct" is a poor trade when the liaison is shared infrastructure;
  the failure lands on every other query in flight.
- **It makes the cost model unpredictable.** Two queries that look alike would differ by orders
  of magnitude depending on a schema property the author cannot see in the query text. A hard
  rule — *the target must be the entity* — is something a user can reason about in advance.
- **It doubles the surface.** Two implementations of one function, a static chooser between them,
  opposite replica-dedup requirements (union is idempotent, summation is not), and a differential
  test to keep them honest — all to serve queries the data model says should be written
  differently.
- **It is inconsistent with the rest of this design**, which already rejects rather than
  approximates: array and timestamp tags (§6), numeric functions over string tags (§6), bucket
  a bucket width with no source to resolve it from (§5.3), tag aggregation on the row engine (§8).

Rejection is also the honest signal. A `COUNT_DISTINCT` over a non-entity tag is usually a
modelling mismatch, and the error should say so rather than silently serving it expensively:

```
count_distinct(service_id) is not supported: the target must be part of the measure's
entity (entity_id), or every entity tag must appear in GROUP BY. Either group by
service_id and count_distinct(entity_id), or model service_id as an entity tag.
```

The user is not stuck: `GROUP BY <target>` and counting the returned rows client-side still
works exactly as it does today, and is the same shape the fallback would have implemented — only
with the cost visible to the caller who chose it.

#### Policy, not necessity

Three of the rejections above are **product decisions and should be read as such**, not as
mathematical boundaries:

- The condition is *sufficient*, not necessary. Fixed predicates and proven functional
  dependencies could pin an ungrouped routing tag; this design models neither.
- Grouping by every effective routing tag can make a **non-routing target** decomposable. Such
  queries are rejected because the analyzer does not reason about it, not because they are
  unanswerable.
- Rejecting uniformly in standalone (§7.4) forgoes queries a single node could union locally and
  exactly. That is a portability choice — one contract regardless of deployment shape — and is
  only defensible while it is stated deliberately rather than presented as a limit of the maths.

Each is a place where the engine could later do more without changing any wire format. Recording
them as policy keeps that door visible; describing them as necessity would quietly close it.

#### De-duplicating the partials

Replicas of a shard hold identical data, so selecting one copy per replica set is the right
mechanism. It needs two fixes to be correct, because today's deduped unit is neither a shard nor
a stage.

**(a) Today's partial is per-node, and its shard label is a hint.** `computeKey`
(`aggregation.go:523`) builds the group key from `keyIndices` — the group-by tags only — and
`newGroup` stamps the group with the shard id of the *first row that created it*. A node holding
shards 1 and 2 emits **one** partial for group `g` covering both, labelled with whichever shard
appeared first. "The replicas are all the same" is true of shards and not of these partials.

Within a node each shard appears exactly once — there are no duplicate shards on a node — so a
per-shard partial is always well defined. That property is what makes the fix below *correct*;
it is not what makes today's labelling *sufficient*. What breaks today is that node shard-*sets*
differ, and under replication they do, by construction:

```go
// pkg/node/round_robin.go:218
func (r *roundRobinSelector) selectNode(index int, replicasID uint32) string {
	adjustedIndex := index + int(replicasID)
	return r.nodes[adjustedIndex%len(r.nodes)]
}
```

Placement is *staggered*, so with 4 nodes, 4 shards and `replicas=1`:

| | shard 0 | shard 1 | shard 2 | shard 3 |
|---|---|---|---|---|
| replica 0 | n0 | n1 | n2 | n3 |
| replica 1 | n1 | n2 | n3 | n0 |

giving `n0={s0,s3}`, `n1={s0,s1}`, `n2={s1,s2}`, `n3={s2,s3}`. No node holds a shard twice, every
shard is on exactly two nodes, and **no two nodes hold the same set** — all three at once. `Broadcast` (`queue/pub/pub.go:309`) reaches all four
(there is no shard-level query routing, and `segment.Tables()` exposes every shard a node holds
with no primary/replica distinction), so four partials arrive for group `g`, each covering two
shards and labelled with one. Deduping on `(shard_label, group)` then either drops a whole
two-shard partial or keeps two that overlap. Concretely, for a group present in `s0` and `s1`:
`n0` reports `s0`, `n2` reports `s1`, and `n1` reports `s0+s1` under whichever label its first
row carried. If `n1` labels itself `s0` the answer is right by luck; if it labels itself `s1`,
`s0` is counted twice. Same data, same topology, different answer depending on row order.

At `replicas=0` the table collapses to one row, node sets are disjoint, every shard is covered
exactly once and the dedup never fires — consistent with this not having been observed.

> **Fix:** in the map phase, add the shard-id column index to `keyIndices`. Each emitted row is
> then exactly one shard's contribution, by construction rather than by labelling. The shard is
> already on every input row — `BuildBatchSchema` puts `RoleShardID` in its fixed prefix
> (`integration.go:64-69`) — so this is one entry in a slice, not new plumbing.
>
> This does not replace replica dedup; it is the precondition that makes it correct. Once each
> partial *is* one shard's contribution, "the replicas are all the same, so keep one" becomes
> literally true of the rows being deduped, and `markDedupSeen` does exactly the right thing.

**(b) The shard id is not unique across stages.** Where more than one stage is in play, hot and
warm nodes answer one broadcast and both report shard 3 for group `g` with **different, disjoint**
data. They are not replicas of each other, so deduping on `(shard, group)` would drop one — an
undercount, the opposite failure from (a).

This is about *replica identity only*. It is emphatically **not** an argument that stage-local
distinct counts can be summed once the stages are distinguishable; they cannot, which is why
multi-stage `COUNT_DISTINCT` is rejected outright above. The identity matters here because
`SUM`/`COUNT`/`MEAN` — which *are* additive across stages — run through the same reduce and are
not rejected.

Note also that no stage identity exists in the partial wire schema today
(`vectorized/measure/frame/frame.go:119-175`), so it has to be added for the reduce to key on it.

> **Fix:** the identity is `(stage, shard, group_key)`, for the additive functions. The stage is
> known at broadcast time and must travel with the partial.

**(c) The liaison then dedups and sums**, which is `AggModeReduce` unchanged apart from the key:

```
per-node partials ──▶ drop repeats of (stage, shard, group)   ← replicas: identical data, idempotent
                  ──▶ SUM survivors per group                 ← shards: disjoint in value space
                                                                 (stages: additive functions only;
                                                                  DISTINCT is single-stage by rule)
```

`markDedupSeen` (`aggregation_reduce.go:83`) already builds `LE64(shardID) ++ groupKey`; it needs
the stage prepended and the guarantee from (a) that the shard field is exact.

**Cost.** Partials are one int64 per `(stage, shard, group)` **that holds data** — a group whose
values live on 3 shards emits 3 rows, not `shard_num` rows. Wire volume is therefore independent
of cardinality: 1M distinct values cost at most `shard_num` integers per group.

> ⚠️ **The fix ships with this work; the investigation runs alongside it.** Both (a) and (b)
> describe today's `SUM`/`COUNT`/`MEAN` reduce, not something `COUNT_DISTINCT` introduces — so if
> the analysis holds, a replicated cluster (`replicas ≥ 1`) is already mis-aggregating every
> distributed `agg` query, silently and non-deterministically.
>
> That does **not** make it a sequencing blocker. Making the partial per-shard is a small,
> self-contained change that is correct whether or not the defect reproduces today: it costs one
> extra entry in `keyIndices` and bounded extra partial rows, and it is the precondition that
> makes replica dedup mean what it says. Build it as part of stage 5 rather than waiting on an
> investigation to authorise it.
>
> Run the experiment in parallel, because its outcome decides something the code change does not
> — whether this is *also* a live defect that existing deployments need told about:
>
> - stand up 4 data nodes, 4 shards, `replicas=1`, write a known dataset, and compare
>   `SUM(value)` from a distributed `agg` query against the same sum computed from a raw
>   (non-agg) scan of the same range;
> - repeat at `replicas=0`, where the analysis predicts agreement.
>
> Divergence at 1 and agreement at 0 confirms it. If it reproduces, the per-shard change is not
> merely a refinement — it is a correctness fix for `SUM`/`COUNT`/`MEAN` that warrants its own
> entry in `CHANGES.md`, and possibly a patch release, independently of this design. Same code
> either way; a materially different thing to communicate.

#### Composition and touch points

`groupKeys` above is whatever §7.2 produced — with `time_bucket` set it is `[bucket] ++ tagKeys`,
so the bucket is just another leading key column and the two features compose without a special
case. Push-down composes especially well with the streaming design: the node's distinct set is
released when a bucket closes, so it emits a scalar per `(bucket, group)` and never holds more
than one bucket's values.

Touch points: `distributedAggFunc` (`:1142`) gains an entry mapping `COUNT_DISTINCT` to the
existing SUM reducer; `BuildOperators` (`plan.go:61`) builds the local distinct slot in the map
phase and adds the shard to `keyIndices`; `markDedupSeen` takes the stage; the analyzer gains the
condition check and its rejection. `nodeTemplate.Limit` is already `MaxUint32` whenever
`Agg != nil` (`:222`), which remains harmless here since partials are per-group scalars.
### 7.5 Ranking, ordering and paging

**Ranking is unchanged.** `ApplyTopToReduce` (`reduce.go:290`) resolves `RoleField && Name ==
top.FieldName` on the reduced batch, and the aggregate result is exactly such a column;
`offset`/`limit` are applied by `iteratorFromBatches` (`distributed.go:988`). The ranked case
therefore falls out of §7.4 with no extra work.

**Node-local ordering needs no sort pass — but that is the only place it's free.** Because §7.2
streams buckets in ascending order, and `BatchAggregation` already emits groups in insertion
order (`BatchAggregation.insertion`), one node's own bucketed output is bucket-ascending by
construction, with no extra pass. This is the second dividend of streaming: a sort over
`buckets × tagGroups` rows would have been cheap in absolute terms but would have re-imposed a
blocking stage on an otherwise pipelined query, forfeiting the latency win. The third dividend is
that the bucket-advance check this operator already performs is exactly the span boundary
run-folding needs (§7.7).

**An earlier version of this section claimed the liaison merge and index mode inherit that same
ordering "by construction." They do not, and implementing §7.2 surfaced both gaps:**

- **The liaison reduce.** `ReducePartialBatches` consumes each node's partial fully before moving
  to the next, so `BatchAggregation.insertion` ends up only *piecewise* ascending — node A's
  `[2000, 3000]` followed by node B's `[1000, 2000]` inserts as `[2000, 3000, 1000]`, not merged.
  `iteratorFromBatches`'s offset/limit pagination needs the globally merged order, not the
  per-partial one.
- **Index mode's map fallback.** §7.2's non-streaming path holds one persistent aggregator across
  the whole scan precisely because its input may not be time-ordered — so its insertion order is
  first-seen, not bucket order, by design.

Both are fixed the same way: `BatchAggregation.SortInsertionByBucket()` stable-sorts
`a.insertion` by each group's captured bucket timestamp immediately before it is drained (once in
`ReducePartialBatches`, after `Finalize`; once in `BatchTimeBucket`'s map-mode terminal drain). It
is a no-op for a streaming-mode instance, since every group in one such instance already shares
the same bucket by construction — so the fix costs nothing on the already-correct path and only
does real work where ordering was not actually free. **Any future aggregate that reuses this
reduce path (COUNT_DISTINCT included — see §7.4, which pushes down through the same
`AggModeMap`/`AggModeReduce` machinery) inherits this fix automatically and must not re-introduce
the "ordering is free" assumption.**

Ordering and `top` interact in the obvious way — `top` selects globally (§3), then the surviving
rows are emitted in bucket order. A client wanting "the global top 20 devices, plotted over time"
gets exactly that; a client wanting per-bucket top-N wants the TopN RPC.

### 7.6 Memory safety

A distinct set grows with cardinality, unlike the fixed-size numeric slots, so it must be charged
**per inserted value**, not per group. `BatchAggregation.Consume` already reserves `entrySize`
per new group from the shared `MemoryTracker` (`aggregation.go:326-333`); the distinct path
additionally reserves on each `added` value and refunds the total in `Close`. A high-cardinality
target then fails with the existing `"aggregation memory budget exceeded"` message against the
256 MiB default (`VectorizedConfig.QueryMemoryMiB`).

**This also requires fixing `reduce.go:133`, which passes `entrySize=0`** — the liaison reduce is
unbudgeted today. That is tolerable for scalar partials and not tolerable for sets, since the
liaison is exactly where the union of every node's distinct values lands. Note this is a
**node-side** concern specifically. Because §7.4 pushes down, the liaison holds no sets at all —
one int64 per `(stage, shard, group)` — so the sets that need charging live only on the nodes,
where the scan-sized state already is.

**Time bucketing does not multiply the live group count**, because §7.2 streams: a bucketed query
holds one bucket's groups, not `buckets × tagGroups`. This is the main reason the streaming shape
matters rather than being a mere optimisation — the naive map would have made a 24-hour/1-minute
query cost 1440× the unbucketed group count, and for `COUNT_DISTINCT` each of those 1440× groups
would have held its own set.

What bucketing *does* change is the shape of the peak: the live set is now the widest single
bucket rather than the whole window. A bucket with pathological cardinality still fails the
budget, and that is the correct outcome — but it fails on one bucket's data, which is both a
smaller number and a far more interpretable error than "the query as a whole was too big".

Two bounds keep this defensible:

- **Per bucket**, live groups ≤ distinct `(tags…)` in that bucket ≤ rows in that bucket. The
  aggregation can never hold more than the scan hands it for the bucket it is on.
- **Across the window**, bucket count is `time_range / width`. Streaming means that no longer
  governs memory, but it still governs *output* rows, which is what `limit` must accommodate
  (§10). A very small width against a long window is therefore a paging problem rather than a
  memory one.

---

### 7.7 The local scan: entity and indexed tags never touch the data file

A `COUNT_DISTINCT` over a tag, grouped by a bucket and other tags, can in the common case be
answered **without decoding a single tag column** — because the tag values are already resident
from the series index, and the only thing the scan needs from the part is which series appear in
which bucket.

**The projection is split before the data file is reached.** `searchSeriesList`
(`query.go:314-356`) sorts every requested tag into one of three buckets:

| Tag kind | Resolved from | Reaches the block cursor? |
|---|---|---|
| **Entity tag** | `projectedEntityOffsets` → `SeriesList[].EntityValues` (`query.go:486-493`) | **no** — `continue TAG` |
| **Indexed tag** (`is.fieldIndexLocation`) | `indexProjection` → the series index `fieldResult` | **no** — `continue TAG` |
| anything else | the part's tag columns | yes, via `newTagProjection` |

Only the third category is appended to `newTagProjection`, and only `newTagProjection` becomes
`bc.tagProjection`. So `unmarshalTagFamily` never reads a column for an entity or indexed tag,
and `copyAllTo` (`block.go:535-552`) takes the value from `indexValue[tagName]` and skips the
block column outright. Both kinds are series-scoped — one value replicated across the block's
rows — which is precisely why they can live outside the data file at all.

**What a pure index-resolved aggregation actually reads** — valid only when *every* required tag
(group keys, aggregation target, and any tag a residual predicate touches) resolves from the
series index. One part-resident tag anywhere in the query reinstates the `.tfm`/`.tf` reads for
its family, so this is a property of the whole query, not of the aggregation target alone:

| File | Read? |
|---|---|
| `sidx/` (series index) | once per segment — series list plus projected index values |
| `meta.bin` | at part open only; the primary-block index stays resident |
| `primary.bin` | selectively, per matching series |
| `timestamps.bin` | per block — but see below |
| `<family>.tfm` / `.tf` | **never** |
| `field.bin` | **never** — this query shape projects no field |

**It goes one step further than "metadata plus timestamps".** `blockMetadata` carries
`seriesID`, `timestamps.min`, `timestamps.max` and `count`, all resident after
`readPrimaryBlock` with no data-file read. When `bucketOf(min) == bucketOf(max)` the whole block
lies inside one bucket, so the series' presence in that bucket is known from metadata alone and
`timestamps.bin` need not be read either — **but only if the block is also fully contained in the
query's time range**. Block selection is range-*aware*, not range-*clipped*: it returns any block
whose `[min,max]` overlaps the requested range (`query.go:594-629`). A block with rows at 09:00
and 09:59 occupies a single hourly bucket, yet a query for 09:30–09:31 matches neither row.
Testing `bucketOf(min) == bucketOf(max)` alone would report a bucket that has no matching data. The scan degenerates to:

```
for each block matching (series filter, time range):
    group key  ← seriesID → routing tag values      (series index, already in hand)
    bucket(s)  ← [min,max] ONLY IF the block is fully inside the query range
                 AND min/max share a bucket AND no residual predicate can exclude rows;
                 otherwise read timestamps.bin and filter
    count distinct seriesID per (bucket, group)
```

All three conditions are required. Dropping the containment check reports empty buckets;
dropping the predicate check reports buckets whose rows are all filtered out.

For `COUNT_DISTINCT` over a tag that is **functionally determined by the series** — an entity
tag, which every series pins by construction — this is exact by identity: counting distinct
values *is* counting distinct series, and a series either appears in a bucket or it does not. It
does **not** extend to an arbitrary index-resolved tag: an indexed non-entity tag is
series-constant, so it is still readable without the data file, but several series may share a
value, and the count must then go through the distinct set rather than the series count.

Four things bound the optimisation, and they should be settled before it is built:

- **Straddling *or* partially-selected blocks still need `timestamps.bin`.** A block spanning
  09:00–11:00 with no rows in the 10:00 bucket must not report presence there, so `[min,max]`
  cannot be over-approximated into a bucket range; and a block only partly inside the query range
  must be filtered rather than assumed present. How much the shortcut actually buys therefore
  depends on bucket width against block span *and* on how often a block sits wholly inside the
  requested range — which is why the payoff is worth measuring before this stage is built rather
  than assumed from the bucket width alone.
- **It applies to `COUNT` and `COUNT_DISTINCT` only.** `SUM`/`MIN`/`MAX`/`MEAN` over a field
  obviously need field data, and over a non-entity, non-indexed tag any function needs the tag
  column.
- **`blockMetadata.count` is not a row count for `COUNT`.** It counts stored entries including
  superseded versions, while the query path dedups `(seriesID, timestamp)` keeping the highest
  version (`queryResult.merge`). Using it directly would over-count duplicates. The identity
  holds for distinct-series counting, which is version-insensitive, and fails for row counting —
  a sharp distinction worth encoding in a test.
- **It is a separate execution strategy, not a tweak.** The pipeline today decodes blocks into
  `MeasureResult` rows and feeds the vec operators; this bypasses block decode entirely. Bounded
  work, but a new path with its own correctness surface — so it belongs after the main design
  lands, gated on a differential test against the decoding path.

#### Intermediate step: fold by run, not by row

The metadata-only scan above is the end state. There is a cheaper step on the way to it that
needs **no new execution strategy at all**, only a change inside the aggregation operator — and
it removes most of the same waste.

An index-resolved tag is series-constant, but nothing downstream knows that, so the single value
is multiplied out three times before it is folded:

1. **Replicated.** `copyAllTo` expands one value into `n` pointers —
   `for i := 0; i < size; i++ { t.Values[i] = indexValue[tagName] }`.
2. **Materialised.** `appendTagValueAsTyped` appends it into the native column one row at a time,
   so the column physically holds `n` entries of the same value.
3. **Folded.** `BatchAggregation.Consume` is row-oriented: per row it calls `computeKey`, probes
   `a.groups[key]`, and folds. `computeKey` ends in `return string(buf)`, and that string is
   stored in the group map, so it escapes — **a heap allocation per row**. Go's
   `m[string(b)]` no-allocation optimisation does not apply across the function boundary.

So a block costs `n` × {key encode, string allocation, map hash + probe + compare, fold}, and
from the second row onward every one of those reproduces an identical result. For
`COUNT_DISTINCT` the `n−1` redundant set inserts are idempotent no-ops that still pay a hash and
a probe each. `n` here is rows per series per block, bounded by `maxBlockLength = 8192`
(`banyand/measure/measure.go:46`).

**Two facts make run-folding almost free to add:**

- **A batch is single-series.** `BuildMeasureBatchFromResult` consumes exactly one
  `model.MeasureResult`, and `queryResult.merge` returns as soon as the seriesID changes — so
  `seriesID` never varies inside a batch and needs no checking.
- **§7.2 already finds the boundaries.** The streaming operator walks the timestamp column
  looking for bucket advances. Because input is timestamp-ascending and the series is fixed, the
  rows form *contiguous* spans of constant `(series, bucket)` — and a bucket advance is exactly a
  span boundary. The detection is already there; only the folding is per-row.

**The change:** for each maximal span sharing `(series, bucket)`, compute the key once, resolve
the group once, and fold the span in one operation.

| Aggregate | Per span |
|---|---|
| `COUNT_DISTINCT` over a series-constant target | **one** set insert |
| `COUNT` | `count += spanLength` |
| `SUM` / `MIN` / `MAX` / `MEAN` over a varying column | still per row — but the key work is saved |

**Magnitude.** 8192 rows of 1-minute data is roughly 5.7 days inside one block; at hour buckets
that is about 137 spans, so 8192 key computations, allocations and map probes collapse to ~137.
Unbucketed, 8192 collapses to 1.

**What it needs:** a plan-time flag marking which key columns are index-resolved, which
`searchSeriesList`'s three-way split already determines — the information exists, it simply is
not carried forward today. Correctness rests on the series-constancy guarantee, so the flag must
be derived from that split rather than inferred by sampling column values at runtime; a column
that merely *happens* to be constant in one batch must not take this path.

Ordering relative to §7.7's metadata-only scan: this step is strictly cheaper than today, strictly
simpler than the metadata-only scan, and the two are compatible — run-folding helps every
aggregation whose keys are index-resolved, including the ones that still have to read a part-file
column for the aggregate itself, which the metadata-only path cannot serve at all.

**`index_mode` measures are this section's structural case.** They hold no part-resident tags at
all — `buildIndexQueryResult` errors on any projected tag that is neither an entity offset nor an
indexed field — so an index-mode aggregation is index-only by construction rather than by
fortunate schema design. Two consequences: the `.tf`/`.tfm` rows of the table above do not merely
go unread, they do not exist; and the "straddling blocks" bound does not apply, since there are
no data blocks to straddle. Index mode should therefore be the first place the metadata-only path
is built and measured.

Note the asymmetry with §7.2: index mode is the *best* case for scan cost and the *worst* case
for ordering, since it bypasses the timestamp-ordering default entirely. It reads least and
streams least.

One caveat for run-folding specifically: index mode materialises one row per index entry
(`copyTo` builds a single-timestamp `MeasureResult`), so the multi-row single-series batch that
run-folding collapses may not arise on that path. Run-folding targets the part-file scan; the
batch-oriented `PullBatch` path for index mode should be checked before assuming it benefits.

This is also an argument for the §7.4 modelling guidance from the other direction: putting the
distinct target in the routing key does not merely make the query *pushable*, it makes the local
scan metadata-only.

## 8. The deprecated row engine

**This section is stale — the package it describes no longer exists.** It originally said
`pkg/query/logical/measure` would reject all three new shapes (`agg.tag_name`, `COUNT_DISTINCT`,
`group_by.time_bucket`) with a `--measure-vectorized-enabled=true` message, on the reasoning that
implementing parity in the row iterators would roughly double the work to protect a rollback path
that was itself being retired.

That rollback path was removed outright — `chore: remove the row-based query path from measure,
stream and trace` (#1326) deleted `pkg/query/logical/measure` entirely, *before* this design doc
was even merged (#1326 landed 2026-09-16; #1360 merged 2026-09-17). There is no row-path analyzer
left to add a guard to, for any of the three shapes, because there is no row path. The
`--measure-vectorized-enabled` flag itself is now a no-op compatibility shim
(`removedRowQueryFlag` in `banyand/measure/measure.go`): `=true` (or omitting the flag) is
accepted; `=false` hard-fails at startup with `"row-based query was removed in 0.12.0, see
apache/skywalking#13998"`. The vectorized engine (`pkg/query/vectorized/measure`) is the only
engine, and each new shape's validation lives directly in its analyzer
(`pkg/query/vectorized/measure/plan/analyzer.go`) rather than in a parallel rejection guard.

---

## 9. Alternatives considered

| Alternative | Why rejected |
|---|---|
| **Aggregate inside the native inverted index** | The engine already aggregates locally on data nodes, so this would not reduce network cost — it only moves which layer owns local aggregation, while duplicating grouping, arithmetic, memory accounting and distributed reduction. Estimated 5–8+ engineer-weeks against 2–4 for extending the engine. |
| **HyperLogLog partials** | Constant wire cost and no decomposability condition to check, but it needs a new partial wire type and an API contract about error bounds. §7.4 already gets cardinality-independent wire *exactly* whenever the condition holds, which covers the motivating shape — so a sketch would buy only the queries §7.4 rejects, and would buy them approximately. That is the right thing to revisit if those rejections turn out to matter, and the wrong thing to build speculatively. |
| **A value-shipping fallback** for queries that fail the condition — nodes ship distinct values, liaison unions | Always correct and needs no condition check, but its wire volume and liaison memory scale with cardinality, so a single query can funnel every value into shared infrastructure. It also doubles the surface: two implementations, a static chooser, opposite replica-dedup requirements, and a differential test to keep them honest. Rejecting is consistent with how this design treats every other unsupported shape. §7.4. |
| **Ship per-node counts and sum them** | Wrong. A value appearing on two nodes is counted twice; that is the same class of bug as the multi-bucket `COUNT(*)` this design removes. |
| **Emit an explicit set-typed partial column** | Same wire volume as shipping distinct tuples, but needs a new column type, a new frame wire type, and a new `Partial` shape. Reusing the group-by shape costs nothing extra. |
| **Solve it upstream instead** — change SkyWalking to store labelled metric values as tags | An OAP core change touching all three storage plugins, and it multiplies row count by label cardinality — the packed representation exists precisely to prevent that. Out of scope here regardless, since it does not remove the need for `COUNT_DISTINCT`. |
| **Requiring the width to be a multiple of `Measure.interval`** | Rejected after the fact that motivated it turned out not to hold: `interval` is a declared write cadence, not a storage invariant, so data points do not reliably land on interval boundaries. With arbitrary timestamps every width bins just as meaningfully as any other, and the rule would refuse useful queries to protect an alignment that was never guaranteed. §5.3. |
| **A `multiplier` integer instead of a duration string** | Makes invalid widths unrepresentable, but `multiplier: 5` cannot be read without the measure schema in hand — it means 5 minutes or 5 hours depending on the measure. A duration string matches `Measure.interval`'s own encoding, parses with the same helper, and matches the `INTERVAL <width>` spelling the bucketing functions in §1 use. Validation is one modulo. |
| **Bucket on `segment_interval`** rather than `Measure.interval` | `ResourceOpts.segment_interval` is an `IntervalRule` restricted to HOUR/DAY and describes physical segmentation, not data-point cadence. It is both too coarse and semantically unrelated. |
| **Hash-map the bucket** — add it to `keyIndices` and hold every `(bucket, tags…)` until the scan ends | The path of least resistance, and wrong for a time-series database. It holds `buckets × tagGroups` live groups (1440× for a day at 1-minute), blocks all output until the last row, and then needs a sort to restore time order — all three paid for information the scan already provides, since measure results arrive timestamp-ascending by default (`query.go:286`). §7.2 streams instead. |
| **Reuse the TopN RPC for series** | `TopNResponse.lists` is already a per-timestamp series, but the RPC ranks a single pre-aggregated field by a pre-declared `TopNAggregation` rule. It cannot express an ad-hoc `COUNT_DISTINCT` over a tag, and it requires schema registration ahead of the query. |

---

## 10. Compatibility and follow-ups

- **Wire.** All three API additions are additive. Old clients never set `tag_name` or
  `time_bucket`, and never receive a `COUNT_DISTINCT` response.
- **No new configuration.** `COUNT_DISTINCT` either pushes down or the request is rejected, so
  there is no flag, no default to choose and no second path to keep warm. The §7.4 per-shard
  partial ships as part of that single path rather than behind a gate, and is correct on its own
  terms; only the question of whether it *also* fixes a live defect in `SUM`/`COUNT` is deferred
  to the parallel experiment.
- **Mixed-version clusters.** A liaison that sends a `COUNT_DISTINCT` request to an older data
  node gets the node's existing unknown-function error rather than a wrong number, because
  `protoAggFuncToInternal` (`plan.go:167`) rejects enum values it does not know. **`time_bucket`
  has no such guard** — an old node silently ignores the unknown proto field and returns
  whole-range partials, which the liaison would otherwise reduce into one row per tag group with
  no error: a series collapsed to a point, silently.

  The check is **self-describing**, needing no capability negotiation. A bucketed query's partial
  carries a `RoleTimestamp` column, because §7.2 reverses D2 exactly when a bucket key is
  present. A node that ignored the field emits partials *without* that column, so the liaison
  detects the absence directly and hard-errors. This is deliberately not a node-version or
  capability lookup: `database.v1.Node` carries only `metadata`, `roles`, addresses and a generic
  `labels` map, so no such mechanism exists to consult — and inventing one to answer a question
  the data already answers would be the wrong trade.
- **`limit` with a bucketed query.** Result rows are `buckets × tagGroups`, so the default
  `limit` of 100 (`measure_analyzer.go:32 defaultLimit`) truncates a 1-hour/1-minute/50-device
  series at 100 of its 3000 rows. The default is left alone — changing it would alter unbucketed
  queries too — but clients must set `limit` explicitly, and this needs to be prominent in the
  BydbQL docs rather than buried in the API reference.
- **Possible follow-up: multiple aggregates, then aliases.** Out of scope here by decision (§3),
  and recorded only so the route stays visible if the remainder of §1.1 is ever wanted.
  `AggSpec` is already a slice and `BatchAggregation` already loops over `a.aggs`, so the
  constraint is the singular `QueryRequest.agg` field and BydbQL's one-aggregate rule — API and
  planning, not execution. Aliases need a BydbQL `AS` production and an
  `Aggregation.output_name` field together, and the two share one question: what a result column
  is called when there is more than one.
- **Follow-up: per-bucket `top`.** If demand appears, the honest move is to extend the TopN RPC
  rather than overload `QueryRequest.top`, since TopN already returns a per-timestamp series.
- **Follow-up: gap filling.** Needs a declared bucket set and a zero value per aggregate; SUM and
  COUNT fill with 0, MIN/MAX/MEAN have no defensible fill.

### Fixed: adjacent inconsistency in the liaison-side key resolution

`reduce.go resolveKeyIndices` used to match key tags by `Name` only, ignoring `TagFamily`, unlike
`plan.go:140` and `distributed.go:945`, which both compare the family. An earlier version of
this note claimed that was safe because "tag names are unique per measure" — that directly
contradicts §5.1 above, which debunks exactly that assumption (tag-family validation does not
reject a name repeated across families, and `pkg/query/vectorized/schema_test.go` exercises it
as a valid case). A bucketed or tag-keyed distributed reduce over such a schema could silently
bind to the wrong family's column. Fixed by threading the key's `TagFamily` alongside its names
end to end — `distributedGroupByTagKey` (`distributed.go`) now returns `(family, names)` instead
of discarding the family, and `resolveKeyIndices` matches on `(family, name)`, same as `plan.go`
and `distributed.go`'s row-merge path.

---

## 11. Testing strategy

**Unit**

- `pkg/query/aggregation` — **this package has no tests at all today.** Add set semantics, reset,
  and the `NewMap`/`NewReduce` rejection that guards the TopN path.
- `vectorized/measure/aggregation_test.go` — its `aggIntSchema()`/`aggFloatSchema()` fixtures use
  a `RoleField` agg column exclusively; they need a tag-input sibling. Cover the numeric
  functions over an INT tag, `COUNT` over a string tag, `COUNT_DISTINCT` over string/int/bytes,
  null exclusion, budget exhaustion on a high-cardinality set, and the distinct-tuple map shape.
- `vectorized/measure/plan_test.go:73-78` enumerates the five functions explicitly and must gain
  the sixth; likewise `test/cases/measure/cmd/generate/layer3_features.go:197`, which hard-codes
  `{"MEAN","MAX","MIN","COUNT","SUM"}` in the generated feature matrix. Both under-cover a new
  function silently — the `exhaustive` linter will not catch either, because the enum switches
  have `default` branches.
- `plan/analyzer_test.go` — the full §6 rejection matrix, plus implicit projection and `HideTag`.
  For bucketing, the §5.3 table is the test table: width from the request; width absent but
  interval present; both absent (reject); unparseable, zero and negative widths (reject). A width
  that is *not* a multiple of the interval must be **accepted** — that is the case the removed
  rule would have wrongly rejected.
- `plan/distributed_test.go` — node template, liaison reduce over synthetic frames, replica
  idempotence, and `top` + `offset` over the distinct result.
- **The `COUNT_DISTINCT` push-down condition (§7.4) is the thing to test hardest**, because a
  mis-evaluated condition does not crash — it returns an inflated count that looks entirely
  plausible. There is no fallback path to diff against, so the tests must pin the predicate
  directly and pin the rejection.
  - **Accept/reject table over schemas**, driven by `routingTags` not entity tags:
    `routingTags ⊆ groupBy ∪ target` (accept); a routing tag in neither (reject); `index_mode`
    with the same containment (**accept** — §7.4); an unresolvable tag, routing key or stage
    (reject — indeterminate must not be permissive, and must not panic).
  - **The sharding-key counterexample**, which an entity-based rule accepts and must not: entity
    `{e}`, sharding key `{s}`, rows carrying the same `e` under two `s` values, and
    `COUNT_DISTINCT(e)` with no grouping on `s`. Must reject. Summing scalar partials here
    returns 2 for one distinct entity.
  - **Multi-stage rejection**: the same query must be rejected when the resolved stage set has
    more than one entry, bucketed or not — and the resolution must expand `DefaultStages`, not
    only an explicit `stages` list. Pair it with the positive case: the same target in two
    *disjoint buckets* of a single stage must count once per bucket and must **not** be rejected
    for that reason.
  - **Ambiguous tag name**: two families each containing `region`; an aggregation naming `region`
    must resolve by its family qualifier, and an unqualified reference must fail naming both
    families rather than silently binding to whichever family `RegisterTag` wrote last.
  - **The rejection is a contract, not an implementation detail.** Assert the error is returned
    at analyze time, before any node is contacted, and that its text names the target, the
    entity, and the two ways out. A test on the message is warranted here because the message is
    the entire remediation path for a user who has hit it.
  - **An overlapping fixture must be rejected, not silently summed**: the same distinct value
    present on two shards for one group. This is precisely the data shape that makes push-down
    wrong, so what is under test is that the condition catches it rather than that the arithmetic
    handles it.
  - **Replica duplication must be deduped**: feed the same `(stage, shard, group)` partial twice
    and assert the count does not double. Then feed two partials differing only in shard, and
    assert they *do* add — the two halves of §7.4(c), which fail in opposite directions.
  - **Standalone/distributed agreement**: the same query and fixture must return the same number
    single-node and across nodes. Single-node has no sharding, so this is the cheapest end-to-end
    check that the partition arithmetic is right.
  - **Replicated-topology agreement** (§7.4): the same query and fixture must return the same
    number at `replicas=0` and `replicas=1`. This is the regression test the per-shard partial
    exists for, and it is the one that would have caught the defect the 4a experiment is
    investigating — so it belongs in the suite permanently, not just in the investigation.
- **Bucketing needs its own unit coverage**, and two properties matter more than the happy path:
  - `bucketStart` agrees with `getWindowStart` (`sliding_window.go:302`) across a table of
    timestamps and widths, including a boundary timestamp (`ts % width == 0` stays put) and a
    negative one. Asserting the two formulas agree keeps the query path and the TopN
    pre-aggregation path from drifting into two definitions of "the 5-minute bucket".
  - `plan/build_test.go:221` currently asserts the output schema drops the timestamp. That
    assertion must stay for the unbucketed case and gain a bucketed sibling asserting the
    timestamp **is** present and carries the bucket start — the two together pin D2 as
    conditional rather than repealed.
  - **`COUNT` and `COUNT_DISTINCT` must be allowed to disagree at the interval width.** An
    earlier draft proposed asserting they are *equal* when bucketed at exactly
    `Measure.interval`, on the belief that storage holds one row per `(entity, bucket)`. It does
    not — the interval is a declared write cadence (§5.3) — so that test would fail on perfectly
    legal data. Invert it: seed one entity with two off-cadence points inside one bucket
    (`09:01:00` and `09:01:34` on a 1-minute measure) and assert `COUNT` reports 2 while
    `COUNT_DISTINCT` reports 1. That is the whole reason the function exists, and it is the
    fixture a naive implementation passes only by accident.
- **The §7.7 metadata shortcut needs its range guard tested**: one block with rows at 09:00 and
  09:59 (a single hourly bucket) queried over 09:30–09:31 must yield **no** bucket row; the same
  block fully inside the range must take the shortcut. Without the first case the optimisation
  reports buckets that contain nothing.
- **Index-mode ordering** (§7.2): index-mode rows whose index order is deliberately
  non-chronological — timestamps arriving A, B, A — must still produce one result per bucket. This
  is the case that proves index mode took the map rather than the streaming operator, and it fails
  loudly if someone later "optimises" it onto the streaming path.
- **The streaming operator needs tests for the property it depends on, not just its output**
  (§7.2):
  - **Bucket closure**: after the input advances past bucket *n*, the operator has released
    bucket *n*'s groups. Assert on live state, not just emitted rows — a version that emits
    correctly while retaining everything passes an output-only test and fails the whole point of
    the design.
  - **Monotonicity guard**: feed deliberately out-of-order timestamps and assert a loud error
    rather than duplicate rows for a reopened bucket. This is the silent-corruption path, so it
    needs a test that would fail if someone later removes the check as redundant.
  - **Pipelining**: the first bucket's output is available before the source is exhausted. A
    `BreakerOperator`-shaped implementation passes every correctness test and fails this one.
  - **Ordering assumption**: a test pinning `applyMeasureQueryOrdering`'s `Order == nil` →
    `orderByTS = true` default (`query.go:286`), since the streaming operator's correctness rests
    on it and nothing in the vec package would otherwise notice if that default changed.

**Integration** — `test/cases/measure/`

Cases are `input/<name>.yaml` + `input/<name>.ql` + golden `want/<name>.yaml`, registered as a
`g.Entry` in `measure.go`; `data.go verifyQLWithRequest` proto-compares the QL transform against
the YAML request, so both inputs are required.

The load-bearing case is a **multi-bucket cardinality fixture**: one user with 3 api-keys spread
across 3 hour buckets, a second user with 2, asserting `3` and `2` for the scalar case and the
correct ordering for the ranked, paged case. It must run in the **distributed** suite
(`test/integration/distributed/query/vectorized_test.go`, liaison + 2 data nodes) as well as
standalone — the cross-node union has no single-node analogue, and a fixture confined to one
bucket is exactly what lets the naive `COUNT(*)` over-count slip through unnoticed.

The same fixture serves the series case at a second bucket width, and that is the point of
reusing it: at a 1-hour bucket the same data must yield one row per `(hour, user)` whose counts
sum to neither 3 nor 2, while at whole-range it must yield exactly 3 and 2. A fixture that only
ever asks one width cannot catch a bucket key that is computed but ignored.

**The decisive test for bucketing is a single-bucket decomposition check.** The oracle is
constructed *by the test*, not borrowed from any client: a bucketed query over a window must
equal the union of one-bucket-wide queries over the same window, because each of those is exact
by the storage invariant (§1, workaround 1).

```
for each bucket B in window:                        one bucketed query over the window
    query(time_range = B, group_by = tags)    ==    (group_by = TIME_BUCKET(w), tags)
```

Run both against the same fixture and compare row-for-row, keyed by `(bucket, tags…)`. This is
worth more than any hand-written golden file, because each single-bucket query is independently
trustworthy — it is the one shape the engine already answers exactly — so the comparison has a
real oracle rather than a recorded expectation. It catches the whole class of off-by-one
bucket-boundary errors that a golden file would simply enshrine. It also gives any caller
currently issuing per-bucket queries a migration check they can run themselves before switching
to a single bucketed query.

Golden files must be **timestamp-stable**. `data.go` ignores timestamps when comparing, which is
precisely the field a bucketed result asserts on, so these cases need the comparison to include
it — otherwise the suite would pass with every bucket collapsed to zero.

**Manual**

```sql
-- Scalar case — one user, 3 buckets. Must return 3, not 6.
SELECT COUNT(DISTINCT entity_id) FROM MEASURE … WHERE service_id = '<user>' TIME > '-3h';

-- Ranked case — all users, ranked, paged.
SELECT service_id, COUNT(DISTINCT entity_id) FROM MEASURE … TIME > '-3h'
GROUP BY service_id ORDER BY entity_id DESC LIMIT 20;

-- Series case — one row per (5-minute bucket, host).
SELECT host, COUNT(host) FROM MEASURE … TIME > '-1h'
GROUP BY TIME_BUCKET('5m'), host LIMIT 1000;
```

---

## 12. Delivery order

Each stage compiles and is testable on its own.

| # | Stage | Principal files |
|---|---|---|
| 0 | Proto (`COUNT_DISTINCT`, `agg.tag_name`, `group_by.time_bucket`), regeneration, `model.MeasureAgg` + `model.MeasureGroupBy` | `api/proto/banyandb/{model,measure}/v1/*.proto`, `pkg/query/model/model.go` |
| 1 | Tag binding for the existing five functions, standalone and distributed | `plan/analyzer.go`, `plan.go`, `integration.go`, `aggregation.go` |
| 2 | Time bucketing: `bucketStart`, `BatchTimeBucket`, streaming close + monotonicity guard on the part path, non-streaming map for index mode, conditional D2 reversal | `vectorized/measure/{aggregation,plan}.go`, `plan/analyzer.go`, new bucket operator |
| 3 | Time bucketing distributed: bucket key in the node template, then the liaison k-way merge on bucket | `plan/distributed.go`, `reduce.go` |
| 4 | `COUNT_DISTINCT` single node, with the per-value memory charge | `pkg/query/aggregation/distinct.go`, `aggregation.go`, `groupby.go` |
| 4a | **Runs in parallel with 5, does not gate it**: the 4-node/`replicas=1` experiment (§7.4). Its outcome decides whether stage 5's per-shard partial is also a correctness fix for existing `SUM`/`COUNT`/`MEAN` — i.e. whether it needs a `CHANGES.md` entry and a possible patch release | test harness; `CHANGES.md` if confirmed |
| 5 | `COUNT_DISTINCT` distributed: push-down condition + rejection, shard in the map-phase key, `(stage, shard, group)` dedup, SUM reduce | `plan/analyzer.go`, `plan.go`, `aggregation.go`, `aggregation_reduce.go`, `plan/distributed.go` |
| 6 | Guards: row-path rejection, §5.3 width resolution, §6 matrix, multi-stage rejection, ambiguous-tag rejection, TopN, the `time_bucket` staleness check | `pkg/query/logical/measure/measure_analyzer.go`, `plan/analyzer.go` |
| 7 | BydbQL (`COUNT(DISTINCT …)`, `TIME_BUCKET(…)`) and the `bydbctl` planner | `pkg/bydbql/{grammar,transformer}.go`, `bydbctl/internal/tui/planner/plan_types.go` |
| 8 | Tests and docs, including `index_mode` coverage in the accept/reject table and the integration fixtures | above, plus `docs/interacting/bydbql.md`; `docs/api-reference.md` is regenerated, never hand-edited |
| 8a | *Optional*: run-folding — fold each `(series, bucket)` span once instead of per row (§7.7). No new execution path; needs the index-resolved flag carried into the plan | `vectorized/measure/aggregation.go`, `plan/analyzer.go`, `plan/dispatch.go` |
| 9 | *Optional, after 8a*: metadata-only scan for entity/indexed-tag aggregation (§7.7), behind a differential test against the decoding path | `banyand/measure/query.go`, `part_iter.go`, `plan/dispatch.go` |

Stages 2–3 (time bucketing) and 4–5 (`COUNT_DISTINCT`) are independent of each other and both
build on stage 1. Either pair can ship first; the ordering above front-loads bucketing because it
is the smaller change and because it exercises the conditional-D2 output schema that stage 5 also
depends on. Nothing in the table waits on 4a: the per-shard partial it concerns is built in
stage 5 on its own merits, and 4a only determines how the result is *communicated*.

Generated protobuf code is gitignored and rebuilt by `make -C api generate`. The full gate is
`make pre-push`.
