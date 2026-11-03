# Native inverted-index stream soak — pilot findings

Findings from building and smoke-running the log-shaped native-index soak
(`scripts/soak-stream-index.sh`, fixture in `test/soak/cmd/stream_index.go`).

These are **defects in the native index query path**, found while building the
harness, not harness problems. They are reproducible against a seeded
`soak-idx-fixture` group, and they constrain what the soak can assert, so they
are recorded here rather than left in a chat log.

F1 is **fixed**; F2 through F7 are open.

---

## F1 — `BINARY_OP_MATCH` and `BINARY_OP_HAVING` returned zero rows for every scalar tag (FIXED)

**Severity:** high (full-text search over a stream tag is a documented feature)
**Status:** fixed in `pkg/query/logical/expr_literal.go` — see "Root cause" below.

Symptom on a live fixture, before the fix:

| query | field | analyzer | rows |
|---|---|---|---|
| `level EQ "ERROR"` | `level` | keyword | 20 |
| `level MATCH "ERROR"` | `level` | keyword | **0** |
| `pod MATCH "pod-003"` | `pod` | keyword | **0** |
| `level MATCH "no-such-value"` | `level` | keyword | 0 |

EQ worked on exactly the fields where MATCH returned nothing, so the failure was
specific to the MATCH path rather than to the field, the analyzer, or the index.

### Root cause

`pkg/query/logical/expr_literal.go` compared scalar literals by **pointer**
where they needed **value** comparison:

```go
func (s *strLiteral) Contains(other LiteralExpr) bool {
	if o, ok := other.(*strLiteral); ok {
		return s == o        // *strLiteral pointer comparison
	}
```

A literal carries no identity, so this only succeeds when both sides are the
*same allocation* — which never happens for a value read out of a batch. The
array branches of the same file already used `slices.Contains` (a value
comparison); only the scalar branches compared pointers.

That silently broke two operators, both of which route through the affected
comparisons:

- `BINARY_OP_MATCH` → `matchTag.Match` → `expr.Contains(...)` → always false
- `BINARY_OP_HAVING` → `havingTag.Match` → `expr.BelongTo(...)` → always false

`BINARY_OP_EQ` was unaffected because `eqTag.Match` uses `Equal()`, which
already compared values. That asymmetry is why the defect hid behind an
operator that demonstrably worked.

Fixed by comparing values in all four scalar comparisons:
`strLiteral.Contains`, `strLiteral.BelongTo`, `int64Literal.Contains`,
`int64Literal.BelongTo`.

### How the two wrong hypotheses were eliminated

Both of the initial hypotheses for this defect were wrong, and instrumentation
is what killed them:

1. *"The early-out at `searcher.go:106` fires because the query-side
   `index.FieldKey` loses the analyzer."* — Instrumenting `Searcher.Match` showed
   it was called with `analyzer="keyword"`, `ruleID=10`, and it did **not**
   early-out. It even returned hits (`hits=4` for the series owning the term).
2. *"The vectorized stream path does not implement MATCH over the element
   index."* — Also wrong: the index path was fine. The loss happened
   **downstream**, in the egress filter, where the scan-time predicate
   re-evaluated the same criterion and rejected every row.

A unit test at the filter layer (`pkg/query/logical/stream`) passed throughout,
which is itself the evidence that pointed at the scan-time path rather than the
index path.

### Coverage added

- `pkg/query/logical/expr_literal_scalar_test.go` — pins value comparison for
  the four scalar comparisons, and pins the array and cross-type behaviour that
  must not change. Each of the four fails against the unfixed code.
- `pkg/query/logical/stream/native_match_integration_test.go` — MATCH against a
  real native store for the keyword and simple analyzers, plus
  `OPERATOR_OR` / `OPERATOR_AND` semantics. There was previously **no** MATCH
  coverage for the stream filter at all, which is how a total operator failure
  went unnoticed.

### Verification

Same fixture, before → after:

| query | before | after |
|---|---|---|
| `level MATCH "ERROR"` | 0 rows | **20 rows** |
| `pod MATCH "pod-003"` | 0 rows | **20 rows** |
| `level MATCH "no-such-value"` | 0 rows | 0 rows |

MATCH now agrees with EQ, and a term that was never indexed still matches
nothing.

---

## F2 — element-index fields with a non-keyword analyzer are unqueryable

**Severity:** high (any analyzed index rule is dead weight)

`analyzeTerms` (`nativeadapter/searcher.go:483`) is a one-line delegation to
`nativeanalysis.Analyze`, which is also what the write path calls
(`nativeadapter/adapter.go:42`). So both sides tokenize identically — and yet
no query form finds an analyzed field's terms.

Direct A/B, changing only the analyzer on `ir_message` (id 13) between two
otherwise identical seeded runs, querying the identical exact stored value
`"exhausted to election after 33 ms"`:

| `ir_message` analyzer | query | rows |
|---|---|---|
| `simple` | `message EQ "exhausted to election after 33 ms"` | **0** |
| `keyword` | `message EQ "exhausted to election after 33 ms"` | **1** |

With `simple`, *every* form returns 0 — the exact stored value, individual
tokens (`EQ "exhausted"`, `EQ "ms"`), and `MATCH` on tokens. With `keyword` the
exact-value query resolves. Meanwhile `pod`, `level` and `trace_id` (all
`keyword`) work correctly throughout, so the write path, the schema rule
persistence (confirmed via `/api/v1/index-rule/schema/lists/{group}`, which
reports `"analyzer":"simple"`) and the field-key marshalling are all fine.

**The `nativeadapter` boundary is NOT the culprit, and this was initially wrong
in this document.** `pkg/index/nativeadapter/searcher_test.go:31`
(`TestSearcherMatchPreservesPerInputMatchGroups`) already covers exactly this
case and passes: it writes three documents through `Adapter.Batch` with
`index.FieldKey{Analyzer: index.AnalyzerSimple}`, then reads them back with
`Searcher.Match(key, []string{"alpha beta", "alpha gamma"}, OPERATOR_AND)` and
asserts the correct document comes back. So analyzed fields round-trip
correctly through write → store → `Searcher.Match` when the *same*
`index.FieldKey` is used on both sides.

That moves the suspect above the adapter. The live failure therefore has to be a
**field-key mismatch between the stream writer and the stream query planner**:
`banyand/stream/write_standalone.go:254` builds
`index.FieldKey{IndexRuleID: r.GetMetadata().GetId(), Analyzer: r.Analyzer, SeriesID: series.ID}`,
while the query builds its key from the resolved index rule via
`pkg/query/logical/stream/index_filter.go:205`. If those two disagree on any
component, the query addresses a native field name the writer never created —
which would return zero rows for every operator on every analyzed field, exactly
as observed. Comparing the two `FieldKey` constructions (including the `Type`
component, which neither snippet above shows) is the next step.

**Note on scope:** the `nativeadapter` unit test above already passes, so this
is a stream-layer defect, not an engine one. Reproduce it at the stream layer
(bake a schema with an analyzed rule, write through `StreamService.Write`, query
back) rather than at the adapter.

**Interim workaround in the pilot:** the fixture pins `ir_message` to
`keyword` and the catalog queries the message by exact value. This keeps a
shared-vocabulary term with a large posting (256 distinct values across the
fixture, so ~1/256 of elements per term) — which is the structural property the
soak wanted from full-text — but it does **not** exercise tokenization. Once F2
is fixed, switch `ir_message` back to `simple` and restore a `BINARY_OP_MATCH`
catalog entry.

---

## Harness notes

Two fixture parameters had to be corrected before the baseline was non-empty,
both caught by the fail-fast baseline gate rather than by a silent pass:

- **Duration distribution.** `exp(3.0 + 1.1·gauss)` put `duration > 1500` at
  ~4e-5, so the numeric-range query returned 0 rows at every fixture size the
  pilot can write. Now `exp(5.5 + 1.1·gauss)`: median ≈ 245 ms,
  `P(>1500) ≈ 5%`, `P(≥1000) ≈ 10%`, so the range and sort+filter thresholds
  land on real percentiles.
- **Message values.** The first revision interpolated the duration into the
  message, making every message unique — the field then held only 1-element
  postings and no addressable term. Now the message is drawn from a 16-word
  vocabulary with no random component: 256 distinct values, each posted by
  ~1/256 of the fixture.

Both are the shape of mistake the fail-fast Phase 0 gate exists to catch, and
both were caught by it on the first run.

## Unrelated pre-existing failure

`TestLoadStreamCatalogParsesAllShapes` (`test/soak/cmd/stream_test.go:36`)
fails on `main` (`fade662e`) independent of this work: it asserts the generic
`catalog/stream.json` has 4 entries, and it has had 6 since the stream catalog
was extended. Left untouched to keep this change focused.
---

## F3 — `countGoroutines` reported 0 against every current Go runtime (driver bug, fixed here)

`test/soak/cmd`'s goroutine counter only recognised the per-goroutine profile
shape (`goroutine N [state]:`). But `?debug=1` has emitted a **summary** header
(`goroutine profile: total N`) since well before Go 1.21 — per-goroutine stacks
are `?debug=2`. So the counter was parsing the wrong debug mode, not merely
reading an old version:

```
goroutine profile: total 167
42 @ 0x48de0e 0x41ca2e
```

so the counter returned **0** on every standalone run. Because
`scripts/soak-vectorized.sh` parses the same `total N` header in shell while the
Go helper did not, this was invisible to the existing harness and silently
disabled every goroutine-leak gate.

Fixed by reading the header first and keeping the legacy shape as a fallback.
The first fix still returned 0 for an unreadable body (an HTML error page served
with HTTP 200, or a truncated transfer), which is the same silent-zero weakness
as the original bug — so `countGoroutines` now returns an error for anything
matching neither shape, and `pprof-grab` prints `unknown` rather than `0`.
`TestCountGoroutinesRejectsUnreadableBodies` pins it.

## F4 — the native engine does not publish most of its own index metrics

`pkg/index/metrics/metrics.go` defines 26 metrics, but a standalone server
exports only a subset. Confirmed absent from `/metrics` on a running container:

- `inverted_index_total_mem_segments`
- `inverted_index_total_file_segments`
- `inverted_index_cur_on_disk_bytes`
- `inverted_index_total_merge_started` / `_finished` / `_latency` / `_errors`
- `native_time_segments_total` and `native_time_candidates_pruned_total` are
  **absent until at least one query has run** (they are registered lazily), so
  a scrape taken before any traffic reads all zeros.

The soak therefore gates on `native_snapshot_bytes` (which is published) and on
`.seg` file counts on disk, and treats the missing ones as unavailable rather
than as zeros.

Also note there are **two independent index scopes** and they must not be summed
or conflated:

| scope | index | labelled by |
|---|---|---|
| `banyandb_stream_storage_inverted_index_*` | **series** index (`sidx/`) | `group`, `seg` |
| `banyandb_stream_tst_inverted_index_*` | **element** index (`idx/`) | `group`, `seg` |

## F5 — the shared query budget is a hard cap (KNOWN stream-query design issue)

> **Status: known limitation of the stream vectorized query path, not a defect
> introduced here, and not actionable in this change.** Recorded because the soak
> has to be sized around it: a query reserves on the order of ~19 KB per
> candidate document, so the practical fixture ceiling is set by the container's
> memory limit rather than by data volume. Anyone reading a
> `query memory pool exhausted` failure from this harness should read it as
> "the fixture is too big for the container limit", not as a defect.

`banyand/protector/query_budget.go` reserves query memory from a pool sized at
`protector limit / 4`. Reservations grow with actual execution allocation, not
just `limit`, so concurrency is what exhausts it. An 8-worker probe running
alongside parity diffs and a write load produced:

```
rpc error: code = ResourceExhausted desc = query memory pool exhausted
```

on **every** catalog query, and parity then failed with 11/11 divergences — a
soak that measured its own harness rather than the index. Dropping to 2 workers
(`PROBE_WORKERS`) cleared it completely. The gates report this separately as
`latency_no_resource_exhausted` so it can never again be mistaken for the index
getting slower.

## F6 — time pruning was not exercised by a single-batch fixture (fixed here)

Every run reports `disjoint=0`, `overlap_trie=0`, `candidates_pruned=0`, with a
large `overlap_fallback`. Reading the raw metrics:

```
banyandb_stream_tst_native_time_segments_total{class="contained",seg="20261011"} 18688
banyandb_stream_tst_native_time_segments_total{class="overlap_fallback",seg="20261012"} 12288
```

This is **not yet shown to be a defect**. Writing 14,400 elements in one burst
makes the engine cut one small segment per batch, so most segments span
milliseconds and are legitimately *contained* in any 15-minute window; only the
newest segment is still being written and has no merged trie yet. Narrow-window
queries do narrow correctly (`narrow_level_error` returns 11 rows where the wide
`filter_level_error` returns its 50-row limit).

### Root cause, and the fix (landed)

The cause was **batching, not write speed or element ordering**. A segment's
`timeMin`/`timeMax` come from the documents it holds (`newMemorySegment`,
`owner.go`), and a segment is cut per write batch. Writing the whole fixture
down ONE `StreamService.Write` stream put the entire 24 h span into a single
batch and therefore a single segment — which can only ever be `contained` or
`overlap`. `disjoint` was *structurally unreachable*, not merely unobserved.

`streamIdxWriteFixture` now splits the fixture across `--chunks` Write RPCs (one
per contiguous slice of the timeline, default 96 = one per 15 min), so the
fixture becomes many segments with narrow, non-overlapping bounds. Element order
is unchanged, so generated values are bit-identical for the same seed.

Confirmed by re-running the smoke:

| | before | after |
|---|---|---|
| `disjoint` | 0 | **912,350** |
| `overlap_fallback` | ~85,000 | 36,758 |

### Still open: `overlap_trie` is never used

`overlap_trie` and `candidates_pruned` remain **0**, and the obvious explanation
is wrong. It is not `trieMinCandidates = 128`: re-seeding with 2 chunks
(7,200 documents per segment instead of 150) still produced
`overlap_fallback` with no trie at all, a ~48x increase in candidates.

So `trieUsable()` (`time_prune.go:297`) is failing, not the cost gate. It probes
the segment dictionary for shift-prefixed `_timestamp` terms over `[0x24,0x25)`
and `[0x5C,0x5D)`. Those terms are written per document in `newMemorySegment`,
so the likely causes are that the probe cannot locate the prefix in a
file-served/paged FST, or that the element-index path stores `_timestamp` under a
field name the probe does not look at. **This is a separate issue from the
batching fix and is unresolved.**

Impact on the soak: none of the latency/CPU/memory/parity gates depend on it —
those are all green. But the trie branch of the pruner is currently unverified,
and a regression there would be invisible. Treat it as an open investigation
item for the pilot rather than a solved problem.


---

## Known limitations of the harness itself

Recorded after an external review (`codex exec`) of the first working version.
These limits and subsequent harness fixes describe what the pilot does and does
not cover.

1. **`trim_warmup` discards 25% of samples.** On the default 2-hour run that is
   the first ~30 minutes, not "the first minute". Fine for a plateau test, but a
   genuinely slow leak that starts immediately and only becomes visible late can
   hide inside the trimmed head.

2. **`rss_growth_tracks_data` permits proportional leaks.** It asserts RSS does
   not outgrow the element-index document count, so a per-write leak that scales
   1:1 with documents passes. It measures *relative* growth and does not
   establish a plateau. For a true leak test, run a window with the write load
   disabled (`SOAK_WRITE_RPS=0`) and assert an absolute plateau there.

3. **The p99 drift gate bounds change, not level.** Queries whose change stays
   under 20 ms are not ratio-tested (correctly — a 3 ms query drifting to 4 ms is
   scheduling noise), but that also means the gate is weak for fast queries; the
   absolute level is covered only by the coarse 1000 ms jump clause. Real
   per-query latency budgets belong in the driver, not here.

4. **`index_no_errors` reads a single scrape.** It checks the last sample only,
   so a transient burst of index errors between samples is invisible.

5. **`index_segments_on_disk` counts `*.seg` by extension.** A segment file with
   an unexpected extension would not be counted, and files without `.seg` are
   not asserted on at all.

6. **The determinism test is narrower than its name.** It checks the service/pod
   identifiers, not every generated tag value. The full fixture is also not
   purely seed-derived: the base timestamp and the gRPC `message_id` values come
   from the wall clock. Neither causes phantom replay divergence in the
   seed-once/snapshot-restore workflow, but a test that claims more than it
   checks will mislead.

7. **Probe windows now require enough observations.** `latency-probe` defaults
   to `--min-calls=10` successful calls per query. Smaller windows retain their
   observations and errors for the next reporting interval; emitting one query
   does not reset another query's pending samples. At shutdown, remaining data
   is emitted with `low_confidence: true` when below the threshold, so no tail
   observations disappear. Tests exercise the reporting loop through an in-memory
   gRPC server with three workers, concurrent accumulator writes, the exact
   emission boundary, retained errors, and sparse final windows.

8. **Drift and row comparisons now share a 10-call minimum.** Windows below 10
   calls or explicitly marked low-confidence are excluded from both comparisons;
   error gates still count every window. Drift compares call-weighted means of
   eligible window p99s in the first and last thirds, keeping the 20 ms noise
   exemption and 1000 ms absolute-jump clause. Row checks use eligible windows'
   `rows_min`/`rows_max` (falling back to `rows` for older reports). Insufficient
   eligible coverage fails the comparison gates. Synthetic report-directory
   tests cover false alarms, hidden real degradation, and the unchanged guards.
   Weighted window p99s are not a pooled call-level p99.

   The pilot contained **1,596 windows with fewer than 10 calls and 1,596 with
   at least 10**. The three-call startup windows made `series_exact_svc`'s naive
   mean of window p50s **20.4 ms**, versus **5.8 ms** when weighted over **4,440
   calls** (about 3.5x inflation). Ten excludes these startup windows while
   admitting the roughly 36-call steady windows; with three workers it requires
   at least four successful rounds. It is a practical minimum for avoiding this
   artifact, not a claim that ten observations precisely estimate a tail
   percentile.

## Pilot result (2 h, 2026-10-12)

`dist/soak-index/20261012T152306/` — 50k fixture + 2 h of write load; 18/18 gates
PASS on reevaluation (the initial summary recorded 17/17). 110 parity runs / 1,540 queries, 0 divergences. 62,160 probe calls, 0
errors. Element index grew 24.5k -> 1.62M docs with RSS +13% and goroutines flat
at 200. `disjoint = 14,850,633`, confirming the chunking fix holds at scale.

Every query is flat or improved over the run once windows with too few samples
are excluded:

| query | first p50 | last p50 | ratio |
|---|---|---|---|
| sort_duration_filtered | 2088 ms | 2016 ms | 0.97x |
| filter_duration_gt | 1062 ms | 1020 ms | 0.96x |
| filter_level_info | 387 ms | 332 ms | 0.86x |
| series_exact_svc | 3.5 ms | 3.0 ms | 0.86x |
| narrow_sort_duration | 3.3 ms | 3.1 ms | 0.96x |

An earlier reading of this run appeared to show `series_exact_svc` degrading
2.4x. That was an artifact of averaging window p50s without regard to how many
calls each window contained: the first window after each probe process starts
has only 3 calls, where p50 is the 2nd of 3 observations. Call-weighted, that
query averages 5.8 ms over 4,440 calls, against a naive window-mean of 20.4 ms.
It does not degrade. The write load cannot affect it either — the load writes to
`soak-idx-load` while the catalog queries `soak-idx-fixture`, and the catalog
references the load group zero times.

The apparent `series_exact_svc` degradation was a harness artifact, not an
index regression. The probe threshold and gate weighting fixes above address it.
Reevaluating the original pilot artifacts with both gate implementations leaves
every PASS/FAIL verdict unchanged: all 18 gates pass. All 14 eligible,
call-weighted last/first p99 ratios remain below 1.0 (0.665–0.949;
`series_exact_svc`: 0.767).

## F7 — exact-value queries on an int tag return no rows (OPEN)

**Severity:** medium (numeric equality is a common filter; range queries are unaffected)

Found while validating the F1 fix, and **not** the same defect: it affects
`BINARY_OP_EQ` and `BINARY_OP_MATCH` equally, so it is not the scalar
comparison bug and is not covered by the F1 fix.

On the soak fixture (`ir_duration`, rule id 14, `TAG_TYPE_INT`):

| query | rows |
|---|---|
| `duration EQ 406` (a value harvested from a projection) | **0** |
| `duration MATCH 406` | **0** |

The value is correct — it was read back from a projection of the same rows, so
the stored value and the queried value agree.

The likely mechanism is in how the write path encodes an int term
(`pkg/index/nativeadapter/adapter.go`, the `FloatTermValue` branch): the stored
value is the sortable-encoded form (`numericPrefix(sortable, 0)`) and the indexed
terms are the raw string plus the shift-prefixed sortables. A query for the
plain decimal string therefore addresses a term the index does not hold, and
the range path (which compares encoded bounds) keeps working — which is why
range queries on the same field behave.

Not fixed here. The first step is the same as for F2: a unit test at the
`nativeadapter` boundary that writes an int field and reads it back with an
exact-value term, so the encoding asymmetry is pinned before anything is
changed in the encoder.

---

## Open question: `overlap_trie` is never used

`native_time_segments_total{class="overlap_trie"}` and
`native_time_candidates_pruned_total` remain 0 after 2 h, with 594,100
`overlap_fallback` classifications. `trieMinCandidates = 128` is NOT the cause:
re-seeding with 7,200 documents per segment (48x the smoke's candidates) still
produced no trie. So `trieUsable()` is failing to locate the shift-prefixed
`_timestamp` terms it probes for (`[0x24,0x25)` and `[0x5C,0x5D)`) — most likely
in a file-served/paged FST, or because the element-index path stores
`_timestamp` under a different field name.

Not the same as F5: this is in `pkg/index/native`'s pruner rather than the query
budget. Unresolved.
