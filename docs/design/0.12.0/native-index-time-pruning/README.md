# Time-Range Pruning in the Native Inverted Index

Status: **implemented**.

The native inverted index (`pkg/index/native`, `pkg/index/internal/nativeice`) already writes
everything a time-range query needs: per-segment time bounds and a multi-precision `_timestamp`
term trie. No query reads either. Every query applies its time range per document, after
decoding that document's stored fields. This design makes the time range narrow candidates
**before** any stored document is decoded, in two layers:

1. **Segment pruning** — skip a segment whose time bounds miss the query range, and drop the
   per-document time check for a segment the range fully contains.
2. **Trie candidates** — for a segment the range only overlaps, turn the range into a few dozen
   `_timestamp` terms at mixed precision, OR their postings, and intersect the result with the
   series and filter candidates.

It also fixes a prerequisite defect: the streaming merge writes **zero** time bounds, so a merged
segment currently reports "no timestamps".

The change needs no format change, no new persisted data, and no schema or API change. It is
confined to the native query layer and the merge writer.

---

## 1. Problem

### 1.1 What every query does today

All time-scoped native reads go through one of three paths, each checking time per document:

| Entry point | Per-document time check |
|---|---|
| `MatchTermsSet`, `MatchAllTermSets`, `MatchField`, `MatchRange` → `projectCandidates` | `query.go:965` |
| `NewSortCursor` / `NextPage` → `SortCursor.project` | `sort_cursor.go:309` |
| `memorySegment.MatchTerms` | `owner.go:2421` |

Candidates are built from series postings (`seriesCandidates`, `query.go:897`) and filter terms
only. Each candidate is then projected, which means:
- `VisitDocument` decodes its stored fields;
- the identifier and series ID are copied;
- the prefix-coded timestamp is decoded.

Only after all that is the document dropped for being out of range. The sort cursor's "no series
scope" path walks *every* document in the segment (`all = true`).

### 1.2 Why it matters

A segment of the stream element index spans one storage segment (a day by default), while typical
queries ask for minutes. For a 15-minute window over a day-long segment:
- about 99% of the candidates of each matching series are decoded and thrown away;
- in a sort query the same documents also pay for the per-document sort-value lookup and the
  top-N heap work.

Measured on a related shape (`banyand/stream`, 500k documents, 4 CPUs, `main` at `ca86e9a2`):
- the native `SortCursor` costs about 0.97 µs wall, 1.4 µs CPU and 16 allocations per candidate;
- every out-of-range candidate pays that cost in full.

### 1.3 Data the index already has

**Per-document `_timestamp`** (`owner.go:2141-2160`), written for every document with a non-zero
timestamp:
- stored;
- sortable doc value;
- indexed under **16 terms**, one per precision level.

```go
for shift := uint(0); shift <= 60; shift += 4 {
    EncodePrefixCodedInt64Shift(ts, shift) // [0x20+shift][7-bit groups of (ts^signbit)>>shift]
}
```

The term at shift `s` stands for the bucket of all timestamps sharing `ts >> s`. The levels run
from 1 ns (shift 0) through 4.3 s (32), 68.7 s (36), 18.3 min (40), 4.9 h (44) and 3.3 days (48),
up to decades (60). The shift byte leads, so the terms of one level are contiguous in the
dictionary and sorted by bucket. This is the classic numeric trie that Lucene's
`NumericRangeQuery` uses. The native writer emits it to stay compatible with the previous
release's segments (`nidx-03-series-index.md`, field table).

**Per-segment bounds:**
- flush segments record `timeMin`/`timeMax` in the segment footer (`footer[36:52]`);
- the snapshot manifest also records them (`SnapshotSegment.TimeMin/TimeMax`);
- in memory, `segmentHandle.timeMin/timeMax/hasTime` (`owner.go:362-365`) hold them.

### 1.4 Defect: merged segments lose their bounds

`streamMerger.write` writes `0` for both bounds (`merge_stream.go:432-433`). `MergeStats` carries
no bounds, and `newSegmentFromMergeFile` (`merge_staging.go:186`) builds the handle with
`hasTime = false`. Every merged segment therefore claims to have no timestamps, and so does its
manifest entry once persisted (`owner.go:905`, `989`). Merged segments soon hold most of the data,
so segment pruning is worthless until this is fixed.

---

## 2. Goals and non-goals

**Goals**
- G1. A segment the query range misses costs no document decode.
- G2. In a segment the query range overlaps, out-of-range documents cost no stored-field decode.
- G3. A segment the query range fully contains pays no per-document time check.
- G4. Results are identical to today's for every query, including documents without a timestamp,
  deleted documents, and inclusive or exclusive bounds.
- G5. Merged segments carry exact time bounds, in the footer and in the manifest.
- G6. No format change. Segments written by older code, including merged segments with zero
  bounds, stay readable and return correct results.

**Non-goals**
- Index-ordered sort that stops early (walking the sort field's terms in order). This design
  shrinks the sort cursor's candidate set but keeps it a full pass over that set. See §9.
- Projecting only the top-N rows in the stream vectorized scan (sort-then-fetch). That is a
  separate fix for the wide-row memory-budget failure.
- Any change to how callers express time ranges (`index.RangeOpts`, `native.TimeRange`).

---

## 3. Design overview

For each query, segment by segment:

```
classify(segment, range):
    bounds unknown            → OVERLAP
    bounds ∩ range = ∅        → DISJOINT   — skip the segment
    bounds ⊆ range            → CONTAINED  — no range comparison
    otherwise                 → OVERLAP

OVERLAP:
    if segment.trieUsable:
        timeBits := trieCandidates(segment, range)     — exact
        candidates := candidates ∧ timeBits            (or timeBits when "all")
        project without a time check
    else:
        project with today's per-document time check  — fallback
```

| Layer | Removes | Cost |
|---|---|---|
| Segment classification | all work on disjoint segments; the per-document check on contained ones | two integer compares per segment |
| Trie candidates | stored-field decode of out-of-range documents in overlapping segments | at most a few hundred dictionary lookups and posting ORs per segment |
| Merge bounds (prerequisite) | — | one timestamp decode per live document during merge, on a field the merge already decodes |

---

## 4. Detailed design

### 4.1 Exact time bounds for merged segments

**Where:** `internal/nativeice/merge_sections.go` (`writeStored`), `merge_stream.go` (footer),
`merge.go` (`MergeStats`, the buffered `MergeSegments`), `native/merge_staging.go`.

1. `writeStored` already walks every stored document of every input, with its fields decoded, and
   knows whether that document survives (`dropped`). For each **surviving** document, decode the
   `_timestamp` stored value with `DecodePrefixCodedInt64` and fold it into running `int64`
   bounds. Skip documents with no `_timestamp`, as the flush path skips `Timestamp == 0`.
2. Extend `MergeStats` with `TimeMin`, `TimeMax` and `HasTime`. The footer writes the bounds into
   the existing `footer[36:44]` and `footer[44:52]` slots, encoded as `uint64(int64)` exactly as
   the flush path does (`owner.go:2141-2149`).
3. `newSegmentFromMergeFile` sets `handle.timeMin/timeMax/hasTime` from the stats. Persistence
   (`owner.go:905`, `989`) and lifecycle (`lifecycle.go:126`) already copy the handle's bounds
   into the manifest, and the manifest/footer consistency check (`nativeice.go:1548`, `1699`) then
   holds by construction.
4. The buffered `MergeSegments` path gets the same treatment, so both merge forms agree.

The bounds are computed from the surviving documents rather than combined from the inputs'
bounds. Combining would work, but inputs written by today's code have no bounds; computing them
directly means every later merge repairs old merged segments, with no migration.

**Compatibility:** flush segments already write non-zero bounds into the same footer slots, so a
merged segment with non-zero bounds is not a new case for any reader, including the previous
release's rollback path (the NIDX-03 rollback proof covered flush segments with bounds).

### 4.2 Classifying segments

**Where:** a new helper in `native/query.go`, used by every entry point in §1.1.

```go
type timeClass uint8 // timeDisjoint, timeContained, timeOverlap

func classifyTime(h *segmentHandle, r *TimeRange) timeClass
```

- `r == nil` → `timeContained`: the query has no time restriction, so behaviour is unchanged.
- `!h.hasTime` → `timeOverlap`. Unknown bounds never prune.
- Compare in `int64`, converting the handle's `uint64` slots back. If `int64(timeMin) >
  int64(timeMax)`, the bounds were folded across a sign change, which a pre-epoch timestamp could
  cause; treat them as unknown (`timeOverlap`).
- Turn `r` into a closed `int64` interval `[lo, hi]`:
  - `lo = r.Lower` if inclusive, else `r.Lower + 1`;
  - `hi = r.Upper` if inclusive, else `r.Upper - 1`;
  - guard the ±1 against overflow;
  - if `lo > hi`, the range is empty and every segment is `timeDisjoint`.
- Then:
  - `timeMax < lo || timeMin > hi` → `timeDisjoint`;
  - `lo <= timeMin && timeMax <= hi` → `timeContained`;
  - otherwise `timeOverlap`.

**Exactness of `timeContained`:** the bounds cover exactly the documents that have a timestamp.
A contained segment still has to drop documents *without* a timestamp, because today's contract
excludes them: "a timestamp range excludes documents without a stored native timestamp",
`QueryScope` doc, `query.go:58`. Projection keeps a cheap `hasTimestamp` check for those but skips
the range comparison. The `_timestamp` decode itself is still needed because callers consume
`QueryHit.Timestamp`.

### 4.3 Turning a range into trie candidates

**Where:** `internal/nativeice/numeric.go` (range split) and `native/query.go` (posting union).

**Range split.** Port Lucene's `NumericUtils.splitRange` for a 64-bit value with precision step 4.
It works on the sign-flipped value (`uint64(v) ^ 1<<63`, the same transform the encoder applies),
so unsigned order matches signed order.

```go
// SplitSortableRange covers the closed sortable interval [lo, hi] with
// the fewest trie buckets and emits each run of consecutive buckets as
// (shift, firstBucket, lastBucket), in sortable units at that shift.
func SplitSortableRange(lo, hi uint64, emit func(shift uint, first, last uint64))

for shift := uint(0); ; shift += 4 {
    diff := uint64(1) << (shift + 4)          // width of the next level's bucket
    mask := uint64(0xF) << shift              // this level's digit
    hasLower := lo&mask != 0                  // lo is not aligned to the next level
    hasUpper := hi&mask != mask               // hi does not end the next level's bucket
    nextLo := lo; if hasLower { nextLo += diff }; nextLo &^= mask
    nextHi := hi; if hasUpper { nextHi -= diff }; nextHi &^= mask
    if shift+4 >= 64 || nextLo > nextHi || nextLo < lo || nextHi > hi {
        emit(shift, lo>>shift, hi>>shift)     // the remaining middle, at this level
        return
    }
    if hasLower { emit(shift, lo>>shift, (lo|mask)>>shift) }
    if hasUpper { emit(shift, (hi&^mask)>>shift, hi>>shift) }
    lo, hi = nextLo, nextHi
}
```

Each emitted run spans at most 16 buckets at its level. The cover therefore has at most
`2·15·15 + 16 = 466` terms. For realistic windows it has a few dozen: a 15-minute window is about
12 buckets at shift 36 plus at most 15 at each finer level on each edge.

**Encoding.** Add `encodePrefixCodedSortableShift(sortable, shift)`, the existing encoder minus
its sign flip. Each emitted run `(shift, first, last)` becomes the dictionary interval
`[enc(first, shift), enc(last, shift)]`. Dictionary iterators take an **exclusive** end
(`dictionary_iterator.go:120`), so pass `enc(last+1, shift)`. When `last` is the largest bucket
at that level, pass the first key of the next shift level instead: `[]byte{0x20 + shift + 1}`,
or nil above shift 60.

**Posting union.**

```go
func trieCandidates(ctx context.Context, s *memorySegment, r *TimeRange) (*roaring.Bitmap, error)
```

- For each run, iterate the `_timestamp` dictionary over the interval and OR each term's posting
  (the reader's `TermPostingBitmap`, or the iterator's posting accessor) into one bitmap.
- Check `ctx` between runs.
- The bitmap is **exact**. Every timestamped document has exactly one term at each level. The
  cover partitions `[lo, hi]` into disjoint buckets, so a document is in the union if and only if
  its timestamp is in `[lo, hi]`. Documents without a timestamp have no `_timestamp` terms and
  are excluded, which matches today's contract.
- Deleted documents can appear in the bitmap. Deletion is still applied where it is today (the
  `segment.deleted` check in projection and in `SortCursor.project`).

**Cost.** Every document in range appears in exactly one posting of the cover. So building the
bitmap costs O(dictionary lookups in the cover + documents in range) in roaring word operations,
against today's O(candidates) stored-field decodes. The term count is fixed, and no `MaxTerms`
expansion limit applies, because the cover never iterates exact timestamps.

### 4.4 Checking that a segment has the trie

The trie is correct only if a segment actually carries the coarse terms. Native and NIDX-era
segments do (`owner.go:2151`), and merges copy every term. Older segments are meant to, by the
compatibility contract, but a segment missing its coarse terms would silently **lose** documents
under §4.3. To guard against that, every segment handle gets a lazily computed, cached flag:

```go
trieOnce   sync.Once
trieUsable bool
```

- `trieUsable` is true when the segment has no `_timestamp` field.
- Otherwise it is true only if the `_timestamp` dictionary holds at least one term at shift 4
  **and** at least one at shift 60. That is two single-key iterator probes:
  `[0x24, 0x25)` and `[0x5C, 0x5D)`.
- Every timestamped document writes both levels, so a segment that has timestamps but misses
  either level was written without the trie, or with a different precision step.

When `trieUsable` is false, the segment takes the fallback path: today's per-document check. A
counter records how often that happens (§6), so no fallback can go unseen.

### 4.5 Integration per entry point

**`projectCandidates` callers** (`MatchTermsSet`, `MatchAllTermSets`, `MatchField`,
`MatchRange`). Per segment, before projection:
- `timeDisjoint` → `continue`. Its candidates do not count toward `MaxCandidates`.
- `timeContained` → project with the contained flag set, i.e. the `hasTimestamp` check only.
- `timeOverlap` with a usable trie → `candidates.And(trie)`, then project with the contained flag.
- `timeOverlap` without a usable trie → project as today.

`projectCandidates` gets a `timeMode` parameter in place of its bare `*TimeRange`. The candidate
total behind `MaxCandidates` is computed **after** the intersection. This is a deliberate
behaviour change: a time-narrowed query that used to fail with `ErrQueryLimit` now succeeds.
Ranges outside the time window should never have counted.

**`NewSortCursor`.**
- `timeDisjoint` segments are left out of `cursor.segments`.
- For `timeOverlap` segments with a usable trie:
  - the series path ANDs the trie into `candidates`;
  - the `all = true` path replaces "every ordinal" with the trie bitmap (`all = false`,
    `candidates = trie`).
- Each `sortCursorSegment` records whether its candidates are exact in time. `project` then skips
  the range comparison for them (`sort_cursor.go:309`) but keeps the `hasTimestamp` requirement.

**`memorySegment.MatchTerms`** (`owner.go:2361`). The same classification applies.
`timeDisjoint` returns an empty result, and overlap intersects the term postings with the trie --
subject to the same `trieMinCandidates` threshold as every other entry point (§4.6): the posting
being narrowed is the candidate set here, so a point lookup whose posting is small is not worth
narrowing either.

**Not affected:**
- `FilterTermsSet` and `FilterRange` already reject `Scope.TimeRange`.
- `ProjectHit` and `ProjectSortValue` act on already-selected hits.

### 4.6 When to build the trie bitmap

When the non-time candidates are already tiny, decoding them can cost less than OR-ing the
cover's postings. For example, a point lookup by series with two hits in a long range.

Rule: build the trie bitmap only when `candidates.GetCardinality() >= trieMinCandidates`;
otherwise use the per-document check. For the `all` path the trie is always built. The threshold
is a package constant, chosen by the benchmark in §7.2: 128, the crossover
`BenchmarkPointLookupCrossover` measured between a 64-candidate posting (per-document still wins)
and a 128-candidate one (the trie wins decisively). It is not a user-facing flag.

---

## 5. Correctness argument

| Concern | Why results are unchanged |
|---|---|
| Disjoint pruning | Bounds are exact over the timestamped documents (§4.1, flush path), so a disjoint segment has no in-range document. Untimestamped documents are excluded by contract anyway. Unknown bounds never prune. |
| Contained skip | Every timestamped document is in range; the `hasTimestamp` check still removes untimestamped ones. |
| Trie bitmap | Exact cover of `[lo, hi]` (§4.3); usable only where both probe levels exist (§4.4). |
| Exclusive bounds | Normalised once to a closed `int64` interval with overflow guards; an empty interval prunes everything. |
| Deletions | Unchanged: still applied at projection and in the sort cursor. |
| Older segments | Zero bounds read as unknown, so no pruning. A missing trie falls back. Both heal on the next merge (§4.1). |
| Negative or mixed-sign timestamps | Trie order is sign-correct (sign-flipped sortable). Mixed-sign `uint64` bounds are detected and treated as unknown (§4.2). |
| Concurrency | Classification reads immutable handle fields. The `trieOnce` probe is per immutable handle and reads only the pinned reader. |

---

## 6. Observability

`pkg/index/metrics` gets per-owner counters, labelled like the existing native metrics:

- `native_time_segments_total{class="disjoint|contained|overlap_trie|overlap_fallback"}`
- `native_time_candidates_pruned_total`: candidates removed by the trie intersection, i.e.
  cardinality before minus after.

A non-zero `overlap_fallback` on a cluster whose data was written by native code points to a bug.
On a cluster with segments from older writers it is expected, and should fall as those segments
are merged.

---

## 7. Validation

### 7.1 Tests

**`nativeice` unit tests**
- `SplitSortableRange` against brute force:
  - property test over random `[lo, hi]` pairs (fixed seed, about 10k cases), including
    `lo == hi`, aligned and unaligned edges, `MinInt64`, `MaxInt64`, and ranges spanning zero;
  - every value in the range lies in exactly one emitted bucket, and no value outside does;
  - the term count stays at or below 466.
- The encoder: `encodePrefixCodedSortableShift(uint64(v)^1<<63, s)` equals
  `EncodePrefixCodedInt64Shift(v, s)` for all 16 shifts.

**Merge tests**
- Footer and manifest bounds equal the exact min/max over surviving documents.
- Documents dropped by deletion or `Drop` are excluded.
- All-deleted or untimestamped input yields `HasTime == false` and zero slots.
- Streaming and buffered merge agree.
- Merging an input with zero bounds (today's merged output) yields exact bounds.

**Native query tests**, for each entry point in §4.5:
- Results equal those from the per-document path (forced on through a test hook) over random
  documents, random windows (empty, point, inclusive and exclusive edges, full span, disjoint)
  and random deletions.
- Run over each kind of segment:
  - flush;
  - persisted-and-promoted;
  - streaming-merged;
  - external-received;
  - a segment written without coarse terms, built through the internal encoder by emitting only
    shift-0 terms. This must take the fallback path and still match.
- A disjoint segment's reader is never asked to visit a document: a test hook counts
  `VisitDocument` calls.
- `MaxCandidates` is checked after intersection, and a time-narrowed query that used to fail
  now passes.

**Integration:** the existing stream, measure and trace standalone and distributed suites, plus
the stream element-index golden cases, all unchanged. Per the repository rule, the tests must
not reference the retired legacy library.

### 7.2 Benchmarks

The benchmarks are short `go test -bench` runs pinned to 4 CPUs, reporting wall time, process
CPU (rusage) per operation, B/op and allocs/op. No long soak is part of this design.

The fixture extends `banyand/stream`'s index-order benchmark (500k documents over a day-scale
segment, 100 series):

| Axis | Values |
|---|---|
| Window, as a share of the segment's span | 0.1%, 1%, 10%, 100% |
| Query | filter (`EQ` on an indexed tag, per series) · index sort `LIMIT 20` · series point lookup |
| Segment state | flushed only · after a full merge |
| Build | `main` · this design |

**Gates:**
- **G1/G2:** at a 1% window, filter and sort CPU per operation drop by at least 5×, and
  allocated bytes drop by a similar factor.
- **No regression:** at a 100% window, and for the point lookup, CPU and B/op stay within ±5%
  of `main`.
- **Merged state:** after a merge, the 1% window shows the same gain as before it. This proves
  the fix in §4.1.
- `trieMinCandidates` is set at the crossover where the point lookup stops regressing.

---

## 8. Rollout and compatibility

- **On-disk format:** unchanged. Merged segments start filling two footer slots that flush
  segments already fill. Manifests are unchanged in shape.
- **Mixed or rolled-back binaries:** an older binary ignores the bounds for queries and keeps
  checking per document. A newer binary treats older merged segments as unknown-bounds. Both
  return correct results.
- **No flag:** the optimisation is exact. The fallback path and the
  `native_time_segments_total{class="overlap_fallback"}` counter cover segments that cannot take
  it.
- **Delivery:** one PR with three commits: (1) merge bounds plus its tests; (2) range split,
  `trieUsable` and `trieCandidates`, plus their unit tests; (3) entry-point integration, metrics
  and benchmarks.

---

## 9. Follow-ups, out of scope

1. **Index sort that stops early.** Walk the sort field's dictionary in sort order, intersect
   each term's posting with the (now time-narrowed) candidates, and stop after N hits. Combined
   with this design, an index-ordered `LIMIT N` over a short window becomes a short posting walk
   instead of a candidate scan. Needs its own benchmark against the vectorized block sort.
2. **Vectorized sort-then-fetch for stream.** Order on the sort key plus row references, then
   decode the projection for only the top N. It fixes the `LIMIT 20` query that fails today with
   `vectorized query memory budget exceeded` on wide rows.
3. **Faster projection.** With time handled by the candidate bitmap, contained and overlap-trie
   projection still decode `_timestamp` for `QueryHit.Timestamp`. Reading it from the
   `_timestamp` doc value instead of the stored document would avoid the stored walk for callers
   that need only identifier, series and timestamp.

## 10. Open questions

1. Should `trieMinCandidates` instead compare candidates against the segment's document count in
   range (a selectivity ratio), rather than an absolute count? Decide from the §7.2 data.
2. Do any production segments predate the 0..60/step-4 trie, i.e. legacy-written element indexes
   never migrated? The `overlap_fallback` counter answers this in the field; no code depends on
   the answer.
