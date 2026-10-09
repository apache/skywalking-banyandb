// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package native

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	roaringpkg "github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"
)

// timePruneDocument builds a document with a timestamp and the fields the
// parity tests select on.
func timePruneDocument(identifier string, timestamp int64, status string) Document {
	return Document{
		Identifier: []byte(identifier), Timestamp: timestamp,
		Fields: []Field{
			{Name: "status", Value: []byte(status), Index: true, Store: true},
			{Name: "series", Value: []byte("series-a"), Index: true, Store: true},
			{Name: "value", Value: []byte(fmt.Sprintf("%d", timestamp)), Index: true, Store: true, Sort: true},
		},
	}
}

// timePruneWindows are the range shapes the design calls out: empty, point,
// inclusive and exclusive edges, full span, and disjoint on both sides.
func timePruneWindows() []*TimeRange {
	const base = int64(1_700_000_000_000_000_000)
	return []*TimeRange{
		{Lower: base + 1000, Upper: base + 1000, IncludesLower: false, IncludesUpper: false},
		{Lower: base + 1000, Upper: base + 1000, IncludesLower: true, IncludesUpper: true},
		{Lower: base + 1000, Upper: base + 2000, IncludesLower: false, IncludesUpper: true},
		{Lower: base + 1000, Upper: base + 2000, IncludesLower: true, IncludesUpper: false},
		{Lower: base + 1000, Upper: base + 2000, IncludesLower: false, IncludesUpper: false},
		{Lower: base, Upper: base + 10_000, IncludesLower: true, IncludesUpper: true},
		{Lower: base - 10_000, Upper: base - 1000, IncludesLower: true, IncludesUpper: true},
		{Lower: base + 100_000, Upper: base + 200_000, IncludesLower: true, IncludesUpper: true},
	}
}

// identifiersOf sorts identifiers so two runs compare without depending on
// segment or document order.
func identifiersOf(hits []QueryHit) []string {
	result := make([]string, 0, len(hits))
	for _, hit := range hits {
		result = append(result, string(hit.Identifier))
	}
	sort.Strings(result)
	return result
}

func matchIdentifiersOf(result MatchResult) []string {
	out := make([]string, 0, len(result.Identifiers))
	for _, identifier := range result.Identifiers {
		out = append(out, string(identifier))
	}
	sort.Strings(out)
	return out
}

// seedTimePruneOwner writes a spread of documents, some deleted, so every
// window shape has in-range, out-of-range and missing documents.
func seedTimePruneOwner(t *testing.T, count int, seed int64) *Owner {
	t.Helper()
	owner := newTestOwner(t, nil)
	const base = int64(1_700_000_000_000_000_000)
	source := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic fixture
	documents := make([]Document, 0, count)
	for index := range count {
		timestamp := base + int64(index)*100 + int64(source.Intn(100))
		status := "ok"
		if index%3 == 0 {
			status = "bad"
		}
		documents = append(documents, timePruneDocument(fmt.Sprintf("doc-%04d", index), timestamp, status))
	}
	// Some documents carry no timestamp at all; a time range must never return
	// them, but a query with no range must.
	for index := 0; index < count/10; index++ {
		documents = append(documents, Document{
			Identifier: []byte(fmt.Sprintf("undated-%04d", index)),
			Fields: []Field{
				{Name: "status", Value: []byte("ok"), Index: true, Store: true},
				{Name: "series", Value: []byte("series-a"), Index: true, Store: true},
			},
		})
	}
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: documents}))
	// Delete a slice of the middle so the range has to respect deletions.
	deletes := make([][]byte, 0, count/10)
	for index := count / 5; index < count/5+count/10; index++ {
		deletes = append(deletes, []byte(fmt.Sprintf("doc-%04d", index)))
	}
	if len(deletes) > 0 {
		require.NoError(t, owner.Batch(context.Background(), Batch{Deletes: deletes}))
	}
	return owner
}

// TestTimePruningMatchesThePerDocumentPath is the central correctness claim:
// narrowing a segment by time must return exactly what checking each document's
// timestamp returns, for every entry point, every window shape, and a segment
// whose candidates are dense enough to take the trie path.
func TestTimePruningMatchesThePerDocumentPath(t *testing.T) {
	// Above trieMinCandidates, so the trie is actually exercised rather than
	// the small-candidate fallback.
	owner := seedTimePruneOwner(t, 2000, 3)
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })

	windows := append(timePruneWindows(), nil)
	for _, window := range windows {
		name := "no-range"
		if window != nil {
			name = fmt.Sprintf("%d..%d[%v%v", window.Lower, window.Upper, window.IncludesLower, window.IncludesUpper)
		}
		t.Run(name, func(t *testing.T) {
			scope := QueryScope{SeriesField: "series", SeriesID: []byte("series-a")}
			scope.TimeRange = window

			// MatchTermsSet: the exact-term filter path.
			terms := TermSetRequest{
				Field: "status", Terms: [][]byte{[]byte("ok")}, Mode: MatchAnyTerm, Scope: scope,
			}
			narrowed, narrowedErr := view.MatchTermsSet(context.Background(), terms)
			require.NoError(t, narrowedErr)

			// The per-document reference: the same request with the range
			// removed, filtered here by the rule the old code applied.
			unranged := terms
			unranged.Scope.TimeRange = nil
			all, allErr := view.MatchTermsSet(context.Background(), unranged)
			require.NoError(t, allErr)
			require.Equal(t, referenceFiltered(all, window), identifiersOf(narrowed),
				"MatchTermsSet disagreed with the per-document path")

			// MatchField: the presence path, which relies on MatchField
			// returning documents that never set the field.
			fieldRequest := FieldRequest{Field: "series", Scope: scope, MaxTerms: 8}
			narrowedField, fieldErr := view.MatchField(context.Background(), fieldRequest)
			require.NoError(t, fieldErr)
			unrangedField := fieldRequest
			unrangedField.Scope.TimeRange = nil
			allField, allFieldErr := view.MatchField(context.Background(), unrangedField)
			require.NoError(t, allFieldErr)
			require.Equal(t, referenceFiltered(allField, window), identifiersOf(narrowedField),
				"MatchField disagreed with the per-document path")

			// MatchTerms: the in-memory path.
			matched, matchedErr := view.MatchTerms(context.Background(), MatchRequest{
				Field: "status", Term: []byte("ok"), SeriesField: "series", SeriesID: []byte("series-a"),
				TimeRange: window,
			})
			require.NoError(t, matchedErr)
			unrangedMatched, unrangedMatchedErr := view.MatchTerms(context.Background(), MatchRequest{
				Field: "status", Term: []byte("ok"), SeriesField: "series", SeriesID: []byte("series-a"),
			})
			require.NoError(t, unrangedMatchedErr)
			require.Equal(t, referenceMatched(unrangedMatched, window), matchIdentifiersOf(matched),
				"MatchTerms disagreed with the per-document path")
		})
	}
}

// referenceFiltered applies today's rule to an unranged result: a document with
// no timestamp is dropped whenever a range is present, and an out-of-range
// document is dropped.
func referenceFiltered(hits []QueryHit, window *TimeRange) []string {
	if window == nil {
		return identifiersOf(hits)
	}
	kept := make([]string, 0, len(hits))
	for _, hit := range hits {
		if hit.Timestamp == 0 {
			continue
		}
		if !window.contains(hit.Timestamp) {
			continue
		}
		kept = append(kept, string(hit.Identifier))
	}
	sort.Strings(kept)
	return kept
}

func referenceMatched(result MatchResult, window *TimeRange) []string {
	if window == nil {
		return matchIdentifiersOf(result)
	}
	kept := make([]string, 0, len(result.Identifiers))
	for index, identifier := range result.Identifiers {
		if result.Timestamps[index] == 0 {
			continue
		}
		if !window.contains(result.Timestamps[index]) {
			continue
		}
		kept = append(kept, string(identifier))
	}
	sort.Strings(kept)
	return kept
}

// TestTimePruningFallsBackWithoutTheTrie builds a segment carrying only
// shift-zero _timestamp terms, which no current writer emits, and checks it
// still returns the right answers through the per-document path.
func TestTimePruningFallsBackWithoutTheTrie(t *testing.T) {
	const base = int64(1_700_000_000_000_000_000)
	documents := make([]Document, 0, 600)
	for index := range 600 {
		documents = append(documents, timePruneDocument(fmt.Sprintf("doc-%04d", index), base+int64(index), "ok"))
	}
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: documents}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })

	segment, segmentErr := view.segmentAt(0)
	require.NoError(t, segmentErr)
	usable, usableErr := segment.handle.trieUsable()
	require.NoError(t, usableErr)
	require.True(t, usable, "a natively written segment must report a usable trie")

	// The fallback contract: a segment the probe rejects keeps today's results,
	// which is what a segment without coarse terms has to rely on.
	window := closedTestRange(base+100, base+200)
	fallbackHandle := &segmentHandle{
		reader: segment.handle.reader, count: segment.handle.count,
		timeMin: segment.handle.timeMin, timeMax: segment.handle.timeMax,
		hasTime: segment.handle.hasTime, trieUsableValue: false,
	}
	// Mark the lazy probe as already run, or the first call would recompute it
	// against the real reader and overwrite the forced answer.
	fallbackHandle.trieOnce.Do(func() {})
	fallbackSegment := &memorySegment{handle: fallbackHandle}
	narrowed, exact, narrowErr := narrowCandidatesToRange(
		context.Background(), fallbackSegment, allOrdinals(fallbackSegment.handle.count), timeOverlap, window)
	require.NoError(t, narrowErr)
	require.False(t, exact, "a segment that cannot use the trie must not claim an exact narrowing")
	require.Equal(t, uint64(600), narrowed.GetCardinality(), "the fallback must leave the candidate set untouched")
}

func closedTestRange(lower, upper int64) *TimeRange {
	return &TimeRange{Lower: lower, Upper: upper, IncludesLower: true, IncludesUpper: true}
}

func allOrdinals(count uint64) *roaringpkg.Bitmap {
	bitmap := roaringpkg.New()
	for value := uint64(0); value < count; value++ {
		bitmap.Add(uint32(value))
	}
	return bitmap
}

// TestTrieIsActuallyUsed guards the optimisation: without this, a refactor
// could silently fall back everywhere and every other test would still pass.
func TestTrieIsActuallyUsed(t *testing.T) {
	owner := seedTimePruneOwner(t, 2000, 5)
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })
	segment, segmentErr := view.segmentAt(0)
	require.NoError(t, segmentErr)

	const base = int64(1_700_000_000_000_000_000)
	window := closedTestRange(base+200, base+200_000)
	narrowed, exact, narrowErr := narrowCandidatesToRange(
		context.Background(), segment, allOrdinals(segment.handle.count), timeOverlap, window)
	require.NoError(t, narrowErr)
	require.True(t, exact, "a wide overlap on a usable segment must narrow exactly")
	require.Less(t, narrowed.GetCardinality(), segment.handle.count,
		"the trie must actually remove out-of-range documents")
	require.NotZero(t, narrowed.GetCardinality(), "the trie must keep in-range documents")
}

// TestDisjointSegmentVisitsNothing proves the strongest saving: a segment the
// range misses must not decode a single document.
func TestDisjointSegmentVisitsNothing(t *testing.T) {
	const base = int64(1_700_000_000_000_000_000)
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		timePruneDocument("a", base, "ok"), timePruneDocument("b", base+10, "ok"),
	}}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })

	far := closedTestRange(base+1_000_000, base+2_000_000)
	hits, hitsErr := view.MatchTermsSet(context.Background(), TermSetRequest{
		Field: "status", Terms: [][]byte{[]byte("ok")}, Mode: MatchAnyTerm,
		Scope: QueryScope{TimeRange: far},
	})
	require.NoError(t, hitsErr)
	require.Empty(t, hits, "a disjoint segment must contribute nothing")
}

// TestTimePruningSurvivesMergedSegments checks the pruning still works after a
// merge, which is where the bounds used to be missing entirely.
func TestTimePruningSurvivesMergedSegments(t *testing.T) {
	const base = int64(1_700_000_000_000_000_000)
	// The in-memory merge path writes the same footer bounds and terms as the
	// on-disk staged one, which pkg/index/internal/nativeice already covers.
	owner := newTestOwner(t, nil)
	for batch := range 4 {
		documents := make([]Document, 0, 500)
		for index := range 500 {
			ordinal := batch*500 + index
			documents = append(documents, timePruneDocument(
				fmt.Sprintf("doc-%04d", ordinal), base+int64(ordinal)*10, "ok"))
		}
		require.NoError(t, owner.Batch(context.Background(), Batch{Documents: documents}))
	}
	before, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	require.Greater(t, len(before.root.segments), 1, "the fixture needs several segments to merge")
	segmentCount := len(before.root.segments)
	require.NoError(t, before.Close())

	// The tiered planner never schedules below two comparably sized segments
	// in a tier, which this small fixture may not satisfy; force the merge so
	// the test is about merged segments rather than about the planner. The view
	// is released first, because compacting under a pinned root is rejected.
	require.NoError(t, owner.forceMergeAll(context.Background()))

	merged, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, merged.Close()) })
	require.Less(t, len(merged.root.segments), segmentCount, "compaction should have merged segments")

	// A merged segment must now carry bounds, which is the prerequisite the
	// whole design rests on.
	for index, current := range merged.root.segments {
		segment := current.(*memorySegment)
		require.True(t, segment.handle.hasTime, "merged segment %d has no time bounds", index)
	}

	for _, window := range timePruneWindows() {
		narrowed, narrowedErr := merged.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "status", Terms: [][]byte{[]byte("ok")}, Mode: MatchAnyTerm,
			Scope: QueryScope{TimeRange: window},
		})
		require.NoError(t, narrowedErr)
		unranged, unrangedErr := merged.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "status", Terms: [][]byte{[]byte("ok")}, Mode: MatchAnyTerm,
		})
		require.NoError(t, unrangedErr)
		require.Equal(t, referenceFiltered(unranged, window), identifiersOf(narrowed),
			"merged segments disagreed with the per-document path for %+v", window)
	}
}

// TestTrieCoverageMatchesThePerDocumentSet is the exactness claim in its purest
// form: the trie's document set has to equal a brute-force scan of the segment.
func TestTrieCoverageMatchesThePerDocumentSet(t *testing.T) {
	const base = int64(1_700_000_000_000_000_000)
	source := rand.New(rand.NewSource(99)) //nolint:gosec // deterministic fixture
	documents := make([]Document, 0, 800)
	timestamps := make([]int64, 0, 800)
	for index := range 800 {
		timestamp := base + int64(source.Intn(2_000_000))
		timestamps = append(timestamps, timestamp)
		documents = append(documents, timePruneDocument(fmt.Sprintf("doc-%04d", index), timestamp, "ok"))
	}
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: documents}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })
	segment, segmentErr := view.segmentAt(0)
	require.NoError(t, segmentErr)

	for iteration := 0; iteration < 200; iteration++ {
		lower := base + int64(source.Intn(2_000_000))
		upper := lower + int64(source.Intn(50_000))
		window := closedTestRange(lower, upper)
		bitmap, bitmapErr := trieCandidates(context.Background(), segment, window)
		require.NoError(t, bitmapErr)

		expected := make(map[uint32]struct{})
		for index, timestamp := range timestamps {
			if timestamp >= lower && timestamp <= upper {
				expected[uint32(index)] = struct{}{}
			}
		}
		require.Equal(t, len(expected), int(bitmap.GetCardinality()),
			"trie cardinality differs from brute force for [%d, %d]", lower, upper)
		for ordinal := range expected {
			require.True(t, bitmap.Contains(ordinal),
				"trie is missing ordinal %d for [%d, %d]", ordinal, lower, upper)
		}
	}
}

// TestTimestampTermsSurviveAMerge keeps the trie honest after a merge, since a
// merge rewrites every term.
func TestTimestampTermsSurviveAMerge(t *testing.T) {
	const base = int64(1_700_000_000_000_000_000)
	owner := newTestOwner(t, nil)
	for batch := range 3 {
		documents := make([]Document, 0, 400)
		for index := range 400 {
			ordinal := batch*400 + index
			documents = append(documents, timePruneDocument(
				fmt.Sprintf("doc-%04d", ordinal), base+int64(ordinal)*37, "ok"))
		}
		require.NoError(t, owner.Batch(context.Background(), Batch{Documents: documents}))
	}
	// Compact before acquiring: a pinned root makes the merge stale.
	require.NoError(t, owner.forceMergeAll(context.Background()))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })

	for index, current := range view.root.segments {
		segment := current.(*memorySegment)
		usable, usableErr := segment.handle.trieUsable()
		require.NoError(t, usableErr)
		require.True(t, usable, "merged segment %d lost its coarse timestamp terms", index)

		window := closedTestRange(base+1000, base+5000)
		bitmap, bitmapErr := trieCandidates(context.Background(), segment, window)
		require.NoError(t, bitmapErr)
		require.NotZero(t, bitmap.GetCardinality(), "merged segment %d returned nothing for an in-range window", index)
	}
}
