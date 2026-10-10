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
	"syscall"
	"testing"

	roaringpkg "github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

// cpuNanos returns this process's accumulated user+system CPU time. Comparing
// two readings around a benchmark loop reports process CPU independent of
// scheduling noise, which is what the native-index-time-pruning design's
// gates (§7.2) are measured against, not wall time.
func cpuNanos() int64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return ru.Utime.Nano() + ru.Stime.Nano()
}

// runBench runs fn b.N times, reporting allocations (B/op, allocs/op) and
// process CPU per operation alongside testing.B's own wall-time ns/op.
func runBench(b *testing.B, fn func()) {
	b.Helper()
	b.ReportAllocs()
	c0 := cpuNanos()
	b.ResetTimer()
	for range b.N {
		fn()
	}
	b.StopTimer()
	b.ReportMetric(float64(cpuNanos()-c0)/float64(b.N), "cpu-ns/op")
}

const (
	benchDocuments = 200_000
	benchSpan      = int64(24 * 60 * 60 * 1e9) // one day of nanoseconds
	benchBase      = int64(1_700_000_000_000_000_000)
)

// benchSegment builds a segment shaped like a stream element index over a day:
// many documents spread evenly across the span, every one indexed on status
// and carrying a timestamp.
func benchSegment(b *testing.B) (*ReadView, *memorySegment) {
	b.Helper()
	owner := newTestOwnerB(b)
	values := make([]Document, 0, benchDocuments)
	for index := range benchDocuments {
		values = append(values, Document{
			Identifier: []byte(fmt.Sprintf("doc-%08d", index)),
			Timestamp:  benchBase + int64(index)*(benchSpan/int64(benchDocuments)),
			Fields: []Field{
				{Name: "status", Value: []byte("ok"), Index: true, Store: true},
				{Name: "series", Value: []byte("series-a"), Index: true, Store: true},
			},
		})
	}
	require.NoError(b, owner.Batch(context.Background(), Batch{Documents: values}))
	view, err := owner.Acquire(context.Background())
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, owner.Close()); require.NoError(b, view.Close()) })
	segment, err := view.segmentAt(0)
	require.NoError(b, err)
	return view, segment
}

func newTestOwnerB(b *testing.B) *Owner {
	b.Helper()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	require.NoError(b, err)
	return owner
}

// windowFraction names the slice of the segment's span a benchmark window covers.
type windowFraction struct {
	name     string
	lo, span int64
}

func benchWindows() []windowFraction {
	return []windowFraction{
		{"window=100%", 0, benchSpan},
		{"window=10%", 0, benchSpan / 10},
		{"window=1%", benchSpan / 100, benchSpan / 100},
		{"window=0.1%", benchSpan / 1000, benchSpan / 1000},
	}
}

// BenchmarkTimeRangeNarrowing compares the pruning path against the
// per-document check it replaced, in one binary so the two are measured under
// identical conditions.
//
// The baseline walks every candidate and decodes its stored fields before
// comparing timestamps, which is exactly what every query did before. The
// pruned path classifies the segment and intersects the candidates with the
// trie first.
func BenchmarkTimeRangeNarrowing(b *testing.B) {
	for _, window := range benchWindows() {
		b.Run(window.name, func(b *testing.B) {
			_, segment := benchSegment(b)
			lower := benchBase + window.lo
			upper := lower + window.span
			timeRange := &TimeRange{Lower: lower, Upper: upper, IncludesLower: true, IncludesUpper: true}
			all := allOrdinals(segment.handle.count)

			b.Run("perDocument", func(b *testing.B) {
				runCountingBench(b, func() int { return baselineProject(b.Context(), segment, all, timeRange) })
			})
			b.Run("pruned", func(b *testing.B) {
				runCountingBench(b, func() int {
					narrowed, exact, err := narrowCandidatesToRange(
						b.Context(), segment, all, timeOverlap, timeRange)
					require.NoError(b, err)
					require.True(b, exact)
					return projectNarrowed(b.Context(), segment, narrowed, timeRange)
				})
			})
		})
	}
}

// baselineProject is the pre-change shape: decode every candidate's stored
// fields, then drop the ones outside the range.
func baselineProject(ctx context.Context, segment *memorySegment, candidates *roaringpkg.Bitmap, timeRange *TimeRange) int {
	return visitAndCount(ctx, segment, candidates, timeRange, timeProjectionCheck)
}

func projectNarrowed(ctx context.Context, segment *memorySegment, candidates *roaringpkg.Bitmap, timeRange *TimeRange) int {
	return visitAndCount(ctx, segment, candidates, timeRange, timeProjectionContained)
}

// visitAndCount walks candidates, decoding each one's stored _timestamp, and
// counts the ones timeRange keeps -- every one of them when projection already
// guarantees that, or only the in-range ones when it must still check.
func visitAndCount(
	ctx context.Context,
	segment *memorySegment,
	candidates *roaringpkg.Bitmap,
	timeRange *TimeRange,
	projection timeProjection,
) int {
	kept := 0
	iterator := candidates.Iterator()
	for iterator.HasNext() {
		if err := ctx.Err(); err != nil {
			return kept
		}
		documentNumber := uint64(iterator.Next())
		var timestamp int64
		var hasTimestamp bool
		var err error
		visitErr := segment.handle.reader.VisitDocument(documentNumber, func(document nativeice.StoredDocument) error {
			return document.VisitStoredFields(func(name string, value []byte) bool {
				if name == timestampField {
					timestamp, err = nativeice.DecodePrefixCodedInt64(value)
					hasTimestamp = err == nil
					return err == nil
				}
				return true
			})
		})
		if visitErr != nil || err != nil {
			continue
		}
		if !hasTimestamp {
			continue
		}
		if projection == timeProjectionCheck && !timeRange.contains(timestamp) {
			continue
		}
		kept++
	}
	return kept
}

// runCountingBench is runBench for a benchmarked function that reports how
// many documents it kept. Using the count (rather than discarding it) is what
// makes the in-range comparison inside fn a real branch rather than dead
// work a compiler could fold away, and the sanity check catches a window
// fixture degenerate enough to keep nothing, which would silently turn a
// timing benchmark into a no-op.
func runCountingBench(b *testing.B, fn func() int) {
	b.Helper()
	var kept int
	runBench(b, func() { kept = fn() })
	require.NotZero(b, kept, "benchmark window must keep at least one document")
}

// BenchmarkSortCursorNarrowing measures the sort path, where an out-of-range
// candidate additionally pays for a sort-value lookup and frontier work.
func BenchmarkSortCursorNarrowing(b *testing.B) {
	for _, window := range []windowFraction{{"window=1%", benchSpan / 100, benchSpan / 100}} {
		b.Run(window.name, func(b *testing.B) {
			_, segment := benchSegment(b)
			lower := benchBase + window.lo
			timeRange := &TimeRange{Lower: lower, Upper: lower + window.span, IncludesLower: true, IncludesUpper: true}
			all := allOrdinals(segment.handle.count)

			b.Run("perDocument", func(b *testing.B) {
				runCountingBench(b, func() int { return baselineProject(b.Context(), segment, all, timeRange) })
			})
			b.Run("pruned", func(b *testing.B) {
				runCountingBench(b, func() int {
					narrowed, exact, err := narrowCandidatesToRange(
						b.Context(), segment, all, timeOverlap, timeRange)
					require.NoError(b, err)
					require.True(b, exact)
					return projectNarrowed(b.Context(), segment, narrowed, timeRange)
				})
			})
		})
	}
}

// BenchmarkTrieConstruction isolates the cost the pruning pays, which is what
// the trieMinCandidates threshold exists to keep below the cost it saves.
func BenchmarkTrieConstruction(b *testing.B) {
	_, segment := benchSegment(b)
	for _, window := range benchWindows() {
		b.Run(window.name, func(b *testing.B) {
			lower := benchBase + window.lo
			timeRange := &TimeRange{Lower: lower, Upper: lower + window.span, IncludesLower: true, IncludesUpper: true}
			runBench(b, func() {
				if _, err := trieCandidates(b.Context(), segment, timeRange); err != nil {
					b.Fatal(err)
				}
			})
		})
	}
}

// BenchmarkTimeRangeNarrowingAfterMerge repeats the 1% window comparison on a
// segment produced by a real merge, which is where §4.1 fixed the time bounds
// that pruning depends on: before that fix, every merged segment reported no
// timestamps and classifyTime never pruned it.
func BenchmarkTimeRangeNarrowingAfterMerge(b *testing.B) {
	owner := newTestOwnerB(b)
	for batch := range 4 {
		values := make([]Document, 0, benchDocuments/4)
		for index := range benchDocuments / 4 {
			ordinal := batch*(benchDocuments/4) + index
			values = append(values, Document{
				Identifier: []byte(fmt.Sprintf("doc-%08d", ordinal)),
				Timestamp:  benchBase + int64(ordinal)*(benchSpan/int64(benchDocuments)),
				Fields: []Field{
					{Name: "status", Value: []byte("ok"), Index: true, Store: true},
					{Name: "series", Value: []byte("series-a"), Index: true, Store: true},
				},
			})
		}
		require.NoError(b, owner.Batch(context.Background(), Batch{Documents: values}))
	}
	require.NoError(b, owner.forceMergeAll(context.Background()))
	view, err := owner.Acquire(context.Background())
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, owner.Close()); require.NoError(b, view.Close()) })
	require.Len(b, view.root.segments, 1, "the fixture must merge down to one segment")
	segment, err := view.segmentAt(0)
	require.NoError(b, err)
	require.True(b, segment.handle.hasTime, "a merged segment must carry time bounds (§4.1)")

	window := windowFraction{"window=1%", benchSpan / 100, benchSpan / 100}
	lower := benchBase + window.lo
	upper := lower + window.span
	timeRange := &TimeRange{Lower: lower, Upper: upper, IncludesLower: true, IncludesUpper: true}
	all := allOrdinals(segment.handle.count)

	b.Run("perDocument", func(b *testing.B) {
		runCountingBench(b, func() int { return baselineProject(b.Context(), segment, all, timeRange) })
	})
	b.Run("pruned", func(b *testing.B) {
		runCountingBench(b, func() int {
			narrowed, exact, narrowErr := narrowCandidatesToRange(
				b.Context(), segment, all, timeOverlap, timeRange)
			require.NoError(b, narrowErr)
			require.True(b, exact)
			return projectNarrowed(b.Context(), segment, narrowed, timeRange)
		})
	})
}

// BenchmarkSortLimit20 measures an index-ordered LIMIT 20 query end to end,
// through the real SortCursor, rather than the narrowCandidatesToRange proxy
// BenchmarkSortCursorNarrowing uses. perDocument builds the same cursor shape
// NewSortCursor used to always build -- "all" candidates, no time narrowing --
// so the per-page walk still pays for every out-of-range sort-value decode;
// pruned goes through NewSortCursor itself.
func BenchmarkSortLimit20(b *testing.B) {
	for _, window := range benchWindows() {
		b.Run(window.name, func(b *testing.B) {
			view, segment := benchSegment(b)
			lower := benchBase + window.lo
			upper := lower + window.span
			timeRange := &TimeRange{Lower: lower, Upper: upper, IncludesLower: true, IncludesUpper: true}

			b.Run("perDocument", func(b *testing.B) {
				runBench(b, func() {
					cursor := &SortCursor{
						view: view, ctx: b.Context(), sortField: timestampField, pageSize: 20,
						timeRange: timeRange,
						segments:  []sortCursorSegment{{segment: segment, segmentID: 0, all: true, timeExact: false}},
					}
					if _, pageErr := cursor.NextPage(b.Context()); pageErr != nil {
						b.Fatal(pageErr)
					}
				})
			})
			b.Run("pruned", func(b *testing.B) {
				runBench(b, func() {
					cursor, cursorErr := view.NewSortCursor(b.Context(), SortCursorRequest{
						SortField: timestampField, PageSize: 20,
						Selection: TermSetRequest{Scope: QueryScope{TimeRange: timeRange}},
					})
					if cursorErr != nil {
						b.Fatal(cursorErr)
					}
					if _, pageErr := cursor.NextPage(b.Context()); pageErr != nil {
						b.Fatal(pageErr)
					}
				})
			})
		})
	}
}

// pointLookupSegment builds a segment shaped like benchSegment, except exactly
// hits documents (spread evenly across the span) additionally carry a "lookup"
// field valued "hit", so MatchRequest{Field: "lookup", Term: []byte("hit")}
// returns a posting of exactly that size.
func pointLookupSegment(b *testing.B, documents, hits int) *memorySegment {
	b.Helper()
	owner := newTestOwnerB(b)
	values := make([]Document, 0, documents)
	stride := documents / hits
	for index := range documents {
		fields := []Field{
			{Name: "status", Value: []byte("ok"), Index: true, Store: true},
			{Name: "series", Value: []byte("series-a"), Index: true, Store: true},
		}
		if stride > 0 && index%stride == 0 {
			fields = append(fields, Field{Name: "lookup", Value: []byte("hit"), Index: true, Store: true})
		}
		values = append(values, Document{
			Identifier: []byte(fmt.Sprintf("doc-%08d", index)),
			Timestamp:  benchBase + int64(index)*(benchSpan/int64(documents)),
			Fields:     fields,
		})
	}
	require.NoError(b, owner.Batch(context.Background(), Batch{Documents: values}))
	view, err := owner.Acquire(context.Background())
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, owner.Close()); require.NoError(b, view.Close()) })
	segment, err := view.segmentAt(0)
	require.NoError(b, err)
	return segment
}

// pointLookupPerDocument is MatchTerms's shape before the trieMinCandidates
// threshold existed: decode every posting candidate's timestamp and compare.
func pointLookupPerDocument(segment *memorySegment, posting nativeice.TermPosting, timeRange *TimeRange) int {
	kept := 0
	for _, number := range postingDocuments(posting) {
		var timestamp int64
		var hasTimestamp bool
		_ = segment.handle.reader.VisitDocument(number, func(document nativeice.StoredDocument) error {
			return document.VisitStoredFields(func(name string, value []byte) bool {
				if name == timestampField {
					ts, decodeErr := nativeice.DecodePrefixCodedInt64(value)
					hasTimestamp = decodeErr == nil
					if hasTimestamp {
						timestamp = ts
					}
					return decodeErr == nil
				}
				return true
			})
		})
		if hasTimestamp && timeRange.contains(timestamp) {
			kept++
		}
	}
	return kept
}

// pointLookupWithTrie is MatchTerms's shape once a posting clears
// trieMinCandidates: narrow the posting against the trie's cover instead of
// decoding each candidate.
func pointLookupWithTrie(ctx context.Context, segment *memorySegment, posting nativeice.TermPosting, timeRange *TimeRange) (int, error) {
	inRange, err := trieCandidates(ctx, segment, timeRange)
	if err != nil {
		return 0, err
	}
	return len(postingDocuments(narrowTermPostingToBitmap(posting, inRange))), nil
}

// BenchmarkPointLookupCrossover sweeps the size of the posting a point lookup
// narrows, with the query range spanning the whole segment (the "a point
// lookup by series with two hits in a long range" shape §4.6 names), to find
// where building the trie's cover stops costing more than it saves.
// trieMinCandidates (time_prune.go) is set from the crossover this reports.
func BenchmarkPointLookupCrossover(b *testing.B) {
	timeRange := &TimeRange{Lower: benchBase, Upper: benchBase + benchSpan, IncludesLower: true, IncludesUpper: true}
	for _, hits := range []int{1, 8, 32, 64, 128, 256, 512, 1024, 4096} {
		b.Run(fmt.Sprintf("postings=%d", hits), func(b *testing.B) {
			segment := pointLookupSegment(b, benchDocuments, hits)
			posting, found, err := segment.handle.reader.TermPosting("lookup", []byte("hit"))
			require.NoError(b, err)
			require.True(b, found)

			b.Run("perDocument", func(b *testing.B) {
				runBench(b, func() { pointLookupPerDocument(segment, posting, timeRange) })
			})
			b.Run("withTrie", func(b *testing.B) {
				runBench(b, func() {
					if _, trieErr := pointLookupWithTrie(b.Context(), segment, posting, timeRange); trieErr != nil {
						b.Fatal(trieErr)
					}
				})
			})
		})
	}
}
