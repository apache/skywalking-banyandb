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
	"testing"

	roaringpkg "github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

const (
	benchDocuments = 200_000
	benchSpan      = int64(24 * 60 * 60 * 1e9) // one day of nanoseconds
	benchBase      = int64(1_700_000_000_000_000_000)
)

// benchSegment builds a segment shaped like a stream element index over a day:
// many documents spread evenly across the span, every one indexed on status
// and carrying a timestamp.
func benchSegment(b *testing.B, documents int) (*Owner, *ReadView, *memorySegment) {
	b.Helper()
	owner := newTestOwnerB(b)
	values := make([]Document, 0, documents)
	for index := range documents {
		values = append(values, Document{
			Identifier: []byte(fmt.Sprintf("doc-%08d", index)),
			Timestamp:  benchBase + int64(index)*(benchSpan/int64(documents)),
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
	return owner, view, segment
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
			_, _, segment := benchSegment(b, benchDocuments)
			lower := benchBase + window.lo
			upper := lower + window.span
			timeRange := &TimeRange{Lower: lower, Upper: upper, IncludesLower: true, IncludesUpper: true}
			all := allOrdinals(segment.handle.count)

			b.Run("perDocument", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					baselineProject(b.Context(), segment, all, timeRange)
				}
			})
			b.Run("pruned", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					narrowed, exact, err := narrowCandidatesToRange(
						b.Context(), segment, all, timeOverlap, timeRange)
					require.NoError(b, err)
					require.True(b, exact)
					projectNarrowed(b.Context(), segment, narrowed, timeRange)
				}
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

// BenchmarkSortCursorNarrowing measures the sort path, where an out-of-range
// candidate additionally pays for a sort-value lookup and frontier work.
func BenchmarkSortCursorNarrowing(b *testing.B) {
	for _, window := range []windowFraction{{"window=1%", benchSpan / 100, benchSpan / 100}} {
		b.Run(window.name, func(b *testing.B) {
			_, _, segment := benchSegment(b, benchDocuments)
			lower := benchBase + window.lo
			timeRange := &TimeRange{Lower: lower, Upper: lower + window.span, IncludesLower: true, IncludesUpper: true}
			all := allOrdinals(segment.handle.count)

			b.Run("perDocument", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					baselineProject(b.Context(), segment, all, timeRange)
				}
			})
			b.Run("pruned", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					narrowed, exact, err := narrowCandidatesToRange(
						b.Context(), segment, all, timeOverlap, timeRange)
					require.NoError(b, err)
					require.True(b, exact)
					projectNarrowed(b.Context(), segment, narrowed, timeRange)
				}
			})
		})
	}
}

// BenchmarkTrieConstruction isolates the cost the pruning pays, which is what
// the trieMinCandidates threshold exists to keep below the cost it saves.
func BenchmarkTrieConstruction(b *testing.B) {
	_, _, segment := benchSegment(b, benchDocuments)
	for _, window := range benchWindows() {
		b.Run(window.name, func(b *testing.B) {
			lower := benchBase + window.lo
			timeRange := &TimeRange{Lower: lower, Upper: lower + window.span, IncludesLower: true, IncludesUpper: true}
			b.ReportAllocs()
			for range b.N {
				if _, err := trieCandidates(b.Context(), segment, timeRange); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
