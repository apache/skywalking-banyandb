// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package trace

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

const dropSetBenchmarkTraceCount = 33353

func TestDroppedTraceIDsKeepsEncodedValuesByExactID(t *testing.T) {
	dropped := acquireDroppedTraceIDs()
	t.Cleanup(func() { releaseDroppedTraceIDs(dropped) })

	dropped.add("trace-a")
	dropped.add("trace-b")
	dropped.add("trace-b")
	dropped.add("trace-c")

	require.Equal(t, 3, dropped.len())
	require.False(t, dropped.keepEncoded(append([]byte{byte(idFormatV1)}, []byte("trace-b")...)))
	require.True(t, dropped.keepEncoded(append([]byte{byte(idFormatV1)}, []byte("trace")...)), "prefixes must not match")
	require.True(t, dropped.keepEncoded(append([]byte{byte(idFormatV1)}, []byte("trace-b-tail")...)), "extensions must not match")
	require.True(t, dropped.keepEncoded(nil), "malformed values must fail open")
	require.True(t, dropped.keepEncoded([]byte{0xff, 't'}), "unknown encodings must fail open")
}

func BenchmarkDroppedTraceIDLookup(b *testing.B) {
	traceIDs := make([]string, dropSetBenchmarkTraceCount)
	encoded := make([][]byte, dropSetBenchmarkTraceCount)
	for traceIdx := range traceIDs {
		traceIDs[traceIdx] = fmt.Sprintf("service-a-%032x", traceIdx)
	}
	for traceIdx := range encoded {
		queryIdx := traceIdx * 7919 % len(traceIDs)
		encoded[traceIdx] = append([]byte{byte(idFormatV1)}, traceIDs[queryIdx]...)
	}
	for _, ratio := range []int{1, 35, 99} {
		droppedTraceIDs := make([]string, 0, len(traceIDs)*ratio/100)
		for traceIdx, traceID := range traceIDs {
			if traceIdx%100 < ratio {
				droppedTraceIDs = append(droppedTraceIDs, traceID)
			}
		}
		b.Run(fmt.Sprintf("ratio-%d/compact-hash", ratio), func(b *testing.B) {
			dropped := acquireDroppedTraceIDs()
			for _, traceID := range droppedTraceIDs {
				dropped.add(traceID)
			}
			defer releaseDroppedTraceIDs(dropped)
			benchmarkDropSetLookup(b, encoded, dropped.keepEncoded)
		})
		b.Run(fmt.Sprintf("ratio-%d/sorted-slice", ratio), func(b *testing.B) {
			dropped := droppedTraceIDs
			benchmarkDropSetLookup(b, encoded, func(data []byte) bool {
				traceID := convert.BytesToString(data[1:])
				matchIdx := sort.SearchStrings(dropped, traceID)
				return matchIdx == len(dropped) || dropped[matchIdx] != traceID
			})
		})
		b.Run(fmt.Sprintf("ratio-%d/go-map", ratio), func(b *testing.B) {
			dropped := make(map[string]struct{}, len(droppedTraceIDs))
			for _, traceID := range droppedTraceIDs {
				dropped[traceID] = struct{}{}
			}
			benchmarkDropSetLookup(b, encoded, func(data []byte) bool {
				_, exists := dropped[string(data[1:])]
				return !exists
			})
		})
	}
}

// TestProjectedIndexBytesMatchesBuildIndex asserts projectedIndexBytes is
// exact, not conservative, at every tested n including the power-of-two
// boundaries where nextPow2(2n) is discontinuous.
func TestProjectedIndexBytesMatchesBuildIndex(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 4, 5, 7, 8, 9, 1023, 1024, 1025, 65535, 65536, 65537} {
		n := n
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			// A fresh set, not acquireDroppedTraceIDs: on a pooled object buildIndex
			// reuses carried capacity, so cap(slots) would exceed the projection and
			// the allocation-exactness assertion below would depend on which tests
			// ran first. That reuse case is covered by
			// TestProjectedIndexBytesCoversPooledReuse.
			dropped := &droppedTraceIDs{}
			for traceIdx := range n {
				dropped.add(fmt.Sprintf("service-a-%032x", traceIdx))
			}
			dropped.buildIndex()
			require.Equal(t, int64(len(dropped.slots))*8, projectedIndexBytes(n))
			require.Equal(t, int64(cap(dropped.slots))*8, projectedIndexBytes(n),
				"on a fresh set the projection must equal exactly what buildIndex allocated")
		})
	}
}

// TestProjectedIndexBytesCoversPooledReuse asserts the projection still
// describes the index's residency when buildIndex satisfies its slot count from
// capacity a pooled set carried in and so allocates nothing. Asserting on len
// alone would pass while the resident capacity silently exceeded it.
func TestProjectedIndexBytesCoversPooledReuse(t *testing.T) {
	dropped := &droppedTraceIDs{slots: make([]uint64, 0, 1<<16)}
	for traceIdx := range 8 {
		dropped.add(fmt.Sprintf("service-a-%032x", traceIdx))
	}
	carriedCap := cap(dropped.slots)
	dropped.buildIndex()
	require.Equal(t, carriedCap, cap(dropped.slots), "buildIndex must reuse the carried capacity")
	require.Equal(t, int64(len(dropped.slots))*8, projectedIndexBytes(8))

	// The carried capacity is resident regardless of what the index needs, so
	// residentBytes — not the projection — is what a budget must be judged against.
	tracker := dropTracker{exact: dropped}
	require.Greater(t, tracker.residentBytes(), tracker.liveBytes()+projectedIndexBytes(8))
}

// TestCanAcceptStopsAtBudget forces a dropTracker to its ceiling at three
// budgets and asserts (a) canAccept never returns true again once it returns
// false, (b) the exact residency at the stop point is at or below the budget
// maxIDs was derived from, and (c) building the index afterward does not push
// measured residency above the budget.
func TestCanAcceptStopsAtBudget(t *testing.T) {
	for _, budget := range []uint64{1 << 20, 8 << 20, 32 << 20} {
		budget := budget
		t.Run(fmt.Sprintf("budget=%d", budget), func(t *testing.T) {
			// A fresh, non-pooled set: acquireDroppedTraceIDs draws from the shared
			// reuse pool, whose objects may carry over capacity from unrelated
			// earlier tests, which would make the exact residency assertions below
			// depend on test execution order instead of on n and budget alone.
			dropped := &droppedTraceIDs{}
			tracker := dropTracker{exact: dropped, budget: budget}

			traceIdx := 0
			for tracker.canAccept() {
				tracker.record(fmt.Sprintf("service-a-%032x", traceIdx))
				traceIdx++
			}
			require.True(t, tracker.full)
			require.Positive(t, tracker.maxIDs, "maxIDs must be derived from the first recorded ID")
			stoppedLen := dropped.len()

			// (a) canAccept never returns true again.
			for probe := range 3 {
				require.False(t, tracker.canAccept(), "probe %d", probe)
			}

			// (b) the exact residency at the stop point, including the index the
			// next attempted drop would have built, is at or below the budget.
			projected := tracker.liveBytes() + projectedIndexBytes(stoppedLen+1)
			require.LessOrEqual(t, projected, int64(budget))

			// (c) building the index for real leaves measured residency at or
			// below the budget too. residentBytes folds in cap(slots), so it is
			// the post-build residency; liveBytes alone would omit the index.
			dropped.buildIndex()
			require.LessOrEqual(t, tracker.residentBytes(), int64(budget))
		})
	}
}

// TestZeroBudgetIsUnlimited asserts a zero budget never bounds a dropTracker and
// never derives a ceiling. Production merges resolve a non-zero ceiling, so this
// pins the opt-out path: a filter built without a budget must behave exactly as
// the unbounded collector did before the ceiling existed.
func TestZeroBudgetIsUnlimited(t *testing.T) {
	dropped := acquireDroppedTraceIDs()
	t.Cleanup(func() { releaseDroppedTraceIDs(dropped) })
	tracker := dropTracker{exact: dropped}
	require.Zero(t, maxIDsForBudget(0, 42))

	for traceIdx := range 500_000 {
		require.True(t, tracker.canAccept(), "traceIdx=%d", traceIdx)
		tracker.record(fmt.Sprintf("service-a-%032x", traceIdx))
	}
	require.False(t, tracker.full)
	require.Zero(t, tracker.maxIDs, "an unlimited tracker must not derive a ceiling")
}

// TestTinyBudgetStillBounds asserts a budget too small to hold even one entry
// resolves to a ceiling of one rather than dividing to zero and being read back
// as the unlimited sentinel. Without the clamp in maxIDsForBudget, the tightest
// budget would disable the ceiling entirely.
func TestTinyBudgetStillBounds(t *testing.T) {
	const idLen = len("service-a-0")
	entryBytes := uint64(dropSetBytesPerEntry(idLen))
	for _, budget := range []uint64{1, entryBytes - 1, entryBytes} {
		budget := budget
		t.Run(fmt.Sprintf("budget=%d", budget), func(t *testing.T) {
			require.Equal(t, 1, maxIDsForBudget(budget, idLen))
			dropped := &droppedTraceIDs{}
			tracker := dropTracker{exact: dropped, budget: budget}
			require.True(t, tracker.canAccept())
			tracker.record("service-a-0")
			require.False(t, tracker.canAccept())
			require.True(t, tracker.full)
		})
	}
}

// TestBytesPerEntryTracksIDLength is the DS-1b regression test: with a fixed
// bytes/entry constant priced for a 42-byte ID, longer trace IDs overshot the
// budget proportionally (about 1.4x at 96 bytes, 3x at 256), so the ceiling was
// not a ceiling. Deriving the price from the recorded length must keep measured
// residency at or below the budget at every length.
func TestBytesPerEntryTracksIDLength(t *testing.T) {
	const budget = 1 << 20
	for _, idLen := range []int{42, 64, 96, 128, 256} {
		idLen := idLen
		t.Run(fmt.Sprintf("idLen=%d", idLen), func(t *testing.T) {
			dropped := &droppedTraceIDs{}
			tracker := dropTracker{exact: dropped, budget: budget}
			traceIdx := 0
			for tracker.canAccept() {
				tracker.record(fmt.Sprintf("%0*x", idLen, traceIdx))
				traceIdx++
			}
			require.Equal(t, idLen, tracker.sampledIDLen)
			dropped.buildIndex()
			require.LessOrEqual(t, tracker.residentBytes(), int64(budget))
			// Longer IDs must yield a strictly smaller ceiling, or the price is not
			// tracking length at all.
			require.Equal(t, maxIDsForBudget(budget, idLen), tracker.maxIDs)
		})
	}
	require.Greater(t, maxIDsForBudget(budget, 42), maxIDsForBudget(budget, 256))
}

// TestTrackerRederivesOnLongerID asserts the derived ceiling is self-correcting:
// a shard is expected to have one trace-ID length, but if a longer ID does appear
// the price is recomputed rather than left at the first length observed, so the
// uniformity is not a load-bearing assumption.
func TestTrackerRederivesOnLongerID(t *testing.T) {
	const budget = 4 << 10
	dropped := &droppedTraceIDs{}
	tracker := dropTracker{exact: dropped, budget: budget}

	tracker.record(fmt.Sprintf("%040x", 0))
	shortCeiling := tracker.maxIDs
	require.Equal(t, maxIDsForBudget(budget, 40), shortCeiling)

	tracker.record(fmt.Sprintf("%0400x", 1))
	require.Equal(t, 400, tracker.sampledIDLen)
	require.Equal(t, maxIDsForBudget(budget, 400), tracker.maxIDs)
	require.Less(t, tracker.maxIDs, shortCeiling, "a longer ID must lower the ceiling")

	// A subsequent shorter ID must not raise the ceiling back up.
	tracker.record(fmt.Sprintf("%041x", 2))
	require.Equal(t, 400, tracker.sampledIDLen)
	require.Equal(t, maxIDsForBudget(budget, 400), tracker.maxIDs)
}

// BenchmarkDropSetResidency reports bytes/entry for a fully-built drop set at
// two scales, so DS-9's arena-backed IDs have a baseline to improve on.
func BenchmarkDropSetResidency(b *testing.B) {
	for _, entryCount := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("entries-%d", entryCount), func(b *testing.B) {
			// A fresh set rather than acquireDroppedTraceIDs: a pooled object can
			// carry slot capacity over from an unrelated earlier run, which would
			// make the reported bytes/entry depend on execution order and so be
			// useless as DS-9's baseline.
			dropped := &droppedTraceIDs{}
			for traceIdx := range entryCount {
				dropped.add(fmt.Sprintf("service-a-%032x", traceIdx))
			}
			tracker := dropTracker{exact: dropped}
			bytesPerEntry := float64(tracker.liveBytes()+projectedIndexBytes(entryCount)) / float64(entryCount)
			// ResetTimer clears any metric reported before it, so report after.
			b.ResetTimer()
			b.ReportMetric(bytesPerEntry, "bytes/entry")
			for range b.N {
				_ = tracker.canAccept()
			}
			// Deliberately not returned to the pool: seeding it with this set's
			// capacity is the ordering dependency the fresh set above avoids.
		})
	}
}

func benchmarkDropSetLookup(b *testing.B, encoded [][]byte, keep func([]byte) bool) {
	b.Helper()
	b.ReportMetric(float64(len(encoded)), "lookups/op")
	b.ReportAllocs()
	b.ResetTimer()
	var retained int
	for range b.N {
		for _, data := range encoded {
			if keep(data) {
				retained++
			}
		}
	}
	if retained == 0 {
		b.Fatal("lookup result was not consumed")
	}
}
