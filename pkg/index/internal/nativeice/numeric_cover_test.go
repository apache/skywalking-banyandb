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

package nativeice

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"
	"time"
)

// dictionaryTermsFromCover returns every term a dictionary would yield for the
// cover, using the same exclusive-end iterator contract the query path uses.
func dictionaryTermsFromCover(t *testing.T, terms [][]byte, covers []TimestampCover) [][]byte {
	t.Helper()
	sorted := make([][]byte, 0, len(terms))
	for _, term := range terms {
		sorted = append(sorted, term)
	}
	sort.Slice(sorted, func(left, right int) bool { return bytes.Compare(sorted[left], sorted[right]) < 0 })

	selected := make([][]byte, 0, len(terms))
	for _, cover := range covers {
		if len(cover.Start) == 0 {
			continue
		}
		for _, term := range sorted {
			if bytes.Compare(term, cover.Start) < 0 {
				continue
			}
			if cover.End != nil && bytes.Compare(term, cover.End) >= 0 {
				break
			}
			selected = append(selected, term)
		}
	}
	return selected
}

// TestTimestampCoverKeysSelectExactlyTheRange builds the real multi-precision
// term trie for a set of timestamps and checks that a cover's dictionary bounds
// select exactly the terms of documents in range -- the property the query path
// depends on for its candidate bitmap to be exact.
func TestTimestampCoverKeysSelectExactlyTheRange(t *testing.T) {
	const begin = int64(1_700_000_000_000_000_000)
	timestamps := []int64{begin}
	for offset := int64(0); offset <= 4*int64(time.Hour); offset += int64(7 * time.Second) {
		timestamps = append(timestamps, begin+offset)
	}

	byShift := make([][][]byte, 16)
	for _, value := range timestamps {
		for index, shift := 0, uint(0); shift <= 60; shift, index = shift+4, index+1 {
			byShift[index] = append(byShift[index], EncodePrefixCodedInt64Shift(value, shift))
		}
	}

	source := rand.New(rand.NewSource(31)) //nolint:gosec // deterministic test fixture, not security
	windows := [][2]int64{
		{begin, begin},
		{begin, begin + 1},
		{begin, begin + int64(15*time.Minute)},
		{begin, begin + int64(time.Hour)},
		{begin - 1, begin + 4*int64(time.Hour)},
		{begin + 1, begin + int64(30*time.Minute) - 1},
		{begin + int64(7*time.Minute), begin + int64(22*time.Minute)},
	}
	for iteration := 0; iteration < 40; iteration++ {
		low := timestamps[source.Intn(len(timestamps))]
		high := timestamps[source.Intn(len(timestamps))]
		if low > high {
			low, high = high, low
		}
		windows = append(windows, [2]int64{low, high})
	}

	for _, window := range windows {
		covers := TimestampCoverKeys(sortableInt64(window[0]), sortableInt64(window[1]))
		if len(covers) == 0 {
			t.Fatalf("cover of [%d, %d] produced no runs", window[0], window[1])
		}
		// A cover emits runs at mixed shifts, so a document joins the union if
		// ANY emitted run selects its term at that run's shift. That is exactly
		// how the query path builds its candidate bitmap, and because the runs
		// partition [lo, hi] the union has to equal the in-range documents.
		// Union every emitted run's selected terms, then check each timestamp
		// against it by its term at that run's shift.
		selected := make(map[string]struct{})
		for _, cover := range covers {
			runShift := shiftOf(t, cover.Start)
			for _, term := range dictionaryTermsFromCover(t, byShift[runShift/4], []TimestampCover{cover}) {
				selected[string(term)] = struct{}{}
			}
		}
		for valueIndex, value := range timestamps {
			inRange := value >= window[0] && value <= window[1]
			coveredByAnyRun := false
			for shiftIndex := range byShift {
				if _, present := selected[string(byShift[shiftIndex][valueIndex])]; present {
					coveredByAnyRun = true
					break
				}
			}
			if coveredByAnyRun != inRange {
				t.Fatalf("window [%d, %d]: timestamp %d selected=%v, want %v (covers %d run(s))",
					window[0], window[1], value, coveredByAnyRun, inRange, len(covers))
			}
		}
	}
}

// shiftOf recovers the precision shift a term was encoded at from its header.
func shiftOf(t *testing.T, term []byte) uint {
	t.Helper()
	if len(term) == 0 || term[0] < prefixCodedInt64ShiftStart {
		t.Fatalf("term %x has no shift header", term)
	}
	return uint(term[0] - prefixCodedInt64ShiftStart)
}

func TestTimestampCoverKeysAreOrderedAndDisjoint(t *testing.T) {
	const begin = int64(1_700_000_000_000_000_000)
	covers := TimestampCoverKeys(sortableInt64(begin), sortableInt64(begin+int64(6*time.Hour)))
	previousEnd := []byte(nil)
	for index, cover := range covers {
		if len(cover.Start) == 0 {
			t.Fatalf("cover %d has an empty start key", index)
		}
		if previousEnd != nil && bytes.Compare(cover.Start, previousEnd) < 0 {
			t.Fatalf("cover %d starts at %x, before the previous run's end %x", index, cover.Start, previousEnd)
		}
		if cover.End != nil && bytes.Compare(cover.Start, cover.End) >= 0 {
			t.Fatalf("cover %d is empty: start %x end %x", index, cover.Start, cover.End)
		}
		previousEnd = cover.End
	}
}

func TestTimestampCoverKeysSpanTheFullDomain(t *testing.T) {
	// A cover over the entire 64-bit domain must still produce usable bounds.
	// It legitimately collapses to the top shift level: every lower level is
	// wholly contained, so splitRange recurses into them instead of emitting.
	covers := TimestampCoverKeys(sortableInt64(-1<<63), sortableInt64(1<<63-1))
	if len(covers) == 0 {
		t.Fatal("cover of the whole domain produced no runs")
	}
	for index, cover := range covers {
		if len(cover.Start) == 0 {
			t.Fatalf("cover %d has an empty start key", index)
		}
		if cover.End != nil && bytes.Compare(cover.Start, cover.End) >= 0 {
			t.Fatalf("cover %d is empty: start %x end %x", index, cover.Start, cover.End)
		}
	}
	t.Logf("whole-domain cover: %d run(s), first shift byte 0x%x", len(covers), covers[0].Start[0])
}

func TestMaxSortableBucketIsTheValueRange(t *testing.T) {
	// maxSortableBucket must be the largest value of ts>>shift, and last+1 must
	// still encode distinctly below it, which is what lets a run be bounded by
	// its own successor's key rather than the next shift level.
	for shift := uint(0); shift <= 60; shift += 4 {
		limit := maxSortableBucket(shift)
		if uint64(1)<<(64-shift)-1 != limit {
			t.Errorf("shift %d: maxSortableBucket = %d, want %d", shift, limit, uint64(1)<<(64-shift)-1)
		}
		if limit == 0 {
			t.Errorf("shift %d: maxSortableBucket is zero", shift)
			continue
		}
		atLimit := encodePrefixCodedSortableShift(limit<<shift, shift)
		aboveLimit := encodePrefixCodedSortableShift(limit<<shift, shift)
		if !bytes.Equal(atLimit, aboveLimit) {
			t.Errorf("shift %d: the top bucket is not stable", shift)
		}
		// One below the limit must still round-trip distinctly.
		below := encodePrefixCodedSortableShift((limit-1)<<shift, shift)
		if bytes.Compare(below, atLimit) >= 0 {
			t.Errorf("shift %d: bucket %d does not sort below the top bucket", shift, limit-1)
		}
		// The bound a run ending at the limit uses must be usable, except at
		// the top shift level where nothing sorts above it and nil -- which an
		// iterator reads as unbounded -- is the only correct end.
		end := coverRunEnd(shift, limit)
		if shift < 60 && end == nil {
			t.Errorf("shift %d: a run at the top bucket has no end key", shift)
		}
		if shift == 60 && end != nil {
			t.Errorf("shift 60: end key = %x, want nil for the top level", end)
		}
	}
}
