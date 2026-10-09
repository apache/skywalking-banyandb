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
	"math"
	"math/rand"
	"testing"
)

type sortableRun struct {
	shift       uint
	first, last uint64
}

func coverOf(lo, hi uint64) []sortableRun {
	runs := make([]sortableRun, 0, 64)
	SplitSortableRange(lo, hi, func(shift uint, first, last uint64) {
		runs = append(runs, sortableRun{shift: shift, first: first, last: last})
	})
	return runs
}

// covered reports whether a sortable value falls in the cover.
func covered(runs []sortableRun, value uint64) bool {
	for _, run := range runs {
		shifted := value >> run.shift
		if shifted >= run.first && shifted <= run.last {
			return true
		}
	}
	return false
}

// checkCover asserts the two properties the query path relies on: the cover is
// exact in both directions, and it stays within the documented term bound.
func checkCover(t *testing.T, lo, hi uint64) {
	t.Helper()
	runs := coverOf(lo, hi)
	if len(runs) == 0 {
		t.Fatalf("cover of [%d, %d] is empty", lo, hi)
	}
	if len(runs) > sortableRangeMaxTerms {
		t.Fatalf("cover of [%d, %d] used %d terms, above the %d bound", lo, hi, len(runs), sortableRangeMaxTerms)
	}
	for _, run := range runs {
		if run.first > run.last {
			t.Fatalf("cover of [%d, %d] emitted an inverted run %+v", lo, hi, run)
		}
		if run.shift > 60 {
			t.Fatalf("cover of [%d, %d] emitted shift %d, above the writer's top level", lo, hi, run.shift)
		}
	}
	// Endpoints and their immediate neighbours pin down both directions
	// without walking a 64-bit span.
	probes := []uint64{lo, hi}
	if lo > 0 {
		probes = append(probes, lo-1)
	}
	if hi < math.MaxUint64 {
		probes = append(probes, hi+1)
	}
	// Values sharing a bucket with each endpoint must be covered when the
	// endpoint is, which is what makes the cover exact for a whole bucket.
	for _, probe := range []uint64{lo, hi} {
		for shift := uint(0); shift <= 60; shift += 4 {
			probes = append(probes, probe&^(uint64(0xF)<<shift))
			probes = append(probes, probe|(uint64(0xF)<<shift))
		}
	}
	for _, probe := range probes {
		want := probe >= lo && probe <= hi
		if got := covered(runs, probe); got != want {
			t.Fatalf("cover of [%d, %d]: value %d covered=%v, want %v (runs %+v)", lo, hi, probe, got, want, runs)
		}
	}
}

func TestSplitSortableRangeCoversExactly(t *testing.T) {
	cases := [][2]uint64{
		{0, 0},
		{0, 15},
		{0, 16},
		{0, 17},
		{15, 15},
		{16, 31},
		{1, 14},
		{5, 10},
		{1 << 32, 1<<32 + 15},
		{0, math.MaxUint64},
		{math.MaxUint64 - 1, math.MaxUint64},
		{0x8000000000000000, 0x8000000000000000},
		{0x7FFFFFFFFFFFFFFF, 0x8000000000000000},
		{0, 0x8000000000000000},
	}
	for _, testCase := range cases {
		checkCover(t, testCase[0], testCase[1])
	}
}

func TestSplitSortableRangeMatchesBruteForce(t *testing.T) {
	// Fixed seed: a failure has to be reproducible from the printed case.
	source := rand.New(rand.NewSource(20261009)) //nolint:gosec // deterministic test fixture, not security
	for iteration := 0; iteration < 10000; iteration++ {
		var lo, hi uint64
		switch iteration % 4 {
		case 0:
			lo, hi = source.Uint64(), source.Uint64()
		case 1:
			// Small ranges, which exercise the fine levels.
			lo = source.Uint64() % 4096
			hi = lo + source.Uint64()%4096
		case 2:
			// Ranges aligned to a level, which skip whole levels.
			shift := uint(source.Intn(16)) * 4
			lo = source.Uint64() &^ (uint64(0xF) << shift)
			hi = lo | (uint64(0xF) << shift)
		default:
			// Ranges spanning the sign flip in sortable space.
			lo = 0x8000000000000000 - source.Uint64()%1024
			hi = 0x8000000000000000 + source.Uint64()%1024
			if lo > hi {
				lo, hi = hi, lo
			}
		}
		if lo > hi {
			lo, hi = hi, lo
		}
		runs := coverOf(lo, hi)
		// Exhaustively verify narrow ranges so an unsound bucket cannot hide
		// in a region the probe set missed.
		if hi-lo <= 8192 {
			for value := lo; ; value++ {
				if !covered(runs, value) {
					t.Fatalf("cover of [%d, %d] misses %d", lo, hi, value)
				}
				if value == hi {
					break
				}
			}
			if lo > 0 && covered(runs, lo-1) {
				t.Fatalf("cover of [%d, %d] includes %d below the range", lo, hi, lo-1)
			}
			if hi < math.MaxUint64 && covered(runs, hi+1) {
				t.Fatalf("cover of [%d, %d] includes %d above the range", lo, hi, hi+1)
			}
		}
		checkCover(t, lo, hi)
	}
}

func TestSplitSortableRangeTermBoundIsTight(t *testing.T) {
	// The widest cover a 64-bit range produces must stay within the bound the
	// query path budgets dictionary lookups against.
	worst := 0
	source := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic test fixture, not security
	for iteration := 0; iteration < 2000; iteration++ {
		lo := source.Uint64() & 0x0FFF0FFF0FFF0FFF
		hi := source.Uint64() & 0x0FFF0FFF0FFF0FFF
		if lo > hi {
			lo, hi = hi, lo
		}
		if terms := len(coverOf(lo, hi)); terms > worst {
			worst = terms
		}
	}
	if worst > sortableRangeMaxTerms {
		t.Fatalf("widest cover was %d terms, above the %d bound", worst, sortableRangeMaxTerms)
	}
	t.Logf("widest observed cover: %d terms (bound %d)", worst, sortableRangeMaxTerms)
}

func TestSplitSortableRangeRealisticWindowIsSmall(t *testing.T) {
	// A 15-minute window expressed in nanoseconds, at a plausible epoch: the
	// case the design motivates, and one that must not cost hundreds of
	// dictionary lookups per segment.
	const begin = int64(1_700_000_000_000_000_000)
	const fifteenMinutes = int64(15 * 60 * 1e9)
	terms := len(coverOf(sortableInt64(begin), sortableInt64(begin+fifteenMinutes-1)))
	if terms > 64 {
		t.Fatalf("a 15-minute window needed %d terms, want a few dozen", terms)
	}
	t.Logf("15-minute window: %d terms", terms)
}

func TestEncodePrefixCodedSortableShiftMatchesTheSignedEncoder(t *testing.T) {
	values := []int64{0, 1, -1, 42, -42, math.MaxInt64, math.MinInt64, math.MinInt64 + 1}
	source := rand.New(rand.NewSource(11)) //nolint:gosec // deterministic test fixture, not security
	for iteration := 0; iteration < 2000; iteration++ {
		values = append(values, int64(source.Uint64()))
	}
	for _, value := range values {
		for shift := uint(0); shift <= 60; shift += 4 {
			want := EncodePrefixCodedInt64Shift(value, shift)
			got := encodePrefixCodedSortableShift(sortableInt64(value), shift)
			if string(want) != string(got) {
				t.Fatalf("value %d shift %d: sortable encoder = %x, signed encoder = %x", value, shift, got, want)
			}
		}
	}
}

func TestEncodePrefixCodedSortableShiftPreservesOrder(t *testing.T) {
	// Encoded terms at a fixed shift must be non-decreasing in sortable order,
	// which is what lets a cover be turned into a dictionary range. Two values
	// sharing a bucket at that shift encode identically, so strictness is only
	// required when they differ at that shift.
	values := []int64{math.MinInt64, -1_000_000, -1, 0, 1, 1_000_000, math.MaxInt64}
	for shift := uint(0); shift <= 60; shift += 4 {
		for outer := 0; outer+1 < len(values); outer++ {
			left, right := values[outer], values[outer+1]
			leftEncoded := encodePrefixCodedSortableShift(sortableInt64(left), shift)
			rightEncoded := encodePrefixCodedSortableShift(sortableInt64(right), shift)
			if string(leftEncoded) > string(rightEncoded) {
				t.Fatalf("shift %d: %d encoded to %x, above %d's %x",
					shift, left, leftEncoded, right, rightEncoded)
			}
			sameBucket := sortableInt64(left)>>shift == sortableInt64(right)>>shift
			if !sameBucket && string(leftEncoded) == string(rightEncoded) {
				t.Fatalf("shift %d: %d and %d are in different buckets but both encoded to %x",
					shift, left, right, leftEncoded)
			}
		}
	}
}

func TestSplitSortableRangeEmitsNonOverlappingBuckets(t *testing.T) {
	// The emitted runs must partition the range: no value may sit in two runs,
	// or the cover would double-count documents.
	source := rand.New(rand.NewSource(23)) //nolint:gosec // deterministic test fixture, not security
	for iteration := 0; iteration < 5000; iteration++ {
		lo := source.Uint64() % (1 << uint(source.Intn(48)+1))
		hi := lo + source.Uint64()%(1<<uint(source.Intn(20)))
		if hi-lo > 4096 {
			hi = lo + 4096
		}
		runs := coverOf(lo, hi)
		hits := make([]int, len(runs))
		for value := lo; ; value++ {
			for index, run := range runs {
				if (value>>run.shift) >= run.first && (value>>run.shift) <= run.last {
					hits[index]++
				}
			}
			if value == hi {
				break
			}
		}
		for index, run := range runs {
			if hits[index] == 0 {
				t.Fatalf("cover of [%d, %d] emitted an empty run %+v at index %d", lo, hi, run, index)
			}
		}
	}
}
