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
	"math"
	"testing"
)

// handleWithBounds builds a handle whose footer slots hold the given
// timestamps, taking int64 so the uint64(int64) conversion the owner performs
// happens here rather than on a constant.
func handleWithBounds(minBound, maxBound int64, hasTime bool) *segmentHandle {
	return &segmentHandle{timeMin: uint64(minBound), timeMax: uint64(maxBound), hasTime: hasTime}
}

func closedRange(lower, upper int64) *TimeRange {
	return &TimeRange{Lower: lower, Upper: upper, IncludesLower: true, IncludesUpper: true}
}

func TestClassifyTime(t *testing.T) {
	tests := []struct {
		handle    *segmentHandle
		timeRange *TimeRange
		name      string
		want      timeClass
	}{
		{
			name:      "no range is fully covered",
			handle:    handleWithBounds(100, 200, true),
			timeRange: nil,
			want:      timeContained,
		},
		{
			name:      "unknown bounds never prune",
			handle:    handleWithBounds(0, 0, false),
			timeRange: closedRange(100, 200),
			want:      timeOverlap,
		},
		{
			name: "bounds inverted by a sign change are unknown",
			// This is what an unsigned fold over a set spanning zero produces:
			// uint64(-5000) is the larger slot, so the pair reads back inverted.
			handle:    handleWithBounds(5000, -5000, true),
			timeRange: closedRange(-10, 10),
			want:      timeOverlap,
		},
		{
			name:      "segment below the range",
			handle:    handleWithBounds(10, 20, true),
			timeRange: closedRange(100, 200),
			want:      timeDisjoint,
		},
		{
			name:      "segment above the range",
			handle:    handleWithBounds(300, 400, true),
			timeRange: closedRange(100, 200),
			want:      timeDisjoint,
		},
		{
			name:      "segment entirely inside the range",
			handle:    handleWithBounds(120, 180, true),
			timeRange: closedRange(100, 200),
			want:      timeContained,
		},
		{
			name:      "segment exactly equal to the range",
			handle:    handleWithBounds(100, 200, true),
			timeRange: closedRange(100, 200),
			want:      timeContained,
		},
		{
			name:      "segment straddling the lower edge",
			handle:    handleWithBounds(50, 150, true),
			timeRange: closedRange(100, 200),
			want:      timeOverlap,
		},
		{
			name:      "segment straddling the upper edge",
			handle:    handleWithBounds(150, 250, true),
			timeRange: closedRange(100, 200),
			want:      timeOverlap,
		},
		{
			name:      "segment enclosing the range",
			handle:    handleWithBounds(50, 250, true),
			timeRange: closedRange(100, 200),
			want:      timeOverlap,
		},
		{
			name:      "exclusive lower excludes a touching segment",
			handle:    handleWithBounds(50, 100, true),
			timeRange: &TimeRange{Lower: 100, Upper: 200, IncludesLower: false, IncludesUpper: true},
			want:      timeDisjoint,
		},
		{
			name:      "inclusive lower keeps a touching segment",
			handle:    handleWithBounds(50, 100, true),
			timeRange: closedRange(100, 200),
			want:      timeOverlap,
		},
		{
			name:      "exclusive upper excludes a touching segment",
			handle:    handleWithBounds(200, 250, true),
			timeRange: &TimeRange{Lower: 100, Upper: 200, IncludesLower: true, IncludesUpper: false},
			want:      timeDisjoint,
		},
		{
			name:      "empty exclusive range at one value",
			handle:    handleWithBounds(100, 200, true),
			timeRange: &TimeRange{Lower: 100, Upper: 100, IncludesLower: false, IncludesUpper: true},
			want:      timeDisjoint,
		},
		{
			name:      "single inclusive value",
			handle:    handleWithBounds(100, 200, true),
			timeRange: closedRange(150, 150),
			want:      timeOverlap,
		},
		{
			name:      "empty range after exclusive steps",
			handle:    handleWithBounds(100, 200, true),
			timeRange: &TimeRange{Lower: 200, Upper: 100, IncludesLower: false, IncludesUpper: false},
			want:      timeDisjoint,
		},
		{
			name:      "exclusive bounds at the int64 limits do not wrap",
			handle:    handleWithBounds(math.MinInt64, math.MaxInt64, true),
			timeRange: &TimeRange{Lower: math.MinInt64, Upper: math.MaxInt64, IncludesLower: false, IncludesUpper: false},
			want:      timeContained,
		},
		{
			name:      "bounds at the int64 limits stay ordered",
			handle:    handleWithBounds(math.MinInt64, math.MaxInt64, true),
			timeRange: closedRange(math.MinInt64, math.MaxInt64),
			want:      timeContained,
		},
		{
			name:      "empty range at the int64 limits",
			handle:    handleWithBounds(math.MinInt64, math.MaxInt64, true),
			timeRange: &TimeRange{Lower: math.MinInt64, Upper: math.MinInt64, IncludesLower: false, IncludesUpper: false},
			want:      timeDisjoint,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyTime(test.handle, test.timeRange); got != test.want {
				t.Errorf("classifyTime = %v, want %v", got, test.want)
			}
		})
	}
}

func TestClassifyTimeAgreesWithPerDocumentCheck(t *testing.T) {
	// The classification has to agree with what the per-document check would
	// decide, for every combination of bounds and window. A disagreement is
	// either a lost document or a wrong one.
	bounds := [][2]int64{
		{0, 0}, {100, 100}, {50, 150}, {99, 101}, {100, 200}, {50, 250}, {200, 300}, {300, 400},
	}
	windows := []*TimeRange{
		nil,
		closedRange(100, 200),
		closedRange(150, 150),
		closedRange(0, 1000),
		closedRange(120, 180),
		closedRange(110, 190),
		closedRange(100, 199),
		closedRange(101, 200),
		{Lower: 100, Upper: 200, IncludesLower: false, IncludesUpper: false},
		closedRange(250, 350),
	}
	for _, bound := range bounds {
		// A segment's bounds cover the timestamps it holds; a document at the
		// low or high bound is inside the range whenever the segment overlaps.
		handle := handleWithBounds(bound[0], bound[1], true)
		for _, window := range windows {
			class := classifyTime(handle, window)
			if class == timeDisjoint {
				if window == nil {
					t.Fatalf("bounds %v: a nil range classified disjoint", bound)
				}
				lower := window.Lower
				if !window.IncludesLower {
					lower++
				}
				upper := window.Upper
				if !window.IncludesUpper {
					upper--
				}
				if lower <= upper && !(bound[1] < lower || bound[0] > upper) {
					t.Fatalf("bounds %v window %+v: disjoint but the segment overlaps", bound, window)
				}
			}
			if class == timeContained && window != nil {
				lower := window.Lower
				if !window.IncludesLower {
					lower++
				}
				upper := window.Upper
				if !window.IncludesUpper {
					upper--
				}
				if !(lower <= bound[0] && bound[1] <= upper) {
					t.Fatalf("bounds %v window %+v: contained but not fully covered", bound, window)
				}
			}
		}
	}
}

func TestEmptyTimeRange(t *testing.T) {
	tests := []struct {
		timeRange *TimeRange
		want      bool
	}{
		{closedRange(1, 2), false},
		{&TimeRange{Lower: 2, Upper: 1, IncludesLower: true, IncludesUpper: true}, true},
		{&TimeRange{Lower: 5, Upper: 5, IncludesLower: true, IncludesUpper: true}, false},
		{&TimeRange{Lower: 5, Upper: 5, IncludesLower: false, IncludesUpper: false}, true},
		{&TimeRange{Lower: 5, Upper: 5, IncludesLower: false, IncludesUpper: true}, false},
		{&TimeRange{Lower: math.MaxInt64, Upper: math.MaxInt64, IncludesLower: false, IncludesUpper: true}, true},
		{&TimeRange{Lower: math.MinInt64, Upper: math.MinInt64, IncludesLower: true, IncludesUpper: false}, true},
		{&TimeRange{Lower: math.MinInt64, Upper: math.MaxInt64, IncludesLower: false, IncludesUpper: false}, false},
	}
	for _, test := range tests {
		if got := emptyTimeRange(test.timeRange); got != test.want {
			t.Errorf("emptyTimeRange(%+v) = %v, want %v", test.timeRange, got, test.want)
		}
	}
}

func TestStepInt64DoesNotWrap(t *testing.T) {
	if got := stepInt64(math.MinInt64, false); got != math.MinInt64 {
		t.Errorf("stepInt64(MinInt64, decrease) = %d, want MinInt64 rather than a wrap", got)
	}
	if got := stepInt64(math.MinInt64, true); got != math.MinInt64 {
		t.Errorf("stepInt64(MinInt64, increase) = %d, want MinInt64", got)
	}
	if got := stepInt64(math.MaxInt64, false); got != math.MaxInt64 {
		t.Errorf("stepInt64(MaxInt64, decrease) = %d, want MaxInt64", got)
	}
	if got := stepInt64(5, false); got != 4 {
		t.Errorf("stepInt64(5, decrease) = %d, want 4", got)
	}
	if got := stepInt64(5, true); got != 6 {
		t.Errorf("stepInt64(5, increase) = %d, want 6", got)
	}
}

func TestSortableInt64OrdersLikeSigned(t *testing.T) {
	values := []int64{math.MinInt64, -1, 0, 1, math.MaxInt64}
	for outer := 0; outer+1 < len(values); outer++ {
		if sortableInt64(values[outer]) >= sortableInt64(values[outer+1]) {
			t.Errorf("sortableInt64(%d) >= sortableInt64(%d)", values[outer], values[outer+1])
		}
	}
}
