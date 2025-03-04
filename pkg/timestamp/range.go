// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package timestamp

import (
	"time"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// TimeRange is a range of periods into which data can be written or retrieved.
type TimeRange struct {
	Start        time.Time
	End          time.Time
	IncludeStart bool
	IncludeEnd   bool
}

// Before returns whether the TimeRange is before the other time.
func (t TimeRange) Before(other time.Time) bool {
	if t.IncludeEnd {
		return t.End.Before(other)
	}
	return !t.End.After(other)
}

// Contains returns whether the unixNano is in the TimeRange.
func (t TimeRange) Contains(unixNano int64) bool {
	tp := time.Unix(0, unixNano)
	if t.Start.Equal(tp) {
		return t.IncludeStart
	}
	if t.End.Equal(tp) {
		return t.IncludeEnd
	}
	return !tp.Before(t.Start) && !tp.After(t.End)
}

// Overlapping returns whether TimeRanges intersect each other.
func (t TimeRange) Overlapping(other TimeRange) bool {
	if t.Start.Equal(other.End) {
		return t.IncludeStart && other.IncludeEnd
	}
	if other.Start.Equal(t.End) {
		return t.IncludeEnd && other.IncludeStart
	}
	return !t.Start.After(other.End) && !other.Start.After(t.End)
}

// Include returns whether the TimeRange includes the other TimeRange.
func (t TimeRange) Include(other TimeRange) bool {
	var start, end bool
	if t.Start.Equal(other.Start) {
		start = t.IncludeStart || !other.IncludeStart
	} else {
		start = !t.Start.After(other.Start)
	}
	if t.End.Equal(other.End) {
		end = t.IncludeEnd || !other.IncludeEnd
	} else {
		end = !t.End.Before(other.End)
	}
	return start && end
}

// Duration converts TimeRange to time.Duration.
func (t TimeRange) Duration() time.Duration {
	return t.End.Sub(t.Start)
}

// String shows the string representation.
func (t TimeRange) String() string {
	var buf []byte
	if t.IncludeStart {
		buf = []byte("[")
	} else {
		buf = []byte("(")
	}
	buf = append(buf, t.Start.String()...)
	buf = append(buf, ", "...)
	buf = append(buf, t.End.String()...)
	if t.IncludeEnd {
		buf = append(buf, "]"...)
	} else {
		buf = append(buf, ")"...)
	}
	return string(buf)
}

// NewInclusiveTimeRange returns TimeRange includes start and end time.
func NewInclusiveTimeRange(start, end time.Time) TimeRange {
	return TimeRange{
		Start:        start,
		End:          end,
		IncludeStart: true,
		IncludeEnd:   true,
	}
}

// NewInclusiveTimeRangeDuration returns TimeRange includes start and end time.
// It is created from a start time and the Duration.
func NewInclusiveTimeRangeDuration(start time.Time, duration time.Duration) TimeRange {
	return NewTimeRangeDuration(start, duration, true, true)
}

// NewSectionTimeRange returns TimeRange includes start time.
// This function is for creating blocks and segments of tsdb.
func NewSectionTimeRange(start, end time.Time) TimeRange {
	return NewTimeRange(start, end, true, false)
}

// NewTimeRange returns TimeRange.
func NewTimeRange(start, end time.Time, includeStart, includeEnd bool) TimeRange {
	return TimeRange{
		Start:        start,
		End:          end,
		IncludeStart: includeStart,
		IncludeEnd:   includeEnd,
	}
}

// NewTimeRangeDuration returns TimeRange from a start time and the Duration.
func NewTimeRangeDuration(start time.Time, duration time.Duration, includeStart, includeEnd bool) TimeRange {
	return NewTimeRange(start, start.Add(duration), includeStart, includeEnd)
}

// FindRange returns the indices of the first and last elements in a sorted 'timestamps' slice that are within the min and max range.
func FindRange[T int64 | uint64](timestamps []T, minVal, maxVal T) (int, int, bool) {
	if len(timestamps) == 0 {
		return -1, -1, false
	}
	isAsc := timestamps[0] <= timestamps[len(timestamps)-1]
	if isAsc && (timestamps[0] > maxVal || timestamps[len(timestamps)-1] < minVal) {
		return -1, -1, false
	}
	if !isAsc && (timestamps[0] < minVal || timestamps[len(timestamps)-1] > maxVal) {
		return -1, -1, false
	}

	start, end := -1, len(timestamps)
	for start < len(timestamps)-1 {
		start++
		if isAsc && timestamps[start] >= minVal || !isAsc && timestamps[start] <= maxVal {
			break
		}
	}
	for end > 0 {
		end--
		if isAsc && timestamps[end] <= maxVal || !isAsc && timestamps[end] >= minVal {
			break
		}
	}
	return start, end, start <= end
}

// Find returns the index of the target in the sorted 'timestamps' slice.
func Find(timestamps []int64, target int64) int {
	if len(timestamps) == 0 {
		return -1
	}
	if timestamps[0] > target || timestamps[len(timestamps)-1] < target {
		return -1
	}
	left, right := 0, len(timestamps)-1
	for left <= right {
		mid := (left + right) / 2
		if timestamps[mid] == target {
			return mid
		}
		if timestamps[mid] < target {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return -1
}

// PbHasOverlap checks if two TimeRanges have an overlap.
// Returns true if they have at least one point in common, false otherwise.
// Note: this follows [begin, end] (inclusive on both ends)
//
//	other follows [begin, end) (inclusive begin, exclusive end)
func PbHasOverlap(this, other *modelv1.TimeRange) bool {
	if this == nil || other == nil {
		return false
	}

	if this.Begin == nil || this.End == nil || other.Begin == nil || other.End == nil {
		return false
	}

	thisBegin := this.Begin.GetSeconds()*1e9 + int64(this.Begin.GetNanos())
	thisEnd := this.End.GetSeconds()*1e9 + int64(this.End.GetNanos())
	otherBegin := other.Begin.GetSeconds()*1e9 + int64(other.Begin.GetNanos())
	otherEnd := other.End.GetSeconds()*1e9 + int64(other.End.GetNanos())

	// Check for overlap:
	// - For x [begin, end] (inclusive on both ends)
	// - For other [begin, end) (inclusive begin, exclusive end)
	// They overlap when:
	// thisBegin <= otherEnd-1 && thisEnd >= otherBegin
	// Simplified to: thisBegin < otherEnd && thisEnd >= otherBegin
	return thisBegin < otherEnd && thisEnd >= otherBegin
}
