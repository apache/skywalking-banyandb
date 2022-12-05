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
)

// TimeRange is a range of periods into which data can be written or retrieved.
type TimeRange struct {
	Start        time.Time
	End          time.Time
	IncludeStart bool
	IncludeEnd   bool
}

// Contains returns whether the unixNano is in the TimeRange.
func (t TimeRange) Contains(unixNano uint64) bool {
	tp := time.Unix(0, int64(unixNano))
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
