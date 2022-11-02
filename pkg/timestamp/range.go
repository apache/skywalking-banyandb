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

type TimeRange struct {
	Start        time.Time
	End          time.Time
	IncludeStart bool
	IncludeEnd   bool
}

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

func (t TimeRange) Overlapping(other TimeRange) bool {
	if t.Start.Equal(other.End) {
		return t.IncludeStart && other.IncludeEnd
	}
	if other.Start.Equal(t.End) {
		return t.IncludeEnd && other.IncludeStart
	}
	return !t.Start.After(other.End) && !other.Start.After(t.End)
}

func (t TimeRange) Duration() time.Duration {
	return t.End.Sub(t.Start)
}

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

func NewInclusiveTimeRange(start, end time.Time) TimeRange {
	return TimeRange{
		Start:        start,
		End:          end,
		IncludeStart: true,
		IncludeEnd:   true,
	}
}

func NewInclusiveTimeRangeDuration(start time.Time, duration time.Duration) TimeRange {
	return NewTimeRangeDuration(start, duration, true, true)
}

func NewSectionTimeRange(start, end time.Time) TimeRange {
	return NewTimeRange(start, end, true, false)
}

func NewTimeRange(start, end time.Time, includeStart, includeEnd bool) TimeRange {
	return TimeRange{
		Start:        start,
		End:          end,
		IncludeStart: includeStart,
		IncludeEnd:   includeEnd,
	}
}

func NewTimeRangeDuration(start time.Time, duration time.Duration, includeStart, includeEnd bool) TimeRange {
	return NewTimeRange(start, start.Add(duration), includeStart, includeEnd)
}
