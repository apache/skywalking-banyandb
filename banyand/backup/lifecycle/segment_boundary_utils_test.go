// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package lifecycle

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
)

func TestCalculateTargetSegments(t *testing.T) {
	//nolint:govet // fieldalignment: test struct optimization not critical
	tests := []struct {
		name           string
		expected       []time.Time
		targetInterval storage.IntervalRule
		partMinTS      int64
		partMaxTS      int64
	}{
		{
			name:      "2-day to 3-day segments - part spans multiple segments",
			partMinTS: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano(), // Day 2
			partMaxTS: time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC).UnixNano(), // Day 4
			targetInterval: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  3,
			},
			expected: []time.Time{
				time.Date(2024, 1, 0, 0, 0, 0, 0, time.UTC), // Day 0-3
				time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC), // Day 3-6
			},
		},
		{
			name:      "2-day to 3-day segments - part fits in single segment",
			partMinTS: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(), // Day 1
			partMaxTS: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano(), // Day 2
			targetInterval: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  3,
			},
			expected: []time.Time{
				time.Date(2024, 1, 0, 0, 0, 0, 0, time.UTC), // Day 0-3
			},
		},
		{
			name:      "hour to day segments - part spans multiple segments",
			partMinTS: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano(), // Hour 12
			partMaxTS: time.Date(2024, 1, 1, 18, 0, 0, 0, time.UTC).UnixNano(), // Hour 18
			targetInterval: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  1,
			},
			expected: []time.Time{
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), // Day 1
			},
		},
		{
			name:      "day to hour segments - part spans multiple segments",
			partMinTS: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),    // Day 1
			partMaxTS: time.Date(2024, 1, 1, 23, 59, 59, 0, time.UTC).UnixNano(), // End of Day 1
			targetInterval: storage.IntervalRule{
				Unit: storage.HOUR,
				Num:  6,
			},
			expected: []time.Time{
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),  // Hour 0-6
				time.Date(2024, 1, 1, 6, 0, 0, 0, time.UTC),  // Hour 6-12
				time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC), // Hour 12-18
				time.Date(2024, 1, 1, 18, 0, 0, 0, time.UTC), // Hour 18-24
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateTargetSegments(tt.partMinTS, tt.partMaxTS, tt.targetInterval)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetSegmentTimeRange(t *testing.T) {
	//nolint:govet // fieldalignment: test struct optimization not critical
	tests := []struct {
		segmentStart  time.Time
		expectedStart time.Time
		expectedEnd   time.Time
		name          string
		interval      storage.IntervalRule
	}{
		{
			name:         "3-day segment",
			segmentStart: time.Date(2024, 1, 0, 0, 0, 0, 0, time.UTC),
			interval: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  3,
			},
			expectedStart: time.Date(2024, 1, 0, 0, 0, 0, 0, time.UTC),
			expectedEnd:   time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
		},
		{
			name:         "6-hour segment",
			segmentStart: time.Date(2024, 1, 1, 6, 0, 0, 0, time.UTC),
			interval: storage.IntervalRule{
				Unit: storage.HOUR,
				Num:  6,
			},
			expectedStart: time.Date(2024, 1, 1, 6, 0, 0, 0, time.UTC),
			expectedEnd:   time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getSegmentTimeRange(tt.segmentStart, tt.interval)
			assert.Equal(t, tt.expectedStart, result.Start)
			assert.Equal(t, tt.expectedEnd, result.End)
		})
	}
}
