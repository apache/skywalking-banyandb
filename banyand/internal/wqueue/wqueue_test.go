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

package wqueue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type mockSubQueue struct{}

func (m *mockSubQueue) Close() error {
	return nil
}

type mockSubQueueOption struct{}

func TestQueue_GetTimeRange(t *testing.T) {
	tests := []struct {
		inputTime       time.Time
		expectedStart   time.Time
		expectedEnd     time.Time
		name            string
		segmentInterval storage.IntervalRule
	}{
		{
			name: "hour interval",
			segmentInterval: storage.IntervalRule{
				Unit: storage.HOUR,
				Num:  1,
			},
			inputTime:     time.Date(2023, 12, 15, 14, 30, 45, 123456789, time.UTC),
			expectedStart: time.Date(2023, 12, 15, 14, 0, 0, 0, time.UTC),
			expectedEnd:   time.Date(2023, 12, 15, 15, 0, 0, 0, time.UTC),
		},
		{
			name: "day interval",
			segmentInterval: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  1,
			},
			inputTime:     time.Date(2023, 12, 15, 14, 30, 45, 123456789, time.UTC),
			expectedStart: time.Date(2023, 12, 15, 0, 0, 0, 0, time.UTC),
			expectedEnd:   time.Date(2023, 12, 16, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "multiple hours interval",
			segmentInterval: storage.IntervalRule{
				Unit: storage.HOUR,
				Num:  2,
			},
			inputTime:     time.Date(2023, 12, 15, 14, 30, 45, 123456789, time.UTC),
			expectedStart: time.Date(2023, 12, 15, 14, 0, 0, 0, time.UTC),
			expectedEnd:   time.Date(2023, 12, 15, 16, 0, 0, 0, time.UTC),
		},
		{
			name: "multiple days interval",
			segmentInterval: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  3,
			},
			inputTime:     time.Date(2023, 12, 15, 14, 30, 45, 123456789, time.UTC),
			expectedStart: time.Date(2023, 12, 15, 0, 0, 0, 0, time.UTC),
			expectedEnd:   time.Date(2023, 12, 18, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary directory for the test
			tempDir := t.TempDir()

			// Create queue options
			opts := Opts[*mockSubQueue, mockSubQueueOption]{
				Option:          mockSubQueueOption{},
				Metrics:         nil,
				SubQueueCreator: createMockSubQueue,
				Location:        tempDir,
				ShardNum:        1,
				SegmentInterval: tt.segmentInterval,
				GetNodes:        func(_ common.ShardID) []string { return []string{"node1"} },
			}

			// Create context with position
			ctx := context.Background()
			ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
				return common.Position{
					Database: "test-db",
					Stage:    "test-stage",
				}
			})
			ctx = context.WithValue(ctx, logger.ContextKey, logger.GetLogger("test"))

			// Create queue
			queue, err := Open[*mockSubQueue, mockSubQueueOption](ctx, opts, "test-group")
			require.NoError(t, err)
			defer queue.Close()

			// Test GetTimeRange
			timeRange := queue.GetTimeRange(tt.inputTime)

			// Verify results
			assert.Equal(t, tt.expectedStart, timeRange.Start)
			assert.Equal(t, tt.expectedEnd, timeRange.End)
			assert.True(t, timeRange.IncludeStart)
			assert.False(t, timeRange.IncludeEnd)
		})
	}
}

func createMockSubQueue(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
	_ mockSubQueueOption, _ any, _ string, _ common.ShardID, _ func() []string,
) (*mockSubQueue, error) {
	return &mockSubQueue{}, nil
}

func TestQueue_IsValidTime(t *testing.T) {
	tests := []struct {
		tickTime        time.Time
		testTime        time.Time
		name            string
		description     string
		segmentInterval storage.IntervalRule
		ttl             storage.IntervalRule
		expectedValid   bool
	}{
		{
			name: "valid time within range - hour interval",
			segmentInterval: storage.IntervalRule{
				Unit: storage.HOUR,
				Num:  1,
			},
			ttl: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  2, // 2 days TTL
			},
			tickTime:      time.Date(2023, 12, 15, 14, 30, 0, 0, time.UTC),
			testTime:      time.Date(2023, 12, 15, 10, 0, 0, 0, time.UTC), // 4.5 hours before tick
			expectedValid: true,
			description:   "time within valid range should be accepted",
		},
		{
			name: "expired time - before TTL window",
			segmentInterval: storage.IntervalRule{
				Unit: storage.HOUR,
				Num:  1,
			},
			ttl: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  2, // 2 days TTL
			},
			tickTime:      time.Date(2023, 12, 15, 14, 30, 0, 0, time.UTC),
			testTime:      time.Date(2023, 12, 13, 10, 0, 0, 0, time.UTC), // 2+ days old
			expectedValid: false,
			description:   "time older than TTL should be rejected",
		},
		{
			name: "future time - after latest segment",
			segmentInterval: storage.IntervalRule{
				Unit: storage.HOUR,
				Num:  1,
			},
			ttl: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  2,
			},
			tickTime:      time.Date(2023, 12, 15, 14, 30, 0, 0, time.UTC),
			testTime:      time.Date(2023, 12, 15, 16, 0, 0, 0, time.UTC), // After current segment end (15:00)
			expectedValid: true,
			description:   "time after latest segment end should trigger time range update and be accepted",
		},
		{
			name: "valid time at oldest segment start - day interval",
			segmentInterval: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  1,
			},
			ttl: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  7, // 7 days TTL
			},
			tickTime:      time.Date(2023, 12, 15, 14, 30, 0, 0, time.UTC),
			testTime:      time.Date(2023, 12, 8, 0, 0, 0, 0, time.UTC), // Exactly 7 days old (start of oldest segment)
			expectedValid: true,
			description:   "time at oldest segment start boundary should be accepted (IncludeStart=true)",
		},
		{
			name: "valid time at current segment start",
			segmentInterval: storage.IntervalRule{
				Unit: storage.HOUR,
				Num:  1,
			},
			ttl: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  2,
			},
			tickTime:      time.Date(2023, 12, 15, 14, 30, 0, 0, time.UTC),
			testTime:      time.Date(2023, 12, 15, 14, 0, 0, 0, time.UTC), // Current segment start
			expectedValid: true,
			description:   "time at current segment start should be accepted",
		},
		{
			name: "valid time at latest segment end boundary",
			segmentInterval: storage.IntervalRule{
				Unit: storage.HOUR,
				Num:  1,
			},
			ttl: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  2,
			},
			tickTime:      time.Date(2023, 12, 15, 14, 30, 0, 0, time.UTC),
			testTime:      time.Date(2023, 12, 15, 15, 0, 0, 0, time.UTC), // Exactly at segment end
			expectedValid: true,
			description:   "time at latest segment end boundary should trigger update and be accepted",
		},
		{
			name: "valid time with multi-day segment",
			segmentInterval: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  3, // 3-day segments
			},
			ttl: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  10,
			},
			tickTime:      time.Date(2023, 12, 15, 14, 30, 0, 0, time.UTC),
			testTime:      time.Date(2023, 12, 10, 12, 0, 0, 0, time.UTC), // 5 days old, within TTL
			expectedValid: true,
			description:   "time within multi-day segment interval should be accepted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary directory for the test
			tempDir := t.TempDir()

			// Create queue options with TTL
			opts := Opts[*mockSubQueue, mockSubQueueOption]{
				Option:          mockSubQueueOption{},
				Metrics:         nil,
				SubQueueCreator: createMockSubQueue,
				Location:        tempDir,
				ShardNum:        1,
				SegmentInterval: tt.segmentInterval,
				TTL:             tt.ttl,
				GetNodes:        func(_ common.ShardID) []string { return []string{"node1"} },
			}

			// Create context with position
			ctx := context.Background()
			ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
				return common.Position{
					Database: "test-db",
					Stage:    "test-stage",
				}
			})
			ctx = context.WithValue(ctx, logger.ContextKey, logger.GetLogger("test"))

			// Create queue
			queue, err := Open[*mockSubQueue, mockSubQueueOption](ctx, opts, "test-group")
			require.NoError(t, err)
			defer queue.Close()

			// Initialize the time range by validating the tick time
			// This triggers the first update of validTimeRange
			_ = queue.IsValidTime(tt.tickTime)

			// Test IsValidTime
			isValid := queue.IsValidTime(tt.testTime)

			// Verify result
			assert.Equal(t, tt.expectedValid, isValid, tt.description)
		})
	}
}

func TestQueue_IsValidTime_BeforeFirstValidation(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()

	// Create queue options
	opts := Opts[*mockSubQueue, mockSubQueueOption]{
		Option:          mockSubQueueOption{},
		Metrics:         nil,
		SubQueueCreator: createMockSubQueue,
		Location:        tempDir,
		ShardNum:        1,
		SegmentInterval: storage.IntervalRule{
			Unit: storage.HOUR,
			Num:  1,
		},
		TTL: storage.IntervalRule{
			Unit: storage.DAY,
			Num:  2,
		},
		GetNodes: func(_ common.ShardID) []string { return []string{"node1"} },
	}

	// Create context with position
	ctx := context.Background()
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{
			Database: "test-db",
			Stage:    "test-stage",
		}
	})
	ctx = context.WithValue(ctx, logger.ContextKey, logger.GetLogger("test"))

	// Create queue
	queue, err := Open[*mockSubQueue, mockSubQueueOption](ctx, opts, "test-group")
	require.NoError(t, err)
	defer queue.Close()

	// Test various times before first validation - all should be valid
	testTimes := []time.Time{
		time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),      // Very old
		time.Date(2023, 12, 15, 14, 30, 0, 0, time.UTC),  // Recent
		time.Date(2030, 12, 31, 23, 59, 59, 0, time.UTC), // Future
	}

	for _, testTime := range testTimes {
		t.Run(testTime.Format(time.RFC3339), func(t *testing.T) {
			isValid := queue.IsValidTime(testTime)
			assert.True(t, isValid, "before first validation, all times should be valid")
		})
	}
}

func TestQueue_IsValidTime_DynamicRangeUpdate(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()

	// Create queue options
	opts := Opts[*mockSubQueue, mockSubQueueOption]{
		Option:          mockSubQueueOption{},
		Metrics:         nil,
		SubQueueCreator: createMockSubQueue,
		Location:        tempDir,
		ShardNum:        1,
		SegmentInterval: storage.IntervalRule{
			Unit: storage.HOUR,
			Num:  1,
		},
		TTL: storage.IntervalRule{
			Unit: storage.DAY,
			Num:  2,
		},
		GetNodes: func(_ common.ShardID) []string { return []string{"node1"} },
	}

	// Create context with position
	ctx := context.Background()
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{
			Database: "test-db",
			Stage:    "test-stage",
		}
	})
	ctx = context.WithValue(ctx, logger.ContextKey, logger.GetLogger("test"))

	// Create queue
	queue, err := Open[*mockSubQueue, mockSubQueueOption](ctx, opts, "test-group")
	require.NoError(t, err)
	defer queue.Close()

	// Initialize at 14:30
	initialTime := time.Date(2023, 12, 15, 14, 30, 0, 0, time.UTC)
	_ = queue.IsValidTime(initialTime)

	// Check the initial valid range
	vtr := queue.validTimeRange.Load()
	require.NotNil(t, vtr)
	initialEnd := vtr.End

	// Test a time within the initial range
	withinRangeTime := time.Date(2023, 12, 15, 14, 45, 0, 0, time.UTC)
	assert.True(t, queue.IsValidTime(withinRangeTime), "time within initial range should be valid")

	// Test a future time that exceeds the current range (16:30, after the 15:00 end)
	futureTime := time.Date(2023, 12, 15, 16, 30, 0, 0, time.UTC)
	assert.True(t, queue.IsValidTime(futureTime), "future time should trigger range update and be valid")

	// Verify the time range was updated
	vtr = queue.validTimeRange.Load()
	require.NotNil(t, vtr)
	assert.True(t, vtr.End.After(initialEnd), "time range end should be extended after future timestamp")

	// The new range should now include the future time
	assert.True(t, queue.IsValidTime(futureTime), "future time should still be valid after range update")

	// Test that old times (beyond TTL) are still rejected even after range update
	veryOldTime := time.Date(2023, 12, 13, 10, 0, 0, 0, time.UTC) // 2+ days old
	assert.False(t, queue.IsValidTime(veryOldTime), "very old time should be rejected even after range update")

	// Test another future time
	evenFurtherTime := time.Date(2023, 12, 15, 18, 0, 0, 0, time.UTC)
	assert.True(t, queue.IsValidTime(evenFurtherTime), "even further future time should be accepted")
}

func TestQueue_IsValidTime_SnappingInValidation(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()

	// Create queue options
	opts := Opts[*mockSubQueue, mockSubQueueOption]{
		Option:          mockSubQueueOption{},
		Metrics:         nil,
		SubQueueCreator: createMockSubQueue,
		Location:        tempDir,
		ShardNum:        1,
		SegmentInterval: storage.IntervalRule{
			Unit: storage.HOUR,
			Num:  1,
		},
		TTL: storage.IntervalRule{
			Unit: storage.DAY,
			Num:  2,
		},
		GetNodes: func(_ common.ShardID) []string { return []string{"node1"} },
	}

	// Create context with position
	ctx := context.Background()
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{
			Database: "test-db",
			Stage:    "test-stage",
		}
	})
	ctx = context.WithValue(ctx, logger.ContextKey, logger.GetLogger("test"))

	// Create queue
	queue, err := Open[*mockSubQueue, mockSubQueueOption](ctx, opts, "test-group")
	require.NoError(t, err)
	defer queue.Close()

	// Initialize at 14:50
	initialTime := time.Date(2023, 12, 15, 14, 50, 0, 0, time.UTC)
	_ = queue.IsValidTime(initialTime)

	// Get initial range: [Dec 13 14:50, Dec 15 15:00] (2-day TTL, 1-hour segment)
	vtr := queue.validTimeRange.Load()
	require.NotNil(t, vtr)
	initialEnd := vtr.End
	initialLatestTick := queue.latestTickTime.Load()
	t.Logf("Initial range: [%v, %v], latestTickTime: %v", vtr.Start, vtr.End, time.Unix(0, initialLatestTick))

	// Test 1: First future timestamp beyond range (triggers update)
	// Range end is 15:00, so futureTime1 = 15:05 will exceed it
	// latestTickTime = 14:50, futureTime1 = 15:05
	// Check: 15:05 - 10min = 14:55 >= 14:50? YES, will update
	futureTime1 := time.Date(2023, 12, 15, 15, 5, 0, 0, time.UTC)
	assert.True(t, queue.IsValidTime(futureTime1), "future time should be valid")

	// Verify the range WAS updated
	vtr = queue.validTimeRange.Load()
	newEnd1 := vtr.End
	newLatestTick1 := queue.latestTickTime.Load()
	assert.True(t, newEnd1.After(initialEnd), "range end should be extended for first future timestamp")
	assert.Equal(t, futureTime1.UnixNano(), newLatestTick1, "latestTickTime should be updated to futureTime1")
	t.Logf("After first update: range: [%v, %v], latestTickTime: %v", vtr.Start, vtr.End, time.Unix(0, newLatestTick1))

	// Test 2: Future timestamp within snap window (< 10 min from newLatestTick1)
	// newLatestTick1 = 15:05, so snap window is until 15:15
	// New range end is 16:00 (next hour segment)
	// Choose futureTime2 that's >= 16:00 but < 15:05 + 10min = 15:15
	// This is impossible since 16:00 > 15:15
	// The key insight: snapping prevents updates when ts is close to latestTickTime in time,
	// not related to the range end
	// So let's test with timestamps that exceed range but are close in time to latestTickTime

	// Actually, to demonstrate snapping, we need a timestamp that:
	// 1. ts >= vtr.End (exceeds range)
	// 2. ts < latestTickTime + 10min (within snap window)
	// This means: vtr.End <= ts < latestTickTime + 10min
	// After update, vtr.End = 16:00, latestTickTime = 15:05
	// So we need: 16:00 <= ts < 15:15, which is impossible!

	// The snapping is designed for rapid successive calls with increasing timestamps
	// Let's test a different scenario where range end is close to latestTickTime

	// Let me start over with a better scenario
	tempDir2 := t.TempDir()
	opts2 := opts
	opts2.Location = tempDir2
	queue2, err := Open[*mockSubQueue, mockSubQueueOption](ctx, opts2, "test-group-2")
	require.NoError(t, err)
	defer queue2.Close()

	// Initialize very close to segment boundary
	initialTime2 := time.Date(2023, 12, 15, 14, 55, 0, 0, time.UTC) // 5 min before next segment
	_ = queue2.IsValidTime(initialTime2)

	vtr2 := queue2.validTimeRange.Load()
	t.Logf("Queue2 initial range: [%v, %v]", vtr2.Start, vtr2.End)

	// First future timestamp just beyond range (15:00 is the end)
	futureTime2_1 := time.Date(2023, 12, 15, 15, 2, 0, 0, time.UTC) // 2 min past range end
	assert.True(t, queue2.IsValidTime(futureTime2_1), "future time should be valid")

	// Verify update: 15:02 - 10min = 14:52 >= 14:55? NO! Won't update
	latestTick2_1 := queue2.latestTickTime.Load()
	assert.Equal(t, initialTime2.UnixNano(), latestTick2_1, "latestTickTime should NOT change (within snap window)")

	// Second future timestamp beyond snap window
	futureTime2_2 := time.Date(2023, 12, 15, 15, 6, 0, 0, time.UTC) // 11 min after initial tick
	assert.True(t, queue2.IsValidTime(futureTime2_2), "future time should be valid")

	// Verify update: 15:06 - 10min = 14:56 >= 14:55? YES! Will update
	latestTick2_2 := queue2.latestTickTime.Load()
	assert.Equal(t, futureTime2_2.UnixNano(), latestTick2_2, "latestTickTime should be updated to futureTime2_2")
}
