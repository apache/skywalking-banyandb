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
