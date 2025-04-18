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

package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type boundaryUpdateTracker struct {
	updates []*modelv1.TimeRange
	called  int
	mu      sync.Mutex
}

func (t *boundaryUpdateTracker) recordUpdate(_, _ string, boundary *modelv1.TimeRange) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.updates = append(t.updates, boundary)
	t.called++
}

func TestSegmentBoundaryUpdateFn(t *testing.T) {
	t.Run("called when a segment is created", func(t *testing.T) {
		tsdb, c, _, tracker, dfFn := setUpDB(t)
		defer dfFn()

		// Initial segment creation should have triggered it once
		require.Equal(t, 1, tracker.called)

		// Create new segment
		ts := c.Now().Add(23*time.Hour + time.Second)
		tsdb.Tick(ts.UnixNano())

		// Verify the function was called again
		assert.Eventually(t, func() bool {
			tracker.mu.Lock()
			defer tracker.mu.Unlock()
			return tracker.called >= 2
		}, flags.EventuallyTimeout, time.Millisecond, "boundary update function should be called")
	})

	t.Run("called when a segment is deleted", func(t *testing.T) {
		tsdb, c, segCtrl, tracker, dfFn := setUpDB(t)
		defer dfFn()

		ts := c.Now()
		for i := 0; i < 4; i++ {
			ts = ts.Add(23 * time.Hour)
			c.Set(ts)
			tsdb.Tick(ts.UnixNano())
			t.Logf("current time: %s", ts.Format(time.RFC3339))
			expected := i + 2
			require.EventuallyWithTf(t, func(ct *assert.CollectT) {
				segments, _ := segCtrl.segments(false)
				if len(segments) != expected {
					ct.Errorf("expect %d segments, got %d", expected, len(segments))
				}
			}, flags.EventuallyTimeout, time.Millisecond, "wait for %d segment to be created", expected)
			ts = ts.Add(time.Hour)
		}

		tracker.mu.Lock()
		called := tracker.called
		tracker.mu.Unlock()

		c.Set(ts)
		tsdb.Tick(ts.UnixNano())

		assert.Eventually(t, func() bool {
			tracker.mu.Lock()
			defer tracker.mu.Unlock()
			return tracker.called > called
		}, flags.EventuallyTimeout, time.Millisecond, "boundary update function should be called after deletion")
	})
}

func TestForwardRotation(t *testing.T) {
	t.Run("create a new segment when the time is up", func(t *testing.T) {
		tsdb, c, segCtrl, _, dfFn := setUpDB(t)
		defer dfFn()
		ts := c.Now().Add(23*time.Hour + time.Second)
		t.Logf("current time: %s", ts.Format(time.RFC3339))
		tsdb.Tick(ts.UnixNano())
		assert.Eventually(t, func() bool {
			segments, _ := segCtrl.segments(false)
			return len(segments) == 2
		}, flags.EventuallyTimeout, time.Millisecond, "wait for the second segment to be created")
	})

	t.Run("no new segment created when the time is not up", func(t *testing.T) {
		tsdb, c, segCtrl, _, dfFn := setUpDB(t)
		defer dfFn()
		ts := c.Now().Add(22*time.Hour + 59*time.Minute + 59*time.Second)
		t.Logf("current time: %s", ts.Format(time.RFC3339))
		tsdb.Tick(ts.UnixNano())
		assert.Never(t, func() bool {
			segments, _ := segCtrl.segments(false)
			return len(segments) == 2
		}, flags.NeverTimeout, time.Millisecond, "wait for the second segment never to be created")
	})
}

func TestRetention(t *testing.T) {
	t.Run("delete the segment and index when the TTL is up", func(t *testing.T) {
		tsdb, c, segCtrl, _, dfFn := setUpDB(t)
		defer dfFn()
		ts := c.Now()
		for i := 0; i < 4; i++ {
			ts = ts.Add(23 * time.Hour)

			t.Logf("current time: %s", ts.Format(time.RFC3339))
			c.Set(ts)
			tsdb.Tick(ts.UnixNano())
			expected := i + 2
			require.EventuallyWithTf(t, func(ct *assert.CollectT) {
				segments, _ := segCtrl.segments(false)
				if len(segments) != expected {
					ct.Errorf("expect %d segments, got %d", expected, len(segments))
				}
			}, flags.EventuallyTimeout, time.Millisecond, "wait for %d segment to be created", expected)
			// amend the time to the next day
			ts = ts.Add(time.Hour)
		}
		t.Logf("current time: %s", ts.Format(time.RFC3339))
		c.Set(ts)
		tsdb.Tick(ts.UnixNano())
		assert.Eventually(t, func() bool {
			segments, _ := segCtrl.segments(false)
			return len(segments) == 4
		}, flags.EventuallyTimeout, time.Millisecond, "wait for the 1st segment to be deleted")
	})

	t.Run("keep the segment volume stable", func(t *testing.T) {
		tsdb, c, segCtrl, _, dfFn := setUpDB(t)
		defer dfFn()
		ts := c.Now()
		for i := 0; i < 10; i++ {
			ts = ts.Add(23 * time.Hour)
			c.Set(ts)
			tsdb.Tick(ts.UnixNano())
			ts = ts.Add(time.Hour)
			require.EventuallyWithTf(t, func(ct *assert.CollectT) {
				ss, _ := segCtrl.segments(false)
				defer func() {
					for i := range ss {
						ss[i].DecRef()
					}
				}()
				latest := ss[len(ss)-1]
				if !latest.Contains(ts.UnixNano()) {
					ct.Errorf("expect the last segment %s to contain the time %s", latest, ts.Format(time.RFC3339))
					return
				}
				if tsdb.rotationProcessOn.Load() {
					ct.Errorf("expect the rotation process to be off")
				}
			}, flags.EventuallyTimeout, time.Millisecond, "wait for segment to be created")
			// amend the time to the next day
			c.Set(ts)
			tsdb.Tick(ts.UnixNano())
			require.EventuallyWithTf(t, func(ct *assert.CollectT) {
				ss, _ := segCtrl.segments(false)
				defer func() {
					for i := range ss {
						ss[i].DecRef()
					}
				}()
				if len(ss) > 4 {
					ct.Errorf("expect the segment number never to exceed 4, got %d", len(ss))
					return
				}
				if tsdb.rotationProcessOn.Load() {
					ct.Errorf("expect the rotation process to be off")
				}
			}, flags.EventuallyTimeout, time.Millisecond, "wait for the segment number never to exceed 4")
		}
	})
}

func setUpDB(t *testing.T) (*database[*MockTSTable, any], timestamp.MockClock, *segmentController[*MockTSTable, any], *boundaryUpdateTracker, func()) {
	dir, defFn := test.Space(require.New(t))

	// Create tracker for boundary updates
	tracker := &boundaryUpdateTracker{
		updates: make([]*modelv1.TimeRange, 0),
	}

	TSDBOpts := TSDBOpts[*MockTSTable, any]{
		Location:        dir,
		SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
		TTL:             IntervalRule{Unit: DAY, Num: 3},
		ShardNum:        1,
		TSTableCreator:  MockTSTableCreator,
		// Add the boundary update function
		SegmentBoundaryUpdateFn: tracker.recordUpdate,
	}
	ctx := context.Background()
	mc := timestamp.NewMockClock()
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 00:00:00", time.Local)
	require.NoError(t, err)
	mc.Set(ts)
	ctx = timestamp.SetClock(ctx, mc)

	tsdb, err := OpenTSDB(ctx, TSDBOpts)
	require.NoError(t, err)
	seg, err := tsdb.CreateSegmentIfNotExist(ts)
	require.NoError(t, err)
	defer seg.DecRef()

	db := tsdb.(*database[*MockTSTable, any])
	segments, _ := db.segmentController.segments(false)
	require.Equal(t, len(segments), 1)
	return db, mc, db.segmentController, tracker, func() {
		tsdb.Close()
		defFn()
	}
}

type MockTSTable struct{}

func (m *MockTSTable) Close() error {
	return nil
}

func (m *MockTSTable) Collect(_ Metrics) {}

func (m *MockTSTable) TakeFileSnapshot(_ string) error {
	return nil
}

var MockTSTableCreator = func(_ fs.FileSystem, _ string, _ common.Position,
	_ *logger.Logger, _ timestamp.TimeRange, _, _ any,
) (*MockTSTable, error) {
	return &MockTSTable{}, nil
}

type MockMetrics struct{}

func (m *MockMetrics) DeleteAll() {}

func (m *MockMetrics) Factory() *observability.Factory {
	return nil
}

var MockMetricsCreator = func(_ common.Position) Metrics { return &MockMetrics{} }
