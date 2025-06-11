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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const group = "test"

// MockGaugeWithValue wraps MockGauge to track the value.
type MockGaugeWithValue struct {
	*meter.MockGauge
	value float64
}

// GetValue returns the current value stored in the mock gauge.
func (m *MockGaugeWithValue) GetValue() float64 {
	return m.value
}

// NewMockGauge creates a MockGaugeWithValue that tracks Set values.
func NewMockGauge(ctrl *gomock.Controller) *MockGaugeWithValue {
	mockGauge := meter.NewMockGauge(ctrl)
	wrapper := &MockGaugeWithValue{MockGauge: mockGauge}

	mockGauge.EXPECT().Set(gomock.Any(), gomock.Any()).AnyTimes().
		Do(func(value float64, _ ...string) {
			wrapper.value = value
		})

	return wrapper
}

// MockCounterWithValue wraps MockCounter to track the value.
type MockCounterWithValue struct {
	*meter.MockCounter
	value float64
}

// GetValue returns the current value stored in the mock counter.
func (m *MockCounterWithValue) GetValue() float64 {
	return m.value
}

// NewMockCounter creates a MockCounterWithValue that tracks Inc values.
func NewMockCounter(ctrl *gomock.Controller) *MockCounterWithValue {
	mockCounter := meter.NewMockCounter(ctrl)
	wrapper := &MockCounterWithValue{MockCounter: mockCounter}

	mockCounter.EXPECT().Inc(gomock.Any(), gomock.Any()).AnyTimes().
		Do(func(delta float64, _ ...string) {
			wrapper.value += delta
		})

	return wrapper
}

func TestOpenTSDB(t *testing.T) {
	logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})

	t.Run("create new TSDB", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		opts := TSDBOpts[*MockTSTable, any]{
			Location:        dir,
			SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
			TTL:             IntervalRule{Unit: DAY, Num: 3},
			ShardNum:        1,
			TSTableCreator:  MockTSTableCreator,
		}

		ctx := context.Background()
		mc := timestamp.NewMockClock()
		ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 00:00:00", time.Local)
		require.NoError(t, err)
		mc.Set(ts)
		ctx = timestamp.SetClock(ctx, mc)

		serviceCache := NewServiceCache()
		tsdb, err := OpenTSDB(ctx, opts, serviceCache, group)
		require.NoError(t, err)
		require.NotNil(t, tsdb)

		seg, err := tsdb.CreateSegmentIfNotExist(ts)
		require.NoError(t, err)
		defer seg.DecRef()

		db := tsdb.(*database[*MockTSTable, any])
		ss, _ := db.segmentController.segments(false)
		require.Equal(t, len(ss), 1)
		tsdb.Close()
	})

	t.Run("reopen existing TSDB", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		opts := TSDBOpts[*MockTSTable, any]{
			Location:        dir,
			SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
			TTL:             IntervalRule{Unit: DAY, Num: 3},
			ShardNum:        1,
			TSTableCreator:  MockTSTableCreator,
		}

		ctx := context.Background()
		mc := timestamp.NewMockClock()
		ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 00:00:00", time.Local)
		require.NoError(t, err)
		mc.Set(ts)
		ctx = timestamp.SetClock(ctx, mc)

		// Create new TSDB
		serviceCache := NewServiceCache()
		tsdb, err := OpenTSDB(ctx, opts, serviceCache, group)
		require.NoError(t, err)
		require.NotNil(t, tsdb)

		seg, err := tsdb.CreateSegmentIfNotExist(ts)
		require.NoError(t, err)
		seg.DecRef()

		db := tsdb.(*database[*MockTSTable, any])
		segs, _ := db.segmentController.segments(false)
		require.Equal(t, len(segs), 1)
		for i := range segs {
			segs[i].DecRef()
		}
		tsdb.Close()

		// Reopen existing TSDB
		tsdb, err = OpenTSDB(ctx, opts, serviceCache, group)
		require.NoError(t, err)
		require.NotNil(t, tsdb)

		db = tsdb.(*database[*MockTSTable, any])
		segs, _ = db.segmentController.segments(false)
		require.Equal(t, len(segs), 1)
		for i := range segs {
			segs[i].DecRef()
		}
		tsdb.Close()
	})

	t.Run("Changed options", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		opts := TSDBOpts[*MockTSTable, any]{
			Location:        dir,
			SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
			TTL:             IntervalRule{Unit: DAY, Num: 3},
			ShardNum:        1,
			TSTableCreator:  MockTSTableCreator,
		}

		ctx := context.Background()
		mc := timestamp.NewMockClock()
		ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 00:00:00", time.Local)
		require.NoError(t, err)
		mc.Set(ts)
		ctx = timestamp.SetClock(ctx, mc)

		// Create new TSDB
		serviceCache := NewServiceCache()
		tsdb, err := OpenTSDB(ctx, opts, serviceCache, group)
		require.NoError(t, err)
		require.NotNil(t, tsdb)

		seg, err := tsdb.CreateSegmentIfNotExist(ts)
		require.NoError(t, err)
		seg.DecRef()

		tsdb.UpdateOptions(&commonv1.ResourceOpts{
			ShardNum: 2,
			SegmentInterval: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  2,
			},
			Ttl: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  6,
			},
		})

		tsdb.Close()
	})
}

func TestTakeFileSnapshot(t *testing.T) {
	logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})

	t.Run("Take snapshot of existing TSDB", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		snapshotDir := filepath.Join(dir, "snapshot")

		opts := TSDBOpts[*MockTSTable, any]{
			Location:        dir,
			SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
			TTL:             IntervalRule{Unit: DAY, Num: 3},
			ShardNum:        1,
			TSTableCreator:  MockTSTableCreator,
		}

		ctx := context.Background()
		mc := timestamp.NewMockClock()

		ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 00:00:00", time.Local)
		require.NoError(t, err)
		mc.Set(ts)
		ctx = timestamp.SetClock(ctx, mc)

		serviceCache := NewServiceCache()
		tsdb, err := OpenTSDB(ctx, opts, serviceCache, group)
		require.NoError(t, err)
		require.NotNil(t, tsdb)

		seg, err := tsdb.CreateSegmentIfNotExist(ts)
		require.NoError(t, err)
		require.NotNil(t, seg)
		segLocation := seg.(*segment[*MockTSTable, any]).location // to verify snapshot files/dirs later
		seg.DecRef()

		err = tsdb.TakeFileSnapshot(snapshotDir)
		require.NoError(t, err, "taking file snapshot should not produce an error")

		segDir := filepath.Join(snapshotDir, filepath.Base(segLocation))
		require.DirExists(t, segDir, "snapshot of the segment directory should exist")

		indexDir := filepath.Join(segDir, seriesIndexDirName) // "seriesIndexDirName" comes from the TSDB code.
		require.DirExists(t, indexDir,
			"index directory must be present in the snapshot")

		require.NoError(t, tsdb.Close())
	})
}

func TestTSDBCollect(t *testing.T) {
	logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})

	dir, defFn := test.Space(require.New(t))
	defer defFn()

	// Create gomock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock gauges
	lastTickTimeGauge := NewMockGauge(ctrl)
	totalSegRefsGauge := NewMockGauge(ctrl)

	// Create a metrics struct with our mock gauges
	mockMetrics := &metrics{
		lastTickTime:                 lastTickTimeGauge.MockGauge,
		totalSegRefs:                 totalSegRefsGauge.MockGauge,
		totalRotationStarted:         NewMockCounter(ctrl).MockCounter,
		totalRotationFinished:        NewMockCounter(ctrl).MockCounter,
		totalRotationErr:             NewMockCounter(ctrl).MockCounter,
		totalRetentionStarted:        NewMockCounter(ctrl).MockCounter,
		totalRetentionFinished:       NewMockCounter(ctrl).MockCounter,
		totalRetentionHasData:        NewMockCounter(ctrl).MockCounter,
		totalRetentionErr:            NewMockCounter(ctrl).MockCounter,
		totalRetentionHasDataLatency: NewMockCounter(ctrl).MockCounter,
		schedulerMetrics:             nil, // Not needed for this test
	}

	opts := TSDBOpts[*MockTSTable, any]{
		Location:        dir,
		SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
		TTL:             IntervalRule{Unit: DAY, Num: 3},
		ShardNum:        1,
		TSTableCreator:  MockTSTableCreator,
	}

	ctx := context.Background()
	mc := timestamp.NewMockClock()
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 00:00:00", time.Local)
	require.NoError(t, err)
	mc.Set(ts)
	ctx = timestamp.SetClock(ctx, mc)

	serviceCache := NewServiceCache()
	tsdb, err := OpenTSDB(ctx, opts, serviceCache, group)
	require.NoError(t, err)
	require.NotNil(t, tsdb)

	// Create a segment to have some data for metrics to track
	seg, err := tsdb.CreateSegmentIfNotExist(ts)
	require.NoError(t, err)
	require.NotNil(t, seg)
	defer seg.DecRef()

	// Set the metrics in the database
	db := tsdb.(*database[*MockTSTable, any])
	db.metrics = mockMetrics

	// Set a specific tick time for testing
	tickTime := int64(123456789)
	db.latestTickTime.Store(tickTime)

	// Call the collect method
	db.collect()

	// Verify the metrics were set correctly
	require.Equal(t, float64(tickTime), lastTickTimeGauge.GetValue())

	// There should be at least one segment reference
	require.Greater(t, totalSegRefsGauge.GetValue(), float64(0),
		"totalSegRefs should be greater than 0 with an active segment")

	// Clean up
	require.NoError(t, tsdb.Close())
}

func TestCollectWithPartialClosedSegments(t *testing.T) {
	logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})

	dir, defFn := test.Space(require.New(t))
	defer defFn()

	// Create gomock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock metrics
	lastTickTimeGauge := NewMockGauge(ctrl)
	totalSegRefsGauge := NewMockGauge(ctrl)

	mockMetrics := &metrics{
		lastTickTime:                 lastTickTimeGauge.MockGauge,
		totalSegRefs:                 totalSegRefsGauge.MockGauge,
		totalRotationStarted:         NewMockCounter(ctrl).MockCounter,
		totalRotationFinished:        NewMockCounter(ctrl).MockCounter,
		totalRotationErr:             NewMockCounter(ctrl).MockCounter,
		totalRetentionStarted:        NewMockCounter(ctrl).MockCounter,
		totalRetentionFinished:       NewMockCounter(ctrl).MockCounter,
		totalRetentionHasData:        NewMockCounter(ctrl).MockCounter,
		totalRetentionErr:            NewMockCounter(ctrl).MockCounter,
		totalRetentionHasDataLatency: NewMockCounter(ctrl).MockCounter,
		schedulerMetrics:             nil,
	}

	opts := TSDBOpts[*MockTSTable, any]{
		Location:           dir,
		SegmentInterval:    IntervalRule{Unit: DAY, Num: 1},
		TTL:                IntervalRule{Unit: DAY, Num: 7},
		ShardNum:           2,
		TSTableCreator:     MockTSTableCreator,
		SegmentIdleTimeout: 100 * time.Millisecond, // Short idle timeout for testing
	}

	ctx := context.Background()
	mc := timestamp.NewMockClock()
	baseDate, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 00:00:00", time.Local)
	require.NoError(t, err)
	mc.Set(baseDate)
	ctx = timestamp.SetClock(ctx, mc)

	serviceCache := NewServiceCache()
	tsdb, err := OpenTSDB(ctx, opts, serviceCache, group)
	require.NoError(t, err)
	require.NotNil(t, tsdb)

	db := tsdb.(*database[*MockTSTable, any])
	db.metrics = mockMetrics

	// Create multiple segments for different days
	segmentDates := []time.Time{
		baseDate.AddDate(0, 0, -3), // 3 days ago
		baseDate.AddDate(0, 0, -2), // 2 days ago
		baseDate.AddDate(0, 0, -1), // yesterday
		baseDate,                   // today
	}

	var segments []Segment[*MockTSTable, any]

	// Create segments and keep track of them
	for _, date := range segmentDates {
		mc.Set(date)
		seg, err := tsdb.CreateSegmentIfNotExist(date)
		require.NoError(t, err)
		require.NotNil(t, seg)
		segments = append(segments, seg)
		seg.DecRef() // Release reference
	}

	// Set a specific tick time for testing
	tickTime := int64(123456789)
	db.latestTickTime.Store(tickTime)

	// Manually close some segments (first and third)
	segments[0].DecRef() // Close first segment
	segments[2].DecRef() // Close third segment

	// Manually set the closed segments to have idle time in the past
	sc := db.segmentController
	ss, _ := sc.segments(false)
	for _, s := range ss {
		// Find segments we want to mark as idle (first and third)
		if s.Start.Equal(segmentDates[0]) || s.Start.Equal(segmentDates[2]) {
			s.lastAccessed.Store(time.Now().Add(-time.Hour).UnixNano())
			s.DecRef() // Force close
		}
		s.DecRef() // Release our reference from segments()
	}

	// Call closeIdleSegments to ensure our target segments are closed
	closedCount := sc.closeIdleSegments()
	require.Equal(t, 2, closedCount, "Should have closed 2 segments")

	// Verify segments 0 and 2 are closed, 1 and 3 are open
	ss, _ = sc.segments(false)
	for _, s := range ss {
		if s.Start.Equal(segmentDates[0]) || s.Start.Equal(segmentDates[2]) {
			require.Equal(t, int32(0), s.refCount, "Segment should be closed")
		} else {
			require.Greater(t, s.refCount, int32(0), "Segment should be open")
		}
		s.DecRef() // Release reference
	}

	// Call the collect method with mixed open/closed segments
	db.collect()

	// Verify the metrics were set correctly
	require.Equal(t, float64(tickTime), lastTickTimeGauge.GetValue())

	// Should only count references for open segments (segments 1 and 3)
	// Each segment has at least one reference
	require.GreaterOrEqual(t, totalSegRefsGauge.GetValue(), float64(2),
		"totalSegRefs should count at least 2 references (one per open segment)")

	// Test that closed segments are not counted in the metrics
	require.Less(t, totalSegRefsGauge.GetValue(), float64(4),
		"totalSegRefs should not count all 4 segments since 2 are closed")

	// Clean up remaining open segments
	segments[1].DecRef()
	segments[3].DecRef()

	// Clean up
	require.NoError(t, tsdb.Close())
}
