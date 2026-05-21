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
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/initerror"
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
		ss, _ := db.segmentController.segments(context.Background(), false)
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
		segs, _ := db.segmentController.segments(context.Background(), false)
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
		segs, _ = db.segmentController.segments(context.Background(), false)
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

		created, snapshotErr := tsdb.TakeFileSnapshot(snapshotDir)
		require.NoError(t, snapshotErr, "taking file snapshot should not produce an error")
		require.True(t, created, "snapshot should have been created")

		segDir := filepath.Join(snapshotDir, filepath.Base(segLocation))
		require.DirExists(t, segDir, "snapshot of the segment directory should exist")

		indexDir := filepath.Join(segDir, seriesIndexDirName) // "seriesIndexDirName" comes from the TSDB code.
		require.DirExists(t, indexDir,
			"index directory must be present in the snapshot")

		require.NoError(t, tsdb.Close())
	})

	t.Run("Take snapshot without segments", func(t *testing.T) {
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

		created, snapshotErr := tsdb.TakeFileSnapshot(snapshotDir)
		require.NoError(t, snapshotErr, "taking file snapshot should not produce an error")
		require.False(t, created, "snapshot should not have been created when no segments exist")

		require.NoDirExists(t, snapshotDir, "snapshot directory should not exist when no segments")

		require.NoError(t, tsdb.Close())
	})

	t.Run("Epoch timestamp segment creation is rejected", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		opts := TSDBOpts[*SnapshotMockTSTable, any]{
			Location:        dir,
			SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
			TTL:             IntervalRule{Unit: DAY, Num: 7},
			ShardNum:        1,
			TSTableCreator:  SnapshotMockTSTableCreator,
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
		defer tsdb.Close()

		normalSeg, segErr := tsdb.CreateSegmentIfNotExist(ts)
		require.NoError(t, segErr)
		normalSeg.DecRef()

		_, epochErr := tsdb.CreateSegmentIfNotExist(time.Unix(0, 0))
		require.ErrorIs(t, epochErr, ErrInvalidSegmentTimestamp,
			"creating a segment with epoch timestamp should be rejected")
	})
}

func TestEpochSegmentCleanupOnOpen(t *testing.T) {
	logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})

	dir, defFn := test.Space(require.New(t))
	defer defFn()

	opts := TSDBOpts[*MockTSTable, any]{
		Location:        dir,
		SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
		TTL:             IntervalRule{Unit: DAY, Num: 7},
		ShardNum:        1,
		TSTableCreator:  MockTSTableCreator,
	}

	ctx := context.Background()
	mc := timestamp.NewMockClock()
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 00:00:00", time.Local)
	require.NoError(t, err)
	mc.Set(ts)
	ctx = timestamp.SetClock(ctx, mc)

	// Create a TSDB with a valid segment
	sc := NewServiceCache()
	tsdb1, err := OpenTSDB(ctx, opts, sc, group)
	require.NoError(t, err)

	seg, segErr := tsdb1.CreateSegmentIfNotExist(ts)
	require.NoError(t, segErr)
	seg.DecRef()
	require.NoError(t, tsdb1.Close())

	// Manually create a fake epoch segment directory with metadata to simulate the bug
	epochSegDir := filepath.Join(dir, "seg-19700101")
	lfs := fs.NewLocalFileSystem()
	lfs.MkdirIfNotExist(epochSegDir, DirPerm)
	metaPath := filepath.Join(epochSegDir, metadataFilename)
	lf, lockErr := lfs.CreateLockFile(metaPath, FilePerm)
	require.NoError(t, lockErr)
	_, writeErr := lf.Write([]byte(`{"version":"v1","end_time":"1970-01-02T00:00:00Z"}`))
	require.NoError(t, writeErr)
	require.NoError(t, lf.Close())

	require.DirExists(t, epochSegDir, "epoch segment should exist before reopening")

	// Reopen TSDB - it should clean up the epoch segment
	tsdb2, err := OpenTSDB(ctx, opts, sc, group)
	require.NoError(t, err)
	defer tsdb2.Close()

	require.NoDirExists(t, epochSegDir, "epoch segment should be removed on open")
}

// TestHalfBornSegmentCleanupOnOpen reproduces the production failure where a
// segment directory exists on disk without a metadata file (e.g. crash between
// MkdirPanicIfExist and metadata write, or partial sync receive). Prior to the
// errors.Is fix in pkg/fs, localFileSystem.Read returned a *FileSystemError
// that did not satisfy errors.Is(err, io/fs.ErrNotExist), so the segment
// controller's cleanup branch was never taken and OpenTSDB failed, leaving the
// group with db=nil forever.
func TestHalfBornSegmentCleanupOnOpen(t *testing.T) {
	logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})
	dir, defFn := test.Space(require.New(t))
	defer defFn()

	opts := TSDBOpts[*MockTSTable, any]{
		Location:        dir,
		SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
		TTL:             IntervalRule{Unit: DAY, Num: 7},
		ShardNum:        1,
		TSTableCreator:  MockTSTableCreator,
	}

	ctx := context.Background()
	mc := timestamp.NewMockClock()
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 00:00:00", time.Local)
	require.NoError(t, err)
	mc.Set(ts)
	ctx = timestamp.SetClock(ctx, mc)

	sc := NewServiceCache()
	tsdb1, err := OpenTSDB(ctx, opts, sc, group)
	require.NoError(t, err)
	seg, segErr := tsdb1.CreateSegmentIfNotExist(ts)
	require.NoError(t, segErr)
	seg.DecRef()
	require.NoError(t, tsdb1.Close())

	halfBornDir := filepath.Join(dir, "seg-20240502")
	lfs := fs.NewLocalFileSystem()
	lfs.MkdirIfNotExist(halfBornDir, DirPerm)
	require.NoFileExists(t, filepath.Join(halfBornDir, metadataFilename),
		"half-born segment must have no metadata file to reproduce the bug")

	tsdb2, err := OpenTSDB(ctx, opts, sc, group)
	require.NoError(t, err, "OpenTSDB must not fail on a half-born segment")
	require.NotNil(t, tsdb2)
	defer tsdb2.Close()

	require.NoDirExists(t, halfBornDir, "half-born segment should be removed on open")

	db := tsdb2.(*database[*MockTSTable, any])
	segs, _ := db.segmentController.segments(context.Background(), false)
	require.Equal(t, 1, len(segs), "only the valid segment should remain after cleanup")
	for i := range segs {
		segs[i].DecRef()
	}
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
		SegmentIdleTimeout: time.Hour, // Long enough that only segments with manually backdated lastAccessed are idle
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

	// Create segments and keep track of them.
	// openSegment.initialize sets refCount=1, then createSegment.incRef
	// bumps it to 2. The DecRef below releases the createSegment ref,
	// leaving refCount=1 (the initialize ref) so the idle reclaimer can
	// CAS-bump and see refAtBump==1.
	for _, date := range segmentDates {
		mc.Set(date)
		seg, segErr := tsdb.CreateSegmentIfNotExist(date)
		require.NoError(t, segErr)
		require.NotNil(t, seg)
		segments = append(segments, seg)
		seg.DecRef() // Release createSegment's ref; refCount 2->1
	}

	// Set a specific tick time for testing
	tickTime := int64(123456789)
	db.latestTickTime.Store(tickTime)

	// Mark the first and third segments as idle by backdating their
	// lastAccessed timestamp. Do NOT DecRef them — they must stay alive
	// (refCount=1) so closeIdleSegments can CAS-bump (refAtBump==1) and
	// close them.
	sc := db.segmentController
	ss, _ := sc.segments(context.Background(), false)
	for _, s := range ss {
		if s.Start.Equal(segmentDates[0]) || s.Start.Equal(segmentDates[2]) {
			s.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
		}
		s.DecRef() // Release our reference from segments()
	}

	// closeIdleSegments should close the 2 idle segments (refAtBump==1 and
	// lastAccessed older than idleThreshold) and leave the other 2 open.
	closedCount := sc.closeIdleSegments()
	require.Equal(t, 2, closedCount, "Should have closed 2 segments")

	// Verify segments 0 and 2 are closed, 1 and 3 are open
	ss, _ = sc.segments(context.Background(), false)
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

// newTestSegmentSkeleton seeds a minimal on-disk segment layout under dir with
// the supplied storage version stamp. The layout matches readSegmentMeta's
// "old format" (version-only file body), which is sufficient to exercise
// checkVersion at TSDB open time.
func newTestSegmentSkeleton(t *testing.T, dir, version string) {
	t.Helper()
	segDir := filepath.Join(dir, "seg-20240501")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	metaPath := filepath.Join(segDir, metadataFilename)
	require.NoError(t, os.WriteFile(metaPath, []byte(version), 0o600))
}

// TestTSDBOpen_LockReleasedAfterFailedOpen reproduces the lock-file leak that
// the F5 v2 lock-fix targets. Prior to the fix, OpenTSDB acquired the lock
// file but never released it on the segmentController.open() error path, so
// a second OpenTSDB call against the same directory failed with
// "resource temporarily unavailable" from CreateLockFile. The fix adds a
// defer-release with a `released` boolean flipped to true only on the
// success path. This test seeds an incompatible segment to force the first
// OpenTSDB to fail, then removes the segment and asserts the second
// OpenTSDB succeeds — which is only possible if the lock fd was released.
func TestTSDBOpen_LockReleasedAfterFailedOpen(t *testing.T) {
	logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})

	dir, defFn := test.Space(require.New(t))
	defer defFn()

	newTestSegmentSkeleton(t, dir, "1.3.0")

	opts := TSDBOpts[*MockTSTable, any]{
		Location:        dir,
		SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
		TTL:             IntervalRule{Unit: DAY, Num: 3},
		ShardNum:        1,
		TSTableCreator:  MockTSTableCreator,
	}

	ctx := context.Background()
	mc := timestamp.NewMockClock()
	ts, parseErr := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 00:00:00", time.Local)
	require.NoError(t, parseErr)
	mc.Set(ts)
	ctx = timestamp.SetClock(ctx, mc)

	// First open fails on the incompatible segment. With the leak fix the
	// defer in OpenTSDB releases the lock fd before returning the error.
	serviceCache := NewServiceCache()
	_, openErr := OpenTSDB(ctx, opts, serviceCache, group)
	require.Error(t, openErr, "first OpenTSDB must fail on the incompatible segment")
	require.True(t, initerror.IsPermanent(openErr), "first OpenTSDB error must be permanent")

	// Remove the bad segment so the second open's segmentController scan succeeds.
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "seg-20240501")))

	// Second open against the same dir must succeed — only possible if the
	// lock fd from the first attempt was released. Without the fix this
	// returns "resource temporarily unavailable" from CreateLockFile.
	tsdb, reopenErr := OpenTSDB(ctx, opts, serviceCache, group)
	require.NoError(t, reopenErr, "second OpenTSDB must succeed after the failed first attempt")
	require.NotNil(t, tsdb)
	require.NoError(t, tsdb.Close())
}

// TestTSDBOpen_RejectsIncompatibleSegment verifies that opening a TSDB whose
// on-disk segment is stamped with an incompatible version surfaces a permanent
// initialization error so the caller fails fast instead of silently dropping
// the affected group.
func TestTSDBOpen_RejectsIncompatibleSegment(t *testing.T) {
	logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})

	dir, defFn := test.Space(require.New(t))
	defer defFn()

	newTestSegmentSkeleton(t, dir, "1.3.0")

	opts := TSDBOpts[*MockTSTable, any]{
		Location:        dir,
		SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
		TTL:             IntervalRule{Unit: DAY, Num: 3},
		ShardNum:        1,
		TSTableCreator:  MockTSTableCreator,
	}

	ctx := context.Background()
	mc := timestamp.NewMockClock()
	ts, parseErr := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 00:00:00", time.Local)
	require.NoError(t, parseErr)
	mc.Set(ts)
	ctx = timestamp.SetClock(ctx, mc)

	serviceCache := NewServiceCache()
	_, openErr := OpenTSDB(ctx, opts, serviceCache, group)
	require.Error(t, openErr, "OpenTSDB must reject an incompatible-version segment")
	require.True(t, initerror.IsPermanent(openErr), "incompatible-version error must surface as permanent")
	require.True(t, errors.Is(openErr, errVersionIncompatible), "error must still match the version-incompatible sentinel")
}
