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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// mockTSTable is a minimal implementation of TSTable for testing.
type mockTSTable struct {
	ID common.ShardID
}

func (m mockTSTable) Close() error {
	return nil
}

func (m mockTSTable) Collect(Metrics) {}

func (m mockTSTable) TakeFileSnapshot(string) (bool, error) { return true, nil }

// mockTSTableOpener implements the necessary functions to open a TSTable.
type mockTSTableOpener struct{}

func setupTestEnvironment(t *testing.T) (string, func()) {
	t.Helper()
	tempDir := t.TempDir()

	// Create test logger
	err := logger.Init(logger.Logging{
		Env:   "test",
		Level: "info",
	})
	require.NoError(t, err)

	return tempDir, func() {
		// Cleanup function
		os.RemoveAll(tempDir)
	}
}

func TestSegmentOpenAndReopen(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	l := logger.GetLogger("test-segment")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{
			Database: "test-db",
			Stage:    "test-stage",
		}
	})

	opts := TSDBOpts[mockTSTable, mockTSTableOpener]{
		TSTableCreator: func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
			_ timestamp.TimeRange, _ mockTSTableOpener, _ any,
		) (mockTSTable, error) {
			return mockTSTable{ID: common.ShardID(0)}, nil
		},
		ShardNum: 2,
		SegmentInterval: IntervalRule{
			Unit: DAY,
			Num:  1,
		},
		TTL: IntervalRule{
			Unit: DAY,
			Num:  7,
		},
		SeriesIndexFlushTimeoutSeconds: 10,
		SeriesIndexCacheMaxBytes:       1024 * 1024,
	}

	serviceCache := NewServiceCache().(*serviceCache)
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,           // indexMetrics
		nil,           // metrics
		5*time.Minute, // idleTimeout
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache,
		group,
	)

	now := time.Now().UTC()
	startTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	endTime := startTime.Add(24 * time.Hour)

	// Create and open a segment
	segmentPath := filepath.Join(tempDir, "segment-"+startTime.Format(dayFormat))
	err := os.MkdirAll(segmentPath, DirPerm)
	require.NoError(t, err)

	// Write metadata file
	metadataPath := filepath.Join(segmentPath, metadataFilename)
	err = os.WriteFile(metadataPath, []byte(currentVersion), FilePerm)
	require.NoError(t, err)

	// Open segment
	suffix := startTime.Format(dayFormat)
	segment, err := sc.openSegment(ctx, startTime, endTime, segmentPath, suffix, sc.groupCache)
	require.NoError(t, err)
	require.NotNil(t, segment)

	// Verify segment is open
	assert.Greater(t, segment.refCount, int32(0))

	// Close segment by decrementing reference count
	initialRefCount := segment.refCount
	segment.DecRef()

	// Verify segment is closed (refCount reduced)
	assert.Equal(t, initialRefCount-1, segment.refCount)

	// Reopen segment
	segment.incRef(ctx)

	// Verify segment is properly reopened
	assert.Equal(t, initialRefCount, segment.refCount)

	// Verify we can still access segment data
	assert.NotNil(t, segment.index)
	assert.Equal(t, startTime, segment.Start)
	assert.Equal(t, endTime, segment.End)

	// Test that we can create a TSTable after reopening
	table, err := segment.CreateTSTableIfNotExist(0)
	require.NoError(t, err)
	assert.Equal(t, common.ShardID(0), table.ID)
}

func TestSegmentCloseIfIdle(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	l := logger.GetLogger("test-segment")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{
			Database: "test-db",
			Stage:    "test-stage",
		}
	})

	opts := TSDBOpts[mockTSTable, mockTSTableOpener]{
		TSTableCreator: func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
			_ timestamp.TimeRange, _ mockTSTableOpener, _ any,
		) (mockTSTable, error) {
			return mockTSTable{ID: common.ShardID(0)}, nil
		},
		ShardNum: 2,
		SegmentInterval: IntervalRule{
			Unit: DAY,
			Num:  1,
		},
		TTL: IntervalRule{
			Unit: DAY,
			Num:  7,
		},
		SeriesIndexFlushTimeoutSeconds: 10,
		SeriesIndexCacheMaxBytes:       1024 * 1024,
	}

	serviceCache := NewServiceCache().(*serviceCache)
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,         // indexMetrics
		nil,         // metrics
		time.Second, // Set short idle timeout for testing,
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache,
		group,
	)

	// Test time parameters
	now := time.Now().UTC()
	startTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	endTime := startTime.Add(24 * time.Hour)

	// Create and open a segment
	segmentPath := filepath.Join(tempDir, "segment-"+startTime.Format(dayFormat))
	err := os.MkdirAll(segmentPath, DirPerm)
	require.NoError(t, err)

	// Write metadata file
	metadataPath := filepath.Join(segmentPath, metadataFilename)
	err = os.WriteFile(metadataPath, []byte(currentVersion), FilePerm)
	require.NoError(t, err)

	// Open segment
	suffix := startTime.Format(dayFormat)
	segment, err := sc.openSegment(ctx, startTime, endTime, segmentPath, suffix, sc.groupCache)
	require.NoError(t, err)

	// Force last access time to be in the past
	segment.lastAccessed.Store(time.Now().Add(-time.Minute).UnixNano())

	// Close if idle should succeed
	segment.DecRef()

	// Verify segment is closed
	assert.Nil(t, segment.index)

	// Test reopening the segment
	segment.incRef(ctx)

	// Verify segment is properly reopened
	assert.NotNil(t, segment.index)
	assert.Greater(t, segment.refCount, int32(0))
}

func TestCloseIdleAndSelectSegments(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	l := logger.GetLogger("test-segment-controller")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{
			Database: "test-db",
			Stage:    "test-stage",
		}
	})

	opts := TSDBOpts[mockTSTable, mockTSTableOpener]{
		TSTableCreator: func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
			_ timestamp.TimeRange, _ mockTSTableOpener, _ any,
		) (mockTSTable, error) {
			return mockTSTable{ID: common.ShardID(0)}, nil
		},
		ShardNum: 2,
		SegmentInterval: IntervalRule{
			Unit: DAY,
			Num:  1,
		},
		TTL: IntervalRule{
			Unit: DAY,
			Num:  7,
		},
		SeriesIndexFlushTimeoutSeconds: 10,
		SeriesIndexCacheMaxBytes:       1024 * 1024,
	}

	// Create segment controller with a short idle timeout (100ms)
	idleTimeout := 100 * time.Millisecond
	serviceCache := NewServiceCache().(*serviceCache)
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,         // indexMetrics
		nil,         // metrics
		idleTimeout, // short idle timeout,
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache,
		group,
	)

	// Test time parameters
	now := time.Now().UTC()
	day1 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)
	day3 := day2.Add(24 * time.Hour)

	// Create multiple segments
	createSegment := func(startTime time.Time) *segment[mockTSTable, mockTSTableOpener] {
		segmentPath := filepath.Join(tempDir, "segment-"+startTime.Format(dayFormat))
		err := os.MkdirAll(segmentPath, DirPerm)
		require.NoError(t, err)

		// Write metadata file
		metadataPath := filepath.Join(segmentPath, metadataFilename)
		err = os.WriteFile(metadataPath, []byte(currentVersion), FilePerm)
		require.NoError(t, err)

		// Open segment
		suffix := startTime.Format(dayFormat)
		segment, err := sc.openSegment(ctx, startTime, startTime.Add(24*time.Hour), segmentPath, suffix, sc.groupCache)
		require.NoError(t, err)

		// Add segment to controller's list
		sc.Lock()
		sc.lst = append(sc.lst, segment)
		sc.sortLst()
		sc.Unlock()

		return segment
	}

	// Create three segments
	seg1 := createSegment(day1)
	seg2 := createSegment(day2)
	seg3 := createSegment(day3)

	// Verify we have three segments
	require.Len(t, sc.lst, 3)

	// Make sure all segments have reference counts > 0
	require.Greater(t, seg1.refCount, int32(0))
	require.Greater(t, seg2.refCount, int32(0))
	require.Greater(t, seg3.refCount, int32(0))

	// Force segments 1 and 3 to be idle (setting last accessed time in the past)
	seg1.lastAccessed.Store(time.Now().Add(-time.Second).UnixNano())
	seg3.lastAccessed.Store(time.Now().Add(-time.Second).UnixNano())

	// Keep seg2 active
	seg2.lastAccessed.Store(time.Now().UnixNano())

	// Close idle segments
	closedCount := sc.closeIdleSegments()

	// We should have closed 2 segments (seg1 and seg3)
	assert.Equal(t, 2, closedCount)

	// Check that seg1 and seg3 are closed (index is nil)
	assert.Nil(t, seg1.index)
	assert.Nil(t, seg3.index)

	// While seg2 remains open
	assert.NotNil(t, seg2.index)

	// Now select segments using the entire time range
	timeRange := timestamp.NewInclusiveTimeRange(day1, day3.Add(24*time.Hour))
	selectedSegments, err := sc.selectSegments(timeRange)
	require.NoError(t, err)

	// Should have selected all 3 segments
	require.Len(t, selectedSegments, 3)

	// Verify segments were reopened (they should have an index again)
	for _, s := range selectedSegments {
		seg := s.(*segment[mockTSTable, mockTSTableOpener])
		assert.NotNil(t, seg.index, "Selected segment should be reopened with a valid index")
		assert.Greater(t, seg.refCount, int32(0), "Selected segment should have positive reference count")
		seg.DecRef() // Cleanup
	}
}

func TestOpenExistingSegmentWithShards(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	l := logger.GetLogger("test-segment-shards")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{
			Database: "test-db",
			Stage:    "test-stage",
		}
	})

	opts := TSDBOpts[mockTSTable, mockTSTableOpener]{
		TSTableCreator: func(_ fs.FileSystem, location string, _ common.Position, _ *logger.Logger,
			_ timestamp.TimeRange, _ mockTSTableOpener, _ any,
		) (mockTSTable, error) {
			shardID := common.ShardID(0)
			// Extract shard ID from the path
			if shardPath := filepath.Base(location); len(shardPath) > 6 {
				if id, err := strconv.Atoi(shardPath[6:]); err == nil {
					shardID = common.ShardID(id)
				}
			}
			return mockTSTable{ID: shardID}, nil
		},
		ShardNum: 2,
		SegmentInterval: IntervalRule{
			Unit: DAY,
			Num:  1,
		},
		TTL: IntervalRule{
			Unit: DAY,
			Num:  7,
		},
		SeriesIndexFlushTimeoutSeconds: 10,
		SeriesIndexCacheMaxBytes:       1024 * 1024,
	}

	serviceCache := NewServiceCache().(*serviceCache)
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,           // indexMetrics
		nil,           // metrics
		5*time.Minute, // idleTimeout,
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache,
		group,
	)

	// Test time parameters
	now := time.Now().UTC()
	startTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	endTime := startTime.Add(24 * time.Hour)

	// Create segment directory
	suffix := startTime.Format(dayFormat)
	segmentPath := filepath.Join(tempDir, "segment-"+suffix)
	err := os.MkdirAll(segmentPath, DirPerm)
	require.NoError(t, err)

	// Write metadata file
	metadataPath := filepath.Join(segmentPath, metadataFilename)
	err = os.WriteFile(metadataPath, []byte(currentVersion), FilePerm)
	require.NoError(t, err)

	// Create shard directories
	for i := 0; i < int(opts.ShardNum); i++ {
		shardPath := filepath.Join(segmentPath, fmt.Sprintf("shard-%d", i))
		err = os.MkdirAll(shardPath, DirPerm)
		require.NoError(t, err)

		// Add a metadata file to each shard
		shardMetadataPath := filepath.Join(shardPath, metadataFilename)
		err = os.WriteFile(shardMetadataPath, []byte(currentVersion), FilePerm)
		require.NoError(t, err)
	}

	// Open the segment
	segment, err := sc.openSegment(ctx, startTime, endTime, segmentPath, suffix, sc.groupCache)
	require.NoError(t, err)
	require.NotNil(t, segment)

	// Verify both shards were loaded
	shardList := segment.sLst.Load()
	require.NotNil(t, shardList, "Shard list should not be nil")
	require.Len(t, *shardList, 2, "Segment should have loaded two shards")

	// Verify shard IDs
	shardIDs := make([]common.ShardID, 2)
	for i, shard := range *shardList {
		shardIDs[i] = shard.id
	}
	assert.Contains(t, shardIDs, common.ShardID(0), "Shard 0 should be loaded")
	assert.Contains(t, shardIDs, common.ShardID(1), "Shard 1 should be loaded")

	// Verify tables can be retrieved
	tables, _ := segment.Tables()
	require.Len(t, tables, 2, "Should have 2 tables")

	// Make sure each table has the correct ID
	tableIDs := make([]common.ShardID, 2)
	for i, table := range tables {
		tableIDs[i] = table.ID
	}
	assert.Contains(t, tableIDs, common.ShardID(0), "Table for shard 0 should exist")
	assert.Contains(t, tableIDs, common.ShardID(1), "Table for shard 1 should exist")

	// Clean up
	segment.DecRef()
}

func TestDeleteExpiredSegmentsWithClosedSegments(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	l := logger.GetLogger("test-expired-segments")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{
			Database: "test-db",
			Stage:    "test-stage",
		}
	})

	// Use a short TTL for testing - 3 days
	opts := TSDBOpts[mockTSTable, mockTSTableOpener]{
		TSTableCreator: func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
			_ timestamp.TimeRange, _ mockTSTableOpener, _ any,
		) (mockTSTable, error) {
			return mockTSTable{ID: common.ShardID(0)}, nil
		},
		ShardNum: 2,
		SegmentInterval: IntervalRule{
			Unit: DAY,
			Num:  1,
		},
		TTL: IntervalRule{
			Unit: DAY,
			Num:  3, // 3 days TTL
		},
		SeriesIndexFlushTimeoutSeconds: 10,
		SeriesIndexCacheMaxBytes:       1024 * 1024,
	}

	// Create segment controller with a short idle timeout
	idleTimeout := 100 * time.Millisecond
	serviceCache := NewServiceCache().(*serviceCache)
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,         // indexMetrics
		nil,         // metrics
		idleTimeout, // short idle timeout
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache,
		group,
	)

	// Create segments spanning 6 days - some will be expired, some won't
	now := time.Now().UTC()
	baseDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	// Create segments representing different days
	// day1 through day3 should be considered expired (older than TTL)
	// day4 through day6 should not be expired
	segmentDates := []time.Time{
		baseDate.AddDate(0, 0, -6), // day1 - expired
		baseDate.AddDate(0, 0, -5), // day2 - expired
		baseDate.AddDate(0, 0, -4), // day3 - expired
		baseDate.AddDate(0, 0, -2), // day4 - not expired
		baseDate.AddDate(0, 0, -1), // day5 - not expired
		baseDate,                   // day6 - not expired (today)
	}

	var segments []*segment[mockTSTable, mockTSTableOpener]

	// Create segments
	for _, date := range segmentDates {
		segmentPath := filepath.Join(tempDir, "segment-"+date.Format(dayFormat))
		err := os.MkdirAll(segmentPath, DirPerm)
		require.NoError(t, err)

		// Write metadata file
		metadataPath := filepath.Join(segmentPath, metadataFilename)
		err = os.WriteFile(metadataPath, []byte(currentVersion), FilePerm)
		require.NoError(t, err)

		// Open segment
		suffix := date.Format(dayFormat)
		segment, err := sc.openSegment(ctx, date, date.Add(24*time.Hour), segmentPath, suffix, sc.groupCache)
		require.NoError(t, err)

		// Add segment to controller
		sc.Lock()
		sc.lst = append(sc.lst, segment)
		sc.sortLst()
		sc.Unlock()

		segments = append(segments, segment)
	}

	// Verify all segments are initially open
	for i, seg := range segments {
		assert.Greater(t, seg.refCount, int32(0), "Segment %d should be open", i)
	}

	// Set the "lastAccessed" time for some segments to make them idle
	// Make the first, third, and fifth segments idle (0, 2, 4)
	// Use a time that's definitely older than the idle timeout
	idleTime := time.Now().Add(-2 * idleTimeout).UnixNano()
	segments[0].lastAccessed.Store(idleTime) // day1 - expired
	segments[2].lastAccessed.Store(idleTime) // day3 - expired
	segments[4].lastAccessed.Store(idleTime) // day5 - not expired

	// Keep segments 1, 3, and 5 active by setting their lastAccessed to current time
	// Use a time that's definitely newer than the idle timeout threshold
	activeTime := time.Now().UnixNano()
	segments[1].lastAccessed.Store(activeTime) // day2 - expired but should stay open
	segments[3].lastAccessed.Store(activeTime) // day4 - not expired
	segments[5].lastAccessed.Store(activeTime) // day6 - not expired

	// Close idle segments
	closedCount := sc.closeIdleSegments()
	assert.Equal(t, 3, closedCount, "Should have closed 3 segments")

	// Verify segments 0, 2, and 4 are closed
	assert.Equal(t, int32(0), segments[0].refCount, "Segment 0 should be closed")
	assert.NotNil(t, segments[1].index, "Segment 1 should remain open")
	assert.Equal(t, int32(0), segments[2].refCount, "Segment 2 should be closed")
	assert.NotNil(t, segments[3].index, "Segment 3 should remain open")
	assert.Equal(t, int32(0), segments[4].refCount, "Segment 4 should be closed")
	assert.NotNil(t, segments[5].index, "Segment 5 should remain open")

	// Now delete expired segments
	// Use the same segment dates (UTC) used when creating segments 0, 1, 2 to avoid timezone mismatch with time.Now()
	deletedCount := sc.deleteExpiredSegments([]string{
		segmentDates[0].Format(dayFormat),
		segmentDates[1].Format(dayFormat),
		segmentDates[2].Format(dayFormat),
	})
	assert.Equal(t, int64(3), deletedCount, "Should have deleted 3 expired segments")

	// Verify segment controller's segment list
	assert.Len(t, sc.lst, 3, "Should have 3 segments remaining")

	// Verify the remaining segments are the non-expired ones (day4, day5, day6)
	expectedDates := []time.Time{
		baseDate.AddDate(0, 0, -2), // day4
		baseDate.AddDate(0, 0, -1), // day5
		baseDate,                   // day6
	}

	for i, seg := range sc.lst {
		assert.Equal(t, expectedDates[i].Format(dayFormat), seg.TimeRange.Start.Format(dayFormat),
			"Remaining segment %d should be from the expected date", i)
	}
}

func TestCreateSegmentWritesJSONMetadata(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	l := logger.GetLogger("test-segment-metadata")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{
			Database: "test-db",
			Stage:    "test-stage",
		}
	})

	opts := TSDBOpts[mockTSTable, mockTSTableOpener]{
		TSTableCreator: func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
			_ timestamp.TimeRange, _ mockTSTableOpener, _ any,
		) (mockTSTable, error) {
			return mockTSTable{ID: common.ShardID(0)}, nil
		},
		ShardNum:                       1,
		SegmentInterval:                IntervalRule{Unit: DAY, Num: 1},
		TTL:                            IntervalRule{Unit: DAY, Num: 7},
		SeriesIndexFlushTimeoutSeconds: 10,
		SeriesIndexCacheMaxBytes:       1024 * 1024,
	}

	serviceCache := NewServiceCache().(*serviceCache)
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,
		nil,
		5*time.Minute,
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache,
		group,
	)

	now := time.Now().UTC()
	startTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	seg, createErr := sc.create(startTime)
	require.NoError(t, createErr)
	require.NotNil(t, seg)

	// Read metadata from disk and verify it's JSON with endTime
	suffix := startTime.Format(dayFormat)
	metadataPath := filepath.Join(tempDir, fmt.Sprintf("seg-%s", suffix), metadataFilename)
	rawMeta, readErr := os.ReadFile(metadataPath)
	require.NoError(t, readErr)

	meta, parseErr := readSegmentMeta(rawMeta)
	require.NoError(t, parseErr)
	assert.Equal(t, currentVersion, meta.Version)
	assert.NotEmpty(t, meta.EndTime, "endTime should be persisted in metadata")

	// Verify the endTime matches the segment's End
	expectedEnd := startTime.Add(24 * time.Hour)
	assert.Equal(t, expectedEnd.Format(time.RFC3339Nano), meta.EndTime)

	seg.DecRef()
}

func TestOpenReadsPersistedEndTime(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	l := logger.GetLogger("test-open-endtime")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{
			Database: "test-db",
			Stage:    "test-stage",
		}
	})

	group := "test-group"
	opts := TSDBOpts[mockTSTable, mockTSTableOpener]{
		TSTableCreator: func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
			_ timestamp.TimeRange, _ mockTSTableOpener, _ any,
		) (mockTSTable, error) {
			return mockTSTable{ID: common.ShardID(0)}, nil
		},
		ShardNum:                       1,
		SegmentInterval:                IntervalRule{Unit: DAY, Num: 1},
		TTL:                            IntervalRule{Unit: DAY, Num: 30},
		SeriesIndexFlushTimeoutSeconds: 10,
		SeriesIndexCacheMaxBytes:       1024 * 1024,
	}

	serviceCache := NewServiceCache().(*serviceCache)
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,
		nil,
		5*time.Minute,
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache,
		group,
	)

	now := time.Now().UTC()
	day1 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)

	// Manually create segment directories with JSON metadata.
	// day1 with a specific endTime that differs from the default NextTime.
	customEnd := day1.Add(12 * time.Hour)
	segPath1 := filepath.Join(tempDir, "seg-"+day1.Format(dayFormat))
	require.NoError(t, os.MkdirAll(segPath1, DirPerm))
	meta1 := segmentMeta{Version: currentVersion, EndTime: customEnd.Format(time.RFC3339Nano)}
	meta1Data, marshalErr := json.Marshal(meta1)
	require.NoError(t, marshalErr)
	require.NoError(t, os.WriteFile(filepath.Join(segPath1, metadataFilename), meta1Data, FilePerm))

	// day2 with standard endTime.
	segPath2 := filepath.Join(tempDir, "seg-"+day2.Format(dayFormat))
	require.NoError(t, os.MkdirAll(segPath2, DirPerm))
	meta2 := segmentMeta{Version: currentVersion, EndTime: day2.Add(24 * time.Hour).Format(time.RFC3339Nano)}
	meta2Data, marshalErr := json.Marshal(meta2)
	require.NoError(t, marshalErr)
	require.NoError(t, os.WriteFile(filepath.Join(segPath2, metadataFilename), meta2Data, FilePerm))

	// Open the controller (loads segments from disk).
	openErr := sc.open()
	require.NoError(t, openErr)

	// Verify segments loaded correctly.
	require.Len(t, sc.lst, 2)

	// First segment should have the custom endTime from metadata.
	assert.Equal(t, customEnd, sc.lst[0].End, "segment 0 should use persisted endTime")
	// Second segment should have its endTime from metadata.
	assert.Equal(t, day2.Add(24*time.Hour), sc.lst[1].End, "segment 1 should use persisted endTime")

	for _, seg := range sc.lst {
		seg.DecRef()
	}
}

func TestOpenFallbackOldFormatMetadata(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	l := logger.GetLogger("test-open-fallback")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{
			Database: "test-db",
			Stage:    "test-stage",
		}
	})

	group := "test-group"
	opts := TSDBOpts[mockTSTable, mockTSTableOpener]{
		TSTableCreator: func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
			_ timestamp.TimeRange, _ mockTSTableOpener, _ any,
		) (mockTSTable, error) {
			return mockTSTable{ID: common.ShardID(0)}, nil
		},
		ShardNum:                       1,
		SegmentInterval:                IntervalRule{Unit: DAY, Num: 1},
		TTL:                            IntervalRule{Unit: DAY, Num: 30},
		SeriesIndexFlushTimeoutSeconds: 10,
		SeriesIndexCacheMaxBytes:       1024 * 1024,
	}

	serviceCache := NewServiceCache().(*serviceCache)
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,
		nil,
		5*time.Minute,
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache,
		group,
	)

	now := time.Now()
	day1 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	day2 := day1.Add(24 * time.Hour)

	// Create segments with OLD format metadata (plain version string).
	segPath1 := filepath.Join(tempDir, "seg-"+day1.Format(dayFormat))
	require.NoError(t, os.MkdirAll(segPath1, DirPerm))
	require.NoError(t, os.WriteFile(filepath.Join(segPath1, metadataFilename), []byte("1.4.0"), FilePerm))

	segPath2 := filepath.Join(tempDir, "seg-"+day2.Format(dayFormat))
	require.NoError(t, os.MkdirAll(segPath2, DirPerm))
	require.NoError(t, os.WriteFile(filepath.Join(segPath2, metadataFilename), []byte("1.4.0"), FilePerm))

	// Open should succeed with fallback end time computation.
	openErr := sc.open()
	require.NoError(t, openErr)

	require.Len(t, sc.lst, 2)

	// First segment's end should be day2's start (fallback: end = next segment's start).
	assert.Equal(t, day2, sc.lst[0].End, "old format should use fallback end time")
	// Second segment's end should be day2 + 24h (fallback: end = NextTime(start)).
	assert.Equal(t, day2.Add(24*time.Hour), sc.lst[1].End, "last segment should use NextTime fallback")

	for _, seg := range sc.lst {
		seg.DecRef()
	}
}

// TestSegment_IndexDB_ReturnsUntypedNilWhenClosed is a regression guard for
// the cold-tier nil-pointer panic. After a segment has been idle-closed
// (s.index == nil), segment.IndexDB() must return an untyped nil interface
// so that the standard caller pattern `if indexDB == nil { ... }` short
// circuits correctly. Returning the underlying *seriesIndex would box a
// typed-nil interface, defeat the nil check, and crash on the next method
// call.
func TestSegment_IndexDB_ReturnsUntypedNilWhenClosed(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	l := logger.GetLogger("test-untyped-nil")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{Database: "test-db", Stage: "test-stage"}
	})

	opts := TSDBOpts[mockTSTable, mockTSTableOpener]{
		TSTableCreator: func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
			_ timestamp.TimeRange, _ mockTSTableOpener, _ any,
		) (mockTSTable, error) {
			return mockTSTable{ID: common.ShardID(0)}, nil
		},
		ShardNum:                       1,
		SegmentInterval:                IntervalRule{Unit: DAY, Num: 1},
		TTL:                            IntervalRule{Unit: DAY, Num: 7},
		SeriesIndexFlushTimeoutSeconds: 10,
		SeriesIndexCacheMaxBytes:       1024 * 1024,
	}

	serviceCache := NewServiceCache().(*serviceCache)
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx, tempDir, l, opts, nil, nil, time.Second,
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache, group,
	)

	now := time.Now().UTC()
	startTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	endTime := startTime.Add(24 * time.Hour)

	segPath := filepath.Join(tempDir, "segment-"+startTime.Format(dayFormat))
	require.NoError(t, os.MkdirAll(segPath, DirPerm))
	require.NoError(t, os.WriteFile(
		filepath.Join(segPath, metadataFilename),
		[]byte(currentVersion), FilePerm))

	seg, err := sc.openSegment(ctx, startTime, endTime, segPath,
		startTime.Format(dayFormat), sc.groupCache)
	require.NoError(t, err)

	// Idle-close: drop the only reference so performCleanup runs and the
	// segment ends up in the residual state seen on cold-tier nodes
	// (refCount=0, s.index=nil, but segment still reachable for reopen).
	seg.DecRef()
	require.Nil(t, seg.index)
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount))

	indexDB := seg.IndexDB()
	require.True(t, indexDB == nil,
		"IndexDB() must return untyped nil after idle close so the caller's "+
			"`if indexDB == nil` guard fires")

	// Drive the exact caller pattern from collectSeriesIndexInfo. It must
	// short-circuit on the nil check and never dispatch Stats() on a nil
	// receiver.
	var dataCount, dataSize int64
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("caller pattern must not panic but got: %v", r)
			}
		}()
		if indexDB != nil {
			dataCount, dataSize = indexDB.Stats()
		}
	}()
	assert.Equal(t, int64(0), dataCount)
	assert.Equal(t, int64(0), dataSize)
}

// TestSegment_ConcurrentReopen_RefCountConsistent stress-tests the
// reference-count contract of segment.incRef under concurrent reopen.
// Before the fix, segment.initialize() returned without AddInt32 when
// another goroutine had already reopened the segment, so the second caller
// "owned" a reference it had never accounted for. The first DecRef would
// then trigger performCleanup while another goroutine was still using the
// segment.
//
// After the fix, every concurrent incRef must add exactly one reference
// regardless of which path it takes.
//
// Reliability note: a single 64-goroutine round can occasionally serialize
// well enough that only the first caller enters the slow path
// (initialize) and the rest take the fast path (CAS+1). That serialized
// shape would also pass on the buggy code. To make the test deterministic
// across machines, the experiment is repeated for `rounds` cycles, each
// starting from the residual state. The bug becomes effectively
// impossible to miss across all rounds combined.
func TestSegment_ConcurrentReopen_RefCountConsistent(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	l := logger.GetLogger("test-concurrent-reopen")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{Database: "test-db", Stage: "test-stage"}
	})

	opts := TSDBOpts[mockTSTable, mockTSTableOpener]{
		TSTableCreator: func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
			_ timestamp.TimeRange, _ mockTSTableOpener, _ any,
		) (mockTSTable, error) {
			return mockTSTable{ID: common.ShardID(0)}, nil
		},
		ShardNum:                       2,
		SegmentInterval:                IntervalRule{Unit: DAY, Num: 1},
		TTL:                            IntervalRule{Unit: DAY, Num: 7},
		SeriesIndexFlushTimeoutSeconds: 10,
		SeriesIndexCacheMaxBytes:       1024 * 1024,
	}

	serviceCache := NewServiceCache().(*serviceCache)
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx, tempDir, l, opts, nil, nil, time.Second,
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache, group,
	)

	now := time.Now().UTC()
	startTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	endTime := startTime.Add(24 * time.Hour)

	segPath := filepath.Join(tempDir, "segment-"+startTime.Format(dayFormat))
	require.NoError(t, os.MkdirAll(segPath, DirPerm))
	require.NoError(t, os.WriteFile(
		filepath.Join(segPath, metadataFilename),
		[]byte(currentVersion), FilePerm))

	seg, err := sc.openSegment(ctx, startTime, endTime, segPath,
		startTime.Format(dayFormat), sc.groupCache)
	require.NoError(t, err)

	// Bring the segment into the cold-tier residual state.
	seg.DecRef()
	require.Nil(t, seg.index)
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount))

	const (
		N      = 64
		rounds = 10
	)
	for r := 0; r < rounds; r++ {
		require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount),
			"round %d: segment must start at refCount=0", r)
		require.Nil(t, seg.index, "round %d: segment.index must start nil", r)

		var (
			wg     sync.WaitGroup
			start  = make(chan struct{})
			errCnt int64
		)
		wg.Add(N)
		for i := 0; i < N; i++ {
			go func() {
				defer wg.Done()
				<-start
				if incErr := seg.incRef(ctx); incErr != nil {
					atomic.AddInt64(&errCnt, 1)
				}
			}()
		}
		close(start)
		wg.Wait()

		require.Equal(t, int64(0), atomic.LoadInt64(&errCnt),
			"round %d: incRef must not error", r)
		require.Equal(t, int32(N), atomic.LoadInt32(&seg.refCount),
			"round %d: every concurrent incRef must add exactly one reference", r)
		require.NotNil(t, seg.index, "round %d: segment.index must be reopened", r)

		// Drain references; the last DecRef must trigger performCleanup
		// and bring the segment back to the residual state for the next
		// round.
		for i := 0; i < N; i++ {
			seg.DecRef()
		}
		require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount),
			"round %d", r)
		require.Nil(t, seg.index,
			"round %d: segment.index must be cleared by performCleanup", r)
	}
}

// TestSegment_ConcurrentReopenAndClose_NoPanic exercises the cold-tier
// reopen pattern under heavy concurrency: many goroutines repeatedly call
// selectSegments, exercise IndexDB().Stats() on each returned segment, and
// release with DecRef. With idle-closed segments as the starting state,
// each iteration triggers initialize() to reopen the segment and the final
// DecRef triggers performCleanup() to close it again, so closes and
// reopens are interleaved across goroutines.
//
// Verifies (run with -race):
//   - no goroutine panics (covers both the typed-nil and refcount fixes)
//   - no data race is detected on the segment lifecycle
//   - all segments end up cleanly closed (refCount == 0, index == nil)
func TestSegment_ConcurrentReopenAndClose_NoPanic(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	l := logger.GetLogger("test-concurrent-reopen-close")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{Database: "test-db", Stage: "test-stage"}
	})

	opts := TSDBOpts[mockTSTable, mockTSTableOpener]{
		TSTableCreator: func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
			_ timestamp.TimeRange, _ mockTSTableOpener, _ any,
		) (mockTSTable, error) {
			return mockTSTable{ID: common.ShardID(0)}, nil
		},
		ShardNum:                       2,
		SegmentInterval:                IntervalRule{Unit: DAY, Num: 1},
		TTL:                            IntervalRule{Unit: DAY, Num: 7},
		SeriesIndexFlushTimeoutSeconds: 10,
		SeriesIndexCacheMaxBytes:       1024 * 1024,
	}

	serviceCache := NewServiceCache().(*serviceCache)
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx, tempDir, l, opts, nil, nil, time.Second,
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache, group,
	)

	now := time.Now().UTC()
	day1 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)
	day3 := day2.Add(24 * time.Hour)
	days := []time.Time{day1, day2, day3}

	for _, d := range days {
		segPath := filepath.Join(tempDir, "segment-"+d.Format(dayFormat))
		require.NoError(t, os.MkdirAll(segPath, DirPerm))
		require.NoError(t, os.WriteFile(
			filepath.Join(segPath, metadataFilename),
			[]byte(currentVersion), FilePerm))
		seg, openErr := sc.openSegment(ctx, d, d.Add(24*time.Hour),
			segPath, d.Format(dayFormat), sc.groupCache)
		require.NoError(t, openErr)
		sc.Lock()
		sc.lst = append(sc.lst, seg)
		sc.sortLst()
		sc.Unlock()
		// Drop the open reference so each segment starts in the residual state.
		seg.DecRef()
		require.Nil(t, seg.index)
	}

	timeRange := timestamp.NewInclusiveTimeRange(day1, day3.Add(24*time.Hour))

	const (
		goroutines = 8
		cycles     = 200
	)
	var (
		wg       sync.WaitGroup
		start    = make(chan struct{})
		panicCnt int64
	)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt64(&panicCnt, 1)
					t.Errorf("goroutine-%d panicked: %v", id, r)
				}
			}()
			<-start
			for c := 0; c < cycles; c++ {
				segs, selErr := sc.selectSegments(timeRange)
				if selErr != nil {
					t.Errorf("goroutine-%d cycle %d selectSegments: %v", id, c, selErr)
					return
				}
				for _, s := range segs {
					if idx := s.IndexDB(); idx != nil {
						_, _ = idx.Stats()
					}
					s.DecRef()
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()

	require.Equal(t, int64(0), atomic.LoadInt64(&panicCnt),
		"no goroutine should panic during the concurrent reopen/close cycle")

	sc.RLock()
	final := append([]*segment[mockTSTable, mockTSTableOpener]{}, sc.lst...)
	sc.RUnlock()
	for _, s := range final {
		require.Equal(t, int32(0), atomic.LoadInt32(&s.refCount),
			"segment %s leaked references", s.suffix)
		require.Nil(t, s.index, "segment %s should be cleaned up", s.suffix)
	}
}

// newAlignmentTestController is a compact helper for tests covering the
// epoch-aligned segment.create() behavior introduced by the lifecycle fix.
// It spins up a real segmentController backed by a temp dir, with the given
// SegmentInterval, and returns the controller plus a cleanup function.
func newAlignmentTestController(
	t *testing.T, interval IntervalRule,
) (*segmentController[mockTSTable, mockTSTableOpener], string, func()) {
	t.Helper()
	tempDir, cleanup := setupTestEnvironment(t)

	ctx := context.Background()
	l := logger.GetLogger("test-alignment")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{
			Database: "test-db",
			Stage:    "alignment",
		}
	})

	opts := TSDBOpts[mockTSTable, mockTSTableOpener]{
		TSTableCreator: func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
			_ timestamp.TimeRange, _ mockTSTableOpener, _ any,
		) (mockTSTable, error) {
			return mockTSTable{ID: common.ShardID(0)}, nil
		},
		ShardNum:                       1,
		SegmentInterval:                interval,
		TTL:                            IntervalRule{Unit: DAY, Num: 60},
		SeriesIndexFlushTimeoutSeconds: 10,
		SeriesIndexCacheMaxBytes:       1024 * 1024,
	}

	serviceCache := NewServiceCache().(*serviceCache)
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,
		nil,
		5*time.Minute,
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache,
		group,
	)
	return sc, tempDir, cleanup
}

// TestIntervalRule_Standard_AlignsToEpochGrid pins the alignment math itself.
// Two timestamps in the same N*Unit bucket measured from epoch must round
// down to the exact same instant; two timestamps in adjacent buckets must
// round to instants exactly N*Unit apart.
func TestIntervalRule_Standard_AlignsToEpochGrid(t *testing.T) {
	//nolint:govet // fieldalignment: test struct optimization not critical
	cases := []struct {
		name     string
		ir       IntervalRule
		probes   []time.Time
		expected []time.Time
	}{
		{
			name: "DAY Num=15 matches calculateTargetSegments grid",
			ir:   IntervalRule{Unit: DAY, Num: 15},
			probes: []time.Time{
				time.Date(2026, 4, 7, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 16, 6, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 19, 23, 59, 59, 0, time.UTC),
				time.Date(2026, 4, 22, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 5, 7, 12, 30, 0, 0, time.UTC),
			},
			expected: []time.Time{
				time.Date(2026, 4, 7, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 7, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 7, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 22, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "DAY Num=5",
			ir:   IntervalRule{Unit: DAY, Num: 5},
			// 1970-01-01 + N*5 day boundaries near today: 2026-04-02 (day 20545),
			// 2026-04-07 (day 20550), 2026-04-12 (day 20555).
			probes: []time.Time{
				time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 3, 6, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 6, 23, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 7, 0, 0, 0, 0, time.UTC),
			},
			expected: []time.Time{
				time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 7, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "HOUR Num=6",
			ir:   IntervalRule{Unit: HOUR, Num: 6},
			probes: []time.Time{
				time.Date(2026, 4, 19, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 19, 5, 59, 59, 0, time.UTC),
				time.Date(2026, 4, 19, 6, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 19, 11, 30, 0, 0, time.UTC),
				time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC),
			},
			expected: []time.Time{
				time.Date(2026, 4, 19, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 19, 0, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 19, 6, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 19, 6, 0, 0, 0, time.UTC),
				time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC),
			},
		},
		{
			name:     "DAY Num=1 matches old Unit.Standard behavior",
			ir:       IntervalRule{Unit: DAY, Num: 1},
			probes:   []time.Time{time.Date(2026, 4, 19, 6, 30, 0, 0, time.UTC)},
			expected: []time.Time{time.Date(2026, 4, 19, 0, 0, 0, 0, time.UTC)},
		},
		{
			name:     "Num=0 falls back to Unit.Standard",
			ir:       IntervalRule{Unit: DAY, Num: 0},
			probes:   []time.Time{time.Date(2026, 4, 19, 6, 30, 0, 0, time.UTC)},
			expected: []time.Time{time.Date(2026, 4, 19, 0, 0, 0, 0, time.UTC)},
		},
		{
			// Pre-epoch inputs: floor division puts them in the bucket below
			// rather than collapsing to epoch as truncated division would.
			name: "DAY Num=15 pre-epoch uses floor division",
			ir:   IntervalRule{Unit: DAY, Num: 15},
			probes: []time.Time{
				time.Date(1969, 12, 25, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC),
			},
			expected: []time.Time{
				time.Date(1969, 12, 17, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 17, 0, 0, 0, 0, time.UTC),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, len(tc.probes), len(tc.expected))
			for i, p := range tc.probes {
				got := tc.ir.Standard(p)
				assert.Equal(t, tc.expected[i], got,
					"Standard(%s) under %d %s expected %s got %s",
					p.Format(time.RFC3339), tc.ir.Num, tc.ir.Unit.String(),
					tc.expected[i].Format(time.RFC3339), got.Format(time.RFC3339))
			}
		})
	}
}

// TestIntervalRule_Standard_PreservesLocation verifies that the returned
// time keeps the input's Location while the bucket instant stays invariant
// across timezones - the grid is anchored in UTC for cross-node consistency
// but presented in the caller's local time.
func TestIntervalRule_Standard_PreservesLocation(t *testing.T) {
	shanghai := time.FixedZone("CST", 8*3600)
	losAngeles := time.FixedZone("PST", -8*3600)

	cases := []struct {
		probe time.Time
		ir    IntervalRule
	}{
		{time.Date(2026, 4, 19, 6, 0, 0, 0, time.UTC), IntervalRule{Unit: DAY, Num: 15}},
		{time.Date(2026, 4, 19, 11, 30, 0, 0, time.UTC), IntervalRule{Unit: HOUR, Num: 6}},
	}
	for _, c := range cases {
		want := c.ir.Standard(c.probe)
		for _, loc := range []*time.Location{time.UTC, shanghai, losAngeles} {
			got := c.ir.Standard(c.probe.In(loc))
			assert.Equal(t, loc, got.Location(),
				"Standard must return time in input.Location(); ir=%+v loc=%s", c.ir, loc)
			assert.True(t, got.Equal(want),
				"bucket instant must be timezone-invariant; ir=%+v loc=%s utc=%s got=%s",
				c.ir, loc, want.Format(time.RFC3339), got.Format(time.RFC3339))
		}
	}
}

// TestCreateSegment_OutOfOrderArrival_SameBucket reproduces the production
// truncation observed on demo-banyandb-data-cold-0 on 2026-04-30 where a part
// with MinTimestamp ~2026-04-19 created seg-20260419 first and a later-
// processed part with MinTimestamp ~2026-04-16 produced a 3-day truncated
// seg-20260416. With Standard() honoring Num, both timestamps resolve to the
// same epoch-aligned bucket [04/07, 04/22), so the second create returns the
// same segment instead of creating a truncated neighbor.
func TestCreateSegment_OutOfOrderArrival_SameBucket(t *testing.T) {
	sc, _, cleanup := newAlignmentTestController(t, IntervalRule{Unit: DAY, Num: 15})
	defer cleanup()

	laterFirst := time.Date(2026, 4, 19, 6, 0, 0, 0, time.UTC)
	earlierAfter := time.Date(2026, 4, 16, 6, 0, 0, 0, time.UTC)
	expectedBucketStart := time.Date(2026, 4, 7, 0, 0, 0, 0, time.UTC)
	expectedBucketEnd := time.Date(2026, 4, 22, 0, 0, 0, 0, time.UTC)

	seg1, err := sc.create(laterFirst)
	require.NoError(t, err)
	require.NotNil(t, seg1)
	defer seg1.DecRef()

	seg2, err := sc.create(earlierAfter)
	require.NoError(t, err)
	require.NotNil(t, seg2)

	assert.Same(t, seg1, seg2,
		"both timestamps fall in the same 15d bucket; create() must reuse the existing segment")
	assert.Equal(t, expectedBucketStart, seg1.Start,
		"segment must start at the epoch-aligned bucket boundary, not at the first arrival's day")
	assert.Equal(t, expectedBucketEnd, seg1.End)
	assert.Equal(t, 15*24*time.Hour, seg1.End.Sub(seg1.Start))
}

// TestCreateSegment_OutOfOrderArrival_AdjacentBuckets covers the case where
// the late-arrival timestamp falls in a strictly different epoch bucket from
// the first-arrival one. The new segment must occupy a clean N*Unit span
// without being truncated by the existing later segment - the truncation
// branch in create() must collapse to "stdEnd == next.Start" exactly, leaving
// span = N*Unit.
func TestCreateSegment_OutOfOrderArrival_AdjacentBuckets(t *testing.T) {
	sc, _, cleanup := newAlignmentTestController(t, IntervalRule{Unit: DAY, Num: 15})
	defer cleanup()

	laterFirst := time.Date(2026, 5, 5, 6, 0, 0, 0, time.UTC)    // bucket [04/22, 05/07)
	earlierAfter := time.Date(2026, 4, 15, 6, 0, 0, 0, time.UTC) // bucket [04/07, 04/22)

	seg1, err := sc.create(laterFirst)
	require.NoError(t, err)
	require.NotNil(t, seg1)
	defer seg1.DecRef()
	assert.Equal(t, time.Date(2026, 4, 22, 0, 0, 0, 0, time.UTC), seg1.Start)
	assert.Equal(t, time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC), seg1.End)
	assert.Equal(t, 15*24*time.Hour, seg1.End.Sub(seg1.Start))

	seg2, err := sc.create(earlierAfter)
	require.NoError(t, err)
	require.NotNil(t, seg2)
	defer seg2.DecRef()
	assert.NotSame(t, seg1, seg2,
		"timestamps in different buckets must not collapse to one segment")
	assert.Equal(t, time.Date(2026, 4, 7, 0, 0, 0, 0, time.UTC), seg2.Start)
	assert.Equal(t, time.Date(2026, 4, 22, 0, 0, 0, 0, time.UTC), seg2.End,
		"new earlier segment must abut next.Start with stdEnd==next.Start (no internal truncation)")
	assert.Equal(t, 15*24*time.Hour, seg2.End.Sub(seg2.Start))
}

// TestCreateSegment_HourInterval_OutOfOrder verifies the same alignment
// guarantee for HOUR-based segment intervals (Num=6).
func TestCreateSegment_HourInterval_OutOfOrder(t *testing.T) {
	sc, _, cleanup := newAlignmentTestController(t, IntervalRule{Unit: HOUR, Num: 6})
	defer cleanup()

	t1 := time.Date(2026, 4, 19, 11, 30, 0, 0, time.UTC) // bucket [06:00, 12:00)
	t2 := time.Date(2026, 4, 19, 8, 15, 0, 0, time.UTC)  // same bucket
	t3 := time.Date(2026, 4, 19, 13, 0, 0, 0, time.UTC)  // bucket [12:00, 18:00)

	seg1, err := sc.create(t1)
	require.NoError(t, err)
	defer seg1.DecRef()
	assert.Equal(t, time.Date(2026, 4, 19, 6, 0, 0, 0, time.UTC), seg1.Start)
	assert.Equal(t, 6*time.Hour, seg1.End.Sub(seg1.Start))

	seg2, err := sc.create(t2)
	require.NoError(t, err)
	assert.Same(t, seg1, seg2, "08:15 must reuse the [06:00, 12:00) segment")

	seg3, err := sc.create(t3)
	require.NoError(t, err)
	defer seg3.DecRef()
	assert.NotSame(t, seg1, seg3)
	assert.Equal(t, time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC), seg3.Start)
	assert.Equal(t, 6*time.Hour, seg3.End.Sub(seg3.Start))
}

// TestCreateSegment_ConcurrentCreates_DeterministicAlignment exercises the
// receiver under contention: many goroutines call create() with random
// timestamps spread across a window that covers multiple epoch-aligned
// buckets. Whatever interleaving the scheduler picks, the resulting segment
// set must (a) cover every probe timestamp, (b) all start on epoch-aligned
// boundaries, (c) every span be exactly the configured N*Unit, and (d) be
// non-overlapping. This pins down the property that motivated the fix:
// alignment is determined by the global grid, not by arrival order.
func TestCreateSegment_ConcurrentCreates_DeterministicAlignment(t *testing.T) {
	sc, _, cleanup := newAlignmentTestController(t, IntervalRule{Unit: DAY, Num: 15})
	defer cleanup()

	// Probe range covers 4 epoch-aligned 15d buckets:
	// [03/08, 03/23), [03/23, 04/07), [04/07, 04/22), [04/22, 05/07).
	rangeStart := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	rangeEndExclusive := time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC)
	rangeNanos := rangeEndExclusive.UnixNano() - rangeStart.UnixNano()

	const goroutines = 32
	const probesPerGoroutine = 64
	const knuthMul = uint64(0x9E3779B97F4A7C15)
	probes := make([]time.Time, 0, goroutines*probesPerGoroutine)
	for i := 0; i < goroutines*probesPerGoroutine; i++ {
		offset := int64((uint64(i+1)*knuthMul)>>1) % rangeNanos
		probes = append(probes, time.Unix(0, rangeStart.UnixNano()+offset).UTC())
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(idx int) {
			defer wg.Done()
			start := idx * probesPerGoroutine
			end := start + probesPerGoroutine
			for _, ts := range probes[start:end] {
				seg, err := sc.create(ts)
				if err != nil {
					t.Errorf("create(%s) returned error: %v", ts.Format(time.RFC3339), err)
					return
				}
				if seg == nil {
					t.Errorf("create(%s) returned nil segment", ts.Format(time.RFC3339))
					return
				}
				if !seg.Contains(ts.UnixNano()) {
					t.Errorf("probe %s landed in segment [%s, %s) which does not contain it",
						ts.Format(time.RFC3339), seg.Start.Format(time.RFC3339), seg.End.Format(time.RFC3339))
				}
				seg.DecRef()
			}
		}(g)
	}
	wg.Wait()

	sc.RLock()
	segs := slices.Clone(sc.lst)
	sc.RUnlock()

	require.NotEmpty(t, segs, "concurrent creates must produce at least one segment")
	sort.Slice(segs, func(i, j int) bool { return segs[i].Start.Before(segs[j].Start) })

	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	fifteenDays := 15 * 24 * time.Hour
	for i, seg := range segs {
		assert.Equal(t, fifteenDays, seg.End.Sub(seg.Start),
			"segment[%d] %s span must be exactly 15d, got %v",
			i, seg.suffix, seg.End.Sub(seg.Start))
		offset := seg.Start.Sub(epoch)
		assert.Equal(t, time.Duration(0), offset%fifteenDays,
			"segment[%d] start %s must be on the epoch-aligned 15d grid",
			i, seg.Start.Format(time.RFC3339))
		if i > 0 {
			assert.False(t, segs[i-1].End.After(seg.Start),
				"segments must not overlap: prev.End=%s next.Start=%s",
				segs[i-1].End.Format(time.RFC3339), seg.Start.Format(time.RFC3339))
		}
	}
}

// TestCreateSegment_LegacyOffGridNeighbour_TransitionThenGrid: a pre-fix
// off-grid segment on disk must produce a transition segment that ends on
// the global grid, so subsequent segments self-heal back to it.
func TestCreateSegment_LegacyOffGridNeighbour_TransitionThenGrid(t *testing.T) {
	sc, tempDir, cleanup := newAlignmentTestController(t, IntervalRule{Unit: DAY, Num: 15})
	defer cleanup()

	ctx := context.Background()
	l := logger.GetLogger("test-legacy-transition")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{Database: "test-db", Stage: "cold"}
	})

	// Inject a legacy off-grid segment [2026-05-01, 2026-05-16). The new 15d
	// epoch grid puts buckets at 04/22, 05/07, 05/22, 06/06, ...; legacy
	// straddles 05/07 and ends inside [05/07, 05/22).
	legacyStart := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	legacyEnd := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	suffix := legacyStart.Format(dayFormat)
	segPath := filepath.Join(tempDir, fmt.Sprintf("seg-%s", suffix))
	require.NoError(t, os.MkdirAll(segPath, DirPerm))
	require.NoError(t, os.WriteFile(filepath.Join(segPath, metadataFilename), []byte(currentVersion), FilePerm))
	legacy, err := sc.openSegment(ctx, legacyStart, legacyEnd, segPath, suffix, sc.groupCache)
	require.NoError(t, err)
	sc.lst = append(sc.lst, legacy)
	sc.sortLst()

	// First write past legacy.End: aligned start (05/07) is inside legacy,
	// bump pushes start to 05/16 (legacy.End), but stdEnd was locked to the
	// original grid bucket end (05/22). So the transition segment is shorter
	// than 15d but still ends on the grid.
	probe := time.Date(2026, 5, 17, 6, 0, 0, 0, time.UTC)
	transition, err := sc.create(probe)
	require.NoError(t, err)
	require.NotNil(t, transition)
	defer transition.DecRef()

	gridBoundary := time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)
	assert.True(t, transition.Contains(probe.UnixNano()),
		"transition segment must cover the probe; got [%s, %s)",
		transition.Start.Format(time.RFC3339), transition.End.Format(time.RFC3339))
	assert.Equal(t, legacyEnd, transition.Start,
		"start must be bumped to legacy.End (no overlap, no misroute)")
	assert.Equal(t, gridBoundary, transition.End,
		"end must land on the next 15d-from-epoch boundary, not legacy.End+15d")
	assert.Less(t, transition.End.Sub(transition.Start), 15*24*time.Hour,
		"transition segment span is expected to be shorter than the configured SegmentInterval")

	// Second write past the transition segment: aligned start lands cleanly
	// on the next grid bucket [05/22, 06/06) and produces a full 15d segment.
	postProbe := time.Date(2026, 5, 25, 6, 0, 0, 0, time.UTC)
	gridSeg, err := sc.create(postProbe)
	require.NoError(t, err)
	require.NotNil(t, gridSeg)
	defer gridSeg.DecRef()

	assert.Equal(t, gridBoundary, gridSeg.Start,
		"first post-transition segment must start on the global grid")
	assert.Equal(t, time.Date(2026, 6, 6, 0, 0, 0, 0, time.UTC), gridSeg.End,
		"first post-transition segment must span a full 15d on the grid")
	assert.Equal(t, 15*24*time.Hour, gridSeg.End.Sub(gridSeg.Start),
		"transition self-heals: subsequent segments are full Num*Unit")
}

// TestCreateSegment_PersistedMetadataReflectsAlignedRange writes a segment
// via create() with an unaligned timestamp, then verifies that the on-disk
// metadata records the epoch-aligned start/end - the alignment must survive
// a process restart.
func TestCreateSegment_PersistedMetadataReflectsAlignedRange(t *testing.T) {
	sc, tempDir, cleanup := newAlignmentTestController(t, IntervalRule{Unit: DAY, Num: 15})
	defer cleanup()

	probe := time.Date(2026, 4, 16, 6, 30, 0, 0, time.UTC)
	expectedStart := time.Date(2026, 4, 7, 0, 0, 0, 0, time.UTC)
	expectedEnd := time.Date(2026, 4, 22, 0, 0, 0, 0, time.UTC)

	seg, err := sc.create(probe)
	require.NoError(t, err)
	require.NotNil(t, seg)
	assert.Equal(t, expectedStart, seg.Start)
	assert.Equal(t, expectedEnd, seg.End)
	seg.DecRef()

	suffix := expectedStart.Format(dayFormat)
	metadataPath := filepath.Join(tempDir, fmt.Sprintf("seg-%s", suffix), metadataFilename)
	rawMeta, readErr := os.ReadFile(metadataPath)
	require.NoError(t, readErr)
	meta, parseErr := readSegmentMeta(rawMeta)
	require.NoError(t, parseErr)
	assert.Equal(t, currentVersion, meta.Version)
	assert.Equal(t, expectedEnd.Format(time.RFC3339Nano), meta.EndTime,
		"persisted endTime must reflect the epoch-aligned bucket end, not start+15d from the probe day")
}
