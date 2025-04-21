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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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

func (m mockTSTable) TakeFileSnapshot(string) error { return nil }

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

	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,           // indexMetrics
		nil,           // metrics
		nil,           // segmentBoundaryUpdateFn
		5*time.Minute, // idleTimeout
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
	segment, err := sc.openSegment(ctx, startTime, endTime, segmentPath, suffix)
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

	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,         // indexMetrics
		nil,         // metrics
		nil,         // segmentBoundaryUpdateFn
		time.Second, // Set short idle timeout for testing
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
	segment, err := sc.openSegment(ctx, startTime, endTime, segmentPath, suffix)
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
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,         // indexMetrics
		nil,         // metrics
		nil,         // segmentBoundaryUpdateFn
		idleTimeout, // short idle timeout
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
		segment, err := sc.openSegment(ctx, startTime, startTime.Add(24*time.Hour), segmentPath, suffix)
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

	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,           // indexMetrics
		nil,           // metrics
		nil,           // segmentBoundaryUpdateFn
		5*time.Minute, // idleTimeout
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
	segment, err := sc.openSegment(ctx, startTime, endTime, segmentPath, suffix)
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
	tables := segment.Tables()
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
