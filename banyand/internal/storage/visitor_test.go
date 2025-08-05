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

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// TestVisitor tracks visited segments and shards for testing.
type TestVisitor struct {
	seriesErrors  map[string]error
	shardErrors   map[common.ShardID]error
	visitedSeries []string
	visitedShards []struct {
		path    string
		shardID common.ShardID
	}
}

// NewTestVisitor creates a new test visitor.
func NewTestVisitor() *TestVisitor {
	return &TestVisitor{
		seriesErrors:  make(map[string]error),
		shardErrors:   make(map[common.ShardID]error),
		visitedSeries: make([]string, 0),
		visitedShards: make([]struct {
			path    string
			shardID common.ShardID
		}, 0),
	}
}

// VisitSeries implements SegmentVisitor.VisitSeries.
func (v *TestVisitor) VisitSeries(_ *timestamp.TimeRange, seriesIndexPath string, _ []common.ShardID) error {
	if err, exists := v.seriesErrors[seriesIndexPath]; exists {
		return err
	}
	v.visitedSeries = append(v.visitedSeries, seriesIndexPath)
	return nil
}

// VisitShard implements SegmentVisitor.VisitShard.
func (v *TestVisitor) VisitShard(_ *timestamp.TimeRange, shardID common.ShardID, shardPath string) error {
	if err, exists := v.shardErrors[shardID]; exists {
		return err
	}
	v.visitedShards = append(v.visitedShards, struct {
		path    string
		shardID common.ShardID
	}{path: shardPath, shardID: shardID})
	return nil
}

// SetSeriesError sets an error to be returned when visiting a specific series path.
func (v *TestVisitor) SetSeriesError(path string, err error) {
	v.seriesErrors[path] = err
}

// SetShardError sets an error to be returned when visiting a specific shard ID.
func (v *TestVisitor) SetShardError(shardID common.ShardID, err error) {
	v.shardErrors[shardID] = err
}

func TestVisitSegmentsInTimeRange(t *testing.T) {
	logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})

	t.Run("visit single segment with single shard", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		// Create TSDB with single shard
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

		// Create segment
		seg, err := tsdb.CreateSegmentIfNotExist(ts)
		require.NoError(t, err)
		defer seg.DecRef()

		// Create shard
		shard, err := seg.CreateTSTableIfNotExist(common.ShardID(0))
		require.NoError(t, err)
		require.NotNil(t, shard)

		tsdb.Close()

		// Test visitor
		visitor := NewTestVisitor()
		timeRange := timestamp.NewSectionTimeRange(ts, ts.Add(24*time.Hour))

		err = VisitSegmentsInTimeRange(dir, timeRange, visitor, opts.SegmentInterval)
		require.NoError(t, err)

		// Verify series index was visited
		require.Len(t, visitor.visitedSeries, 1)
		expectedSeriesPath := filepath.Join(dir, "seg-20240501", seriesIndexDirName)
		require.Equal(t, expectedSeriesPath, visitor.visitedSeries[0])

		// Verify shard was visited
		require.Len(t, visitor.visitedShards, 1)
		require.Equal(t, common.ShardID(0), visitor.visitedShards[0].shardID)
		expectedShardPath := filepath.Join(dir, "seg-20240501", "shard-0")
		require.Equal(t, expectedShardPath, visitor.visitedShards[0].path)
	})

	t.Run("visit multiple segments with multiple shards", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		// Create TSDB with multiple shards
		opts := TSDBOpts[*MockTSTable, any]{
			Location:        dir,
			SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
			TTL:             IntervalRule{Unit: DAY, Num: 7},
			ShardNum:        3,
			TSTableCreator:  MockTSTableCreator,
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

		// Create segments for 3 consecutive days
		segmentDates := []time.Time{
			baseDate,
			baseDate.AddDate(0, 0, 1), // 2024-05-02
			baseDate.AddDate(0, 0, 2), // 2024-05-03
		}

		for _, date := range segmentDates {
			mc.Set(date)
			seg, segErr := tsdb.CreateSegmentIfNotExist(date)
			require.NoError(t, segErr)

			// Create all shards for each segment
			for shardID := common.ShardID(0); shardID < common.ShardID(3); shardID++ {
				shard, shardErr := seg.CreateTSTableIfNotExist(shardID)
				require.NoError(t, shardErr)
				require.NotNil(t, shard)
			}
			seg.DecRef()
		}

		tsdb.Close()

		// Test visitor with time range covering all segments
		visitor := NewTestVisitor()
		timeRange := timestamp.NewSectionTimeRange(baseDate, baseDate.AddDate(0, 0, 3))

		err = VisitSegmentsInTimeRange(dir, timeRange, visitor, opts.SegmentInterval)
		require.NoError(t, err)

		// Verify all series indices were visited
		require.Len(t, visitor.visitedSeries, 3)
		expectedSeriesPaths := []string{
			filepath.Join(dir, "seg-20240501", seriesIndexDirName),
			filepath.Join(dir, "seg-20240502", seriesIndexDirName),
			filepath.Join(dir, "seg-20240503", seriesIndexDirName),
		}
		for _, expectedPath := range expectedSeriesPaths {
			require.Contains(t, visitor.visitedSeries, expectedPath)
		}

		// Verify all shards were visited (3 segments × 3 shards = 9 total)
		require.Len(t, visitor.visitedShards, 9)

		// Verify shard IDs are correct
		shardIDs := make(map[common.ShardID]int)
		for _, shard := range visitor.visitedShards {
			shardIDs[shard.shardID]++
		}
		require.Equal(t, 3, shardIDs[0]) // Each shard ID should appear 3 times (once per segment)
		require.Equal(t, 3, shardIDs[1])
		require.Equal(t, 3, shardIDs[2])
	})

	t.Run("visit segments with time range filtering", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		// Create TSDB
		opts := TSDBOpts[*MockTSTable, any]{
			Location:        dir,
			SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
			TTL:             IntervalRule{Unit: DAY, Num: 7},
			ShardNum:        2,
			TSTableCreator:  MockTSTableCreator,
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

		// Create segments for 5 consecutive days
		segmentDates := []time.Time{
			baseDate,                  // 2024-05-01
			baseDate.AddDate(0, 0, 1), // 2024-05-02
			baseDate.AddDate(0, 0, 2), // 2024-05-03
			baseDate.AddDate(0, 0, 3), // 2024-05-04
			baseDate.AddDate(0, 0, 4), // 2024-05-05
		}

		for _, date := range segmentDates {
			mc.Set(date)
			seg, segErr := tsdb.CreateSegmentIfNotExist(date)
			require.NoError(t, segErr)

			// Create shards for each segment
			for shardID := common.ShardID(0); shardID < common.ShardID(2); shardID++ {
				shard, shardErr := seg.CreateTSTableIfNotExist(shardID)
				require.NoError(t, shardErr)
				require.NotNil(t, shard)
			}
			seg.DecRef()
		}

		tsdb.Close()

		// Test visitor with time range covering only middle segments (2024-05-02 to 2024-05-04)
		visitor := NewTestVisitor()
		startTime := baseDate.AddDate(0, 0, 1) // 2024-05-02
		endTime := baseDate.AddDate(0, 0, 4)   // 2024-05-05 (exclusive)
		timeRange := timestamp.NewSectionTimeRange(startTime, endTime)

		err = VisitSegmentsInTimeRange(dir, timeRange, visitor, opts.SegmentInterval)
		require.NoError(t, err)

		// Verify only middle segments were visited (3 segments)
		require.Len(t, visitor.visitedSeries, 3)
		expectedSeriesPaths := []string{
			filepath.Join(dir, "seg-20240502", seriesIndexDirName),
			filepath.Join(dir, "seg-20240503", seriesIndexDirName),
			filepath.Join(dir, "seg-20240504", seriesIndexDirName),
		}
		for _, expectedPath := range expectedSeriesPaths {
			require.Contains(t, visitor.visitedSeries, expectedPath)
		}

		// Verify shards were visited (3 segments × 2 shards = 6 total)
		require.Len(t, visitor.visitedShards, 6)
	})

	t.Run("visit empty directory", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		visitor := NewTestVisitor()
		timeRange := timestamp.NewSectionTimeRange(time.Now(), time.Now().Add(24*time.Hour))
		intervalRule := IntervalRule{Unit: DAY, Num: 1}

		err := VisitSegmentsInTimeRange(dir, timeRange, visitor, intervalRule)
		require.NoError(t, err)

		// Verify nothing was visited
		require.Len(t, visitor.visitedSeries, 0)
		require.Len(t, visitor.visitedShards, 0)
	})

	t.Run("visit with series error", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		// Create TSDB
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

		// Create segment
		seg, err := tsdb.CreateSegmentIfNotExist(ts)
		require.NoError(t, err)
		defer seg.DecRef()

		// Create shard
		shard, err := seg.CreateTSTableIfNotExist(common.ShardID(0))
		require.NoError(t, err)
		require.NotNil(t, shard)

		tsdb.Close()

		// Test visitor with series error
		visitor := NewTestVisitor()
		expectedSeriesPath := filepath.Join(dir, "seg-20240501", seriesIndexDirName)
		visitor.SetSeriesError(expectedSeriesPath, errors.New("series access error"))

		timeRange := timestamp.NewSectionTimeRange(ts, ts.Add(24*time.Hour))

		err = VisitSegmentsInTimeRange(dir, timeRange, visitor, opts.SegmentInterval)
		require.Error(t, err)
		require.Contains(t, err.Error(), "series access error")
		require.Contains(t, err.Error(), "failed to visit series index for segment")
	})

	t.Run("visit with shard error", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		// Create TSDB
		opts := TSDBOpts[*MockTSTable, any]{
			Location:        dir,
			SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
			TTL:             IntervalRule{Unit: DAY, Num: 3},
			ShardNum:        2,
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

		// Create segment
		seg, err := tsdb.CreateSegmentIfNotExist(ts)
		require.NoError(t, err)
		defer seg.DecRef()

		// Create shards
		for shardID := common.ShardID(0); shardID < common.ShardID(2); shardID++ {
			shard, shardErr := seg.CreateTSTableIfNotExist(shardID)
			require.NoError(t, shardErr)
			require.NotNil(t, shard)
		}

		tsdb.Close()

		// Test visitor with shard error
		visitor := NewTestVisitor()
		visitor.SetShardError(common.ShardID(1), errors.New("shard access error"))

		timeRange := timestamp.NewSectionTimeRange(ts, ts.Add(24*time.Hour))

		err = VisitSegmentsInTimeRange(dir, timeRange, visitor, opts.SegmentInterval)
		require.Error(t, err)
		require.Contains(t, err.Error(), "shard access error")
		require.Contains(t, err.Error(), "failed to visit shards for segment")
	})

	t.Run("visit with hour-based segments", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		// Create TSDB with hour-based segments
		opts := TSDBOpts[*MockTSTable, any]{
			Location:        dir,
			SegmentInterval: IntervalRule{Unit: HOUR, Num: 1},
			TTL:             IntervalRule{Unit: DAY, Num: 1},
			ShardNum:        1,
			TSTableCreator:  MockTSTableCreator,
		}

		ctx := context.Background()
		mc := timestamp.NewMockClock()
		baseTime, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 10:00:00", time.Local)
		require.NoError(t, err)
		mc.Set(baseTime)
		ctx = timestamp.SetClock(ctx, mc)

		serviceCache := NewServiceCache()
		tsdb, err := OpenTSDB(ctx, opts, serviceCache, group)
		require.NoError(t, err)
		require.NotNil(t, tsdb)

		// Create segments for 3 consecutive hours
		segmentTimes := []time.Time{
			baseTime,                    // 2024-05-01 10:00:00
			baseTime.Add(1 * time.Hour), // 2024-05-01 11:00:00
			baseTime.Add(2 * time.Hour), // 2024-05-01 12:00:00
		}

		for _, segmentTime := range segmentTimes {
			mc.Set(segmentTime)
			seg, segErr := tsdb.CreateSegmentIfNotExist(segmentTime)
			require.NoError(t, segErr)

			// Create shard for each segment
			shard, shardErr := seg.CreateTSTableIfNotExist(common.ShardID(0))
			require.NoError(t, shardErr)
			require.NotNil(t, shard)
			seg.DecRef()
		}

		tsdb.Close()

		// Test visitor with time range covering all segments
		visitor := NewTestVisitor()
		timeRange := timestamp.NewSectionTimeRange(baseTime, baseTime.Add(3*time.Hour))

		err = VisitSegmentsInTimeRange(dir, timeRange, visitor, opts.SegmentInterval)
		require.NoError(t, err)

		// Verify all series indices were visited
		require.Len(t, visitor.visitedSeries, 3)
		expectedSeriesPaths := []string{
			filepath.Join(dir, "seg-2024050110", seriesIndexDirName),
			filepath.Join(dir, "seg-2024050111", seriesIndexDirName),
			filepath.Join(dir, "seg-2024050112", seriesIndexDirName),
		}
		for _, expectedPath := range expectedSeriesPaths {
			require.Contains(t, visitor.visitedSeries, expectedPath)
		}

		// Verify all shards were visited
		require.Len(t, visitor.visitedShards, 3)
		for _, shard := range visitor.visitedShards {
			require.Equal(t, common.ShardID(0), shard.shardID)
		}
	})
}
