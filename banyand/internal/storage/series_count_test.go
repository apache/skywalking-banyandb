// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	seriesCountMetric         = "banyandb_test_series_count"
	seriesCountPreviousMetric = "banyandb_test_series_count_previous"
)

// seriesCountValue returns the series_count sample of the test group, and
// whether the series exists.
func seriesCountValue(t *testing.T, reg *prometheus.Registry) (float64, bool) {
	t.Helper()
	return gaugeValue(t, reg, seriesCountMetric)
}

// seriesCountPreviousValue returns the series_count_previous sample of the
// test group, and whether the series exists.
func seriesCountPreviousValue(t *testing.T, reg *prometheus.Registry) (float64, bool) {
	t.Helper()
	return gaugeValue(t, reg, seriesCountPreviousMetric)
}

func gaugeValue(t *testing.T, reg *prometheus.Registry, name string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		require.Len(t, family.GetMetric(), 1)
		return family.GetMetric()[0].GetGauge().GetValue(), true
	}
	return 0, false
}

func openSeriesCountTSDB(t *testing.T, ts time.Time) (*database[*MockTSTable, any], *prometheus.Registry) {
	t.Helper()
	logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})
	dir, defFn := test.Space(require.New(t))
	t.Cleanup(defFn)

	reg := prometheus.NewRegistry()
	scope := meter.NewHierarchicalScope("banyandb", "_").SubScope("test").
		ConstLabels(meter.ToLabelPairs(common.DBLabelNames(), []string{group}))
	opts := TSDBOpts[*MockTSTable, any]{
		RootLease:             staticTestLease{},
		Location:              dir,
		SegmentInterval:       IntervalRule{Unit: DAY, Num: 1},
		TTL:                   IntervalRule{Unit: DAY, Num: 3},
		ShardNum:              1,
		TSTableCreator:        MockTSTableCreator,
		DisableRetention:      true,
		DisableRotation:       true,
		StorageMetricsFactory: obsservice.NewFactory(prom.NewProvider(scope, reg), nil, nil),
	}
	mc := timestamp.NewMockClock()
	mc.Set(ts)
	ctx := timestamp.SetClock(context.Background(), mc)
	tsdb, err := OpenTSDB(ctx, opts, NewServiceCache(), group)
	require.NoError(t, err)
	return tsdb.(*database[*MockTSTable, any]), reg
}

func insertSeries(t *testing.T, db *database[*MockTSTable, any], at time.Time, subject string, n int) {
	t.Helper()
	seg, err := db.CreateSegmentIfNotExist(at)
	require.NoError(t, err)
	defer seg.DecRef()
	require.NoError(t, seg.IndexDB().Insert(buildTestSeriesDocsNamed(t, subject, n)))
}

func TestSeriesCountReportsCurrentSegment(t *testing.T) {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 10:00:00", time.Local)
	require.NoError(t, err)
	db, reg := openSeriesCountTSDB(t, ts)
	defer func() { require.NoError(t, db.Close()) }()

	insertSeries(t, db, ts, "current", 5)
	db.latestTickTime.Store(ts.UnixNano())

	require.Eventually(t, func() bool {
		db.collect()
		value, ok := seriesCountValue(t, reg)
		return ok && value == 5
	}, 5*time.Second, 20*time.Millisecond)
}

func TestSeriesCountIgnoresPrecreatedSegment(t *testing.T) {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 23:30:00", time.Local)
	require.NoError(t, err)
	db, reg := openSeriesCountTSDB(t, ts)
	defer func() { require.NoError(t, db.Close()) }()

	insertSeries(t, db, ts, "current", 5)
	next := ts.Add(time.Hour)
	insertSeries(t, db, next, "next", 2)
	db.latestTickTime.Store(ts.UnixNano())

	require.Eventually(t, func() bool {
		db.collect()
		value, ok := seriesCountValue(t, reg)
		return ok && value == 5
	}, 5*time.Second, 20*time.Millisecond, "the segment created ahead of time must not be reported")

	db.latestTickTime.Store(next.UnixNano())
	require.Eventually(t, func() bool {
		db.collect()
		value, ok := seriesCountValue(t, reg)
		return ok && value == 2
	}, 5*time.Second, 20*time.Millisecond, "the gauge follows the tick into the next segment")
}

func TestSeriesCountIsZeroWithoutCurrentSegment(t *testing.T) {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 10:00:00", time.Local)
	require.NoError(t, err)
	db, reg := openSeriesCountTSDB(t, ts)
	defer func() { require.NoError(t, db.Close()) }()

	insertSeries(t, db, ts, "current", 3)
	db.latestTickTime.Store(ts.Add(72 * time.Hour).UnixNano())
	db.collect()

	value, ok := seriesCountValue(t, reg)
	require.True(t, ok)
	require.Zero(t, value)
}

func TestSeriesCountIsZeroForClosedSegment(t *testing.T) {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 10:00:00", time.Local)
	require.NoError(t, err)
	db, reg := openSeriesCountTSDB(t, ts)
	defer func() { require.NoError(t, db.Close()) }()

	insertSeries(t, db, ts, "current", 3)
	db.latestTickTime.Store(ts.UnixNano())
	for _, seg := range db.segmentController.copySegments() {
		seg.mu.Lock()
		seg.closeResourcesLocked()
		seg.mu.Unlock()
	}
	db.collect()

	value, ok := seriesCountValue(t, reg)
	require.True(t, ok)
	require.Zero(t, value, "a closed segment is not reopened to read its series")
	for _, seg := range db.segmentController.copySegments() {
		require.Nil(t, seg.index)
	}
}

func TestSeriesCountRemovedOnClose(t *testing.T) {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 10:00:00", time.Local)
	require.NoError(t, err)
	db, reg := openSeriesCountTSDB(t, ts)

	insertSeries(t, db, ts, "current", 3)
	db.latestTickTime.Store(ts.UnixNano())
	db.collect()
	_, ok := seriesCountValue(t, reg)
	require.True(t, ok)

	require.NoError(t, db.Close())
	_, ok = seriesCountValue(t, reg)
	require.False(t, ok, "closing the database must remove the series")
}

func closeAllSegments(db *database[*MockTSTable, any]) {
	for _, seg := range db.segmentController.copySegments() {
		seg.mu.Lock()
		seg.closeResourcesLocked()
		seg.mu.Unlock()
	}
}

func TestSeriesCountPreviousReportsPreviousSegment(t *testing.T) {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-02 10:00:00", time.Local)
	require.NoError(t, err)
	db, reg := openSeriesCountTSDB(t, ts)
	defer func() { require.NoError(t, db.Close()) }()

	insertSeries(t, db, ts.Add(-24*time.Hour), "previous", 3)
	insertSeries(t, db, ts, "current", 5)
	db.latestTickTime.Store(ts.UnixNano())

	require.Eventually(t, func() bool {
		db.collect()
		current, _ := seriesCountValue(t, reg)
		previous, ok := seriesCountPreviousValue(t, reg)
		return ok && current == 5 && previous == 3
	}, 5*time.Second, 20*time.Millisecond)
}

func TestSeriesCountPreviousSkipsGapAndPrecreatedSegment(t *testing.T) {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-05 23:30:00", time.Local)
	require.NoError(t, err)
	db, reg := openSeriesCountTSDB(t, ts)
	defer func() { require.NoError(t, db.Close()) }()

	insertSeries(t, db, ts.Add(-72*time.Hour), "previous", 3)
	insertSeries(t, db, ts, "current", 5)
	insertSeries(t, db, ts.Add(time.Hour), "next", 7)
	db.latestTickTime.Store(ts.UnixNano())

	require.Eventually(t, func() bool {
		db.collect()
		previous, ok := seriesCountPreviousValue(t, reg)
		return ok && previous == 3
	}, 5*time.Second, 20*time.Millisecond, "the previous segment is the one right before the current, not the pre-created next")

	db.latestTickTime.Store(ts.Add(time.Hour).UnixNano())
	require.Eventually(t, func() bool {
		db.collect()
		previous, _ := seriesCountPreviousValue(t, reg)
		return previous == 5
	}, 5*time.Second, 20*time.Millisecond, "after the tick enters the next segment the previous one is the old current")
}

func TestSeriesCountPreviousReadsClosedSegmentWithoutReopen(t *testing.T) {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-02 10:00:00", time.Local)
	require.NoError(t, err)
	db, reg := openSeriesCountTSDB(t, ts)
	defer func() { require.NoError(t, db.Close()) }()

	insertSeries(t, db, ts.Add(-24*time.Hour), "previous", 3)
	insertSeries(t, db, ts, "current", 5)
	db.latestTickTime.Store(ts.UnixNano())
	closeAllSegments(db)

	for range 2 {
		db.collect()
		previous, ok := seriesCountPreviousValue(t, reg)
		require.True(t, ok)
		require.EqualValues(t, 3, previous)
	}
	for _, seg := range db.segmentController.copySegments() {
		require.Nil(t, seg.index, "a closed segment is not reopened to read its series")
	}
}

func TestSeriesCountPreviousFollowsLateWrites(t *testing.T) {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-02 10:00:00", time.Local)
	require.NoError(t, err)
	db, reg := openSeriesCountTSDB(t, ts)
	defer func() { require.NoError(t, db.Close()) }()

	insertSeries(t, db, ts.Add(-24*time.Hour), "previous", 3)
	insertSeries(t, db, ts, "current", 5)
	db.latestTickTime.Store(ts.UnixNano())
	closeAllSegments(db)
	db.collect()
	previous, _ := seriesCountPreviousValue(t, reg)
	require.EqualValues(t, 3, previous)

	// A back-fill reopens the previous segment and adds series; the segment is
	// then closed again before the next collection.
	insertSeries(t, db, ts.Add(-24*time.Hour), "late", 4)
	db.collect()
	previous, _ = seriesCountPreviousValue(t, reg)
	require.EqualValues(t, 7, previous, "an open segment is read live")
	closeAllSegments(db)
	db.collect()
	previous, _ = seriesCountPreviousValue(t, reg)
	require.EqualValues(t, 7, previous, "a segment closed after new writes is read again")
}

func TestSeriesCountPreviousIsZeroWithoutPreviousSegment(t *testing.T) {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-02 10:00:00", time.Local)
	require.NoError(t, err)
	db, reg := openSeriesCountTSDB(t, ts)
	defer func() { require.NoError(t, db.Close()) }()

	insertSeries(t, db, ts, "current", 5)
	db.latestTickTime.Store(ts.UnixNano())
	db.collect()
	previous, ok := seriesCountPreviousValue(t, reg)
	require.True(t, ok)
	require.Zero(t, previous)

	db.latestTickTime.Store(ts.Add(72 * time.Hour).UnixNano())
	db.collect()
	previous, _ = seriesCountPreviousValue(t, reg)
	require.Zero(t, previous, "no current segment")
}

func TestSeriesCountPreviousSurvivesRestart(t *testing.T) {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-02 10:00:00", time.Local)
	require.NoError(t, err)
	logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})
	dir, defFn := test.Space(require.New(t))
	t.Cleanup(defFn)

	open := func() (*database[*MockTSTable, any], *prometheus.Registry) {
		reg := prometheus.NewRegistry()
		scope := meter.NewHierarchicalScope("banyandb", "_").SubScope("test").
			ConstLabels(meter.ToLabelPairs(common.DBLabelNames(), []string{group}))
		mc := timestamp.NewMockClock()
		mc.Set(ts)
		tsdb, openErr := OpenTSDB(timestamp.SetClock(context.Background(), mc), TSDBOpts[*MockTSTable, any]{
			RootLease:             staticTestLease{},
			Location:              dir,
			SegmentInterval:       IntervalRule{Unit: DAY, Num: 1},
			TTL:                   IntervalRule{Unit: DAY, Num: 3},
			ShardNum:              1,
			TSTableCreator:        MockTSTableCreator,
			DisableRetention:      true,
			DisableRotation:       true,
			StorageMetricsFactory: obsservice.NewFactory(prom.NewProvider(scope, reg), nil, nil),
		}, NewServiceCache(), group)
		require.NoError(t, openErr)
		return tsdb.(*database[*MockTSTable, any]), reg
	}

	db, _ := open()
	insertSeries(t, db, ts.Add(-24*time.Hour), "previous", 3)
	insertSeries(t, db, ts, "current", 5)
	require.NoError(t, db.Close())

	db, reg := open()
	defer func() { require.NoError(t, db.Close()) }()
	require.Zero(t, db.latestTickTime.Load())
	db.collect()
	previous, ok := seriesCountPreviousValue(t, reg)
	require.True(t, ok)
	require.Zero(t, previous, "no tick yet, so there is no current segment")

	db.latestTickTime.Store(ts.UnixNano())
	require.Eventually(t, func() bool {
		db.collect()
		previous, _ = seriesCountPreviousValue(t, reg)
		return previous == 3
	}, 5*time.Second, 20*time.Millisecond)
}

func TestSeriesCountPreviousRemovedOnClose(t *testing.T) {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-02 10:00:00", time.Local)
	require.NoError(t, err)
	db, reg := openSeriesCountTSDB(t, ts)

	insertSeries(t, db, ts.Add(-24*time.Hour), "previous", 3)
	insertSeries(t, db, ts, "current", 5)
	db.latestTickTime.Store(ts.UnixNano())
	db.collect()
	_, ok := seriesCountPreviousValue(t, reg)
	require.True(t, ok)

	require.NoError(t, db.Close())
	_, ok = seriesCountPreviousValue(t, reg)
	require.False(t, ok, "closing the database must remove the series")
}
