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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// These tests cover the rotation task (database.Tick -> startRotationTask) on
// groups whose segments the idle reclaimer has closed: the pass must keep
// pre-creating the next segment without reopening any closed one.

// rotationGridBase is 2024-05-02 00:00 local time: day 19845 since the local
// epoch, which is divisible by both 3 and 7, so it lies on the 1/3/7-day grids.
// It must be local because the rotation task converts ticks with time.Unix
// (local), exactly as production writes do.
func rotationGridBase(t *testing.T) time.Time {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-02 00:00:00", time.Local)
	require.NoError(t, err)
	return ts
}

type idleRotationDBOpts struct {
	interval         IntervalRule
	disableRotation  bool
	disableRetention bool
}

func openIdleRotationDB(t *testing.T, o idleRotationDBOpts) (*database[*MockTSTable, any], func()) {
	dir, defFn := test.Space(require.New(t))
	opts := TSDBOpts[*MockTSTable, any]{
		RootLease:        staticTestLease{},
		Location:         dir,
		SegmentInterval:  o.interval,
		TTL:              IntervalRule{Unit: DAY, Num: 30},
		ShardNum:         1,
		TSTableCreator:   MockTSTableCreator,
		DisableRotation:  o.disableRotation,
		DisableRetention: o.disableRetention,
	}
	mc := timestamp.NewMockClock()
	mc.Set(rotationGridBase(t))
	ctx := timestamp.SetClock(context.Background(), mc)
	tsdb, err := OpenTSDB(ctx, opts, NewServiceCache(), group)
	require.NoError(t, err)
	return tsdb.(*database[*MockTSTable, any]), func() {
		tsdb.Close()
		defFn()
	}
}

// createIdleClosedSegment creates the segment covering ts, releases the creator's
// reference and idle-closes it, mimicking the idle reclaimer.
func createIdleClosedSegment(t *testing.T, db *database[*MockTSTable, any], ts time.Time) *segment[*MockTSTable, any] {
	s, err := db.segmentController.createSegment(ts)
	require.NoError(t, err)
	s.DecRef()
	// Age the segment past the idle timeout and close it the way the reclaimer
	// does, so the real idle check decides.
	s.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	require.True(t, s.closeIfIdle(time.Now().Add(-time.Hour).UnixNano()), "segment must be idle-closable")
	return s
}

// processRotationTick hands ts straight to the rotation goroutine (bypassing the
// Tick dedupe) and returns only after that pass has fully finished: the
// channel is unbuffered, so the second (no-op) send completes only once the
// goroutine is back in select. The barrier ts=1 is a no-op: the retention
// deadline is far in the past, no segment ends before it, and the gap to the
// latest segment is far larger than newSegmentTimeGap.
func processRotationTick(db *database[*MockTSTable, any], ts time.Time) {
	db.tsEventCh <- ts.UnixNano()
	db.tsEventCh <- 1
}

// The latest segment of a quiet group has been idle-closed; a tick in the last
// hour of it must still pre-create the next segment, without reopening any
// closed segment and without disturbing the refCount of a held one.
func TestRotationIdleSegment_PrecreatesWhenLatestIdleClosed(t *testing.T) {
	db, done := openIdleRotationDB(t, idleRotationDBOpts{interval: IntervalRule{Unit: DAY, Num: 1}})
	defer done()
	base := rotationGridBase(t)

	held, err := db.segmentController.createSegment(base.AddDate(0, 0, -2))
	require.NoError(t, err)
	defer held.DecRef()
	older := createIdleClosedSegment(t, db, base.AddDate(0, 0, -1))
	latest := createIdleClosedSegment(t, db, base)

	processRotationTick(db, latest.End.Add(-30*time.Minute))

	ss := db.segmentController.copySegments()
	require.Len(t, ss, 4, "next segment must be pre-created")
	next := ss[3]
	require.True(t, next.Start.Equal(latest.End))
	require.True(t, next.End.Equal(latest.End.AddDate(0, 0, 1)))

	require.False(t, segmentIsOpen(older), "rotation must not reopen an idle-closed segment")
	require.False(t, segmentIsOpen(latest), "rotation must not reopen the idle-closed latest segment")
	require.Equal(t, int32(0), atomic.LoadInt32(&older.refCount))
	require.Equal(t, int32(0), atomic.LoadInt32(&latest.refCount))
	require.True(t, segmentIsOpen(held))
	require.Equal(t, int32(1), atomic.LoadInt32(&held.refCount), "rotation must not change a held segment's refCount")
}

// A segment flagged for deletion (retention deleted it while it was still in
// the list, refCount==0) refuses incRef with ErrSegmentClosed. The rotation
// pass must still pre-create the next segment.
func TestRotationIdleSegment_FlaggedSegmentDoesNotAbortPass(t *testing.T) {
	db, done := openIdleRotationDB(t, idleRotationDBOpts{interval: IntervalRule{Unit: DAY, Num: 1}, disableRetention: true})
	defer done()
	base := rotationGridBase(t)

	flagged := createIdleClosedSegment(t, db, base.AddDate(0, 0, -1))
	atomic.StoreUint32(&flagged.mustBeDeleted, 1)
	require.ErrorIs(t, flagged.incRef(context.Background()), ErrSegmentClosed)
	latest := createIdleClosedSegment(t, db, base)

	processRotationTick(db, latest.End.Add(-10*time.Minute))
	require.Len(t, db.segmentController.copySegments(), 3, "pre-creation must not be aborted by a deleting segment")
}

// Multi-day (metricsDay/metricsHour style) and hour intervals: a tick within
// newSegmentTimeGap of the latest (closed) segment's end creates exactly the
// adjacent grid-aligned segment; a tick earlier than that creates nothing.
func TestRotationIdleSegment_IntervalsPrecreateAdjacentSegment(t *testing.T) {
	cases := []struct {
		name     string
		interval IntervalRule
	}{
		{"hour-1", IntervalRule{Unit: HOUR, Num: 1}},
		{"hour-4", IntervalRule{Unit: HOUR, Num: 4}},
		{"day-1", IntervalRule{Unit: DAY, Num: 1}},
		{"day-3", IntervalRule{Unit: DAY, Num: 3}},
		{"day-7", IntervalRule{Unit: DAY, Num: 7}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, done := openIdleRotationDB(t, idleRotationDBOpts{interval: tc.interval})
			defer done()
			latest := createIdleClosedSegment(t, db, rotationGridBase(t))
			require.True(t, latest.End.Equal(tc.interval.NextTime(latest.Start)))

			if tc.interval.Unit == DAY || tc.interval.Num > 1 {
				processRotationTick(db, latest.End.Add(-61*time.Minute))
				require.Len(t, db.segmentController.copySegments(), 1, "gap > newSegmentTimeGap must not pre-create")
			}

			processRotationTick(db, latest.End.Add(-59*time.Minute))
			ss := db.segmentController.copySegments()
			require.Len(t, ss, 2)
			require.True(t, ss[1].Start.Equal(latest.End), "start=%s want %s", ss[1].Start, latest.End)
			require.True(t, ss[1].End.Equal(tc.interval.NextTime(latest.End)), "end=%s", ss[1].End)
			require.False(t, segmentIsOpen(latest))

			// A second tick inside the same window is idempotent.
			processRotationTick(db, latest.End.Add(-1*time.Minute))
			require.Len(t, db.segmentController.copySegments(), 2)
		})
	}
}

// DisableRotation (lifecycle stages) and future ticks (gap<=0) never create a
// segment, and neither reopens the closed segments.
func TestRotationIdleSegment_NoCreateCases(t *testing.T) {
	t.Run("disable-rotation", func(t *testing.T) {
		db, done := openIdleRotationDB(t, idleRotationDBOpts{interval: IntervalRule{Unit: DAY, Num: 3}, disableRotation: true})
		defer done()
		latest := createIdleClosedSegment(t, db, rotationGridBase(t))
		processRotationTick(db, latest.End.Add(-time.Minute))
		require.Len(t, db.segmentController.copySegments(), 1)
		require.False(t, segmentIsOpen(latest))
	})
	t.Run("future-tick", func(t *testing.T) {
		db, done := openIdleRotationDB(t, idleRotationDBOpts{interval: IntervalRule{Unit: DAY, Num: 1}})
		defer done()
		older := createIdleClosedSegment(t, db, rotationGridBase(t).AddDate(0, 0, -1))
		latest := createIdleClosedSegment(t, db, rotationGridBase(t))
		processRotationTick(db, latest.End.Add(2*time.Hour))
		require.Len(t, db.segmentController.copySegments(), 2)
		require.False(t, segmentIsOpen(older))
		require.False(t, segmentIsOpen(latest))
	})
	t.Run("disable-retention-still-rotates", func(t *testing.T) {
		db, done := openIdleRotationDB(t, idleRotationDBOpts{interval: IntervalRule{Unit: DAY, Num: 1}, disableRetention: true})
		defer done()
		latest := createIdleClosedSegment(t, db, rotationGridBase(t))
		processRotationTick(db, latest.End.Add(-time.Minute))
		require.Len(t, db.segmentController.copySegments(), 2)
	})
}

// A cache reset on an open segment keeps it open and usable; a segment closed
// before the tick and reopened afterwards comes back with a fresh series index,
// so skipping its reset loses nothing.
func TestRotationIdleSegment_ResetIndexOpenAndReopened(t *testing.T) {
	db, done := openIdleRotationDB(t, idleRotationDBOpts{interval: IntervalRule{Unit: DAY, Num: 1}})
	defer done()
	base := rotationGridBase(t)
	open, err := db.segmentController.createSegment(base.AddDate(0, 0, -1))
	require.NoError(t, err)
	open.DecRef()
	closed := createIdleClosedSegment(t, db, base.AddDate(0, 0, -2))
	latest, err := db.segmentController.createSegment(base)
	require.NoError(t, err)
	latest.DecRef()

	processRotationTick(db, latest.End.Add(-5*time.Minute))
	require.Len(t, db.segmentController.copySegments(), 4)
	require.True(t, segmentIsOpen(open), "resetIndex must not close an open segment")
	require.False(t, segmentIsOpen(closed))

	before := closed.index
	require.Nil(t, before)
	require.NoError(t, closed.incRef(context.Background()))
	defer closed.DecRef()
	require.NotNil(t, closed.index, "reopen builds a new series index (fresh cache)")
}
