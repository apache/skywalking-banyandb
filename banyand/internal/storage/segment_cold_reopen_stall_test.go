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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// These tests reproduce a production write stall observed on hot data nodes:
// every 10 minutes the liaison logged "failed to get response from publish
// local index: DeadlineExceeded" for 10-30s.
//
// The chain is:
//  1. The idle reclaimer (10-minute ticker) closes yesterday's segment, which
//     has not been written for over an hour.
//  2. The next write calls Tick, and the rotation task used to acquire every
//     segment, reopening the just-closed one. Reopening
//     loads its series index and shards, which takes seconds on a
//     real data node.
//  3. The reopen runs while segmentController's read lock is held, and every
//     write goes through segmentController.create, which takes the write lock.
//     All writes of the group stall until the reopen finishes.
//  4. incRef does not refresh lastAccessed, so the reopened segment is idle
//     again at the next reclaimer tick, and the cycle repeats forever.
//
// The fixture models the slow reopen with a reopenGate: the TSTable creator
// parks on the gate when it opens a shard of the gated segment, so a reopen
// stays in flight until the test releases it. A write queued behind such a
// reopen never completes on its own, which makes the stall checks independent
// of how fast the machine is.

// testWaitTimeout bounds every wait for something that must happen. It only
// limits how long a failing test hangs: a passing test never waits for it, and
// a write stuck behind a gated reopen never completes, however long it is.
const testWaitTimeout = 10 * time.Second

type coldReopenFixture struct {
	db      *database[*MockTSTable, any]
	coldDay time.Time
	hotDay  time.Time
	// gate holds the reopen of one chosen segment in flight (see holdReopen).
	gate      reopenGate
	coldOpens atomic.Int32
}

func newColdReopenFixture(t *testing.T) *coldReopenFixture {
	t.Helper()
	dir, defFn := test.Space(require.New(t))
	f := &coldReopenFixture{}
	f.coldDay = time.Date(2024, 5, 1, 0, 0, 0, 0, time.Local)
	f.hotDay = f.coldDay.AddDate(0, 0, 1)

	opts := TSDBOpts[*MockTSTable, any]{
		RootLease:       staticTestLease{},
		Location:        dir,
		SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
		TTL:             IntervalRule{Unit: DAY, Num: 3},
		ShardNum:        1,
		// The hot-tier default (see queue/pub.ResolveStage).
		SegmentIdleTimeout: time.Hour,
		TSTableCreator: func(_ fs.FileSystem, _ string, _ common.Position,
			_ *logger.Logger, tr timestamp.TimeRange, _, _ any,
		) (*MockTSTable, error) {
			if tr.Start.Equal(f.coldDay) {
				f.coldOpens.Add(1)
			}
			f.gate.hit(tr.Start)
			return &MockTSTable{}, nil
		},
	}
	ctx := context.Background()
	mc := timestamp.NewMockClock()
	mc.Set(f.hotDay.Add(12 * time.Hour))
	ctx = timestamp.SetClock(ctx, mc)

	tsdb, err := OpenTSDB(ctx, opts, NewServiceCache(), group)
	require.NoError(t, err)
	f.db = tsdb.(*database[*MockTSTable, any])
	t.Cleanup(func() {
		tsdb.Close()
		defFn()
	})

	// Data was written into both days, so both segments have a shard on disk.
	for _, day := range []time.Time{f.coldDay, f.hotDay} {
		f.write(t, day.Add(12*time.Hour))
	}
	// Writes have moved on to the hot day; the cold segment is no longer touched.
	f.makeColdIdle(t)
	return f
}

// write simulates one data-node write: locate the segment by the element
// timestamp, then the shard table (see stream write_index.go localIndexCallback).
func (f *coldReopenFixture) write(t *testing.T, ts time.Time) {
	t.Helper()
	seg, err := f.db.CreateSegmentIfNotExist(ts)
	require.NoError(t, err)
	defer seg.DecRef()
	_, err = seg.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)
}

func (f *coldReopenFixture) segmentAt(t *testing.T, day time.Time) *segment[*MockTSTable, any] {
	t.Helper()
	sc := f.db.segmentController
	sc.RLock()
	defer sc.RUnlock()
	for _, s := range sc.lst {
		if s.Start.Equal(day) {
			return s
		}
	}
	t.Fatalf("no segment starts at %s", day)
	return nil
}

func (f *coldReopenFixture) isOpen(t *testing.T, day time.Time) bool {
	s := f.segmentAt(t, day)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index != nil
}

// makeColdIdle runs the idle reclaimer tick after the cold segment has not been
// accessed for longer than the idle timeout.
func (f *coldReopenFixture) makeColdIdle(t *testing.T) {
	t.Helper()
	f.segmentAt(t, f.coldDay).lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	f.segmentAt(t, f.hotDay).lastAccessed.Store(time.Now().UnixNano())
	require.Equal(t, 1, f.db.segmentController.closeIdleSegments(), "the reclaimer must close the cold segment")
	require.False(t, f.isOpen(t, f.coldDay))
	f.coldOpens.Store(0)
}

// tickAsync hands a write timestamp to the rotation task, as database.Tick does
// for the first write of every 10 minutes, and reports once the task has fully
// processed it (see processRotationTick).
func (f *coldReopenFixture) tickAsync(ts time.Time) <-chan error {
	done := make(chan error, 1)
	go func() {
		// If the test already failed while this pass was stuck, cleanup closes
		// the database and its tick channel under the pending send.
		defer func() { _ = recover() }()
		processRotationTick(f.db, ts)
		done <- nil
	}()
	return done
}

// tick hands a write timestamp to the rotation task and waits until the task
// has finished processing it.
func (f *coldReopenFixture) tick(t *testing.T, ts time.Time) {
	t.Helper()
	requireFinished(t, f.tickAsync(ts), "the rotation pass")
}

// TestRotationTick_DoesNotReopenIdleClosedSegment: a write tick on the hot
// segment must not resurrect yesterday's segment that the idle reclaimer just
// closed.
func TestRotationTick_DoesNotReopenIdleClosedSegment(t *testing.T) {
	f := newColdReopenFixture(t)

	f.tick(t, f.hotDay.Add(12*time.Hour+20*time.Minute))

	require.Zero(t, f.coldOpens.Load(), "the rotation tick must not reload the cold segment's shards")
	require.False(t, f.isOpen(t, f.coldDay), "the cold segment must stay closed after a rotation tick")
}

// TestIdleReclaim_NoCloseReopenCycle simulates an hour of steady writes on the
// hot day: a write tick every 10 minutes interleaved with the 10-minute idle
// reclaimer. The cold segment must be closed once and stay closed, not be
// reopened every cycle.
func TestIdleReclaim_NoCloseReopenCycle(t *testing.T) {
	f := newColdReopenFixture(t)
	sc := f.db.segmentController

	ts := f.hotDay.Add(12 * time.Hour)
	var reopenCycles int
	for i := 0; i < 6; i++ {
		ts = ts.Add(10*time.Minute + time.Second)
		f.write(t, ts)
		before := f.coldOpens.Load()
		f.tick(t, ts)
		if f.coldOpens.Load() > before {
			reopenCycles++
		}
		sc.closeIdleSegments()
	}
	require.Zero(t, reopenCycles, "the cold segment was reopened by %d of 6 rotation ticks", reopenCycles)
}

// TestRotationTick_DoesNotStallHotWrites: while the rotation task handles a
// tick, writes to the hot segment must keep flowing. The cold segment's reopen
// is gated, so a rotation pass that reopens it never finishes and holds up the
// write for as long as the gate is closed.
func TestRotationTick_DoesNotStallHotWrites(t *testing.T) {
	f := newColdReopenFixture(t)
	f.holdReopen(t, f.coldDay)

	requireWithin(t, f.tickAsync(f.hotDay.Add(12*time.Hour+20*time.Minute)), "the rotation pass")
	requireWithin(t, f.writeAsync(f.hotDay.Add(12*time.Hour+21*time.Minute)), "hot write")
	require.Zero(t, f.coldOpens.Load(), "the rotation pass must not reopen the cold segment")
}

// TestColdQuery_DoesNotStallHotWrites: a dashboard query over yesterday
// legitimately reopens the cold segment. Writes to today's segment must not
// wait for that reopen.
func TestColdQuery_DoesNotStallHotWrites(t *testing.T) {
	f := newColdReopenFixture(t)
	f.holdReopen(t, f.coldDay)

	query := make(chan error, 1)
	go func() {
		tr := timestamp.NewInclusiveTimeRange(f.coldDay.Add(10*time.Hour), f.coldDay.Add(11*time.Hour))
		ss, err := f.db.SelectSegments(tr, true)
		for _, s := range ss {
			s.DecRef()
		}
		query <- err
	}()
	f.gate.waitEntered(t) // the query is now parked inside the cold reopen

	requireWithin(t, f.writeAsync(f.hotDay.Add(12*time.Hour+30*time.Minute)), "hot write")

	f.gate.unblock()
	requireFinished(t, query, "the cold query")
	require.True(t, f.isOpen(t, f.coldDay), "the query must have reopened the cold segment")
}

// reopenGate blocks the shard-table open of one segment, chosen by its start
// time, so a test can keep that segment's reopen in flight: the reopening
// goroutine is parked inside acquire and holds the segment's mutex until
// unblock is called. Only the first open after arm blocks.
type reopenGate struct {
	day     time.Time
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	armed   bool
}

func (g *reopenGate) arm(day time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.day = day
	g.armed = true
	g.entered = make(chan struct{})
	g.release = make(chan struct{})
	g.once = sync.Once{}
}

func (g *reopenGate) hit(start time.Time) {
	g.mu.Lock()
	if !g.armed || !start.Equal(g.day) {
		g.mu.Unlock()
		return
	}
	g.armed = false
	entered, release := g.entered, g.release
	g.mu.Unlock()
	close(entered)
	<-release
}

func (g *reopenGate) waitEntered(t *testing.T) {
	t.Helper()
	g.mu.Lock()
	entered := g.entered
	g.mu.Unlock()
	select {
	case <-entered:
	case <-time.After(testWaitTimeout):
		t.Fatal("the gated segment was never reopened")
	}
}

func (g *reopenGate) unblock() {
	g.mu.Lock()
	release := g.release
	g.mu.Unlock()
	if release == nil {
		return
	}
	g.once.Do(func() { close(release) })
}

// holdReopen arms the gate for the segment starting at day. The gate is
// released on cleanup so a failed test never leaves a goroutine parked on the
// segment's mutex (which would hang the database Close).
func (f *coldReopenFixture) holdReopen(t *testing.T, day time.Time) {
	t.Helper()
	f.gate.arm(day)
	t.Cleanup(f.gate.unblock)
}

// writeAsync performs one write at ts in the background and reports its result.
func (f *coldReopenFixture) writeAsync(ts time.Time) <-chan error {
	done := make(chan error, 1)
	go func() {
		seg, err := f.db.CreateSegmentIfNotExist(ts)
		if err != nil {
			done <- err
			return
		}
		_, err = seg.CreateTSTableIfNotExist(common.ShardID(0))
		// Release before reporting, so a receiver observes the release's effects
		// (refCount back to 0, a deferred delete having run).
		seg.DecRef()
		done <- err
	}()
	return done
}

// startLateWrite begins an out-of-order write into the idle-closed cold
// segment and returns once that write is parked inside the reopen.
func (f *coldReopenFixture) startLateWrite(t *testing.T) <-chan error {
	t.Helper()
	f.holdReopen(t, f.coldDay)
	done := f.writeAsync(f.coldDay.Add(13 * time.Hour))
	f.gate.waitEntered(t)
	return done
}

// requireWithin fails unless done delivers a nil error while a gated reopen is
// still in flight. Anything stuck behind that reopen never delivers, so the
// generous timeout only bounds how long a failure takes to report.
func requireWithin(t *testing.T, done <-chan error, what string) {
	t.Helper()
	select {
	case err := <-done:
		require.NoError(t, err, what)
	case <-time.After(testWaitTimeout):
		t.Fatalf("%s is stuck behind a cold-segment reopen", what)
	}
}

// requireFinished waits for a goroutine released by unblock and checks its result.
func requireFinished(t *testing.T, done <-chan error, what string) {
	t.Helper()
	select {
	case err := <-done:
		require.NoError(t, err, what)
	case <-time.After(testWaitTimeout):
		t.Fatalf("%s did not finish", what)
	}
}

// The disk monitor's PeekOldestSegmentEndTime runs while a late write is
// reopening the oldest segment. The peek must answer without waiting for the
// reopen, and writes to today's segment must keep flowing.
func TestPeekOldestSegmentEndTime_DoesNotStallHotWritesDuringColdReopen(t *testing.T) {
	f := newColdReopenFixture(t)
	late := f.startLateWrite(t)

	peeked := make(chan error, 1)
	go func() {
		end, ok := f.db.PeekOldestSegmentEndTime()
		if !ok || !end.Equal(f.hotDay) {
			peeked <- fmt.Errorf("peek reported end=%s ok=%v, want %s", end, ok, f.hotDay)
			return
		}
		peeked <- nil
	}()
	requireWithin(t, peeked, "PeekOldestSegmentEndTime")
	requireWithin(t, f.writeAsync(f.hotDay.Add(12*time.Hour+30*time.Minute)), "hot write")

	f.gate.unblock()
	requireFinished(t, late, "late write")
}

// Disk-pressure cleanup evicts the oldest segment while a late write is
// reopening it. The eviction waits for that reopen outside the controller
// lock, so writes to today's segment keep flowing; the directory goes away on
// the writer's last DecRef.
func TestDeleteOldestSegment_DoesNotStallHotWritesDuringColdReopen(t *testing.T) {
	f := newColdReopenFixture(t)
	sc := f.db.segmentController
	cold := f.segmentAt(t, f.coldDay)
	late := f.startLateWrite(t)

	removed := make(chan error, 1)
	go func() {
		ok, err := f.db.DeleteOldestSegment()
		if err == nil && !ok {
			err = fmt.Errorf("the oldest segment was not removed")
		}
		removed <- err
	}()
	require.Eventually(t, func() bool { return len(sc.copySegments()) == 1 },
		testWaitTimeout, time.Millisecond, "forced cleanup must drop the oldest segment from the list")
	requireWithin(t, f.writeAsync(f.hotDay.Add(12*time.Hour+30*time.Minute)), "hot write")

	f.gate.unblock()
	requireFinished(t, removed, "DeleteOldestSegment")
	requireFinished(t, late, "late write")
	require.True(t, cold.deleted.Load())
	require.NoDirExists(t, cold.location)
}

// A query reopening the cold segment must not block the creation of a new
// segment (the write path's segmentController.create takes the controller's
// write lock).
func TestCreateSegment_NotBlockedByInflightQueryReopen(t *testing.T) {
	f := newColdReopenFixture(t)
	f.holdReopen(t, f.coldDay)
	query := make(chan error, 1)
	go func() {
		ss, err := f.db.SelectSegments(timestamp.NewInclusiveTimeRange(f.coldDay.Add(time.Hour), f.coldDay.Add(2*time.Hour)), true)
		for _, s := range ss {
			s.DecRef()
		}
		query <- err
	}()
	f.gate.waitEntered(t)

	created := make(chan error, 1)
	go func() {
		s, err := f.db.segmentController.createSegment(f.hotDay.AddDate(0, 0, 1))
		if err == nil {
			s.DecRef()
		}
		created <- err
	}()
	requireWithin(t, created, "segment creation")
	select {
	case <-query:
		t.Fatal("the query must still be waiting on the cold reopen")
	default:
	}

	f.gate.unblock()
	requireFinished(t, query, "query")
	require.True(t, f.isOpen(t, f.coldDay))
}
