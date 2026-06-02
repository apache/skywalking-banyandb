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
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// These tests pin the two opposing invariants that any fix for the cold-node
// snapshot panic / inspect flock must satisfy simultaneously:
//
//   1. An actively-held segment (one a caller obtained a live reference to,
//      e.g. an in-flight snapshot or query) must NEVER be reclaimed by the
//      idle reclaimer or torn down by retention. Violating this is the root
//      cause of the production nil-index panic in the snapshot path and the
//      "exclusive lock" flock churn.
//
//   2. A genuinely idle segment with no active reference must STILL be
//      reclaimed so its bluge index writer is released -- the leak PR #1128
//      (https://github.com/apache/skywalking-banyandb/pull/1128) fixed by
//      removing the lastAccessed refresh from incRef.
//
// The "Keeps" / "Delete" tests reproduce the current bug (they FAIL on the
// unfixed tree). The "Reclaims" / "DoesNotRefreshLastAccessed" tests guard the
// PR #1128 behavior (they PASS on the unfixed tree and must keep passing).

// newReclaimTestController builds a segment controller with the given idle
// timeout, mirroring the setup shared across segment_test.go.
func newReclaimTestController(
	t *testing.T, tempDir string, idleTimeout time.Duration,
) (*segmentController[mockTSTable, mockTSTableOpener], context.Context) {
	t.Helper()
	l := logger.GetLogger("test-idle-reclaim")
	ctx := context.WithValue(context.Background(), logger.ContextKey, l)
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
		ctx,
		tempDir,
		l,
		opts,
		nil, // indexMetrics
		nil, // metrics
		idleTimeout,
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache,
		group,
	)
	return sc, ctx
}

// openReclaimTestSegment opens a single day segment and registers it with the
// controller's segment list, matching the helper inlined in segment_test.go.
func openReclaimTestSegment(
	t *testing.T, sc *segmentController[mockTSTable, mockTSTableOpener], ctx context.Context, tempDir string, start time.Time,
) *segment[mockTSTable, mockTSTableOpener] {
	t.Helper()
	segmentPath := filepath.Join(tempDir, "segment-"+start.Format(dayFormat))
	require.NoError(t, os.MkdirAll(segmentPath, DirPerm))
	require.NoError(t, os.WriteFile(filepath.Join(segmentPath, metadataFilename), []byte(currentVersion), FilePerm))
	seg, err := sc.openSegment(ctx, start, start.Add(24*time.Hour), segmentPath, start.Format(dayFormat), sc.groupCache)
	require.NoError(t, err)
	sc.Lock()
	sc.lst = append(sc.lst, seg)
	sc.sortLst()
	sc.Unlock()
	return seg
}

func reclaimTestDay(t *testing.T) time.Time {
	t.Helper()
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
}

// TestCloseIdleSegments_KeepsActivelyHeldReopenedSegment reproduces the cold
// node snapshot panic. A snapshot reopens an idle-closed cold segment through
// segments(ctx,true) (the call in the snapshot path) and holds the reference
// for the duration of the snapshot. Because that reopen yields refCount==1 --
// which the old reclaimer could not distinguish from an idle segment -- and
// segments(ctx,true) does not refresh lastAccessed, the idle reclaimer reclaims
// the segment mid-snapshot and sets index=nil. The held caller then dereferences
// seg.index.store and panics.
//
// Invariant under test (model-agnostic): while a live reference is held, the
// segment's index must stay open. FAILS on the unfixed tree.
func TestCloseIdleSegments_KeepsActivelyHeldReopenedSegment(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, 100*time.Millisecond)

	seg := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))

	// 1) Drive the segment into the idle-closed state every cold segment lives
	// in: stale lastAccessed -> reclaimed -> index writer released.
	seg.lastAccessed.Store(time.Now().Add(-time.Hour).UnixNano())
	require.Equal(t, 1, sc.closeIdleSegments())
	require.Nil(t, seg.index, "precondition: the cold segment must start idle-closed")

	// 2) The snapshot path reopens every segment via segments(ctx,true) and
	// holds the references until the snapshot finishes (see
	// (*database).TakeFileSnapshot).
	held, err := sc.segments(ctx, true)
	require.NoError(t, err)
	require.Len(t, held, 1)
	defer func() {
		for _, s := range held {
			s.DecRef()
		}
	}()
	require.NotNil(t, seg.index, "the snapshot path must reopen the cold segment")

	// 3) The 10-minute idle reclaimer tick fires while the snapshot is still
	// holding the segment. (lastAccessed is still stale because
	// segments(ctx,true)/incRef intentionally does not refresh it.)
	sc.closeIdleSegments()

	// 4) The held segment MUST still be open. On the unfixed tree it has been
	// reclaimed (index==nil); the next seg.index.store access -- e.g. the
	// hard-link in the snapshot path -- then panics with a nil pointer dereference.
	require.NotNil(t, seg.index,
		"the idle reclaimer must not close a segment that the snapshot path is actively holding")
}

// TestDelete_KeepsIndexWhileActivelyHeld reproduces the second face of the same
// defect: before the fix, retention's delete() tore the index down (and removed
// the segment directory) even while a caller held a live reference, because the
// teardown bypassed the refCount>0 guard once mustBeDeleted was set. The holder
// was left with a use-after-free. Under the new model delete() defers to the
// last DecRef while the segment is held.
//
// This is an isolated, deterministic reproduction of the segment-level defect;
// in production it is hit as a race between retention and an in-flight reopen
// on a cold node. FAILS on the unfixed tree.
func TestDelete_KeepsIndexWhileActivelyHeld(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, 100*time.Millisecond)

	seg := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))

	// Reduce to the cold-segment shape where a reopen yields the lone reference
	// (refCount==1): idle-close, then reopen as an active holder.
	seg.lastAccessed.Store(time.Now().Add(-time.Hour).UnixNano())
	require.Equal(t, 1, sc.closeIdleSegments())
	require.Nil(t, seg.index)

	require.NoError(t, seg.incRef(ctx)) // active holder, e.g. an in-flight query/snapshot
	require.NotNil(t, seg.index)

	// Retention marks the segment for deletion while the holder is still using
	// it.
	seg.delete()

	require.NotNil(t, seg.index,
		"delete() must not close the index while a caller still holds a reference")
	require.DirExists(t, seg.location,
		"delete() must not remove the segment directory while a caller still holds a reference")

	// Once the holder releases its reference, deletion proceeds.
	seg.DecRef()
	require.Nil(t, seg.index, "the index must be released once the last reference is dropped")
	require.NoDirExists(t, seg.location, "the directory must be removed once the last reference is dropped")
}

// TestCloseIdleSegments_ReclaimsTrulyIdleSegment guards the PR #1128 goal: a
// segment that is open but has no active reference and has gone idle must be
// reclaimed so its bluge index writer is released. Passes on the unfixed tree
// and must keep passing after the fix.
func TestCloseIdleSegments_ReclaimsTrulyIdleSegment(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, 100*time.Millisecond)

	seg := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))
	require.NotNil(t, seg.index)

	// No extra reference is held; only the segment's own open state keeps it
	// alive. Make it idle.
	seg.lastAccessed.Store(time.Now().Add(-time.Hour).UnixNano())

	require.Equal(t, 1, sc.closeIdleSegments(), "an idle, unreferenced segment must be reclaimed")
	require.Nil(t, seg.index, "a reclaimed segment must release its index writer (PR #1128)")
}

// TestIncRef_DoesNotRefreshLastAccessed pins the invariant PR #1128 established:
// incRef -- used by the rotation housekeeping scan that touches every segment
// via segments(ctx,true) on each tick -- must NOT refresh lastAccessed. If it
// did, no segment outside the active write window would ever look idle and the
// reclaimer could never release their bluge writers, reintroducing the exact
// leak #1128 fixed.
//
// This guards against the tempting-but-wrong fix of re-adding
// lastAccessed.Store(now) inside incRef to paper over the reclaim race.
func TestIncRef_DoesNotRefreshLastAccessed(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, 100*time.Millisecond)

	seg := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))

	stale := time.Now().Add(-time.Hour).UnixNano()
	seg.lastAccessed.Store(stale)

	require.NoError(t, seg.incRef(ctx))
	defer seg.DecRef()

	require.Equal(t, stale, seg.lastAccessed.Load(),
		"incRef must not refresh lastAccessed (would defeat the idle reclaimer and reintroduce PR #1128's writer leak)")
}

// A freshly opened segment starts dormant -- open (index!=nil) with no
// active reference (refCount==0).
func TestOpenSegment_StartsDormant(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour)

	seg := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))
	require.NotNil(t, seg.index, "a freshly opened segment is open")
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount), "a freshly opened segment has no active reference")
}

// A dormant but freshly-accessed segment (refCount==0, lastAccessed recent)
// must NOT be reclaimed by the idle reclaimer.
func TestCloseIdleSegments_KeepsFreshDormantSegment(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour)

	seg := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount))

	require.Equal(t, 0, sc.closeIdleSegments(), "a fresh dormant segment must not be reclaimed")
	require.NotNil(t, seg.index, "a fresh dormant segment must stay open")
}

// DecRef to zero leaves the segment dormant (open) and reusable; a
// subsequent incRef reuses it without reopening.
func TestDecRef_ToZeroLeavesDormantAndReusable(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour)

	seg := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))
	require.NoError(t, seg.incRef(ctx))
	require.Equal(t, int32(1), atomic.LoadInt32(&seg.refCount))

	seg.DecRef()
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount))
	require.NotNil(t, seg.index, "DecRef to zero keeps the segment open (dormant)")

	require.NoError(t, seg.incRef(ctx))
	require.Equal(t, int32(1), atomic.LoadInt32(&seg.refCount), "incRef reuses the dormant segment")
	require.NotNil(t, seg.index)
	seg.DecRef()
}

// A reclaimed (idle-closed) segment reopens on the next incRef.
func TestReclaimedSegment_ReopensOnIncRef(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, 100*time.Millisecond)

	seg := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))
	seg.lastAccessed.Store(time.Now().Add(-time.Hour).UnixNano())
	require.Equal(t, 1, sc.closeIdleSegments())
	require.Nil(t, seg.index)

	require.NoError(t, seg.incRef(ctx))
	require.NotNil(t, seg.index, "a reclaimed segment reopens on incRef")
	require.Equal(t, int32(1), atomic.LoadInt32(&seg.refCount))
	seg.DecRef()
}

// Deleting a dormant segment (no active reference) closes it and removes its
// directory immediately.
func TestDelete_DormantSegmentDeletedImmediately(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour)

	seg := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount))

	seg.delete()
	require.Nil(t, seg.index, "deleting a dormant segment closes it immediately")
	require.NoDirExists(t, seg.location, "deleting a dormant segment removes its directory immediately")
}

// deleteExpiredSegments defers the actual delete of a segment that a caller
// is actively holding until the last reference is dropped.
func TestDeleteExpiredSegments_DefersWhileHeld(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour)

	seg := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))
	require.NoError(t, seg.incRef(ctx)) // an in-flight query/snapshot holds it

	require.Equal(t, int64(1), sc.deleteExpiredSegments([]string{seg.suffix}))
	require.NotNil(t, seg.index, "deletion must be deferred while a reference is held")
	require.DirExists(t, seg.location, "the directory must survive while a reference is held")

	seg.DecRef()
	require.Nil(t, seg.index, "the index is released once the last reference is dropped")
	require.NoDirExists(t, seg.location, "the directory is removed once the last reference is dropped")
}

// Many open/reclaim cycles must not leak the bluge writer lock -- every
// reopen (which acquires the exclusive directory lock) must keep succeeding.
func TestReopenCycles_NoWriterLeak(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, 100*time.Millisecond)

	seg := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))
	for i := 0; i < 8; i++ {
		require.NoError(t, seg.incRef(ctx), "round %d: reopen must succeed (writer lock not leaked)", i)
		require.NotNil(t, seg.index)
		seg.DecRef()
		seg.lastAccessed.Store(time.Now().Add(-time.Hour).UnixNano())
		require.True(t, seg.closeIfIdle(time.Now().UnixNano()), "round %d: reclaim", i)
		require.Nil(t, seg.index)
	}
}

// Under concurrent incRef/DecRef racing a tight closeIdleSegments
// loop, an acquired reference must ALWAYS observe an open index (the reclaimer
// never steals an active reference / leaves refCount>=1 with index==nil), no
// goroutine panics, and the terminal refCount is conserved at 0. Run with -race.
func TestConcurrent_IncRefVsCloseIdle_NoStolenRefOrPanic(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	// Nanosecond idle timeout: the reclaimer considers the segment idle on every
	// tick, maximizing the race against acquirers.
	sc, ctx := newReclaimTestController(t, tempDir, time.Nanosecond)
	defer sc.close()
	seg := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))

	// A fixed per-acquirer cycle count (rather than a wall-clock duration) keeps
	// the race deterministic across machines: every run exercises the same number
	// of incRef/DecRef vs closeIdleSegments interleavings, so a slow or single-core
	// CPU only makes the test slower -- it never makes it pass vacuously or fail.
	const (
		acquirers    = 8
		cyclesPerAcq = 2000
	)
	var (
		acquirersWG sync.WaitGroup
		reclaimerWG sync.WaitGroup
		reclaimDone = make(chan struct{})
		violations  int64
		panics      int64
	)
	reclaimerWG.Add(1)
	go func() {
		defer reclaimerWG.Done()
		defer func() {
			if r := recover(); r != nil {
				atomic.AddInt64(&panics, 1)
			}
		}()
		for {
			select {
			case <-reclaimDone:
				return
			default:
			}
			sc.closeIdleSegments()
		}
	}()
	for i := 0; i < acquirers; i++ {
		acquirersWG.Add(1)
		go func() {
			defer acquirersWG.Done()
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt64(&panics, 1)
				}
			}()
			for c := 0; c < cyclesPerAcq; c++ {
				if err := seg.incRef(ctx); err != nil {
					atomic.AddInt64(&violations, 1)
					return
				}
				// Invariant: a held reference implies an open index.
				seg.mu.RLock()
				if seg.index == nil {
					atomic.AddInt64(&violations, 1)
				}
				seg.mu.RUnlock()
				seg.DecRef()
			}
		}()
	}
	acquirersWG.Wait()
	close(reclaimDone)
	reclaimerWG.Wait()

	require.Equal(t, int64(0), atomic.LoadInt64(&panics), "no goroutine may panic")
	require.Equal(t, int64(0), atomic.LoadInt64(&violations),
		"a held reference must always see an open index (no stolen ref / nil index)")
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount), "terminal refCount must be conserved at 0")
}

// The idle reclaimer must refuse to close a segment flagged for deletion,
// so it never races performDelete's directory removal. closeIfIdle leaves such a
// segment for delete()/performDelete to tear down.
func TestCloseIfIdle_RefusesWhenMustBeDeleted(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour)

	seg := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))
	atomic.StoreUint32(&seg.mustBeDeleted, 1)
	seg.lastAccessed.Store(time.Now().Add(-time.Hour).UnixNano()) // also idle

	require.False(t, seg.closeIfIdle(time.Now().UnixNano()),
		"closeIfIdle must refuse a segment flagged for deletion")
	require.NotNil(t, seg.index, "a flagged segment is left for performDelete, not idle-closed")
}

// Once a dormant segment is deleted (its directory removed by
// performDelete), incRef must NOT reopen it -- the acquire slow path returns
// ErrSegmentClosed rather than opening resources on the now-missing directory.
func TestIncRef_AfterDelete_ReturnsErrSegmentClosed(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour)

	seg := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))
	seg.delete()
	require.Nil(t, seg.index)
	require.NoDirExists(t, seg.location)

	require.ErrorIs(t, seg.incRef(ctx), ErrSegmentClosed,
		"incRef must refuse to reopen a deleted segment")
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount), "a refused acquire adds no reference")
}

// A DecRef on a segment that is already closed (refCount==0, index==nil),
// e.g. a stale release from a caller that obtained the segment before the idle
// reclaimer closed it, is a harmless no-op and never drives refCount negative.
func TestDecRef_OnClosedSegment_IsNoOp(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour)

	seg := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))
	seg.lastAccessed.Store(time.Now().Add(-time.Hour).UnixNano())
	require.True(t, seg.closeIfIdle(time.Now().UnixNano()))
	require.Nil(t, seg.index)

	seg.DecRef()
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount), "DecRef on a closed segment must not underflow")
	require.Nil(t, seg.index, "DecRef must not resurrect a closed segment")
}

// peekOldestSegmentEndTime reports the oldest segment's end time only while
// that segment is open (index loaded), preserving the pre-dormant-model behavior
// that gated on refCount>0. An idle-closed oldest segment is reported as absent.
func TestPeekOldestSegmentEndTime_OnlyWhileOpen(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour)

	// Keep-one rule: with no segment, or a single segment, peek reports absent.
	_, ok := sc.peekOldestSegmentEndTime()
	require.False(t, ok, "no segments -> not reported")

	day1 := reclaimTestDay(t)
	oldest := openReclaimTestSegment(t, sc, ctx, tempDir, day1)
	_, ok = sc.peekOldestSegmentEndTime()
	require.False(t, ok, "a single segment is not reported (keep-one rule)")

	openReclaimTestSegment(t, sc, ctx, tempDir, day1.Add(24*time.Hour)) // a newer segment; peek needs len>1

	// Open (dormant) oldest: its end time is reported.
	end, ok := sc.peekOldestSegmentEndTime()
	require.True(t, ok, "an open oldest segment must be reported")
	require.Equal(t, oldest.End, end)

	// Idle-close the oldest: it must now be reported as absent.
	oldest.lastAccessed.Store(time.Now().Add(-time.Hour).UnixNano())
	require.True(t, oldest.closeIfIdle(time.Now().UnixNano()))
	require.Nil(t, oldest.index)
	_, ok = sc.peekOldestSegmentEndTime()
	require.False(t, ok, "a closed oldest segment must not be reported")

	// Reopen on access: it is reported again.
	require.NoError(t, oldest.incRef(ctx))
	end, ok = sc.peekOldestSegmentEndTime()
	require.True(t, ok, "a reopened oldest segment is reported again")
	require.Equal(t, oldest.End, end)
	oldest.DecRef()
}

// Startup: the controller's disk-scan startup path (open()) loads every
// on-disk segment in the DORMANT state -- open (index!=nil) but with refCount==0
// -- which the pre-refactor model could not express (it loaded segments with
// refCount==1). A loaded segment must be immediately usable (a real read pins
// and releases it) and reclaimable once idle.
func TestControllerOpen_LoadsSegmentsDormant(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, _ := newReclaimTestController(t, tempDir, time.Hour)

	day1 := reclaimTestDay(t)
	day2 := day1.Add(24 * time.Hour)
	for _, d := range []time.Time{day1, day2} {
		segPath := filepath.Join(tempDir, "seg-"+d.Format(dayFormat))
		require.NoError(t, os.MkdirAll(segPath, DirPerm))
		require.NoError(t, os.WriteFile(filepath.Join(segPath, metadataFilename), []byte(currentVersion), FilePerm))
	}

	require.NoError(t, sc.open())
	require.Len(t, sc.lst, 2)

	// Startup invariant: every loaded segment is open yet dormant.
	for _, s := range sc.lst {
		require.NotNil(t, s.index, "a freshly loaded segment must be open")
		require.Equal(t, int32(0), atomic.LoadInt32(&s.refCount), "a freshly loaded segment must be dormant")
	}

	// Usable: a real read reopens/pins each segment, and the matching DecRef
	// returns it to dormant (still open, refCount back to 0).
	tr := timestamp.NewInclusiveTimeRange(day1, day2.Add(24*time.Hour))
	got, err := sc.selectSegments(tr, true)
	require.NoError(t, err)
	require.Len(t, got, 2)
	for _, s := range got {
		s.DecRef()
	}
	for _, s := range sc.lst {
		require.Equal(t, int32(0), atomic.LoadInt32(&s.refCount), "back to dormant after a read/release cycle")
		require.NotNil(t, s.index, "still open after a read/release cycle")
	}

	// Reclaimable: once idle, the reclaimer closes every loaded segment.
	for _, s := range sc.lst {
		s.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	}
	require.Equal(t, 2, sc.closeIdleSegments())
	for _, s := range sc.lst {
		require.Nil(t, s.index, "a loaded segment is reclaimable to closed")
	}
}

// Shutdown: the controller's close() releases the resources of EVERY
// segment regardless of refCount (DecRef no longer closes on reaching zero), so
// it must force-close even an actively-held segment. A segment flagged for
// deletion additionally has its directory removed; the rest keep their data.
func TestControllerClose_ReleasesAllAndDeletesFlagged(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour)

	day1 := reclaimTestDay(t)
	dormant := openReclaimTestSegment(t, sc, ctx, tempDir, day1)
	held := openReclaimTestSegment(t, sc, ctx, tempDir, day1.Add(24*time.Hour))
	flagged := openReclaimTestSegment(t, sc, ctx, tempDir, day1.Add(48*time.Hour))

	require.NoError(t, held.incRef(ctx)) // an active reference survives into shutdown
	require.Equal(t, int32(1), atomic.LoadInt32(&held.refCount))
	atomic.StoreUint32(&flagged.mustBeDeleted, 1)
	dormantLoc, heldLoc, flaggedLoc := dormant.location, held.location, flagged.location

	sc.close()

	require.Empty(t, sc.lst, "controller segment list is cleared on shutdown")
	require.Nil(t, dormant.index, "dormant segment resources are released on shutdown")
	require.Nil(t, held.index, "an actively-held segment is force-closed on shutdown")
	require.Nil(t, flagged.index, "flagged segment resources are released on shutdown")

	// Only the flagged segment's directory is removed.
	require.DirExists(t, dormantLoc, "a normal segment keeps its data on disk")
	require.DirExists(t, heldLoc, "a held segment keeps its data on disk")
	require.NoDirExists(t, flaggedLoc, "a segment flagged for deletion is removed on shutdown")
}

// removeOldest (disk-pressure retention) deletes the oldest dormant segment
// outright -- closing it and removing its directory immediately via the new
// delete() path -- while keeping the rest.
func TestRemoveOldest_DeletesDormantOldestKeepsRest(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour)

	day1 := reclaimTestDay(t)
	oldest := openReclaimTestSegment(t, sc, ctx, tempDir, day1)
	newer := openReclaimTestSegment(t, sc, ctx, tempDir, day1.Add(24*time.Hour))
	oldestLoc := oldest.location

	removed, err := sc.removeOldest()
	require.NoError(t, err)
	require.True(t, removed)
	require.Len(t, sc.lst, 1)
	require.Equal(t, newer.id, sc.lst[0].id, "the newer segment is kept")
	require.Nil(t, oldest.index, "the removed oldest is closed")
	require.NoDirExists(t, oldestLoc, "a dormant oldest segment's directory is removed immediately")
}

// removeOldest honors the keep-one rule -- it never deletes the last
// remaining segment.
func TestRemoveOldest_KeepOneRule(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour)

	only := openReclaimTestSegment(t, sc, ctx, tempDir, reclaimTestDay(t))
	removed, err := sc.removeOldest()
	require.NoError(t, err)
	require.False(t, removed, "the last remaining segment must never be removed")
	require.Len(t, sc.lst, 1)
	require.NotNil(t, only.index, "the kept segment stays open")
}

// removeOldest defers the actual teardown of the oldest segment while a
// caller still holds a reference: it leaves the list immediately, but its index
// and directory survive until the last DecRef.
func TestRemoveOldest_DefersWhileOldestHeld(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour)

	day1 := reclaimTestDay(t)
	oldest := openReclaimTestSegment(t, sc, ctx, tempDir, day1)
	openReclaimTestSegment(t, sc, ctx, tempDir, day1.Add(24*time.Hour))
	require.NoError(t, oldest.incRef(ctx)) // an in-flight reader holds the oldest
	oldestLoc := oldest.location

	removed, err := sc.removeOldest()
	require.NoError(t, err)
	require.True(t, removed)
	require.Len(t, sc.lst, 1, "the oldest is dropped from the list immediately")
	require.NotNil(t, oldest.index, "deletion is deferred while a reference is held")
	require.DirExists(t, oldestLoc, "the directory survives while held")

	oldest.DecRef()
	require.Nil(t, oldest.index)
	require.NoDirExists(t, oldestLoc, "the directory is removed once the last reference drops")
}

// getExpiredSegmentsTimeRange spans exactly the segments older than the TTL
// deadline, and -- going through segments(false) -- must not leave a leaked
// reference on any scanned dormant segment.
func TestGetExpiredSegmentsTimeRange(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour) // opts.TTL = 7 days

	today := reclaimTestDay(t)
	expired := openReclaimTestSegment(t, sc, ctx, tempDir, today.Add(-10*24*time.Hour))
	openReclaimTestSegment(t, sc, ctx, tempDir, today) // fresh, not expired

	tr := sc.getExpiredSegmentsTimeRange()
	require.Equal(t, expired.Start, tr.Start, "range starts at the expired segment")
	require.Equal(t, expired.End, tr.End, "range ends at the expired segment")

	for _, s := range sc.lst {
		require.Equal(t, int32(0), atomic.LoadInt32(&s.refCount),
			"scanning the expired range via segments(false) must not leak references")
		require.NotNil(t, s.index, "scanning must not close any segment")
	}
}
