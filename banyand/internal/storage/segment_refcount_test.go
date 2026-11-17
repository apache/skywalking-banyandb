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
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// Reference-count ownership across the paths that look at segments without
// reading them. A stats peek (SelectSegments(_, false), used by the measure /
// stream / trace metadata APIs) and the retention passes (remove,
// deleteExpiredSegments, getExpiredSegmentsTimeRange) run concurrently with
// queries and writes. None of them may release a reference that a query or a
// writer holds: that would let the idle reclaimer or a deferred retention
// delete tear the segment down under its holder.

type refSeg = segment[mockTSTable, mockTSTableOpener]

func refCountOf(s *refSeg) int32 { return atomic.LoadInt32(&s.refCount) }

func refSegIsOpen(s *refSeg) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index != nil
}

// newDormantRefSegment opens one dormant (open, unreferenced) day segment.
func newDormantRefSegment(t *testing.T) (*segmentController[mockTSTable, mockTSTableOpener], *refSeg) {
	t.Helper()
	tempDir, cleanup := setupTestEnvironment(t)
	t.Cleanup(cleanup)
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour)
	t.Cleanup(sc.close)
	return sc, openReclaimTestSegment(ctx, t, sc, tempDir, reclaimTestDay(t))
}

// A stats peek finds the segment dormant and returns it without a reference.
// A query then pins it, and the peek's caller releases its result. The query's
// reference must survive, so a retention delete that follows is deferred until
// the query is done.
func TestSelectSegmentsPeek_DecRefDoesNotStealQueryRef(t *testing.T) {
	sc, seg := newDormantRefSegment(t)
	tr := timestamp.NewInclusiveTimeRange(seg.Start.Add(time.Hour), seg.Start.Add(2*time.Hour))

	peek, err := sc.selectSegments(tr, false)
	require.NoError(t, err)
	require.Len(t, peek, 1)
	require.Zero(t, refCountOf(seg), "the peek must not pin a dormant segment")

	query, err := sc.selectSegments(tr, true)
	require.NoError(t, err)
	require.Len(t, query, 1)
	require.EqualValues(t, 1, refCountOf(seg))

	peek[0].DecRef()
	require.EqualValues(t, 1, refCountOf(seg), "the peek released the query's reference")

	seg.delete()
	require.NotNil(t, query[0].IndexDB(), "retention tore the segment down under the query")
	require.DirExists(t, seg.location)
	query[0].DecRef()
	require.True(t, seg.deleted.Load(), "the deferred delete runs on the query's last DecRef")
}

// Same interleaving with a writer: after the peek releases its result, the
// idle reclaimer must not close the segment the writer still holds.
func TestSelectSegmentsPeek_DecRefDoesNotStealWriterRef(t *testing.T) {
	sc, seg := newDormantRefSegment(t)
	tr := timestamp.NewInclusiveTimeRange(seg.Start, seg.Start.Add(23*time.Hour))

	peek, err := sc.selectSegments(tr, false)
	require.NoError(t, err)
	require.Len(t, peek, 1)

	w, err := sc.createSegment(seg.Start.Add(time.Hour))
	require.NoError(t, err)
	require.Same(t, seg, w)
	defer w.DecRef()

	for _, p := range peek {
		p.DecRef()
	}
	require.EqualValues(t, 1, refCountOf(seg), "the peek released the writer's reference")

	seg.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	require.Zero(t, sc.closeIdleSegments(), "the reclaimer must not close a held segment")
	require.NotNil(t, w.IndexDB())
}

// Retention expires a segment while a writer holds it and a stats peek holds
// an unpinned result for it. The delete is deferred to the writer: neither
// remove nor the peek's release may run it early, and the writer can keep
// using its shards until it is done.
func TestRemove_DoesNotStealHeldRef(t *testing.T) {
	sc, seg := newDormantRefSegment(t)
	tr := timestamp.NewInclusiveTimeRange(seg.Start, seg.Start.Add(23*time.Hour))

	peek, err := sc.selectSegments(tr, false)
	require.NoError(t, err)
	w, err := sc.createSegment(seg.Start.Add(time.Hour))
	require.NoError(t, err)

	has, err := sc.remove(context.Background(), seg.End.Add(time.Hour))
	require.NoError(t, err)
	require.True(t, has)
	require.EqualValues(t, 1, refCountOf(seg), "remove must not release the writer's reference")
	require.False(t, seg.deleted.Load(), "remove must defer the delete of a held segment")

	for _, p := range peek {
		p.DecRef()
	}
	require.False(t, seg.deleted.Load(), "the peek's release ran the deferred delete under the writer")
	require.DirExists(t, seg.location)
	_, err = w.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)

	w.DecRef()
	require.True(t, seg.deleted.Load())
	require.NoDirExists(t, seg.location)
	require.Zero(t, refCountOf(seg))
}

// The scheduled TTL deletion (deleteExpiredSegments) hits a segment a query
// pinned after a stats peek looked at it. The query keeps the segment open
// until it releases it.
func TestDeleteExpiredSegments_DoesNotStealHeldRef(t *testing.T) {
	sc, seg := newDormantRefSegment(t)
	tr := timestamp.NewInclusiveTimeRange(seg.Start, seg.Start.Add(23*time.Hour))

	peek, err := sc.selectSegments(tr, false)
	require.NoError(t, err)
	query, err := sc.selectSegments(tr, true)
	require.NoError(t, err)
	require.Len(t, query, 1)

	require.Equal(t, int64(1), sc.deleteExpiredSegments([]string{seg.suffix}))
	for _, p := range peek {
		p.DecRef()
	}
	require.EqualValues(t, 1, refCountOf(seg), "the query's reference was released by retention or the peek")
	require.NotNil(t, query[0].IndexDB(), "the segment was torn down under the query")
	require.DirExists(t, seg.location)

	query[0].DecRef()
	require.True(t, seg.deleted.Load())
	require.NoDirExists(t, seg.location)
}

// Computing the expired range for the snapshot/backup path scans every
// segment. It must leave a held segment's reference in place and must not
// reopen an idle-closed one.
func TestGetExpiredSegmentsTimeRange_LeavesReferencesUntouched(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	t.Cleanup(cleanup)
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour) // TTL 7 days
	t.Cleanup(sc.close)
	today := reclaimTestDay(t)
	closed := openReclaimTestSegment(ctx, t, sc, tempDir, today.Add(-20*24*time.Hour))
	held := openReclaimTestSegment(ctx, t, sc, tempDir, today.Add(-10*24*time.Hour))
	openReclaimTestSegment(ctx, t, sc, tempDir, today)
	closed.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	require.True(t, closed.closeIfIdle(time.Now().UnixNano()))
	require.NoError(t, held.incRef(ctx))
	defer held.DecRef()

	tr := sc.getExpiredSegmentsTimeRange()
	require.Equal(t, closed.Start, tr.Start)
	require.Equal(t, held.End, tr.End)
	require.EqualValues(t, 1, refCountOf(held), "the scan released the holder's reference")
	require.Zero(t, refCountOf(closed))
	require.False(t, refSegIsOpen(closed), "the scan must not reopen an idle-closed segment")
}

// gatedController holds n consecutive day segments (oldest first), each with
// one shard on disk and idle-closed. A reopen of the segment armed on gate
// parks until the gate is released, modeling a slow reopen on a data node.
type gatedController struct {
	sc   *segmentController[mockTSTable, mockTSTableOpener]
	ctx  context.Context
	segs []*refSeg
	gate reopenGate
}

func newGatedController(t *testing.T, n int) *gatedController {
	t.Helper()
	dir, cleanup := setupTestEnvironment(t)
	t.Cleanup(cleanup)
	sc, ctx := newReclaimTestController(t, dir, time.Hour)
	t.Cleanup(sc.close)
	g := &gatedController{sc: sc, ctx: ctx}
	sc.opts.TSTableCreator = func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
		tr timestamp.TimeRange, _ mockTSTableOpener, _ any,
	) (mockTSTable, error) {
		g.gate.hit(tr.Start)
		return mockTSTable{ID: common.ShardID(0)}, nil
	}
	now := time.Now()
	// Local midnight and the seg-<day> directory name, as sc.create derives
	// them, so recreating a removed segment hits the same path.
	first := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local).AddDate(0, 0, -n)
	for i := 0; i < n; i++ {
		start := first.AddDate(0, 0, i)
		suffix := sc.format(start)
		segPath := filepath.Join(dir, fmt.Sprintf(segTemplate, suffix))
		require.NoError(t, os.MkdirAll(segPath, DirPerm))
		require.NoError(t, os.WriteFile(filepath.Join(segPath, metadataFilename), []byte(currentVersion), FilePerm))
		s, err := sc.openSegment(ctx, start, start.AddDate(0, 0, 1), segPath, suffix, sc.groupCache)
		require.NoError(t, err)
		sc.Lock()
		sc.lst = append(sc.lst, s)
		sc.sortLst()
		sc.Unlock()
		_, err = s.CreateTSTableIfNotExist(common.ShardID(0))
		require.NoError(t, err)
		s.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
		g.segs = append(g.segs, s)
	}
	require.Equal(t, n, sc.closeIdleSegments())
	return g
}

type selectResult struct {
	err  error
	segs []Segment[mockTSTable, mockTSTableOpener]
}

// startQuery runs selectSegments(reopen=true) over every segment and returns
// once it is parked in the reopen of the newest one: the list snapshot is
// taken and the controller lock released, but no older segment is touched yet.
func (g *gatedController) startQuery(t *testing.T) <-chan selectResult {
	t.Helper()
	newest := g.segs[len(g.segs)-1]
	g.gate.arm(newest.Start)
	t.Cleanup(g.gate.unblock)
	out := make(chan selectResult, 1)
	go func() {
		tr := timestamp.NewInclusiveTimeRange(g.segs[0].Start, newest.End)
		segs, err := g.sc.selectSegments(tr, true)
		out <- selectResult{err: err, segs: segs}
	}()
	g.gate.waitEntered(t)
	return out
}

func (g *gatedController) finish(t *testing.T, out <-chan selectResult) selectResult {
	t.Helper()
	g.gate.unblock()
	select {
	case r := <-out:
		return r
	case <-time.After(testWaitTimeout):
		t.Fatal("selectSegments did not return")
		return selectResult{}
	}
}

// Retention removes the oldest segment while a query that already snapshotted
// the list is still reopening a newer one. Retention is not blocked by that
// reopen, and the query skips the removed segment: no error, the survivors in
// newest-first order, no reference left behind, nothing reopened on the
// removed directory.
func TestSelectSegments_SkipsSegmentRemovedAfterSnapshot(t *testing.T) {
	g := newGatedController(t, 3)
	oldest := g.segs[0]
	out := g.startQuery(t)

	removed := make(chan error, 1)
	go func() {
		_, err := g.sc.remove(g.ctx, oldest.End.Add(time.Minute))
		removed <- err
	}()
	requireWithin(t, removed, "retention")
	require.NoDirExists(t, oldest.location)

	r := g.finish(t, out)
	require.NoError(t, r.err)
	require.Len(t, r.segs, 2)
	require.Same(t, g.segs[2], r.segs[0].(*refSeg), "selectSegments returns newest first")
	require.Same(t, g.segs[1], r.segs[1].(*refSeg))
	require.False(t, refSegIsOpen(oldest), "a removed segment must not be reopened")
	require.NoDirExists(t, oldest.location, "a removed segment's directory must not be recreated")
	for _, s := range r.segs {
		s.DecRef()
	}
	for _, s := range g.segs {
		require.Zero(t, refCountOf(s), "references must be balanced for %s", s)
	}
}

// Retention removes a segment that a writer holds, after a query snapshotted
// the list. The query may still pin it through its stale snapshot; the
// directory survives until the last reference is gone, recreating the segment
// at the same path is refused with ErrSegmentDeleting meanwhile, and works
// once the deferred delete has run. The stale segment never comes back.
func TestSelectSegments_HeldSegmentRemovedAfterSnapshot(t *testing.T) {
	g := newGatedController(t, 2)
	older := g.segs[0]
	require.NoError(t, older.incRef(g.ctx)) // a long-lived writer
	out := g.startQuery(t)

	// Retention must not wait for the query's in-flight reopen.
	removed := make(chan error, 1)
	go func() {
		_, err := g.sc.remove(g.ctx, older.End.Add(time.Minute))
		removed <- err
	}()
	requireWithin(t, removed, "retention")
	require.DirExists(t, older.location, "a held segment keeps its directory")
	g.sc.RLock()
	tracked := g.sc.deleting[older.location]
	g.sc.RUnlock()
	require.Same(t, older, tracked)

	r := g.finish(t, out)
	require.NoError(t, r.err)
	require.Len(t, r.segs, 2, "the held segment is still pinnable through the stale snapshot")
	require.EqualValues(t, 2, refCountOf(older))

	_, err := g.sc.create(g.ctx, older.Start)
	require.ErrorIs(t, err, ErrSegmentDeleting)

	for _, s := range r.segs {
		s.DecRef()
	}
	require.DirExists(t, older.location)
	older.DecRef()
	require.True(t, older.deleted.Load())
	require.NoDirExists(t, older.location)
	require.Zero(t, refCountOf(older))

	recreated, err := g.sc.create(g.ctx, older.Start)
	require.NoError(t, err)
	require.NotSame(t, older, recreated)
	require.ErrorIs(t, older.incRef(g.ctx), ErrSegmentClosed)
	require.False(t, refSegIsOpen(older))
}

// A query whose range covers a segment that fails to reopen (for example a
// corrupted shard) must get the error, not a silently shortened result, and
// must not leak the references it already took on newer segments.
func TestSelectSegments_ReopenFailurePropagatesAndReleasesPins(t *testing.T) {
	g := newGatedController(t, 2)
	reopenErr := errors.New("injected reopen failure")
	g.sc.opts.TSTableCreator = func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
		tr timestamp.TimeRange, _ mockTSTableOpener, _ any,
	) (mockTSTable, error) {
		if tr.Start.Equal(g.segs[0].Start) {
			return mockTSTable{}, reopenErr
		}
		return mockTSTable{ID: common.ShardID(0)}, nil
	}
	tr := timestamp.NewInclusiveTimeRange(g.segs[0].Start, g.segs[1].End)
	segs, err := g.sc.selectSegments(tr, true) // the newest reopens first, then the oldest fails
	require.Error(t, err, "a reopen failure other than a deleted segment must propagate")
	require.Nil(t, segs)
	for _, s := range g.segs {
		require.Zero(t, refCountOf(s), "references must be released for %s", s)
	}
}

// The rotation pass reopens every segment through segments(ctx, true). When a
// later segment fails to reopen, the references already taken on earlier
// segments must be released, or those segments stay held forever and are never
// idle-closed or deleted.
func TestSegments_ReopenFailureReleasesPins(t *testing.T) {
	g := newGatedController(t, 2)
	reopenErr := errors.New("injected reopen failure")
	g.sc.opts.TSTableCreator = func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
		tr timestamp.TimeRange, _ mockTSTableOpener, _ any,
	) (mockTSTable, error) {
		if tr.Start.Equal(g.segs[1].Start) {
			return mockTSTable{}, reopenErr
		}
		return mockTSTable{ID: common.ShardID(0)}, nil
	}
	segs, err := g.sc.segments(g.ctx, true) // the oldest reopens first, then the newest fails
	require.ErrorContains(t, err, reopenErr.Error())
	require.Nil(t, segs)
	for _, s := range g.segs {
		require.Zero(t, refCountOf(s), "references must be released for %s", s)
	}
}
