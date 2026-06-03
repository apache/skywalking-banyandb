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
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

// TestSegmentLifecycle_EndToEnd is a full-flow integration test that drives the
// entire segment lifecycle through the public TSDB API against a real series
// index (which persists to disk), asserting the dormant-refcount invariants and
// on-disk durability at every transition:
//
//	create+write -> active(refCount==1) -> DecRef(dormant, still open) ->
//	query reopen/pin/release -> snapshot via live state ->
//	idle reclaim (flush to disk, release writer lock) ->
//	closed-segment stats + snapshot (read-only / hard-link, NO reopen) ->
//	reopen on read -> restart (Close + reopen, data survives on disk) ->
//	retention delete (directory removed).
func TestSegmentLifecycle_EndToEnd(t *testing.T) {
	dir := snapshotTestDir(t)
	const seriesCount = 12
	tr := allTimeRange()

	tsdb, sc := openSnapshotTSDB(t, dir, 3)

	// 1. Create + write: a fresh segment is open and actively referenced.
	s, err := tsdb.CreateSegmentIfNotExist(snapshotTestBase)
	require.NoError(t, err)
	seg := s.(*segment[*MockTSTable, any])
	require.NotNil(t, seg.index, "a freshly created segment is open")
	require.Equal(t, int32(1), atomic.LoadInt32(&seg.refCount), "create yields exactly one active reference")
	require.NoError(t, s.IndexDB().Insert(buildTestSeriesDocs(t, seriesCount)))

	// 2. Release to dormant: DecRef to zero keeps the segment OPEN (the
	// dormant-refcount invariant -- it does not close), and the data stays readable.
	s.DecRef()
	require.NotNil(t, seg.index, "DecRef to zero leaves the segment open (dormant), not closed")
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount))
	count, size := seg.SeriesIndexStats()
	require.Equal(t, int64(seriesCount), count, "data is readable while dormant")
	require.Positive(t, size)

	// 3. Query path: a real read reopens-if-needed, pins the segment, and the
	// matching DecRef returns it to dormant (still open).
	got, err := tsdb.SelectSegments(tr, true)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, int32(1), atomic.LoadInt32(&seg.refCount), "a query pins the segment")
	for _, g := range got {
		g.DecRef()
	}
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount), "query released back to dormant")
	require.NotNil(t, seg.index)

	// 4. Snapshot while open (dormant): goes through the live-state path, must
	// not close the segment, and captures every document.
	openSnap := filepath.Join(t.TempDir(), "snap-open")
	created, err := tsdb.TakeFileSnapshot(openSnap)
	require.NoError(t, err)
	require.True(t, created)
	require.NotNil(t, seg.index, "snapshot must not close an open segment")
	require.Equal(t, int64(seriesCount), snapshotSeriesDocCount(t, openSnap, seg), "open-path snapshot holds all docs")

	// 5. Idle reclaim: closes the segment, flushing the index to disk and
	// releasing the bluge exclusive writer lock.
	idleClose(t, sc, seg)
	require.NoFileExists(t, filepath.Join(seg.location, seriesIndexDirName, inverted.LockFilename),
		"idle-close releases the bluge writer lock (bluge.pid)")

	// 6. Closed segment: stats are read from disk read-only and must NOT reopen
	// the writable index.
	count, size = seg.SeriesIndexStats()
	require.Equal(t, int64(seriesCount), count, "closed-segment data is read from disk")
	require.Positive(t, size)
	require.Nil(t, seg.index, "reading stats must not reopen a closed segment")

	// 7. Snapshot while closed: hard-link path, identical content, still no reopen.
	closedSnap := filepath.Join(t.TempDir(), "snap-closed")
	created, err = tsdb.TakeFileSnapshot(closedSnap)
	require.NoError(t, err)
	require.True(t, created)
	require.Nil(t, seg.index, "snapshot must not reopen a closed segment")
	require.Equal(t, int64(seriesCount), snapshotSeriesDocCount(t, closedSnap, seg),
		"closed-path snapshot captures the same docs as the open-path snapshot")

	// 8. Reopen on access: a real read reopens the closed segment.
	got, err = tsdb.SelectSegments(tr, true)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotNil(t, seg.index, "a read reopens a closed segment")
	for _, g := range got {
		g.DecRef()
	}

	// 9. Restart: closing the DB flushes everything; reopening reloads the
	// segment dormant and the series data survives on disk.
	require.NoError(t, tsdb.Close())
	tsdb2, sc2 := openSnapshotTSDB(t, dir, 3)
	require.Len(t, sc2.lst, 1, "the segment is reloaded from disk on restart")
	reloaded := sc2.lst[0]
	require.NotNil(t, reloaded.index, "a reloaded segment is open")
	require.Equal(t, int32(0), atomic.LoadInt32(&reloaded.refCount), "a reloaded segment starts dormant")
	count, _ = reloaded.SeriesIndexStats()
	require.Equal(t, int64(seriesCount), count, "series data survives a restart on disk")

	// 10. Retention: removeOldest honors keep-one, so add a newer segment, then
	// delete the oldest -- its directory is removed from disk.
	_ = createSegmentWithSeries(t, tsdb2, snapshotDay(1), 3)
	oldestLoc := reloaded.location
	removed, err := sc2.removeOldest()
	require.NoError(t, err)
	require.True(t, removed)
	require.NoDirExists(t, oldestLoc, "retention removes the oldest segment's directory")
	require.Len(t, sc2.lst, 1, "the newer segment remains after retention")

	require.NoError(t, tsdb2.Close())
}

// TestSegment_PostDeleteOperations_NoPanic pins the safety of operations that
// can still run against a segment AFTER it has been deleted (its directory
// removed): inspect (SeriesIndexStats + CollectClosedShardInfo) and backup
// (snapshotInto). Under the dormant-refcount model a dormant segment is NOT
// pinned by the stats path, so these can race a concurrent delete that tears
// down resources and removes the directory. All such operations must degrade
// gracefully (no panic, best-effort zero results), never read a removed dir
// fatally. Run with -race.
func TestSegment_PostDeleteOperations_NoPanic(t *testing.T) {
	dir := snapshotTestDir(t)
	tsdb, _, seg := openSnapshotTestTSDB(t, dir)
	defer func() { _ = tsdb.Close() }()

	// Real on-disk data so inspect/snapshot do meaningful work before deletion.
	require.NoError(t, seg.IndexDB().Insert(buildTestSeriesDocs(t, 8)))

	snapBase := t.TempDir()
	const (
		readers      = 4
		snapshotters = 2
		cycles       = 200
	)
	var (
		wg      sync.WaitGroup
		ready   sync.WaitGroup
		panics  int64
		snapSeq int64
	)
	ready.Add(readers + snapshotters)
	// Each worker runs a fixed number of cycles (no wall-clock timing) and signals
	// `ready` after its first iteration. The delete fires only once every worker
	// has run at least once, so it is guaranteed to race operations that are still
	// looping -- deterministically, on any CPU, with no sleep a slow machine could
	// under- or over-shoot.
	guard := func(fn func()) {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				atomic.AddInt64(&panics, 1)
			}
		}()
		for c := 0; c < cycles; c++ {
			fn()
			if c == 0 {
				ready.Done()
			}
		}
	}

	// Inspect readers.
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go guard(func() {
			_, _ = seg.SeriesIndexStats()
			_, _ = CollectClosedShardInfo(seg.Location())
		})
	}
	// Backup writers, each to its own destination.
	for i := 0; i < snapshotters; i++ {
		wg.Add(1)
		go guard(func() {
			dst := filepath.Join(snapBase, "snap-"+strconv.FormatInt(atomic.AddInt64(&snapSeq, 1), 10))
			_, _ = seg.snapshotInto(dst)
		})
	}

	ready.Wait()
	seg.delete() // races the operations that are still looping
	wg.Wait()

	require.Equal(t, int64(0), atomic.LoadInt64(&panics),
		"inspect/snapshot operations on a deleted segment must not panic")
	require.NoDirExists(t, seg.location,
		"once no operation holds a reference, the deleted segment's directory is gone")
	require.Nil(t, seg.index, "the deleted segment is closed")
}
