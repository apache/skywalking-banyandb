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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// buildTestSeriesDocs builds n series-index documents with distinct doc IDs.
func buildTestSeriesDocs(t *testing.T, n int) index.Documents {
	t.Helper()
	var docs index.Documents
	for i := 0; i < n; i++ {
		var series pbv1.Series
		series.Subject = "series_index_stats"
		series.EntityValues = []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("entity_%d", i)}}},
		}
		require.NoError(t, series.Marshal())
		require.Positive(t, series.ID)
		docs = append(docs, index.Document{
			DocID:        uint64(series.ID),
			EntityValues: append([]byte(nil), series.Buffer...),
		})
	}
	return docs
}

// snapshotTestBase is the fixed wall-clock time the snapshot-test TSDB factories
// pin the mock clock to.
var snapshotTestBase = time.Date(2024, 5, 1, 0, 0, 0, 0, time.Local)

// snapshotTestDir initializes logging and returns a fresh temp dir whose cleanup
// is registered with t.
func snapshotTestDir(t *testing.T) string {
	t.Helper()
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))
	dir, defFn := test.Space(require.New(t))
	t.Cleanup(defFn)
	return dir
}

// openSnapshotTSDB opens a TSDB (1-day segments, 1-hour idle timeout, ttlDays
// TTL) with the clock pinned at snapshotTestBase and NO segment created, so a
// test can drive the real controller flow (CreateSegmentIfNotExist +
// IndexDB().Insert + closeIdleSegments).
func openSnapshotTSDB(t *testing.T, dir string, ttlDays int) (TSDB[*MockTSTable, any], *segmentController[*MockTSTable, any]) {
	t.Helper()
	opts := TSDBOpts[*MockTSTable, any]{
		Location:           dir,
		SegmentInterval:    IntervalRule{Unit: DAY, Num: 1},
		TTL:                IntervalRule{Unit: DAY, Num: ttlDays},
		ShardNum:           1,
		TSTableCreator:     MockTSTableCreator,
		SegmentIdleTimeout: time.Hour,
	}
	mc := timestamp.NewMockClock()
	mc.Set(snapshotTestBase)
	ctx := timestamp.SetClock(context.Background(), mc)
	tsdb, err := OpenTSDB(ctx, opts, NewServiceCache(), group)
	require.NoError(t, err)
	return tsdb, tsdb.(*database[*MockTSTable, any]).segmentController
}

// openSnapshotTestTSDB opens a TSDB with a single day segment created at
// snapshotTestBase and returns the db, its controller and the (open) segment.
func openSnapshotTestTSDB(t *testing.T, dir string) (TSDB[*MockTSTable, any], *segmentController[*MockTSTable, any], *segment[*MockTSTable, any]) {
	t.Helper()
	tsdb, sc := openSnapshotTSDB(t, dir, 3)
	seg, err := tsdb.CreateSegmentIfNotExist(snapshotTestBase)
	require.NoError(t, err)
	seg.DecRef() // release the create ref, leaving it open but dormant (refCount==0)
	require.Len(t, sc.lst, 1)
	return tsdb, sc, sc.lst[0]
}

// TestTakeFileSnapshot_ClosedSegmentIsNotReopened is the core backup
// guarantee: snapshotting an idle-closed segment must NOT reopen it (no
// OpenWriter, no exclusive-lock churn, no nil-index panic). The closed segment
// is hard-linked from its quiescent on-disk files instead.
//
// "Did not reopen" is asserted by checking the segment stays closed
// (index == nil) and unreferenced (refCount == 0) after the snapshot.
func TestTakeFileSnapshot_ClosedSegmentIsNotReopened(t *testing.T) {
	dir := snapshotTestDir(t)

	tsdb, sc, seg := openSnapshotTestTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	// Drive the segment into the idle-closed state every cold segment lives in.
	seg.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	require.Equal(t, 1, sc.closeIdleSegments())
	require.Nil(t, seg.index, "precondition: segment must be idle-closed")
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount))

	snapshotDir := filepath.Join(dir, "snapshot")
	created, err := tsdb.TakeFileSnapshot(snapshotDir)
	require.NoError(t, err, "snapshot of a closed segment must succeed")
	require.True(t, created)

	// The crux: the closed segment was NOT reopened.
	require.Nil(t, seg.index, "snapshot must not reopen a closed segment")
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount), "snapshot must not leave a dangling reference")

	// And the series index was carried into the snapshot via hard-link.
	segDir := filepath.Join(snapshotDir, filepath.Base(seg.location))
	require.DirExists(t, segDir)
	require.DirExists(t, filepath.Join(segDir, seriesIndexDirName))
}

// TestTakeFileSnapshot_ClosedSegmentExcludesTransientArtifacts verifies the
// closed-segment hard-link path copies current data and manifests but skips
// transient / non-current artifacts (the failed-parts directory, the
// external-segment temp directory, and .tmp files), matching what the open
// path produces.
func TestTakeFileSnapshot_ClosedSegmentExcludesTransientArtifacts(t *testing.T) {
	dir := snapshotTestDir(t)

	tsdb, sc, seg := openSnapshotTestTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	seg.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	require.Equal(t, 1, sc.closeIdleSegments())
	require.Nil(t, seg.index)

	// Lay out a closed segment's on-disk artifacts: a current part (kept) plus
	// transient/non-current ones that must NOT be carried into the snapshot.
	loc := seg.location
	writeFile := func(rel string) {
		p := filepath.Join(loc, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), DirPerm))
		require.NoError(t, os.WriteFile(p, []byte("x"), FilePerm))
	}
	writeFile(filepath.Join("shard-0", "0000000000000001", "metadata.json"))
	writeFile(filepath.Join("shard-0", FailedPartsDirName, "junk.bin"))
	writeFile(filepath.Join("shard-0", "stale.tmp"))
	writeFile(filepath.Join(seriesIndexDirName, inverted.ExternalSegmentTempDirName, "t.bin"))
	writeFile(filepath.Join(seriesIndexDirName, inverted.LockFilename))

	snapshotDir := filepath.Join(dir, "snapshot")
	created, err := tsdb.TakeFileSnapshot(snapshotDir)
	require.NoError(t, err)
	require.True(t, created)

	segSnap := filepath.Join(snapshotDir, filepath.Base(loc))
	require.FileExists(t, filepath.Join(segSnap, "shard-0", "0000000000000001", "metadata.json"), "current part must be copied")
	require.DirExists(t, filepath.Join(segSnap, seriesIndexDirName), "series index must be copied")
	require.NoDirExists(t, filepath.Join(segSnap, "shard-0", FailedPartsDirName), "failed-parts must be excluded")
	require.NoFileExists(t, filepath.Join(segSnap, "shard-0", "stale.tmp"), ".tmp must be excluded")
	require.NoDirExists(t, filepath.Join(segSnap, seriesIndexDirName, inverted.ExternalSegmentTempDirName), "external-segment temp must be excluded")
	require.NoFileExists(t, filepath.Join(segSnap, seriesIndexDirName, inverted.LockFilename), "bluge lock file must be excluded")
}

// TestSeriesIndexStats_ClosedSegmentIsNotReopened verifies the inspection
// primitive: reading a closed segment's series index stats must read from disk
// read-only and must NOT reopen the writable index.
func TestSeriesIndexStats_ClosedSegmentIsNotReopened(t *testing.T) {
	dir := snapshotTestDir(t)

	tsdb, sc, seg := openSnapshotTestTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	// Write a known number of series into the open segment's index, then drive
	// it idle-closed (closeIfIdle flushes the index to disk on close).
	const seriesCount = 10
	require.NoError(t, seg.IndexDB().Insert(buildTestSeriesDocs(t, seriesCount)))

	seg.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	require.Equal(t, 1, sc.closeIdleSegments())
	require.Nil(t, seg.index, "precondition: segment is idle-closed")

	count, size := seg.SeriesIndexStats()
	require.Equal(t, int64(seriesCount), count, "closed series index doc count must be read from disk read-only")
	require.Positive(t, size, "closed series index on-disk size must be reported")
	require.Nil(t, seg.index, "reading series index stats must not reopen a closed segment")
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount))
}

// TestTakeFileSnapshot_OpenSegmentUsesLiveState verifies the other branch: an
// open segment is snapshotted through its live state and is not closed by the
// snapshot (the held reference keeps it open).
func TestTakeFileSnapshot_OpenSegmentUsesLiveState(t *testing.T) {
	dir := snapshotTestDir(t)

	tsdb, _, seg := openSnapshotTestTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	require.NotNil(t, seg.index, "precondition: segment is open")

	snapshotDir := filepath.Join(dir, "snapshot")
	created, err := tsdb.TakeFileSnapshot(snapshotDir)
	require.NoError(t, err)
	require.True(t, created)

	require.NotNil(t, seg.index, "open segment must stay open after snapshot")
	segDir := filepath.Join(snapshotDir, filepath.Base(seg.location))
	require.DirExists(t, filepath.Join(segDir, seriesIndexDirName))
}

// TestTakeFileSnapshot_ConcurrentWithReclaim_ClosedPath_NoPanic exercises the
// closed-segment hard-link path against a concurrently running idle reclaimer.
// Because the snapshot never reopens the closed segment, the reclaimer can
// never steal a reference out from under the snapshot, so the original
// nil-index panic cannot occur. Run with -race.
//
// NOTE: the broader "query-driven reopen racing the idle reclaimer" scenario
// (and the open-segment reclaim race) is a separate concern in the segment
// reference-counting model and is intentionally NOT covered here.
func TestTakeFileSnapshot_ConcurrentWithReclaim_ClosedPath_NoPanic(t *testing.T) {
	dir := snapshotTestDir(t)

	tsdb, sc, seg := openSnapshotTestTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	// Put the segment into the idle-closed state and keep it there: nothing
	// reopens it (the snapshot hard-links it without opening a writer).
	seg.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	require.Equal(t, 1, sc.closeIdleSegments())
	require.Nil(t, seg.index)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Reclaimer: keep hammering closeIdleSegments (a no-op on an already-closed
	// segment, but it must never collide with the snapshot's hard-link).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			seg.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
			sc.closeIdleSegments()
		}
	}()

	// Snapshotter: take snapshots into unique directories.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = tsdb.TakeFileSnapshot(filepath.Join(dir, "snap", "s"+strconv.Itoa(i)))
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	require.Nil(t, seg.index, "the segment must stay closed: snapshot never reopens it")
	_, err := tsdb.TakeFileSnapshot(filepath.Join(dir, "final-snapshot"))
	require.NoError(t, err)
}

// newEmptySnapshotTSDB opens a TSDB (1-day segments, 1-hour idle timeout) with
// newEmptySnapshotTSDB opens a snapshot-test TSDB with no segment created yet.
func newEmptySnapshotTSDB(t *testing.T, dir string) (TSDB[*MockTSTable, any], *segmentController[*MockTSTable, any]) {
	t.Helper()
	return openSnapshotTSDB(t, dir, 30)
}

// snapshotDay returns the i-th distinct day starting at snapshotTestBase.
func snapshotDay(i int) time.Time {
	return snapshotTestBase.Add(time.Duration(i) * 24 * time.Hour)
}

// createSegmentWithSeries creates (via the controller) the segment containing
// ts, inserts n series documents into its series index, and returns the
// internal segment left open but dormant (the create reference is released).
func createSegmentWithSeries(t *testing.T, tsdb TSDB[*MockTSTable, any], ts time.Time, n int) *segment[*MockTSTable, any] {
	t.Helper()
	s, err := tsdb.CreateSegmentIfNotExist(ts)
	require.NoError(t, err)
	if n > 0 {
		require.NoError(t, s.IndexDB().Insert(buildTestSeriesDocs(t, n)))
	}
	seg := s.(*segment[*MockTSTable, any])
	s.DecRef()
	return seg
}

// snapshotSeriesDocCount reads the series-index doc count of seg's snapshot copy
// under snapshotDir, read-only.
func snapshotSeriesDocCount(t *testing.T, snapshotDir string, seg *segment[*MockTSTable, any]) int64 {
	t.Helper()
	count, err := inverted.ReadOnlyDocCount(filepath.Join(snapshotDir, filepath.Base(seg.location), seriesIndexDirName))
	require.NoError(t, err)
	return count
}

// assertContainsHardLink asserts at least one regular file under dstDir is the
// same inode as its counterpart in srcDir (i.e. hard-linked, not byte-copied).
func assertContainsHardLink(t *testing.T, srcDir, dstDir string) {
	t.Helper()
	entries, err := os.ReadDir(srcDir)
	require.NoError(t, err)
	sawRegularFile := false
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		sawRegularFile = true
		srcInfo, srcErr := os.Stat(filepath.Join(srcDir, e.Name()))
		dstInfo, dstErr := os.Stat(filepath.Join(dstDir, e.Name()))
		if srcErr == nil && dstErr == nil && os.SameFile(srcInfo, dstInfo) {
			return
		}
	}
	require.True(t, sawRegularFile, "precondition: %s must contain at least one regular file to compare", srcDir)
	t.Fatalf("expected at least one hard-linked file between %s and %s", srcDir, dstDir)
}

// TestSnapshotInto_OpenSegmentIsRestorable drives the open-segment branch
// through the real controller: an open segment with live series data is
// snapshotted via its live state, stays open, leaves no dangling reference, and
// the snapshot's series index is independently readable with the same doc count.
func TestSnapshotInto_OpenSegmentIsRestorable(t *testing.T) {
	dir := snapshotTestDir(t)

	tsdb, _ := newEmptySnapshotTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	const seriesCount = 8
	seg := createSegmentWithSeries(t, tsdb, snapshotDay(0), seriesCount)
	require.NotNil(t, seg.index, "precondition: segment is open")
	refBefore := atomic.LoadInt32(&seg.refCount)

	snapshotDir := filepath.Join(dir, "snapshot")
	created, err := tsdb.TakeFileSnapshot(snapshotDir)
	require.NoError(t, err)
	require.True(t, created)

	require.NotNil(t, seg.index, "open segment must stay open after snapshot")
	require.Equal(t, refBefore, atomic.LoadInt32(&seg.refCount), "open-path snapshot must balance its reference bump")
	require.Equal(t, int64(seriesCount), snapshotSeriesDocCount(t, snapshotDir, seg), "snapshot series index must hold the live docs")
}

// TestSnapshotInto_ClosedSegmentIsRestorableViaHardLink drives the
// closed-segment branch: an idle-closed segment is snapshotted by hard-linking
// its quiescent files (not reopened), stays closed, and the snapshot's series
// index is readable with the same doc count and shares inodes with the source.
func TestSnapshotInto_ClosedSegmentIsRestorableViaHardLink(t *testing.T) {
	dir := snapshotTestDir(t)

	tsdb, sc := newEmptySnapshotTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	const seriesCount = 5
	seg := createSegmentWithSeries(t, tsdb, snapshotDay(0), seriesCount)
	seg.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	require.Equal(t, 1, sc.closeIdleSegments())
	require.Nil(t, seg.index, "precondition: segment is idle-closed")

	snapshotDir := filepath.Join(dir, "snapshot")
	created, err := tsdb.TakeFileSnapshot(snapshotDir)
	require.NoError(t, err)
	require.True(t, created)

	require.Nil(t, seg.index, "snapshot must not reopen the closed segment")
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount))
	require.Equal(t, int64(seriesCount), snapshotSeriesDocCount(t, snapshotDir, seg))

	srcIndex := filepath.Join(seg.location, seriesIndexDirName)
	dstIndex := filepath.Join(snapshotDir, filepath.Base(seg.location), seriesIndexDirName)
	assertContainsHardLink(t, srcIndex, dstIndex)
}

// TestTakeFileSnapshot_MixedOpenAndClosedSegments snapshots several segments at
// once where some are open and some idle-closed: every segment is captured with
// the correct per-segment data, the open/closed states are preserved (the
// closed ones are not reopened), and references stay balanced.
func TestTakeFileSnapshot_MixedOpenAndClosedSegments(t *testing.T) {
	dir := snapshotTestDir(t)

	tsdb, sc := newEmptySnapshotTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	seg0 := createSegmentWithSeries(t, tsdb, snapshotDay(0), 3)
	seg1 := createSegmentWithSeries(t, tsdb, snapshotDay(1), 5)
	seg2 := createSegmentWithSeries(t, tsdb, snapshotDay(2), 7)

	// Idle-close the two older segments; keep the newest open.
	seg0.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	seg1.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	require.Equal(t, 2, sc.closeIdleSegments())
	require.Nil(t, seg0.index)
	require.Nil(t, seg1.index)
	require.NotNil(t, seg2.index)

	snapshotDir := filepath.Join(dir, "snapshot")
	created, err := tsdb.TakeFileSnapshot(snapshotDir)
	require.NoError(t, err)
	require.True(t, created)

	// States preserved: closed stay closed, open stays open.
	require.Nil(t, seg0.index, "closed segment must not be reopened by snapshot")
	require.Nil(t, seg1.index, "closed segment must not be reopened by snapshot")
	require.NotNil(t, seg2.index, "open segment must stay open")

	// Every segment is present and restorable with its own doc count.
	require.Equal(t, int64(3), snapshotSeriesDocCount(t, snapshotDir, seg0))
	require.Equal(t, int64(5), snapshotSeriesDocCount(t, snapshotDir, seg1))
	require.Equal(t, int64(7), snapshotSeriesDocCount(t, snapshotDir, seg2))
}

// TestTakeFileSnapshot_DeletedSegmentIsSkipped verifies a segment flagged for
// deletion (the concurrent-retention race window) is skipped by snapshotInto
// while the surviving segment is still snapshotted.
func TestTakeFileSnapshot_DeletedSegmentIsSkipped(t *testing.T) {
	dir := snapshotTestDir(t)

	tsdb, _ := newEmptySnapshotTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	doomed := createSegmentWithSeries(t, tsdb, snapshotDay(0), 3)
	survivor := createSegmentWithSeries(t, tsdb, snapshotDay(1), 4)

	// Model a segment that retention has flagged for deletion but that is still
	// in the controller list when the snapshot copies it.
	atomic.StoreUint32(&doomed.mustBeDeleted, 1)

	snapshotDir := filepath.Join(dir, "snapshot")
	created, err := tsdb.TakeFileSnapshot(snapshotDir)
	require.NoError(t, err)
	require.True(t, created, "the surviving segment was written")

	require.NoDirExists(t, filepath.Join(snapshotDir, filepath.Base(doomed.location)), "a segment flagged for deletion must be skipped")
	require.DirExists(t, filepath.Join(snapshotDir, filepath.Base(survivor.location)), "the surviving segment must be snapshotted")
}

// TestTakeFileSnapshot_AllSegmentsDeletedReturnsFalse verifies that when every
// segment is flagged for deletion, the snapshot writes nothing and reports
// success=false with no error.
func TestTakeFileSnapshot_AllSegmentsDeletedReturnsFalse(t *testing.T) {
	dir := snapshotTestDir(t)

	tsdb, _ := newEmptySnapshotTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	seg0 := createSegmentWithSeries(t, tsdb, snapshotDay(0), 3)
	seg1 := createSegmentWithSeries(t, tsdb, snapshotDay(1), 4)
	atomic.StoreUint32(&seg0.mustBeDeleted, 1)
	atomic.StoreUint32(&seg1.mustBeDeleted, 1)

	snapshotDir := filepath.Join(dir, "snapshot")
	created, err := tsdb.TakeFileSnapshot(snapshotDir)
	require.NoError(t, err)
	require.False(t, created, "no segment content was written")
	require.NoDirExists(t, snapshotDir, "nothing should be written when all segments are skipped")
}

// TestSnapshotInto_EmptyOpenSegment verifies an open segment that has no
// ingested data yet is still snapshotted (metadata + an empty series index) and
// reports success.
func TestSnapshotInto_EmptyOpenSegment(t *testing.T) {
	dir := snapshotTestDir(t)

	tsdb, _ := newEmptySnapshotTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	seg := createSegmentWithSeries(t, tsdb, snapshotDay(0), 0)
	require.NotNil(t, seg.index, "precondition: segment is open")

	snapshotDir := filepath.Join(dir, "snapshot")
	created, err := tsdb.TakeFileSnapshot(snapshotDir)
	require.NoError(t, err)
	require.True(t, created)

	segSnap := filepath.Join(snapshotDir, filepath.Base(seg.location))
	require.FileExists(t, filepath.Join(segSnap, SegmentMetadataFilename), "segment metadata must be snapshotted")
	require.DirExists(t, filepath.Join(segSnap, seriesIndexDirName), "series index dir must be snapshotted")
	require.Equal(t, int64(0), snapshotSeriesDocCount(t, snapshotDir, seg))
}

// allTimeRange covers every segment the snapshot-test factories create.
func allTimeRange() timestamp.TimeRange {
	return timestamp.TimeRange{Start: time.Unix(0, 0), End: time.Unix(0, timestamp.MaxNanoTime)}
}

// idleClose drives seg into the idle-closed state and asserts it is closed.
func idleClose(t *testing.T, sc *segmentController[*MockTSTable, any], seg *segment[*MockTSTable, any]) {
	t.Helper()
	seg.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	require.Equal(t, 1, sc.closeIdleSegments())
	require.Nil(t, seg.index, "precondition: segment must be idle-closed")
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount), "precondition: closed segment has refCount 0")
}

// TestSelectSegments_ReopenTrue_ReopensClosedSegment: reopenClosed=true on a
// CLOSED segment reopens it (incRef -> acquire), takes refCount to 1, and
// refreshes lastAccessed (a real read keeps the segment hot). DecRef to zero
// then leaves it dormant (open), not closed.
func TestSelectSegments_ReopenTrue_ReopensClosedSegment(t *testing.T) {
	dir := snapshotTestDir(t)
	tsdb, sc := newEmptySnapshotTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	seg := createSegmentWithSeries(t, tsdb, snapshotDay(0), 5)
	idleClose(t, sc, seg)
	closedAccessed := seg.lastAccessed.Load()

	got, err := sc.selectSegments(allTimeRange(), true)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotNil(t, seg.index, "reopenClosed=true must reopen a closed segment")
	require.Equal(t, int32(1), atomic.LoadInt32(&seg.refCount), "acquire takes refCount to 1")
	require.Greater(t, seg.lastAccessed.Load(), closedAccessed, "a real read refreshes lastAccessed")

	got[0].DecRef()
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount))
	require.NotNil(t, seg.index, "DecRef to zero leaves the reopened segment dormant (open), not closed")
}

// TestSelectSegments_ReopenTrue_DormantSegmentAcquiredAndRefreshed:
// reopenClosed=true on a DORMANT open segment (refCount==0, index!=nil) acquires
// it (0->1) and refreshes lastAccessed without re-opening; DecRef returns it to
// dormant.
func TestSelectSegments_ReopenTrue_DormantSegmentAcquiredAndRefreshed(t *testing.T) {
	dir := snapshotTestDir(t)
	tsdb, sc := newEmptySnapshotTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	seg := createSegmentWithSeries(t, tsdb, snapshotDay(0), 0)
	require.NotNil(t, seg.index)
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount), "dormant baseline: open, refCount 0")
	seg.lastAccessed.Store(time.Now().Add(-time.Hour).UnixNano())
	before := seg.lastAccessed.Load()

	got, err := sc.selectSegments(allTimeRange(), true)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotNil(t, seg.index, "dormant open segment stays open (no reopen needed)")
	require.Equal(t, int32(1), atomic.LoadInt32(&seg.refCount), "acquire takes the dormant segment 0->1")
	require.Greater(t, seg.lastAccessed.Load(), before, "a real read refreshes lastAccessed")

	got[0].DecRef()
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount))
}

// TestSelectSegments_ReopenFalse_ClosedSegmentNotReopened: reopenClosed=false on
// a CLOSED segment returns it WITHOUT reopening, without bumping refCount, and
// without refreshing lastAccessed; the caller's DecRef is a safe no-op.
func TestSelectSegments_ReopenFalse_ClosedSegmentNotReopened(t *testing.T) {
	dir := snapshotTestDir(t)
	tsdb, sc := newEmptySnapshotTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	seg := createSegmentWithSeries(t, tsdb, snapshotDay(0), 5)
	idleClose(t, sc, seg)
	closedAccessed := seg.lastAccessed.Load()

	got, err := sc.selectSegments(allTimeRange(), false)
	require.NoError(t, err)
	require.Len(t, got, 1, "closed segment is still returned for stats")
	require.Nil(t, seg.index, "reopenClosed=false must NOT reopen a closed segment")
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount), "closed segment is returned without a ref bump")
	require.Equal(t, closedAccessed, seg.lastAccessed.Load(), "a stats peek must not refresh lastAccessed")

	got[0].DecRef() // safe no-op on a refCount==0 segment
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount))
	require.Nil(t, seg.index)
}

// TestSelectSegments_ReopenFalse_DormantSegmentNotPinned: reopenClosed=false on a
// DORMANT open segment (refCount==0) returns it WITHOUT pinning. By design the
// stats path never bumps a refCount==0 segment (that would race the idle
// reclaimer); stats are best-effort. The segment stays open and its idle timer
// is not refreshed.
func TestSelectSegments_ReopenFalse_DormantSegmentNotPinned(t *testing.T) {
	dir := snapshotTestDir(t)
	tsdb, sc := newEmptySnapshotTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	seg := createSegmentWithSeries(t, tsdb, snapshotDay(0), 0)
	require.NotNil(t, seg.index)
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount), "dormant: open, refCount 0")
	seg.lastAccessed.Store(time.Now().Add(-time.Hour).UnixNano())
	before := seg.lastAccessed.Load()

	got, err := sc.selectSegments(allTimeRange(), false)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotNil(t, seg.index, "dormant open segment stays open")
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount), "a dormant segment is not pinned by the stats path")
	require.Equal(t, before, seg.lastAccessed.Load(), "a stats peek must not refresh lastAccessed")

	got[0].DecRef() // no-op
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount))
}

// TestSelectSegments_ReopenFalse_InUseSegmentPinned: reopenClosed=false on a
// segment already in active use (refCount>1) pins it (+1), never disturbing the
// references other callers hold.
func TestSelectSegments_ReopenFalse_InUseSegmentPinned(t *testing.T) {
	dir := snapshotTestDir(t)
	tsdb, sc := newEmptySnapshotTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	seg := createSegmentWithSeries(t, tsdb, snapshotDay(0), 0)
	require.NoError(t, seg.incRef(context.Background())) // an active reader holds a ref
	require.Equal(t, int32(1), atomic.LoadInt32(&seg.refCount), "one active reader")

	got, err := sc.selectSegments(allTimeRange(), false)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, int32(2), atomic.LoadInt32(&seg.refCount), "an in-use (refCount>0) segment is pinned (+1)")

	got[0].DecRef() // release the selectSegments pin
	seg.DecRef()    // release the active reader
	require.Equal(t, int32(0), atomic.LoadInt32(&seg.refCount), "back to dormant")
}

// TestSelectSegments_TimeRangeFilters: only segments overlapping the requested
// range are returned; a range that ends before the newest segment exercises the
// sorted early-break, and a range covering one segment returns exactly that one.
func TestSelectSegments_TimeRangeFilters(t *testing.T) {
	dir := snapshotTestDir(t)
	tsdb, sc := newEmptySnapshotTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	_ = createSegmentWithSeries(t, tsdb, snapshotDay(0), 0)
	seg1 := createSegmentWithSeries(t, tsdb, snapshotDay(1), 0)
	_ = createSegmentWithSeries(t, tsdb, snapshotDay(2), 0)
	require.Len(t, sc.lst, 3)

	// Range inside day 1 only -> exactly seg1.
	only := timestamp.NewInclusiveTimeRange(snapshotDay(1).Add(time.Hour), snapshotDay(1).Add(2*time.Hour))
	got, err := sc.selectSegments(only, false)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, seg1.location, got[0].(*segment[*MockTSTable, any]).location)
	got[0].DecRef()

	// Range entirely after every segment -> none (early-break path).
	after := timestamp.NewInclusiveTimeRange(snapshotDay(10), snapshotDay(11))
	got, err = sc.selectSegments(after, true)
	require.NoError(t, err)
	require.Empty(t, got)

	// Range entirely before every segment -> none.
	before := timestamp.NewInclusiveTimeRange(snapshotDay(-5), snapshotDay(-4))
	got, err = sc.selectSegments(before, true)
	require.NoError(t, err)
	require.Empty(t, got)
}

// TestSelectSegments_PublicWrapper verifies the exported SelectSegments delegates
// to the controller and short-circuits to nil once the database is closed.
func TestSelectSegments_PublicWrapper(t *testing.T) {
	dir := snapshotTestDir(t)
	tsdb, _ := newEmptySnapshotTSDB(t, dir)

	seg := createSegmentWithSeries(t, tsdb, snapshotDay(0), 0)
	require.NotNil(t, seg.index)

	got, err := tsdb.SelectSegments(allTimeRange(), false)
	require.NoError(t, err)
	require.Len(t, got, 1)
	got[0].DecRef()

	require.NoError(t, tsdb.Close())
	got, err = tsdb.SelectSegments(allTimeRange(), true)
	require.NoError(t, err)
	require.Nil(t, got, "a closed database returns no segments")
}
