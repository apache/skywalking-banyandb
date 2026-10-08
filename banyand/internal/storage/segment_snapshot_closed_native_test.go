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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// buildTestSeriesDocsNamed is buildTestSeriesDocs with a caller-chosen
// subject, so two calls produce documents with disjoint identifiers (the
// series ID folds the subject in) instead of InsertIfAbsent silently
// skipping a repeat of the same entity_0..entity_{n-1} identifiers.
func buildTestSeriesDocsNamed(t *testing.T, subject string, n int) index.Documents {
	t.Helper()
	var docs index.Documents
	for i := 0; i < n; i++ {
		var series pbv1.Series
		series.Subject = subject
		series.EntityValues = []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("entity_%d", i)}}},
		}
		require.NoError(t, series.Marshal())
		docs = append(docs, index.Document{EntityValues: append([]byte(nil), series.Buffer...)})
	}
	return docs
}

// admitSynchronously admits one document into owner and blocks for its
// durability callback, so the caller can rely on the generation being fully
// persisted (a new manifest and segment file on disk) before proceeding.
func admitSynchronously(t *testing.T, owner *native.Owner, identifier string) {
	t.Helper()
	done := make(chan error, 1)
	require.NoError(t, owner.Batch(context.Background(), native.Batch{
		Documents:          []native.Document{{Identifier: []byte(identifier)}},
		PersistentCallback: func(err error) { done <- err },
	}))
	require.NoError(t, <-done)
}

// TestNewestSeriesIndexGenerationFilesExcludesSuperseded pins the NIDX-03 §8
// fix directly against the two helper functions snapshotClosed uses: a sidx
// directory holding two generations (as a live owner does between an
// admission and the next garbage collection, or across any admission the
// maintenance compaction threshold has not yet reached) must resolve a keep
// set naming only the newest manifest, and closedSnapshotFilter must apply
// that set only inside the series index directory, leaving everything
// outside it untouched.
func TestNewestSeriesIndexGenerationFilesExcludesSuperseded(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "sidx")
	owner, err := native.NewOwner(native.OwnerOptions{Lease: &testRootLease{}, Path: indexPath})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()

	// Two independent admissions each durably persist their own generation
	// (manifest + segment). Both are far below the default compaction
	// threshold (16 segments), so no compaction or garbage collection runs
	// in between: generation 1's manifest is still on disk, superseded by
	// generation 2's, when the keep set below is resolved.
	admitSynchronously(t, owner, "a")
	admitSynchronously(t, owner, "b")

	entries, err := os.ReadDir(indexPath)
	require.NoError(t, err)
	var manifests []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".snp" {
			manifests = append(manifests, e.Name())
		}
	}
	require.GreaterOrEqual(t, len(manifests), 2, "precondition: both generations' manifests are still on disk (no GC has run)")
	sort.Strings(manifests) // "%012x.snp" zero-padded hex sorts lexicographically == numerically.
	oldest, newest := manifests[0], manifests[len(manifests)-1]

	keep, err := newestSeriesIndexGenerationFiles(indexPath)
	require.NoError(t, err)
	_, newestKept := keep[newest]
	require.True(t, newestKept, "the newest manifest must be in the keep set")
	_, oldestKept := keep[oldest]
	require.False(t, oldestKept, "a superseded manifest must not be in the keep set")

	filter := closedSnapshotFilter(indexPath, keep)
	require.True(t, filter(filepath.Join(indexPath, newest)), "the newest manifest must be kept")
	require.False(t, filter(filepath.Join(indexPath, oldest)), "a superseded manifest must be excluded")
	require.True(t, filter(indexPath), "the series index directory itself must always be included so the walk descends")

	// Outside indexPath entirely: unchanged includeInClosedSnapshot behavior.
	outside := filepath.Dir(indexPath)
	require.True(t, filter(filepath.Join(outside, "shard-0", "metadata.json")))
	// L8 regression: a seg-*/lock exclusive-lock file (lockFilename) is the
	// offline index-mode copy tool's targetIdxStore lease lock, created
	// beside the target sidx directory and removed only on a clean
	// closeAll. A tool crash leaves it on disk; it must never be hard-linked
	// into a closed-segment snapshot the same way the TSDB root's own
	// same-named lock file never is.
	require.False(t, filter(filepath.Join(outside, lockFilename)), "a stray seg-*/lock file must be excluded from closed-segment snapshots")
}

// TestNewestSeriesIndexGenerationFilesNoCommittedGeneration verifies a sidx
// directory with no committed generation yet (brand new, never flushed, or
// simply absent on disk) resolves an empty keep set rather than an error.
func TestNewestSeriesIndexGenerationFilesNoCommittedGeneration(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sidx")

	keep, err := newestSeriesIndexGenerationFiles(dir)
	require.NoError(t, err)
	require.Empty(t, keep)
}

// TestClosedSegmentHelpers_CorruptNewestManifestFailsClosed is the L2
// regression. newestSeriesIndexGenerationFiles and closedSeriesIndexDocCount
// used native.OpenReadOnlyGeneration, which falls back to the last complete
// generation when the newest manifest is corrupt -- correct for the
// best-effort offline tools it also serves, but wrong here: silently
// hard-linking (or reporting the doc count of) an OLDER generation while
// claiming it is current would make a closed-segment backup drop
// acknowledged data with no error. Both helpers must instead open strict
// (native.OpenReadOnlyGenerationStrict, the same fail-closed mode a writer
// reopening the directory uses) and fail the same way native.NewOwner
// already does on the same corrupt directory, rather than disagreeing with
// it.
func TestClosedSegmentHelpers_CorruptNewestManifestFailsClosed(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "sidx")
	owner, err := native.NewOwner(native.OwnerOptions{Lease: &testRootLease{}, Path: indexPath})
	require.NoError(t, err)
	admitSynchronously(t, owner, "a")
	admitSynchronously(t, owner, "b")
	require.NoError(t, owner.Close())

	entries, err := os.ReadDir(indexPath)
	require.NoError(t, err)
	var manifests []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".snp" {
			manifests = append(manifests, e.Name())
		}
	}
	require.GreaterOrEqual(t, len(manifests), 2, "precondition: two generations' manifests are both still on disk")
	sort.Strings(manifests)
	newest := manifests[len(manifests)-1]

	// Overwrite the newest manifest with garbage, corrupting it while
	// leaving the older, complete generation intact on disk -- exactly the
	// condition the lenient OpenReadOnlyGeneration would silently roll back
	// from.
	require.NoError(t, os.WriteFile(filepath.Join(indexPath, newest), []byte("not a valid manifest"), 0o600))

	_, countErr := closedSeriesIndexDocCount(indexPath)
	require.Error(t, countErr, "a corrupt newest manifest must fail closedSeriesIndexDocCount, not silently report an older generation's count")

	_, filesErr := newestSeriesIndexGenerationFiles(indexPath)
	require.Error(t, filesErr, "a corrupt newest manifest must fail newestSeriesIndexGenerationFiles, not silently keep an older generation's files")

	// Consistency check (the coordinator's own probe): a live owner
	// reopening the same corrupt directory must fail too, not disagree with
	// the closed-segment helpers above.
	_, reopenErr := native.NewOwner(native.OwnerOptions{Lease: &testRootLease{}, Path: indexPath})
	require.Error(t, reopenErr, "NewOwner must also refuse to reopen a directory with a corrupt newest manifest")
}

// TestSnapshotClosed_ExcludesSupersededManifest drives the fix through the
// real segment/TSDB stack: two independent synchronous admissions each
// durably persist their own generation (manifest + segment) with no
// compaction between them (far below the default compaction threshold), so
// the live sidx directory still holds the first, now-superseded manifest
// when the segment is idle-closed and snapshotted. The hard-linked copy must
// carry only the newest manifest.
func TestSnapshotClosed_ExcludesSupersededManifest(t *testing.T) {
	dir := snapshotTestDir(t)
	tsdb, sc, seg := openSnapshotTestTSDB(t, dir)
	defer func() { require.NoError(t, tsdb.Close()) }()

	require.NoError(t, seg.IndexDB().Insert(buildTestSeriesDocsNamed(t, "gen1", 2)))
	require.NoError(t, seg.IndexDB().Insert(buildTestSeriesDocsNamed(t, "gen2", 2)))

	sidxPath := filepath.Join(seg.location, seriesIndexDirName)
	srcManifests := countExt(t, sidxPath, ".snp")
	require.GreaterOrEqual(t, srcManifests, 2, "precondition: more than one generation's manifest is still on disk")

	seg.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	require.Equal(t, 1, sc.closeIdleSegments())
	require.Nil(t, seg.index)

	snapshotDir := filepath.Join(dir, "snapshot")
	created, err := tsdb.TakeFileSnapshot(snapshotDir)
	require.NoError(t, err)
	require.True(t, created)

	dstSidx := filepath.Join(snapshotDir, filepath.Base(seg.location), seriesIndexDirName)
	require.Equal(t, 1, countExt(t, dstSidx, ".snp"), "only the newest committed manifest must be hard-linked")
	require.Equal(t, int64(4), snapshotSeriesDocCount(t, snapshotDir, seg), "the snapshot copy still reads back every live document")
}

// countExt counts the regular files under dir whose extension is ext.
func countExt(t *testing.T, dir, ext string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	count := 0
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ext {
			count++
		}
	}
	return count
}
