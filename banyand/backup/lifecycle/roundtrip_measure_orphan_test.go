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

package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// removeSegmentSidxDirs removes the sidx directory of every segment that backs the
// given part dirs (partDir = .../seg-X/shard-N/<partID>; the sidx lives at
// .../seg-X/sidx). With the sidx gone, the iterator's PartSeriesMap finds nothing,
// so every block's EntityValues are empty and each series must fall through to the
// column rebuild — letting a test drive the sidx-gap path deterministically.
func removeSegmentSidxDirs(t *testing.T, partDirs []string) {
	t.Helper()
	seen := make(map[string]struct{})
	for _, partDir := range partDirs {
		segDir := filepath.Dir(filepath.Dir(partDir)) // .../seg-X
		sidxDir := filepath.Join(segDir, "sidx")
		if _, ok := seen[sidxDir]; ok {
			continue
		}
		seen[sidxDir] = struct{}{}
		require.NoError(t, os.RemoveAll(sidxDir))
	}
}

// TestMeasureOrphanAndSidxGapInSamePart proves the orphan-archive path and the
// sidx-gap path both fire through the real replayPart machinery and that the
// orphan archive is durably written, so the two source-retention semantics
// (orphan: source still deleted; sidx-gap: source retained) are exercised end to
// end.
//
// Note on layout: a measure never stores its entity tags as columns (see
// banyand/measure/write_standalone.go, which skips entity tags when building tag
// families), so an entity is recoverable ONLY from the sidx. Removing the sidx
// therefore makes EVERY series in that segment unresolvable — there is no way to
// keep one series orphan-resolvable (needs the sidx) while making a sibling a
// sidx gap (needs the sidx gone) within the very same on-disk part. The test thus
// drives the two paths over the same part in two passes: pass 1 with the sidx
// intact yields the orphan skip + archive; pass 2 with the sidx removed yields the
// sidx-gap skip. Both counts come from replayPart, and the pass-1 archive persists.
func TestMeasureOrphanAndSidxGapInSamePart(t *testing.T) {
	archiveRoot, partDirs, r, stop := setupMeasureOrphanScenario(t,
		orphanConfig{policy: orphanArchive, rootDir: filepath.Join(t.TempDir(), "orphan-archive")})
	defer stop()
	defer r.Close()

	// Pass 1: sidx intact. The deleted measure resolves to its subject via the
	// sidx, hits errOrphanSchema, and is archived + counted as an orphan skip.
	orphanPass := replayAllParts(t, r, partDirs)
	assert.Greater(t, orphanPass.orphanSkipped, 0, "the deleted-schema measure must be counted as an orphan skip")
	assert.Equal(t, 0, orphanPass.skipped, "with the sidx intact there is no sidx gap")

	jsonlPaths := findFilesWithSuffix(t, archiveRoot, ".jsonl.gz")
	require.NotEmpty(t, jsonlPaths, "the orphan archive must exist after the orphan pass")
	archivedRows := 0
	for _, p := range jsonlPaths {
		for _, rec := range readArchiveRecords(t, p) {
			require.Equal(t, orphanRTDeletedName, rec.Measure, "only the deleted measure must be archived")
			archivedRows++
		}
	}
	assert.Equal(t, orphanPass.orphanSkipped, archivedRows, "every orphan-skipped row must be archived")

	// Pass 2: remove the sidx so no series resolves. Close the resident resolver
	// first so it reopens against the now-empty index (the open bluge store still
	// holds file handles to the deleted dir otherwise). Every block then falls
	// through to the (impossible) column rebuild and is counted as a sidx gap, which
	// is the retain-source signal.
	closeIndexResolver(&r.irMu, &r.irResolver, &r.irPath)
	removeSegmentSidxDirs(t, partDirs)
	gapPass := replayAllParts(t, r, partDirs)
	assert.Greater(t, gapPass.skipped, 0, "removing the sidx must surface unresolved series as sidx-gap skips")
}

// TestMeasureOrphanArchiveWriteFailureAbortsPartAndRetainsSource proves an archive
// write failure during replayPart aborts the part (a non-nil error) and is NOT
// silently counted as an orphan skip, so the caller retains the source segment and
// a resume retries. The failure is induced by pre-creating the computed archive
// file path as a directory so the JSONL OpenFile fails.
func TestMeasureOrphanArchiveWriteFailureAbortsPartAndRetainsSource(t *testing.T) {
	archiveRoot := filepath.Join(t.TempDir(), "orphan-archive")
	_, partDirs, r, stop := setupMeasureOrphanScenario(t, orphanConfig{policy: orphanArchive, rootDir: archiveRoot})
	defer stop()
	defer r.Close()

	// Pre-create every part's would-be .jsonl path as a directory so partWriter's
	// OpenFile fails, turning the archive write into a fatal abort.
	for _, partDir := range partDirs {
		loc := locFromPartDir(t, partDir)
		require.NoError(t, os.MkdirAll(r.archiver.partFilePath(loc), 0o750))
	}

	var sawError bool
	var total partReplayResult
	for _, partDir := range partDirs {
		res, err := r.replayPart(context.TODO(), partDir)
		if err != nil {
			sawError = true
		}
		total.orphanSkipped += res.orphanSkipped
	}
	require.True(t, sawError, "an archive write failure must surface as a non-nil error so the part aborts")
	assert.Equal(t, 0, total.orphanSkipped, "an aborted part must not silently count orphan skips (source is retained)")
}

// TestMeasureOrphanResumeReplaysPartTwice proves a part replayed twice (resume) is
// idempotent: the .jsonl is O_TRUNC-rewritten (one set of rows, not doubled) and
// the manifest totals equal the single-pass values.
func TestMeasureOrphanResumeReplaysPartTwice(t *testing.T) {
	archiveRoot := filepath.Join(t.TempDir(), "orphan-archive")
	_, partDirs, r, stop := setupMeasureOrphanScenario(t, orphanConfig{policy: orphanArchive, rootDir: archiveRoot})
	defer stop()
	defer r.Close()

	first := replayAllParts(t, r, partDirs)
	require.Greater(t, first.orphanSkipped, 0, "first pass must archive orphan rows")
	firstJSONLRows := countArchiveRows(t, archiveRoot)
	firstManifestRows, firstManifestSeries := sumManifest(t, archiveRoot)

	second := replayAllParts(t, r, partDirs)
	secondJSONLRows := countArchiveRows(t, archiveRoot)
	secondManifestRows, secondManifestSeries := sumManifest(t, archiveRoot)

	assert.Equal(t, first.orphanSkipped, second.orphanSkipped, "each pass skips the same orphan rows")
	assert.Equal(t, firstJSONLRows, secondJSONLRows, "the .jsonl must hold exactly one set of rows (O_TRUNC), not doubled")
	assert.Equal(t, firstManifestRows, secondManifestRows, "manifest total_rows must not double on resume")
	assert.Equal(t, firstManifestSeries, secondManifestSeries, "manifest total_series must not double on resume")
	assert.Equal(t, first.orphanSkipped, secondJSONLRows, "archived rows equal the single-pass orphan-skip count")
}

// locFromPartDir derives the archive sourceLoc for a flushed part dir
// (.../seg-X/shard-N/<partID>) the same way replayPart does.
func locFromPartDir(t *testing.T, partDir string) sourceLoc {
	t.Helper()
	partID, _, segmentPath, err := parseReplayPartPath(partDir)
	require.NoError(t, err)
	shardPath := filepath.Dir(partDir)
	return sourceLoc{
		Stage:   orphanRTSrcStage,
		Segment: segmentSuffixFromPath(segmentPath),
		Shard:   shardFromPath(shardPath),
		Part:    fmt.Sprintf("%016x", partID),
	}
}

// countArchiveRows sums the JSONL rows across every archive file under root.
func countArchiveRows(t *testing.T, root string) int {
	t.Helper()
	total := 0
	for _, p := range findFilesWithSuffix(t, root, ".jsonl.gz") {
		total += len(readArchiveRecords(t, p))
	}
	return total
}

// sumManifest sums total_rows and total_series across every manifest under root.
func sumManifest(t *testing.T, root string) (rows, series int) {
	t.Helper()
	for _, p := range findFilesWithSuffix(t, root, "manifest.json") {
		var m manifestFile
		mb, err := os.ReadFile(p)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(mb, &m))
		rows += m.TotalRows
		series += m.TotalSeries
	}
	return rows, series
}
