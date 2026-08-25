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

package inverted

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/inverted/internal/icev3"
)

// TestE2EReadOnlyDocCount is the end-to-end contract, and it reproduces the
// operation BanyanDB actually performs: inspecting the document count of a
// series index directory that a live writer owns, the way
// storage.segment.SeriesIndexStats does when a cold segment is reported without
// reopening its writable index.
//
// The pinned legacy writer creates and commits the logical documents doc-11 and
// doc-12 and then stays open, holding the exclusive directory lock, while the
// production seam ReadOnlyDocCount is called against the same directory.
//
// R2 ReadOnlyDocCount returns the visible document count while a legacy writer
// owns the directory lock and the committed generation is visible, and the
// directory it read is unchanged afterwards.
//
// R3 A malformed copy of that same live directory is classified through the
// same seam as a typed corruption rather than as a count.
func TestE2EReadOnlyDocCount(t *testing.T) {
	indexDir := filepath.Join(t.TempDir(), "sidx")
	require.NoError(t, os.MkdirAll(indexDir, 0o750))

	writer, openErr := bluge.OpenWriter(bluge.DefaultConfig(indexDir))
	require.NoError(t, openErr, "failed to open the pinned legacy writer")
	writerClosed := false
	defer func() {
		if !writerClosed {
			_ = writer.Close()
		}
	}()

	batch := bluge.NewBatch()
	for _, docID := range logicalFixtureDocuments {
		doc := bluge.NewDocument(docID)
		doc.AddField(bluge.NewKeywordField("_series_id", "nidx-01a").StoreValue())
		batch.Insert(doc)
	}
	require.NoError(t, writer.Batch(batch))

	require.Eventually(t, func() bool {
		return hasCommittedGeneration(indexDir)
	}, 30*time.Second, 50*time.Millisecond, "the legacy writer published no committed generation")

	// The writer is still open, so the lock file it owns must be present: that
	// is the premise of R2, not an artifact of the read under test.
	require.FileExists(t, filepath.Join(indexDir, LockFilename),
		"the legacy writer must still own the directory lock while the count is read")

	before := publishedInventoryOf(t, indexDir)

	outcome := callReadOnlyDocCount(t, indexDir)

	require.Nil(t, outcome.panicValue, "ReadOnlyDocCount panicked: %v", outcome.panicValue)
	require.NoError(t, outcome.err)
	assert.Equal(t, fixtureVisibleCount, outcome.count,
		"the two committed documents must be visible through the production seam")
	assert.Equal(t, before, publishedInventoryOf(t, indexDir),
		"ReadOnlyDocCount must not change the published files of a live index")
	assert.FileExists(t, filepath.Join(indexDir, LockFilename),
		"ReadOnlyDocCount must leave the writer's lock alone")

	// Copying the live directory's published files, without the writer's lock,
	// is what a snapshot of a segment index contains. A malformed copy of it
	// must classify rather than count.
	malformed := copyPublishedFiles(t, indexDir)
	truncateSegmentFooter(t, malformed)

	malformedOutcome := callReadOnlyDocCount(t, malformed)

	require.Nil(t, malformedOutcome.panicValue, "ReadOnlyDocCount panicked: %v", malformedOutcome.panicValue)
	require.ErrorIs(t, malformedOutcome.err, icev3.ErrCorruptSegment)
	assert.Zero(t, malformedOutcome.count)

	require.NoError(t, writer.Close())
	writerClosed = true

	// The same directory still counts the same once the writer has released it,
	// so the count is a property of the committed generation and not of the
	// writer being open.
	closedOutcome := callReadOnlyDocCount(t, indexDir)
	require.NoError(t, closedOutcome.err)
	assert.Equal(t, fixtureVisibleCount, closedOutcome.count)
}

// hasCommittedGeneration reports whether the directory holds at least one
// published snapshot manifest and one segment file.
func hasCommittedGeneration(dir string) bool {
	manifests, manifestErr := filepath.Glob(filepath.Join(dir, "*.snp"))
	if manifestErr != nil || len(manifests) == 0 {
		return false
	}
	segments, segmentErr := filepath.Glob(filepath.Join(dir, "*.seg"))
	return segmentErr == nil && len(segments) > 0
}

// publishedInventoryOf returns the observable state of the index files a live
// directory has published, excluding the writer's own lock file, which belongs
// to the writer rather than to the read under test.
func publishedInventoryOf(t *testing.T, dir string) []fileFacts {
	t.Helper()
	facts := make([]fileFacts, 0, 4)
	for _, candidate := range inventoryOf(t, dir) {
		switch filepath.Ext(candidate.name) {
		case ".seg", ".snp":
			facts = append(facts, candidate)
		}
	}
	require.NotEmpty(t, facts, "the live index published no files")
	return facts
}

// copyPublishedFiles copies the published index files of dir into a fresh
// temporary directory and returns its path.
func copyPublishedFiles(t *testing.T, dir string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "copy")
	require.NoError(t, os.MkdirAll(dst, 0o750))
	for _, candidate := range publishedInventoryOf(t, dir) {
		content, readErr := os.ReadFile(filepath.Join(dir, candidate.name))
		require.NoError(t, readErr)
		require.NoError(t, os.WriteFile(filepath.Join(dst, candidate.name), content, 0o600))
	}
	return dst
}
