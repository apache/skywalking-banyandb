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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	roaringpkg "github.com/RoaringBitmap/roaring"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/inverted/internal/nativeice"
)

// TestNIDX02BLifecycleE2E is the milestone's end-to-end case: one Property
// shard's worth of documents travels the whole path an index lifecycle manager
// drives a segment plugin through, and the generation that comes out the far
// end must be the generation the shard already has on disk.
//
// The cycle is the real one. A batch of analyzed documents becomes a segment;
// the manager persists that segment through WriteTo and reopens it through
// Load; a later merge compacts it against the deletion masks the shard
// accumulated, and the manager persists the merged segment in its place and
// publishes it. Everything a Property shard reads afterwards -- the visible
// count, the stored-document walk, the exact-term selections, the ascending
// repair pages, and a query through the reader a rollback would run -- must
// match the checked-in NIDX-02A corpus row for row.
//
// The corpus is the oracle and nothing under test produced it: its bytes came
// from pkg/index/inverted.NewStore at the dependency set its provenance
// manifest pins by content hash. A plugin that is self-consistently wrong --
// one whose own Load happily reads its own New's output -- disagrees here.
//
// Requirement proved here:
//
//	R9 -- a generation produced by driving the plugin through build, persist,
//	      reopen, merge under deletion masks, persist and publish serves the
//	      corpus's visible count, stored-document walk, exact-term selections
//	      and ascending repair pages, and the pinned compatibility reader
//	      resolves a live row and refuses a masked one against it, both on a
//	      first open and after a restart.
func TestNIDX02BLifecycleE2E(t *testing.T) {
	tester := require.New(t)

	built, count, newErr := nidx02bNew(nidx02bCorpusDocuments(), nidx02bNormCalc)
	tester.NoError(newErr)
	tester.NotNil(built)
	tester.Equal(uint64(nidx02aPhysicalRowCount), count,
		"the batch holds every declared row, the ones the shard later masked included")

	staging := t.TempDir()
	reopened := nidx02bReopen(t, nidx02bPersist(t, built,
		filepath.Join(staging, fmt.Sprintf("%012x%s", nidx02aEncodedSegmentID, segExt))))
	tester.Equal(uint64(nidx02aPhysicalRowCount), reopened.Count())

	drops := roaringpkg.New()
	for _, documentNumber := range nidx02bMaskedDocumentNumbers() {
		drops.Add(documentNumber)
	}
	tester.Equal(uint64(nidx02aPhysicalRowCount-nidx02aVisibleRowCount), drops.GetCardinality(),
		"the shard masked two of its six rows")

	merger := nidx02bMerge([]segment.Segment{reopened}, []*roaringpkg.Bitmap{drops}, nidx02bMergeBufferSize)
	tester.NotNil(merger)
	mergedBytes := nidx02bPersistMerger(t, merger, filepath.Join(staging, "merged"+segExt))

	published := nidx02bPublish(t, mergedBytes)

	generation := nidx02aOpen(t, published)
	tester.Equal(nidx02aSnapshotID, generation.SnapshotID(),
		"the published generation is the one the corpus's manifest names")

	visibleCount, visibleCountErr := ReadOnlyDocCount(published)
	tester.NoError(visibleCountErr)
	tester.Equal(nidx02aVisibleRowCount, visibleCount,
		"the merged generation leaves exactly the rows the corpus leaves visible")

	tester.Equal(nidx02aWalk(t, nidx02aShardDir), nidx02aWalk(t, published),
		"the merged generation must yield the corpus's stored-document walk, repeated values included")

	for _, selection := range nidx02aSelections() {
		tester.Equal(nidx02aSelect(t, nidx02aShardDir, selection), nidx02aSelect(t, published, selection),
			"the merged generation must serve the corpus's selection on field %x", selection.Field)
	}

	tester.Equal(nidx02aPages(t, nidx02aShardDir), nidx02aPages(t, published),
		"the merged generation must serve the corpus's ascending repair pages")

	visible := nidx02aVisibleRows()[0]
	masked := nidx02aRows[1]
	tester.True(masked.masked, "the corpus's second row must be the masked one this case queries for")
	for _, attempt := range []string{"first open", "restart"} {
		store, openErr := NewStore(StoreOpts{Path: published})
		tester.NoError(openErr, "the compatibility reader must open the published generation on %s", attempt)

		found := nidx02aCompatibilitySearch(t, store, nidx02aDocID(visible))
		tester.Len(found, 1, "the compatibility reader must resolve a live row on %s", attempt)
		tester.Equal(visible.sha, string(found[0].Fields[nidx02aSHAField]),
			"the compatibility reader must read the live row's stored SHA on %s", attempt)

		tester.Empty(nidx02aCompatibilitySearch(t, store, nidx02aDocID(masked)),
			"the compatibility reader must not resolve a masked row on %s", attempt)

		tester.NoError(store.Close())
	}
}

// nidx02bPublish publishes mergedBytes as one committed generation and returns
// the directory holding it.
//
// The manifest is the one the native encoder writes for the same generation,
// because publishing a segment is the lifecycle manager's job rather than the
// plugin's: the plugin owns the segment bytes, and only those bytes are taken
// from it here. The directory's segment file is the plugin's own output, so
// every reading taken from the directory is a reading of what the plugin
// produced.
func nidx02bPublish(t *testing.T, mergedBytes []byte) string {
	t.Helper()
	encoded := filepath.Join(t.TempDir(), "encoded")
	require.NoError(t, nativeice.Encode(encoded,
		nidx02bEncoderGeneration(nidx02aVisibleRows(), nidx02aEncodedSegmentID, nidx02aEncodedSnapshotID)))
	manifest, manifestErr := os.ReadFile(nidx02bSnapshotFile(t, encoded))
	require.NoError(t, manifestErr)

	published := filepath.Join(t.TempDir(), "shard-0")
	require.NoError(t, os.MkdirAll(published, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(published, fmt.Sprintf("%012x%s", nidx02aEncodedSegmentID, segExt)), mergedBytes, 0o600))
	require.NoError(t, os.WriteFile(
		filepath.Join(published, fmt.Sprintf("%012x%s", nidx02aEncodedSnapshotID, snpExt)), manifest, 0o600))
	return published
}
