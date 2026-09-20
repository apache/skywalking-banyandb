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
	"go/parser"
	"go/token"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	roaringpkg "github.com/RoaringBitmap/roaring"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted/internal/nativeice"
)

// TestNIDX02BPluginBoundary guards the boundary itself rather than any
// behavior behind it.
//
// Requirement proved here:
//
//	R1 -- the milestone adds exactly three entry points, New, Load and Merge,
//	      carrying exactly the signatures an index lifecycle manager's segment
//	      plugin fields are typed as, in one private package that exports
//	      nothing else. Nothing registers them: no production source under
//	      pkg/index/inverted reaches the adapter, so the store's configuration
//	      and behavior are unchanged. The native reader beside it exports
//	      exactly what its own recorded allowlist declares, and keeps the
//	      third-party dependency budget it already had.
//
// The native reader's allowlist may grow, because the adapter reaches the ICE
// v3 grammar through it. Growing it is a deliberate edit to the recorded list
// rather than a side effect: an export that appears without being recorded
// fails here, and so does an entry recorded for an export that never arrived.
func TestNIDX02BPluginBoundary(t *testing.T) {
	tester := require.New(t)

	tester.NotNil(nidx02bNew, "New must satisfy the segment plugin's New contract")
	tester.NotNil(nidx02bLoad, "Load must satisfy the segment plugin's Load contract")
	tester.NotNil(nidx02bMerge, "Merge must satisfy the segment plugin's Merge contract")

	tester.Equal(nidx02bAdapterSurface, nidx02bExportedSurfaceOf(t, nidx02bAdapterDir),
		"the adapter exports the plugin's three entry points and nothing else")

	for _, directory := range []string{".", nativeReaderDir} {
		for _, source := range nidx02bProductionSources(t, directory) {
			tester.NotContains(nidx02bImportPathsOf(t, source), nidx02bAdapterPackagePath,
				"%s reaches the adapter; NIDX-02B registers no plugin and changes no production path", source)
		}
	}

	tester.Equal(nativeReaderSurface, exportedSurfaceOf(t),
		"the native reader exports something its own recorded allowlist does not declare")
	assertNativeReaderImportsAreAllowed(t)
}

// TestNIDX02BNewBuildsASegmentFromAnalyzedDocuments hands the plugin one
// batch's analyzed documents and asks the returned segment every question the
// segment contract defines.
//
// A plugin that only serializes bytes would satisfy the lifecycle manager's
// persist path and fail every caller that reads a segment before it is
// persisted, which is what an in-memory batch is. The documents are written by
// hand here rather than taken from the corpus, and one field is analyzed into
// two terms whose concatenation is neither of them, so a segment that indexed
// a field's whole value instead of its terms resolves the wrong documents.
//
// Requirement proved here:
//
//	R2 -- New reports how many documents it covers and returns a segment that
//	      answers its type, version, size, field set, stored records, term
//	      dictionaries, term matches, doc values and collection statistics from
//	      the documents it was built from, before any persist. The reserved
//	      identifier field names the document and is recorded once.
func TestNIDX02BNewBuildsASegmentFromAnalyzedDocuments(t *testing.T) {
	tester := require.New(t)

	built, count, newErr := nidx02bNew(nidx02bAnalyzedDocuments(), nidx02bNormCalc)
	tester.NoError(newErr)
	tester.NotNil(built)
	tester.Equal(uint64(3), count, "New reports the number of documents the batch held")
	tester.Equal(uint64(3), built.Count())

	tester.Equal(nidx02bSegmentType, built.Type())
	tester.Equal(nidx02bSegmentVersion, built.Version())
	tester.Positive(built.Size(), "a segment holding three documents occupies memory")

	fields := append([]string(nil), built.Fields()...)
	sort.Strings(fields)
	tester.Equal([]string{nidx02bIdentifierField, nidx02bColorField, nidx02bTitleField}, fields,
		"the segment's field set is every name its documents recorded, the color only two of them carry included")

	tester.Equal([]string{
		fmt.Sprintf("%x=%x", nidx02bIdentifierField, "doc-0"),
		fmt.Sprintf("%x=%x", nidx02bColorField, "red"),
		fmt.Sprintf("%x=%x", nidx02bTitleField, "hello world"),
	}, nidx02bRenderSegmentDocuments(t, built)[0],
		"a stored walk yields the identifier first and the remaining names in ascending order")
	tester.Equal([]string{
		fmt.Sprintf("%x=%x", nidx02bIdentifierField, "doc-2"),
		fmt.Sprintf("%x=%x", nidx02bTitleField, "quiet"),
	}, nidx02bRenderSegmentDocuments(t, built)[2])

	tester.Equal([]uint64{0, 1}, nidx02bDocsMatching(t, built, nidx02bTitleField, "hello"))
	tester.Equal([]uint64{0}, nidx02bDocsMatching(t, built, nidx02bTitleField, "world"))
	tester.Empty(nidx02bDocsMatching(t, built, nidx02bTitleField, "hello world"),
		"an analyzed field records its terms, not the value they were analyzed from")
	tester.Equal([]uint64{0, 1}, nidx02bDocsMatching(t, built, nidx02bColorField, "red", "blue"),
		"several terms of one field resolve to the union of their documents")
	tester.Equal([]uint64{2}, nidx02bDocsMatching(t, built, nidx02bIdentifierField, "doc-2"))

	tester.Equal([]string{"hello", "quiet", "there", "world"}, nidx02bDictionaryTerms(t, built, nidx02bTitleField),
		"a field's dictionary holds every term its documents yielded, in ascending order")
	nidx02bAssertPostings(t, built, nidx02bTitleField, "hello", []uint64{0, 1})
	nidx02bAssertPostings(t, built, nidx02bTitleField, "missing", nil)

	tester.Equal([]string{nidx02bColorField + "=red"}, nidx02bDocValues(t, built, 0, nidx02bColorField))
	tester.Empty(nidx02bDocValues(t, built, 2, nidx02bColorField),
		"a document that recorded no color records no color doc value")

	nidx02bAssertCollectionStats(t, built, nidx02bTitleField, 3, 3, 5)
	nidx02bAssertCollectionStats(t, built, nidx02bColorField, 3, 2, 2)
	nidx02bAssertCollectionStats(t, built, nidx02bIdentifierField, 3, 3, 3)
}

// TestNIDX02BPersistedSegmentReopensThroughLoad drives the persist and reopen
// path an index lifecycle manager drives: the segment New built is written
// through WriteTo to the file the manager would create, and the bytes that
// reached that file are handed back to Load.
//
// Requirement proved here:
//
//	R3 -- a segment persisted through WriteTo reopens through Load, and the
//	      reopened segment answers the whole segment contract exactly as the
//	      segment it was persisted from: same count, type, version, time
//	      bounds, field set, stored records, dictionaries, term matches, doc
//	      values and collection statistics.
func TestNIDX02BPersistedSegmentReopensThroughLoad(t *testing.T) {
	tester := require.New(t)

	built, _, newErr := nidx02bNew(nidx02bAnalyzedDocuments(), nidx02bNormCalc)
	tester.NoError(newErr)
	tester.NotNil(built)

	payload := nidx02bPersist(t, built, filepath.Join(t.TempDir(), "000000000001"+segExt))
	tester.NotEmpty(payload, "persisting a segment writes its bytes")
	reopened := nidx02bReopen(t, payload)

	tester.Equal(built.Count(), reopened.Count())
	tester.Equal(built.Type(), reopened.Type())
	tester.Equal(built.Version(), reopened.Version())
	builtStart, builtEnd := built.Timestamp()
	reopenedStart, reopenedEnd := reopened.Timestamp()
	tester.Equal(builtStart, reopenedStart, "a reopened segment reports the time bounds it was persisted with")
	tester.Equal(builtEnd, reopenedEnd)
	tester.Positive(reopened.Size())

	builtFields := append([]string(nil), built.Fields()...)
	reopenedFields := append([]string(nil), reopened.Fields()...)
	sort.Strings(builtFields)
	sort.Strings(reopenedFields)
	tester.Equal(builtFields, reopenedFields)

	tester.Equal(nidx02bRenderSegmentDocuments(t, built), nidx02bRenderSegmentDocuments(t, reopened),
		"a reopened segment yields the stored records it was persisted with")

	for _, field := range []string{nidx02bTitleField, nidx02bColorField, nidx02bIdentifierField} {
		tester.Equal(nidx02bDictionaryTerms(t, built, field), nidx02bDictionaryTerms(t, reopened, field),
			"field %q keeps its dictionary across a persist", field)
		nidx02bAssertStatsMatch(t, built, reopened, field)
	}
	for _, term := range []string{"hello", "world", "there", "quiet", "missing"} {
		tester.Equal(nidx02bDocsMatching(t, built, nidx02bTitleField, term),
			nidx02bDocsMatching(t, reopened, nidx02bTitleField, term),
			"term %q resolves to the same documents after a reopen", term)
	}
	for documentNumber := uint64(0); documentNumber < reopened.Count(); documentNumber++ {
		tester.Equal(nidx02bDocValues(t, built, documentNumber, nidx02bColorField),
			nidx02bDocValues(t, reopened, documentNumber, nidx02bColorField))
	}
}

// TestNIDX02BLoadReopensRetiredWriterSegment hands Load a segment file the
// retired writer produced, taken from the checked-in NIDX-02A corpus.
//
// This is the milestone's backward-compatibility requirement. Every shard on
// disk today holds segments that writer wrote, and a loader that only reads
// back what this milestone's own encoder emits would make those shards
// unreadable the moment the plugin is registered. The corpus is the oracle:
// its bytes were produced by pkg/index/inverted.NewStore at the dependency set
// its provenance manifest pins by content hash, and nothing under test made
// them.
//
// Requirement proved here:
//
//	R4 -- Load reopens a segment written by the retired writer and serves its
//	      six physical documents' stored records and exact-term matches. The
//	      documents the generation's deletion masks hide are physical documents
//	      of the segment, because a deletion mask belongs to the snapshot
//	      manifest and not to the segment.
func TestNIDX02BLoadReopensRetiredWriterSegment(t *testing.T) {
	tester := require.New(t)

	payload, readErr := os.ReadFile(nidx02bSegmentFile(t, nidx02aShardDir))
	tester.NoError(readErr)
	reopened := nidx02bReopen(t, payload)

	tester.Equal(uint64(nidx02aPhysicalRowCount), reopened.Count(),
		"the corpus segment holds every physical row, masked rows included")
	tester.Equal(nidx02bSegmentType, reopened.Type())
	tester.Equal(nidx02bSegmentVersion, reopened.Version())

	tester.Equal(nidx02bExpectedCorpusRecords(nidx02aRows), nidx02bRenderSegmentDocuments(t, reopened),
		"the retired writer's stored records must read back exactly as the rows declare them")

	tester.Equal([]uint64{0, 1, 2, 3}, nidx02bDocsMatching(t, reopened, nidx02aGroupField, "g-a"))
	tester.Equal([]uint64{4, 5}, nidx02bDocsMatching(t, reopened, nidx02aGroupField, "g-b"))
	tester.Equal([]uint64{0, 1, 2}, nidx02bDocsMatching(t, reopened, nidx02aEntityField, "e-1"))
	tester.Equal([]uint64{0, 1, 3, 5}, nidx02bDocsMatching(t, reopened, nidx02aTagFieldName(), "red"))
	tester.Equal([]uint64{2, 4}, nidx02bDocsMatching(t, reopened, nidx02aTagFieldName(), "blue"))
}

// TestNIDX02BMergeUnionsInputsMinusDeletions drives the merge path the
// lifecycle manager drives: two segments and one positional deletion mask
// each, merged into the segment the manager persists in their place.
//
// The corpus rows are split across two inputs so the mapping merge reports is
// per input rather than global, and one row is dropped from each input so a
// merge that honored only the first mask is caught.
//
// Requirement proved here:
//
//	R5 -- a merged segment holds the union of its inputs' documents less the
//	      documents their masks drop, in input order and ascending document
//	      number within an input; its reported mapping gives every surviving
//	      input document its new number, indexed by its old one, and marks a
//	      dropped document with the sentinel the grammar reserves; and the
//	      merged bytes reopen through Load with the same documents.
func TestNIDX02BMergeUnionsInputsMinusDeletions(t *testing.T) {
	tester := require.New(t)

	leading, trailing := nidx02bSplitCorpusSegments(t)
	leadingDrops := roaringpkg.New()
	leadingDrops.Add(1)
	trailingDrops := roaringpkg.New()
	trailingDrops.Add(2)

	merger := nidx02bMerge([]segment.Segment{leading, trailing},
		[]*roaringpkg.Bitmap{leadingDrops, trailingDrops}, nidx02bMergeBufferSize)
	tester.NotNil(merger)
	payload := nidx02bPersistMerger(t, merger, filepath.Join(t.TempDir(), "000000000009"+segExt))

	dropped := uint64(math.MaxInt64)
	tester.Equal([][]uint64{{0, dropped, 1}, {2, 3, dropped}}, merger.DocumentNumbers(),
		"every surviving input document is reported at its old number with the number it took in the merge")

	merged := nidx02bReopen(t, payload)
	tester.Equal(uint64(nidx02aVisibleRowCount), merged.Count(),
		"the merged segment holds the four rows the two masks left")
	tester.Equal(nidx02bExpectedCorpusRecords(nidx02aVisibleRows()), nidx02bRenderSegmentDocuments(t, merged),
		"the merged segment holds its inputs' surviving rows, in input order")

	tester.Equal([]uint64{0, 1, 2}, nidx02bDocsMatching(t, merged, nidx02aGroupField, "g-a"))
	tester.Equal([]uint64{3}, nidx02bDocsMatching(t, merged, nidx02aGroupField, "g-b"))
	tester.Empty(nidx02bDocsMatching(t, merged, nidx02aEntityField, "e-4"),
		"a dropped row leaves no term behind in the merged segment")
}

// TestNIDX02BOutputMatchesTheNativeEncoder compares the bytes the plugin
// persists against the bytes NIDX-02A's encoder writes for the same
// generation.
//
// The issue requires the plugin's output to stay readable by the native reader
// and by the pinned compatibility reader exactly as NIDX-02A's is. Byte
// equality with that encoder is how the requirement is held: NIDX-02A already
// proves those bytes open in both readers and match the corpus row for row, so
// a plugin that emits the same bytes inherits the proof, and one that drifts is
// caught here rather than in a rollback.
//
// Requirement proved here:
//
//	R6 -- the segment file the plugin persists for a document set is byte for
//	      byte the segment file the native encoder writes for the equivalent
//	      generation, both for a segment New built and for a segment Merge
//	      produced.
func TestNIDX02BOutputMatchesTheNativeEncoder(t *testing.T) {
	tester := require.New(t)

	built, _, newErr := nidx02bNew(nidx02bCorpusDocuments(), nidx02bNormCalc)
	tester.NoError(newErr)
	tester.NotNil(built)
	builtBytes := nidx02bPersist(t, built, filepath.Join(t.TempDir(), "00000000000a"+segExt))
	tester.Equal(nidx02bEncoderSegmentBytes(t, nidx02aRows), builtBytes,
		"a segment built from the corpus's documents must be the segment the native encoder writes for them")

	leading, trailing := nidx02bSplitCorpusSegments(t)
	leadingDrops := roaringpkg.New()
	leadingDrops.Add(1)
	trailingDrops := roaringpkg.New()
	trailingDrops.Add(2)
	merger := nidx02bMerge([]segment.Segment{leading, trailing},
		[]*roaringpkg.Bitmap{leadingDrops, trailingDrops}, nidx02bMergeBufferSize)
	mergedBytes := nidx02bPersistMerger(t, merger, filepath.Join(t.TempDir(), "00000000000b"+segExt))
	tester.Equal(nidx02bEncoderSegmentBytes(t, nidx02aVisibleRows()), mergedBytes,
		"a merged segment must be the segment the native encoder writes for the rows that survived")
}

// TestNIDX02BUnpublishedAndDamagedSegmentsAreSafe covers the two states an
// interrupted write leaves behind.
//
// A segment reaches disk before any manifest names it, so a crash between the
// two leaves a complete segment file no generation references; a crash inside
// the write leaves a truncated one. Neither may change what a reader sees, and
// neither may take a reader down.
//
// Requirement proved here:
//
//	R7 -- a persisted segment no manifest references leaves the directory's
//	      committed generation, its visible count and its document walk exactly
//	      as they were, and Load reports bytes truncated by an interrupted
//	      write as an error rather than decoding them.
func TestNIDX02BUnpublishedAndDamagedSegmentsAreSafe(t *testing.T) {
	tester := require.New(t)

	directory := filepath.Join(t.TempDir(), "shard-0")
	nidx02aCopyDir(t, nidx02aShardDir, directory)
	priorWalk := nidx02aWalk(t, directory)
	priorCount, priorCountErr := ReadOnlyDocCount(directory)
	tester.NoError(priorCountErr)
	tester.Equal(nidx02aVisibleRowCount, priorCount)

	built, _, newErr := nidx02bNew(nidx02bCorpusDocuments(), nidx02bNormCalc)
	tester.NoError(newErr)
	tester.NotNil(built)
	payload := nidx02bPersist(t, built, filepath.Join(directory, "00000000000c"+segExt))

	unpublished := nidx02aOpen(t, directory)
	tester.Equal(nidx02aSnapshotID, unpublished.SnapshotID(),
		"a segment no manifest references must not become the directory's newest generation")
	unpublishedCount, unpublishedCountErr := ReadOnlyDocCount(directory)
	tester.NoError(unpublishedCountErr)
	tester.Equal(priorCount, unpublishedCount)
	tester.Equal(priorWalk, nidx02aWalk(t, directory),
		"an unpublished segment must leave the committed generation's documents untouched")

	truncated, truncateErr := nidx02bLoad(segment.NewDataBytes(payload[:len(payload)/2]))
	tester.Error(truncateErr, "a segment truncated by an interrupted write must be reported, not decoded")
	tester.Nil(truncated)

	empty, emptyErr := nidx02bLoad(segment.NewDataBytes(nil))
	tester.Error(emptyErr, "a segment file an interrupted write never filled must be reported")
	tester.Nil(empty)
}

// TestNIDX02BStaysOffTheRetiredIndexLibrary guards the milestone's lexical
// gate.
//
// Requirement proved here:
//
//	R8 -- the adapter's third-party dependencies are the bitmap type the
//	      plugin's masks are expressed in and the neutral segment API alias,
//	      and nothing else; the retired search engine's index package, where
//	      the plugin struct itself lives, is not among them. No tracked Go
//	      source in the repository imports the retired index library.
func TestNIDX02BStaysOffTheRetiredIndexLibrary(t *testing.T) {
	tester := require.New(t)

	sources := nidx02bProductionSources(t, nidx02bAdapterDir)
	tester.NotEmpty(sources, "the adapter package must have source files")
	for _, source := range sources {
		for _, importPath := range nidx02bImportPathsOf(t, source) {
			if isStandardLibraryImport(importPath) || strings.HasPrefix(importPath, banyanDBModulePrefix) {
				continue
			}
			_, allowed := nidx02bAllowedAdapterImports[importPath]
			tester.True(allowed, "%s imports %s outside the adapter's dependency budget", source, importPath)
		}
	}

	for _, source := range nidx02bTrackedGoSources(t) {
		tester.NotContains(nidx02bImportPathsOf(t, source), nidx02bRetiredIndexLibrary,
			"%s imports the retired index library the native encoder replaces", source)
	}
}

// nidx02bSplitCorpusSegments builds the two segments a merge is driven with:
// the corpus's first three rows and its last three, each persisted and
// reopened the way the lifecycle manager holds a segment it is about to merge.
func nidx02bSplitCorpusSegments(t *testing.T) (leading, trailing segment.Segment) {
	t.Helper()
	documents := nidx02bCorpusDocuments()
	directory := t.TempDir()
	return nidx02bSegmentOf(t, documents[:3], filepath.Join(directory, "000000000007"+segExt)),
		nidx02bSegmentOf(t, documents[3:], filepath.Join(directory, "000000000008"+segExt))
}

// nidx02bSegmentOf builds one segment from documents, persists it and returns
// it reopened.
func nidx02bSegmentOf(t *testing.T, documents []segment.Document, path string) segment.Segment {
	t.Helper()
	built, count, newErr := nidx02bNew(documents, nidx02bNormCalc)
	require.NoError(t, newErr)
	require.NotNil(t, built)
	require.Equal(t, uint64(len(documents)), count)
	return nidx02bReopen(t, nidx02bPersist(t, built, path))
}

// nidx02bEncoderSegmentBytes writes rows as one generation with the native
// encoder and returns the segment file it produced.
func nidx02bEncoderSegmentBytes(t *testing.T, rows []nidx02aRow) []byte {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "encoded")
	require.NoError(t, nativeice.Encode(directory,
		nidx02bEncoderGeneration(rows, nidx02aEncodedSegmentID, nidx02aEncodedSnapshotID)))
	payload, readErr := os.ReadFile(nidx02bSegmentFile(t, directory))
	require.NoError(t, readErr)
	return payload
}

// nidx02bExpectedCorpusRecords renders the stored record every declared row
// must read back as, derived from the rows rather than from any encoder.
//
// A stored walk yields the identifier first and the remaining names in
// ascending byte order, which puts the Property deletion marker before the SHA
// value, the SHA value before the sources, and the sources before the
// timestamp. A row's repeated source values stay in the order the row lists
// them.
func nidx02bExpectedCorpusRecords(rows []nidx02aRow) []string {
	records := make([]string, 0, len(rows))
	for _, row := range rows {
		values := []string{fmt.Sprintf("%x=%x", nidx02bIdentifierField, nidx02aDocID(row))}
		if row.deletedAt > 0 {
			values = append(values, fmt.Sprintf("%x=%x", nidx02aDeletedField, convert.Int64ToBytes(row.deletedAt)))
		}
		values = append(values, fmt.Sprintf("%x=%x", nidx02aSHAField, row.sha))
		for _, source := range row.sources {
			values = append(values, fmt.Sprintf("%x=%x", nidx02aSourceField, source))
		}
		values = append(values, fmt.Sprintf("%x=%x", nidx02aTimestampField, nidx02aEncodedTimestamps[row.timestamp]))
		records = append(records, strings.Join(values, " "))
	}
	return records
}

// nidx02bAssertPostings checks the documents one term's postings list holds.
func nidx02bAssertPostings(t *testing.T, seg segment.Segment, field, term string, expected []uint64) {
	t.Helper()
	dictionary, dictionaryErr := seg.Dictionary(field)
	require.NoError(t, dictionaryErr)
	require.NotNil(t, dictionary)

	contains, containsErr := dictionary.Contains([]byte(term))
	require.NoError(t, containsErr)
	require.Equal(t, len(expected) > 0, contains, "Contains must agree with the postings list for %q", term)

	postings, postingsErr := dictionary.PostingsList([]byte(term), nil, nil)
	require.NoError(t, postingsErr)
	require.NotNil(t, postings)
	require.Equal(t, uint64(len(expected)), postings.Count(), "term %q holds %d documents", term, len(expected))

	iterator, iteratorErr := postings.Iterator(false, false, false, nil)
	require.NoError(t, iteratorErr)
	var numbers []uint64
	for {
		posting, nextErr := iterator.Next()
		require.NoError(t, nextErr)
		if posting == nil {
			break
		}
		numbers = append(numbers, posting.Number())
	}
	require.NoError(t, iterator.Close())
	require.Equal(t, expected, numbers, "term %q resolves to these document numbers", term)
	require.NoError(t, dictionary.Close())
}

// nidx02bAssertCollectionStats checks one field's collection statistics.
func nidx02bAssertCollectionStats(t *testing.T, seg segment.Segment, field string, total, documents, frequency uint64) {
	t.Helper()
	stats, statsErr := seg.CollectionStats(field)
	require.NoError(t, statsErr)
	require.NotNil(t, stats)
	require.Equal(t, total, stats.TotalDocumentCount(), "field %q spans every document of the segment", field)
	require.Equal(t, documents, stats.DocumentCount(), "field %q is recorded by this many documents", field)
	require.Equal(t, frequency, stats.SumTotalTermFrequency(), "field %q recorded this many term occurrences", field)
}

// nidx02bAssertStatsMatch checks that two segments report one field alike.
func nidx02bAssertStatsMatch(t *testing.T, left, right segment.Segment, field string) {
	t.Helper()
	leftStats, leftErr := left.CollectionStats(field)
	require.NoError(t, leftErr)
	rightStats, rightErr := right.CollectionStats(field)
	require.NoError(t, rightErr)
	require.Equal(t, leftStats.TotalDocumentCount(), rightStats.TotalDocumentCount(), "field %q", field)
	require.Equal(t, leftStats.DocumentCount(), rightStats.DocumentCount(), "field %q", field)
	require.Equal(t, leftStats.SumTotalTermFrequency(), rightStats.SumTotalTermFrequency(), "field %q", field)
}

// nidx02bExportedSurfaceOf lists the exported top-level identifiers and
// exported methods on exported types the non-test sources of a package
// declare, sorted.
func nidx02bExportedSurfaceOf(t *testing.T, directory string) []string {
	t.Helper()
	var surface []string
	for _, source := range nidx02bProductionSources(t, directory) {
		fileSet := token.NewFileSet()
		file, parseErr := parser.ParseFile(fileSet, source, nil, parser.SkipObjectResolution)
		require.NoError(t, parseErr)
		surface = append(surface, exportedNamesIn(file)...)
	}
	sort.Strings(surface)
	return surface
}

// nidx02bProductionSources lists the non-test Go sources one directory holds.
func nidx02bProductionSources(t *testing.T, directory string) []string {
	t.Helper()
	entries, readErr := os.ReadDir(directory)
	require.NoError(t, readErr, "the package at %s must exist", directory)
	var sources []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		sources = append(sources, filepath.Join(directory, name))
	}
	sort.Strings(sources)
	return sources
}

// nidx02bImportPathsOf lists the package paths one Go source imports.
func nidx02bImportPathsOf(t *testing.T, source string) []string {
	t.Helper()
	fileSet := token.NewFileSet()
	file, parseErr := parser.ParseFile(fileSet, source, nil, parser.ImportsOnly)
	require.NoError(t, parseErr)
	paths := make([]string, 0, len(file.Imports))
	for _, spec := range file.Imports {
		importPath, unquoteErr := strconv.Unquote(spec.Path.Value)
		require.NoError(t, unquoteErr)
		paths = append(paths, importPath)
	}
	return paths
}

// nidx02bTrackedGoSources lists every Go source the repository tracks, so the
// lexical gate is measured over the whole module rather than one package.
func nidx02bTrackedGoSources(t *testing.T) []string {
	t.Helper()
	root := filepath.Join("..", "..", "..")
	var sources []string
	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "vendor", "testdata", "dist":
				return fs.SkipDir
			default:
				return nil
			}
		}
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, ".pb.go") {
			sources = append(sources, path)
		}
		return nil
	})
	require.NoError(t, walkErr)
	require.NotEmpty(t, sources)
	return sources
}
