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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted/internal/nativeice"
)

const (
	// nidx02aEncodedSegmentID and nidx02aEncodedSnapshotID number the generation
	// the native encoder is asked to write. They match the identifiers the
	// compatibility writer gave the checked-in corpus, so the two generations
	// are comparable as generations and not merely as document sets.
	nidx02aEncodedSegmentID  = uint64(2)
	nidx02aEncodedSnapshotID = nidx02aSnapshotID

	// nidx02aEncodedPageSize is the page size both generations are paged at.
	// Two of the corpus's four visible rows fit in a page, so the resume cursor
	// is exercised and the masked row that sorts between the first two visible
	// rows would surface inside the first page.
	nidx02aEncodedPageSize = 2

	// nidx02aLaterSegmentID and nidx02aLaterSnapshotID number the generation the
	// encoder appends beside the compatibility writer's in the mixed-provenance
	// directory. Both are above the corpus's own, so the appended generation is
	// the directory's newest.
	nidx02aLaterSegmentID  = uint64(8)
	nidx02aLaterSnapshotID = uint64(9)
)

// TestNativeEncodedGenerationMatchesCompatibilityFixture is the milestone's
// end-to-end case: the native encoder is handed the documents the checked-in
// corpus was written from, and everything a Property shard reads out of the
// generation it produces must match, row for row, what the same reads return
// from the corpus the compatibility writer produced.
//
// The corpus is the oracle. Its bytes came from pkg/index/inverted.NewStore at
// the dependency set testdata/nidx02a/provenance.json pins by content hash, and
// no reader or writer under test produced them, so an encoder that is
// self-consistently wrong disagrees here.
//
// Requirement proved here:
//
//	R6 -- a generation the native encoder writes from the corpus's input
//	      documents serves the same pinned snapshot identifier, the same
//	      visible document count, the same stored-document walk, the same
//	      exact-term selections and the same ascending repair pages as the
//	      checked-in generation the compatibility writer produced.
func TestNativeEncodedGenerationMatchesCompatibilityFixture(t *testing.T) {
	tester := require.New(t)
	encoded := nidx02aEncodeCorpus(t)

	expectedCount, countErr := ReadOnlyDocCount(nidx02aShardDir)
	tester.NoError(countErr)
	actualCount, actualCountErr := ReadOnlyDocCount(encoded)
	tester.NoError(actualCountErr)
	tester.Equal(expectedCount, actualCount, "the encoded generation must leave the same rows visible")
	tester.Equal(nidx02aVisibleRowCount, actualCount)

	tester.Equal(nidx02aWalk(t, nidx02aShardDir), nidx02aWalk(t, encoded),
		"the encoded generation must yield the corpus's stored-document walk, repeated values included")

	for _, selection := range nidx02aSelections() {
		tester.Equal(nidx02aSelect(t, nidx02aShardDir, selection), nidx02aSelect(t, encoded, selection),
			"the encoded generation must serve the corpus's selection on field %x", selection.Field)
	}

	tester.Equal(nidx02aPages(t, nidx02aShardDir), nidx02aPages(t, encoded),
		"the encoded generation must serve the corpus's ascending repair pages")

	expected := nidx02aOpen(t, nidx02aShardDir)
	actual := nidx02aOpen(t, encoded)
	tester.Equal(expected.SnapshotID(), actual.SnapshotID())
	tester.Equal(nidx02aEncodedSnapshotID, actual.SnapshotID())
}

// TestNativeEncodedGenerationOpensInTheCompatibilityReader hands the encoder's
// output to the reader a rollback would run.
//
// A native generation that only the new reader can open is a generation no
// operator can roll back from, so this is the milestone's compatibility
// requirement rather than a convenience: the bytes must be acceptable to the
// pinned reader both on a first open and across a restart, which is what a
// mixed-version cluster does to them.
//
// Requirement proved here:
//
//	R7 -- the pinned compatibility reader opens a directory the native encoder
//	      wrote, resolves a query against it, and resolves the same query again
//	      after the store is closed and reopened. A row the generation's
//	      deletion masks hide is absent from both.
func TestNativeEncodedGenerationOpensInTheCompatibilityReader(t *testing.T) {
	tester := require.New(t)
	encoded := nidx02aEncodeCorpus(t)

	visible := nidx02aVisibleRows()[0]
	masked := nidx02aRows[1]
	tester.True(masked.masked, "the corpus's second row must be the masked one this case queries for")

	for _, attempt := range []string{"first open", "restart"} {
		store, openErr := NewStore(StoreOpts{Path: encoded})
		tester.NoError(openErr, "the compatibility reader must open the encoded generation on %s", attempt)

		found := nidx02aCompatibilitySearch(t, store, nidx02aDocID(visible))
		tester.Len(found, 1, "the compatibility reader must resolve a live row on %s", attempt)
		tester.Equal(visible.sha, string(found[0].Fields[nidx02aSHAField]),
			"the compatibility reader must read the live row's stored SHA on %s", attempt)

		tester.Empty(nidx02aCompatibilitySearch(t, store, nidx02aDocID(masked)),
			"the compatibility reader must not resolve a masked row on %s", attempt)

		tester.NoError(store.Close())
	}
}

// TestNativeEncodedGenerationJoinsACompatibilityWrittenDirectory appends an
// encoder generation to a directory whose existing generation came from the
// compatibility writer.
//
// This is the shape of a shard mid-rollout: older generations on disk were
// written by the retired writer and the newest by the native encoder, and both
// have to stay readable through one reader. It also covers the rollback
// direction -- when the newest generation is lost, the retired writer's bytes
// must still open exactly as they did before.
//
// Requirement proved here:
//
//	R8 -- encoding into a directory that already holds a committed generation
//	      leaves that generation's bytes intact and publishes the new one as the
//	      directory's newest. Losing the newer generation's manifest returns the
//	      directory to the older generation, whose visible count and
//	      stored-document walk are unchanged.
func TestNativeEncodedGenerationJoinsACompatibilityWrittenDirectory(t *testing.T) {
	tester := require.New(t)
	directory := filepath.Join(t.TempDir(), "shard-0")
	nidx02aCopyDir(t, nidx02aShardDir, directory)

	priorWalk := nidx02aWalk(t, directory)
	priorCount, priorCountErr := ReadOnlyDocCount(directory)
	tester.NoError(priorCountErr)
	tester.Equal(nidx02aVisibleRowCount, priorCount)

	later := nativeice.Generation{
		SegmentID:  nidx02aLaterSegmentID,
		SnapshotID: nidx02aLaterSnapshotID,
		Documents:  []nativeice.EncodeDocument{nidx02aEncodeDocumentOf(nidx02aRows[4])},
	}
	tester.NoError(nativeice.Encode(directory, later))

	appended := nidx02aOpen(t, directory)
	tester.Equal(nidx02aLaterSnapshotID, appended.SnapshotID(), "the appended generation must be the directory's newest")
	appendedCount, appendedCountErr := ReadOnlyDocCount(directory)
	tester.NoError(appendedCountErr)
	tester.Equal(int64(1), appendedCount, "the appended generation holds exactly the one document it was given")

	nidx02aRemoveNewestManifest(t, directory)

	rolledBack := nidx02aOpen(t, directory)
	tester.Equal(nidx02aSnapshotID, rolledBack.SnapshotID(),
		"losing the newer manifest must return the directory to the compatibility writer's generation")
	rolledBackCount, rolledBackCountErr := ReadOnlyDocCount(directory)
	tester.NoError(rolledBackCountErr)
	tester.Equal(priorCount, rolledBackCount)
	tester.Equal(priorWalk, nidx02aWalk(t, directory),
		"the compatibility writer's bytes must read exactly as they did before the encoder wrote beside them")
}

// TestNativeEncoderSurfaceStaysWithinNIDX02A guards the boundary itself rather
// than any behavior behind it.
//
// Requirement proved here:
//
//	R9 -- the milestone adds one encoder and the types its input is expressed
//	      in, and nothing else: no plugin adapter, no merger, no segment
//	      implementation, no lifecycle hook. The sentinel a rejected generation
//	      is classified with stays distinct from the four the package already
//	      publishes, and the native reader package's third-party dependency set
//	      is unchanged, so the encoder reaches no retired index library.
func TestNativeEncoderSurfaceStaysWithinNIDX02A(t *testing.T) {
	tester := require.New(t)

	tester.ErrorIs(nativeice.ErrInvalidGeneration, nativeice.ErrInvalidGeneration)
	tester.NotErrorIs(nativeice.ErrInvalidGeneration, ErrCorruptIndex)
	tester.NotErrorIs(nativeice.ErrInvalidGeneration, ErrNoCommittedIndex)
	tester.NotErrorIs(nativeice.ErrInvalidGeneration, ErrInvalidSelection)
	tester.NotErrorIs(nativeice.ErrInvalidGeneration, ErrInvalidRepairPage)

	tester.Equal(nativeReaderSurface, exportedSurfaceOf(t),
		"the native reader package's exported surface changed; NIDX-02A admits one encoder and its input types")
	assertNativeReaderImportsAreAllowed(t)
}

// nidx02aEncodeCorpus writes the corpus's declared input documents into a fresh
// directory with the native encoder and returns it.
func nidx02aEncodeCorpus(t *testing.T) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "shard-0")
	require.NoError(t, os.MkdirAll(directory, 0o755))
	documents := make([]nativeice.EncodeDocument, 0, len(nidx02aRows))
	for _, row := range nidx02aRows {
		documents = append(documents, nidx02aEncodeDocumentOf(row))
	}
	require.NoError(t, nativeice.Encode(directory, nativeice.Generation{
		SegmentID:  nidx02aEncodedSegmentID,
		SnapshotID: nidx02aEncodedSnapshotID,
		Documents:  documents,
	}))
	return directory
}

// nidx02aEncodeDocumentOf renders one declared row as the document the native
// encoder is handed. It is an independent second mapping of the same corpus:
// nidx02aBatch renders the row for the compatibility writer, this renders it
// for the encoder, and the end-to-end case asserts the two generations read
// alike.
//
// The revision is supplied as the literal encoded value the corpus declares
// rather than as a number, because the encoder applies no numeric coding: a
// term sorts and reads back as exactly the bytes it was handed.
func nidx02aEncodeDocumentOf(row nidx02aRow) nativeice.EncodeDocument {
	fields := []nativeice.EncodeField{
		{Name: nidx02aEntityField, Value: []byte(row.entityID), Index: true, Sort: true},
		{Name: nidx02aGroupField, Value: []byte(row.group), Index: true, Sort: true},
		{Name: nidx02aNameField, Value: []byte(row.name), Index: true, Sort: true},
		{Name: nidx02aTagFieldName(), Value: []byte(row.tag), Index: true, Sort: true},
	}
	for _, source := range row.sources {
		fields = append(fields, nativeice.EncodeField{Name: nidx02aSourceField, Value: []byte(source), Store: true})
	}
	if row.deletedAt > 0 {
		fields = append(fields, nativeice.EncodeField{
			Name: nidx02aDeletedField, Value: convert.Int64ToBytes(row.deletedAt), Store: true,
		})
	}
	fields = append(fields,
		nativeice.EncodeField{Name: nidx02aSHAField, Value: []byte(row.sha), Store: true},
		nativeice.EncodeField{
			Name: nidx02aTimestampField, Value: nidx02aEncodedTimestamps[row.timestamp],
			Index: true, Store: true, Sort: true,
		},
	)
	return nativeice.EncodeDocument{Identifier: []byte(nidx02aDocID(row)), Fields: fields, Deleted: row.masked}
}

// nidx02aSelections are the exact-term selections both generations are asked
// for: a group two visible rows and one masked row recorded, a group one
// visible row recorded, a Property tag under its hashed field name, and a term
// no row recorded.
func nidx02aSelections() []TermSelection {
	return []TermSelection{
		{Field: nidx02aGroupField, Terms: [][]byte{[]byte("g-a")}},
		{Field: nidx02aGroupField, Terms: [][]byte{[]byte("g-b")}},
		{Field: nidx02aGroupField, Terms: [][]byte{[]byte("g-a"), []byte("g-b")}},
		{Field: nidx02aTagFieldName(), Terms: [][]byte{[]byte("red")}},
		{Field: nidx02aTagFieldName(), Terms: [][]byte{[]byte("green")}},
		{Field: nidx02aEntityField, Terms: [][]byte{[]byte("e-1")}},
	}
}

// nidx02aWalk renders every live document of a directory's newest committed
// generation as its stored values, in the order the walk yields them.
func nidx02aWalk(t *testing.T, directory string) []string {
	t.Helper()
	var documents []string
	require.NoError(t, ReadOnlyWalkDocuments(context.Background(), directory, func(document StoredDocument) error {
		documents = append(documents, nidx02aRenderDocument(document))
		return nil
	}))
	return documents
}

// nidx02aSelect renders the live documents one selection holds.
func nidx02aSelect(t *testing.T, directory string, selection TermSelection) []string {
	t.Helper()
	var documents []string
	require.NoError(t, ReadOnlySelectDocuments(context.Background(), directory, selection,
		func(document StoredDocument) error {
			documents = append(documents, nidx02aRenderDocument(document))
			return nil
		}))
	return documents
}

// nidx02aPages renders every ascending repair page of a directory's newest
// committed generation, paged to exhaustion.
func nidx02aPages(t *testing.T, directory string) [][]string {
	t.Helper()
	generation := nidx02aOpen(t, directory)
	var pages [][]string
	request := RepairPageRequest{PageSize: nidx02aEncodedPageSize}
	for {
		rows, pageErr := generation.RepairTuplePage(context.Background(), request)
		require.NoError(t, pageErr)
		if len(rows) == 0 {
			return pages
		}
		rendered := make([]string, 0, len(rows))
		for _, row := range rows {
			components := make([]string, 0, len(row.SortValues)+1)
			for _, value := range row.SortValues {
				components = append(components, fmt.Sprintf("%x", value))
			}
			rendered = append(rendered, strings.Join(components, "|")+"=>"+fmt.Sprintf("%x", row.Value))
		}
		pages = append(pages, rendered)
		request.After = rows[len(rows)-1].Cursor
	}
}

// nidx02aRenderDocument renders one document's stored values as a single
// comparable string, keeping the walk's own order so a collapsed repeated value
// or a reordered field is visible in the difference.
func nidx02aRenderDocument(document StoredDocument) string {
	var values []string
	_ = document.VisitStoredFields(func(name string, value []byte) bool {
		values = append(values, fmt.Sprintf("%x=%x", name, value))
		return true
	})
	return strings.Join(values, " ")
}

// nidx02aOpen pins a directory's newest committed generation for the duration
// of the test.
func nidx02aOpen(t *testing.T, directory string) *ReadOnlyGeneration {
	t.Helper()
	generation, openErr := OpenReadOnlyGeneration(directory)
	require.NoError(t, openErr)
	require.NotNil(t, generation)
	t.Cleanup(func() {
		require.NoError(t, generation.Close())
	})
	return generation
}

// nidx02aCompatibilitySearch resolves one document identifier through the
// pinned compatibility reader.
func nidx02aCompatibilitySearch(t *testing.T, store index.SeriesStore, documentID string) []index.SeriesDocument {
	t.Helper()
	query, queryErr := store.BuildQuery([]index.SeriesMatcher{
		{Match: []byte(documentID), Type: index.SeriesMatcherTypeExact},
	}, nil, nil)
	require.NoError(t, queryErr)
	found, searchErr := store.Search(context.Background(), []index.FieldKey{
		{TagName: nidx02aIDField}, {TagName: nidx02aSHAField}, {TagName: nidx02aSourceField},
	}, query, len(nidx02aRows))
	require.NoError(t, searchErr)
	return found
}

// nidx02aCopyDir copies a checked-in corpus into a writable directory, so a
// case that appends a generation never touches the bytes under testdata.
func nidx02aCopyDir(t *testing.T, source, destination string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(destination, 0o755))
	entries, readErr := os.ReadDir(source)
	require.NoError(t, readErr)
	for _, entry := range entries {
		payload, fileErr := os.ReadFile(filepath.Join(source, entry.Name()))
		require.NoError(t, fileErr)
		require.NoError(t, os.WriteFile(filepath.Join(destination, entry.Name()), payload, 0o600))
	}
}

// nidx02aRemoveNewestManifest models the loss of the newest generation's
// publication while every segment stays on disk, which is the state a rollback
// or an interrupted write leaves a directory in.
func nidx02aRemoveNewestManifest(t *testing.T, directory string) {
	t.Helper()
	entries, readErr := os.ReadDir(directory)
	require.NoError(t, readErr)
	var manifests []string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == snpExt {
			manifests = append(manifests, entry.Name())
		}
	}
	require.NotEmpty(t, manifests, "the directory must hold a published generation to lose")
	sort.Strings(manifests)
	require.NoError(t, os.Remove(filepath.Join(directory, manifests[len(manifests)-1])))
}
