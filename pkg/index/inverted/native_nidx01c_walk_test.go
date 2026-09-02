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
	"encoding/hex"
	"errors"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

// liveDocumentWalker is the whole of NIDX-01C's contract: one call from an
// index directory path to a streamed visit of every live document the newest
// structurally complete committed generation holds, each exposing its repeated
// raw stored (field name, value) pairs, or to a classified error.
//
// Issue #14010 names one BanyanDB-owned read-only document visitor as the
// milestone's only test seam, and BDB-NIDX-SPEC-001 revision 0.2 NIDX-01
// forbids exposing a general reader beside it. Everything the milestone adds --
// the fields index, the stored chunk table, block decompression, the stored
// meta triples, deletion-aware iteration -- sits behind this signature and
// stays private. The coder is free to move any of it; the coder may not move
// this.
type liveDocumentWalker func(ctx context.Context, path string, visit func(doc StoredDocument) error) error

// nidx01cBoundary binds the contract to the production symbol that satisfies
// it, so a change to ReadOnlyWalkDocuments's signature fails to compile here
// rather than quietly redefining the milestone.
var nidx01cBoundary liveDocumentWalker = ReadOnlyWalkDocuments

// walkedField is one stored value a document handed the walk, with its bytes
// rendered as hexadecimal so a mismatch reads as bytes rather than as mojibake.
type walkedField struct {
	name  string
	value string
}

// walkNIDX01C walks path through the boundary and returns each visited
// document's stored fields, in visit order, together with the walk's error.
func walkNIDX01C(ctx context.Context, path string) ([][]walkedField, error) {
	var documents [][]walkedField
	err := nidx01cBoundary(ctx, path, func(doc StoredDocument) error {
		var fields []walkedField
		if visitErr := doc.VisitStoredFields(func(name string, value []byte) bool {
			fields = append(fields, walkedField{name: name, value: hex.EncodeToString(value)})
			return true
		}); visitErr != nil {
			return visitErr
		}
		documents = append(documents, fields)
		return nil
	})
	return documents, err
}

// declaredFieldsOf renders one declared corpus document as the unordered set of
// stored fields the walk must hand back for it: its identity, its stored
// timestamp and version, and one entry per repeated tag value. Every value here
// comes from issue #14010's declaration or from a BanyanDB encoder, never from
// the reader under test.
func declaredFieldsOf(document nidx01cDocument) []walkedField {
	fields := []walkedField{
		{name: docIDField, value: hex.EncodeToString(document.identity)},
		{name: timestampField, value: document.storedTimestampHex},
		{name: versionField, value: hex.EncodeToString(convert.Int64ToBytes(document.version))},
	}
	for _, tagValue := range document.tagValues {
		fields = append(fields, walkedField{name: nidx01cTagName, value: hex.EncodeToString([]byte(tagValue))})
	}
	return fields
}

// sortedFields orders stored fields by name then value so a document's content
// can be compared without pinning the order the writer happened to lay its
// fields out in. Repeated values of one name keep a stable relative order,
// which valuesOf checks separately.
func sortedFields(fields []walkedField) []walkedField {
	ordered := append([]walkedField(nil), fields...)
	sort.Slice(ordered, func(left, right int) bool {
		if ordered[left].name != ordered[right].name {
			return ordered[left].name < ordered[right].name
		}
		return ordered[left].value < ordered[right].value
	})
	return ordered
}

// valuesOf returns the values a document carried for one field name, in the
// order the walk visited them.
func valuesOf(fields []walkedField, name string) []string {
	var values []string
	for _, field := range fields {
		if field.name == name {
			values = append(values, field.value)
		}
	}
	return values
}

// hexValues renders declared tag values as hexadecimal, in the declared order.
func hexValues(values []string) []string {
	encoded := make([]string, 0, len(values))
	for _, value := range values {
		encoded = append(encoded, hex.EncodeToString([]byte(value)))
	}
	return encoded
}

// liveDeclaredDocuments returns the corpus documents a walk must visit, in
// declaration order, dropping the ones the corpus deletes.
func liveDeclaredDocuments(documents []nidx01cDocument) []nidx01cDocument {
	var live []nidx01cDocument
	for _, document := range documents {
		if !document.deleted {
			live = append(live, document)
		}
	}
	return live
}

// TestNativeStoredDocumentWalkDeclaredDocuments is the boundary contract for
// NIDX-01C. It exercises inverted.ReadOnlyWalkDocuments, and nothing behind it,
// against the checked-in NIDX-01C corpus.
//
// Requirement proved here:
//
//	R1 -- the visitor streams only live documents of the pinned committed
//	      generation and preserves every repeated field name and value byte
//	      sequence exactly. The corpus declares documents 101 and 202 live and
//	      303 deleted, so the walk is exactly 101 then 202; 101's repeated tag
//	      values stay in the declared order blue then green; and every stored
//	      value arrives as the bytes the compatibility writer recorded,
//	      including an identity that is not valid UTF-8.
func TestNativeStoredDocumentWalkDeclaredDocuments(t *testing.T) {
	tester := require.New(t)

	documents, err := walkNIDX01C(context.Background(), nidx01cSourceADir)
	tester.NoError(err)

	declared := liveDeclaredDocuments(nidx01cSourceADocuments)
	tester.Len(documents, len(declared),
		"the walk must visit every live document of the pinned generation and no deleted one")

	deletedIdentities := map[string]string{}
	for _, document := range nidx01cSourceADocuments {
		if document.deleted {
			deletedIdentities[hex.EncodeToString(document.identity)] = document.label
		}
	}

	for position, expected := range declared {
		visited := documents[position]
		tester.Equal(sortedFields(declaredFieldsOf(expected)), sortedFields(visited),
			"document at walk position %d must be the corpus document labeled %s", position, expected.label)
		tester.Equal(hexValues(expected.tagValues), valuesOf(visited, nidx01cTagName),
			"document %s must keep its repeated %q values in the declared order", expected.label, nidx01cTagName)
		for _, identity := range valuesOf(visited, docIDField) {
			label, deleted := deletedIdentities[identity]
			tester.False(deleted, "the walk visited the deleted document labeled %s", label)
		}
	}
}

// TestNativeStoredDocumentWalkSecondSource walks the corpus' second source, the
// one the series-union case reads alongside the first.
//
// Requirement proved here:
//
//	R1 -- a source holding a single live document yields exactly that document,
//	      with the same repeated values as the copy of it in the first source,
//	      so a union reading both sources sees the same document twice rather
//	      than two different ones.
func TestNativeStoredDocumentWalkSecondSource(t *testing.T) {
	tester := require.New(t)

	documents, err := walkNIDX01C(context.Background(), nidx01cSourceBDir)
	tester.NoError(err)

	declared := liveDeclaredDocuments(nidx01cSourceBDocuments)
	tester.Len(documents, len(declared))
	tester.Equal(sortedFields(declaredFieldsOf(declared[0])), sortedFields(documents[0]))
	tester.Equal(hexValues(declared[0].tagValues), valuesOf(documents[0], nidx01cTagName))
}

// TestNativeStoredDocumentWalkStopsOnCancellation cancels the walk from inside
// the first document's callback.
//
// Requirement proved here:
//
//	R4 -- cancellation stops the walk between two documents. The corpus source
//	      holds two live documents; a walk canceled while the first is being
//	      visited never delivers the second and reports the cancellation, which
//	      is also what makes the walk streaming rather than a materialized list.
func TestNativeStoredDocumentWalkStopsOnCancellation(t *testing.T) {
	tester := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	visited := 0
	err := nidx01cBoundary(ctx, nidx01cSourceADir, func(_ StoredDocument) error {
		visited++
		cancel()
		return nil
	})

	tester.ErrorIs(err, context.Canceled, "a canceled walk must report the cancellation")
	tester.Equal(1, visited, "a walk canceled during the first document must not deliver a second one")
}

// TestNativeStoredDocumentWalkStopsOnVisitError returns an error from the
// caller's document callback.
//
// Requirement proved here:
//
//	R4 -- the document callback owns the walk: an error it returns stops the
//	      walk at that document and reaches the caller unchanged, so a source
//	      reader that fails to rebuild one document aborts instead of
//	      publishing a partial destination.
func TestNativeStoredDocumentWalkStopsOnVisitError(t *testing.T) {
	tester := require.New(t)

	rebuildFailure := errors.New("rebuild refused this document")
	visited := 0
	err := nidx01cBoundary(context.Background(), nidx01cSourceADir, func(_ StoredDocument) error {
		visited++
		return rebuildFailure
	})

	tester.ErrorIs(err, rebuildFailure)
	tester.Equal(1, visited, "the walk must stop at the document whose callback failed")
}

// TestNativeStoredDocumentWalkDamagedStoredRegionIsCorrupt walks a copy of the
// corpus whose stored-document region -- the chunk table, the chunk offsets and
// the compressed blocks the fields index root sits above -- has been damaged.
// Counting documents never reads that region, so NIDX-01B reports this
// directory as healthy; walking them cannot.
//
// Requirement proved here:
//
//	R4 -- a truncated stored chunk, an invalid length and a decode-limit
//	      overflow are rejected with the native typed corruption error rather
//	      than a panic, a hang or an unbounded allocation.
func TestNativeStoredDocumentWalkDamagedStoredRegionIsCorrupt(t *testing.T) {
	tester := require.New(t)

	damaged := copyIndexDir(t, nidx01cSourceADir)
	damageStoredDocumentRegion(t, newestSegmentFile(t, damaged))

	visited := 0
	err := nidx01cBoundary(context.Background(), damaged, func(_ StoredDocument) error {
		visited++
		return nil
	})

	tester.ErrorIs(err, ErrCorruptIndex, "damaged stored bytes must be classified as a corrupt index")
	tester.NotErrorIs(err, ErrNoCommittedIndex,
		"a damaged generation is not an absent one; callers classify the two differently")
	tester.Zero(visited, "no document may be handed to the caller out of a damaged stored region")
}

// TestNativeStoredDocumentWalkIgnoresReservedCRC32 rewrites the reserved CRC32
// slot of every file in a copy of the corpus.
//
// Requirement proved here:
//
//	R4 -- CRC32 remains ignored. BDB-NIDX-SPEC-001 revision 0.2 DEC-007 keeps
//	      the historical CRC32 fields as carried bytes that are never
//	      calculated, validated or used to classify corruption, so a walk over
//	      a directory whose CRC32 slots hold values no writer computed returns
//	      exactly the declared documents.
func TestNativeStoredDocumentWalkIgnoresReservedCRC32(t *testing.T) {
	tester := require.New(t)

	rewritten := copyIndexDir(t, nidx01cSourceADir)
	fillReservedCRC32(t, rewritten, []byte{0x01, 0x23, 0x45, 0x67})

	documents, err := walkNIDX01C(context.Background(), rewritten)
	tester.NoError(err)

	declared := liveDeclaredDocuments(nidx01cSourceADocuments)
	tester.Len(documents, len(declared))
	for position, expected := range declared {
		tester.Equal(sortedFields(declaredFieldsOf(expected)), sortedFields(documents[position]))
	}
}

// TestNativeStoredDocumentWalkLeavesDirectoryUnchanged inventories the corpus
// directory before and after a full walk.
//
// Requirement proved here:
//
//	R7 -- the walk writes no bytes. Every entry's name, size, mode,
//	      modification time and content hash is unchanged afterwards, which is
//	      what lets a source read run against a directory another process owns.
func TestNativeStoredDocumentWalkLeavesDirectoryUnchanged(t *testing.T) {
	tester := require.New(t)

	before := dirInventory(t, nidx01cSourceADir)
	documents, err := walkNIDX01C(context.Background(), nidx01cSourceADir)
	tester.NoError(err)
	tester.Len(documents, len(liveDeclaredDocuments(nidx01cSourceADocuments)))
	tester.Equal(before, dirInventory(t, nidx01cSourceADir),
		"a read-only walk must leave every file's bytes, mode and modification time alone")
}

// TestNativeStoredDocumentWalkBesideOpenWriter walks a directory a live writer
// holds open.
//
// Requirement proved here:
//
//	R7 -- the walk takes no exclusive directory lock, so a migration source
//	      read can inspect a segment index whose writer is still open. The
//	      writer's own runtime lock file sits outside the on-disk grammar and
//	      is ignored rather than mistaken for a generation.
func TestNativeStoredDocumentWalkBesideOpenWriter(t *testing.T) {
	tester := require.New(t)

	shared := copyIndexDir(t, nidx01cSourceADir)
	writer, err := NewStore(StoreOpts{Path: shared})
	tester.NoError(err)
	defer func() {
		tester.NoError(writer.Close())
	}()

	documents, walkErr := walkNIDX01C(context.Background(), shared)
	tester.NoError(walkErr)
	tester.Len(documents, len(liveDeclaredDocuments(nidx01cSourceADocuments)),
		"an open writer must not change what the pinned committed generation holds")
}

// TestNativeStoredDocumentWalkNoCommittedGeneration walks a directory that has
// never been flushed.
//
// Requirement proved here:
//
//	R7 -- a source directory holding no committed generation is classified as
//	      absent, not damaged. A fresh segment whose index writer never
//	      received a document is an ordinary state during migration, and the
//	      source readers treat it as an empty source rather than aborting.
func TestNativeStoredDocumentWalkNoCommittedGeneration(t *testing.T) {
	tester := require.New(t)

	visited := 0
	err := nidx01cBoundary(context.Background(), t.TempDir(), func(_ StoredDocument) error {
		visited++
		return nil
	})

	tester.ErrorIs(err, ErrNoCommittedIndex)
	tester.NotErrorIs(err, ErrCorruptIndex,
		"an unflushed index is not a damaged one; callers classify the two differently")
	tester.Zero(visited)
}

// TestNativeStoredDocumentWalkHistoricalCorpusStillReads walks the corpora the
// preceding read-only milestones checked in, which were written by an older
// shape of the store and hold no tag, timestamp or version field at all.
//
// The NIDX-01B corpus is also this milestone's crash case: its newest
// generation references a segment the directory does not hold, which is what a
// directory looks like when a process died between publishing a segment and
// committing the manifest that adopts it. A walk must fall back to the newest
// structurally complete generation and expose exactly that generation's live
// documents.
//
// Requirement proved here:
//
//	R7 -- historical directories still read. The NIDX-01A corpus yields its two
//	      declared documents; the NIDX-01B corpus yields the four live
//	      documents of its complete generation, with the deleted document
//	      absent and the incomplete newer generation's document invisible.
func TestNativeStoredDocumentWalkHistoricalCorpusStillReads(t *testing.T) {
	tester := require.New(t)

	firstCorpus, err := walkNIDX01C(context.Background(), nidx01aIndexDir)
	tester.NoError(err)
	tester.Equal(identityHexOf(nidx01aDocIDs), walkedIdentities(firstCorpus),
		"the NIDX-01A corpus must still expose the documents its manifest declares")

	secondCorpus, err := walkNIDX01C(context.Background(), nidx01bIndexDir)
	tester.NoError(err)
	visibleDocIDs := []uint64{21, 23, 24, 25}
	tester.Equal(identityHexOf(visibleDocIDs), walkedIdentities(secondCorpus),
		"the NIDX-01B corpus must expose the live documents of its newest complete generation: "+
			"doc-%d is deleted and doc-%d belongs to the incomplete newer generation",
		nidx01bDeletedDocID, nidx01bPublishedDocID)
}

// walkedIdentities returns the identity value of each walked document, in walk
// order, sorted so the comparison does not pin the order segments are laid out
// in.
func walkedIdentities(documents [][]walkedField) []string {
	identities := make([]string, 0, len(documents))
	for _, document := range documents {
		identities = append(identities, valuesOf(document, docIDField)...)
	}
	sort.Strings(identities)
	return identities
}

// identityHexOf renders the identities the element-shaped historical corpora
// record for a set of numeric document identifiers, sorted to match
// walkedIdentities.
func identityHexOf(docIDs []uint64) []string {
	identities := make([]string, 0, len(docIDs))
	for _, docID := range docIDs {
		identities = append(identities, hex.EncodeToString(convert.Uint64ToBytes(docID)))
	}
	sort.Strings(identities)
	return identities
}

// TestNativeStoredDocumentWalkBoundarySurface guards the boundary itself rather
// than any behavior behind it.
//
// Requirement proved here:
//
//	R6 -- the milestone is delivered entirely behind
//	      inverted.ReadOnlyWalkDocuments and the StoredDocument it hands out.
//	      The corpus lives where the corpus of every preceding read-only
//	      milestone lives, the two sentinels callers classify with remain the
//	      ones the boundary already publishes, and the private native reader
//	      exports no operation beyond opening a committed generation, counting
//	      it and walking its live documents. An entry appearing there for
//	      dictionaries, term postings, doc values, sorting, search-after or any
//	      writer is the milestone growing surface NIDX-01 explicitly denied it.
func TestNativeStoredDocumentWalkBoundarySurface(t *testing.T) {
	tester := require.New(t)

	tester.NotNil(nidx01cBoundary, "ReadOnlyWalkDocuments must satisfy the live document walker contract")
	tester.ErrorIs(ErrCorruptIndex, ErrCorruptIndex)
	tester.ErrorIs(ErrNoCommittedIndex, ErrNoCommittedIndex)

	for _, source := range []string{nidx01cSourceADir, nidx01cSourceBDir} {
		tester.NotEmpty(dirInventory(t, source), "the NIDX-01C corpus source %s must be checked in", source)
	}
	tester.NotEmpty(dirInventory(t, filepath.Dir(nidx01cManifest)))

	tester.Equal(nativeReaderSurface, exportedSurfaceOf(t, nativeReaderDir),
		"the native reader's exported surface changed; NIDX-01C may only add the live document walk")
}
