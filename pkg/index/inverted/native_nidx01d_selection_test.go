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
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

// selectedDocumentWalker is the whole of NIDX-01D's contract: one call from an
// index directory path plus one bounded term selection to a streamed visit of
// the live documents that selection holds, each exposing its repeated raw
// stored (field name, value) pairs, or to a classified error.
//
// Issue #14011 permits the native visitor exactly one new input -- "field
// equals any of these literal terms" -- and forbids exposing a general query
// language, FST type, posting iterator or analyzer beside it. Everything the
// milestone adds behind this signature -- the ICE term dictionary, the posting
// records, the union, the deletion mask intersection -- stays private. The
// coder is free to move any of it; the coder may not move this.
type selectedDocumentWalker func(ctx context.Context, path string, selection TermSelection, visit func(doc StoredDocument) error) error

// nidx01dBoundary binds the contract to the production symbol that satisfies
// it, so a change to ReadOnlySelectDocuments's signature fails to compile here
// rather than quietly redefining the milestone.
var nidx01dBoundary selectedDocumentWalker = ReadOnlySelectDocuments

// The identity byte strings below are issue #14010's declaration of the
// NIDX-01C corpus, restated here as the terms NIDX-01D selects on. They are the
// corpus's own labels rather than anything a reader computes: 101 and 202 are
// live in sourceA's pinned generation and 303 is covered by its deletion mask.
// nidx01dAbsentIdentity is a byte string the corpus declares for no document,
// so no dictionary in it holds that term.
var (
	nidx01dIdentity101    = []byte{0x01, 0x02, 0x03}
	nidx01dIdentity202    = []byte{0x04, 0x05, 0x06}
	nidx01dIdentity303    = []byte{0x07, 0x08, 0x09}
	nidx01dAbsentIdentity = []byte{0xAA, 0xBB, 0xCC}
)

// nidx01dOversizedTermCount and nidx01dOversizedTermLength are far past any
// bound a read-only reader could reasonably serve, so a rejection at these
// sizes proves a bound exists without pinning the coder to a particular one.
const (
	nidx01dOversizedTermCount  = 1 << 20
	nidx01dOversizedTermLength = 1 << 20

	// nidx01cUnselectedOrdinal is the local document number of the corpus
	// document 040506, which issue #14010 declares second in sourceA. It is
	// the document the "filter precedes decode" case damages while selecting
	// the first one.
	nidx01cUnselectedOrdinal = uint64(1)

	// storedDocumentOffsetWidth is the width BDB-NIDX-SPEC-001 revision 0.2
	// section 08 gives each entry of a segment's stored-document offset index.
	storedDocumentOffsetWidth = uint64(8)
)

// identitySelection builds the selection that asks for the documents whose
// identity field records any of the given identities.
func identitySelection(identities ...[]byte) TermSelection {
	return TermSelection{Field: docIDField, Terms: identities}
}

// selectNIDX01D reads path through the boundary and returns each visited
// document's stored fields, in visit order, together with the read's error.
func selectNIDX01D(ctx context.Context, path string, selection TermSelection) ([][]walkedField, error) {
	var documents [][]walkedField
	err := nidx01dBoundary(ctx, path, selection, func(doc StoredDocument) error {
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

// declaredDocumentOf returns the corpus document issue #14010 declares under
// the given identity.
func declaredDocumentOf(t *testing.T, identity []byte) nidx01cDocument {
	t.Helper()
	for _, document := range nidx01cSourceADocuments {
		if hex.EncodeToString(document.identity) == hex.EncodeToString(identity) {
			return document
		}
	}
	t.Fatalf("the NIDX-01C corpus declares no document with identity %x", identity)
	return nidx01cDocument{}
}

// TestNativeExactTermsSelectsOneDeclaredDocument selects a single exact term.
//
// Requirement proved here:
//
//	R1 -- an exact lookup resolves only the requested field and term. The
//	      NIDX-01C corpus declares three documents under the identities
//	      010203, 040506 and 070809; asking for 010203 alone yields exactly
//	      document 101, with every repeated stored value the compatibility
//	      writer recorded for it, and yields neither of the other two.
func TestNativeExactTermsSelectsOneDeclaredDocument(t *testing.T) {
	tester := require.New(t)

	documents, err := selectNIDX01D(context.Background(), nidx01cSourceADir, identitySelection(nidx01dIdentity101))
	tester.NoError(err)

	tester.Len(documents, 1, "identity 010203 is declared by exactly one corpus document")
	tester.Equal(sortedFields(declaredFieldsOf(declaredDocumentOf(t, nidx01dIdentity101))), sortedFields(documents[0]))
	tester.Equal(hexValues([]string{"blue", "green"}), valuesOf(documents[0], nidx01cTagName),
		"a selected document must preserve its repeated tag values in the declared order")
}

// TestNativeExactTermsUnionsTwoDeclaredTerms selects two exact terms at once.
//
// Requirement proved here:
//
//	R1 -- an exact-term OR unions the requested postings and visits each live
//	      document once. Asking for identities 010203 and 040506 together
//	      yields documents 101 and 202, in ascending local document order, and
//	      each exactly once.
func TestNativeExactTermsUnionsTwoDeclaredTerms(t *testing.T) {
	tester := require.New(t)

	documents, err := selectNIDX01D(context.Background(), nidx01cSourceADir,
		identitySelection(nidx01dIdentity101, nidx01dIdentity202))
	tester.NoError(err)

	tester.Len(documents, 2, "the union of two declared identities is two documents, neither repeated")
	tester.Equal([]string{
		hex.EncodeToString(nidx01dIdentity101),
		hex.EncodeToString(nidx01dIdentity202),
	}, []string{
		valuesOf(documents[0], docIDField)[0],
		valuesOf(documents[1], docIDField)[0],
	}, "a union must stream in ascending local document order, which is the corpus's declaration order")
	tester.Equal(sortedFields(declaredFieldsOf(declaredDocumentOf(t, nidx01dIdentity202))), sortedFields(documents[1]))
}

// TestNativeExactTermsRepeatedTermVisitsDocumentOnce asks for the same term
// twice in one selection.
//
// Requirement proved here:
//
//	R1 -- a document several of the selection's terms cover is visited once,
//	      not once per covering term. The union is over documents, not over
//	      postings entries.
func TestNativeExactTermsRepeatedTermVisitsDocumentOnce(t *testing.T) {
	tester := require.New(t)

	documents, err := selectNIDX01D(context.Background(), nidx01cSourceADir,
		identitySelection(nidx01dIdentity101, nidx01dIdentity101))
	tester.NoError(err)

	tester.Len(documents, 1, "a document two identical terms select must be visited once")
}

// TestNativeExactTermsSkipsDeletedDocument selects a term whose only document
// the pinned generation deletes.
//
// Requirement proved here:
//
//	R1 -- the selection applies the pinned snapshot's deletion masks. Issue
//	      #14010 declares document 303, identity 070809, deleted in sourceA's
//	      selected generation. Its term is still in the corpus's dictionary, so
//	      a selection that ignored deletions would return it; asking for 070809
//	      beside the live 010203 must yield document 101 alone.
func TestNativeExactTermsSkipsDeletedDocument(t *testing.T) {
	tester := require.New(t)

	deletedOnly, err := selectNIDX01D(context.Background(), nidx01cSourceADir, identitySelection(nidx01dIdentity303))
	tester.NoError(err, "a term whose documents are all deleted is an empty result, not a failure")
	tester.Empty(deletedOnly)

	documents, err := selectNIDX01D(context.Background(), nidx01cSourceADir,
		identitySelection(nidx01dIdentity101, nidx01dIdentity303))
	tester.NoError(err)
	tester.Len(documents, 1)
	tester.Equal(hex.EncodeToString(nidx01dIdentity101), valuesOf(documents[0], docIDField)[0])
}

// TestNativeExactTermsUnknownTermSelectsNothing asks for a term no dictionary
// in the corpus holds, and for no term at all.
//
// Requirement proved here:
//
//	R1 -- a term the dictionary does not hold, an empty term set and an
//	      unknown field each select no document and are not failures. Resolving
//	      a schema kind that no catalog document carries is an ordinary
//	      migration state, not an error the caller must classify.
func TestNativeExactTermsUnknownTermSelectsNothing(t *testing.T) {
	tester := require.New(t)

	for _, selection := range []TermSelection{
		identitySelection(nidx01dAbsentIdentity),
		identitySelection(),
		{Field: "_a_field_the_corpus_never_wrote", Terms: [][]byte{nidx01dIdentity101}},
	} {
		documents, err := selectNIDX01D(context.Background(), nidx01cSourceADir, selection)
		tester.NoError(err, "selection %+v must be an empty result rather than a failure", selection)
		tester.Empty(documents)
	}
}

// TestNativeExactTermsFilterPrecedesStoredDecode damages the stored record of a
// document the selection excludes.
//
// Requirement proved here:
//
//	R3 -- filtering happens before stored-field decode. Document 202's stored
//	      record is made unreadable while document 101's is left intact;
//	      selecting 101 must still succeed, because a selection that decoded
//	      stored fields before filtering would trip over 202. Selecting 202
//	      itself must then report the typed corruption error, so the success of
//	      the first read is filtering rather than damage the reader ignores.
func TestNativeExactTermsFilterPrecedesStoredDecode(t *testing.T) {
	tester := require.New(t)

	damaged := copyIndexDir(t, nidx01cSourceADir)
	damageStoredDocumentOffset(t, newestSegmentFile(t, damaged), nidx01cUnselectedOrdinal)

	documents, err := selectNIDX01D(context.Background(), damaged, identitySelection(nidx01dIdentity101))
	tester.NoError(err, "a damaged document the selection excludes must never be decoded")
	tester.Len(documents, 1)
	tester.Equal(sortedFields(declaredFieldsOf(declaredDocumentOf(t, nidx01dIdentity101))), sortedFields(documents[0]))

	visited := 0
	selectErr := nidx01dBoundary(context.Background(), damaged, identitySelection(nidx01dIdentity202),
		func(_ StoredDocument) error {
			visited++
			return nil
		})
	tester.ErrorIs(selectErr, ErrCorruptIndex, "selecting the damaged document must report the typed corruption error")
	tester.Zero(visited)
}

// TestNativeExactTermsRejectsUnboundedSelection asks for selections outside any
// bound a read-only reader serves.
//
// Requirement proved here:
//
//	R4 -- term count and term length are bounded, and a selection past those
//	      bounds is rejected with a typed error before any document is visited
//	      rather than being served by an unbounded allocation. A selection
//	      naming no field is rejected the same way: match-all is
//	      ReadOnlyWalkDocuments, not an empty field name.
func TestNativeExactTermsRejectsUnboundedSelection(t *testing.T) {
	tester := require.New(t)

	tooManyTerms := make([][]byte, nidx01dOversizedTermCount)
	for termIndex := range tooManyTerms {
		tooManyTerms[termIndex] = convert.Uint64ToBytes(uint64(termIndex))
	}

	for name, selection := range map[string]TermSelection{
		"no field":       {Terms: [][]byte{nidx01dIdentity101}},
		"too many terms": {Field: docIDField, Terms: tooManyTerms},
		"term too long":  {Field: docIDField, Terms: [][]byte{make([]byte, nidx01dOversizedTermLength)}},
	} {
		visited := 0
		err := nidx01dBoundary(context.Background(), nidx01cSourceADir, selection, func(_ StoredDocument) error {
			visited++
			return nil
		})
		tester.ErrorIs(err, ErrInvalidSelection, "selection %q must be rejected as out of bounds", name)
		tester.NotErrorIs(err, ErrCorruptIndex,
			"selection %q asks for more than the reader serves; the corpus is not damaged", name)
		tester.Zero(visited, "selection %q must be rejected before any document is visited", name)
	}
}

// TestNativeExactTermsStopsOnCancellation cancels the context before reading.
//
// Requirement proved here:
//
//	R4 -- a selection is cancellable. A caller that abandons a schema walk --
//	      a migration aborted by its operator -- stops the read rather than
//	      waiting for the pinned generation to drain.
func TestNativeExactTermsStopsOnCancellation(t *testing.T) {
	tester := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	visited := 0
	err := nidx01dBoundary(ctx, nidx01cSourceADir, identitySelection(nidx01dIdentity101, nidx01dIdentity202),
		func(_ StoredDocument) error {
			visited++
			return nil
		})

	tester.ErrorIs(err, context.Canceled)
	tester.Zero(visited)
}

// TestNativeExactTermsStopsOnVisitError returns an error from the callback.
//
// Requirement proved here:
//
//	R4 -- an error the caller's callback returns stops the read and reaches
//	      the caller unwrapped, so a loader that fails on one document's
//	      contents does not have its own error reclassified as a corrupt index.
func TestNativeExactTermsStopsOnVisitError(t *testing.T) {
	tester := require.New(t)

	callerErr := errors.New("schema loader refused this document")
	visited := 0
	err := nidx01dBoundary(context.Background(), nidx01cSourceADir,
		identitySelection(nidx01dIdentity101, nidx01dIdentity202), func(_ StoredDocument) error {
			visited++
			return callerErr
		})

	tester.ErrorIs(err, callerErr)
	tester.NotErrorIs(err, ErrCorruptIndex)
	tester.Equal(1, visited, "the read must stop at the first document the caller rejects")
}

// TestNativeExactTermsIgnoresReservedCRC32 rewrites the reserved CRC32 slot of
// every file in a copy of the corpus and then selects.
//
// Requirement proved here:
//
//	R4 -- CRC32 remains ignored. BDB-NIDX-SPEC-001 revision 0.2 DEC-007 keeps
//	      the historical CRC32 fields as carried bytes that are never
//	      calculated, validated or used to classify corruption, so a selection
//	      over a directory whose CRC32 slots hold values no writer computed
//	      returns exactly the declared document.
func TestNativeExactTermsIgnoresReservedCRC32(t *testing.T) {
	tester := require.New(t)

	rewritten := copyIndexDir(t, nidx01cSourceADir)
	fillReservedCRC32(t, rewritten, []byte{0x01, 0x23, 0x45, 0x67})

	documents, err := selectNIDX01D(context.Background(), rewritten, identitySelection(nidx01dIdentity101))
	tester.NoError(err)
	tester.Len(documents, 1)
	tester.Equal(sortedFields(declaredFieldsOf(declaredDocumentOf(t, nidx01dIdentity101))), sortedFields(documents[0]))
}

// TestNativeExactTermsLeavesDirectoryUnchanged inventories the corpus directory
// before and after a selection.
//
// Requirement proved here:
//
//	R4 -- resolving a dictionary and decoding postings introduces no reader
//	      lock and no directory mutation. Every entry's name, size, mode,
//	      modification time and content hash is unchanged afterwards, and a
//	      directory a live writer holds open still serves its pinned
//	      generation, which is what lets a schema walk run against a live
//	      catalog mount.
func TestNativeExactTermsLeavesDirectoryUnchanged(t *testing.T) {
	tester := require.New(t)

	before := dirInventory(t, nidx01cSourceADir)
	documents, err := selectNIDX01D(context.Background(), nidx01cSourceADir, identitySelection(nidx01dIdentity101))
	tester.NoError(err)
	tester.Len(documents, 1)
	tester.Equal(before, dirInventory(t, nidx01cSourceADir),
		"a read-only selection must leave every file's bytes, mode and modification time alone")

	shared := copyIndexDir(t, nidx01cSourceADir)
	writer, err := NewStore(StoreOpts{Path: shared})
	tester.NoError(err)
	defer func() {
		tester.NoError(writer.Close())
	}()

	beside, besideErr := selectNIDX01D(context.Background(), shared, identitySelection(nidx01dIdentity101))
	tester.NoError(besideErr)
	tester.Len(beside, 1, "an open writer must not change what the pinned committed generation selects")
}

// TestNativeExactTermsHistoricalCorpusStillReads selects against the corpora
// the preceding read-only milestones checked in.
//
// The NIDX-01B corpus is also this milestone's crash case: its newest
// generation references a segment the directory does not hold, which is what a
// directory looks like when a process died between publishing a segment and
// committing the manifest that adopts it. A selection must resolve against the
// newest structurally complete generation instead, so the document that exists
// only in the incomplete newer generation stays unselectable and the document
// that generation deletes stays absent.
//
// Requirement proved here:
//
//	R6 -- historical directories still select. Every corpus written by an
//	      older shape of the store answers exact terms over the field its own
//	      writer indexed, and a generation left incomplete by a crash is
//	      skipped rather than half-read.
func TestNativeExactTermsHistoricalCorpusStillReads(t *testing.T) {
	tester := require.New(t)

	for _, docID := range nidx01aDocIDs {
		documents, err := selectNIDX01D(context.Background(), nidx01aIndexDir,
			identitySelection(convert.Uint64ToBytes(docID)))
		tester.NoError(err)
		tester.Len(documents, 1, "the NIDX-01A corpus must still resolve doc-%d by exact term", docID)
		tester.Equal(hex.EncodeToString(convert.Uint64ToBytes(docID)), valuesOf(documents[0], docIDField)[0])
	}

	live, err := selectNIDX01D(context.Background(), nidx01bIndexDir,
		identitySelection(convert.Uint64ToBytes(nidx01bFirstBatch[0])))
	tester.NoError(err)
	tester.Len(live, 1, "the NIDX-01B corpus must resolve a live document of its newest complete generation")

	for _, absent := range []uint64{nidx01bDeletedDocID, nidx01bPublishedDocID} {
		documents, absentErr := selectNIDX01D(context.Background(), nidx01bIndexDir,
			identitySelection(convert.Uint64ToBytes(absent)))
		tester.NoError(absentErr)
		tester.Empty(documents,
			"doc-%d is deleted in, or published only after, the newest structurally complete generation",
			absent)
	}
}

// TestNativeExactTermsClassifiesAbsentAndDamagedDirectories selects against a
// directory holding no committed generation, and against one whose committed
// bytes are damaged.
//
// Requirement proved here:
//
//	R4 -- the two failures a caller must distinguish stay distinguishable
//	      through the selection path. A catalog that was never flushed is
//	      absent, not damaged, and a damaged one hands out no document.
func TestNativeExactTermsClassifiesAbsentAndDamagedDirectories(t *testing.T) {
	tester := require.New(t)

	absentErr := nidx01dBoundary(context.Background(), t.TempDir(), identitySelection(nidx01dIdentity101),
		func(_ StoredDocument) error { return nil })
	tester.ErrorIs(absentErr, ErrNoCommittedIndex)
	tester.NotErrorIs(absentErr, ErrCorruptIndex)

	damaged := copyIndexDir(t, nidx01cSourceADir)
	damageStoredDocumentRegion(t, newestSegmentFile(t, damaged))

	visited := 0
	damagedErr := nidx01dBoundary(context.Background(), damaged, identitySelection(nidx01dIdentity101),
		func(_ StoredDocument) error {
			visited++
			return nil
		})
	tester.ErrorIs(damagedErr, ErrCorruptIndex)
	tester.NotErrorIs(damagedErr, ErrNoCommittedIndex)
	tester.Zero(visited)
}

// TestNativeExactTermsBoundarySurface guards the boundary itself rather than
// any behavior behind it.
//
// Requirement proved here:
//
//	R5 -- the milestone is delivered entirely behind
//	      inverted.ReadOnlySelectDocuments and the TermSelection it accepts.
//	      The selection carries one field and a set of literal terms and
//	      nothing else, the sentinels callers classify with remain the ones the
//	      boundary publishes, and the private native reader exports no
//	      operation beyond opening a committed generation, counting it, walking
//	      its live documents and walking the ones a term selection holds. An
//	      entry appearing there for dictionaries, posting iterators, doc
//	      values, ranges, prefixes, wildcards, analyzers, sorting,
//	      search-after or any writer is the milestone growing surface NIDX-01
//	      explicitly denied it.
func TestNativeExactTermsBoundarySurface(t *testing.T) {
	tester := require.New(t)

	tester.NotNil(nidx01dBoundary, "ReadOnlySelectDocuments must satisfy the selected document walker contract")
	tester.ErrorIs(ErrInvalidSelection, ErrInvalidSelection)
	tester.NotErrorIs(ErrInvalidSelection, ErrCorruptIndex,
		"an out-of-bounds request and damaged committed bytes must stay separately classifiable")
	tester.NotErrorIs(ErrInvalidSelection, ErrNoCommittedIndex)

	tester.Equal([]string{"Field:string", "Terms:[][]uint8"}, selectionShape(),
		"TermSelection is one field name and its literal terms; anything more is a query language")

	tester.Equal(nativeReaderSurface, exportedSurfaceOf(t, nativeReaderDir),
		"the native reader's exported surface changed; NIDX-01D may only add the exact-term selection")
}

// damageStoredDocumentOffset makes one document's stored record unreachable by
// pointing its entry in the segment's stored-document offset index past the end
// of the chunk that holds it, leaving every other document's record intact. It
// is the smallest damage that is invisible until the named document is decoded,
// which is what makes "filtering precedes decoding" observable.
func damageStoredDocumentOffset(t *testing.T, segmentPath string, documentNumber uint64) {
	t.Helper()
	payload, err := os.ReadFile(segmentPath)
	require.NoError(t, err)
	require.Greater(t, len(payload), iceFooterLength)
	footer := payload[len(payload)-iceFooterLength:]
	storedIndex := binary.BigEndian.Uint64(footer[iceFooterStoredIndexStart:iceFooterStoredIndexEnd])
	require.Positive(t, storedIndex, "segment %s holds no stored document index to damage", segmentPath)
	entry := storedIndex + documentNumber*storedDocumentOffsetWidth
	require.LessOrEqual(t, entry+storedDocumentOffsetWidth, uint64(len(payload)-iceFooterLength),
		"segment %s holds no stored document %d", segmentPath, documentNumber)
	binary.BigEndian.PutUint64(payload[entry:entry+storedDocumentOffsetWidth], math.MaxUint64)
	require.NoError(t, os.WriteFile(segmentPath, payload, 0o600))
}

// selectionShape renders every field TermSelection declares as name:type, in
// declaration order, so a field added to carry a range bound, a negation, an
// analyzer or a sort order fails the boundary test instead of quietly widening
// the milestone.
func selectionShape() []string {
	selectionType := reflect.TypeOf(TermSelection{})
	shape := make([]string, 0, selectionType.NumField())
	for fieldIndex := range selectionType.NumField() {
		field := selectionType.Field(fieldIndex)
		shape = append(shape, field.Name+":"+field.Type.String())
	}
	return shape
}
