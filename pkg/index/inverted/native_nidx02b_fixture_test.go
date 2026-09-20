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
	"sort"
	"strings"
	"testing"

	roaringpkg "github.com/RoaringBitmap/roaring"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted/internal/nativeice"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted/internal/nativeplugin"
)

// The three function types below are the NIDX-02B boundary. They are the
// fields an index lifecycle manager's segment plugin is built from, written
// out here rather than borrowed from the retired search engine's index
// package: the neutral segment API alias carries every type they mention, so
// the contract is stated without naming the library being retired.
//
// The coder owns everything behind these three signatures and may move any of
// it. The coder may not move these: a change to one of them stops compiling
// here rather than quietly redefining the milestone, and a later milestone
// registers the plugin by assigning these same three functions to its fields.
type (
	// segmentPluginNew builds a segment from one batch's analyzed documents and
	// reports how many documents it covers.
	segmentPluginNew func(results []segment.Document, normCalc func(string, int) float32) (segment.Segment, uint64, error)

	// segmentPluginLoad reopens a segment from the bytes it was persisted as.
	segmentPluginLoad func(data *segment.Data) (segment.Segment, error)

	// segmentPluginMerge returns a merger over segments and their positional
	// deletion masks.
	segmentPluginMerge func(segments []segment.Segment, drops []*roaringpkg.Bitmap, mergeBufferSize int) segment.Merger
)

// The three bindings below hold the production symbols to the boundary.
var (
	nidx02bNew   segmentPluginNew   = nativeplugin.New
	nidx02bLoad  segmentPluginLoad  = nativeplugin.Load
	nidx02bMerge segmentPluginMerge = nativeplugin.Merge
)

const (
	// nidx02bAdapterDir is the package the milestone adds. It is private to
	// pkg/index/inverted, like the native reader beside it.
	nidx02bAdapterDir = "internal/nativeplugin"

	// nidx02bAdapterPackagePath is that package's import path.
	nidx02bAdapterPackagePath = banyanDBModulePrefix + "pkg/index/inverted/internal/nativeplugin"

	// nidx02bRetiredIndexLibrary is the index library the native encoder
	// replaces. No tracked Go source may import it.
	nidx02bRetiredIndexLibrary = "github.com/blugelabs/ice"

	// nidx02bNeutralSegmentAPI is the neutral alias every segment contract type
	// is reachable through, and the only third-party segment import the adapter
	// is allowed. It carries no query language, no analyzer and no codec.
	nidx02bNeutralSegmentAPI = "github.com/blugelabs/bluge_segment_api"

	// nidx02bSegmentType and nidx02bSegmentVersion are the type and version an
	// ICE v3 segment reports. They are the values a snapshot manifest records
	// for the segments it references, pinned here as literals.
	nidx02bSegmentType    = "ice"
	nidx02bSegmentVersion = uint32(3)

	// nidx02bMergeBufferSize is the write buffer a merger is driven through.
	nidx02bMergeBufferSize = 1 << 16

	// nidx02bIdentifierField is the field name ICE v3 reserves for the document
	// identifier.
	nidx02bIdentifierField = "_id"

	// nidx02bTitleField and nidx02bColorField are the two field names the
	// hand-written document set records. One is analyzed into several terms and
	// the other is a single keyword, so a segment that indexed a field's whole
	// value instead of its terms is caught.
	nidx02bTitleField = "title"
	nidx02bColorField = "color"
)

// nidx02bAdapterSurface is every identifier the adapter package is permitted
// to export: the three plugin entry points and nothing else.
//
// A segment implementation, a merger type, a loader, an error sentinel or a
// registration helper does not appear here. The plugin's three fields are the
// whole contract other packages observe, so everything that answers them stays
// private and the coder stays free to reshape it.
var nidx02bAdapterSurface = []string{
	"Load",
	"Merge",
	"New",
}

// nidx02bAllowedAdapterImports is the adapter's whole third-party dependency
// budget: the bitmap type the plugin's deletion masks are expressed in, and
// the neutral segment API the contract types come from.
//
// The retired search engine's index package is deliberately absent. It is
// where the plugin struct itself lives, and reaching it is how an adapter
// grows a dependency on the library the workstream is removing; the three
// signatures above are sufficient without it.
var nidx02bAllowedAdapterImports = map[string]struct{}{
	"github.com/RoaringBitmap/roaring": {},
	nidx02bNeutralSegmentAPI:           {},
}

// nidx02bField is one value a hand-written document contributes.
type nidx02bField struct {
	name      string
	terms     [][]byte
	value     []byte
	index     bool
	store     bool
	docValues bool
}

func (f *nidx02bField) Name() string {
	return f.name
}

func (f *nidx02bField) Value() []byte {
	return f.value
}

func (f *nidx02bField) Length() int {
	return len(f.terms)
}

func (f *nidx02bField) Index() bool {
	return f.index
}

func (f *nidx02bField) Store() bool {
	return f.store
}

func (f *nidx02bField) IndexDocValues() bool {
	return f.docValues
}

func (f *nidx02bField) EachTerm(visit segment.VisitTerm) {
	for termIndex := range f.terms {
		visit(&nidx02bTerm{term: f.terms[termIndex]})
	}
}

// nidx02bTerm is one term an indexed field yields.
type nidx02bTerm struct {
	term []byte
}

func (t *nidx02bTerm) Term() []byte {
	return t.term
}

func (t *nidx02bTerm) Frequency() int {
	return 1
}

func (t *nidx02bTerm) EachLocation(_ segment.VisitLocation) {}

// nidx02bDocument is one analyzed document handed to the plugin's New, in the
// shape the lifecycle manager hands one over: already analyzed, visiting its
// fields in the order it records them.
type nidx02bDocument struct {
	fields    []nidx02bField
	timestamp int64
}

func (d *nidx02bDocument) Analyze() {}

func (d *nidx02bDocument) EachField(visit segment.VisitField) {
	for fieldIndex := range d.fields {
		visit(&d.fields[fieldIndex])
	}
}

func (d *nidx02bDocument) Timestamp() int64 {
	return d.timestamp
}

// nidx02bKeywordField builds a field whose single term is its whole value,
// which is how every Property field the corpus records is analyzed.
func nidx02bKeywordField(name string, value []byte, index, store, docValues bool) nidx02bField {
	field := nidx02bField{name: name, value: value, index: index, store: store, docValues: docValues}
	if index {
		field.terms = [][]byte{value}
	}
	return field
}

// nidx02bNormCalc is the length norm the lifecycle manager would score with.
// The plugin accepts it; the ICE v3 grammar carries a fixed norm, so no
// assertion in this contract depends on what it returns.
func nidx02bNormCalc(_ string, length int) float32 {
	if length <= 0 {
		return 1
	}
	return 1 / float32(length)
}

// nidx02bAnalyzedDocuments are three documents written by hand for this
// contract, independent of the checked-in corpus.
//
// Document 0 and document 1 share the term "hello" under an analyzed field
// whose whole value is neither document's term, so a segment that indexed
// values instead of terms resolves the wrong documents. Document 2 records no
// color at all, so a field that only some documents carry is covered: it
// still belongs to the segment's field set, and its collection statistics
// count two documents out of three.
func nidx02bAnalyzedDocuments() []segment.Document {
	return []segment.Document{
		&nidx02bDocument{timestamp: 10, fields: []nidx02bField{
			nidx02bKeywordField(nidx02bIdentifierField, []byte("doc-0"), true, true, false),
			{
				name: nidx02bTitleField, value: []byte("hello world"), index: true, store: true,
				terms: [][]byte{[]byte("hello"), []byte("world")},
			},
			nidx02bKeywordField(nidx02bColorField, []byte("red"), true, true, true),
		}},
		&nidx02bDocument{timestamp: 20, fields: []nidx02bField{
			nidx02bKeywordField(nidx02bIdentifierField, []byte("doc-1"), true, true, false),
			{
				name: nidx02bTitleField, value: []byte("hello there"), index: true, store: true,
				terms: [][]byte{[]byte("hello"), []byte("there")},
			},
			nidx02bKeywordField(nidx02bColorField, []byte("blue"), true, true, true),
		}},
		&nidx02bDocument{timestamp: 30, fields: []nidx02bField{
			nidx02bKeywordField(nidx02bIdentifierField, []byte("doc-2"), true, true, false),
			{
				name: nidx02bTitleField, value: []byte("quiet"), index: true, store: true,
				terms: [][]byte{[]byte("quiet")},
			},
		}},
	}
}

// nidx02bCorpusDocument renders one declared corpus row as the analyzed
// document the lifecycle manager would hand the plugin.
//
// This is a third independent mapping of the same declared rows: nidx02aBatch
// renders a row for the compatibility writer, nidx02aEncodeDocumentOf renders
// it for the native encoder, and this renders it for the plugin. The corpus's
// own readings stay the oracle all three are measured against.
func nidx02bCorpusDocument(row nidx02aRow) segment.Document {
	fields := []nidx02bField{
		nidx02bKeywordField(nidx02bIdentifierField, []byte(nidx02aDocID(row)), true, true, false),
		nidx02bKeywordField(nidx02aEntityField, []byte(row.entityID), true, false, true),
		nidx02bKeywordField(nidx02aGroupField, []byte(row.group), true, false, true),
		nidx02bKeywordField(nidx02aNameField, []byte(row.name), true, false, true),
		nidx02bKeywordField(nidx02aTagFieldName(), []byte(row.tag), true, false, true),
	}
	for _, source := range row.sources {
		fields = append(fields, nidx02bKeywordField(nidx02aSourceField, []byte(source), false, true, false))
	}
	if row.deletedAt > 0 {
		fields = append(fields,
			nidx02bKeywordField(nidx02aDeletedField, convert.Int64ToBytes(row.deletedAt), false, true, false))
	}
	fields = append(fields,
		nidx02bKeywordField(nidx02aSHAField, []byte(row.sha), false, true, false),
		nidx02bKeywordField(nidx02aTimestampField, nidx02aEncodedTimestamps[row.timestamp], true, true, true),
	)
	return &nidx02bDocument{fields: fields, timestamp: row.timestamp}
}

// nidx02bCorpusDocuments renders every declared corpus row, masked rows
// included. A masked row is a live document of the segment New builds: masking
// is a property of the generation's deletion masks, which the lifecycle manager
// supplies to Merge, not of the segment's document set.
func nidx02bCorpusDocuments() []segment.Document {
	documents := make([]segment.Document, 0, len(nidx02aRows))
	for _, row := range nidx02aRows {
		documents = append(documents, nidx02bCorpusDocument(row))
	}
	return documents
}

// nidx02bMaskedDocumentNumbers are the document numbers the corpus's deletion
// masks cover, read off the declared rows.
func nidx02bMaskedDocumentNumbers() []uint32 {
	var masked []uint32
	for rowIndex, row := range nidx02aRows {
		if row.masked {
			masked = append(masked, uint32(rowIndex))
		}
	}
	return masked
}

// nidx02bEncoderGeneration renders the rows as the generation the native
// encoder writes, using NIDX-02A's own landed mapping of a row to an encoder
// document.
func nidx02bEncoderGeneration(rows []nidx02aRow, segmentID, snapshotID uint64) nativeice.Generation {
	documents := make([]nativeice.EncodeDocument, 0, len(rows))
	for _, row := range rows {
		documents = append(documents, nidx02aEncodeDocumentOf(row))
	}
	return nativeice.Generation{SegmentID: segmentID, SnapshotID: snapshotID, Documents: documents}
}

// nidx02bPersist writes seg the way an index lifecycle manager persists a
// segment: it creates the segment file, hands the writer to WriteTo, syncs and
// closes it. It returns the bytes that reached the file.
func nidx02bPersist(t *testing.T, seg segment.Segment, path string) []byte {
	t.Helper()
	file, createErr := os.Create(path)
	require.NoError(t, createErr)
	written, writeErr := seg.WriteTo(file, nil)
	if writeErr != nil {
		require.NoError(t, file.Close())
		require.NoError(t, writeErr, "the lifecycle manager persists a segment through WriteTo")
	}
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
	payload, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, int64(len(payload)), written, "WriteTo must report the number of bytes it wrote")
	return payload
}

// nidx02bPersistMerger writes a merger the way the lifecycle manager persists a
// merged segment, and returns the bytes that reached the file.
func nidx02bPersistMerger(t *testing.T, merger segment.Merger, path string) []byte {
	t.Helper()
	file, createErr := os.Create(path)
	require.NoError(t, createErr)
	written, writeErr := merger.WriteTo(file, nil)
	if writeErr != nil {
		require.NoError(t, file.Close())
		require.NoError(t, writeErr, "the lifecycle manager persists a merged segment through WriteTo")
	}
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
	payload, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, int64(len(payload)), written, "WriteTo must report the number of bytes it wrote")
	return payload
}

// nidx02bReopen hands bytes back to the plugin the way the lifecycle manager
// reopens a persisted segment.
func nidx02bReopen(t *testing.T, payload []byte) segment.Segment {
	t.Helper()
	reopened, loadErr := nidx02bLoad(segment.NewDataBytes(payload))
	require.NoError(t, loadErr, "Load must reopen the bytes WriteTo persisted")
	require.NotNil(t, reopened)
	return reopened
}

// nidx02bSegmentFile names the only segment file in a directory.
func nidx02bSegmentFile(t *testing.T, directory string) string {
	t.Helper()
	matches, globErr := filepath.Glob(filepath.Join(directory, "*"+segExt))
	require.NoError(t, globErr)
	require.Len(t, matches, 1, "%s must hold exactly one segment file", directory)
	return matches[0]
}

// nidx02bSnapshotFile names the newest snapshot manifest in a directory.
func nidx02bSnapshotFile(t *testing.T, directory string) string {
	t.Helper()
	matches, globErr := filepath.Glob(filepath.Join(directory, "*"+snpExt))
	require.NoError(t, globErr)
	require.NotEmpty(t, matches, "%s must hold a published generation", directory)
	sort.Strings(matches)
	return matches[len(matches)-1]
}

// nidx02bRenderSegmentDocuments renders every document a segment stores as one
// comparable string per document, keeping the walk's own field order so a
// collapsed repeated value or a reordered field shows up in the difference.
func nidx02bRenderSegmentDocuments(t *testing.T, seg segment.Segment) []string {
	t.Helper()
	rendered := make([]string, 0, seg.Count())
	for documentNumber := uint64(0); documentNumber < seg.Count(); documentNumber++ {
		var values []string
		require.NoError(t, seg.VisitStoredFields(documentNumber, func(field string, value []byte) bool {
			values = append(values, fmt.Sprintf("%x=%x", field, value))
			return true
		}))
		rendered = append(rendered, strings.Join(values, " "))
	}
	return rendered
}

// nidx02bDocsMatching resolves one field's terms against a segment and returns
// the matching document numbers in ascending order.
func nidx02bDocsMatching(t *testing.T, seg segment.Segment, field string, terms ...string) []uint64 {
	t.Helper()
	asked := make([]segment.Term, 0, len(terms))
	for _, term := range terms {
		asked = append(asked, &nidx02bAskedTerm{field: field, term: []byte(term)})
	}
	matching, matchErr := seg.DocsMatchingTerms(asked)
	require.NoError(t, matchErr)
	require.NotNil(t, matching)
	numbers := make([]uint64, 0, matching.GetCardinality())
	iterator := matching.Iterator()
	for iterator.HasNext() {
		numbers = append(numbers, uint64(iterator.Next()))
	}
	return numbers
}

// nidx02bAskedTerm is one field-and-term pair a caller resolves.
type nidx02bAskedTerm struct {
	field string
	term  []byte
}

func (a *nidx02bAskedTerm) Field() string {
	return a.field
}

func (a *nidx02bAskedTerm) Term() []byte {
	return a.term
}

// nidx02bDictionaryTerms lists a field's dictionary in ascending term order.
func nidx02bDictionaryTerms(t *testing.T, seg segment.Segment, field string) []string {
	t.Helper()
	dictionary, dictionaryErr := seg.Dictionary(field)
	require.NoError(t, dictionaryErr)
	require.NotNil(t, dictionary)
	iterator := dictionary.Iterator(nil, nil, nil)
	var terms []string
	for {
		entry, nextErr := iterator.Next()
		require.NoError(t, nextErr)
		if entry == nil {
			break
		}
		terms = append(terms, entry.Term())
	}
	require.NoError(t, iterator.Close())
	require.NoError(t, dictionary.Close())
	return terms
}

// nidx02bDocValues lists the doc values one field records for one document.
func nidx02bDocValues(t *testing.T, seg segment.Segment, documentNumber uint64, fields ...string) []string {
	t.Helper()
	reader, readerErr := seg.DocumentValueReader(fields)
	require.NoError(t, readerErr)
	require.NotNil(t, reader)
	var values []string
	require.NoError(t, reader.VisitDocumentValues(documentNumber, func(field string, term []byte) {
		values = append(values, fmt.Sprintf("%s=%s", field, term))
	}))
	sort.Strings(values)
	return values
}
