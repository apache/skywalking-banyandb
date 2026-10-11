// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package nativeice

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The corpus below is declared here rather than derived from anything the
// encoder produces. Every expected count, walk, selection and page in this file
// is read off this table by hand; nothing in it is computed the way Encode
// computes it, so an assertion can disagree with the implementation.
//
// It is shaped like one Property shard generation: three entities across two
// groups, one of them holding two revisions, with the group, name, entity and
// revision indexed and sorted, the payload and repair SHA stored, and the
// revision stored as well.
const (
	encodeGroupField     = "_group"
	encodeNameField      = "_im_name"
	encodeEntityField    = "_entity_id"
	encodeTimestampField = "_timestamp"
	encodeSourceField    = "_source"
	encodeSHAField       = "_sha_value"

	// encodeSegmentID and encodeSnapshotID number the generation the corpus is
	// written as. They are the caller's choice, and Reader.SnapshotID must
	// report the snapshot identifier back.
	encodeSegmentID  = uint64(2)
	encodeSnapshotID = uint64(5)

	// encodeVisibleDocCount is how many of the corpus's four documents survive
	// its deletion masks: every document except the deleted revision of e-1.
	encodeVisibleDocCount = int64(3)

	// encodePageSize is the page size the repair order is measured at. The
	// deleted revision sorts second of the four rows, so a page built without
	// the deletion masks returns it inside this first page rather than at the
	// end of the order.
	encodePageSize = 2
)

// encodeRow is one physical document of the corpus.
type encodeRow struct {
	identifier string
	group      string
	name       string
	entity     string
	sha        string
	sources    []string
	timestamp  []byte
	deleted    bool
}

// encodeRows is the corpus, in the order its documents take document numbers.
//
// Ascending by (_group, _im_name, _entity_id, _timestamp) the rows order as
// d-1, d-2, d-3, d-4, which is also insertion order; d-2 is the row the
// generation's deletion masks hide.
//
// Row d-1 records two _source values, so an encoder that collapsed repeated
// stored values into one is caught by the walk. The revision bytes are opaque
// literals rather than encoded numbers: Encode applies no numeric coding, so a
// term sorts and reads back as exactly the bytes declared here.
var encodeRows = []encodeRow{
	{
		identifier: "d-1", group: "g-a", name: "n-a", entity: "e-1",
		timestamp: []byte{0x20, 0x01, 0xa0}, sources: []string{"s-1a", "s-1b"}, sha: "sha-1",
	},
	{
		identifier: "d-2", group: "g-a", name: "n-a", entity: "e-1",
		timestamp: []byte{0x20, 0x01, 0xb0}, sources: []string{"s-2"}, sha: "sha-2", deleted: true,
	},
	{
		identifier: "d-3", group: "g-a", name: "n-b", entity: "e-2",
		timestamp: []byte{0x20, 0x01, 0xc0}, sources: []string{"s-3"}, sha: "sha-3",
	},
	{
		identifier: "d-4", group: "g-b", name: "n-a", entity: "e-3",
		timestamp: []byte{0x20, 0x01, 0xd0}, sources: []string{"s-4"}, sha: "sha-4",
	},
}

// TestNativeEncodeRoundTripsThroughTheMergedReader is the contract's entry
// point: a generation the encoder writes is a generation the merged reader
// opens.
//
// Requirement proved here:
//
//	R1 -- Encode writes one committed ICE v3 generation that Open selects. The
//	      opened generation reports the snapshot identifier the caller asked
//	      for and the number of documents its deletion masks leave visible, and
//	      its stored-document walk yields every live document's stored values:
//	      the identifier first, the remaining names in ascending byte order, a
//	      repeated name's values consecutive and in the order the document
//	      lists them, and no deleted document at all.
func TestNativeEncodeRoundTripsThroughTheMergedReader(t *testing.T) {
	directory := encodeCorpus(t)

	reader := openEncoded(t, directory)
	if reader.SnapshotID() != encodeSnapshotID {
		t.Fatalf("SnapshotID() = %d, want %d", reader.SnapshotID(), encodeSnapshotID)
	}
	count, countErr := reader.VisibleDocCount()
	if countErr != nil {
		t.Fatal(countErr)
	}
	if count != encodeVisibleDocCount {
		t.Fatalf("VisibleDocCount() = %d, want %d", count, encodeVisibleDocCount)
	}

	walked := walkDocuments(t, reader)
	want := [][]string{
		{
			"_id=" + hexOf("d-1"), "_sha_value=" + hexOf("sha-1"),
			"_source=" + hexOf("s-1a"), "_source=" + hexOf("s-1b"), "_timestamp=2001a0",
		},
		{"_id=" + hexOf("d-3"), "_sha_value=" + hexOf("sha-3"), "_source=" + hexOf("s-3"), "_timestamp=2001c0"},
		{"_id=" + hexOf("d-4"), "_sha_value=" + hexOf("sha-4"), "_source=" + hexOf("s-4"), "_timestamp=2001d0"},
	}
	if !reflect.DeepEqual(want, walked) {
		t.Fatalf("VisitLiveDocuments() =\n%v\nwant\n%v", walked, want)
	}
}

// TestNativeEncodeSelectsLiveDocumentsByExactTerm asks the encoder's generation
// for the documents one field's exact terms select.
//
// Requirement proved here:
//
//	R2 -- every field a document marks indexed contributes its value to that
//	      field's term dictionary, so a selection on the field and the literal
//	      value reaches exactly the live documents that recorded it. A term no
//	      document recorded selects nothing rather than failing, a document
//	      several terms select is visited once, and a document the deletion
//	      masks hide is never visited however many terms selected it.
func TestNativeEncodeSelectsLiveDocumentsByExactTerm(t *testing.T) {
	reader := openEncoded(t, encodeCorpus(t))

	for _, testCase := range []struct {
		field string
		want  []string
		terms []string
	}{
		// "g-a" was recorded by d-1, d-2 and d-3; d-2 is masked.
		{field: encodeGroupField, terms: []string{"g-a"}, want: []string{"d-1", "d-3"}},
		// "g-z" was recorded by no document and must not fail the selection.
		{field: encodeGroupField, terms: []string{"g-b", "g-z"}, want: []string{"d-4"}},
		// Both terms select d-1, which must still be visited exactly once.
		{field: encodeGroupField, terms: []string{"g-a", "g-a"}, want: []string{"d-1", "d-3"}},
		// "e-1" was recorded by d-1 and the masked d-2.
		{field: encodeEntityField, terms: []string{"e-1"}, want: []string{"d-1"}},
		{field: encodeEntityField, terms: []string{"e-9"}, want: nil},
	} {
		terms := make([][]byte, 0, len(testCase.terms))
		for _, term := range testCase.terms {
			terms = append(terms, []byte(term))
		}
		var selected []string
		selectErr := reader.VisitSelectedDocuments(context.Background(), testCase.field, terms,
			func(document StoredDocument) error {
				return document.VisitStoredFields(func(name string, value []byte) bool {
					if name == identifierField {
						selected = append(selected, string(value))
					}
					return true
				})
			})
		if selectErr != nil {
			t.Fatalf("VisitSelectedDocuments(%q, %v) failed: %v", testCase.field, testCase.terms, selectErr)
		}
		if !reflect.DeepEqual(testCase.want, selected) {
			t.Fatalf("VisitSelectedDocuments(%q, %v) = %v, want %v",
				testCase.field, testCase.terms, selected, testCase.want)
		}
	}
}

// TestNativeEncodePagesRepairTuplesInAscendingOrder pages the encoder's
// generation in the ascending Property repair order.
//
// Requirement proved here:
//
//	R3 -- every field a document marks sortable contributes a doc value, so the
//	      generation serves the bounded repair page: live rows ascend by all
//	      four sort components, a page resumes strictly after the cursor the
//	      previous page ended on, and the deleted row -- which sorts second of
//	      the four and would therefore land inside the first page -- never
//	      appears.
func TestNativeEncodePagesRepairTuplesInAscendingOrder(t *testing.T) {
	reader := openEncoded(t, encodeCorpus(t))
	request := RepairPageRequest{
		SortFields:   [repairSortFieldCount]string{encodeGroupField, encodeNameField, encodeEntityField, encodeTimestampField},
		ProjectField: encodeSHAField,
		PageSize:     encodePageSize,
	}

	first, firstErr := reader.RepairTuplePage(context.Background(), request)
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	wantFirst := []string{
		repairRow("g-a", "n-a", "e-1", "2001a0", "sha-1"),
		repairRow("g-a", "n-b", "e-2", "2001c0", "sha-3"),
	}
	if got := renderRepairRows(first); !reflect.DeepEqual(wantFirst, got) {
		t.Fatalf("first page = %v, want %v", got, wantFirst)
	}

	request.After = &first[len(first)-1].Cursor
	second, secondErr := reader.RepairTuplePage(context.Background(), request)
	if secondErr != nil {
		t.Fatal(secondErr)
	}
	wantSecond := []string{repairRow("g-b", "n-a", "e-3", "2001d0", "sha-4")}
	if got := renderRepairRows(second); !reflect.DeepEqual(wantSecond, got) {
		t.Fatalf("second page = %v, want %v", got, wantSecond)
	}

	request.After = &second[len(second)-1].Cursor
	third, thirdErr := reader.RepairTuplePage(context.Background(), request)
	if thirdErr != nil {
		t.Fatal(thirdErr)
	}
	if len(third) != 0 {
		t.Fatalf("third page = %v, want the order to be exhausted", renderRepairRows(third))
	}
}

// TestNativeEncodeLeavesNoPartiallyVisibleGeneration interrupts a write between
// its two durable steps -- the segment is on disk, the snapshot manifest that
// publishes it is not -- and asks what the directory then holds.
//
// Requirement proved here:
//
//	R4 -- a generation becomes visible only when its snapshot manifest is
//	      published, so an interruption before publication leaves no partially
//	      visible generation. A directory that already held a complete
//	      generation opens at that prior generation, with its identifier and
//	      visible count unchanged; a directory that never published one reports
//	      ErrNoSnapshot rather than the unpublished segment.
func TestNativeEncodeLeavesNoPartiallyVisibleGeneration(t *testing.T) {
	directory := encodeCorpus(t)
	later := Generation{
		SegmentID:  encodeSegmentID + 2,
		SnapshotID: encodeSnapshotID + 2,
		Documents:  []EncodeDocument{encodeDocumentOf(encodeRows[0])},
	}
	if encodeErr := Encode(directory, later); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	if published := openEncoded(t, directory); published.SnapshotID() != later.SnapshotID {
		t.Fatalf("SnapshotID() = %d, want the later generation %d", published.SnapshotID(), later.SnapshotID)
	}

	interruptPublication(t, directory)

	reader := openEncoded(t, directory)
	if reader.SnapshotID() != encodeSnapshotID {
		t.Fatalf("SnapshotID() = %d, want the prior complete generation %d", reader.SnapshotID(), encodeSnapshotID)
	}
	count, countErr := reader.VisibleDocCount()
	if countErr != nil {
		t.Fatal(countErr)
	}
	if count != encodeVisibleDocCount {
		t.Fatalf("VisibleDocCount() = %d, want the prior generation's %d", count, encodeVisibleDocCount)
	}

	fresh := t.TempDir()
	if encodeErr := Encode(fresh, encodeGeneration()); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	interruptPublication(t, fresh)
	if _, openErr := Open(fresh); !errors.Is(openErr, ErrNoSnapshot) {
		t.Fatalf("Open() error = %v, want an error wrapping ErrNoSnapshot", openErr)
	}
}

// TestNativeEncodeRejectsAnUnencodableGeneration hands the encoder a generation
// the ICE v3 grammar has no representation for.
//
// Requirement proved here:
//
//	R5 -- a generation carrying a document with no identifier, or a field with
//	      no name, is rejected with ErrInvalidGeneration, and the rejection
//	      leaves the directory's committed state exactly as it was: the
//	      generation already published there still opens, at the same
//	      identifier and the same visible count.
func TestNativeEncodeRejectsAnUnencodableGeneration(t *testing.T) {
	directory := encodeCorpus(t)

	for name, generation := range map[string]Generation{
		"document without an identifier": {
			SegmentID:  encodeSegmentID + 2,
			SnapshotID: encodeSnapshotID + 2,
			Documents: []EncodeDocument{{
				Fields: []EncodeField{{Name: encodeGroupField, Value: []byte("g-a"), Index: true}},
			}},
		},
		"field without a name": {
			SegmentID:  encodeSegmentID + 2,
			SnapshotID: encodeSnapshotID + 2,
			Documents: []EncodeDocument{{
				Identifier: []byte("d-9"),
				Fields:     []EncodeField{{Value: []byte("g-a"), Index: true}},
			}},
		},
	} {
		if encodeErr := Encode(directory, generation); !errors.Is(encodeErr, ErrInvalidGeneration) {
			t.Fatalf("Encode(%s) error = %v, want an error wrapping ErrInvalidGeneration", name, encodeErr)
		}
		reader := openEncoded(t, directory)
		if reader.SnapshotID() != encodeSnapshotID {
			t.Fatalf("after rejecting a %s, SnapshotID() = %d, want %d", name, reader.SnapshotID(), encodeSnapshotID)
		}
		count, countErr := reader.VisibleDocCount()
		if countErr != nil {
			t.Fatal(countErr)
		}
		if count != encodeVisibleDocCount {
			t.Fatalf("after rejecting a %s, VisibleDocCount() = %d, want %d", name, count, encodeVisibleDocCount)
		}
	}
}

// encodeGeneration renders the declared corpus as the generation Encode is
// asked to write.
func encodeGeneration() Generation {
	documents := make([]EncodeDocument, 0, len(encodeRows))
	for _, row := range encodeRows {
		documents = append(documents, encodeDocumentOf(row))
	}
	return Generation{SegmentID: encodeSegmentID, SnapshotID: encodeSnapshotID, Documents: documents}
}

// encodeDocumentOf renders one declared row as the document Encode is handed:
// the group, name, entity and revision indexed and sorted, the revision also
// stored, and the payloads and repair SHA stored only.
func encodeDocumentOf(row encodeRow) EncodeDocument {
	fields := []EncodeField{
		{Name: encodeEntityField, Value: []byte(row.entity), Index: true, Sort: true},
		{Name: encodeGroupField, Value: []byte(row.group), Index: true, Sort: true},
		{Name: encodeNameField, Value: []byte(row.name), Index: true, Sort: true},
	}
	for _, source := range row.sources {
		fields = append(fields, EncodeField{Name: encodeSourceField, Value: []byte(source), Store: true})
	}
	fields = append(fields,
		EncodeField{Name: encodeSHAField, Value: []byte(row.sha), Store: true},
		EncodeField{Name: encodeTimestampField, Value: row.timestamp, Index: true, Store: true, Sort: true},
	)
	return EncodeDocument{Identifier: []byte(row.identifier), Fields: fields, Deleted: row.deleted}
}

// encodeCorpus writes the declared corpus into a fresh directory and returns it.
func encodeCorpus(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if encodeErr := Encode(directory, encodeGeneration()); encodeErr != nil {
		t.Fatalf("Encode() failed: %v", encodeErr)
	}
	return directory
}

// openEncoded pins a directory's newest committed generation for the duration
// of the test.
func openEncoded(t *testing.T, directory string) *Reader {
	t.Helper()
	reader, openErr := Open(directory)
	if openErr != nil {
		t.Fatalf("Open(%q) failed: %v", directory, openErr)
	}
	t.Cleanup(func() {
		if closeErr := reader.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	return reader
}

// interruptPublication models a write that stopped between its two durable
// steps: it removes the newest snapshot manifest and leaves every segment in
// place, which is the state a directory is in when a writer died after the
// segment reached disk and before the manifest naming it did.
func interruptPublication(t *testing.T, directory string) {
	t.Helper()
	entries, readErr := os.ReadDir(directory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	manifests, segments := []string{}, 0
	for _, entry := range entries {
		switch filepath.Ext(entry.Name()) {
		case ".snp":
			manifests = append(manifests, entry.Name())
		case ".seg":
			segments++
		}
	}
	if len(manifests) == 0 || segments == 0 {
		t.Fatalf("directory %q holds %d manifests and %d segments; "+
			"a generation must be published by a manifest naming a segment", directory, len(manifests), segments)
	}
	sort.Strings(manifests)
	newest := manifests[len(manifests)-1]
	if removeErr := os.Remove(filepath.Join(directory, newest)); removeErr != nil {
		t.Fatal(removeErr)
	}
	remaining, remainingErr := os.ReadDir(directory)
	if remainingErr != nil {
		t.Fatal(remainingErr)
	}
	unpublished := 0
	for _, entry := range remaining {
		if filepath.Ext(entry.Name()) == ".seg" {
			unpublished++
		}
	}
	if unpublished != segments {
		t.Fatalf("removing manifest %q removed segments too: %d of %d remain", newest, unpublished, segments)
	}
}

// walkDocuments renders every live document's stored values, one slice per
// document, in the order the walk yields them.
func walkDocuments(t *testing.T, reader *Reader) [][]string {
	t.Helper()
	var documents [][]string
	walkErr := reader.VisitLiveDocuments(context.Background(), func(document StoredDocument) error {
		var values []string
		if visitErr := document.VisitStoredFields(func(name string, value []byte) bool {
			values = append(values, fmt.Sprintf("%s=%x", name, value))
			return true
		}); visitErr != nil {
			return visitErr
		}
		documents = append(documents, values)
		return nil
	})
	if walkErr != nil {
		t.Fatalf("VisitLiveDocuments() failed: %v", walkErr)
	}
	return documents
}

// renderRepairRows renders a repair page as one readable string per row: the
// four ascending sort components followed by the projected value.
func renderRepairRows(rows []RepairTupleRow) []string {
	rendered := make([]string, 0, len(rows))
	for _, row := range rows {
		components := make([]string, 0, len(row.SortValues)+1)
		for _, value := range row.SortValues {
			components = append(components, fmt.Sprintf("%x", value))
		}
		components = append(components, fmt.Sprintf("%x", row.Value))
		rendered = append(rendered, strings.Join(components, "|"))
	}
	return rendered
}

// hexOf renders a declared literal the way the assertions print values.
func hexOf(value string) string {
	return fmt.Sprintf("%x", value)
}

// repairRow renders one expected repair row: the group, name and entity as the
// literal bytes their doc values hold, the revision as the literal hexadecimal
// the corpus declares, and the projected repair SHA.
func repairRow(group, name, entity, timestampHex, sha string) string {
	return strings.Join([]string{hexOf(group), hexOf(name), hexOf(entity), timestampHex, hexOf(sha)}, "|")
}
