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
	"bytes"
	"context"
	"testing"
)

func TestEncodeIdentifierDocValuesOptIn(t *testing.T) {
	generation := Generation{SegmentID: 1, SnapshotID: 1, IdentifierDocValues: true, Documents: []EncodeDocument{
		{Identifier: []byte("doc-1")},
		{Identifier: []byte("doc-2")},
	}}
	payload, encodeErr := EncodeSegment(generation)
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := OpenSegment(payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	for documentNumber, want := range [][]byte{[]byte("doc-1"), []byte("doc-2")} {
		values, valuesErr := reader.DocumentValues(identifierField, uint64(documentNumber))
		if valuesErr != nil {
			t.Fatal(valuesErr)
		}
		if len(values) != 1 || !bytes.Equal(values[0], want) {
			t.Fatalf("document %d identifier doc value = %#v, want [%q]", documentNumber, values, want)
		}
	}
}

func TestEncodeWithoutIdentifierDocValuesOmitsColumn(t *testing.T) {
	generation := Generation{SegmentID: 1, SnapshotID: 1, Documents: []EncodeDocument{
		{Identifier: []byte("doc-1")},
	}}
	payload, encodeErr := EncodeSegment(generation)
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := OpenSegment(payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	values, valuesErr := reader.DocumentValues(identifierField, 0)
	if valuesErr != nil {
		t.Fatal(valuesErr)
	}
	if len(values) != 0 {
		t.Fatalf("identifier doc values = %#v, want none", values)
	}
}

// TestMergeSegmentsSplicesIdentifierDocValues proves merge carries "_id" doc
// values forward exactly like any other optional doc-value field: present
// when an input wrote them, absent otherwise, independent of merge dropping
// documents and renumbering survivors.
func TestMergeSegmentsSplicesIdentifierDocValues(t *testing.T) {
	firstPath := t.TempDir()
	if encodeErr := Encode(firstPath, Generation{
		SegmentID: 1, SnapshotID: 1, IdentifierDocValues: true,
		Documents: []EncodeDocument{{Identifier: []byte("first")}, {Identifier: []byte("dropped"), Deleted: true}},
	}); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	secondPath := t.TempDir()
	if encodeErr := Encode(secondPath, Generation{
		SegmentID: 2, SnapshotID: 1,
		Documents: []EncodeDocument{{Identifier: []byte("second")}},
	}); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	firstReader, openErr := OpenStrict(firstPath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = firstReader.Close() }()
	secondReader, openErr := OpenStrict(secondPath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = secondReader.Close() }()

	merged, mergeErr := MergeSegments(context.Background(), []MergeInput{
		{Reader: firstReader},
		{Reader: secondReader},
	})
	if mergeErr != nil {
		t.Fatal(mergeErr)
	}
	reader, openErr := OpenSegment(merged.Payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()

	// "first" survives at output document 0 and keeps its doc value; "second"
	// (from an input that never wrote "_id" doc values) has none.
	firstValues, valuesErr := reader.DocumentValues(identifierField, 0)
	if valuesErr != nil {
		t.Fatal(valuesErr)
	}
	if len(firstValues) != 1 || !bytes.Equal(firstValues[0], []byte("first")) {
		t.Fatalf("merged document 0 identifier doc value = %#v, want [\"first\"]", firstValues)
	}
	secondValues, valuesErr := reader.DocumentValues(identifierField, 1)
	if valuesErr != nil {
		t.Fatal(valuesErr)
	}
	if len(secondValues) != 0 {
		t.Fatalf("merged document 1 identifier doc value = %#v, want none", secondValues)
	}

	// The term dictionary must still resolve both identifiers, proving the
	// doc-value splice did not disturb the freshly rebuilt "_id" terms.
	for _, want := range []string{"first", "second"} {
		_, found, postingErr := reader.TermPosting(identifierField, []byte(want))
		if postingErr != nil {
			t.Fatal(postingErr)
		}
		if !found {
			t.Fatalf("identifier term %q missing after merge", want)
		}
	}
}

func TestMergeSegmentsOmitsIdentifierDocValuesWhenNoInputHasThem(t *testing.T) {
	path := t.TempDir()
	if encodeErr := Encode(path, Generation{
		SegmentID: 1, SnapshotID: 1,
		Documents: []EncodeDocument{{Identifier: []byte("first")}},
	}); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := OpenStrict(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	merged, mergeErr := MergeSegments(context.Background(), []MergeInput{{Reader: reader}})
	if mergeErr != nil {
		t.Fatal(mergeErr)
	}
	mergedReader, openErr := OpenSegment(merged.Payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = mergedReader.Close() }()
	values, valuesErr := mergedReader.DocumentValues(identifierField, 0)
	if valuesErr != nil {
		t.Fatal(valuesErr)
	}
	if len(values) != 0 {
		t.Fatalf("identifier doc values = %#v, want none", values)
	}
}
