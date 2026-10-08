// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
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
	"errors"
	"testing"

	roaringpkg "github.com/RoaringBitmap/roaring"
)

//nolint:gocyclo // one fixture intentionally asserts every merge modality.
func TestMergeSegmentsPreservesModesMasksAndMappings(t *testing.T) {
	firstPath := t.TempDir()
	firstGeneration := Generation{SegmentID: 1, SnapshotID: 1, Documents: []EncodeDocument{
		{Identifier: []byte("first"), Fields: []EncodeField{
			{Name: "tag", Value: []byte("raw"), Terms: []EncodeTerm{{Value: []byte("alpha"), Frequency: 2}}, Index: true, Store: true},
			{Name: "repeat", Value: []byte("one"), Store: true, Sort: true},
			{Name: "repeat", Value: []byte("two"), Store: true, Sort: true},
			{Name: "empty", Value: []byte("empty"), Index: true, Store: true, Terms: []EncodeTerm{}},
		}},
		{Identifier: []byte("deleted"), Deleted: true},
	}}
	if encodeErr := Encode(firstPath, firstGeneration); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	secondPath := t.TempDir()
	if encodeErr := Encode(secondPath, Generation{SegmentID: 2, SnapshotID: 1, Documents: []EncodeDocument{
		{Identifier: []byte("second")}, {Identifier: []byte("dropped")},
	}}); encodeErr != nil {
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
	drop := roaringpkg.New()
	drop.Add(1)
	merged, mergeErr := MergeSegments(context.Background(), []MergeInput{
		{Reader: firstReader, IndexedFields: []string{"empty"}},
		{Reader: secondReader, Drop: drop, IndexedFields: []string{"empty"}},
	})
	if mergeErr != nil {
		t.Fatal(mergeErr)
	}
	reference, referenceErr := referenceMergeSegments(context.Background(), []MergeInput{
		{Reader: firstReader, IndexedFields: []string{"empty"}},
		{Reader: secondReader, Drop: drop, IndexedFields: []string{"empty"}},
	})
	if referenceErr != nil {
		t.Fatal(referenceErr)
	}
	if !bytes.Equal(reference.Payload, merged.Payload) {
		t.Fatal("merged payload differs from the reference re-encoding")
	}
	if len(reference.Mappings) != 2 || len(reference.Mappings[0]) != 2 || len(reference.Mappings[1]) != 2 {
		t.Fatalf("mappings shape = %#v", reference.Mappings)
	}
	if reference.Mappings[0][0] != 0 || reference.Mappings[0][1] != DroppedDocumentNumber ||
		reference.Mappings[1][0] != 1 || reference.Mappings[1][1] != DroppedDocumentNumber {
		t.Fatalf("mappings = %#v", reference.Mappings)
	}
	reader, openErr := OpenSegment(merged.Payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	tagPosting, found, postingErr := reader.TermPosting("tag", []byte("alpha"))
	if postingErr != nil || !found || len(tagPosting.Frequencies) != 1 || tagPosting.Frequencies[0].Frequency != 2 {
		t.Fatalf("merged tag posting = %#v, found %v, err %v", tagPosting, found, postingErr)
	}
	values, valuesErr := reader.DocumentValues("repeat", 0)
	if valuesErr != nil {
		t.Fatal(valuesErr)
	}
	if len(values) != 2 || string(values[0]) != "one" || string(values[1]) != "two" {
		t.Fatalf("merged repeated sort values = %q, want [one two]", values)
	}
	var identifiers [][]byte
	if visitErr := reader.VisitLiveDocuments(context.Background(), func(document StoredDocument) error {
		return document.VisitStoredFields(func(name string, value []byte) bool {
			if name == identifierField {
				identifiers = append(identifiers, append([]byte(nil), value...))
			}
			return true
		})
	}); visitErr != nil {
		t.Fatal(visitErr)
	}
	if len(identifiers) != 2 || string(identifiers[0]) != "first" || string(identifiers[1]) != "second" {
		t.Fatalf("merged identifiers = %q", identifiers)
	}
	storedEmpty := make(map[string]int)
	var currentIdentifier string
	if visitErr := reader.VisitLiveDocuments(context.Background(), func(document StoredDocument) error {
		return document.VisitStoredFields(func(name string, value []byte) bool {
			if name == identifierField {
				currentIdentifier = string(value)
			}
			if name == "empty" {
				storedEmpty[currentIdentifier]++
			}
			return true
		})
	}); visitErr != nil {
		t.Fatal(visitErr)
	}
	if storedEmpty["first"] != 1 || storedEmpty["second"] != 0 {
		t.Fatalf("stored empty-index field counts = %#v, want first=1 second=0", storedEmpty)
	}
	fields, fieldsErr := reader.Fields()
	if fieldsErr != nil {
		t.Fatal(fieldsErr)
	}
	if !containsString(fields, "empty") {
		t.Fatalf("merged fields = %v, want empty indexed field metadata", fields)
	}
	if emptyPosting, emptyFound, emptyErr := reader.TermPosting("empty", nil); emptyErr != nil || emptyFound || emptyPosting.Bitmap != nil {
		t.Fatalf("empty indexed term = %#v, found %v, err %v", emptyPosting, emptyFound, emptyErr)
	}
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func TestMergeSegmentsHonorsCancellation(t *testing.T) {
	payload, encodeErr := EncodeSegment(Generation{Documents: []EncodeDocument{{Identifier: []byte("id")}}})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := OpenSegment(payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, mergeErr := MergeSegments(ctx, []MergeInput{{Reader: reader}}); !errors.Is(mergeErr, context.Canceled) {
		t.Fatalf("canceled merge error = %v, want context.Canceled", mergeErr)
	}
}
