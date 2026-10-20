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

package nativeice

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestByteSeamPreservesAllFieldFacets(t *testing.T) {
	generation := Generation{Documents: []EncodeDocument{{
		Identifier: []byte("doc-1"),
		Fields: []EncodeField{
			{Name: "indexed", Value: []byte("raw"), Index: true, Sort: true},
			{Name: "stored", Value: []byte("stored-value"), Store: true, Sort: true},
			{Name: "docvalue", Value: []byte("sort-value"), Sort: true},
			{Name: "docvalue", Value: []byte("sort-value-2"), Sort: true},
			{Name: "analyzed", Value: []byte("ignored"), Index: true, Sort: true, Terms: []EncodeTerm{{Value: []byte("token"), Frequency: 3}}},
			{Name: "empty", Value: []byte("ignored"), Index: true, Terms: []EncodeTerm{}},
		},
	}}}
	payload, encodeErr := EncodeSegment(generation)
	if encodeErr != nil {
		t.Fatalf("EncodeSegment() error = %v", encodeErr)
	}
	reader, openErr := OpenSegment(payload)
	if openErr != nil {
		t.Fatalf("OpenSegment() error = %v", openErr)
	}
	defer func() { _ = reader.Close() }()
	fields, fieldsErr := reader.Fields()
	if fieldsErr != nil {
		t.Fatalf("Fields() error = %v", fieldsErr)
	}
	for _, fieldName := range []string{"_id", "indexed", "stored", "docvalue", "analyzed", "empty"} {
		found := false
		for _, actual := range fields {
			if actual == fieldName {
				found = true
			}
		}
		if !found {
			t.Errorf("Fields() omitted %q: %v", fieldName, fields)
		}
	}
	terms, termsErr := reader.Terms("analyzed")
	if termsErr != nil || len(terms) != 1 || !bytes.Equal(terms[0], []byte("token")) {
		t.Fatalf("Terms(analyzed) = %#v, error %v", terms, termsErr)
	}
	emptyTerms, emptyErr := reader.Terms("empty")
	if emptyErr != nil || len(emptyTerms) != 0 {
		t.Fatalf("Terms(empty) = %#v, error %v", emptyTerms, emptyErr)
	}
	legacyTerms, legacyErr := reader.Terms("indexed")
	if legacyErr != nil || len(legacyTerms) != 1 || !bytes.Equal(legacyTerms[0], []byte("raw")) {
		t.Fatalf("Terms(indexed) = %#v, error %v", legacyTerms, legacyErr)
	}
	postings, postingsErr := reader.TermDocuments("indexed", []byte("raw"))
	if postingsErr != nil || len(postings) != 1 || len(postings[0].DocumentNumber) != 1 {
		t.Fatalf("TermDocuments() = %#v, error %v", postings, postingsErr)
	}
	rows, rowsErr := reader.RepairTuplePage(context.Background(), RepairPageRequest{
		SortFields: [repairSortFieldCount]string{"docvalue", "indexed", "analyzed", "stored"}, ProjectField: "docvalue", PageSize: 1,
	})
	if rowsErr != nil || len(rows) != 1 || len(rows[0].SortValues) == 0 || !bytes.Equal(rows[0].SortValues[0], []byte("sort-value")) {
		t.Fatalf("RepairTuplePage() = %#v, error %v", rows, rowsErr)
	}
	docValues, docValuesErr := reader.DocValues("docvalue")
	if docValuesErr != nil || len(docValues) != 1 || len(docValues[0].Values) != 1 || len(docValues[0].Values[0]) != 2 ||
		!bytes.Equal(docValues[0].Values[0][0], []byte("sort-value")) || !bytes.Equal(docValues[0].Values[0][1], []byte("sort-value-2")) {
		t.Fatalf("DocValues() = %#v, error %v", docValues, docValuesErr)
	}
}

func TestByteSeamOwnsPayloadAndRejectsTruncation(t *testing.T) {
	payload, encodeErr := EncodeSegment(Generation{Documents: []EncodeDocument{{Identifier: []byte("id")}}})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	original := append([]byte(nil), payload...)
	reader, openErr := OpenSegment(payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	payload[0] ^= 0xff
	terms, termsErr := reader.Terms("_id")
	if termsErr != nil || len(terms) != 1 || !bytes.Equal(terms[0], []byte("id")) {
		t.Fatalf("owned payload was changed: terms=%q err=%v", terms, termsErr)
	}
	_ = reader.Close()
	if _, truncationErr := OpenSegment(original[:len(original)-1]); !errors.Is(truncationErr, ErrCorrupt) {
		t.Fatalf("truncated segment error = %v, want ErrCorrupt", truncationErr)
	}
}
