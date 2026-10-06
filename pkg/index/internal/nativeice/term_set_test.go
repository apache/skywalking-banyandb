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
	"fmt"
	"sync"
	"testing"
)

func TestSmallSegmentTermSetAnswersExactLookups(t *testing.T) {
	documents := []EncodeDocument{
		{Identifier: []byte("doc-a"), Fields: []EncodeField{{Name: "tag", Value: []byte("alpha"), Index: true}, {Name: "tag", Value: []byte(""), Index: true}}},
		{Identifier: []byte("doc-b"), Fields: []EncodeField{{Name: "tag", Value: []byte("beta"), Index: true}}},
	}
	payload, encodeErr := EncodeSegment(Generation{Documents: documents})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := OpenSegmentBorrowed(payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	cases := []struct {
		field, term string
		found       bool
	}{
		{"tag", "alpha", true},
		{"tag", "beta", true},
		{"tag", "", true},
		{"tag", "gamma", false},
		{identifierField, "doc-a", true},
		{identifierField, "doc-c", false},
		{"missing-field", "alpha", false},
	}
	// Concurrent first use builds each field's set once under -race.
	var wait sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for _, lookup := range cases {
				_, found, postingErr := reader.TermPosting(lookup.field, []byte(lookup.term))
				if postingErr != nil || found != lookup.found {
					t.Errorf("TermPosting(%q, %q) = found %v, err %v; want found %v", lookup.field, lookup.term, found, postingErr, lookup.found)
				}
				_, found, postingErr = reader.TermPostingBitmap(lookup.field, []byte(lookup.term))
				if postingErr != nil || found != lookup.found {
					t.Errorf("TermPostingBitmap(%q, %q) = found %v, err %v; want found %v", lookup.field, lookup.term, found, postingErr, lookup.found)
				}
			}
		}()
	}
	wait.Wait()
}

func TestLargeSegmentAnswersLookupsThroughBloomFilter(t *testing.T) {
	documents := make([]EncodeDocument, 500)
	for index := range documents {
		documents[index] = EncodeDocument{Identifier: []byte(fmt.Sprintf("doc-%04d", index)), Fields: []EncodeField{
			{Name: "tag", Value: []byte(fmt.Sprintf("value-%d", index%37)), Index: true},
		}}
	}
	reader := openTermSetSegment(t, documents)
	var wait sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			// A bloom filter may report a false positive but never a false
			// negative: every present term must still be found.
			for index := range documents {
				if _, found, postingErr := reader.TermPosting(identifierField, documents[index].Identifier); postingErr != nil || !found {
					t.Errorf("TermPosting(%s) = found %v, err %v", documents[index].Identifier, found, postingErr)
				}
			}
			for value := 0; value < 37; value++ {
				if _, found, postingErr := reader.TermPostingBitmap("tag", []byte(fmt.Sprintf("value-%d", value))); postingErr != nil || !found {
					t.Errorf("TermPostingBitmap(value-%d) = found %v, err %v", value, found, postingErr)
				}
			}
			for _, missing := range []string{"doc-9999", "", "value-37"} {
				if _, found, postingErr := reader.TermPosting(identifierField, []byte(missing)); postingErr != nil || found {
					t.Errorf("TermPosting(%q) = found %v, err %v; want absent", missing, found, postingErr)
				}
			}
		}()
	}
	wait.Wait()
	storedReader, readerErr := reader.storedReader(0)
	if readerErr != nil {
		t.Fatal(readerErr)
	}
	if len(storedReader.smallTermSets) != 0 || storedReader.termBlooms[identifierField] == nil {
		t.Fatalf("a %d-document segment must use a bloom filter, not an exact term set", len(documents))
	}
}

func TestDictionaryAboveBloomCapKeepsPlainLookups(t *testing.T) {
	documents := make([]EncodeDocument, maxBloomFilterTerms+1)
	for index := range documents {
		documents[index] = EncodeDocument{Identifier: []byte(fmt.Sprintf("doc-%06d", index))}
	}
	reader := openTermSetSegment(t, documents)
	for _, lookup := range []struct {
		term  string
		found bool
	}{{"doc-000000", true}, {fmt.Sprintf("doc-%06d", maxBloomFilterTerms), true}, {"doc-999999", false}} {
		if _, found, postingErr := reader.TermPosting(identifierField, []byte(lookup.term)); postingErr != nil || found != lookup.found {
			t.Fatalf("TermPosting(%q) = found %v, err %v; want %v", lookup.term, found, postingErr, lookup.found)
		}
	}
	storedReader, readerErr := reader.storedReader(0)
	if readerErr != nil {
		t.Fatal(readerErr)
	}
	if bloom, built := storedReader.termBlooms[identifierField]; !built || bloom != nil {
		t.Fatalf("a %d-term dictionary must record a nil filter, got built=%v filter=%v", len(documents), built, bloom)
	}
}

func openTermSetSegment(t *testing.T, documents []EncodeDocument) *Reader {
	t.Helper()
	payload, encodeErr := EncodeSegment(Generation{Documents: documents})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := OpenSegmentBorrowed(payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = reader.Close() })
	return reader
}

func TestLargeSegmentSkipsTermSet(t *testing.T) {
	documents := make([]EncodeDocument, smallSegmentTermSetDocuments+1)
	for index := range documents {
		documents[index] = EncodeDocument{Identifier: []byte(fmt.Sprintf("doc-%03d", index))}
	}
	payload, encodeErr := EncodeSegment(Generation{Documents: documents})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := OpenSegmentBorrowed(payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	for _, lookup := range []struct {
		term  string
		found bool
	}{{"doc-000", true}, {fmt.Sprintf("doc-%03d", smallSegmentTermSetDocuments), true}, {"doc-999", false}} {
		_, found, postingErr := reader.TermPosting(identifierField, []byte(lookup.term))
		if postingErr != nil || found != lookup.found {
			t.Fatalf("TermPosting(%q) = found %v, err %v; want %v", lookup.term, found, postingErr, lookup.found)
		}
	}
	storedReader, readerErr := reader.storedReader(0)
	if readerErr != nil {
		t.Fatal(readerErr)
	}
	if len(storedReader.smallTermSets) != 0 {
		t.Fatalf("a %d-document segment built a term set", len(documents))
	}
}
