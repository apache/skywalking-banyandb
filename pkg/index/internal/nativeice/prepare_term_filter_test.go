// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to You under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at
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
	"testing"
)

// PrepareTermFilter's one-segment restriction (see its doc comment) matters
// for every production caller -- newMemorySegment, newSegmentFromPayload,
// newSegmentFromFile, loadPersistedRoot's per-segment readers, and the
// persisted-handle promotion reader all open exactly one segment at a time
// -- so it is exercised implicitly by every test below rather than by a
// separate multi-segment fixture.

// TestPrepareTermFilterBuildsTheSmallTermSetEagerly is half of the proof that
// PrepareTermFilter does the work a first exact lookup would otherwise do
// lazily: for a segment at or under smallSegmentTermSetDocuments, termAbsent
// answers from an exact in-memory term set (storedSegmentReader.termSets),
// built on first use and kept with the reader. This asserts the cache
// already holds the field directly -- never calling termAbsent/TermPosting
// first, which would build it itself and make the assertion meaningless.
func TestPrepareTermFilterBuildsTheSmallTermSetEagerly(t *testing.T) {
	documents := []EncodeDocument{
		{Identifier: []byte("doc-a"), Fields: []EncodeField{{Name: "tag", Value: []byte("alpha"), Index: true}}},
		{Identifier: []byte("doc-b"), Fields: []EncodeField{{Name: "tag", Value: []byte("beta"), Index: true}}},
	}
	reader := openTermSetSegment(t, documents)
	if err := reader.PrepareTermFilter(identifierField); err != nil {
		t.Fatal(err)
	}
	storedReader, err := reader.storedReader(0)
	if err != nil {
		t.Fatal(err)
	}
	_, built := storedReader.termSets.Load(identifierField)
	if !built {
		t.Fatal("PrepareTermFilter did not build the small-segment exact term set for identifierField")
	}
}

// TestPrepareTermFilterBuildsTheBloomFilterEagerly is the bloom-filter half:
// a segment over smallSegmentTermSetDocuments answers termAbsent from a
// bloom filter (storedSegmentReader.termFilters) instead. Same assertion
// shape: the cache must already hold the field before any lookup runs.
func TestPrepareTermFilterBuildsTheBloomFilterEagerly(t *testing.T) {
	documents := make([]EncodeDocument, smallSegmentTermSetDocuments+1)
	for index := range documents {
		documents[index] = EncodeDocument{
			Identifier: []byte(fmt.Sprintf("doc-%04d", index)),
			Fields:     []EncodeField{{Name: "tag", Value: []byte(fmt.Sprintf("value-%d", index%37)), Index: true}},
		}
	}
	reader := openTermSetSegment(t, documents)
	if err := reader.PrepareTermFilter(identifierField); err != nil {
		t.Fatal(err)
	}
	storedReader, err := reader.storedReader(0)
	if err != nil {
		t.Fatal(err)
	}
	_, built := cachedTermFilter(storedReader, identifierField)
	if !built {
		t.Fatal("PrepareTermFilter did not build the bloom filter for identifierField")
	}
}
