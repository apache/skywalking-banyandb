// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with the
// License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nativeice

import (
	"encoding/binary"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"testing"
)

const regressionDictionaryTermCount = 300_000

func largeDictionarySegment(t testing.TB) ([]byte, []byte) {
	t.Helper()
	terms := make([]EncodeTerm, regressionDictionaryTermCount)
	var state uint64 = 0x9e3779b97f4a7c15
	var selected []byte
	for termIndex := range terms {
		term := make([]byte, 64)
		for byteIndex := range term {
			state ^= state << 7
			state ^= state >> 9
			state ^= state << 8
			term[byteIndex] = byte(state)
		}
		terms[termIndex] = EncodeTerm{Value: term, Frequency: 1}
		if termIndex == regressionDictionaryTermCount/2 {
			selected = append([]byte(nil), term...)
		}
	}
	payload, encodeErr := EncodeSegment(Generation{Documents: []EncodeDocument{{
		Identifier: []byte("large-dictionary-document"),
		Fields:     []EncodeField{{Name: "large", Value: []byte("large"), Index: true, Terms: terms}},
	}}})
	requireNativeNoError(t, encodeErr)
	return payload, selected
}

func TestReaderAcceptsDictionaryBetweenGenericReadLimitAndSelectionLimit(t *testing.T) {
	payload, selectedTerm := largeDictionarySegment(t)
	reader, openErr := OpenSegment(payload)
	requireNativeNoError(t, openErr)
	t.Cleanup(func() { requireNativeNoError(t, reader.Close()) })

	storedReader, readerErr := newStoredSegmentReader(reader.segments[0].file, reader.segments[0].size, reader.segments[0].record)
	requireNativeNoError(t, readerErr)
	dictionaryOffset, found, offsetErr := storedReader.dictionaryOffset("large")
	requireNativeNoError(t, offsetErr)
	requireNative(t, found, "large dictionary field was not found")
	cursor := dictionaryOffset
	dictionaryLength, lengthErr := storedReader.readUvarint(&cursor, storedReader.footer.docValueOffset)
	requireNativeNoError(t, lengthErr)
	requireNative(t, dictionaryLength > uint64(maxStoredChunkTableSize), "dictionary length %d is not above generic read limit", dictionaryLength)
	requireNative(t, dictionaryLength <= uint64(maxSelectionDictionarySize), "dictionary length %d exceeds selection limit", dictionaryLength)

	terms, termsErr := reader.Terms("large")
	requireNativeNoError(t, termsErr)
	requireNative(t, len(terms) == regressionDictionaryTermCount, "term count = %d, want %d", len(terms), regressionDictionaryTermCount)
	documents, documentsErr := reader.TermDocuments("large", selectedTerm)
	requireNativeNoError(t, documentsErr)
	requireNative(t, len(documents) == 1 && reflect.DeepEqual([]uint64{0}, documents[0].DocumentNumber), "documents = %#v", documents)
	missing, missingErr := reader.TermDocuments("large", []byte("missing"))
	requireNativeNoError(t, missingErr)
	requireNative(t, len(missing) == 0, "missing term matched %#v", missing)
}

func TestReaderRejectsOversizedOrTruncatedDictionary(t *testing.T) {
	payload, _ := largeDictionarySegment(t)
	reader, openErr := OpenSegment(payload)
	requireNativeNoError(t, openErr)
	t.Cleanup(func() { requireNativeNoError(t, reader.Close()) })
	storedReader, readerErr := newStoredSegmentReader(reader.segments[0].file, reader.segments[0].size, reader.segments[0].record)
	requireNativeNoError(t, readerErr)
	dictionaryOffset, found, offsetErr := storedReader.dictionaryOffset("large")
	requireNativeNoError(t, offsetErr)
	requireNative(t, found, "large dictionary field was not found")
	cursor := dictionaryOffset
	_, lengthErr := storedReader.readUvarint(&cursor, storedReader.footer.docValueOffset)
	requireNativeNoError(t, lengthErr)
	lengthWidth := cursor - dictionaryOffset

	oversized := append([]byte(nil), payload...)
	encodedLength := make([]byte, binary.MaxVarintLen64)
	encodedWidth := binary.PutUvarint(encodedLength, maxSelectionDictionarySize+1)
	copy(oversized[dictionaryOffset:], encodedLength[:encodedWidth])
	if uint64(encodedWidth) < lengthWidth {
		copy(oversized[dictionaryOffset+uint64(encodedWidth):], payload[dictionaryOffset+lengthWidth:])
		oversized = oversized[:len(oversized)-int(lengthWidth-uint64(encodedWidth))]
	}
	oversizedReader, oversizedErr := OpenSegment(oversized)
	if oversizedErr == nil {
		_, oversizedErr = oversizedReader.Terms("large")
		_ = oversizedReader.Close()
	}
	requireNativeErrorIs(t, oversizedErr, ErrCorrupt)

	outOfBounds := append([]byte(nil), payload...)
	encodedWidth = binary.PutUvarint(encodedLength, maxSelectionDictionarySize)
	copy(outOfBounds[dictionaryOffset:], encodedLength[:encodedWidth])
	outOfBoundsReader, outOfBoundsErr := OpenSegment(outOfBounds)
	if outOfBoundsErr == nil {
		_, outOfBoundsErr = outOfBoundsReader.Terms("large")
		_ = outOfBoundsReader.Close()
	}
	requireNativeErrorIs(t, outOfBoundsErr, ErrCorrupt)

	_, truncatedErr := OpenSegment(payload[:len(payload)-1])
	requireNativeErrorIs(t, truncatedErr, ErrCorrupt)
}

func TestReaderDictionaryCachePreservesConcurrentLookupSemantics(t *testing.T) {
	payload, selectedTerm := largeDictionarySegment(t)
	reader, openErr := OpenSegment(payload)
	requireNativeNoError(t, openErr)
	t.Cleanup(func() { requireNativeNoError(t, reader.Close()) })

	for lookup := 0; lookup < 3; lookup++ {
		documents, documentsErr := reader.TermDocuments("large", selectedTerm)
		requireNativeNoError(t, documentsErr)
		requireNative(t, len(documents) == 1 && reflect.DeepEqual([]uint64{0}, documents[0].DocumentNumber), "documents = %#v", documents)
	}
	var wait sync.WaitGroup
	lookupErrors := make(chan error, 16)
	for worker := 0; worker < 16; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for lookup := 0; lookup < 20; lookup++ {
				found, foundErr := reader.TermExists("large", selectedTerm)
				if foundErr != nil || !found {
					lookupErrors <- errors.Join(foundErr, errors.New("selected term was not found"))
					return
				}
			}
		}()
	}
	wait.Wait()
	close(lookupErrors)
	for lookupErr := range lookupErrors {
		requireNativeNoError(t, lookupErr)
	}
}

func TestDictionaryIteratorOwnsRangeBounds(t *testing.T) {
	payload, encodeErr := EncodeSegment(Generation{Documents: []EncodeDocument{{
		Identifier: []byte("range-document"),
		Fields: []EncodeField{{Name: "tag", Value: []byte("beta"), Index: true, Terms: []EncodeTerm{
			{Value: []byte("alpha"), Frequency: 1},
			{Value: []byte("beta"), Frequency: 1},
			{Value: []byte("gamma"), Frequency: 1},
		}}},
	}}})
	requireNativeNoError(t, encodeErr)
	reader, openErr := OpenSegment(payload)
	requireNativeNoError(t, openErr)
	defer func() { requireNativeNoError(t, reader.Close()) }()

	start := []byte("beta")
	end := []byte("gamma")
	iterator, iteratorErr := reader.NewDictionaryTermIterator("tag", nil, start, end)
	requireNativeNoError(t, iteratorErr)
	// Search retains its bounds, so the iterator must own copies rather than
	// observing later query-buffer reuse by the caller.
	copy(start, "zeta")
	copy(end, "alpha")
	term, count, nextErr := iterator.NextString()
	requireNativeNoError(t, nextErr)
	requireNative(t, term == "beta" && count == 1, "first term = %q/%d, want beta/1", term, count)
	term, count, nextErr = iterator.NextString()
	requireNativeNoError(t, nextErr)
	requireNative(t, term == "" && count == 0, "second term = %q/%d, want exhaustion", term, count)
	requireNativeNoError(t, iterator.Close())
}

// TestBorrowedDictionaryIteratorReportsReaderClose checks an iterator that
// outlives its Reader neither reads freed state nor keeps serving: it
// reports ErrReaderClosed.
func TestBorrowedDictionaryIteratorReportsReaderClose(t *testing.T) {
	payload, encodeErr := EncodeSegment(Generation{Documents: []EncodeDocument{{
		Identifier: []byte("iterator-document"),
		Fields:     []EncodeField{{Name: "tag", Index: true, Terms: []EncodeTerm{{Value: []byte("term"), Frequency: 1}}}},
	}}})
	requireNativeNoError(t, encodeErr)
	reader, openErr := OpenSegmentBorrowed(payload)
	requireNativeNoError(t, openErr)
	iterator, iteratorErr := reader.NewDictionaryTermIterator("tag", nil, nil, nil)
	requireNativeNoError(t, iteratorErr)
	requireNativeNoError(t, reader.Close())
	runtime.GC()

	_, _, nextErr := iterator.NextString()
	requireNative(t, errors.Is(nextErr, ErrReaderClosed), "next after close = %v, want ErrReaderClosed", nextErr)
	requireNativeNoError(t, iterator.Close())
}

func TestTermDocumentsRejectsMalformedPostingOffset(t *testing.T) {
	term := []byte("shared")
	payload, encodeErr := EncodeSegment(Generation{Documents: []EncodeDocument{
		{Identifier: []byte("one"), Fields: []EncodeField{{Name: "tag", Value: term, Index: true, Terms: []EncodeTerm{{Value: term, Frequency: 1}}}}},
		{Identifier: []byte("two"), Fields: []EncodeField{{Name: "tag", Value: term, Index: true, Terms: []EncodeTerm{{Value: term, Frequency: 1}}}}},
	}})
	requireNativeNoError(t, encodeErr)
	reader, openErr := OpenSegment(payload)
	requireNativeNoError(t, openErr)
	storedReader, readerErr := newStoredSegmentReader(reader.segments[0].file, reader.segments[0].size, reader.segments[0].record)
	requireNativeNoError(t, readerErr)
	dictionary, dictionaryErr := storedReader.dictionary("tag")
	requireNativeNoError(t, dictionaryErr)
	postingOffset, found, lookupErr := lookupTermPosting(dictionary, term)
	requireNativeNoError(t, lookupErr)
	requireNative(t, found, "shared term was not found")
	requireNative(t, postingOffset&fstValueEncodingMask == 0, "posting offset has encoded flags: %x", postingOffset)
	requireNative(t, postingOffset+binary.MaxVarintLen64 < uint64(len(payload)), "posting offset %d is outside payload", postingOffset)
	_ = reader.Close()

	corruptPayload := append([]byte(nil), payload...)
	for offset := uint64(0); offset < binary.MaxVarintLen64; offset++ {
		corruptPayload[postingOffset+offset] = 0xff
	}
	corruptReader, corruptOpenErr := OpenSegment(corruptPayload)
	requireNativeNoError(t, corruptOpenErr)
	defer func() { requireNativeNoError(t, corruptReader.Close()) }()
	_, documentsErr := corruptReader.TermDocuments("tag", term)
	requireNativeErrorIs(t, documentsErr, ErrCorrupt)
	_, countsErr := corruptReader.TermDocumentCounts("tag", [][]byte{term})
	requireNativeErrorIs(t, countsErr, ErrCorrupt)
	postingsErr := corruptReader.VisitTermPostings("tag", [][]byte{term}, func([]byte, []uint64, []TermFrequency) error {
		return nil
	})
	requireNativeErrorIs(t, postingsErr, ErrCorrupt)
	_, _, termPostingErr := corruptReader.TermPosting("tag", term)
	requireNativeErrorIs(t, termPostingErr, ErrCorrupt)
}

func requireNativeNoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func requireNative(t testing.TB, condition bool, format string, args ...any) {
	t.Helper()
	if !condition {
		t.Fatalf(format, args...)
	}
}

func requireNativeErrorIs(t testing.TB, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want errors.Is(_, %v)", err, target)
	}
}

func TestDictionaryIteratorNextTermDistinguishesEmptyKeyFromExhaustion(t *testing.T) {
	payload, encodeErr := EncodeSegment(Generation{Documents: []EncodeDocument{{
		Identifier: []byte("empty-key-document"),
		Fields: []EncodeField{{Name: "tag", Index: true, Terms: []EncodeTerm{
			{Value: nil, Frequency: 1},
			{Value: []byte("alpha"), Frequency: 1},
		}}},
	}}})
	requireNativeNoError(t, encodeErr)
	reader, openErr := OpenSegment(payload)
	requireNativeNoError(t, openErr)
	defer func() { requireNativeNoError(t, reader.Close()) }()

	iterator, iteratorErr := reader.NewDictionaryTermIterator("tag", nil, nil, nil)
	requireNativeNoError(t, iteratorErr)
	term, nextErr := iterator.NextTerm()
	requireNativeNoError(t, nextErr)
	requireNative(t, term != nil && len(term) == 0, "first term = %#v, want a non-nil empty key", term)
	term, nextErr = iterator.NextTerm()
	requireNativeNoError(t, nextErr)
	requireNative(t, string(term) == "alpha", "second term = %q, want alpha", term)
	term, nextErr = iterator.NextTerm()
	requireNativeNoError(t, nextErr)
	requireNative(t, term == nil, "third term = %#v, want exhaustion", term)
	requireNativeNoError(t, iterator.Close())
}

// TestConcurrentFieldMetadataReads reads a segment's field metadata and probes
// its term dictionary from two goroutines at once, so the race detector sees
// the lazily initialized field state shared between Fields and TermExists.
func TestConcurrentFieldMetadataReads(t *testing.T) {
	payload, encodeErr := EncodeSegment(Generation{Documents: []EncodeDocument{{
		Identifier: []byte("id"),
		Fields:     []EncodeField{{Name: "series", Value: []byte("series"), Index: true, Terms: []EncodeTerm{{Value: []byte("series"), Frequency: 1}}}},
	}}})
	requireNativeNoError(t, encodeErr)
	reader, openErr := OpenSegment(payload)
	requireNativeNoError(t, openErr)
	defer func() { requireNativeNoError(t, reader.Close()) }()
	var wait sync.WaitGroup
	errs := make(chan error, 2)
	wait.Add(2)
	go func() {
		defer wait.Done()
		for range 100 {
			if _, fieldsErr := reader.Fields(); fieldsErr != nil {
				errs <- fieldsErr
				return
			}
		}
	}()
	go func() {
		defer wait.Done()
		for range 100 {
			if _, existsErr := reader.TermExists("series", []byte("series")); existsErr != nil {
				errs <- existsErr
				return
			}
		}
	}()
	wait.Wait()
	close(errs)
	for readErr := range errs {
		requireNativeNoError(t, readErr)
	}
}
