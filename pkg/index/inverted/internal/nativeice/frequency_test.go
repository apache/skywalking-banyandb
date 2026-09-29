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
	"encoding/binary"
	"errors"
	"strconv"
	"testing"
)

func TestTermFrequenciesDecodeAdaptiveChunksWithGap(t *testing.T) {
	const documentCount = 4096
	documents := make([]EncodeDocument, documentCount)
	for documentNumber := range documents {
		documents[documentNumber].Identifier = []byte("id-" + strconv.Itoa(documentNumber))
		if documentNumber < 1000 || documentNumber >= 3000 {
			documents[documentNumber].Fields = []EncodeField{{
				Name:  "keyword",
				Index: true,
				Terms: []EncodeTerm{{Value: []byte("term"), Frequency: uint64(documentNumber%7 + 1)}},
			}}
		}
	}
	payload, encodeErr := EncodeSegment(Generation{Documents: documents})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := OpenSegment(payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = reader.Close() })
	segments, frequencyErr := reader.TermFrequencies("keyword", []byte("term"))
	if frequencyErr != nil {
		t.Fatal(frequencyErr)
	}
	if len(segments) != 1 {
		t.Fatalf("segments = %d, want 1", len(segments))
	}
	values := segments[0].Values
	if len(values) != 2096 {
		t.Fatalf("frequency values = %d, want 2096", len(values))
	}
	for valueIndex, value := range values {
		documentNumber := valueIndex
		if documentNumber >= 1000 {
			documentNumber += 2000
		}
		wantFrequency := uint64(documentNumber%7 + 1)
		if value.DocumentNumber != uint64(documentNumber) || value.Frequency != wantFrequency {
			t.Fatalf("value %d = %+v, want document %d frequency %d", valueIndex, value, documentNumber, wantFrequency)
		}
	}
}

func TestTermFrequenciesCombineRepeatedTermEntries(t *testing.T) {
	payload, encodeErr := EncodeSegment(Generation{Documents: []EncodeDocument{{
		Identifier: []byte("id"),
		Fields: []EncodeField{{Name: "keyword", Index: true, Terms: []EncodeTerm{
			{Value: []byte("term"), Frequency: 2},
			{Value: []byte("term"), Frequency: 3},
		}}},
	}}})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := OpenSegment(payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = reader.Close() })
	segments, frequencyErr := reader.TermFrequencies("keyword", []byte("term"))
	if frequencyErr != nil {
		t.Fatal(frequencyErr)
	}
	if len(segments) != 1 || len(segments[0].Values) != 1 || segments[0].Values[0].Frequency != 5 {
		t.Fatalf("frequencies = %+v, want one document with frequency 5", segments)
	}
	documents, totalFrequency, statsErr := reader.FieldStats("keyword")
	if statsErr != nil {
		t.Fatal(statsErr)
	}
	if documents != 1 || totalFrequency != 5 {
		t.Fatalf("FieldStats() = (%d, %d), want (1, 5)", documents, totalFrequency)
	}
}

func TestTermFrequenciesRejectMalformedChunkHeaders(t *testing.T) {
	payload := frequencyTestPayload(t)
	frequencyOffset, postingOffset := frequencyTestOffsets(t, payload)

	t.Run("zero chunks", func(t *testing.T) {
		corrupt := append([]byte(nil), payload...)
		corrupt[frequencyOffset] = 0
		assertCorruptFrequencyPayload(t, corrupt)
	})

	t.Run("oversized chunk count", func(t *testing.T) {
		corrupt := append([]byte(nil), payload...)
		binary.PutUvarint(corrupt[frequencyOffset:], maxFrequencyChunkCount+1)
		assertCorruptFrequencyPayload(t, corrupt)
	})

	t.Run("truncated compressed chunk", func(t *testing.T) {
		corrupt := append([]byte(nil), payload...)
		if frequencyOffset+1 >= postingOffset {
			t.Fatal("frequency stream has no chunk offset")
		}
		corrupt[frequencyOffset+1] = 1
		assertCorruptFrequencyPayload(t, corrupt)
	})
}

func frequencyTestPayload(t *testing.T) []byte {
	t.Helper()
	payload, encodeErr := EncodeSegment(Generation{Documents: []EncodeDocument{{
		Identifier: []byte("id"),
		Fields:     []EncodeField{{Name: "keyword", Index: true, Terms: []EncodeTerm{{Value: []byte("term"), Frequency: 3}}}},
	}}})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	return payload
}

func frequencyTestOffsets(t *testing.T, payload []byte) (frequencyOffset, postingOffset uint64) {
	t.Helper()
	file := &byteSegmentFile{data: payload}
	footer, footerErr := readSegmentFooter(file, uint64(len(payload)), "test")
	if footerErr != nil {
		t.Fatal(footerErr)
	}
	record := segmentRecord{path: "test", documentCount: footer.documentCount, timeMin: footer.timeMin, timeMax: footer.timeMax}
	storedReader, readerErr := newStoredSegmentReader(file, uint64(len(payload)), record)
	if readerErr != nil {
		t.Fatal(readerErr)
	}
	dictionary, dictionaryErr := storedReader.dictionary("keyword")
	if dictionaryErr != nil {
		t.Fatal(dictionaryErr)
	}
	defer func() { _ = dictionary.Close() }()
	postingOffset, found, lookupErr := lookupTermPosting(dictionary, []byte("term"))
	if lookupErr != nil || !found {
		t.Fatalf("lookup term: found=%t err=%v", found, lookupErr)
	}
	cursor := postingOffset
	var frequencyErr error
	frequencyOffset, frequencyErr = storedReader.readUvarint(&cursor, footer.fieldsIndexOffset)
	if frequencyErr != nil {
		t.Fatal(frequencyErr)
	}
	return frequencyOffset, postingOffset
}

func assertCorruptFrequencyPayload(t *testing.T, payload []byte) {
	t.Helper()
	reader, openErr := OpenSegment(payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	_, frequencyErr := reader.TermFrequencies("keyword", []byte("term"))
	if !errors.Is(frequencyErr, ErrCorrupt) {
		t.Fatalf("TermFrequencies() error = %v, want ErrCorrupt", frequencyErr)
	}
}
