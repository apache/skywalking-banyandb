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
	"errors"
	"math"
	"testing"

	"github.com/klauspost/compress/s2"
)

type repairDocValueCheck struct {
	value    []byte
	document uint64
}

func TestRepairCursorOrdersTupleTies(t *testing.T) {
	values := [repairSortFieldCount][]byte{[]byte("group"), []byte("name"), []byte("entity"), []byte("timestamp")}
	cursor := &RepairCursor{SortValues: values[:], SegmentID: 2, DocumentNumber: 3}
	for _, testCase := range []struct {
		name           string
		segmentID      uint64
		documentNumber uint64
		want           int
	}{
		{"same row", 2, 3, 0},
		{"next document", 2, 4, 1},
		{"previous document", 2, 2, -1},
		{"next segment", 3, 0, 1},
		{"previous segment", 1, 9, -1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			candidate := repairTupleCandidate{sortValues: values, segmentID: testCase.segmentID, documentNumber: testCase.documentNumber}
			if got := compareRepairCandidateCursor(candidate, cursor); got != testCase.want {
				t.Fatalf("cursor comparison = %d, want %d", got, testCase.want)
			}
		})
	}
}

func TestRepairCandidateRejectsMissingSortField(t *testing.T) {
	reader := &repairSegmentPageReader{}
	_, candidateErr := reader.candidate(0, 1)
	if !errors.Is(candidateErr, ErrCorrupt) {
		t.Fatalf("missing field error = %v, want ErrCorrupt", candidateErr)
	}
}

func TestDecodeRepairDocValueTermEscapes(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		encoded   []byte
		want      []byte
		wantError bool
	}{
		{"invalid escape", []byte{'\\', 'A', 0xff}, nil, true},
		{"truncated escape", []byte{'\\'}, nil, true},
		{"escaped backslash", []byte{'\\', '\\', 0xff}, []byte{'\\'}, false},
		{"escaped separator", []byte{'\\', 0xff, 0xff}, []byte{0xff}, false},
		{"empty value", []byte{0xff}, []byte{}, false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			value, remaining, decodeErr := decodeRepairDocValueTerm(testCase.encoded)
			if (decodeErr != nil) != testCase.wantError {
				t.Fatalf("decode error = %v, want error %t", decodeErr, testCase.wantError)
			}
			if decodeErr == nil && (!bytes.Equal(value, testCase.want) || len(remaining) != 0) {
				t.Fatalf("decoded value = %x, remaining = %x, want %x", value, remaining, testCase.want)
			}
		})
	}
}

func TestRepairDocValueReaderAcceptsLegacyEmptyChunkPositions(t *testing.T) {
	emptyChunk := emptyLegacyDocValueChunk()
	for _, testCase := range []struct {
		name   string
		chunks [][]byte
		checks []repairDocValueCheck
	}{
		{
			name:   "leading",
			chunks: [][]byte{emptyChunk, populatedLegacyDocValueChunk(docValueDocumentsPerChunk)},
			checks: []repairDocValueCheck{{document: 0}, {document: docValueDocumentsPerChunk, value: []byte{'v'}}},
		},
		{
			name:   "interior",
			chunks: [][]byte{populatedLegacyDocValueChunk(0), emptyChunk, populatedLegacyDocValueChunk(2 * docValueDocumentsPerChunk)},
			checks: []repairDocValueCheck{{document: 0, value: []byte{'v'}}, {document: docValueDocumentsPerChunk}, {document: 2 * docValueDocumentsPerChunk, value: []byte{'v'}}},
		},
		{
			name:   "trailing",
			chunks: [][]byte{populatedLegacyDocValueChunk(0), emptyChunk},
			checks: []repairDocValueCheck{{document: 0, value: []byte{'v'}}, {document: docValueDocumentsPerChunk}},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			reader := newRepairDocValueChunkTestReader(testCase.chunks...)
			for _, check := range testCase.checks {
				values, valuesErr := reader.values(check.document)
				if valuesErr != nil {
					t.Fatalf("values(%d) error = %v, want no error", check.document, valuesErr)
				}
				if check.value == nil {
					if values != nil {
						t.Fatalf("values(%d) = %#v, want nil", check.document, values)
					}
					continue
				}
				if len(values) != 1 || !bytes.Equal(values[0], check.value) {
					t.Fatalf("values(%d) = %#v, want [[%s]]", check.document, values, check.value)
				}
			}
		})
	}
}

func TestRepairDocValueReaderRejectsMalformedLegacyEmptyChunk(t *testing.T) {
	for _, testCase := range []struct {
		name string
		tail []byte
	}{
		{name: "missing compression stream", tail: nil},
		{name: "malformed compression stream", tail: []byte{1}},
		{name: "nonempty compression stream", tail: s2.Encode(nil, []byte{'x'})},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			chunk := appendNativeUvarint(nil, 0)
			chunk = append(chunk, testCase.tail...)
			reader := newRepairDocValueChunkTestReader(chunk)
			if loadErr := reader.loadChunk(0); !errors.Is(loadErr, ErrCorrupt) {
				t.Fatalf("loadChunk() error = %v, want ErrCorrupt", loadErr)
			}
		})
	}
}

func newRepairDocValueChunkTestReader(chunks ...[]byte) *repairDocValueReader {
	data := make([]byte, 0)
	offsets := make([]uint64, len(chunks))
	for chunkIndex, chunk := range chunks {
		data = append(data, chunk...)
		offsets[chunkIndex] = uint64(len(data))
	}
	return &repairDocValueReader{
		chunkNumber:        math.MaxUint64,
		chunkOffsets:       offsets,
		path:               "test",
		file:               &byteSegmentFile{data: data},
		size:               uint64(len(data)),
		totalDocumentCount: uint64(len(chunks)) * docValueDocumentsPerChunk,
	}
}

func emptyLegacyDocValueChunk() []byte {
	chunk := appendNativeUvarint(nil, 0)
	return append(chunk, s2.Encode(nil, nil)...)
}

func populatedLegacyDocValueChunk(documentNumber uint64) []byte {
	chunk := appendNativeUvarint(nil, 1)
	chunk = appendNativeUvarint(chunk, documentNumber)
	chunk = appendNativeUvarint(chunk, 2)
	return append(chunk, s2.Encode(nil, []byte{'v', 0xff})...)
}
