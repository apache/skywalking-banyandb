// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
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
	"context"
	"encoding/binary"
	"errors"
	"testing"
)

func TestOpenSegmentAcceptsLegacyExactBoundaryTerminalOffset(t *testing.T) {
	payload := storedChunkBoundaryPayload(t, 128)
	legacyPayload := duplicateStoredTerminalOffset(t, payload)
	reader, err := OpenSegment(legacyPayload)
	if err != nil {
		t.Fatalf("OpenSegment() rejected legacy terminal offset: %v", err)
	}
	visited := 0
	if visitErr := reader.VisitPhysicalDocuments(context.Background(), func(document StoredDocument, _ bool) error {
		visited++
		return document.VisitStoredFields(func(string, []byte) bool { return true })
	}); visitErr != nil {
		t.Fatal(visitErr)
	}
	if visited != 128 {
		t.Fatalf("visited %d documents; want 128", visited)
	}
	if closeErr := reader.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
}

func TestOpenSegmentRejectsLegacyTerminalOffsetAwayFromBoundary(t *testing.T) {
	payload := storedChunkBoundaryPayload(t, 127)
	legacyPayload := duplicateStoredTerminalOffset(t, payload)
	if _, err := OpenSegment(legacyPayload); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("OpenSegment() error = %v, want ErrCorrupt", err)
	}
}

func TestOpenSegmentRejectsMultipleLegacyTerminalOffsets(t *testing.T) {
	payload := storedChunkBoundaryPayload(t, 128)
	legacyPayload := duplicateStoredTerminalOffsets(t, payload, 2)
	if _, err := OpenSegment(legacyPayload); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("OpenSegment() error = %v, want ErrCorrupt", err)
	}
}

func storedChunkBoundaryPayload(t *testing.T, documentCount int) []byte {
	t.Helper()
	documents := make([]EncodeDocument, documentCount)
	for documentIndex := range documents {
		documents[documentIndex] = EncodeDocument{Identifier: []byte{byte(documentIndex), 1}}
	}
	payload, err := EncodeSegment(Generation{Documents: documents})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func duplicateStoredTerminalOffset(t *testing.T, payload []byte) []byte {
	return duplicateStoredTerminalOffsets(t, payload, 1)
}

func duplicateStoredTerminalOffsets(t *testing.T, payload []byte, extraCount int) []byte {
	t.Helper()
	file := &byteSegmentFile{data: payload}
	footer, err := readSegmentFooter(file, uint64(len(payload)), "test")
	if err != nil {
		t.Fatal(err)
	}
	tableFooterOffset := int(footer.storedIndexOffset) - storedChunkTableFooterLength
	offsetLength := binary.BigEndian.Uint32(payload[tableFooterOffset : tableFooterOffset+4])
	chunkCount := binary.BigEndian.Uint32(payload[tableFooterOffset+4 : tableFooterOffset+8])
	tableStart := tableFooterOffset - int(offsetLength)
	decoder := byteDecoder{payload: payload[tableStart:tableFooterOffset]}
	var terminal uint64
	for range chunkCount {
		var decodeErr error
		terminal, decodeErr = decoder.uvarint()
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
	}
	extra := make([]byte, 0, extraCount*binary.MaxVarintLen64)
	for range extraCount {
		extra = appendNativeUvarint(extra, terminal)
	}
	insertAt := tableFooterOffset
	legacy := make([]byte, 0, len(payload)+len(extra))
	legacy = append(legacy, payload[:insertAt]...)
	legacy = append(legacy, extra...)
	legacy = append(legacy, payload[insertAt:]...)
	newTableFooterOffset := tableFooterOffset + len(extra)
	binary.BigEndian.PutUint32(legacy[newTableFooterOffset:newTableFooterOffset+4], offsetLength+uint32(len(extra)))
	binary.BigEndian.PutUint32(legacy[newTableFooterOffset+4:newTableFooterOffset+8], chunkCount+uint32(extraCount))
	newFooterOffset := len(legacy) - segmentFooterLength
	binary.BigEndian.PutUint64(legacy[newFooterOffset+8:newFooterOffset+16], footer.storedIndexOffset+uint64(len(extra)))
	binary.BigEndian.PutUint64(legacy[newFooterOffset+16:newFooterOffset+24], footer.fieldsIndexOffset+uint64(len(extra)))
	if footer.docValueOffset != ^uint64(0) {
		binary.BigEndian.PutUint64(legacy[newFooterOffset+24:newFooterOffset+32], footer.docValueOffset+uint64(len(extra)))
	}
	return legacy
}
