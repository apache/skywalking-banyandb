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
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	roaringpkg "github.com/RoaringBitmap/roaring"
)

func writeTestSegmentFile(t testing.TB, generation Generation) string {
	t.Helper()
	payload, encodeErr := EncodeSegment(generation)
	requireNativeNoError(t, encodeErr)
	path := filepath.Join(t.TempDir(), "0000000000000001.seg")
	requireNativeNoError(t, os.WriteFile(path, payload, 0o600))
	return path
}

// TestPersistedSegmentServesMoreDocumentsThanOldOffsetIndexCap covers a segment
// past the 2 Mi documents the stored offset index was once capped at. Merges
// plan by bytes, so a busy shard produces such segments.
func TestPersistedSegmentServesMoreDocumentsThanOldOffsetIndexCap(t *testing.T) {
	const documentCount = (16<<20)/storedDocumentOffsetByteWidth + 1000
	documents := make([]EncodeDocument, documentCount)
	for documentIndex := range documents {
		identifier := make([]byte, 8)
		binary.BigEndian.PutUint64(identifier, uint64(documentIndex))
		documents[documentIndex] = EncodeDocument{Identifier: identifier}
	}
	reader, openErr := OpenSegmentFile(writeTestSegmentFile(t, Generation{Documents: documents}))
	requireNativeNoError(t, openErr)
	t.Cleanup(func() { requireNativeNoError(t, reader.Close()) })

	requireNativef(t, reader.DocumentCount() == documentCount, "document count = %d, want %d", reader.DocumentCount(), documentCount)
	visited := 0
	requireNativeNoError(t, reader.VisitDocument(documentCount-1, func(StoredDocument) error {
		visited++
		return nil
	}))
	requireNativef(t, visited == 1, "visited %d documents", visited)
}

// TestPersistedSegmentServesChunkAboveOldDecodedLimit covers a stored chunk of
// 128 large documents. The chunk size is fixed by the format, so the decoded
// size follows the documents and may pass the 64 MiB the reader once capped.
func TestPersistedSegmentServesChunkAboveOldDecodedLimit(t *testing.T) {
	const (
		documentCount = storedDocumentsPerChunk + 2
		valueLength   = 600 << 10
	)
	var state uint64 = 0x9e3779b97f4a7c15
	documents := make([]EncodeDocument, documentCount)
	var want []byte
	for documentIndex := range documents {
		value := make([]byte, valueLength)
		for byteIndex := range value {
			state ^= state << 7
			state ^= state >> 9
			state ^= state << 8
			value[byteIndex] = byte(state)
		}
		if documentIndex == 5 {
			want = value
		}
		identifier := make([]byte, 8)
		binary.BigEndian.PutUint64(identifier, uint64(documentIndex))
		documents[documentIndex] = EncodeDocument{
			Identifier: identifier,
			Fields:     []EncodeField{{Name: "content", Value: value, Store: true}},
		}
	}
	requireNativef(t, documentCount*valueLength > 64<<20, "chunk does not exceed the old decoded limit")
	reader, openErr := OpenSegmentFile(writeTestSegmentFile(t, Generation{Documents: documents}))
	requireNativeNoError(t, openErr)
	t.Cleanup(func() { requireNativeNoError(t, reader.Close()) })

	var got []byte
	requireNativeNoError(t, reader.VisitDocumentContext(context.Background(), 5, func(document StoredDocument) error {
		return document.VisitStoredFields(func(name string, value []byte) bool {
			if name == "content" {
				got = append([]byte(nil), value...)
			}
			return true
		})
	}))
	requireNativef(t, bytes.Equal(got, want), "stored value of document 5 differs: got %d bytes, want %d", len(got), len(want))
}

// TestMaxPostingsLengthCoversEncodedBitmaps checks the derived posting bound
// against real roaring serializations of the shapes that cost the most.
func TestMaxPostingsLengthCoversEncodedBitmaps(t *testing.T) {
	for _, documentCount := range []uint64{1, 100, 70_000, 5_000_000} {
		reader := storedSegmentReader{footer: segmentFooter{documentCount: documentCount}}
		bound := reader.maxPostingsLength()
		for name, step := range map[string]uint64{"dense": 1, "every-third": 3, "one-per-container": 65536, "sparse": 997} {
			bitmap := roaringpkg.New()
			for document := uint64(0); document < documentCount; document += step {
				bitmap.Add(uint32(document))
			}
			bitmap.RunOptimize()
			encoded, marshalErr := bitmap.ToBytes()
			requireNativeNoError(t, marshalErr)
			requireNativef(t, uint64(len(encoded)) <= bound, "%s over %d documents encodes to %d bytes, bound %d", name, documentCount, len(encoded), bound)
		}
	}
}

// TestPublishSnapshotRejectsManifestTheReaderWouldRefuse checks that an
// oversized manifest fails the publication before any file is written. Left to
// the reader, it would be skipped for an older snapshot and silently drop the
// generation.
func TestPublishSnapshotRejectsManifestTheReaderWouldRefuse(t *testing.T) {
	directory := t.TempDir()
	payload, encodeErr := EncodeSegment(Generation{Documents: []EncodeDocument{{Identifier: []byte("live")}}})
	requireNativeNoError(t, encodeErr)
	segments := []SnapshotSegmentPayload{{SnapshotSegment: SnapshotSegment{ID: 1, Size: uint64(len(payload)), DocumentCount: 1}, Payload: payload}}

	original := manifestSizeLimit
	manifestSizeLimit = 8
	publishErr := PublishSnapshot(directory, 1, segments)
	manifestSizeLimit = original
	requireNativeErrorIs(t, publishErr, errManifestTooLarge)
	entries, readErr := os.ReadDir(directory)
	requireNativeNoError(t, readErr)
	requireNativef(t, len(entries) == 0, "rejected publication left %d files in the index directory", len(entries))

	requireNativeNoError(t, PublishSnapshot(directory, 1, segments))
	reader, openErr := OpenStrict(directory)
	requireNativeNoError(t, openErr)
	requireNativeNoError(t, reader.Close())
}
