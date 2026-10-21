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
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

func TestSelectedStoredChunkRejectsBeforeDecodedAllocation(t *testing.T) {
	documents := make([]EncodeDocument, storedDocumentsPerChunk+1)
	for i := range documents {
		value := "small"
		if i == storedDocumentsPerChunk {
			value = strings.Repeat("large", 1<<18)
		}
		documents[i] = EncodeDocument{Identifier: []byte(fmt.Sprintf("doc-%d", i)), Fields: []EncodeField{{Name: "value", Value: []byte(value), Store: true}}}
	}
	payload, err := EncodeSegment(Generation{Documents: documents})
	require.NoError(t, err)
	reader, err := OpenSegment(payload)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	failure := errors.New("selected allocation exceeds budget")
	var used int64
	reserve := func(bytes int64) error {
		if bytes > 64*1024-used {
			return failure
		}
		used += bytes
		return nil
	}
	release := func(bytes int64) { used -= bytes }
	ctx, scope := query.WithAllocationScope(context.Background(), reserve, release)
	defer func() { require.NoError(t, scope.Close()) }()
	require.NoError(t, reader.VisitDocumentContext(ctx, 0, func(StoredDocument) error { return nil }), "a point read must not reserve the unrelated large chunk")
	require.Positive(t, used)
	require.NoError(t, scope.Close())
	require.Zero(t, used)
	stored, err := reader.storedReader(0)
	require.NoError(t, err)
	beforeCapacity := cap(stored.decodedBuffer)
	ctx, scope = query.WithAllocationScope(context.Background(), reserve, release)
	visited := false
	err = reader.VisitDocumentContext(ctx, storedDocumentsPerChunk, func(StoredDocument) error { visited = true; return nil })
	require.ErrorIs(t, err, failure)
	require.False(t, visited)
	require.Equal(t, beforeCapacity, cap(stored.decodedBuffer), "rejection must precede decoded buffer growth")
	require.NoError(t, scope.Close())
	require.Zero(t, used)
	// Warm the selected chunk outside this query; a cached borrow still consumes
	// the new query allowance even though the reader does not allocate again.
	require.NoError(t, reader.VisitDocument(storedDocumentsPerChunk, func(StoredDocument) error { return nil }))
	ctx, scope = query.WithAllocationScope(context.Background(), reserve, release)
	defer func() { require.NoError(t, scope.Close()) }()
	require.ErrorIs(t, reader.VisitDocumentContext(ctx, storedDocumentsPerChunk, func(StoredDocument) error { return nil }), failure)
}

func TestSelectedDocValueChunkRejectsBeforeDecodedAllocation(t *testing.T) {
	payload, err := EncodeSegment(Generation{Documents: []EncodeDocument{{
		Identifier: []byte("doc"),
		Fields:     []EncodeField{{Name: "sort", Value: []byte(strings.Repeat("large", 1<<12)), Sort: true}},
	}}})
	require.NoError(t, err)
	reader, err := OpenSegment(payload)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	failure := errors.New("doc-value allocation exceeds budget")
	var used int64
	ctx, scope := query.WithAllocationScope(context.Background(), func(bytes int64) error {
		if bytes > 4096-used {
			return failure
		}
		used += bytes
		return nil
	}, func(bytes int64) { used -= bytes })
	defer func() { require.NoError(t, scope.Close()) }()
	_, err = reader.DocumentValuesContext(ctx, "sort", 0)
	require.ErrorIs(t, err, failure)
	require.Zero(t, cap(reader.docValueReaders["sort"].decodedBuffer))
	require.NoError(t, scope.Close())
	require.Zero(t, used)
}

func TestSelectedPostingReservationPrecedesBitmapDecode(t *testing.T) {
	documents := make([]EncodeDocument, 256)
	for i := range documents {
		documents[i] = EncodeDocument{Identifier: []byte(fmt.Sprintf("doc-%d", i)), Fields: []EncodeField{{Name: "status", Value: []byte("common"), Index: true}}}
	}
	documents[0].Fields = append(documents[0].Fields, EncodeField{Name: "rare", Value: []byte("point"), Index: true})
	payload, err := EncodeSegment(Generation{Documents: documents})
	require.NoError(t, err)
	reader, err := OpenSegment(payload)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	failure := errors.New("posting allowance exhausted")
	var used int64
	ctx, scope := query.WithAllocationScope(context.Background(), func(bytes int64) error {
		if bytes > 1024-used {
			return failure
		}
		used += bytes
		return nil
	}, func(bytes int64) { used -= bytes })
	defer func() { require.NoError(t, scope.Close()) }()
	posting, found, err := reader.TermPostingBitmapContext(ctx, "rare", []byte("point"))
	require.NoError(t, err, "a point term must not charge an unrelated common posting")
	require.True(t, found)
	require.Equal(t, uint64(1), posting.GetCardinality())
	stored, err := reader.storedReader(0)
	require.NoError(t, err)
	dictionary, err := stored.dictionary("status")
	require.NoError(t, err)
	offset, found, err := lookupTermPosting(dictionary, []byte("common"))
	require.NoError(t, err)
	require.True(t, found)
	cursor := offset
	for i := 0; i < 3; i++ {
		_, err = stored.readUvarint(&cursor, stored.footer.docValueOffset)
		require.NoError(t, err)
	}
	stored.file.(*byteSegmentFile).data[cursor] = 0
	_, _, err = reader.TermPostingBitmapContext(ctx, "status", []byte("common"))
	require.ErrorIs(t, err, failure, "allowance rejection must happen before decoding the corrupt selected bitmap")
	_, _, err = reader.TermPostingBitmap("status", []byte("common"))
	require.ErrorIs(t, err, ErrCorrupt)
	require.NoError(t, scope.Close())
	require.Zero(t, used)
}
