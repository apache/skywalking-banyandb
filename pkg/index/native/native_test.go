// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package native

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

func TestReadOnlyGenerationStoredFieldsUsesFirstLiveDocument(t *testing.T) {
	path := t.TempDir()
	require.NoError(t, nativeice.Encode(path, nativeice.Generation{Documents: []nativeice.EncodeDocument{
		{Identifier: []byte("series-a"), Deleted: true, Fields: []nativeice.EncodeField{{Name: "tag", Value: []byte("deleted"), Store: true}}},
		{Identifier: []byte("series-a"), Fields: []nativeice.EncodeField{
			{Name: "tag", Value: []byte("one"), Store: true},
			{Name: "tag", Value: []byte("two"), Store: true},
			{Name: "_timestamp", Value: []byte("internal"), Store: true},
		}},
		{Identifier: []byte("series-a"), Fields: []nativeice.EncodeField{{Name: "tag", Value: []byte("later"), Store: true}}},
	}}))

	reader, err := OpenReadOnlyGeneration(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()

	fields, err := reader.StoredFields(context.Background(), []byte("series-a"), "tag", "_timestamp")
	require.NoError(t, err)
	require.Equal(t, map[string][][]byte{"tag": {[]byte("one"), []byte("two")}}, fields)

	missing, err := reader.StoredFields(context.Background(), []byte("missing"))
	require.NoError(t, err)
	require.Nil(t, missing)
}

func TestReadOnlyGenerationStoredFieldsReadsRetainedCompatibilityFixture(t *testing.T) {
	fixture := filepath.Join("..", "inverted", "testdata", "nidx01c", "sourceA")
	reader, err := OpenReadOnlyGeneration(fixture)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	fields, err := reader.StoredFields(context.Background(), []byte{0x01, 0x02, 0x03}, "color", "_timestamp")
	require.NoError(t, err)
	require.Equal(t, map[string][][]byte{"color": {[]byte("blue"), []byte("green")}}, fields)
}

func TestReadOnlyGenerationVisitIdentifiersStopsAndCopies(t *testing.T) {
	path := t.TempDir()
	require.NoError(t, nativeice.Encode(path, nativeice.Generation{Documents: []nativeice.EncodeDocument{
		{Identifier: []byte("a")},
		{Identifier: []byte("b")},
	}}))
	reader, err := OpenReadOnlyGeneration(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()

	var got [][]byte
	err = reader.VisitIdentifiers(context.Background(), func(identifier []byte) bool {
		got = append(got, identifier)
		return false
	})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a")}, got)
	got[0][0] = 'x'
	var all [][]byte
	require.NoError(t, reader.VisitIdentifiers(context.Background(), func(identifier []byte) bool {
		all = append(all, identifier)
		return true
	}))
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, all)
}

func TestReadOnlyGenerationVisitIdentifiersIncludesDeletedMetadataTerms(t *testing.T) {
	fixture := filepath.Join("..", "inverted", "testdata", "nidx01c", "sourceA")
	reader, err := OpenReadOnlyGeneration(fixture)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	var identifiers [][]byte
	require.NoError(t, reader.VisitIdentifiers(context.Background(), func(identifier []byte) bool {
		identifiers = append(identifiers, identifier)
		return true
	}))
	require.Contains(t, identifiers, []byte{0x07, 0x08, 0x09})
}

func TestReadOnlyGenerationVisitIdentifiersWalksMultipleSegments(t *testing.T) {
	fixture := filepath.Join("..", "inverted", "testdata", "nidx01b", "index")
	reader, err := OpenReadOnlyGeneration(fixture)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	var identifiers [][]byte
	require.NoError(t, reader.VisitIdentifiers(context.Background(), func(identifier []byte) bool {
		identifiers = append(identifiers, identifier)
		return true
	}))
	require.Len(t, identifiers, 5)
	require.Contains(t, identifiers, convert.Uint64ToBytes(22))
}

func TestReadOnlyGenerationStoredFieldsHonorsCancellation(t *testing.T) {
	path := t.TempDir()
	require.NoError(t, nativeice.Encode(path, nativeice.Generation{Documents: []nativeice.EncodeDocument{{Identifier: []byte("a")}}}))
	reader, err := OpenReadOnlyGeneration(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = reader.StoredFields(ctx, []byte("a"))
	require.ErrorIs(t, err, context.Canceled)
	assertNotStopVisit(t, err)
}

func TestReadOnlyGenerationVisitIdentifiersHonorsCancellation(t *testing.T) {
	path := t.TempDir()
	require.NoError(t, nativeice.Encode(path, nativeice.Generation{Documents: []nativeice.EncodeDocument{{Identifier: []byte("a")}}}))
	reader, err := OpenReadOnlyGeneration(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, reader.VisitIdentifiers(ctx, func([]byte) bool { return true }), context.Canceled)
}

func assertNotStopVisit(t *testing.T, err error) {
	t.Helper()
	require.False(t, errors.Is(err, errStopVisit))
}
