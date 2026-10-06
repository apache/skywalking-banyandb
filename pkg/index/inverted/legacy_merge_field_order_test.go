// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package inverted

import (
	"bytes"
	"testing"

	roaring "github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

// TestLegacyFastCopyMergePreservesStoredFieldsBeforeIdentifier is a
// compatibility oracle: it proves the native encoder's merged output is
// byte-compatible with the retired legacy ICE reader/merger, so this stays in
// pkg/index/inverted (which already depends on the retired packages in
// production) rather than inside pkg/index/internal/nativeice, whose
// dependency boundary excludes every retired package, test files included.
func TestLegacyFastCopyMergePreservesStoredFieldsBeforeIdentifier(t *testing.T) {
	firstPayload, err := nativeice.EncodeSegment(nativeice.Generation{Documents: []nativeice.EncodeDocument{{
		Identifier: []byte("one"),
		Fields:     []nativeice.EncodeField{{Name: "_a", Value: []byte("one-a"), Index: true, Store: true}},
	}}})
	require.NoError(t, err)
	secondPayload, err := nativeice.EncodeSegment(nativeice.Generation{Documents: []nativeice.EncodeDocument{{
		Identifier: []byte("two"),
		Fields:     []nativeice.EncodeField{{Name: "_a", Value: []byte("two-a"), Index: true, Store: true}},
	}}})
	require.NoError(t, err)
	first, err := loadLegacySegment(newSegmentBytes(firstPayload))
	require.NoError(t, err)
	second, err := loadLegacySegment(newSegmentBytes(secondPayload))
	require.NoError(t, err)
	merged := mergeLegacySegments([]segmentValue{first, second}, []*roaring.Bitmap{nil, nil})
	var mergedPayload bytes.Buffer
	_, err = merged.WriteTo(&mergedPayload, nil)
	require.NoError(t, err)

	legacyMerged, err := loadLegacySegment(newSegmentBytes(mergedPayload.Bytes()))
	require.NoError(t, err)
	for documentNumber, expected := range []map[string][]byte{
		{"_a": []byte("one-a"), "_id": []byte("one")},
		{"_a": []byte("two-a"), "_id": []byte("two")},
	} {
		fields := make(map[string][]byte)
		require.NoError(t, legacyMerged.VisitStoredFields(uint64(documentNumber), func(name string, value []byte) bool {
			fields[name] = append([]byte(nil), value...)
			return true
		}))
		require.Equal(t, expected, fields)
	}
	legacyDictionary, err := legacyMerged.Dictionary("_a")
	require.NoError(t, err)
	legacyPostings, err := legacyDictionary.PostingsList([]byte("two-a"), nil, nil)
	require.NoError(t, err)
	legacyIterator, err := legacyPostings.Iterator(false, false, false, nil)
	require.NoError(t, err)
	legacyPosting, err := legacyIterator.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), legacyPosting.Number())
	require.NoError(t, legacyIterator.Close())
	require.NoError(t, legacyDictionary.Close())

	nativeMerged, err := nativeice.OpenSegment(mergedPayload.Bytes())
	require.NoError(t, err)
	defer func() { require.NoError(t, nativeMerged.Close()) }()
	for documentNumber, expected := range []struct {
		identifier string
		field      string
		term       string
	}{
		{identifier: "one", field: "_a", term: "one-a"},
		{identifier: "two", field: "_a", term: "two-a"},
	} {
		stored := make(map[string][]byte)
		err := nativeMerged.VisitDocument(uint64(documentNumber), func(document nativeice.StoredDocument) error {
			return document.VisitStoredFields(func(name string, value []byte) bool {
				stored[name] = append([]byte(nil), value...)
				return true
			})
		})
		require.NoError(t, err)
		require.Equal(t, []byte(expected.identifier), stored[docIDField])
		require.Equal(t, []byte(expected.term), stored[expected.field])
		posting, found, err := nativeMerged.TermPosting(expected.field, []byte(expected.term))
		require.NoError(t, err)
		require.True(t, found)
		if posting.OneHit {
			require.Equal(t, uint64(documentNumber), posting.DocumentNumber)
		} else {
			require.Equal(t, []uint64{uint64(documentNumber)}, posting.Documents)
		}
	}
}
