// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
package stream

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/blugelabs/bluge"
	blugesearch "github.com/blugelabs/bluge/search"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
)

// TestNativeElementIndexSegmentsCarryIdentifierDocValuesForRollback is the
// NIDX-03 §16 regression: the Stream element index went native in #1390
// without "_id" doc values, so the previous release's bluge reader read
// every document's identifier back as the zero value after a rollback.
// pkg/index/nativeadapter.NewStore now opens the element index's owner with
// IdentifierDocValues: true (the same setting banyand/internal/storage's
// series index has used since NIDX-03 phase 2), so this proves a fresh
// element-index segment carries "_id" as a doc value a legacy bluge.Reader
// can load, mirroring banyand/measure/migration_indexmode_copy_test.go's
// readIndexModeDocsLegacy and pkg/index/inverted's own legacy-compatibility
// oracles (for example legacy_merge_field_order_test.go).
//
// R7 (NIDX-03 §12 item 2's element-index extension): a fresh single-segment
// write already carries doc values trivially (termAbsent's own prepared
// filter and a plain docID->field write never touch a merge path); the
// regression this guards is specifically about a segment the native owner's
// *merge* produced, since a merge builds a brand-new on-disk segment from
// scratch and is the one place "doc values on write" could have been
// forgotten for the merged output. So this writes across several batches
// (forcing several live segments), explicitly drives one bounded merge via
// the owner's Compact, and only then captures the doc-values/_id check --
// across TWO documents with distinct timestamps and field values, so the
// per-document identity is proven, not just presence of the _id field.
func TestNativeElementIndexSegmentsCarryIdentifierDocValuesForRollback(t *testing.T) {
	root := t.TempDir()
	lease := newTestRootLease(t, root)
	element, err := newElementIndex(context.Background(), root, 0, nil, lease)
	require.NoError(t, err)
	defer func() { require.NoError(t, element.Close()) }()

	type doc struct {
		value     string
		docID     uint64
		timestamp int64
	}
	docs := []doc{
		{docID: 42, timestamp: 100, value: "ok-42"},
		{docID: 43, timestamp: 200, value: "ok-43"},
	}
	// One Write call per document: each becomes its own live segment (no
	// batch-level collapsing), so the owner has more than one segment to
	// merge below.
	for _, d := range docs {
		field := index.NewStringField(index.FieldKey{IndexRuleID: 1, SeriesID: 7}, d.value)
		field.Store = true
		require.NoError(t, element.Write(index.Documents{{DocID: d.docID, Timestamp: d.timestamp, Fields: []index.Field{field}}}))
	}

	// Drive one bounded merge (pkg/index/native/owner.go's Compact): a
	// tiered-merge no-op (nil, nil) is still "settled" for a 2-segment
	// fixture this small, so a single call either merges both into one new
	// segment or confirms there was nothing eligible to merge -- either way,
	// the subsequent read exercises whatever segment layout the owner
	// actually produced, not an assumption about it.
	require.NoError(t, element.store.Owner.Compact(context.Background()))

	// Flush to disk: TakeFileSnapshot streams the committed generation's
	// disk-backed segments, which the previous release's bluge reader opens
	// the same way migration_indexmode_copy_test.go's readIndexModeDocsLegacy
	// opens a sidx directory.
	snapshotPath := filepath.Join(t.TempDir(), "snapshot")
	require.NoError(t, element.store.TakeFileSnapshot(snapshotPath))

	reader, openErr := bluge.OpenReader(bluge.DefaultConfig(snapshotPath))
	require.NoError(t, openErr, "the previous release's reader must still open a native-written element-index segment")
	defer func() { _ = reader.Close() }()

	matches, searchErr := reader.Search(context.Background(), bluge.NewAllMatches(bluge.NewMatchAllQuery()))
	require.NoError(t, searchErr)

	seen := make(map[uint64]doc, len(docs))
	for {
		match, nextErr := matches.Next()
		require.NoError(t, nextErr)
		if match == nil {
			break
		}
		require.NoError(t, match.LoadDocumentValues(blugesearch.NewSearchContext(1, 0), []string{"_id", "_timestamp"}))
		idValues := match.DocValues("_id")
		require.NotEmpty(t, idValues, `"_id" doc values must be present for the previous release to read document identity after a rollback`)
		docID := convert.BytesToUint64(idValues[0])
		var timestamp int64
		if timestampValues := match.DocValues("_timestamp"); len(timestampValues) > 0 {
			// "_timestamp" is prefix-coded (shift-zero layout), not a plain
			// int64 big-endian value -- see native.DecodeTimestamp and
			// pkg/index/native/owner.go's newMemorySegment encoder.
			decoded, decodeErr := native.DecodeTimestamp(timestampValues[0])
			require.NoError(t, decodeErr)
			timestamp = decoded
		}
		wantFieldName := index.FieldKey{IndexRuleID: 1, SeriesID: 7}.Marshal()
		var fieldValue string
		visitErr := match.VisitStoredFields(func(field string, value []byte) bool {
			if field == wantFieldName {
				fieldValue = string(value)
			}
			return true
		})
		require.NoError(t, visitErr)
		seen[docID] = doc{docID: docID, timestamp: timestamp, value: fieldValue}
	}

	require.Len(t, seen, len(docs), "every document must survive the merge, not just the first or the last")
	for _, want := range docs {
		got, found := seen[want.docID]
		require.True(t, found, "document %d must be present after the merge", want.docID)
		require.Equal(t, want.timestamp, got.timestamp, "document %d's timestamp must survive the merge", want.docID)
		require.Equal(t, want.value, got.value, "document %d's field value must survive the merge", want.docID)
	}
}
