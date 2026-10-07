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

package stream

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

// TestCountElementIndexDocsNativeMigrationWriter proves CountElementIndexDocs
// (NIDX-04's rename + native cutover of the former raw-engine doc counter)
// correctly counts an idx/ directory produced the same way the migration
// writer itself produces one: through targetIdxStore.get + Batch + closeAll
// (migration_element_index.go), never through a live elementIndex/TSDB.
func TestCountElementIndexDocsNativeMigrationWriter(t *testing.T) {
	idxPath := filepath.Join(t.TempDir(), "shard-0", elementIndexFilename)
	stores := newTargetIdxStore()
	store, err := stores.get(idxPath)
	require.NoError(t, err)

	field1 := index.NewStringField(index.FieldKey{IndexRuleID: 1, SeriesID: 1}, "ok")
	field1.Store = true
	field2 := index.NewStringField(index.FieldKey{IndexRuleID: 1, SeriesID: 2}, "err")
	field2.Store = true
	docs := index.Documents{
		{DocID: 1001, Timestamp: 100, Fields: []index.Field{field1}},
		{DocID: 1002, Timestamp: 200, Fields: []index.Field{field2}},
	}
	require.NoError(t, store.Batch(context.Background(), index.Batch{Documents: docs}))
	// closeAll flushes and durably persists every store it opened; a close
	// failure means an incomplete idx/ on disk, so it is asserted here just
	// like the real migration's deferred closeAll error handling.
	require.NoError(t, stores.closeAll())

	count, countErr := CountElementIndexDocs(idxPath)
	require.NoError(t, countErr)
	require.EqualValues(t, len(docs), count, "native-migrated idx/ must count exactly the docs the migration wrote")
}

// TestCountElementIndexDocsLegacyWrittenIdx proves CountElementIndexDocs reads
// an idx/ directory the OLD retired third-party engine wrote
// (pkg/index/inverted, kept around purely as a legacy-compatibility writer)
// just as well as a native-written one -- the scenario the element index's
// "direct stream copy" migration hits when it byte-copies a previous-release
// idx/ directory instead of rebuilding it.
func TestCountElementIndexDocsLegacyWrittenIdx(t *testing.T) {
	idxPath := filepath.Join(t.TempDir(), "shard-0", elementIndexFilename)
	require.NoError(t, os.MkdirAll(idxPath, storage.DirPerm))
	legacyStore, err := inverted.NewStore(inverted.StoreOpts{Path: idxPath, BatchWaitSec: 0})
	require.NoError(t, err)

	field1 := index.NewStringField(index.FieldKey{IndexRuleID: 1, SeriesID: 1}, "ok")
	field1.Store = true
	field2 := index.NewStringField(index.FieldKey{IndexRuleID: 1, SeriesID: 2}, "err")
	field2.Store = true
	field3 := index.NewStringField(index.FieldKey{IndexRuleID: 1, SeriesID: 3}, "ok")
	field3.Store = true
	docs := index.Documents{
		{DocID: 2001, Timestamp: 100, Fields: []index.Field{field1}},
		{DocID: 2002, Timestamp: 200, Fields: []index.Field{field2}},
		{DocID: 2003, Timestamp: 300, Fields: []index.Field{field3}},
	}
	require.NoError(t, legacyStore.Batch(index.Batch{Documents: docs}))
	require.NoError(t, legacyStore.Close())

	count, countErr := CountElementIndexDocs(idxPath)
	require.NoError(t, countErr)
	require.EqualValues(t, len(docs), count, "a legacy-engine-written idx/ must count exactly the docs it holds")
}

// TestCountElementIndexDocsEmptyDirReturnsZero mirrors CountSeriesIndexDocs'
// ErrNoSnapshot handling: a directory that was created but never written to
// (no committed generation at all) counts as 0 docs, not an error, since
// EnumerateGroupTarget walks every seg/shard directory on disk regardless of
// whether an element index ever received any document.
func TestCountElementIndexDocsEmptyDirReturnsZero(t *testing.T) {
	idxPath := filepath.Join(t.TempDir(), "shard-0", elementIndexFilename)
	require.NoError(t, os.MkdirAll(idxPath, storage.DirPerm))

	count, countErr := CountElementIndexDocs(idxPath)
	require.NoError(t, countErr)
	require.Zero(t, count)
}
