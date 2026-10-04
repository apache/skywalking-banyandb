// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package inverted

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestNativeSeriesStoreQueriesAndDeduplicatesInMemorySegment(t *testing.T) {
	root := t.TempDir()
	localFS := fs.NewLocalFileSystem()
	lock, lockErr := localFS.CreateLockFile(filepath.Join(root, "lock"), 0o600)
	require.NoError(t, lockErr)
	owner, ownerErr := NewNativeWriterOwner(lock, root)
	require.NoError(t, ownerErr)
	store, storeErr := NewNativeStore(StoreOpts{
		Path:   filepath.Join(root, "shard-0"),
		Logger: logger.GetLogger("native-lazy-test"),
	}, owner)
	require.NoError(t, storeErr)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
		require.NoError(t, owner.Close())
	})

	batch := index.Batch{Documents: make(index.Documents, 1000)}
	for documentIndex := range batch.Documents {
		seriesID := []byte(fmt.Sprintf("series-%04d", documentIndex))
		batch.Documents[documentIndex] = index.Document{EntityValues: seriesID, Fields: []index.Field{
			index.NewStringField(index.FieldKey{IndexRuleID: 7}, string(seriesID)),
		}}
	}
	require.NoError(t, store.InsertSeriesBatch(batch))
	require.NoError(t, store.Close())
	store, storeErr = NewNativeStore(StoreOpts{
		Path:   filepath.Join(root, "shard-0"),
		Logger: logger.GetLogger("native-lazy-test"),
	}, owner)
	require.NoError(t, storeErr)
	require.NoError(t, store.InsertSeriesBatch(batch))

	query, queryErr := store.BuildQuery([]index.SeriesMatcher{{Type: index.SeriesMatcherTypeExact, Match: []byte("series-0420")}}, nil, nil)
	require.NoError(t, queryErr)
	result, searchErr := store.Search(context.Background(), nil, query, 0)
	require.NoError(t, searchErr)
	require.Len(t, result, 1)
	require.Equal(t, []byte("series-0420"), result[0].Key.EntityValues)

	iterator, iteratorErr := store.SeriesIterator(context.Background())
	require.NoError(t, iteratorErr)
	// Publishing another batch replaces the snapshot while the dictionary
	// iterator still owns the reader that opened the previous persisted bytes.
	require.NoError(t, store.InsertSeriesBatch(batch))
	seriesCount := 0
	for iterator.Next() {
		seriesCount++
	}
	require.Equal(t, 1000, seriesCount)
	require.NoError(t, iterator.Close())
	require.NoError(t, iterator.Close())

	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	canceledIterator, canceledErr := store.SeriesIterator(canceledContext)
	require.NoError(t, canceledErr)
	require.False(t, canceledIterator.Next())
	require.ErrorIs(t, canceledIterator.Close(), context.Canceled)
	require.ErrorIs(t, canceledIterator.Close(), context.Canceled)
}
