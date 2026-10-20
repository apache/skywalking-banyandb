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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	common "github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
)

func TestNativeElementIndexRequiresLeaseAndWrites(t *testing.T) {
	root := t.TempDir()
	_, err := newElementIndex(context.Background(), root, 0, nil, nil)
	require.Error(t, err)
	_, statErr := os.Stat(filepath.Join(root, elementIndexFilename))
	require.ErrorIs(t, statErr, os.ErrNotExist)
	lease := newTestRootLease(t, root)
	element, err := newElementIndex(context.Background(), root, 0, nil, nil, lease)
	require.NoError(t, err)
	field := index.NewStringField(index.FieldKey{IndexRuleID: 1, SeriesID: 1}, "ok")
	field.Store = true
	require.NoError(t, element.Write(index.Documents{{DocID: 1, Timestamp: 100, Fields: []index.Field{field}}}))
	iterator, err := element.Sort(context.Background(), []common.SeriesID{1}, field.Key, modelv1.Sort_SORT_ASC, nil, 1)
	require.NoError(t, err)
	require.True(t, iterator.Next())
	require.Equal(t, uint64(1), iterator.Val().DocID)
	require.Equal(t, int64(100), iterator.Val().Timestamp)
	require.Equal(t, common.SeriesID(1), iterator.Val().SeriesID)
	require.Equal(t, []byte("ok"), iterator.Val().SortedValue)
	require.NoError(t, iterator.Close())
	snapshotPath := filepath.Join(t.TempDir(), "snapshot")
	require.NoError(t, element.store.TakeFileSnapshot(snapshotPath))
	snapshotReader, snapshotErr := native.OpenReadOnlyGeneration(snapshotPath)
	require.NoError(t, snapshotErr)
	stored, storedErr := snapshotReader.StoredFields(context.Background(), convert.Uint64ToBytes(1), "\x00\x00\x00\x01")
	require.NoError(t, storedErr)
	require.Equal(t, map[string][][]byte{"\x00\x00\x00\x01": {[]byte("ok")}}, stored)
	require.NoError(t, snapshotReader.Close())
	require.NoError(t, element.Close())
	element, err = newElementIndex(context.Background(), root, 0, nil, nil, lease)
	require.NoError(t, err)
	defer element.Close()
	iterator, err = element.Sort(context.Background(), []common.SeriesID{1}, field.Key, modelv1.Sort_SORT_ASC, nil, 1)
	require.NoError(t, err)
	require.True(t, iterator.Next())
	require.Equal(t, uint64(1), iterator.Val().DocID)
	require.NoError(t, iterator.Close())
}

func TestNativeElementIndexAdmitsStoredRowsAfterRequestCancellation(t *testing.T) {
	root := t.TempDir()
	element, err := newElementIndex(context.Background(), root, 0, nil, nil, newTestRootLease(t, root))
	require.NoError(t, err)
	defer element.Close()
	field := index.NewStringField(index.FieldKey{IndexRuleID: 1, SeriesID: 1}, "ok")
	field.Store = true
	docs := index.Documents{{DocID: 1, Timestamp: 100, Fields: []index.Field{field}}}

	// The write handlers store raw elements before indexing them, and the
	// request context may be canceled in between. Indexing with that context
	// is rejected, so the handlers pass it through context.WithoutCancel.
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, element.WriteContext(requestCtx, docs), context.Canceled)
	require.NoError(t, element.WriteContext(context.WithoutCancel(requestCtx), docs))

	iterator, err := element.Sort(context.Background(), []common.SeriesID{1}, field.Key, modelv1.Sort_SORT_ASC, nil, 1)
	require.NoError(t, err)
	require.True(t, iterator.Next(), "the stored row must be indexed")
	require.Equal(t, uint64(1), iterator.Val().DocID)
	require.NoError(t, iterator.Close())
}
