// Licensed to Apache Software Foundation (ASF) under one or more
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

package stream

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/index/nativeadapter"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

func TestNativeMatchFieldPresence(t *testing.T) {
	root := t.TempDir()
	lock, err := fs.NewLocalFileSystem().CreateLockFile(filepath.Join(root, "lock"), 0o600)
	require.NoError(t, err)
	lease, err := native.NewFileRootLease(lock, root)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lease.Revoke())
		require.NoError(t, lock.Close())
	}()
	ownerPath := filepath.Join(root, "index")
	owner, err := native.NewOwner(native.OwnerOptions{Lease: lease, Path: ownerPath, CompactionThreshold: -1})
	require.NoError(t, err)
	waitPersist := func() {
		persisted := make(chan error, 1)
		require.NoError(t, owner.Batch(context.Background(), native.Batch{PersistentCallback: func(callbackErr error) { persisted <- callbackErr }}))
		require.NoError(t, <-persisted)
	}

	seriesOne := convert.Uint64ToBytes(1)
	seriesTwo := convert.Uint64ToBytes(2)
	term := func(value string) []native.Term {
		return []native.Term{{Value: []byte(value), Frequency: 1}}
	}
	seriesField := func(value []byte) native.Field {
		return native.Field{Name: "_series_id", Value: value, Terms: []native.Term{{Value: value, Frequency: 1}}, Store: true, Index: true}
	}
	presentField := func(value string) native.Field {
		return native.Field{Name: string(convert.Uint32ToBytes(7)), Value: []byte(value), Terms: term(value), Store: true, Index: true}
	}
	presenceName := string(convert.Uint32ToBytes(7))
	docs := []native.Document{
		{Identifier: convert.Uint64ToBytes(1), Timestamp: 100, Fields: []native.Field{presentField("x"), seriesField(seriesOne)}},
		{Identifier: convert.Uint64ToBytes(2), Timestamp: 100, Fields: []native.Field{presentField("y"), seriesField(seriesOne)}},
		{Identifier: convert.Uint64ToBytes(3), Timestamp: 200, Fields: []native.Field{
			{Name: presenceName, Value: []byte("stored-only"), Store: true}, seriesField(seriesOne),
		}},
		{Identifier: convert.Uint64ToBytes(4), Timestamp: 300, Fields: []native.Field{presentField("x"), seriesField(seriesOne)}},
		{Identifier: convert.Uint64ToBytes(5), Timestamp: 400, Fields: []native.Field{
			{Name: presenceName, Value: []byte("zero-token"), Terms: []native.Term{}, Store: true, Index: true}, seriesField(seriesOne),
		}},
		{Identifier: convert.Uint64ToBytes(6), Timestamp: 600, Fields: []native.Field{
			{Name: presenceName, Value: []byte{}, Terms: term(""), Store: true, Index: true}, seriesField(seriesOne),
		}},
		{Identifier: convert.Uint64ToBytes(7), Timestamp: 700, Fields: []native.Field{presentField("x"), seriesField(seriesTwo)}},
	}
	require.NoError(t, owner.Batch(context.Background(), native.Batch{Documents: docs, InsertOnly: true}))
	require.NoError(t, owner.Batch(context.Background(), native.Batch{Deletes: [][]byte{convert.Uint64ToBytes(4)}}))
	waitPersist()

	adapter := &nativeadapter.Adapter{Owner: owner}
	oldSearcher, err := adapter.NewSearcher(context.Background(), nativeadapter.SearcherOptions{MaxTerms: 32, MaxCandidates: 32})
	require.NoError(t, err)
	rule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: 7, Name: "presence"},
		Tags:     []string{"presence"},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
		Analyzer: index.AnalyzerKeyword,
	}
	expr, err := logical.ParseExpr(&modelv1.Condition{Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "x"}}}})
	require.NoError(t, err)
	notFilter := newNot(rule, newEq(rule, expr))
	search := func(searcher index.Searcher, timeRange *index.RangeOpts) (posting.List, posting.List) {
		result, timestamps, executeErr := notFilter.Execute(func(databasev1.IndexRule_Type) (index.Searcher, error) {
			return searcher, nil
		}, common.SeriesID(1), timeRange)
		require.NoError(t, executeErr)
		return result, timestamps
	}
	result, timestamps := search(oldSearcher, nil)
	require.Equal(t, []uint64{2, 6}, result.ToSlice())
	require.Equal(t, []uint64{100, 600}, timestamps.ToSlice())
	timeRange := index.NewIntRangeOpts(50, 150, true, true)
	windowResult, windowTimestamps := search(oldSearcher, &timeRange)
	require.Equal(t, []uint64{2}, windowResult.ToSlice())
	require.Equal(t, []uint64{100}, windowTimestamps.ToSlice())

	require.NoError(t, owner.Batch(context.Background(), native.Batch{Deletes: [][]byte{convert.Uint64ToBytes(2)}}))
	waitPersist()
	oldResult, oldTimestamps := search(oldSearcher, nil)
	require.Equal(t, []uint64{2, 6}, oldResult.ToSlice())
	require.Equal(t, []uint64{100, 600}, oldTimestamps.ToSlice())
	require.NoError(t, oldSearcher.Close())
	freshSearcher, err := adapter.NewSearcher(context.Background(), nativeadapter.SearcherOptions{MaxTerms: 32, MaxCandidates: 32})
	require.NoError(t, err)
	freshResult, freshTimestamps := search(freshSearcher, nil)
	require.Equal(t, []uint64{6}, freshResult.ToSlice())
	require.Equal(t, []uint64{100, 600}, freshTimestamps.ToSlice())
	require.NoError(t, freshSearcher.Close())
	require.NoError(t, owner.Close())

	reopened, err := native.NewOwner(native.OwnerOptions{Lease: lease, Path: ownerPath, CompactionThreshold: -1})
	require.NoError(t, err)
	reopenedAdapter := &nativeadapter.Adapter{Owner: reopened}
	reopenedSearcher, err := reopenedAdapter.NewSearcher(context.Background(), nativeadapter.SearcherOptions{MaxTerms: 32, MaxCandidates: 32})
	require.NoError(t, err)
	reopenedResult, reopenedTimestamps := search(reopenedSearcher, nil)
	require.Equal(t, []uint64{6}, reopenedResult.ToSlice())
	require.Equal(t, []uint64{100, 600}, reopenedTimestamps.ToSlice())
	require.NoError(t, reopenedSearcher.Close())
	require.NoError(t, reopened.Close())

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	canceledSearcher, err := reopenedAdapter.NewSearcher(canceled, nativeadapter.SearcherOptions{MaxTerms: 32, MaxCandidates: 32})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, canceledSearcher)
}
