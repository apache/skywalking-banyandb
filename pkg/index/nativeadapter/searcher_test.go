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

package nativeadapter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	postingroaring "github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
)

func TestSearcherMatchPreservesPerInputMatchGroups(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	adapter := &Adapter{Owner: owner}
	key := index.FieldKey{IndexRuleID: 17, SeriesID: 3, Analyzer: index.AnalyzerSimple}
	doc0 := index.NewStringField(key, "alpha beta gamma")
	doc1 := index.NewStringField(key, "alpha beta")
	doc2 := index.NewStringField(key, "alpha gamma")
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: []index.Document{
		{DocID: 1, Fields: []index.Field{doc0}},
		{DocID: 2, Fields: []index.Field{doc1}},
		{DocID: 3, Fields: []index.Field{doc2}},
	}}))
	searcher, err := adapter.NewSearcher(context.Background(), SearcherOptions{MaxTerms: 32})
	require.NoError(t, err)
	defer func() { require.NoError(t, searcher.Close()) }()
	list, _, err := searcher.Match(key, []string{"alpha beta", "alpha gamma"}, &modelv1.Condition_MatchOption{Operator: modelv1.Condition_MatchOption_OPERATOR_AND})
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, list.ToSlice())
}

func TestSearcherSortWithoutSeriesSelection(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	adapter := &Adapter{Owner: owner}
	key := index.FieldKey{IndexRuleID: 18, SeriesID: 1}
	otherKey := key
	otherKey.SeriesID = 2
	doc0 := index.NewStringField(key, "b")
	doc1 := index.NewStringField(otherKey, "a")
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: []index.Document{
		{DocID: 1, Fields: []index.Field{doc0}},
		{DocID: 2, Fields: []index.Field{doc1}},
	}}))
	searcher, err := adapter.NewSearcher(context.Background(), SearcherOptions{MaxTerms: 32})
	require.NoError(t, err)
	defer func() { require.NoError(t, searcher.Close()) }()
	iterator, err := searcher.Sort(context.Background(), nil, key, modelv1.Sort_SORT_ASC, nil, 2)
	require.NoError(t, err)
	defer func() { require.NoError(t, iterator.Close()) }()
	var ids []uint64
	for iterator.Next() {
		ids = append(ids, iterator.Val().DocID)
	}
	require.Equal(t, []uint64{2, 1}, ids)
}

func TestSearcherRejectsMalformedIdentifierWithoutPanic(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	require.NoError(t, owner.Batch(context.Background(), native.Batch{Documents: []native.Document{{
		Identifier: []byte("bad"), Fields: []native.Field{{Name: "rank", Value: []byte("a"), Sort: true}},
	}}}))
	adapter := &Adapter{Owner: owner}
	searcher, err := adapter.NewSearcher(context.Background(), SearcherOptions{MaxTerms: 8})
	require.NoError(t, err)
	defer func() { require.NoError(t, searcher.Close()) }()
	iterator, err := searcher.Sort(context.Background(), nil, index.FieldKey{TagName: "rank"}, modelv1.Sort_SORT_ASC, nil, 1)
	require.NoError(t, err)
	require.False(t, iterator.Next())
	require.ErrorIs(t, iterator.Close(), native.ErrCorrupt)
}

func TestSearcherEmptyQueryLeavesSharedPostingListEmpty(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()

	adapter := &Adapter{Owner: owner}
	key := index.FieldKey{IndexRuleID: 20, SeriesID: 1, Analyzer: index.AnalyzerSimple}
	doc := index.NewStringField(key, "alpha beta")
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: []index.Document{
		{DocID: 1, Fields: []index.Field{doc}},
	}}))

	searcher, err := adapter.NewSearcher(context.Background(), SearcherOptions{MaxTerms: 32, MaxCandidates: 32})
	require.NoError(t, err)
	defer func() { require.NoError(t, searcher.Close()) }()

	require.True(t, postingroaring.DummyPostingList.IsEmpty(), "DummyPostingList must start empty")

	t.Run("Match nonexistent term", func(t *testing.T) {
		list, timestamps, err := searcher.Match(key, []string{"nonexistent"}, nil)
		assertEmptyQueryAndSharedIntact(t, list, timestamps, err)
	})

	t.Run("MatchTerms nonexistent term", func(t *testing.T) {
		field := index.NewStringField(key, "nonexistent")
		list, timestamps, err := searcher.MatchTerms(field)
		assertEmptyQueryAndSharedIntact(t, list, timestamps, err)
	})

	t.Run("Range non-matching bounds", func(t *testing.T) {
		rangeOpts := index.NewStringRangeOpts("x_start", "x_end", true, true)
		list, timestamps, err := searcher.Range(key, rangeOpts)
		assertEmptyQueryAndSharedIntact(t, list, timestamps, err)
	})
}

func assertEmptyQueryAndSharedIntact(t *testing.T, list, timestamps posting.List, err error) {
	t.Helper()
	require.NoError(t, err)
	require.True(t, list.IsEmpty(), "result must be empty")
	require.True(t, timestamps.IsEmpty(), "timestamps must be empty")
	require.True(t, postingroaring.DummyPostingList.IsEmpty(), "DummyPostingList must remain empty")
}
