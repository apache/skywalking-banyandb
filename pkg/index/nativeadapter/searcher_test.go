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

// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package nativeadapter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
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
