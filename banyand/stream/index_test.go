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
	"testing"

	"github.com/stretchr/testify/require"

	common "github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	postingroaring "github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
)

func TestElementIndexSearch_EmptyFirstSeriesDoesNotMutateDummyPostingList(t *testing.T) {
	root := t.TempDir()
	ei, err := newElementIndex(context.Background(), root, 0, nil, nil, newTestRootLease(t, root))
	require.NoError(t, err)
	defer func() { require.NoError(t, ei.Close()) }()

	// Index a document only on series 2 with term "target".
	key := index.FieldKey{IndexRuleID: 1, SeriesID: 2}
	field := index.NewStringField(key, "target")
	require.NoError(t, ei.Write(index.Documents{{DocID: 42, Timestamp: 100, Fields: []index.Field{field}}}))

	require.True(t, postingroaring.DummyPostingList.IsEmpty(), "DummyPostingList must start empty")

	// Filter queries for term "target" scoped to each series ID passed to Execute.
	filter := &testTermFilter{
		key:  index.FieldKey{IndexRuleID: 1},
		term: "target",
	}

	// Series 1 matches nothing; Series 2 matches doc 42.
	res, resTS, err := ei.Search(context.Background(), []uint64{1, 2}, filter, nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, []uint64{42}, res.ToSlice())
	require.NotNil(t, resTS)

	// Regression assertion: DummyPostingList was never written to.
	require.True(t, postingroaring.DummyPostingList.IsEmpty(), "DummyPostingList must remain empty")
	require.Equal(t, 0, postingroaring.DummyPostingList.Len(), "DummyPostingList was mutated by elementIndex.Search")
}

type testTermFilter struct {
	key  index.FieldKey
	term string
}

func (f *testTermFilter) Execute(getSearcher index.GetSearcher, seriesID common.SeriesID, _ *index.RangeOpts) (posting.List, posting.List, error) {
	s, err := getSearcher(databasev1.IndexRule_TYPE_INVERTED)
	if err != nil {
		return nil, nil, err
	}
	k := f.key
	k.SeriesID = seriesID
	return s.MatchTerms(index.NewStringField(k, f.term))
}

func (f *testTermFilter) String() string { return "testTermFilter" }

func (f *testTermFilter) ShouldSkip(index.FilterOp) (bool, error) { return false, nil }
