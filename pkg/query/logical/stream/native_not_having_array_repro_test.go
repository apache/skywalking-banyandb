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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/index/nativeadapter"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

func TestReproNotHavingOnArrayTag(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: streamLease{}})
	require.NoError(t, err)
	defer owner.Close()
	adapter := &nativeadapter.Adapter{Owner: owner}
	rule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: 9}, Tags: []string{"extended_tags"},
		Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: index.AnalyzerKeyword,
	}
	fk := index.FieldKey{IndexRuleID: 9, Analyzer: index.AnalyzerKeyword, SeriesID: 21}
	otherFK := index.FieldKey{IndexRuleID: 10, Analyzer: index.AnalyzerKeyword, SeriesID: 21}
	// doc 100: extended_tags = ["a", "b"] (does NOT contain "c")
	// doc 101: extended_tags = ["c"]      (DOES contain "c")
	// doc 102: extended_tags absent entirely (only an unrelated field is set)
	docs := []index.Document{
		{DocID: 100, Timestamp: 100, Fields: []index.Field{
			index.NewStringField(fk, "a"),
			index.NewStringField(fk, "b"),
		}},
		{DocID: 101, Timestamp: 200, Fields: []index.Field{
			index.NewStringField(fk, "c"),
		}},
		{DocID: 102, Timestamp: 300, Fields: []index.Field{
			index.NewStringField(otherFK, "unrelated"),
		}},
	}
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: docs}))

	searcher, err := adapter.NewSearcher(context.Background(), nativeadapter.SearcherOptions{MaxTerms: nativeadapter.DefaultMaxTerms, MaxCandidates: 0})
	require.NoError(t, err)
	defer searcher.Close()
	timeRange := index.NewIntRangeOpts(0, 1000, true, true)

	cExpr, err := logical.ParseExpr(&modelv1.Condition{Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "c"}}}})
	require.NoError(t, err)

	t.Run("MatchField is a scope scan, not a presence filter", func(t *testing.T) {
		list, _, err := searcher.MatchField(fk)
		require.NoError(t, err)
		t.Logf("MatchField result: len=%d contains100=%v contains101=%v contains102=%v",
			list.Len(), list.Contains(100), list.Contains(101), list.Contains(102))
		require.True(t, list.Contains(100))
		require.True(t, list.Contains(101))
		require.True(t, list.Contains(102), "doc 102 (extended_tags absent) must still be in scope, matching the legacy engine's Range-with-empty-options contract")
		require.Equal(t, 3, list.Len())
	})

	t.Run("eq(c) matches only doc 101", func(t *testing.T) {
		eqFilter := newEq(rule, cExpr)
		list, _, err := eqFilter.Execute(func(databasev1.IndexRule_Type) (index.Searcher, error) { return searcher, nil }, 21, &timeRange)
		require.NoError(t, err)
		t.Logf("eq(c) result: len=%d contains100=%v contains101=%v", list.Len(), list.Contains(100), list.Contains(101))
		require.False(t, list.Contains(100), "doc 100 (tags a,b) should NOT match eq(c)")
		require.True(t, list.Contains(101), "doc 101 (tag c) should match eq(c)")
		require.Equal(t, 1, list.Len())
	})

	t.Run("NOT HAVING (c) keeps doc 100 and doc 102 (field absent)", func(t *testing.T) {
		and := newAnd(1)
		and.append(newEq(rule, cExpr))
		notFilter := newNot(rule, and)
		list, _, err := notFilter.Execute(func(databasev1.IndexRule_Type) (index.Searcher, error) { return searcher, nil }, 21, &timeRange)
		require.NoError(t, err)
		t.Logf("NOT HAVING(c) result: len=%d contains100=%v contains101=%v contains102=%v",
			list.Len(), list.Contains(100), list.Contains(101), list.Contains(102))
		require.True(t, list.Contains(100), "doc 100 (tags a,b) should remain after NOT HAVING(c)")
		require.False(t, list.Contains(101), "doc 101 (tag c) should be excluded by NOT HAVING(c)")
		require.True(t, list.Contains(102), "doc 102 (extended_tags entirely absent) should match NOT HAVING(c) per the generator oracle's documented semantics")
		require.Equal(t, 2, list.Len())
	})
}
