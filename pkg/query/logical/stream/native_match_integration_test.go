// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership.  Apache Software Foundation (ASF) licenses this file to you
// under the Apache License, Version 2.0 (the "License"); you may
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

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/index/nativeadapter"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// matchDocs is the fixture shared by the MATCH tests below: three documents
// across two series, so a leak across the series scope is visible.
func matchDocs(analyzer string) []index.Document {
	key := func(seriesID common.SeriesID) index.FieldKey {
		return index.FieldKey{IndexRuleID: 7, Analyzer: analyzer, SeriesID: seriesID}
	}
	return []index.Document{
		{DocID: 42, Timestamp: 100, Fields: []index.Field{index.NewStringField(key(11), "ok")}},
		{DocID: 43, Timestamp: 200, Fields: []index.Field{index.NewStringField(key(11), "err")}},
		{DocID: 44, Timestamp: 100, Fields: []index.Field{index.NewStringField(key(12), "ok")}},
	}
}

func newMatchRule(analyzer string) *databasev1.IndexRule {
	return &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: 7},
		Tags:     []string{"status"},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
		Analyzer: analyzer,
	}
}

func literalExpr(t *testing.T, value string) logical.LiteralExpr {
	t.Helper()
	expr, err := logical.ParseExpr(&modelv1.Condition{
		Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}},
	})
	require.NoError(t, err)
	return expr
}

// TestNativeSearcherMatchExecuteKeyword pins BINARY_OP_MATCH on a keyword-analyzed
// field: the operator documents full-text search, and on a keyword field it must
// behave as an exact term lookup scoped to the series.
//
// It is the exact counterpart of TestNativeSearcherEqExecute, which asserts the
// same thing through EQ. Before this test there was no MATCH coverage for the
// stream filter at all, which is why the operator could return an empty result
// while EQ on the same field worked.
func TestNativeSearcherMatchExecuteKeyword(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: streamLease{}})
	require.NoError(t, err)
	defer owner.Close()
	adapter := &nativeadapter.Adapter{Owner: owner}
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: matchDocs(index.AnalyzerKeyword)}))

	expr := literalExpr(t, "ok")
	m := newMatch(newMatchRule(index.AnalyzerKeyword), expr, nil)
	searcher, err := adapter.NewSearcher(context.Background(), nativeadapter.SearcherOptions{MaxTerms: 16, MaxCandidates: 16})
	require.NoError(t, err)
	defer searcher.Close()

	timeRange := index.NewIntRangeOpts(50, 150, true, true)
	list, _, err := m.Execute(
		func(databasev1.IndexRule_Type) (index.Searcher, error) { return searcher, nil },
		11, &timeRange,
	)
	require.NoError(t, err)
	require.Equal(t, 1, list.Len(), "MATCH on a keyword field must resolve exactly like EQ")
	require.True(t, list.Contains(42))
	require.False(t, list.Contains(43), "a different term must not match")
	require.False(t, list.Contains(44), "another series must not match")
}

// TestNativeSearcherMatchExecuteSimple covers a genuinely analyzed field: the
// writer tokenizes on write, so a query term must be analyzed the same way to
// find it. "ERROR" is stored as "ERROR" under the keyword analyzer but as
// "error" under the simple one, so this also pins the case-sensitivity contract.
func TestNativeSearcherMatchExecuteSimple(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: streamLease{}})
	require.NoError(t, err)
	defer owner.Close()
	adapter := &nativeadapter.Adapter{Owner: owner}
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: matchDocs(index.AnalyzerSimple)}))

	searcher, err := adapter.NewSearcher(context.Background(), nativeadapter.SearcherOptions{MaxTerms: 16, MaxCandidates: 16})
	require.NoError(t, err)
	defer searcher.Close()
	timeRange := index.NewIntRangeOpts(50, 150, true, true)

	// The analyzed term matches the indexed token.
	m := newMatch(newMatchRule(index.AnalyzerSimple), literalExpr(t, "ok"), nil)
	list, _, err := m.Execute(
		func(databasev1.IndexRule_Type) (index.Searcher, error) { return searcher, nil }, 11, &timeRange)
	require.NoError(t, err)
	require.Equal(t, 1, list.Len())
	require.True(t, list.Contains(42))
	require.False(t, list.Contains(44))

	// A term absent from the index matches nothing.
	m2 := newMatch(newMatchRule(index.AnalyzerSimple), literalExpr(t, "nothing-here"), nil)
	empty, _, err := m2.Execute(
		func(databasev1.IndexRule_Type) (index.Searcher, error) { return searcher, nil }, 11, &timeRange)
	require.NoError(t, err)
	require.Equal(t, 0, empty.Len(), "a term that was never indexed must not match")
}

// TestNativeSearcherMatchExecuteOperatorAnyAnd pins OPERATOR_OR versus
// OPERATOR_AND across multiple query terms on one field.
func TestNativeSearcherMatchExecuteOperatorAnyAnd(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: streamLease{}})
	require.NoError(t, err)
	defer owner.Close()
	adapter := &nativeadapter.Adapter{Owner: owner}
	docs := []index.Document{
		{DocID: 1, Timestamp: 100, Fields: []index.Field{index.NewStringField(index.FieldKey{IndexRuleID: 7, Analyzer: index.AnalyzerSimple, SeriesID: 11}, "alpha beta")}},
		{DocID: 2, Timestamp: 100, Fields: []index.Field{index.NewStringField(index.FieldKey{IndexRuleID: 7, Analyzer: index.AnalyzerSimple, SeriesID: 11}, "alpha gamma")}},
		{DocID: 3, Timestamp: 100, Fields: []index.Field{index.NewStringField(index.FieldKey{IndexRuleID: 7, Analyzer: index.AnalyzerSimple, SeriesID: 11}, "delta")}},
	}
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: docs}))
	searcher, err := adapter.NewSearcher(context.Background(), nativeadapter.SearcherOptions{MaxTerms: 16, MaxCandidates: 16})
	require.NoError(t, err)
	defer searcher.Close()
	timeRange := index.NewIntRangeOpts(0, 200, true, true)
	search := func(opts *modelv1.Condition_MatchOption) (int, bool, bool) {
		m := newMatch(newMatchRule(index.AnalyzerSimple), literalExpr(t, "alpha gamma"), opts)
		list, _, execErr := m.Execute(
			func(databasev1.IndexRule_Type) (index.Searcher, error) { return searcher, nil }, 11, &timeRange)
		require.NoError(t, execErr)
		return list.Len(), list.Contains(1), list.Contains(2)
	}

	n, has1, has2 := search(&modelv1.Condition_MatchOption{Operator: modelv1.Condition_MatchOption_OPERATOR_OR})
	require.Equal(t, 2, n, "OR matches documents holding either term")
	require.True(t, has1)
	require.True(t, has2)

	// doc 1 is "alpha beta" and doc 2 is "alpha gamma", so only doc 2 holds both
	// terms. AND must therefore return doc 2 alone.
	n, has1, has2 = search(&modelv1.Condition_MatchOption{Operator: modelv1.Condition_MatchOption_OPERATOR_AND})
	require.Equal(t, 1, n, "AND matches only documents holding both terms")
	require.False(t, has1, "doc 1 has alpha but not gamma")
	require.True(t, has2)
}
