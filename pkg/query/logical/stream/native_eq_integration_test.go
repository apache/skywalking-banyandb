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

type streamLease struct{}

func (streamLease) Validate() error { return nil }
func TestNativeSearcherEqExecute(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: streamLease{}})
	require.NoError(t, err)
	defer owner.Close()
	adapter := &nativeadapter.Adapter{Owner: owner}
	rule := &databasev1.IndexRule{Metadata: &commonv1.Metadata{Id: 7}, Tags: []string{"status"}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: index.AnalyzerKeyword}
	docs := []index.Document{{DocID: 42, Timestamp: 100, Fields: []index.Field{index.NewStringField(index.FieldKey{IndexRuleID: 7, Analyzer: index.AnalyzerKeyword, SeriesID: 11}, "ok")}}, {DocID: 43, Timestamp: 200, Fields: []index.Field{index.NewStringField(index.FieldKey{IndexRuleID: 7, Analyzer: index.AnalyzerKeyword, SeriesID: 11}, "ok")}}, {DocID: 44, Timestamp: 100, Fields: []index.Field{index.NewStringField(index.FieldKey{IndexRuleID: 7, Analyzer: index.AnalyzerKeyword, SeriesID: 12}, "ok")}}} //nolint:lll
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: docs}))
	expr, err := logical.ParseExpr(&modelv1.Condition{Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "ok"}}}})
	require.NoError(t, err)
	eq := newEq(rule, expr)
	searcher, err := adapter.NewSearcher(context.Background(), nativeadapter.SearcherOptions{MaxTerms: 16, MaxCandidates: 16})
	require.NoError(t, err)
	defer searcher.Close()
	timeRange := index.NewIntRangeOpts(50, 150, true, true)
	list, _, err := eq.Execute(func(databasev1.IndexRule_Type) (index.Searcher, error) { return searcher, nil }, 11, &timeRange)
	require.NoError(t, err)
	require.Equal(t, 1, list.Len())
	require.True(t, list.Contains(42))
	require.False(t, list.Contains(43))
	require.False(t, list.Contains(44))
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: []index.Document{{DocID: 45, Timestamp: 100, Fields: []index.Field{index.NewStringField(index.FieldKey{IndexRuleID: 7, Analyzer: index.AnalyzerKeyword, SeriesID: 11}, "ok")}}}})) //nolint:lll
	oldList, _, oldErr := eq.Execute(func(databasev1.IndexRule_Type) (index.Searcher, error) { return searcher, nil }, 11, &timeRange)
	require.NoError(t, oldErr)
	require.Equal(t, 1, oldList.Len())
	require.True(t, oldList.Contains(42))
	fresh, freshErr := adapter.NewSearcher(context.Background(), nativeadapter.SearcherOptions{MaxTerms: 16, MaxCandidates: 16})
	require.NoError(t, freshErr)
	defer fresh.Close()
	freshList, _, freshErr := eq.Execute(func(databasev1.IndexRule_Type) (index.Searcher, error) { return fresh, nil }, 11, &timeRange)
	require.NoError(t, freshErr)
	require.Equal(t, 2, freshList.Len())
	require.True(t, freshList.Contains(45))
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	canceled, canceledErr := adapter.NewSearcher(cancelCtx, nativeadapter.SearcherOptions{MaxTerms: 16, MaxCandidates: 16})
	require.Error(t, canceledErr)
	require.Nil(t, canceled)
}
