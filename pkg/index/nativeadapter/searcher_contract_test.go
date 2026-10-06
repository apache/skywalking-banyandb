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
package nativeadapter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
)

func TestSearcherMatchDuplicatePhysicalDocuments(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer owner.Close()
	a := &Adapter{Owner: owner}
	key := index.FieldKey{IndexRuleID: 8, Analyzer: index.AnalyzerSimple}
	alpha := index.NewStringField(key, "alpha")
	beta := index.NewStringField(key, "beta")
	require.NoError(t, a.Batch(context.Background(), index.Batch{Documents: []index.Document{{DocID: 7, Fields: []index.Field{alpha}}, {DocID: 7, Fields: []index.Field{beta}}, {DocID: 9, Fields: []index.Field{alpha, beta}}}})) //nolint:lll
	searcher, err := a.NewSearcher(context.Background(), SearcherOptions{MaxTerms: 32, MaxCandidates: 32})
	require.NoError(t, err)
	defer searcher.Close()
	hits, _, err := searcher.Match(key, []string{"alpha", "beta"}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, hits.Len())
}

func TestSearcherPinnedSnapshotAndCancellation(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer owner.Close()
	adapter := &Adapter{Owner: owner}
	key := index.FieldKey{IndexRuleID: 9, Analyzer: index.AnalyzerSimple}
	first := index.NewStringField(key, "alpha")
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: []index.Document{{DocID: 1, Fields: []index.Field{first}}}}))
	old, err := adapter.NewSearcher(context.Background(), SearcherOptions{MaxTerms: 32, MaxCandidates: 32})
	require.NoError(t, err)
	defer old.Close()
	second := index.NewStringField(key, "beta")
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: []index.Document{{DocID: 2, Fields: []index.Field{second}}}}))
	hits, _, err := old.Match(key, []string{"beta"}, nil)
	require.NoError(t, err)
	require.Equal(t, 0, hits.Len())
	fresh, err := adapter.NewSearcher(context.Background(), SearcherOptions{MaxTerms: 32, MaxCandidates: 32})
	require.NoError(t, err)
	defer fresh.Close()
	hits, _, err = fresh.Match(key, []string{"beta"}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, hits.Len())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	canceled, err := adapter.NewSearcher(ctx, SearcherOptions{MaxTerms: 32, MaxCandidates: 32})
	require.Error(t, err)
	require.Nil(t, canceled)
}
