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

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestSeriesIndexPatternBeforeProjectionAndOrder(t *testing.T) {
	dir, cleanup := setUp(require.New(t))
	defer cleanup()
	si, err := newSeriesIndex(t.Context(), dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	defer func() { require.NoError(t, si.Close()) }()
	var docs index.Documents
	var series []*pbv1.Series
	for i, name := range []string{"Beta", "Alpine", "alpha", "zeta"} {
		doc, selector := seriesWithIndexedField(t, testSubjectSvc, i, 77, name)
		docs = append(docs, doc)
		series = append(series, selector)
	}
	require.NoError(t, si.Insert(docs))
	field := index.FieldKey{IndexRuleID: 77}

	opts := IndexSearchOpts{
		PatternRequests: []native.TermSetRequest{{
			Field:            field.Marshal(),
			WildcardPatterns: []native.WildcardPattern{{Pattern: "al%", Escape: "\\", IgnoreCase: true, Syntax: native.WildcardSyntaxSQL}},
			PatternBudget:    native.NewWildcardBudget(native.WildcardLimits{}),
		}},
		Order: &index.OrderBy{Type: index.OrderByTypeIndex, Index: &databasev1.IndexRule{Metadata: &commonv1.Metadata{Id: 77}}},
	}

	result, sortValues, err := si.Search(t.Context(), series, opts)
	require.NoError(t, err)
	require.Len(t, result.SeriesList, 2)
	require.Equal(t, [][]byte{[]byte("Alpine"), []byte("alpha")}, sortValues)
	opts.PatternRequests[0].WildcardPatterns[0].Pattern = "missing%"
	result, _, err = si.Search(t.Context(), series, opts)
	require.NoError(t, err)
	require.Empty(t, result.SeriesList)
	opts.PatternRequests[0].WildcardPatterns[0].Pattern = "%a%"
	opts.PatternRequests[0].PatternBudget = native.NewWildcardBudget(native.WildcardLimits{MaxWork: 1})
	_, _, err = si.Search(t.Context(), series, opts)
	require.ErrorIs(t, err, native.ErrQueryLimit)
}
