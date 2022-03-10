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

package logical_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

func TestPlanExecution_TableScan_Limit(t *testing.T) {
	tester := require.New(t)
	streamSvc, metaService, deferFunc := setup(tester)
	defer deferFunc()
	baseTs := setupQueryData(t, "multiple_shards.json", streamSvc)

	metadata := &commonv1.Metadata{
		Name:  "sw",
		Group: "default",
	}

	sT, eT := baseTs, baseTs.Add(1*time.Hour)

	analyzer, err := logical.CreateStreamAnalyzerFromMetaService(metaService)
	tester.NoError(err)
	tester.NotNil(analyzer)

	tests := []struct {
		name           string
		unresolvedPlan logical.UnresolvedPlan
		wantLength     int
	}{
		{
			name:           "Limit 1",
			unresolvedPlan: logical.Limit(logical.IndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry}, nil), 1),
			wantLength:     1,
		},
		{
			name:           "Limit 5",
			unresolvedPlan: logical.Limit(logical.IndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry}, nil), 5),
			wantLength:     5,
		},
		{
			name:           "Limit 10",
			unresolvedPlan: logical.Limit(logical.IndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry}, nil), 10),
			wantLength:     5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tester := require.New(t)
			schema, err := analyzer.BuildStreamSchema(context.TODO(), metadata)
			tester.NoError(err)

			plan, err := tt.unresolvedPlan.Analyze(schema)
			tester.NoError(err)
			tester.NotNil(plan)

			entities, err := plan.(executor.StreamExecutable).Execute(streamSvc)
			tester.NoError(err)
			tester.Len(entities, tt.wantLength)
			tester.True(logical.SortedByTimestamp(entities, modelv1.Sort_SORT_ASC))
		})
	}
}

func TestPlanExecution_Offset(t *testing.T) {
	tester := require.New(t)
	streamSvc, metaService, deferFunc := setup(tester)
	defer deferFunc()
	baseTs := setupQueryData(t, "multiple_shards.json", streamSvc)

	metadata := &commonv1.Metadata{
		Name:  "sw",
		Group: "default",
	}

	sT, eT := baseTs, baseTs.Add(1*time.Hour)

	analyzer, err := logical.CreateStreamAnalyzerFromMetaService(metaService)
	tester.NoError(err)
	tester.NotNil(analyzer)

	tests := []struct {
		name           string
		unresolvedPlan logical.UnresolvedPlan
		wantLength     int
	}{
		{
			name:           "Offset 0",
			unresolvedPlan: logical.Offset(logical.IndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry}, nil), 0),
			wantLength:     5,
		},
		{
			name:           "Offset 3",
			unresolvedPlan: logical.Offset(logical.IndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry}, nil), 3),
			wantLength:     2,
		},
		{
			name:           "Limit 5",
			unresolvedPlan: logical.Offset(logical.IndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry}, nil), 5),
			wantLength:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tester := require.New(t)
			schema, err := analyzer.BuildStreamSchema(context.TODO(), metadata)
			tester.NoError(err)

			plan, err := tt.unresolvedPlan.Analyze(schema)
			tester.NoError(err)
			tester.NotNil(plan)

			entities, err := plan.(executor.StreamExecutable).Execute(streamSvc)
			tester.NoError(err)
			tester.Len(entities, tt.wantLength)
		})
	}
}

func TestPlanExecution_TraceIDFetch(t *testing.T) {
	tester := require.New(t)
	streamSvc, metaService, deferFunc := setup(tester)
	defer deferFunc()
	_ = setupQueryData(t, "multiple_shards.json", streamSvc)

	m := &commonv1.Metadata{
		Name:  "sw",
		Group: "default",
	}

	analyzer, err := logical.CreateStreamAnalyzerFromMetaService(metaService)
	tester.NoError(err)
	tester.NotNil(analyzer)

	tests := []struct {
		name       string
		traceID    string
		wantLength int
	}{
		{
			name:       "traceID = 1",
			traceID:    "1",
			wantLength: 1,
		},
		{
			name:       "traceID = 2",
			traceID:    "2",
			wantLength: 1,
		},
		{
			name:       "traceID = 3",
			traceID:    "3",
			wantLength: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tester := require.New(t)
			s, err := analyzer.BuildStreamSchema(context.TODO(), m)
			tester.NoError(err)

			p, err := logical.GlobalIndexScan(m, []logical.Expr{
				logical.Eq(logical.NewTagRef("searchable", "trace_id"), logical.Str(tt.traceID)),
			}, logical.NewTags("searchable", "trace_id")).Analyze(s)
			tester.NoError(err)
			tester.NotNil(p)
			entities, err := p.(executor.StreamExecutable).Execute(streamSvc)
			tester.NoError(err)
			for _, entity := range entities {
				tester.Len(entity.GetTagFamilies(), 1)
				tester.Len(entity.GetTagFamilies()[0].GetTags(), 1)
				tester.Equal(entity.GetTagFamilies()[0].GetTags()[0].GetValue().GetStr().GetValue(), tt.traceID)
			}
			tester.Len(entities, tt.wantLength)
		})
	}
}

func TestPlanExecution_IndexScan(t *testing.T) {
	tester := require.New(t)
	streamSvc, metaService, deferFunc := setup(tester)
	defer deferFunc()
	baseTs := setupQueryData(t, "multiple_shards.json", streamSvc)

	metadata := &commonv1.Metadata{
		Name:  "sw",
		Group: "default",
	}

	sT, eT := baseTs, baseTs.Add(1*time.Hour)

	analyzer, err := logical.CreateStreamAnalyzerFromMetaService(metaService)
	tester.NoError(err)
	tester.NotNil(analyzer)

	tests := []struct {
		name           string
		unresolvedPlan logical.UnresolvedPlan
		wantLength     int
	}{
		{
			name: "Single Index Search using POST without entity returns nothing",
			unresolvedPlan: logical.IndexScan(sT, eT, metadata, []logical.Expr{
				logical.Eq(logical.NewTagRef("searchable", "http.method"), logical.Str("POST")),
			}, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry}, nil),
			wantLength: 0,
		},
		{
			name: "Single Index Search using inverted index",
			unresolvedPlan: logical.IndexScan(sT, eT, metadata, []logical.Expr{
				logical.Eq(logical.NewTagRef("searchable", "http.method"), logical.Str("GET")),
			}, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry}, nil),
			wantLength: 3,
		},
		{
			name: "Single Index Search using LSM tree index",
			unresolvedPlan: logical.IndexScan(sT, eT, metadata, []logical.Expr{
				logical.Lt(logical.NewTagRef("searchable", "duration"), logical.Int(100)),
			}, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry}, nil),
			wantLength: 2,
		},
		{
			name: "Single Index Search without entity returns results",
			unresolvedPlan: logical.IndexScan(sT, eT, metadata, []logical.Expr{
				logical.Eq(logical.NewTagRef("searchable", "endpoint_id"), logical.Str("/home_id")),
			}, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry}, nil),
			wantLength: 2,
		},
		{
			name: "Multiple Index Search",
			unresolvedPlan: logical.IndexScan(sT, eT, metadata, []logical.Expr{
				logical.Eq(logical.NewTagRef("searchable", "http.method"), logical.Str("GET")),
				logical.Eq(logical.NewTagRef("searchable", "endpoint_id"), logical.Str("/home_id")),
			}, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry}, nil),
			wantLength: 1,
		},
		{
			name: "Multiple Index Search with a combination of numerical index and textual index",
			unresolvedPlan: logical.IndexScan(sT, eT, metadata, []logical.Expr{
				logical.Eq(logical.NewTagRef("searchable", "http.method"), logical.Str("GET")),
				logical.Lt(logical.NewTagRef("searchable", "duration"), logical.Int(100)),
			}, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry}, nil),
			wantLength: 2,
		},
		{
			name: "Multiple Index With One Empty Result(ChunkID)",
			unresolvedPlan: logical.IndexScan(sT, eT, metadata, []logical.Expr{
				logical.Eq(logical.NewTagRef("searchable", "http.method"), logical.Str("GET")),
				logical.Eq(logical.NewTagRef("searchable", "endpoint_id"), logical.Str("/unknown")),
			}, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry}, nil),
			wantLength: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tester := require.New(t)
			schema, err := analyzer.BuildStreamSchema(context.TODO(), metadata)
			tester.NoError(err)

			plan, err := tt.unresolvedPlan.Analyze(schema)
			tester.NoError(err)
			tester.NotNil(plan)

			entities, err := plan.(executor.StreamExecutable).Execute(streamSvc)
			tester.NoError(err)
			tester.Len(entities, tt.wantLength)
		})
	}
}

func TestPlanExecution_OrderBy(t *testing.T) {
	tester := require.New(t)
	streamSvc, metaService, deferFunc := setup(tester)
	defer deferFunc()
	baseTs := setupQueryData(t, "multiple_shards.json", streamSvc)

	metadata := &commonv1.Metadata{
		Name:  "sw",
		Group: "default",
	}

	sT, eT := baseTs, baseTs.Add(1*time.Hour)

	analyzer, err := logical.CreateStreamAnalyzerFromMetaService(metaService)
	tester.NoError(err)
	tester.NotNil(analyzer)

	tests := []struct {
		name            string
		targetIndexRule string
		sortDirection   modelv1.Sort
		// TODO: avoid hardcoded index?
		targetFamilyIdx int
		targetTagIdx    int
	}{
		{
			name:            "Sort By duration ASC",
			targetIndexRule: "duration",
			sortDirection:   modelv1.Sort_SORT_ASC,
			targetFamilyIdx: 0,
			targetTagIdx:    0,
		},
		{
			name:            "Sort By duration DESC",
			targetIndexRule: "duration",
			sortDirection:   modelv1.Sort_SORT_DESC,
			targetFamilyIdx: 0,
			targetTagIdx:    0,
		},
		{
			name:            "Sort By start_time DESC",
			targetIndexRule: "",
			sortDirection:   modelv1.Sort_SORT_DESC,
		},
		{
			name:            "Sort By start_time ASC",
			targetIndexRule: "",
			sortDirection:   modelv1.Sort_SORT_ASC,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tester := require.New(t)
			schema, err := analyzer.BuildStreamSchema(context.TODO(), metadata)
			tester.NoError(err)
			tester.NotNil(schema)

			if tt.targetIndexRule == "" {
				p, err := logical.IndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
					logical.OrderBy("", tt.sortDirection), logical.NewTags("searchable", "start_time")).
					Analyze(schema)
				tester.NoError(err)
				tester.NotNil(p)

				entities, err := p.(executor.StreamExecutable).Execute(streamSvc)
				tester.NoError(err)
				tester.NotNil(entities)

				tester.True(logical.SortedByTimestamp(entities, tt.sortDirection))
			} else {
				p, err := logical.IndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
					logical.OrderBy(tt.targetIndexRule, tt.sortDirection), logical.NewTags("searchable", tt.targetIndexRule)).
					Analyze(schema)
				tester.NoError(err)
				tester.NotNil(p)

				entities, err := p.(executor.StreamExecutable).Execute(streamSvc)
				tester.NoError(err)
				tester.NotNil(entities)

				tester.True(logical.SortedByIndex(entities, tt.targetFamilyIdx, tt.targetTagIdx, tt.sortDirection))
			}
		})
	}
}
