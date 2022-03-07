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
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

func TestMeasurePlanExecution_IndexScan(t *testing.T) {
	tester := require.New(t)
	measureSvc, metaService, deferFunc := setupMeasure(tester)
	defer deferFunc()
	baseTs := setupMeasureQueryData(t, "measure_query_data.json", measureSvc)

	metadata := &commonv1.Metadata{
		Name:  "cpm",
		Group: "default",
	}

	sT, eT := baseTs, baseTs.Add(1*time.Hour)

	analyzer, err := logical.CreateMeasureAnalyzerFromMetaService(metaService)
	tester.NoError(err)
	tester.NotNil(analyzer)

	tests := []struct {
		name           string
		unresolvedPlan logical.UnresolvedPlan
		wantLength     int
		tagLength      []int
		fieldLength    int
	}{
		{
			name: "Single Index Search using scope returns all results",
			unresolvedPlan: logical.MeasureIndexScan(sT, eT, metadata, []logical.Expr{
				logical.Eq(logical.NewTagRef("default", "scope"), logical.Str("minute")),
			}, tsdb.Entity{tsdb.AnyEntry}, nil, nil),
			wantLength: 3,
		},
		{
			name: "Single Index Search using scope returns all results",
			unresolvedPlan: logical.MeasureIndexScan(sT, eT, metadata, []logical.Expr{
				logical.Eq(logical.NewTagRef("default", "scope"), logical.Str("hour")),
			}, tsdb.Entity{tsdb.AnyEntry}, nil, nil),
			wantLength: 0,
		},
		{
			name: "Single Index Search using scope returns all results with tag projection",
			unresolvedPlan: logical.MeasureIndexScan(sT, eT, metadata, []logical.Expr{
				logical.Eq(logical.NewTagRef("default", "scope"), logical.Str("minute")),
			}, tsdb.Entity{tsdb.AnyEntry}, [][]*logical.Tag{{logical.NewTag("default", "scope")}},
				nil),
			wantLength: 3,
			tagLength:  []int{1},
		},
		{
			name: "Single Index Search using scope returns all results with field projection",
			unresolvedPlan: logical.MeasureIndexScan(sT, eT, metadata, []logical.Expr{
				logical.Eq(logical.NewTagRef("default", "scope"), logical.Str("minute")),
			}, tsdb.Entity{tsdb.AnyEntry}, nil,
				[]*logical.Field{logical.NewField("summation"), logical.NewField("count"), logical.NewField("value")}),
			wantLength:  3,
			fieldLength: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tester := require.New(t)
			schema, err := analyzer.BuildMeasureSchema(context.TODO(), metadata)
			tester.NoError(err)

			plan, err := tt.unresolvedPlan.Analyze(schema)
			tester.NoError(err)
			tester.NotNil(plan)

			dataPoints, err := plan.(executor.MeasureExecutable).Execute(measureSvc)
			tester.NoError(err)
			tester.Len(dataPoints, tt.wantLength)

			for _, dp := range dataPoints {
				tester.Len(dp.GetFields(), tt.fieldLength)
				tester.Len(dp.GetTagFamilies(), len(tt.tagLength))
				for tagFamilyIdx, tagFamily := range dp.GetTagFamilies() {
					tester.Len(tagFamily.GetTags(), tt.tagLength[tagFamilyIdx])
				}
			}
		})
	}
}
