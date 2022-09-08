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

package measure_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

func TestMeasurePlanExecution_IndexScan(t *testing.T) {
	tester := require.New(t)
	measureSvc, metaService, deferFunc := setupMeasure(tester, "service_cpm_minute")
	defer deferFunc()
	baseTs := setupMeasureQueryData(t, "measure_query_data.json", measureSvc)

	metadata := &commonv1.Metadata{
		Name:  "service_cpm_minute",
		Group: "sw_metric",
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
			name: "Single Index Search using id returns a result",
			unresolvedPlan: logical.MeasureIndexScan(sT, eT, metadata, []logical.Expr{
				logical.Eq(logical.NewTagRef("default", "id"), logical.ID("1")),
			}, tsdb.Entity{tsdb.AnyEntry}, nil, nil, false, nil),
			wantLength: 1,
		},
		{
			name: "Single Index Search using id returns nothing",
			unresolvedPlan: logical.MeasureIndexScan(sT, eT, metadata, []logical.Expr{
				logical.Eq(logical.NewTagRef("default", "id"), logical.ID("unknown")),
			}, tsdb.Entity{tsdb.AnyEntry}, nil, nil, false, nil),
			wantLength: 0,
		},
		{
			name: "Single Index Search using id returns a result with tag projection",
			unresolvedPlan: logical.MeasureIndexScan(sT, eT, metadata, []logical.Expr{
				logical.Eq(logical.NewTagRef("default", "id"), logical.ID("1")),
			}, tsdb.Entity{tsdb.AnyEntry}, [][]*logical.Tag{{logical.NewTag("default", "id")}},
				nil, false, nil),
			wantLength: 1,
			tagLength:  []int{1},
		},
		{
			name: "Single Index Search using id returns a result with field projection",
			unresolvedPlan: logical.MeasureIndexScan(sT, eT, metadata, []logical.Expr{
				logical.Eq(logical.NewTagRef("default", "id"), logical.ID("1")),
			}, tsdb.Entity{tsdb.AnyEntry}, nil,
				[]*logical.Field{logical.NewField("total"), logical.NewField("value")}, false, nil),
			wantLength:  1,
			fieldLength: 2,
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

			iter, err := plan.(executor.MeasureExecutable).Execute(measureSvc)
			tester.NoError(err)
			defer func() {
				err = iter.Close()
				tester.NoError(err)
			}()
			dataSize := 0
			for iter.Next() {
				dataPoints := iter.Current()
				if len(dataPoints) < 1 {
					continue
				}
				dp := dataPoints[0]
				dataSize++
				tester.Len(dp.GetFields(), tt.fieldLength)
				tester.Len(dp.GetTagFamilies(), len(tt.tagLength))
				for tagFamilyIdx, tagFamily := range dp.GetTagFamilies() {
					tester.Len(tagFamily.GetTags(), tt.tagLength[tagFamilyIdx])
				}
			}
			tester.Equal(dataSize, tt.wantLength)
		})
	}
}

func TestMeasurePlanExecution_GroupByAndIndexScan(t *testing.T) {
	tester := require.New(t)
	measureSvc, metaService, deferFunc := setupMeasure(tester, "service_cpm_minute")
	defer deferFunc()
	baseTs := setupMeasureQueryData(t, "measure_query_data.json", measureSvc)

	metadata := &commonv1.Metadata{
		Name:  "service_cpm_minute",
		Group: "sw_metric",
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
			name: "Group By with Max",
			unresolvedPlan: logical.Aggregation(
				logical.GroupBy(
					logical.MeasureIndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry},
						[][]*logical.Tag{logical.NewTags("default", "entity_id")},
						[]*logical.Field{logical.NewField("value")}, true, nil),
					[][]*logical.Tag{logical.NewTags("default", "entity_id")},
					true,
				),
				logical.NewField("value"), modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX,
				true,
			),
			wantLength:  1,
			tagLength:   []int{1},
			fieldLength: 1,
		},
		{
			name: "Group By without Field",
			unresolvedPlan: logical.GroupBy(
				logical.MeasureIndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry},
					[][]*logical.Tag{logical.NewTags("default", "entity_id")},
					[]*logical.Field{}, true, nil),
				[][]*logical.Tag{logical.NewTags("default", "entity_id")},
				true,
			),
			wantLength:  1,
			tagLength:   []int{1},
			fieldLength: 0,
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

			iter, err := plan.(executor.MeasureExecutable).Execute(measureSvc)
			tester.NoError(err)
			defer func() {
				err = iter.Close()
				tester.NoError(err)
			}()
			dataSize := 0
			for iter.Next() {
				dataPoints := iter.Current()
				if len(dataPoints) < 1 {
					continue
				}
				dp := dataPoints[0]
				dataSize++
				tester.Len(dp.GetFields(), tt.fieldLength)
				tester.Len(dp.GetTagFamilies(), len(tt.tagLength))
				for tagFamilyIdx, tagFamily := range dp.GetTagFamilies() {
					tester.Len(tagFamily.GetTags(), tt.tagLength[tagFamilyIdx])
				}
			}
			tester.Equal(dataSize, tt.wantLength)
		})
	}
}

func TestMeasurePlanExecution_Cursor(t *testing.T) {
	tester := require.New(t)
	measureSvc, metaService, deferFunc := setupMeasure(tester, "service_cpm_minute")
	defer deferFunc()
	baseTs := setupMeasureQueryData(t, "measure_top_data.json", measureSvc)

	metadata := &commonv1.Metadata{
		Name:  "service_cpm_minute",
		Group: "sw_metric",
	}

	sT, eT := baseTs, baseTs.Add(1*time.Hour)

	analyzer, err := logical.CreateMeasureAnalyzerFromMetaService(metaService)
	tester.NoError(err)
	tester.NotNil(analyzer)

	tests := []struct {
		name           string
		unresolvedPlan logical.UnresolvedPlan
		want           []int64
	}{
		{
			name: "top 2",
			unresolvedPlan: logical.Top(logical.Aggregation(
				logical.GroupBy(
					logical.MeasureIndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry},
						[][]*logical.Tag{logical.NewTags("default", "entity_id")},
						[]*logical.Field{logical.NewField("value")}, true, nil),
					[][]*logical.Tag{logical.NewTags("default", "entity_id")},
					true,
				),
				logical.NewField("value"), modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN,
				true,
			), &measurev1.QueryRequest_Top{
				Number:         2,
				FieldName:      "value",
				FieldValueSort: modelv1.Sort_SORT_DESC,
			},
			),
			want: []int64{5, 3},
		},
		{
			name: "bottom 2",
			unresolvedPlan: logical.Top(logical.Aggregation(
				logical.GroupBy(
					logical.MeasureIndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry},
						[][]*logical.Tag{logical.NewTags("default", "entity_id")},
						[]*logical.Field{logical.NewField("value")}, true, nil),
					[][]*logical.Tag{logical.NewTags("default", "entity_id")},
					true,
				),
				logical.NewField("value"), modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN,
				true,
			), &measurev1.QueryRequest_Top{
				Number:         2,
				FieldName:      "value",
				FieldValueSort: modelv1.Sort_SORT_ASC,
			},
			),
			want: []int64{1, 3},
		},
		{
			name: "order by time ASC",
			unresolvedPlan: logical.MeasureIndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry},
				[][]*logical.Tag{logical.NewTags("default", "entity_id")},
				[]*logical.Field{logical.NewField("value")}, false,
				logical.NewOrderBy("", modelv1.Sort_SORT_ASC)),
			want: []int64{1, 1, 1, 5, 4, 5},
		},
		{
			name: "order by time DESC",
			unresolvedPlan: logical.MeasureIndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry},
				[][]*logical.Tag{logical.NewTags("default", "entity_id")},
				[]*logical.Field{logical.NewField("value")}, false,
				logical.NewOrderBy("", modelv1.Sort_SORT_DESC)),
			want: []int64{5, 4, 5, 1, 1, 1},
		},
		{
			name: "limit 3, 2",
			unresolvedPlan: logical.MeasureLimit(
				logical.MeasureIndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry},
					[][]*logical.Tag{logical.NewTags("default", "entity_id")},
					[]*logical.Field{logical.NewField("value")}, false,
					logical.NewOrderBy("", modelv1.Sort_SORT_ASC)),
				3, 2),
			want: []int64{5, 4},
		},
		{
			name: "limit 0, 100",
			unresolvedPlan: logical.MeasureLimit(
				logical.MeasureIndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry},
					[][]*logical.Tag{logical.NewTags("default", "entity_id")},
					[]*logical.Field{logical.NewField("value")}, false,
					logical.NewOrderBy("", modelv1.Sort_SORT_ASC)),
				0, 100),
			want: []int64{1, 1, 1, 5, 4, 5},
		},
		{
			name: "limit 0, 4",
			unresolvedPlan: logical.MeasureLimit(
				logical.MeasureIndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry},
					[][]*logical.Tag{logical.NewTags("default", "entity_id")},
					[]*logical.Field{logical.NewField("value")}, false,
					logical.NewOrderBy("", modelv1.Sort_SORT_ASC)),
				0, 4),
			want: []int64{1, 1, 1, 5},
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

			iter, err := plan.(executor.MeasureExecutable).Execute(measureSvc)
			tester.NoError(err)
			defer func() {
				err = iter.Close()
				tester.NoError(err)
			}()
			got := make([]int64, 0)
			for iter.Next() {
				dataPoints := iter.Current()
				if len(dataPoints) < 1 {
					continue
				}
				dp := dataPoints[0]
				for _, f := range dp.Fields {
					if f.Name == "value" {
						got = append(got, f.Value.GetInt().Value)
					}
				}
			}
			tester.Equal(tt.want, got)
		})
	}
}

func TestMeasurePlanExecution_Searchable(t *testing.T) {
	tester := require.New(t)
	measureSvc, metaService, deferFunc := setupMeasure(tester, "service_instance_traffic")
	defer deferFunc()
	baseTs := setupMeasureQueryData(t, "measure_search_data.json", measureSvc)

	metadata := &commonv1.Metadata{
		Name:  "service_instance_traffic",
		Group: "sw_metric",
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
			name: "search node a",
			unresolvedPlan: logical.MeasureIndexScan(sT, eT, metadata, []logical.Expr{
				logical.Match(logical.NewTagRef("default", "name"), logical.Str("nodea")),
			}, tsdb.Entity{tsdb.AnyEntry}, nil, nil, false, nil),
			wantLength: 2,
		},
		{
			name: "search nodes in us",
			unresolvedPlan: logical.MeasureIndexScan(sT, eT, metadata, []logical.Expr{
				logical.Match(logical.NewTagRef("default", "name"), logical.Str("us")),
			}, tsdb.Entity{tsdb.AnyEntry}, nil, nil, false, nil),
			wantLength: 2,
		},
		{
			name: "search nodes in cn",
			unresolvedPlan: logical.MeasureIndexScan(sT, eT, metadata, []logical.Expr{
				logical.Match(logical.NewTagRef("default", "name"), logical.Str("cn")),
			}, tsdb.Entity{tsdb.AnyEntry}, nil, nil, false, nil),
			wantLength: 1,
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

			iter, err := plan.(executor.MeasureExecutable).Execute(measureSvc)
			tester.NoError(err)
			defer func() {
				err = iter.Close()
				tester.NoError(err)
			}()
			dataSize := 0
			for iter.Next() {
				dataPoints := iter.Current()
				if len(dataPoints) < 1 {
					continue
				}
				dp := dataPoints[0]
				dataSize++
				tester.Len(dp.GetFields(), tt.fieldLength)
				tester.Len(dp.GetTagFamilies(), len(tt.tagLength))
				for tagFamilyIdx, tagFamily := range dp.GetTagFamilies() {
					tester.Len(tagFamily.GetTags(), tt.tagLength[tagFamilyIdx])
				}
			}
			tester.Equal(dataSize, tt.wantLength)
		})
	}
}
