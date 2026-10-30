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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func distinctBucketFixture(rawTime bool) (*rawSource, *AggregatePlan) {
	timestamp := vectorized.ColumnDef{Name: "time", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64}
	value := vectorized.ColumnDef{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
	bucket := vectorized.ColumnDef{Name: "__bucket", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{timestamp, value})
	batch := vectorized.NewRecordBatch(schema, 4)
	for row := 0; row < 4; row++ {
		batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(int64(row) * int64(time.Second))
		batch.Columns[1].(*vectorized.TypedColumn[int64]).Append(1)
	}
	batch.Len = 4
	plan := &AggregatePlan{
		TimeBucketWidth: "2s", GroupBy: []vectorized.ColumnDef{bucket}, Calls: []AggregateCall{{Source: value, Function: AggregateSum}},
		GroupExpressions: &ExpressionPlan{Outputs: []ExpressionOutput{{Column: bucket, Expr: &Expression{
			Op: "time_bucket", Type: ExpressionTimestamp, Literal: ExpressionScalar{Int64: int64(2 * time.Second)},
			Args: []*Expression{{Op: "column", Source: timestamp}},
		}}}},
	}
	if rawTime {
		plan.GroupBy = []vectorized.ColumnDef{timestamp, bucket}
		plan.TimeBucketIndex = 1
		plan.Outputs = append(plan.Outputs, AggregateOutput{
			Column: vectorized.ColumnDef{Name: "raw", TagFamily: "__ql", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeInt64}, IsGroup: true, Timestamp: true,
		})
	}
	for _, name := range []string{"bucket", "bucket_again"} {
		plan.Outputs = append(plan.Outputs, AggregateOutput{
			Column:  vectorized.ColumnDef{Name: name, TagFamily: "__ql", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeInt64},
			IsGroup: true, Index: plan.TimeBucketIndex, Timestamp: true,
		})
	}
	plan.Outputs = append(plan.Outputs, AggregateOutput{Column: vectorized.ColumnDef{Name: "sum", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}})
	return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}, plan
}

func collectDistinctBucketPoints(ctx context.Context, t *testing.T, source vectorized.PullOperator, plan *RawPlan) []*measurev1.DataPoint {
	t.Helper()
	operator, err := WrapRaw(ctx, source, plan)
	require.NoError(t, err)
	iterator, err := RawIterator(ctx, operator)
	require.NoError(t, err)
	var points []*measurev1.DataPoint
	for iterator.Next() {
		for _, point := range iterator.Current() {
			points = append(points, point.DataPoint)
		}
	}
	require.NoError(t, iterator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	return points
}

func TestDistinctVisibleBucketOwnsTimestampAfterGroupingAndHaving(t *testing.T) {
	for _, rawTime := range []bool{false, true} {
		for _, having := range []bool{false, true} {
			source, aggregate := distinctBucketFixture(rawTime)
			plan := &RawPlan{Distinct: true, Aggregate: aggregate, Slots: []Slot{
				{Source: source.schema.Columns[0], Output: source.schema.Columns[0]},
				{Source: source.schema.Columns[1], Output: source.schema.Columns[1]},
			}}
			if having {
				sum := aggregate.Outputs[len(aggregate.Outputs)-1].Column
				plan.Having = &HavingPlan{Predicate: &Expression{Op: ">", Args: []*Expression{{Op: "column", Source: sum}, expressionInt(0)}}, Outputs: []Slot{}}
				for _, output := range aggregate.Outputs {
					alias := output.Column
					alias.Name += "_alias"
					plan.Having.Outputs = append(plan.Having.Outputs, Slot{Source: output.Column, Output: alias, Timestamp: output.Timestamp})
				}
			}
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
			points := collectDistinctBucketPoints(ctx, t, source, plan)
			wantRows := 2
			if rawTime {
				wantRows = 4
			}
			require.Len(t, points, wantRows)
			for _, point := range points {
				firstBucket := 0
				if rawTime {
					firstBucket = 1
				}
				bucket := point.TagFamilies[0].Tags[firstBucket].Value.GetTimestamp()
				require.NotNil(t, point.Timestamp, "DISTINCT must preserve a directly visible bucket timestamp")
				require.Equal(t, bucket.AsTime(), point.Timestamp.AsTime())
				require.Equal(t, bucket.AsTime(), point.TagFamilies[0].Tags[firstBucket+1].Value.GetTimestamp().AsTime())
				if rawTime {
					raw := point.TagFamilies[0].Tags[0].Value.GetTimestamp().AsTime().UnixNano()
					require.Equal(t, raw-raw%int64(2*time.Second), point.Timestamp.AsTime().UnixNano())
				}
			}
		}
	}
}

func TestDistinctHiddenBucketAndRawMetadataNeverChangeVisibleTupleEquality(t *testing.T) {
	for _, having := range []bool{false, true} {
		source, aggregate := distinctBucketFixture(false)
		sum := aggregate.Outputs[len(aggregate.Outputs)-1]
		plan := &RawPlan{Distinct: true, Aggregate: aggregate, Slots: []Slot{
			{Source: source.schema.Columns[0], Output: source.schema.Columns[0]},
			{Source: source.schema.Columns[1], Output: source.schema.Columns[1]},
		}}
		if having {
			plan.Having = &HavingPlan{
				Predicate: &Expression{Op: ">", Args: []*Expression{{Op: "column", Source: sum.Column}, expressionInt(0)}},
				Outputs:   []Slot{{Source: sum.Column, Output: sum.Column}},
			}
		} else {
			aggregate.Outputs = []AggregateOutput{sum}
		}
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		points := collectDistinctBucketPoints(ctx, t, source, plan)
		require.Len(t, points, 1, "hidden bucket must not participate in DISTINCT equality")
		require.Nil(t, points[0].Timestamp)
		require.Empty(t, points[0].TagFamilies)
		require.EqualValues(t, 2, points[0].Fields[0].Value.GetInt().Value)
	}
	source, _ := distinctBucketFixture(false)
	value := source.schema.Columns[1]
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	points := collectDistinctBucketPoints(ctx, t, source, &RawPlan{Distinct: true, Slots: []Slot{{Source: value, Output: value}}})
	require.Len(t, points, 1)
	require.Nil(t, points[0].Timestamp)
}

func TestDistinctNativeBucketUsesDeclaredKeyThroughHavingAliases(t *testing.T) {
	for _, hidden := range []bool{false, true} {
		for _, having := range []bool{false, true} {
			rawTime := vectorized.ColumnDef{Name: "time", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64}
			bucket := vectorized.ColumnDef{Name: "bucket_value", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
			value := vectorized.ColumnDef{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeFloat64}
			outputs := []AggregateOutput{
				{
					Column:  vectorized.ColumnDef{Name: "raw", TagFamily: "__ql", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeInt64},
					IsGroup: true, Timestamp: true,
				},
				{
					Column:  vectorized.ColumnDef{Name: "bucket", TagFamily: "__ql", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeInt64},
					IsGroup: true, Index: 1, Timestamp: true,
				},
				{Column: vectorized.ColumnDef{Name: "count", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}},
			}
			plan := &NativeAggregatePlan{MaxCount: 3, Aggregate: &AggregatePlan{
				GroupBy: []vectorized.ColumnDef{rawTime, bucket}, Calls: []AggregateCall{{Source: value, Function: AggregateCount}},
				Outputs: outputs, TimeBucketWidth: "2s", TimeBucketIndex: 1,
			}, Leaves: []NativeAggregateLeaf{{Source: value, Function: AggregateCount}}}
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
			merged, err := MergeNativeAggregate(ctx, plan, func(context.Context, NativeAggregateLeaf) (vectorized.PullOperator, error) {
				schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{rawTime, bucket, value})
				batch := vectorized.NewRecordBatch(schema, 3)
				batch.Columns[0].(*vectorized.TypedColumn[int64]).AppendSlice([]int64{500000000, 2500000000, 3500000000})
				batch.Columns[1].(*vectorized.TypedColumn[int64]).AppendSlice([]int64{0, 2000000000, 2000000000})
				batch.Columns[2].(*vectorized.TypedColumn[float64]).AppendSlice([]float64{1, 1, 1})
				batch.Len = 3
				return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}, nil
			})
			require.NoError(t, err)
			visible := []Slot{}
			for index, output := range outputs {
				if hidden && index != 2 {
					continue
				}
				alias := output.Column
				alias.Name += "_alias"
				visible = append(visible, Slot{Source: output.Column, Output: alias, Timestamp: output.Timestamp})
			}
			raw := &RawPlan{Distinct: true, Slots: visible}
			if having {
				raw.Having = &HavingPlan{
					Predicate: &Expression{Op: ">", Args: []*Expression{{Op: "column", Source: outputs[2].Column}, expressionInt(0)}},
					Outputs:   visible,
				}
			}
			points := collectDistinctBucketPoints(ctx, t, merged, raw)
			if hidden {
				require.Len(t, points, 1)
				require.Nil(t, points[0].Timestamp)
				require.Empty(t, points[0].TagFamilies)
			} else {
				require.Len(t, points, 3)
				for _, point := range points {
					bucketValue := point.TagFamilies[0].Tags[1].Value.GetTimestamp()
					require.NotNil(t, point.Timestamp)
					require.Equal(t, bucketValue.AsTime(), point.Timestamp.AsTime())
					rawValue := point.TagFamilies[0].Tags[0].Value.GetTimestamp().AsTime().UnixNano()
					require.Equal(t, rawValue-rawValue%int64(2*time.Second), point.Timestamp.AsTime().UnixNano())
				}
			}
		}
	}
}
