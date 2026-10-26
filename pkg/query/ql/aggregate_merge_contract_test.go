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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func nativeMergeScalarSource(def vectorized.ColumnDef, values ...float64) *rawSource {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{def})
	batch := vectorized.NewRecordBatch(schema, len(values))
	for _, value := range values {
		if def.Type == vectorized.ColumnTypeFloat64 {
			batch.Columns[0].(*vectorized.TypedColumn[float64]).Append(value)
		} else {
			batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(int64(value))
		}
	}
	batch.Len = len(values)
	return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
}

func TestNativeAggregateMergeRejectsMalformedCountsAndGroups(t *testing.T) {
	value := aggregateField("v", vectorized.ColumnTypeFloat64)
	for _, testCase := range []struct {
		name   string
		values []float64
	}{
		{name: "negative", values: []float64{-1}},
		{name: "fraction", values: []float64{1.5}},
		{name: "nan", values: []float64{math.NaN()}},
		{name: "infinity", values: []float64{math.Inf(1)}},
		{name: "above-bound", values: []float64{101}},
		{name: "duplicate-final-group", values: []float64{1, 1}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			plan := nativeMergePlan(nil, value, AggregateCount)
			plan.Leaves = []NativeAggregateLeaf{{Source: value, Function: AggregateCount}}
			source := nativeMergeScalarSource(value, testCase.values...)
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
			op, err := MergeNativeAggregate(ctx, plan, func(context.Context, NativeAggregateLeaf) (vectorized.PullOperator, error) { return source, nil })
			require.NoError(t, err)
			require.NoError(t, op.Init(ctx))
			batch, err := op.NextBatch(ctx)
			require.Error(t, err)
			require.Nil(t, batch)
			require.NoError(t, op.Close())
			require.Equal(t, 1, source.closes)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
}

func TestNativeAggregateMergeExactCountBoundaryAndIntegerExtrema(t *testing.T) {
	floatDef := aggregateField("f", vectorized.ColumnTypeFloat64)
	plan := nativeMergePlan(nil, floatDef, AggregateCount)
	plan.MaxCount = 1 << 53
	plan.Leaves = []NativeAggregateLeaf{{Source: floatDef, Function: AggregateCount}}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := MergeNativeAggregate(ctx, plan, func(context.Context, NativeAggregateLeaf) (vectorized.PullOperator, error) {
		return nativeMergeScalarSource(floatDef, 1<<53), nil
	})
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	batch, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1<<53), batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0])
	require.NoError(t, op.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())

	intDef := aggregateField("v", vectorized.ColumnTypeInt64)
	plan = nativeMergePlan(nil, intDef, AggregateMin, AggregateMax, AggregateCount)
	plan.Leaves = []NativeAggregateLeaf{{Source: intDef, Function: AggregateMin}, {Source: intDef, Function: AggregateMax}, {Source: intDef, Function: AggregateCount}}
	op, err = MergeNativeAggregate(ctx, plan, func(_ context.Context, leaf NativeAggregateLeaf) (vectorized.PullOperator, error) {
		var actual int64 = 1
		switch leaf.Function {
		case AggregateMin:
			actual = 1<<53 + 1
		case AggregateMax:
			actual = math.MaxInt64
		default:
		}
		source := nativeMergeScalarSource(intDef, 0)
		source.batches[0].Columns[0] = vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, []int64{actual})
		return source, nil
	})
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	batch, err = op.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1<<53+1), batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0])
	require.Equal(t, int64(math.MaxInt64), batch.Columns[1].(*vectorized.TypedColumn[int64]).Data()[0])
	require.NoError(t, op.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestNativeAggregateMergeMissingValuesAndEmptySemantics(t *testing.T) {
	value := aggregateField("v", vectorized.ColumnTypeFloat64)
	key := aggregateField("key", vectorized.ColumnTypeInt64)
	for _, grouped := range []bool{false, true} {
		for _, count := range []float64{0, 1} {
			plan := nativeMergePlan(nil, value, AggregateCount, AggregateSum, AggregateAvg)
			if grouped {
				plan.Aggregate.GroupBy = []vectorized.ColumnDef{key}
			}
			plan.Leaves = []NativeAggregateLeaf{{Source: value, Function: AggregateCount}, {Source: value, Function: AggregateSum}}
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
			op, err := MergeNativeAggregate(ctx, plan, func(_ context.Context, leaf NativeAggregateLeaf) (vectorized.PullOperator, error) {
				defs := append(append([]vectorized.ColumnDef(nil), plan.Aggregate.GroupBy...), value)
				schema := vectorized.NewBatchSchema(defs)
				if leaf.Function == AggregateSum {
					return &rawSource{schema: schema}, nil
				}
				batch := vectorized.NewRecordBatch(schema, 1)
				if grouped {
					batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(7)
				}
				batch.Columns[len(defs)-1].(*vectorized.TypedColumn[float64]).Append(count)
				batch.Len = 1
				return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}, nil
			})
			require.NoError(t, err)
			require.NoError(t, op.Init(ctx))
			batch, err := op.NextBatch(ctx)
			if count > 0 {
				require.ErrorContains(t, err, "positive COUNT")
				require.Nil(t, batch)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, batch.Len)
				require.Zero(t, batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0])
				require.True(t, batch.Columns[1].IsNull(0))
				require.True(t, batch.Columns[2].IsNull(0))
			}
			require.NoError(t, op.Close())
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		}
	}
	for _, grouped := range []bool{false, true} {
		plan := nativeMergePlan(nil, value, AggregateCount, AggregateSum, AggregateAvg)
		if grouped {
			plan.Aggregate.GroupBy = []vectorized.ColumnDef{key}
		}
		plan.Leaves = []NativeAggregateLeaf{{Source: value, Function: AggregateCount}, {Source: value, Function: AggregateSum}}
		op, err := MergeNativeAggregate(t.Context(), plan, func(context.Context, NativeAggregateLeaf) (vectorized.PullOperator, error) {
			return &rawSource{schema: vectorized.NewBatchSchema(append(append([]vectorized.ColumnDef(nil), plan.Aggregate.GroupBy...), value))}, nil
		})
		require.NoError(t, err)
		require.NoError(t, op.Init(t.Context()))
		batch, err := op.NextBatch(t.Context())
		require.NoError(t, err)
		if grouped {
			require.Nil(t, batch)
		} else {
			require.Equal(t, 1, batch.Len)
			require.Zero(t, batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0])
			require.True(t, batch.Columns[1].IsNull(0))
			require.True(t, batch.Columns[2].IsNull(0))
		}
		require.NoError(t, op.Close())
	}
}

func TestNativeAggregateMergeSingleIntegerSumAndAverage(t *testing.T) {
	value := aggregateField("v", vectorized.ColumnTypeInt64)
	plan := nativeMergePlan(nil, value, AggregateSum, AggregateCount, AggregateAvg)
	plan.MaxCount = 1
	plan.Leaves = []NativeAggregateLeaf{{Source: value, Function: AggregateSum}, {Source: value, Function: AggregateCount}}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := MergeNativeAggregate(ctx, plan, func(_ context.Context, leaf NativeAggregateLeaf) (vectorized.PullOperator, error) {
		number := int64(math.MaxInt64)
		if leaf.Function == AggregateCount {
			number = 1
		}
		source := nativeMergeScalarSource(value, 0)
		source.batches[0].Columns[0] = vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, []int64{number})
		return source, nil
	})
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	batch, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0])
	require.Equal(t, int64(1), batch.Columns[1].(*vectorized.TypedColumn[int64]).Data()[0])
	require.Equal(t, float64(math.MaxInt64), batch.Columns[2].(*vectorized.TypedColumn[float64]).Data()[0])
	stats, ok := NativeAggregateStatistics(op)
	require.True(t, ok)
	require.EqualValues(t, 3, stats.StateLanes)
	require.NoError(t, op.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	plan.MaxCount = 2
	_, err = MergeNativeAggregate(ctx, plan, func(context.Context, NativeAggregateLeaf) (vectorized.PullOperator, error) {
		t.Fatal("unsafe integer aggregate dispatched a native leaf")
		return nil, nil
	})
	require.ErrorContains(t, err, "at most one")
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestNativeAggregateMergeRejectsChangingLeafSchemas(t *testing.T) {
	value := aggregateField("v", vectorized.ColumnTypeFloat64)
	plan := nativeMergePlan(nil, value, AggregateCount)
	plan.Leaves = []NativeAggregateLeaf{{Source: value, Function: AggregateCount}}
	source := nativeMergeScalarSource(value, 1)
	source.batches[0].Schema = vectorized.NewBatchSchema([]vectorized.ColumnDef{aggregateField("other", vectorized.ColumnTypeFloat64)})
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := MergeNativeAggregate(ctx, plan, func(context.Context, NativeAggregateLeaf) (vectorized.PullOperator, error) { return source, nil })
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	batch, err := op.NextBatch(ctx)
	require.ErrorContains(t, err, "schema")
	require.Nil(t, batch)
	require.NoError(t, op.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
