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

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestNativeAggregateMergeAcrossSelectionBoundary(t *testing.T) {
	for _, permuted := range []bool{false, true} {
		stats := validateNativeMergeKernel(t, makeNativeMergeKernelFixture(65537, permuted))
		require.EqualValues(t, 65537, stats.Groups)
		require.EqualValues(t, 2, stats.StateLanes)
		require.EqualValues(t, 65537*18, stats.StateBytes)
	}
}

func TestNativeAggregateMergeIncludesBucketTimestampInKey(t *testing.T) {
	key := vectorized.ColumnDef{Name: "host", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeString}
	bucket := vectorized.ColumnDef{Name: "timestamp", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64}
	value := aggregateField("v", vectorized.ColumnTypeFloat64)
	plan := nativeMergePlan([]vectorized.ColumnDef{key, bucket}, value, AggregateCount)
	plan.Aggregate.Outputs[1].Column = aggregateField("bucket", vectorized.ColumnTypeInt64)
	plan.Leaves = []NativeAggregateLeaf{{Source: value, Function: AggregateCount}}
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{key, bucket, value})
	batch := vectorized.NewRecordBatch(schema, 2)
	batch.Columns[0].(*vectorized.TypedColumn[string]).AppendSlice([]string{"a", "a"})
	batch.Columns[1].(*vectorized.TypedColumn[int64]).AppendSlice([]int64{100, 200})
	batch.Columns[2].(*vectorized.TypedColumn[float64]).AppendSlice([]float64{2, 3})
	batch.Len = 2
	op, err := MergeNativeAggregate(t.Context(), plan, func(context.Context, NativeAggregateLeaf) (vectorized.PullOperator, error) {
		return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}, nil
	})
	require.NoError(t, err)
	require.NoError(t, op.Init(t.Context()))
	actual, err := op.NextBatch(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, actual.Len)
	require.Equal(t, -1, actual.Schema.TimestampIndex())
	require.Equal(t, []int64{100, 200}, actual.Columns[1].(*vectorized.TypedColumn[int64]).Data())
	require.Equal(t, []int64{2, 3}, actual.Columns[2].(*vectorized.TypedColumn[int64]).Data())
	require.NoError(t, op.Close())
}

func TestNativeAggregateMergeUnionsMissingSourceGroupsAndNullKeys(t *testing.T) {
	key := vectorized.ColumnDef{Name: "host", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeString}
	left, right := aggregateField("left", vectorized.ColumnTypeFloat64), aggregateField("right", vectorized.ColumnTypeFloat64)
	plan := nativeMergePlan([]vectorized.ColumnDef{key}, left, AggregateCount, AggregateCount)
	plan.Aggregate.Calls[1].Source = right
	plan.Leaves = []NativeAggregateLeaf{{Source: left, Function: AggregateCount}, {Source: right, Function: AggregateCount}}
	op, err := MergeNativeAggregate(t.Context(), plan, func(_ context.Context, leaf NativeAggregateLeaf) (vectorized.PullOperator, error) {
		schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{key, leaf.Source})
		batch := vectorized.NewRecordBatch(schema, 2)
		if leaf.Source.Name == "left" {
			batch.Columns[0].AppendNull()
			batch.Columns[0].(*vectorized.TypedColumn[string]).Append("shared")
			batch.Columns[1].(*vectorized.TypedColumn[float64]).AppendSlice([]float64{2, 3})
		} else {
			batch.Columns[0].(*vectorized.TypedColumn[string]).AppendSlice([]string{"", "shared"})
			batch.Columns[1].(*vectorized.TypedColumn[float64]).AppendSlice([]float64{4, 5})
		}
		batch.Len = 2
		return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}, nil
	})
	require.NoError(t, err)
	require.NoError(t, op.Init(t.Context()))
	actual, err := op.NextBatch(t.Context())
	require.NoError(t, err)
	require.Equal(t, 3, actual.Len)
	require.True(t, actual.Columns[0].IsNull(0))
	require.False(t, actual.Columns[0].IsNull(2))
	require.Equal(t, []int64{2, 3, 0}, actual.Columns[1].(*vectorized.TypedColumn[int64]).Data())
	require.Equal(t, []int64{0, 5, 4}, actual.Columns[2].(*vectorized.TypedColumn[int64]).Data())
	require.NoError(t, op.Close())
}
