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

func nativeMergePlan(groups []vectorized.ColumnDef, source vectorized.ColumnDef, functions ...AggregateFunction) *NativeAggregatePlan {
	plan := &AggregatePlan{GroupBy: groups}
	for index, group := range groups {
		output := group
		output.Entity = false
		plan.Outputs = append(plan.Outputs, AggregateOutput{Column: output, Index: index, IsGroup: true})
	}
	for index, function := range functions {
		call := AggregateCall{Source: source, Function: function}
		plan.Calls = append(plan.Calls, call)
		plan.Outputs = append(plan.Outputs, AggregateOutput{Column: aggregateField(string(rune('a'+index)), aggregateOutputType(call)), Index: index})
	}
	return &NativeAggregatePlan{Aggregate: plan, MaxCount: 100}
}

func TestNativeAggregateMergeMatchesFullKeysAcrossLeafOrders(t *testing.T) {
	groups := []vectorized.ColumnDef{
		{Name: "zone", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeString},
		aggregateField("rack", vectorized.ColumnTypeInt64),
	}
	value := aggregateField("v", vectorized.ColumnTypeFloat64)
	for _, reverse := range []bool{false, true} {
		plan := nativeMergePlan(groups, value, AggregateSum, AggregateCount, AggregateAvg, AggregateSum)
		plan.Leaves = []NativeAggregateLeaf{{Source: value, Function: AggregateSum}, {Source: value, Function: AggregateCount}}
		if reverse {
			plan.Leaves[0], plan.Leaves[1] = plan.Leaves[1], plan.Leaves[0]
		}
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		var opened []*rawSource
		open := func(_ context.Context, leaf NativeAggregateLeaf) (vectorized.PullOperator, error) {
			for _, previous := range opened {
				require.Equal(t, 1, previous.closes, "a leaf must close before the next opens")
			}
			schema := vectorized.NewBatchSchema(append(append([]vectorized.ColumnDef(nil), groups...), value))
			batch := vectorized.NewRecordBatch(schema, 3)
			zones := []string{"a", "a", "b"}
			racks := []int64{1, 2, 1}
			values := []float64{12, 9, 0}
			if leaf.Function == AggregateCount {
				zones, racks, values = []string{"b", "a", "a"}, []int64{1, 2, 1}, []float64{0, 3, 2}
			}
			batch.Columns[0].(*vectorized.TypedColumn[string]).AppendSlice(zones)
			batch.Columns[1].(*vectorized.TypedColumn[int64]).AppendSlice(racks)
			batch.Columns[2].(*vectorized.TypedColumn[float64]).AppendSlice(values)
			batch.Len = 3
			source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
			opened = append(opened, source)
			return source, nil
		}
		op, err := MergeNativeAggregate(ctx, plan, open)
		require.NoError(t, err)
		require.NoError(t, op.Init(ctx))
		batch, err := op.NextBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, batch.Len)
		seen := map[string]bool{}
		for row := range batch.Len {
			zone := batch.Columns[0].(*vectorized.TypedColumn[string]).Data()[row]
			rack := batch.Columns[1].(*vectorized.TypedColumn[int64]).Data()[row]
			key := zone + string(rune('0'+rack))
			require.False(t, seen[key])
			seen[key] = true
			switch key {
			case "a1":
				require.Equal(t, 12., batch.Columns[2].(*vectorized.TypedColumn[float64]).Data()[row])
				require.Equal(t, int64(2), batch.Columns[3].(*vectorized.TypedColumn[int64]).Data()[row])
				require.Equal(t, 6., batch.Columns[4].(*vectorized.TypedColumn[float64]).Data()[row])
			case "a2":
				require.Equal(t, 9., batch.Columns[2].(*vectorized.TypedColumn[float64]).Data()[row])
				require.Equal(t, int64(3), batch.Columns[3].(*vectorized.TypedColumn[int64]).Data()[row])
				require.Equal(t, 3., batch.Columns[4].(*vectorized.TypedColumn[float64]).Data()[row])
			case "b1":
				require.Zero(t, batch.Columns[3].(*vectorized.TypedColumn[int64]).Data()[row])
				require.True(t, batch.Columns[2].IsNull(row))
				require.True(t, batch.Columns[4].IsNull(row))
			default:
				t.Fatalf("unexpected group %q", key)
			}
			require.Equal(t, batch.Columns[2].IsNull(row), batch.Columns[5].IsNull(row))
			require.Equal(t, batch.Columns[2].(*vectorized.TypedColumn[float64]).Data()[row], batch.Columns[5].(*vectorized.TypedColumn[float64]).Data()[row])
		}
		require.NoError(t, op.Close())
		require.NoError(t, op.Close())
		require.Len(t, opened, 2)
		require.Equal(t, 1, opened[1].closes)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}
