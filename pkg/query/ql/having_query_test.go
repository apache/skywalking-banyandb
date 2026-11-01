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

package ql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func havingFixture() (*rawSource, *RawPlan, vectorized.ColumnDef) {
	host := vectorized.ColumnDef{Name: "host", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeString}
	value := aggregateField("v", vectorized.ColumnTypeInt64)
	sum := aggregateField("__sum", vectorized.ColumnTypeInt64)
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{host, value})
	batch := vectorized.NewRecordBatch(schema, 7)
	batch.Columns[0].(*vectorized.TypedColumn[string]).AppendSlice([]string{"a", "a", "b", "b", "c", "c", "d"})
	batch.Columns[1].(*vectorized.TypedColumn[int64]).AppendSlice([]int64{1, 1, 2, 2, 3, 3, 0})
	batch.Columns[1].MarkNullAt(6)
	batch.Len = 7
	source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
	plan := &RawPlan{
		Slots: []Slot{{Source: host, Output: host}, {Source: value, Output: value}},
		Aggregate: &AggregatePlan{
			GroupBy: []vectorized.ColumnDef{host},
			Calls:   []AggregateCall{{Source: value, Function: AggregateSum}},
			Outputs: []AggregateOutput{{Index: 0, IsGroup: true, Column: host}, {Index: 0, Column: sum}},
		},
	}
	return source, plan, sum
}

func TestHavingFiltersCompletedGroupsBeforePaginationAndHidesAggregate(t *testing.T) {
	source, plan, sum := havingFixture()
	host := source.schema.Columns[0]
	visible := host
	visible.Name = "node"
	plan.Having = &HavingPlan{
		Predicate: &Expression{Op: ">", Args: []*Expression{{Op: "column", Source: sum}, expressionInt(3)}},
		Outputs:   []Slot{{Source: host, Output: visible}},
	}
	limit := uint32(1)
	plan.Offset, plan.Limit = 1, &limit
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapRaw(ctx, source, plan)
	require.NoError(t, err)
	iterator, err := RawIterator(ctx, operator)
	require.NoError(t, err)
	require.True(t, iterator.Next())
	row := iterator.Current()[0].DataPoint
	require.Empty(t, row.Fields)
	require.Len(t, row.TagFamilies, 1)
	require.Len(t, row.TagFamilies[0].Tags, 1)
	require.Equal(t, "node", row.TagFamilies[0].Tags[0].Key)
	require.Equal(t, "c", row.TagFamilies[0].Tags[0].Value.GetStr().Value)
	require.False(t, iterator.Next())
	require.NoError(t, iterator.Close())
	require.Equal(t, 2, source.pulls)
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestHavingRunsAfterEveryNativeLeafAndSharesRepeatedAggregateState(t *testing.T) {
	host := vectorized.ColumnDef{Name: "host", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeString}
	value := aggregateField("v", vectorized.ColumnTypeFloat64)
	native := nativeMergePlan([]vectorized.ColumnDef{host}, value, AggregateSum, AggregateCount, AggregateAvg, AggregateSum)
	native.Leaves = []NativeAggregateLeaf{{Source: value, Function: AggregateSum}, {Source: value, Function: AggregateCount}}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	var sources []*rawSource
	merged, err := MergeNativeAggregate(ctx, native, func(_ context.Context, leaf NativeAggregateLeaf) (vectorized.PullOperator, error) {
		schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{host, value})
		batch := vectorized.NewRecordBatch(schema, 4)
		batch.Columns[0].(*vectorized.TypedColumn[string]).AppendSlice([]string{"a", "b", "c", "d"})
		values := []float64{2, 4, 6, 0}
		if leaf.Function == AggregateCount {
			values = []float64{2, 2, 2, 0}
		}
		batch.Columns[1].(*vectorized.TypedColumn[float64]).AppendSlice(values)
		if leaf.Function == AggregateSum {
			batch.Columns[1].MarkNullAt(3)
		}
		batch.Len = 4
		source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
		sources = append(sources, source)
		return source, nil
	})
	require.NoError(t, err)
	sum, avg := native.Aggregate.Outputs[1].Column, native.Aggregate.Outputs[3].Column
	visible, repeated := sum, sum
	visible.Name, repeated.Name = "total", "again"
	limit := uint32(1)
	operator, err := WrapRaw(ctx, merged, &RawPlan{
		Having: &HavingPlan{
			Predicate: &Expression{Op: ">", Args: []*Expression{{Op: "column", Source: avg}, expressionInt(1)}},
			Outputs:   []Slot{{Source: host, Output: host}, {Source: sum, Output: visible}, {Source: sum, Output: repeated}},
		},
		Offset: 1, Limit: &limit,
	})
	require.NoError(t, err)
	iterator, err := RawIterator(ctx, operator)
	require.NoError(t, err)
	require.True(t, iterator.Next())
	require.Len(t, sources, 2, "all SUM/COUNT leaves finish before filtering AVG")
	for _, source := range sources {
		require.Equal(t, 1, source.closes)
	}
	row := iterator.Current()[0].DataPoint
	require.Equal(t, "c", row.TagFamilies[0].Tags[0].Value.GetStr().Value)
	require.Len(t, row.Fields, 2)
	require.Equal(t, "total", row.Fields[0].Name)
	require.Equal(t, "again", row.Fields[1].Name)
	require.EqualValues(t, 6, row.Fields[0].Value.GetFloat().Value)
	require.EqualValues(t, 6, row.Fields[1].Value.GetFloat().Value)
	require.False(t, iterator.Next())
	require.NoError(t, iterator.Close())
	stats, ok := HavingStatistics(operator)
	require.True(t, ok)
	require.EqualValues(t, 4, stats.InputRows)
	require.EqualValues(t, 2, stats.TrueRows)
	require.EqualValues(t, 1, stats.UnknownRows)
	nativeStats, ok := NativeAggregateStatistics(operator)
	require.True(t, ok)
	require.EqualValues(t, 2, nativeStats.Branches)
	require.EqualValues(t, 4, nativeStats.Groups)
	require.EqualValues(t, 2, nativeStats.StateLanes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestHavingPreservesScalarEmptyAndNullOnlyAggregateSemantics(t *testing.T) {
	for _, empty := range []bool{false, true} {
		for _, keep := range []bool{false, true} {
			source, plan, sum := havingFixture()
			plan.Aggregate.GroupBy = nil
			plan.Aggregate.Outputs = plan.Aggregate.Outputs[1:]
			if !empty {
				source.batches[0].Selection = []uint16{6}
			}
			plan.Empty = empty
			op := ">"
			if keep {
				op = "is_null"
			}
			predicate := &Expression{Op: op, Args: []*Expression{{Op: "column", Source: sum}}}
			if !keep {
				predicate.Args = append(predicate.Args, expressionInt(0))
			}
			plan.Having = &HavingPlan{Predicate: predicate, Outputs: []Slot{{Source: sum, Output: sum}}}
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
			operator, err := WrapRaw(ctx, source, plan)
			require.NoError(t, err)
			iterator, err := RawIterator(ctx, operator)
			require.NoError(t, err)
			require.Equal(t, keep, iterator.Next())
			if keep {
				require.NotNil(t, iterator.Current()[0].DataPoint.Fields[0].Value.GetNull())
				require.False(t, iterator.Next())
			}
			require.NoError(t, iterator.Close())
			stats, ok := HavingStatistics(operator)
			require.True(t, ok)
			require.EqualValues(t, 1, stats.InputRows)
			require.Equal(t, uint64(1), stats.TrueRows+stats.UnknownRows)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		}
	}
}

func TestHavingRunsAfterRawResidualAndSharesLocalAggregateCalls(t *testing.T) {
	source, plan, sum := havingFixture()
	value := source.schema.Columns[1]
	plan.Filter = &Expression{Op: ">", Args: []*Expression{{Op: "column", Source: value}, expressionInt(1)}}
	plan.Aggregate.Calls = append(plan.Aggregate.Calls, plan.Aggregate.Calls[0])
	repeated := sum
	repeated.Name = "again"
	plan.Aggregate.Outputs = append(plan.Aggregate.Outputs, AggregateOutput{Index: 1, Column: repeated})
	host := source.schema.Columns[0]
	plan.Having = &HavingPlan{
		Predicate: &Expression{Op: ">", Args: []*Expression{{Op: "column", Source: sum}, expressionInt(3)}},
		Outputs:   []Slot{{Source: host, Output: host}},
	}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapRaw(ctx, source, plan)
	require.NoError(t, err)
	require.NoError(t, operator.Init(ctx))
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, batch.ActiveLen())
	require.Len(t, batch.Columns, 1)
	require.Equal(t, []string{"b", "c"}, batch.Columns[0].(*vectorized.TypedColumn[string]).Data())
	require.NoError(t, operator.Close())
	stats, ok := AggregateStatistics(operator)
	require.True(t, ok)
	require.EqualValues(t, 4, stats.InputRows)
	require.EqualValues(t, 2, stats.Groups)
	require.EqualValues(t, 2, stats.StateLanes)
	having, ok := HavingStatistics(operator)
	require.True(t, ok)
	require.EqualValues(t, 2, having.InputRows)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
