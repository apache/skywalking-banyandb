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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestMeasureFilterPipelineKeepsHiddenInputsOutOfResults(t *testing.T) {
	source, _ := rawFixture()
	visible := source.schema.Columns[5]
	visible.Name = "result"
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	limit := uint32(1)
	operator, err := WrapRaw(ctx, source, &RawPlan{
		Slots:    []Slot{{Source: source.schema.Columns[5], Output: visible}},
		Filter:   &Expression{Op: ">", Args: []*Expression{expressionValueColumn(), expressionInt(1)}},
		Residual: true, Ranges: []MillisRange{{Begin: 0, End: 1}, {Begin: 3000, End: 3001}}, Limit: &limit,
	})
	require.NoError(t, err)
	iterator, err := RawIterator(ctx, operator)
	require.NoError(t, err)
	require.True(t, iterator.Next())
	row := iterator.Current()[0].DataPoint
	require.Len(t, row.Fields, 1)
	require.Equal(t, "result", row.Fields[0].Name)
	require.EqualValues(t, 99, row.Fields[0].Value.GetInt().Value)
	require.Equal(t, time.Unix(3, 0).UTC(), row.Timestamp.AsTime())
	require.Empty(t, row.TagFamilies)
	require.EqualValues(t, 7, row.Sid)
	require.EqualValues(t, 9, row.Version)
	require.False(t, iterator.Next())
	require.NoError(t, iterator.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestMeasureFilterPipelineRunsBeforeProjectionAndPagination(t *testing.T) {
	source, _ := rawFixture()
	value := source.schema.Columns[4]
	result := vectorized.ColumnDef{Name: "result", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	limit := uint32(1)
	operator, err := WrapRaw(ctx, source, &RawPlan{
		Slots:  []Slot{{Source: value, Output: value}},
		Filter: &Expression{Op: ">", Args: []*Expression{expressionValueColumn(), expressionInt(0)}},
		Expressions: &ExpressionPlan{Outputs: []ExpressionOutput{{
			Expr: &Expression{Op: "/", Args: []*Expression{expressionInt(8), expressionValueColumn()}}, Column: result,
		}}},
		Offset: 1, Limit: &limit,
	})
	require.NoError(t, err)
	iterator, err := RawIterator(ctx, operator)
	require.NoError(t, err)
	require.True(t, iterator.Next())
	row := iterator.Current()[0].DataPoint
	require.Len(t, row.Fields, 1)
	require.EqualValues(t, 2, row.Fields[0].Value.GetInt().Value)
	require.Equal(t, time.Unix(3, 0).UTC(), row.Timestamp.AsTime())
	require.False(t, iterator.Next())
	require.NoError(t, iterator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestMeasureFilterPipelineConsumesAllRowsBeforeLocalAggregation(t *testing.T) {
	source, _ := rawFixture()
	value := source.schema.Columns[4]
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	limit := uint32(1)
	aggregate := &AggregatePlan{
		Calls: []AggregateCall{{Source: value, Function: AggregateCount}, {Source: value, Function: AggregateSum}},
		Outputs: []AggregateOutput{
			{Index: 0, Column: vectorized.ColumnDef{Name: "c", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}},
			{Index: 1, Column: vectorized.ColumnDef{Name: "s", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}},
		},
	}
	operator, err := WrapRaw(ctx, source, &RawPlan{
		Slots: []Slot{{Source: value, Output: value}}, Aggregate: aggregate, Limit: &limit,
		Filter: &Expression{Op: ">", Args: []*Expression{expressionValueColumn(), expressionInt(0)}},
	})
	require.NoError(t, err)
	iterator, err := RawIterator(ctx, operator)
	require.NoError(t, err)
	require.True(t, iterator.Next())
	row := iterator.Current()[0].DataPoint
	require.EqualValues(t, 2, row.Fields[0].Value.GetInt().Value)
	require.EqualValues(t, 4, row.Fields[1].Value.GetInt().Value)
	require.False(t, iterator.Next())
	require.NoError(t, iterator.Close())
	require.Equal(t, 2, source.pulls)
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestMeasureFilterTimeResidualSkipsInvalidRowsAndLimitZeroSkipsExecution(t *testing.T) {
	for _, limit := range []uint32{0, 1} {
		source, _ := rawFixture()
		value := source.schema.Columns[4]
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		operator, err := WrapRaw(ctx, source, &RawPlan{
			Slots: []Slot{{Source: value, Output: value}}, Limit: &limit,
			Residual: true, Ranges: []MillisRange{{Begin: 1000, End: 1001}, {Begin: 3000, End: 3001}},
			Filter: &Expression{Op: ">", Args: []*Expression{
				{Op: "/", Args: []*Expression{expressionInt(8), expressionValueColumn()}}, expressionInt(3),
			}},
		})
		require.NoError(t, err)
		iterator, err := RawIterator(ctx, operator)
		require.NoError(t, err)
		if limit != 0 {
			require.True(t, iterator.Next())
			row := iterator.Current()[0].DataPoint
			require.EqualValues(t, 1, row.Fields[0].Value.GetInt().Value)
			require.Equal(t, time.Unix(1, 0).UTC(), row.Timestamp.AsTime())
		}
		require.False(t, iterator.Next())
		require.NoError(t, iterator.Close())
		if limit == 0 {
			require.Zero(t, source.inits)
			require.Zero(t, source.pulls)
		}
		require.Equal(t, 1, source.closes)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}

func TestMeasureFilterPipelineRejectsNativeAggregateInput(t *testing.T) {
	source, _ := rawFixture()
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapRaw(ctx, source, &RawPlan{
		Filter:          &Expression{Op: "literal", Type: ExpressionBool, Literal: ExpressionScalar{Bool: true}},
		NativeAggregate: &NativeAggregatePlan{},
	})
	require.ErrorIs(t, err, ErrInvalidExpression)
	require.Nil(t, operator)
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
