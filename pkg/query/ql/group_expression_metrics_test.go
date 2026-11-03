//go:build querymetrics

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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestGroupKeyApplicationsCountOnlyInvokedRoots(t *testing.T) {
	value := vectorized.ColumnDef{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
	keys := []vectorized.ColumnDef{
		{Name: "key0", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
		{Name: "key1", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
	}
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{value})
	batch := vectorized.NewRecordBatch(schema, 2)
	batch.Columns[0].(*vectorized.TypedColumn[int64]).AppendSlice([]int64{0, 1})
	batch.Len = 2
	source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	before := GroupExpressionExecutionStats()
	operator, err := WrapAggregate(ctx, source, &AggregatePlan{GroupBy: keys, GroupExpressions: &ExpressionPlan{Outputs: []ExpressionOutput{
		{Column: keys[0], Expr: &Expression{Op: "/", Args: []*Expression{expressionInt(1), {Op: "column", Source: value}}}},
		{Column: keys[1], Expr: &Expression{Op: "+", Args: []*Expression{{Op: "column", Source: value}, expressionInt(1)}}},
	}}, Outputs: []AggregateOutput{{Column: keys[0], IsGroup: true}}})
	require.NoError(t, err)
	require.NoError(t, operator.Init(ctx))
	_, err = operator.NextBatch(ctx)
	require.ErrorContains(t, err, "division by zero")
	require.NoError(t, operator.Close())
	after := GroupExpressionExecutionStats()
	// The second key never runs after the first key fails. This observation
	// distinguishes measured root applications from rows times planned keys.
	require.EqualValues(t, 2, after.KeyEvaluations-before.KeyEvaluations)
	require.EqualValues(t, 1, after.KeyRootCalls-before.KeyRootCalls)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestGroupKeyRootsCountSelectedBatchesAndCachedScalarWork(t *testing.T) {
	value := vectorized.ColumnDef{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
	keys := []vectorized.ColumnDef{
		{Name: "key0", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
		{Name: "key1", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
	}
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{value})
	var batches []*vectorized.RecordBatch
	for start := 0; start < 8; start += 4 {
		batch := vectorized.NewRecordBatch(schema, 4)
		for row := start; row < start+4; row++ {
			batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(int64(row))
		}
		batch.Len, batch.Selection = 4, []uint16{0, 2}
		batches = append(batches, batch)
	}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	before := GroupExpressionExecutionStats()
	operator, err := WrapAggregate(ctx, &rawSource{schema: schema, batches: batches}, &AggregatePlan{
		GroupBy: keys,
		GroupExpressions: &ExpressionPlan{Outputs: []ExpressionOutput{
			{Column: keys[0], Expr: &Expression{Op: "+", Args: []*Expression{{Op: "column", Source: value}, expressionInt(1)}}},
			{Column: keys[1], Expr: &Expression{Op: "+", Args: []*Expression{expressionInt(2), expressionInt(3)}}},
		}}, Outputs: []AggregateOutput{
			{Column: keys[0], IsGroup: true},
			{Column: keys[1], Index: 1, IsGroup: true},
			{Column: vectorized.ColumnDef{Name: "again", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}, IsGroup: true},
		},
	})
	require.NoError(t, err)
	require.NoError(t, operator.Init(ctx))
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3, 5, 7}, batch.Columns[0].(*vectorized.TypedColumn[int64]).Data())
	require.Equal(t, []int64{5, 5, 5, 5}, batch.Columns[1].(*vectorized.TypedColumn[int64]).Data())
	require.Equal(t, batch.Columns[0].(*vectorized.TypedColumn[int64]).Data(), batch.Columns[2].(*vectorized.TypedColumn[int64]).Data())
	require.NoError(t, operator.Close())
	after := GroupExpressionExecutionStats()
	require.EqualValues(t, 2, after.Batches-before.Batches)
	require.EqualValues(t, 4, after.InputRows-before.InputRows)
	require.EqualValues(t, 4, after.KeyRootCalls-before.KeyRootCalls, "two roots are actually called in each of two selected batches")
	require.EqualValues(t, 8, after.KeyEvaluations-before.KeyEvaluations)
	require.EqualValues(t, 4, after.VectorRows-before.VectorRows)
	require.EqualValues(t, 1, after.ScalarEvaluations-before.ScalarEvaluations, "constant arithmetic is evaluated once and reused by both batches")
	require.Equal(t, after.ReservedBytes-before.ReservedBytes, after.ReleasedBytes-before.ReleasedBytes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
