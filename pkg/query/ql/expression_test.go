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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func expressionInt(value int64) *Expression {
	return &Expression{Op: "literal", Type: ExpressionInt, Literal: ExpressionScalar{Int64: value}}
}

func expressionFixture(t *testing.T, expr *Expression, selection []uint16) (context.Context, vectorized.PullOperator) {
	t.Helper()
	source, _ := rawFixture()
	source.batches[0].Selection = selection
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	resolved, err := ResolveExpression(expr)
	require.NoError(t, err)
	op, err := WrapExpressions(ctx, source, &ExpressionPlan{Outputs: []ExpressionOutput{{
		Expr: resolved, Column: vectorized.ColumnDef{Name: "result", Role: vectorized.RoleField},
	}}})
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	t.Cleanup(func() {
		require.NoError(t, op.Close())
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	})
	return ctx, op
}

func expressionValueColumn() *Expression {
	return &Expression{Op: "column", Source: vectorized.ColumnDef{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}}
}

func TestExpressionProjectionArithmeticPreservesNullSelectionAndMetadata(t *testing.T) {
	expr := &Expression{Op: "+", Args: []*Expression{expressionValueColumn(), expressionInt(2)}}
	ctx, op := expressionFixture(t, expr, []uint16{3, 2, 0})
	batch, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint16{3, 2, 0}, batch.Selection)
	require.Equal(t, 4, batch.Len)
	value := batch.Columns[0].(*vectorized.TypedColumn[int64])
	require.Equal(t, int64(5), value.Data()[3])
	require.True(t, value.IsNull(2))
	require.Equal(t, int64(2), value.Data()[0])
	require.Equal(t, 3, len(batch.Schema.Columns)-1)
	require.Equal(t, int64(7), batch.Columns[2].(*vectorized.TypedColumn[int64]).Data()[0])
}

func TestExpressionConditionalSkipsDivisionByZeroAndUnusedInvalidCast(t *testing.T) {
	zero := expressionInt(0)
	expr := &Expression{Op: "case", Args: []*Expression{
		{Op: "=", Args: []*Expression{expressionValueColumn(), zero}},
		zero,
		{Op: "/", Args: []*Expression{expressionInt(8), expressionValueColumn()}},
	}}
	ctx, op := expressionFixture(t, expr, nil)
	batch, err := op.NextBatch(ctx)
	require.NoError(t, err)
	column := batch.Columns[0].(*vectorized.TypedColumn[int64])
	require.Equal(t, []int64{0, 8, 0, 2}, column.Data())
	require.True(t, column.IsNull(2))
	badCast := &Expression{Op: "cast", Type: ExpressionInt, Args: []*Expression{{Op: "literal", Type: ExpressionString, Literal: ExpressionScalar{String: "invalid"}}}}
	ctx, op = expressionFixture(t, &Expression{Op: "coalesce", Args: []*Expression{expressionInt(7), badCast}}, nil)
	batch, err = op.NextBatch(ctx)
	require.NoError(t, err)
	constant, ok := vectorized.AsConst(batch.Columns[0])
	require.True(t, ok)
	require.Equal(t, int64(7), constant.Int64())
}

func TestExpressionDecodesPassthroughSourceOnlyForEligibleRows(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	def := vectorized.ColumnDef{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeFieldValue}
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{def})
	batch := vectorized.NewRecordBatch(schema, 3)
	column := batch.Columns[0].(*vectorized.TypedColumn[*modelv1.FieldValue])
	column.Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 2}}})
	column.Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: "wrong type in inactive row"}}})
	column.Append(nil)
	batch.Len = 3
	batch.Selection = []uint16{0, 2}
	source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
	op, err := WrapExpressions(ctx, source, &ExpressionPlan{Outputs: []ExpressionOutput{{
		Expr:   &Expression{Op: "+", Args: []*Expression{expressionValueColumn(), expressionInt(2)}},
		Column: vectorized.ColumnDef{Name: "result", Role: vectorized.RoleField},
	}}})
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	result, err := op.NextBatch(ctx)
	require.NoError(t, err)
	output := result.Columns[0].(*vectorized.TypedColumn[int64])
	require.Equal(t, int64(4), output.Data()[0])
	require.True(t, output.IsNull(2))
	require.NoError(t, op.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestExpressionRemainderAndTypedNullConditional(t *testing.T) {
	ctx, op := expressionFixture(t, &Expression{Op: "%", Args: []*Expression{expressionInt(math.MinInt64), expressionInt(-1)}}, nil)
	batch, err := op.NextBatch(ctx)
	require.NoError(t, err)
	constant, ok := vectorized.AsConst(batch.Columns[0])
	require.True(t, ok)
	require.Equal(t, int64(0), constant.Int64())
	ctx, op = expressionFixture(t, &Expression{Op: "+", Args: []*Expression{
		{Op: "coalesce", Args: []*Expression{
			{Op: "literal", Type: ExpressionNull, Literal: ExpressionScalar{Null: true}},
			{Op: "literal", Type: ExpressionNull, Literal: ExpressionScalar{Null: true}},
		}}, expressionInt(2),
	}}, nil)
	batch, err = op.NextBatch(ctx)
	require.NoError(t, err)
	require.True(t, batch.Columns[0].IsNull(0))
}

func TestExpressionNonfiniteComparisonsAgreeWithAggregateOrdering(t *testing.T) {
	floating := func(value float64) *Expression {
		return &Expression{Op: "literal", Type: ExpressionFloat, Literal: ExpressionScalar{Float64: value}}
	}
	for _, test := range []struct {
		op          string
		left, right float64
		expected    int64
	}{
		{"=", math.NaN(), math.NaN(), 1},
		{">", math.NaN(), math.Inf(1), 1},
		{"<", math.Inf(1), math.NaN(), 1},
		{"!=", math.NaN(), math.NaN(), 0},
		{"<=", math.NaN(), math.NaN(), 1},
		{">=", math.NaN(), math.NaN(), 1},
		{"=", math.Copysign(0, -1), 0, 1},
	} {
		t.Run(test.op, func(t *testing.T) {
			ctx, op := expressionFixture(t, &Expression{Op: "cast", Type: ExpressionInt, Args: []*Expression{
				{Op: test.op, Args: []*Expression{floating(test.left), floating(test.right)}},
			}}, nil)
			batch, err := op.NextBatch(ctx)
			require.NoError(t, err)
			column, ok := vectorized.AsConst(batch.Columns[0])
			require.True(t, ok)
			require.Equal(t, test.expected, column.Int64())
		})
	}
}

func TestExpressionLiteralReservationPrecedesCopyAndOwnsSourceOnFailure(t *testing.T) {
	source, _ := rawFixture()
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 16<<10)
	literal := &Expression{Op: "literal", Type: ExpressionBytes, Literal: ExpressionScalar{Bytes: make([]byte, 64<<10)}}
	op, err := WrapExpressions(ctx, source, &ExpressionPlan{Outputs: []ExpressionOutput{{Expr: literal}}})
	require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
	require.Nil(t, op)
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestExpressionPassthroughNullBitmapTakesPriorityOverCellPayload(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeFieldValue}})
	batch := vectorized.NewRecordBatch(schema, 1)
	column := batch.Columns[0].(*vectorized.TypedColumn[*modelv1.FieldValue])
	column.Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: "stale null cell"}}})
	column.MarkNullAt(0)
	batch.Len = 1
	source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
	op, err := WrapExpressions(ctx, source, &ExpressionPlan{Outputs: []ExpressionOutput{{
		Expr: &Expression{Op: "+", Args: []*Expression{expressionValueColumn(), expressionInt(2)}},
	}}})
	require.NoError(t, err)
	result, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.True(t, result.Columns[0].IsNull(0))
	require.NoError(t, op.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestExpressionScalarCachingAndMaskedWorkStatistics(t *testing.T) {
	constant := &Expression{Op: "cast", Type: ExpressionFloat, Args: []*Expression{{Op: "literal", Type: ExpressionString, Literal: ExpressionScalar{String: "1.5"}}}}
	source, _ := rawFixture()
	source.batches = append(source.batches, source.batches[0])
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := WrapExpressions(ctx, source, &ExpressionPlan{Outputs: []ExpressionOutput{{Expr: constant}}})
	require.NoError(t, err)
	for range 2 {
		batch, pullErr := op.NextBatch(ctx)
		require.NoError(t, pullErr)
		require.Equal(t, float64(1.5), batch.Columns[0].(*vectorized.ConstColumn).Float64())
	}
	stats, ok := ExpressionStatistics(op)
	require.True(t, ok)
	require.Equal(t, uint64(1), stats.Executions)
	require.Equal(t, uint64(2), stats.Batches)
	require.Equal(t, uint64(8), stats.InputRows)
	require.Equal(t, uint64(8), stats.OutputRows)
	require.Equal(t, uint64(1), stats.ScalarEvaluations)
	require.Zero(t, stats.FullColumnAllocations)
	require.NoError(t, op.Close())
	stats, ok = ExpressionStatistics(op)
	require.True(t, ok)
	require.Zero(t, stats.LiveBytes)
	require.Equal(t, stats.ReservedBytes, stats.ReleasedBytes)
	for _, test := range []struct {
		first int64
		later uint64
	}{{0, 0}, {1, 4}} {
		source, _ = rawFixture()
		source.batches[0].Columns[4] = vectorized.NewConstInt64(test.first, 4)
		expr := &Expression{Op: "case", Args: []*Expression{{Op: "=", Args: []*Expression{expressionValueColumn(), expressionInt(0)}}, expressionInt(7), expressionInt(9)}}
		op, err = WrapExpressions(ctx, source, &ExpressionPlan{Outputs: []ExpressionOutput{{Expr: expr}}})
		require.NoError(t, err)
		_, err = op.NextBatch(ctx)
		require.NoError(t, err)
		stats, ok = ExpressionStatistics(op)
		require.True(t, ok)
		require.Equal(t, test.later, stats.LaterArmRows)
		require.Equal(t, uint64(4), stats.ArmRows)
		require.NoError(t, op.Close())
	}
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
