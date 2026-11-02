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

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/decimal"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestDecimalCoalesceZeroAndComparison(t *testing.T) {
	source := &Expression{Op: "column", Source: vectorized.ColumnDef{Name: "total", Role: vectorized.RoleField, Type: vectorized.ColumnTypeDecimal, Scale: 10}}
	resolved, err := ResolveExpression(&Expression{Op: "coalesce", Args: []*Expression{source, expressionInt(0)}})
	require.NoError(t, err)
	require.Equal(t, ExpressionDecimal, resolved.Type)
	require.Equal(t, uint8(10), resolved.Scale)
	require.Equal(t, ExpressionDecimal, resolved.Args[1].Type)
	again, err := ResolveExpression(resolved)
	require.NoError(t, err)
	require.Equal(t, uint8(10), again.Scale)
	null := &Expression{Op: "literal", Type: ExpressionNull, Literal: ExpressionScalar{Null: true}}
	withNull, err := ResolveExpression(&Expression{Op: "coalesce", Args: []*Expression{source, null}})
	require.NoError(t, err)
	require.Equal(t, uint8(10), withNull.Args[1].Scale)
	_, err = ResolveExpression(withNull)
	require.NoError(t, err)
	_, err = ResolveExpression(&Expression{Op: "literal", Type: ExpressionDecimal, Scale: 19})
	require.Error(t, err)
	_, err = ResolveExpression(&Expression{Op: "coalesce", Args: []*Expression{source, expressionInt(1)}})
	require.Error(t, err)
	left := &expressionVector{typ: ExpressionDecimal, scale: 10, decimals: []decimal.Int128{decimal.FromInt64(-1), decimal.FromInt64(1)}}
	zero := &expressionVector{typ: ExpressionInt, constant: true}
	require.True(t, expressionComparison("<", ExpressionDecimal)(left, zero, 0))
	require.True(t, expressionComparison(">", ExpressionDecimal)(left, zero, 1))
}

func TestCopyDecimalViewPreservesValuesAndNulls(t *testing.T) {
	column := vectorized.NewColumnForType(vectorized.ColumnTypeDecimal, 2).(*vectorized.TypedColumn[decimal.Int128])
	column.Append(decimal.FromInt64(-42))
	column.AppendNull()
	copied, err := copyViewColumn(column)
	require.NoError(t, err)
	result := copied.(*vectorized.TypedColumn[decimal.Int128])
	require.Equal(t, "-0.42", result.Data()[0].String(2))
	require.True(t, result.IsNull(1))
	column.Reset()
	require.Equal(t, "-0.42", result.Data()[0].String(2))
}

func TestDecimalLiteralProjectionRespectsBudgetAndScale(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	source := expressionResourceFixture(65536)
	plan := &ExpressionPlan{Outputs: []ExpressionOutput{{
		Expr:   &Expression{Op: "literal", Type: ExpressionDecimal, Scale: 10},
		Column: vectorized.ColumnDef{Name: "total", Role: vectorized.RoleField},
	}}}
	operator, err := WrapExpressions(ctx, source, plan)
	require.NoError(t, err)
	require.Equal(t, uint8(10), operator.OutputSchema().Columns[0].Scale)
	require.NoError(t, operator.Init(ctx))
	_, err = operator.NextBatch(ctx)
	require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
	require.NoError(t, operator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestDecimalMembershipUsesEveryOperandType(t *testing.T) {
	defs := []vectorized.ColumnDef{
		{Name: "amount", Role: vectorized.RoleField, Type: vectorized.ColumnTypeDecimal, Scale: 1},
		{Name: "other", Role: vectorized.RoleField, Type: vectorized.ColumnTypeDecimal, Scale: 2},
		{Name: "probe", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
	}
	amount := &Expression{Op: "column", Source: defs[0]}
	other := &Expression{Op: "column", Source: defs[1]}
	probe := &Expression{Op: "column", Source: defs[2]}
	for _, test := range []struct {
		name     string
		args     []*Expression
		expected []int64
		nulls    []int
	}{
		{"integer_first", []*Expression{expressionInt(1), amount}, []int64{1, 0, 0, 0}, []int{3}},
		{"decimal_last", []*Expression{expressionInt(1), expressionInt(2), amount}, []int64{1, 0, 0, 0}, []int{3}},
		{"integer_column", []*Expression{probe, amount}, []int64{1, 0, 1, 0}, []int{3}},
		{"decimal_first", []*Expression{amount, expressionInt(1)}, []int64{1, 0, 0, 0}, []int{3}},
		{"different_scales", []*Expression{amount, other}, []int64{1, 0, 1, 0}, []int{3}},
		{"null_candidate", []*Expression{expressionInt(1), amount, expressionNullLiteral()}, []int64{1, 0, 0, 0}, []int{1, 2, 3}},
		{"match_after_decimal", []*Expression{expressionInt(1), amount, expressionInt(1)}, []int64{1, 1, 1, 1}, nil},
	} {
		for _, operation := range []string{"in", "not_in"} {
			t.Run(test.name+"/"+operation, func(t *testing.T) {
				schema := vectorized.NewBatchSchema(defs)
				batch := vectorized.NewRecordBatch(schema, 4)
				for row, coefficient := range []int64{10, 15, -10} {
					batch.Columns[0].(*vectorized.TypedColumn[decimal.Int128]).Append(decimal.FromInt64(coefficient))
					batch.Columns[1].(*vectorized.TypedColumn[decimal.Int128]).Append(decimal.FromInt64([]int64{100, 250, -100}[row]))
					batch.Columns[2].(*vectorized.TypedColumn[int64]).Append([]int64{1, 1, -1}[row])
				}
				batch.Columns[0].AppendNull()
				batch.Columns[1].AppendNull()
				batch.Columns[2].(*vectorized.TypedColumn[int64]).Append(1)
				batch.Len = 4
				source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
				ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
				expression := expressionBoolProjection(&Expression{Op: operation, Args: test.args})
				op, err := WrapExpressions(ctx, source, &ExpressionPlan{Outputs: []ExpressionOutput{{
					Expr: expression, Column: vectorized.ColumnDef{Name: "result", Role: vectorized.RoleField},
				}}})
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, op.Close())
					require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
				})
				require.NoError(t, op.Init(ctx))
				require.NotPanics(t, func() {
					result, nextErr := op.NextBatch(ctx)
					require.NoError(t, nextErr)
					expected := append([]int64(nil), test.expected...)
					if operation == "not_in" {
						for row := range expected {
							expected[row] = 1 - expected[row]
						}
					}
					expressionAssertInts(t, result, expected, test.nulls)
				})
			})
		}
	}
}
