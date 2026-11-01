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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func expressionBoolProjection(expr *Expression) *Expression {
	return &Expression{Op: "cast", Type: ExpressionInt, Args: []*Expression{expr}}
}

func expressionNullLiteral() *Expression {
	return &Expression{Op: "literal", Type: ExpressionNull, Literal: ExpressionScalar{Null: true}}
}

func expressionAssertInts(t *testing.T, batch *vectorized.RecordBatch, expected []int64, nullRows []int) {
	t.Helper()
	nulls := make(map[int]bool, len(nullRows))
	for _, row := range nullRows {
		nulls[row] = true
	}
	constant, isConstant := vectorized.AsConst(batch.Columns[0])
	var data []int64
	if !isConstant {
		data = batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()
	}
	for row, want := range expected {
		require.Equal(t, nulls[row], batch.Columns[0].IsNull(row), "row %d", row)
		if nulls[row] {
			continue
		}
		if isConstant {
			require.Equal(t, want, constant.Int64(), "row %d", row)
		} else {
			require.Equal(t, want, data[row], "row %d", row)
		}
	}
}

func TestExpressionThreeValuedPredicatesAndMaskedBooleanBranches(t *testing.T) {
	comparison := func(op string, left, right *Expression) *Expression {
		return &Expression{Op: op, Args: []*Expression{left, right}}
	}
	for _, test := range []struct {
		name     string
		expr     *Expression
		expected []int64
		nulls    []int
	}{
		{"in-null", &Expression{Op: "in", Args: []*Expression{expressionValueColumn(), expressionInt(1), expressionNullLiteral()}}, []int64{0, 1, 0, 0}, []int{0, 2, 3}},
		{
			"not-in-null",
			&Expression{Op: "not_in", Args: []*Expression{expressionValueColumn(), expressionInt(1), expressionNullLiteral()}},
			[]int64{0, 0, 0, 0},
			[]int{0, 2, 3},
		},
		{"empty-in", &Expression{Op: "in", Args: []*Expression{expressionValueColumn()}}, []int64{0, 0, 0, 0}, nil},
		{"empty-not-in", &Expression{Op: "not_in", Args: []*Expression{expressionValueColumn()}}, []int64{1, 1, 1, 1}, nil},
		{"and", comparison("and", comparison("!=", expressionValueColumn(), expressionInt(0)),
			comparison(">", comparison("/", expressionInt(8), expressionValueColumn()), expressionInt(1))), []int64{0, 1, 0, 1}, []int{2}},
		{"or", comparison("or", comparison("=", expressionValueColumn(), expressionInt(0)),
			comparison(">", comparison("/", expressionInt(8), expressionValueColumn()), expressionInt(1))), []int64{1, 1, 0, 1}, []int{2}},
		{"not", &Expression{Op: "not", Args: []*Expression{comparison("=", expressionValueColumn(), expressionInt(0))}}, []int64{0, 1, 0, 1}, []int{2}},
		{"null", &Expression{Op: "is_null", Args: []*Expression{expressionValueColumn()}}, []int64{0, 0, 1, 0}, nil},
		{"not-null", &Expression{Op: "is_not_null", Args: []*Expression{expressionValueColumn()}}, []int64{1, 1, 0, 1}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, op := expressionFixture(t, expressionBoolProjection(test.expr), nil)
			batch, err := op.NextBatch(ctx)
			require.NoError(t, err)
			expressionAssertInts(t, batch, test.expected, test.nulls)
		})
	}
}

func TestExpressionSimpleCaseNumericPromotionAndArrayValues(t *testing.T) {
	ctx, op := expressionFixture(t, &Expression{Op: "simple_case", Args: []*Expression{
		expressionValueColumn(), expressionInt(0), expressionInt(10), expressionInt(1), expressionInt(11), expressionInt(12),
	}}, nil)
	batch, err := op.NextBatch(ctx)
	require.NoError(t, err)
	expressionAssertInts(t, batch, []int64{10, 11, 12, 12}, nil)
	floatLiteral := &Expression{Op: "literal", Type: ExpressionFloat, Literal: ExpressionScalar{Float64: 0.5}}
	ctx, op = expressionFixture(t, &Expression{Op: "+", Args: []*Expression{expressionValueColumn(), floatLiteral}}, nil)
	batch, err = op.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []float64{0.5, 1.5, 0, 3.5}, batch.Columns[0].(*vectorized.TypedColumn[float64]).Data())
	require.True(t, batch.Columns[0].IsNull(2))
	array := &Expression{Op: "literal", Type: ExpressionIntArray, Literal: ExpressionScalar{Ints: []int64{7, 9}}}
	ctx, op = expressionFixture(t, &Expression{Op: "coalesce", Args: []*Expression{expressionNullLiteral(), array}}, nil)
	batch, err = op.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, [][]int64{{7, 9}, {7, 9}, {7, 9}, {7, 9}}, batch.Columns[0].(*vectorized.TypedColumn[[]int64]).Data())
}

func TestExpressionCheckedNumericErrorsReleaseSourceAndBudget(t *testing.T) {
	floatLiteral := func(value float64) *Expression {
		return &Expression{Op: "literal", Type: ExpressionFloat, Literal: ExpressionScalar{Float64: value}}
	}
	stringLiteral := func(value string) *Expression {
		return &Expression{Op: "literal", Type: ExpressionString, Literal: ExpressionScalar{String: value}}
	}
	for _, test := range []struct {
		expr *Expression
		name string
	}{
		{&Expression{Op: "+", Args: []*Expression{expressionInt(math.MaxInt64), expressionInt(1)}}, "add"},
		{&Expression{Op: "-", Args: []*Expression{expressionInt(math.MinInt64), expressionInt(1)}}, "subtract"},
		{&Expression{Op: "*", Args: []*Expression{expressionInt(math.MinInt64), expressionInt(-1)}}, "multiply"},
		{&Expression{Op: "negate", Args: []*Expression{expressionInt(math.MinInt64)}}, "negate"},
		{&Expression{Op: "/", Args: []*Expression{expressionInt(math.MinInt64), expressionInt(-1)}}, "divide-overflow"},
		{&Expression{Op: "/", Args: []*Expression{expressionInt(1), expressionValueColumn()}}, "divide-zero"},
		{&Expression{Op: "%", Args: []*Expression{expressionInt(1), expressionValueColumn()}}, "remainder-zero"},
		{&Expression{Op: "cast", Type: ExpressionInt, Args: []*Expression{floatLiteral(0x1p63)}}, "double-range"},
		{&Expression{Op: "cast", Type: ExpressionInt, Args: []*Expression{floatLiteral(math.NaN())}}, "double-nan"},
		{&Expression{Op: "cast", Type: ExpressionInt, Args: []*Expression{floatLiteral(math.Inf(1))}}, "double-infinite"},
		{&Expression{Op: "cast", Type: ExpressionFloat, Args: []*Expression{stringLiteral("no-number")}}, "invalid-string"},
		{&Expression{Op: "cast", Type: ExpressionTimestamp, Args: []*Expression{stringLiteral("3000-01-01T00:00:00Z")}}, "timestamp-range"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, op := expressionFixture(t, test.expr, nil)
			batch, err := op.NextBatch(ctx)
			require.ErrorIs(t, err, ErrInvalidExpression)
			require.Nil(t, batch)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
	ctx, op := expressionFixture(t, &Expression{Op: "/", Args: []*Expression{expressionInt(-5), expressionInt(2)}}, nil)
	batch, err := op.NextBatch(ctx)
	require.NoError(t, err)
	expressionAssertInts(t, batch, []int64{-2, -2, -2, -2}, nil)
}

func TestExpressionTimestampCastsNormalizeUTCAndPreserveNanoseconds(t *testing.T) {
	timestamp := &Expression{Op: "cast", Type: ExpressionTimestamp, Args: []*Expression{
		{Op: "literal", Type: ExpressionString, Literal: ExpressionScalar{String: "1970-01-01T08:00:00.000000123+08:00"}},
	}}
	ctx, op := expressionFixture(t, &Expression{Op: "cast", Type: ExpressionInt, Args: []*Expression{timestamp}}, nil)
	batch, err := op.NextBatch(ctx)
	require.NoError(t, err)
	expressionAssertInts(t, batch, []int64{123, 123, 123, 123}, nil)
	ctx, op = expressionFixture(t, &Expression{Op: "cast", Type: ExpressionString, Args: []*Expression{timestamp}}, nil)
	batch, err = op.NextBatch(ctx)
	require.NoError(t, err)
	constant, ok := vectorized.AsConst(batch.Columns[0])
	require.True(t, ok)
	require.Equal(t, "1970-01-01T00:00:00.000000123Z", constant.Str())
	for _, nanos := range []int64{math.MinInt64, math.MaxInt64} {
		text := time.Unix(0, nanos).UTC().Format(time.RFC3339Nano)
		ctx, op = expressionFixture(t, &Expression{Op: "cast", Type: ExpressionTimestamp, Args: []*Expression{
			{Op: "literal", Type: ExpressionString, Literal: ExpressionScalar{String: text}},
		}}, nil)
		batch, err = op.NextBatch(ctx)
		require.NoError(t, err)
		expressionAssertInts(t, batch, []int64{nanos, nanos, nanos, nanos}, nil)
	}
}

func TestExpressionSelectionBoundsEmptyBatchesAndCleanup(t *testing.T) {
	for _, size := range []int{65536, 65537} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			source, _ := rawFixture()
			batch := source.batches[0]
			batch.Len = size
			for index, def := range batch.Schema.Columns {
				batch.Columns[index] = vectorized.NewConstNull(def.Type, size)
			}
			batch.Columns[4] = vectorized.NewConstInt64(2, size)
			source.batches = []*vectorized.RecordBatch{{Schema: source.schema, Columns: batch.Columns, Len: size, Selection: []uint16{}}, batch}
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), 8<<20)
			op, err := WrapExpressions(ctx, source, &ExpressionPlan{Outputs: []ExpressionOutput{{
				Expr: &Expression{Op: "+", Args: []*Expression{expressionValueColumn(), expressionInt(2)}},
			}}})
			require.NoError(t, err)
			result, err := op.NextBatch(ctx)
			if size == 65537 {
				require.ErrorIs(t, err, ErrInvalidExpression)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.Equal(t, size, result.Len)
				require.Equal(t, int64(4), result.Columns[0].(*vectorized.ConstColumn).Int64())
			}
			require.NoError(t, op.Close())
			require.Equal(t, 1, source.closes)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
	source, _ := rawFixture()
	ctx, cancel := context.WithCancel(vectorized.WithQueryMemoryTracker(t.Context(), 1<<20))
	op, err := WrapExpressions(ctx, source, &ExpressionPlan{})
	require.NoError(t, err)
	cancel()
	batch, err := op.NextBatch(ctx)
	require.Nil(t, batch)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
