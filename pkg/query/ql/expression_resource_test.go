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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func expressionResourceFixture(rows int) *rawSource {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}})
	batch := vectorized.NewRecordBatch(schema, rows)
	column := batch.Columns[0].(*vectorized.TypedColumn[int64])
	for row := 0; row < rows; row++ {
		column.Append(int64(row % 17))
	}
	batch.Len = rows
	return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
}

func TestExpressionRealQueryBudgetsCoverWideProjectionAndCancellation(t *testing.T) {
	const rows = 65536
	for _, wrapped := range []bool{false, true} {
		for _, test := range []struct {
			name      string
			width     int
			memoryMiB int64
			reject    bool
			cancel    bool
		}{
			{name: "narrow16", width: 2, memoryMiB: 16},
			{name: "wide16", width: 32, memoryMiB: 16, reject: true},
			{name: "wide256", width: 32, memoryMiB: 256},
			{name: "cancel256", width: 32, memoryMiB: 256, cancel: true},
		} {
			t.Run(fmt.Sprintf("raw=%t/%s", wrapped, test.name), func(t *testing.T) {
				ctx, cancel := context.WithCancel(vectorized.WithQueryMemoryTracker(t.Context(), test.memoryMiB<<20))
				defer cancel()
				source := expressionResourceFixture(rows)
				plan := &ExpressionPlan{Outputs: make([]ExpressionOutput, test.width)}
				for index := range plan.Outputs {
					plan.Outputs[index] = ExpressionOutput{
						Expr:   &Expression{Op: "+", Args: []*Expression{expressionValueColumn(), expressionInt(int64(index))}},
						Column: vectorized.ColumnDef{Name: fmt.Sprintf("result_%d", index), Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
					}
				}
				var operator vectorized.PullOperator
				var err error
				if wrapped {
					input := source.schema.Columns[0]
					operator, err = WrapRaw(ctx, source, &RawPlan{Expressions: plan, Slots: []Slot{{Source: input, Output: input}}})
				} else {
					operator, err = WrapExpressions(ctx, source, plan)
				}
				require.NoError(t, err)
				defer func() { require.NoError(t, operator.Close()) }()
				require.NoError(t, operator.Init(ctx))
				result, err := operator.NextBatch(ctx)
				if test.reject {
					require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
					require.Nil(t, result)
					if stats, ok := ExpressionStatistics(operator); ok {
						require.Positive(t, stats.FullColumnAllocations)
						require.Less(t, stats.FullColumnAllocations, uint64(test.width))
					}
				} else {
					require.NoError(t, err)
					require.Equal(t, rows, result.Len)
					require.Equal(t, test.width, len(result.Columns))
					for index, output := range result.Columns {
						values := output.(*vectorized.TypedColumn[int64]).Data()
						for row, value := range values {
							expected := int64(row%17) + int64(index)
							if value != expected || output.IsNull(row) {
								t.Fatalf("output %d row %d got %d expected %d", index, row, value, expected)
							}
						}
					}
					require.Greater(t, vectorized.QueryMemoryTracker(ctx, 0).Used(), int64(rows*test.width*8))
					if test.cancel {
						cancel()
						result, err = operator.NextBatch(ctx)
						require.ErrorIs(t, err, context.Canceled)
						require.Nil(t, result)
					}
				}
				require.NoError(t, operator.Close())
				require.NoError(t, operator.Close())
				require.Equal(t, 1, source.closes)
				require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
				if stats, ok := ExpressionStatistics(operator); ok {
					require.Zero(t, stats.LiveBytes)
					require.Equal(t, stats.ReservedBytes, stats.ReleasedBytes)
				}
			})
		}
	}
}

func TestExpressionArrayConstantSharesPayloadAndChargesFinalProjectionHeaders(t *testing.T) {
	const rows = 65536
	payload := make([]int64, 8192)
	payload[0], payload[len(payload)-1] = 11, 73
	source := expressionResourceFixture(rows)
	// Every conditional is constant. Its payload stays scalar until projection must
	// supply typed array columns to the existing serializer, which lacks array constants.
	literal := &Expression{Op: "literal", Type: ExpressionIntArray, Literal: ExpressionScalar{Ints: payload}}
	expression := &Expression{Op: "coalesce", Args: []*Expression{
		{Op: "case", Args: []*Expression{{Op: "literal", Type: ExpressionBool, Literal: ExpressionScalar{Bool: true}}, literal, expressionNullLiteral()}},
		{Op: "literal", Type: ExpressionIntArray, Literal: ExpressionScalar{Ints: []int64{99}}},
	}}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 16<<20)
	operator, err := WrapExpressions(ctx, source, &ExpressionPlan{Outputs: []ExpressionOutput{{
		Expr: expression, Column: vectorized.ColumnDef{Name: "result", TagFamily: "__ql", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeInt64Array},
	}}})
	require.NoError(t, err)
	defer func() { require.NoError(t, operator.Close()) }()
	payload[0] = 1000
	result, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	values := result.Columns[0].(*vectorized.TypedColumn[[]int64]).Data()
	require.Len(t, values, rows)
	for _, row := range []int{0, 1, rows / 2, rows - 1} {
		require.Len(t, values[row], len(payload))
		require.Equal(t, int64(11), values[row][0])
		require.Equal(t, int64(73), values[row][len(payload)-1])
		require.Same(t, &values[0][0], &values[row][0])
	}
	require.NotSame(t, &payload[0], &values[0][0])
	stats, ok := ExpressionStatistics(operator)
	require.True(t, ok)
	require.Equal(t, uint64(1), stats.FullColumnAllocations)
	require.GreaterOrEqual(t, stats.BufferBytes, uint64(rows*24))
	require.GreaterOrEqual(t, stats.LiveBytes, int64(rows*24+len(payload)*8))
	require.Less(t, stats.LiveBytes, int64(4<<20))
	require.Zero(t, stats.LaterArmRows)
	require.NoError(t, operator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
