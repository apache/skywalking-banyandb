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

func TestAggregateExpressionFinalizesBeforeCoalesce(t *testing.T) {
	for _, empty := range []bool{false, true} {
		source, defs := aggregateFixture(false)
		if empty {
			source.batches = nil
		}
		sum := aggregateField("hidden_sum", vectorized.ColumnTypeInt64)
		result := aggregateField("total", vectorized.ColumnTypeInt64)
		expression, err := ResolveExpression(&Expression{Op: "coalesce", Args: []*Expression{{Op: "column", Source: sum}, expressionInt(0)}})
		require.NoError(t, err)
		plan := &RawPlan{Slots: []Slot{{Source: defs[1], Output: defs[1]}}, Aggregate: &AggregatePlan{
			Calls: []AggregateCall{{Source: defs[1], Function: AggregateSum}}, Outputs: []AggregateOutput{{Column: sum, Index: 0}},
		}, Expressions: &ExpressionPlan{Outputs: []ExpressionOutput{{Expr: expression, Column: result}}}}
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		op, err := WrapRaw(ctx, source, plan)
		require.NoError(t, err)
		require.NoError(t, op.Init(ctx))
		batch, err := op.NextBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, batch.Len)
		want := int64(12)
		if empty {
			want = 0
		}
		if constant, ok := vectorized.AsConst(batch.Columns[0]); ok {
			require.Equal(t, want, constant.Int64())
		} else {
			require.Equal(t, want, batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0])
		}
		require.NoError(t, op.Close())
	}
}

func TestAggregateCompositionReleasesOnCancellationAndBudgetFailure(t *testing.T) {
	for _, budget := range []int64{1, 1 << 20} {
		source, defs := aggregateFixture(false)
		input := aggregateField("computed", vectorized.ColumnTypeInt64)
		sum := aggregateField("sum", vectorized.ColumnTypeInt64)
		result := aggregateField("result", vectorized.ColumnTypeInt64)
		plan := &RawPlan{
			Slots: []Slot{{Source: defs[1], Output: defs[1]}},
			Aggregate: &AggregatePlan{
				GroupExpressions: &ExpressionPlan{Outputs: []ExpressionOutput{{
					Expr: &Expression{Op: "+", Args: []*Expression{{Op: "column", Source: defs[1]}, expressionInt(1)}}, Column: input,
				}}},
				Calls: []AggregateCall{{Source: input, Function: AggregateSum}}, Outputs: []AggregateOutput{{Column: sum, Index: 0}},
			},
			Expressions: &ExpressionPlan{Outputs: []ExpressionOutput{{
				Expr: &Expression{Op: "coalesce", Args: []*Expression{{Op: "column", Source: sum}, expressionInt(0)}}, Column: result,
			}}},
		}
		ctx, cancel := context.WithCancel(vectorized.WithQueryMemoryTracker(t.Context(), budget))
		op, err := WrapRaw(ctx, source, plan)
		if budget == 1 {
			require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
		} else {
			require.NoError(t, err)
			require.NoError(t, op.Init(ctx))
			cancel()
			_, err = op.NextBatch(ctx)
			require.ErrorIs(t, err, context.Canceled)
			require.NoError(t, op.Close())
		}
		cancel()
		require.Equal(t, 1, source.closes)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}
