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

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestAggregateCountDistinctComposesWithCountAndSum(t *testing.T) {
	source, defs := aggregateFixture(false)
	source.batches[0].Columns[1] = vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, []int64{2, 2, 2, 4, 0, 0})
	plan := &AggregatePlan{GroupBy: defs[:1]}
	for i, function := range []AggregateFunction{AggregateCountDistinct, AggregateCount, AggregateSum, AggregateCountDistinct} {
		plan.Calls = append(plan.Calls, AggregateCall{Source: defs[1], Function: function})
		plan.Outputs = append(plan.Outputs, AggregateOutput{Column: aggregateField(string(rune('a'+i)), vectorized.ColumnTypeInt64), Index: i})
	}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := WrapAggregate(ctx, source, plan)
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	batch, err := op.NextBatch(ctx)
	require.NoError(t, err)
	for i, expected := range [][]int64{{1, 2, 1, 1}, {2, 2, 1, 1}, {4, 6, 0, 0}, {1, 2, 1, 1}} {
		require.Equal(t, expected, batch.Columns[i].(*vectorized.TypedColumn[int64]).Data())
	}
	require.NoError(t, op.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func countDistinctPlan(def vectorized.ColumnDef) *AggregatePlan {
	return &AggregatePlan{
		Calls:   []AggregateCall{{Source: def, Function: AggregateCountDistinct}},
		Outputs: []AggregateOutput{{Column: aggregateField("distinct", vectorized.ColumnTypeInt64)}},
	}
}

func TestAggregateCountDistinctScalarTypesAndNulls(t *testing.T) {
	for _, tc := range []struct {
		column vectorized.Column
		name   string
		typ    vectorized.ColumnType
		want   int64
	}{
		{vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, []int64{0, 0, 1, 1, 2}), "int", vectorized.ColumnTypeInt64, 3},
		{
			vectorized.NewColumnFromSlice(vectorized.ColumnTypeFloat64, []float64{0, math.Copysign(0, -1), math.NaN(), math.Float64frombits(0x7ff8000000000001), 1}),
			"float", vectorized.ColumnTypeFloat64, 3,
		},
		{vectorized.NewColumnFromSlice(vectorized.ColumnTypeString, []string{"", "", "a", "a", "b"}), "string", vectorized.ColumnTypeString, 3},
		{vectorized.NewColumnFromSlice(vectorized.ColumnTypeBytes, [][]byte{nil, {}, {'a'}, {'a'}, {'b'}}), "bytes", vectorized.ColumnTypeBytes, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			def := aggregateField("value", tc.typ)
			schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{def})
			tc.column.AppendNull()
			batch := vectorized.NewRecordBatch(schema, 0)
			batch.Columns = []vectorized.Column{tc.column}
			batch.Len = 6
			// Repeating the batch must not increase the count; NULL never contributes.
			source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch, batch}}
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
			op, err := WrapAggregate(ctx, source, countDistinctPlan(def))
			require.NoError(t, err)
			require.NoError(t, op.Init(ctx))
			result, err := op.NextBatch(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.want, result.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0])
			require.NoError(t, op.Close())
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
}

func TestAggregateCountDistinctEmptyAndSelectedNulls(t *testing.T) {
	for _, empty := range []bool{false, true} {
		source, defs := aggregateFixture(false)
		if empty {
			source.batches = nil
		} else {
			source.batches[0].Selection = []uint16{3, 5}
		}
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		op, err := WrapAggregate(ctx, source, countDistinctPlan(defs[1]))
		require.NoError(t, err)
		require.NoError(t, op.Init(ctx))
		batch, err := op.NextBatch(ctx)
		require.NoError(t, err)
		require.False(t, batch.Columns[0].IsNull(0))
		require.Equal(t, int64(0), batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0])
		require.NoError(t, op.Close())
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}

func TestAggregateCountDistinctBudgetAndCancellationRelease(t *testing.T) {
	for _, cancelQuery := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelQuery), func(t *testing.T) {
			source, defs := aggregateUniqueKeysSource(65537)
			budget := int64(32 << 10)
			if cancelQuery {
				budget = 16 << 20
			}
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), budget)
			var input vectorized.PullOperator = source
			if cancelQuery {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				input = &aggregateCancelSource{rawSource: source, cancel: cancel}
			}
			op, err := WrapAggregate(ctx, input, countDistinctPlan(defs[0]))
			require.NoError(t, err)
			require.NoError(t, op.Init(ctx))
			batch, err := op.NextBatch(ctx)
			if cancelQuery {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
			}
			require.Nil(t, batch)
			require.NoError(t, op.Close())
			require.Equal(t, 1, source.closes)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
}

// The second pull changes a borrowed native decode buffer before presenting the
// same column again. Distinct must retain the first value independently.
type distinctReusingSource struct {
	*rawSource
	data []byte
}

func (s *distinctReusingSource) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if s.pulls == 1 {
		copy(s.data, "two")
	}
	return s.rawSource.NextBatch(ctx)
}

func TestAggregateCountDistinctOwnsBorrowedBytes(t *testing.T) {
	def := aggregateField("value", vectorized.ColumnTypeBytes)
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{def})
	data := []byte("one")
	batch := vectorized.NewRecordBatch(schema, 0)
	batch.Columns = []vectorized.Column{vectorized.NewColumnFromSlice(vectorized.ColumnTypeBytes, [][]byte{data})}
	batch.Len = 1
	source := &distinctReusingSource{rawSource: &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch, batch}}, data: data}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := WrapAggregate(ctx, source, countDistinctPlan(def))
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	result, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0])
	stats, ok := AggregateStatistics(op)
	require.True(t, ok)
	require.EqualValues(t, 2, stats.DistinctKeys)
	require.Positive(t, stats.DistinctKeyBytes)
	require.GreaterOrEqual(t, stats.DistinctKeyCapacity, stats.DistinctKeyBytes)
	require.NoError(t, op.Close())
	closedStats, ok := AggregateStatistics(op)
	require.True(t, ok)
	require.Equal(t, stats, closedStats)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
