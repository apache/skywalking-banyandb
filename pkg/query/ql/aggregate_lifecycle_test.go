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

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type aggregateCancelSource struct {
	*rawSource
	cancel context.CancelFunc
}

func (s *aggregateCancelSource) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	batch, err := s.rawSource.NextBatch(ctx)
	if s.pulls == 2 {
		s.cancel()
	}
	return batch, err
}

func TestAggregateCancellationAfterRetainingGroupsReleasesState(t *testing.T) {
	source, defs := aggregateUniqueKeysSource(65537)
	budget := vectorized.WithQueryMemoryTracker(t.Context(), 16<<20)
	ctx, cancel := context.WithCancel(budget)
	defer cancel()
	wrapped := &aggregateCancelSource{rawSource: source, cancel: cancel}
	op, err := WrapAggregate(ctx, wrapped, aggregateCountPlan(defs[:1], defs[1]))
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	batch, err := op.NextBatch(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, batch)
	stats, ok := AggregateStatistics(op)
	require.True(t, ok)
	require.EqualValues(t, 65536, stats.Groups)
	require.NoError(t, op.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(budget, 0).Used())
}

func TestAggregateIntegerSumOverflowAndFloatAverage(t *testing.T) {
	for _, function := range []AggregateFunction{AggregateSum, AggregateAvg} {
		source, defs := aggregateFixture(false)
		source.batches[0].Columns[1] = vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, []int64{math.MaxInt64, math.MaxInt64})
		source.batches[0].Len = 2
		call := AggregateCall{Source: defs[1], Function: function}
		plan := &AggregatePlan{Calls: []AggregateCall{call}, Outputs: []AggregateOutput{{Column: aggregateField("value", aggregateOutputType(call))}}}
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		op, err := WrapAggregate(ctx, source, plan)
		require.NoError(t, err)
		require.NoError(t, op.Init(ctx))
		batch, err := op.NextBatch(ctx)
		if function == AggregateSum {
			require.ErrorContains(t, err, "exceeds int64")
			require.Nil(t, batch)
		} else {
			require.NoError(t, err)
			require.Equal(t, float64(math.MaxInt64), batch.Columns[0].(*vectorized.TypedColumn[float64]).Data()[0])
		}
		require.NoError(t, op.Close())
		require.Equal(t, 1, source.closes)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}
