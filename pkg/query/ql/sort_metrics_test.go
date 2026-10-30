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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestSortCountersObserveActualKeyCallsAndOutputPeak(t *testing.T) {
	source, outputs, keys := sortRuntimeFixture()
	source.batches[0].Selection = []uint16{5, 2, 4, 1}
	limit := uint32(2)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	before := SortExecutionStats()
	operator, err := WrapSort(ctx, source, &SortPlan{
		Keys:    []SortKey{{Expr: &Expression{Op: "+", Args: []*Expression{keys[0], expressionInt(1)}}}, {Expr: keys[1]}},
		Outputs: outputs, Limit: &limit, Offset: 1,
	})
	require.NoError(t, err)
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 5}, batch.Columns[1].(*vectorized.TypedColumn[int64]).Data())
	stats, ok := SortStatistics(operator)
	require.True(t, ok)
	after := SortExecutionStats()
	require.Equal(t, uint64(4), after.InputRows-before.InputRows)
	require.Equal(t, uint64(8), after.KeyEvaluations-before.KeyEvaluations)
	require.Equal(t, uint64(2), after.KeyRootCalls-before.KeyRootCalls)
	require.Equal(t, uint64(4), after.KeyKernelRows-before.KeyKernelRows)
	require.Zero(t, after.KeyScalarEvaluations-before.KeyScalarEvaluations)
	require.Equal(t, stats.PeakBytes, after.PeakBytes-before.PeakBytes)
	require.Equal(t, stats.Comparisons, after.Comparisons-before.Comparisons)
	require.NoError(t, operator.Close())
	after = SortExecutionStats()
	require.Equal(t, after.ReservedBytes-before.ReservedBytes, after.ReleasedBytes-before.ReleasedBytes)
	require.Equal(t, before.LiveBytes, after.LiveBytes)
}

func TestSortInstrumentedFullSortAndCopiedBatchCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(vectorized.WithQueryMemoryTracker(t.Context(), 1<<20))
	defer cancel()
	source, outputs, keys := sortRuntimeFixture()
	limit := uint32(1)
	ctx = WithFullSort(ctx)
	restore := SetSortExecutionHookForTest(func(_ context.Context, stats SortStats) error {
		require.True(t, stats.FullSort)
		require.Equal(t, uint64(6), stats.Candidates)
		require.Zero(t, stats.SourcePins)
		cancel()
		return ctx.Err()
	})
	defer restore()
	operator, err := WrapSort(ctx, source, &SortPlan{Keys: []SortKey{{Expr: keys[0]}}, Outputs: outputs, Limit: &limit})
	require.NoError(t, err)
	batch, err := operator.NextBatch(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, batch)
	require.NoError(t, operator.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
