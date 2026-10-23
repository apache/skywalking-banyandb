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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestFilterMetricsOwnKernelReservationsAndReturnToBaseline(t *testing.T) {
	before := FilterExecutionStats()
	expressionBefore := ExpressionExecutionStats()
	source, _ := rawFixture()
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapFilter(ctx, source, &Expression{Op: ">", Args: []*Expression{expressionValueColumn(), expressionInt(0)}})
	require.NoError(t, err)
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint16{1, 3}, batch.Selection)
	live := FilterExecutionStats()
	stats, ok := FilterStatistics(operator)
	require.True(t, ok)
	require.EqualValues(t, 1, live.Executions-before.Executions)
	require.EqualValues(t, 1, live.Batches-before.Batches)
	require.EqualValues(t, 4, live.InputRows-before.InputRows)
	require.EqualValues(t, 2, live.OutputRows-before.OutputRows)
	require.EqualValues(t, 2, live.TrueRows-before.TrueRows)
	require.EqualValues(t, 1, live.FalseRows-before.FalseRows)
	require.EqualValues(t, 1, live.UnknownRows-before.UnknownRows)
	require.Equal(t, stats.SelectionBytes, live.SelectionBytes-before.SelectionBytes)
	require.Equal(t, stats.ReservedBytes, live.ReservedBytes-before.ReservedBytes)
	require.Zero(t, live.CopiedBytes-before.CopiedBytes)
	require.Greater(t, live.LiveBytes, before.LiveBytes)
	batch, err = operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Nil(t, batch)
	after := FilterExecutionStats()
	require.Equal(t, before.LiveBytes, after.LiveBytes)
	require.Equal(t, after.ReservedBytes-before.ReservedBytes, after.ReleasedBytes-before.ReleasedBytes)
	require.Equal(t, expressionBefore, ExpressionExecutionStats())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
