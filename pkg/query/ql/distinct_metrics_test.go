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

func TestDistinctMetricsAreSeparateAndReleaseOnClose(t *testing.T) {
	before := DistinctExecutionStats()
	aggregateBefore := AggregateExecutionStats()
	source, defs := aggregateFixture(false)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapRaw(ctx, source, &RawPlan{Distinct: true, Slots: []Slot{{Source: defs[0], Output: defs[0]}}})
	require.NoError(t, err)
	require.NoError(t, operator.Init(ctx))
	_, err = operator.NextBatch(ctx)
	require.NoError(t, err)
	stats, ok := DistinctStatistics(operator)
	require.True(t, ok)
	live := DistinctExecutionStats()
	require.EqualValues(t, 1, live.Executions-before.Executions)
	require.EqualValues(t, 6, live.InputRows-before.InputRows)
	require.EqualValues(t, 4, live.Groups-before.Groups)
	require.Equal(t, stats.KeyBytes, live.KeyBytes-before.KeyBytes)
	require.Zero(t, live.StateLanes-before.StateLanes)
	require.Zero(t, live.StateCapacity-before.StateCapacity)
	require.Greater(t, live.LiveBytes, before.LiveBytes)
	require.Equal(t, aggregateBefore, AggregateExecutionStats())
	require.NoError(t, operator.Close())
	after := DistinctExecutionStats()
	require.Equal(t, before.LiveBytes, after.LiveBytes)
	require.Equal(t, after.ReservedBytes-before.ReservedBytes, after.ReleasedBytes-before.ReleasedBytes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	require.Equal(t, aggregateBefore, AggregateExecutionStats())
}

func TestDistinctLimitMetricsCountOnlyExaminedRowsAndRetainedKeys(t *testing.T) {
	before := DistinctExecutionStats()
	aggregateBefore := AggregateExecutionStats()
	source, slots := rawFixture()
	limit := uint32(1)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapRaw(ctx, source, &RawPlan{Distinct: true, Slots: slots[1:2], Limit: &limit, Offset: 1})
	require.NoError(t, err)
	require.NoError(t, operator.Init(ctx))
	_, err = operator.NextBatch(ctx)
	require.NoError(t, err)
	live := DistinctExecutionStats()
	require.EqualValues(t, 1, live.Executions-before.Executions)
	require.EqualValues(t, 2, live.InputRows-before.InputRows)
	require.EqualValues(t, 2, live.Groups-before.Groups)
	require.Zero(t, live.StateLanes-before.StateLanes)
	require.Zero(t, live.StateCapacity-before.StateCapacity)
	require.Equal(t, aggregateBefore, AggregateExecutionStats())
	require.NoError(t, operator.Close())
	after := DistinctExecutionStats()
	require.Equal(t, before.LiveBytes, after.LiveBytes)
	require.Equal(t, after.ReservedBytes-before.ReservedBytes, after.ReleasedBytes-before.ReleasedBytes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
