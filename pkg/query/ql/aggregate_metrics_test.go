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

func TestAggregateMetricsCaptureCompletedScanAndRelease(t *testing.T) {
	before := AggregateExecutionStats()
	source, defs := aggregateFixture(false)
	plan := aggregateCallsPlan(defs)
	plan.GroupBy = defs[:1]
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := WrapAggregate(ctx, source, plan)
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	_, err = op.NextBatch(ctx)
	require.NoError(t, err)
	stats, ok := AggregateStatistics(op)
	require.True(t, ok)
	live := AggregateExecutionStats()
	require.EqualValues(t, 1, live.Executions-before.Executions)
	require.EqualValues(t, 6, live.InputRows-before.InputRows)
	require.EqualValues(t, 4, live.Groups-before.Groups)
	require.Equal(t, stats.StateBytes, live.StateBytes-before.StateBytes)
	require.Equal(t, stats.StateCapacity, live.StateCapacity-before.StateCapacity)
	require.Equal(t, stats.KeyBytes, live.KeyBytes-before.KeyBytes)
	require.Equal(t, stats.KeyCapacity, live.KeyCapacity-before.KeyCapacity)
	require.Greater(t, live.LiveBytes, before.LiveBytes)
	require.NoError(t, op.Close())
	after := AggregateExecutionStats()
	require.Equal(t, before.LiveBytes, after.LiveBytes)
	require.Equal(t, after.ReservedBytes-before.ReservedBytes, after.ReleasedBytes-before.ReleasedBytes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
