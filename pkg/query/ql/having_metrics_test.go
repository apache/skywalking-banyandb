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

func TestHavingMetricsDoNotCountRawFilterRows(t *testing.T) {
	before := FilterExecutionStats()
	havingBefore := HavingExecutionStats()
	source, plan, sum := havingFixture()
	plan.Having = &HavingPlan{
		Predicate: &Expression{Op: ">", Args: []*Expression{{Op: "column", Source: sum}, expressionInt(3)}},
		Outputs:   []Slot{{Source: sum, Output: sum}},
	}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapRaw(ctx, source, plan)
	require.NoError(t, err)
	require.NoError(t, operator.Init(ctx))
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, batch.ActiveLen())
	require.NoError(t, operator.Close())
	require.Equal(t, before, FilterExecutionStats(), "completed groups must not be attributed to the raw WHERE filter")
	havingAfter := HavingExecutionStats()
	require.EqualValues(t, 1, havingAfter.Executions-havingBefore.Executions)
	require.EqualValues(t, 4, havingAfter.InputRows-havingBefore.InputRows)
	require.EqualValues(t, 2, havingAfter.OutputRows-havingBefore.OutputRows)
	require.EqualValues(t, 1, havingAfter.UnknownRows-havingBefore.UnknownRows)
	require.Equal(t, havingBefore.LiveBytes, havingAfter.LiveBytes)
	require.Equal(t, havingAfter.ReservedBytes-havingBefore.ReservedBytes, havingAfter.ReleasedBytes-havingBefore.ReleasedBytes)
	stats, ok := HavingStatistics(operator)
	require.True(t, ok)
	require.EqualValues(t, 4, stats.InputRows)
	require.Zero(t, stats.LiveBytes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
