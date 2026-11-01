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

package bydbql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestComputedGroupingCountsUniqueKeyCellsAfterSelection(t *testing.T) {
	expression := "CASE WHEN v >= 4 THEN 'high' ELSE 'low' END"
	var previous ql.ExpressionStats
	for _, group := range []string{expression + ", " + expression, "1,1"} {
		execution := typedExecution(t, "SELECT "+expression+" AS band, SUM(v) AS total FROM g.m WHERE v > 0 GROUP BY "+group)
		before := ql.GroupExpressionExecutionStats()
		beforeProjection := ql.ExpressionExecutionStats()
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		source := computedGroupFixture(execution.Plan.Slots)
		op, err := ql.WrapRaw(ctx, source, execution.Plan)
		require.NoError(t, err)
		require.NoError(t, op.Init(ctx))
		batch, err := op.NextBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, batch.ActiveLen())
		require.NoError(t, op.Close())
		after := ql.GroupExpressionExecutionStats()
		work := ql.ExpressionStats{
			InputRows: after.InputRows - before.InputRows, KeyEvaluations: after.KeyEvaluations - before.KeyEvaluations,
			Batches: after.Batches - before.Batches, VectorRows: after.VectorRows - before.VectorRows,
		}
		require.EqualValues(t, 3, work.InputRows)
		require.EqualValues(t, 3, work.KeyEvaluations)
		require.EqualValues(t, 1, work.Batches)
		require.Equal(t, before.LiveBytes, after.LiveBytes)
		require.Equal(t, after.ReservedBytes-before.ReservedBytes, after.ReleasedBytes-before.ReleasedBytes)
		require.Equal(t, beforeProjection, ql.ExpressionExecutionStats())
		if previous.InputRows != 0 {
			require.Equal(t, previous, work)
		}
		previous = work
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}
