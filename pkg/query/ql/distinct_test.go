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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestDistinctCancellationAndBudgetFailureReleaseEveryReservation(t *testing.T) {
	for _, cancelScan := range []bool{false, true} {
		t.Run(map[bool]string{false: "budget", true: "cancellation"}[cancelScan], func(t *testing.T) {
			source, defs := aggregateUniqueKeysSource(65537)
			budget := vectorized.WithQueryMemoryTracker(t.Context(), 16<<20)
			ctx, cancel := context.WithCancel(budget)
			defer cancel()
			var input vectorized.PullOperator = source
			if cancelScan {
				input = &aggregateCancelSource{rawSource: source, cancel: cancel}
			}
			operator, err := WrapRaw(ctx, input, &RawPlan{Distinct: true, Slots: []Slot{{Source: defs[0], Output: defs[0]}}})
			require.NoError(t, err)
			require.NoError(t, operator.Init(ctx))
			tracker := vectorized.QueryMemoryTracker(ctx, 0)
			var held int64
			if !cancelScan {
				held = (16 << 20) - tracker.Used() - 256
				require.NoError(t, tracker.Reserve(held))
			}
			batch, err := operator.NextBatch(ctx)
			require.Nil(t, batch)
			if cancelScan {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
			}
			require.NoError(t, operator.Close())
			tracker.Release(held)
			require.Zero(t, tracker.Used())
			require.Equal(t, 1, source.closes)
			stats, ok := DistinctStatistics(operator)
			require.True(t, ok)
			require.Zero(t, stats.StateLanes)
			if cancelScan {
				require.EqualValues(t, 65536, stats.Groups)
			}
		})
	}
}

type distinctErrorSource struct {
	*rawSource
	failure error
}

func (s *distinctErrorSource) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if s.pulls > 0 {
		return nil, s.failure
	}
	return s.rawSource.NextBatch(ctx)
}

func TestDistinctUpstreamErrorAndEarlyCloseReleaseState(t *testing.T) {
	for _, earlyClose := range []bool{false, true} {
		source, slots := rawFixture()
		failure := errors.New("native read failed")
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		operator, err := WrapRaw(ctx, &distinctErrorSource{rawSource: source, failure: failure}, &RawPlan{Distinct: true, Slots: slots[1:2]})
		require.NoError(t, err)
		require.NoError(t, operator.Init(ctx))
		if !earlyClose {
			_, err = operator.NextBatch(ctx)
			require.ErrorIs(t, err, failure)
		}
		require.NoError(t, operator.Close())
		require.NoError(t, operator.Close())
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		require.Equal(t, 1, source.closes)
	}
}

func TestDistinctPreservesGlobalAggregateOnEmptyInput(t *testing.T) {
	source, slots := rawFixture()
	output := vectorized.ColumnDef{Name: "count", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapRaw(ctx, source, &RawPlan{Distinct: true, Empty: true, Slots: []Slot{{Source: slots[1].Source, Output: slots[1].Source}}, Aggregate: &AggregatePlan{
		Calls: []AggregateCall{{Source: slots[1].Source, Function: AggregateCount}}, Outputs: []AggregateOutput{{Column: output}},
	}})
	require.NoError(t, err)
	require.NoError(t, operator.Init(ctx))
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Equal(t, []int64{0}, batch.Columns[0].(*vectorized.TypedColumn[int64]).Data())
	require.NoError(t, operator.Close())
	require.Zero(t, source.pulls)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
