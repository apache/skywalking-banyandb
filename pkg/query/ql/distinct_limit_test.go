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

func TestDistinctLimitClosesSourceWithoutAnotherPullAndOwnsResponse(t *testing.T) {
	source, slots := rawFixture()
	limit := uint32(1)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	input := &distinctErrorSource{rawSource: source, failure: errors.New("must not pull after required unique")}
	operator, err := WrapRaw(ctx, input, &RawPlan{Distinct: true, Slots: slots[:1], Limit: &limit})
	require.NoError(t, err)
	iterator, err := RawIterator(ctx, operator)
	require.NoError(t, err)
	require.True(t, iterator.Next())
	point := iterator.Current()[0].DataPoint
	require.Equal(t, 1, source.pulls)
	require.Equal(t, 1, source.closes)
	require.Equal(t, []byte("xxxxx"), source.poison)
	require.False(t, iterator.Next())
	require.NoError(t, iterator.Close())
	require.NoError(t, iterator.Close())
	require.Equal(t, []byte("first"), point.TagFamilies[0].Tags[0].Value.GetBinaryData())
	require.Equal(t, 1, source.pulls)
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestDistinctLimitStopsWithinActiveSelection(t *testing.T) {
	source, slots := rawFixture()
	source.batches[0].Selection = []uint16{3, 1, 0}
	limit := uint32(1)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapRaw(ctx, source, &RawPlan{Distinct: true, Slots: slots[1:2], Limit: &limit, Offset: 1})
	require.NoError(t, err)
	require.NoError(t, operator.Init(ctx))
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint16{1}, batch.Selection)
	require.Equal(t, []int64{3, 1}, batch.Columns[0].(*vectorized.TypedColumn[int64]).Data())
	stats, ok := DistinctStatistics(operator)
	require.True(t, ok)
	require.EqualValues(t, 2, stats.InputRows)
	require.EqualValues(t, 2, stats.Groups)
	require.Equal(t, 1, source.pulls)
	require.Equal(t, 1, source.closes)
	require.NoError(t, operator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestDistinctLimitPropagatesFailureBeforeRequiredUniques(t *testing.T) {
	for _, failureKind := range []string{"source", "budget", "cancellation"} {
		t.Run(failureKind, func(t *testing.T) {
			source, defs := aggregateUniqueKeysSource(65537)
			ctx, cancel := context.WithCancel(vectorized.WithQueryMemoryTracker(t.Context(), 16<<20))
			defer cancel()
			tracker := vectorized.QueryMemoryTracker(ctx, 0)
			failure := errors.New("native read failed before enough unique rows")
			var input vectorized.PullOperator = source
			switch failureKind {
			case "source":
				input = &distinctErrorSource{rawSource: source, failure: failure}
			case "cancellation":
				input = &aggregateCancelSource{rawSource: source, cancel: cancel}
				failure = context.Canceled
			case "budget":
				failure = vectorized.ErrMemoryBudgetExceeded
			}
			limit := uint32(65537)
			operator, err := WrapRaw(ctx, input, &RawPlan{Distinct: true, Slots: []Slot{{Source: defs[0], Output: defs[0]}}, Limit: &limit})
			require.NoError(t, err)
			require.NoError(t, operator.Init(ctx))
			var held int64
			if failureKind == "budget" {
				held = (16 << 20) - tracker.Used() - 256
				require.NoError(t, tracker.Reserve(held))
			}
			batch, err := operator.NextBatch(ctx)
			require.Nil(t, batch)
			require.ErrorIs(t, err, failure)
			require.NoError(t, operator.Close())
			tracker.Release(held)
			require.Zero(t, tracker.Used())
			require.Equal(t, 1, source.closes)
		})
	}
}

type distinctCloseErrorSource struct {
	*rawSource
	failure error
}

func (s *distinctCloseErrorSource) Close() error {
	_ = s.rawSource.Close()
	return s.failure
}

func TestDistinctLimitPropagatesSourceCloseFailureAndReleasesState(t *testing.T) {
	source, slots := rawFixture()
	failure := errors.New("source close failed")
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	limit := uint32(1)
	operator, err := WrapRaw(ctx, &distinctCloseErrorSource{rawSource: source, failure: failure}, &RawPlan{Distinct: true, Slots: slots[:1], Limit: &limit})
	require.NoError(t, err)
	require.NoError(t, operator.Init(ctx))
	batch, err := operator.NextBatch(ctx)
	require.Nil(t, batch)
	require.ErrorIs(t, err, failure)
	require.NoError(t, operator.Close())
	require.Equal(t, 1, source.pulls)
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
