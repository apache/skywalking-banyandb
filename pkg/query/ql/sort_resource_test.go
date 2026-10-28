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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func sortWideRuntimeFixture(rows int) (*rawSource, *SortPlan) {
	defs := []vectorized.ColumnDef{
		{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
		{Name: "payload", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeBytes},
	}
	schema := vectorized.NewBatchSchema(defs)
	values := vectorized.NewColumnForType(vectorized.ColumnTypeInt64, rows).(*vectorized.TypedColumn[int64])
	for row := 0; row < rows; row++ {
		values.Append(int64(rows - row))
	}
	payload := make([]byte, 4096)
	copy(payload, "owned")
	batch := &vectorized.RecordBatch{
		Schema: schema, Columns: []vectorized.Column{values, vectorized.NewConstBytes(payload, rows)}, Len: rows,
	}
	return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}, poison: payload}, &SortPlan{
		Keys:    []SortKey{{Expr: &Expression{Op: "column", Source: defs[0]}}},
		Outputs: []Slot{{Source: defs[0], Output: defs[0]}, {Source: defs[1], Output: defs[1]}},
	}
}

func TestSortBudgetExhaustionReleasesOwnedCandidatesAndRecovers(t *testing.T) {
	for _, fullSort := range []bool{false, true} {
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 128<<10)
		source, plan := sortWideRuntimeFixture(512)
		limit := uint32(64)
		plan.Limit, plan.FullSort = &limit, fullSort
		operator, err := WrapSort(ctx, source, plan)
		require.NoError(t, err)
		batch, err := operator.NextBatch(ctx)
		require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
		require.Nil(t, batch)
		stats, ok := SortStatistics(operator)
		require.True(t, ok)
		require.Positive(t, stats.AcceptedRows)
		require.Zero(t, stats.LiveBytes)
		require.Equal(t, stats.ReservedBytes, stats.ReleasedBytes)
		require.NoError(t, operator.Close())
		require.Equal(t, 1, source.closes)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())

		source, plan = sortWideRuntimeFixture(512)
		limit = 4
		plan.Limit = &limit
		operator, err = WrapSort(ctx, source, plan)
		require.NoError(t, err)
		batch, err = operator.NextBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, []int64{1, 2, 3, 4}, batch.Columns[0].(*vectorized.TypedColumn[int64]).Data())
		for _, payload := range batch.Columns[1].(*vectorized.TypedColumn[[]byte]).Data() {
			require.Equal(t, []byte("owned"), payload[:5], "source poisoning on EOF/Close must not change copied candidates")
		}
		stats, ok = SortStatistics(operator)
		require.True(t, ok)
		require.Equal(t, uint64(4), stats.Candidates)
		require.Positive(t, stats.EvictedRows)
		require.LessOrEqual(t, stats.PayloadCapacity, 2*stats.PayloadBytes)
		require.Zero(t, stats.SourcePins)
		require.NoError(t, operator.Close())
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}

func TestSortZeroLimitAndWideOffsetDoNotWrap(t *testing.T) {
	for _, fullSort := range []bool{false, true} {
		source, outputs, keys := sortRuntimeFixture()
		limit := uint32(0)
		plan := &SortPlan{Keys: []SortKey{{Expr: keys[0]}}, Outputs: outputs, Limit: &limit, FullSort: fullSort}
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		operator, err := WrapSort(ctx, source, plan)
		require.NoError(t, err)
		require.NoError(t, operator.Init(ctx))
		batch, err := operator.NextBatch(ctx)
		require.NoError(t, err)
		require.Nil(t, batch)
		require.Zero(t, source.inits)
		require.Zero(t, source.pulls)
		require.NoError(t, operator.Close())
		require.Equal(t, 1, source.closes)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())

		source, outputs, keys = sortRuntimeFixture()
		limit = math.MaxUint32
		operator, err = WrapSort(ctx, source, &SortPlan{
			Keys: []SortKey{{Expr: keys[0]}}, Outputs: outputs, Limit: &limit, Offset: math.MaxUint32, FullSort: fullSort,
		})
		require.NoError(t, err)
		batch, err = operator.NextBatch(ctx)
		require.NoError(t, err)
		require.Nil(t, batch)
		stats, ok := SortStatistics(operator)
		require.True(t, ok)
		require.Equal(t, uint64(8589934590), stats.K)
		require.Equal(t, uint64(6), stats.Candidates)
		require.NoError(t, operator.Close())
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}

type sortRuntimeFailureSource struct {
	*rawSource
	pullErr error
	cancel  context.CancelFunc
}

func (s *sortRuntimeFailureSource) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if s.pulls == 1 {
		if s.cancel != nil {
			s.cancel()
		}
		return nil, s.pullErr
	}
	return s.rawSource.NextBatch(ctx)
}

func TestSortCancellationAndSourceErrorsReleaseEarlierCandidates(t *testing.T) {
	sentinel := errors.New("source failed after copied rows")
	for _, canceled := range []bool{false, true} {
		source, plan := sortWideRuntimeFixture(4)
		ctx, cancel := context.WithCancel(vectorized.WithQueryMemoryTracker(t.Context(), 1<<20))
		failure := &sortRuntimeFailureSource{rawSource: source, pullErr: sentinel}
		want := sentinel
		if canceled {
			failure.cancel, failure.pullErr, want = cancel, nil, context.Canceled
		}
		operator, err := WrapSort(ctx, failure, plan)
		require.NoError(t, err)
		batch, err := operator.NextBatch(ctx)
		require.ErrorIs(t, err, want)
		require.Nil(t, batch)
		stats, ok := SortStatistics(operator)
		require.True(t, ok)
		require.Positive(t, stats.AcceptedRows)
		require.Zero(t, stats.LiveBytes)
		require.Equal(t, stats.ReservedBytes, stats.ReleasedBytes)
		require.NoError(t, operator.Close())
		require.Equal(t, 1, source.closes)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		cancel()
	}
}

func TestSortRejectsInvalidRecipesAndInitializationFailuresWithoutLeaks(t *testing.T) {
	for _, test := range []string{"nil-outputs", "nil-keys", "missing-output", "missing-key", "array-key", "budget", "canceled", "init-error"} {
		t.Run(test, func(t *testing.T) {
			source, outputs, keys := sortRuntimeFixture()
			plan := &SortPlan{Keys: []SortKey{{Expr: keys[0]}}, Outputs: outputs}
			budget := int64(1 << 20)
			if test == "budget" {
				budget = 1
			}
			ctx, cancel := context.WithCancel(vectorized.WithQueryMemoryTracker(t.Context(), budget))
			defer cancel()
			want := errors.New("sort initialization failed")
			switch test {
			case "nil-outputs":
				plan.Outputs = nil
			case "nil-keys":
				plan.Keys = nil
			case "missing-output":
				plan.Outputs = []Slot{{Source: vectorized.ColumnDef{Name: "absent", Role: vectorized.RoleField}}}
			case "missing-key":
				plan.Keys[0].Expr = &Expression{Op: "column", Source: vectorized.ColumnDef{Name: "absent", Role: vectorized.RoleField}}
			case "array-key":
				plan.Keys[0].Expr = &Expression{Op: "literal", Type: ExpressionIntArray, Literal: ExpressionScalar{Ints: []int64{1}}}
			case "canceled":
				cancel()
			case "init-error":
				source.initErr = want
			}
			operator, err := WrapSort(ctx, source, plan)
			if test == "init-error" {
				require.NoError(t, err)
				require.ErrorIs(t, operator.Init(ctx), want)
				require.NoError(t, operator.Close())
			} else {
				require.Error(t, err)
				require.Nil(t, operator)
			}
			require.Equal(t, 1, source.closes)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
}

func sortBoxedIntArrayRuntimeFixture(count int) (*rawSource, *SortPlan) {
	defs := []vectorized.ColumnDef{
		{Name: "values", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeTagValue},
		{Name: "rid", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
	}
	schema := vectorized.NewBatchSchema(defs)
	batch := vectorized.NewRecordBatch(schema, 2)
	values := batch.Columns[0].(*vectorized.TypedColumn[*modelv1.TagValue])
	values.Append(&modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{7}}}})
	values.Append(&modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: make([]int64, count)}}})
	batch.Columns[1].(*vectorized.TypedColumn[int64]).Append(1)
	batch.Columns[1].(*vectorized.TypedColumn[int64]).Append(2)
	batch.Len = 2
	return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}, &SortPlan{
		Keys:    []SortKey{{Expr: &Expression{Op: "column", Source: defs[1]}}},
		Outputs: []Slot{{Source: defs[0], Output: defs[0]}, {Source: defs[1], Output: defs[1]}},
	}
}

func TestSortBoxedPackedIntegerArraysRespectOwnedMemoryBudget(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		for _, fullSort := range []bool{false, true} {
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), 512<<10)
			source, plan := sortBoxedIntArrayRuntimeFixture(131072)
			limit := uint32(2)
			plan.Limit, plan.FullSort = &limit, fullSort
			var operator vectorized.PullOperator
			var err error
			if wrapped {
				operator, err = WrapRaw(ctx, source, &RawPlan{Slots: plan.Outputs, Sort: plan, Limit: &limit})
			} else {
				operator, err = WrapSort(ctx, source, plan)
			}
			require.NoError(t, err)
			batch, err := operator.NextBatch(ctx)
			require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded, "packed zeros must reserve their full native int64 array")
			require.Nil(t, batch)
			stats, ok := SortStatistics(operator)
			require.True(t, ok)
			require.Equal(t, uint64(1), stats.AcceptedRows, "an earlier candidate must be released when the wide candidate exceeds budget")
			require.Zero(t, stats.LiveBytes)
			require.Equal(t, stats.ReservedBytes, stats.ReleasedBytes)
			require.NoError(t, operator.Close())
			require.Equal(t, 1, source.closes)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())

			source, plan = sortBoxedIntArrayRuntimeFixture(16)
			original := source.batches[0].Columns[0].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()
			plan.Limit, plan.FullSort = &limit, fullSort
			operator, err = WrapSort(ctx, source, plan)
			require.NoError(t, err)
			batch, err = operator.NextBatch(ctx)
			require.NoError(t, err)
			original[0].GetIntArray().Value[0] = 99
			original[1].GetIntArray().Value[0] = 99
			copied := batch.Columns[0].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()
			require.Equal(t, int64(7), copied[0].GetIntArray().Value[0])
			require.Equal(t, int64(0), copied[1].GetIntArray().Value[0])
			require.NoError(t, operator.Close())
			require.Equal(t, 1, source.closes)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		}
	}

	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 4<<20)
	source, plan := sortBoxedIntArrayRuntimeFixture(131072)
	operator, err := WrapSort(ctx, source, plan)
	require.NoError(t, err)
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch.Columns[0].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()[1].GetIntArray().Value, 131072)
	require.GreaterOrEqual(t, vectorized.QueryMemoryTracker(ctx, 0).Used(), int64(1<<20), "the native array footprint must be charged")
	require.NoError(t, operator.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
