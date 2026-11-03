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

type nativeMergeFailSource struct {
	*rawSource
	nextErr  error
	closeErr error
	cancel   context.CancelFunc
}

func (s *nativeMergeFailSource) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if s.cancel != nil {
		s.cancel()
	}
	if s.nextErr != nil {
		return nil, s.nextErr
	}
	return s.rawSource.NextBatch(ctx)
}

func (s *nativeMergeFailSource) Close() error {
	_ = s.rawSource.Close()
	return s.closeErr
}

func TestNativeAggregateMergeFailuresCloseEveryOpenedLeaf(t *testing.T) {
	value := aggregateField("v", vectorized.ColumnTypeFloat64)
	sentinel := errors.New("native source failed")
	for _, stage := range []string{"open", "init", "next", "close", "cancel", "schema", "null-count"} {
		t.Run(stage, func(t *testing.T) {
			budget := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
			ctx, cancel := context.WithCancel(budget)
			defer cancel()
			plan := nativeMergePlan(nil, value, AggregateSum)
			plan.Leaves = []NativeAggregateLeaf{{Source: value, Function: AggregateSum}, {Source: value, Function: AggregateCount}}
			var sources []*nativeMergeFailSource
			op, err := MergeNativeAggregate(ctx, plan, func(_ context.Context, leaf NativeAggregateLeaf) (vectorized.PullOperator, error) {
				source := &nativeMergeFailSource{rawSource: nativeMergeScalarSource(value, 2)}
				sources = append(sources, source)
				if leaf.Function != AggregateCount {
					return source, nil
				}
				switch stage {
				case "open":
					return source, sentinel
				case "init":
					source.initErr = sentinel
				case "next":
					source.nextErr = sentinel
				case "close":
					source.closeErr = sentinel
				case "cancel":
					source.cancel = cancel
				case "schema":
					source.schema = vectorized.NewBatchSchema(nil)
				case "null-count":
					source.batches[0].Columns[0].Reset()
					source.batches[0].Columns[0].AppendNull()
				}
				return source, nil
			})
			require.NoError(t, err)
			require.NoError(t, op.Init(ctx))
			batch, err := op.NextBatch(ctx)
			require.Error(t, err)
			require.Nil(t, batch)
			if stage == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			} else if stage != "schema" && stage != "null-count" {
				require.ErrorIs(t, err, sentinel)
			}
			require.NoError(t, op.Close())
			require.Len(t, sources, 2)
			for _, source := range sources {
				require.Equal(t, 1, source.closes)
			}
			require.Zero(t, vectorized.QueryMemoryTracker(budget, 0).Used())
		})
	}
}

func TestNativeAggregateMergeDenialBeforeOpenAndGrowthReleases(t *testing.T) {
	value := aggregateField("v", vectorized.ColumnTypeFloat64)
	key := aggregateField("key", vectorized.ColumnTypeInt64)
	for _, scenario := range []string{"constructor-budget", "canceled", "group-cap", "group-budget"} {
		t.Run(scenario, func(t *testing.T) {
			limit := int64(1 << 20)
			switch scenario {
			case "constructor-budget":
				limit = 1
			case "group-budget":
				limit = 32 << 10
			default:
			}
			budget := vectorized.WithQueryMemoryTracker(t.Context(), limit)
			ctx, cancel := context.WithCancel(budget)
			defer cancel()
			plan := nativeMergePlan([]vectorized.ColumnDef{key}, value, AggregateCount)
			plan.Leaves = []NativeAggregateLeaf{{Source: value, Function: AggregateCount}}
			if scenario == "group-cap" {
				plan.MaxGroups = 100
			}
			if scenario == "canceled" {
				cancel()
			}
			schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{key, value})
			batch := vectorized.NewRecordBatch(schema, 1024)
			for row := range 1024 {
				batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(int64(row))
				batch.Columns[1].(*vectorized.TypedColumn[float64]).Append(1)
			}
			batch.Len = 1024
			source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
			opens := 0
			op, err := MergeNativeAggregate(ctx, plan, func(context.Context, NativeAggregateLeaf) (vectorized.PullOperator, error) {
				opens++
				return source, nil
			})
			if scenario == "constructor-budget" || scenario == "canceled" {
				require.Error(t, err)
				require.Nil(t, op)
				require.Zero(t, opens)
			} else {
				require.NoError(t, err)
				require.NoError(t, op.Init(ctx))
				actual, pullErr := op.NextBatch(ctx)
				require.ErrorIs(t, pullErr, vectorized.ErrMemoryBudgetExceeded)
				require.Nil(t, actual)
				require.NoError(t, op.Close())
				require.Equal(t, 1, source.closes)
			}
			require.Zero(t, vectorized.QueryMemoryTracker(budget, 0).Used())
		})
	}
}

func TestNativeAggregateMergeOwnsKeysBeforeNativeClose(t *testing.T) {
	value := aggregateField("v", vectorized.ColumnTypeFloat64)
	key := vectorized.ColumnDef{Name: "key", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeBytes}
	plan := nativeMergePlan([]vectorized.ColumnDef{key}, value, AggregateCount)
	plan.Leaves = []NativeAggregateLeaf{{Source: value, Function: AggregateCount}}
	payload := []byte("first")
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{key, value})
	batch := vectorized.NewRecordBatch(schema, 1)
	batch.Columns[0].(*vectorized.TypedColumn[[]byte]).Append(payload)
	batch.Columns[1].(*vectorized.TypedColumn[float64]).Append(2)
	batch.Len = 1
	source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}, poison: payload}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := MergeNativeAggregate(ctx, plan, func(context.Context, NativeAggregateLeaf) (vectorized.PullOperator, error) { return source, nil })
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	actual, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("xxxxx"), payload)
	require.Equal(t, []byte("first"), actual.Columns[0].(*vectorized.TypedColumn[[]byte]).Data()[0])
	require.Equal(t, int64(2), actual.Columns[1].(*vectorized.TypedColumn[int64]).Data()[0])
	require.NoError(t, op.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestNativeAggregateMergeCancellationAfterLargeRetainedGroupSet(t *testing.T) {
	fixture := makeNativeMergeKernelFixture(65537, false)
	fixture.plan.Aggregate.Calls = fixture.plan.Aggregate.Calls[1:2]
	fixture.plan.Aggregate.Outputs = []AggregateOutput{{Column: aggregateField("count", vectorized.ColumnTypeInt64)}}
	fixture.plan.Leaves = []NativeAggregateLeaf{{Source: fixture.plan.Leaves[1].Source, Function: AggregateCount}}
	budget := vectorized.WithQueryMemoryTracker(t.Context(), 16<<20)
	ctx, cancel := context.WithCancel(budget)
	defer cancel()
	source := &aggregateCancelSource{
		rawSource: &rawSource{schema: fixture.batches[1][0].Schema, batches: fixture.batches[1]}, cancel: cancel,
	}
	op, err := MergeNativeAggregate(ctx, fixture.plan, func(context.Context, NativeAggregateLeaf) (vectorized.PullOperator, error) { return source, nil })
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	batch, err := op.NextBatch(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, batch)
	stats, ok := NativeAggregateStatistics(op)
	require.True(t, ok)
	require.EqualValues(t, 65536, stats.Groups)
	require.EqualValues(t, 65536, stats.InputRows)
	require.NoError(t, op.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(budget, 0).Used())
}
