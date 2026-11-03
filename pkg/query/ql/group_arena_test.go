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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestDistinctRetainsCompactHighCardinalityKeys(t *testing.T) {
	source, defs := aggregateUniqueKeysSource(4096)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	before := pool.AllRefsCount()
	operator, err := WrapRaw(ctx, source, &RawPlan{Distinct: true, Slots: []Slot{{Source: defs[0], Output: defs[0]}}})
	require.NoError(t, err)
	defer operator.Close()
	require.NoError(t, operator.Init(ctx))
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, vectorized.DefaultBatchSize, batch.Len)
	stats, ok := DistinctStatistics(operator)
	require.True(t, ok)
	require.EqualValues(t, 4096, stats.Groups)
	require.EqualValues(t, 40960, stats.KeyBytes)
	// The retained index, encoded keys and scratch must fit below 150 KiB.
	require.Less(t, stats.KeyCapacity, uint64(150<<10))
	require.NoError(t, operator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	require.Equal(t, before, pool.AllRefsCount())
}

func TestAggregateSmallBudgetKeepsSmallKeysUsable(t *testing.T) {
	source, defs := aggregateUniqueKeysSource(1)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 4400)
	operator, err := WrapAggregate(ctx, source, &AggregatePlan{
		GroupBy: defs[:1], Outputs: []AggregateOutput{{Column: defs[0], IsGroup: true}},
	})
	require.NoError(t, err)
	defer operator.Close()
	require.NoError(t, operator.Init(ctx))
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{0}, batch.Columns[0].(*vectorized.TypedColumn[int64]).Data())
	require.NoError(t, operator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestDistinctFinalValuesSurviveKeyStorageReuse(t *testing.T) {
	before := pool.AllRefsCount()
	var retained []*measurev1.DataPoint
	for _, marker := range []string{"a", "z", "x"} {
		source, slots := arenaStringBytesSource(marker, 1024)
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 8<<20)
		operator, err := WrapRaw(ctx, source, &RawPlan{Distinct: true, Slots: slots})
		require.NoError(t, err)
		iterator, err := RawIterator(ctx, operator)
		require.NoError(t, err)
		count := 0
		for iterator.Next() {
			point := iterator.Current()[0].DataPoint
			if marker == "a" {
				retained = append(retained, point)
			}
			count++
		}
		require.NoError(t, iterator.Close())
		require.Equal(t, 1024, count)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		require.Equal(t, before, pool.AllRefsCount())
	}
	for row, point := range retained {
		require.Equal(t, fmt.Sprintf("%04d-%s", row, strings.Repeat("a", 96)), point.TagFamilies[0].Tags[0].Value.GetStr().Value)
		require.Equal(t, []byte{0, 255, byte(row), byte(row >> 8)}, point.Fields[0].Value.GetBinaryData())
	}
}

func arenaStringBytesSource(marker string, rows int) (*rawSource, []Slot) {
	defs := []vectorized.ColumnDef{
		{Name: "key", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeString},
		aggregateField("binary", vectorized.ColumnTypeBytes),
	}
	schema := vectorized.NewBatchSchema(defs)
	batch := vectorized.NewRecordBatch(schema, rows)
	for row := range rows {
		batch.Columns[0].(*vectorized.TypedColumn[string]).Append(fmt.Sprintf("%04d-%s", row, strings.Repeat(marker, 96)))
		batch.Columns[1].(*vectorized.TypedColumn[[]byte]).Append([]byte{0, 255, byte(row), byte(row >> 8)})
	}
	batch.Len = rows
	return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}, []Slot{
		{Source: defs[0], Output: defs[0]}, {Source: defs[1], Output: defs[1]},
	}
}

func TestDistinctKeyStorageReleasesAfterRetainingGroups(t *testing.T) {
	for _, cancellation := range []bool{false, true} {
		t.Run(map[bool]string{false: "budget", true: "cancellation"}[cancellation], func(t *testing.T) {
			before := pool.AllRefsCount()
			source, defs := aggregateUniqueKeysSource(65537)
			budget := int64(1 << 20)
			if cancellation {
				budget = 16 << 20
			}
			ctx, cancel := context.WithCancel(vectorized.WithQueryMemoryTracker(t.Context(), budget))
			defer cancel()
			var input vectorized.PullOperator = source
			if cancellation {
				input = &aggregateCancelSource{rawSource: source, cancel: cancel}
			}
			operator, err := WrapRaw(ctx, input, &RawPlan{Distinct: true, Slots: []Slot{{Source: defs[0], Output: defs[0]}}})
			require.NoError(t, err)
			defer operator.Close()
			require.NoError(t, operator.Init(ctx))
			batch, err := operator.NextBatch(ctx)
			require.Nil(t, batch)
			if cancellation {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
			}
			stats, ok := DistinctStatistics(operator)
			require.True(t, ok)
			require.Greater(t, stats.Groups, uint64(4096))
			require.NoError(t, operator.Close())
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
			require.Equal(t, before, pool.AllRefsCount())
		})
	}
}

func TestAggregatePreservesKeysLargerThanNormalChunk(t *testing.T) {
	def := vectorized.ColumnDef{Name: "key", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeString}
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{def})
	large := strings.Repeat("x", 70<<10) + "-tail"
	batch := vectorized.NewRecordBatch(schema, 3)
	batch.Columns[0].(*vectorized.TypedColumn[string]).AppendSlice([]string{large, "short", large})
	batch.Len = 3
	source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
	before := pool.AllRefsCount()
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapAggregate(ctx, source, &AggregatePlan{
		GroupBy: []vectorized.ColumnDef{def}, Outputs: []AggregateOutput{{Column: def, IsGroup: true}},
	})
	require.NoError(t, err)
	defer operator.Close()
	require.NoError(t, operator.Init(ctx))
	output, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{large, "short"}, output.Columns[0].(*vectorized.TypedColumn[string]).Data())
	require.NoError(t, operator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	require.Equal(t, before, pool.AllRefsCount())
}
