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

func TestAggregateCollidingKeysSurviveGrowthAndRepeatedLookup(t *testing.T) {
	defs := []vectorized.ColumnDef{aggregateField("key", vectorized.ColumnTypeInt64), aggregateField("v", vectorized.ColumnTypeInt64)}
	schema := vectorized.NewBatchSchema(defs)
	batch := vectorized.NewRecordBatch(schema, 6)
	// These logical integer keys collide in the first three low hash bits. They
	// exercise equality probing before and after the group index grows.
	batch.Columns[0].(*vectorized.TypedColumn[int64]).AppendSlice([]int64{0, 3, 4, 4, 0, 3})
	batch.Columns[1] = vectorized.NewConstInt64(1, 6)
	batch.Len = 6
	source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := WrapAggregate(ctx, source, aggregateCountPlan(defs[:1], defs[1]))
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	out, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 3, 4}, out.Columns[0].(*vectorized.TypedColumn[int64]).Data())
	require.Equal(t, []int64{2, 2, 2}, out.Columns[1].(*vectorized.TypedColumn[int64]).Data())
	require.NoError(t, op.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestAggregateHighCardinalityRespectsQueryMemoryLimits(t *testing.T) {
	const rows = 262145
	for _, budget := range []int64{16 << 20, 256 << 20} {
		source, defs := aggregateUniqueKeysSource(rows)
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), budget)
		op, err := WrapAggregate(ctx, source, aggregateCountPlan(defs[:1], defs[1]))
		require.NoError(t, err)
		require.NoError(t, op.Init(ctx))
		var seen int
		for {
			var batch *vectorized.RecordBatch
			batch, err = op.NextBatch(ctx)
			if err != nil || batch == nil {
				break
			}
			seen += batch.Len
			for _, count := range batch.Columns[1].(*vectorized.TypedColumn[int64]).Data() {
				require.EqualValues(t, 1, count)
			}
		}
		if budget == 16<<20 {
			require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
			require.Zero(t, seen)
		} else {
			require.NoError(t, err)
			require.Equal(t, rows, seen)
			stats, ok := AggregateStatistics(op)
			require.True(t, ok)
			require.EqualValues(t, rows, stats.Groups)
			require.EqualValues(t, rows*8, stats.StateBytes)
			require.LessOrEqual(t, stats.StateCapacity, stats.StateBytes*2)
			require.Less(t, stats.KeyCapacity, uint64(rows*80))
		}
		require.NoError(t, op.Close())
		require.Equal(t, 1, source.closes)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}

func aggregateUniqueKeysSource(rows int) (*rawSource, []vectorized.ColumnDef) {
	defs := []vectorized.ColumnDef{aggregateField("key", vectorized.ColumnTypeInt64), aggregateField("v", vectorized.ColumnTypeInt64)}
	schema := vectorized.NewBatchSchema(defs)
	source := &rawSource{schema: schema}
	for begin := 0; begin < rows; begin += 65536 {
		end := min(begin+65536, rows)
		key := vectorized.NewInt64Column(end - begin)
		for row := begin; row < end; row++ {
			key.Append(int64(row))
		}
		batch := &vectorized.RecordBatch{Schema: schema, Columns: []vectorized.Column{key, vectorized.NewConstInt64(1, end-begin)}, Len: end - begin}
		source.batches = append(source.batches, batch)
	}
	return source, defs
}
