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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func sortRuntimeFixture() (*rawSource, []Slot, []*Expression) {
	defs := []vectorized.ColumnDef{
		{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
		{Name: "rid", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
		{Name: "timestamp", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Name: "sid", Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Name: "version", Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
	}
	schema := vectorized.NewBatchSchema(defs)
	batch := vectorized.NewRecordBatch(schema, 6)
	for row, value := range []int64{0, 2, 4, 0, 6, 0} {
		batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(value)
		batch.Columns[1].(*vectorized.TypedColumn[int64]).Append(int64(row + 1))
		batch.Columns[2].(*vectorized.TypedColumn[int64]).Append(int64(row) * 1_000_000_000)
		batch.Columns[3].(*vectorized.TypedColumn[int64]).Append(int64(row + 10))
		batch.Columns[4].(*vectorized.TypedColumn[int64]).Append(int64(row + 20))
	}
	batch.Columns[0].MarkNullAt(3)
	batch.Columns[0].MarkNullAt(5)
	batch.Len = 6
	outputs := []Slot{{Source: defs[0], Output: defs[0]}, {Source: defs[1], Output: defs[1]}}
	keys := []*Expression{{Op: "column", Source: defs[0]}, {Op: "column", Source: defs[1]}}
	return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}, outputs, keys
}

func TestSortKeepsExactTypedLexicographicOrder(t *testing.T) {
	for _, test := range []struct {
		name   string
		column vectorized.Column
		asc    []int64
		desc   []int64
	}{
		{
			name: "int64",
			column: vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64,
				[]int64{9007199254740993, 9007199254740992, math.MaxInt64, math.MinInt64, -1, 0, 0}),
			asc: []int64{4, 5, 6, 2, 1, 3, 7}, desc: []int64{7, 3, 1, 2, 6, 5, 4},
		},
		{
			name: "float64",
			column: vectorized.NewColumnFromSlice(vectorized.ColumnTypeFloat64,
				[]float64{math.NaN(), math.Inf(1), 1, math.Inf(-1), math.Copysign(0, -1), 0, math.NaN(), 0}),
			asc: []int64{4, 5, 6, 3, 2, 1, 7, 8}, desc: []int64{8, 1, 7, 2, 3, 5, 6, 4},
		},
		{
			name: "string", column: vectorized.NewColumnFromSlice(vectorized.ColumnTypeString, []string{"é", "z", "\x00", "a", "Z", "", "", ""}),
			asc: []int64{6, 7, 3, 5, 4, 2, 1, 8}, desc: []int64{8, 1, 2, 4, 5, 3, 6, 7},
		},
		{
			name: "bytes", column: vectorized.NewColumnFromSlice(vectorized.ColumnTypeBytes, [][]byte{{255}, {'z'}, {0}, {'a'}, {'Z'}, {}, {}, {}}),
			asc: []int64{6, 7, 3, 5, 4, 2, 1, 8}, desc: []int64{8, 1, 2, 4, 5, 3, 6, 7},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.column.MarkNullAt(test.column.Len() - 1)
			for _, descending := range []bool{false, true} {
				for _, fullSort := range []bool{false, true} {
					defs := []vectorized.ColumnDef{
						{Name: "key", Role: vectorized.RoleField, Type: test.column.Type()},
						{Name: "rid", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
					}
					schema := vectorized.NewBatchSchema(defs)
					rids := vectorized.NewColumnForType(vectorized.ColumnTypeInt64, test.column.Len()).(*vectorized.TypedColumn[int64])
					for row := 0; row < test.column.Len(); row++ {
						rids.Append(int64(row + 1))
					}
					source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{{Schema: schema, Columns: []vectorized.Column{test.column, rids}, Len: rids.Len()}}}
					limit := uint32(4)
					ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
					operator, err := WrapSort(ctx, source, &SortPlan{
						Keys: []SortKey{
							{Expr: &Expression{Op: "column", Source: defs[0]}, Descending: descending, NullsFirst: descending},
							{Expr: &Expression{Op: "column", Source: defs[1]}},
						}, Outputs: []Slot{{Source: defs[1], Output: defs[1]}}, Limit: &limit, Offset: 1, FullSort: fullSort,
					})
					require.NoError(t, err)
					batch, err := operator.NextBatch(ctx)
					require.NoError(t, err)
					want := test.asc
					if descending {
						want = test.desc
					}
					require.Equal(t, want[1:5], batch.Columns[0].(*vectorized.TypedColumn[int64]).Data())
					require.NoError(t, operator.Close())
					require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
				}
			}
		})
	}
}

func TestSortOrdersCompletedRowsAndPreservesMetadata(t *testing.T) {
	source, outputs, keys := sortRuntimeFixture()
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapSort(ctx, source, &SortPlan{
		Keys: []SortKey{{Expr: keys[0], Descending: true}, {Expr: keys[1]}}, Outputs: outputs,
	})
	require.NoError(t, err)
	require.NoError(t, operator.Init(ctx))
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{5, 3, 2, 1, 4, 6}, batch.Columns[1].(*vectorized.TypedColumn[int64]).Data())
	require.Equal(t, []int64{14, 12, 11, 10, 13, 15}, batch.Columns[batch.Schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64]).Data())
	require.Equal(t, []int64{24, 22, 21, 20, 23, 25}, batch.Columns[batch.Schema.VersionIndex()].(*vectorized.TypedColumn[int64]).Data())
	require.Equal(t, []int64{4e9, 2e9, 1e9, 0, 3e9, 5e9}, batch.Columns[batch.Schema.TimestampIndex()].(*vectorized.TypedColumn[int64]).Data())
	require.Equal(t, vectorized.BatchHints{}, batch.Hints)
	require.NoError(t, operator.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestRawSortHidesKeyAndPaginatesAfterFiltering(t *testing.T) {
	source, slots, keys := sortRuntimeFixture()
	limit := uint32(2)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapRaw(ctx, source, &RawPlan{
		Slots: slots, Filter: &Expression{Op: ">", Args: []*Expression{keys[0], expressionInt(0)}}, Limit: &limit, Offset: 1,
		Sort: &SortPlan{Keys: []SortKey{{Expr: keys[0], Descending: true}, {Expr: keys[1]}}, Outputs: slots[1:]},
	})
	require.NoError(t, err)
	require.NoError(t, operator.Init(ctx))
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch.Schema.FieldColumns, 1)
	require.Equal(t, "rid", batch.Schema.Columns[0].Name)
	require.Equal(t, []int64{3, 2}, batch.Columns[0].(*vectorized.TypedColumn[int64]).Data())
	require.NoError(t, operator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestSortTopKMatchesFullSortWithBoundedCandidates(t *testing.T) {
	for _, fullSort := range []bool{false, true} {
		source, outputs, keys := sortRuntimeFixture()
		limit := uint32(2)
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		operator, err := WrapSort(ctx, source, &SortPlan{
			Keys: []SortKey{{Expr: keys[0], Descending: true}, {Expr: keys[1]}}, Outputs: outputs,
			Limit: &limit, Offset: 1, FullSort: fullSort,
		})
		require.NoError(t, err)
		batch, err := operator.NextBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, []int64{3, 2}, batch.Columns[1].(*vectorized.TypedColumn[int64]).Data())
		stats, ok := SortStatistics(operator)
		require.True(t, ok)
		if fullSort {
			require.Equal(t, uint64(6), stats.Candidates)
		} else {
			require.Equal(t, uint64(3), stats.K)
			require.LessOrEqual(t, stats.Candidates, stats.K)
			require.LessOrEqual(t, stats.PeakCandidates, stats.K)
			require.LessOrEqual(t, stats.CandidateCapacity, stats.K)
		}
		require.NoError(t, operator.Close())
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}

func TestSortedTimestampAliasesKeepTimestampEgress(t *testing.T) {
	source, slots := rawFixture()
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	visible := []Slot{{Source: slots[3].Output, Output: slots[3].Output, Timestamp: true}}
	operator, err := WrapRaw(ctx, source, &RawPlan{
		Slots: slots,
		Sort:  &SortPlan{Keys: []SortKey{{Expr: &Expression{Op: "column", Source: slots[1].Output}}}, Outputs: visible},
	})
	require.NoError(t, err)
	iterator, err := RawIterator(ctx, operator)
	require.NoError(t, err)
	require.True(t, iterator.Next())
	point := iterator.Current()[0].DataPoint
	require.NotNil(t, point.TagFamilies[0].Tags[0].Value.GetTimestamp())
	require.Equal(t, point.Timestamp.AsTime(), point.TagFamilies[0].Tags[0].Value.GetTimestamp().AsTime())
	require.NoError(t, iterator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestSortPreservesNullArrayOutputs(t *testing.T) {
	defs := []vectorized.ColumnDef{
		{Name: "values", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeInt64Array},
		{Name: "rid", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
	}
	schema := vectorized.NewBatchSchema(defs)
	source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{{
		Schema: schema, Len: 2,
		Columns: []vectorized.Column{vectorized.NewConstNull(vectorized.ColumnTypeInt64Array, 2), vectorized.NewConstInt64(1, 2)},
	}}}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapSort(ctx, source, &SortPlan{
		Keys: []SortKey{{Expr: &Expression{Op: "column", Source: defs[1]}}}, Outputs: []Slot{{Source: defs[0], Output: defs[0]}},
	})
	require.NoError(t, err)
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, batch.Len)
	require.True(t, batch.Columns[0].IsNull(0))
	require.True(t, batch.Columns[0].IsNull(1))
	require.NoError(t, operator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestRawSortPaginatesAfterCompleteDistinct(t *testing.T) {
	source, slots, keys := sortRuntimeFixture()
	limit := uint32(1)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapRaw(ctx, source, &RawPlan{
		Slots: slots[:1], Distinct: true, Limit: &limit, Offset: 1,
		Sort: &SortPlan{Keys: []SortKey{{Expr: keys[0], Descending: true}}, Outputs: slots[:1]},
	})
	require.NoError(t, err)
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{4}, batch.Columns[0].(*vectorized.TypedColumn[int64]).Data())
	stats, ok := DistinctStatistics(operator)
	require.True(t, ok)
	require.Equal(t, uint64(6), stats.InputRows)
	require.NoError(t, operator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
