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

func TestMeasureAggregatePipelineFiltersBeforeAggregationAndPaginatesGroups(t *testing.T) {
	source, _ := rawFixture()
	defs := source.schema.Columns
	outputs := []AggregateOutput{
		{Index: 0, Column: vectorized.ColumnDef{Name: "c", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}},
		{Index: 1, Column: vectorized.ColumnDef{Name: "s", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}},
		{Index: 2, Column: vectorized.ColumnDef{Name: "a", Role: vectorized.RoleField, Type: vectorized.ColumnTypeFloat64}},
	}
	aggregate := &AggregatePlan{Calls: []AggregateCall{
		{Source: defs[4], Function: AggregateCount},
		{Source: defs[4], Function: AggregateSum},
		{Source: defs[4], Function: AggregateAvg},
	}, Outputs: outputs}
	limit := uint32(1)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	plan := &RawPlan{
		Slots: []Slot{{Source: defs[4], Output: defs[4]}}, Aggregate: aggregate, Limit: &limit,
		Residual: true, Ranges: []MillisRange{{Begin: 0, End: 1000}, {Begin: 2000, End: 3001}},
	}
	op, err := WrapRaw(ctx, source, plan)
	require.NoError(t, err)
	iterator, err := RawIterator(ctx, op)
	require.NoError(t, err)
	require.True(t, iterator.Next())
	row := iterator.Current()[0].DataPoint
	require.EqualValues(t, 2, row.Fields[0].Value.GetInt().Value)
	require.EqualValues(t, 3, row.Fields[1].Value.GetInt().Value)
	require.Equal(t, 1.5, row.Fields[2].Value.GetFloat().Value)
	require.Nil(t, row.Timestamp)
	require.Zero(t, row.Sid)
	require.Zero(t, row.Version)
	require.False(t, iterator.Next())
	require.NoError(t, iterator.Close())
	require.Equal(t, 2, source.pulls)
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestMeasureAggregatePipelineHandlesEmptyInputAndFinalPagination(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, offset := range []uint32{0, 1} {
			source, _ := rawFixture()
			defs := source.schema.Columns
			aggregate := &AggregatePlan{
				Calls: []AggregateCall{{Source: defs[4], Function: AggregateCount}, {Source: defs[4], Function: AggregateSum}},
				Outputs: []AggregateOutput{
					{Index: 0, Column: vectorized.ColumnDef{Name: "c", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}},
					{Index: 1, Column: vectorized.ColumnDef{Name: "s", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}},
				},
			}
			if grouped {
				aggregate.GroupBy = []vectorized.ColumnDef{defs[3]}
			}
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
			op, err := WrapRaw(ctx, source, &RawPlan{
				Slots:     []Slot{{Source: defs[3], Output: defs[3]}, {Source: defs[4], Output: defs[4]}},
				Aggregate: aggregate, Empty: true, Offset: offset,
			})
			require.NoError(t, err)
			iterator, err := RawIterator(ctx, op)
			require.NoError(t, err)
			if !grouped && offset == 0 {
				require.True(t, iterator.Next())
				row := iterator.Current()[0].DataPoint
				require.EqualValues(t, 0, row.Fields[0].Value.GetInt().Value)
				require.NotNil(t, row.Fields[1].Value.GetNull())
			}
			require.False(t, iterator.Next())
			require.NoError(t, iterator.Close())
			require.Zero(t, source.inits)
			require.Zero(t, source.pulls)
			require.Equal(t, 1, source.closes)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		}
	}
}
