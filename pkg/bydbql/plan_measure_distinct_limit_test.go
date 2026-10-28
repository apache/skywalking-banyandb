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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestMeasureDistinctUnorderedLimitStopsWithinBatch(t *testing.T) {
	execution := bindDistinct(t, "SELECT DISTINCT a, b FROM g.m LIMIT 2 OFFSET 1")
	source := distinctStringSource([]string{"x", "x", "x", "y", "z", "extra"}, []string{"1", "1", "2", "3", "4", "5"})
	rows, stats := runDistinct(t, execution, source)
	require.Equal(t, [][]any{{"x", "2"}, {"y", "3"}}, rows)
	require.EqualValues(t, 4, stats.InputRows)
	require.EqualValues(t, 3, stats.Groups)
}

func TestMeasureDistinctLimitDeduplicatesAcrossBatches(t *testing.T) {
	execution := bindDistinct(t, "SELECT DISTINCT a FROM g.m LIMIT 2 OFFSET 1")
	source := distinctStringSource([]string{"x", "x"}, []string{"1", "2"})
	second := distinctStringSource([]string{"x", "y", "y", "z", "ignored"}, []string{"3", "4", "5", "6", "7"})
	third := distinctStringSource([]string{"untouched"}, []string{"8"})
	source.batches = append(source.batches, second.batches[0], third.batches[0])
	rows, stats := runDistinct(t, execution, source)
	require.Equal(t, [][]any{{"y"}, {"z"}}, rows)
	require.EqualValues(t, 6, stats.InputRows)
	require.EqualValues(t, 3, stats.Groups)
	require.Equal(t, 2, source.next)
}

func TestMeasureDistinctOrderedLimitConsumesCompleteInput(t *testing.T) {
	execution := bindDistinct(t, "SELECT DISTINCT TIME FROM g.m ORDER BY TIME LIMIT 1")
	source := distinctStringSource([]string{"x", "y", "z"}, []string{"1", "2", "3"})
	rows, stats := runDistinct(t, execution, source)
	require.Equal(t, [][]any{{int64(0)}}, rows)
	require.EqualValues(t, 3, stats.InputRows)
	require.EqualValues(t, 3, stats.Groups)
}

func TestMeasureDistinctLimitOffsetAdditionDoesNotWrap(t *testing.T) {
	execution := bindDistinct(t, "SELECT DISTINCT a FROM g.m LIMIT 4294967295 OFFSET 2")
	source := distinctStringSource([]string{"x", "y", "z"}, []string{"1", "2", "3"})
	rows, stats := runDistinct(t, execution, source)
	require.Equal(t, [][]any{{"z"}}, rows)
	require.EqualValues(t, 3, stats.InputRows)
	require.EqualValues(t, 3, stats.Groups)
}

func TestMeasureDistinctLimitKeepsNullAndNormalizedFloatEquality(t *testing.T) {
	execution := bindDistinct(t, "SELECT DISTINCT f FROM g.m LIMIT 2 OFFSET 1")
	def := vectorized.ColumnDef{Name: "f", Role: vectorized.RoleField, Type: vectorized.ColumnTypeFloat64}
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{def})
	column := vectorized.NewColumnFromSlice(vectorized.ColumnTypeFloat64, []float64{
		0, math.Copysign(0, -1), math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0x7ff8000000001234),
	})
	column.AppendNull()
	column.Append(42)
	source := &expressionContractSource{schema: schema, batches: []*vectorized.RecordBatch{{Schema: schema, Columns: []vectorized.Column{column}, Len: 6}}}
	rows, stats := runDistinct(t, execution, source)
	require.Len(t, rows, 2)
	require.True(t, math.IsNaN(rows[0][0].(float64)))
	require.Nil(t, rows[1][0])
	require.EqualValues(t, 5, stats.InputRows)
	require.EqualValues(t, 3, stats.Groups)
}

func TestMeasureDistinctLimitWaitsForGroupingAndHaving(t *testing.T) {
	execution := bindDistinct(t, "SELECT DISTINCT COUNT(v) AS n FROM g.m GROUP BY a HAVING COUNT(v) > 1 LIMIT 1")
	source := distinctStringSource([]string{"x", "y", "x", "y", "y"}, []string{"1", "2", "3", "4", "5"})
	def := vectorized.ColumnDef{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
	source.schema = vectorized.NewBatchSchema(append(source.schema.Columns, def))
	source.batches[0].Schema = source.schema
	source.batches[0].Columns = append(source.batches[0].Columns, vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, []int64{1, 2, 3, 4, 5}))
	rows, stats := runDistinct(t, execution, source)
	require.Equal(t, [][]any{{int64(2)}}, rows)
	require.EqualValues(t, 1, stats.InputRows)
	require.EqualValues(t, 1, stats.Groups)
}

func TestMeasureDistinctUnorderedTimestampLimitKeepsTimestampValues(t *testing.T) {
	execution := bindDistinct(t, "SELECT DISTINCT TIME AS observed FROM g.m LIMIT 2")
	source := distinctStringSource([]string{"x", "x", "y", "z"}, []string{"1", "2", "3", "4"})
	source.batches[0].Columns[2] = vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, []int64{1000000, 1000000, 2000000, 3000000})
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := ql.WrapRaw(ctx, source, execution.Plan)
	require.NoError(t, err)
	iterator, err := ql.RawIterator(ctx, operator)
	require.NoError(t, err)
	for _, stamp := range []int64{1000000, 2000000} {
		require.True(t, iterator.Next())
		point := iterator.Current()[0].DataPoint
		require.Nil(t, point.Timestamp)
		require.Equal(t, stamp, point.TagFamilies[0].Tags[0].Value.GetTimestamp().AsTime().UnixNano())
	}
	require.False(t, iterator.Next())
	require.NoError(t, iterator.Close())
	stats, ok := ql.DistinctStatistics(operator)
	require.True(t, ok)
	require.EqualValues(t, 3, stats.InputRows)
	require.EqualValues(t, 2, stats.Groups)
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
