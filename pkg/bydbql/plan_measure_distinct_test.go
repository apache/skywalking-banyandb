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
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func distinctMeasureSchema() *databasev1.Measure {
	return &databasev1.Measure{TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
		{Name: "a", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "b", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "arr", Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
	}}}, Fields: []*databasev1.FieldSpec{
		{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		{Name: "f", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT},
	}}
}

func bindDistinct(t *testing.T, query string) *bydbql.MeasureExecution {
	t.Helper()
	statement, err := bydbql.Prepare(query)
	require.NoError(t, err)
	template, err := bydbql.NewTransformerWithSource(nil, nil).CompileMeasureWithSchemas(t.Context(), statement,
		map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: distinctMeasureSchema()})
	require.NoError(t, err)
	bound, err := statement.Bind(nil)
	require.NoError(t, err)
	execution, err := template.Bind(bound, time.Unix(0, 0))
	require.NoError(t, err)
	require.NotNil(t, execution.Plan)
	require.True(t, execution.Plan.Distinct)
	require.EqualValues(t, math.MaxUint32, execution.Request.Limit)
	require.Zero(t, execution.Request.Offset)
	return execution
}

func runDistinct(t *testing.T, execution *bydbql.MeasureExecution, source *expressionContractSource) ([][]any, ql.AggregateStats) {
	t.Helper()
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := ql.WrapRaw(ctx, source, execution.Plan)
	require.NoError(t, err)
	require.NoError(t, operator.Init(ctx))
	var rows [][]any
	for {
		batch, nextErr := operator.NextBatch(ctx)
		require.NoError(t, nextErr)
		if batch == nil {
			break
		}
		for position := 0; position < batch.ActiveLen(); position++ {
			row := position
			if batch.Selection != nil {
				row = int(batch.Selection[position])
			}
			values := make([]any, len(batch.Columns))
			for i, column := range batch.Columns {
				if column.IsNull(row) {
					continue
				}
				switch typed := column.(type) {
				case *vectorized.TypedColumn[string]:
					values[i] = typed.Data()[row]
				case *vectorized.TypedColumn[int64]:
					values[i] = typed.Data()[row]
				case *vectorized.TypedColumn[float64]:
					values[i] = typed.Data()[row]
				default:
					t.Fatalf("unexpected DISTINCT column %T", column)
				}
			}
			rows = append(rows, values)
		}
	}
	stats, ok := ql.DistinctStatistics(operator)
	require.True(t, ok)
	require.Zero(t, stats.StateLanes)
	require.Zero(t, stats.StateCapacity)
	require.NoError(t, operator.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	after, ok := ql.DistinctStatistics(operator)
	require.True(t, ok)
	require.Equal(t, stats, after)
	return rows, stats
}

func distinctStringSource(a, b []string) *expressionContractSource {
	defs := []vectorized.ColumnDef{
		{Name: "a", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeString},
		{Name: "b", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeString},
		{Name: "time", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Name: "sid", Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
	}
	schema := vectorized.NewBatchSchema(defs)
	batch := vectorized.NewRecordBatch(schema, len(a))
	for i := range a {
		batch.Columns[0].(*vectorized.TypedColumn[string]).Append(a[i])
		batch.Columns[1].(*vectorized.TypedColumn[string]).Append(b[i])
		batch.Columns[2].(*vectorized.TypedColumn[int64]).Append(int64(i) * 1000000)
		batch.Columns[3].(*vectorized.TypedColumn[int64]).Append(int64(i))
	}
	batch.Len = len(a)
	return &expressionContractSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
}

func TestMeasureDistinctCompleteVisibleTupleBeforePagination(t *testing.T) {
	execution := bindDistinct(t, "SELECT DISTINCT a AS x, b FROM g.m LIMIT 2 OFFSET 1")
	a, b := make([]string, 150), make([]string, 150)
	for i := range a {
		a[i], b[i] = "ab", "c"
	}
	a[147], b[147] = "a", "bc"
	a[148], b[148] = "", ""
	a[149], b[149] = "ab", "c"
	source := distinctStringSource(a, b)
	rows, stats := runDistinct(t, execution, source)
	require.Equal(t, [][]any{{"a", "bc"}, {"", ""}}, rows)
	require.EqualValues(t, 149, stats.InputRows)
	require.EqualValues(t, 3, stats.Groups)
	require.EqualValues(t, 66, stats.KeyBytes)
}

func TestMeasureDistinctRunsAfterSelectExpressions(t *testing.T) {
	execution := bindDistinct(t, "SELECT DISTINCT CASE WHEN a = 'x' THEN 'same' ELSE 'other' END AS label FROM g.m")
	rows, stats := runDistinct(t, execution, distinctStringSource([]string{"x", "x", "y", "z"}, []string{"1", "2", "3", "4"}))
	require.Equal(t, [][]any{{"same"}, {"other"}}, rows)
	require.EqualValues(t, 2, stats.Groups)
}

func TestMeasureDistinctRunsAfterAggregationAndHaving(t *testing.T) {
	execution := bindDistinct(t, "SELECT DISTINCT COUNT(v) AS n FROM g.m GROUP BY a HAVING COUNT(v) > 0")
	source := distinctStringSource([]string{"x", "y", "z", "z"}, []string{"1", "2", "3", "4"})
	def := vectorized.ColumnDef{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
	source.schema = vectorized.NewBatchSchema(append(source.schema.Columns, def))
	source.batches[0].Schema = source.schema
	source.batches[0].Columns = append(source.batches[0].Columns, vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, []int64{1, 2, 3, 4}))
	rows, stats := runDistinct(t, execution, source)
	require.Equal(t, [][]any{{int64(1)}, {int64(2)}}, rows)
	require.EqualValues(t, 3, stats.InputRows)
}

func TestMeasureDistinctNormalizesFloatKeysAndKeepsNull(t *testing.T) {
	execution := bindDistinct(t, "SELECT DISTINCT f FROM g.m")
	def := vectorized.ColumnDef{Name: "f", Role: vectorized.RoleField, Type: vectorized.ColumnTypeFloat64}
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{def})
	column := vectorized.NewColumnFromSlice(vectorized.ColumnTypeFloat64, []float64{
		0, math.Copysign(0, -1), math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0x7ff8000000001234),
	})
	column.AppendNull()
	source := &expressionContractSource{schema: schema, batches: []*vectorized.RecordBatch{{Schema: schema, Columns: []vectorized.Column{column}, Len: 5}}}
	rows, stats := runDistinct(t, execution, source)
	require.Len(t, rows, 3)
	require.Equal(t, float64(0), rows[0][0])
	require.False(t, math.Signbit(rows[0][0].(float64)))
	require.True(t, math.IsNaN(rows[1][0].(float64)))
	require.Nil(t, rows[2][0])
	require.EqualValues(t, 3, stats.Groups)
}

func TestMeasureDistinctLimitZeroDoesNotScan(t *testing.T) {
	execution := bindDistinct(t, "SELECT DISTINCT a FROM g.m LIMIT 0")
	source := distinctStringSource([]string{"x"}, []string{"y"})
	rows, stats := runDistinct(t, execution, source)
	require.Empty(t, rows)
	require.Zero(t, source.next)
	require.Zero(t, stats.InputRows)
}

func TestMeasureDistinctRejectsHiddenOrderAndArrayKeys(t *testing.T) {
	for _, query := range []string{
		"SELECT DISTINCT a FROM g.m ORDER BY TIME", "SELECT DISTINCT a FROM g.m ORDER BY idx_a",
		"SELECT DISTINCT arr FROM g.m",
	} {
		t.Run(query, func(t *testing.T) {
			statement, err := bydbql.Prepare(query)
			require.NoError(t, err)
			_, err = bydbql.NewTransformerWithSource(nil, nil).CompileMeasureWithSchemas(context.Background(), statement,
				map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: distinctMeasureSchema()})
			require.ErrorIs(t, err, bydbql.ErrInvalidMeasureQuery)
		})
	}
}

func TestMeasureDistinctNullIsDifferentFromEmptyString(t *testing.T) {
	execution := bindDistinct(t, "SELECT DISTINCT a FROM g.m")
	source := distinctStringSource([]string{"", "", "", "x"}, []string{"1", "2", "3", "4"})
	column := vectorized.NewStringColumn(4)
	column.AppendNull()
	column.Append("")
	column.AppendNull()
	column.Append("x")
	source.batches[0].Columns[0] = column
	rows, stats := runDistinct(t, execution, source)
	require.Equal(t, [][]any{{nil}, {""}, {"x"}}, rows)
	require.EqualValues(t, 3, stats.Groups)
}

func TestMeasureDistinctTimestampPreservesVisibleTagAndDropsIncidentalIdentity(t *testing.T) {
	for _, projection := range []string{"TIME", "TIME AS observed"} {
		execution := bindDistinct(t, "SELECT DISTINCT "+projection+" FROM g.m ORDER BY TIME")
		source := distinctStringSource([]string{"x", "x", "y"}, []string{"1", "2", "3"})
		source.batches[0].Columns[2] = vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, []int64{1000000, 1000000, 2000000})
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		operator, err := ql.WrapRaw(ctx, source, execution.Plan)
		require.NoError(t, err)
		iterator, err := ql.RawIterator(ctx, operator)
		require.NoError(t, err)
		for _, stamp := range []int64{1000000, 2000000} {
			require.True(t, iterator.Next())
			point := iterator.Current()[0].DataPoint
			require.Nil(t, point.Timestamp)
			require.Zero(t, point.Sid)
			require.Zero(t, point.Version)
			require.Len(t, point.TagFamilies, 1)
			require.Equal(t, stamp, point.TagFamilies[0].Tags[0].Value.GetTimestamp().AsTime().UnixNano())
		}
		require.False(t, iterator.Next())
		require.NoError(t, iterator.Close())
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}

func TestMeasureDistinctBindingsOwnFreshExpressionValues(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT DISTINCT CASE WHEN a = ? THEN 'match' ELSE a END AS label FROM g.m")
	require.NoError(t, err)
	template, err := bydbql.NewTransformerWithSource(nil, nil).CompileMeasureWithSchemas(t.Context(), statement,
		map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: distinctMeasureSchema()})
	require.NoError(t, err)
	bind := func(value string) *bydbql.MeasureExecution {
		bound, bindErr := statement.Bind([]*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}}})
		require.NoError(t, bindErr)
		execution, executionErr := template.Bind(bound, time.Unix(0, 0))
		require.NoError(t, executionErr)
		return execution
	}
	first, second := bind("x"), bind("y")
	for i, execution := range []*bydbql.MeasureExecution{first, second} {
		rows, _ := runDistinct(t, execution, distinctStringSource([]string{"x", "y", "x"}, []string{"1", "2", "3"}))
		require.Equal(t, [][][]any{{{"match"}, {"y"}}, {{"x"}, {"match"}}}[i], rows)
	}
}

func TestMeasureDistinctAfterNamedGroupOnly(t *testing.T) {
	execution := bindDistinct(t, "SELECT DISTINCT a FROM g.m GROUP BY a,b")
	rows, stats := runDistinct(t, execution, distinctStringSource([]string{"x", "x", "y", "x"}, []string{"1", "2", "3", "1"}))
	require.Equal(t, [][]any{{"x"}, {"y"}}, rows)
	require.EqualValues(t, 3, stats.InputRows)
	require.EqualValues(t, 2, stats.Groups)
}

func TestMeasureGroupOnlyReferenceKeepsLegacyRouting(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		query := "SELECT a,b FROM g.m GROUP BY a,b"
		if legacy {
			query = "SELECT a,b FROM MEASURE m IN g GROUP BY a,b"
		}
		statement, err := bydbql.Prepare(query)
		require.NoError(t, err)
		template, err := bydbql.NewTransformerWithSource(nil, nil).CompileMeasureWithSchemas(t.Context(), statement,
			map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: distinctMeasureSchema()})
		require.NoError(t, err)
		bound, err := statement.Bind(nil)
		require.NoError(t, err)
		execution, err := template.Bind(bound, time.Unix(0, 0))
		require.NoError(t, err)
		if legacy {
			require.Nil(t, execution.Plan)
			require.NotNil(t, execution.Request.GroupBy)
		} else {
			require.NotNil(t, execution.Plan.Aggregate)
			require.Len(t, execution.Plan.Aggregate.GroupBy, 2)
			require.Empty(t, execution.Plan.Aggregate.Calls)
		}
	}
}

func TestMeasureDistinctPrepare(t *testing.T) {
	_, err := bydbql.Prepare("SELECT DISTINCT zone FROM MEASURE m IN g")
	require.NoError(t, err)
}

func TestMeasureDistinctQualifiedAggregatePrepare(t *testing.T) {
	for _, source := range []string{"replicated_group.ql_t15", "MEASURE ql_t15 IN replicated_group"} {
		_, err := bydbql.Prepare("SELECT DISTINCT SUM(v::FIELD) AS total FROM " + source +
			" WHERE time >= ? AND time < ? GROUP BY host HAVING (COUNT(v::FIELD) >= 1)")
		require.NoError(t, err)
	}
}

func TestMeasureDistinctQualifiedNumericAggregateHaving(t *testing.T) {
	for _, sourceName := range []string{"g.m", "MEASURE m IN g"} {
		for _, field := range []string{"v", "f"} {
			for _, inner := range []bool{false, true} {
				position := "outer"
				argument, suffix := field, "::FIELD"
				if inner {
					position, argument, suffix = "inner", field+"::FIELD", ""
				}
				t.Run(sourceName+"/"+field+"/"+position, func(t *testing.T) {
					statement, err := bydbql.Prepare("SELECT DISTINCT SUM(" + argument + ")" + suffix + " AS total FROM " + sourceName +
						" GROUP BY a HAVING (COUNT(" + field + "::FIELD) >= 1)")
					require.NoError(t, err)
					schema := distinctMeasureSchema()
					schema.TagFamilies[0].Tags = append(schema.TagFamilies[0].Tags, &databasev1.TagSpec{Name: field, Type: databasev1.TagType_TAG_TYPE_STRING})
					template, err := bydbql.NewTransformerWithSource(nil, nil).CompileMeasureWithSchemas(t.Context(), statement,
						map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: schema})
					require.NoError(t, err)
					bound, err := statement.Bind(nil)
					require.NoError(t, err)
					execution, err := template.Bind(bound, time.Unix(0, 0))
					require.NoError(t, err)
					source := distinctStringSource([]string{"x", "y", "z", "z"}, []string{"1", "2", "3", "4"})
					typ := vectorized.ColumnTypeInt64
					var column vectorized.Column = vectorized.NewColumnFromSlice(typ, []int64{1, 1, 1, 1})
					expected := [][]any{{int64(1)}, {int64(2)}}
					if field == "f" {
						typ = vectorized.ColumnTypeFloat64
						column = vectorized.NewColumnFromSlice(typ, []float64{1, 1, 1, 1})
						expected = [][]any{{float64(1)}, {float64(2)}}
					}
					source.schema = vectorized.NewBatchSchema(append(source.schema.Columns, vectorized.ColumnDef{Name: field, Role: vectorized.RoleField, Type: typ}))
					source.batches[0].Schema = source.schema
					source.batches[0].Columns = append(source.batches[0].Columns, column)
					rows, stats := runDistinct(t, execution, source)
					require.Equal(t, expected, rows)
					require.EqualValues(t, 3, stats.InputRows)
				})
			}
		}
	}
}

func TestQualifiedAggregateRejectsNonMeasureAndDuplicateQualification(t *testing.T) {
	for _, query := range []string{
		"SELECT SUM(v::FIELD) FROM STREAM m IN g",
		"SELECT SUM(v::FIELD) FROM TRACE m IN g",
		"SELECT SUM(v::FIELD) FROM PROPERTY m IN g",
		"SELECT DISTINCT SUM(v::FIELD)::TAG FROM g.m",
		"SELECT DISTINCT SUM(v::FIELD)::FIELD FROM g.m",
	} {
		_, err := bydbql.Prepare(query)
		require.Error(t, err, query)
	}
}
