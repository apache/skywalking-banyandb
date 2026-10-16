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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestMeasureExpressionProjection(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT v + 2 AS shifted FROM g.m WHERE time >= ? AND time < ?")
	require.NoError(t, err)
	schema := &databasev1.Measure{Fields: []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT}}}
	transformer := bydbql.NewTransformerWithSource(nil, nil)
	template, err := transformer.CompileMeasureWithSchemas(context.Background(), statement,
		map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: schema})
	require.NoError(t, err)
	bound, err := statement.Bind([]*modelv1.TagValue{strParam("1970-01-01T00:00:00Z"), strParam("1970-01-01T00:00:01Z")})
	require.NoError(t, err)
	execution, err := template.Bind(bound, time.Unix(0, 0))
	require.NoError(t, err)
	require.NotNil(t, execution.Plan)
	require.Len(t, execution.Plan.Expressions.Outputs, 1)
	require.Equal(t, "shifted", execution.Plan.Expressions.Outputs[0].Column.Name)
	require.Equal(t, ql.ExpressionInt, execution.Plan.Expressions.Outputs[0].Expr.Type)
	require.Equal(t, []string{"v"}, execution.Request.FieldProjection.Names)
}

func TestMeasureExpressionGrammarAndOutputTypes(t *testing.T) {
	cases := []struct {
		expression string
		name       string
		typ        ql.ExpressionType
		role       vectorized.ColumnRole
	}{
		{"v::FIELD + 2", "expr_1", ql.ExpressionInt, vectorized.RoleField},
		{"v::FIELD + 1.25e-2 AS mixed", "mixed", ql.ExpressionFloat, vectorized.RoleField},
		{".5 + 1E2 AS decimal", "decimal", ql.ExpressionFloat, vectorized.RoleField},
		{"1+2*3 AS constant", "constant", ql.ExpressionInt, vectorized.RoleField},
		{"-9223372036854775808 AS minimum", "minimum", ql.ExpressionInt, vectorized.RoleField},
		{"CAST(NULL AS BIGINT) AS typed_null", "typed_null", ql.ExpressionInt, vectorized.RoleField},
		{"v::DOUBLE AS widened", "widened", ql.ExpressionFloat, vectorized.RoleField},
		{"CAST(v::FIELD IN () AS INT) AS absent", "absent", ql.ExpressionInt, vectorized.RoleField},
		{"CAST(v::FIELD NOT IN (2, NULL) AS STRING) AS member", "member", ql.ExpressionString, vectorized.RoleField},
		{"CAST(NOT (v::FIELD = 0) AND (v::FIELD IS NOT NULL OR FALSE) AS INT) AS truth", "truth", ql.ExpressionInt, vectorized.RoleField},
		{"CASE WHEN v::FIELD = 0 THEN 0 ELSE 8 / v::FIELD END AS ratio", "ratio", ql.ExpressionInt, vectorized.RoleField},
		{"CASE v::FIELD WHEN 0 THEN 2 WHEN 2 THEN 4 ELSE NULL END AS choice", "choice", ql.ExpressionInt, vectorized.RoleField},
		{"COALESCE(v::FIELD, -1) AS filled", "filled", ql.ExpressionInt, vectorized.RoleField},
		{"CAST(0 AS TIMESTAMP) AS epoch", "epoch", ql.ExpressionTimestamp, vectorized.RoleTag},
		{"CAST('2026-07-06T18:00:00+08:00' AS TIMESTAMP) AS clock", "clock", ql.ExpressionTimestamp, vectorized.RoleTag},
		{"CAST(time AS BIGINT) AS nanos", "nanos", ql.ExpressionInt, vectorized.RoleField},
		{"'new constant' AS text", "text", ql.ExpressionString, vectorized.RoleField},
		{"'host' AS node", "node", ql.ExpressionString, vectorized.RoleTag},
		{"'v'::FIELD + 2 AS quoted", "quoted", ql.ExpressionInt, vectorized.RoleField},
	}
	for _, item := range cases {
		t.Run(item.expression, func(t *testing.T) {
			execution := typedExecution(t, "SELECT "+item.expression+" FROM g.m")
			if execution.Plan.Expressions == nil {
				require.Len(t, execution.Plan.Slots, 1)
				require.Equal(t, item.role, execution.Plan.Slots[0].Output.Role)
				return
			}
			require.Len(t, execution.Plan.Expressions.Outputs, 1)
			output := execution.Plan.Expressions.Outputs[0]
			require.Equal(t, item.typ, output.Expr.Type)
			require.Equal(t, item.name, output.Column.Name)
			require.Equal(t, item.role, output.Column.Role)
			if item.typ == ql.ExpressionTimestamp {
				require.Equal(t, "__ql", output.Column.TagFamily)
			}
			require.NotEmpty(t, execution.Plan.Slots)
		})
	}
}

func TestMeasureExpressionBindingIsFreshAndOrdered(t *testing.T) {
	query := "SELECT CAST(? AS DOUBLE) + v::FIELD AS scaled, CAST(v::FIELD IN (?) AS INT) AS member FROM g.m WHERE time >= ? AND time < ? LIMIT ?"
	statement, err := bydbql.Prepare(query)
	require.NoError(t, err)
	require.Equal(t, 5, statement.NumPlaceholders())
	template, err := typedTransformer().CompileMeasure(context.Background(), statement)
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, amount := range []string{"1.5", "2.75"} {
				bound, bindErr := statement.Bind([]*modelv1.TagValue{
					strParam(amount),
					{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{0, 2}}}},
					strParam("1970-01-01T00:00:00Z"), strParam("1970-01-01T00:00:01Z"), intParam(2),
				})
				require.NoError(t, bindErr)
				execution, executionErr := template.Bind(bound, time.Unix(0, 0))
				require.NoError(t, executionErr)
				expression := execution.Plan.Expressions.Outputs[0].Expr
				require.Equal(t, amount, expression.Args[0].Args[0].Literal.String)
				require.Len(t, execution.Plan.Expressions.Outputs[1].Expr.Args[0].Args, 3)
				require.EqualValues(t, 2, *execution.Plan.Limit)
				expression.Args[0].Args[0].Literal.String = "changed"
			}
		}()
	}
	wg.Wait()
	empty, err := statement.Bind([]*modelv1.TagValue{
		strParam("1.5"),
		{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{}}},
		strParam("1970-01-01T00:00:00Z"), strParam("1970-01-01T00:00:01Z"), intParam(2),
	})
	require.NoError(t, err)
	execution, err := template.Bind(empty, time.Unix(0, 0))
	require.NoError(t, err)
	require.Len(t, execution.Plan.Expressions.Outputs[1].Expr.Args[0].Args, 1)
}

func TestMeasureExpressionParametersPreserveExistingWireTypes(t *testing.T) {
	cases := []struct {
		parameter *modelv1.TagValue
		query     string
		typ       ql.ExpressionType
		role      vectorized.ColumnRole
	}{
		{
			parameter: &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte{1, 2}}},
			query:     "SELECT ? AS payload FROM g.m", typ: ql.ExpressionBytes, role: vectorized.RoleField,
		},
		{
			parameter: &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{2, 4}}}},
			query:     "SELECT ? AS values FROM g.m", typ: ql.ExpressionIntArray, role: vectorized.RoleTag,
		},
		{
			parameter: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"a", "b"}}}},
			query:     "SELECT ? AS values FROM g.m", typ: ql.ExpressionStringArray, role: vectorized.RoleTag,
		},
		{
			parameter: &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(time.Unix(0, 1234))}},
			query:     "SELECT ? AS clock FROM g.m", typ: ql.ExpressionTimestamp, role: vectorized.RoleTag,
		},
	}
	for _, item := range cases {
		execution := typedExecution(t, item.query, item.parameter)
		output := execution.Plan.Expressions.Outputs[0]
		require.Equal(t, item.typ, output.Expr.Type)
		require.Equal(t, item.role, output.Column.Role)
	}
}

func TestMeasureExpressionsRejectUnsupportedCompositions(t *testing.T) {
	cases := []string{
		"SELECT TRUE FROM g.m",
		"SELECT NULL FROM g.m",
		"SELECT v + 1 AS repeated, f + 1 AS repeated FROM g.m",
		"SELECT host + 1 FROM g.m",
		"SELECT CAST(host AS BYTES) FROM g.m",
		"SELECT v + 1 FROM g.m GROUP BY host",
		"SELECT SUM(v), v + 1 FROM g.m",
		"SELECT v + 1 AS shifted FROM g.m ORDER BY shifted",
		"SELECT v + 1 FROM g.m WHERE v = 1",
		"SELECT \"unknown\", v + 1 FROM g.m",
		"SELECT v + 1 AS hidden FROM g.m WHERE hidden = 1",
	}
	for _, query := range cases {
		t.Run(query, func(t *testing.T) {
			statement, err := bydbql.Prepare(query)
			if err != nil {
				return
			}
			template, err := typedTransformer().CompileMeasure(context.Background(), statement)
			if err == nil {
				bound, bindErr := statement.Bind(nil)
				require.NoError(t, bindErr)
				_, err = template.Bind(bound, time.Unix(0, 0))
			}
			require.ErrorIs(t, err, bydbql.ErrInvalidMeasureQuery)
		})
	}
	_, err := bydbql.Prepare("SELECT v + 1 FROM STREAM m IN g")
	require.Error(t, err)
	for _, expression := range []string{strings.Repeat("NOT ", 65) + "TRUE", "v" + strings.Repeat(" + 1", 65)} {
		_, err = bydbql.Prepare("SELECT CAST(" + expression + " AS INT) FROM g.m")
		require.ErrorContains(t, err, "nesting")
	}
}

func TestMeasureExpressionMutableBindingOrderAndListExpansion(t *testing.T) {
	query := "SELECT CAST(? AS DOUBLE) AS amount, CAST(v::FIELD IN (?) AS INT) AS member FROM g.m TIME > ? WHERE host = ? LIMIT ?"
	grammar, err := bydbql.ParseQuery(query)
	require.NoError(t, err)
	err = bydbql.BindParams(grammar, []*modelv1.TagValue{
		strParam("1.5"),
		{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{2, 4}}}},
		strParam("1970-01-01T00:00:01Z"), strParam("a"), intParam(3),
	})
	require.NoError(t, err)
	columns := grammar.Select.Projection.Columns
	require.Equal(t, "1.5", columns[0].Expression.Args[0].Literal.String)
	require.False(t, columns[0].Expression.Args[0].Param)
	require.Len(t, columns[1].Expression.Args[0].Args, 3)
	require.EqualValues(t, 4, columns[1].Expression.Args[0].Args[2].Literal.Int64)
	require.Equal(t, "1970-01-01T00:00:01Z", *grammar.Select.Time.Value.String)
	require.Equal(t, "a", *grammar.Select.Where.Expr.Left.Left.Binary.Tail.Compare.Value.String)
	require.Equal(t, 3, grammar.Select.Limit.Value)
	require.Error(t, bydbql.BindParams(grammar, nil))
}

func TestMeasureExpressionQuotedNamesAndLegacyOperators(t *testing.T) {
	schema := &databasev1.Measure{Fields: []*databasev1.FieldSpec{
		{Name: "metric-value", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		{Name: "rate*total", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
	}}
	statement, err := bydbql.Prepare("SELECT metric-value::FIELD, rate*total::FIELD, metric-value::FIELD - 2 AS shifted FROM g.m")
	require.NoError(t, err)
	template, err := bydbql.NewTransformerWithSource(nil, nil).CompileMeasureWithSchemas(context.Background(), statement,
		map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: schema})
	require.NoError(t, err)
	bound, err := statement.Bind(nil)
	require.NoError(t, err)
	execution, err := template.Bind(bound, time.Unix(0, 0))
	require.NoError(t, err)
	require.Len(t, execution.Plan.Expressions.Outputs, 3)
	require.Equal(t, "metric-value", execution.Plan.Expressions.Outputs[0].Column.Name)
	require.Equal(t, "rate*total", execution.Plan.Expressions.Outputs[1].Column.Name)
	require.Equal(t, []string{"metric-value", "rate*total"}, execution.Request.FieldProjection.Names)
	quoted := typedExecution(t, "SELECT 'text' AS constant, \"host\" AS node, v + 2 AS shifted FROM g.m")
	require.Equal(t, ql.ExpressionString, quoted.Plan.Expressions.Outputs[0].Expr.Type)
	require.Equal(t, vectorized.RoleTag, quoted.Plan.Expressions.Outputs[1].Column.Role)
}

func TestMeasureExpressionTimestampDomainAndSourceTag(t *testing.T) {
	schema := &databasev1.Measure{TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
		{Name: "created", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
		{Name: "TRUE", Type: databasev1.TagType_TAG_TYPE_INT},
	}}}}
	statement, err := bydbql.Prepare("SELECT CAST(created::TAG AS BIGINT) AS nanos, TRUE::TAG + 1 AS legacy FROM g.m")
	require.NoError(t, err)
	template, err := bydbql.NewTransformerWithSource(nil, nil).CompileMeasureWithSchemas(context.Background(), statement,
		map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: schema})
	require.NoError(t, err)
	bound, err := statement.Bind(nil)
	require.NoError(t, err)
	execution, err := template.Bind(bound, time.Unix(0, 0))
	require.NoError(t, err)
	source := execution.Plan.Expressions.Outputs[0].Expr.Args[0]
	require.Equal(t, ql.ExpressionTimestamp, source.Type)
	require.Equal(t, vectorized.ColumnTypeInt64, source.Source.Type)
	require.Equal(t, vectorized.ColumnTypeTagValue, execution.Plan.Slots[0].Source.Type)

	timestampStatement, err := bydbql.Prepare("SELECT ? AS timestamp FROM g.m")
	require.NoError(t, err)
	for _, instant := range []time.Time{time.Unix(0, -9223372036854775807-1), time.Unix(0, 9223372036854775807)} {
		_, err = timestampStatement.Bind([]*modelv1.TagValue{{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(instant)}}})
		require.NoError(t, err)
	}
	for _, instant := range []time.Time{time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC), time.Date(2500, time.January, 1, 0, 0, 0, 0, time.UTC)} {
		_, err = timestampStatement.Bind([]*modelv1.TagValue{{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(instant)}}})
		require.ErrorContains(t, err, "nanosecond range")
	}
	legacy, err := bydbql.Prepare("SELECT host FROM g.m WHERE host IN (?)")
	require.NoError(t, err)
	_, err = legacy.Bind([]*modelv1.TagValue{{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{}}}})
	require.ErrorContains(t, err, "array parameter must not be empty")
}

func TestMeasureExpressionEntityBindingsUseNativeAlternatives(t *testing.T) {
	schema := &databasev1.Measure{
		Entity: &databasev1.Entity{TagNames: []string{"host", "region"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
		Fields: []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}
	for _, predicate := range []string{"host IN ('a', 'b') AND region IN ('east', 'west')", "host = 'a' AND host = 'b'"} {
		statement, err := bydbql.Prepare("SELECT v + 2 AS shifted FROM g.m WHERE " + predicate)
		require.NoError(t, err)
		template, err := bydbql.NewTransformerWithSource(nil, nil).CompileMeasureWithSchemas(context.Background(), statement,
			map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: schema})
		require.NoError(t, err)
		bound, err := statement.Bind(nil)
		require.NoError(t, err)
		execution, err := template.Bind(bound, time.Unix(0, 0))
		require.NoError(t, err)
		require.Nil(t, execution.Plan.NativeAggregate)
		if strings.Contains(predicate, " IN ") {
			require.False(t, execution.Plan.Empty)
			require.Equal(t, modelv1.LogicalExpression_LOGICAL_OP_OR, execution.Request.Criteria.GetLe().GetOp())
		} else {
			require.True(t, execution.Plan.Empty)
		}
	}
}
