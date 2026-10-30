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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

func TestMeasureHavingHiddenAggregateUsesCompletedGroups(t *testing.T) {
	execution := typedExecution(t, "SELECT host FROM MEASURE m IN g GROUP BY host HAVING SUM(v::FIELD) > 3 LIMIT 1 OFFSET 2")
	require.NotNil(t, execution.Plan)
	require.NotNil(t, execution.Plan.Aggregate)
	require.Len(t, execution.Plan.Aggregate.Calls, 1)
	require.Len(t, execution.Plan.Aggregate.Outputs, 2)
	require.EqualValues(t, 1, *execution.Plan.Limit)
	require.EqualValues(t, 2, execution.Plan.Offset)
	require.Zero(t, execution.Request.Offset)
	require.EqualValues(t, 4294967295, execution.Request.Limit)
}

func membershipTransformer() *bydbql.Transformer {
	return bydbql.NewTransformerWithSource(nil, typedSource{schema: &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "m", Group: "g"},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "labels", Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
			{Name: "nums", Type: databasev1.TagType_TAG_TYPE_INT_ARRAY},
		}}},
		Fields: []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}})
}

func TestMeasureContainsPreservesLegacyNativeRequest(t *testing.T) {
	for _, pair := range [][2]string{{"HAVING", "CONTAINS"}, {"NOT HAVING", "NOT CONTAINS"}} {
		for _, parameters := range [][]*modelv1.TagValue{
			{strParam("red")},
			{{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"red"}}}}},
			{{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"red", "blue"}}}}},
			{{Value: &modelv1.TagValue_Null{Null: 0}}},
		} {
			var reference *bydbql.MeasureExecution
			for index, spelling := range pair {
				statement, err := bydbql.Prepare("SELECT host FROM MEASURE m IN g WHERE labels " + spelling + " ?")
				require.NoError(t, err)
				template, err := membershipTransformer().CompileMeasure(context.Background(), statement)
				require.NoError(t, err)
				execution, err := template.BindParams(parameters, time.UnixMilli(0))
				require.NoError(t, err)
				require.Nil(t, execution.Plan)
				if index == 0 {
					reference = execution
				} else {
					require.True(t, proto.Equal(reference.Request, execution.Request))
				}
			}
		}
	}
}

func TestMeasureContainsComposesWithResidualAndGroupedHaving(t *testing.T) {
	for _, spelling := range []string{"HAVING", "CONTAINS", "NOT HAVING", "NOT CONTAINS"} {
		statement, err := bydbql.Prepare("SELECT host, SUM(v) AS total FROM g.m WHERE labels " + spelling + " (?) AND v::FIELD > ? " +
			"GROUP BY host HAVING SUM(v::FIELD) > ? LIMIT ?")
		require.NoError(t, err)
		require.Equal(t, 4, statement.NumPlaceholders())
		template, err := membershipTransformer().CompileMeasure(context.Background(), statement)
		require.NoError(t, err)
		execution, err := template.BindParams([]*modelv1.TagValue{
			{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"red", "blue"}}}},
			intParam(0), intParam(3), intParam(1),
		}, time.UnixMilli(0))
		require.NoError(t, err)
		require.NotNil(t, execution.Plan.Filter)
		require.NotNil(t, execution.Plan.Having)
		require.Len(t, execution.Plan.Aggregate.Calls, 1)
		require.Len(t, execution.Plan.Having.Outputs, 2)
		require.Nil(t, execution.Plan.NativeAggregate)
	}
}

func TestMeasureHavingSharesRepeatedCallsAndKeepsNativePushdown(t *testing.T) {
	base := " FROM g.m WHERE host IN ('a','b') AND time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:06Z' GROUP BY host"
	plain := nativeAggregateExecution(t, "SELECT host, SUM(f) AS total, AVG(f) AS mean"+base, nativeAggregateSchema())
	filtered := nativeAggregateExecution(t, "SELECT host, SUM(f) AS total, AVG(f) AS mean, SUM(f) AS again"+base+
		" HAVING SUM(f::FIELD)>3 AND AVG(f::FIELD)>1 AND SUM(f::FIELD)<100", nativeAggregateSchema())
	require.NotNil(t, filtered.Plan.Having)
	require.NotNil(t, filtered.Plan.NativeAggregate)
	require.Len(t, filtered.Plan.Aggregate.Calls, 2)
	require.Len(t, filtered.Plan.Having.Outputs, 4)
	require.Len(t, filtered.Plan.Aggregate.Outputs, 4)
	require.Len(t, filtered.Plan.NativeAggregate.Leaves, len(plain.Plan.NativeAggregate.Leaves))
	for index, leaf := range plain.Plan.NativeAggregate.Leaves {
		require.True(t, proto.Equal(leaf.Request, filtered.Plan.NativeAggregate.Leaves[index].Request))
	}
}

func TestMeasureHavingResolvesHiddenGroupsAndThreeValuedPredicates(t *testing.T) {
	hiddenKey := typedExecution(t, "SELECT SUM(v) AS total FROM g.m GROUP BY host HAVING host = 'b'")
	require.Len(t, hiddenKey.Plan.Aggregate.Outputs, 2)
	require.Len(t, hiddenKey.Plan.Having.Outputs, 1)
	require.Equal(t, "total", hiddenKey.Plan.Having.Outputs[0].Output.Name)
	require.Equal(t, hiddenKey.Plan.Aggregate.Outputs[1].Column, hiddenKey.Plan.Having.Predicate.Args[0].Source)
	for _, predicate := range []string{
		"TRUE", "FALSE", "NULL", "SUM(v::FIELD) IS NULL", "COALESCE(SUM(v::FIELD),0)>3",
		"CASE WHEN COUNT(v::FIELD)=0 THEN NULL ELSE SUM(v::FIELD)>3 END", "SUM(v)::FIELD > 3", "SUM(v::FIELD)::FLOAT > 3",
	} {
		execution := typedExecution(t, "SELECT host AS node, SUM(v) AS total FROM g.m GROUP BY host HAVING "+predicate)
		require.Equal(t, ql.ExpressionBool, execution.Plan.Having.Predicate.Type)
		require.Len(t, execution.Plan.Having.Outputs, 2)
	}
	scalar := typedExecution(t, "SELECT SUM(v) AS total FROM g.m HAVING SUM(v::FIELD)>3")
	require.Empty(t, scalar.Plan.Aggregate.GroupBy)
	require.Len(t, scalar.Plan.Aggregate.Calls, 1)
	grouped := typedExecution(t, "SELECT host FROM g.m GROUP BY host HAVING host='a'")
	require.Empty(t, grouped.Plan.Aggregate.Calls)
}

func TestMeasureHavingValidatesScopeAndClauseOrderBeforeDispatch(t *testing.T) {
	queries := []string{
		"SELECT host AS node, SUM(v) AS total FROM g.m GROUP BY host HAVING node='a'",
		"SELECT host, SUM(v) AS total FROM g.m GROUP BY host HAVING total>3",
		"SELECT host, SUM(v) FROM g.m GROUP BY host HAVING v>3",
		"SELECT host, SUM(v) FROM g.m GROUP BY host HAVING missing>3",
		"SELECT host, SUM(v) FROM g.m GROUP BY host HAVING SUM(missing)>3",
		"SELECT host, SUM(v) FROM g.m GROUP BY host HAVING SUM(SUM(v))>3",
		"SELECT host, SUM(v) FROM g.m GROUP BY host HAVING SUM(v)",
		"SELECT host, SUM(v) FROM g.m GROUP BY host HAVING SUM(v)='three'",
		"SELECT host FROM g.m HAVING TRUE",
		"SELECT host FROM g.m HAVING SUM(v)>3",
		"SELECT host, SUM(v) FROM g.m HAVING SUM(v)>3 GROUP BY host",
		"SELECT SUM(v) FROM g.m LIMIT 1 HAVING SUM(v)>3",
		"SELECT * FROM STREAM s IN g HAVING TRUE",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			statement, err := bydbql.Prepare(query)
			if err == nil {
				var template *bydbql.MeasureTemplate
				template, err = typedTransformer().CompileMeasure(context.Background(), statement)
				if err == nil {
					_, err = template.BindParams(nil, time.UnixMilli(0))
				}
			}
			require.Error(t, err)
		})
	}
}

func TestMeasureHavingCachedBindingsAreIndependentAndOrdered(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT host AS node, SUM(v) AS total FROM g.m TIME BETWEEN ? AND ? " +
		"WHERE v::FIELD > ? GROUP BY host HAVING SUM(v::FIELD)>? AND host IN (?) LIMIT ? OFFSET ?")
	require.NoError(t, err)
	require.Equal(t, 7, statement.NumPlaceholders())
	template, err := typedTransformer().CompileMeasure(context.Background(), statement)
	require.NoError(t, err)
	retained := template.EstimatedSize()
	var workers sync.WaitGroup
	for index := int64(0); index < 8; index++ {
		workers.Go(func() {
			parameters := []*modelv1.TagValue{
				strParam("1970-01-01T00:00:00Z"), strParam("1970-01-01T00:00:09Z"),
				intParam(index), intParam(index + 3),
				{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"a", "b"}}}},
				intParam(index + 1), intParam(index),
			}
			execution, bindErr := template.BindParams(parameters, time.UnixMilli(index))
			require.NoError(t, bindErr)
			require.Equal(t, index, execution.Plan.Filter.Args[1].Literal.Int64)
			require.Equal(t, index+3, execution.Plan.Having.Predicate.Args[0].Args[1].Literal.Int64)
			require.Len(t, execution.Plan.Having.Predicate.Args[1].Args, 3)
			require.EqualValues(t, index+1, *execution.Plan.Limit)
			require.EqualValues(t, index, execution.Plan.Offset)
			execution.Plan.Having.Predicate.Args[0].Args[1].Literal.Int64 = -999
			execution.Plan.Having.Outputs[0].Source.Name = "poison"
			execution.Plan.Aggregate.Outputs[0].Column.Name = "poison"
		})
	}
	workers.Wait()
	require.Equal(t, retained, template.EstimatedSize())
}

func TestMeasureContainsMixedResidualPreservesLiteralConversion(t *testing.T) {
	tests := []struct {
		name, source, rhs string
		parameters        []*modelv1.TagValue
		want              ql.ExpressionScalar
		typ               ql.ExpressionType
		invalid           bool
	}{
		{name: "scalar", source: "labels", rhs: "?", parameters: []*modelv1.TagValue{strParam("red")}, want: ql.ExpressionScalar{String: "red"}, typ: ql.ExpressionString},
		{name: "one-array-collapses", source: "labels", rhs: "?", parameters: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"red"}}}},
		}, want: ql.ExpressionScalar{String: "red"}, typ: ql.ExpressionString},
		{
			name: "list-keeps-array", source: "labels", rhs: "(?)", parameters: []*modelv1.TagValue{strParam("red")},
			want: ql.ExpressionScalar{Strings: []string{"red"}}, typ: ql.ExpressionStringArray,
		},
		{
			name: "array-expands-in-list", source: "labels", rhs: "('first',?,'last')", parameters: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"red", "blue"}}}},
			},
			want: ql.ExpressionScalar{Strings: []string{"first", "red", "blue", "last"}}, typ: ql.ExpressionStringArray,
		},
		{name: "double-quoted-string", source: "labels", rhs: `"red"`, want: ql.ExpressionScalar{String: "red"}, typ: ql.ExpressionString},
		{name: "positive-integer", source: "nums", rhs: "+2", want: ql.ExpressionScalar{Int64: 2}, typ: ql.ExpressionInt},
		{name: "negative-integer", source: "nums", rhs: "-2", want: ql.ExpressionScalar{Int64: -2}, typ: ql.ExpressionInt},
		{name: "string-source-coercion", source: "labels", rhs: "2", want: ql.ExpressionScalar{String: "2"}, typ: ql.ExpressionString},
		{name: "integer-source-coercion", source: "nums", rhs: "'2'", want: ql.ExpressionScalar{Int64: 2}, typ: ql.ExpressionInt},
		{
			name: "null-scalar", source: "labels", rhs: "?", parameters: []*modelv1.TagValue{{Value: &modelv1.TagValue_Null{Null: 0}}},
			want: ql.ExpressionScalar{Null: true}, typ: ql.ExpressionNull,
		},
		{name: "null-list", source: "labels", rhs: "(?)", parameters: []*modelv1.TagValue{{Value: &modelv1.TagValue_Null{Null: 0}}}, invalid: true},
		{name: "empty-array-parameter", source: "labels", rhs: "?", parameters: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{}}},
		}, invalid: true},
		{name: "bad-integer", source: "nums", rhs: "'two'", invalid: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, spelling := range []string{"HAVING", "CONTAINS", "NOT HAVING", "NOT CONTAINS"} {
				for _, tail := range []string{" AND v>0", " AND v::FIELD>0"} {
					statement, err := bydbql.Prepare("SELECT host FROM g.m WHERE " + test.source + " " + spelling + " " + test.rhs + tail)
					require.NoError(t, err)
					template, err := membershipTransformer().CompileMeasure(context.Background(), statement)
					require.NoError(t, err)
					execution, err := template.BindParams(test.parameters, time.UnixMilli(0))
					if test.invalid {
						require.Error(t, err)
						continue
					}
					require.NoError(t, err)
					require.Equal(t, test.typ, execution.Plan.Filter.Args[0].Args[1].Type)
					require.Equal(t, test.want, execution.Plan.Filter.Args[0].Args[1].Literal)
				}
			}
		})
	}
}

func TestMeasureHavingMutableBindingWalksClauseAndMembership(t *testing.T) {
	query := "SELECT SUM(v) AS total FROM g.m TIME > ? WHERE labels CONTAINS (?) AND v::FIELD>? HAVING SUM(v)>? LIMIT ? OFFSET ?"
	grammar, err := bydbql.ParseQuery(query)
	require.NoError(t, err)
	parameters := []*modelv1.TagValue{strParam("1970-01-01T00:00:00Z"), strParam("red"), intParam(1), intParam(3), intParam(2), intParam(0)}
	require.NoError(t, bydbql.BindParams(grammar, parameters))
	require.False(t, grammar.Select.Having.Expression.Args[1].Param)
	require.EqualValues(t, 3, grammar.Select.Having.Expression.Args[1].Literal.Int64)
	require.False(t, grammar.Select.Where.Expression.Args[0].Args[1].Param)
}

func TestMeasureHavingWithoutCallsKeepsGroupingLocal(t *testing.T) {
	execution := nativeAggregateExecution(t, "SELECT host FROM g.m WHERE host IN ('a','b') GROUP BY host HAVING host='a'", nativeAggregateSchema())
	require.NotNil(t, execution.Plan.Aggregate)
	require.Empty(t, execution.Plan.Aggregate.Calls)
	require.Nil(t, execution.Plan.NativeAggregate)
}
