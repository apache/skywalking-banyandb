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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

func TestMeasureFilterHiddenFieldAndFreshBinding(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT rid FROM MEASURE m IN g WHERE v > CAST(? AS DOUBLE) LIMIT 1")
	require.NoError(t, err)
	require.Equal(t, 1, statement.NumPlaceholders())
	template, err := typedTransformer().CompileMeasure(context.Background(), statement)
	require.NoError(t, err)
	for _, threshold := range []string{"1", "3"} {
		bound, bindErr := statement.Bind([]*modelv1.TagValue{strParam(threshold)})
		require.NoError(t, bindErr)
		execution, bindErr := template.Bind(bound, time.Unix(0, 0))
		require.NoError(t, bindErr)
		require.NotNil(t, execution.Plan)
		require.Len(t, execution.Plan.Slots, 1)
		require.Equal(t, "rid", execution.Plan.Slots[0].Output.Name)
		require.Equal(t, []string{"v"}, execution.Request.FieldProjection.Names)
		require.EqualValues(t, 4294967295, execution.Request.Limit)
		require.EqualValues(t, 1, *execution.Plan.Limit)
	}
}

func TestMeasureFilterLegacyComparisonActivatesTypedPlan(t *testing.T) {
	execution := typedExecution(t, "SELECT rid FROM MEASURE m IN g WHERE v > ?", intParam(1))
	require.NotNil(t, execution.Plan)
	require.NotNil(t, execution.Plan.Filter)
	require.Equal(t, ql.ExpressionBool, execution.Plan.Filter.Type)
	require.Nil(t, execution.Request.Criteria)
	require.Equal(t, []string{"v"}, execution.Request.FieldProjection.Names)
}

func TestMeasureFilterMixedTimeOrKeepsFullResidual(t *testing.T) {
	execution := typedExecution(t, "SELECT rid FROM g.m TIME BETWEEN '1970-01-01T00:00:01Z' AND '1970-01-01T00:00:05Z' "+
		"WHERE (time < '1970-01-01T00:00:02Z' AND v > 2) OR (time >= '1970-01-01T00:00:04Z' AND f > 0.5)")
	require.NotNil(t, execution.Plan.Filter)
	require.True(t, execution.Plan.Residual)
	require.Equal(t, []ql.MillisRange{{Begin: 1000, End: 2000}, {Begin: 4000, End: 5001}}, execution.Plan.Ranges)
	require.Nil(t, execution.Request.Criteria)
	require.Nil(t, execution.Plan.NativeAggregate)
}

func TestMeasureFilterNecessaryTagPushdownAndLocalAggregation(t *testing.T) {
	execution := typedExecution(t, "SELECT SUM(v) AS total, COUNT(v) AS count FROM g.m "+
		"WHERE host IN ('a', 'b') AND v > CAST(? AS DOUBLE)", strParam("1"))
	require.NotNil(t, execution.Plan.Filter)
	require.Nil(t, execution.Plan.NativeAggregate)
	require.Nil(t, execution.Request.Agg)
	require.NotNil(t, execution.Plan.Aggregate)
	require.NotNil(t, execution.Request.Criteria.GetCondition())
	require.Equal(t, "host", execution.Request.Criteria.GetCondition().Name)
	require.Equal(t, []string{"a", "b"}, execution.Request.Criteria.GetCondition().GetValue().GetStrArray().Value)
	mixed := typedExecution(t, "SELECT rid FROM g.m WHERE host = 'a' OR v > 1")
	require.NotNil(t, mixed.Plan.Filter)
	require.Nil(t, mixed.Request.Criteria)
}

func TestMeasureFilterTimeClockAndParameterOrdering(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT CAST(? AS INT) AS amount FROM g.m TIME BETWEEN ? AND ? " +
		"WHERE (time >= now() - INTERVAL '1s' AND time <= now()) AND v > CAST(? AS DOUBLE) LIMIT ? OFFSET ?")
	require.NoError(t, err)
	require.Equal(t, 6, statement.NumPlaceholders())
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	var group sync.WaitGroup
	for index := int64(0); index < 8; index++ {
		group.Go(func() {
			now := time.UnixMilli(3000 + index*1000)
			execution, bindErr := template.BindParams([]*modelv1.TagValue{
				intParam(index), strParam("1970-01-01T00:00:00Z"), strParam("1970-01-01T00:01:00Z"), strParam("1.5"), intParam(2), intParam(1),
			}, now)
			require.NoError(t, bindErr)
			require.Equal(t, []ql.MillisRange{{Begin: now.UnixMilli() - 1000, End: now.UnixMilli() + 1}}, execution.Plan.Ranges)
			require.NotNil(t, execution.Plan.Filter)
			require.Equal(t, index, execution.Plan.Expressions.Outputs[0].Expr.Args[0].Literal.Int64)
			require.EqualValues(t, 2, *execution.Plan.Limit)
			require.EqualValues(t, 1, execution.Plan.Offset)
			execution.Plan.Filter.Op = "changed"
		})
	}
	group.Wait()
}

func TestMeasureFilterGrammarTypingAndLegacyBoundaries(t *testing.T) {
	for _, predicate := range []string{
		"TRUE", "FALSE", "NULL", "CAST(NULL AS BOOL)", "v::FIELD + 2 > 3", "f > .5e0", "NOT (v::FIELD = 0)", "v::FIELD IS NULL", "v::FIELD IS NOT NULL",
		"v::FIELD IN ()", "v::FIELD NOT IN (2,NULL)", "CASE WHEN v::FIELD = 0 THEN TRUE ELSE 8 / v::FIELD > 1 END",
		"COALESCE(v::FIELD,0)>1", "clash::FIELD > 1 AND clash::TAG = 'a'", "default.rid::TAG > 0",
		"(time >= '1970-01-01T00:00:02Z' OR host = 'a')", "time >= CAST(? AS TIMESTAMP) AND v::FIELD > 0",
	} {
		t.Run(predicate, func(t *testing.T) {
			var parameters []*modelv1.TagValue
			if strings.Contains(predicate, "?") {
				parameters = []*modelv1.TagValue{strParam("1970-01-01T00:00:02Z")}
			}
			execution := typedExecution(t, "SELECT rid FROM g.m WHERE "+predicate, parameters...)
			require.NotNil(t, execution.Plan.Filter)
			require.Equal(t, ql.ExpressionBool, execution.Plan.Filter.Type)
			require.Nil(t, execution.Request.Agg)
		})
	}
	legacy := typedExecution(t, "SELECT rid FROM MEASURE m IN g WHERE host = ?", strParam("a"))
	require.Nil(t, legacy.Plan)
	ambiguousLegacy := typedExecution(t, "SELECT rid FROM MEASURE m IN g WHERE clash = 'a'")
	require.Nil(t, ambiguousLegacy.Plan)
	for _, source := range []string{"STREAM s IN g", "TRACE t IN g", "PROPERTY p IN g"} {
		_, err := bydbql.Prepare("SELECT * FROM " + source + " WHERE v > CAST(? AS INT)")
		require.Error(t, err)
	}
}

func TestMeasureFilterInvalidReferencesAndResultTypes(t *testing.T) {
	for _, predicate := range []string{"v::FIELD + 1", "clash > 1", "missing > 1", "v::FIELD = 'one'", "v::FIELD + now() > 0", "v::FIELD > 1 + INTERVAL '1s'"} {
		t.Run(predicate, func(t *testing.T) {
			statement, err := bydbql.Prepare("SELECT rid AS seen FROM g.m WHERE " + predicate)
			if err != nil {
				return
			}
			template, err := typedTransformer().CompileMeasure(t.Context(), statement)
			if err == nil {
				_, err = template.BindParams(nil, time.Unix(0, 0))
			}
			require.Error(t, err)
		})
	}
	_, err := bydbql.Prepare("SELECT rid FROM g.m WHERE " + strings.Repeat("NOT ", 65) + "TRUE")
	require.ErrorContains(t, err, "nesting")
	long, err := bydbql.Prepare("SELECT rid FROM MEASURE m IN g WHERE v > 0" + strings.Repeat(" AND v > 0", 100))
	require.NoError(t, err)
	_, err = typedTransformer().CompileMeasure(t.Context(), long)
	require.ErrorContains(t, err, "nesting")
}

func TestMeasureFilterEmptyBoundMembershipAndMutableBind(t *testing.T) {
	for _, values := range [][]int64{nil, {2, 4}} {
		statement, err := bydbql.Prepare("SELECT rid FROM g.m WHERE v::FIELD IN (?)")
		require.NoError(t, err)
		template, err := typedTransformer().CompileMeasure(t.Context(), statement)
		require.NoError(t, err)
		parameter := &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: values}}}
		execution, err := template.BindParams([]*modelv1.TagValue{parameter}, time.Unix(0, 0))
		require.NoError(t, err)
		require.Len(t, execution.Plan.Filter.Args, len(values)+1)
		grammar, err := bydbql.ParseQuery("SELECT CAST(? AS INT) AS copy FROM g.m TIME > ? WHERE v::FIELD IN (?) LIMIT ?")
		require.NoError(t, err)
		require.NoError(t, bydbql.BindParams(grammar, []*modelv1.TagValue{intParam(1), strParam("1970-01-01T00:00:00Z"), parameter, intParam(2)}))
		require.Len(t, grammar.Select.Where.Expression.Args, len(values)+1)
	}
}
