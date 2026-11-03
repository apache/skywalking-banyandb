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

package bydbql_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestMeasureFilterEmptyMembershipBindingSkipsInput(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT COUNT(DISTINCT rid) AS n FROM g.m " +
		"WHERE time >= ? AND time < ? AND v::FIELD > 0 AND host IN (?)")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	for _, sample := range []struct {
		parameter *modelv1.TagValue
		empty     bool
	}{
		{parameter: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{}}}, empty: true},
		{parameter: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"a"}}}}},
		{parameter: &modelv1.TagValue{Value: &modelv1.TagValue_Null{Null: 0}}, empty: true},
		{parameter: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{}}}, empty: true},
	} {
		execution, bindErr := template.BindParams([]*modelv1.TagValue{
			strParam("1970-01-01T00:00:00Z"), strParam("1970-01-02T00:00:00Z"), sample.parameter,
		}, time.Unix(0, 0))
		require.NoError(t, bindErr)
		require.Equal(t, sample.empty, execution.Plan.Empty)
		require.NotNil(t, execution.Plan.Aggregate)
	}
}

func TestMeasureFilterEmptyProofPreservesBooleanAndErrorSemantics(t *testing.T) {
	for _, sample := range []struct {
		predicate string
		empty     bool
	}{
		{predicate: "v::FIELD > 0 AND host IN ()", empty: true},
		{predicate: "host IN (NULL) AND v::FIELD > 0", empty: true},
		{predicate: "host IN () OR host IN (NULL)", empty: true},
		{predicate: "host IN () OR v::FIELD > 0"},
		{predicate: "host IN ('a', NULL)"},
		{predicate: "host NOT IN ()"},
		{predicate: "host NOT IN (NULL)", empty: true},
		{predicate: "host NOT IN ('a', NULL)", empty: true},
		{predicate: "host NOT IN (host, NULL)", empty: true},
		{predicate: "host NOT IN (NULL) OR v::FIELD > 0"},
		{predicate: "NOT (host IN ())"},
		{predicate: "NOT (host IN (NULL))"},
		{predicate: "host IN (host)"},
		{predicate: "8 / v::FIELD > 1 AND host IN ()"},
		{predicate: "CAST(host AS INT) > 0 AND host IN (NULL)"},
		{predicate: "8 / v::FIELD > 1 AND host NOT IN (NULL)"},
		{predicate: "CAST(host AS INT) > 0 AND host NOT IN ('a', NULL)"},
		{predicate: "(v::FIELD + 1 > 0 OR host IN ()) AND host IN (NULL)"},
	} {
		t.Run(sample.predicate, func(t *testing.T) {
			execution := typedExecution(t, "SELECT COUNT(DISTINCT rid) AS n FROM g.m WHERE ("+sample.predicate+") AND v::FIELD >= 0")
			require.Equal(t, sample.empty, execution.Plan.Empty)
		})
	}
	statement, err := bydbql.Prepare("SELECT rid FROM g.m WHERE v::FIELD = 'invalid' AND host IN ()")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	if err == nil {
		_, err = template.BindParams(nil, time.Unix(0, 0))
	}
	require.Error(t, err, "an empty binding must not suppress type validation")
}

func TestMeasureFilterNotInNullBindingSkipsInput(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT COUNT(DISTINCT rid) AS n FROM g.m WHERE v::FIELD > 0 AND host NOT IN (?)")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	for _, sample := range []struct {
		parameter *modelv1.TagValue
		empty     bool
	}{
		{parameter: &modelv1.TagValue{Value: &modelv1.TagValue_Null{Null: 0}}, empty: true},
		{parameter: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{}}}},
		{parameter: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"a"}}}}},
		{parameter: &modelv1.TagValue{Value: &modelv1.TagValue_Null{Null: 0}}, empty: true},
	} {
		execution, bindErr := template.BindParams([]*modelv1.TagValue{sample.parameter}, time.Unix(0, 0))
		require.NoError(t, bindErr)
		require.Equal(t, sample.empty, execution.Plan.Empty)
	}
}
