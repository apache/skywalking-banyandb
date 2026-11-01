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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestMeasurePatternPredicateBindsBeforeAggregation(t *testing.T) {
	execution := typedExecution(t, "SELECT COUNT(DISTINCT rid) FROM g.m WHERE host ILIKE ? AND v::FIELD > ?", strParam("%a%"), intParam(0))
	require.NotNil(t, execution.Plan.Aggregate)
	require.NotNil(t, execution.Plan.Filter)
	require.Nil(t, execution.Plan.NativeAggregate)
}

func TestMeasurePatternNativePage(t *testing.T) {
	execution := typedExecution(t, "SELECT host FROM g.m WHERE host LIKE ? AND time >= ? ORDER BY time DESC LIMIT ? OFFSET ?",
		strParam("a%"), strParam("2026-01-01T00:00:00Z"), intParam(2), intParam(1))
	require.Nil(t, execution.Plan.Filter)
	require.Nil(t, execution.Plan.Sort)
	require.EqualValues(t, 3, execution.Request.Limit)
	require.Zero(t, execution.Request.Offset)
	require.EqualValues(t, 2, *execution.Plan.Limit)
	require.EqualValues(t, 1, execution.Plan.Offset)
}

func TestMeasurePatternBindingsStayIndependent(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT COUNT(DISTINCT rid) FROM g.m WHERE time >= ? AND (host ILIKE ? ESCAPE '!' AND v::FIELD > ?) LIMIT ?")
	require.NoError(t, err)
	require.Equal(t, 4, statement.NumPlaceholders())
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	size := template.EstimatedSize()
	for _, pattern := range []string{"%a%", "a!_b", ""} {
		execution, bindErr := template.BindParams([]*modelv1.TagValue{strParam("2026-01-01T00:00:00Z"), strParam(pattern), intParam(0), intParam(1)}, time.Unix(0, 0))
		require.NoError(t, bindErr)
		require.Len(t, execution.Plan.Patterns, 1)
		require.Equal(t, pattern, execution.Plan.Patterns[0].Pattern)
		require.Equal(t, "!", execution.Plan.Patterns[0].Escape)
		require.True(t, execution.Plan.Patterns[0].IgnoreCase)
		require.Equal(t, "host", execution.Plan.Patterns[0].TagName)
		require.Equal(t, "default", execution.Plan.Patterns[0].TagFamily)
		require.False(t, execution.Plan.Empty)
		execution.Plan.Patterns[0].Pattern = "mutated"
		require.Equal(t, size, template.EstimatedSize())
	}
	null := typedExecution(t, "SELECT host FROM g.m WHERE host LIKE NULL")
	require.True(t, null.Plan.Empty)
	require.Len(t, null.Plan.Patterns, 1)
}

func TestMeasurePatternRejectsUnsupportedPlacementAndValues(t *testing.T) {
	for _, predicate := range []string{
		"host LIKE '%' OR rid = 1", "NOT (host ILIKE '%')", "host NOT LIKE '%'", "rid LIKE '%'", "v::FIELD LIKE '%'",
		"CAST(host AS STRING) LIKE '%'", "host LIKE rid", "host LIKE '%' ESCAPE 'xx'", "host LIKE '%' ESCAPE NULL", "host LIKE '%' ESCAPE ?",
	} {
		t.Run(predicate, func(t *testing.T) {
			statement, err := bydbql.Prepare("SELECT host FROM g.m WHERE " + predicate)
			if err == nil {
				_, err = typedTransformer().CompileMeasure(t.Context(), statement)
			}
			require.Error(t, err)
		})
	}
	statement, err := bydbql.Prepare("SELECT host FROM g.m WHERE host LIKE ?")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	for _, value := range []*modelv1.TagValue{intParam(1), strParam("a\\"), strParam(string([]byte{0xff})), strParam(strings.Repeat("a", 1025))} {
		_, bindErr := template.BindParams([]*modelv1.TagValue{value}, time.Unix(0, 0))
		require.Error(t, bindErr)
	}
	_, err = template.BindParams([]*modelv1.TagValue{strParam("a%")}, time.Unix(0, 0))
	require.NoError(t, err)
}

func TestMeasurePatternConcurrentBindings(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT host FROM g.m WHERE host ILIKE ?")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	for _, pattern := range []string{"%a%", "A_", "", "%", "x!%"} {
		t.Run(pattern, func(t *testing.T) {
			t.Parallel()
			for range 10 {
				execution, bindErr := template.BindParams([]*modelv1.TagValue{strParam(pattern)}, time.Unix(0, 0))
				require.NoError(t, bindErr)
				require.Equal(t, pattern, execution.Plan.Patterns[0].Pattern)
				execution.Plan.Patterns[0].Pattern = "changed"
			}
		})
	}
}

func TestMeasurePatternRawSortRequiresNativeIndex(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT host FROM g.m WHERE host LIKE '%' ORDER BY host")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	_, err = template.BindParams(nil, time.Unix(0, 0))
	require.ErrorContains(t, err, "native index rule")
}
