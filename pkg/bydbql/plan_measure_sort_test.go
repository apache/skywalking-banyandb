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
	"time"

	"github.com/stretchr/testify/require"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestMeasureSortVisibleAliasesAndNulls(t *testing.T) {
	execution := typedExecution(t, "SELECT time AS original, v AS value FROM g.m ORDER BY value DESC NULLS LAST, original ASC")
	points := executeMeasureSort(t, execution.Plan)
	times := make([]int64, 0, len(points))
	for _, point := range points {
		times = append(times, point.Timestamp.AsTime().Unix())
		require.Len(t, point.Fields, 1)
		require.Equal(t, "value", point.Fields[0].Name)
		require.Len(t, point.TagFamilies, 1)
		require.Equal(t, "original", point.TagFamilies[0].Tags[0].Key)
	}
	require.Equal(t, []int64{4, 2, 1, 0, 3, 5}, times)
}

func executeMeasureSort(t *testing.T, plan *ql.RawPlan) []*measurev1.DataPoint {
	t.Helper()
	source := computedGroupFixture(plan.Slots)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 2<<20)
	operator, err := ql.WrapRaw(ctx, source, plan)
	require.NoError(t, err)
	iterator, err := ql.RawIterator(ctx, operator)
	require.NoError(t, err)
	var points []*measurev1.DataPoint
	for iterator.Next() {
		for _, row := range iterator.Current() {
			points = append(points, row.DataPoint)
		}
	}
	require.NoError(t, iterator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	require.Equal(t, 1, source.closes)
	return points
}

func TestMeasureSortHiddenExpressionRebindsBeforePagination(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT time AS original FROM g.m ORDER BY v * ? DESC NULLS LAST, original ASC LIMIT ? OFFSET ?")
	require.NoError(t, err)
	require.Equal(t, 3, statement.NumPlaceholders())
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	originalSize := template.EstimatedSize()
	for _, item := range []struct {
		want   []int64
		factor int64
	}{{[]int64{2, 1}, 1}, {[]int64{1, 2}, -1}} {
		execution, bindErr := template.BindParams([]*modelv1.TagValue{intParam(item.factor), intParam(2), intParam(1)}, time.Unix(0, 0))
		require.NoError(t, bindErr)
		require.Equal(t, uint32(math.MaxUint32), execution.Request.Limit)
		require.Zero(t, execution.Request.Offset)
		require.Equal(t, []string{"v"}, execution.Request.FieldProjection.Names)
		points := executeMeasureSort(t, execution.Plan)
		var times []int64
		for _, point := range points {
			times = append(times, point.Timestamp.AsTime().Unix())
			require.Empty(t, point.Fields)
			require.Len(t, point.TagFamilies[0].Tags, 1)
		}
		require.Equal(t, item.want, times)
	}
	require.Equal(t, originalSize, template.EstimatedSize())
}

func TestMeasureSortCompletedGroupsAfterHaving(t *testing.T) {
	execution := typedExecution(t, "SELECT host AS node, SUM(v) AS total FROM g.m GROUP BY host HAVING SUM(v) > 3 "+
		"ORDER BY total DESC NULLS LAST, node ASC LIMIT 1 OFFSET 1")
	points := executeMeasureSort(t, execution.Plan)
	require.Len(t, points, 1)
	require.Equal(t, "b", points[0].TagFamilies[0].Tags[0].Value.GetStr().Value)
	require.EqualValues(t, 4, points[0].Fields[0].Value.GetInt().Value)
}

func TestMeasureSortHiddenAggregateAndVisibleDistinct(t *testing.T) {
	for _, query := range []string{
		"SELECT host AS node FROM g.m GROUP BY host ORDER BY SUM(v) DESC NULLS LAST, node ASC",
		"SELECT DISTINCT host AS node FROM g.m ORDER BY node DESC NULLS LAST",
	} {
		execution := typedExecution(t, query)
		points := executeMeasureSort(t, execution.Plan)
		var hosts []string
		for _, point := range points {
			hosts = append(hosts, point.TagFamilies[0].Tags[0].Value.GetStr().Value)
			require.Empty(t, point.Fields)
			require.Len(t, point.TagFamilies[0].Tags, 1)
		}
		require.Equal(t, []string{"c", "b", "a"}, hosts)
	}
}

func TestMeasureSortOrdinalCountsImplicitTimeProjection(t *testing.T) {
	execution := typedExecution(t, "SELECT time, host FROM g.m ORDER BY 1 DESC NULLS LAST")
	points := executeMeasureSort(t, execution.Plan)
	var times []int64
	for _, point := range points {
		times = append(times, point.Timestamp.AsTime().Unix())
	}
	require.Equal(t, []int64{5, 4, 3, 2, 1, 0}, times)
}

func TestMeasureSortRejectsInvalidOrdinals(t *testing.T) {
	for _, ordinal := range []string{"0", "-1", "3"} {
		t.Run(ordinal, func(t *testing.T) {
			statement, err := bydbql.Prepare("SELECT time, host FROM g.m ORDER BY " + ordinal)
			require.NoError(t, err)
			_, err = typedTransformer().CompileMeasure(t.Context(), statement)
			require.ErrorIs(t, err, bydbql.ErrInvalidMeasureQuery)
			require.ErrorContains(t, err, "ordinal")
		})
	}
}

func TestMeasureSortParametersRemainValues(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT time AS original FROM g.m ORDER BY ?, original DESC")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	for _, parameter := range []*modelv1.TagValue{intParam(1), strParam("host"), nullParam()} {
		execution, bindErr := template.BindParams([]*modelv1.TagValue{parameter}, time.Unix(0, 0))
		require.NoError(t, bindErr)
		points := executeMeasureSort(t, execution.Plan)
		var times []int64
		for _, point := range points {
			times = append(times, point.Timestamp.AsTime().Unix())
		}
		require.Equal(t, []int64{5, 4, 3, 2, 1, 0}, times)
	}
}

func TestMeasureSortPreservesHiddenFilterInputs(t *testing.T) {
	for _, query := range []string{
		"SELECT host AS node FROM g.m WHERE f > 1.0 ORDER BY node NULLS LAST",
		"SELECT host AS node FROM g.m WHERE f > 1.0 ORDER BY v DESC NULLS LAST",
		"SELECT v + 1 AS value FROM g.m WHERE f > 1.0 ORDER BY value NULLS LAST",
		"SELECT host, SUM(v) AS total FROM g.m WHERE f > 1.0 GROUP BY host ORDER BY total NULLS LAST",
		"SELECT DISTINCT host FROM g.m WHERE f > 1.0 ORDER BY host NULLS LAST",
	} {
		t.Run(query, func(t *testing.T) {
			execution := typedExecution(t, query)
			require.NotNil(t, execution.Plan.Filter)
			require.Contains(t, execution.Request.GetFieldProjection().GetNames(), "f")
		})
	}
}

func TestMeasureSortQualifiedComputedTimestampAlias(t *testing.T) {
	for _, projection := range []string{"CAST(time AS TIMESTAMP) AS observed, v AS observed", "time AS observed, v + 1 AS shifted"} {
		for _, key := range []string{"observed::TAG", "__ql.observed::TAG"} {
			execution := typedExecution(t, "SELECT "+projection+" FROM g.m ORDER BY "+key+" DESC NULLS LAST")
			points := executeMeasureSort(t, execution.Plan)
			var times []int64
			for _, point := range points {
				times = append(times, point.Timestamp.AsTime().Unix())
				require.Equal(t, point.Timestamp.AsTime(), point.TagFamilies[0].Tags[0].Value.GetTimestamp().AsTime())
			}
			require.Equal(t, []int64{5, 4, 3, 2, 1, 0}, times)
		}
	}
}

func TestMeasureSortQualifiedAliasRebindsOutputType(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT ? AS value FROM g.m ORDER BY value::TAG, time DESC")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	retained := template.EstimatedSize()
	parameter := timestampParam(time.Unix(1, 0))
	execution, err := template.BindParams([]*modelv1.TagValue{parameter}, time.Unix(0, 0))
	require.NoError(t, err)
	points := executeMeasureSort(t, execution.Plan)
	require.Len(t, points, 6)
	_, err = template.BindParams([]*modelv1.TagValue{intParam(1)}, time.Unix(0, 0))
	require.ErrorIs(t, err, bydbql.ErrInvalidMeasureQuery)
	execution, err = template.BindParams([]*modelv1.TagValue{parameter}, time.Unix(0, 0))
	require.NoError(t, err)
	require.Len(t, executeMeasureSort(t, execution.Plan), 6)
	require.Equal(t, retained, template.EstimatedSize())
}
