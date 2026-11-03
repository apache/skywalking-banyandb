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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestMeasureCountDistinctComposesBeforeFinalPagination(t *testing.T) {
	execution := typedExecution(t, "SELECT host AS node, COUNT(DISTINCT v) AS unique_values, COUNT(v) AS samples, SUM(v) AS total "+
		"FROM g.m GROUP BY host HAVING COUNT(DISTINCT v) > 0 ORDER BY unique_values DESC, node ASC LIMIT 1 OFFSET 1")
	require.Nil(t, execution.Request.Agg)
	require.Equal(t, uint32(math.MaxUint32), execution.Request.Limit)
	require.Zero(t, execution.Request.Offset)
	points := executeMeasureSort(t, execution.Plan)
	require.Len(t, points, 1)
	require.Equal(t, "b", points[0].TagFamilies[0].Tags[0].GetValue().GetStr().GetValue())
	require.Len(t, points[0].Fields, 3)
	require.Equal(t, int64(1), points[0].Fields[0].GetValue().GetInt().GetValue())
	require.Equal(t, int64(1), points[0].Fields[1].GetValue().GetInt().GetValue())
	require.Equal(t, int64(4), points[0].Fields[2].GetValue().GetInt().GetValue())
}

func TestMeasureCountDistinctRetainsDistinctOutputIdentity(t *testing.T) {
	execution := typedExecution(t, "SELECT COUNT(v), COUNT(DISTINCT v), COUNT(DISTINCT host) AS hosts FROM g.m")
	points := executeMeasureSort(t, execution.Plan)
	require.Len(t, points, 1)
	require.Equal(t, "COUNT(v)", points[0].Fields[0].Name)
	require.Equal(t, "COUNT(DISTINCT v)", points[0].Fields[1].Name)
	require.Equal(t, int64(4), points[0].Fields[0].GetValue().GetInt().GetValue())
	require.Equal(t, int64(4), points[0].Fields[1].GetValue().GetInt().GetValue())
	require.Equal(t, int64(3), points[0].Fields[2].GetValue().GetInt().GetValue())
}

func TestMeasureCountDistinctAliasesAndMultipleGroupsCompile(t *testing.T) {
	for _, query := range []string{
		"SELECT COUNT(DISTINCT host) AS n FROM MEASURE m IN g",
		"SELECT host, rid, COUNT(DISTINCT v) AS n FROM MEASURE m IN g GROUP BY host,rid ORDER BY n DESC,host ASC,rid ASC",
		"SELECT host FROM g.m GROUP BY host ORDER BY COUNT(DISTINCT v) DESC, host ASC",
		"SELECT COUNT(DISTINCT v) AS n FROM MEASURE m IN g ON (warm,cold) STAGES",
	} {
		t.Run(query, func(t *testing.T) {
			statement, err := bydbql.Prepare(query)
			require.NoError(t, err)
			_, err = typedTransformer().CompileMeasure(t.Context(), statement)
			require.NoError(t, err)
		})
	}
}

func TestMeasureCountDistinctDoesNotSumOverlappingNativePartials(t *testing.T) {
	execution := nativeAggregateExecution(t, "SELECT host, COUNT(DISTINCT f) AS unique_values, COUNT(f) AS samples "+
		"FROM g.m WHERE host IN ('a','b') AND time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:06Z' GROUP BY host",
		nativeAggregateSchema())
	require.Nil(t, execution.Plan.NativeAggregate)
	require.Nil(t, execution.Request.Agg)
	require.Nil(t, execution.Request.GroupBy)
	require.Equal(t, uint32(math.MaxUint32), execution.Request.Limit)
}

func TestMeasureCountDistinctPreservesLegacyNativeRouting(t *testing.T) {
	for _, query := range []string{
		"SELECT COUNT(DISTINCT host) FROM MEASURE m IN g",
		"SELECT TOP 500 host DESC, rid, COUNT(DISTINCT host) FROM MEASURE m IN g GROUP BY rid LIMIT 20 OFFSET 0",
	} {
		t.Run(query, func(t *testing.T) {
			execution := typedExecution(t, query)
			require.Nil(t, execution.Plan)
			require.NotNil(t, execution.Request.Agg)
			require.Equal(t, "host", execution.Request.Agg.TagName)
		})
	}
}

func TestLegacyScalarCountDistinctZeroLimit(t *testing.T) {
	for _, limit := range []int64{0, 1, 20} {
		execution := typedExecution(t, "SELECT COUNT(DISTINCT host) FROM MEASURE m IN g LIMIT ?",
			&modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: limit}}})
		if limit == 0 {
			require.NotNil(t, execution.Plan)
			require.True(t, execution.Plan.Empty)
		} else {
			require.Nil(t, execution.Plan)
			// Final scalar pagination must not truncate native partial input.
			require.Equal(t, uint32(math.MaxUint32), execution.Request.Limit)
		}
	}
	execution := typedExecution(t, "SELECT COUNT(DISTINCT host) FROM MEASURE m IN g")
	require.Nil(t, execution.Plan)
}
