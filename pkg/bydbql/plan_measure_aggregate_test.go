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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestMeasureSQLAggregationUsesOneCompleteRawScan(t *testing.T) {
	execution := typedExecution(t, "SELECT COUNT(v) AS c, SUM(v) AS s, MIN(v) AS lo, MAX(v) AS hi, AVG(f) AS a FROM g.m LIMIT 1 OFFSET 2")
	require.NotNil(t, execution.Plan)
	require.Nil(t, execution.Request.Agg)
	require.Nil(t, execution.Request.GroupBy)
	require.Equal(t, uint32(math.MaxUint32), execution.Request.Limit)
	require.Zero(t, execution.Request.Offset)
	require.Equal(t, []string{"v", "f"}, execution.Request.FieldProjection.Names)
	require.Equal(t, uint32(1), *execution.Plan.Limit)
	require.Equal(t, uint32(2), execution.Plan.Offset)
}

func TestMeasureSQLAggregateGroupingAndSharedCalls(t *testing.T) {
	execution := typedExecution(t, "SELECT host AS node, COUNT(v) AS c, SUM(v) AS s, AVG(v) AS a, MEAN(v) AS b, SUM(v) AS again FROM MEASURE m IN g GROUP BY host, f::FIELD")
	require.NotNil(t, execution.Plan.Aggregate)
	require.Len(t, execution.Plan.Aggregate.GroupBy, 2)
	require.Len(t, execution.Plan.Aggregate.Calls, 3)
	require.Len(t, execution.Plan.Aggregate.Outputs, 6)
	require.Equal(t, "node", execution.Plan.Aggregate.Outputs[0].Column.Name)
	require.True(t, execution.Plan.Aggregate.Outputs[0].IsGroup)
	require.Equal(t, execution.Plan.Aggregate.Outputs[3].Index, execution.Plan.Aggregate.Outputs[4].Index)
	require.Equal(t, execution.Plan.Aggregate.Outputs[2].Index, execution.Plan.Aggregate.Outputs[5].Index)
	require.Equal(t, vectorized.ColumnTypeFloat64, execution.Plan.Aggregate.Outputs[3].Column.Type)
	require.Equal(t, []string{"f", "v"}, execution.Request.FieldProjection.Names)

	unnamed := typedExecution(t, "SELECT COUNT(f), AVG(f) FROM MEASURE m IN g")
	require.Equal(t, "COUNT(f)", unnamed.Plan.Aggregate.Outputs[0].Column.Name)
	require.Equal(t, "AVG(f)", unnamed.Plan.Aggregate.Outputs[1].Column.Name)
	require.Nil(t, unnamed.Request.Agg)
	legacy := typedExecution(t, "SELECT host, MEAN(v) FROM MEASURE m IN g GROUP BY host")
	require.Nil(t, legacy.Plan)
	require.Equal(t, modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN, legacy.Request.Agg.Function)
}

func TestMeasureSQLAggregateRejectsUnsupportedShapesBeforeScan(t *testing.T) {
	queries := []string{
		"SELECT host, SUM(v) AS s FROM g.m",
		"SELECT host, SUM(v) AS s FROM g.m GROUP BY rid",
		"SELECT SUM(host) FROM g.m",
		"SELECT MIN(host) FROM g.m",
		"SELECT SUM(v) AS x, COUNT(v) AS x FROM g.m",
		"SELECT SUM(v), SUM(v) FROM g.m",
		"SELECT COUNT(clash) FROM g.m",
		"SELECT COUNT(missing) FROM g.m",
		"SELECT host AS node, SUM(v) AS s FROM g.m GROUP BY node",
		"SELECT SUM(v) FROM g.m ORDER BY time",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			statement, err := bydbql.Prepare(query)
			require.NoError(t, err)
			_, err = typedTransformer().CompileMeasure(context.Background(), statement)
			require.ErrorIs(t, err, bydbql.ErrInvalidMeasureQuery)
		})
	}
}

func TestMeasureSQLAggregateBindingDoesNotMutateCachedTemplate(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT host AS node, SUM(v) AS total FROM g.m WHERE host = ? GROUP BY host LIMIT ?")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(context.Background(), statement)
	require.NoError(t, err)
	bytes := template.EstimatedSize()
	for index := int64(0); index < 3; index++ {
		execution, bindErr := template.BindParams([]*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "a"}}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: index}}},
		}, time.UnixMilli(index))
		require.NoError(t, bindErr)
		require.Equal(t, "host", execution.Plan.Aggregate.GroupBy[0].Name)
		require.Equal(t, "v", execution.Plan.Aggregate.Calls[0].Source.Name)
		require.Equal(t, "total", execution.Plan.Aggregate.Outputs[1].Column.Name)
		execution.Plan.Aggregate.GroupBy[0].Name = "poison"
		execution.Plan.Aggregate.Calls[0].Source.Name = "poison"
		execution.Plan.Aggregate.Outputs[1].Column.Name = "poison"
	}
	require.Equal(t, bytes, template.EstimatedSize())
}
