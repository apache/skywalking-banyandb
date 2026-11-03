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
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

func nativeAggregateSchema() *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "m", Group: "g"}, Entity: &databasev1.Entity{TagNames: []string{"host"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "zone", Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
		Fields: []*databasev1.FieldSpec{
			{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
			{Name: "f", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT},
		},
	}
}

func nativeAggregateExecution(t *testing.T, query string, schema *databasev1.Measure) *bydbql.MeasureExecution {
	t.Helper()
	statement, err := bydbql.Prepare(query)
	require.NoError(t, err)
	template, err := bydbql.NewTransformer(nil).CompileMeasureWithSchemas(context.Background(), statement,
		map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: schema})
	require.NoError(t, err)
	execution, err := template.BindParams(nil, time.UnixMilli(0))
	require.NoError(t, err)
	return execution
}

func TestMeasureNativeAggregationSharesSumCountAverage(t *testing.T) {
	execution := nativeAggregateExecution(t, "SELECT host, SUM(f) AS s, COUNT(f) AS c, AVG(f) AS a, AVG(f) AS again "+
		"FROM g.m WHERE host IN ('a','b') AND time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:06Z' GROUP BY host LIMIT 1 OFFSET 2",
		nativeAggregateSchema())
	plan := execution.Plan.NativeAggregate
	require.NotNil(t, plan)
	require.Len(t, plan.Leaves, 2)
	require.EqualValues(t, 6000, plan.MaxCount)
	require.Equal(t, ql.AggregateSum, plan.Leaves[0].Function)
	require.Equal(t, ql.AggregateCount, plan.Leaves[1].Function)
	for _, leaf := range plan.Leaves {
		require.NotNil(t, leaf.Request.Agg)
		require.NotEqual(t, modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN, leaf.Request.Agg.Function)
		require.Equal(t, "f", leaf.Request.Agg.FieldName)
		require.Equal(t, uint32(math.MaxUint32), leaf.Request.Limit)
		require.Zero(t, leaf.Request.Offset)
		require.Equal(t, []string{"host"}, leaf.Request.GroupBy.TagProjection.TagFamilies[0].Tags)
	}
	require.Nil(t, execution.Request.Agg)
	require.Equal(t, uint32(1), *execution.Plan.Limit)
	require.Equal(t, uint32(2), execution.Plan.Offset)
}

func TestMeasureNativeAggregationKeepsUnsafeShapesLocal(t *testing.T) {
	for _, test := range []struct {
		name, projection, predicate, groups string
	}{
		{"unchecked_int_sum", "SUM(v) AS n", "host = 'a'", " GROUP BY host"},
		{"int_average_uses_float_sum", "AVG(v) AS n", "host = 'a'", " GROUP BY host"},
		{"float_min_infinity", "MIN(f) AS n", "host = 'a'", " GROUP BY host"},
		{"float_max_nan", "MAX(f) AS n", "host = 'a'", " GROUP BY host"},
		{"unpruned_nullable_key", "AVG(f) AS n", "", " GROUP BY host"},
		{"multiple_scalar_entities", "AVG(f) AS n", "host IN ('a','b')", ""},
		{"nonentity_group", "AVG(f) AS n", "host = 'a' AND zone = 'x'", " GROUP BY host, zone"},
		{"field_group", "COUNT(f) AS n", "host = 'a'", " GROUP BY host, v::FIELD"},
		{"tag_count", "COUNT(host) AS n", "host = 'a'", " GROUP BY host"},
		{"null_key", "COUNT(f) AS n", "host = NULL", " GROUP BY host"},
		{"residual_time", "AVG(f) AS n", "host = 'a' AND (time = '1970-01-01T00:00:01Z' OR time = '1970-01-01T00:00:03Z')", " GROUP BY host"},
	} {
		t.Run(test.name, func(t *testing.T) {
			query := "SELECT " + test.projection + " FROM g.m"
			if test.predicate != "" {
				query += " WHERE " + test.predicate
			}
			execution := nativeAggregateExecution(t, query+test.groups, nativeAggregateSchema())
			require.Nil(t, execution.Plan.NativeAggregate)
			require.NotNil(t, execution.Plan.Aggregate)
			require.Nil(t, execution.Request.Agg)
		})
	}
	indexed := nativeAggregateSchema()
	indexed.IndexMode = true
	execution := nativeAggregateExecution(t, "SELECT AVG(f) AS n FROM g.m WHERE host = 'a' GROUP BY host", indexed)
	require.Nil(t, execution.Plan.NativeAggregate)
}

func TestMeasureNativeAggregationProvesIntegerOnePositionAndScalarEntity(t *testing.T) {
	for _, function := range []string{"SUM", "AVG"} {
		execution := nativeAggregateExecution(t, "SELECT "+function+"(v) AS n FROM g.m WHERE host = 'a' "+
			"AND time = '1970-01-01T00:00:01Z'", nativeAggregateSchema())
		require.NotNil(t, execution.Plan.NativeAggregate)
		require.Len(t, execution.Plan.NativeAggregate.Leaves, 2)
		require.EqualValues(t, 1, execution.Plan.NativeAggregate.MaxCount)
		require.Nil(t, execution.Plan.NativeAggregate.Leaves[0].Request.GroupBy)
	}
	count := nativeAggregateExecution(t, "SELECT COUNT(f) AS n FROM g.m WHERE host = '' GROUP BY host", nativeAggregateSchema())
	require.NotNil(t, count.Plan.NativeAggregate)
	require.Greater(t, count.Plan.NativeAggregate.MaxCount, int64(0))
	require.LessOrEqual(t, count.Plan.NativeAggregate.MaxCount, int64(1<<53))
}

func TestMeasureNativeAggregationBindsIndependentCachedRecipes(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT AVG(f) AS n FROM g.m WHERE host = ? GROUP BY host")
	require.NoError(t, err)
	template, err := bydbql.NewTransformer(nil).CompileMeasureWithSchemas(context.Background(), statement,
		map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: nativeAggregateSchema()})
	require.NoError(t, err)
	bytes := template.EstimatedSize()
	for _, host := range []string{"a", "b", "c"} {
		execution, bindErr := template.BindParams([]*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: host}}}}, time.UnixMilli(0))
		require.NoError(t, bindErr)
		native := execution.Plan.NativeAggregate
		require.NotNil(t, native)
		require.Equal(t, host, native.Leaves[0].Request.Criteria.GetCondition().Value.GetStr().Value)
		native.Leaves[0].Request.Criteria.GetCondition().Value.GetStr().Value = "poison"
		native.Aggregate.Calls[0].Source.Name = "poison"
		require.Equal(t, host, execution.Request.Criteria.GetCondition().Value.GetStr().Value)
		require.Equal(t, host, native.Leaves[1].Request.Criteria.GetCondition().Value.GetStr().Value)
	}
	require.Equal(t, bytes, template.EstimatedSize())
}
