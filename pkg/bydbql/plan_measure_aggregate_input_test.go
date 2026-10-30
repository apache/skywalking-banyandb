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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestMeasureComputedAggregateInputs(t *testing.T) {
	execution := typedExecution(t, "SELECT SUM(v + f) AS total, COUNT(DISTINCT v + 1) AS unique_count FROM g.m")
	require.Len(t, execution.Plan.Aggregate.Calls, 2)
	require.Len(t, execution.Plan.Aggregate.GroupExpressions.Outputs, 2)
	require.Equal(t, vectorized.ColumnTypeFloat64, execution.Plan.Aggregate.Calls[0].Source.Type)
	require.Equal(t, vectorized.ColumnTypeInt64, execution.Plan.Aggregate.Calls[1].Source.Type)
	require.Equal(t, []string{"v", "f"}, execution.Request.FieldProjection.Names)
}

func TestMeasureComputedAggregateInputParameter(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT SUM(v + ?) AS total FROM g.m")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(context.Background(), statement)
	require.NoError(t, err)
	execution, err := template.BindParams([]*modelv1.TagValue{{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 2}}}}, time.Now())
	require.NoError(t, err)
	require.Equal(t, vectorized.ColumnTypeInt64, execution.Plan.Aggregate.Calls[0].Source.Type)
}

func TestMeasureComputedAggregateInputDeduplication(t *testing.T) {
	execution := typedExecution(t, "SELECT SUM(v + 1) AS a, SUM(v + 1) AS b, COUNT(v + 1) AS c FROM g.m")
	require.Len(t, execution.Plan.Aggregate.Calls, 2)
	require.Len(t, execution.Plan.Aggregate.GroupExpressions.Outputs, 2)
}

func TestMeasureComputedAggregateInputAvoidsAmbiguousSourceName(t *testing.T) {
	const collision = "__ql_aggregate_input_0"
	schema := &databasev1.Measure{
		Metadata:    &commonv1.Metadata{Name: "m", Group: "g"},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{{Name: collision, Type: databasev1.TagType_TAG_TYPE_INT}}}},
		Fields:      []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT}, {Name: collision, FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}
	statement, err := bydbql.Prepare("SELECT SUM(v + 1), SUM(__ql_aggregate_input_0::FIELD) FROM g.m")
	require.NoError(t, err)
	transformer := bydbql.NewTransformerWithSource(nil, typedSource{schema: schema})
	template, err := transformer.CompileMeasure(context.Background(), statement)
	require.NoError(t, err)
	execution, err := template.BindParams(nil, time.Now())
	require.NoError(t, err)
	require.NotEqual(t, collision, execution.Plan.Aggregate.Calls[0].Source.Name)
	require.Equal(t, collision, execution.Plan.Aggregate.Calls[1].Source.Name)
}

func TestMeasureScaledDecimalAggregate(t *testing.T) {
	execution := typedExecution(t, "SELECT SUM(DECIMAL_FROM_SCALED(v, 10)) AS total FROM g.m")
	require.Len(t, execution.Plan.Aggregate.Calls, 1)
	require.Equal(t, vectorized.ColumnTypeInt64, execution.Plan.Aggregate.Calls[0].Source.Type)
	require.True(t, execution.Plan.Aggregate.Calls[0].Decimal)
	require.Equal(t, uint8(10), execution.Plan.Aggregate.Calls[0].DecimalScale)
	require.Equal(t, vectorized.ColumnTypeDecimal, execution.Plan.Aggregate.Outputs[0].Column.Type)
}

func TestMeasureScaledDecimalOrderBy(t *testing.T) {
	execution := typedExecution(t, "SELECT SUM(DECIMAL_FROM_SCALED(v, 10)) AS total FROM g.m ORDER BY total ASC")
	require.NotNil(t, execution.Plan.Sort)
	require.Equal(t, ql.ExpressionDecimal, execution.Plan.Sort.Keys[0].Expr.Type)
}

func TestMeasureScaledDecimalBoundaries(t *testing.T) {
	for _, test := range []struct{ name, query string }{
		{"negative scale", "SELECT SUM(DECIMAL_FROM_SCALED(v, -1)) FROM g.m"},
		{"scale above maximum", "SELECT SUM(DECIMAL_FROM_SCALED(v, 19)) FROM g.m"},
		{"nonliteral scale", "SELECT SUM(DECIMAL_FROM_SCALED(v, v)) FROM g.m"},
		{"decimal arithmetic", "SELECT SUM(DECIMAL_FROM_SCALED(v, 10)) + 1 FROM g.m"},
		{"float comparison", "SELECT SUM(DECIMAL_FROM_SCALED(v, 10)) AS total FROM g.m HAVING SUM(DECIMAL_FROM_SCALED(v, 10)) > 0.5"},
	} {
		t.Run(test.name, func(t *testing.T) {
			statement, err := bydbql.Prepare(test.query)
			require.NoError(t, err)
			template, err := typedTransformer().CompileMeasure(context.Background(), statement)
			if err != nil {
				return
			}
			_, err = template.BindParams(nil, time.Now())
			require.Error(t, err)
		})
	}
}

func TestMeasureScaledDecimalCoalesceZero(t *testing.T) {
	execution := typedExecution(t, "SELECT COALESCE(SUM(DECIMAL_FROM_SCALED(v, 10)), 0) AS total FROM g.m")
	require.NotNil(t, execution.Plan.Expressions)
	require.Equal(t, vectorized.ColumnTypeDecimal, execution.Plan.Expressions.Outputs[0].Column.Type)
	require.Equal(t, uint8(10), execution.Plan.Expressions.Outputs[0].Column.Scale)
}
