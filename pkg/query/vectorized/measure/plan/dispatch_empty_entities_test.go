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

package plan

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	logicalmeasure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type entityValidationEC struct {
	fakeEC
}

func (e *entityValidationEC) Query(ctx context.Context, opts model.MeasureQueryOptions) (model.MeasureQueryResult, error) {
	if len(opts.Entities) == 0 {
		e.called = true
		return nil, fmt.Errorf("invalid query options: series is required")
	}
	return e.fakeEC.Query(ctx, opts)
}

func TestDispatchEmptyEntityINReturnsEmptyWithoutStorage(t *testing.T) {
	schema := testMeasureSchema()
	schema.Entity = &databasev1.Entity{TagNames: []string{tagCount}}
	logicalSchema, err := logicalmeasure.BuildSchema(schema, nil)
	require.NoError(t, err)
	req := bareReq()
	req.Criteria = &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name: tagCount, Op: modelv1.Condition_BINARY_OP_IN,
		Value: &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{}}},
	}}}
	ec := &entityValidationEC{}
	iter, _, err := Dispatch(context.Background(), req, schema.Metadata, schema, logicalSchema, ec, dispatchCfg(), false, false)
	require.NoError(t, err)
	require.NotNil(t, iter)
	require.False(t, iter.Next())
	require.NoError(t, iter.Close())
	require.False(t, ec.called, "an impossible entity selection must not reach storage")
}

func TestDispatchQLImpossibleEntityPredicates(t *testing.T) {
	cases := []struct {
		name      string
		predicate string
		empty     bool
		wantError bool
	}{
		{name: "empty-int", predicate: "count_tag IN ()", empty: true},
		{name: "empty-and-value", predicate: "count_tag IN () AND count_tag = 1", empty: true},
		{name: "value-and-empty", predicate: "count_tag = 1 AND count_tag IN ()", empty: true},
		{name: "empty-or-value", predicate: "count_tag IN () OR count_tag = 1"},
		{name: "value-or-empty", predicate: "count_tag = 1 OR count_tag IN ()"},
		{name: "empty-and-empty", predicate: "count_tag IN () AND count_tag IN ()", empty: true},
		{name: "empty-or-empty", predicate: "count_tag IN () OR count_tag IN ()", empty: true},
		{name: "contradiction", predicate: "count_tag = 1 AND count_tag = 2", empty: true},
		{name: "nonempty", predicate: "count_tag IN (1)"},
		{name: "all"},
		{name: "alternative-intersection", predicate: "(count_tag = 1 OR count_tag = 2) AND (count_tag = 2 OR count_tag = 3)", wantError: true},
	}
	for _, example := range cases {
		t.Run(example.name, func(t *testing.T) {
			schema := testMeasureSchema()
			schema.Entity = &databasev1.Entity{TagNames: []string{tagCount}}
			logicalSchema, err := logicalmeasure.BuildSchema(schema, nil)
			require.NoError(t, err)
			query := "SELECT value::FIELD FROM MEASURE demo IN default TIME BETWEEN '1970-01-01T00:00:00Z' AND '1970-01-01T00:00:01Z'"
			if example.predicate != "" {
				query += " WHERE " + example.predicate
			}
			statement, err := bydbql.Prepare(query)
			require.NoError(t, err)
			template, err := bydbql.NewTransformer(nil).CompileMeasureWithSchemas(t.Context(), statement,
				map[bydbql.MeasureSource]*databasev1.Measure{{Group: defaultName, Name: "demo"}: schema})
			require.NoError(t, err)
			execution, err := template.BindParams(nil, time.Unix(0, 0))
			require.NoError(t, err)
			require.Nil(t, execution.Plan, "exercise the legacy QL-to-native request path")
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
			ec := &entityValidationEC{}
			iter, _, err := Dispatch(ctx, execution.Request, schema.Metadata, schema, logicalSchema, ec, dispatchCfg(), false, false)
			if example.wantError {
				require.ErrorContains(t, err, "alternative entity selectors")
				require.False(t, ec.called)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, iter)
			require.False(t, iter.Next())
			require.NoError(t, iter.Close())
			require.Equal(t, !example.empty, ec.called)
			if !example.empty && example.predicate != "" {
				require.Len(t, ec.lastOpts.Entities, 1)
				require.EqualValues(t, 1, ec.lastOpts.Entities[0][0].GetInt().Value)
			}
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
}

func TestDispatchIndexModeWithoutEntitiesStillQueriesStorage(t *testing.T) {
	schema := testMeasureSchema()
	schema.IndexMode = true
	schema.Entity = &databasev1.Entity{TagNames: []string{tagCount}}
	logicalSchema, err := logicalmeasure.BuildSchema(schema, nil)
	require.NoError(t, err)
	ec := &fakeEC{}
	iter, _, err := Dispatch(t.Context(), bareReq(), schema.Metadata, schema, logicalSchema, ec, dispatchCfg(), false, false)
	require.NoError(t, err)
	require.False(t, iter.Next())
	require.NoError(t, iter.Close())
	require.True(t, ec.called)
	require.Empty(t, ec.lastOpts.Entities)
}

func TestExtractSeriesMatchersEmptyWithIndexedWildcard(t *testing.T) {
	schema := entityExtractTestSchema(t)
	entityDict, entity := entityTemplate(schema.EntityList())
	empty := strArrayCond("service", modelv1.Condition_BINARY_OP_IN)
	filter := strCond("region", "us-west")
	for _, op := range []modelv1.LogicalExpression_LogicalOp{
		modelv1.LogicalExpression_LOGICAL_OP_AND, modelv1.LogicalExpression_LOGICAL_OP_OR,
	} {
		for _, emptyLeft := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/empty-left-%t", op, emptyLeft), func(t *testing.T) {
				left, right := filter, empty
				if emptyLeft {
					left, right = empty, filter
				}
				remaining, entities, _, err := extractSeriesMatchers(logicalExpr(op, left, right), schema, entityDict, entity)
				require.NoError(t, err)
				if op == modelv1.LogicalExpression_LOGICAL_OP_AND {
					require.Empty(t, entities)
					require.Nil(t, remaining)
				} else {
					require.Same(t, filter, remaining)
					require.Equal(t, [][]*modelv1.TagValue{entity}, entities)
				}
			})
		}
	}
	remaining, entities, _, err := extractSeriesMatchers(logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_AND,
		empty, strCond("unindexed", "invalid")), schema, entityDict, entity)
	require.ErrorContains(t, err, "mandatory index rule", "an impossible branch must not hide invalid criteria")
	require.Nil(t, remaining)
	require.Nil(t, entities)
}

func TestExtractSeriesMatchersRejectsUnsupportedAlternativeIntersection(t *testing.T) {
	schema := entityExtractTestSchema(t)
	entityDict, entity := entityTemplate(schema.EntityList())
	left := logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_OR, strCond("service", "a"), strCond("service", "b"))
	right := logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_OR, strCond("service", "b"), strCond("service", "c"))
	remaining, entities, _, err := extractSeriesMatchers(logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_AND,
		left, right), schema, entityDict, entity)
	require.Error(t, err, "native entity merger cannot infer an empty result from intersecting multiple alternatives")
	require.Nil(t, remaining)
	require.Nil(t, entities)
}
