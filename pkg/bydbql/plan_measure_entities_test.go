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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

func TestMeasureAggregateEntityConjunctionPreservesAllRequestedTuples(t *testing.T) {
	execution := bindEntityAggregate(t, "host IN ('a','b','c') AND region IN ('east','west')")
	require.NotNil(t, execution.Plan.NativeAggregate)
	require.Equal(t, []string{"a/east", "a/west", "b/east", "b/west", "c/east", "c/west"}, nativeEntityTuples(t, execution.Request.Criteria))
	for _, leaf := range execution.Plan.NativeAggregate.Leaves {
		require.True(t, proto.Equal(execution.Request.Criteria, leaf.Request.Criteria), "native and local scans must request the same entity tuples")
		require.Equal(t, []string{"a/east", "a/west", "b/east", "b/west", "c/east", "c/west"}, nativeEntityTuples(t, leaf.Request.Criteria))
	}
}

func TestMeasureAggregateEntitySetsIntersectAndDeduplicate(t *testing.T) {
	cases := []struct {
		predicate string
		want      []string
	}{
		{"host IN ('a','a','b') AND region IN ('east','east','west')", []string{"a/east", "a/west", "b/east", "b/west"}},
		{"host IN ('a','b','c') AND host IN ('b','c','d') AND region IN ('east','west')", []string{"b/east", "b/west", "c/east", "c/west"}},
		{"host IN ('','a') AND region IN ('','east')", []string{"/", "/east", "a/", "a/east"}},
		{"host IN ('a','b') AND host = 'b' AND region = 'east'", []string{"b/east"}},
	}
	for _, example := range cases {
		t.Run(example.predicate, func(t *testing.T) {
			execution := bindEntityAggregate(t, example.predicate)
			require.Equal(t, example.want, nativeEntityTuples(t, execution.Request.Criteria))
		})
	}
}

func TestMeasureAggregateEntityNormalizationPreservesSimpleShapes(t *testing.T) {
	single := bindEntityAggregate(t, "host IN ('a','b')")
	require.NotNil(t, single.Request.Criteria.GetCondition())
	require.Equal(t, modelv1.Condition_BINARY_OP_IN, single.Request.Criteria.GetCondition().Op)
	require.Equal(t, []string{"a/*", "b/*"}, nativeEntityTuples(t, single.Request.Criteria))
	exact := bindEntityAggregate(t, "host = 'a' AND region = 'east'")
	require.Equal(t, modelv1.LogicalExpression_LOGICAL_OP_AND, exact.Request.Criteria.GetLe().Op)
	require.NotNil(t, exact.Request.Criteria.GetLe().Left.GetCondition())
	require.NotNil(t, exact.Request.Criteria.GetLe().Right.GetCondition())
	require.Equal(t, []string{"a/east"}, nativeEntityTuples(t, exact.Request.Criteria))
}

func TestMeasureAggregateEntityNormalizationAlsoAppliesToLocalFallback(t *testing.T) {
	predicate := "host IN ('a','b','c') AND region IN ('east','west')"
	native := bindEntityAggregate(t, predicate)
	schema := entityAggregateSchema()
	schema.Fields[1].FieldType = databasev1.FieldType_FIELD_TYPE_INT
	template := entityAggregateTemplateWithSchema(t, predicate, schema)
	local, err := template.BindParams(nil, time.UnixMilli(0))
	require.NoError(t, err)
	require.Nil(t, local.Plan.NativeAggregate, "integer SUM retains the local checked-overflow implementation")
	require.True(t, proto.Equal(native.Request.Criteria, local.Request.Criteria))
	require.Equal(t, []string{"a/east", "a/west", "b/east", "b/west", "c/east", "c/west"}, nativeEntityTuples(t, local.Request.Criteria))
}

func TestMeasureAggregateEntityNormalizationRetainsIntegerZero(t *testing.T) {
	schema := entityAggregateSchema()
	schema.TagFamilies[0].Tags[len(schema.TagFamilies[0].Tags)-1].Type = databasev1.TagType_TAG_TYPE_INT
	template := entityAggregateTemplateWithSchema(t, "host IN ('','a') AND region IN (0,1)", schema)
	execution, err := template.BindParams(nil, time.UnixMilli(0))
	require.NoError(t, err)
	require.Equal(t, []string{"/0", "/1", "a/0", "a/1"}, nativeEntityTuples(t, execution.Request.Criteria))
}

func TestMeasureAggregateEntityNormalizationRejectsUnsafeOrUnboundedExpansion(t *testing.T) {
	cases := []struct {
		predicate string
		message   string
	}{
		{"host IN ('a','b') AND region IN ('east','west') AND (host = 'a' OR host = 'b')", "entity OR predicate"},
		{entityInPredicate("host", 23) + " AND " + entityInPredicate("region", 23), "512 tuples"},
	}
	for _, example := range cases {
		t.Run(example.message+example.predicate[:12], func(t *testing.T) {
			template := entityAggregateTemplate(t, example.predicate)
			_, err := template.BindParams(nil, time.UnixMilli(0))
			require.ErrorIs(t, err, bydbql.ErrInvalidMeasureQuery)
			require.ErrorContains(t, err, example.message)
		})
	}
	boundary := bindEntityAggregate(t, entityInPredicate("host", 16)+" AND "+entityInPredicate("region", 32))
	require.Len(t, nativeEntityTuples(t, boundary.Request.Criteria), 512)
}

func TestMeasureAggregateEntityContradictionsProduceEmptyInput(t *testing.T) {
	for _, predicate := range []string{
		"host = 'a' AND host = 'b' AND region = 'east'",
		"host IN ('a','b') AND host IN ('c','d') AND region = 'east'",
	} {
		t.Run(predicate, func(t *testing.T) {
			execution := bindEntityAggregate(t, predicate)
			require.True(t, execution.Plan.Empty)
			require.Nil(t, execution.Plan.NativeAggregate)
			require.NotNil(t, execution.Plan.Aggregate)
		})
	}
	statement, err := bydbql.Prepare("SELECT COUNT(f) AS c FROM g.m WHERE host IN ()")
	require.NoError(t, err)
	template, err := bydbql.NewTransformer(nil).CompileMeasureWithSchemas(context.Background(), statement,
		map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: entityAggregateSchema()})
	require.NoError(t, err)
	execution, err := template.BindParams(nil, time.UnixMilli(0))
	require.NoError(t, err)
	require.True(t, execution.Plan.Empty)
	require.Nil(t, execution.Plan.NativeAggregate)
	require.Empty(t, execution.Plan.Aggregate.GroupBy)
	require.Equal(t, ql.AggregateCount, execution.Plan.Aggregate.Calls[0].Function)
}

func entityInPredicate(name string, count int) string {
	values := make([]string, count)
	for index := range values {
		values[index] = fmt.Sprintf("'%s%02d'", name, index)
	}
	return name + " IN (" + strings.Join(values, ",") + ")"
}

func bindEntityAggregate(t *testing.T, predicate string) *bydbql.MeasureExecution {
	t.Helper()
	template := entityAggregateTemplate(t, predicate)
	execution, err := template.BindParams(nil, time.UnixMilli(0))
	require.NoError(t, err)
	return execution
}

func entityAggregateTemplate(t *testing.T, predicate string) *bydbql.MeasureTemplate {
	t.Helper()
	return entityAggregateTemplateWithSchema(t, predicate, entityAggregateSchema())
}

func entityAggregateSchema() *databasev1.Measure {
	schema := nativeAggregateSchema()
	schema.Entity.TagNames = []string{"host", "region"}
	schema.TagFamilies[0].Tags = append(schema.TagFamilies[0].Tags, &databasev1.TagSpec{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING})
	return schema
}

func entityAggregateTemplateWithSchema(t *testing.T, predicate string, schema *databasev1.Measure) *bydbql.MeasureTemplate {
	t.Helper()
	statement, err := bydbql.Prepare("SELECT host, region, SUM(f) AS s, COUNT(f) AS c FROM g.m WHERE " + predicate +
		" AND time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:06Z' GROUP BY host, region")
	require.NoError(t, err)
	template, err := bydbql.NewTransformer(nil).CompileMeasureWithSchemas(context.Background(), statement,
		map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: schema})
	require.NoError(t, err)
	return template
}

// nativeEntityTuples drives the existing public native entity parser with the
// actual bound request. Expected tuple lists are independently specified above.
func nativeEntityTuples(t *testing.T, criteria *modelv1.Criteria) []string {
	t.Helper()
	base := []*modelv1.TagValue{pbv1.AnyTagValue, pbv1.AnyTagValue}
	var parse func(*modelv1.Criteria) [][]*modelv1.TagValue
	parse = func(current *modelv1.Criteria) [][]*modelv1.TagValue {
		if current == nil {
			return [][]*modelv1.TagValue{base}
		}
		if expression := current.GetLe(); expression != nil {
			return logical.ParseEntities(expression.Op, base, parse(expression.Left), parse(expression.Right))
		}
		_, tuples, err := logical.ParseExprOrEntity(map[string]int{"host": 0, "region": 1}, base, current.GetCondition())
		require.NoError(t, err)
		return tuples
	}
	var result []string
	for _, tuple := range parse(criteria) {
		values := make([]string, len(tuple))
		for index, value := range tuple {
			if value == pbv1.AnyTagValue {
				values[index] = "*"
			} else if integer, ok := value.GetValue().(*modelv1.TagValue_Int); ok {
				values[index] = strconv.FormatInt(integer.Int.GetValue(), 10)
			} else {
				values[index] = value.GetStr().GetValue()
			}
		}
		result = append(result, strings.Join(values, "/"))
	}
	sort.Strings(result)
	return result
}
