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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestMeasureContainsRemainsAnOrdinaryIdentifier(t *testing.T) {
	for _, resource := range []string{"MEASURE", "STREAM", "TRACE", "PROPERTY"} {
		for _, name := range []string{"contains", "contains-svc", "contains_svc"} {
			query := fmt.Sprintf("SELECT contains FROM %s %s IN %s WHERE contains = ?", resource, name, name)
			t.Run(resource+"/"+name, func(t *testing.T) {
				grammar, err := bydbql.ParseQuery(query)
				require.NoError(t, err)
				require.Equal(t, name, grammar.Select.From.ResourceName)
				require.Equal(t, []string{name}, grammar.Select.From.In.Groups)
				statement, err := bydbql.Prepare(query)
				require.NoError(t, err)
				_, err = statement.Bind([]*modelv1.TagValue{strParam("red")})
				require.NoError(t, err)
			})
		}
	}
	transformer := bydbql.NewTransformerWithSource(nil, typedSource{schema: &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "contains-svc", Group: "contains"},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "contains", Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
		Fields: []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}})
	for _, predicate := range []string{"contains = ?", "contains::TAG = ? AND v::FIELD > 0", "contains CONTAINS ?"} {
		statement, err := bydbql.Prepare("SELECT contains FROM MEASURE contains-svc IN contains WHERE " + predicate)
		require.NoError(t, err)
		template, err := transformer.CompileMeasure(context.Background(), statement)
		require.NoError(t, err)
		execution, err := template.BindParams([]*modelv1.TagValue{strParam("red")}, time.UnixMilli(0))
		require.NoError(t, err)
		require.Equal(t, "contains-svc", execution.Request.Name)
	}
}

func TestMeasureContainsAliasesStayMeasureScoped(t *testing.T) {
	for _, resource := range []string{"STREAM", "TRACE", "PROPERTY"} {
		for _, spelling := range []string{"CONTAINS", "NOT CONTAINS", "contains", "not contains"} {
			for _, query := range []string{
				fmt.Sprintf("SELECT host FROM %s s IN g WHERE labels %s ?", resource, spelling),
				fmt.Sprintf("SELECT host FROM %s s IN g WHERE host = 'a' OR (labels %s ? AND host = 'b')", resource, spelling),
				fmt.Sprintf("SHOW TOP 1 FROM %s s IN g WHERE labels %s ?", resource, spelling),
			} {
				t.Run(query, func(t *testing.T) {
					_, err := bydbql.Prepare(query)
					require.ErrorContains(t, err, "CONTAINS is supported only for measures")
				})
			}
		}
		statement, err := bydbql.Prepare(fmt.Sprintf("SELECT host FROM %s s IN g WHERE labels HAVING ?", resource))
		require.NoError(t, err)
		_, err = statement.Bind([]*modelv1.TagValue{strParam("red")})
		require.NoError(t, err)
	}
}

func TestMeasureHavingDistinctUsesTheSelectAggregateRules(t *testing.T) {
	for _, function := range []string{"SUM", "AVG", "MEAN", "MIN", "MAX"} {
		for _, expression := range []string{function + "(DISTINCT v)>0", "COALESCE(" + function + "(DISTINCT v),0)>0"} {
			query := "SELECT host FROM g.m GROUP BY host HAVING " + expression
			_, err := bydbql.Prepare(query)
			require.ErrorContains(t, err, "DISTINCT is only valid inside COUNT, got "+function+"(DISTINCT ...)")
		}
	}
	statement, err := bydbql.Prepare("SELECT host FROM g.m GROUP BY host HAVING COUNT(DISTINCT v)>0")
	require.NoError(t, err)
	_, err = typedTransformer().CompileMeasure(context.Background(), statement)
	require.NoError(t, err)
}
