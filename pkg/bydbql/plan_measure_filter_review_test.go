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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

type filterReviewSchemas map[string]*databasev1.Measure

func (s filterReviewSchemas) Measure(group, _ string) (*databasev1.Measure, bool) {
	schema, found := s[group]
	return schema, found
}
func (filterReviewSchemas) Stream(string, string) (*databasev1.Stream, bool) { return nil, false }
func (filterReviewSchemas) Trace(string, string) (*databasev1.Trace, bool)   { return nil, false }

func TestMeasureLegacyWhereIgnoresUnrelatedSchemaTypeDifferences(t *testing.T) {
	for _, different := range []string{"field", "tag", "both"} {
		t.Run(different, func(t *testing.T) {
			schemas := filterReviewSchemas{}
			for index, group := range []string{"g1", "g2"} {
				fieldType, tagType := databasev1.FieldType_FIELD_TYPE_INT, databasev1.TagType_TAG_TYPE_INT
				if index == 1 && different != "tag" {
					fieldType = databasev1.FieldType_FIELD_TYPE_FLOAT
				}
				if index == 1 && different != "field" {
					tagType = databasev1.TagType_TAG_TYPE_STRING
				}
				schemas[group] = &databasev1.Measure{
					Metadata: &commonv1.Metadata{Group: group, Name: "m"},
					TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
						{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
						{Name: "unused_tag", Type: tagType},
						{Name: "clash", Type: databasev1.TagType_TAG_TYPE_STRING},
					}}},
					Fields: []*databasev1.FieldSpec{{Name: "unused", FieldType: fieldType}, {Name: "clash", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
				}
			}
			transformer := bydbql.NewTransformerWithSource(nil, schemas)
			for _, suffix := range []string{"", " WHERE host = ? AND clash = 'a'"} {
				statement, err := bydbql.Prepare("SELECT host FROM MEASURE m IN (g1,g2) TIME BETWEEN '2026-01-01T00:00:00Z' AND '2026-01-01T00:01:00Z'" + suffix)
				require.NoError(t, err)
				var parameters []*modelv1.TagValue
				if suffix != "" {
					parameters = []*modelv1.TagValue{strParam("a")}
				}
				bound, err := statement.Bind(parameters)
				require.NoError(t, err)
				native, err := transformer.TransformBound(t.Context(), bound)
				require.NoError(t, err)
				template, err := transformer.CompileMeasure(t.Context(), statement)
				require.NoError(t, err)
				execution, err := template.Bind(bound, time.Unix(0, 0))
				require.NoError(t, err)
				require.Nil(t, execution.Plan)
				require.True(t, proto.Equal(native.QueryRequest.(*measurev1.QueryRequest), execution.Request), "legacy native request changed")
			}
			statement, err := bydbql.Prepare("SELECT host FROM MEASURE m IN (g1,g2) WHERE unused > 1")
			require.NoError(t, err)
			_, err = transformer.CompileMeasure(t.Context(), statement)
			require.ErrorIs(t, err, bydbql.ErrInvalidMeasureQuery)
			require.ErrorContains(t, err, "incompatible types across source groups")
		})
	}
}

func TestMeasureExpressionAndWhereArrayBindingsKeepHiddenInputsPrivate(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT CAST(v::FIELD IN (?) AS INT) AS member FROM g.m WHERE host::TAG IN (?) AND v::FIELD NOT IN (?)")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	for _, input := range []struct {
		integers, excluded []int64
		hosts              []string
	}{
		{}, {integers: []int64{1, 3}, excluded: []int64{2}, hosts: []string{"a", "b"}},
	} {
		execution, bindErr := template.BindParams([]*modelv1.TagValue{
			{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: input.integers}}},
			{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: input.hosts}}},
			{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: input.excluded}}},
		}, time.Unix(0, 0))
		require.NoError(t, bindErr)
		require.Len(t, execution.Plan.Slots, 1)
		require.Equal(t, "v", execution.Plan.Slots[0].Source.Name)
		require.Equal(t, []string{"v"}, execution.Request.FieldProjection.Names)
		require.Len(t, execution.Request.TagProjection.TagFamilies, 1)
		require.Equal(t, []string{"host"}, execution.Request.TagProjection.TagFamilies[0].Tags)
		selected := execution.Plan.Expressions.Outputs[0].Expr.Args[0]
		require.Len(t, selected.Args, len(input.integers)+1)
		for index, want := range input.integers {
			require.Equal(t, ql.ExpressionInt, selected.Args[index+1].Type)
			require.Equal(t, want, selected.Args[index+1].Literal.Int64)
		}
		membership, excluded := execution.Plan.Filter.Args[0], execution.Plan.Filter.Args[1]
		require.Len(t, membership.Args, len(input.hosts)+1)
		require.Len(t, excluded.Args, len(input.excluded)+1)
		for index, want := range input.hosts {
			require.Equal(t, ql.ExpressionString, membership.Args[index+1].Type)
			require.Equal(t, want, membership.Args[index+1].Literal.String)
		}
		for index, want := range input.excluded {
			require.Equal(t, ql.ExpressionInt, excluded.Args[index+1].Type)
			require.Equal(t, want, excluded.Args[index+1].Literal.Int64)
		}
	}
}
