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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func reusableMeasureSchema(group string) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Group: group, Name: "m", ModRevision: 10},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
		Fields: []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}
}

func TestMeasureTemplateCapturedSchemasAndFreshParams(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT host AS node, v::FIELD AS reading FROM MEASURE m IN (g2,g1,g2) WHERE time <= now() AND host IN (?) LIMIT ?")
	require.NoError(t, err)
	sources := []bydbql.MeasureSource{{Group: "g2", Name: "m"}, {Group: "g1", Name: "m"}, {Group: "g2", Name: "m"}}
	require.Equal(t, sources, statement.MeasureSources())
	require.Equal(t, "SELECT host AS node, v::FIELD AS reading FROM MEASURE m IN (g2,g1,g2) WHERE time <= now() AND host IN (?) LIMIT ?", statement.QueryText())
	sources[0].Group = "changed"
	require.Equal(t, "g2", statement.MeasureSources()[0].Group)

	schemas := map[bydbql.MeasureSource]*databasev1.Measure{
		{Group: "g1", Name: "m"}: reusableMeasureSchema("g1"),
		{Group: "g2", Name: "m"}: reusableMeasureSchema("g2"),
	}
	transformer := bydbql.NewTransformer(nil)
	template, err := transformer.CompileMeasureWithSchemas(context.Background(), statement, schemas)
	require.NoError(t, err)
	require.NotNil(t, template)
	initialSize := template.EstimatedSize()
	require.Greater(t, initialSize, statement.EstimatedSize())
	schemas[bydbql.MeasureSource{Group: "g2", Name: "m"}].TagFamilies[0].Tags[0].Name = "changed"
	for i := int64(0); i < 2; i++ {
		params := []*modelv1.TagValue{
			{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{strings.Repeat("a", int(i+1))}}}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: i}}},
		}
		execution, bindErr := template.BindParams(params, time.UnixMilli(1000+i))
		require.NoError(t, bindErr)
		require.Equal(t, []string{"g2", "g1", "g2"}, execution.Request.Groups)
		require.EqualValues(t, 1000+i, execution.Request.TimeRange.End.AsTime().UnixMilli())
		require.Equal(t, i == 0, execution.Plan.Empty)
		require.EqualValues(t, i, *execution.Plan.Limit)
		require.Equal(t, "host", execution.Plan.Slots[0].Source.Name)
		_, marshalErr := proto.Marshal(execution.Request)
		require.NoError(t, marshalErr)
		require.Equal(t, initialSize, template.EstimatedSize())
	}
	// A reparse may replace the parse-cache entry while the compiled template survives.
	reparsed, err := bydbql.Prepare("SELECT host AS node, v::FIELD AS reading FROM MEASURE m IN (g2,g1,g2) WHERE time <= now() AND host IN (?) LIMIT ?")
	require.NoError(t, err)
	bound, err := reparsed.Bind([]*modelv1.TagValue{
		{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"other"}}}},
		{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 2}}},
	})
	require.NoError(t, err)
	execution, err := template.Bind(bound, time.UnixMilli(1000))
	require.NoError(t, err)
	require.EqualValues(t, 2, *execution.Plan.Limit)
	different, err := bydbql.Prepare("SELECT host AS other, v::FIELD AS reading FROM MEASURE m IN (g2,g1,g2) WHERE time <= now() AND host IN (?) LIMIT ?")
	require.NoError(t, err)
	otherBound, err := different.Bind([]*modelv1.TagValue{
		{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"other"}}}},
		{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 2}}},
	})
	require.NoError(t, err)
	_, err = template.Bind(otherBound, time.UnixMilli(1000))
	require.ErrorIs(t, err, bydbql.ErrInvalidMeasureQuery)
	_, err = template.BindParams(nil, time.UnixMilli(1000))
	require.Error(t, err)
}

func TestMeasureTemplateCapturedSchemaMissAndRetainedSize(t *testing.T) {
	transformer := bydbql.NewTransformer(nil)
	statement, err := bydbql.Prepare("SELECT host, v::FIELD FROM MEASURE m IN g WHERE host = ?")
	require.NoError(t, err)
	_, err = transformer.CompileMeasureWithSchemas(context.Background(), statement, nil)
	require.ErrorContains(t, err, "measure g/m has no schema")
	schemas := map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: reusableMeasureSchema("g")}
	template, err := transformer.CompileMeasureWithSchemas(context.Background(), statement, schemas)
	require.NoError(t, err)
	initialSize := template.EstimatedSize()
	require.Greater(t, initialSize, statement.EstimatedSize())
	_, err = template.BindParams([]*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "a"}}}}, time.UnixMilli(1000))
	require.NoError(t, err)
	require.Equal(t, initialSize, template.EstimatedSize())
	wider, err := bydbql.Prepare("SELECT host AS first, host AS second, v::FIELD AS value, v::FIELD AS repeated FROM g.m WHERE time >= ?")
	require.NoError(t, err)
	wideTemplate, err := transformer.CompileMeasureWithSchemas(context.Background(), wider, schemas)
	require.NoError(t, err)
	require.Greater(t, wideTemplate.EstimatedSize(), initialSize)
	nonMeasure, err := bydbql.Prepare("SELECT * FROM STREAM s IN g WHERE host = ?")
	require.NoError(t, err)
	require.Empty(t, nonMeasure.MeasureSources())
	absent, err := transformer.CompileMeasureWithSchemas(context.Background(), nonMeasure, nil)
	require.NoError(t, err)
	require.Nil(t, absent)
}

func TestMeasureTemplateCapturedCompilationCancellation(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT host AS node FROM g.m WHERE time >= ?")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	template, err := bydbql.NewTransformer(nil).CompileMeasureWithSchemas(ctx, statement,
		map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: reusableMeasureSchema("g")})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, template)
}
