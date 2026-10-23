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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type typedSource struct{ schema *databasev1.Measure }

func (s typedSource) Measure(string, string) (*databasev1.Measure, bool) { return s.schema, true }
func (typedSource) Stream(string, string) (*databasev1.Stream, bool)     { return nil, false }
func (typedSource) Trace(string, string) (*databasev1.Trace, bool)       { return nil, false }
func typedTransformer() *bydbql.Transformer {
	return bydbql.NewTransformerWithSource(nil, typedSource{schema: &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "m", Group: "g"},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "default", Tags: []*databasev1.TagSpec{
				{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "rid", Type: databasev1.TagType_TAG_TYPE_INT},
				{Name: "clash", Type: databasev1.TagType_TAG_TYPE_STRING},
			}},
			{Name: "extra", Tags: []*databasev1.TagSpec{{Name: "location", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			{Name: "__ql", Tags: []*databasev1.TagSpec{{Name: "clock", Type: databasev1.TagType_TAG_TYPE_STRING}}},
		},
		Fields: []*databasev1.FieldSpec{
			{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
			{Name: "f", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT},
			{Name: "clash", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		},
	}})
}

func TestTypedMeasureAliasesAndFreshBinding(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT host AS node, v::FIELD AS value, v::FIELD AS copy, time AS observed FROM g.m WHERE time >= ? AND time < ? LIMIT ?")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(context.Background(), statement)
	require.NoError(t, err)
	require.NotNil(t, template)
	for i := int64(0); i < 2; i++ {
		start := time.UnixMilli(1000 + i*1000).UTC()
		bound, bindErr := statement.Bind([]*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: start.Format(time.RFC3339Nano)}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: start.Add(time.Second).Format(time.RFC3339Nano)}}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 3}}},
		})
		require.NoError(t, bindErr)
		execution, bindErr := template.Bind(bound, time.Unix(0, 0))
		require.NoError(t, bindErr)
		require.Equal(t, start, execution.Request.GetTimeRange().GetBegin().AsTime())
		require.Len(t, execution.Plan.Slots, 4)
		require.Equal(t, "node", execution.Plan.Slots[0].Output.Name)
		require.Equal(t, "host", execution.Plan.Slots[0].Source.Name)
		require.Equal(t, "copy", execution.Plan.Slots[2].Output.Name)
		require.Equal(t, vectorized.RoleTimestamp, execution.Plan.Slots[3].Source.Role)
		require.Equal(t, "__ql", execution.Plan.Slots[3].Output.TagFamily)
		require.Equal(t, uint32(3), *execution.Plan.Limit)
	}
}

func typedExecution(t *testing.T, query string, params ...*modelv1.TagValue) *bydbql.MeasureExecution {
	t.Helper()
	statement, err := bydbql.Prepare(query)
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(context.Background(), statement)
	require.NoError(t, err)
	bound, err := statement.Bind(params)
	require.NoError(t, err)
	execution, err := template.Bind(bound, time.UnixMilli(0))
	require.NoError(t, err)
	return execution
}

func TestTypedMeasureTimeLattice(t *testing.T) {
	tests := []struct {
		predicate  string
		begin, end int64
		empty      bool
	}{
		{"time > '1970-01-01T00:00:01Z' AND time <= '1970-01-01T00:00:03Z'", 1001, 3000, false},
		{"time = '1970-01-01T00:00:02Z'", 2000, 2000, false},
		{"time >= '1970-01-01T00:00:01.000001Z' AND time < '1970-01-01T00:00:02.000001Z'", 1001, 2000, false},
		{"time > '1969-12-31T23:59:59.999500Z' AND time <= '1970-01-01T00:00:00.000500Z'", 0, 0, false},
		{"time = '1970-01-01T00:00:02.000001Z'", 0, 0, true},
		{"time > '1970-01-01T00:00:03Z' AND time < '1970-01-01T00:00:02Z'", 0, 0, true},
	}
	for _, test := range tests {
		t.Run(test.predicate, func(t *testing.T) {
			execution := typedExecution(t, "SELECT rid FROM g.m WHERE "+test.predicate)
			require.Equal(t, test.empty, execution.Plan.Empty)
			require.Equal(t, test.begin, execution.Request.TimeRange.Begin.AsTime().UnixMilli())
			require.Equal(t, test.end, execution.Request.TimeRange.End.AsTime().UnixMilli())
			require.False(t, execution.Plan.Residual)
		})
	}
}

func TestTypedMeasureTimeUnionAndLegacyIntersection(t *testing.T) {
	execution := typedExecution(t, "SELECT rid FROM g.m TIME BETWEEN '1970-01-01T00:00:01Z' AND '1970-01-01T00:00:05Z' "+
		"WHERE (time <= '1970-01-01T00:00:01Z' OR time >= '1970-01-01T00:00:05Z') AND host = 'a' LIMIT 1 OFFSET 2")
	require.True(t, execution.Plan.Residual)
	require.Len(t, execution.Plan.Ranges, 2)
	require.EqualValues(t, 1000, execution.Plan.Ranges[0].Begin)
	require.EqualValues(t, 1001, execution.Plan.Ranges[0].End)
	require.EqualValues(t, 5000, execution.Plan.Ranges[1].Begin)
	require.EqualValues(t, 5001, execution.Plan.Ranges[1].End)
	require.EqualValues(t, 1000, execution.Request.TimeRange.Begin.AsTime().UnixMilli())
	require.EqualValues(t, 5000, execution.Request.TimeRange.End.AsTime().UnixMilli())
	require.EqualValues(t, 0, execution.Request.Offset)
	require.EqualValues(t, 1, *execution.Plan.Limit)
	require.EqualValues(t, 2, execution.Plan.Offset)
	require.NotNil(t, execution.Request.Criteria)
	require.Equal(t, "a", execution.Request.Criteria.GetCondition().GetValue().GetStr().GetValue())
}

func TestTypedMeasureFreshNowAndLimitPresence(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT host AS node FROM MEASURE m IN g WHERE time >= now() - INTERVAL '1s' AND time <= now() LIMIT ?")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(context.Background(), statement)
	require.NoError(t, err)
	for i := int64(0); i < 2; i++ {
		bound, bindErr := statement.Bind([]*modelv1.TagValue{{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: i}}}})
		require.NoError(t, bindErr)
		execution, bindErr := template.Bind(bound, time.UnixMilli(2000+i*1000))
		require.NoError(t, bindErr)
		require.EqualValues(t, 1000+i*1000, execution.Request.TimeRange.Begin.AsTime().UnixMilli())
		require.EqualValues(t, 2000+i*1000, execution.Request.TimeRange.End.AsTime().UnixMilli())
		require.Equal(t, i == 0, execution.Plan.Empty)
	}
	uncapped := typedExecution(t, "SELECT host FROM g.m")
	require.Nil(t, uncapped.Plan.Limit)
	require.NotNil(t, uncapped.Request.TimeRange)
	require.EqualValues(t, -9223372036854, uncapped.Request.TimeRange.Begin.AsTime().UnixMilli())
	require.EqualValues(t, 9223372036854, uncapped.Request.TimeRange.End.AsTime().UnixMilli())
	require.EqualValues(t, 4294967295, uncapped.Request.Limit)
	legacy := typedExecution(t, "SELECT host FROM MEASURE m IN g LIMIT 0")
	require.Nil(t, legacy.Plan)
	require.Zero(t, legacy.Request.Limit)
}

func TestTypedMeasureNamesFailBeforeExecution(t *testing.T) {
	queries := []string{
		"SELECT clash FROM g.m",
		"SELECT v::FIELD AS same, f::FIELD AS same FROM MEASURE m IN g",
		"SELECT host AS same, rid AS same FROM MEASURE m IN g",
		"SELECT __ql.clock::TAG, time AS clock FROM g.m",
		"SELECT missing FROM g.m",
		"SELECT host AS node FROM g.m WHERE node = 'a'",
		"SELECT host FROM g.m WHERE time >= now() OR host = 'a'",
		"SELECT host, SUM(v) AS total FROM MEASURE m IN g",
		"SELECT host FROM g.m LIMIT -1",
		"SELECT host FROM g.m WHERE time = 1",
		"SELECT host FROM g.m WHERE time = '2500-01-01T00:00:00Z'",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			statement, err := bydbql.Prepare(query)
			require.NoError(t, err)
			template, err := typedTransformer().CompileMeasure(context.Background(), statement)
			if err == nil {
				bound, bindErr := statement.Bind(nil)
				require.NoError(t, bindErr)
				_, err = template.Bind(bound, time.UnixMilli(0))
			}
			require.ErrorIs(t, err, bydbql.ErrInvalidMeasureQuery)
		})
	}
	qualified := typedExecution(t, "SELECT clash::TAG AS tag_clash, clash::FIELD AS field_clash, extra.location::TAG AS city FROM g.m")
	require.Len(t, qualified.Plan.Slots, 3)
	require.Equal(t, "extra", qualified.Plan.Slots[2].Source.TagFamily)
}

func TestTypedMeasureTimestampBindingAndDomain(t *testing.T) {
	boundTime := time.UnixMilli(2000).UTC()
	execution := typedExecution(t, "SELECT rid FROM g.m WHERE time = ?",
		&modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(boundTime)}})
	require.Equal(t, boundTime, execution.Request.TimeRange.Begin.AsTime())
	for _, millis := range []int64{-9223372036854, 9223372036854} {
		point := time.UnixMilli(millis)
		execution = typedExecution(t, "SELECT rid FROM g.m WHERE time = ?",
			&modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(point)}})
		require.False(t, execution.Plan.Empty)
		require.Equal(t, millis, execution.Request.TimeRange.Begin.AsTime().UnixMilli())
		require.Equal(t, millis, execution.Request.TimeRange.End.AsTime().UnixMilli())
	}
}

func TestTypedMeasureConcurrentInstancesAndSchemaSnapshot(t *testing.T) {
	schema := &databasev1.Measure{
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING}}}},
		Fields:      []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}
	transformer := bydbql.NewTransformerWithSource(nil, typedSource{schema: schema})
	statement, err := bydbql.Prepare("SELECT host AS node, v::FIELD AS value FROM g.m WHERE time = ? AND host = ? LIMIT ?")
	require.NoError(t, err)
	template, err := transformer.CompileMeasure(context.Background(), statement)
	require.NoError(t, err)
	schema.TagFamilies[0].Tags[0].Name = "poisoned"
	schema.Fields[0].FieldType = databasev1.FieldType_FIELD_TYPE_FLOAT
	var wait sync.WaitGroup
	errors := make(chan error, 32)
	for i := int64(0); i < 32; i++ {
		wait.Go(func() {
			point := time.UnixMilli(1000 + i)
			bound, bindErr := statement.Bind([]*modelv1.TagValue{
				{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(point)}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprint(i)}}},
				{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: i}}},
			})
			if bindErr != nil {
				errors <- bindErr
				return
			}
			execution, bindErr := template.Bind(bound, time.Now())
			if bindErr != nil {
				errors <- bindErr
				return
			}
			if execution.Request.TimeRange.Begin.AsTime().UnixMilli() != point.UnixMilli() || *execution.Plan.Limit != uint32(i) {
				errors <- fmt.Errorf("bound values leaked across requests")
				return
			}
			if execution.Plan.Slots[0].Source.Name != "host" || execution.Plan.Slots[1].Source.Type != vectorized.ColumnTypeInt64 {
				errors <- fmt.Errorf("schema snapshot mutated")
				return
			}
			execution.Plan.Slots[0].Output.Name = "poisoned-output"
			execution.Request.TagProjection.TagFamilies[0].Tags[0] = "poisoned-request"
		})
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
}

func TestTypedMeasureExtensionsDoNotChangeOtherDispatch(t *testing.T) {
	for _, query := range []string{
		"SELECT now, interval, as FROM STREAM now IN interval",
		"SELECT a-b, a*b FROM STREAM sw IN default WHERE host = 'a'",
		"SELECT host FROM STREAM sw IN default",
		"SHOW TOP 1 FROM MEASURE rule IN default",
	} {
		statement, err := bydbql.Prepare(query)
		require.NoError(t, err)
		template, err := typedTransformer().CompileMeasure(context.Background(), statement)
		require.NoError(t, err)
		require.Nil(t, template)
	}
	for _, query := range []string{
		"SELECT host AS node FROM STREAM sw IN default",
		"SELECT host AS node FROM TRACE tr IN default",
		"SELECT host AS node FROM PROPERTY p IN default",
		"SELECT host FROM STREAM sw IN default WHERE time = '1970-01-01T00:00:01Z'",
	} {
		statement, err := bydbql.Prepare(query)
		require.NoError(t, err)
		bound, err := statement.Bind(nil)
		require.NoError(t, err)
		_, err = typedTransformer().TransformBound(context.Background(), bound)
		require.ErrorContains(t, err, "typed measure planning")
	}
	for _, query := range []string{
		"SELECT host FROM STREAM sw IN default WHERE host = now()",
		"SELECT host FROM MEASURE m IN g WHERE host = 'a' - INTERVAL '1s'",
		"SELECT host FROM MEASURE m IN g WHERE host IN (now())",
		"SHOW TOP 1 FROM g.m",
	} {
		_, err := bydbql.Prepare(query)
		require.Error(t, err)
	}
}

func TestTypedMeasureTimeAliasOrdering(t *testing.T) {
	execution := typedExecution(t, "SELECT time AS observed FROM g.m ORDER BY observed DESC")
	require.NotNil(t, execution.Request.TagProjection)
	require.Len(t, execution.Plan.Slots, 1)
	require.Equal(t, vectorized.RoleTimestamp, execution.Plan.Slots[0].Source.Role)
	require.Empty(t, execution.Request.OrderBy.IndexRuleName)
	require.Equal(t, modelv1.Sort_SORT_DESC, execution.Request.OrderBy.Sort)
	for _, query := range []string{
		"SELECT host AS node FROM g.m ORDER BY node",
		"SELECT host AS node FROM g.m ORDER BY host",
		"SELECT host AS node, v::FIELD AS node FROM g.m ORDER BY node",
	} {
		statement, err := bydbql.Prepare(query)
		require.NoError(t, err)
		_, err = typedTransformer().CompileMeasure(context.Background(), statement)
		require.ErrorIs(t, err, bydbql.ErrInvalidMeasureQuery)
	}
	fallback := typedExecution(t, "SELECT host AS node FROM g.m ORDER BY idx_host DESC")
	require.Equal(t, "idx_host", fallback.Request.OrderBy.IndexRuleName)
}

func TestMeasureLegacyTemplateMatchesNativeTransformer(t *testing.T) {
	tests := []struct {
		query  string
		params []*modelv1.TagValue
	}{
		{
			query: "SELECT host, v::FIELD FROM MEASURE m IN g TIME BETWEEN '1970-01-01T00:00:00Z' AND '1970-01-01T00:00:10Z' WHERE host IN (?) LIMIT ? OFFSET ?",
			params: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"a", "b"}}}},
				{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 3}}},
				{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 1}}},
			},
		},
		{query: "SELECT host, SUM(v) FROM MEASURE m IN g TIME BETWEEN '1970-01-01T00:00:00Z' AND '1970-01-01T00:00:10Z' GROUP BY host"},
		{query: "SELECT host, COUNT(DISTINCT host) FROM MEASURE m IN g TIME BETWEEN '1970-01-01T00:00:00Z' AND '1970-01-01T00:00:10Z' GROUP BY host"},
		{
			query:  "SELECT TOP ? v, host, v::FIELD FROM MEASURE m IN g TIME BETWEEN '1970-01-01T00:00:00Z' AND '1970-01-01T00:00:10Z'",
			params: []*modelv1.TagValue{{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 2}}}},
		},
	}
	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			statement, err := bydbql.Prepare(test.query)
			require.NoError(t, err)
			transformer := typedTransformer()
			bound, err := statement.Bind(test.params)
			require.NoError(t, err)
			original, err := transformer.TransformBound(context.Background(), bound)
			require.NoError(t, err)
			template, err := transformer.CompileMeasure(context.Background(), statement)
			require.NoError(t, err)
			execution, err := template.Bind(bound, time.Now())
			require.NoError(t, err)
			require.Nil(t, execution.Plan)
			require.True(t, proto.Equal(original.QueryRequest, execution.Request), "legacy native request changed")
		})
	}
}
