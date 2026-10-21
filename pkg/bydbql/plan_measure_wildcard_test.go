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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestMeasureWildcardProjectionOrder(t *testing.T) {
	schema := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "m", Group: "g"},
		Entity:   &databasev1.Entity{TagNames: []string{"rid"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "rid", Type: databasev1.TagType_TAG_TYPE_INT},
			{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "zone", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "labels", Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
			{Name: "nums", Type: databasev1.TagType_TAG_TYPE_INT_ARRAY},
		}}},
		Fields: []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT}, {Name: "f", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT}},
	}
	transformer := bydbql.NewTransformerWithSource(nil, typedSource{schema: schema})
	statement, err := bydbql.Prepare("SELECT * FROM MEASURE m IN g WHERE (rid <= 4 OR (rid <= 2 AND host = 'a')) ORDER BY TIME ASC")
	require.NoError(t, err)
	bound, err := statement.Bind(nil)
	require.NoError(t, err)
	// Recompile to exercise independently created lookup maps rather than
	// accepting one accidentally favorable map iteration order.
	for range 100 {
		template, compileErr := transformer.CompileMeasure(t.Context(), statement)
		require.NoError(t, compileErr)
		execution, bindErr := template.Bind(bound, time.Unix(0, 0))
		require.NoError(t, bindErr)
		require.Equal(t, []string{"rid", "host", "zone", "labels", "nums"}, execution.Request.TagProjection.TagFamilies[0].Tags)
		require.Equal(t, []string{"v", "f"}, execution.Request.FieldProjection.Names)
		direct, transformErr := transformer.TransformBound(t.Context(), bound)
		require.NoError(t, transformErr)
		require.Equal(t, execution.Request.TagProjection, direct.QueryRequest.(*measurev1.QueryRequest).TagProjection)
		require.Equal(t, execution.Request.FieldProjection, direct.QueryRequest.(*measurev1.QueryRequest).FieldProjection)
	}
}

func TestLegacyMeasureEmptyCountLimit(t *testing.T) {
	for _, example := range []struct {
		query string
		empty bool
	}{
		{query: "SELECT COUNT(DISTINCT rid) FROM MEASURE m IN g LIMIT 0", empty: true},
		{query: "SELECT COUNT(DISTINCT rid) FROM MEASURE m IN g LIMIT 1"},
		{query: "SELECT COUNT(DISTINCT rid) FROM MEASURE m IN g"},
		{query: "SELECT rid, COUNT(DISTINCT rid) FROM MEASURE m IN g GROUP BY rid LIMIT 0"},
	} {
		t.Run(example.query, func(t *testing.T) {
			execution := typedExecution(t, example.query)
			if example.empty {
				require.NotNil(t, execution.Plan)
				require.True(t, execution.Plan.Empty)
			} else {
				require.Nil(t, execution.Plan)
			}
		})
	}
}

func TestLegacyScalarCountPaginationKeepsCompleteInput(t *testing.T) {
	for _, query := range []string{
		"SELECT COUNT(DISTINCT rid) FROM MEASURE m IN g LIMIT 1",
		"SELECT COUNT(DISTINCT rid) FROM MEASURE m IN g LIMIT 20",
		"SELECT COUNT(DISTINCT rid) FROM MEASURE m IN g",
	} {
		t.Run(query, func(t *testing.T) {
			execution := typedExecution(t, query)
			require.Nil(t, execution.Plan)
			require.EqualValues(t, 4294967295, execution.Request.Limit)
			require.Zero(t, execution.Request.Offset)
		})
	}
	for _, query := range []string{
		"SELECT COUNT(DISTINCT rid) FROM MEASURE m IN g OFFSET 1",
		"SELECT COUNT(DISTINCT rid) FROM MEASURE m IN g LIMIT 1 OFFSET 1",
	} {
		t.Run(query, func(t *testing.T) {
			execution := typedExecution(t, query)
			require.NotNil(t, execution.Plan)
			require.True(t, execution.Plan.Empty)
		})
	}
}
