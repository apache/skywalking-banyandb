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

package stream

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestBuildStreamBatchSchemaWithOrderTag(t *testing.T) {
	tagProjection := []model.TagProjection{
		{Family: "searchable", Names: []string{"service_id", "state"}},
		{Family: "data", Names: []string{"data_binary"}},
	}
	schema := BuildStreamBatchSchema(tagProjection, "searchable", "state")

	// Timestamp, ElementID, SeriesID, 3 tags, OrderKey.
	require.Len(t, schema.Columns, 7)

	require.Equal(t, StreamColumnNameTimestamp, schema.Columns[0].Name)
	require.Equal(t, vectorized.RoleTimestamp, schema.Columns[0].Role)
	require.Equal(t, vectorized.ColumnTypeInt64, schema.Columns[0].Type)

	require.Equal(t, StreamColumnNameElementID, schema.Columns[1].Name)
	require.Equal(t, vectorized.RoleElementID, schema.Columns[1].Role)
	require.Equal(t, vectorized.ColumnTypeInt64, schema.Columns[1].Type)

	require.Equal(t, StreamColumnNameSeriesID, schema.Columns[2].Name)
	require.Equal(t, vectorized.RoleSeriesID, schema.Columns[2].Role)
	require.Equal(t, vectorized.ColumnTypeInt64, schema.Columns[2].Type)

	for i := 3; i <= 5; i++ {
		require.Equal(t, vectorized.RoleTag, schema.Columns[i].Role)
		require.Equal(t, vectorized.ColumnTypeTagValue, schema.Columns[i].Type)
	}

	require.Equal(t, StreamColumnNameOrderKey, schema.Columns[6].Name)
	require.Equal(t, vectorized.RoleOrderKey, schema.Columns[6].Role)
	require.Equal(t, vectorized.ColumnTypeBytes, schema.Columns[6].Type)

	require.Equal(t, 0, schema.TimestampIndex())
	require.Equal(t, 1, schema.ElementIDIndex())
	require.Equal(t, 2, schema.SeriesIDIndex())
	require.Equal(t, 6, schema.OrderKeyIndex())

	// Two families in projection order, grouped by family.
	require.Len(t, schema.TagFamilyGroups, 2)
	require.Equal(t, "searchable", schema.TagFamilyGroups[0].Family)
	require.Equal(t, []int{3, 4}, schema.TagFamilyGroups[0].Columns)
	require.Equal(t, "data", schema.TagFamilyGroups[1].Family)
	require.Equal(t, []int{5}, schema.TagFamilyGroups[1].Columns)

	serviceIdx, ok := schema.TagIndex("searchable", "service_id")
	require.True(t, ok)
	require.Equal(t, 3, serviceIdx)
	stateIdx, ok := schema.TagIndex("searchable", "state")
	require.True(t, ok)
	require.Equal(t, 4, stateIdx)
	binaryIdx, ok := schema.TagIndex("data", "data_binary")
	require.True(t, ok)
	require.Equal(t, 5, binaryIdx)
}

func TestBuildStreamBatchSchemaNoOrderTag(t *testing.T) {
	tagProjection := []model.TagProjection{
		{Family: "searchable", Names: []string{"service_id", "state"}},
		{Family: "data", Names: []string{"data_binary"}},
	}
	schema := BuildStreamBatchSchema(tagProjection, "", "")

	// Timestamp, ElementID, SeriesID, 3 tags — no OrderKey.
	require.Len(t, schema.Columns, 6)
	require.Equal(t, -1, schema.OrderKeyIndex())
	require.Equal(t, 0, schema.TimestampIndex())
	require.Equal(t, 1, schema.ElementIDIndex())
	require.Equal(t, 2, schema.SeriesIDIndex())
}

func TestElementIDColumnRoundTrip(t *testing.T) {
	cases := []uint64{
		0,
		1,
		math.MaxInt64,
		math.MaxUint64,
		math.MaxInt64 + 1,
	}
	for _, id := range cases {
		require.Equal(t, id, ColumnToElementID(ElementIDToColumn(id)))
		require.Equal(t, id, ColumnToSeriesID(SeriesIDToColumn(id)))
	}
}
