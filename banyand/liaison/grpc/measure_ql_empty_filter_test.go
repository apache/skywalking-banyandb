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

package grpc

import (
	"testing"

	"github.com/stretchr/testify/require"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestQLEmptyBoundFilterCompletesAggregatesWithoutNativeScan(t *testing.T) {
	service, repo := newQLIndexService(t, true)
	repo.measure.Fields = []*databasev1.FieldSpec{
		{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		{Name: "hidden", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
	}
	service.measureSvc.entityRepo.OnAddOrUpdate(measureAddEvent(repo.measure))
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	for _, sample := range []struct {
		projection string
		suffix     string
		rows       int
		fields     int
	}{
		{projection: "COUNT(DISTINCT name) AS n, SUM(value) AS total", rows: 1, fields: 2},
		{projection: "name"},
		{projection: "name, COUNT(DISTINCT name) AS n", suffix: " GROUP BY name"},
		{projection: "COUNT(DISTINCT name) AS n", suffix: " HAVING COUNT(DISTINCT name) = 0", rows: 1, fields: 1},
		{projection: "COUNT(DISTINCT name) AS n", suffix: " HAVING COUNT(DISTINCT name) > 0"},
		{projection: "COUNT(DISTINCT name) AS n", suffix: " LIMIT 0"},
		{projection: "COUNT(DISTINCT name) AS n", suffix: " OFFSET 1"},
	} {
		for _, parameter := range []*modelv1.TagValue{
			{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{}}},
			{Value: &modelv1.TagValue_Null{Null: 0}},
		} {
			request := &bydbqlv1.QueryRequest{
				Query: "SELECT " + sample.projection + " FROM g.m " +
					"WHERE time >= ? AND time < ? AND hidden::FIELD > 0 AND name IN (?)" + sample.suffix,
				Params: []*modelv1.TagValue{
					bydbqlStrParam("1970-01-01T00:00:00Z"), bydbqlStrParam("1970-01-02T00:00:00Z"), parameter,
				},
			}
			response, err := service.Query(ctx, request)
			require.NoError(t, err, request.Query)
			rows := response.GetMeasureResult().GetDataPoints()
			require.Len(t, rows, sample.rows, request.Query)
			if sample.rows != 0 {
				require.Len(t, rows[0].Fields, sample.fields)
				require.Equal(t, "n", rows[0].Fields[0].Name)
				require.IsType(t, &modelv1.FieldValue_Int{}, rows[0].Fields[0].Value.Value)
				require.EqualValues(t, 0, rows[0].Fields[0].Value.GetInt().GetValue())
				if sample.fields == 2 {
					require.IsType(t, &modelv1.FieldValue_Null{}, rows[0].Fields[1].Value.Value)
				}
			}
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		}
	}
}

func TestQLNotInNullCompletesCountWithoutNativeScan(t *testing.T) {
	service, repo := newQLIndexService(t, true)
	repo.measure.Fields = []*databasev1.FieldSpec{{Name: "hidden", FieldType: databasev1.FieldType_FIELD_TYPE_INT}}
	service.measureSvc.entityRepo.OnAddOrUpdate(measureAddEvent(repo.measure))
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	for _, sample := range []struct {
		parameter *modelv1.TagValue
		list      string
	}{
		{list: "?", parameter: &modelv1.TagValue{Value: &modelv1.TagValue_Null{Null: 0}}},
		{list: "?, NULL", parameter: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"a"}}}}},
	} {
		request := &bydbqlv1.QueryRequest{
			Query: "SELECT COUNT(DISTINCT name) AS n FROM g.m " +
				"WHERE time >= ? AND time < ? AND hidden::FIELD > 0 AND name NOT IN (" + sample.list + ")",
			Params: []*modelv1.TagValue{
				bydbqlStrParam("1970-01-01T00:00:00Z"), bydbqlStrParam("1970-01-02T00:00:00Z"), sample.parameter,
			},
		}
		response, err := service.Query(ctx, request)
		require.NoError(t, err, request.Query)
		rows := response.GetMeasureResult().GetDataPoints()
		require.Len(t, rows, 1)
		require.Len(t, rows[0].Fields, 1)
		require.IsType(t, &modelv1.FieldValue_Int{}, rows[0].Fields[0].Value.Value)
		require.EqualValues(t, 0, rows[0].Fields[0].Value.GetInt().GetValue())
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}
