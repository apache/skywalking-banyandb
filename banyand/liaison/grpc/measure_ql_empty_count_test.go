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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

func TestCompleteEmptyQLCountDistinct(t *testing.T) {
	base := &measurev1.QueryRequest{Agg: &measurev1.QueryRequest_Aggregation{
		Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT, TagName: "entity_id", TagFamily: "default",
	}}
	for _, field := range []bool{false, true} {
		request := proto.Clone(base).(*measurev1.QueryRequest)
		if field {
			request.Agg.TagName = ""
			request.Agg.FieldName = "value"
		}
		empty := &measurev1.QueryResponse{Trace: &commonv1.Trace{}, GroupStatuses: map[string]modelv1.Status{"g": modelv1.Status_STATUS_SUCCEED}}
		result := completeEmptyQLCountDistinct(request, empty)
		require.Empty(t, empty.DataPoints)
		require.Len(t, result.DataPoints, 1)
		require.Nil(t, result.DataPoints[0].Timestamp)
		require.Len(t, result.DataPoints[0].Fields, 1)
		require.Equal(t, int64(0), result.DataPoints[0].Fields[0].Value.GetInt().GetValue())
		name := "entity_id"
		if field {
			name = "value"
		}
		require.Equal(t, name, result.DataPoints[0].Fields[0].Name)
		require.True(t, proto.Equal(empty.Trace, result.Trace))
		require.Equal(t, empty.GroupStatuses, result.GroupStatuses)
	}
	cases := []struct {
		edit func(*measurev1.QueryRequest, *measurev1.QueryResponse)
		name string
	}{
		{name: "nonempty", edit: func(_ *measurev1.QueryRequest, response *measurev1.QueryResponse) {
			response.DataPoints = []*measurev1.DataPoint{{Sid: 42}}
		}},
		{name: "grouped", edit: func(request *measurev1.QueryRequest, _ *measurev1.QueryResponse) {
			request.GroupBy = &measurev1.QueryRequest_GroupBy{}
		}},
		{name: "top", edit: func(request *measurev1.QueryRequest, _ *measurev1.QueryResponse) {
			request.Top = &measurev1.QueryRequest_Top{Number: 1}
		}},
		{name: "offset", edit: func(request *measurev1.QueryRequest, _ *measurev1.QueryResponse) { request.Offset = 1 }},
		{name: "other aggregate", edit: func(request *measurev1.QueryRequest, _ *measurev1.QueryResponse) {
			request.Agg.Function = modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM
		}},
		{name: "raw projection", edit: func(request *measurev1.QueryRequest, _ *measurev1.QueryResponse) { request.Agg = nil }},
		{name: "failed gate", edit: func(_ *measurev1.QueryRequest, response *measurev1.QueryResponse) {
			response.GroupStatuses = map[string]modelv1.Status{"g": modelv1.Status_STATUS_NOT_FOUND}
		}},
	}
	for _, example := range cases {
		t.Run(example.name, func(t *testing.T) {
			request := proto.Clone(base).(*measurev1.QueryRequest)
			response := &measurev1.QueryResponse{}
			example.edit(request, response)
			require.Same(t, response, completeEmptyQLCountDistinct(request, response))
		})
	}
	require.Nil(t, completeEmptyQLCountDistinct(base, nil))
}

func TestEmptyQLCountDistinctDoesNotHideFailures(t *testing.T) {
	request := &measurev1.QueryRequest{Groups: []string{"g"}, Name: "m", Agg: &measurev1.QueryRequest_Aggregation{
		Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT, TagName: "entity_id", TagFamily: "default",
	}}
	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		service := &bydbQLService{measureSvc: &measureService{}}
		response, err := service.measureSvc.queryWithRawPlan(ctx, request, true, nil, true)
		require.Nil(t, response)
		require.Equal(t, codes.Canceled, status.Code(err))
	})
	t.Run("invalid range", func(t *testing.T) {
		service := newTestMeasureService(seededLocatorRepo(identity{group: "g", name: "m"}), 0)
		service.groupRepo = &groupRepo{inflight: make(map[string]*groupInflight)}
		response, err := service.queryWithRawPlan(t.Context(), request, true, nil, true)
		require.Nil(t, response)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})
	t.Run("failed gate", func(t *testing.T) {
		service := newTestMeasureService(seededLocatorRepo(identity{group: "g", name: "m"}), 0)
		service.groupRepo = &groupRepo{inflight: make(map[string]*groupInflight)}
		gated := proto.Clone(request).(*measurev1.QueryRequest)
		gated.GroupModRevisions = map[string]int64{"g": 50}
		now := time.Now().Truncate(time.Millisecond)
		gated.TimeRange = &modelv1.TimeRange{Begin: timestamppb.New(now.Add(-time.Minute)), End: timestamppb.New(now)}
		response, err := service.queryWithRawPlan(t.Context(), gated, true, nil, true)
		require.NoError(t, err)
		require.Empty(t, response.DataPoints)
		require.Equal(t, modelv1.Status_STATUS_EXPIRED_SCHEMA, response.GroupStatuses["g"])
	})
}

func TestFinalizeEmptyCountOnlyCompletesLegacyQL(t *testing.T) {
	request := &measurev1.QueryRequest{Agg: &measurev1.QueryRequest_Aggregation{
		Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT, TagName: "entity_id",
	}}
	for _, example := range []struct {
		plan     *ql.RawPlan
		name     string
		fromQL   bool
		wantRows int
	}{
		{name: "native request"},
		{name: "legacy QL", fromQL: true, wantRows: 1},
		{name: "typed QL", fromQL: true, plan: &ql.RawPlan{}},
		{name: "explicit zero limit", fromQL: true, plan: &ql.RawPlan{Empty: true}},
	} {
		t.Run(example.name, func(t *testing.T) {
			response := &measurev1.QueryResponse{}
			var queryErr error
			finalizeEmptyQLCountDistinct(t.Context(), request, &response, &queryErr, example.fromQL, example.plan)
			require.NoError(t, queryErr)
			require.Len(t, response.DataPoints, example.wantRows)
		})
	}
}
