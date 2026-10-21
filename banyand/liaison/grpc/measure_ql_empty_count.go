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

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

// completeEmptyQLCountDistinct supplies the scalar SQL identity after native
// execution succeeds. Native aggregation only emits groups encountered in input;
// grouped queries and pages beyond the scalar row must remain empty.
func completeEmptyQLCountDistinct(request *measurev1.QueryRequest, response *measurev1.QueryResponse) *measurev1.QueryResponse {
	if response == nil || len(response.DataPoints) != 0 || request.GetAgg().GetFunction() != modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT ||
		request.GroupBy != nil || request.Top != nil || request.Offset != 0 {
		return response
	}
	for _, groupStatus := range response.GroupStatuses {
		if groupStatus != modelv1.Status_STATUS_SUCCEED {
			return response
		}
	}
	name := request.Agg.TagName
	if name == "" {
		name = request.Agg.FieldName
	}
	// Empty native responses can be a shared singleton. Never mutate them.
	result := &measurev1.QueryResponse{Trace: response.Trace, GroupStatuses: response.GroupStatuses}
	result.DataPoints = []*measurev1.DataPoint{{
		Fields: []*measurev1.DataPoint_Field{{Name: name, Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 0}}}}},
	}}
	return result
}

func finalizeEmptyQLCountDistinct(ctx context.Context, request *measurev1.QueryRequest, response **measurev1.QueryResponse, queryErr *error,
	fromQL bool, plan *ql.RawPlan,
) {
	if !fromQL || plan != nil {
		return
	}
	if *queryErr != nil {
		return
	}
	if *queryErr = ctx.Err(); *queryErr != nil {
		return
	}
	*response = completeEmptyQLCountDistinct(request, *response)
}
