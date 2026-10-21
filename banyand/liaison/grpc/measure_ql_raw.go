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

package grpc

import (
	"context"
	"fmt"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

func isEmptyRawQuery(plan *ql.RawPlan) bool {
	return plan != nil && (plan.Empty || plan.Limit != nil && *plan.Limit == 0)
}

func measureQueryResponseWithPlan(ctx context.Context, data any, statuses map[string]modelv1.Status,
	plan *ql.RawPlan,
) (*measurev1.QueryResponse, error) {
	if plan == nil {
		return measureQueryResponse(ctx, data, statuses)
	}
	return measureRawQueryResponse(ctx, data, statuses, plan)
}

func measureRawQueryResponse(ctx context.Context, data any, statuses map[string]modelv1.Status,
	plan *ql.RawPlan,
) (_ *measurev1.QueryResponse, err error) {
	result, ok := data.(vmeasure.BatchQueryResult)
	if !ok {
		if failure, failed := data.(error); failed {
			return nil, queryStatus(failure)
		}
		return nil, fmt.Errorf("typed measure query requires a native batch result, got %T", data)
	}
	scan := ql.NewNativeScan(result)
	defer func() {
		if closeErr := scan.Close(); err == nil {
			err = queryStatus(closeErr)
		}
	}()
	operator, err := ql.WrapRaw(ctx, scan, plan)
	if err != nil {
		return nil, queryStatus(err)
	}
	iterator, err := ql.RawIterator(ctx, operator)
	if err != nil {
		return nil, queryStatus(err)
	}
	defer func() {
		if closeErr := iterator.Close(); err == nil {
			err = queryStatus(closeErr)
		}
	}()
	response := &measurev1.QueryResponse{GroupStatuses: statuses}
	for iterator.Next() {
		if cancelErr := ctx.Err(); cancelErr != nil {
			return nil, queryStatus(cancelErr)
		}
		current := iterator.Current()
		if len(current) > 0 {
			response.DataPoints = append(response.DataPoints, current[0].GetDataPoint())
		}
	}
	if failed, hasError := iterator.(interface{ Err() error }); hasError {
		if err = failed.Err(); err != nil {
			return nil, queryStatus(err)
		}
	}
	if err = ctx.Err(); err != nil {
		return nil, queryStatus(err)
	}
	if err = iterator.Close(); err != nil {
		return nil, queryStatus(err)
	}
	response.Trace = result.QueryTrace()
	return response, nil
}
