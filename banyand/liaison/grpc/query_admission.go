// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package grpc

import (
	"context"
	"errors"
	"fmt"

	pkgerrors "github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const maxQueryWindow uint32 = 100000

func admitQuery(ctx context.Context, budget *protector.QueryBudget, limit, offset, defaultLimit uint32) (context.Context, func(), error) {
	if budget == nil {
		budget = protector.QueryBudgetFor(nil)
	}
	if limit == 0 {
		limit = defaultLimit
	}
	// MaxUint32 is the historical list-all / max-limit sentinel used by OAP and
	// integration fixtures. Admit as a scan; capacity hints and ChargeResult bound memory.
	if query.IsUnboundedLimit(limit, offset) {
		admittedCtx, release, admissionErr := budget.AdmitScanContext(ctx)
		if admissionErr != nil {
			return ctx, nil, queryStatus(admissionErr)
		}
		return admittedCtx, release, nil
	}
	window, valid := query.AddWindow(limit, offset)
	if !valid || window > uint64(maxQueryWindow) {
		return ctx, nil, status.Error(codes.InvalidArgument, "query limit and offset exceed the maximum window")
	}
	// Admit reserves the complete dynamic assignment after accounting for current
	// sampled memory and outstanding requests.
	admittedCtx, release, reserveErr := budget.AdmitContext(ctx, limit, offset, defaultLimit)
	if reserveErr != nil {
		if errors.Is(reserveErr, context.Canceled) || errors.Is(reserveErr, context.DeadlineExceeded) {
			return ctx, nil, status.FromContextError(reserveErr).Err()
		}
		if errors.Is(reserveErr, protector.ErrQueryTooLarge) {
			return ctx, nil, status.Error(codes.InvalidArgument, reserveErr.Error())
		}
		return ctx, nil, status.Error(codes.ResourceExhausted, reserveErr.Error())
	}
	return admittedCtx, release, nil
}

// dataNodeQueryError turns a data node's error response into an error. A
// resource-exhausted status keeps the protector sentinel, so queryStatus can still
// report RESOURCE_EXHAUSTED to the client instead of a generic failure.
func dataNodeQueryError(dataErr *common.Error) error {
	if dataErr.Status() == modelv1.Status_STATUS_RESOURCE_EXHAUSTED {
		return fmt.Errorf("%w: %s", protector.ErrQueryResourceExhausted, dataErr.Error())
	}
	return pkgerrors.WithMessage(errQueryMsg, dataErr.Error())
}

// queryStatus preserves typed local resource failures at the public boundary.
func queryStatus(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if errors.Is(err, protector.ErrQueryTooLarge) || errors.Is(err, ql.ErrInvalidExpression) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, native.ErrQueryLimit) || errors.Is(err, protector.ErrQueryResourceExhausted) ||
		errors.Is(err, vectorized.ErrMemoryBudgetExceeded) {
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	return err
}

// Unordered Property queries use the limit only as a stopping condition, not a
// preallocation size. Preserve list-all callers while bounding actual results.
func admitPropertyQuery(ctx context.Context, budget *protector.QueryBudget, req *propertyv1.QueryRequest) (context.Context, func(), error) {
	if req.OrderBy != nil && req.OrderBy.TagName != "" {
		return admitQuery(ctx, budget, req.Limit, 0, 100)
	}
	if budget == nil {
		budget = protector.QueryBudgetFor(nil)
	}
	admittedCtx, release, admissionErr := budget.AdmitScanContext(ctx)
	if admissionErr != nil {
		return ctx, nil, queryStatus(admissionErr)
	}
	return admittedCtx, release, nil
}
