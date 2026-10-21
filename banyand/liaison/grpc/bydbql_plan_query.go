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
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

func planQueryStatus(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, protector.ErrQueryTooLarge) || errors.Is(err, protector.ErrQueryResourceExhausted) {
		return queryStatus(err)
	}
	if errors.Is(err, bydbql.ErrInvalidMeasureQuery) {
		return status.Errorf(codes.InvalidArgument, "failed to plan measure query: %v", err)
	}
	return status.Errorf(codes.Internal, "failed to transform to native request: %v", err)
}

func (b *bydbQLService) planQueryWithCache(ctx context.Context, statement *bydbql.PreparedStatement, bound *bydbql.BoundQuery,
) (*bydbql.TransformResult, *ql.RawPlan, *compiledPlanHandle, string, error) {
	if b.planCache == nil || len(statement.MeasureSources()) == 0 {
		result, plan, err := b.planQuery(ctx, statement, bound)
		return result, plan, nil, "", err
	}
	var now time.Time
	captured := false
	for attempt := 0; attempt < 2; attempt++ {
		handle, outcome, err := b.planCache.acquire(ctx, statement.QueryText(), statement)
		if err != nil {
			return nil, nil, nil, outcome, err
		}
		if handle == nil {
			handle, outcome, err = b.planCache.uncached(ctx, statement)
			if err != nil {
				return nil, nil, nil, outcome, err
			}
		}
		if hook := bydbQLPlanCacheHooks().AfterAcquire; hook != nil {
			hook(ctx, statement.QueryText())
		}
		if err = ctx.Err(); err != nil {
			handle.Release()
			return nil, nil, nil, outcome, err
		}
		if outcome != "bypass" && !handle.valid() {
			handle.Release()
			continue
		}
		if !captured {
			now = ql.ExecutionNow()
			captured = true
		}
		started := time.Now()
		execution, bindErr := handle.template.Bind(bound, now)
		b.planCache.observeDuration("instantiate", started)
		if bindErr != nil {
			handle.Release()
			return nil, nil, nil, outcome, bindErr
		}
		if outcome != "bypass" && !handle.valid() {
			handle.Release()
			continue
		}
		return &bydbql.TransformResult{Type: bydbql.QueryTypeMeasure, QueryRequest: execution.Request}, execution.Plan, handle, outcome, nil
	}
	// Continuous schema movement cannot pin cache lookup state or trigger retries forever.
	handle, outcome, err := b.planCache.uncached(ctx, statement)
	if err != nil {
		return nil, nil, nil, outcome, err
	}
	if !captured {
		now = ql.ExecutionNow()
	}
	started := time.Now()
	execution, err := handle.template.Bind(bound, now)
	b.planCache.observeDuration("instantiate", started)
	if err != nil {
		handle.Release()
		return nil, nil, nil, outcome, err
	}
	return &bydbql.TransformResult{Type: bydbql.QueryTypeMeasure, QueryRequest: execution.Request}, execution.Plan, handle, outcome, nil
}
