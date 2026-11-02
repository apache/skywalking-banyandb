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
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
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
) (*measurev1.QueryResponse, error) {
	result, ok := data.(vmeasure.BatchQueryResult)
	if !ok {
		if failure, failed := data.(error); failed {
			return nil, queryStatus(failure)
		}
		return nil, fmt.Errorf("typed measure query requires a native batch result, got %T", data)
	}
	response, collectErr := collectPlannedMeasure(ctx, ql.NewNativeScan(result), statuses, plan)
	if collectErr != nil {
		return nil, collectErr
	}
	response.Trace = result.QueryTrace()
	return response, nil
}

func collectPlannedMeasure(ctx context.Context, scan vectorized.PullOperator, statuses map[string]modelv1.Status,
	plan *ql.RawPlan,
) (_ *measurev1.QueryResponse, err error) {
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
	return response, nil
}

func emptyPlannedMeasure(ctx context.Context, statuses map[string]modelv1.Status, plan *ql.RawPlan) (*measurev1.QueryResponse, error) {
	if plan == nil || plan.Aggregate == nil || plan.Limit != nil && *plan.Limit == 0 {
		return &measurev1.QueryResponse{GroupStatuses: statuses}, nil
	}
	tracker := vectorized.QueryMemoryTracker(ctx, 256<<20)
	bytes := int64(2048 + len(plan.Slots)*1024)
	if err := tracker.Reserve(bytes); err != nil {
		return nil, queryStatus(err)
	}
	columns := make([]vectorized.ColumnDef, len(plan.Slots))
	for index, slot := range plan.Slots {
		columns[index] = slot.Source
	}
	empty := *plan
	empty.Empty, empty.Residual, empty.Ranges = true, false, nil
	// The bound predicate has already been validated. No row reaches it on
	// empty input, so its hidden source columns need not exist in this schema.
	empty.Filter = nil
	source := &emptyMeasureScan{schema: vectorized.NewBatchSchema(columns), tracker: tracker, reserved: bytes}
	return collectPlannedMeasure(ctx, source, statuses, &empty)
}

type emptyMeasureScan struct {
	schema   *vectorized.BatchSchema
	tracker  *vectorized.MemoryTracker
	reserved int64
}

func (*emptyMeasureScan) Init(ctx context.Context) error          { return ctx.Err() }
func (s *emptyMeasureScan) OutputSchema() *vectorized.BatchSchema { return s.schema }
func (*emptyMeasureScan) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	return nil, ctx.Err()
}

func (s *emptyMeasureScan) Close() error {
	if s.reserved != 0 {
		s.tracker.Release(s.reserved)
		s.reserved = 0
	}
	s.schema = nil
	return nil
}
