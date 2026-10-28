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
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/data"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

func (ms *measureService) canUseNativeAggregate(request *measurev1.QueryRequest, plan *ql.RawPlan) bool {
	if plan == nil || plan.NativeAggregate == nil || nativeAggregateHooks().Disabled || ms.groupRepo == nil || len(request.Groups) != 1 {
		return false
	}
	// Group lifecycle options are watched independently of the measure schema
	// and therefore must be checked after binding a cached measure template.
	ms.groupRepo.RLock()
	defer ms.groupRepo.RUnlock()
	options := ms.groupRepo.resourceOpts[request.Groups[0]]
	if options == nil {
		return false
	}
	stages := request.Stages
	if len(stages) == 0 {
		stages = options.DefaultStages
	}
	if len(stages) == 0 {
		return len(options.Stages) == 0
	}
	// A custom global hot selector is outside the liaison's schema view.
	if len(stages) != 1 || strings.EqualFold(stages[0], "hot") {
		return false
	}
	matches := 0
	for _, stage := range options.Stages {
		if strings.EqualFold(stage.Name, stages[0]) && strings.TrimSpace(stage.NodeSelector) != "" {
			matches++
		}
	}
	return matches == 1
}

func (ms *measureService) executeNativeAggregate(ctx context.Context, request *measurev1.QueryRequest,
	statuses map[string]modelv1.Status, plan *ql.RawPlan,
) (response *measurev1.QueryResponse, err error) {
	native := *plan.NativeAggregate
	hooks := nativeAggregateHooks()
	native.MaxGroups = hooks.MaxGroups
	var tracer *query.Tracer
	var span *query.Span
	if request.Trace {
		tracer, ctx = query.NewTracer(ctx, "ql-native-aggregation")
		span, ctx = tracer.StartSpan(ctx, "native-aggregate-merge")
		defer func() {
			if err != nil {
				span.Error(err)
			}
			span.Stop()
			if response != nil {
				response.Trace = tracer.ToProto()
			}
		}()
	}
	branch := 0
	open := func(branchCtx context.Context, leaf ql.NativeAggregateLeaf) (vectorized.PullOperator, error) {
		if cancelErr := branchCtx.Err(); cancelErr != nil {
			return nil, cancelErr
		}
		// Bind owns each leaf request privately. Carry forward the current
		// creation-time clamp and revision/authorization provenance.
		leaf.Request.TimeRange = request.TimeRange
		leaf.Request.GroupModRevisions = request.GroupModRevisions
		if hooks.BeforeBranch != nil {
			if hookErr := hooks.BeforeBranch(branchCtx, branch, leaf.Request); hookErr != nil {
				return nil, hookErr
			}
		}
		if cancelErr := branchCtx.Err(); cancelErr != nil {
			return nil, cancelErr
		}
		if !ms.canUseNativeAggregate(request, plan) {
			return nil, status.Error(codes.FailedPrecondition, "native aggregate lifecycle selection changed before dispatch")
		}
		if gateErr := ms.checkNativeAggregateGate(leaf.Request); gateErr != nil {
			return nil, gateErr
		}
		branch++
		result, openErr := ms.openNativeAggregateBranch(branchCtx, leaf, native.Aggregate.GroupBy)
		if openErr != nil {
			return nil, openErr
		}
		return &nativeAggregateBranchResult{PullOperator: result, finish: func() {
			if span != nil {
				if traced, ok := result.(vmeasure.BatchQueryResult); ok {
					span.AddSubTrace(traced.QueryTrace())
				}
			}
		}}, nil
	}
	if len(native.Leaves) == 0 {
		return nil, fmt.Errorf("native aggregate plan has no leaves")
	}
	// The processor initializes the shared memory scope with its configured
	// limit. Open the first leaf before constructing local state so no default
	// liaison limit can replace that processor-owned configuration.
	first, err := open(ctx, native.Leaves[0])
	if err != nil {
		return nil, err
	}
	defer func() {
		if first != nil {
			_ = first.Close()
		}
	}()
	operator, err := ql.MergeNativeAggregate(ctx, &native, func(branchCtx context.Context, leaf ql.NativeAggregateLeaf) (vectorized.PullOperator, error) {
		if first != nil {
			result := first
			first = nil
			return result, nil
		}
		return open(branchCtx, leaf)
	})
	if err != nil {
		return nil, err
	}
	tracker := vectorized.QueryMemoryTracker(ctx, 256<<20)
	bytes := int64(len(native.Aggregate.Outputs)) * 256
	if err = tracker.Reserve(bytes); err != nil {
		_ = operator.Close()
		return nil, err
	}
	outputs := make([]ql.Slot, len(native.Aggregate.Outputs))
	for index, output := range native.Aggregate.Outputs {
		outputs[index] = ql.Slot{ID: ql.SlotID(index), Source: output.Column, Output: output.Column, Nullable: true}
	}
	response, collectErr := collectPlannedMeasure(ctx, operator, statuses, &ql.RawPlan{
		Slots: outputs, Having: plan.Having, Limit: plan.Limit, Offset: plan.Offset,
	})
	tracker.Release(bytes)
	if collectErr != nil {
		return nil, collectErr
	}
	return response, nil
}

func (ms *measureService) checkNativeAggregateGate(request *measurev1.QueryRequest) error {
	registry := schemaRevisionRegistry(ms.metadataRepo)
	statuses, shortCircuit := checkQueryGate(request.Groups, request.Name, request.GroupModRevisions,
		func(name, group string) (int64, bool) {
			locator, found := ms.entityRepo.getLocator(identity{name: name, group: group})
			return resolveQueryGateRevision(registry, schema.KindMeasure, group, name, locator.ModRevision, found)
		}, ms.maxWaitDuration)
	if shortCircuit {
		return status.Errorf(codes.FailedPrecondition, "native aggregate branch failed its revision gate: %v", statuses)
	}
	return nil
}

func (ms *measureService) openNativeAggregateBranch(ctx context.Context, leaf ql.NativeAggregateLeaf,
	keys []vectorized.ColumnDef,
) (vectorized.PullOperator, error) {
	future, err := ms.broadcaster.Publish(ctx, data.TopicMeasureQuery,
		bus.NewMessage(bus.MessageID(time.Now().UnixNano()), &vmeasure.BatchQueryRequest{Request: leaf.Request}))
	if err != nil {
		return nil, err
	}
	message, err := future.Get()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if err == nil {
		switch result := message.Data().(type) {
		case vmeasure.BatchQueryResult:
			return result, nil
		case error:
			return nil, result
		case *measurev1.QueryResponse:
			return nil, status.Errorf(codes.FailedPrecondition, "native aggregate branch returned an incomplete revision-gated result: %v", result.GetGroupStatuses())
		default:
			return nil, fmt.Errorf("native aggregate branch requires batches, got %T", result)
		}
	}
	tracker := vectorized.QueryMemoryTracker(ctx, 256<<20)
	reserved := int64(2048 + (len(keys)+1)*1024)
	if err = tracker.Reserve(reserved); err != nil {
		return nil, err
	}
	columns := make([]vectorized.ColumnDef, len(keys), len(keys)+1)
	copy(columns, keys)
	for index := range columns {
		columns[index].Entity = false
	}
	value := leaf.Source
	value.Role, value.TagFamily, value.Entity = vectorized.RoleField, "", false
	if leaf.Function == ql.AggregateCount && value.Type != vectorized.ColumnTypeFloat64 {
		value.Type = vectorized.ColumnTypeInt64
	}
	columns = append(columns, value)
	return &emptyMeasureScan{schema: vectorized.NewBatchSchema(columns), tracker: tracker, reserved: reserved}, nil
}

type nativeAggregateBranchResult struct {
	vectorized.PullOperator
	finish func()
	closed bool
}

func (r *nativeAggregateBranchResult) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	err := r.PullOperator.Close()
	r.finish()
	return err
}
