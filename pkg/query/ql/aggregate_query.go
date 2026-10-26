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

package ql

import (
	"context"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func wrapAggregateQuery(ctx context.Context, source vectorized.PullOperator, plan *RawPlan) (vectorized.PullOperator, error) {
	ctx = vectorized.WithQueryMemoryTracker(ctx, 256<<20)
	input := *plan
	input.Aggregate = nil
	input.Limit = nil
	input.Offset = 0
	filtered, err := WrapRaw(ctx, source, &input)
	if err != nil {
		return nil, err
	}
	grouped, err := WrapAggregate(ctx, filtered, plan.Aggregate)
	if err != nil {
		return nil, err
	}
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	bytes := int64(len(plan.Aggregate.Outputs)) * 256
	if err = tracker.Reserve(bytes); err != nil {
		_ = grouped.Close()
		return nil, err
	}
	defer tracker.Release(bytes)
	outputs := make([]Slot, len(plan.Aggregate.Outputs))
	for index, output := range plan.Aggregate.Outputs {
		outputs[index] = Slot{ID: SlotID(index), Source: output.Column, Output: output.Column, Nullable: true}
	}
	// Pagination applies to finalized groups, never to the raw aggregate input.
	return WrapRaw(ctx, grouped, &RawPlan{Slots: outputs, Limit: plan.Limit, Offset: plan.Offset})
}
