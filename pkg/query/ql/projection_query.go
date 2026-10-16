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

package ql

import (
	"context"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func wrapExpressionQuery(ctx context.Context, source vectorized.PullOperator, plan *RawPlan) (vectorized.PullOperator, error) {
	if plan.Aggregate != nil {
		if source != nil {
			_ = source.Close()
		}
		return nil, fmt.Errorf("projection expressions cannot be combined with aggregation")
	}
	ctx = vectorized.WithQueryMemoryTracker(ctx, 256<<20)
	input := *plan
	input.Expressions, input.Limit, input.Offset = nil, nil, 0
	filtered, err := WrapRaw(ctx, source, &input)
	if err != nil {
		return nil, err
	}
	projected, err := WrapExpressions(ctx, filtered, plan.Expressions)
	if err != nil {
		return nil, err
	}
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	bytes := int64(len(plan.Expressions.Outputs)) * 256
	if err = tracker.Reserve(bytes); err != nil {
		_ = projected.Close()
		return nil, err
	}
	defer tracker.Release(bytes)
	outputs := make([]Slot, len(plan.Expressions.Outputs))
	for index, output := range plan.Expressions.Outputs {
		outputs[index] = Slot{
			ID: SlotID(index), Source: output.Column, Output: output.Column, Nullable: true,
			Timestamp: output.Expr.Type == ExpressionTimestamp,
		}
	}
	return WrapRaw(ctx, projected, &RawPlan{Slots: outputs, Limit: plan.Limit, Offset: plan.Offset})
}
