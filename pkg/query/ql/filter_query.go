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

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func wrapFilterQuery(ctx context.Context, source vectorized.PullOperator, plan *RawPlan) (vectorized.PullOperator, error) {
	if plan.NativeAggregate != nil {
		if source != nil {
			_ = source.Close()
		}
		return nil, expressionError("a residual filter requires raw input and local aggregation")
	}
	ctx = vectorized.WithQueryMemoryTracker(ctx, 256<<20)
	if plan.Residual || plan.Empty {
		timed, err := wrapFilterTimeInput(ctx, source, plan)
		if err != nil {
			return nil, err
		}
		source = timed
	}
	filtered, err := WrapFilter(ctx, source, plan.Filter)
	if err != nil {
		return nil, err
	}
	remaining := *plan
	remaining.Filter = nil
	remaining.Ranges = nil
	remaining.Residual = false
	remaining.Empty = false
	return WrapRaw(ctx, filtered, &remaining)
}

func wrapFilterTimeInput(ctx context.Context, source vectorized.PullOperator, plan *RawPlan) (vectorized.PullOperator, error) {
	if source == nil || source.OutputSchema() == nil {
		if source != nil {
			_ = source.Close()
		}
		return nil, expressionError("filter requires a source schema")
	}
	schema := source.OutputSchema()
	bytes, err := rawDescriptorBound(len(schema.Columns), 0, 0)
	if err != nil {
		_ = source.Close()
		return nil, err
	}
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	if err = tracker.Reserve(bytes); err != nil {
		_ = source.Close()
		return nil, err
	}
	defer tracker.Release(bytes)
	// The time stage retains hidden predicate inputs. Only the final plan's
	// projection determines visible results, after the predicate has run.
	slots := make([]Slot, 0, len(schema.Columns))
	for _, column := range schema.Columns {
		if column.Role == vectorized.RoleTag || column.Role == vectorized.RoleField {
			slots = append(slots, Slot{Source: column, Output: column})
		}
	}
	return WrapRaw(ctx, source, &RawPlan{Slots: slots, Ranges: plan.Ranges, Residual: plan.Residual, Empty: plan.Empty})
}
