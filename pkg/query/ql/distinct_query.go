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
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// DistinctStatistics returns visible-tuple grouping state independently of aggregation.
func DistinctStatistics(source vectorized.PullOperator) (AggregateStats, bool) {
	for current := source; current != nil; current = operatorSource(current) {
		if op, ok := current.(*aggregateOperator); ok && op.distinct {
			return op.statistics(), true
		}
	}
	return AggregateStats{}, false
}

func distinctSlotCount(plan *RawPlan) int {
	if plan.Having != nil {
		return len(plan.Having.Outputs)
	}
	if plan.Expressions != nil {
		return len(plan.Expressions.Outputs)
	}
	if plan.Aggregate != nil {
		return len(plan.Aggregate.Outputs)
	}
	return len(plan.Slots)
}

func distinctSlot(plan *RawPlan, i int) Slot {
	if plan.Having != nil {
		return plan.Having.Outputs[i]
	}
	if plan.Expressions != nil {
		output := plan.Expressions.Outputs[i]
		return Slot{Source: output.Column, Output: output.Column, Timestamp: output.Expr.Type == ExpressionTimestamp}
	}
	if plan.Aggregate != nil {
		output := plan.Aggregate.Outputs[i]
		return Slot{Source: output.Column, Output: output.Column, Timestamp: output.Timestamp}
	}
	return plan.Slots[i]
}

// distinctBucketOutput keeps bucket metadata only when an existing visible key
// owns that value. Hidden grouping keys never affect DISTINCT equality.
func distinctBucketOutput(source vectorized.PullOperator, plan *RawPlan) (width string, visible int) {
	for current := source; current != nil; current = operatorSource(current) {
		var outputs []AggregateOutput
		var bucket int
		switch aggregate := current.(type) {
		case *aggregateOperator:
			outputs, width, bucket = aggregate.outputs, aggregate.bucketWidth, aggregate.bucketIndex
		case *nativeAggregateOperator:
			outputs, width, bucket = aggregate.outputs, aggregate.bucketWidth, aggregate.bucketIndex
		default:
			continue
		}
		if width == "" || bucket < 0 {
			return "", 0
		}
		for index := 0; index < distinctSlotCount(plan); index++ {
			column := distinctSlot(plan, index).Source
			for _, output := range outputs {
				if output.IsGroup && output.Index == bucket && output.Column.Role == column.Role && output.Column.Name == column.Name &&
					output.Column.TagFamily == column.TagFamily {
					return width, index
				}
			}
		}
		return "", 0
	}
	return "", 0
}

// ValidateDistinct rejects unsupported visible key types before execution.
func ValidateDistinct(plan *RawPlan) error {
	if plan == nil {
		return fmt.Errorf("DISTINCT requires a plan")
	}
	for i := 0; i < distinctSlotCount(plan); i++ {
		slot := distinctSlot(plan, i)
		if !aggregateScalarType(slot.Output.Type) {
			return fmt.Errorf("DISTINCT output %q has unsupported key type %s; array keys are not supported", slot.Output.Name, slot.Output.Type)
		}
	}
	return nil
}

func wrapDistinctQuery(ctx context.Context, source vectorized.PullOperator, plan *RawPlan) (vectorized.PullOperator, error) {
	ctx = vectorized.WithQueryMemoryTracker(ctx, 256<<20)
	if err := ValidateDistinct(plan); err != nil {
		if source != nil {
			_ = source.Close()
		}
		return nil, err
	}
	input := *plan
	input.Distinct, input.Limit, input.Offset = false, nil, 0
	projected, err := WrapRaw(ctx, source, &input)
	if err != nil {
		return nil, err
	}
	width := distinctSlotCount(plan)
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	bytes := int64(width) * 512
	if err = tracker.Reserve(bytes); err != nil {
		_ = projected.Close()
		return nil, err
	}
	defer tracker.Release(bytes)
	bucketWidth, bucketIndex := distinctBucketOutput(projected, plan)
	grouping := &AggregatePlan{
		GroupBy: make([]vectorized.ColumnDef, width), Outputs: make([]AggregateOutput, width), TimeBucketWidth: bucketWidth, TimeBucketIndex: bucketIndex,
	}
	outputs := make([]Slot, width)
	for i := 0; i < width; i++ {
		slot := distinctSlot(plan, i)
		grouping.GroupBy[i] = slot.Output
		grouping.Outputs[i] = AggregateOutput{Column: slot.Output, Index: i, IsGroup: true}
		outputs[i] = Slot{Source: slot.Output, Output: slot.Output, Timestamp: slot.Timestamp || slot.Source.Role == vectorized.RoleTimestamp}
	}
	grouped, err := wrapAggregate(ctx, projected, grouping, true)
	if err != nil {
		return nil, err
	}
	distinct := grouped.(*aggregateOperator)
	if !plan.Ordered && plan.Limit != nil && *plan.Limit != 0 {
		// Widen before addition: OFFSET and LIMIT can sum beyond uint32.
		distinct.distinctLimit = uint64(plan.Offset) + uint64(*plan.Limit)
	}
	for i := range outputs {
		distinct.groupReaders[i].timestamp = outputs[i].Timestamp
	}
	return WrapRaw(ctx, grouped, &RawPlan{Slots: outputs, Limit: plan.Limit, Offset: plan.Offset})
}
