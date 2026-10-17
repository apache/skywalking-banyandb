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

package bydbql

import (
	"math"

	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func (m *MeasureTemplate) compileNativeEntities(schemas map[string]*databasev1.Measure, columns *measureColumns) {
	if len(m.request.Groups) != 1 {
		return
	}
	schema := schemas[m.request.Groups[0]]
	if schema == nil || schema.GetIndexMode() || len(schema.GetEntity().GetTagNames()) == 0 {
		return
	}

	var entities []vectorized.ColumnDef
	for _, source := range columns.ordered {
		if source.Entity {
			// Native entity predicates have unqualified names. Ambiguous names
			// cannot establish a unique non-null entity assignment.
			if !columns.unambiguousTags[columnIdentity(source)] {
				return
			}
			entities = append(entities, source)
		}
	}
	if len(entities) == len(schema.GetEntity().GetTagNames()) {
		m.nativeEntities = entities
	}
}

func (m *MeasureTemplate) bindNativeAggregate(request *measurev1.QueryRequest, plan *ql.RawPlan) *ql.NativeAggregatePlan {
	if plan.Aggregate == nil || plan.Empty || plan.Residual || plan.Filter != nil || len(plan.Ranges) != 1 || len(m.nativeEntities) == 0 {
		return nil
	}
	// Normal writes are millisecond-aligned and native scans deduplicate
	// (complete entity, timestamp). Each group below identifies at most one
	// entity, bounding even native float COUNT by timestamp positions.
	interval := plan.Ranges[0]
	if interval.End <= interval.Begin {
		return nil
	}
	countBound := uint64(interval.End) - uint64(interval.Begin)
	if countBound > 1<<53 {
		return nil
	}
	grouped := make(map[measureColumnKey]bool)
	var family string
	for index, key := range plan.Aggregate.GroupBy {
		if key.Role != vectorized.RoleTag || !key.Entity || (index > 0 && family != key.TagFamily) {
			return nil
		}
		family = key.TagFamily
		proved, _ := exactEntityConstraint(request.Criteria, key.Name)
		if !proved {
			return nil
		}
		grouped[columnIdentity(key)] = true
	}
	for _, entity := range m.nativeEntities {
		if !grouped[columnIdentity(entity)] {
			proved, single := exactEntityConstraint(request.Criteria, entity.Name)
			if !proved || !single {
				return nil
			}
		}
	}
	native := &ql.NativeAggregatePlan{Aggregate: plan.Aggregate, MaxCount: int64(countBound)}
	seen := make(map[ql.AggregateCall]bool)
	add := func(source vectorized.ColumnDef, function ql.AggregateFunction) {
		call := ql.AggregateCall{Source: source, Function: function}
		if seen[call] {
			return
		}
		seen[call] = true
		native.Leaves = append(native.Leaves, ql.NativeAggregateLeaf{
			Source: source, Function: function, Request: nativeAggregateRequest(request, plan.Aggregate.GroupBy, call),
		})
	}
	for _, call := range plan.Aggregate.Calls {
		if call.Source.Role != vectorized.RoleField {
			return nil
		}
		switch call.Function {
		case ql.AggregateCount:
			add(call.Source, ql.AggregateCount)
		case ql.AggregateSum, ql.AggregateAvg:
			// Native integer SUM wraps; SQL SUM checks overflow and AVG uses
			// a float accumulator. A one-position group cannot overflow.
			if call.Source.Type == vectorized.ColumnTypeInt64 && countBound > 1 {
				return nil
			}
			add(call.Source, ql.AggregateSum)
			add(call.Source, ql.AggregateCount)
		case ql.AggregateMin, ql.AggregateMax:
			// Native floating extrema use finite sentinels and ignore NaNs.
			if call.Source.Type != vectorized.ColumnTypeInt64 {
				return nil
			}
			add(call.Source, call.Function)
			add(call.Source, ql.AggregateCount)
		default:
			return nil
		}
	}
	if len(native.Leaves) > 32 {
		return nil
	}
	return native
}

// exactEntityConstraint proves a non-null entity value set, and optionally a
// single assignment. Native entity encodings distinguish null from zero/empty.
// AND can only narrow a proven set; OR is left to the complete local fallback.
func exactEntityConstraint(criteria *modelv1.Criteria, name string) (proved, single bool) {
	if criteria == nil {
		return false, false
	}
	if logical := criteria.GetLe(); logical != nil {
		if logical.Op != modelv1.LogicalExpression_LOGICAL_OP_AND {
			return false, false
		}
		left, leftSingle := exactEntityConstraint(logical.Left, name)
		right, rightSingle := exactEntityConstraint(logical.Right, name)
		return left || right, left && leftSingle || right && rightSingle
	}
	condition := criteria.GetCondition()
	if condition == nil || condition.Name != name || condition.Value == nil {
		return false, false
	}
	switch condition.Op {
	case modelv1.Condition_BINARY_OP_EQ:
		switch value := condition.Value.Value.(type) {
		case *modelv1.TagValue_Str:
			return value.Str != nil, value.Str != nil
		case *modelv1.TagValue_Int:
			return value.Int != nil, value.Int != nil
		default:
			return false, false
		}
	case modelv1.Condition_BINARY_OP_IN:
		switch value := condition.Value.Value.(type) {
		case *modelv1.TagValue_StrArray:
			if value.StrArray != nil && len(value.StrArray.Value) > 0 {
				return true, len(value.StrArray.Value) == 1
			}
		case *modelv1.TagValue_IntArray:
			if value.IntArray != nil && len(value.IntArray.Value) > 0 {
				return true, len(value.IntArray.Value) == 1
			}
		default:
		}
	default:
	}
	return false, false
}

func nativeAggregateRequest(request *measurev1.QueryRequest, keys []vectorized.ColumnDef, call ql.AggregateCall) *measurev1.QueryRequest {
	leaf := proto.Clone(request).(*measurev1.QueryRequest)
	leaf.Offset, leaf.Limit = 0, math.MaxUint32
	leaf.OrderBy, leaf.Top = nil, nil
	var slots []ql.Slot
	for _, key := range keys {
		slots = append(slots, ql.Slot{Source: key})
	}
	if len(slots) > 0 {
		projection, _ := rawScanProjection(slots)
		leaf.GroupBy = &measurev1.QueryRequest_GroupBy{TagProjection: projection}
	}
	slots = append(slots, ql.Slot{Source: call.Source})
	leaf.TagProjection, leaf.FieldProjection = rawScanProjection(slots)
	function := modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT
	switch call.Function {
	case ql.AggregateSum:
		function = modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM
	case ql.AggregateMin:
		function = modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN
	case ql.AggregateMax:
		function = modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX
	default:
	}
	leaf.Agg = &measurev1.QueryRequest_Aggregation{Function: function}
	if call.Source.Role == vectorized.RoleField {
		leaf.Agg.FieldName = call.Source.Name
	} else {
		leaf.Agg.TagFamily, leaf.Agg.TagName = call.Source.TagFamily, call.Source.Name
	}
	return leaf
}
