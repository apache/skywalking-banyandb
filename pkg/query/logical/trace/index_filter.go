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

package trace

import (
	"math"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// buildFilter builds an index filter for trace queries.
// Unlike stream queries, trace doesn't need indexRuleType parameter.
// Trace conditions are either entity or skipping index.
// Trace creates explicit index rules for skipping index on all tags that don't belong to entity.
// Returns min/max int64 values for the orderByTag if provided, otherwise returns math.MaxInt64, math.MinInt64.
func buildFilter(criteria *modelv1.Criteria, schema logical.Schema, tagNames map[string]bool,
	entityDict map[string]int, entity []*modelv1.TagValue, traceIDTagName, spanIDTagName string, orderByTag string,
) (index.Filter, [][]*modelv1.TagValue, []string, []string, int64, int64, error) {
	if criteria == nil {
		return nil, [][]*modelv1.TagValue{entity}, nil, nil, math.MinInt64, math.MaxInt64, nil
	}

	switch criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		return buildFilterFromCondition(criteria.GetCondition(), schema, tagNames, entityDict, entity, traceIDTagName, spanIDTagName, orderByTag)
	case *modelv1.Criteria_Le:
		return buildFilterFromLogicalExpression(criteria.GetLe(), schema, tagNames, entityDict, entity, traceIDTagName, spanIDTagName, orderByTag)
	}

	return nil, nil, nil, nil, math.MinInt64, math.MaxInt64, logical.ErrInvalidCriteriaType
}

func parseConditionToFilter(cond *modelv1.Condition, schema logical.Schema,
	entity []*modelv1.TagValue, expr logical.LiteralExpr,
) (index.Filter, [][]*modelv1.TagValue, error) {
	switch cond.Op {
	case modelv1.Condition_BINARY_OP_GT:
		return &traceRangeFilter{op: "gt", tagName: cond.Name, cond: cond, expr: expr}, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_GE:
		return &traceRangeFilter{op: "ge", tagName: cond.Name, cond: cond, expr: expr}, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_LT:
		return &traceRangeFilter{op: "lt", tagName: cond.Name, cond: cond, expr: expr}, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_LE:
		return &traceRangeFilter{op: "le", tagName: cond.Name, cond: cond, expr: expr}, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_EQ:
		return &traceEqFilter{op: "eq", tagName: cond.Name, cond: cond, expr: expr}, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_NE:
		return &traceFilter{op: "ne", tagName: cond.Name}, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_MATCH:
		return &traceMatchFilter{op: "match", tagName: cond.Name}, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_HAVING:
		return &traceHavingFilter{op: "having", tagName: cond.Name, expr: expr}, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_NOT_HAVING:
		return &traceFilter{op: "not_having", tagName: cond.Name}, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_IN:
		if schema != nil {
			tagSpec := schema.FindTagSpecByName(cond.Name)
			if tagSpec != nil && (tagSpec.Spec.GetType() == databasev1.TagType_TAG_TYPE_STRING_ARRAY || tagSpec.Spec.GetType() == databasev1.TagType_TAG_TYPE_INT_ARRAY) {
				return nil, nil, errors.Errorf("in condition is not supported for array type")
			}
		}
		return &traceFilter{op: "in", tagName: cond.Name}, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_NOT_IN:
		if schema != nil {
			tagSpec := schema.FindTagSpecByName(cond.Name)
			if tagSpec != nil && (tagSpec.Spec.GetType() == databasev1.TagType_TAG_TYPE_STRING_ARRAY || tagSpec.Spec.GetType() == databasev1.TagType_TAG_TYPE_INT_ARRAY) {
				return nil, nil, errors.Errorf("not in condition is not supported for array type")
			}
		}
		return &traceFilter{op: "not_in", tagName: cond.Name}, [][]*modelv1.TagValue{entity}, nil
	}
	return nil, nil, errors.Errorf("unsupported condition operation: %v", cond.Op)
}

// traceFilter implements index.Filter for trace queries.
type traceFilter struct {
	op      string
	tagName string
}

func (tf *traceFilter) Execute(_ index.GetSearcher, _ common.SeriesID, _ *index.RangeOpts) (posting.List, posting.List, error) {
	panic("traceFilter.Execute should not be invoked")
}

func (tf *traceFilter) ShouldSkip(_ index.FilterOp) (bool, error) {
	// For trace queries, we don't skip based on index filters
	// The actual filtering is handled by tag filters
	return false, nil
}

func (tf *traceFilter) String() string {
	return tf.op + ":" + tf.tagName
}

// traceAndFilter implements index.Filter for AND operations in trace queries.
type traceAndFilter struct {
	left  index.Filter
	right index.Filter
}

func (taf *traceAndFilter) Execute(_ index.GetSearcher, _ common.SeriesID, _ *index.RangeOpts) (posting.List, posting.List, error) {
	panic("traceAndFilter.Execute should not be invoked")
}

func (taf *traceAndFilter) ShouldSkip(op index.FilterOp) (bool, error) {
	// For AND operations, skip only if both sides should be skipped
	leftSkip, err := taf.left.ShouldSkip(op)
	if err != nil {
		return false, err
	}
	rightSkip, err := taf.right.ShouldSkip(op)
	if err != nil {
		return false, err
	}
	return leftSkip && rightSkip, nil
}

func (taf *traceAndFilter) String() string {
	return "and(" + taf.left.String() + "," + taf.right.String() + ")"
}

// traceOrFilter implements index.Filter for OR operations in trace queries.
type traceOrFilter struct {
	left  index.Filter
	right index.Filter
}

func (tof *traceOrFilter) Execute(_ index.GetSearcher, _ common.SeriesID, _ *index.RangeOpts) (posting.List, posting.List, error) {
	panic("traceOrFilter.Execute should not be invoked")
}

func (tof *traceOrFilter) ShouldSkip(op index.FilterOp) (bool, error) {
	// For OR operations, skip only if both sides should be skipped
	leftSkip, err := tof.left.ShouldSkip(op)
	if err != nil {
		return false, err
	}
	rightSkip, err := tof.right.ShouldSkip(op)
	if err != nil {
		return false, err
	}
	return leftSkip && rightSkip, nil
}

func (tof *traceOrFilter) String() string {
	return "or(" + tof.left.String() + "," + tof.right.String() + ")"
}

// traceEqFilter implements index.Filter for EQ operations in trace queries.
type traceEqFilter struct {
	expr    logical.LiteralExpr
	cond    *modelv1.Condition
	op      string
	tagName string
}

func (tef *traceEqFilter) Execute(_ index.GetSearcher, _ common.SeriesID, _ *index.RangeOpts) (posting.List, posting.List, error) {
	panic("traceEqFilter.Execute should not be invoked")
}

func (tef *traceEqFilter) ShouldSkip(tagFilters index.FilterOp) (bool, error) {
	// Use the parsed expression to get the tag value and invoke tagFilters.Eq
	if tef.expr != nil {
		tagValue := tef.expr.String()
		return !tagFilters.Eq(tef.tagName, tagValue), nil
	}
	return false, nil
}

func (tef *traceEqFilter) String() string {
	return tef.op + ":" + tef.tagName
}

// traceRangeFilter implements index.Filter for range operations in trace queries.
type traceRangeFilter struct {
	expr    logical.LiteralExpr
	cond    *modelv1.Condition
	op      string
	tagName string
}

func (trf *traceRangeFilter) Execute(_ index.GetSearcher, _ common.SeriesID, _ *index.RangeOpts) (posting.List, posting.List, error) {
	panic("traceRangeFilter.Execute should not be invoked")
}

func (trf *traceRangeFilter) ShouldSkip(tagFilters index.FilterOp) (bool, error) {
	// Use the parsed expression to build RangeOpts and invoke tagFilters.Range
	if trf.expr != nil {
		var opts index.RangeOpts
		switch trf.cond.Op {
		case modelv1.Condition_BINARY_OP_GT:
			opts = trf.expr.RangeOpts(false, false, false)
		case modelv1.Condition_BINARY_OP_GE:
			opts = trf.expr.RangeOpts(false, true, false)
		case modelv1.Condition_BINARY_OP_LT:
			opts = trf.expr.RangeOpts(true, false, false)
		case modelv1.Condition_BINARY_OP_LE:
			opts = trf.expr.RangeOpts(true, false, true)
		default:
			// For non-range operations, return false (don't skip)
			return false, nil
		}
		return tagFilters.Range(trf.tagName, opts)
	}
	return false, nil
}

func (trf *traceRangeFilter) String() string {
	return trf.op + ":" + trf.tagName
}

// traceMatchFilter implements index.Filter for MATCH operations in trace queries.
type traceMatchFilter struct {
	op      string
	tagName string
}

func (tmf *traceMatchFilter) Execute(_ index.GetSearcher, _ common.SeriesID, _ *index.RangeOpts) (posting.List, posting.List, error) {
	panic("traceMatchFilter.Execute should not be invoked")
}

func (tmf *traceMatchFilter) ShouldSkip(_ index.FilterOp) (bool, error) {
	// Similar to stream match filter - don't skip for match operations
	return false, nil
}

func (tmf *traceMatchFilter) String() string {
	return tmf.op + ":" + tmf.tagName
}

// traceHavingFilter implements index.Filter for HAVING operations in trace queries.
type traceHavingFilter struct {
	expr    logical.LiteralExpr
	op      string
	tagName string
}

func (thf *traceHavingFilter) Execute(_ index.GetSearcher, _ common.SeriesID, _ *index.RangeOpts) (posting.List, posting.List, error) {
	panic("traceHavingFilter.Execute should not be invoked")
}

func (thf *traceHavingFilter) ShouldSkip(tagFilters index.FilterOp) (bool, error) {
	// Use the parsed expression to get the tag values and invoke tagFilters.Having
	if thf.expr != nil {
		subExprs := thf.expr.SubExprs()
		tagValues := make([]string, len(subExprs))
		for i, subExpr := range subExprs {
			tagValues[i] = subExpr.String()
		}
		return !tagFilters.Having(thf.tagName, tagValues), nil
	}
	return false, nil
}

func (thf *traceHavingFilter) String() string {
	return thf.op + ":" + thf.tagName
}

// extractIDsFromCondition extracts IDs from equal and in conditions.
func extractIDsFromCondition(cond *modelv1.Condition) []string {
	var ids []string

	switch cond.Op {
	case modelv1.Condition_BINARY_OP_EQ:
		if cond.Value != nil && cond.Value.Value != nil {
			switch val := cond.Value.Value.(type) {
			case *modelv1.TagValue_Str:
				if val.Str != nil {
					ids = append(ids, val.Str.Value)
				}
			case *modelv1.TagValue_StrArray:
				if val.StrArray != nil {
					ids = append(ids, val.StrArray.Value...)
				}
			}
		}
	case modelv1.Condition_BINARY_OP_IN:
		if cond.Value != nil && cond.Value.Value != nil {
			switch val := cond.Value.Value.(type) {
			case *modelv1.TagValue_StrArray:
				if val.StrArray != nil {
					ids = append(ids, val.StrArray.Value...)
				}
			case *modelv1.TagValue_Str:
				if val.Str != nil {
					ids = append(ids, val.Str.Value)
				}
			}
		}
	case modelv1.Condition_BINARY_OP_NE, modelv1.Condition_BINARY_OP_LT, modelv1.Condition_BINARY_OP_GT,
		modelv1.Condition_BINARY_OP_LE, modelv1.Condition_BINARY_OP_GE, modelv1.Condition_BINARY_OP_HAVING,
		modelv1.Condition_BINARY_OP_NOT_HAVING, modelv1.Condition_BINARY_OP_NOT_IN, modelv1.Condition_BINARY_OP_MATCH:
		// These operations don't support ID extraction
	}

	return ids
}

// buildFilterFromCondition handles single condition filtering and min/max extraction.
func buildFilterFromCondition(cond *modelv1.Condition, schema logical.Schema, tagNames map[string]bool, entityDict map[string]int,
	entity []*modelv1.TagValue, traceIDTagName, spanIDTagName, orderByTag string,
) (index.Filter, [][]*modelv1.TagValue, []string, []string, int64, int64, error) {
	var collectedTagNames []string
	var traceIDs []string
	minVal := int64(math.MinInt64)
	maxVal := int64(math.MaxInt64)

	if !tagNames[cond.Name] {
		return nil, nil, collectedTagNames, traceIDs, minVal, maxVal, errors.Errorf("tag name '%s' not found in trace schema", cond.Name)
	}

	// Extract min/max bounds if this condition matches orderByTag
	if cond.Name == orderByTag {
		condMin, condMax := extractBoundsFromCondition(cond)
		if condMin != math.MaxInt64 {
			minVal = condMin
		}
		if condMax != math.MinInt64 {
			maxVal = condMax
		}
	}

	if cond.Name == traceIDTagName && (cond.Op == modelv1.Condition_BINARY_OP_EQ || cond.Op == modelv1.Condition_BINARY_OP_IN) {
		traceIDs = extractIDsFromCondition(cond)
	} else if cond.Name != spanIDTagName {
		collectedTagNames = append(collectedTagNames, cond.Name)
	}

	_, parsedEntity, err := logical.ParseExprOrEntity(entityDict, entity, cond)
	if err != nil {
		return nil, nil, collectedTagNames, traceIDs, minVal, maxVal, err
	}
	if parsedEntity != nil {
		return nil, parsedEntity, collectedTagNames, traceIDs, minVal, maxVal, nil
	}
	// For trace, all non-entity tags have skipping index
	expr, _, err := logical.ParseExprOrEntity(entityDict, entity, cond)
	if err != nil {
		return nil, nil, collectedTagNames, traceIDs, minVal, maxVal, err
	}
	filter, entities, err := parseConditionToFilter(cond, schema, entity, expr)
	return filter, entities, collectedTagNames, traceIDs, minVal, maxVal, err
}

// buildFilterFromLogicalExpression handles logical expression (AND/OR) filtering and min/max extraction.
func buildFilterFromLogicalExpression(le *modelv1.LogicalExpression, schema logical.Schema, tagNames map[string]bool, entityDict map[string]int,
	entity []*modelv1.TagValue, traceIDTagName, spanIDTagName, orderByTag string,
) (index.Filter, [][]*modelv1.TagValue, []string, []string, int64, int64, error) {
	var collectedTagNames []string
	var traceIDs []string
	minVal := int64(math.MaxInt64)
	maxVal := int64(math.MinInt64)

	if le.GetLeft() == nil && le.GetRight() == nil {
		return nil, nil, nil, traceIDs, minVal, maxVal, errors.WithMessagef(logical.ErrInvalidLogicalExpression, "both sides(left and right) of [%v] are empty", le)
	}
	if le.GetLeft() == nil {
		return buildFilter(le.Right, schema, tagNames, entityDict, entity, traceIDTagName, spanIDTagName, orderByTag)
	}
	if le.GetRight() == nil {
		return buildFilter(le.Left, schema, tagNames, entityDict, entity, traceIDTagName, spanIDTagName, orderByTag)
	}

	left, leftEntities, leftTagNames, leftTraceIDs, leftMin, leftMax, err := buildFilter(le.Left, schema, tagNames, entityDict, entity,
		traceIDTagName, spanIDTagName, orderByTag)
	if err != nil {
		return nil, nil, leftTagNames, leftTraceIDs, minVal, maxVal, err
	}
	right, rightEntities, rightTagNames, rightTraceIDs, rightMin, rightMax, err := buildFilter(le.Right, schema, tagNames, entityDict, entity,
		traceIDTagName, spanIDTagName, orderByTag)
	if err != nil {
		return nil, nil, append(leftTagNames, rightTagNames...), append(leftTraceIDs, rightTraceIDs...), minVal, maxVal, err
	}

	// Merge tag names from both sides
	collectedTagNames = append(collectedTagNames, leftTagNames...)
	collectedTagNames = append(collectedTagNames, rightTagNames...)

	// Merge trace IDs from both sides
	traceIDs = append(traceIDs, leftTraceIDs...)
	traceIDs = append(traceIDs, rightTraceIDs...)

	// Merge min/max values based on logical operation
	finalMin, finalMax := mergeMinMaxBounds(le.Op, leftMin, leftMax, rightMin, rightMax)

	entities := logical.ParseEntities(le.Op, entity, leftEntities, rightEntities)
	if entities == nil {
		return nil, nil, collectedTagNames, traceIDs, finalMin, finalMax, nil
	}
	if left == nil {
		return right, entities, collectedTagNames, traceIDs, finalMin, finalMax, nil
	}
	if right == nil {
		return left, entities, collectedTagNames, traceIDs, finalMin, finalMax, nil
	}
	switch le.Op {
	case modelv1.LogicalExpression_LOGICAL_OP_AND:
		return &traceAndFilter{left: left, right: right}, entities, collectedTagNames, traceIDs, finalMin, finalMax, nil
	case modelv1.LogicalExpression_LOGICAL_OP_OR:
		return &traceOrFilter{left: left, right: right}, entities, collectedTagNames, traceIDs, finalMin, finalMax, nil
	}
	return nil, nil, collectedTagNames, traceIDs, finalMin, finalMax, logical.ErrInvalidCriteriaType
}

// mergeMinMaxBounds merges min/max bounds based on logical operation.
func mergeMinMaxBounds(op modelv1.LogicalExpression_LogicalOp, leftMin, leftMax, rightMin, rightMax int64) (int64, int64) {
	switch op {
	case modelv1.LogicalExpression_LOGICAL_OP_AND:
		// Intersection - take tighter bounds
		finalMin := leftMin
		if rightMin > finalMin {
			finalMin = rightMin
		}
		finalMax := leftMax
		if rightMax < finalMax {
			finalMax = rightMax
		}
		return finalMin, finalMax
	case modelv1.LogicalExpression_LOGICAL_OP_OR:
		// Union - take wider bounds
		finalMin := leftMin
		if rightMin < finalMin {
			finalMin = rightMin
		}
		finalMax := leftMax
		if rightMax > finalMax {
			finalMax = rightMax
		}
		return finalMin, finalMax
	}
	return leftMin, leftMax
}

// extractBoundsFromCondition extracts bounds from a single condition.
func extractBoundsFromCondition(cond *modelv1.Condition) (int64, int64) {
	if cond.Value == nil || cond.Value.Value == nil {
		return math.MaxInt64, math.MinInt64
	}

	var value int64

	switch v := cond.Value.Value.(type) {
	case *modelv1.TagValue_Int:
		if v.Int != nil {
			value = v.Int.Value
		} else {
			return math.MaxInt64, math.MinInt64
		}
	case *modelv1.TagValue_Timestamp:
		if v.Timestamp != nil {
			value = v.Timestamp.AsTime().UnixNano()
		} else {
			return math.MaxInt64, math.MinInt64
		}
	default:
		// Only support int64 and timestamp for range operations
		return math.MaxInt64, math.MinInt64
	}

	switch cond.Op {
	case modelv1.Condition_BINARY_OP_GT:
		// value > X means min is X+1, max is unbounded
		if value < math.MaxInt64 {
			return value + 1, math.MinInt64
		}
		return math.MaxInt64, math.MinInt64
	case modelv1.Condition_BINARY_OP_GE:
		// value >= X means min is X, max is unbounded
		return value, math.MinInt64
	case modelv1.Condition_BINARY_OP_LT:
		// value < X means min is unbounded, max is X-1
		if value > math.MinInt64 {
			return math.MaxInt64, value - 1
		}
		return math.MaxInt64, math.MinInt64
	case modelv1.Condition_BINARY_OP_LE:
		// value <= X means min is unbounded, max is X
		return math.MaxInt64, value
	default:
		// Non-range operations don't contribute bounds
		return math.MaxInt64, math.MinInt64
	}
}
