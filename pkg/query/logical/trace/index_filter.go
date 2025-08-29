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
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// buildFilter builds an index filter for trace queries.
// Unlike stream queries, trace doesn't need indexRuleType parameter.
// Trace conditions are either entity or skipping index.
// Trace creates explicit index rules for skipping index on all tags that don't belong to entity.
func buildFilter(criteria *modelv1.Criteria, tagNames map[string]bool,
	entityDict map[string]int, entity []*modelv1.TagValue,
) (index.Filter, [][]*modelv1.TagValue, error) {
	if criteria == nil {
		return nil, [][]*modelv1.TagValue{entity}, nil
	}
	switch criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		cond := criteria.GetCondition()
		// Check if the tag name exists in the allowed tag names
		if !tagNames[cond.Name] {
			return nil, nil, errors.Errorf("tag name '%s' not found in trace schema", cond.Name)
		}
		_, parsedEntity, err := logical.ParseExprOrEntity(entityDict, entity, cond)
		if err != nil {
			return nil, nil, err
		}
		if parsedEntity != nil {
			return nil, parsedEntity, nil
		}
		// For trace, all non-entity tags have skipping index
		expr, _, err := logical.ParseExprOrEntity(entityDict, entity, cond)
		if err != nil {
			return nil, nil, err
		}
		return parseConditionToFilter(cond, entity, expr)
	case *modelv1.Criteria_Le:
		le := criteria.GetLe()
		if le.GetLeft() == nil && le.GetRight() == nil {
			return nil, nil, errors.WithMessagef(logical.ErrInvalidLogicalExpression, "both sides(left and right) of [%v] are empty", criteria)
		}
		if le.GetLeft() == nil {
			return buildFilter(le.Right, tagNames, entityDict, entity)
		}
		if le.GetRight() == nil {
			return buildFilter(le.Left, tagNames, entityDict, entity)
		}
		left, leftEntities, err := buildFilter(le.Left, tagNames, entityDict, entity)
		if err != nil {
			return nil, nil, err
		}
		right, rightEntities, err := buildFilter(le.Right, tagNames, entityDict, entity)
		if err != nil {
			return nil, nil, err
		}
		entities := logical.ParseEntities(le.Op, entity, leftEntities, rightEntities)
		if entities == nil {
			return nil, nil, nil
		}
		if left == nil {
			return right, entities, nil
		}
		if right == nil {
			return left, entities, nil
		}
		switch le.Op {
		case modelv1.LogicalExpression_LOGICAL_OP_AND:
			return &traceAndFilter{left: left, right: right}, entities, nil
		case modelv1.LogicalExpression_LOGICAL_OP_OR:
			return &traceOrFilter{left: left, right: right}, entities, nil
		}
	}
	return nil, nil, logical.ErrInvalidCriteriaType
}

func parseConditionToFilter(cond *modelv1.Condition, entity []*modelv1.TagValue, expr logical.LiteralExpr) (index.Filter, [][]*modelv1.TagValue, error) {
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
		return &traceFilter{op: "having", tagName: cond.Name}, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_NOT_HAVING:
		return &traceFilter{op: "not_having", tagName: cond.Name}, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_IN:
		return &traceFilter{op: "in", tagName: cond.Name}, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_NOT_IN:
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
