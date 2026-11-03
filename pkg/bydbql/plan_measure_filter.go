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

package bydbql

import (
	"fmt"
	"time"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func (m *MeasureTemplate) compileWhere(columns *measureColumns) error {
	where := m.statement.template.Select.Where
	expression := where.Expression
	var err error
	if expression == nil {
		if !legacyWhereNeedsFilter(where.Expr, columns.fieldOnly) {
			m.predicate, err = compileMeasureOr(where.Expr, columns)
			return err
		}
		expression, err = legacyFilterOr(where.Expr)
		if err != nil {
			return err
		}
	}
	if err = validateExpressionDepth(expression); err != nil {
		return err
	}
	expression, err = normalizeFilterTime(expression)
	if err != nil {
		return err
	}
	if hasMeasurePattern(expression) {
		expression, err = m.extractPatterns(expression, columns)
		if err != nil || expression == nil {
			return err
		}
	}
	inputs := newMeasureExpressionInputs(m.slots)
	recipe, err := compileMeasureExpression(expression, columns, inputs)
	if err != nil {
		return err
	}
	m.filter, m.filterTags = recipe, columns.unambiguousTags
	tags, fields := rawScanProjection(inputs.slots)
	if tags != nil || fields != nil {
		m.request.TagProjection, m.request.FieldProjection = tags, fields
	}

	return nil
}

func legacyFilterOr(expression *GrammarOrExpr) (*GrammarExpression, error) {
	left, err := legacyFilterAnd(expression.Left)
	if err != nil {
		return nil, err
	}
	for _, right := range expression.Right {
		next, nextErr := legacyFilterAnd(right.Right)
		if nextErr != nil {
			return nil, nextErr
		}
		left = &GrammarExpression{Op: "or", Args: []*GrammarExpression{left, next}}
	}
	return left, nil
}

func legacyFilterAnd(expression *GrammarAndExpr) (*GrammarExpression, error) {
	left, err := legacyFilterPredicate(expression.Left)
	if err != nil {
		return nil, err
	}
	for _, right := range expression.Right {
		next, nextErr := legacyFilterPredicate(right.Right)
		if nextErr != nil {
			return nil, nextErr
		}
		left = &GrammarExpression{Op: "and", Args: []*GrammarExpression{left, next}}
	}
	return left, nil
}

func legacyFilterPredicate(predicate *GrammarPredicate) (*GrammarExpression, error) {
	if predicate.Paren != nil {
		return legacyFilterOr(predicate.Paren)
	}
	if predicate.Binary != nil {
		comparison := predicate.Binary.Tail.Compare
		if comparison == nil {
			return nil, fmt.Errorf("MATCH mixed with residual WHERE expressions is not supported")
		}
		if isBuiltinTime(predicate.Binary.Identifier) {
			return &GrammarExpression{Op: "time_compare", Temporal: comparison}, nil
		}
		if comparison.Value.Now != nil || comparison.Adjustment != nil {
			return nil, fmt.Errorf("now() and INTERVAL require a time comparison")
		}
		return &GrammarExpression{Op: comparison.Operator, Args: []*GrammarExpression{
			{Op: "column", Identifier: predicate.Binary.Identifier}, grammarValueExpression(comparison.Value),
		}}, nil
	}
	if predicate.In != nil {
		membership := predicate.In
		op := "in"
		if membership.Not != nil {
			op = "not_in"
		}
		expression := &GrammarExpression{Op: op, Args: []*GrammarExpression{{Op: "column", Identifier: membership.Identifier}}}
		for _, value := range membership.Values {
			expression.Args = append(expression.Args, grammarValueExpression(value))
		}
		return expression, nil
	}
	if predicate.Having != nil {
		membership := predicate.Having
		op := "contains"
		if membership.Not != nil {
			op = "not_contains"
		}
		tagType := columnTypeTag
		expression := &GrammarExpression{
			Op: op, Args: []*GrammarExpression{{Op: "column", Identifier: membership.Identifier, TypeSpec: &tagType}},
			MembershipList: membership.Values.Single == nil,
		}
		values := membership.Values.Array
		if membership.Values.Single != nil {
			values = []*GrammarValue{membership.Values.Single}
		}
		for _, value := range values {
			expression.Args = append(expression.Args, grammarValueExpression(value))
		}
		return expression, nil
	}
	return nil, fmt.Errorf("invalid residual WHERE predicate")
}

func grammarValueExpression(value *GrammarValue) *GrammarExpression {
	expression := &GrammarExpression{Op: "literal", Param: value.Param, ParamIndex: value.ParamIndex}
	switch {
	case value.String != nil:
		expression.Type, expression.Literal.String = ql.ExpressionString, *value.String
	case value.Integer != nil:
		expression.Type, expression.Literal.Int64 = ql.ExpressionInt, *value.Integer
	case value.Null:
		expression.Type, expression.Literal.Null = ql.ExpressionNull, true
	}
	return expression
}

func normalizeFilterTime(expression *GrammarExpression) (*GrammarExpression, error) {
	if expression.Op == "clock" || expression.Op == "time_adjust" {
		return nil, fmt.Errorf("now() and INTERVAL require a time comparison")
	}
	if isTimeFilterComparison(expression) {
		value, adjustment, ok := filterTimeValue(expression.Args[1])
		if ok {
			if adjustment != nil {
				if _, err := fixedInterval(adjustment); err != nil {
					return nil, err
				}
			}
			return &GrammarExpression{Op: "time_compare", Temporal: &GrammarCompareTail{Operator: expression.Op, Value: value, Adjustment: adjustment}}, nil
		}
	}
	copied := *expression
	copied.Args = make([]*GrammarExpression, len(expression.Args))
	for index, child := range expression.Args {
		next, err := normalizeFilterTime(child)
		if err != nil {
			return nil, err
		}
		copied.Args[index] = next
	}
	return &copied, nil
}

func isTimeFilterComparison(expression *GrammarExpression) bool {
	switch expression.Op {
	case "=", "!=", "<", "<=", ">", ">=":
	default:
		return false
	}
	return len(expression.Args) == 2 && expression.Args[0].Op == "column" && expression.Args[0].TypeSpec == nil && isBuiltinTime(expression.Args[0].Identifier)
}

func filterTimeValue(expression *GrammarExpression) (*GrammarValue, *GrammarIntervalAdjustment, bool) {
	var adjustment *GrammarIntervalAdjustment
	if expression.Op == "time_adjust" {
		adjustment = expression.Temporal.Adjustment
		expression = expression.Args[0]
	}
	switch {
	case expression.Op == "clock":
		return &GrammarValue{Now: &GrammarNowValue{Now: "NOW"}}, adjustment, true
	case expression.Param:
		return &GrammarValue{Param: true, ParamIndex: expression.ParamIndex}, adjustment, true
	case expression.Op == "literal" && expression.Type == ql.ExpressionString:
		value := expression.Literal.String
		return &GrammarValue{String: &value}, adjustment, true
	default:
		return nil, nil, false
	}
}

func boundExpressionLiteral(bound resolvedParam) *ql.Expression {
	if bound.expression != nil {
		return cloneExpressionLiteral(bound.expression)
	}
	if len(bound.values) == 1 {
		return grammarLiteralExpression(bound.values[0])
	}
	expression := &ql.Expression{Op: "literal", Type: ql.ExpressionIntArray}
	for _, value := range bound.values {
		switch {
		case value.Integer != nil:
			if len(expression.Literal.Strings) > 0 {
				return &ql.Expression{Op: "invalid_mixed_list"}
			}
			expression.Literal.Ints = append(expression.Literal.Ints, *value.Integer)
		case value.String != nil:
			if len(expression.Literal.Ints) > 0 {
				return &ql.Expression{Op: "invalid_mixed_list"}
			}
			expression.Type = ql.ExpressionStringArray
			expression.Literal.Strings = append(expression.Literal.Strings, *value.String)
		default:
			return &ql.Expression{Op: "invalid_list_value"}
		}
	}
	return expression
}

func grammarLiteralExpression(value *GrammarValue) *ql.Expression {
	expression := grammarValueExpression(value)
	return &ql.Expression{Op: expression.Op, Type: expression.Type, Literal: expression.Literal}
}

// bindFilter returns the complete predicate and a conservative timestamp
// envelope. A non-time branch may match every stored timestamp; AND intersects
// envelopes while OR unions them, retaining the full predicate for correlation.
func (m *measureExpression) bindFilter(bound *BoundQuery, now time.Time) (*ql.Expression, []ql.MillisRange, error) {
	if m.temporal != nil {
		ranges, err := bindTimeComparison(&transformRun{bound: bound.values}, now, m.temporal)
		if err != nil {
			return nil, nil, err
		}
		return filterRangeExpression(ranges), ranges, nil
	}
	if m.parameter {
		return boundExpressionLiteral(bound.values[m.paramIndex]), allMeasureTime(), nil
	}
	expression := m.value
	expression.Args = make([]*ql.Expression, 0, len(m.args))
	ranges := allMeasureTime()
	for index, child := range m.args {
		next, childRanges, err := child.bindFilter(bound, now)
		if err != nil {
			return nil, nil, err
		}
		switch expression.Op {
		case "and":
			ranges = intersectTimeRanges(ranges, childRanges)
		case "or":
			if index == 0 {
				ranges = childRanges
			} else {
				ranges = unionTimeRanges(ranges, childRanges)
			}
		}
		appendBoundExpressionArgument(&expression, index, next)
	}
	resolved, err := m.bindMembership(&expression)
	return resolved, ranges, err
}

func filterRangeExpression(ranges []ql.MillisRange) *ql.Expression {
	var result *ql.Expression
	for _, interval := range ranges {
		var value *ql.Expression
		if interval.Begin > minMeasureMillis {
			value = filterTimeBound(">=", interval.Begin)
		}
		if interval.End < measureEndMillis {
			upper := filterTimeBound("<", interval.End)
			if value == nil {
				value = upper
			} else {
				value = &ql.Expression{Op: "and", Args: []*ql.Expression{value, upper}}
			}
		}
		if value == nil {
			value = &ql.Expression{Op: "literal", Type: ql.ExpressionBool, Literal: ql.ExpressionScalar{Bool: true}}
		}
		if result == nil {
			result = value
		} else {
			result = &ql.Expression{Op: "or", Args: []*ql.Expression{result, value}}
		}
	}
	if result == nil {
		result = &ql.Expression{Op: "literal", Type: ql.ExpressionBool, Literal: ql.ExpressionScalar{Bool: false}}
	}
	return result
}

func filterTimeBound(operator string, millis int64) *ql.Expression {
	return &ql.Expression{Op: operator, Args: []*ql.Expression{
		{Op: "column", Type: ql.ExpressionTimestamp, Source: vectorized.ColumnDef{Name: "time", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64}},
		{Op: "literal", Type: ql.ExpressionTimestamp, Literal: ql.ExpressionScalar{Int64: millis * int64(time.Millisecond)}},
	}}
}

// necessaryFilterCriteria pushes only scalar tag constraints proven necessary by
// conjunction. A mixed OR cannot prune either branch using one branch's tag.
func necessaryFilterCriteria(expression *ql.Expression, tags map[measureColumnKey]bool) *modelv1.Criteria {
	if expression.Op == "and" {
		return combineMeasureCriteria(necessaryFilterCriteria(expression.Args[0], tags), necessaryFilterCriteria(expression.Args[1], tags), false)
	}
	if expression.Op != "=" && expression.Op != "in" || len(expression.Args) < 2 {
		return nil
	}
	source := expression.Args[0]
	if source.Op != "column" || !tags[columnIdentity(source.Source)] || source.Type != ql.ExpressionInt && source.Type != ql.ExpressionString {
		return nil
	}
	condition := &modelv1.Condition{Name: source.Source.Name, Op: modelv1.Condition_BINARY_OP_EQ}
	condition.Value = necessaryFilterValue(expression, source.Type)
	if condition.Value == nil {
		return nil
	}
	if expression.Op == "in" {
		condition.Op = modelv1.Condition_BINARY_OP_IN
	}
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: condition}}
}

func (m *MeasureTemplate) bindWhere(bound *BoundQuery, now time.Time, request *measurev1.QueryRequest, plan *ql.RawPlan) ([]ql.MillisRange, error) {
	ranges := allMeasureTime()
	var err error
	if err = m.bindPatterns(bound, plan); err != nil {
		return nil, err
	}
	if m.predicate != nil {
		ranges, request.Criteria, err = m.predicate.bind(&transformRun{bound: bound.values}, now)
		if err != nil {
			return nil, err
		}
	}
	if m.filter != nil {
		var filterRanges []ql.MillisRange
		plan.Filter, filterRanges, err = m.filter.bindFilter(bound, now)
		if err != nil {
			return nil, err
		}
		plan.Filter, err = ql.ResolvePredicate(plan.Filter)
		if err != nil {
			return nil, err
		}
		plan.Empty = plan.Empty || filterRejectsAllRows(plan.Filter)

		ranges = intersectTimeRanges(ranges, filterRanges)
		request.Criteria = combineMeasureCriteria(request.Criteria, necessaryFilterCriteria(plan.Filter, m.filterTags), false)
	}
	return ranges, nil
}
