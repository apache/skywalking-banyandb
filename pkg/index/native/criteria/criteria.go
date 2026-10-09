// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package criteria translates modelv1.Criteria into pkg/index/native engine
// calls over a candidate set of native.QueryHit. It was promoted out of
// Property's native store (banyand/property/db/native_property.go, NIDX-03
// design §5) so another native-backed caller -- for example the series
// index -- can reuse the same EQ/NE/IN/NOT_IN/HAVING/NOT_HAVING/range/MATCH/
// AND/OR semantics with its own field naming, supplied through FieldResolver.
package criteria

import (
	"context"
	"fmt"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/index/nativeanalysis"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// FieldResolver maps one modelv1.Criteria condition's tag name to the engine
// field that carries it and the analyzer schema assigns the tag for MATCH.
// Field returns ok false when the tag is not indexed for this caller: a
// positive condition (EQ, IN, HAVING, MATCH) then resolves to no hits, and a
// negative one (NE, NOT_IN, NOT_HAVING) resolves to the unchanged universe,
// since an unindexed tag can never have a live posting to find or exclude.
type FieldResolver interface {
	Field(tagName string) (field string, analyzer string, ok bool)
}

// Filter narrows universe, a candidate set already produced by the caller
// (for example view.MatchAllTermSets over series or identity matchers), to
// the hits criteria additionally selects. It translates EQ, NE, IN, NOT_IN,
// HAVING, NOT_HAVING, GT/GE/LT/LE ranges, MATCH, AND, and OR into FilterRange
// and FilterTermsSet calls on view; it never decodes a stored document. A nil
// criteria returns universe unchanged.
func Filter(
	ctx context.Context, view *native.ReadView, universe []native.QueryHit, criteria *modelv1.Criteria, fields FieldResolver,
) ([]native.QueryHit, error) {
	return matchCriteria(ctx, view, universe, criteria, fields)
}

func matchCriteria(
	ctx context.Context, view *native.ReadView, universe []native.QueryHit, criteria *modelv1.Criteria, fields FieldResolver,
) ([]native.QueryHit, error) {
	if criteria == nil {
		return universe, nil
	}
	switch expression := criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		return matchCondition(ctx, view, universe, expression.Condition, fields)
	case *modelv1.Criteria_Le:
		if expression.Le == nil || (expression.Le.Left == nil && expression.Le.Right == nil) {
			return nil, fmt.Errorf("logical expression has no operands: %w", logical.ErrInvalidLogicalExpression)
		}
		if expression.Le.Left == nil {
			return matchCriteria(ctx, view, universe, expression.Le.Right, fields)
		}
		if expression.Le.Right == nil {
			return matchCriteria(ctx, view, universe, expression.Le.Left, fields)
		}
		left, err := matchCriteria(ctx, view, universe, expression.Le.Left, fields)
		if err != nil {
			return nil, err
		}
		right, err := matchCriteria(ctx, view, universe, expression.Le.Right, fields)
		if err != nil {
			return nil, err
		}
		switch expression.Le.Op {
		case modelv1.LogicalExpression_LOGICAL_OP_OR:
			return unionHits(left, right), nil
		case modelv1.LogicalExpression_LOGICAL_OP_AND:
			return intersectHits(left, right), nil
		default:
			return nil, fmt.Errorf("unsupported logical operator %d: %w", expression.Le.Op, logical.ErrInvalidLogicalExpression)
		}
	default:
		return nil, logical.ErrInvalidCriteriaType
	}
}

//nolint:gocyclo // the condition-op switch is kept explicit, matching the code this was promoted from.
func matchCondition(
	ctx context.Context, view *native.ReadView, universe []native.QueryHit, condition *modelv1.Condition, fields FieldResolver,
) ([]native.QueryHit, error) {
	if condition == nil || condition.Value == nil || condition.Value.Value == nil {
		return nil, logical.ErrUnsupportedConditionValue
	}
	if condition.Op == modelv1.Condition_BINARY_OP_MATCH {
		if _, ok := condition.Value.Value.(*modelv1.TagValue_Str); !ok {
			return nil, logical.ErrUnsupportedConditionValue
		}
	}
	if condition.Op == modelv1.Condition_BINARY_OP_IN || condition.Op == modelv1.Condition_BINARY_OP_NOT_IN {
		switch condition.Value.Value.(type) {
		case *modelv1.TagValue_StrArray, *modelv1.TagValue_IntArray:
		default:
			return nil, logical.ErrUnsupportedConditionValue
		}
	}
	expr, err := logical.ParseExpr(condition)
	if err != nil {
		return nil, err
	}
	negated := condition.Op == modelv1.Condition_BINARY_OP_NE || condition.Op == modelv1.Condition_BINARY_OP_NOT_IN ||
		condition.Op == modelv1.Condition_BINARY_OP_NOT_HAVING
	field, analyzer, ok := fields.Field(condition.Name)
	if !ok {
		if negated {
			return universe, nil
		}
		return nil, nil
	}
	values := expr.Bytes()
	match := func(terms [][]byte, mode native.TermSetMode) ([]native.QueryHit, error) {
		return view.FilterTermsSet(ctx, universe, native.TermSetRequest{Field: field, Terms: terms, Mode: mode})
	}
	switch condition.Op {
	case modelv1.Condition_BINARY_OP_EQ:
		if len(values) != 1 {
			return nil, logical.ErrUnsupportedConditionOp
		}
		return match(values, native.MatchAnyTerm)
	case modelv1.Condition_BINARY_OP_IN:
		return match(values, native.MatchAnyTerm)
	case modelv1.Condition_BINARY_OP_HAVING:
		return match(values, native.MatchAllTerms)
	case modelv1.Condition_BINARY_OP_MATCH:
		if len(values) != 1 {
			return nil, logical.ErrUnsupportedConditionOp
		}
		// The request's own analyzer choice (if any) wins over the schema's,
		// exactly as Property's MATCH handling always worked.
		requestAnalyzer := condition.MatchOption.GetAnalyzer()
		if requestAnalyzer == "" {
			requestAnalyzer = analyzer
		}
		terms, analyzeErr := nativeanalysis.Analyze(requestAnalyzer, values[0])
		if analyzeErr != nil {
			return nil, analyzeErr
		}
		analyzed := make([][]byte, 0, len(terms))
		for _, term := range terms {
			analyzed = append(analyzed, term.Value)
		}
		mode := native.MatchAnyTerm
		if condition.MatchOption.GetOperator() == modelv1.Condition_MatchOption_OPERATOR_AND {
			mode = native.MatchAllTerms
		}
		return match(analyzed, mode)
	case modelv1.Condition_BINARY_OP_GT, modelv1.Condition_BINARY_OP_GE,
		modelv1.Condition_BINARY_OP_LT, modelv1.Condition_BINARY_OP_LE:
		if len(values) != 1 {
			return nil, logical.ErrUnsupportedConditionOp
		}
		request := native.RangeRequest{Field: field, MaxTerms: ^uint64(0), Lower: nil, Upper: nil}
		switch condition.Op {
		case modelv1.Condition_BINARY_OP_GT, modelv1.Condition_BINARY_OP_GE:
			request.Lower, request.IncludesLower = values[0], condition.Op == modelv1.Condition_BINARY_OP_GE
		default:
			request.Upper, request.IncludesUpper = values[0], condition.Op == modelv1.Condition_BINARY_OP_LE
		}
		return view.FilterRange(ctx, universe, request)
	case modelv1.Condition_BINARY_OP_NE, modelv1.Condition_BINARY_OP_NOT_IN,
		modelv1.Condition_BINARY_OP_NOT_HAVING:
		if condition.Op == modelv1.Condition_BINARY_OP_NE && len(values) != 1 {
			return nil, logical.ErrUnsupportedConditionOp
		}
		mode := native.MatchAnyTerm
		if condition.Op == modelv1.Condition_BINARY_OP_NOT_HAVING {
			mode = native.MatchAllTerms
		}
		excluded, matchErr := match(values, mode)
		if matchErr != nil {
			return nil, matchErr
		}
		return subtractHits(universe, excluded), nil
	default:
		return nil, logical.ErrUnsupportedConditionOp
	}
}

// hitKey identifies one native.QueryHit by its physical coordinates, so set
// operations over hit slices do not depend on Identifier byte equality.
type hitKey struct {
	segment  uint64
	document uint64
}

func keyOf(hit native.QueryHit) hitKey {
	return hitKey{segment: hit.Segment, document: hit.DocumentNumber}
}

func intersectHits(left, right []native.QueryHit) []native.QueryHit {
	rightSet := make(map[hitKey]struct{}, len(right))
	for _, hit := range right {
		rightSet[keyOf(hit)] = struct{}{}
	}
	result := make([]native.QueryHit, 0, len(left))
	for _, hit := range left {
		if _, ok := rightSet[keyOf(hit)]; ok {
			result = append(result, hit)
		}
	}
	return result
}

func unionHits(left, right []native.QueryHit) []native.QueryHit {
	result := make([]native.QueryHit, 0, len(left)+len(right))
	seen := make(map[hitKey]struct{}, len(left)+len(right))
	for _, hits := range [][]native.QueryHit{left, right} {
		for _, hit := range hits {
			key := keyOf(hit)
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				result = append(result, hit)
			}
		}
	}
	return result
}

func subtractHits(universe, excluded []native.QueryHit) []native.QueryHit {
	set := make(map[hitKey]struct{}, len(excluded))
	for _, hit := range excluded {
		set[keyOf(hit)] = struct{}{}
	}
	result := make([]native.QueryHit, 0, len(universe))
	for _, hit := range universe {
		if _, ok := set[keyOf(hit)]; !ok {
			result = append(result, hit)
		}
	}
	return result
}
