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

// SingleTermResolver is an optional interface of a FieldResolver. SingleTerm reports that a tag's field holds exactly one
// term per document (a scalar that is not analyzed), which is what lets two range conditions on it fold into one range at
// posting level. A resolver that does not implement it, or a tag it does not vouch for, gets no folding.
type SingleTermResolver interface {
	SingleTerm(tagName string) bool
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

// resolvedCondition is a condition checked and reduced to what both translations need: the engine field and the terms or
// range it selects, whether the selection is excluded, and whether the tag is indexed at all.
type resolvedCondition struct {
	terms      [][]byte
	field      string
	rng        native.RangeRequest
	mode       native.TermSetMode
	isRange    bool
	negated    bool // NE, NOT_IN and NOT_HAVING exclude the hits the terms select
	unresolved bool // the tag is not indexed for this caller
	singleTerm bool // the resolver vouches that the field holds one term per document
}

// resolveCondition validates condition, parses its value, resolves its tag and builds the terms or range it selects. It is
// the one place that decides what a condition means, so that the hit-level filter and the posting-level translation cannot
// drift apart. The checks run in the order the hit-level filter has always run them: a tag that is not indexed resolves
// before the operator's operand count is checked.
//
//nolint:gocyclo // the condition-op switch is kept explicit, matching the code this was promoted from.
func resolveCondition(condition *modelv1.Condition, fields FieldResolver) (resolvedCondition, error) {
	var resolved resolvedCondition
	if condition == nil || condition.Value == nil || condition.Value.Value == nil {
		return resolved, logical.ErrUnsupportedConditionValue
	}
	if condition.Op == modelv1.Condition_BINARY_OP_MATCH {
		if _, ok := condition.Value.Value.(*modelv1.TagValue_Str); !ok {
			return resolved, logical.ErrUnsupportedConditionValue
		}
	}
	if condition.Op == modelv1.Condition_BINARY_OP_IN || condition.Op == modelv1.Condition_BINARY_OP_NOT_IN {
		switch condition.Value.Value.(type) {
		case *modelv1.TagValue_StrArray, *modelv1.TagValue_IntArray:
		default:
			return resolved, logical.ErrUnsupportedConditionValue
		}
	}
	expr, err := logical.ParseExpr(condition)
	if err != nil {
		return resolved, err
	}
	resolved.negated = condition.Op == modelv1.Condition_BINARY_OP_NE || condition.Op == modelv1.Condition_BINARY_OP_NOT_IN ||
		condition.Op == modelv1.Condition_BINARY_OP_NOT_HAVING
	field, analyzer, ok := fields.Field(condition.Name)
	if !ok {
		resolved.unresolved = true
		return resolved, nil
	}
	resolved.field = field
	if single, vouches := fields.(SingleTermResolver); vouches {
		resolved.singleTerm = single.SingleTerm(condition.Name)
	}
	values := expr.Bytes()
	switch condition.Op {
	case modelv1.Condition_BINARY_OP_EQ, modelv1.Condition_BINARY_OP_NE:
		if len(values) != 1 {
			return resolved, logical.ErrUnsupportedConditionOp
		}
		resolved.terms, resolved.mode = values, native.MatchAnyTerm
	case modelv1.Condition_BINARY_OP_IN, modelv1.Condition_BINARY_OP_NOT_IN:
		resolved.terms, resolved.mode = values, native.MatchAnyTerm
	case modelv1.Condition_BINARY_OP_HAVING, modelv1.Condition_BINARY_OP_NOT_HAVING:
		resolved.terms, resolved.mode = values, native.MatchAllTerms
	case modelv1.Condition_BINARY_OP_MATCH:
		if len(values) != 1 {
			return resolved, logical.ErrUnsupportedConditionOp
		}
		if resolved.terms, resolved.mode, err = analyzeMatch(condition, analyzer, values[0]); err != nil {
			return resolved, err
		}
	case modelv1.Condition_BINARY_OP_GT, modelv1.Condition_BINARY_OP_GE,
		modelv1.Condition_BINARY_OP_LT, modelv1.Condition_BINARY_OP_LE:
		if len(values) != 1 {
			return resolved, logical.ErrUnsupportedConditionOp
		}
		resolved.isRange = true
		resolved.rng = native.RangeRequest{Field: field, MaxTerms: ^uint64(0)}
		switch condition.Op {
		case modelv1.Condition_BINARY_OP_GT, modelv1.Condition_BINARY_OP_GE:
			resolved.rng.Lower, resolved.rng.IncludesLower = values[0], condition.Op == modelv1.Condition_BINARY_OP_GE
		default:
			resolved.rng.Upper, resolved.rng.IncludesUpper = values[0], condition.Op == modelv1.Condition_BINARY_OP_LE
		}
	default:
		return resolved, logical.ErrUnsupportedConditionOp
	}
	return resolved, nil
}

// analyzeMatch analyzes the text of a MATCH condition and returns the terms and the mode that select the documents. The
// request's own analyzer choice (if any) wins over the schema's, exactly as Property's MATCH handling always worked.
func analyzeMatch(condition *modelv1.Condition, schemaAnalyzer string, text []byte) ([][]byte, native.TermSetMode, error) {
	requestAnalyzer := condition.MatchOption.GetAnalyzer()
	if requestAnalyzer == "" {
		requestAnalyzer = schemaAnalyzer
	}
	terms, err := nativeanalysis.Analyze(requestAnalyzer, text)
	if err != nil {
		return nil, 0, err
	}
	analyzed := make([][]byte, 0, len(terms))
	for _, term := range terms {
		analyzed = append(analyzed, term.Value)
	}
	mode := native.MatchAnyTerm
	if condition.MatchOption.GetOperator() == modelv1.Condition_MatchOption_OPERATOR_AND {
		mode = native.MatchAllTerms
	}
	return analyzed, mode, nil
}

func matchCondition(
	ctx context.Context, view *native.ReadView, universe []native.QueryHit, condition *modelv1.Condition, fields FieldResolver,
) ([]native.QueryHit, error) {
	resolved, err := resolveCondition(condition, fields)
	if err != nil {
		return nil, err
	}
	if resolved.unresolved {
		// An unindexed tag has no live posting to find or exclude.
		if resolved.negated {
			return universe, nil
		}
		return nil, nil
	}
	if resolved.isRange {
		return view.FilterRange(ctx, universe, resolved.rng)
	}
	matched, err := view.FilterTermsSet(ctx, universe, native.TermSetRequest{Field: resolved.field, Terms: resolved.terms, Mode: resolved.mode})
	if err != nil {
		return nil, err
	}
	if resolved.negated {
		return subtractHits(universe, matched), nil
	}
	return matched, nil
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

// PostingFilterOf translates criteria into a posting-level filter that selects a superset of what Filter selects: the same
// documents when every condition translates, and otherwise the documents the translatable conditions leave. A caller
// narrows its candidates with it at posting level, before it decodes any document, and still applies Filter afterwards, so
// a condition that is left out (an invalid one, or one under an OR next to a condition that is left out) only costs speed,
// never correctness. It returns nil when nothing can be pushed down, which matches every candidate.
func PostingFilterOf(criteria *modelv1.Criteria, fields FieldResolver) *native.PostingFilter {
	filter, _ := PostingFilterExact(criteria, fields)
	return filter
}

// PostingFilterExact is PostingFilterOf that also reports whether the filter selects exactly what Filter selects, so a
// caller can tell how many of the candidates the filter leaves still fall to the hit-level filter.
func PostingFilterExact(criteria *modelv1.Criteria, fields FieldResolver) (filter *native.PostingFilter, exact bool) {
	if criteria == nil {
		return nil, true
	}
	if fields == nil {
		return nil, false
	}
	switch expression := criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		if filter = conditionPostingFilter(expression.Condition, fields); filter == nil {
			return nil, false
		}
		return filter, true
	case *modelv1.Criteria_Le:
		if expression.Le == nil {
			return nil, false
		}
		if expression.Le.Left == nil {
			return PostingFilterExact(expression.Le.Right, fields)
		}
		if expression.Le.Right == nil {
			return PostingFilterExact(expression.Le.Left, fields)
		}
		left, leftExact := PostingFilterExact(expression.Le.Left, fields)
		right, rightExact := PostingFilterExact(expression.Le.Right, fields)
		switch expression.Le.Op {
		case modelv1.LogicalExpression_LOGICAL_OP_AND:
			// A conjunct that cannot be pushed only leaves more documents than the criteria keeps.
			switch {
			case left == nil:
				return right, leftExact && rightExact
			case right == nil:
				return left, leftExact && rightExact
			}
			return native.AllOf(left, right), leftExact && rightExact
		case modelv1.LogicalExpression_LOGICAL_OP_OR:
			// A disjunct that cannot be pushed may select any document, so the union cannot be narrowed.
			if left == nil || right == nil {
				return nil, false
			}
			return native.AnyOf(left, right), leftExact && rightExact
		}
	}
	return nil, false
}

// conditionPostingFilter is the posting-level filter of one condition: the filter of what resolveCondition resolves it to.
// It returns nil for a condition resolveCondition rejects, so that Filter reports the error.
func conditionPostingFilter(condition *modelv1.Condition, fields FieldResolver) *native.PostingFilter {
	resolved, err := resolveCondition(condition, fields)
	if err != nil {
		return nil
	}
	switch {
	case resolved.unresolved:
		// A tag that is not indexed never has a posting to find or exclude.
		if resolved.negated {
			return native.MatchAll()
		}
		return native.MatchNone()
	case resolved.isRange:
		if resolved.singleTerm {
			return native.FilterSingleTermRanges(resolved.rng)
		}
		return native.FilterRanges(resolved.rng)
	}
	terms := native.FilterTerms(native.TermSetRequest{Field: resolved.field, Terms: resolved.terms, Mode: resolved.mode})
	if resolved.negated {
		return native.NotOf(terms)
	}
	return terms
}
