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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type measurePredicate struct {
	left, right          *measurePredicate
	temporal             *GrammarCompareTail
	tag                  *GrammarPredicate
	tagSpec              *tagSpecWithFamily
	tagName              string
	or, hasTime, hasTags bool
}

func compileMeasureOr(expr *GrammarOrExpr, columns *measureColumns) (*measurePredicate, error) {
	left, err := compileMeasureAnd(expr.Left, columns)
	if err != nil {
		return nil, err
	}
	for _, right := range expr.Right {
		next, nextErr := compileMeasureAnd(right.Right, columns)
		if nextErr != nil {
			return nil, nextErr
		}
		if (left.hasTime || next.hasTime) && (left.hasTags || next.hasTags) {
			return nil, fmt.Errorf("OR combining time and tag/field predicates requires residual expression execution")
		}
		left = &measurePredicate{left: left, right: next, or: true, hasTime: left.hasTime || next.hasTime, hasTags: left.hasTags || next.hasTags}
	}
	return left, nil
}

func compileMeasureAnd(expr *GrammarAndExpr, columns *measureColumns) (*measurePredicate, error) {
	left, err := compileMeasurePredicate(expr.Left, columns)
	if err != nil {
		return nil, err
	}
	for _, right := range expr.Right {
		next, nextErr := compileMeasurePredicate(right.Right, columns)
		if nextErr != nil {
			return nil, nextErr
		}
		left = &measurePredicate{left: left, right: next, hasTime: left.hasTime || next.hasTime, hasTags: left.hasTags || next.hasTags}
	}
	return left, nil
}

func compileMeasurePredicate(pred *GrammarPredicate, columns *measureColumns) (*measurePredicate, error) {
	if pred.Paren != nil {
		return compileMeasureOr(pred.Paren, columns)
	}
	var identifier *GrammarIdentifierPath
	switch {
	case pred.Binary != nil:
		identifier = pred.Binary.Identifier
	case pred.In != nil:
		identifier = pred.In.Identifier
	case pred.Having != nil:
		identifier = pred.Having.Identifier
	default:
		return nil, fmt.Errorf("empty WHERE predicate")
	}
	if isBuiltinTime(identifier) {
		if pred.Binary == nil || pred.Binary.Tail.Compare == nil {
			return nil, fmt.Errorf("time WHERE supports comparison predicates only")
		}
		comparison := pred.Binary.Tail.Compare
		if comparison.Value == nil || comparison.Value.Integer != nil || comparison.Value.Null {
			return nil, fmt.Errorf("time WHERE requires timestamp strings, timestamp parameters or now()")
		}
		if comparison.Adjustment != nil {
			if _, err := fixedInterval(comparison.Adjustment); err != nil {
				return nil, err
			}
		}
		return &measurePredicate{temporal: comparison, hasTime: true}, nil
	}
	name, err := identifier.ToString(false)
	if err != nil {
		return nil, err
	}
	source, err := columns.resolve(name, nil)
	if err != nil {
		return nil, err
	}
	if source.Role != vectorized.RoleTag {
		return nil, fmt.Errorf("field WHERE predicates require residual expression execution")
	}
	tagSpec := columns.tagSpecs[source.TagFamily+"."+source.Name]
	// Normalize only this private recipe's identifier, never the prepared AST.
	normalized := &GrammarIdentifierPath{QuotedIdent: &source.Name}
	copyPred := *pred
	switch {
	case pred.Binary != nil:
		if compare := pred.Binary.Tail.Compare; compare != nil && (compare.Value.Now != nil || compare.Adjustment != nil) {
			return nil, fmt.Errorf("now() and INTERVAL require a time predicate")
		}
		binary := *pred.Binary
		binary.Identifier = normalized
		copyPred.Binary = &binary
	case pred.In != nil:
		in := *pred.In
		in.Identifier = normalized
		copyPred.In = &in
	case pred.Having != nil:
		having := *pred.Having
		having.Identifier = normalized
		copyPred.Having = &having
	}
	return &measurePredicate{tag: &copyPred, tagSpec: tagSpec, tagName: source.Name, hasTags: true}, nil
}

func (p *measurePredicate) bind(run *transformRun, now time.Time) ([]ql.MillisRange, *modelv1.Criteria, error) {
	if p.temporal != nil {
		ranges, err := bindTimeComparison(run, now, p.temporal)
		return ranges, nil, err
	}
	if p.tag != nil {
		criteria, err := run.convertPredicate(p.tag, map[string]*tagSpecWithFamily{p.tagName: p.tagSpec}, nil)
		return allMeasureTime(), criteria, err
	}
	left, leftCriteria, err := p.left.bind(run, now)
	if err != nil {
		return nil, nil, err
	}
	right, rightCriteria, err := p.right.bind(run, now)
	if err != nil {
		return nil, nil, err
	}
	if p.or {
		return unionTimeRanges(left, right), combineMeasureCriteria(leftCriteria, rightCriteria, true), nil
	}
	return intersectTimeRanges(left, right), combineMeasureCriteria(leftCriteria, rightCriteria, false), nil
}

func combineMeasureCriteria(left, right *modelv1.Criteria, or bool) *modelv1.Criteria {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	op := modelv1.LogicalExpression_LOGICAL_OP_AND
	if or {
		op = modelv1.LogicalExpression_LOGICAL_OP_OR
	}
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{Op: op, Left: left, Right: right}}}
}

// checkTimeValuePositions prevents unsupported clock expressions from degrading
// into null literals in the legacy native predicate conversion.
func checkTimeValuePositions(grammar *Grammar) error {
	var checkOr func(*GrammarOrExpr) error
	var checkAnd func(*GrammarAndExpr) error
	checkValues := func(values []*GrammarValue) error {
		for _, value := range values {
			if value != nil && value.Now != nil {
				return fmt.Errorf("syntax error: now() requires a time comparison")
			}
		}
		return nil
	}
	checkPredicate := func(pred *GrammarPredicate) error {
		if pred.Paren != nil {
			return checkOr(pred.Paren)
		}
		switch {
		case pred.Binary != nil:
			tail := pred.Binary.Tail
			if tail.Compare != nil && !isBuiltinTime(pred.Binary.Identifier) {
				if tail.Compare.Adjustment != nil {
					return fmt.Errorf("syntax error: INTERVAL requires a time comparison")
				}
				return checkValues([]*GrammarValue{tail.Compare.Value})
			}
			if tail.Match != nil && tail.Match.Values != nil {
				if err := checkValues([]*GrammarValue{tail.Match.Values.Single}); err != nil {
					return err
				}
				return checkValues(tail.Match.Values.Array)
			}
		case pred.In != nil:
			return checkValues(pred.In.Values)
		case pred.Having != nil:
			if err := checkValues([]*GrammarValue{pred.Having.Values.Single}); err != nil {
				return err
			}
			return checkValues(pred.Having.Values.Array)
		}
		return nil
	}
	checkAnd = func(expr *GrammarAndExpr) error {
		if err := checkPredicate(expr.Left); err != nil {
			return err
		}
		for _, right := range expr.Right {
			if err := checkPredicate(right.Right); err != nil {
				return err
			}
		}
		return nil
	}
	checkOr = func(expr *GrammarOrExpr) error {
		if err := checkAnd(expr.Left); err != nil {
			return err
		}
		for _, right := range expr.Right {
			if err := checkAnd(right.Right); err != nil {
				return err
			}
		}
		return nil
	}
	if grammar.Select != nil && grammar.Select.Where != nil && grammar.Select.Where.Expr != nil {
		return checkOr(grammar.Select.Where.Expr)
	}
	if grammar.TopN != nil && grammar.TopN.Where != nil {
		return checkAnd(grammar.TopN.Where.Expr)
	}
	return nil
}
