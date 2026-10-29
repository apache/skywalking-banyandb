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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

func (p *expressionParser) membership(source *GrammarExpression, not bool) (*GrammarExpression, error) {
	if !p.where && !p.having {
		return nil, fmt.Errorf("membership predicates require WHERE or HAVING")
	}
	if source.Op != "column" {
		return nil, fmt.Errorf("membership requires a source tag")
	}
	if source.TypeSpec == nil {
		kind := columnTypeTag
		source.TypeSpec = &kind
	}
	op := "contains"
	if not {
		op = "not_contains"
	}
	expression := &GrammarExpression{Op: op, Args: []*GrammarExpression{source}, MembershipList: p.take("(")}
	for {
		value, err := p.membershipValue()
		if err != nil {
			return nil, err
		}
		if (value.Op == "negate" || value.Op == "positive") && len(value.Args) == 1 && value.Args[0].Op == "literal" && value.Args[0].Type == ql.ExpressionInt {
			number := value.Args[0].Literal.Int64
			if value.Op == "negate" {
				number = -number
			}
			value = &GrammarExpression{Op: "literal", Type: ql.ExpressionInt, Literal: ql.ExpressionScalar{Int64: number}}
		}
		if value.Op != "literal" || !value.Param && value.Type != ql.ExpressionInt && value.Type != ql.ExpressionString && value.Type != ql.ExpressionNull {
			return nil, fmt.Errorf("membership values accept only integers, strings, NULL or list parameters")
		}
		value.ListParam = value.Param
		expression.Args = append(expression.Args, value)
		if !expression.MembershipList || !p.take(",") {
			break
		}
	}
	if expression.MembershipList {
		if err := p.need(")"); err != nil {
			return nil, err
		}
	}
	return expression, nil
}

func (p *expressionParser) membershipValue() (*GrammarExpression, error) {
	if p.pos < len(p.tokens) && (p.tokens[p.pos].kind == "quoted" || p.tokens[p.pos].kind == "string") {
		text, err := expressionUnquote(p.tokens[p.pos].text)
		p.pos++
		return &GrammarExpression{Op: "literal", Type: ql.ExpressionString, Literal: ql.ExpressionScalar{String: text}}, err
	}
	return p.prefix()
}

// bindMembership uses the existing native literal conversion, including its
// single/list shape, source-type coercions and NULL-list rejection. Only the
// execution location changes when another predicate needs a QL residual.
func (m *measureExpression) bindMembership(expression *ql.Expression) (*ql.Expression, error) {
	if m.membershipTag == nil {
		return expression, nil
	}
	values := make([]*GrammarValue, 0, len(expression.Args)-1)
	for _, argument := range expression.Args[1:] {
		if argument.Op != "literal" {
			return nil, fmt.Errorf("membership values must be literals or list parameters")
		}
		switch {
		case argument.Literal.Null:
			values = append(values, &GrammarValue{Null: true})
		case argument.Type == ql.ExpressionInt:
			value := argument.Literal.Int64
			values = append(values, &GrammarValue{Integer: &value})
		case argument.Type == ql.ExpressionString:
			value := argument.Literal.String
			values = append(values, &GrammarValue{String: &value})
		case argument.Type == ql.ExpressionIntArray:
			for _, value := range argument.Literal.Ints {
				values = append(values, &GrammarValue{Integer: &value})
			}
		case argument.Type == ql.ExpressionStringArray:
			for _, value := range argument.Literal.Strings {
				values = append(values, &GrammarValue{String: &value})
			}
		default:
			return nil, fmt.Errorf("unsupported membership value type %d", argument.Type)
		}
	}
	condition := &modelv1.Condition{}
	run := &transformRun{}
	var err error
	if !m.membershipList && len(values) == 1 {
		err = run.setGrammarConditionValue(condition, values[0], m.membershipTag)
	} else {
		err = run.setGrammarMultiValueCondition(condition, values, m.membershipTag)
	}
	if err != nil {
		return nil, err
	}
	literal, err := resolveExpressionParam(condition.Value)
	if err != nil {
		return nil, err
	}
	expression.Args = []*ql.Expression{expression.Args[0], literal}
	return expression, nil
}
