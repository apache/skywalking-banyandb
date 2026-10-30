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
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

func (p *expressionParser) castSuffix(value *GrammarExpression) (*GrammarExpression, error) {
	if p.pos >= len(p.tokens) {
		return nil, fmt.Errorf("expected cast type")
	}
	name := strings.ToUpper(p.tokens[p.pos].text)
	p.pos++
	if name == "TAG" || name == "FIELD" {
		if value.Op != "column" || value.TypeSpec != nil {
			return nil, fmt.Errorf("::%s requires an unqualified column", name)
		}
		value.TypeSpec = &name
		return value, nil
	}
	target, err := expressionCastType(name)
	return &GrammarExpression{Op: "cast", Type: target, Args: []*GrammarExpression{value}}, err
}

func expressionCastType(name string) (ql.ExpressionType, error) {
	switch name {
	case "INT", "BIGINT":
		return ql.ExpressionInt, nil
	case "FLOAT", "DOUBLE":
		return ql.ExpressionFloat, nil
	case "BOOL", "BOOLEAN":
		return ql.ExpressionBool, nil
	case "STRING":
		return ql.ExpressionString, nil
	case "TIMESTAMP":
		return ql.ExpressionTimestamp, nil
	default:
		return ql.ExpressionNull, fmt.Errorf("unsupported cast type %q", name)
	}
}

func (p *expressionParser) cast() (*GrammarExpression, error) {
	if err := p.need("("); err != nil {
		return nil, err
	}
	value, err := p.parse(0)
	if err != nil {
		return nil, err
	}
	if err = p.need("AS"); err != nil {
		return nil, err
	}
	cast, err := p.castSuffix(value)
	if err != nil {
		return nil, err
	}
	if cast.Op != "cast" {
		return nil, fmt.Errorf("CAST requires a value type")
	}
	if err = p.need(")"); err != nil {
		return nil, err
	}
	return cast, nil
}

func (p *expressionParser) coalesce() (*GrammarExpression, error) {
	if err := p.need("("); err != nil {
		return nil, err
	}
	args, err := p.list()
	if err != nil {
		return nil, err
	}
	return &GrammarExpression{Op: "coalesce", Args: args}, nil
}

func (p *expressionParser) in(value *GrammarExpression, not bool) (*GrammarExpression, error) {
	if err := p.need("("); err != nil {
		return nil, err
	}
	args, err := p.list()
	if err != nil {
		return nil, err
	}
	op := "in"
	if not {
		op = "not_in"
	}
	return &GrammarExpression{Op: op, Args: append([]*GrammarExpression{value}, args...)}, nil
}

func (p *expressionParser) list() ([]*GrammarExpression, error) {
	var values []*GrammarExpression
	if p.take(")") {
		return values, nil
	}
	for {
		value, err := p.parse(0)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
		if !p.take(",") {
			break
		}
	}
	return values, p.need(")")
}

func (p *expressionParser) caseExpr() (*GrammarExpression, error) {
	expression := &GrammarExpression{Op: "case"}
	if p.pos >= len(p.tokens) {
		return nil, fmt.Errorf("incomplete CASE")
	}
	if !strings.EqualFold(p.tokens[p.pos].text, "WHEN") {
		value, err := p.parse(0)
		if err != nil {
			return nil, err
		}
		expression.Op = "simple_case"
		expression.Args = append(expression.Args, value)
	}
	count := 0
	for p.take("WHEN") {
		condition, err := p.parse(0)
		if err != nil {
			return nil, err
		}
		if err = p.need("THEN"); err != nil {
			return nil, err
		}
		value, err := p.parse(0)
		if err != nil {
			return nil, err
		}
		expression.Args = append(expression.Args, condition, value)
		count++
	}
	if count == 0 {
		return nil, fmt.Errorf("CASE requires WHEN")
	}
	fallback := &GrammarExpression{Op: "literal", Type: ql.ExpressionNull, Literal: ql.ExpressionScalar{Null: true}}
	if p.take("ELSE") {
		var err error
		fallback, err = p.parse(0)
		if err != nil {
			return nil, err
		}
	}
	expression.Args = append(expression.Args, fallback)
	return expression, p.need("END")
}
