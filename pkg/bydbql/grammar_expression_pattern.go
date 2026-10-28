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
	"unicode/utf8"

	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

func (p *expressionParser) pattern(left *GrammarExpression, op string, precedence int) (*GrammarExpression, error) {
	if !p.where || p.having {
		return nil, fmt.Errorf("%s is supported only in source WHERE predicates", op)
	}
	pattern, err := p.parse(precedence + 1)
	if err != nil {
		return nil, err
	}
	escape := "\\"
	if p.take("ESCAPE") {
		if p.pos >= len(p.tokens) || p.tokens[p.pos].kind != "string" {
			return nil, fmt.Errorf("ESCAPE requires a literal string containing zero or one Unicode character")
		}
		escape, err = expressionUnquote(p.tokens[p.pos].text)
		p.pos++
		if err != nil || !utf8.ValidString(escape) || utf8.RuneCountInString(escape) > 1 {
			return nil, fmt.Errorf("ESCAPE requires a literal string containing zero or one Unicode character")
		}
	}
	return &GrammarExpression{Op: strings.ToLower(op), Args: []*GrammarExpression{left, pattern}, Literal: ql.ExpressionScalar{String: escape}}, nil
}
