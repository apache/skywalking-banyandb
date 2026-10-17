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
	"errors"
	"fmt"
	"strings"
)

var errNoExpressionWhere = errors.New("no WHERE expression")

// parseExpressionWhere keeps the source and trailing clauses in the legacy
// grammar while sharing the typed SELECT expression parser for WHERE.
func parseExpressionWhere(query string) (*Grammar, error) {
	tokens, err := expressionTokens(query)
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 || !strings.EqualFold(tokens[0].text, "SELECT") {
		return nil, fmt.Errorf("expression WHERE requires SELECT")
	}
	where, end := expressionWhereBounds(tokens)
	if where < 0 {
		return nil, errNoExpressionWhere
	}
	if where+1 >= end {
		return nil, fmt.Errorf("missing WHERE expression")
	}
	parser := expressionParser{tokens: tokens[where+1 : end], where: true}
	expression, err := parser.parse(0)
	if err != nil {
		return nil, err
	}
	if parser.pos != len(parser.tokens) {
		return nil, fmt.Errorf("unexpected WHERE expression token %q", parser.tokens[parser.pos].text)
	}
	if err = validateExpressionDepth(expression); err != nil {
		return nil, err
	}
	tail := len(query)
	if end < len(tokens) {
		tail = tokens[end].begin
	}
	rewritten := query[:tokens[where].begin] + "WHERE __ql_where = 0 " + query[tail:]
	grammar, err := particpleParser.ParseString("", rewritten)
	if err != nil {
		grammar, err = parseExpressionProjection(rewritten)
	}
	if err != nil {
		return nil, err
	}
	grammar.Select.Where.Expr = nil
	grammar.Select.Where.Expression = expression
	return grammar, nil
}

func expressionQualifiedToken(tokens []expressionToken, index int) bool {
	return index > 0 && tokens[index-1].text == "." || index+1 < len(tokens) && (tokens[index+1].text == "." || tokens[index+1].text == "::")
}

func expressionSourceName(tokens []expressionToken, index int) bool {
	if expressionQualifiedToken(tokens, index) {
		return true
	}
	if index > 0 {
		switch strings.ToUpper(tokens[index-1].text) {
		case "MEASURE", "STREAM", "TRACE", "PROPERTY", "IN", "ON":
			return true
		}
	}
	return false
}

func expressionClauseToken(tokens []expressionToken, index int) bool {
	if index+1 >= len(tokens) || expressionQualifiedToken(tokens, index) {
		return false
	}
	next := strings.ToUpper(tokens[index+1].text)
	switch strings.ToUpper(tokens[index].text) {
	case "GROUP", "ORDER":
		return next == "BY"
	case "WITH":
		return next == "QUERY_TRACE"
	case "LIMIT", "OFFSET":
		return tokens[index+1].kind == "number" || next == "?" || next == "-" || next == "+"
	default:
		return false
	}
}

func expressionWhereBounds(tokens []expressionToken) (int, int) {
	where, end := -1, len(tokens)
	var nesting expressionNesting
	from := false
	for index, token := range tokens {
		if !nesting.topLevelToken(tokens, index) {
			continue
		}
		if expressionFromToken(tokens, index) {
			from = true
			continue
		}
		if !from {
			continue
		}
		if where < 0 {
			if strings.EqualFold(token.text, "WHERE") && !expressionSourceName(tokens, index) {
				where = index
			}
			continue
		}
		if expressionClauseToken(tokens, index) {
			end = index
		}
		if end != len(tokens) {
			break
		}
	}
	return where, end
}

// expressionNesting tracks clause boundaries outside parentheses and CASE.
// Qualified names such as default.end do not open or close CASE expressions.
type expressionNesting struct {
	parentheses int
	cases       int
}

func (n *expressionNesting) topLevelToken(tokens []expressionToken, index int) bool {
	token := tokens[index]
	qualified := expressionQualifiedToken(tokens, index)
	switch strings.ToUpper(token.text) {
	case "(":
		n.parentheses++
	case ")":
		n.parentheses--
	case "CASE":
		if token.kind == "ident" && !qualified {
			n.cases++
		}
	case "END":
		if n.cases > 0 && token.kind == "ident" && !qualified {
			n.cases--
		}
	}
	return n.parentheses == 0 && n.cases == 0 && token.kind == "ident" && !qualified
}
