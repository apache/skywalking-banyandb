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

var errNoExpressionOrder = errors.New("no typed ORDER BY clause")

func parseExpressionOrder(query string) (*Grammar, error) {
	tokens, err := expressionTokens(query)
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 || !strings.EqualFold(tokens[0].text, "SELECT") {
		return nil, errNoExpressionOrder
	}
	var nesting expressionNesting
	begin := -1
	from := false
	for index := range tokens {
		if !nesting.topLevelToken(tokens, index) {
			continue
		}
		if expressionFromToken(tokens, index) {
			from = true
		}
		if from && strings.EqualFold(tokens[index].text, "ORDER") && index+1 < len(tokens) &&
			strings.EqualFold(tokens[index+1].text, "BY") && !expressionSourceName(tokens, index) {
			begin = index + 2
			break
		}
	}
	if begin < 0 {
		return nil, errNoExpressionOrder
	}
	prefix, prefixErr := parseQuerySyntax(query[:tokens[begin-2].begin])
	if prefixErr != nil {
		return nil, prefixErr
	}
	if prefix.Select.WithQueryTrace != nil || prefix.Select.Limit != nil || prefix.Select.Offset != nil {
		return nil, fmt.Errorf("ORDER BY must precede WITH, LIMIT and OFFSET")
	}
	nesting = expressionNesting{}
	end := len(tokens)
	start := begin
	var segments [][]expressionToken
	for index := begin; index < len(tokens); index++ {
		top := nesting.topLevelToken(tokens, index)
		if top && (strings.EqualFold(tokens[index].text, "LIMIT") || strings.EqualFold(tokens[index].text, "OFFSET") || strings.EqualFold(tokens[index].text, "WITH")) {
			end = index
			break
		}
		if nesting.parentheses == 0 && nesting.cases == 0 && tokens[index].text == "," {
			segments = append(segments, tokens[start:index])
			start = index + 1
		}
	}
	segments = append(segments, tokens[start:end])
	keys, err := parseOrderKeys(segments)
	if err != nil {
		return nil, err
	}
	tail := len(query)
	if end < len(tokens) {
		tail = tokens[end].begin
	}
	grammar, err := parseQuerySyntax(query[:tokens[begin-2].begin] + " " + query[tail:])
	if err != nil {
		return nil, err
	}
	grammar.Select.OrderBy = &GrammarSelectOrderByClause{Order: "ORDER", By: "BY", Keys: keys}
	return grammar, nil
}

func parseOrderKeys(segments [][]expressionToken) ([]*GrammarOrderKey, error) {
	keys := make([]*GrammarOrderKey, 0, len(segments))
	for _, segment := range segments {
		if len(segment) == 0 {
			return nil, fmt.Errorf("missing ORDER BY expression")
		}
		parser := expressionParser{tokens: segment, having: true, aggregateContext: "ORDER BY"}
		expression, parseErr := parser.parse(0)
		if parseErr != nil {
			return nil, parseErr
		}
		if parseErr = validateExpressionDepth(expression); parseErr != nil {
			return nil, parseErr
		}
		key := &GrammarOrderKey{Expression: expression}
		if parser.take("DESC") {
			key.Descending = true
		} else {
			parser.take("ASC")
		}
		key.NullsFirst = key.Descending
		if parser.take("NULLS") {
			switch {
			case parser.take("FIRST"):
				key.NullsFirst = true
			case parser.take("LAST"):
				key.NullsFirst = false
			default:
				return nil, fmt.Errorf("NULLS requires FIRST or LAST")
			}
		}
		if parser.pos != len(segment) {
			return nil, fmt.Errorf("unexpected ORDER BY token %q", segment[parser.pos].text)
		}
		keys = append(keys, key)
	}
	return keys, nil
}
