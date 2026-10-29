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

var errNoExpressionHaving = errors.New("no HAVING clause")

// parseExpressionHaving removes only a clause whose preceding SELECT is valid.
// An infix membership HAVING leaves an incomplete WHERE and cannot pass that
// check, so the legacy position and precedence remain unchanged.
func parseExpressionHaving(query string) (*Grammar, error) {
	tokens, err := expressionTokens(query)
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 || !strings.EqualFold(tokens[0].text, "SELECT") {
		return nil, errNoExpressionHaving
	}
	var nesting expressionNesting
	from := false
	for index, token := range tokens {
		if !nesting.topLevelToken(tokens, index) {
			continue
		}
		if expressionFromToken(tokens, index) {
			from = true
		}
		if !from || !strings.EqualFold(token.text, "HAVING") || expressionSourceName(tokens, index) {
			continue
		}
		grammar, candidateErr := parseHavingCandidate(query, tokens, index)
		if errors.Is(candidateErr, errNoExpressionHaving) {
			continue
		}
		return grammar, candidateErr
	}
	return nil, errNoExpressionHaving
}

func parseHavingCandidate(query string, tokens []expressionToken, index int) (*Grammar, error) {
	end := havingExpressionEnd(tokens, index+1)
	tail := len(query)
	if end < len(tokens) {
		tail = tokens[end].begin
	}
	grammar, prefixErr := parseSelectSyntax(query[:tokens[index].begin] + " " + query[tail:])
	if prefixErr != nil {
		return nil, errNoExpressionHaving
	}
	if grammar.Select.OrderBy != nil || grammar.Select.WithQueryTrace != nil || grammar.Select.Limit != nil || grammar.Select.Offset != nil {
		// A trailing clause belongs after HAVING, not before it. Reparse
		// only the prefix to distinguish that case from the suffix above.
		prefix, prefixOnlyErr := parseSelectSyntax(query[:tokens[index].begin])
		if prefixOnlyErr == nil && (prefix.Select.OrderBy != nil || prefix.Select.WithQueryTrace != nil ||
			prefix.Select.Limit != nil || prefix.Select.Offset != nil) {
			return nil, fmt.Errorf("HAVING must precede ORDER BY, WITH, LIMIT and OFFSET")
		}
	}
	if end < len(tokens) && strings.EqualFold(tokens[end].text, "GROUP") {
		return nil, fmt.Errorf("HAVING must follow GROUP BY")
	}
	if index+1 == end {
		return nil, fmt.Errorf("missing HAVING expression")
	}
	parser := expressionParser{tokens: tokens[index+1 : end], having: true}
	expression, parseErr := parser.parse(0)
	if parseErr != nil {
		return nil, parseErr
	}
	if parser.pos != len(parser.tokens) {
		return nil, fmt.Errorf("unexpected HAVING expression token %q", parser.tokens[parser.pos].text)
	}
	if parseErr = validateExpressionDepth(expression); parseErr != nil {
		return nil, parseErr
	}
	grammar.Select.Having = &GrammarHavingClause{Expression: expression}
	return grammar, nil
}

func havingExpressionEnd(tokens []expressionToken, begin int) int {
	var nesting expressionNesting
	for index := begin; index < len(tokens); index++ {
		if nesting.topLevelToken(tokens, index) && expressionClauseToken(tokens, index) {
			return index
		}
	}
	return len(tokens)
}
