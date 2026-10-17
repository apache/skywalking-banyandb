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

var errNoExpressionGroupBy = errors.New("no computed GROUP BY clause")

func parseExpressionGroupBy(query string) (*Grammar, error) {
	tokens, err := expressionTokens(query)
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 || !strings.EqualFold(tokens[0].text, "SELECT") {
		return nil, errNoExpressionGroupBy
	}
	var nesting expressionNesting
	begin := -1
	for index := range tokens {
		if nesting.topLevelToken(tokens, index) && strings.EqualFold(tokens[index].text, "GROUP") &&
			index+1 < len(tokens) && strings.EqualFold(tokens[index+1].text, "BY") && !expressionSourceName(tokens, index) {
			begin = index + 2
			break
		}
	}
	if begin < 0 {
		return nil, errNoExpressionGroupBy
	}
	end := len(tokens)
	nesting = expressionNesting{}
	var segments [][]expressionToken
	segmentStart := begin
	for index := begin; index < len(tokens); index++ {
		top := nesting.topLevelToken(tokens, index)
		if top && (expressionClauseToken(tokens, index) || strings.EqualFold(tokens[index].text, "HAVING")) {
			end = index
			break
		}
		if nesting.parentheses == 0 && nesting.cases == 0 && tokens[index].text == "," {
			segments = append(segments, tokens[segmentStart:index])
			segmentStart = index + 1
		}
	}
	segments = append(segments, tokens[segmentStart:end])
	groups := make([]*GrammarGroupByColumn, 0, len(segments))
	for _, segment := range segments {
		if len(segment) == 0 {
			return nil, fmt.Errorf("empty GROUP BY expression")
		}
		ordinary, ordinaryErr := particpleParser.ParseString("", "SELECT v FROM MEASURE m IN g GROUP BY "+query[segment[0].begin:segment[len(segment)-1].end])
		if ordinaryErr == nil {
			groups = append(groups, ordinary.Select.GroupBy.Columns[0])
			continue
		}
		parser := expressionParser{tokens: segment, having: true, aggregateContext: "GROUP BY"}
		expr, parseErr := parser.parse(0)
		if parseErr != nil {
			return nil, parseErr
		}
		if expr.Param {
			return nil, fmt.Errorf("GROUP BY requires an expression or literal ordinal, not a bare placeholder")
		}
		if parser.pos != len(segment) {
			return nil, originalGroupParseError(query, fmt.Errorf("unexpected GROUP BY expression token %q", segment[parser.pos].text))
		}
		if parseErr = validateExpressionDepth(expr); parseErr != nil {
			return nil, parseErr
		}
		groups = append(groups, &GrammarGroupByColumn{Expression: expr})
	}
	tail := len(query)
	if end < len(tokens) {
		tail = tokens[end].begin
	}
	grammar, err := parseSelectWithoutComputedGroups(query[:tokens[begin-2].begin] + " GROUP BY __ql_group_placeholder " + query[tail:])
	if err != nil {
		return nil, originalGroupParseError(query, err)
	}
	grammar.Select.GroupBy.Columns = groups
	return grammar, nil
}

func originalGroupParseError(query string, fallback error) error {
	if _, err := particpleParser.ParseString("", query); err != nil {
		return err
	}
	return fallback
}
