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
	"unicode"
)

type expressionToken struct {
	text, kind string
	begin, end int
}

func expressionTokens(text string) ([]expressionToken, error) {
	var tokens []expressionToken
	for index := 0; index < len(text); {
		if unicode.IsSpace(rune(text[index])) {
			index++
			continue
		}
		token, err := scanExpressionToken(text, index)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
		index = token.end
	}
	return tokens, nil
}

func scanExpressionToken(text string, begin int) (expressionToken, error) {
	token := expressionToken{kind: "operator", begin: begin, end: begin + 1}
	ch := text[begin]
	switch {
	case ch == '\'' || ch == '"':
		token.kind = "string"
		if ch == '"' {
			token.kind = "quoted"
		}
		for token.end < len(text) && text[token.end] != ch {
			if text[token.end] == '\\' {
				token.end++
			}
			token.end++
		}
		if token.end >= len(text) {
			return token, fmt.Errorf("unterminated quoted value")
		}
		token.end++
	case expressionDigit(ch) || ch == '.' && begin+1 < len(text) && expressionDigit(text[begin+1]):
		token.kind = "number"
		var err error
		token.end, err = scanExpressionNumber(text, begin)
		if err != nil {
			return token, err
		}
	case expressionIdentStart(ch):
		token.kind = "ident"
		for token.end < len(text) && expressionIdentContinue(text[token.end]) {
			token.end++
		}
	default:
		if begin+1 < len(text) {
			switch text[begin : begin+2] {
			case "::", "!=", "<=", ">=", "<>":
				token.end++
			}
		}
	}
	token.text = text[begin:token.end]
	return token, nil
}

func scanExpressionNumber(text string, begin int) (int, error) {
	index := begin
	for index < len(text) && expressionDigit(text[index]) {
		index++
	}
	if index < len(text) && text[index] == '.' {
		index++
		for index < len(text) && expressionDigit(text[index]) {
			index++
		}
	}
	if index < len(text) && (text[index] == 'e' || text[index] == 'E') {
		index++
		if index < len(text) && (text[index] == '+' || text[index] == '-') {
			index++
		}
		begin = index
		for index < len(text) && expressionDigit(text[index]) {
			index++
		}
		if begin == index {
			return index, fmt.Errorf("invalid exponent")
		}
	}
	return index, nil
}

func expressionDigit(ch byte) bool { return ch >= '0' && ch <= '9' }

func expressionIdentContinue(ch byte) bool {
	return expressionIdentStart(ch) || expressionDigit(ch) || ch == '-' || ch == '*'
}

func expressionIdentStart(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '_'
}
