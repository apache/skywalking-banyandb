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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package qlgen

import (
	"fmt"
	"unicode"
	"unicode/utf8"
)

type patternToken struct {
	literal rune
	kind    byte
}

// patternMatch is an independent dynamic-programming oracle, not an index automaton.
func patternMatch(value, pattern, escape string, ignoreCase bool) (bool, error) {
	if !utf8.ValidString(pattern) || !utf8.ValidString(escape) || utf8.RuneCountInString(escape) > 1 {
		return false, fmt.Errorf("invalid pattern or escape")
	}
	var tokens []patternToken
	chars := []rune(pattern)
	escapes := []rune(escape)
	for i := 0; i < len(chars); i++ {
		ch := chars[i]
		switch {
		case len(escapes) > 0 && ch == escapes[0]:
			i++
			if i == len(chars) {
				return false, fmt.Errorf("trailing escape")
			}
			tokens = append(tokens, patternToken{literal: chars[i]})
		case ch == '%' || ch == '_':
			tokens = append(tokens, patternToken{kind: byte(ch)})
		default:
			tokens = append(tokens, patternToken{literal: ch})
		}
	}
	input := []rune(value)
	previous := make([]bool, len(input)+1)
	previous[0] = true
	equal := func(a, b rune) bool {
		if a == b {
			return true
		}
		if ignoreCase {
			for next := unicode.SimpleFold(a); next != a; next = unicode.SimpleFold(next) {
				if next == b {
					return true
				}
			}
		}
		return false
	}
	for _, token := range tokens {
		current := make([]bool, len(input)+1)
		if token.kind == '%' {
			current[0] = previous[0]
		}
		for i, ch := range input {
			switch token.kind {
			case '%':
				current[i+1] = previous[i+1] || current[i]
			case '_':
				current[i+1] = previous[i]
			default:
				current[i+1] = previous[i] && equal(ch, token.literal)
			}
		}
		previous = current
	}
	return previous[len(input)], nil
}

func patternCount(pattern any, escape string, ignoreCase, residual bool) (int64, error) {
	if pattern == nil {
		return 0, nil
	}
	input, ok := pattern.(string)
	if !ok {
		return 0, fmt.Errorf("pattern must be string")
	}
	matches := map[any]bool{}
	for _, row := range patternRows() {
		if row.Name == nil || residual && row.Value <= 0 {
			continue
		}
		matched, err := patternMatch(row.Name.(string), input, escape, ignoreCase)
		if err != nil {
			return 0, err
		}
		if matched {
			matches[row.Name] = true
		}
	}
	return int64(len(matches)), nil
}
