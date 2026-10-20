// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Package nativeanalysis contains the bounded analyzers used by the native
// query adapter.
package nativeanalysis

import (
	"bytes"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/blevesearch/segment"
)

// ErrUnknownAnalyzer reports an analyzer name without a native implementation.
var ErrUnknownAnalyzer = errors.New("nativeanalysis: unknown analyzer")

// Term is an analyzed owned token and its occurrence count.
type Term struct {
	Value     []byte
	Frequency uint64
}

// Tokens returns the token sequence, in input order and with repeats, that
// keyword, simple, standard, or URL analysis produces for input.
func Tokens(name string, input []byte) ([]string, error) {
	switch strings.ToLower(name) {
	case "", "keyword":
		return []string{string(input)}, nil
	case "simple":
		return split(input, true, true), nil
	case "standard":
		return standardTokens(input), nil
	case "url":
		return split(input, false, false), nil
	default:
		return nil, ErrUnknownAnalyzer
	}
}

// Analyze tokenizes input using keyword, simple, standard, or URL semantics.
func Analyze(name string, input []byte) ([]Term, error) {
	tokens, err := Tokens(name, input)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]uint64, len(tokens))
	order := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if _, ok := counts[token]; !ok {
			order = append(order, token)
		}
		counts[token]++
	}
	result := make([]Term, 0, len(order))
	for _, token := range order {
		result = append(result, Term{Value: []byte(token), Frequency: counts[token]})
	}
	return result, nil
}

func split(input []byte, lettersOnly, lower bool) []string {
	if !utf8.Valid(input) {
		return nil
	}
	var out []string
	var buf []rune
	flush := func() {
		if len(buf) > 0 {
			out = append(out, string(buf))
			buf = nil
		}
	}
	for len(input) > 0 {
		r, n := utf8.DecodeRune(input)
		input = input[n:]
		if r == utf8.RuneError && n == 1 {
			flush()
			continue
		}
		allowed := unicode.IsLetter(r)
		if !lettersOnly {
			allowed = allowed || unicode.IsNumber(r)
		}
		if allowed {
			if lower {
				r = unicode.ToLower(r)
			}
			buf = append(buf, r)
		} else {
			flush()
		}
	}
	flush()
	return out
}

func standardTokens(input []byte) []string {
	s := segment.NewWordSegmenterDirect(input)
	var tokens []string
	for s.Segment() {
		if s.Type() != segment.None {
			tokens = append(tokens, string(bytes.ToLower(s.Bytes())))
		}
	}
	return tokens
}
