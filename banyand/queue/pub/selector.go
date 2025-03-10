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

package pub

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// LabelSelector is a selector for labels.
type LabelSelector struct {
	criteria []condition
}

type condition struct {
	Key    string
	Values []string
	Op     operator
}

type operator int

const (
	opEquals operator = iota
	opNotEquals
	opIn
	opNotIn
	opExists
	opNotExists
)

// ParseLabelSelector parses a label selector string.
func ParseLabelSelector(selector string) (*LabelSelector, error) {
	if selector == "" {
		return &LabelSelector{}, nil
	}

	var conditions []condition
	for _, expr := range splitSelector(selector) {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			continue
		}

		cond, err := parseCondition(expr)
		if err != nil {
			return nil, fmt.Errorf("invalid selector %q: %w", selector, err)
		}
		conditions = append(conditions, cond)
	}

	return &LabelSelector{conditions}, nil
}

func splitSelector(selector string) []string {
	var exprs []string
	var current []rune
	parenLevel := 0
	for _, r := range selector {
		switch r {
		case '(':
			parenLevel++
		case ')':
			if parenLevel > 0 {
				parenLevel--
			}
		case ',':
			if parenLevel == 0 {
				exprs = append(exprs, strings.TrimSpace(string(current)))
				current = []rune{}
				continue
			}
		}
		current = append(current, r)
	}
	if len(current) > 0 {
		exprs = append(exprs, strings.TrimSpace(string(current)))
	}
	return exprs
}

func parseCondition(expr string) (condition, error) {
	if strings.HasPrefix(expr, "!") {
		key := strings.TrimSpace(expr[1:])
		if err := validateLabelKey(key); err != nil {
			return condition{}, err
		}
		return condition{Key: key, Op: opNotExists}, nil
	}

	var op operator
	var key, valuesStr string

	switch {
	case strings.Contains(expr, "!="):
		parts := strings.SplitN(expr, "!=", 2)
		key = strings.TrimSpace(parts[0])
		valuesStr = strings.TrimSpace(parts[1])
		op = opNotEquals
	case strings.Contains(expr, "="):
		parts := strings.SplitN(expr, "=", 2)
		key = strings.TrimSpace(parts[0])
		valuesStr = strings.TrimSpace(parts[1])
		op = opEquals
	case strings.Contains(expr, " notin "):
		parts := strings.SplitN(expr, " notin ", 2)
		key = strings.TrimSpace(parts[0])
		valuesStr = strings.TrimSpace(parts[1])
		op = opNotIn
	case strings.Contains(expr, " in "):
		parts := strings.SplitN(expr, " in ", 2)
		key = strings.TrimSpace(parts[0])
		valuesStr = strings.TrimSpace(parts[1])
		op = opIn
	default:
		if err := validateLabelKey(expr); err != nil {
			return condition{}, err
		}
		return condition{Key: expr, Op: opExists}, nil
	}

	if err := validateLabelKey(key); err != nil {
		return condition{}, err
	}

	values, err := parseValues(valuesStr, op)
	if err != nil {
		return condition{}, err
	}

	return condition{
		Key:    key,
		Op:     op,
		Values: values,
	}, nil
}

func parseValues(valuesStr string, op operator) ([]string, error) {
	if op == opIn || op == opNotIn {
		if !strings.HasPrefix(valuesStr, "(") || !strings.HasSuffix(valuesStr, ")") {
			return nil, fmt.Errorf("set operations require parenthesized values")
		}
		valuesStr = strings.Trim(valuesStr, "()")
	}

	var values []string
	for _, v := range strings.Split(valuesStr, ",") {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		values = append(values, v)
	}

	if len(values) == 0 {
		return nil, errors.New("empty value list")
	}
	return values, nil
}

func validateLabelKey(key string) error {
	if key == "" {
		return errors.New("empty key")
	}
	for _, c := range key {
		if !unicode.IsLower(c) && !unicode.IsDigit(c) && c != '-' && c != '_' && c != '.' {
			return fmt.Errorf("invalid key %q: must match [a-z0-9-_.]", key)
		}
	}
	return nil
}

// Matches returns true if the labels match the selector.
func (s *LabelSelector) Matches(labels map[string]string) bool {
	for _, req := range s.criteria {
		if !req.matches(labels) {
			return false
		}
	}
	return true
}

func (r condition) matches(labels map[string]string) bool {
	value, exists := labels[r.Key]

	switch r.Op {
	case opExists:
		return exists
	case opNotExists:
		return !exists
	case opEquals:
		return exists && value == r.Values[0]
	case opNotEquals:
		return !exists || value != r.Values[0]
	case opIn:
		if !exists {
			return false
		}
		for _, v := range r.Values {
			if v == value {
				return true
			}
		}
		return false
	case opNotIn:
		if !exists {
			return true
		}
		for _, v := range r.Values {
			if v == value {
				return false
			}
		}
		return true
	default:
		return false
	}
}
