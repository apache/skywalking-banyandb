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

package ql

import (
	"context"
	"math"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Constant string membership needs one lookup per active row. Numeric lists keep
// the comparison kernel's widening and decimal rules; nonliteral RHS expressions
// retain its evaluation order and error behavior.
func expressionStringMembership(expression *Expression) bool {
	if (expression.Op != "in" && expression.Op != "not_in") || len(expression.Args) < 2 || expression.Args[0].Type != ExpressionString {
		return false
	}
	for _, argument := range expression.Args[1:] {
		if argument.Op != "literal" || (argument.Type != ExpressionString && argument.Type != ExpressionStringArray) {
			return false
		}
	}
	return true
}

type expressionStringSet struct {
	lookup    map[string]struct{}
	arguments []*Expression
	values    []string
	hasNull   bool
	ready     bool
}

func (s *expressionStringSet) prepare(ctx context.Context, operator *expressionOperator) error {
	if s.ready {
		return nil
	}
	count := int64(0)
	for index, argument := range s.arguments {
		if index%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if argument.Literal.Null {
			s.hasNull = true
			continue
		}
		length := int64(1)
		if argument.Type == ExpressionStringArray {
			length = int64(len(argument.Literal.Strings))
		}
		if length > (math.MaxInt64-256)/64-count {
			return expressionError("membership size overflows")
		}
		count += length
	}
	// Tiny lists avoid a hash table. The larger bound covers keys, bucket slack,
	// control bytes and directory growth; string payloads remain borrowed from
	// this execution's immutable literals. Reserve before allocating either form.
	bytes := count*16 + 128
	if count > 16 {
		bytes = count*64 + 256
	}
	if err := operator.reserve(bytes); err != nil {
		return err
	}
	if count > 16 {
		s.lookup = make(map[string]struct{}, count)
	} else {
		s.values = make([]string, 0, count)
	}
	visited := 0
	add := func(value string) error {
		if visited%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		visited++
		if s.lookup != nil {
			s.lookup[value] = struct{}{}
		} else {
			s.values = append(s.values, value)
		}
		return nil
	}
	for _, argument := range s.arguments {
		if argument.Literal.Null {
			continue
		}
		if argument.Type == ExpressionStringArray {
			for _, value := range argument.Literal.Strings {
				if err := add(value); err != nil {
					return err
				}
			}
		} else if err := add(argument.Literal.String); err != nil {
			return err
		}
	}
	s.arguments = nil
	s.ready = true
	return nil
}

func (s *expressionStringSet) contains(value string) bool {
	if s.lookup != nil {
		_, found := s.lookup[value]
		return found
	}
	for _, candidate := range s.values {
		if value == candidate {
			return true
		}
	}
	return false
}

func (n *expressionNode) stringMembership(arguments []*Expression, not bool) func(context.Context, *vectorized.RecordBatch, []uint16) (*expressionVector, error) {
	set := &expressionStringSet{arguments: arguments}
	return func(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16) (*expressionVector, error) {
		left, err := n.args[0].eval(ctx, batch, mask)
		if err != nil {
			return nil, err
		}
		if err = set.prepare(ctx, n.op); err != nil {
			return nil, err
		}
		if len(set.values) == 0 && set.lookup == nil && !set.hasNull {
			n.value.constant = true
			n.value.literal = ExpressionScalar{Bool: not}
			return &n.value, nil
		}
		if err = n.buffer(batch.Len); err != nil {
			return nil, err
		}
		for pos := 0; pos < expressionRows(batch, mask); pos++ {
			if pos%1024 == 0 {
				if err = ctx.Err(); err != nil {
					return nil, err
				}
			}
			row := expressionRow(mask, pos)
			found := !left.isNull(row) && set.contains(left.stringAt(row))
			n.value.bools[row] = found != not
			n.value.nulls[row] = left.isNull(row) || (!found && set.hasNull)
		}
		return &n.value, nil
	}
}
