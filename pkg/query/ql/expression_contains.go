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

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func resolveExpressionContains(expression *Expression) error {
	if len(expression.Args) != 2 {
		return expressionError("%s requires a source and one scalar or array value", expression.Op)
	}
	domain := ExpressionNull
	for _, arg := range expression.Args {
		typ := arg.Type
		switch typ {
		case ExpressionNull:
			continue
		case ExpressionIntArray:
			typ = ExpressionInt
		case ExpressionStringArray:
			typ = ExpressionString
		case ExpressionInt, ExpressionString:
		default:
			return expressionError("%s requires INT or STRING scalar/array operands", expression.Op)
		}
		if domain != ExpressionNull && domain != typ {
			return expressionError("%s requires matching operand types", expression.Op)
		}
		domain = typ
	}
	expression.Type = ExpressionBool
	return nil
}

type expressionContainsValue func(context.Context, *expressionVector, *expressionVector, int) (bool, error)

func expressionContainsMatcher(left, right ExpressionType) expressionContainsValue {
	// Measure criteria uses native MatchAllTerms: scalar values and array
	// elements compare by value, and every requested term must be present.
	noMatch := func(context.Context, *expressionVector, *expressionVector, int) (bool, error) { return false, nil }
	if left == ExpressionNull || right == ExpressionNull {
		return noMatch
	}
	switch left {
	case ExpressionInt:
		if right == ExpressionInt {
			return func(_ context.Context, a, b *expressionVector, row int) (bool, error) {
				return a.intAt(row) == b.intAt(row), nil
			}
		}
		return func(ctx context.Context, a, b *expressionVector, row int) (bool, error) {
			return containsExpressionMembers(ctx, []int64{a.intAt(row)}, b.intArrayAt(row))
		}
	case ExpressionString:
		if right == ExpressionString {
			return func(_ context.Context, a, b *expressionVector, row int) (bool, error) {
				return a.stringAt(row) == b.stringAt(row), nil
			}
		}
		return func(ctx context.Context, a, b *expressionVector, row int) (bool, error) {
			return containsExpressionMembers(ctx, []string{a.stringAt(row)}, b.stringArrayAt(row))
		}
	case ExpressionIntArray:
		if right == ExpressionInt {
			return func(ctx context.Context, a, b *expressionVector, row int) (bool, error) {
				return containsExpressionMember(ctx, a.intArrayAt(row), b.intAt(row))
			}
		}
		return func(ctx context.Context, a, b *expressionVector, row int) (bool, error) {
			return containsExpressionMembers(ctx, a.intArrayAt(row), b.intArrayAt(row))
		}
	case ExpressionStringArray:
		if right == ExpressionString {
			return func(ctx context.Context, a, b *expressionVector, row int) (bool, error) {
				return containsExpressionMember(ctx, a.stringArrayAt(row), b.stringAt(row))
			}
		}
		return func(ctx context.Context, a, b *expressionVector, row int) (bool, error) {
			return containsExpressionMembers(ctx, a.stringArrayAt(row), b.stringArrayAt(row))
		}
	default:
	}
	return noMatch
}

func (v *expressionVector) intArrayAt(row int) []int64 {
	if v.constant {
		return v.literal.Ints
	}
	return v.intArrays[row]
}

func (v *expressionVector) stringArrayAt(row int) []string {
	if v.constant {
		return v.literal.Strings
	}
	return v.stringArrays[row]
}

func containsExpressionMember[T comparable](ctx context.Context, values []T, required T) (bool, error) {
	for index, value := range values {
		if index > 0 && index%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return false, err
			}
		}
		if value == required {
			return true, nil
		}
	}
	return false, nil
}

func containsExpressionMembers[T comparable](ctx context.Context, values, required []T) (bool, error) {
	// Native FilterTermsSet treats an empty requested term set as no matches.
	if len(required) == 0 {
		return false, nil
	}
	for index, value := range required {
		if index%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return false, err
			}
		}
		present, err := containsExpressionMember(ctx, values, value)
		if err != nil || !present {
			return false, err
		}
	}
	return true, nil
}

func (n *expressionNode) contains(not bool, leftType, rightType ExpressionType) func(context.Context, *vectorized.RecordBatch, []uint16) (*expressionVector, error) {
	match := expressionContainsMatcher(leftType, rightType)
	return func(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16) (*expressionVector, error) {
		left, err := n.args[0].eval(ctx, batch, mask)
		if err != nil {
			return nil, err
		}
		right, err := n.args[1].eval(ctx, batch, mask)
		if err != nil {
			return nil, err
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
			present := false
			// Measure collection membership is two-valued: NULL never contains
			// a value, and NOT CONTAINS is the boolean complement, including NULL.
			if !left.isNull(row) && !right.isNull(row) {
				present, err = match(ctx, left, right, row)
				if err != nil {
					return nil, err
				}
			}
			n.value.bools[row] = present != not
		}
		return &n.value, nil
	}
}
