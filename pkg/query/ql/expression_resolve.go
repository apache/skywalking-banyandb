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
	"errors"
	"fmt"
	"slices"
)

// ErrInvalidExpression identifies an invalid expression or an expression value error.
var ErrInvalidExpression = errors.New("invalid QL expression")

func expressionError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidExpression, fmt.Sprintf(format, args...))
}

// ResolveExpression validates and deep-copies an expression, assigning every node a type.
// It never evaluates values: invalid casts in an unreachable branch remain unreachable.
func ResolveExpression(expression *Expression) (*Expression, error) {
	if expression == nil {
		return nil, expressionError("expression is required")
	}
	resolved := *expression
	resolved.Literal.Bytes = slices.Clone(expression.Literal.Bytes)
	resolved.Literal.Ints = slices.Clone(expression.Literal.Ints)
	resolved.Literal.Strings = slices.Clone(expression.Literal.Strings)
	resolved.Args = make([]*Expression, len(expression.Args))
	for index, arg := range expression.Args {
		child, err := ResolveExpression(arg)
		if err != nil {
			return nil, err
		}
		resolved.Args[index] = child
	}
	if err := resolveExpressionType(&resolved); err != nil {
		return nil, err
	}
	return &resolved, nil
}

// ResolvePredicate validates and deep-copies a boolean expression.
// Untyped NULL becomes BOOLEAN NULL without evaluating its value.
func ResolvePredicate(expression *Expression) (*Expression, error) {
	resolved, err := ResolveExpression(expression)
	if err != nil {
		return nil, err
	}
	switch resolved.Type {
	case ExpressionBool:
		return resolved, nil
	case ExpressionNull:
		return &Expression{Op: "cast", Type: ExpressionBool, Args: []*Expression{resolved}}, nil
	default:
		return nil, expressionError("filter predicate must be BOOLEAN")
	}
}

func resolveExpressionType(expression *Expression) error {
	switch expression.Op {
	case "literal":
		if expression.Type > ExpressionStringArray {
			return expressionError("unknown literal type")
		}
	case "column":
		if expression.Type != ExpressionTimestamp {
			expression.Type = expressionColumnType(expression.Source)
		}
		if expression.Type == ExpressionNull {
			return expressionError("source column has no logical type")
		}
	case "+", "-", "*", "/", "%":
		if len(expression.Args) != 2 {
			return expressionError("%s requires two arguments", expression.Op)
		}
		typ, err := commonExpressionType(expression.Args)
		if err != nil || (typ != ExpressionInt && typ != ExpressionFloat && typ != ExpressionNull) {
			return expressionError("%s requires numeric arguments", expression.Op)
		}
		if typ == ExpressionNull {
			typ = ExpressionInt
		}
		expression.Type = typ
		coerceExpressionArgs(expression.Args, typ)
	case "cast":
		if len(expression.Args) != 1 || !expressionCastAllowed(expression.Args[0].Type, expression.Type) {
			return expressionError("unsupported cast")
		}
	case "negate", "positive":
		return resolveExpressionSign(expression)
	case "=", "!=", "<", "<=", ">", ">=", "in", "not_in":
		return resolveExpressionComparison(expression)
	case "contains", "not_contains":
		return resolveExpressionContains(expression)
	case "and", "or", "not":
		count := 2
		if expression.Op == "not" {
			count = 1
		}
		if len(expression.Args) != count {
			return expressionError("%s requires %d arguments", expression.Op, count)
		}
		for _, arg := range expression.Args {
			if arg.Type != ExpressionBool && arg.Type != ExpressionNull {
				return expressionError("%s requires BOOLEAN arguments", expression.Op)
			}
		}
		coerceExpressionArgs(expression.Args, ExpressionBool)
		expression.Type = ExpressionBool
	case "is_null", "is_not_null":
		return resolveExpressionNullTest(expression)
	case "case", "simple_case", "coalesce":
		return resolveExpressionConditional(expression)
	default:
		return expressionError("unknown operator %q", expression.Op)
	}
	return nil
}

func resolveExpressionSign(expression *Expression) error {
	if len(expression.Args) != 1 {
		return expressionError("unary sign requires one argument")
	}
	typ := expression.Args[0].Type
	if typ == ExpressionNull {
		typ = ExpressionInt
		coerceExpressionArgs(expression.Args, typ)
	}
	if typ != ExpressionInt && typ != ExpressionFloat {
		return expressionError("unary sign requires a numeric argument")
	}
	expression.Type = typ
	return nil
}

func resolveExpressionNullTest(expression *Expression) error {
	if len(expression.Args) != 1 {
		return expressionError("%s requires one argument", expression.Op)
	}
	expression.Type = ExpressionBool
	return nil
}

func expressionCastAllowed(from, to ExpressionType) bool {
	if to == ExpressionNull || to > ExpressionStringArray {
		return false
	}
	if from == ExpressionNull || from == to {
		return true
	}
	switch to {
	case ExpressionInt, ExpressionFloat, ExpressionBool:
		return from == ExpressionInt || from == ExpressionFloat || from == ExpressionBool || from == ExpressionString || (to == ExpressionInt && from == ExpressionTimestamp)
	case ExpressionString:
		return from == ExpressionInt || from == ExpressionFloat || from == ExpressionBool || from == ExpressionTimestamp
	case ExpressionTimestamp:
		return from == ExpressionInt || from == ExpressionString
	default:
		return false
	}
}

func resolveExpressionComparison(expression *Expression) error {
	minimum := 2
	if expression.Op == "in" || expression.Op == "not_in" {
		minimum = 1
	}
	if len(expression.Args) < minimum || (minimum == 2 && len(expression.Args) != 2) {
		return expressionError("invalid %s arguments", expression.Op)
	}
	typ, err := commonExpressionType(expression.Args)
	if err != nil {
		return err
	}
	if typ == ExpressionIntArray || typ == ExpressionStringArray {
		return expressionError("array comparison requires a scalar operand")
	}
	if typ == ExpressionBool && expression.Op != "=" && expression.Op != "!=" && expression.Op != "in" && expression.Op != "not_in" {
		return expressionError("ordered BOOLEAN comparison is unsupported")
	}
	coerceExpressionArgs(expression.Args, typ)
	expression.Type = ExpressionBool
	return nil
}

func resolveExpressionConditional(expression *Expression) error {
	if len(expression.Args) == 0 {
		return expressionError("%s requires arguments", expression.Op)
	}
	if expression.Op == "coalesce" {
		typ, err := commonExpressionType(expression.Args)
		if err != nil {
			return err
		}
		expression.Type = typ
		coerceExpressionArgs(expression.Args, typ)
		return nil
	}
	start := 0
	if expression.Op == "simple_case" {
		start = 1
	}
	if len(expression.Args) < start+3 || (len(expression.Args)-start)%2 != 1 {
		return expressionError("CASE requires WHEN/value pairs and ELSE")
	}
	values := make([]*Expression, 0, (len(expression.Args)-start+1)/2)
	conditions := make([]*Expression, 0, len(expression.Args)/2+1)
	if start == 1 {
		conditions = append(conditions, expression.Args[0])
	}
	for index := start; index < len(expression.Args)-1; index += 2 {
		condition := expression.Args[index]
		if start == 0 {
			if condition.Type != ExpressionBool && condition.Type != ExpressionNull {
				return expressionError("CASE WHEN requires BOOLEAN")
			}
			if condition.Type == ExpressionNull {
				condition.Type = ExpressionBool
				condition.Literal.Null = true
			}
		} else {
			conditions = append(conditions, condition)
		}
		values = append(values, expression.Args[index+1])
	}
	if start == 1 {
		typ, err := commonExpressionType(conditions)
		if err != nil {
			return err
		}
		if typ == ExpressionIntArray || typ == ExpressionStringArray {
			return expressionError("simple CASE requires scalar comparisons")
		}
		coerceExpressionArgs(conditions, typ)
		expression.Args[0] = conditions[0]
		for index := 1; index < len(conditions); index++ {
			expression.Args[2*index-1] = conditions[index]
		}
	}
	values = append(values, expression.Args[len(expression.Args)-1])
	typ, err := commonExpressionType(values)
	if err != nil {
		return err
	}
	coerceExpressionArgs(values, typ)
	for index := start; index < len(expression.Args)-1; index += 2 {
		expression.Args[index+1] = values[(index-start)/2]
	}
	expression.Args[len(expression.Args)-1] = values[len(values)-1]
	expression.Type = typ
	return nil
}

func commonExpressionType(args []*Expression) (ExpressionType, error) {
	typ := ExpressionNull
	for _, arg := range args {
		if arg.Type == ExpressionNull || arg.Type == typ {
			continue
		}
		if typ == ExpressionNull {
			typ = arg.Type
			continue
		}
		if (typ == ExpressionInt && arg.Type == ExpressionFloat) || (typ == ExpressionFloat && arg.Type == ExpressionInt) {
			typ = ExpressionFloat
			continue
		}
		return ExpressionNull, expressionError("incompatible expression types %d and %d", typ, arg.Type)
	}
	return typ, nil
}

func coerceExpressionArgs(args []*Expression, typ ExpressionType) {
	for index, arg := range args {
		if arg.Type == typ {
			continue
		}
		if arg.Type == ExpressionNull && arg.Op == "literal" {
			arg.Type, arg.Literal.Null = typ, true
		} else {
			args[index] = &Expression{Op: "cast", Type: typ, Args: []*Expression{arg}}
		}
	}
}
