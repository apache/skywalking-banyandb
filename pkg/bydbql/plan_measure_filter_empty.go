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

package bydbql

import "github.com/apache/skywalking-banyandb/pkg/query/ql"

// filterRejectsAllRows runs only after complete binding and type validation.
// A rejecting conjunct cannot suppress a row-dependent error in its siblings.
// Keep the proof conservative: no arithmetic, conversions or conditional arms.
func filterRejectsAllRows(expression *ql.Expression) bool {
	return filterCannotFail(expression) && filterCannotSelect(expression)
}

func filterCannotFail(expression *ql.Expression) bool {
	switch expression.Op {
	case "column", "literal", "=", "!=", "<", "<=", ">", ">=", "in", "not_in", "and", "or", "not", "is_null", "is_not_null":
	default:
		return false
	}
	for _, child := range expression.Args {
		if !filterCannotFail(child) {
			return false
		}
	}
	return true
}

func filterCannotSelect(expression *ql.Expression) bool {
	switch expression.Op {
	case "and":
		return filterCannotSelect(expression.Args[0]) || filterCannotSelect(expression.Args[1])
	case "or":
		return filterCannotSelect(expression.Args[0]) && filterCannotSelect(expression.Args[1])
	case "literal":
		return expression.Literal.Null || expression.Type == ql.ExpressionBool && !expression.Literal.Bool
	case "not_in":
		// A matching value yields FALSE; otherwise the explicit NULL yields
		// UNKNOWN. Empty NOT IN has no NULL and must still select every row.
		for _, argument := range expression.Args[1:] {
			if argument.Op == "literal" && argument.Literal.Null {
				return true
			}
		}
		return false
	case "in":
		// Empty IN is FALSE, and IN containing only NULL is UNKNOWN. Neither
		// can select a row.
		for _, argument := range expression.Args[1:] {
			if argument.Op != "literal" {
				return false
			}
			if argument.Literal.Null {
				continue
			}
			switch argument.Type {
			case ql.ExpressionIntArray:
				if len(argument.Literal.Ints) != 0 {
					return false
				}
			case ql.ExpressionStringArray:
				if len(argument.Literal.Strings) != 0 {
					return false
				}
			default:
				return false
			}
		}
		return true
	default:
		return false
	}
}
