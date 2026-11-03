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

import (
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

// necessaryFilterValue preserves native index pruning for compact bound lists
// without constructing an intermediate protobuf scalar for every list element.
func necessaryFilterValue(expression *ql.Expression, typ ql.ExpressionType) *modelv1.TagValue {
	count := 0
	for _, argument := range expression.Args[1:] {
		if argument.Op != "literal" {
			return nil
		}
		if argument.Literal.Null {
			continue
		}
		switch {
		case argument.Type == typ:
			count++
		case expression.Op == "in" && typ == ql.ExpressionInt && argument.Type == ql.ExpressionIntArray:
			count += len(argument.Literal.Ints)
		case expression.Op == "in" && typ == ql.ExpressionString && argument.Type == ql.ExpressionStringArray:
			count += len(argument.Literal.Strings)
		default:
			return nil
		}
	}
	if count == 0 {
		return nil
	}
	if expression.Op == "=" {
		literal := expression.Args[1].Literal
		if typ == ql.ExpressionInt {
			return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: literal.Int64}}}
		}
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: literal.String}}}
	}
	if typ == ql.ExpressionInt {
		values := make([]int64, 0, count)
		for _, argument := range expression.Args[1:] {
			if argument.Literal.Null {
				continue
			}
			if argument.Type == ql.ExpressionIntArray {
				values = append(values, argument.Literal.Ints...)
			} else {
				values = append(values, argument.Literal.Int64)
			}
		}
		return &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: values}}}
	}
	values := make([]string, 0, count)
	for _, argument := range expression.Args[1:] {
		if argument.Literal.Null {
			continue
		}
		if argument.Type == ql.ExpressionStringArray {
			values = append(values, argument.Literal.Strings...)
		} else {
			values = append(values, argument.Literal.String)
		}
	}
	return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: values}}}
}
