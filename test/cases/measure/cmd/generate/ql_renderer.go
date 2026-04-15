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

package main

import (
	"fmt"
	"strings"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// RenderQL converts a QueryRequest proto to a BanyanDB QL string.
func RenderQL(req *measurev1.QueryRequest) (string, error) {
	var parts []string

	// SELECT projection
	projection := renderProjection(req)
	// TIME — always required by the BydbQL service; actual range is injected by the test framework
	parts = append(parts,
		fmt.Sprintf("SELECT %s FROM MEASURE %s IN %s", projection, req.GetName(), strings.Join(req.GetGroups(), ",")),
		"TIME > '-15m'",
	)

	// WHERE
	if req.GetCriteria() != nil {
		whereClause, whereErr := renderCriteria(req.GetCriteria())
		if whereErr != nil {
			return "", whereErr
		}
		parts = append(parts, "WHERE "+whereClause)
	}

	// GROUP BY
	if req.GetGroupBy() != nil {
		var groupCols []string
		if tp := req.GetGroupBy().GetTagProjection(); tp != nil {
			for _, family := range tp.GetTagFamilies() {
				groupCols = append(groupCols, family.GetTags()...)
			}
		}
		if fn := req.GetGroupBy().GetFieldName(); fn != "" {
			groupCols = append(groupCols, fn+"::field")
		}
		if len(groupCols) > 0 {
			parts = append(parts, "GROUP BY "+strings.Join(groupCols, ", "))
		}
	}

	// ORDER BY
	if req.GetOrderBy() != nil {
		switch req.GetOrderBy().GetSort() {
		case modelv1.Sort_SORT_ASC:
			parts = append(parts, "ORDER BY ASC")
		case modelv1.Sort_SORT_DESC:
			parts = append(parts, "ORDER BY DESC")
		}
	}

	// LIMIT / OFFSET
	if req.GetLimit() > 0 {
		parts = append(parts, fmt.Sprintf("LIMIT %d", req.GetLimit()))
	}
	if req.GetOffset() > 0 {
		parts = append(parts, fmt.Sprintf("OFFSET %d", req.GetOffset()))
	}

	return strings.Join(parts, " "), nil
}

func renderProjection(req *measurev1.QueryRequest) string {
	var cols []string

	// TOP N in projection
	if top := req.GetTop(); top != nil {
		sortStr := "DESC"
		if top.GetFieldValueSort() == modelv1.Sort_SORT_ASC {
			sortStr = "ASC"
		}
		cols = append(cols, fmt.Sprintf("TOP %d %s %s", top.GetNumber(), top.GetFieldName(), sortStr))
	}

	// Tag projection
	if tp := req.GetTagProjection(); tp != nil {
		for _, family := range tp.GetTagFamilies() {
			cols = append(cols, family.GetTags()...)
		}
	}

	// Aggregation in projection
	if agg := req.GetAgg(); agg != nil {
		aggFn := aggFunctionName(agg.GetFunction())
		fieldName := agg.GetFieldName()
		cols = append(cols, fmt.Sprintf("%s(%s)", aggFn, fieldName))
	}

	// Field projection — all fields need the ::field suffix to disambiguate from tags
	if fp := req.GetFieldProjection(); fp != nil {
		for _, fieldName := range fp.GetNames() {
			cols = append(cols, fieldName+"::field")
		}
	}

	if len(cols) == 0 {
		return "*"
	}
	return strings.Join(cols, ", ")
}

func aggFunctionName(fn modelv1.AggregationFunction) string {
	switch fn {
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN:
		return "MEAN"
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX:
		return "MAX"
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN:
		return "MIN"
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT:
		return "COUNT"
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM:
		return "SUM"
	default:
		return "SUM"
	}
}

func renderCriteria(criteria *modelv1.Criteria) (string, error) {
	switch c := criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		return renderCondition(c.Condition), nil
	case *modelv1.Criteria_Le:
		return renderLogicalExpr(c.Le)
	default:
		return "", fmt.Errorf("unknown criteria type")
	}
}

func renderCondition(cond *modelv1.Condition) string {
	name := cond.GetName()
	op := cond.GetOp()
	val := cond.GetValue()
	valueStr := renderTagValue(val)
	// MATCH and HAVING operations require parentheses around values (even single strings)
	if op == modelv1.Condition_BINARY_OP_MATCH || op == modelv1.Condition_BINARY_OP_HAVING ||
		op == modelv1.Condition_BINARY_OP_NOT_HAVING {
		// If not already wrapped in parentheses (arrays are), wrap it
		if !strings.HasPrefix(valueStr, "(") {
			valueStr = fmt.Sprintf("(%s)", valueStr)
		}
	}
	return fmt.Sprintf("%s %s %s", name, renderBinaryOp(op), valueStr)
}

func renderBinaryOp(op modelv1.Condition_BinaryOp) string {
	switch op {
	case modelv1.Condition_BINARY_OP_EQ:
		return "="
	case modelv1.Condition_BINARY_OP_NE:
		return "!="
	case modelv1.Condition_BINARY_OP_LT:
		return "<"
	case modelv1.Condition_BINARY_OP_GT:
		return ">"
	case modelv1.Condition_BINARY_OP_LE:
		return "<="
	case modelv1.Condition_BINARY_OP_GE:
		return ">="
	case modelv1.Condition_BINARY_OP_IN:
		return "IN"
	case modelv1.Condition_BINARY_OP_NOT_IN:
		return "NOT IN"
	case modelv1.Condition_BINARY_OP_MATCH:
		return "MATCH"
	case modelv1.Condition_BINARY_OP_HAVING:
		return "HAVING"
	case modelv1.Condition_BINARY_OP_NOT_HAVING:
		return "NOT HAVING"
	default:
		return "="
	}
}

func renderTagValue(tv *modelv1.TagValue) string {
	if tv == nil {
		return "NULL"
	}
	switch val := tv.GetValue().(type) {
	case *modelv1.TagValue_Str:
		return fmt.Sprintf("'%s'", val.Str.GetValue())
	case *modelv1.TagValue_Int:
		return fmt.Sprintf("%d", val.Int.GetValue())
	case *modelv1.TagValue_StrArray:
		strs := make([]string, 0, len(val.StrArray.GetValue()))
		for _, strVal := range val.StrArray.GetValue() {
			strs = append(strs, fmt.Sprintf("'%s'", strVal))
		}
		return "(" + strings.Join(strs, ", ") + ")"
	case *modelv1.TagValue_IntArray:
		intStrs := make([]string, 0, len(val.IntArray.GetValue()))
		for _, intVal := range val.IntArray.GetValue() {
			intStrs = append(intStrs, fmt.Sprintf("%d", intVal))
		}
		return "(" + strings.Join(intStrs, ", ") + ")"
	case *modelv1.TagValue_Null:
		return "NULL"
	default:
		return "NULL"
	}
}

func renderLogicalExpr(le *modelv1.LogicalExpression) (string, error) {
	opStr := "AND"
	if le.GetOp() == modelv1.LogicalExpression_LOGICAL_OP_OR {
		opStr = "OR"
	}

	leftStr, leftErr := renderCriteria(le.GetLeft())
	if leftErr != nil {
		return "", leftErr
	}
	rightStr, rightErr := renderCriteria(le.GetRight())
	if rightErr != nil {
		return "", rightErr
	}

	return fmt.Sprintf("(%s %s %s)", leftStr, opStr, rightStr), nil
}
