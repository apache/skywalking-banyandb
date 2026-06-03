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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

// RenderQL converts a trace QueryRequest proto to BanyanDB QL.
func RenderQL(req *tracev1.QueryRequest) (string, error) {
	parts := []string{fmt.Sprintf("SELECT %s FROM TRACE %s IN %s", renderProjection(req), req.GetName(), strings.Join(req.GetGroups(), ", ")), "TIME > '-15m'"}
	if req.GetCriteria() != nil {
		whereClause, whereErr := renderCriteria(req.GetCriteria())
		if whereErr != nil {
			return "", whereErr
		}
		parts = append(parts, "WHERE "+whereClause)
	}
	if req.GetOrderBy() != nil {
		direction := "ASC"
		if req.GetOrderBy().GetSort() == modelv1.Sort_SORT_DESC {
			direction = "DESC"
		}
		parts = append(parts, fmt.Sprintf("ORDER BY %s %s", req.GetOrderBy().GetIndexRuleName(), direction))
	}
	if req.GetLimit() > 0 {
		parts = append(parts, fmt.Sprintf("LIMIT %d", req.GetLimit()))
	}
	if req.GetOffset() > 0 {
		parts = append(parts, fmt.Sprintf("OFFSET %d", req.GetOffset()))
	}
	return strings.Join(parts, "\n"), nil
}

func renderProjection(req *tracev1.QueryRequest) string {
	if len(req.GetTagProjection()) == 0 {
		return "()"
	}
	return strings.Join(req.GetTagProjection(), ", ")
}

// Criteria rendering is copied from test/cases/measure/cmd/generate/ql_renderer.go — keep modelv1 syntax in sync.
func renderCriteria(criteria *modelv1.Criteria) (string, error) {
	switch criteriaExp := criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		return renderCondition(criteriaExp.Condition), nil
	case *modelv1.Criteria_Le:
		return renderLogicalExpr(criteriaExp.Le)
	default:
		return "", fmt.Errorf("unknown criteria type")
	}
}

func renderCondition(cond *modelv1.Condition) string {
	valueStr := renderTagValue(cond.GetValue())
	if cond.GetOp() == modelv1.Condition_BINARY_OP_MATCH || cond.GetOp() == modelv1.Condition_BINARY_OP_HAVING || cond.GetOp() == modelv1.Condition_BINARY_OP_NOT_HAVING {
		if !strings.HasPrefix(valueStr, "(") {
			valueStr = fmt.Sprintf("(%s)", valueStr)
		}
	}
	return fmt.Sprintf("%s %s %s", cond.GetName(), renderBinaryOp(cond.GetOp()), valueStr)
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
	switch typedValue := tv.GetValue().(type) {
	case *modelv1.TagValue_Str:
		return fmt.Sprintf("'%s'", typedValue.Str.GetValue())
	case *modelv1.TagValue_Int:
		return fmt.Sprintf("%d", typedValue.Int.GetValue())
	case *modelv1.TagValue_StrArray:
		values := make([]string, 0, len(typedValue.StrArray.GetValue()))
		for _, strVal := range typedValue.StrArray.GetValue() {
			values = append(values, fmt.Sprintf("'%s'", strVal))
		}
		return "(" + strings.Join(values, ", ") + ")"
	case *modelv1.TagValue_IntArray:
		values := make([]string, 0, len(typedValue.IntArray.GetValue()))
		for _, intVal := range typedValue.IntArray.GetValue() {
			values = append(values, fmt.Sprintf("%d", intVal))
		}
		return "(" + strings.Join(values, ", ") + ")"
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
