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

package bydbql

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type measureExpression struct {
	args       []*measureExpression
	value      ql.Expression
	parameter  bool
	paramIndex int
}

type measureExpressionOutput struct {
	expression *measureExpression
	column     vectorized.ColumnDef
	computed   bool
}

func hasMeasureExpressions(projection *GrammarProjection) bool {
	if projection == nil {
		return false
	}
	for _, column := range projection.Columns {
		if column.Expression != nil {
			return true
		}
	}
	return false
}

func (m *MeasureTemplate) compileExpressions(schemas map[string]*databasev1.Measure) error {
	stmt := m.statement.template.Select
	if stmt.GroupBy != nil || stmt.Projection.TopN != nil || measureAggregateCount(stmt) != 0 {
		return fmt.Errorf("SELECT expressions with aggregation, grouping or TOP are not supported")
	}
	columns, err := resolveMeasureColumns(stmt.From.In.Groups, schemas)
	if err != nil {
		return err
	}
	inputs := make(map[measureColumnKey]bool)
	for i, column := range stmt.Projection.Columns {
		expr := column.Expression
		computed := expr != nil
		if isQuotedMeasureLiteral(column, columns) {
			expr = &GrammarExpression{Op: "literal", Type: ql.ExpressionString, Literal: ql.ExpressionScalar{String: *column.Identifier.QuotedIdent}}
			computed = true
		}
		if expr == nil {
			if column.Identifier == nil {
				return fmt.Errorf("invalid SELECT expression")
			}
			if column.TypeSpec == nil && isBuiltinTime(column.Identifier) && column.Alias == nil {
				continue
			}
			expr = &GrammarExpression{Op: "column", Identifier: column.Identifier, TypeSpec: column.TypeSpec}
		}
		recipe, compileErr := m.compileExpression(expr, columns, inputs)
		if compileErr != nil {
			return compileErr
		}
		name := fmt.Sprintf("expr_%d", i+1)
		output := vectorized.ColumnDef{Name: name, Role: vectorized.RoleField}
		if !computed {
			output = recipe.value.Source
		}
		if column.Alias != nil {
			output.Name, err = column.Alias.ToString(true)
			if err != nil {
				return err
			}
		}
		m.expressions = append(m.expressions, measureExpressionOutput{expression: recipe, column: output, computed: computed})
	}
	if !hasNativeExpressionInput(m.slots) {
		if len(columns.ordered) == 0 {
			return fmt.Errorf("expression projection requires a native source column")
		}
		anchor := columns.ordered[0]
		for _, candidate := range columns.ordered {
			if candidate.Role == vectorized.RoleTag && candidate.Entity {
				anchor = candidate
				break
			}
		}
		m.addExpressionInput(anchor, inputs)
	}
	m.request = baseMeasureRequest(stmt)
	m.request.TagProjection, m.request.FieldProjection = rawScanProjection(m.slots)
	m.compileNativeEntities(schemas, columns)
	var orderSlots []ql.Slot
	for _, output := range m.expressions {
		if output.computed && stmt.OrderBy != nil && stmt.OrderBy.Tail.WithIdent != nil {
			name, nameErr := stmt.OrderBy.Tail.WithIdent.Identifier.ToString(true)
			if nameErr != nil {
				return nameErr
			}
			if name == output.column.Name {
				return fmt.Errorf("ORDER BY expression %q requires column sorting", name)
			}
		}
		if !output.computed {
			orderSlots = append(orderSlots, ql.Slot{Source: output.expression.value.Source, Output: output.column})
		}
	}
	m.request.OrderBy, err = rawOrderBy(stmt.OrderBy, orderSlots, columns)
	if err != nil {
		return err
	}
	if stmt.Where != nil {
		m.predicate, err = compileMeasureOr(stmt.Where.Expr, columns)
	}
	return err
}

func hasNativeExpressionInput(slots []ql.Slot) bool {
	for _, slot := range slots {
		if slot.Source.Role != vectorized.RoleTimestamp {
			return true
		}
	}
	return false
}

func (m *MeasureTemplate) addExpressionInput(column vectorized.ColumnDef, seen map[measureColumnKey]bool) {
	key := measureColumnKey{family: column.TagFamily, name: column.Name, role: column.Role}
	if seen[key] {
		return
	}
	seen[key] = true
	m.slots = append(m.slots, ql.Slot{ID: ql.SlotID(len(m.slots)), Source: column, Output: column, Nullable: true})
}

func (m *MeasureTemplate) compileExpression(expr *GrammarExpression, columns *measureColumns, inputs map[measureColumnKey]bool) (*measureExpression, error) {
	recipe := &measureExpression{value: ql.Expression{Op: expr.Op, Type: expr.Type, Literal: expr.Literal}, parameter: expr.Param, paramIndex: expr.ParamIndex}
	if expr.Op == "column" {
		var source vectorized.ColumnDef
		var err error
		if expr.TypeSpec == nil && isBuiltinTime(expr.Identifier) {
			source = vectorized.ColumnDef{Name: "time", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64}
		} else {
			var name string
			name, err = expr.Identifier.ToString(expr.TypeSpec != nil)
			if err == nil {
				source, err = columns.resolve(name, expr.TypeSpec)
			}
		}
		if err != nil {
			return nil, err
		}
		recipe.value.Source = source
		if tag := columns.tagSpecs[source.TagFamily+"."+source.Name]; tag != nil && tag.tag.Type == databasev1.TagType_TAG_TYPE_TIMESTAMP {
			recipe.value.Type = ql.ExpressionTimestamp
			recipe.value.Source.Type = vectorized.ColumnTypeInt64
		}
		m.addExpressionInput(source, inputs)
	}
	for _, child := range expr.Args {
		compiled, err := m.compileExpression(child, columns, inputs)
		if err != nil {
			return nil, err
		}
		recipe.args = append(recipe.args, compiled)
	}
	return recipe, nil
}

func (m *MeasureTemplate) bindExpressions(bound *BoundQuery) (*ql.ExpressionPlan, error) {
	if m.expressions == nil {
		return nil, nil
	}
	plan := &ql.ExpressionPlan{Outputs: make([]ql.ExpressionOutput, 0, len(m.expressions))}
	seen := make(map[measureColumnKey]bool, len(m.expressions))
	for _, output := range m.expressions {
		expression := output.expression.bind(bound)
		resolved, err := ql.ResolveExpression(expression)
		if err != nil {
			return nil, err
		}
		if resolved.Type == ql.ExpressionBool {
			return nil, fmt.Errorf("boolean projection requires an explicit cast to INT or STRING")
		}
		if resolved.Type == ql.ExpressionNull {
			return nil, fmt.Errorf("NULL projection requires an explicit cast")
		}
		column := output.column
		if output.computed || resolved.Source.Role == vectorized.RoleTimestamp {
			column, err = expressionOutputColumn(column.Name, resolved.Type)
			if err != nil {
				return nil, err
			}
		}
		key := measureColumnKey{family: column.TagFamily, name: column.Name, role: column.Role}
		if seen[key] {
			return nil, fmt.Errorf("duplicate output name %q in container %q", key.name, key.family)
		}
		seen[key] = true
		plan.Outputs = append(plan.Outputs, ql.ExpressionOutput{Expr: resolved, Column: column})
	}
	return plan, nil
}

func (m *measureExpression) bind(bound *BoundQuery) *ql.Expression {
	if m.parameter {
		return cloneExpressionLiteral(bound.values[m.paramIndex].expression)
	}
	value := m.value
	value.Args = make([]*ql.Expression, 0, len(m.args))
	for i, child := range m.args {
		arg := child.bind(bound)
		if i > 0 && (value.Op == "in" || value.Op == "not_in") && arg.Op == "literal" {
			switch arg.Type {
			case ql.ExpressionIntArray:
				for _, element := range arg.Literal.Ints {
					value.Args = append(value.Args, &ql.Expression{Op: "literal", Type: ql.ExpressionInt, Literal: ql.ExpressionScalar{Int64: element}})
				}
				continue
			case ql.ExpressionStringArray:
				for _, element := range arg.Literal.Strings {
					value.Args = append(value.Args, &ql.Expression{Op: "literal", Type: ql.ExpressionString, Literal: ql.ExpressionScalar{String: element}})
				}
				continue
			default:
			}
		}
		value.Args = append(value.Args, arg)
	}
	return &value
}

func cloneExpressionLiteral(expression *ql.Expression) *ql.Expression {
	value := *expression
	value.Literal.Bytes = slices.Clone(value.Literal.Bytes)
	value.Literal.Ints = slices.Clone(value.Literal.Ints)
	value.Literal.Strings = slices.Clone(value.Literal.Strings)
	return &value
}

func expressionOutputColumn(name string, typ ql.ExpressionType) (vectorized.ColumnDef, error) {
	column := vectorized.ColumnDef{Name: name, Role: vectorized.RoleField}
	switch typ {
	case ql.ExpressionInt:
		column.Type = vectorized.ColumnTypeInt64
	case ql.ExpressionFloat:
		column.Type = vectorized.ColumnTypeFloat64
	case ql.ExpressionString:
		column.Type = vectorized.ColumnTypeString
	case ql.ExpressionBytes:
		column.Type = vectorized.ColumnTypeBytes
	case ql.ExpressionTimestamp:
		column.Type, column.Role, column.TagFamily = vectorized.ColumnTypeInt64, vectorized.RoleTag, "__ql"
	case ql.ExpressionIntArray:
		column.Type, column.Role, column.TagFamily = vectorized.ColumnTypeInt64Array, vectorized.RoleTag, "__ql"
	case ql.ExpressionStringArray:
		column.Type, column.Role, column.TagFamily = vectorized.ColumnTypeStrArray, vectorized.RoleTag, "__ql"
	default:
		return column, fmt.Errorf("unsupported expression output type %d", typ)
	}
	return column, nil
}

func resolveExpressionParam(parameter *modelv1.TagValue) (*ql.Expression, error) {
	expression := &ql.Expression{Op: "literal"}
	switch value := parameter.Value.(type) {
	case *modelv1.TagValue_Str:
		expression.Type, expression.Literal.String = ql.ExpressionString, value.Str.GetValue()
	case *modelv1.TagValue_Int:
		expression.Type, expression.Literal.Int64 = ql.ExpressionInt, value.Int.GetValue()
	case *modelv1.TagValue_Null:
		expression.Type, expression.Literal.Null = ql.ExpressionNull, true
	case *modelv1.TagValue_BinaryData:
		expression.Type, expression.Literal.Bytes = ql.ExpressionBytes, slices.Clone(value.BinaryData)
	case *modelv1.TagValue_IntArray:
		expression.Type, expression.Literal.Ints = ql.ExpressionIntArray, slices.Clone(value.IntArray.GetValue())
	case *modelv1.TagValue_StrArray:
		expression.Type, expression.Literal.Strings = ql.ExpressionStringArray, slices.Clone(value.StrArray.GetValue())
	case *modelv1.TagValue_Timestamp:
		timestamp := value.Timestamp
		if err := timestamp.CheckValid(); err != nil {
			return nil, fmt.Errorf("invalid timestamp parameter: %w", err)
		}
		instant := timestamp.AsTime()
		// UnixNano is defined only inside the signed nanosecond domain.
		minimum, maximum := time.Unix(0, math.MinInt64), time.Unix(0, math.MaxInt64)
		if instant.Before(minimum) || instant.After(maximum) {
			return nil, fmt.Errorf("timestamp parameter is outside nanosecond range")
		}
		expression.Type, expression.Literal.Int64 = ql.ExpressionTimestamp, instant.UnixNano()
	default:
		return nil, fmt.Errorf("unsupported expression parameter %T", parameter.Value)
	}
	return expression, nil
}

func walkGrammarExpression(expression *GrammarExpression, visit func(*GrammarExpression)) {
	if expression == nil {
		return
	}
	if expression.Param {
		visit(expression)
	}
	for _, child := range expression.Args {
		walkGrammarExpression(child, visit)
	}
}

func grammarHasExpressions(grammar *Grammar) bool {
	return grammar.Select != nil && hasMeasureExpressions(grammar.Select.Projection)
}

func checkExpressionSource(grammar *Grammar) error {
	if grammarHasExpressions(grammar) && !strings.EqualFold(grammar.Select.From.ResourceType, "MEASURE") {
		return fmt.Errorf("syntax error: SELECT expressions are supported only for measures")
	}
	return nil
}

func isQuotedMeasureLiteral(column *GrammarColumn, columns *measureColumns) bool {
	if !column.SingleQuoted || column.Identifier == nil || column.Identifier.QuotedIdent == nil || column.TypeSpec != nil {
		return false
	}
	_, err := columns.resolve(*column.Identifier.QuotedIdent, nil)
	return err != nil && len(columns.tags[*column.Identifier.QuotedIdent]) == 0
}
