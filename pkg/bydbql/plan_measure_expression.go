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
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type measureExpression struct {
	temporal        *GrammarCompareTail
	membershipTag   *tagSpecWithFamily
	completedOutput *int
	sortOutput      *int
	sortAlias       *measureSortAlias
	args            []*measureExpression
	value           ql.Expression
	paramIndex      int
	parameter       bool
	membershipList  bool
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
		if column.Expression != nil || column.All {
			return true
		}
	}
	return false
}

func (m *MeasureTemplate) compileExpressions(columns *measureColumns) error {
	stmt := m.statement.template.Select
	if stmt.GroupBy != nil || stmt.Projection.TopN != nil || measureAggregateCount(stmt) != 0 {
		return fmt.Errorf("SELECT expressions with aggregation, grouping or TOP are not supported")
	}
	var err error
	inputs := newMeasureExpressionInputs(nil)
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
			if column.TypeSpec == nil && isBuiltinTime(column.Identifier) && column.Alias == nil && !stmt.Distinct {
				continue
			}
			expr = &GrammarExpression{Op: "column", Identifier: column.Identifier, TypeSpec: column.TypeSpec}
		}
		recipe, compileErr := compileMeasureExpression(expr, columns, inputs)
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
	if !hasNativeExpressionInput(inputs.slots) {
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
		inputs.add(anchor)
	}
	m.slots = inputs.slots
	m.request = baseMeasureRequest(stmt)
	m.request.TagProjection, m.request.FieldProjection = rawScanProjection(m.slots)
	if err = m.compileExpressionOrder(stmt, columns); err != nil {
		return err
	}
	if stmt.Where != nil {
		err = m.compileWhere(columns)
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

type measureExpressionInputs struct {
	seen  map[measureColumnKey]bool
	slots []ql.Slot
}

func newMeasureExpressionInputs(slots []ql.Slot) *measureExpressionInputs {
	inputs := &measureExpressionInputs{seen: make(map[measureColumnKey]bool, len(slots))}
	for _, slot := range slots {
		inputs.add(slot.Source)
	}
	return inputs
}

func (i *measureExpressionInputs) add(column vectorized.ColumnDef) {
	key := columnIdentity(column)
	if i.seen[key] {
		return
	}
	i.seen[key] = true
	i.slots = append(i.slots, ql.Slot{ID: ql.SlotID(len(i.slots)), Source: column, Output: column, Nullable: true})
}

func compileMeasureExpression(expr *GrammarExpression, columns *measureColumns, inputs *measureExpressionInputs) (*measureExpression, error) {
	return compileMeasureExpressionRecipe(expr, columns, func(leaf *GrammarExpression) (ql.Expression, error) {
		var source vectorized.ColumnDef
		var err error
		if leaf.TypeSpec == nil && isBuiltinTime(leaf.Identifier) {
			source = vectorized.ColumnDef{Name: "time", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64}
		} else {
			var name string
			name, err = leaf.Identifier.ToString(leaf.TypeSpec != nil)
			if err == nil {
				source, err = columns.resolve(name, leaf.TypeSpec)
			}
		}
		if err != nil {
			return ql.Expression{}, err
		}
		value := ql.Expression{Op: "column", Source: source}
		if tag := columns.tagSpecs[source.TagFamily+"."+source.Name]; tag != nil && tag.tag.Type == databasev1.TagType_TAG_TYPE_TIMESTAMP {
			value.Type = ql.ExpressionTimestamp
			value.Source.Type = vectorized.ColumnTypeInt64
		}
		inputs.add(source)
		return value, nil
	})
}

// compileMeasureExpressionRecipe shares recursive expression construction across
// raw rows and completed groups. Each scope resolves its own column leaves.
func compileMeasureExpressionRecipe(
	expression *GrammarExpression, columns *measureColumns, resolveLeaf func(*GrammarExpression) (ql.Expression, error),
	resolveSubtree ...func(*GrammarExpression) (*measureExpression, bool, error),
) (*measureExpression, error) {
	if len(resolveSubtree) > 0 {
		if recipe, matched, err := resolveSubtree[0](expression); matched || err != nil {
			return recipe, err
		}
	}
	recipe := &measureExpression{
		temporal:  expression.Temporal,
		value:     ql.Expression{Op: expression.Op, Type: expression.Type, Literal: expression.Literal},
		parameter: expression.Param, paramIndex: expression.ParamIndex,
	}
	if expression.Op == "time_bucket" {
		width, err := measureBucketWidth(expression.Literal.String, columns.intervals)
		if err != nil {
			return nil, err
		}
		recipe.value.Literal.Int64 = int64(width)
		recipe.value.Literal.String = ""
		name := "time"
		argument, err := compileMeasureExpressionRecipe(&GrammarExpression{Op: "column", Identifier: &GrammarIdentifierPath{
			First: &GrammarIdentifierPart{Ident: &name},
		}}, columns, resolveLeaf, resolveSubtree...)
		if err != nil {
			return nil, fmt.Errorf("TIME_BUCKET: %w", err)
		}
		recipe.args = []*measureExpression{argument}
		return recipe, nil
	}
	if expression.Op == "time_compare" {
		return recipe, nil
	}
	if expression.Op == "column" || expression.Aggregate != nil {
		var err error
		recipe.value, err = resolveLeaf(expression)
		return recipe, err
	}
	for _, child := range expression.Args {
		next, err := compileMeasureExpressionRecipe(child, columns, resolveLeaf, resolveSubtree...)
		if err != nil {
			return nil, err
		}
		recipe.args = append(recipe.args, next)
	}
	if expression.Op == "contains" || expression.Op == "not_contains" {
		if len(recipe.args) < 2 || recipe.args[0].value.Op != "column" || recipe.args[0].value.Source.Role != vectorized.RoleTag {
			return nil, fmt.Errorf("membership requires a source tag")
		}
		leaf := expression.Args[0]
		name, err := leaf.Identifier.ToString(leaf.TypeSpec != nil)
		if err != nil {
			return nil, err
		}
		source, err := columns.resolve(name, leaf.TypeSpec)
		if err != nil {
			return nil, err
		}
		recipe.membershipTag = columns.tagSpecs[source.TagFamily+"."+source.Name]
		recipe.membershipList = expression.MembershipList
	}
	return recipe, nil
}

func measureBucketWidth(raw string, intervals []string) (time.Duration, error) {
	if raw != "" {
		width, err := timestamp.ParseDuration(raw)
		if err != nil {
			return 0, fmt.Errorf("TIME_BUCKET width %q is invalid: %w", raw, err)
		}
		if width <= 0 {
			return 0, fmt.Errorf("TIME_BUCKET width must be positive")
		}
		return width, nil
	}
	var width time.Duration
	for _, interval := range intervals {
		if interval == "" {
			return 0, fmt.Errorf("TIME_BUCKET() requires a measure interval or an explicit width")
		}
		candidate, err := timestamp.ParseDuration(interval)
		if err != nil || candidate <= 0 {
			return 0, fmt.Errorf("TIME_BUCKET() has invalid measure interval %q", interval)
		}
		if width != 0 && width != candidate {
			return 0, fmt.Errorf("TIME_BUCKET() source intervals differ; use an explicit width")
		}
		width = candidate
	}
	if width == 0 {
		return 0, fmt.Errorf("TIME_BUCKET() requires a measure interval or an explicit width")
	}
	return width, nil
}

func (m *MeasureTemplate) bindExpressions(bound *BoundQuery, aggregate *ql.AggregatePlan) (*ql.ExpressionPlan, error) {
	if m.expressions == nil {
		return nil, nil
	}
	plan := &ql.ExpressionPlan{Outputs: make([]ql.ExpressionOutput, 0, len(m.expressions))}
	seen := make(map[measureColumnKey]bool, len(m.expressions))
	for _, output := range m.expressions {
		expression, err := output.expression.bindCompleted(bound, aggregate)
		if err != nil {
			return nil, err
		}
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

func (m *measureExpression) bind(bound *BoundQuery) (*ql.Expression, error) {
	return m.bindCompleted(bound, nil)
}

func (m *measureExpression) bindCompleted(bound *BoundQuery, aggregate *ql.AggregatePlan) (*ql.Expression, error) {
	return m.bindOutputExpression(bound, aggregate, nil)
}

func (m *measureExpression) bindOutputExpression(bound *BoundQuery, aggregate *ql.AggregatePlan, outputs []ql.Slot) (*ql.Expression, error) {
	if m.sortAlias != nil {
		return m.sortAlias.bind(outputs)
	}
	if m.sortOutput != nil {
		index := *m.sortOutput
		if index < 0 || index >= len(outputs) {
			return nil, fmt.Errorf("sort input output %d is unavailable", index)
		}
		output := outputs[index]
		expression := &ql.Expression{Op: "column", Source: output.Output}
		if output.Timestamp {
			expression.Type = ql.ExpressionTimestamp
		}
		return expression, nil
	}
	if m.completedOutput != nil {
		index := *m.completedOutput
		if aggregate == nil || index < 0 || index >= len(aggregate.Outputs) {
			return nil, fmt.Errorf("completed aggregate output %d is unavailable", index)
		}
		output := aggregate.Outputs[index]
		expression := &ql.Expression{Op: "column", Source: output.Column}
		if output.Timestamp {
			expression.Type = ql.ExpressionTimestamp
		}
		return expression, nil
	}
	if m.parameter {
		return boundExpressionLiteral(bound.values[m.paramIndex]), nil
	}
	value := m.value
	value.Args = make([]*ql.Expression, 0, len(m.args))
	for i, child := range m.args {
		arg, err := child.bindOutputExpression(bound, aggregate, outputs)
		if err != nil {
			return nil, err
		}
		appendBoundExpressionArgument(&value, i, arg)
	}
	return m.bindMembership(&value)
}

// appendBoundExpressionArgument shares positional IN expansion for SELECT and
// WHERE bindings. Only RHS literal arrays expand; the left operand stays scalar.
func appendBoundExpressionArgument(expression *ql.Expression, index int, argument *ql.Expression) {
	if index > 0 && (expression.Op == "in" || expression.Op == "not_in") && argument.Op == "literal" {
		switch argument.Type {
		case ql.ExpressionIntArray:
			for _, value := range argument.Literal.Ints {
				expression.Args = append(expression.Args, &ql.Expression{Op: "literal", Type: ql.ExpressionInt, Literal: ql.ExpressionScalar{Int64: value}})
			}
			return
		case ql.ExpressionStringArray:
			for _, value := range argument.Literal.Strings {
				expression.Args = append(expression.Args, &ql.Expression{Op: "literal", Type: ql.ExpressionString, Literal: ql.ExpressionScalar{String: value}})
			}
			return
		default:
		}
	}
	expression.Args = append(expression.Args, argument)
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
	return grammar.Select != nil && (hasMeasureExpressions(grammar.Select.Projection) ||
		grammar.Select.Where != nil && grammar.Select.Where.Expression != nil || grammar.Select.Having != nil || hasComputedMeasureGroups(grammar.Select.GroupBy))
}

func checkExpressionSource(grammar *Grammar) error {
	if topN := grammar.TopN; topN != nil && !strings.EqualFold(topN.From.ResourceType, "MEASURE") &&
		topN.Where != nil && andHasContainsAlias(topN.Where.Expr) {
		return fmt.Errorf("syntax error: CONTAINS is supported only for measures")
	}
	if grammar.Select == nil || strings.EqualFold(grammar.Select.From.ResourceType, "MEASURE") {
		return nil
	}
	if grammarHasExpressions(grammar) {
		return fmt.Errorf("syntax error: scalar expressions are supported only for measures")
	}
	if where := grammar.Select.Where; where != nil && hasContainsAlias(where.Expr) {
		return fmt.Errorf("syntax error: CONTAINS is supported only for measures")
	}
	return nil
}

func hasContainsAlias(expression *GrammarOrExpr) bool {
	if expression == nil {
		return false
	}
	if andHasContainsAlias(expression.Left) {
		return true
	}
	for _, right := range expression.Right {
		if andHasContainsAlias(right.Right) {
			return true
		}
	}
	return false
}

func andHasContainsAlias(expression *GrammarAndExpr) bool {
	contains := func(predicate *GrammarPredicate) bool {
		return predicate.Having != nil && strings.EqualFold(predicate.Having.Having, "CONTAINS") || hasContainsAlias(predicate.Paren)
	}
	if contains(expression.Left) {
		return true
	}
	for _, right := range expression.Right {
		if contains(right.Right) {
			return true
		}
	}
	return false
}

func isQuotedMeasureLiteral(column *GrammarColumn, columns *measureColumns) bool {
	if !column.SingleQuoted || column.Identifier == nil || column.Identifier.QuotedIdent == nil || column.TypeSpec != nil {
		return false
	}
	_, err := columns.resolve(*column.Identifier.QuotedIdent, nil)
	return err != nil && len(columns.tags[*column.Identifier.QuotedIdent]) == 0
}

func (m *MeasureTemplate) compileExpressionOrder(stmt *GrammarSelectStatement, columns *measureColumns) error {
	var orderSlots []ql.Slot
	localOrder := stmt.OrderBy != nil && len(stmt.OrderBy.Keys) > 0
	for _, output := range m.expressions {
		if output.computed && stmt.OrderBy != nil && stmt.OrderBy.Tail.WithIdent != nil {
			name, nameErr := stmt.OrderBy.Tail.WithIdent.Identifier.ToString(true)
			if nameErr != nil {
				return nameErr
			}
			localOrder = localOrder || name == output.column.Name
		}
		if !output.computed {
			orderSlots = append(orderSlots, ql.Slot{Source: output.expression.value.Source, Output: output.column})
		}
	}
	if !localOrder {
		order, err := rawOrderBy(stmt.OrderBy, orderSlots, columns)
		if err != nil {
			return err
		}
		m.request.OrderBy = order
	}
	return nil
}
