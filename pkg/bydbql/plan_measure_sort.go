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
	"reflect"
	"strings"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type measureSortKey struct {
	expression *measureExpression
	descending bool
	nullsFirst bool
}

func measureOrderNeedsTyped(stmt *GrammarSelectStatement, schemas map[string]*databasev1.Measure) bool {
	order := stmt.OrderBy
	if order == nil {
		return false
	}
	if len(order.Keys) > 0 {
		return true
	}
	if order.Tail.WithIdent == nil || isBuiltinTime(order.Tail.WithIdent.Identifier) {
		return false
	}
	name, err := order.Tail.WithIdent.Identifier.ToString(true)
	if err != nil {
		return true
	}
	for _, schema := range schemas {
		for _, field := range schema.Fields {
			if field.Name == name {
				return true
			}
		}
		for _, family := range schema.TagFamilies {
			for _, tag := range family.Tags {
				if tag.Name == name || family.Name+"."+tag.Name == name {
					return true
				}
			}
		}
	}
	return false
}

func measureOrderKeys(order *GrammarSelectOrderByClause) []*GrammarOrderKey {
	if len(order.Keys) > 0 {
		return order.Keys
	}
	name := "time"
	expression := &GrammarExpression{Op: "column", Identifier: &GrammarIdentifierPath{First: &GrammarIdentifierPart{Ident: &name}}}
	direction := order.Tail.DirOnly
	if order.Tail.WithIdent != nil {
		expression.Identifier = order.Tail.WithIdent.Identifier
		direction = order.Tail.WithIdent.Direction
	}
	descending := direction != nil && strings.EqualFold(*direction, "DESC")
	return []*GrammarOrderKey{{Expression: expression, Descending: descending, NullsFirst: descending}}
}

func (m *MeasureTemplate) sortColumns() []vectorized.ColumnDef {
	var definitions []vectorized.ColumnDef
	switch {
	case m.aggregate != nil:
		for _, output := range m.aggregate.Outputs {
			definitions = append(definitions, output.Column)
		}
	case m.expressions != nil:
		for _, output := range m.expressions {
			definitions = append(definitions, output.column)
		}
	default:
		for _, slot := range m.slots {
			definitions = append(definitions, slot.Output)
		}
	}
	return definitions
}

func (m *MeasureTemplate) compileSort(columns *measureColumns) error {
	stmt := m.statement.template.Select
	if stmt.OrderBy == nil {
		return nil
	}
	if m.aggregate == nil && !stmt.Distinct && len(stmt.OrderBy.Keys) == 0 && m.request.OrderBy != nil {
		// Existing time and explicit index-rule ordering keep native execution.
		return nil
	}
	m.sortVisible = len(m.sortColumns())
	if m.having != nil {
		m.sortVisible = len(m.havingOutputs)
	}
	inputs := newMeasureExpressionInputs(m.slots)
	for _, key := range measureOrderKeys(stmt.OrderBy) {
		expression := key.Expression
		var recipe *measureExpression
		var err error
		if ordinal, positional := sortOrdinalLiteral(expression); positional {
			recipe, err = m.compileSortOrdinal(ordinal, columns, inputs)
		} else {
			recipe, err = m.compileSortExpression(expression, columns, inputs)
		}
		if err != nil {
			return fmt.Errorf("ORDER BY: %w", err)
		}
		m.ordering = append(m.ordering, measureSortKey{expression: recipe, descending: key.Descending, nullsFirst: key.NullsFirst})
	}
	if m.aggregate != nil {
		m.slots = inputs.slots
		if m.having != nil && !stmt.Distinct {
			m.havingOutputs = make([]ql.Slot, len(m.aggregate.Outputs))
			for index, output := range m.aggregate.Outputs {
				m.havingOutputs[index] = ql.Slot{ID: ql.SlotID(index), Source: output.Column, Output: output.Column, Nullable: true}
			}
		}
	} else if m.expressions != nil {
		m.slots = inputs.slots
	}
	m.request.OrderBy = nil
	m.extendSortProjection()
	return nil
}

func (m *MeasureTemplate) extendSortProjection() {
	inputs := newMeasureExpressionInputs(m.slots)
	// WHERE inputs and a time-only scan anchor may exist only in the native
	// projection. Preserve them while adding hidden ordering inputs.
	for _, family := range m.request.GetTagProjection().GetTagFamilies() {
		for _, name := range family.Tags {
			inputs.add(vectorized.ColumnDef{Name: name, TagFamily: family.Name, Role: vectorized.RoleTag})
		}
	}
	for _, name := range m.request.GetFieldProjection().GetNames() {
		inputs.add(vectorized.ColumnDef{Name: name, Role: vectorized.RoleField})
	}
	m.request.TagProjection, m.request.FieldProjection = rawScanProjection(inputs.slots)
}

func sortOrdinalLiteral(expression *GrammarExpression) (int64, bool) {
	sign := int64(1)
	if len(expression.Args) == 1 && (expression.Op == "positive" || expression.Op == "negate") {
		if expression.Op == "negate" {
			sign = -1
		}
		expression = expression.Args[0]
	}
	if expression.Op != "literal" || expression.Param || expression.Type != ql.ExpressionInt {
		return 0, false
	}
	return sign * expression.Literal.Int64, true
}

func (m *MeasureTemplate) compileSortExpression(
	expression *GrammarExpression, columns *measureColumns, inputs *measureExpressionInputs,
) (*measureExpression, error) {
	return compileMeasureExpressionRecipe(expression, columns, func(leaf *GrammarExpression) (ql.Expression, error) {
		name, err := leaf.Identifier.ToString(leaf.TypeSpec != nil)
		if err != nil {
			return ql.Expression{}, err
		}
		return ql.Expression{}, fmt.Errorf("column %q must be grouped or derivable from visible DISTINCT output", name)
	}, func(subtree *GrammarExpression) (*measureExpression, bool, error) {
		if recipe, matched, err := m.sortAlias(subtree); matched || err != nil {
			return recipe, matched, err
		}
		if subtree.Aggregate != nil {
			if m.aggregate == nil {
				return nil, true, fmt.Errorf("aggregate sort keys require grouping or a selected aggregate")
			}
			call, definition, err := resolveMeasureAggregate(subtree.Aggregate, subtree.TypeSpec, columns)
			if err != nil {
				return nil, true, err
			}
			index := -1
			for candidate, existing := range m.aggregate.Calls {
				if reflect.DeepEqual(existing, call) {
					index = candidate
					break
				}
			}
			if m.statement.template.Select.Distinct && !m.visibleAggregate(index, false) {
				return nil, true, fmt.Errorf("aggregate sort key must be derivable from visible DISTINCT output")
			}
			if index < 0 {
				index = addMeasureAggregateCall(m.aggregate, call)
			}
			inputs.add(call.Source)
			return havingCompletedOutput(m.aggregate, definition, index, false), true, nil
		}
		if havingHasAggregate(subtree) {
			return nil, false, nil
		}
		recipe, err := compileMeasureExpression(subtree, columns, newMeasureExpressionInputs(nil))
		if err != nil {
			if subtree.Op == "column" || subtree.Op == "time_bucket" {
				return nil, true, err
			}
			return nil, false, nil
		}
		if m.aggregate != nil {
			index := m.findGroup(recipe)
			if index < 0 {
				return nil, false, nil
			}
			if m.statement.template.Select.Distinct && !m.visibleAggregate(index, true) {
				return nil, true, fmt.Errorf("group sort key must be derivable from visible DISTINCT output")
			}
			return havingCompletedOutput(m.aggregate, m.groups[index].column, index, true), true, nil
		}
		if index := m.visibleSortExpression(recipe); index >= 0 {
			return &measureExpression{sortOutput: &index}, true, nil
		}
		if subtree.Op != "column" {
			return nil, false, nil
		}
		if m.statement.template.Select.Distinct {
			return nil, true, fmt.Errorf("ORDER BY expression must be derivable from visible DISTINCT output")
		}
		source := recipe.value.Source
		index := len(m.sortColumns())
		definition := source
		definition.Name = m.hiddenSortName(columns)
		if m.expressions != nil {
			m.expressions = append(m.expressions, measureExpressionOutput{expression: recipe, column: definition})
			inputs.add(source)
		} else {
			if source.Role == vectorized.RoleTimestamp {
				definition.Role, definition.TagFamily = vectorized.RoleTag, "__ql"
			}
			m.slots = append(m.slots, ql.Slot{ID: ql.SlotID(index), Source: source, Output: definition, Nullable: true, Timestamp: source.Role == vectorized.RoleTimestamp})
		}
		return &measureExpression{sortOutput: &index}, true, nil
	})
}

func (m *MeasureTemplate) visibleAggregate(index int, group bool) bool {
	if index < 0 {
		return false
	}
	for _, output := range m.aggregate.Outputs[:m.sortVisible] {
		if output.Index == index && output.IsGroup == group {
			return true
		}
	}
	return false
}

func (m *MeasureTemplate) visibleSortExpression(recipe *measureExpression) int {
	for index := 0; index < m.sortVisible; index++ {
		if m.expressions != nil {
			if reflect.DeepEqual(m.expressions[index].expression, recipe) {
				return index
			}
		} else if recipe.value.Op == "column" && columnIdentity(m.slots[index].Source) == columnIdentity(recipe.value.Source) {
			return index
		}
	}
	return -1
}

func (m *MeasureTemplate) hiddenSortName(columns *measureColumns) string {
	for suffix := len(m.sortColumns()); ; suffix++ {
		name := fmt.Sprintf("__ql_sort_%d", suffix)
		_, field := columns.fields[name]
		if field || len(columns.tags[name]) > 0 {
			continue
		}
		occupied := false
		for _, definition := range m.sortColumns() {
			occupied = occupied || definition.Name == name
		}
		if !occupied {
			return name
		}
	}
}

func boundSortOutputs(plan *ql.RawPlan) []ql.Slot {
	if plan.Having != nil {
		return plan.Having.Outputs
	}
	if plan.Expressions != nil {
		outputs := make([]ql.Slot, len(plan.Expressions.Outputs))
		for index, output := range plan.Expressions.Outputs {
			outputs[index] = ql.Slot{Source: output.Column, Output: output.Column, Timestamp: output.Expr.Type == ql.ExpressionTimestamp}
		}
		return outputs
	}
	if plan.Aggregate != nil {
		outputs := make([]ql.Slot, len(plan.Aggregate.Outputs))
		for index, output := range plan.Aggregate.Outputs {
			outputs[index] = ql.Slot{Source: output.Column, Output: output.Column, Timestamp: output.Timestamp}
		}
		return outputs
	}
	return plan.Slots
}

func (m *MeasureTemplate) bindSort(bound *BoundQuery, plan *ql.RawPlan) (*ql.SortPlan, error) {
	if len(m.ordering) == 0 {
		return nil, nil
	}
	inputs := boundSortOutputs(plan)
	outputs := make([]ql.Slot, m.sortVisible)
	for index, slot := range inputs[:m.sortVisible] {
		slot.Source = slot.Output
		outputs[index] = slot
	}
	sorting := &ql.SortPlan{Outputs: outputs, Keys: make([]ql.SortKey, len(m.ordering))}
	for index, key := range m.ordering {
		expression, err := key.expression.bindOutputExpression(bound, plan.Aggregate, inputs)
		if err != nil {
			return nil, err
		}
		expression, err = ql.ResolveExpression(expression)
		if err != nil {
			return nil, fmt.Errorf("ORDER BY: %w", err)
		}
		switch expression.Type {
		case ql.ExpressionInt, ql.ExpressionFloat, ql.ExpressionString, ql.ExpressionBytes, ql.ExpressionTimestamp, ql.ExpressionNull:
		default:
			return nil, fmt.Errorf("ORDER BY requires a scalar numeric, string, bytes or timestamp key")
		}
		sorting.Keys[index] = ql.SortKey{Expr: expression, Descending: key.descending, NullsFirst: key.nullsFirst}
	}
	return sorting, nil
}

func (m *MeasureTemplate) compileSortOrdinal(ordinal int64, columns *measureColumns, inputs *measureExpressionInputs) (*measureExpression, error) {
	stmt := m.statement.template.Select
	projection := aggregateProjection(stmt.Projection, columns)
	if ordinal <= 0 || ordinal > int64(len(projection)) {
		return nil, fmt.Errorf("ORDER BY ordinal %d is outside the select list [1, %d]", ordinal, len(projection))
	}
	implicitTime := func(column *GrammarColumn) bool {
		return m.aggregate == nil && !stmt.Distinct && column.Alias == nil && column.TypeSpec == nil && isBuiltinTime(column.Identifier)
	}
	column := projection[ordinal-1]
	if implicitTime(column) {
		return m.compileSortExpression(aggregateColumnExpression(column, columns), columns, inputs)
	}
	index := int(ordinal - 1)
	for _, preceding := range projection[:ordinal-1] {
		if implicitTime(preceding) {
			index--
		}
	}
	return &measureExpression{sortOutput: &index}, nil
}
