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
	"slices"

	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func (m *MeasureTemplate) bindHaving(bound *BoundQuery) (*ql.HavingPlan, error) {
	if m.having == nil {
		return nil, nil
	}
	predicate, err := m.having.bind(bound)
	if err != nil {
		return nil, err
	}
	predicate, err = ql.ResolvePredicate(predicate)
	if err != nil {
		return nil, err
	}
	return &ql.HavingPlan{Predicate: predicate, Outputs: slices.Clone(m.havingOutputs)}, nil
}

func (m *MeasureTemplate) compileHaving(
	columns *measureColumns, plan *ql.AggregatePlan, groups map[measureColumnKey]int, addInput func(vectorized.ColumnDef),
) error {
	statement := m.statement.template.Select
	if statement.GroupBy == nil && len(plan.Calls) == 0 && !havingHasAggregate(statement.Having.Expression) {
		return fmt.Errorf("HAVING requires grouping or an aggregate call")
	}
	m.havingOutputs = make([]ql.Slot, len(plan.Outputs))
	for index, output := range plan.Outputs {
		m.havingOutputs[index] = ql.Slot{ID: ql.SlotID(index), Source: output.Column, Output: output.Column, Nullable: true}
	}
	var err error
	m.having, err = compileHavingExpression(statement.Having.Expression, columns, plan, groups, addInput)
	return err
}

func havingHasAggregate(expression *GrammarExpression) bool {
	if expression.Aggregate != nil {
		return true
	}
	for _, child := range expression.Args {
		if havingHasAggregate(child) {
			return true
		}
	}
	return false
}

func compileHavingExpression(
	expression *GrammarExpression, columns *measureColumns, plan *ql.AggregatePlan,
	groups map[measureColumnKey]int, addInput func(vectorized.ColumnDef),
) (*measureExpression, error) {
	return compileMeasureExpressionRecipe(expression, columns, func(leaf *GrammarExpression) (ql.Expression, error) {
		if leaf.Aggregate != nil {
			call, output, err := resolveMeasureAggregate(leaf.Aggregate, leaf.TypeSpec, columns)
			if err != nil {
				return ql.Expression{}, err
			}
			index := addMeasureAggregateCall(plan, call)
			addInput(call.Source)
			return ql.Expression{Op: "column", Source: havingCompletedOutput(plan, output, index, false)}, nil
		}
		name, err := leaf.Identifier.ToString(leaf.TypeSpec != nil)
		if err != nil {
			return ql.Expression{}, err
		}
		source, err := columns.resolve(name, leaf.TypeSpec)
		if err != nil {
			return ql.Expression{}, err
		}
		index, grouped := groups[columnIdentity(source)]
		if !grouped {
			return ql.Expression{}, fmt.Errorf("HAVING column %q must appear in GROUP BY; SELECT aliases are outside HAVING scope", name)
		}
		return ql.Expression{Op: "column", Source: havingCompletedOutput(plan, source, index, true)}, nil
	})
}

// havingCompletedOutput reuses a visible completed column when possible. A
// hidden output is referenced only inside the group filter and removed by its
// final projection; repeated calls share both state and their completed column.
func havingCompletedOutput(plan *ql.AggregatePlan, definition vectorized.ColumnDef, index int, group bool) vectorized.ColumnDef {
	for _, output := range plan.Outputs {
		if output.IsGroup == group && output.Index == index {
			return output.Column
		}
	}
	for suffix := len(plan.Outputs); ; suffix++ {
		definition.Name = fmt.Sprintf("__ql_having_%d", suffix)
		occupied := false
		for _, output := range plan.Outputs {
			if columnIdentity(output.Column) == columnIdentity(definition) {
				occupied = true
				break
			}
		}
		if !occupied {
			break
		}
	}
	plan.Outputs = append(plan.Outputs, ql.AggregateOutput{Column: definition, Index: index, IsGroup: group})
	return definition
}
