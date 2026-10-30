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
	"time"

	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func aggregateProjection(projection *GrammarProjection, columns *measureColumns) []*GrammarColumn {
	var expanded []*GrammarColumn
	expand := func() {
		for _, source := range columns.ordered {
			name, qualifier := source.Name, "FIELD"
			if source.Role == vectorized.RoleTag {
				name = source.TagFamily + "." + source.Name
				qualifier = "TAG"
			}
			expanded = append(expanded, &GrammarColumn{Identifier: &GrammarIdentifierPath{QuotedIdent: &name}, TypeSpec: &qualifier})
		}
	}
	if projection.All {
		expand()
		return expanded
	}
	for _, column := range projection.Columns {
		if column.All {
			expand()
		} else {
			expanded = append(expanded, column)
		}
	}
	return expanded
}

func aggregateColumnExpression(column *GrammarColumn, columns *measureColumns) *GrammarExpression {
	if column.Expression != nil {
		return column.Expression
	}
	if isQuotedMeasureLiteral(column, columns) {
		return &GrammarExpression{Op: "literal", Type: ql.ExpressionString, Literal: ql.ExpressionScalar{String: *column.Identifier.QuotedIdent}}
	}
	return &GrammarExpression{Op: "column", Identifier: column.Identifier, TypeSpec: column.TypeSpec}
}

func (m *MeasureTemplate) findGroup(recipe *measureExpression) int {
	for index, group := range m.groups {
		if reflect.DeepEqual(group.expression, recipe) {
			return index
		}
	}
	return -1
}

func (m *MeasureTemplate) compileAggregateGroups(
	groupBy *GrammarGroupByClause, projection []*GrammarColumn, columns *measureColumns, inputs *measureExpressionInputs, plan *ql.AggregatePlan,
) error {
	if groupBy == nil {
		return nil
	}
	for _, group := range groupBy.Columns {
		expression := group.Expression
		switch {
		case group.Ordinal != nil:
			ordinal := *group.Ordinal
			if ordinal <= 0 || ordinal > len(projection) {
				return fmt.Errorf("GROUP BY ordinal %d is outside the select list [1, %d]", ordinal, len(projection))
			}
			column := projection[ordinal-1]
			if column.Aggregate != nil {
				return fmt.Errorf("GROUP BY ordinal %d contains an aggregate", ordinal)
			}
			expression = aggregateColumnExpression(column, columns)
		case group.TimeBucket != nil:
			if group.TypeSpec != nil {
				return fmt.Errorf("TIME_BUCKET does not support ::TAG/::FIELD")
			}
			expression = &GrammarExpression{Op: "time_bucket", Type: ql.ExpressionTimestamp}
			if group.TimeBucket.Width != nil {
				expression.Literal.String = *group.TimeBucket.Width
			}
		case expression == nil:
			expression = &GrammarExpression{Op: "column", Identifier: group.Identifier, TypeSpec: group.TypeSpec}
		}
		if havingHasAggregate(expression) {
			return fmt.Errorf("GROUP BY expressions must not contain aggregates")
		}
		recipe, err := compileMeasureExpression(expression, columns, inputs)
		if err != nil {
			return err
		}
		if m.findGroup(recipe) >= 0 {
			continue
		}
		computed := recipe.value.Op != "column"
		definition := recipe.value.Source
		if recipe.value.Op == "time_bucket" {
			if plan.TimeBucketWidth != "" {
				return fmt.Errorf("only one TIME_BUCKET is allowed in GROUP BY")
			}
			plan.TimeBucketWidth = time.Duration(recipe.value.Literal.Int64).String()
			plan.TimeBucketIndex = len(m.groups)
		}
		if computed {
			for suffix := len(m.groups); ; suffix++ {
				definition = vectorized.ColumnDef{Name: fmt.Sprintf("__ql_group_%d", suffix), Role: vectorized.RoleField}
				_, occupied := columns.fields[definition.Name]
				for _, existing := range m.groups {
					occupied = occupied || columnIdentity(existing.column) == columnIdentity(definition)
				}
				if !occupied {
					break
				}
			}
		} else if !scalarAggregateType(definition.Type) {
			return fmt.Errorf("GROUP BY column %q requires a scalar type", definition.Name)
		}
		m.groups = append(m.groups, measureExpressionOutput{expression: recipe, column: definition, computed: computed})
		plan.GroupBy = append(plan.GroupBy, definition)
	}
	return nil
}

func (m *MeasureTemplate) bindAggregate(bound *BoundQuery) (*ql.AggregatePlan, error) {
	if m.aggregate == nil {
		return nil, nil
	}
	plan := &ql.AggregatePlan{
		GroupBy: append([]vectorized.ColumnDef(nil), m.aggregate.GroupBy...),
		Calls:   append([]ql.AggregateCall(nil), m.aggregate.Calls...), Outputs: append([]ql.AggregateOutput(nil), m.aggregate.Outputs...),
		TimeBucketWidth: m.aggregate.TimeBucketWidth,
		TimeBucketIndex: m.aggregate.TimeBucketIndex,
	}
	types := make([]ql.ExpressionType, len(m.groups))
	for index, group := range m.groups {
		expression, err := group.expression.bind(bound)
		if err != nil {
			return nil, err
		}
		resolved, err := ql.ResolveExpression(expression)
		if err != nil {
			return nil, err
		}
		types[index] = resolved.Type
		if !group.computed {
			continue
		}
		definition, err := groupExpressionOutputColumn(group.column.Name, resolved.Type)
		if err != nil {
			return nil, fmt.Errorf("GROUP BY expression: %w", err)
		}
		if !scalarAggregateType(definition.Type) {
			return nil, fmt.Errorf("GROUP BY expression requires a scalar type")
		}
		// Synthetic keys are internal fields even when their values are timestamps.
		definition.Role, definition.TagFamily = vectorized.RoleField, ""
		plan.GroupBy[index] = definition
		if plan.GroupExpressions == nil {
			plan.GroupExpressions = &ql.ExpressionPlan{}
		}
		plan.GroupExpressions.Outputs = append(plan.GroupExpressions.Outputs, ql.ExpressionOutput{Expr: resolved, Column: definition})
	}
	if err := m.bindAggregateInputs(bound, plan); err != nil {
		return nil, err
	}
	seen := make(map[measureColumnKey]bool, len(plan.Outputs))
	for index, output := range plan.Outputs {
		if output.IsGroup {
			if m.groups[output.Index].computed {
				definition, err := expressionOutputColumn(output.Column.Name, types[output.Index])
				if err != nil {
					return nil, err
				}
				output.Column = definition
			}
			output.Timestamp = types[output.Index] == ql.ExpressionTimestamp
			plan.Outputs[index] = output
		}
		key := columnIdentity(output.Column)
		if seen[key] {
			return nil, fmt.Errorf("duplicate output name %q in container %q", key.name, key.family)
		}
		seen[key] = true
	}
	return plan, nil
}

func aggregateGroupOutput(group measureExpressionOutput, name string) (vectorized.ColumnDef, bool, error) {
	if !group.computed {
		if group.column.Role == vectorized.RoleTimestamp {
			definition, err := expressionOutputColumn("time", ql.ExpressionTimestamp)
			return definition, true, err
		}
		return group.column, true, nil
	}
	if expressionHasParameters(group.expression) {
		return vectorized.ColumnDef{Name: name, Role: vectorized.RoleField}, false, nil
	}
	expression, err := group.expression.bind(nil)
	if err != nil {
		return vectorized.ColumnDef{}, true, err
	}
	resolved, err := ql.ResolveExpression(expression)
	if err != nil {
		return vectorized.ColumnDef{}, true, err
	}
	definition, err := groupExpressionOutputColumn(name, resolved.Type)
	return definition, true, err
}

func expressionHasParameters(expression *measureExpression) bool {
	if expression.parameter {
		return true
	}
	for _, child := range expression.args {
		if expressionHasParameters(child) {
			return true
		}
	}
	return false
}

func groupExpressionOutputColumn(name string, typ ql.ExpressionType) (vectorized.ColumnDef, error) {
	if typ == ql.ExpressionBool {
		return vectorized.ColumnDef{}, fmt.Errorf("boolean GROUP BY expression requires an explicit cast to INT or STRING")
	}
	if typ == ql.ExpressionNull {
		return vectorized.ColumnDef{}, fmt.Errorf("NULL GROUP BY expression requires an explicit cast")
	}
	return expressionOutputColumn(name, typ)
}
