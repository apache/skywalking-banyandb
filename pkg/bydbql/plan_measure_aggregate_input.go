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

	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type measureAggregateInput struct {
	expression *measureExpression
	call       int
}

func (m *MeasureTemplate) resolveAggregateInput(aggregate *GrammarAggregateFunction, typeSpec *string, columns *measureColumns,
	plan *ql.AggregatePlan, addInput func(vectorized.ColumnDef),
) (ql.AggregateCall, vectorized.ColumnDef, error) {
	if aggregate.Argument == nil {
		return resolveMeasureAggregate(aggregate, typeSpec, columns)
	}
	var call ql.AggregateCall
	var output vectorized.ColumnDef
	if typeSpec != nil {
		return call, output, fmt.Errorf("aggregate expression cannot have a column qualifier")
	}
	if aggregate.Distinct && !strings.EqualFold(aggregate.Function, "COUNT") {
		return call, output, fmt.Errorf("DISTINCT is only supported with COUNT")
	}
	function, err := sqlAggregateFunction(aggregate.Function)
	if err != nil {
		return call, output, err
	}
	if aggregate.Distinct {
		function = ql.AggregateCountDistinct
	}
	inputs := newMeasureExpressionInputs(nil)
	recipe, err := compileMeasureExpression(aggregate.Argument, columns, inputs)
	if err != nil {
		return call, output, err
	}
	for _, slot := range inputs.slots {
		addInput(slot.Source)
	}
	for _, previous := range m.aggregateInputs {
		if previous.call < len(plan.Calls) && plan.Calls[previous.call].Function == function && reflect.DeepEqual(previous.expression, recipe) {
			call = plan.Calls[previous.call]
			output = vectorized.ColumnDef{Name: strings.ToUpper(aggregate.Function) + "(" + call.Source.Name + ")", Role: vectorized.RoleField, Type: call.Source.Type}
			return call, output, nil
		}
	}
	name := fmt.Sprintf("__ql_aggregate_input_%d", len(m.aggregateInputs))
	for {
		_, occupied := columns.fields[name]
		occupied = occupied || len(columns.tags[name]) > 0
		for _, group := range plan.GroupBy {
			occupied = occupied || group.Role == vectorized.RoleField && group.Name == name
		}
		for _, existing := range plan.Calls {
			occupied = occupied || existing.Source.Role == vectorized.RoleField && existing.Source.Name == name
		}
		if !occupied {
			break
		}
		name += "_"
	}
	source := vectorized.ColumnDef{Name: name, Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
	call = ql.AggregateCall{Source: source, Function: function}
	output = vectorized.ColumnDef{Name: strings.ToUpper(aggregate.Function) + "(" + name + ")", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
	m.aggregateInputs = append(m.aggregateInputs, measureAggregateInput{expression: recipe, call: len(plan.Calls)})
	return call, output, nil
}

func (m *MeasureTemplate) bindAggregateInputs(bound *BoundQuery, plan *ql.AggregatePlan) error {
	for _, input := range m.aggregateInputs {
		expression, err := input.expression.bind(bound)
		if err != nil {
			return err
		}
		resolved, err := ql.ResolveExpression(expression)
		if err != nil {
			return err
		}
		source, err := expressionOutputColumn(plan.Calls[input.call].Source.Name, resolved.Type)
		if err != nil {
			return fmt.Errorf("aggregate argument: %w", err)
		}
		source.Role, source.TagFamily = vectorized.RoleField, ""
		function := plan.Calls[input.call].Function
		if !scalarAggregateType(source.Type) || function != ql.AggregateCount && function != ql.AggregateCountDistinct &&
			source.Type != vectorized.ColumnTypeInt64 && source.Type != vectorized.ColumnTypeFloat64 {
			return fmt.Errorf("aggregate does not support argument type %s", source.Type)
		}
		plan.Calls[input.call].Source = source
		if plan.GroupExpressions == nil {
			plan.GroupExpressions = &ql.ExpressionPlan{}
		}
		plan.GroupExpressions.Outputs = append(plan.GroupExpressions.Outputs, ql.ExpressionOutput{Expr: resolved, Column: source})
		resultType := source.Type
		if function == ql.AggregateCount || function == ql.AggregateCountDistinct {
			resultType = vectorized.ColumnTypeInt64
		}
		if function == ql.AggregateAvg {
			resultType = vectorized.ColumnTypeFloat64
		}
		for index := range plan.Outputs {
			if !plan.Outputs[index].IsGroup && plan.Outputs[index].Index == input.call {
				plan.Outputs[index].Column.Type = resultType
			}
		}
	}
	return nil
}
