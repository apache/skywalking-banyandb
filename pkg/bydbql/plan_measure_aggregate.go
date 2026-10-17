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
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func (m *MeasureTemplate) compileAggregate(columns *measureColumns) error {
	stmt := m.statement.template.Select
	if stmt.Projection.Empty {
		return fmt.Errorf("extended aggregation requires visible group columns or aggregate calls")
	}
	if stmt.Projection.TopN != nil {
		return fmt.Errorf("extended aggregation does not support TOP")
	}
	if stmt.OrderBy != nil {
		return fmt.Errorf("extended aggregation requires post-aggregate column sorting, which is not supported yet")
	}
	projection := aggregateProjection(stmt.Projection, columns)
	inputs := newMeasureExpressionInputs(nil)
	plan := &ql.AggregatePlan{}
	err := m.compileAggregateGroups(stmt.GroupBy, projection, columns, inputs, plan)
	if err != nil {
		return err
	}
	outputs := make(map[measureColumnKey]bool)
	for index, column := range projection {
		var output ql.AggregateOutput
		outputKnown := true
		if column.Aggregate == nil {
			expression := aggregateColumnExpression(column, columns)
			recipe, recipeErr := compileMeasureExpression(expression, columns, newMeasureExpressionInputs(nil))
			if recipeErr != nil {
				return recipeErr
			}
			group := m.findGroup(recipe)
			if group < 0 {
				return fmt.Errorf("projected expression %d must appear in GROUP BY", index+1)
			}
			definition, known, definitionErr := aggregateGroupOutput(m.groups[group], fmt.Sprintf("expr_%d", index+1))
			if definitionErr != nil {
				return definitionErr
			}
			outputKnown = known

			output = ql.AggregateOutput{Column: definition, Index: group, IsGroup: true}
		} else {
			call, definition, callErr := resolveMeasureAggregate(column.Aggregate, column.TypeSpec, columns)
			if callErr != nil {
				return callErr
			}
			output = ql.AggregateOutput{Index: addMeasureAggregateCall(plan, call), Column: definition}
			inputs.add(call.Source)
		}
		if column.Alias != nil {
			output.Column.Name, err = column.Alias.ToString(true)
			if err != nil {
				return err
			}
		}
		key := columnIdentity(output.Column)
		if outputKnown && outputs[key] {
			return fmt.Errorf("duplicate output name %q in container %q", key.name, key.family)
		}
		if outputKnown {
			outputs[key] = true
		}
		plan.Outputs = append(plan.Outputs, output)
	}
	if stmt.Having != nil {
		if err = m.compileHaving(columns, plan, inputs.add); err != nil {
			return err
		}
	}
	// Constant keys and bucket-only grouping still need one native source
	// column to establish the complete raw row domain.
	if !hasNativeExpressionInput(inputs.slots) && len(columns.ordered) > 0 {
		inputs.add(columns.ordered[0])
	}
	m.slots = inputs.slots
	m.aggregate = plan
	m.request = baseMeasureRequest(stmt)
	m.request.TagProjection, m.request.FieldProjection = rawScanProjection(m.slots)
	if stmt.Where != nil {
		err = m.compileWhere(columns)
	}
	return err
}

func resolveMeasureAggregate(aggregate *GrammarAggregateFunction, typeSpec *string, columns *measureColumns) (
	ql.AggregateCall, vectorized.ColumnDef, error,
) {
	var call ql.AggregateCall
	var output vectorized.ColumnDef
	if aggregate.TypeSpec != nil {
		if typeSpec != nil {
			return call, output, fmt.Errorf("invalid aggregate column type: qualifier appears inside and outside the argument")
		}
		typeSpec = aggregate.TypeSpec
	}
	if aggregate.Distinct {
		return call, output, fmt.Errorf("extended COUNT(DISTINCT) is not supported; existing native routing and stage restrictions still apply")
	}
	name, err := aggregate.Column.ToString(true)
	if err != nil {
		return call, output, err
	}
	source, err := columns.resolve(name, typeSpec)
	if err != nil {
		return call, output, err
	}
	function, err := sqlAggregateFunction(aggregate.Function)
	if err != nil {
		return call, output, err
	}
	if !scalarAggregateType(source.Type) || function != ql.AggregateCount &&
		source.Type != vectorized.ColumnTypeInt64 && source.Type != vectorized.ColumnTypeFloat64 {
		return call, output, fmt.Errorf("%s does not support column %q of type %s", aggregate.Function, name, source.Type)
	}
	valueType := source.Type
	switch function {
	case ql.AggregateCount:
		valueType = vectorized.ColumnTypeInt64
	case ql.AggregateAvg:
		valueType = vectorized.ColumnTypeFloat64
	default:
	}
	call = ql.AggregateCall{Source: source, Function: function}
	output = vectorized.ColumnDef{Name: strings.ToUpper(aggregate.Function) + "(" + name + ")", Role: vectorized.RoleField, Type: valueType}
	return call, output, nil
}

func addMeasureAggregateCall(plan *ql.AggregatePlan, call ql.AggregateCall) int {
	for index, existing := range plan.Calls {
		if existing == call {
			return index
		}
	}
	index := len(plan.Calls)
	plan.Calls = append(plan.Calls, call)
	return index
}

func columnIdentity(column vectorized.ColumnDef) measureColumnKey {
	return measureColumnKey{family: column.TagFamily, name: column.Name, role: column.Role}
}

func scalarAggregateType(columnType vectorized.ColumnType) bool {
	return columnType == vectorized.ColumnTypeInt64 || columnType == vectorized.ColumnTypeFloat64 ||
		columnType == vectorized.ColumnTypeString || columnType == vectorized.ColumnTypeBytes
}

func sqlAggregateFunction(name string) (ql.AggregateFunction, error) {
	switch strings.ToUpper(name) {
	case "COUNT":
		return ql.AggregateCount, nil
	case "SUM":
		return ql.AggregateSum, nil
	case "MIN":
		return ql.AggregateMin, nil
	case "MAX":
		return ql.AggregateMax, nil
	case "AVG", "MEAN":
		return ql.AggregateAvg, nil
	default:
		return 0, fmt.Errorf("unsupported SQL aggregation function %q", name)
	}
}
