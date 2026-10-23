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
	if hasMeasureExpressions(stmt.Projection) || stmt.Projection.All || stmt.Projection.Empty {
		return fmt.Errorf("extended aggregation requires named group columns and aggregate calls")
	}
	if stmt.Projection.TopN != nil {
		return fmt.Errorf("extended aggregation does not support TOP")
	}
	if stmt.OrderBy != nil {
		return fmt.Errorf("extended aggregation requires post-aggregate column sorting, which is not supported yet")
	}
	var err error
	plan := &ql.AggregatePlan{}
	var keys map[measureColumnKey]int
	inputs := make(map[measureColumnKey]bool)
	addInput := func(source vectorized.ColumnDef) {
		key := columnIdentity(source)
		if !inputs[key] {
			inputs[key] = true
			m.slots = append(m.slots, ql.Slot{ID: ql.SlotID(len(m.slots)), Source: source, Output: source, Nullable: true})
		}
	}
	plan.GroupBy, keys, err = compileAggregateGroups(stmt.GroupBy, columns)
	if err != nil {
		return err
	}
	for _, source := range plan.GroupBy {
		addInput(source)
	}
	outputs := make(map[measureColumnKey]bool)
	for _, column := range stmt.Projection.Columns {
		var output ql.AggregateOutput
		if column.Aggregate == nil {
			name, nameErr := column.Identifier.ToString(column.TypeSpec != nil)
			if nameErr != nil {
				return nameErr
			}
			source, sourceErr := columns.resolve(name, column.TypeSpec)
			if sourceErr != nil {
				return sourceErr
			}
			index, grouped := keys[columnIdentity(source)]
			if !grouped {
				return fmt.Errorf("projected column %q must appear in GROUP BY", name)
			}
			output = ql.AggregateOutput{Column: source, Index: index, IsGroup: true}
		} else {
			call, definition, callErr := resolveMeasureAggregate(column.Aggregate, column.TypeSpec, columns)
			if callErr != nil {
				return callErr
			}
			output = ql.AggregateOutput{Index: addMeasureAggregateCall(plan, call), Column: definition}
			addInput(call.Source)
		}
		if column.Alias != nil {
			output.Column.Name, err = column.Alias.ToString(true)
			if err != nil {
				return err
			}
		}
		key := columnIdentity(output.Column)
		if outputs[key] {
			return fmt.Errorf("duplicate output name %q in container %q", key.name, key.family)
		}
		outputs[key] = true
		plan.Outputs = append(plan.Outputs, output)
	}
	if stmt.Having != nil {
		if err = m.compileHaving(columns, plan, keys, addInput); err != nil {
			return err
		}
	}
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

func compileAggregateGroups(groupBy *GrammarGroupByClause, columns *measureColumns) ([]vectorized.ColumnDef, map[measureColumnKey]int, error) {
	keys := make(map[measureColumnKey]int)
	var groups []vectorized.ColumnDef
	if groupBy != nil {
		for _, group := range groupBy.Columns {
			if group.TimeBucket != nil {
				return nil, nil, fmt.Errorf("extended aggregation supports named scalar GROUP BY columns; TIME_BUCKET requires native legacy execution")
			}
			name, nameErr := group.Identifier.ToString(group.TypeSpec != nil)
			if nameErr != nil {
				return nil, nil, nameErr
			}
			source, sourceErr := columns.resolve(name, group.TypeSpec)
			if sourceErr != nil {
				return nil, nil, sourceErr
			}
			if !scalarAggregateType(source.Type) {
				return nil, nil, fmt.Errorf("GROUP BY column %q requires a scalar type", name)
			}
			key := columnIdentity(source)
			if _, exists := keys[key]; !exists {
				keys[key] = len(groups)
				groups = append(groups, source)
			}
		}
	}
	return groups, keys, nil
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
