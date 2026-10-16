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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func (m *MeasureTemplate) compileAggregate(schemas map[string]*databasev1.Measure) error {
	stmt := m.statement.template.Select
	if stmt.Projection.TopN != nil {
		return fmt.Errorf("extended aggregation does not support TOP")
	}
	if stmt.OrderBy != nil {
		return fmt.Errorf("extended aggregation requires post-aggregate column sorting, which is not supported yet")
	}
	columns, err := resolveMeasureColumns(stmt.From.In.Groups, schemas)
	if err != nil {
		return err
	}
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
			aggregate := column.Aggregate
			if aggregate.Distinct {
				return fmt.Errorf("extended COUNT(DISTINCT) is not supported; existing native routing and stage restrictions still apply")
			}
			name, nameErr := aggregate.Column.ToString(true)
			if nameErr != nil {
				return nameErr
			}
			source, sourceErr := columns.resolve(name, column.TypeSpec)
			if sourceErr != nil {
				return sourceErr
			}
			function, functionErr := sqlAggregateFunction(aggregate.Function)
			if functionErr != nil {
				return functionErr
			}
			if !scalarAggregateType(source.Type) || function != ql.AggregateCount &&
				source.Type != vectorized.ColumnTypeInt64 && source.Type != vectorized.ColumnTypeFloat64 {
				return fmt.Errorf("%s does not support column %q of type %s", aggregate.Function, name, source.Type)
			}
			call := ql.AggregateCall{Source: source, Function: function}
			index := -1
			for candidate, existing := range plan.Calls {
				if existing == call {
					index = candidate
					break
				}
			}
			if index < 0 {
				index = len(plan.Calls)
				plan.Calls = append(plan.Calls, call)
			}
			valueType := source.Type
			switch function {
			case ql.AggregateCount:
				valueType = vectorized.ColumnTypeInt64
			case ql.AggregateAvg:
				valueType = vectorized.ColumnTypeFloat64
			default:
			}

			output = ql.AggregateOutput{Index: index, Column: vectorized.ColumnDef{
				Name: strings.ToUpper(aggregate.Function) + "(" + name + ")", Role: vectorized.RoleField, Type: valueType,
			}}
			addInput(source)
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
	m.aggregate = plan
	m.request = baseMeasureRequest(stmt)
	m.request.TagProjection, m.request.FieldProjection = rawScanProjection(m.slots)
	if stmt.Where != nil {
		m.predicate, err = compileMeasureOr(stmt.Where.Expr, columns)
	}
	return err
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
