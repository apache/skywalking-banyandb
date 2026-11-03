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

type measureSortAlias struct {
	name      string
	qualifier string
	outputs   []int
}

func sortOutputNameMatches(name string, definition vectorized.ColumnDef) bool {
	return name == definition.Name || name == definition.TagFamily+"."+definition.Name
}

func sortOutputRoleMatches(qualifier string, definition vectorized.ColumnDef) bool {
	return qualifier == "" || qualifier == "TAG" && definition.Role == vectorized.RoleTag ||
		qualifier == "FIELD" && definition.Role == vectorized.RoleField
}

func (m *MeasureTemplate) dynamicSortOutput(index int) bool {
	if m.expressions != nil {
		output := m.expressions[index]
		return output.computed || output.expression.value.Source.Role == vectorized.RoleTimestamp
	}
	if m.aggregate != nil {
		output := m.aggregate.Outputs[index]
		return output.IsGroup && m.groups[output.Index].computed
	}
	return false
}

func (m *MeasureTemplate) sortAlias(expression *GrammarExpression) (*measureExpression, bool, error) {
	if expression.Op != "column" {
		return nil, false, nil
	}
	name, err := expression.Identifier.ToString(true)
	if err != nil {
		return nil, true, err
	}
	reference := &measureSortAlias{name: name}
	if expression.TypeSpec != nil {
		reference.qualifier = strings.ToUpper(*expression.TypeSpec)
	}
	for index, definition := range m.sortColumns()[:m.sortVisible] {
		dynamic := m.dynamicSortOutput(index)
		if !sortOutputNameMatches(name, definition) && (!dynamic || name != "__ql."+definition.Name) {
			continue
		}
		if !dynamic && !sortOutputRoleMatches(reference.qualifier, definition) {
			continue
		}
		reference.outputs = append(reference.outputs, index)
	}
	if len(reference.outputs) == 0 {
		return nil, false, nil
	}
	return &measureExpression{sortAlias: reference}, true, nil
}

func (a *measureSortAlias) bind(outputs []ql.Slot) (*ql.Expression, error) {
	matched := -1
	for _, index := range a.outputs {
		output := outputs[index]
		if !sortOutputNameMatches(a.name, output.Output) || !sortOutputRoleMatches(a.qualifier, output.Output) {
			continue
		}
		if matched >= 0 {
			return nil, fmt.Errorf("ambiguous ORDER BY output name %q", a.name)
		}
		matched = index
	}
	if matched < 0 {
		return nil, fmt.Errorf("ORDER BY output %q does not match its qualifier", a.name)
	}
	output := outputs[matched]
	expression := &ql.Expression{Op: "column", Source: output.Output}
	if output.Timestamp {
		expression.Type = ql.ExpressionTimestamp
	}
	return expression, nil
}
