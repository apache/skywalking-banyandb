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

	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func (m *MeasureTemplate) validateDistinct() error {
	stmt := m.statement.template.Select
	if stmt.OrderBy != nil {
		if stmt.OrderBy.Tail.WithIdent == nil {
			return fmt.Errorf("DISTINCT ORDER BY requires a visible output")
		}
		identifier := stmt.OrderBy.Tail.WithIdent.Identifier
		name, err := identifier.ToString(true)
		if err != nil {
			return err
		}
		visible := false
		for _, slot := range m.slots {
			if len(m.expressions) == 0 && m.aggregate == nil && (slot.Output.Name == name || isBuiltinTime(identifier) && isBuiltinTimeSource(slot.Source)) {
				visible = true
			}
		}
		for _, output := range m.expressions {
			if !output.computed && (output.column.Name == name || isBuiltinTime(identifier) && isBuiltinTimeSource(output.expression.value.Source)) {
				visible = true
			}
		}
		if !visible {
			return fmt.Errorf("DISTINCT ORDER BY expression %q must be derivable from visible output", name)
		}
	}
	// Parameter expression types are resolved at Bind.
	if len(m.expressions) != 0 || len(m.groups) != 0 {
		return nil
	}
	return ql.ValidateDistinct(&ql.RawPlan{Slots: m.slots, Aggregate: m.aggregate})
}

func isBuiltinTimeSource(def vectorized.ColumnDef) bool { return def.Role == vectorized.RoleTimestamp }
