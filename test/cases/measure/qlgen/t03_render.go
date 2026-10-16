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

package qlgen

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type t03Renderer struct {
	queryRenderer
	timestampParams bool
	absolute        bool
}

func renderT03(q T03Query) (string, error) {
	r := t03Renderer{queryRenderer: queryRenderer{bind: q.Bind}, timestampParams: q.TimestampParams}
	columns := make([]string, 0, len(q.Columns))
	for _, column := range q.Columns {
		name := column.Source
		if column.Alias != "" {
			name += " AS " + column.Alias
		}
		columns = append(columns, name)
	}
	text := "SELECT " + strings.Join(columns, ", ") + " FROM "
	if q.Shorthand {
		text += FixtureGroup + "." + T03FixtureName
	} else {
		text += "MEASURE " + T03FixtureName + " IN " + FixtureGroup
	}
	if q.LegacyBegin != nil {
		text += " TIME BETWEEN " + r.value(timePlaceholder(*q.LegacyBegin)) + " AND " + r.value(timePlaceholder(q.LegacyEnd))
	}
	if q.Where != nil {
		predicate, err := r.predicate(q.Where)
		if err != nil {
			return "", err
		}
		text += " WHERE " + predicate
	}
	orderBy := q.OrderBy
	if orderBy == "" {
		orderBy = "TIME"
	}
	text += " ORDER BY " + orderBy + " "
	if q.Descending {
		text += "DESC"
	} else {
		text += "ASC"
	}
	if q.Trace {
		text += " WITH QUERY_TRACE"
	}
	if q.Limit != nil {
		text += " LIMIT " + r.integer(int64(*q.Limit))
	}
	if q.Offset > 0 {
		text += " OFFSET " + r.integer(int64(q.Offset))
	}
	if r.absolute {
		text += "\n#!fixture-domain-interior: true"
	}
	if len(r.params) > 0 {
		content, err := json.Marshal(r.params)
		if err != nil {
			return "", err
		}
		text += "\n#!params: " + string(content)
	}
	return text, nil
}

func (r *t03Renderer) predicate(p *T03Time) (string, error) {
	if p.Op == "AND" || p.Op == "OR" {
		if len(p.Children) < 2 {
			return "", fmt.Errorf("time boolean needs two operands")
		}
		operands := make([]string, 0, len(p.Children))
		for _, child := range p.Children {
			operand, err := r.predicate(child)
			if err != nil {
				return "", err
			}
			operands = append(operands, operand)
		}
		return "(" + strings.Join(operands, " "+p.Op+" ") + ")", nil
	}
	switch p.Op {
	case "=", "!=", ">", ">=", "<", "<=":
	default:
		return "", fmt.Errorf("unknown time comparison %q", p.Op)
	}
	var value string
	switch {
	case p.Absolute != "":
		r.absolute = true
		value = r.value(p.Absolute)
	case p.Now:
		value = "now()"
		if p.Nanos != 0 {
			sign := " + "
			nanos := p.Nanos
			if nanos < 0 {
				sign = " - "
				nanos = -nanos
			}
			value += sign + "INTERVAL '" + time.Duration(nanos).String() + "'"
		}
	default:
		token := fmt.Sprintf("{{nanos:%d}}", p.Nanos)
		if r.bind && r.timestampParams {
			r.params = append(r.params, map[string]any{"timestamp": token})
			value = "?"
		} else {
			value = r.value(token)
		}
	}
	return "time " + p.Op + " " + value, nil
}
