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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package qlgen

import (
	"encoding/json"
	"fmt"
	"strings"
)

// AggregateScenario declares cold, changed-binding warm and uncached calls.
// All expected values are calculated from typed fixture rows before any RPC.
type AggregateScenario struct {
	Name  string `json:"name"`
	Cases []Case `json:"cases"`
}

// AggregateFixture declares QL-only visibility checks for one generated measure.
type AggregateFixture struct {
	Name  string `json:"name"`
	Cases []Case `json:"cases"`
}

type t07Column struct {
	Function string
	Target   string
	Alias    string
}

type t07Query struct {
	Where   *T03Time
	Limit   *uint32
	Fixture string
	Columns []t07Column
	GroupBy []string
	Begin   int64
	End     int64
	Offset  uint32
	Native  bool
	Legacy  bool
}

func (c t07Column) name() string {
	if c.Alias != "" {
		return c.Alias
	}
	if c.Function != "" {
		return strings.ToUpper(c.Function) + "(" + c.Target + ")"
	}
	_, name, qualified := strings.Cut(c.Target, ".")
	if qualified {
		return name
	}
	return c.Target
}

func renderT07(q t07Query) (string, error) {
	var columns []string
	for _, column := range q.Columns {
		expression := column.Target
		if column.Function != "" {
			expression = column.Function + "(" + expression + ")"
		} else if column.Target == "v" || column.Target == "f" {
			expression += "::FIELD"
		}
		if column.Alias != "" {
			expression += " AS " + column.Alias
		}
		columns = append(columns, expression)
	}
	query := "SELECT " + strings.Join(columns, ", ") + " FROM "
	if q.Native {
		query += "MEASURE " + q.Fixture + " IN " + FixtureGroup
	} else {
		query += FixtureGroup + "." + q.Fixture
	}
	var params []any
	if q.Legacy {
		query += " TIME BETWEEN ? AND ?"
		params = []any{jsonValue(timePlaceholder(q.Begin)), jsonValue(timePlaceholder(q.End - 1))}
	} else {
		query += " WHERE time >= ? AND time < ?"
		params = []any{jsonValue(timePlaceholder(q.Begin)), jsonValue(timePlaceholder(q.End))}
	}
	if q.Where != nil {
		renderer := t03Renderer{}
		where, err := renderer.predicate(q.Where)
		if err != nil {
			return "", err
		}
		query += " AND (" + where + ")"
	}
	if len(q.GroupBy) > 0 {
		query += " GROUP BY " + strings.Join(q.GroupBy, ", ")
	}
	if q.Limit != nil {
		query += fmt.Sprintf(" LIMIT %d", *q.Limit)
	}
	if q.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", q.Offset)
	}
	values, err := json.Marshal(params)
	if err != nil {
		return "", err
	}
	return query + "\n#!params: " + string(values), nil
}
