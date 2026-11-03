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

// NativeAggregateScenario declares independently specified native or fallback RPCs.
type NativeAggregateScenario struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Cases    []Case `json:"cases"`
	Branches uint64 `json:"branches,omitempty"`
}

type t08Query struct {
	Where   *T03Time
	Limit   *uint32
	Key     *string
	Fixture string
	Hosts   []string
	Regions []string
	Columns []t07Column
	GroupBy []string
	Begin   int64
	End     int64
}

func renderT08(q t08Query) (string, error) {
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
	query := "SELECT " + strings.Join(columns, ", ") + " FROM " + FixtureGroup + "." + q.Fixture +
		" WHERE time >= ? AND time < ?"
	params := []any{jsonValue(timePlaceholder(q.Begin)), jsonValue(timePlaceholder(q.End))}
	for _, predicate := range []struct {
		column string
		values []string
	}{{"host", q.Hosts}, {"region", q.Regions}} {
		if len(predicate.values) == 0 {
			continue
		}
		if len(predicate.values) == 1 {
			query += " AND " + predicate.column + " = ?"
			params = append(params, jsonValue(predicate.values[0]))
		} else {
			query += " AND " + predicate.column + " IN (?)"
			params = append(params, jsonValue(predicate.values))
		}
	}
	if q.Key != nil {
		query += " AND key = ?"
		params = append(params, jsonValue(*q.Key))
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
	values, err := json.Marshal(params)
	if err != nil {
		return "", err
	}
	return query + "\n#!params: " + string(values), nil
}
