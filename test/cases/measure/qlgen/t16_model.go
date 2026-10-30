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

// GroupingExpectation declares independent key work after the raw WHERE filter.
type GroupingExpectation struct {
	InputRows      uint64 `json:"input_rows"`
	KeyExpressions uint64 `json:"key_expressions"`
	KeyEvaluations uint64 `json:"key_evaluations"`
}

// GroupingScenario rebinds computed or ordinal grouping through the QL endpoint.
type GroupingScenario struct {
	Name     string `json:"name"`
	Cases    []Case `json:"cases"`
	Branches uint64 `json:"branches,omitempty"`
}

type t16Query struct {
	Where, Having *t10Expr
	Fixture       string
	Columns       []t10Column
	GroupBy       []*t10Expr
	Params        []any
	Hosts         []string
	Begin, End    int64
	Distinct      bool
}

type t16ScenarioAdder func(string, t16Query, [][]any, [][2]int64, uint64, ...string) error

func t16Ordinal(position int) *t10Expr {
	return &t10Expr{Op: "ordinal", Value: position, Text: fmt.Sprint(position)}
}

func t16Bucket(width int64, text string) *t10Expr {
	return &t10Expr{Op: "bucket", Value: width, Text: "TIME_BUCKET(" + text + ")"}
}

func t16Timestamp(millis int64) *t10Expr {
	return &t10Expr{Op: "literal", Value: t10Timestamp{Millis: millis}, Text: "CAST('" + timePlaceholder(millis) + "' AS TIMESTAMP)"}
}

func expandT16Columns(q t16Query) []t10Column {
	var columns []t10Column
	for _, column := range q.Columns {
		if column.Expr.Op != "star" {
			columns = append(columns, column)
			continue
		}
		names := []string{"rid", "host", "zone", "v", "f"}
		if q.Fixture == T16StarFixtureName {
			names = []string{"host", "v"}
		}
		for _, name := range names {
			columns = append(columns, t10Column{Expr: t10Source(name)})
		}
	}
	return columns
}

func renderT16(q t16Query) (string, error) {
	var columns []string
	var params []any
	collect := func(expression *t10Expr) { params = appendT16Params(params, expression, q.Params) }
	for _, column := range q.Columns {
		text := column.Expr.sql()
		if column.Alias != "" {
			text += " AS " + column.Alias
		}
		columns = append(columns, text)
		collect(column.Expr)
	}
	prefix := "SELECT "
	if q.Distinct {
		prefix += "DISTINCT "
	}
	query := prefix + strings.Join(columns, ", ") + " FROM " + FixtureGroup + "." + q.Fixture + " WHERE time >= ? AND time < ?"
	params = append(params, jsonValue(timePlaceholder(q.Begin)), jsonValue(timePlaceholder(q.End)))
	if len(q.Hosts) > 0 {
		query += " AND host IN (?)"
		params = append(params, jsonValue(q.Hosts))
	}
	if q.Where != nil {
		query += " AND " + q.Where.sql()
		collect(q.Where)
	}
	var keys []string
	for _, expression := range q.GroupBy {
		keys = append(keys, expression.sql())
		collect(expression)
	}
	if len(keys) > 0 {
		query += " GROUP BY " + strings.Join(keys, ", ")
	}
	if q.Having != nil {
		query += " HAVING " + q.Having.sql()
		collect(q.Having)
	}
	encoded, err := json.Marshal(params)
	return query + "\n#!params: " + string(encoded), err
}

func appendT16Params(params []any, expression *t10Expr, values []any) []any {
	if expression.Op == "parameter" {
		params = append(params, jsonValue(values[expression.Value.(int)]))
	}
	for _, arg := range expression.Args {
		params = appendT16Params(params, arg, values)
	}
	return params
}
