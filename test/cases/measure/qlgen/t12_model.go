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

// FilterExpectation declares independent predicate truth counts and downstream use.
type FilterExpectation struct {
	InputRows   uint64 `json:"input_rows"`
	TrueRows    uint64 `json:"true_rows"`
	FalseRows   uint64 `json:"false_rows"`
	UnknownRows uint64 `json:"unknown_rows"`
	Complete    bool   `json:"complete,omitempty"`
	Aggregate   bool   `json:"aggregate,omitempty"`
}

// FilterScenario declares repeated bindings or an injected failure of a raw field filter.
type FilterScenario struct {
	Name  string `json:"name"`
	Kind  string `json:"kind"`
	Cases []Case `json:"cases"`
}

type t12Query struct {
	Predicate    *t10Expr
	Limit        *uint32
	Columns      []t10Column
	Aggregates   []t07Column
	GroupBy      []string
	Params       []any
	NativeRIDs   []int64
	Begin        int64
	End          int64
	SelectParams int
	Offset       uint32
	Legacy       bool
}

func renderT12(q t12Query) (string, error) {
	columns := make([]string, 0, len(q.Columns)+len(q.Aggregates))
	for _, column := range q.Columns {
		text := column.Expr.sql()
		if column.Alias != "" {
			text += " AS " + column.Alias
		}
		columns = append(columns, text)
	}
	for _, column := range q.Aggregates {
		text := column.Target
		if column.Function != "" {
			text = column.Function + "(" + text + ")"
		}
		if column.Alias != "" {
			text += " AS " + column.Alias
		}
		columns = append(columns, text)
	}
	query := "SELECT " + strings.Join(columns, ", ") + " FROM " + FixtureGroup + "." + T12FixtureName
	params := make([]any, 0, len(q.Params)+2)
	for _, value := range q.Params[:q.SelectParams] {
		params = append(params, jsonValue(value))
	}
	params = append(params, jsonValue(timePlaceholder(q.Begin)), jsonValue(timePlaceholder(q.End)))
	if q.Legacy {
		query += " TIME BETWEEN ? AND ? WHERE "
		params[len(params)-1] = jsonValue(timePlaceholder(q.End - 1))
	} else {
		query += " WHERE time >= ? AND time < ? AND "
	}
	if len(q.NativeRIDs) > 0 {
		candidates := make([]string, 0, len(q.NativeRIDs))
		for _, rid := range q.NativeRIDs {
			candidates = append(candidates, fmt.Sprintf("%d", rid))
		}
		query += "rid IN (" + strings.Join(candidates, ", ") + ") AND "
	}
	query += q.Predicate.sql()
	for _, value := range q.Params[q.SelectParams:] {
		params = append(params, jsonValue(value))
	}
	if len(q.GroupBy) > 0 {
		query += " GROUP BY " + strings.Join(q.GroupBy, ", ")
	}
	if len(q.Aggregates) == 0 {
		query += " ORDER BY TIME ASC"
	}
	if q.Limit != nil {
		query += fmt.Sprintf(" LIMIT %d", *q.Limit)
	}
	if q.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", q.Offset)
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		return "", err
	}
	return query + "\n#!params: " + string(encoded), nil
}

func t12TimeLiteral(millis int64) *t10Expr {
	return &t10Expr{Op: "literal", Value: t10Timestamp{Millis: millis}, Text: "'" + timePlaceholder(millis) + "'"}
}
