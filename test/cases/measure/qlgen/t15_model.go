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

// DistinctScenario rebinds one visible-tuple statement through the compiled cache.
type DistinctScenario struct {
	Name  string `json:"name"`
	Cases []Case `json:"cases"`
}

type t15Query struct {
	Where      *t10Expr
	Having     *t10Expr
	Limit      *uint32
	Order      string
	Columns    []t10Column
	GroupBy    []string
	Params     []any
	Begin, End int64
	Offset     uint32
}

func renderT15(q t15Query) (string, error) {
	var columns []string
	for _, column := range q.Columns {
		text := column.Expr.sql()
		if column.Alias != "" {
			text += " AS " + column.Alias
		}
		columns = append(columns, text)
	}
	query := "SELECT DISTINCT " + strings.Join(columns, ", ") + " FROM " + FixtureGroup + "." + T15FixtureName + " WHERE time >= ? AND time < ?"
	if q.Where != nil {
		query += " AND " + q.Where.sql()
	}
	if len(q.GroupBy) > 0 {
		query += " GROUP BY " + strings.Join(q.GroupBy, ", ")
	}
	if q.Having != nil {
		query += " HAVING " + q.Having.sql()
	}
	if q.Order != "" {
		query += " ORDER BY " + q.Order
	}
	if q.Limit != nil {
		query += fmt.Sprintf(" LIMIT %d", *q.Limit)
	}
	if q.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", q.Offset)
	}
	params := []any{jsonValue(timePlaceholder(q.Begin)), jsonValue(timePlaceholder(q.End))}
	for _, value := range q.Params {
		params = append(params, jsonValue(value))
	}
	encoded, err := json.Marshal(params)
	return query + "\n#!params: " + string(encoded), err
}
