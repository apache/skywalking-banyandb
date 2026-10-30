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

// HavingExpectation declares completed-group truth counts independently of raw filtering.
type HavingExpectation struct {
	InputGroups   uint64 `json:"input_groups"`
	TrueGroups    uint64 `json:"true_groups"`
	FalseGroups   uint64 `json:"false_groups"`
	UnknownGroups uint64 `json:"unknown_groups"`
	RawFilterRows uint64 `json:"raw_filter_rows,omitempty"`
	RawTrueRows   uint64 `json:"raw_true_rows,omitempty"`
}

// HavingScenario declares identical SQL bound repeatedly, with optional native aggregation.
type HavingScenario struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Cases    []Case `json:"cases"`
	Branches uint64 `json:"branches,omitempty"`
}

type t14Query struct {
	Having     *t10Expr
	Where      *t10Expr
	Membership *t14Membership
	Limit      *uint32
	Fixture    string
	Columns    []t07Column
	GroupBy    []string
	Hosts      []string
	Params     []any
	Begin, End int64
	Offset     uint32
	Legacy     bool
}

type t14Membership struct {
	Column   string
	Operator string
	Form     string
	Values   []any
}

func t14Aggregate(function, target string) *t10Expr {
	return &t10Expr{Op: "aggregate", Value: t07Column{Function: function, Target: target}, Text: function + "(" + target + "::FIELD)"}
}

func renderT14(q t14Query) (string, error) {
	var columns []string
	for _, column := range q.Columns {
		text := column.Target
		if column.Function != "" {
			text = column.Function + "(" + text + ")"
		}
		if column.Alias != "" {
			text += " AS " + column.Alias
		}
		columns = append(columns, text)
	}
	query := "SELECT " + strings.Join(columns, ", ") + " FROM " + FixtureGroup + "." + q.Fixture + " WHERE time >= ? AND time < ?"
	if q.Legacy {
		query = "SELECT " + strings.Join(columns, ", ") + " FROM MEASURE " + q.Fixture + " IN " + FixtureGroup + " TIME BETWEEN ? AND ? WHERE"
	}
	end := q.End
	if q.Legacy {
		end--
	}
	params := []any{jsonValue(timePlaceholder(q.Begin)), jsonValue(timePlaceholder(end))}
	if len(q.Hosts) > 0 {
		query += " AND host IN (?)"
		params = append(params, jsonValue(q.Hosts))
	}
	if q.Membership != nil {
		source := q.Membership.Column + "::TAG"
		join := " AND "
		if q.Legacy {
			source = q.Membership.Column
			join = " "
		}
		query += join + source + " " + q.Membership.Operator + " "
		var args []string
		for _, value := range q.Membership.Values {
			if strings.HasPrefix(q.Membership.Form, "literal") {
				args = append(args, t10Literal(value).sql())
			} else {
				args = append(args, "?")
				params = append(params, jsonValue(value))
			}
		}
		if q.Membership.Form == "single" || q.Membership.Form == "literal-single" {
			query += strings.Join(args, ", ")
		} else {
			query += "(" + strings.Join(args, ", ") + ")"
		}
	}
	if q.Where != nil {
		query += " AND " + q.Where.sql()
	}
	if len(q.GroupBy) > 0 {
		query += " GROUP BY " + strings.Join(q.GroupBy, ", ")
	}
	if q.Having != nil {
		query += " HAVING " + q.Having.sql()
	}
	for _, value := range q.Params {
		params = append(params, jsonValue(value))
	}
	if len(q.GroupBy) == 0 && q.Having == nil {
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
