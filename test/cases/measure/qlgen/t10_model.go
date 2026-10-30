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
	"strconv"
	"strings"
)

// ExpressionScenario specifies repeated bindings of one expression projection.
type ExpressionScenario struct {
	Name  string `json:"name"`
	Cases []Case `json:"cases"`
}

type t10Expr struct {
	Value any
	Op    string
	Type  string
	Text  string
	Args  []*t10Expr
}

type t10Column struct {
	Expr  *t10Expr
	Alias string
}

type t10Query struct {
	Where   *T03Time
	Limit   *uint32
	Columns []t10Column
	Params  []any
	Begin   int64
	End     int64
	Offset  uint32
}

func t10Source(name string) *t10Expr     { return &t10Expr{Op: "source", Value: name} }
func t10Literal(value any) *t10Expr      { return &t10Expr{Op: "literal", Value: value} }
func t10Parameter(position int) *t10Expr { return &t10Expr{Op: "parameter", Value: position} }
func t10Cast(value *t10Expr, target string) *t10Expr {
	return &t10Expr{Op: "cast", Type: target, Args: []*t10Expr{value}}
}

func t10Binary(op string, left, right *t10Expr) *t10Expr {
	return &t10Expr{Op: op, Args: []*t10Expr{left, right}}
}

func t10Unary(op string, value *t10Expr) *t10Expr {
	return &t10Expr{Op: op, Args: []*t10Expr{value}}
}
func t10Call(op string, args ...*t10Expr) *t10Expr { return &t10Expr{Op: op, Args: args} }
func t10Field(name string, expression *t10Expr) t10Column {
	return t10Column{Expr: expression, Alias: name}
}

func (e *t10Expr) sql() string {
	if e.Text != "" {
		return e.Text
	}
	switch e.Op {
	case "source":
		name := e.Value.(string)
		if name == "v" || name == "f" || name == "metric-value" || name == "rate_total" {
			return name + "::FIELD"
		}
		return name
	case "parameter":
		return "?"
	case "literal":
		switch value := e.Value.(type) {
		case nil:
			return "NULL"
		case string:
			return "'" + strings.ReplaceAll(value, "'", "\\'") + "'"
		case bool:
			return strings.ToUpper(strconv.FormatBool(value))
		case int64:
			return strconv.FormatInt(value, 10)
		case float64:
			text := strconv.FormatFloat(value, 'g', -1, 64)
			if !strings.ContainsAny(text, ".eE") {
				text += ".0"
			}
			return text
		}
	case "cast":
		return "CAST(" + e.Args[0].sql() + " AS " + e.Type + ")"
	case "annotate":
		return "(" + e.Args[0].sql() + ")::" + e.Type
	case "negate":
		return "(-" + e.Args[0].sql() + ")"
	case "positive":
		return "(+" + e.Args[0].sql() + ")"
	case "NOT":
		return "(NOT " + e.Args[0].sql() + ")"
	case "IS NULL", "IS NOT NULL":
		return "(" + e.Args[0].sql() + " " + e.Op + ")"
	case "IN", "NOT IN":
		parts := make([]string, 0, len(e.Args)-1)
		for _, arg := range e.Args[1:] {
			parts = append(parts, arg.sql())
		}
		return "(" + e.Args[0].sql() + " " + e.Op + " (" + strings.Join(parts, ", ") + "))"
	case "COALESCE":
		parts := make([]string, 0, len(e.Args))
		for _, arg := range e.Args {
			parts = append(parts, arg.sql())
		}
		return "COALESCE(" + strings.Join(parts, ", ") + ")"
	case "CASE", "SIMPLE CASE":
		query, args := "CASE", e.Args
		if e.Op == "SIMPLE CASE" {
			query += " " + args[0].sql()
			args = args[1:]
		}
		for len(args) > 1 {
			query += " WHEN " + args[0].sql() + " THEN " + args[1].sql()
			args = args[2:]
		}
		if len(args) == 0 {
			return query + " END"
		}
		return query + " ELSE " + args[0].sql() + " END"
	default:
		if len(e.Args) == 2 {
			return "(" + e.Args[0].sql() + " " + e.Op + " " + e.Args[1].sql() + ")"
		}
	}
	panic(fmt.Sprintf("unmodeled T10 expression %s", e.Op))
}

func renderT10(q t10Query) (string, error) {
	columns := make([]string, 0, len(q.Columns))
	for _, column := range q.Columns {
		text := column.Expr.sql()
		if column.Alias != "" {
			text += " AS " + column.Alias
		}
		columns = append(columns, text)
	}
	query := "SELECT " + strings.Join(columns, ", ") + " FROM " + FixtureGroup + "." + T10FixtureName +
		" WHERE time >= ? AND time < ?"
	params := make([]any, 0, len(q.Params)+2)
	for _, value := range q.Params {
		params = append(params, jsonValue(value))
	}
	params = append(params, jsonValue(timePlaceholder(q.Begin)), jsonValue(timePlaceholder(q.End)))
	if q.Where != nil {
		renderer := t03Renderer{}
		where, err := renderer.predicate(q.Where)
		if err != nil {
			return "", err
		}
		query += " AND (" + where + ")"
	}
	query += " ORDER BY TIME ASC"
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
