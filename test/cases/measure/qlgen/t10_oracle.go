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
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
)

var errT10Value = errors.New("invalid expression value")

type t10Timestamp struct {
	Absolute string
	Millis   int64
}

type t10EpochNanos struct{ Millis int64 }

func buildT10Case(name string, q t10Query, features ...string) (Case, error) {
	query, err := renderT10(q)
	if err != nil {
		return Case{}, err
	}
	var points []any
	var skipped uint32
	for _, row := range t10Rows() {
		if row.Millis < q.Begin || row.Millis >= q.End {
			continue
		}
		match, matchErr := matchT03Time(q.Where, row.Millis*1_000_000, nil)
		if matchErr != nil {
			return Case{}, matchErr
		}
		if !match {
			continue
		}
		if skipped < q.Offset {
			skipped++
			continue
		}
		if q.Limit != nil && len(points) >= int(*q.Limit) {
			break
		}
		point, projectErr := projectT10(row, q.Columns, q.Params)
		if errors.Is(projectErr, errT10Value) {
			return Case{Name: name, Ticket: "T10", Query: query, Features: features, WantCode: codes.InvalidArgument}, nil
		}
		if projectErr != nil {
			return Case{}, projectErr
		}
		points = append(points, point)
	}
	c, err := t07ExpectedCase(name, query, points, features)
	c.Ticket, c.Ordered = "T10", true
	c.NoScan = q.Limit != nil && *q.Limit == 0
	return c, err
}

func projectT10(row fixtureRow, columns []t10Column, params []any) (map[string]any, error) {
	point := map[string]any{"timestamp": timePlaceholder(row.Millis)}
	var fields, families []any
	tags := make(map[string][]any)
	var familyOrder []string
	for ordinal, column := range columns {
		value, err := evalT10(column.Expr, row, params)
		if err != nil {
			return nil, err
		}
		name, family := column.Alias, ""
		if name == "" {
			name = fmt.Sprintf("expr_%d", ordinal+1)
		}
		if column.Expr.Op == "source" {
			source := column.Expr.Value.(string)
			if column.Alias == "" {
				name = source
			}
			if isTag(source) {
				family = "default"
			}
		}
		typeName := typeT10(column.Expr)
		if typeName == "TIMESTAMP" || typeName == "STRING_ARRAY" || typeName == "INT_ARRAY" {
			family = "__ql"
			if column.Expr.Op == "source" && typeName != "TIMESTAMP" {
				family = "default"
			}
		}
		var encoded any
		switch typed := value.(type) {
		case t10Timestamp:
			timestamp := typed.Absolute
			if timestamp == "" {
				timestamp = timePlaceholder(typed.Millis)
			}
			encoded = map[string]any{"timestamp": timestamp}
		case t10EpochNanos:
			encoded = map[string]any{"int": map[string]any{"value": fmt.Sprintf("{{unix_nanos:%d}}", typed.Millis)}}
		default:
			encoded = jsonValue(value)
		}
		if family == "" {
			fields = append(fields, map[string]any{"name": name, "value": encoded})
			continue
		}
		if _, exists := tags[family]; !exists {
			familyOrder = append(familyOrder, family)
		}
		tags[family] = append(tags[family], map[string]any{"key": name, "value": encoded})
	}
	for _, family := range familyOrder {
		families = append(families, map[string]any{"name": family, "tags": tags[family]})
	}
	if len(fields) > 0 {
		point["fields"] = fields
	}
	if len(families) > 0 {
		point["tagFamilies"] = families
	}
	return point, nil
}

func evalT10(e *t10Expr, row fixtureRow, params []any) (any, error) {
	switch e.Op {
	case "source":
		name := e.Value.(string)
		if name == "time" {
			return t10Timestamp{Millis: row.Millis}, nil
		}
		if name == "metric-value" || name == "rate_total" {
			name = "v"
		}
		value := rowValue(row, name)
		switch typed := value.(type) {
		case []int64:
			if typed == nil {
				return nil, nil
			}
		case []string:
			if typed == nil {
				return nil, nil
			}
		}
		return value, nil
	case "literal":
		return e.Value, nil
	case "parameter":
		return params[e.Value.(int)], nil
	case "CASE", "SIMPLE CASE", "COALESCE":
		return conditionalT10(e, row, params)
	}
	values := make([]any, 0, len(e.Args))
	for _, arg := range e.Args {
		value, err := evalT10(arg, row, params)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	switch e.Op {
	case "cast", "annotate":
		return castT10(values[0], e.Type)
	case "IS NULL":
		return values[0] == nil, nil
	case "IS NOT NULL":
		return values[0] != nil, nil
	case "IN", "NOT IN":
		result := inT10(values)
		if result != nil && e.Op == "NOT IN" {
			result = !result.(bool)
		}
		return result, nil
	case "AND", "OR":
		return logicalT10(e.Op, values[0], values[1]), nil
	}
	for _, value := range values {
		if value == nil {
			return nil, nil
		}
	}
	switch e.Op {
	case "NOT":
		return !values[0].(bool), nil
	case "positive":
		return values[0], nil
	case "negate":
		return arithmeticT10("-", int64(0), values[0])
	case "+", "-", "*", "/", "%":
		return arithmeticT10(e.Op, values[0], values[1])
	case "=", "!=", "<", "<=", ">", ">=":
		return compareResult(e.Op, compareT10(values[0], values[1])), nil
	}
	return nil, fmt.Errorf("unmodeled T10 evaluation %s", e.Op)
}

func conditionalT10(e *t10Expr, row fixtureRow, params []any) (any, error) {
	args := e.Args
	var base any
	if e.Op == "SIMPLE CASE" {
		var err error
		base, err = evalT10(args[0], row, params)
		if err != nil {
			return nil, err
		}
		args = args[1:]
	}
	for len(args) > 0 {
		value, err := evalT10(args[0], row, params)
		if err != nil {
			return nil, err
		}
		if e.Op == "COALESCE" {
			if value != nil {
				return promoteT10(value, typeT10(e))
			}
			args = args[1:]
			continue
		}
		if len(args) == 1 {
			return promoteT10(value, typeT10(e))
		}
		matches := value == true
		if e.Op == "SIMPLE CASE" {
			matches = base != nil && value != nil && compareT10(base, value) == 0
		}
		if matches {
			result, resultErr := evalT10(args[1], row, params)
			if resultErr != nil {
				return nil, resultErr
			}
			return promoteT10(result, typeT10(e))
		}
		args = args[2:]
	}
	return nil, nil
}

func arithmeticT10(op string, left, right any) (any, error) {
	integer, leftInt := left.(int64)
	other, rightInt := right.(int64)
	if leftInt && rightInt {
		if (op == "/" || op == "%") && other == 0 {
			return nil, fmt.Errorf("%w: zero division", errT10Value)
		}
		a, b, result := big.NewInt(integer), big.NewInt(other), new(big.Int)
		switch op {
		case "+":
			result.Add(a, b)
		case "-":
			result.Sub(a, b)
		case "*":
			result.Mul(a, b)
		case "/":
			result.Quo(a, b)
		case "%":
			result.Rem(a, b)
		}
		if !result.IsInt64() {
			return nil, fmt.Errorf("%w: integer overflow", errT10Value)
		}
		return result.Int64(), nil
	}
	a, b := floatT10(left), floatT10(right)
	if (op == "/" || op == "%") && b == 0 {
		return nil, fmt.Errorf("%w: zero division", errT10Value)
	}
	switch op {
	case "+":
		return a + b, nil
	case "-":
		return a - b, nil
	case "*":
		return a * b, nil
	case "/":
		return a / b, nil
	case "%":
		return math.Mod(a, b), nil
	}
	return nil, fmt.Errorf("unmodeled numeric operator %s", op)
}

func castT10(value any, target string) (any, error) {
	if value == nil {
		return nil, nil
	}
	target = strings.ToUpper(target)
	switch target {
	case "INT", "BIGINT":
		return castIntegerT10(value)
	case "FLOAT", "DOUBLE":
		switch typed := value.(type) {
		case int64:
			return float64(typed), nil
		case float64:
			return typed, nil
		case string:
			number, err := strconv.ParseFloat(typed, 64)
			if err == nil {
				return number, nil
			}
		}
	case "BOOLEAN", "BOOL":
		switch typed := value.(type) {
		case bool:
			return typed, nil
		case string:
			boolean, err := strconv.ParseBool(typed)
			if err == nil {
				return boolean, nil
			}
		case int64:
			return typed != 0, nil
		}
	case "STRING":
		switch typed := value.(type) {
		case string:
			return typed, nil
		case int64:
			return strconv.FormatInt(typed, 10), nil
		case float64:
			return strconv.FormatFloat(typed, 'g', -1, 64), nil
		case bool:
			return strconv.FormatBool(typed), nil
		case t10Timestamp:
			if typed.Absolute != "" {
				return typed.Absolute, nil
			}
			return timePlaceholder(typed.Millis), nil
		}
	case "TIMESTAMP":
		switch typed := value.(type) {
		case t10Timestamp:
			return typed, nil
		case int64:
			return t10Timestamp{Absolute: time.Unix(0, typed).UTC().Format(time.RFC3339Nano)}, nil
		case string:
			parsed, err := time.Parse(time.RFC3339Nano, typed)
			if err == nil {
				return t10Timestamp{Absolute: parsed.UTC().Format(time.RFC3339Nano)}, nil
			}
		}
	}
	return nil, fmt.Errorf("%w: cast %T to %s", errT10Value, value, target)
}

func castIntegerT10(value any) (any, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case bool:
		if typed {
			return int64(1), nil
		}
		return int64(0), nil
	case float64:
		if !math.IsNaN(typed) && !math.IsInf(typed, 0) {
			integer, _ := big.NewFloat(typed).Int(nil)
			if integer.IsInt64() {
				return integer.Int64(), nil
			}
		}
	case string:
		integer, err := strconv.ParseInt(typed, 10, 64)
		if err == nil {
			return integer, nil
		}
	case t10Timestamp:
		if typed.Absolute == "" {
			return t10EpochNanos{Millis: typed.Millis}, nil
		}
		parsed, _ := time.Parse(time.RFC3339Nano, typed.Absolute)
		return parsed.UnixNano(), nil
	}
	return nil, fmt.Errorf("%w: cast %T to BIGINT", errT10Value, value)
}

func logicalT10(op string, left, right any) any {
	if op == "AND" {
		if left == false || right == false {
			return false
		}
		if left == nil || right == nil {
			return nil
		}
		return true
	}
	if left == true || right == true {
		return true
	}
	if left == nil || right == nil {
		return nil
	}
	return false
}

func inT10(values []any) any {
	if len(values) == 1 {
		return false
	}
	hasNull := values[0] == nil
	for _, candidate := range values[1:] {
		if candidate == nil {
			hasNull = true
		} else if values[0] != nil && compareT10(values[0], candidate) == 0 {
			return true
		}
	}
	if hasNull {
		return nil
	}
	return false
}

func compareT10(left, right any) int {
	if value, ok := left.(int64); ok {
		if other, valid := right.(int64); valid {
			return big.NewInt(value).Cmp(big.NewInt(other))
		}
	}
	if value, ok := left.(string); ok {
		return strings.Compare(value, right.(string))
	}
	if value, ok := left.(bool); ok {
		if value == right.(bool) {
			return 0
		}
		if value {
			return 1
		}
		return -1
	}
	a, b := floatT10(left), floatT10(right)
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func floatT10(value any) float64 {
	if integer, ok := value.(int64); ok {
		return float64(integer)
	}
	return value.(float64)
}

func promoteT10(value any, target string) (any, error) {
	if target == "DOUBLE" {
		return castT10(value, target)
	}
	return value, nil
}

func typeT10(e *t10Expr) string {
	switch e.Op {
	case "cast", "annotate":
		if e.Type == "INT" {
			return "BIGINT"
		}
		if e.Type == "FLOAT" {
			return "DOUBLE"
		}
		return e.Type
	case "source":
		switch e.Value {
		case "v", "rid", "metric-value", "rate_total":
			return "BIGINT"
		case "f":
			return "DOUBLE"
		case "time":
			return "TIMESTAMP"
		case "labels":
			return "STRING_ARRAY"
		case "nums":
			return "INT_ARRAY"
		}
		return "STRING"
	case "literal":
		switch e.Value.(type) {
		case int64:
			return "BIGINT"
		case float64:
			return "DOUBLE"
		case bool:
			return "BOOLEAN"
		case string:
			return "STRING"
		}
		return "NULL"
	case "=", "!=", "<", "<=", ">", ">=", "AND", "OR", "NOT", "IN", "NOT IN", "IS NULL", "IS NOT NULL":
		return "BOOLEAN"
	}
	args := e.Args
	if e.Op == "SIMPLE CASE" {
		args = args[1:]
	}
	result := "NULL"
	for ordinal, arg := range args {
		if (e.Op == "CASE" || e.Op == "SIMPLE CASE") && ordinal%2 == 0 && ordinal != len(args)-1 {
			continue
		}
		typeName := typeT10(arg)
		if result == "NULL" || typeName == "DOUBLE" {
			result = typeName
		}
	}
	return result
}
