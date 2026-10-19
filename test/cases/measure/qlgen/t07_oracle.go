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

func buildT07Case(name string, q t07Query, features ...string) (Case, error) {
	groups := make(map[string][]fixtureRow)
	var keys []string
	if len(q.GroupBy) == 0 {
		keys = append(keys, "[]")
		groups["[]"] = nil
	}
	var inputRows uint64
	for _, row := range t07Rows(q.Fixture) {
		if row.Millis < q.Begin || row.Millis >= q.End {
			continue
		}
		match, err := matchT03Time(q.Where, row.Millis*1_000_000, nil)
		if err != nil {
			return Case{}, err
		}
		if !match {
			continue
		}
		inputRows++
		values := make([]any, 0, len(q.GroupBy))
		for _, column := range q.GroupBy {
			values = append(values, rowValue(row, t07Source(column)))
		}
		// JSON is an independent typed tuple encoding: null and empty string
		// have different representations. No production group codec is used.
		encoded, err := json.Marshal(values)
		if err != nil {
			return Case{}, err
		}
		key := string(encoded)
		if _, exists := groups[key]; !exists {
			keys = append(keys, key)
		}
		groups[key] = append(groups[key], row)
	}
	var points []any
	for _, key := range keys {
		point, err := t07AggregatePoint(groups[key], q.Columns)
		if err != nil {
			return Case{}, err
		}
		points = append(points, point)
	}
	if q.Offset >= uint32(len(points)) {
		points = nil
	} else {
		points = points[q.Offset:]
	}
	if q.Limit != nil && uint32(len(points)) > *q.Limit {
		points = points[:*q.Limit]
	}
	query, err := renderT07(q)
	if err != nil {
		return Case{}, err
	}
	c, err := t07ExpectedCase(name, query, points, features)
	c.AggregateInputs, c.AggregateGroups = inputRows, uint64(len(keys))
	c.NoScan = q.Limit != nil && *q.Limit == 0
	return c, err
}

func t07ExpectedCase(name, query string, points []any, features []string) (Case, error) {
	want := map[string]any{}
	if len(points) > 0 {
		want["dataPoints"] = points
	}
	content, err := json.Marshal(want)
	if err != nil {
		return Case{}, err
	}
	return Case{Name: name, Ticket: "T07", Query: query, WantJSON: content, Features: features}, nil
}

func t07AggregatePoint(rows []fixtureRow, columns []t07Column) (map[string]any, error) {
	point := map[string]any{}
	var fields, tags []any
	for _, column := range columns {
		var value any
		if column.Function == "" {
			if len(rows) == 0 {
				return nil, fmt.Errorf("a scalar projection requires a group row")
			}
			value = rowValue(rows[0], t07Source(column.Target))
		} else {
			var err error
			value, err = t07Aggregate(rows, column)
			if err != nil {
				return nil, err
			}
		}
		if column.Function == "" && isTag(t07Source(column.Target)) {
			tags = append(tags, map[string]any{"key": column.name(), "value": jsonValue(value)})
		} else {
			fields = append(fields, map[string]any{"name": column.name(), "value": jsonValue(value)})
		}
	}
	if len(tags) > 0 {
		point["tagFamilies"] = []any{map[string]any{"name": "default", "tags": tags}}
	}
	if len(fields) > 0 {
		point["fields"] = fields
	}
	return point, nil
}

func t07Aggregate(rows []fixtureRow, column t07Column) (any, error) {
	seen := make(map[string]bool)
	var count, integerSum int64
	var sum, minimum, maximum float64
	integer := t07Source(column.Target) != "f"
	function := strings.ToUpper(column.Function)
	for _, row := range rows {
		value := rowValue(row, t07Source(column.Target))
		if value == nil {
			continue
		}
		if column.Distinct {
			encoded, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			key := string(encoded)
			if seen[key] {
				continue
			}
			seen[key] = true
		}
		count++
		if function == "COUNT" {
			continue
		}
		var number float64
		switch typed := value.(type) {
		case int64:
			number = float64(typed)
			integerSum += typed
		case float64:
			number = typed
		default:
			return nil, fmt.Errorf("%s requires a numeric fixture column", function)
		}
		sum += number
		if count == 1 || number < minimum {
			minimum = number
		}
		if count == 1 || number > maximum {
			maximum = number
		}
	}
	if function == "COUNT" {
		return count, nil
	}
	if count == 0 {
		return nil, nil
	}
	var result float64
	switch function {
	case "SUM":
		if integer {
			return integerSum, nil
		}
		return sum, nil
	case "AVG", "MEAN":
		return sum / float64(count), nil
	case "MIN":
		result = minimum
	case "MAX":
		result = maximum
	default:
		return nil, fmt.Errorf("unsupported fixture aggregate %s", function)
	}
	if integer {
		return int64(result), nil
	}
	return result, nil
}

func t07Source(column string) string {
	name, _, _ := strings.Cut(column, "::")
	_, field, qualified := strings.Cut(name, ".")
	if qualified {
		return field
	}
	return name
}
