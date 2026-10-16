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
	"sort"
	"strings"
)

// BuildCase renders a query and evaluates it without calling the QL implementation.
func BuildCase(name string, q Query, features ...string) (Case, error) {
	if err := validateQuery(name, q); err != nil {
		return Case{}, err
	}
	columns, err := projection(q.Columns)
	if err != nil {
		return Case{}, err
	}
	for _, column := range q.GroupBy {
		if !isTag(column) || column == "labels" || column == "nums" {
			return Case{}, fmt.Errorf("unsupported group key %q", column)
		}
	}
	rows := make([]fixtureRow, 0)
	for _, row := range fixtureRows() {
		matches, predicateErr := matchesPredicate(q.Where, row)
		if predicateErr != nil {
			return Case{}, predicateErr
		}
		inTime, timeErr := matchesTime(q, row.Millis)
		if timeErr != nil {
			return Case{}, timeErr
		}
		if matches && inTime {
			rows = append(rows, row)
		}
	}
	if direction(q.Direction) == "DESC" {
		sort.Slice(rows, func(i, j int) bool { return rows[i].Millis > rows[j].Millis })
	}
	if q.Top != nil {
		rows, err = topRows(q, rows)
		if err != nil {
			return Case{}, err
		}
	}
	var points []any
	if q.Aggregate != "" {
		points, err = aggregateRows(q, rows, columns)
	} else {
		for _, row := range rows {
			points = append(points, projectRow(row, columns, true))
		}
	}
	if err != nil {
		return Case{}, err
	}
	if q.Offset >= uint32(len(points)) {
		points = nil
	} else {
		points = points[q.Offset:]
	}
	if q.Limit != nil && *q.Limit > 0 && uint32(len(points)) > *q.Limit {
		points = points[:*q.Limit]
	}
	want := map[string]any{}
	if len(points) > 0 {
		want["dataPoints"] = points
	}
	data, err := json.Marshal(want)
	if err != nil {
		return Case{}, err
	}
	text, err := renderQuery(q)
	if err != nil {
		return Case{}, err
	}
	return Case{Name: name, Query: text, WantJSON: data, Features: features, Ordered: q.OrderBy != "" || q.Top != nil, Trace: q.Trace}, nil
}

func projection(columns []string) ([]string, error) {
	if len(columns) == 1 && columns[0] == "*" {
		return []string{"rid", "host", "zone", "labels", "nums", "v", "f"}, nil
	}
	result := make([]string, 0, len(columns))
	seen := map[string]bool{}
	for _, column := range columns {
		parts := strings.Split(column, "::")
		name := parts[0]
		if !isTag(name) && name != "v" && name != "f" {
			return nil, fmt.Errorf("unknown projection %q", column)
		}
		if len(parts) > 2 || len(parts) == 2 && !validAnnotation(name, parts[1]) {
			return nil, fmt.Errorf("incompatible type annotation %q", column)
		}
		if seen[name] {
			return nil, fmt.Errorf("repeated projection needs alias support: %q", column)
		}
		seen[name] = true
		result = append(result, name)
	}
	return result, nil
}

func validAnnotation(name, annotation string) bool {
	switch strings.ToUpper(annotation) {
	case "TAG":
		return isTag(name)
	case "FIELD":
		return !isTag(name)
	default:
		return false
	}
}

func matchesTime(q Query, millis int64) (bool, error) {
	switch strings.ToUpper(q.TimeOp) {
	case "", "BETWEEN":
		return millis >= q.BeginMillis && millis <= q.EndMillis, nil
	case "=":
		return millis == q.BeginMillis, nil
	case ">":
		return millis > q.BeginMillis, nil
	case ">=":
		return millis >= q.BeginMillis, nil
	case "<":
		return millis < q.BeginMillis, nil
	case "<=":
		return millis <= q.BeginMillis, nil
	default:
		return false, fmt.Errorf("unsupported time operator %q", q.TimeOp)
	}
}

func aggregateRows(q Query, rows []fixtureRow, columns []string) ([]any, error) {
	function := strings.ToUpper(q.Aggregate)
	if function != "SUM" && function != "MIN" && function != "MAX" && function != "COUNT" && function != "COUNT_DISTINCT" && function != "AVG" && function != "MEAN" {
		return nil, fmt.Errorf("unsupported aggregate %q", function)
	}
	if q.Target != "v" && q.Target != "f" && ((function != "COUNT" && function != "COUNT_DISTINCT") || !isTag(q.Target)) {
		return nil, fmt.Errorf("unsupported aggregate target %q", q.Target)
	}
	if q.OrderBy != "" {
		return nil, fmt.Errorf("TIME order after aggregation is not independently defined")
	}
	for _, column := range columns {
		if isTag(column) {
			found := false
			for _, key := range q.GroupBy {
				found = found || column == key
			}
			if !found {
				return nil, fmt.Errorf("projection %q is not a grouping key", column)
			}
		} else if column != q.Target {
			return nil, fmt.Errorf("unaggregated field %q", column)
		}
	}
	buckets := map[string][]fixtureRow{}
	var keys []string
	for _, row := range rows {
		var values []any
		if q.BucketMillis > 0 {
			values = append(values, row.Millis/q.BucketMillis*q.BucketMillis)
		}
		for _, key := range q.GroupBy {
			values = append(values, rowValue(row, key))
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return nil, fmt.Errorf("encode oracle grouping key: %w", err)
		}
		key := string(encoded)
		if _, ok := buckets[key]; !ok {
			keys = append(keys, key)
		}
		buckets[key] = append(buckets[key], row)
	}
	sort.Strings(keys)
	var points []any
	for _, key := range keys {
		group := buckets[key]
		value, err := aggregateValue(q, function, group)
		if err != nil {
			return nil, err
		}
		point := projectRow(group[0], q.GroupBy, false)
		if q.BucketMillis > 0 {
			point["timestamp"] = timePlaceholder(group[0].Millis / q.BucketMillis * q.BucketMillis)
		}
		if q.Target == "f" {
			if number, ok := value.(int64); ok {
				value = float64(number)
			}
		}
		point["fields"] = []any{map[string]any{"name": q.Target, "value": jsonValue(value)}}
		points = append(points, point)
	}
	return points, nil
}

func numericValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}

func aggregateValue(q Query, function string, group []fixtureRow) (any, error) {
	count := int64(0)
	sum, minimum, maximum := float64(0), float64(0), float64(0)
	distinct := map[string]bool{}
	for _, row := range group {
		value := rowValue(row, q.Target)
		if value == nil {
			continue
		}
		data, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode oracle distinct value: %w", err)
		}
		distinct[string(data)] = true
		var numeric float64
		switch typed := value.(type) {
		case int64:
			numeric = float64(typed)
		case float64:
			numeric = typed
		}
		if count == 0 || numeric < minimum {
			minimum = numeric
		}
		if count == 0 || numeric > maximum {
			maximum = numeric
		}
		count++
		sum += numeric
	}
	var value any
	switch function {
	case "COUNT":
		value = count
	case "COUNT_DISTINCT":
		value = int64(len(distinct))
	case "SUM":
		if count > 0 {
			value = sum
		}
	case "MIN":
		if count > 0 {
			value = minimum
		}
	case "MAX":
		if count > 0 {
			value = maximum
		}
	case "AVG", "MEAN":
		if count > 0 {
			value = sum / float64(count)
		}
	}
	// Legacy numeric aggregation retains the input field's integer type.
	if numeric, ok := value.(float64); ok && q.Target == "v" {
		value = int64(numeric)
	}

	return value, nil
}

func topRows(q Query, rows []fixtureRow) ([]fixtureRow, error) {
	if q.Top != nil {
		seen := map[float64]bool{}
		for _, row := range rows {
			value, ok := numericValue(rowValue(row, q.Target))
			if !ok || seen[value] {
				return nil, fmt.Errorf("TOP fixture must have unique nonnull numeric values")
			}
			seen[value] = true
		}
		sort.Slice(rows, func(i, j int) bool {
			left, _ := numericValue(rowValue(rows[i], q.Target))
			right, _ := numericValue(rowValue(rows[j], q.Target))
			if direction(q.Direction) == "DESC" {
				return left > right
			}
			return left < right
		})
		if uint32(len(rows)) > *q.Top {
			rows = rows[:*q.Top]
		}
	}

	return rows, nil
}
