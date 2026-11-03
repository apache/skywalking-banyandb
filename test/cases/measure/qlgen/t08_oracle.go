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
	"math/big"
	"slices"
	"strings"
)

func buildT08Case(name string, q t08Query, features ...string) (Case, error) {
	groups := make(map[string][]t08Row)
	var keys []string
	if len(q.GroupBy) == 0 {
		keys = append(keys, "[]")
		groups["[]"] = nil
	}
	var inputRows uint64
	for _, row := range t08Rows(q.Fixture) {
		if row.Millis < q.Begin || row.Millis >= q.End ||
			(len(q.Hosts) > 0 && !slices.Contains(q.Hosts, row.Host)) ||
			(len(q.Regions) > 0 && !slices.Contains(q.Regions, row.Region)) ||
			(q.Key != nil && row.Key != *q.Key) {
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
		var values []any
		for _, column := range q.GroupBy {
			values = append(values, t08Value(row, column))
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return Case{}, err
		}
		key := string(encoded)
		if len(q.GroupBy) == 0 {
			key = "[]"
		}
		if _, exists := groups[key]; !exists {
			keys = append(keys, key)
		}
		groups[key] = append(groups[key], row)
	}
	var points []any
	for _, key := range keys {
		point, err := t08AggregatePoint(groups[key], q.Columns)
		if err != nil {
			return Case{}, err
		}
		points = append(points, point)
	}
	if q.Limit != nil && uint32(len(points)) > *q.Limit {
		points = points[:*q.Limit]
	}
	query, err := renderT08(q)
	if err != nil {
		return Case{}, err
	}
	c, err := t07ExpectedCase(name, query, points, features)
	c.Ticket, c.AggregateInputs, c.AggregateGroups = "T08", inputRows, uint64(len(keys))
	c.NoScan = q.Limit != nil && *q.Limit == 0
	return c, err
}

func t08AggregatePoint(rows []t08Row, columns []t07Column) (map[string]any, error) {
	point := map[string]any{}
	var fields, tags []any
	for _, column := range columns {
		var value any
		if column.Function == "" {
			if len(rows) == 0 {
				return nil, fmt.Errorf("a scalar projection requires a T08 group row")
			}
			value = t08Value(rows[0], column.Target)
		} else {
			var err error
			value, err = t08Aggregate(rows, column)
			if err != nil {
				return nil, err
			}
		}
		if column.Function == "" && t07Source(column.Target) != "v" && t07Source(column.Target) != "f" {
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

func t08Aggregate(rows []t08Row, column t07Column) (any, error) {
	function := strings.ToUpper(column.Function)
	var count int64
	var first any
	sum := new(big.Rat)
	var lowInt, highInt int64
	var lowFloat, highFloat float64
	for _, row := range rows {
		value := t08Value(row, column.Target)
		if value == nil {
			continue
		}
		count++
		if function == "COUNT" {
			continue
		}
		switch typed := value.(type) {
		case int64:
			sum.Add(sum, new(big.Rat).SetInt64(typed))
			if first == nil || typed < lowInt {
				lowInt = typed
			}
			if first == nil || typed > highInt {
				highInt = typed
			}
		case float64:
			sum.Add(sum, new(big.Rat).SetFloat64(typed))
			if first == nil || typed < lowFloat {
				lowFloat = typed
			}
			if first == nil || typed > highFloat {
				highFloat = typed
			}
		default:
			return nil, fmt.Errorf("%s requires a numeric T08 fixture column", function)
		}
		first = value
	}
	if function == "COUNT" {
		return count, nil
	}
	if count == 0 {
		return nil, nil
	}
	_, integer := first.(int64)
	switch function {
	case "SUM":
		if integer {
			if !sum.IsInt() || !sum.Num().IsInt64() {
				return nil, fmt.Errorf("T08 integer sum exceeds int64")
			}
			return sum.Num().Int64(), nil
		}
		value, _ := sum.Float64()
		return value, nil
	case "AVG", "MEAN":
		value, _ := sum.Quo(sum, new(big.Rat).SetInt64(count)).Float64()
		return value, nil
	case "MIN":
		if integer {
			return lowInt, nil
		}
		return lowFloat, nil
	case "MAX":
		if integer {
			return highInt, nil
		}
		return highFloat, nil
	default:
		return nil, fmt.Errorf("unsupported T08 fixture aggregate %s", function)
	}
}

func t08Value(row t08Row, column string) any {
	switch t07Source(column) {
	case "rid":
		return row.RID
	case "host":
		return row.Host
	case "region":
		return row.Region
	case "key":
		return row.Key
	case "v":
		return row.V
	case "f":
		return row.F
	}
	return nil
}
