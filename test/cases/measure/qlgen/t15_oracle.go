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
	"math"
	"slices"
	"sort"
)

func buildT15Case(name string, q t15Query, features ...string) (Case, error) {
	query, err := renderT15(q)
	if err != nil {
		return Case{}, err
	}
	groups, err := collectT15Groups(q)
	if err != nil {
		return Case{}, err
	}
	seen := map[string]bool{}
	type visibleTuple struct {
		point  map[string]any
		values []any
	}
	var distinct []visibleTuple
	var points []any
	for _, rows := range groups {
		if q.Having != nil {
			expr, e := expandT14Aggregates(q.Having, rows)
			if e != nil {
				return Case{}, e
			}
			v, e := evalT10(expr, rows[0], q.Params)
			if e != nil {
				return Case{}, e
			}
			if v != true {
				continue
			}
		}
		columns, tuple, projectErr := projectT15Tuple(q, rows)
		if projectErr != nil {
			return Case{}, projectErr
		}
		data, e := json.Marshal(tuple)
		if e != nil {
			return Case{}, fmt.Errorf("encode independent tuple: %w", e)
		}
		if seen[string(data)] {
			continue
		}
		seen[string(data)] = true
		point, e := projectT10(rows[0], columns, q.Params)
		if e != nil {
			return Case{}, e
		}
		delete(point, "timestamp")
		distinct = append(distinct, visibleTuple{point: point, values: tuple})
	}
	if q.Order != "" {
		// The bounded order family admits TIME ASC as the first visible tuple slot.
		// Compare typed visible values rather than response JSON serialization.
		sort.SliceStable(distinct, func(i, j int) bool { return compareT10(distinct[i].values[0], distinct[j].values[0]) < 0 })
	}
	for _, tuple := range distinct {
		points = append(points, tuple.point)
	}

	unique := len(points)
	if q.Offset >= uint32(len(points)) {
		points = nil
	} else {
		points = points[q.Offset:]
	}
	if q.Limit != nil && int(*q.Limit) < len(points) {
		points = points[:*q.Limit]
	}
	c, err := t07ExpectedCase(name, query, points, features)
	c.Ticket = "T15"
	c.Ordered = q.Order != ""
	c.NoScan = q.Limit != nil && *q.Limit == 0
	c.DistinctGroups = uint64(unique)
	if q.Limit != nil && *q.Limit != 0 && q.Order == "" {
		c.DistinctGroups = min(c.DistinctGroups, uint64(q.Offset)+uint64(*q.Limit))
	}
	if slices.Contains(features, "t15.limit_early") {
		inputs := min(uint64(unique), 1)
		c.DistinctInputs = &inputs
	}
	if slices.Contains(features, "t15.offset_complete") {
		inputs := uint64(len(groups))
		c.DistinctInputs = &inputs
	}
	return c, err
}

func collectT15Groups(q t15Query) ([][]fixtureRow, error) {
	var groups [][]fixtureRow
	if len(q.GroupBy) > 0 || q.Having != nil {
		positions := map[string]int{}
		for _, row := range t15OracleRows() {
			if row.Millis < q.Begin || row.Millis >= q.End {
				continue
			}
			if q.Where != nil {
				value, e := evalT10(q.Where, row, q.Params)
				if e != nil {
					return nil, e
				}
				if value != true {
					continue
				}
			}
			var tuple []any
			for _, column := range q.GroupBy {
				tuple = append(tuple, rowValue(row, column))
			}
			data, marshalErr := json.Marshal(tuple)
			if marshalErr != nil {
				return nil, fmt.Errorf("encode independent group: %w", marshalErr)
			}
			key := string(data)
			position, found := positions[key]
			if !found {
				position = len(groups)
				positions[key] = position
				groups = append(groups, nil)
			}
			groups[position] = append(groups[position], row)
		}
	} else {
		for _, row := range t15OracleRows() {
			if row.Millis < q.Begin || row.Millis >= q.End {
				continue
			}
			if q.Where != nil {
				value, e := evalT10(q.Where, row, q.Params)
				if e != nil {
					return nil, e
				}
				if value != true {
					continue
				}
			}
			groups = append(groups, []fixtureRow{row})
		}
	}

	return groups, nil
}

func projectT15Tuple(q t15Query, rows []fixtureRow) ([]t10Column, []any, error) {
	columns := append([]t10Column(nil), q.Columns...)
	var tuple []any
	for ordinal, column := range columns {
		expression, e := expandT14Aggregates(column.Expr, rows)
		if e != nil {
			return nil, nil, e
		}
		value, e := evalT10(expression, rows[0], q.Params)
		if e != nil {
			return nil, nil, e
		}
		if number, ok := value.(float64); ok && number == 0 {
			value = float64(0)
		}
		if number, ok := value.(float64); ok && math.IsNaN(number) {
			tuple = append(tuple, struct{ NaN bool }{true})
		} else {
			tuple = append(tuple, value)
		}
		columns[ordinal].Expr = expression
	}

	return columns, tuple, nil
}
