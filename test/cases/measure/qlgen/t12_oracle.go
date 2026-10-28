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
	"errors"
	"slices"

	"google.golang.org/grpc/codes"
)

func buildT12Case(name string, q t12Query, features ...string) (Case, error) {
	query, err := renderT12(q)
	if err != nil {
		return Case{}, err
	}
	c := Case{
		Filter: &FilterExpectation{}, Name: name, Ticket: "T12", Query: query,
		Features: features, Ordered: len(q.Aggregates) == 0, NoScan: q.Limit != nil && *q.Limit == 0,
	}
	selected := make([]fixtureRow, 0)
	for _, row := range t12Rows() {
		if row.Millis < q.Begin || row.Millis >= q.End || len(q.NativeRIDs) > 0 && !slices.Contains(q.NativeRIDs, row.RID) {
			continue
		}
		value, evalErr := evalT10(q.Predicate, row, q.Params)
		if errors.Is(evalErr, errT10Value) {
			c.WantCode = codes.InvalidArgument
			return c, nil
		}
		if evalErr != nil {
			return Case{}, evalErr
		}
		c.Filter.InputRows++
		switch value {
		case true:
			c.Filter.TrueRows++
			selected = append(selected, row)
		case false:
			c.Filter.FalseRows++
		default:
			c.Filter.UnknownRows++
		}
	}
	var points []any
	if len(q.Aggregates) > 0 {
		groups := make(map[string][]fixtureRow)
		keys := make([]string, 0)
		if len(q.GroupBy) == 0 {
			groups["[]"] = nil
			keys = append(keys, "[]")
		}
		for _, row := range selected {
			values := make([]any, 0, len(q.GroupBy))
			for _, column := range q.GroupBy {
				values = append(values, rowValue(row, column))
			}
			encoded, encodeErr := json.Marshal(values)
			if encodeErr != nil {
				return Case{}, encodeErr
			}
			key := string(encoded)
			if _, exists := groups[key]; !exists {
				keys = append(keys, key)
			}
			groups[key] = append(groups[key], row)
		}
		for _, key := range keys {
			point, aggregateErr := t07AggregatePoint(groups[key], q.Aggregates)
			if aggregateErr != nil {
				return Case{}, aggregateErr
			}
			points = append(points, point)
		}
		c.Filter.Aggregate = true
		c.AggregateInputs, c.AggregateGroups = uint64(len(selected)), uint64(len(keys))
	} else {
		for _, row := range selected {
			point, projectErr := projectT10(row, q.Columns, q.Params)
			if errors.Is(projectErr, errT10Value) {
				c.WantCode = codes.InvalidArgument
				return c, nil
			}
			if projectErr != nil {
				return Case{}, projectErr
			}
			points = append(points, point)
		}
	}
	if q.Offset >= uint32(len(points)) {
		points = nil
	} else {
		points = points[q.Offset:]
	}
	if q.Limit != nil && len(points) > int(*q.Limit) {
		points = points[:*q.Limit]
	}
	expected, encodeErr := t07ExpectedCase(name, query, points, features)
	if encodeErr != nil {
		return Case{}, encodeErr
	}
	c.WantJSON = expected.WantJSON
	// Final LIMIT may stop future batch pulls. Its row results prove complete
	// uncapped native input, while non-limited cases require all candidates counted.
	c.Filter.Complete = q.Limit == nil && !c.NoScan
	return c, nil
}
