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
	"slices"
	"strings"
	"time"
)

func buildT16Case(name string, q t16Query, features ...string) (Case, error) {
	query, err := renderT16(q)
	if err != nil {
		return Case{}, err
	}
	groups, keys, inputRows, computed, filter, err := collectT16Groups(q)
	if err != nil {
		return Case{}, err
	}
	var points []any
	seen := map[string]bool{}
	having := &HavingExpectation{InputGroups: uint64(len(groups))}
	for _, rows := range groups {
		if q.Having != nil {
			expression, expandErr := expandT16Expression(q.Having, rows, q.Fixture)
			if expandErr != nil {
				return Case{}, expandErr
			}
			truth, evalErr := evalT10(expression, rows[0], q.Params)
			if evalErr != nil {
				return Case{}, evalErr
			}
			if truth != true {
				if truth == nil {
					having.UnknownGroups++
				} else {
					having.FalseGroups++
				}
				continue
			}
			having.TrueGroups++
		}
		columns := expandT16Columns(q)
		for position := range columns {
			columns[position].Expr, err = expandT16Expression(columns[position].Expr, rows, q.Fixture)
			if err != nil {
				return Case{}, err
			}
		}
		point, projectErr := projectT10(rows[0], columns, q.Params)
		if projectErr != nil {
			return Case{}, projectErr
		}
		delete(point, "timestamp")
		if q.Distinct {
			// Only projected values form the DISTINCT tuple. Source metadata and
			// hidden bucket keys cannot distinguish otherwise equal visible rows.
			encoded, encodeErr := json.Marshal(point)
			if encodeErr != nil {
				return Case{}, encodeErr
			}
			if seen[string(encoded)] {
				continue
			}
			seen[string(encoded)] = true
		}
		for _, key := range keys {
			if key.Op != "bucket" {
				continue
			}
			visible, visibleErr := t16BucketVisible(key, q)
			if visibleErr != nil {
				return Case{}, visibleErr
			}
			if q.Distinct && !visible {
				continue
			}
			width, widthErr := t16BucketWidth(key, q.Fixture)
			if widthErr != nil {
				return Case{}, widthErr
			}
			point["timestamp"] = timePlaceholder(t16BucketMillis(rows[0].Millis, width))
		}
		points = append(points, point)
	}
	c, err := t07ExpectedCase(name, query, points, features)
	c.Ticket = "T16"
	c.AggregateInputs, c.AggregateGroups = inputRows, uint64(len(groups))
	c.Grouping = &GroupingExpectation{InputRows: inputRows, KeyExpressions: computed, KeyEvaluations: computed * inputRows}
	if q.Where != nil {
		c.Filter = filter
	}
	if q.Having != nil {
		c.Having = having
	}
	if q.Distinct {
		c.DistinctGroups = uint64(len(points))
	}
	return c, err
}

func collectT16Groups(q t16Query) ([][]fixtureRow, []*t10Expr, uint64, uint64, *FilterExpectation, error) {
	var groups [][]fixtureRow
	positions := map[string]int{}
	var keys []*t10Expr
	computed := map[string]bool{}
	columns := expandT16Columns(q)
	for _, expression := range q.GroupBy {
		if expression.Op == "ordinal" {
			expression = columns[expression.Value.(int)-1].Expr
		}
		keys = append(keys, expression)
		if expression.Op != "source" {
			identity, identityErr := t16ExpressionIdentity(expression, q.Fixture)
			if identityErr != nil {
				return nil, nil, 0, 0, nil, identityErr
			}
			computed[identity] = true
		}
	}
	var inputRows uint64
	filter := &FilterExpectation{Complete: true, Aggregate: true}
	for _, row := range t16Rows(q.Fixture) {
		if row.Millis < q.Begin || row.Millis >= q.End || (len(q.Hosts) > 0 && !slices.Contains(q.Hosts, row.Host)) {
			continue
		}
		filter.InputRows++
		if q.Where != nil {
			truth, evalErr := evalT10(q.Where, row, q.Params)
			if evalErr != nil {
				return nil, nil, 0, 0, nil, evalErr
			}
			if truth != true {
				if truth == nil {
					filter.UnknownRows++
				} else {
					filter.FalseRows++
				}
				continue
			}
		}
		filter.TrueRows++
		inputRows++
		var tuple []any
		for _, expression := range keys {
			expanded, expandErr := expandT16Expression(expression, []fixtureRow{row}, q.Fixture)
			if expandErr != nil {
				return nil, nil, 0, 0, nil, expandErr
			}
			value, evalErr := evalT10(expanded, row, q.Params)
			if evalErr != nil {
				return nil, nil, 0, 0, nil, evalErr
			}
			tuple = append(tuple, value)
		}
		encoded, encodeErr := json.Marshal(tuple)
		if encodeErr != nil {
			return nil, nil, 0, 0, nil, fmt.Errorf("encode independent T16 grouping tuple: %w", encodeErr)
		}
		key := string(encoded)
		position, found := positions[key]
		if !found {
			position = len(groups)
			positions[key] = position
			groups = append(groups, nil)
		}
		groups[position] = append(groups[position], row)
	}
	return groups, keys, inputRows, uint64(len(computed)), filter, nil
}

func expandT16Expression(expression *t10Expr, rows []fixtureRow, fixture string) (*t10Expr, error) {
	switch expression.Op {
	case "aggregate":
		value, err := t07Aggregate(rows, expression.Value.(t07Column))
		return t10Literal(value), err
	case "bucket":
		width, err := t16BucketWidth(expression, fixture)
		if err != nil {
			return nil, err
		}
		return t10Cast(t10Literal(t10Timestamp{Millis: t16BucketMillis(rows[0].Millis, width)}), "TIMESTAMP"), nil
	}
	clone := *expression
	clone.Args = make([]*t10Expr, 0, len(expression.Args))
	for _, arg := range expression.Args {
		expanded, err := expandT16Expression(arg, rows, fixture)
		if err != nil {
			return nil, err
		}
		clone.Args = append(clone.Args, expanded)
	}
	return &clone, nil
}

type t16Identity struct {
	Value any
	Op    string
	Type  string
	Args  []t16Identity
}

// Model nodes retain semantic source names and parameter positions separately
// from Text, which controls only rendering. Resolved widths likewise give the
// finite oracle a structural identity without parsing SQL or using engine plans.
func t16ExpressionIdentity(expression *t10Expr, fixture string) (string, error) {
	identity, err := t16CanonicalExpression(expression, fixture)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(identity)
	return string(encoded), err
}

func t16CanonicalExpression(expression *t10Expr, fixture string) (t16Identity, error) {
	identity := t16Identity{Op: expression.Op, Type: expression.Type, Value: expression.Value}
	switch identity.Type {
	case "INT":
		identity.Type = "BIGINT"
	case "FLOAT":
		identity.Type = "DOUBLE"
	case "BOOL":
		identity.Type = "BOOLEAN"
	}
	switch expression.Op {
	case "source":
		name, _, _ := strings.Cut(expression.Value.(string), "::")
		name = strings.TrimPrefix(name, "default.")
		identity.Value = name
		identity.Type = typeT10(t10Source(name))
	case "literal":
		identity.Type = typeT10(expression)
	case "bucket":
		width, err := t16BucketWidth(expression, fixture)
		if err != nil {
			return t16Identity{}, err
		}
		identity.Value, identity.Type = width, "TIMESTAMP"
	}
	for _, arg := range expression.Args {
		child, err := t16CanonicalExpression(arg, fixture)
		if err != nil {
			return t16Identity{}, err
		}
		identity.Args = append(identity.Args, child)
	}
	return identity, nil
}

func t16BucketWidth(expression *t10Expr, fixture string) (int64, error) {
	if width := expression.Value.(int64); width > 0 {
		return width, nil
	}
	width, err := time.ParseDuration(T16FixtureSchema(fixture).Interval)
	if err != nil || width <= 0 || width%time.Millisecond != 0 {
		return 0, fmt.Errorf("invalid finite fixture bucket interval %q", T16FixtureSchema(fixture).Interval)
	}
	return width.Milliseconds(), nil
}

func t16BucketVisible(bucket *t10Expr, query t16Query) (bool, error) {
	identity, err := t16ExpressionIdentity(bucket, query.Fixture)
	if err != nil {
		return false, err
	}
	for _, column := range expandT16Columns(query) {
		candidate, candidateErr := t16ExpressionIdentity(column.Expr, query.Fixture)
		if candidateErr != nil {
			return false, candidateErr
		}
		if candidate == identity {
			return true, nil
		}
	}
	return false, nil
}

// The T16 runner aligns its positive fixture base to six seconds. All generated
// widths divide that base, so this independent offset calculation has the same
// origin as the established native epoch buckets, including unaligned ranges.
func t16BucketMillis(millis, width int64) int64 {
	return millis / width * width
}
