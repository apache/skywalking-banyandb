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
	"strings"
)

type t17Tuple struct {
	Point map[string]any
	Keys  []any
}

// The finite reference uses stable insertion into complete tuples, independently
// of the production sort/heap algorithms. Key values are evaluated once per row.
func buildT17Case(name string, q t17Query, features ...string) (Case, error) {
	query, err := renderT17(q)
	if err != nil {
		return Case{}, err
	}
	tuples, grouped, aggregateInputs, aggregateGroups, filter, having, err := collectT17Tuples(q)
	if err != nil {
		return Case{}, err
	}
	for index := 1; index < len(tuples); index++ {
		tuple, position := tuples[index], index
		for position > 0 {
			less, compareErr := t17TupleBefore(tuple.Keys, tuples[position-1].Keys, q.Keys)
			if compareErr != nil {
				return Case{}, compareErr
			}
			if !less {
				break
			}
			tuples[position] = tuples[position-1]
			position--
		}
		tuples[position] = tuple
	}
	inputRows := uint64(len(tuples))
	start := min(uint64(q.Offset), inputRows)
	end := inputRows
	if q.Limit != nil {
		end = min(end, start+uint64(*q.Limit))
	}
	var points []any
	for _, tuple := range tuples[start:end] {
		points = append(points, tuple.Point)
	}
	c, err := t07ExpectedCase(name, query, points, features)
	c.Ticket, c.Ordered = "T17", true
	c.NoScan = q.Limit != nil && *q.Limit == 0 || q.Begin >= q.End
	if c.NoScan {
		inputRows = 0
	}
	c.Sorting = &SortingExpectation{
		InputRows: inputRows, OutputRows: uint64(len(points)),
		KeyExpressions: uint64(len(q.Keys)), KeyEvaluations: inputRows * uint64(len(q.Keys)), FullSort: q.Limit == nil, Grouped: grouped,
	}
	c.AggregateInputs, c.AggregateGroups = aggregateInputs, aggregateGroups
	if q.Where != nil {
		c.Filter = filter
	}
	if q.Having != nil {
		c.Having = having
	}
	if q.Distinct {
		c.DistinctGroups = inputRows
	}
	for _, key := range q.GroupBy {
		if key.Op == "ordinal" {
			key = q.Columns[key.Value.(int)-1].Expr
		}
		if key.Op == "bucket" {
			c.Sorting.BucketMetadata = !q.Distinct || t17BucketSelected(key, q.t16Query)
		}
	}
	if !grouped && !q.Distinct {
		for _, key := range q.Keys {
			if t17VisibleSortKey(key.Expr, q.t16Query) {
				continue
			}
			c.Sorting.KeyKernelRows += t17KernelCount(key.Expr) * inputRows
			if inputRows > 0 {
				c.Sorting.KeyScalarEvaluations += t17ScalarCount(key.Expr)
			}
		}
	}
	if q.Limit != nil {
		c.Sorting.K = uint64(q.Offset) + uint64(*q.Limit)
	}
	return c, err
}

func collectT17Tuples(q t17Query) ([]t17Tuple, bool, uint64, uint64, *FilterExpectation, *HavingExpectation, error) {
	grouped := len(q.GroupBy) > 0
	groups, aggregateInputs, filter, err := collectT17Input(q, grouped)
	if err != nil {
		return nil, false, 0, 0, nil, nil, err
	}
	var tuples []t17Tuple
	having := &HavingExpectation{InputGroups: uint64(len(groups))}
	seen := map[string]bool{}
	for _, rows := range groups {
		if q.Having != nil {
			expanded, expandErr := expandT16Expression(q.Having, rows, q.Fixture)
			if expandErr != nil {
				return nil, false, 0, 0, nil, nil, expandErr
			}
			truth, evalErr := evalT10(expanded, rows[0], q.Params)
			if evalErr != nil {
				return nil, false, 0, 0, nil, nil, evalErr
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
		columns := append([]t10Column(nil), q.Columns...)
		for position := range columns {
			columns[position].Expr, err = expandT16Expression(columns[position].Expr, rows, q.Fixture)
			if err != nil {
				return nil, false, 0, 0, nil, nil, err
			}
		}
		point, projectErr := projectT10(rows[0], columns, q.Params)
		if projectErr != nil {
			return nil, false, 0, 0, nil, nil, projectErr
		}
		if grouped || q.Distinct {
			delete(point, "timestamp")
		}
		if q.Distinct {
			encoded, encodeErr := json.Marshal(point)
			if encodeErr != nil {
				return nil, false, 0, 0, nil, nil, encodeErr
			}
			if seen[string(encoded)] {
				continue
			}
			seen[string(encoded)] = true
		}
		for _, key := range q.GroupBy {
			if key.Op == "ordinal" {
				key = q.Columns[key.Value.(int)-1].Expr
			}
			if key.Op == "bucket" && (!q.Distinct || t17BucketSelected(key, q.t16Query)) {
				width, widthErr := t16BucketWidth(key, q.Fixture)
				if widthErr != nil {
					return nil, false, 0, 0, nil, nil, widthErr
				}
				point["timestamp"] = timePlaceholder(t16BucketMillis(rows[0].Millis, width))
			}
		}
		tuple := t17Tuple{Point: point}
		for _, key := range q.Keys {
			expanded, expandErr := expandT16Expression(key.Expr, rows, q.Fixture)
			if expandErr != nil {
				return nil, false, 0, 0, nil, nil, expandErr
			}
			value, evalErr := evalT10(expanded, rows[0], q.Params)
			if evalErr != nil {
				return nil, false, 0, 0, nil, nil, evalErr
			}
			tuple.Keys = append(tuple.Keys, value)
		}
		tuples = append(tuples, tuple)
	}
	return tuples, grouped, aggregateInputs, uint64(len(groups)), filter, having, nil
}

func t17BucketSelected(key *t10Expr, query t16Query) bool {
	selected, err := t16BucketVisible(key, query)
	return err == nil && selected
}

func t17TupleBefore(left, right []any, keys []t17Key) (bool, error) {
	for position, key := range keys {
		a, b := left[position], right[position]
		if a == nil || b == nil {
			if a == nil && b == nil {
				continue
			}
			first := key.Nulls == "FIRST" || key.Nulls == "" && key.Direction == "DESC"
			return (a == nil) == first, nil
		}
		comparison, err := t17CompareValue(a, b)
		if err != nil {
			return false, err
		}
		if comparison == 0 {
			continue
		}
		if key.Direction == "DESC" {
			comparison = -comparison
		}
		return comparison < 0, nil
	}
	return false, nil
}

func t17CompareValue(left, right any) (int, error) {
	switch value := left.(type) {
	case int64:
		other, valid := right.(int64)
		if !valid {
			break
		}
		if value < other {
			return -1, nil
		}
		if value > other {
			return 1, nil
		}
		return 0, nil
	case string:
		other, valid := right.(string)
		if valid {
			return strings.Compare(value, other), nil
		}
	case float64:
		other, valid := right.(float64)
		if !valid {
			break
		}
		if math.IsNaN(value) {
			if math.IsNaN(other) {
				return 0, nil
			}
			return 1, nil
		}
		if math.IsNaN(other) {
			return -1, nil
		}
		if value < other {
			return -1, nil
		}
		if value > other {
			return 1, nil
		}
		return 0, nil
	case bool:
		other, valid := right.(bool)
		if valid {
			if value == other {
				return 0, nil
			}
			if !value {
				return -1, nil
			}
			return 1, nil
		}
	case t10Timestamp:
		other, valid := right.(t10Timestamp)
		if valid {
			return t17CompareValue(value.Millis, other.Millis)
		}
	}
	return 0, fmt.Errorf("incompatible finite sorting values %T and %T", left, right)
}

func t17KernelCount(expression *t10Expr) uint64 {
	if !t17HasSource(expression) {
		return 0
	}
	var count uint64
	if expression.Op != "source" {
		count++
	}
	for _, arg := range expression.Args {
		count += t17KernelCount(arg)
	}
	return count
}

func t17HasSource(expression *t10Expr) bool {
	if expression.Op == "source" {
		return true
	}
	for _, arg := range expression.Args {
		if t17HasSource(arg) {
			return true
		}
	}
	return false
}

func collectT17Input(q t17Query, grouped bool) ([][]fixtureRow, uint64, *FilterExpectation, error) {
	var groups [][]fixtureRow
	var aggregateInputs uint64
	var filter *FilterExpectation
	var err error
	if grouped {
		groups, _, aggregateInputs, _, filter, err = collectT16Groups(q.t16Query)
		if err != nil {
			return nil, 0, nil, err
		}
	} else {
		filter = &FilterExpectation{Complete: true}
		for _, row := range t17Rows(q.Fixture) {
			if row.Millis < q.Begin || row.Millis >= q.End {
				continue
			}
			filter.InputRows++
			if q.Where != nil {
				truth, evalErr := evalT10(q.Where, row, q.Params)
				if evalErr != nil {
					return nil, 0, nil, evalErr
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
			groups = append(groups, []fixtureRow{row})
		}
	}
	return groups, aggregateInputs, filter, nil
}

func t17ScalarCount(expression *t10Expr) uint64 {
	var count uint64
	if expression.Op != "literal" && expression.Op != "parameter" && expression.Op != "source" && !t17HasSource(expression) {
		count++
	}
	for _, arg := range expression.Args {
		count += t17ScalarCount(arg)
	}
	return count
}

func t17VisibleSortKey(key *t10Expr, query t16Query) bool {
	identity, err := t16ExpressionIdentity(key, query.Fixture)
	if err != nil {
		return false
	}
	for _, column := range query.Columns {
		candidate, candidateErr := t16ExpressionIdentity(column.Expr, query.Fixture)
		if candidateErr == nil && candidate == identity {
			return true
		}
	}
	return false
}
