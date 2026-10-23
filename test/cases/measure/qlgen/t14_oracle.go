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

	"google.golang.org/grpc/codes"
)

func buildT14Case(name string, q t14Query, features ...string) (Case, error) {
	query, renderErr := renderT14(q)
	if renderErr != nil {
		return Case{}, renderErr
	}
	if q.Membership != nil {
		if _, bindingErr := t14MembershipValues(q.Membership); bindingErr != nil {
			return Case{
				Name: name, Ticket: "T14", Query: query, WantCode: codes.InvalidArgument, NoScan: true,
				PreparedRejected: t14EmptyArrayBinding(q.Membership), Features: []string{"t14.invalid_membership"},
			}, nil
		}
	}
	groups, keys, inputRows, filter, collectErr := collectT14Groups(q)
	if collectErr != nil {
		return Case{}, collectErr
	}
	expectation := &HavingExpectation{InputGroups: uint64(len(keys)), RawFilterRows: filter.InputRows, RawTrueRows: inputRows}
	var points []any
	for _, key := range keys {
		rows := groups[key]
		if q.Having != nil {
			expression, err := expandT14Aggregates(q.Having, rows)
			if err != nil {
				return Case{}, err
			}
			row := fixtureRow{}
			if len(rows) > 0 {
				row = rows[0]
			}
			truth, evalErr := evalT10(expression, row, q.Params)
			if evalErr != nil {
				return Case{}, evalErr
			}
			if truth == nil {
				expectation.UnknownGroups++
				continue
			}
			if truth != true {
				expectation.FalseGroups++
				continue
			}
			expectation.TrueGroups++
		}
		point, err := t07AggregatePoint(rows, q.Columns)
		if err != nil {
			return Case{}, err
		}
		if q.Having == nil && len(q.GroupBy) == 0 {
			point["timestamp"] = timePlaceholder(rows[0].Millis)
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
	c, err := t07ExpectedCase(name, query, points, features)
	c.Ticket = "T14"
	c.Ordered = q.Having == nil && len(q.GroupBy) == 0
	c.AggregateInputs = inputRows
	c.AggregateGroups = uint64(len(keys))
	if q.Having != nil {
		c.Having = expectation
	}
	if q.Where != nil || q.Membership != nil {
		c.Filter = filter
	}
	c.NoScan = q.Limit != nil && *q.Limit == 0
	return c, err
}

func expandT14Aggregates(expression *t10Expr, rows []fixtureRow) (*t10Expr, error) {
	if expression.Op == "aggregate" {
		value, err := t07Aggregate(rows, expression.Value.(t07Column))
		if err != nil {
			return nil, err
		}
		return t10Literal(value), nil
	}
	clone := *expression
	clone.Args = make([]*t10Expr, 0, len(expression.Args))
	for _, arg := range expression.Args {
		expanded, err := expandT14Aggregates(arg, rows)
		if err != nil {
			return nil, err
		}
		clone.Args = append(clone.Args, expanded)
	}
	return &clone, nil
}

func matchT14Membership(row fixtureRow, membership *t14Membership) (bool, error) {
	values, err := t14MembershipValues(membership)
	if err != nil {
		return false, err
	}
	actual := rowValue(row, membership.Column)
	match := false
	switch typed := actual.(type) {
	case []string:
		if len(typed) > 0 {
			match = true
			for _, wanted := range values {
				text, valid := wanted.(string)
				found := false
				for _, value := range typed {
					found = found || (valid && value == text)
				}
				match = match && found
			}
		}
	case []int64:
		if len(typed) > 0 {
			match = true
			for _, wanted := range values {
				number, valid := wanted.(int64)
				found := false
				for _, value := range typed {
					found = found || (valid && value == number)
				}
				match = match && found
			}
		}
	default:
		// The active measure index requires every encoded term by value. Duplicate
		// required terms therefore do not change scalar membership.
		match = len(values) > 0 && actual != nil
		for _, wanted := range values {
			comparison, compatible := compare(actual, wanted)
			match = match && compatible && comparison == 0
		}
	}
	if strings.HasPrefix(membership.Operator, "NOT ") {
		match = !match
	}
	return match, nil
}

func t14MembershipValues(membership *t14Membership) ([]any, error) {
	var values []any
	for _, value := range membership.Values {
		switch typed := value.(type) {
		case []string:
			if len(typed) == 0 {
				return nil, fmt.Errorf("empty legacy array binding")
			}
			for _, item := range typed {
				values = append(values, item)
			}
		case []int64:
			if len(typed) == 0 {
				return nil, fmt.Errorf("empty legacy array binding")
			}
			for _, item := range typed {
				values = append(values, item)
			}
		default:
			values = append(values, typed)
		}
	}
	list := (membership.Form != "single" && membership.Form != "literal-single") || len(values) != 1
	for index, value := range values {
		if value == nil {
			if list {
				return nil, fmt.Errorf("NULL in legacy membership list")
			}
			continue
		}
		// Legacy tag typing converts integer literals to string tags and decimal
		// string literals to integer tags before membership is evaluated.
		if membership.Column == "labels" || membership.Column == "host" {
			if number, ok := value.(int64); ok {
				values[index] = strconv.FormatInt(number, 10)
			}
		} else if text, ok := value.(string); ok {
			number, err := strconv.ParseInt(text, 10, 64)
			if err != nil {
				return nil, err
			}
			values[index] = number
		}
	}
	return values, nil
}

func collectT14Groups(q t14Query) (map[string][]fixtureRow, []string, uint64, *FilterExpectation, error) {
	groups := make(map[string][]fixtureRow)
	var keys []string
	if len(q.GroupBy) == 0 && q.Having != nil {
		keys = append(keys, "[]")
		groups["[]"] = nil
	}
	var inputRows, rawRows, rawFalse, rawUnknown uint64
	for _, row := range t14Rows(q.Fixture) {
		if row.Millis < q.Begin || row.Millis >= q.End {
			continue
		}
		if len(q.Hosts) > 0 {
			found := false
			for _, host := range q.Hosts {
				found = found || row.Host == host
			}
			if !found {
				continue
			}
		}
		if q.Membership != nil || q.Where != nil {
			rawRows++
		}
		if q.Membership != nil {
			match, err := matchT14Membership(row, q.Membership)
			if err != nil {
				return nil, nil, 0, nil, err
			}
			if !match {
				rawFalse++
				continue
			}
		}
		if q.Where != nil {
			truth, err := evalT10(q.Where, row, q.Params)
			if err != nil {
				return nil, nil, 0, nil, err
			}
			if truth != true {
				if truth == nil {
					rawUnknown++
				} else {
					rawFalse++
				}
				continue
			}
		}
		inputRows++
		values := make([]any, 0, len(q.GroupBy))
		if q.Having == nil && len(q.GroupBy) == 0 {
			values = append(values, row.RID)
		} else {
			for _, column := range q.GroupBy {
				values = append(values, rowValue(row, column))
			}
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return nil, nil, 0, nil, err
		}
		key := string(encoded)
		if _, exists := groups[key]; !exists {
			keys = append(keys, key)
		}
		groups[key] = append(groups[key], row)
	}
	return groups, keys, inputRows, &FilterExpectation{
		InputRows: rawRows, TrueRows: inputRows, FalseRows: rawFalse, UnknownRows: rawUnknown, Complete: true, Aggregate: q.Having != nil,
	}, nil
}

func t14EmptyArrayBinding(membership *t14Membership) bool {
	for _, value := range membership.Values {
		switch typed := value.(type) {
		case []string:
			if typed != nil && len(typed) == 0 {
				return true
			}
		case []int64:
			if typed != nil && len(typed) == 0 {
				return true
			}
		}
	}
	return false
}
