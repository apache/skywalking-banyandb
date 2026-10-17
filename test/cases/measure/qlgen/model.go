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

// Package qlgen generates QL-only integration contracts from a finite model.
package qlgen

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"google.golang.org/grpc/codes"
)

// FixtureName identifies the generated measure.
const FixtureName = "ql_generated"

// FixtureGroup identifies its existing integration group.
const FixtureGroup = "replicated_group"

// Query is a QL-first query model, independent of native query requests.
type Query struct {
	Limit               *uint32
	Top                 *uint32
	Where               *Predicate
	OrderBy             string
	Aggregate           string
	Target              string
	TimeOp              string
	Direction           string
	GroupBy             []string
	Columns             []string
	BucketMillis        int64
	BeginMillis         int64
	EndMillis           int64
	Offset              uint32
	Bind                bool
	Trace               bool
	ParenthesizedGroups bool
}

// Predicate is a scalar comparison or a boolean expression over fixture tags.
type Predicate struct {
	Op       string
	Column   string
	Values   []any
	Children []*Predicate
}

// Case contains a generated QL statement and independently evaluated result.
type Case struct {
	Grouping         *GroupingExpectation `json:"grouping,omitempty"`
	DistinctInputs   *uint64              `json:"distinct_inputs,omitempty"`
	Having           *HavingExpectation   `json:"having,omitempty"`
	Filter           *FilterExpectation   `json:"filter,omitempty"`
	Dimensions       map[string]string    `json:"dimensions,omitempty"`
	Ticket           string               `json:"ticket,omitempty"`
	ClockMillis      *int64               `json:"clock_millis,omitempty"`
	Name             string               `json:"name"`
	Query            string               `json:"query"`
	WantMessage      string               `json:"want_message,omitempty"`
	WantJSON         json.RawMessage      `json:"want,omitempty"`
	Features         []string             `json:"features"`
	AggregateInputs  uint64               `json:"aggregate_inputs,omitempty"`
	DistinctGroups   uint64               `json:"distinct_groups,omitempty"`
	AggregateGroups  uint64               `json:"aggregate_groups,omitempty"`
	WantCode         codes.Code           `json:"want_code,omitempty"`
	Ordered          bool                 `json:"ordered,omitempty"`
	Trace            bool                 `json:"trace,omitempty"`
	PreparedRejected bool                 `json:"prepared_rejected,omitempty"`
	NoScan           bool                 `json:"no_scan,omitempty"`
}

func matchesPredicate(p *Predicate, row fixtureRow) (bool, error) {
	if p == nil {
		return true, nil
	}
	op := strings.ToUpper(p.Op)
	if op == "AND" || op == "OR" {
		return matchLogical(p, row, op)
	}
	if !isTag(p.Column) {
		return false, fmt.Errorf("unsupported predicate column %q", p.Column)
	}
	if len(p.Values) == 0 && op != "IN" && op != "NOT IN" {
		return false, fmt.Errorf("predicate has no values")
	}
	for _, value := range p.Values {
		switch value.(type) {
		case string, int, int64:
		case nil:
			return false, fmt.Errorf("NULL predicates require an explicit QL null-semantics contract")
		default:
			return false, fmt.Errorf("unsupported literal %T", value)
		}
	}
	actual := rowValue(row, p.Column)
	switch op {
	case "=", "!=", "<", "<=", ">", ">=":
		if len(p.Values) != 1 {
			return false, fmt.Errorf("comparison needs one value")
		}
		comparison, compatible := compare(actual, p.Values[0])
		if !compatible {
			return false, nil
		}
		return compareResult(op, comparison), nil
	case "IN", "NOT IN":
		match := false
		for _, value := range p.Values {
			comparison, compatible := compare(actual, value)
			match = match || (compatible && comparison == 0)
		}
		if op == "NOT IN" {
			match = !match
		}
		return match, nil
	case "HAVING", "NOT HAVING":
		return matchesHaving(actual, p.Values, op == "NOT HAVING")
	case "MATCH":
		text, ok := actual.(string)
		if !ok {
			return false, fmt.Errorf("MATCH needs string tag")
		}
		match := true
		for _, value := range p.Values {
			term, valid := value.(string)
			if !valid {
				return false, fmt.Errorf("MATCH needs strings")
			}
			if len(term) != 1 {
				return false, fmt.Errorf("MATCH oracle supports single-character fixture terms")
			}
			match = match && strings.Contains(text, term)
		}
		return match, nil
	default:
		return false, fmt.Errorf("unsupported predicate %q", op)
	}
}

func compare(left, right any) (int, bool) {
	if left == nil || right == nil {
		return 0, left == nil && right == nil
	}
	if value, ok := left.(string); ok {
		other, valid := right.(string)
		if !valid {
			return 0, false
		}
		return strings.Compare(value, other), true
	}
	var a, b int64
	switch v := left.(type) {
	case int64:
		a = v
	case int:
		a = int64(v)
	default:
		return 0, false
	}
	switch v := right.(type) {
	case int64:
		b = v
	case int:
		b = int64(v)
	default:
		return 0, false
	}
	if a < b {
		return -1, true
	}
	if a > b {
		return 1, true
	}
	return 0, true
}

func validateQuery(name string, q Query) error {
	if q.Limit != nil && uint64(*q.Limit)+uint64(q.Offset) > 1<<32-1 {
		return fmt.Errorf("limit plus offset exceeds uint32 execution bound")
	}
	if name == "" {
		return fmt.Errorf("case name is empty")
	}
	if q.Direction != "" && direction(q.Direction) != "ASC" && direction(q.Direction) != "DESC" {
		return fmt.Errorf("unsupported direction %q", q.Direction)
	}
	if q.OrderBy != "" && !strings.EqualFold(q.OrderBy, "TIME") {
		return fmt.Errorf("oracle supports only TIME ordering")
	}
	if q.Top != nil && (q.Aggregate != "" || q.OrderBy != "" || q.Target != "v" && q.Target != "f") {
		return fmt.Errorf("TOP oracle supports raw numeric fields with no separate TIME ordering")
	}
	if q.BucketMillis < 0 || q.BucketMillis > 0 && q.Aggregate == "" {
		return fmt.Errorf("time bucket requires aggregation and positive width")
	}
	if (q.Limit != nil || q.Offset > 0) && q.OrderBy == "" && q.Top == nil {
		return fmt.Errorf("pagination requires explicit unique TIME ordering")
	}

	return nil
}

func matchLogical(p *Predicate, row fixtureRow, op string) (bool, error) {
	if len(p.Children) < 2 {
		return false, fmt.Errorf("%s needs two children", op)
	}
	result := op == "AND"
	for _, child := range p.Children {
		if child == nil {
			return false, fmt.Errorf("nil logical operand")
		}
		match, err := matchesPredicate(child, row)
		if err != nil {
			return false, err
		}
		if op == "AND" {
			result = result && match
		} else {
			result = result || match
		}
	}
	return result, nil
}

func compareResult(op string, comparison int) bool {
	switch op {
	case "=":
		return comparison == 0
	case "!=":
		return comparison != 0
	case "<":
		return comparison < 0
	case "<=":
		return comparison <= 0
	case ">":
		return comparison > 0
	default:
		return comparison >= 0
	}
}

func matchesHaving(actual any, wantedValues []any, negate bool) (bool, error) {
	values := reflect.ValueOf(actual)
	if values.Kind() != reflect.Slice {
		return false, fmt.Errorf("HAVING needs an array tag")
	}
	match := true
	for _, wanted := range wantedValues {
		found := false
		for i := 0; i < values.Len(); i++ {
			comparison, compatible := compare(values.Index(i).Interface(), wanted)
			found = found || (compatible && comparison == 0)
		}
		match = match && found
	}
	if negate {
		match = !match
	}
	return match, nil
}
