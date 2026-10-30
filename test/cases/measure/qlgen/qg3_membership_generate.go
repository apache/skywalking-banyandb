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
	"fmt"
	"strings"
)

func generateQG3Membership() ([]AggregateScenario, error) {
	// The six-row fixture sums to 12; its null-only and empty windows both
	// yield NULL. These worked expectations do not use the query evaluator.
	const unknown = -1
	specs := []struct {
		name, left, list string
		truth            [3]int
	}{
		{"integer_first", "12", "SUM(DECIMAL_FROM_SCALED(v, 0))", [3]int{1, unknown, unknown}},
		{"fractional_mismatch", "12", "SUM(DECIMAL_FROM_SCALED(v, 1))", [3]int{0, unknown, unknown}},
		{"integer_aggregate", "SUM(v)", "0, SUM(DECIMAL_FROM_SCALED(v * 100, 2))", [3]int{1, unknown, unknown}},
		{"mixed_list_scales", "12", "SUM(DECIMAL_FROM_SCALED(v, 1)), SUM(DECIMAL_FROM_SCALED(v * 100, 2))", [3]int{1, unknown, unknown}},
		{"decimal_first", "SUM(DECIMAL_FROM_SCALED(v, 1))", "1, 2", [3]int{0, unknown, unknown}},
		{"different_scales", "SUM(DECIMAL_FROM_SCALED(v * 10, 1))", "SUM(DECIMAL_FROM_SCALED(v * 100, 2))", [3]int{1, unknown, unknown}},
		{"null_candidate", "12", "SUM(DECIMAL_FROM_SCALED(v, 1)), NULL", [3]int{unknown, unknown, unknown}},
		{"match_after_decimal", "12", "SUM(DECIMAL_FROM_SCALED(v, 1)), 12", [3]int{1, 1, 1}},
	}
	windows := [][2]int64{{-1, 6000}, {3000, 4000}, {7000, 8000}}
	var scenarios []AggregateScenario
	for _, spec := range specs {
		for _, operation := range []string{"IN", "NOT IN"} {
			name := "qg3_decimal_membership_" + spec.name + "_" + strings.ReplaceAll(strings.ToLower(operation), " ", "_")
			scenario := AggregateScenario{Name: name}
			for ordinal, window := range windows {
				query, err := renderT07(t07Query{
					Fixture: T07FixtureName, Columns: []t07Column{{Function: "SUM", Target: "v", Alias: "result"}},
					Begin: window[0], End: window[1],
				})
				if err != nil {
					return nil, err
				}
				query = strings.Replace(query, "SUM(v) AS result", "SUM(DECIMAL_FROM_SCALED(v, 1)) AS result", 1)
				predicate := fmt.Sprintf(" HAVING %s %s (%s)", spec.left, operation, spec.list)
				query = strings.Replace(query, "\n#!params:", predicate+"\n#!params:", 1)
				var points []any
				if spec.truth[ordinal] == 1 && operation == "IN" || spec.truth[ordinal] == 0 && operation == "NOT IN" {
					var value any
					if ordinal == 0 {
						value = "1.2"
					}
					points = append(points, map[string]any{"fields": []any{map[string]any{"name": "result", "value": jsonValue(value)}}})
				}
				c, err := t07ExpectedCase(fmt.Sprintf("%s_binding_%d", name, ordinal), query, points, []string{"qg3.decimal", "t07.rebind"})
				if err != nil {
					return nil, err
				}
				c.AggregateInputs, c.AggregateGroups = []uint64{6, 1, 0}[ordinal], 1
				scenario.Cases = append(scenario.Cases, c)
			}
			scenarios = append(scenarios, scenario)
		}
	}
	return scenarios, nil
}
