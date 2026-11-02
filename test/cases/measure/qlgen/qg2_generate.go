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

// generateQG2Composition uses literal expectations from the six-row fixture,
// including a single all-null row and an empty time window.
func generateQG2Composition(b *suiteBuilder) ([]AggregateScenario, error) {
	windows := [][2]int64{{-1, 6000}, {3000, 4000}, {7000, 8000}}
	scenarios := []struct {
		name, projection string
		values           []any
	}{
		{"coalesce_sum", "COALESCE(SUM(v), 0) AS result", []any{int64(12), int64(0), int64(0)}},
		{"depth_limit", strings.Repeat("(", 62) + "SUM(v)" + strings.Repeat(")", 62) + " AS result", []any{int64(12), nil, nil}},
		{"repeated_aggregate", "COALESCE(SUM(v), 0) + COALESCE(SUM(v), 0) AS result", []any{int64(24), int64(0), int64(0)}},
		{"expression_argument", "SUM(COALESCE(v, 0) + v) AS result", []any{int64(24), nil, nil}},
		{"float_promotion", "SUM(v + 1.5) AS result", []any{float64(18), nil, nil}},
		{"distinct_expression", "COUNT(DISTINCT (v + 1)) AS result", []any{int64(4), int64(0), int64(0)}},
	}
	var result []AggregateScenario
	for _, spec := range scenarios {
		scenario := AggregateScenario{Name: "qg2_" + spec.name}
		for ordinal, window := range windows {
			q := t07Query{Fixture: T07FixtureName, Columns: []t07Column{{Function: "SUM", Target: "v", Alias: "result"}}, Begin: window[0], End: window[1]}
			query, err := renderT07(q)
			if err != nil {
				return nil, err
			}
			query = strings.Replace(query, "SUM(v) AS result", spec.projection, 1)
			point := map[string]any{"fields": []any{map[string]any{"name": "result", "value": jsonValue(spec.values[ordinal])}}}
			c, err := t07ExpectedCase(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), query, []any{point}, []string{"qg2.composition", "t07.rebind"})
			if err != nil {
				return nil, err
			}
			c.AggregateInputs, c.AggregateGroups = []uint64{6, 1, 0}[ordinal], 1
			scenario.Cases = append(scenario.Cases, c)
		}
		result = append(result, scenario)
	}
	grouped := AggregateScenario{Name: "qg2_grouped_composition"}
	for ordinal, window := range windows {
		q := t07Query{
			Fixture: T07FixtureName, Columns: []t07Column{{Target: "host"}, {Function: "SUM", Target: "v", Alias: "result"}},
			GroupBy: []string{"host"}, Begin: window[0], End: window[1],
		}
		query, err := renderT07(q)
		if err != nil {
			return nil, err
		}
		query = strings.Replace(query, "SUM(v) AS result", "COALESCE(SUM(v), 0) + COUNT(v) AS result", 1)
		var points []any
		for _, row := range [][]struct {
			host  string
			value int64
		}{{{"a", 4}, {"b", 5}, {"c", 7}}, {{"b", 0}}, {}}[ordinal] {
			points = append(points, map[string]any{
				"tagFamilies": []any{map[string]any{"name": "default", "tags": []any{map[string]any{"key": "host", "value": jsonValue(row.host)}}}},
				"fields":      []any{map[string]any{"name": "result", "value": jsonValue(row.value)}},
			})
		}
		c, err := t07ExpectedCase(fmt.Sprintf("%s_binding_%d", grouped.Name, ordinal), query, points, []string{"qg2.composition", "t07.rebind"})
		if err != nil {
			return nil, err
		}
		c.AggregateInputs, c.AggregateGroups = []uint64{6, 1, 0}[ordinal], []uint64{3, 1, 0}[ordinal]
		grouped.Cases = append(grouped.Cases, c)
	}
	result = append(result, grouped)
	for _, variant := range []struct{ name, suffix string }{
		{"having", " HAVING COALESCE(SUM(v), 0) >= 0"},
		{"order_alias", " ORDER BY result DESC LIMIT 1 OFFSET 1"},
	} {
		scenario := AggregateScenario{Name: "qg2_grouped_" + variant.name}
		for ordinal, original := range grouped.Cases {
			c := original
			c.Name = strings.Replace(c.Name, grouped.Name, scenario.Name, 1)
			c.Query = strings.Replace(c.Query, "\n#!params:", variant.suffix+"\n#!params:", 1)
			if variant.name == "order_alias" {
				var points []any
				if ordinal == 0 {
					points = []any{map[string]any{
						"tagFamilies": []any{map[string]any{"name": "default", "tags": []any{map[string]any{"key": "host", "value": jsonValue("b")}}}},
						"fields":      []any{map[string]any{"name": "result", "value": jsonValue(int64(5))}},
					}}
				}
				expected, err := t07ExpectedCase(c.Name, c.Query, points, c.Features)
				if err != nil {
					return nil, err
				}
				c.WantJSON, c.Ordered = expected.WantJSON, true
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		result = append(result, scenario)
	}
	parameter := AggregateScenario{Name: "qg2_argument_parameter"}
	for ordinal, window := range [][2]int64{{-1, 6000}, {-1, 6000}, {7000, 8000}} {
		q := t07Query{Fixture: T07FixtureName, Columns: []t07Column{{Function: "SUM", Target: "v", Alias: "result"}}, Begin: window[0], End: window[1]}
		query, err := renderT07(q)
		if err != nil {
			return nil, err
		}
		query = strings.Replace(query, "SUM(v) AS result", "SUM(v + ?) AS result", 1)
		parts := strings.SplitN(query, "\n#!params: ", 2)
		var params []any
		if err = json.Unmarshal([]byte(parts[1]), &params); err != nil {
			return nil, err
		}
		// SELECT parameters precede the time parameters in SQL order.
		parameters := []any{int64(1), int64(2), int64(3)}
		params = append([]any{jsonValue(parameters[ordinal])}, params...)
		encoded, err := json.Marshal(params)
		if err != nil {
			return nil, err
		}
		query = parts[0] + "\n#!params: " + string(encoded)
		values := []any{int64(16), int64(20), nil}
		point := map[string]any{"fields": []any{map[string]any{"name": "result", "value": jsonValue(values[ordinal])}}}
		c, err := t07ExpectedCase(fmt.Sprintf("%s_binding_%d", parameter.Name, ordinal), query, []any{point}, []string{"qg2.composition", "t07.rebind"})
		if err != nil {
			return nil, err
		}
		c.AggregateInputs, c.AggregateGroups = []uint64{6, 6, 0}[ordinal], 1
		parameter.Cases = append(parameter.Cases, c)
	}
	b.features["qg2.composition"] = true
	return append(result, parameter), nil
}
