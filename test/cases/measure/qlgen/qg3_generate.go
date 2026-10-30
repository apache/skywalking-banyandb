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

type qg3Row struct {
	value any
	host  string
}

// generateQG3Decimal uses PostgreSQL 17 numeric results for the fixture values.
func generateQG3Decimal(b *suiteBuilder) ([]AggregateScenario, error) {
	windows := [][2]int64{{-1, 13000}, {12000, 13000}, {14000, 15000}}
	specs := []struct {
		name, projection, suffix string
		values                   [][]qg3Row
		grouped, ordered         bool
	}{
		{
			"scalar", "SUM(DECIMAL_FROM_SCALED(v, 10)) AS result", "",
			[][]qg3Row{{{value: "899999999.9999999990", host: ""}}, {{value: nil, host: ""}}, {{value: nil, host: ""}}},
			false, false,
		},
		{
			"coalesce_zero", "COALESCE(SUM(DECIMAL_FROM_SCALED(v, 10)), 0) AS result", "",
			[][]qg3Row{{{value: "899999999.9999999990", host: ""}}, {{value: "0.0000000000", host: ""}}, {{value: "0.0000000000", host: ""}}},
			false, false,
		},
		{
			"coalesce_null", "COALESCE(SUM(DECIMAL_FROM_SCALED(v, 10)), NULL) AS result", "",
			[][]qg3Row{{{value: "899999999.9999999990", host: ""}}, {{value: nil, host: ""}}, {{value: nil, host: ""}}},
			false, false,
		},
		{
			"grouped", "SUM(DECIMAL_FROM_SCALED(v, 10)) AS result", "",
			[][]qg3Row{{
				{value: "999999999.9999999990", host: "a"},
				{value: "-100000000.0000000000", host: "b"},
				{value: nil, host: "c"},
			}, {{value: nil, host: "c"}}, {}},
			true, false,
		},
		{
			"having", "SUM(DECIMAL_FROM_SCALED(v, 10)) AS result", " HAVING SUM(DECIMAL_FROM_SCALED(v, 10)) > 0",
			[][]qg3Row{{{value: "999999999.9999999990", host: "a"}}, {}, {}},
			true, false,
		},
		{
			"sort", "SUM(DECIMAL_FROM_SCALED(v, 10)) AS result", " ORDER BY result ASC",
			[][]qg3Row{{
				{value: "-100000000.0000000000", host: "b"},
				{value: "999999999.9999999990", host: "a"},
				{value: nil, host: "c"},
			}, {{value: nil, host: "c"}}, {}},
			true, true,
		},
		{
			"mixed", "SUM(DECIMAL_FROM_SCALED(v, 10)) AS result, COUNT(v) AS count", "",
			[][]qg3Row{{
				{value: "999999999.9999999990", host: "a"},
				{value: "-100000000.0000000000", host: "b"},
				{value: nil, host: "c"},
			}, {{value: nil, host: "c"}}, {}},
			true, false,
		},
	}
	var scenarios []AggregateScenario
	for _, spec := range specs {
		scenario := AggregateScenario{Name: "qg3_decimal_" + spec.name}
		for ordinal, window := range windows {
			q := t07Query{Fixture: T07DecimalFixtureName, Columns: []t07Column{{Function: "SUM", Target: "v", Alias: "result"}}, Begin: window[0], End: window[1]}
			if spec.grouped {
				q.Columns = append([]t07Column{{Target: "host"}}, q.Columns...)
				q.GroupBy = []string{"host"}
			}
			query, err := renderT07(q)
			if err != nil {
				return nil, err
			}
			query = strings.Replace(query, "SUM(v) AS result", spec.projection, 1)
			query = strings.Replace(query, "\n#!params:", spec.suffix+"\n#!params:", 1)
			var points []any
			for _, row := range spec.values[ordinal] {
				point := map[string]any{"fields": []any{map[string]any{"name": "result", "value": jsonValue(row.value)}}}
				if spec.name == "mixed" {
					counts := map[string]int64{"a": 10, "b": 2, "c": 0}
					point["fields"] = append(point["fields"].([]any), map[string]any{"name": "count", "value": jsonValue(counts[row.host])})
				}
				if spec.grouped {
					point["tagFamilies"] = []any{map[string]any{"name": "default", "tags": []any{map[string]any{"key": "host", "value": jsonValue(row.host)}}}}
				}
				points = append(points, point)
			}
			c, err := t07ExpectedCase(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), query, points, []string{"qg3.decimal", "t07.rebind"})
			if err != nil {
				return nil, err
			}
			c.AggregateInputs = []uint64{13, 1, 0}[ordinal]
			if spec.grouped {
				c.AggregateGroups = []uint64{3, 1, 0}[ordinal]
			} else {
				c.AggregateGroups = 1
			}
			c.Ordered = spec.ordered
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
	}
	extra, err := generateQG3Boundaries()
	if err != nil {
		return nil, err
	}
	scenarios = append(scenarios, extra...)
	ordered, err := generateQG3NumericOrder()
	if err != nil {
		return nil, err
	}
	scenarios = append(scenarios, ordered)
	membership, err := generateQG3Membership()
	if err != nil {
		return nil, err
	}
	scenarios = append(scenarios, membership...)
	b.features["qg3.decimal"] = true
	return scenarios, nil
}

// generateQG3Boundaries checks an ordinary integer sum and cancellation.
func generateQG3Boundaries() ([]AggregateScenario, error) {
	specs := []struct {
		name, projection string
		windows          [][2]int64
		sums             []any
		integers         []any
	}{
		{
			"safe_integer", "SUM(DECIMAL_FROM_SCALED(v, 10)) AS result, SUM(v) AS integer_sum",
			[][2]int64{{10000, 12000}, {12000, 13000}, {14000, 15000}},
			[]any{"-100000000.0000000000", nil, nil},
			[]any{int64(-1000000000000000000), nil, nil},
		},
		{"cancellation", "SUM(DECIMAL_FROM_SCALED(v - v, 10)) AS result", [][2]int64{{-1, 13000}, {12000, 13000}, {14000, 15000}}, []any{"0.0000000000", nil, nil}, nil},
	}
	var scenarios []AggregateScenario
	for _, spec := range specs {
		scenario := AggregateScenario{Name: "qg3_decimal_" + spec.name}
		for ordinal, window := range spec.windows {
			q := t07Query{Fixture: T07DecimalFixtureName, Columns: []t07Column{{Function: "SUM", Target: "v", Alias: "result"}}, Begin: window[0], End: window[1]}
			query, err := renderT07(q)
			if err != nil {
				return nil, err
			}
			query = strings.Replace(query, "SUM(v) AS result", spec.projection, 1)
			fields := []any{map[string]any{"name": "result", "value": jsonValue(spec.sums[ordinal])}}
			if spec.integers != nil {
				fields = append(fields, map[string]any{"name": "integer_sum", "value": jsonValue(spec.integers[ordinal])})
			}
			c, err := t07ExpectedCase(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), query, []any{map[string]any{"fields": fields}}, []string{"qg3.decimal", "t07.rebind"})
			if err != nil {
				return nil, err
			}
			if spec.integers != nil {
				c.AggregateInputs = []uint64{2, 1, 0}[ordinal]
			} else {
				c.AggregateInputs = []uint64{13, 1, 0}[ordinal]
			}
			c.AggregateGroups = 1
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
	}
	return scenarios, nil
}

// generateQG3NumericOrder distinguishes numeric from lexical ordering.
func generateQG3NumericOrder() (AggregateScenario, error) {
	scenario := AggregateScenario{Name: "qg3_decimal_numeric_order"}
	windows := [][2]int64{{-1, 13000}, {12000, 13000}, {14000, 15000}}
	for ordinal, window := range windows {
		q := t07Query{
			Fixture: T07DecimalFixtureName, Columns: []t07Column{{Target: "rid"}, {Function: "SUM", Target: "v", Alias: "result"}},
			GroupBy: []string{"rid"}, Begin: window[0], End: window[1],
		}
		query, err := renderT07(q)
		if err != nil {
			return scenario, err
		}
		query = strings.Replace(query, "SUM(v) AS result", "SUM(DECIMAL_FROM_SCALED(v, 10)) AS result", 1)
		query = strings.Replace(query, "\n#!params:", " ORDER BY result ASC, rid ASC\n#!params:", 1)
		var points []any
		add := func(rid int64, value any) {
			points = append(points, map[string]any{
				"tagFamilies": []any{map[string]any{"name": "default", "tags": []any{map[string]any{"key": "rid", "value": jsonValue(rid)}}}},
				"fields":      []any{map[string]any{"name": "result", "value": jsonValue(value)}},
			})
		}
		switch ordinal {
		case 0:
			add(11, "-99999999.9999999999")
			add(12, "-0.0000000001")
			for rid := int64(1); rid <= 10; rid++ {
				add(rid, "99999999.9999999999")
			}
			add(13, nil)
		case 1:
			add(13, nil)
		}
		c, err := t07ExpectedCase(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), query, points, []string{"qg3.decimal", "t07.rebind"})
		if err != nil {
			return scenario, err
		}
		c.AggregateInputs, c.AggregateGroups = []uint64{13, 1, 0}[ordinal], []uint64{13, 1, 0}[ordinal]
		c.Ordered = true
		scenario.Cases = append(scenario.Cases, c)
	}
	return scenario, nil
}
