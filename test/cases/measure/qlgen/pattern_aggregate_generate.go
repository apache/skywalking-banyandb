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
	"sort"
)

func generatePatternAggregateComposition(b *suiteBuilder, source string) ([]AggregateScenario, error) {
	var scenarios []AggregateScenario
	for _, op := range []string{"LIKE", "ILIKE"} {
		scenario := AggregateScenario{Name: "patterns_aggregate_" + op}
		statement := "SELECT COALESCE(SUM(value::FIELD + 1), 0) AS total, " +
			"COALESCE(SUM(DECIMAL_FROM_SCALED(value::FIELD + 1, 2)), 0) AS amount" + source + " AND name " + op + " ?"
		for ordinal, pattern := range []any{"%amber%", "%Bird", "%", "no-match", nil} {
			rows, err := patternAggregateRows(pattern, op == "ILIKE")
			if err != nil {
				return nil, err
			}
			var total int64
			for _, row := range rows {
				total += row.Value + 1
			}
			// Fixture coefficients are nonnegative and small; quotient/remainder
			// formatting supplies an oracle independent of the decimal engine.
			amount := fmt.Sprintf("%d.%02d", total/100, total%100)
			points := []any{map[string]any{"fields": []any{
				map[string]any{"name": "total", "value": jsonValue(total)},
				map[string]any{"name": "amount", "value": jsonValue(amount)},
			}}}
			c, caseErr := patternCase(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), statement,
				[]any{timePlaceholder(0), timePlaceholder(2000), pattern}, points, "patterns.aggregate_composition")
			if caseErr != nil {
				return nil, caseErr
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
	}
	grouped, err := generatePatternDecimalPage(source)
	if err != nil {
		return nil, err
	}
	b.features["patterns.aggregate_composition"] = true
	return append(scenarios, grouped), nil
}

func generatePatternDecimalPage(source string) (AggregateScenario, error) {
	scenario := AggregateScenario{Name: "patterns_aggregate_decimal_page"}
	statement := "SELECT category, SUM(DECIMAL_FROM_SCALED(value::FIELD * 10, 1)) AS amount" + source +
		" AND name ILIKE ? GROUP BY category HAVING SUM(DECIMAL_FROM_SCALED(value::FIELD * 10, 1)) > 1" +
		" ORDER BY amount DESC, category ASC LIMIT 1 OFFSET ?"
	bindings := []struct {
		pattern any
		offset  int64
	}{{"%amber%", 0}, {"%", 1}, {"a_b", 0}, {"no-match", 0}, {nil, 0}}
	for ordinal, binding := range bindings {
		rows, err := patternAggregateRows(binding.pattern, true)
		if err != nil {
			return AggregateScenario{}, err
		}
		sums := map[string]int64{}
		for _, row := range rows {
			sums[row.Category] += row.Value
		}
		var categories []string
		for category, sum := range sums {
			if sum > 1 {
				categories = append(categories, category)
			}
		}
		// With "%", the fixture totals are 12.0 and 5.0. Numeric descending
		// order differs from text order, and OFFSET must follow aggregation.
		sort.Slice(categories, func(i, j int) bool {
			left, right := categories[i], categories[j]
			if sums[left] != sums[right] {
				return sums[left] > sums[right]
			}
			return left < right
		})
		var points []any
		if binding.offset < int64(len(categories)) {
			category := categories[binding.offset]
			points = append(points, map[string]any{
				"tagFamilies": []any{map[string]any{"name": "default", "tags": []any{
					map[string]any{"key": "category", "value": jsonValue(category)},
				}}},
				"fields": []any{map[string]any{"name": "amount", "value": jsonValue(fmt.Sprintf("%d.0", sums[category]))}},
			})
		}
		c, caseErr := patternCase(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), statement,
			[]any{timePlaceholder(0), timePlaceholder(2000), binding.pattern, binding.offset}, points, "patterns.aggregate_composition")
		if caseErr != nil {
			return AggregateScenario{}, caseErr
		}
		c.Ordered = true
		scenario.Cases = append(scenario.Cases, c)
	}
	return scenario, nil
}

func patternAggregateRows(pattern any, ignoreCase bool) ([]patternRow, error) {
	if pattern == nil {
		return nil, nil
	}
	var rows []patternRow
	for _, row := range patternRows() {
		if row.Name == nil {
			continue
		}
		matched, err := patternMatch(row.Name.(string), pattern.(string), "\\", ignoreCase)
		if err != nil {
			return nil, err
		}
		if matched {
			rows = append(rows, row)
		}
	}
	return rows, nil
}
