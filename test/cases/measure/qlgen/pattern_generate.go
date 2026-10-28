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
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
)

func patternCase(name, statement string, params []any, points []any, features ...string) (Case, error) {
	values := make([]any, 0, len(params))
	for _, value := range params {
		values = append(values, jsonValue(value))
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return Case{}, err
	}
	c, err := t07ExpectedCase(name, statement+"\n#!params: "+string(encoded), points, features)
	c.Ticket = "Patterns"
	return c, err
}

func generatePatterns(b *suiteBuilder) ([]AggregateFixture, []AggregateScenario, error) {
	var points []any
	for _, row := range patternRows() {
		points = append(points, map[string]any{
			"timestamp": timePlaceholder(row.Millis),
			"tagFamilies": []any{map[string]any{"name": "default", "tags": []any{
				map[string]any{"key": "rid", "value": jsonValue(row.RID)}, map[string]any{"key": "name", "value": jsonValue(row.Name)},
			}}},
			"fields": []any{map[string]any{"name": "value", "value": jsonValue(row.Value)}},
		})
	}
	source := " FROM MEASURE " + PatternFixtureName + " IN " + FixtureGroup + " WHERE time >= ? AND time < ?"
	ready, err := patternCase("patterns_ready", "SELECT rid, name, value::FIELD"+source+" AND rid IN (?) LIMIT 100",
		[]any{timePlaceholder(0), timePlaceholder(2000), []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}}, points, "patterns.fixture")
	if err != nil {
		return nil, nil, err
	}
	b.features["patterns.fixture"] = true

	var scenarios []AggregateScenario
	addScalar := func(name, column, op, escape, clause string, patterns []any, residual bool) error {
		scenario := AggregateScenario{Name: "patterns_" + name}
		statement := "SELECT COUNT(DISTINCT name) AS total" + source + " AND " + column + " " + op + " ?" + clause
		if residual {
			statement += " AND value::FIELD > 0"
		}
		for i, pattern := range patterns {
			count, countErr := patternCount(pattern, escape, op == "ILIKE", residual)
			feature := "patterns." + op
			if countErr != nil {
				feature = "patterns.invalid"
			}
			output := []any{map[string]any{"fields": []any{map[string]any{"name": "total", "value": jsonValue(count)}}}}
			c, caseErr := patternCase(fmt.Sprintf("%s_binding_%d", scenario.Name, i), statement,
				[]any{timePlaceholder(0), timePlaceholder(2000), pattern}, output, feature)
			if caseErr != nil {
				return caseErr
			}
			if countErr != nil {
				c.WantCode = codes.InvalidArgument
				c.WantJSON = nil
			}
			b.features[feature] = countErr == nil
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
		return nil
	}
	for _, op := range []string{"LIKE", "ILIKE"} {
		if err = addScalar(op+"_scalar", "name", op, "\\", "",
			[]any{"%amber%", "%Bird", nil, "", "%", "a_b", "%中%", "Σ", "a*b", "a?b", `a\\b`, "Amber-Bird", "Amber%", "%er-B%", "%%a%%"}, false); err != nil {
			return nil, nil, err
		}
		if err = addScalar(op+"_escape", "name", op, "!", " ESCAPE '!'", []any{"a!_b", "20!%", "a_b", "%", "", "no-match"}, false); err != nil {
			return nil, nil, err
		}
		if err = addScalar(op+"_disabled_escape", "name", op, "", " ESCAPE ''", []any{`a\b`, "a_b", "no-match"}, false); err != nil {
			return nil, nil, err
		}
	}
	if err = addScalar("omitted_analyzer", "plain_name", "ILIKE", "\\", " AND name LIKE '%'", []any{"%amber%", "%er-b%", "%"}, false); err != nil {
		return nil, nil, err
	}
	if err = addScalar("field_residual", "name", "ILIKE", "\\", "", []any{"%amber%", "%", "no-match"}, true); err != nil {
		return nil, nil, err
	}
	if err = addScalar("bind_recovery", "name", "LIKE", "\\", "", []any{`bad\`, int64(8), "%Bird"}, false); err != nil {
		return nil, nil, err
	}

	composition, compositionErr := generatePatternComposition(b, source)
	if compositionErr != nil {
		return nil, nil, compositionErr
	}
	scenarios = append(scenarios, composition...)

	for _, rejection := range []struct{ name, predicate string }{
		{"tokenized", "token_name LIKE ?"},
		{"unindexed", "unindexed_name LIKE ?"},
		{"numeric_source", "rid LIKE ?"},
		{"or", "name LIKE ? OR value::FIELD > 0"},
		{"not", "NOT (name LIKE ?)"},
		{"not_like", "name NOT LIKE ?"},
		{"escape_width", "name LIKE ? ESCAPE 'xx'"},
		{"escape_null", "name LIKE ? ESCAPE NULL"},
	} {
		scenario := AggregateScenario{Name: "patterns_reject_" + rejection.name}
		patterns := []any{"%"}
		if rejection.name == "unindexed" {
			patterns = append(patterns, nil)
		}
		for i, pattern := range patterns {
			c, caseErr := patternCase(fmt.Sprintf("%s_binding_%d", scenario.Name, i), "SELECT COUNT(DISTINCT name) AS total"+source+" AND ("+rejection.predicate+")",
				[]any{timePlaceholder(0), timePlaceholder(2000), pattern}, nil, "patterns.invalid")
			if caseErr != nil {
				return nil, nil, caseErr
			}
			c.WantCode = codes.InvalidArgument
			c.WantJSON = nil
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
	}
	b.features["patterns.invalid"] = false
	reviewFixtures, reviewScenarios, reviewErr := generatePatternReview(b, points, source)
	if reviewErr != nil {
		return nil, nil, reviewErr
	}
	fixtures := append([]AggregateFixture{{Name: PatternFixtureName, Cases: []Case{ready}}}, reviewFixtures...)
	return fixtures, append(scenarios, reviewScenarios...), nil
}

func validatePatterns(s Suite) error {
	requiredFixtures := map[string]bool{PatternFixtureName: true, "like": true, "ilike": true, "escape": true, "like-rate": true, "ilike-rate": true, "escape-group": true}
	if len(s.PatternReadiness) != len(requiredFixtures) {
		return fmt.Errorf("pattern visibility witness missing")
	}
	for _, fixture := range s.PatternReadiness {
		if !requiredFixtures[fixture.Name] || len(fixture.Cases) == 0 {
			return fmt.Errorf("pattern visibility witness missing: %s", fixture.Name)
		}
		delete(requiredFixtures, fixture.Name)
	}
	required := map[string]bool{}
	for _, name := range []string{
		"LIKE_scalar", "ILIKE_scalar", "LIKE_escape", "ILIKE_escape", "LIKE_disabled_escape", "ILIKE_disabled_escape",
		"omitted_analyzer", "field_residual", "bind_recovery", "grouped", "native_page", "reject_tokenized", "reject_unindexed", "reject_numeric_source",
		"reject_or", "reject_not", "reject_not_like", "reject_escape_width", "reject_escape_null",
		"identifier_like", "identifier_ilike", "identifier_escape", "identifier_like-rate", "identifier_ilike-rate", "identifier_escape-group",
		"reject_ambiguous_keyword_high_first", "reject_ambiguous_keyword_low_first", "reject_ambiguous_standard_first",
		"reject_ambiguous_keyword_before_standard", "reject_ambiguous_multitag_first", "reject_ambiguous_keyword_before_multitag",
	} {
		required["patterns_"+name] = true
	}
	if len(s.PatternScenarios) != len(required) {
		return fmt.Errorf("pattern scenario coverage missing")
	}
	for _, scenario := range s.PatternScenarios {
		if !required[scenario.Name] || len(scenario.Cases) == 0 {
			return fmt.Errorf("invalid pattern scenario %s", scenario.Name)
		}
		delete(required, scenario.Name)
		query := strings.Split(scenario.Cases[0].Query, "\n#!params:")[0]
		for _, c := range scenario.Cases {
			if c.Ticket != "Patterns" || strings.Split(c.Query, "\n#!params:")[0] != query {
				return fmt.Errorf("pattern scenario changed statement: %s", c.Name)
			}
		}
	}
	return nil
}

func generatePatternComposition(b *suiteBuilder, source string) ([]AggregateScenario, error) {
	var scenarios []AggregateScenario
	// Group and page the independent complete aggregate, never the input names.
	grouped := AggregateScenario{Name: "patterns_grouped"}
	for i, pattern := range []string{"%amber%", "%", "no-match"} {
		groups := map[string]map[any]bool{}
		sums := map[string]int64{}
		for _, row := range patternRows() {
			if row.Name == nil || row.Value <= 0 {
				continue
			}
			matched, matchErr := patternMatch(row.Name.(string), pattern, "\\", true)
			if matchErr != nil {
				return nil, matchErr
			}
			if !matched {
				continue
			}
			if groups[row.Category] == nil {
				groups[row.Category] = map[any]bool{}
			}
			groups[row.Category][row.Name] = true
			sums[row.Category] += row.Value
		}
		var keys []string
		for key := range groups {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var output []any
		for _, key := range keys {
			output = append(output, map[string]any{
				"tagFamilies": []any{map[string]any{"name": "default", "tags": []any{map[string]any{"key": "category", "value": jsonValue(key)}}}},
				"fields":      []any{map[string]any{"name": "total", "value": jsonValue(int64(len(groups[key])))}, map[string]any{"name": "amount", "value": jsonValue(sums[key])}},
			})
		}
		c, caseErr := patternCase(fmt.Sprintf("patterns_grouped_binding_%d", i),
			"SELECT category, COUNT(DISTINCT name) AS total, SUM(value::FIELD) AS amount"+source+
				" AND name ILIKE ? AND value::FIELD > 0 GROUP BY category HAVING COUNT(DISTINCT name) > 0 ORDER BY category ASC LIMIT 10",
			[]any{timePlaceholder(0), timePlaceholder(2000), pattern}, output, "patterns.composition")
		if caseErr != nil {
			return nil, caseErr
		}
		c.Ordered = true
		grouped.Cases = append(grouped.Cases, c)
	}
	b.features["patterns.composition"] = true
	scenarios = append(scenarios, grouped)

	raw := AggregateScenario{Name: "patterns_native_page"}
	for i, pattern := range []string{"%amber%", "%", "no-match"} {
		var matches []patternRow
		for _, row := range patternRows() {
			// A single timestamp makes the native numeric index order a total order.
			if row.Millis != 0 || row.Name == nil {
				continue
			}
			matched, matchErr := patternMatch(row.Name.(string), pattern, "\\", true)
			if matchErr != nil {
				return nil, matchErr
			}
			if matched {
				matches = append(matches, row)
			}
		}
		sort.Slice(matches, func(i, j int) bool { return matches[i].RID < matches[j].RID })
		var output []any
		for j, row := range matches {
			if j < 1 || j >= 3 {
				continue
			}
			output = append(output, map[string]any{
				"timestamp": timePlaceholder(row.Millis),
				"tagFamilies": []any{map[string]any{"name": "default", "tags": []any{
					map[string]any{"key": "rid", "value": jsonValue(row.RID)}, map[string]any{"key": "name", "value": jsonValue(row.Name)},
				}}},
			})
		}
		c, caseErr := patternCase(fmt.Sprintf("patterns_native_page_binding_%d", i), "SELECT rid, name"+source+
			" AND name ILIKE ? ORDER BY ql_patterns_rid ASC LIMIT ? OFFSET ?",
			[]any{timePlaceholder(0), timePlaceholder(1000), pattern, int64(2), int64(1)}, output, "patterns.pagination")
		if caseErr != nil {
			return nil, caseErr
		}
		c.Ordered = true
		raw.Cases = append(raw.Cases, c)
	}
	b.features["patterns.pagination"] = true
	scenarios = append(scenarios, raw)

	return scenarios, nil
}
