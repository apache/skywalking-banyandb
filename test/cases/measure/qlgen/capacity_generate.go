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
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// P1ListQuery returns the exact indexed WHERE experiment, with timestamp parameters first.
func P1ListQuery(negated bool) string {
	op := "IN"
	if negated {
		op = "NOT IN"
	}
	return "SELECT COUNT(DISTINCT entity_id) AS n FROM qlperf.activity WHERE time >= ? AND time < ? AND value::FIELD > 0 AND service_id " + op + " (?)"
}

// P1CaseQuery returns the fixed CASE template, with membership parameters before timestamps.
func P1CaseQuery(branches int) string {
	var query strings.Builder
	query.WriteString("SELECT CASE")
	for branch := 0; branch < branches; branch++ {
		category := fmt.Sprintf("nonmatching-%02d", branch)
		if branch < 2 {
			category = []string{"group-a", "group-b"}[branch]
		}
		fmt.Fprintf(&query, " WHEN service_id IN (?) THEN '%s'", category)
	}
	query.WriteString(" ELSE 'unassigned' END AS category, COUNT(DISTINCT entity_id) AS n FROM qlperf.activity")
	query.WriteString(" WHERE time >= ? AND time < ? AND value::FIELD > 0 GROUP BY 1 ORDER BY category ASC")
	return query.String()
}

// Oracle iterates the specified service domain, independently of stored rows or query execution.
func p1Count(ids []string, negated bool) int64 {
	var count int64
	for service := 0; service < 1024; service++ {
		selected := slices.Contains(ids, fmt.Sprintf("present-%04d", service))
		if selected != negated {
			count += 4
		}
	}
	return count
}

func p1Categories(memberships [][]string) []int64 {
	counts := make([]int64, len(memberships))
	for service := 0; service < 1024; service++ {
		for branch, ids := range memberships {
			if slices.Contains(ids, fmt.Sprintf("present-%04d", service)) {
				counts[branch] += 4
				break
			}
		}
	}
	return counts[:2]
}

func p1Point(category string, n int64) map[string]any {
	point := map[string]any{"fields": []any{map[string]any{"name": "n", "value": jsonValue(n)}}}
	if category != "" {
		point["fields"] = []any{map[string]any{"name": "category", "value": jsonValue(category)}, map[string]any{"name": "n", "value": jsonValue(n)}}
	}
	return point
}

func p1Case(name, statement string, params []any, points []any, ordered bool) (Case, error) {
	c, err := parentChildCase(name, statement, params, points, ordered)
	var values []any
	for _, param := range params {
		if text, ok := param.(string); ok && strings.HasPrefix(text, "{{time:") {
			values = append(values, map[string]any{"timestamp": text})
		} else {
			values = append(values, jsonValue(param))
		}
	}
	encoded, encodeErr := json.Marshal(values)
	if encodeErr != nil {
		return Case{}, encodeErr
	}
	c.Query = statement + "\n#!params: " + string(encoded)
	c.Ticket = "P1"
	c.Features = []string{"p1.directory_lists", "p1.case_maps"}
	return c, err
}

func generateP1() ([]AggregateFixture, []AggregateScenario, error) {
	readiness := []AggregateFixture{{Name: P1FixtureName}}
	for start := 0; start < 1024; start += 32 {
		ids := make([]string, 32)
		var points []any
		for i := range ids {
			ids[i] = fmt.Sprintf("present-%04d", start+i)
			points = append(points, map[string]any{
				"tagFamilies": []any{map[string]any{"name": "searchable", "tags": []any{map[string]any{"key": "service_id", "value": jsonValue(ids[i])}}}},
				"fields": []any{
					map[string]any{"name": "entities", "value": jsonValue(int64(5))},
					map[string]any{"name": "rows", "value": jsonValue(int64(10))},
					map[string]any{"name": "positive", "value": jsonValue(int64(8))},
					map[string]any{"name": "counted", "value": jsonValue(int64(10))},
				},
			})
		}
		statement := "SELECT service_id, COUNT(DISTINCT entity_id) AS entities, COUNT(entity_id) AS rows, " +
			"SUM(value::FIELD) AS positive, SUM(count::FIELD) AS counted FROM qlperf.activity " +
			"WHERE time >= ? AND time < ? AND service_id IN (?) GROUP BY service_id ORDER BY service_id ASC"
		ready, err := p1Case(fmt.Sprintf("p1_ready_%04d", start), statement, []any{timePlaceholder(0), timePlaceholder(2000), ids}, points, true)
		if err != nil {
			return nil, nil, err
		}
		readiness[0].Cases = append(readiness[0].Cases, ready)
		for _, point := range points {
			fields := point.(map[string]any)["fields"].([]any)
			for index, number := range []int64{4, 8, 8, 8} {
				fields[index].(map[string]any)["value"] = jsonValue(number)
			}
		}
		activeStatement := strings.Replace(statement, " AND service_id IN (?)", " AND value::FIELD > 0 AND service_id IN (?)", 1)
		active, activeErr := p1Case(fmt.Sprintf("p1_active_ready_%04d", start), activeStatement, []any{timePlaceholder(0), timePlaceholder(2000), ids}, points, true)
		if activeErr != nil {
			return nil, nil, activeErr
		}
		readiness[0].Cases = append(readiness[0].Cases, active)
	}
	var scenarios []AggregateScenario
	for _, negated := range []bool{false, true} {
		name := "p1_in"
		if negated {
			name = "p1_not_in"
		}
		scenario := AggregateScenario{Name: name}
		lists := [][]string{P1List(10, 8), P1List(1000, 8), P1List(10000, 8), P1List(10, 7), P1List(10, 0), append(P1List(10, 8), P1List(10, 8)...)}
		for i, list := range lists {
			c, caseErr := p1Case(fmt.Sprintf("%s_binding_%d", name, i), P1ListQuery(negated),
				[]any{timePlaceholder(0), timePlaceholder(2000), list}, []any{p1Point("", p1Count(list, negated))}, false)
			if caseErr != nil {
				return nil, nil, caseErr
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		for index, param := range []any{[]string{}, nil} {
			want := int64(0)
			if index == 0 && negated {
				want = 4096
			}
			c, caseErr := p1Case(fmt.Sprintf("%s_contract_%d", name, index), P1ListQuery(negated),
				[]any{timePlaceholder(0), timePlaceholder(2000), param}, []any{p1Point("", want)}, false)
			if caseErr != nil {
				return nil, nil, caseErr
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		// Reuse the cached template after the empty bindings to detect retained
		// empty-input state or execution-owned membership values.
		ids := P1List(10, 8)
		rebound, reboundErr := p1Case(name+"_after_empty", P1ListQuery(negated),
			[]any{timePlaceholder(0), timePlaceholder(2000), ids}, []any{p1Point("", p1Count(ids, negated))}, false)
		if reboundErr != nil {
			return nil, nil, reboundErr
		}
		scenario.Cases = append(scenario.Cases, rebound)
		scenarios = append(scenarios, scenario)
	}
	for _, branches := range []int{2, 16} {
		scenario := AggregateScenario{Name: fmt.Sprintf("p1_case_%d", branches)}
		for _, variant := range []string{"normal", "swap", "overlap"} {
			memberships := P1Memberships(branches, variant)
			var params []any
			for _, list := range memberships {
				params = append(params, list)
			}
			params = append(params, timePlaceholder(0), timePlaceholder(2000))
			counts := p1Categories(memberships)
			c, caseErr := p1Case(scenario.Name+"_"+variant, P1CaseQuery(branches), params,
				[]any{p1Point("group-a", counts[0]), p1Point("group-b", counts[1])}, true)
			if caseErr != nil {
				return nil, nil, caseErr
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
	}
	return readiness, scenarios, nil
}

func validateP1(s Suite) error {
	ready, scenarios, err := generateP1()
	if err != nil {
		return err
	}
	want, err := json.Marshal([]any{ready, scenarios})
	if err != nil {
		return fmt.Errorf("marshal P1 expected cohort: %w", err)
	}
	got, err := json.Marshal([]any{s.CapacityReadiness, s.CapacityScenarios})
	if err != nil {
		return fmt.Errorf("marshal P1 generated cohort: %w", err)
	}
	if !bytes.Equal(want, got) || s.CapacityFixtureDigest != P1FixtureDigest() {
		return fmt.Errorf("P1 generated fixture, coverage or independent oracle changed")
	}
	return nil
}
