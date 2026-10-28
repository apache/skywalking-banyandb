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
)

func parentChildPoint(parent string, count, total int64, legacy, scalar bool) map[string]any {
	point := map[string]any{}
	field := "distinct_count"
	if legacy {
		field = "entity_id"
	}
	if scalar && !legacy {
		field = "total"
	}
	fields := []any{map[string]any{"name": field, "value": jsonValue(count)}}
	if !scalar {
		point["tagFamilies"] = []any{map[string]any{"name": "default", "tags": []any{map[string]any{"key": "service_id", "value": jsonValue(parent)}}}}
		if !legacy {
			fields = append(fields, map[string]any{"name": "total", "value": jsonValue(total)})
		}
	}
	point["fields"] = fields
	return point
}

func parentChildCase(name, statement string, params []any, points []any, ordered bool) (Case, error) {
	var values []any
	for _, value := range params {
		values = append(values, jsonValue(value))
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return Case{}, err
	}
	c, err := t07ExpectedCase(name, statement+"\n#!params: "+string(encoded), points, []string{"catalog.B5"})
	c.Ticket, c.Ordered = "ParentChild", ordered
	return c, err
}

// The oracle groups complete child identities and sums every bucket before paging.
func parentChildOracle(fixture string, begin, end int64, parent string, limit, offset int, legacy, scalar bool) []any {
	type group struct {
		entities map[string]bool
		parent   string
		total    int64
	}
	groups := map[string]*group{}
	for _, row := range parentChildRows(fixture) {
		if row.Millis < begin || row.Millis >= end || (parent != "" && parent != row.Parent) {
			continue
		}
		key := row.Parent
		if scalar {
			key = ""
		}
		if groups[key] == nil {
			groups[key] = &group{parent: key, entities: map[string]bool{}}
		}
		g := groups[key]
		if scalar && !legacy {
			g.entities[row.Parent] = true
		} else {
			g.entities[row.Entity] = true
		}
		g.total += row.Value
	}
	if scalar {
		if limit == 0 || offset > 0 {
			return nil
		}
		count := int64(0)
		if groups[""] != nil {
			count = int64(len(groups[""].entities))
		}
		return []any{parentChildPoint("", count, 0, legacy, true)}
	}
	var sorted []*group
	for _, g := range groups {
		sorted = append(sorted, g)
	}
	sort.Slice(sorted, func(i, j int) bool {
		if len(sorted[i].entities) != len(sorted[j].entities) {
			return len(sorted[i].entities) > len(sorted[j].entities)
		}
		return sorted[i].parent < sorted[j].parent
	})
	var points []any
	for i, g := range sorted {
		if i >= offset && i < offset+limit {
			points = append(points, parentChildPoint(g.parent, int64(len(g.entities)), g.total, legacy, false))
		}
	}
	return points
}

func generateParentChild() ([]AggregateFixture, []AggregateScenario, error) {
	var readiness []AggregateFixture
	for _, name := range []string{ParentChildFixtureName, ParentChildPageFixtureName} {
		fixture := AggregateFixture{Name: name}
		rows := parentChildRows(name)
		for start := 0; start < len(rows); start += 32 {
			end := min(start+32, len(rows))
			var points []any
			var selectors []string
			for _, row := range rows[start:end] {
				point := map[string]any{
					"timestamp": timePlaceholder(row.Millis),
					"tagFamilies": []any{map[string]any{"name": "default", "tags": []any{
						map[string]any{"key": "entity_id", "value": jsonValue(row.Entity)}, map[string]any{"key": "service_id", "value": jsonValue(row.Parent)},
					}}},
					"fields": []any{map[string]any{"name": "value", "value": jsonValue(row.Value)}},
				}
				points = append(points, point)
				selectors = append(selectors, "'"+row.Entity+"'")
			}
			query := "SELECT entity_id, service_id, value::FIELD FROM " + FixtureGroup + "." + name +
				" WHERE time >= ? AND time < ? AND entity_id IN (" + strings.Join(selectors, ",") + ") LIMIT 200"
			c, err := parentChildCase(fmt.Sprintf("parent_child_ready_%s_%d", name, start), query, []any{timePlaceholder(0), timePlaceholder(2000)}, points, false)
			if err != nil {
				return nil, nil, err
			}
			fixture.Cases = append(fixture.Cases, c)
		}
		readiness = append(readiness, fixture)
	}

	var scenarios []AggregateScenario
	add := func(name, fixture, statement string, windows [][2]int64, parents []string, pages [][2]int, legacy, scalar bool) error {
		scenario := AggregateScenario{Name: "parent_child_" + name}
		for i, window := range windows {
			end := window[1]
			if legacy {
				end--
			}
			params := []any{timePlaceholder(window[0]), timePlaceholder(end)}
			parent := ""
			if parents != nil {
				parent = parents[i]
				params = append(params, parent)
			}
			limit, offset := 20, 0
			if pages != nil {
				limit, offset = pages[i][0], pages[i][1]
				params = append(params, int64(limit), int64(offset))
			}
			points := parentChildOracle(fixture, window[0], window[1], parent, limit, offset, legacy, scalar)
			ordered := !scalar && (!legacy || name == "legacy_top")
			c, err := parentChildCase(fmt.Sprintf("%s_binding_%d", scenario.Name, i), statement, params, points, ordered)
			if err != nil {
				return err
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
		return nil
	}
	native := " FROM MEASURE " + ParentChildFixtureName + " IN " + FixtureGroup
	full := [][2]int64{{0, 2000}, {0, 1000}, {2000, 3000}}
	if err := add("legacy_scalar", ParentChildFixtureName, "SELECT COUNT(DISTINCT entity_id)"+native+" TIME BETWEEN ? AND ? WHERE service_id = ?",
		full, []string{"a", "b", "idle"}, nil, true, true); err != nil {
		return nil, nil, err
	}
	if err := add("scalar_pages", ParentChildFixtureName, "SELECT COUNT(DISTINCT entity_id)"+native+" TIME BETWEEN ? AND ? LIMIT ? OFFSET ?",
		[][2]int64{{2000, 3000}, {0, 2000}, {2000, 3000}}, nil, [][2]int{{1, 0}, {0, 0}, {1, 1}}, true, true); err != nil {
		return nil, nil, err
	}
	if err := add("legacy_top", ParentChildFixtureName, "SELECT TOP 500 entity_id DESC, service_id, COUNT(DISTINCT entity_id)"+native+
		" TIME BETWEEN ? AND ? GROUP BY service_id LIMIT 20 OFFSET 0", full, nil, nil, true, false); err != nil {
		return nil, nil, err
	}
	if err := add("legacy_boundary", ParentChildFixtureName, "SELECT COUNT(DISTINCT entity_id)"+native+" TIME BETWEEN ? AND ? WHERE service_id = ?",
		[][2]int64{{0, 1001}, {0, 1}, {1000, 1001}}, []string{"a", "a", "a"}, nil, true, true); err != nil {
		return nil, nil, err
	}
	statement := "SELECT service_id, COUNT(DISTINCT entity_id) AS distinct_count, SUM(value::FIELD) AS total FROM MEASURE " + ParentChildFixtureName +
		" IN " + FixtureGroup + " WHERE time >= ? AND time < ? GROUP BY service_id ORDER BY distinct_count DESC, service_id ASC LIMIT ? OFFSET ?"
	if err := add("ordered", ParentChildFixtureName, statement, full, nil, [][2]int{{20, 0}, {1, 1}, {20, 0}}, false, false); err != nil {
		return nil, nil, err
	}
	if err := add("boundary", ParentChildFixtureName, statement, [][2]int64{{0, 1000}, {1000, 1001}, {1001, 2000}}, nil,
		[][2]int{{20, 0}, {20, 0}, {20, 0}}, false, false); err != nil {
		return nil, nil, err
	}
	if err := add("parent_total", ParentChildFixtureName, "SELECT COUNT(DISTINCT service_id) AS total"+native+" WHERE time >= ? AND time < ?",
		full, nil, nil, false, true); err != nil {
		return nil, nil, err
	}
	if err := add("global_children", ParentChildPageFixtureName,
		"SELECT COUNT(DISTINCT entity_id) FROM MEASURE "+ParentChildPageFixtureName+" IN "+FixtureGroup+" TIME BETWEEN ? AND ?",
		full, nil, nil, true, true); err != nil {
		return nil, nil, err
	}
	pages := strings.ReplaceAll(statement, ParentChildFixtureName, ParentChildPageFixtureName)
	if err := add("tied_pages", ParentChildPageFixtureName, pages, [][2]int64{{0, 2000}, {0, 2000}, {0, 2000}}, nil,
		[][2]int{{7, 100}, {7, 133}, {7, 137}}, false, false); err != nil {
		return nil, nil, err
	}
	return readiness, scenarios, nil
}

func validateParentChild(s Suite) error {
	required := map[string]bool{}
	for _, name := range []string{"legacy_scalar", "scalar_pages", "legacy_top", "legacy_boundary", "ordered", "boundary", "parent_total", "global_children", "tied_pages"} {
		required["parent_child_"+name] = true
	}
	if len(s.ParentChildReadiness) != 2 || len(s.ParentChildScenarios) != len(required) {
		return fmt.Errorf("parent/child coverage missing")
	}
	for i, name := range []string{ParentChildFixtureName, ParentChildPageFixtureName} {
		fixture := s.ParentChildReadiness[i]
		if fixture.Name != name || len(fixture.Cases) != (len(parentChildRows(name))+31)/32 {
			return fmt.Errorf("parent/child QL readiness missing: %s", name)
		}
		for _, c := range fixture.Cases {
			if c.Ticket != "ParentChild" || c.Ordered {
				return fmt.Errorf("invalid parent/child readiness %s", c.Name)
			}
		}
	}
	for _, scenario := range s.ParentChildScenarios {
		if !required[scenario.Name] || len(scenario.Cases) != 3 {
			return fmt.Errorf("invalid parent/child scenario %s", scenario.Name)
		}
		delete(required, scenario.Name)
		statement := strings.Split(scenario.Cases[0].Query, "\n#!params:")[0]
		for _, c := range scenario.Cases {
			if c.Ticket != "ParentChild" || strings.Split(c.Query, "\n#!params:")[0] != statement {
				return fmt.Errorf("parent/child statement changed: %s", c.Name)
			}
		}
	}
	return nil
}
