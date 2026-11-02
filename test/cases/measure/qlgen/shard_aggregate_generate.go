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
	"sort"
	"strings"
)

type p2Totals struct {
	entities map[string]bool
	services map[string]bool
	sum      int64
}

// p2Oracle reads fixture rows without invoking the query parser or execution operators.
func p2Oracle(grouped bool, projection int, window string) []any {
	groups := map[string]*p2Totals{}
	if !grouped {
		groups[""] = &p2Totals{entities: map[string]bool{}, services: map[string]bool{}}
	}
	for _, write := range P1FixtureWrites(P1BaseTime()) {
		row := write.DataPoint
		service := row.TagFamilies[1].Tags[0].GetStr().Value
		if window == "empty" || window == "selected" && service != "present-0000" && service != "present-0001" {
			continue
		}
		if row.Fields[0].GetInt().Value <= 0 {
			continue
		}
		key := ""
		if grouped {
			key = service
		}
		totals := groups[key]
		if totals == nil {
			totals = &p2Totals{entities: map[string]bool{}, services: map[string]bool{}}
			groups[key] = totals
		}
		totals.sum += row.Fields[0].GetInt().Value
		totals.entities[row.TagFamilies[0].Tags[0].GetStr().Value] = true
		totals.services[service] = true
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var points []any
	for _, key := range keys {
		totals := groups[key]
		fields := []any{}
		for index, name := range []string{"total", "entities", "services"} {
			if projection != 3 && projection != index {
				continue
			}
			value := any([]int64{totals.sum, int64(len(totals.entities)), int64(len(totals.services))}[index])
			if index == 0 && len(totals.entities) == 0 {
				value = nil
			}
			fields = append(fields, map[string]any{"name": name, "value": jsonValue(value)})
		}
		point := map[string]any{"fields": fields}
		if grouped {
			point["tagFamilies"] = []any{map[string]any{"name": "searchable", "tags": []any{map[string]any{"key": "service_id", "value": jsonValue(key)}}}}
		}
		points = append(points, point)
	}
	return points
}

func generateP2() ([]AggregateScenario, error) {
	var scenarios []AggregateScenario
	projections := []string{"SUM(value::FIELD) AS total", "COUNT(DISTINCT entity_id) AS entities", "COUNT(DISTINCT service_id) AS services"}
	for _, shape := range []string{"scalar", "grouped", "combined"} {
		scenario := AggregateScenario{Name: "p2_" + shape}
		for variant := 0; variant < 3; variant++ {
			for _, window := range []string{"all", "selected", "empty"} {
				grouped := shape == "grouped" || shape == "combined" && variant == 1
				projection := variant
				selectSQL := projections[variant]
				if shape == "combined" {
					if variant == 2 {
						continue
					}
					projection = 3
					selectSQL = strings.Join(projections, ", ")
				}
				if grouped {
					selectSQL = "service_id, " + selectSQL
				}
				statement := "SELECT " + selectSQL + " FROM qlperf.activity WHERE time >= ? AND time < ? AND value::FIELD > 0"
				params := []any{timePlaceholder(0), timePlaceholder(2000)}
				if window != "all" {
					ids := []string{"present-0000", "present-0001"}
					if window == "empty" {
						ids = []string{"absent-00000"}
					}
					statement += " AND service_id IN (?)"
					params = append(params, ids)
				}
				if grouped {
					statement += " GROUP BY service_id ORDER BY service_id ASC"
				}
				c, err := p1Case(fmt.Sprintf("p2_%s_%d_%s", shape, variant, window), statement, params, p2Oracle(grouped, projection, window), grouped)
				if err != nil {
					return nil, err
				}
				c.Ticket, c.Features = "P2", []string{"p2.shard_aggregates"}
				scenario.Cases = append(scenario.Cases, c)
			}
		}
		scenarios = append(scenarios, scenario)
	}
	return scenarios, nil
}

func validateP2(s Suite) error {
	expected, err := generateP2()
	if err != nil {
		return err
	}
	contribution, contributionErr := P2ContributionReadiness()
	if contributionErr != nil {
		return contributionErr
	}
	readiness := []AggregateFixture{{Name: P1FixtureName, Cases: []Case{contribution}}}
	want, err := json.Marshal([]any{expected, readiness})
	if err != nil {
		return err
	}
	got, err := json.Marshal([]any{s.ShardAggregateScenarios, s.ShardAggregateReadiness})
	if err != nil {
		return err
	}
	if !bytes.Equal(want, got) {
		return fmt.Errorf("P2 generated shard aggregate coverage or independent oracle changed")
	}
	return nil
}

// P2ContributionReadiness checks one repeated service across naturally routed entities.
func P2ContributionReadiness() (Case, error) {
	statement := "SELECT service_id, SUM(value::FIELD) AS total, COUNT(DISTINCT entity_id) AS entities, " +
		"COUNT(DISTINCT service_id) AS services FROM qlperf.activity WHERE time >= ? AND time < ? " +
		"AND value::FIELD > 0 AND service_id IN (?) GROUP BY service_id ORDER BY service_id ASC"
	points := p2Oracle(true, 3, "selected")[:1]
	c, err := p1Case("p2_worker_contribution", statement,
		[]any{timePlaceholder(0), timePlaceholder(2000), []string{"present-0000"}}, points, true)
	c.Ticket, c.Features = "P2", []string{"p2.shard_aggregates"}
	return c, err
}
