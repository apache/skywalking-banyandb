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
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
)

// LegacyCompatibilityScenario declares legacy QL requests across heterogeneous schemas.
type LegacyCompatibilityScenario struct {
	Name  string `json:"name"`
	Cases []Case `json:"cases"`
}

type t12LegacyQuery struct {
	Hosts  []string
	Groups []string
}

func buildT12LegacyCase(name string, q t12LegacyQuery, features ...string) (Case, error) {
	query := "SELECT host, kept::FIELD FROM MEASURE " + T12LegacyFixtureName + " IN (" + strings.Join(q.Groups, ", ") + ") TIME BETWEEN ? AND ?"
	params := []any{jsonValue(timePlaceholder(-1)), jsonValue(timePlaceholder(4000))}
	if len(q.Hosts) > 0 {
		query += " WHERE host IN (?)"
		params = append(params, jsonValue(q.Hosts))
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		return Case{}, err
	}
	query += "\n#!params: " + string(encoded)
	var points []any
	for _, row := range t12LegacyRows() {
		if !slices.Contains(q.Groups, row.Group) || len(q.Hosts) > 0 && !slices.Contains(q.Hosts, row.Host) {
			continue
		}
		points = append(points, map[string]any{
			"timestamp":   timePlaceholder(row.Millis),
			"tagFamilies": []any{map[string]any{"name": "default", "tags": []any{map[string]any{"key": "host", "value": jsonValue(row.Host)}}}},
			"fields":      []any{map[string]any{"name": "kept", "value": jsonValue(row.Kept)}},
		})
	}
	c, caseErr := t07ExpectedCase(name, query, points, features)
	c.Ticket = "T12"
	return c, caseErr
}

func generateT12Legacy() ([]Case, []LegacyCompatibilityScenario, error) {
	var readiness []Case
	groups := T12LegacyFixtureGroups()
	for ordinal, group := range groups {
		c, err := buildT12LegacyCase(fmt.Sprintf("t12_legacy_ready_%d", ordinal), t12LegacyQuery{Groups: []string{group}}, "t12.legacy_schema_fixture")
		if err != nil {
			return nil, nil, err
		}
		readiness = append(readiness, c)
	}
	scenario := LegacyCompatibilityScenario{Name: "t12_legacy_unused_schema_types"}
	for ordinal, hosts := range [][]string{{"legacy-a", "legacy-b"}, {"legacy-a"}, {"missing"}} {
		c, err := buildT12LegacyCase(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), t12LegacyQuery{Groups: groups, Hosts: hosts}, "t12.legacy_schema_compatibility")
		if err != nil {
			return nil, nil, err
		}
		scenario.Cases = append(scenario.Cases, c)
	}
	return readiness, []LegacyCompatibilityScenario{scenario}, nil
}

func validateT12Legacy(readiness []Case, scenarios []LegacyCompatibilityScenario) error {
	if len(readiness) != 2 || len(scenarios) != 1 || len(scenarios[0].Cases) != 3 {
		return fmt.Errorf("generated T12 legacy compatibility lacks both source fixtures or repeated bindings")
	}
	for ordinal, c := range readiness {
		if c.Name != fmt.Sprintf("t12_legacy_ready_%d", ordinal) {
			return fmt.Errorf("generated T12 legacy readiness names changed")
		}
	}
	var first string
	for _, c := range scenarios[0].Cases {
		text, _, _ := strings.Cut(c.Query, "\n#!params:")
		if first == "" {
			first = text
		}
		if text != first || c.WantCode != codes.OK || c.Filter != nil {
			return fmt.Errorf("generated T12 legacy compatibility changes its text, route, or success contract")
		}
	}
	return nil
}
