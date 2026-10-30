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

	"google.golang.org/grpc/codes"
)

func generateT14Review(b *suiteBuilder) ([]HavingScenario, error) {
	scenarios, err := t14IdentifierScenarios()
	if err != nil {
		return nil, err
	}
	indexed, indexedErr := t14IndexedScenarios()
	if indexedErr != nil {
		return nil, indexedErr
	}
	b.features["t14.contextual_contains"] = true
	b.features["t14.indexed_array_rejection"] = false
	return append(scenarios, indexed...), nil
}

func t14IdentifierScenarios() ([]HavingScenario, error) {
	var scenarios []HavingScenario
	for _, shape := range []struct {
		name, operator string
		typed          bool
	}{
		{name: "equal", operator: "="},
		{name: "contains", operator: "cOnTaInS"},
		{name: "not_contains", operator: "NOT CONTAINS"},
		{name: "typed_contains", operator: "CONTAINS", typed: true},
		{name: "typed_not_contains", operator: "NOT CONTAINS", typed: true},
	} {
		scenario := HavingScenario{Name: "t14_contextual_" + shape.name, Kind: "identifier"}
		column := "contains"
		residual := ""
		if shape.typed {
			column += "::TAG"
			residual = " AND v::FIELD > 1"
		}
		query := "SELECT contains FROM MEASURE " + T14ContainsFixtureName + " IN " + T14ContainsFixtureName +
			" TIME BETWEEN ? AND ? WHERE " + column + " " + shape.operator + " ?" + residual + " ORDER BY TIME ASC"
		for ordinal, wanted := range []string{"a", "b", "absent"} {
			var points []any
			for _, row := range t14ContainsRows() {
				matches := row.Host == wanted
				if strings.HasPrefix(shape.operator, "NOT ") {
					matches = !matches
				}
				if !matches || (shape.typed && row.V.(int64) <= 1) {
					continue
				}
				points = append(points, map[string]any{
					"timestamp": timePlaceholder(row.Millis),
					"tagFamilies": []any{map[string]any{"name": "default", "tags": []any{
						map[string]any{"key": "contains", "value": jsonValue(row.Host)},
					}}},
				})
			}
			bound, bindErr := t14ReviewParameters(query, wanted)
			if bindErr != nil {
				return nil, bindErr
			}
			c, caseErr := t07ExpectedCase(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), bound, points, []string{"t14.contextual_contains"})
			if caseErr != nil {
				return nil, caseErr
			}
			c.Ticket, c.Ordered = "T14", true
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
	}
	return scenarios, nil
}

func t14IndexedScenarios() ([]HavingScenario, error) {
	var scenarios []HavingScenario
	for _, column := range []string{"labels", "nums"} {
		values := []any{"red", "blue", []string{"red", "blue"}}
		if column == "nums" {
			values = []any{int64(1), int64(2), []int64{1, 2}}
		}
		for _, operator := range []string{"HAVING", "NOT HAVING", "CONTAINS", "NOT CONTAINS"} {
			for _, typed := range []bool{true, false} {
				source, mode, residual := column, "residual", " AND v::FIELD > 0"
				if typed {
					source, mode, residual = column+"::TAG", "typed", ""
				}
				scenario := HavingScenario{
					Name: "t14_indexed_" + column + "_" + featureName(operator) + "_" + mode, Kind: "indexed-array-rejection",
				}
				query := "SELECT rid FROM MEASURE " + T14IndexedFixtureName + " IN " + FixtureGroup +
					" TIME BETWEEN ? AND ? WHERE " + source + " " + operator + " ?" + residual + " ORDER BY TIME ASC"
				for ordinal, value := range values {
					bound, err := t14ReviewParameters(query, value)
					if err != nil {
						return nil, err
					}
					scenario.Cases = append(scenario.Cases, Case{
						Name: fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), Ticket: "T14", Query: bound,
						WantCode: codes.InvalidArgument, WantMessage: "indexed array", NoScan: true,
						Features: []string{"t14.indexed_array_rejection"},
					})
				}
				scenarios = append(scenarios, scenario)
			}
		}
	}
	return scenarios, nil
}

func t14ReviewParameters(query string, value any) (string, error) {
	params, err := json.Marshal([]any{jsonValue(timePlaceholder(-1)), jsonValue(timePlaceholder(8000)), jsonValue(value)})
	if err != nil {
		return "", err
	}
	return query + "\n#!params: " + string(params), nil
}

func validateT14ReviewScenarios(scenarios []HavingScenario) error {
	if len(scenarios) != 21 {
		return fmt.Errorf("T14 contextual identifiers and indexed-array rejection matrix is incomplete")
	}
	for _, scenario := range scenarios {
		if len(scenario.Cases) != 3 {
			return fmt.Errorf("T14 review scenario %s lacks three bindings", scenario.Name)
		}
		first, _, _ := strings.Cut(scenario.Cases[0].Query, "\n#!params:")
		for _, c := range scenario.Cases {
			text, _, _ := strings.Cut(c.Query, "\n#!params:")
			if text != first {
				return fmt.Errorf("T14 review scenario %s changes SQL text", scenario.Name)
			}
			switch scenario.Kind {
			case "identifier":
				if c.WantCode != codes.OK {
					return fmt.Errorf("T14 contextual identifier %s lost its success expectation", c.Name)
				}
			case "indexed-array-rejection":
				if c.WantCode != codes.InvalidArgument || c.WantMessage != "indexed array" || !c.NoScan {
					return fmt.Errorf("T14 indexed array %s lost its rejection before dispatch", c.Name)
				}
			default:
				return fmt.Errorf("unknown T14 review scenario kind %s", scenario.Kind)
			}
		}
	}
	return nil
}
