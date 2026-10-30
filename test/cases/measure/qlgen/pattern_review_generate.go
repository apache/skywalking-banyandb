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

	"google.golang.org/grpc/codes"
)

func generatePatternReview(b *suiteBuilder, points []any, source string) ([]AggregateFixture, []AggregateScenario, error) {
	var fixtures []AggregateFixture
	var scenarios []AggregateScenario
	for _, name := range PatternFixtureNames()[1:] {
		from := " FROM MEASURE " + name + " IN " + patternFixtureGroup(name) + " WHERE time >= ? AND time < ?"
		ready, err := patternCase("patterns_ready_"+name, "SELECT rid, name, value::FIELD"+from+" AND rid IN (?) LIMIT 100",
			[]any{timePlaceholder(0), timePlaceholder(2000), []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}}, points, "patterns.contextual_identifiers")
		if err != nil {
			return nil, nil, err
		}
		fixtures = append(fixtures, AggregateFixture{Name: name, Cases: []Case{ready}})
		scenario := AggregateScenario{Name: "patterns_identifier_" + name}
		for i, pattern := range []any{"%amber%", "a!_b", "%"} {
			count, countErr := patternCount(pattern, "!", false, false)
			if countErr != nil {
				return nil, nil, countErr
			}
			output := []any{map[string]any{"fields": []any{map[string]any{"name": "total", "value": jsonValue(count)}}}}
			c, caseErr := patternCase(fmt.Sprintf("%s_binding_%d", scenario.Name, i),
				"SELECT COUNT(DISTINCT name) AS total"+from+" AND name LIKE ? ESCAPE '!'",
				[]any{timePlaceholder(0), timePlaceholder(2000), pattern}, output, "patterns.contextual_identifiers")
			if caseErr != nil {
				return nil, nil, caseErr
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
	}
	b.features["patterns.contextual_identifiers"] = true
	for _, tag := range patternAmbiguousTags() {
		scenario := AggregateScenario{Name: "patterns_reject_ambiguous_" + tag}
		for i, pattern := range []any{"%", nil, "no-match"} {
			c, err := patternCase(fmt.Sprintf("%s_binding_%d", scenario.Name, i),
				"SELECT COUNT(DISTINCT name) AS total"+source+" AND "+tag+" LIKE ?",
				[]any{timePlaceholder(0), timePlaceholder(2000), pattern}, nil, "patterns.ambiguous_indexes")
			if err != nil {
				return nil, nil, err
			}
			c.WantCode = codes.InvalidArgument
			c.WantJSON = nil
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
	}
	b.features["patterns.ambiguous_indexes"] = false
	return fixtures, scenarios, nil
}
