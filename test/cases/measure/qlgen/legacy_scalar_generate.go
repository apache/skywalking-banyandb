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
)

// Scalar count identity and final pagination use the existing native aggregate.
// Each case binds the same statement; expectations come only from fixture rows.
func generateLegacyScalarCounts(b *suiteBuilder) {
	statement := "SELECT COUNT(DISTINCT rid) FROM MEASURE " + FixtureName + " IN " + FixtureGroup +
		" TIME BETWEEN ? AND ? WHERE rid >= ? LIMIT ? OFFSET ?"
	for _, scenario := range []struct {
		name                               string
		begin, end, minimum, limit, offset int64
	}{
		{"nonempty", 0, 8000, 0, 1, 0},
		{"idle", 9000, 10000, 0, 1, 0},
		{"no_match", 0, 8000, 99, 1, 0},
		{"idle_limit_zero", 9000, 10000, 0, 0, 0},
		{"nonempty_limit_zero", 0, 8000, 0, 0, 0},
		{"idle_offset", 9000, 10000, 0, 1, 1},
		{"nonempty_offset", 0, 8000, 0, 1, 1},
	} {
		if b.err != nil {
			return
		}
		unique := make(map[int64]struct{})
		for _, row := range fixtureRows() {
			if row.Millis >= scenario.begin && row.Millis <= scenario.end && row.RID >= scenario.minimum {
				unique[row.RID] = struct{}{}
			}
		}
		expected := map[string]any{}
		if scenario.limit > 0 && scenario.offset == 0 {
			expected["dataPoints"] = []any{map[string]any{"fields": []any{map[string]any{
				"name": "rid", "value": jsonValue(int64(len(unique))),
			}}}}
		}
		want, err := json.Marshal(expected)
		if err != nil {
			b.err = fmt.Errorf("encode scalar count expectation: %w", err)
			return
		}
		params, err := json.Marshal([]any{
			jsonValue(timePlaceholder(scenario.begin)), jsonValue(timePlaceholder(scenario.end)),
			jsonValue(scenario.minimum), jsonValue(scenario.limit), jsonValue(scenario.offset),
		})
		if err != nil {
			b.err = fmt.Errorf("encode scalar count parameters: %w", err)
			return
		}
		b.cases = append(b.cases, Case{
			Name: "generated_legacy_scalar_count_" + scenario.name, Ticket: "LegacyScalar",
			Query: statement + "\n#!params: " + string(params), WantJSON: want,
			Features: []string{"aggregate.COUNT_DISTINCT"},
		})
		b.features["aggregate.COUNT_DISTINCT"] = true
	}
}
