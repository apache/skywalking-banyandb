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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountDistinctGeneratedCompositionCoverage(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, scenario := range suite.AggregateScenarios {
		names[scenario.Name] = true
	}
	for _, scenario := range suite.SortingScenarios {
		names[scenario.Name] = true
	}
	for _, name := range []string{
		"t07_count_distinct_scalar", "t07_count_distinct_grouped", "t17_count_distinct_ordered",
		"t17_count_distinct_having", "t17_count_distinct_complete_page",
	} {
		require.True(t, names[name], "missing generated QL composition %s", name)
	}
}

func TestCountDistinctScalarOracleIgnoresNullAndCountsEmpty(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	for _, scenario := range suite.AggregateScenarios {
		if scenario.Name != "t07_count_distinct_scalar" {
			continue
		}
		require.JSONEq(t, `{"dataPoints":[{"fields":[
   {"name":"unique_zones","value":{"int":{"value":"3"}}},
   {"name":"unique_hosts","value":{"int":{"value":"3"}}},
   {"name":"total","value":{"int":{"value":"12"}}}]}]}`, string(scenario.Cases[0].WantJSON))
		require.JSONEq(t, `{"dataPoints":[{"fields":[
   {"name":"unique_zones","value":{"int":{"value":"0"}}},
   {"name":"unique_hosts","value":{"int":{"value":"0"}}},
   {"name":"total","value":{"null":0}}]}]}`, string(scenario.Cases[2].WantJSON))
	}
	for _, scenario := range suite.SortingScenarios {
		if scenario.Name != "t17_count_distinct_complete_page" {
			continue
		}
		first := scenario.Cases[0]
		require.Equal(t, uint64(1153), first.AggregateInputs)
		require.Equal(t, uint64(1153), first.AggregateGroups)
		require.Equal(t, uint64(1153), first.Sorting.InputRows)
		points := t03ExpectedPoints(t, first)
		require.Len(t, points, 7)
		require.Contains(t, string(first.WantJSON), `"value":"1101"`)
		require.Contains(t, string(first.WantJSON), `"value":"1107"`)
	}
}
