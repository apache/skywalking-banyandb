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

func TestParentChildLiteralOracle(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	scenarios := map[string]AggregateScenario{}
	for _, scenario := range suite.ParentChildScenarios {
		scenarios[scenario.Name] = scenario
	}
	require.JSONEq(t, `{"dataPoints":[
 {"tagFamilies":[{"name":"default","tags":[{"key":"service_id","value":{"str":{"value":"a"}}}]}],
 "fields":[{"name":"distinct_count","value":{"int":{"value":"3"}}},{"name":"total","value":{"int":{"value":"42"}}}]},
 {"tagFamilies":[{"name":"default","tags":[{"key":"service_id","value":{"str":{"value":"b"}}}]}],
 "fields":[{"name":"distinct_count","value":{"int":{"value":"2"}}},{"name":"total","value":{"int":{"value":"17"}}}]},
 {"tagFamilies":[{"name":"default","tags":[{"key":"service_id","value":{"str":{"value":"c"}}}]}],
 "fields":[{"name":"distinct_count","value":{"int":{"value":"1"}}},{"name":"total","value":{"int":{"value":"4"}}}]}]}`,
		string(scenarios["parent_child_ordered"].Cases[0].WantJSON))
	require.JSONEq(t, `{"dataPoints":[{"fields":[{"name":"total","value":{"int":{"value":"3"}}}]}]}`, string(scenarios["parent_child_parent_total"].Cases[0].WantJSON))
	require.JSONEq(t, `{"dataPoints":[{"fields":[{"name":"total","value":{"int":{"value":"0"}}}]}]}`, string(scenarios["parent_child_parent_total"].Cases[2].WantJSON))
	require.JSONEq(t, `{"dataPoints":[{"fields":[{"name":"entity_id","value":{"int":{"value":"0"}}}]}]}`, string(scenarios["parent_child_legacy_scalar"].Cases[2].WantJSON))
	require.JSONEq(t, `{"dataPoints":[{"fields":[{"name":"entity_id","value":{"int":{"value":"137"}}}]}]}`,
		string(scenarios["parent_child_global_children"].Cases[0].WantJSON))
	require.JSONEq(t, `{"dataPoints":[{"fields":[{"name":"entity_id","value":{"int":{"value":"3"}}}]}]}`,
		string(scenarios["parent_child_legacy_boundary"].Cases[0].WantJSON))
	require.JSONEq(t, `{"dataPoints":[{"fields":[{"name":"entity_id","value":{"int":{"value":"2"}}}]}]}`,
		string(scenarios["parent_child_legacy_boundary"].Cases[1].WantJSON))
	scalarPages := scenarios["parent_child_scalar_pages"].Cases
	require.JSONEq(t, `{"dataPoints":[{"fields":[{"name":"entity_id","value":{"int":{"value":"0"}}}]}]}`, string(scalarPages[0].WantJSON))
	require.JSONEq(t, `{}`, string(scalarPages[1].WantJSON))
	require.JSONEq(t, `{}`, string(scalarPages[2].WantJSON))
	pages := scenarios["parent_child_tied_pages"].Cases
	require.Len(t, t03ExpectedPoints(t, pages[0]), 7)
	require.Len(t, t03ExpectedPoints(t, pages[1]), 4)
	require.Contains(t, string(pages[0].WantJSON), `"value":"parent-100"`)
	require.Contains(t, string(pages[0].WantJSON), `"value":"parent-106"`)
	require.Contains(t, string(pages[1].WantJSON), `"value":"parent-133"`)
	require.Contains(t, string(pages[1].WantJSON), `"value":"parent-136"`)
	require.JSONEq(t, `{}`, string(pages[2].WantJSON))
	boundary := scenarios["parent_child_boundary"].Cases
	require.Contains(t, string(boundary[0].WantJSON), `"value":"30"`)
	require.Contains(t, string(boundary[1].WantJSON), `"value":"12"`)
	require.JSONEq(t, `{}`, string(boundary[2].WantJSON))
}
