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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

func TestT05GeneratedRebindCohortsKeepExactText(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	require.NotEmpty(t, suite.ReuseScenarios)
	base := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	for _, scenario := range suite.ReuseScenarios {
		if scenario.Kind != "rebind" {
			continue
		}
		t.Run(scenario.Name, func(t *testing.T) {
			require.GreaterOrEqual(t, len(scenario.Cases), 3)
			var first string
			results := make(map[string]bool)
			for _, c := range scenario.Cases {
				rendered, renderErr := Render(c.Query, base)
				require.NoError(t, renderErr)
				query, _, extractErr := helpers.ExtractQL(rendered)
				require.NoError(t, extractErr)
				if first == "" {
					first = query
				}
				require.Equal(t, first, query)
				results[string(c.WantJSON)] = true
			}
			require.GreaterOrEqual(t, len(results), 2, "a rebind cohort must prove that new values alter results")
		})
	}
}

func TestT05ReuseScenarioContractCannotLoseWitnesses(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	suite.ReuseScenarios = nil
	content, err := json.Marshal(suite)
	require.NoError(t, err)
	_, err = Load(content)
	require.ErrorContains(t, err, "T05")
}

func TestT05ScenarioQueriesParseAndBind(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	base := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	for _, scenario := range suite.ReuseScenarios {
		for _, c := range scenario.Cases {
			if c.WantCode == codes.InvalidArgument {
				continue
			}
			t.Run(scenario.Name+"/"+c.Name, func(t *testing.T) {
				rendered, renderErr := Render(c.Query, base)
				require.NoError(t, renderErr)
				query, params, extractErr := helpers.ExtractQL(rendered)
				require.NoError(t, extractErr)
				grammar, parseErr := bydbql.ParseQuery(query)
				require.NoError(t, parseErr)
				require.NoError(t, bydbql.BindParams(grammar, params))
			})
		}
	}
}

func TestT05RebindAndLifecycleExpectedResults(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	for _, scenario := range suite.ReuseScenarios {
		switch scenario.Name {
		case "t05_rebind_typed":
			require.Equal(t, []string{"1", "2"}, t03ExpectedRIDs(t, scenario.Cases[0]))
			require.Equal(t, []string{"3", "4"}, t03ExpectedRIDs(t, scenario.Cases[1]))
			require.Equal(t, []string{"5", "6"}, t03ExpectedRIDs(t, scenario.Cases[2]))
		case "t05_rebind_limit":
			require.Len(t, t03ExpectedPoints(t, scenario.Cases[0]), 3)
			require.Empty(t, t03ExpectedPoints(t, scenario.Cases[1]))
			require.True(t, scenario.Cases[1].NoScan)
			require.Len(t, t03ExpectedPoints(t, scenario.Cases[2]), 5)
		case "t05_rebind_array":
			require.Equal(t, []string{"1", "2"}, t03ExpectedRIDs(t, scenario.Cases[0]))
			require.Empty(t, t03ExpectedPoints(t, scenario.Cases[1]))
			require.Equal(t, []string{"3", "4", "7", "8"}, t03ExpectedRIDs(t, scenario.Cases[2]))
		case "t05_rebind_null":
			require.Equal(t, codes.InvalidArgument, scenario.Cases[1].WantCode)
		case "t05_schema_replace_delete_recreate":
			require.Equal(t, codes.Internal, scenario.Cases[2].WantCode)
			require.Equal(t, codes.OK, scenario.Cases[3].WantCode)
		}
	}
}

func TestT05ScenarioContractRejectsMissingFamilyAndCall(t *testing.T) {
	for _, mode := range []string{"rebind", "lifecycle", "caller-count", "missing-expected", "visibility"} {
		t.Run(mode, func(t *testing.T) {
			suite, err := Generate()
			require.NoError(t, err)
			switch mode {
			case "rebind":
				suite.ReuseScenarios = suite.ReuseScenarios[1:]
			case "lifecycle":
				suite.ReuseScenarios = suite.ReuseScenarios[:len(suite.ReuseScenarios)-1]
			case "caller-count":
				for index := range suite.ReuseScenarios {
					if suite.ReuseScenarios[index].Kind == "cold-burst" {
						suite.ReuseScenarios[index].Callers = 31
					}
				}
			case "missing-expected":
				suite.ReuseScenarios[0].Cases[0].WantJSON = nil
			case "visibility":
				suite.ReuseReadiness = Case{}
			}
			content, err := json.Marshal(suite)
			require.NoError(t, err)
			_, err = Load(content)
			require.ErrorContains(t, err, "T05")
		})
	}
}
