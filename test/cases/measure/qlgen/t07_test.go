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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

func TestT07ScalarOracleHasExactCountAndNullableEmptyResults(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	for _, scenario := range suite.AggregateScenarios {
		if scenario.Name != "t07_multiple_v" && scenario.Name != "t07_multiple_f" {
			continue
		}
		t.Run(scenario.Name, func(t *testing.T) {
			if scenario.Name == "t07_multiple_v" {
				require.JSONEq(t, `{"dataPoints":[{"fields":[
 {"name":"c","value":{"int":{"value":"4"}}},
 {"name":"s","value":{"int":{"value":"12"}}},
 {"name":"lo","value":{"int":{"value":"0"}}},
 {"name":"hi","value":{"int":{"value":"6"}}},
 {"name":"a","value":{"float":{"value":3}}}]}]}`, string(scenario.Cases[0].WantJSON))
			} else {
				require.JSONEq(t, `{"dataPoints":[{"fields":[
 {"name":"c","value":{"int":{"value":"5"}}},
 {"name":"s","value":{"float":{"value":9}}},
 {"name":"lo","value":{"float":{"value":0.25}}},
 {"name":"hi","value":{"float":{"value":4}}},
 {"name":"a","value":{"float":{"value":1.8}}}]}]}`, string(scenario.Cases[0].WantJSON))
			}
			for _, c := range scenario.Cases[1:] {
				require.JSONEq(t, `{"dataPoints":[{"fields":[
 {"name":"c","value":{"int":{"value":"0"}}},
 {"name":"s","value":{"null":0}},{"name":"lo","value":{"null":0}},
 {"name":"hi","value":{"null":0}},{"name":"a","value":{"null":0}}]}]}`, string(c.WantJSON))
			}
		})
	}
}

func TestT07GroupOracleRetainsNullAndEmptyAndCompleteInput(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	for _, scenario := range suite.AggregateScenarios {
		switch scenario.Name {
		case "t07_nullable_key_zone":
			points := t03ExpectedPoints(t, scenario.Cases[0])
			require.Len(t, points, 4)
			var nulls, empty int
			for _, point := range points {
				content, marshalErr := json.Marshal(point["tagFamilies"])
				require.NoError(t, marshalErr)
				if strings.Contains(string(content), `"null":0`) {
					nulls++
				}
				if strings.Contains(string(content), `"value":""`) {
					empty++
				}
			}
			require.Equal(t, 1, nulls)
			require.Equal(t, 1, empty)
			require.Empty(t, t03ExpectedPoints(t, scenario.Cases[2]))
		case "t07_complete_groups":
			require.Len(t, t03ExpectedPoints(t, scenario.Cases[0]), 513)
			require.Equal(t, uint64(1026), scenario.Cases[0].AggregateInputs)
			require.Equal(t, uint64(513), scenario.Cases[0].AggregateGroups)
			require.Len(t, t03ExpectedPoints(t, scenario.Cases[1]), 1)
			require.Empty(t, t03ExpectedPoints(t, scenario.Cases[2]))
		}
	}
}

func TestT07GeneratedCallsParseAndRebindTheSameText(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	base := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	for _, scenario := range suite.AggregateScenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			var first string
			for _, c := range scenario.Cases {
				rendered, renderErr := Render(c.Query, base)
				require.NoError(t, renderErr)
				query, params, extractErr := helpers.ExtractQL(rendered)
				require.NoError(t, extractErr)
				if first == "" {
					first = query
				}
				require.Equal(t, first, query)
				prepared, prepareErr := bydbql.Prepare(query)
				require.NoError(t, prepareErr)
				_, bindErr := prepared.Bind(params)
				require.NoError(t, bindErr)
			}
		})
	}
	for _, fixture := range suite.AggregateReadiness {
		for _, c := range fixture.Cases {
			rendered, renderErr := Render(c.Query, base)
			require.NoError(t, renderErr)
			_, prepareErr := bydbql.Prepare(rendered)
			require.NoError(t, prepareErr)
		}
	}
}

func TestT07FixtureVersionsPreserveLogicalIdentityAndFinalValues(t *testing.T) {
	base := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	for _, entry := range []struct {
		name string
		rows int
	}{{T07FixtureName, 6}, {T07GroupsFixtureName, 1026}} {
		writes := T07FixtureWrites(entry.name, base)
		rows := t07Rows(entry.name)
		require.Len(t, writes, entry.rows*2)
		for ordinal := range entry.rows {
			old, final := writes[ordinal], writes[ordinal+entry.rows]
			require.Equal(t, old.GetDataPoint().GetTimestamp(), final.GetDataPoint().GetTimestamp())
			require.Equal(t, old.GetDataPoint().GetTagFamilies(), final.GetDataPoint().GetTagFamilies())
			require.Equal(t, int64(-999), old.GetDataPoint().GetFields()[0].GetInt().GetValue())
			require.Equal(t, fieldValue(rows[ordinal].V), final.GetDataPoint().GetFields()[0])
		}
	}
}

func TestT07ContractRejectsMissingScenariosAndReadiness(t *testing.T) {
	for _, mode := range []string{"readiness", "scenarios", "bindings", "text", "count-feature"} {
		t.Run(mode, func(t *testing.T) {
			suite, err := Generate()
			require.NoError(t, err)
			switch mode {
			case "readiness":
				suite.AggregateReadiness = nil
			case "scenarios":
				suite.AggregateScenarios = nil
			case "bindings":
				suite.AggregateScenarios[0].Cases = suite.AggregateScenarios[0].Cases[:2]
			case "text":
				suite.AggregateScenarios[0].Cases[1].Query = strings.Replace(suite.AggregateScenarios[0].Cases[1].Query, "SELECT", "SELECT changed,", 1)
			case "count-feature":
				for index := range suite.AggregateScenarios {
					if strings.HasPrefix(suite.AggregateScenarios[index].Name, "t07_count_") {
						suite.AggregateScenarios[index].Cases[0].Features = nil
					}
				}
			}
			require.Error(t, suite.Validate())
		})
	}
}
