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
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

func TestT08ResidualTimeRemainsDisjointForEveryBinding(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	base := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	for _, scenario := range suite.NativeAggregateScenarios {
		if scenario.Name != "t08_fallback_residual_time" {
			continue
		}
		for _, c := range scenario.Cases {
			t.Run(c.Name, func(t *testing.T) {
				rendered, renderErr := Render(c.Query, base)
				require.NoError(t, renderErr)
				query, params, extractErr := helpers.ExtractQL(rendered)
				require.NoError(t, extractErr)
				statement, prepareErr := bydbql.Prepare(query)
				require.NoError(t, prepareErr)
				template, compileErr := bydbql.NewTransformer(nil).CompileMeasureWithSchemas(context.Background(), statement,
					map[bydbql.MeasureSource]*databasev1.Measure{{Group: FixtureGroup, Name: T08FixtureName}: T08FixtureSchema(T08FixtureName)})
				require.NoError(t, compileErr)
				execution, bindErr := template.BindParams(params, base)
				require.NoError(t, bindErr)
				require.False(t, execution.Plan.Empty, "empty storage input must still retain a genuine residual predicate")
				require.True(t, execution.Plan.Residual, "every fallback cohort binding must retain disjoint time ranges")
				require.GreaterOrEqual(t, len(execution.Plan.Ranges), 2)
				require.Nil(t, execution.Plan.NativeAggregate)
			})
		}
		return
	}
	t.Fatal("missing residual time cohort")
}

func TestT08OracleKeepsUnequalPopulationsAndSubOneMeans(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	for _, scenario := range suite.NativeAggregateScenarios {
		if scenario.Name != "t08_scalar_single_entity" {
			continue
		}
		require.JSONEq(t, `{"dataPoints":[{"fields":[
   {"name":"s","value":{"float":{"value":1}}},
   {"name":"c","value":{"int":{"value":"2"}}},
   {"name":"a","value":{"float":{"value":0.5}}},
   {"name":"copy","value":{"float":{"value":0.5}}},
   {"name":"mean","value":{"float":{"value":0.5}}}]}]}`, string(scenario.Cases[0].WantJSON))
		return
	}
	t.Fatal("missing generated scalar aggregation witness")
}

func TestT08OraclePreservesExactIntegerExtremaAndNullableGroups(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	for _, scenario := range suite.NativeAggregateScenarios {
		if scenario.Name != "t08_integer_extrema" {
			continue
		}
		points := t03ExpectedPoints(t, scenario.Cases[0])
		require.Len(t, points, 4)
		fields := points[0]["fields"].([]any)
		require.Equal(t, "9007199254740993", fields[0].(map[string]any)["value"].(map[string]any)["int"].(map[string]any)["value"])
		require.Equal(t, "9007199254740995", fields[1].(map[string]any)["value"].(map[string]any)["int"].(map[string]any)["value"])
		require.Equal(t, "2", fields[2].(map[string]any)["value"].(map[string]any)["int"].(map[string]any)["value"])
		fields = points[3]["fields"].([]any)
		require.Equal(t, map[string]any{"null": float64(0)}, fields[0].(map[string]any)["value"])
		require.Equal(t, map[string]any{"null": float64(0)}, fields[1].(map[string]any)["value"])
		require.Equal(t, "0", fields[2].(map[string]any)["value"].(map[string]any)["int"].(map[string]any)["value"])
		return
	}
	t.Fatal("missing exact integer extrema witness")
}

func TestT08OracleRetainsEveryTupleAndUsesPopulationWeightedMean(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	for _, scenario := range suite.NativeAggregateScenarios {
		switch scenario.Name {
		case "t08_complete_tuple_groups":
			require.Len(t, t03ExpectedPoints(t, scenario.Cases[0]), 129)
			require.Equal(t, uint64(258), scenario.Cases[0].AggregateInputs)
			require.Equal(t, uint64(129), scenario.Cases[0].AggregateGroups)
			require.Empty(t, t03ExpectedPoints(t, scenario.Cases[2]))
		case "t08_fallback_unpruned_scalar":
			require.JSONEq(t, `{"dataPoints":[{"fields":[
 {"name":"s","value":{"float":{"value":2.625}}},
 {"name":"c","value":{"int":{"value":"4"}}},
 {"name":"a","value":{"float":{"value":0.65625}}},
 {"name":"copy","value":{"float":{"value":0.65625}}},
 {"name":"mean","value":{"float":{"value":0.65625}}}]}]}`, string(scenario.Cases[0].WantJSON))
		}
	}
}

func TestT08GeneratedCallsParseAndRebindTheSameText(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	base := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	for _, scenario := range suite.NativeAggregateScenarios {
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
	for _, fixture := range suite.NativeAggregateReadiness {
		for _, c := range fixture.Cases {
			rendered, renderErr := Render(c.Query, base)
			require.NoError(t, renderErr)
			_, prepareErr := bydbql.Prepare(rendered)
			require.NoError(t, prepareErr)
		}
	}
}

func TestT08FixtureVersionsPreserveLogicalIdentityAndFinalValues(t *testing.T) {
	base := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{T08FixtureName, T08GroupsFixtureName} {
		writes := T08FixtureWrites(name, base)
		rows := t08Rows(name)
		require.Len(t, writes, len(rows)*2)
		for ordinal, row := range rows {
			old, final := writes[ordinal], writes[ordinal+len(rows)]
			require.Equal(t, old.GetDataPoint().GetTimestamp(), final.GetDataPoint().GetTimestamp())
			require.Equal(t, old.GetDataPoint().GetTagFamilies(), final.GetDataPoint().GetTagFamilies())
			require.Equal(t, int64(-999), old.GetDataPoint().GetFields()[0].GetInt().GetValue())
			require.Equal(t, fieldValue(row.V), final.GetDataPoint().GetFields()[0])
		}
	}
}

func TestT08IndexedKeyIsConstantWithinTheCompleteEntity(t *testing.T) {
	for _, name := range []string{T08FixtureName, T08GroupsFixtureName} {
		keys := make(map[[2]string]any)
		for _, row := range t08Rows(name) {
			entity := [2]string{row.Host, row.Region}
			key, exists := keys[entity]
			if exists {
				require.Equal(t, key, row.Key, "indexed key must remain constant for %v", entity)
			} else {
				keys[entity] = row.Key
			}
		}
		binding := T08FixtureIndexBinding(name)
		require.Equal(t, name, binding.GetSubject().GetName())
		require.Equal(t, []string{T08FixtureIndexRule(name).GetMetadata().GetName()}, binding.GetRules())
		readiness, err := t08IndexedReadiness(name)
		require.NoError(t, err)
		require.NotEmpty(t, t03ExpectedPoints(t, readiness), "index visibility witness must observe actual rows")
	}
}

func TestT08ContractRejectsMissingReadinessScenariosAndChangedText(t *testing.T) {
	for _, mode := range []string{"readiness", "scenarios", "bindings", "text", "kind"} {
		t.Run(mode, func(t *testing.T) {
			suite, err := Generate()
			require.NoError(t, err)
			switch mode {
			case "readiness":
				suite.NativeAggregateReadiness = nil
			case "scenarios":
				suite.NativeAggregateScenarios = nil
			case "bindings":
				suite.NativeAggregateScenarios[0].Cases = suite.NativeAggregateScenarios[0].Cases[:2]
			case "text":
				suite.NativeAggregateScenarios[0].Cases[1].Query = strings.Replace(suite.NativeAggregateScenarios[0].Cases[1].Query, "SELECT", "SELECT changed,", 1)
			case "kind":
				suite.NativeAggregateScenarios[0].Kind = "unknown"
			}
			require.Error(t, suite.Validate())
		})
	}
}
