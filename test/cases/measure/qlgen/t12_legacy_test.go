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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

func TestT12LegacyGeneratedCompatibilityRetainsLegacyRoute(t *testing.T) {
	readiness, scenarios, err := generateT12Legacy()
	require.NoError(t, err)
	cases := append([]Case(nil), readiness...)
	cases = append(cases, scenarios[0].Cases...)
	schemas := make(map[bydbql.MeasureSource]*databasev1.Measure)
	for _, group := range T12LegacyFixtureGroups() {
		schemas[bydbql.MeasureSource{Group: group, Name: T12LegacyFixtureName}] = T12LegacyFixtureSchema(group)
	}
	base := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			rendered, renderErr := Render(c.Query, base)
			require.NoError(t, renderErr)
			text, params, extractErr := helpers.ExtractQL(rendered)
			require.NoError(t, extractErr)
			statement, prepareErr := bydbql.Prepare(text)
			require.NoError(t, prepareErr)
			template, compileErr := bydbql.NewTransformer(nil).CompileMeasureWithSchemas(context.Background(), statement, schemas)
			require.NoError(t, compileErr, "legacy projections and tag predicates must ignore unrelated heterogeneous columns")
			execution, bindErr := template.BindParams(params, base)
			require.NoError(t, bindErr)
			require.Nil(t, execution.Plan, "unchanged legacy tag predicates retain native execution")
		})
	}
}

func TestT12LegacyIndependentFixtureProjectsOnlyCompatibleColumns(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	require.Len(t, suite.LegacyCompatibilityReadiness, 2)
	require.Len(t, suite.LegacyCompatibilityScenarios, 1)
	cases := suite.LegacyCompatibilityScenarios[0].Cases
	for ordinal, want := range []int{2, 1, 0} {
		require.Len(t, t03ExpectedPoints(t, cases[ordinal]), want)
	}
	points := t03ExpectedPoints(t, cases[0])
	for ordinal, kept := range []string{"11", "21"} {
		fields := points[ordinal]["fields"].([]any)
		require.Len(t, fields, 1)
		require.Equal(t, "kept", fields[0].(map[string]any)["name"])
		require.Equal(t, kept, fields[0].(map[string]any)["value"].(map[string]any)["int"].(map[string]any)["value"])
		tags := points[ordinal]["tagFamilies"].([]any)[0].(map[string]any)["tags"].([]any)
		require.Len(t, tags, 1)
		require.Equal(t, "host", tags[0].(map[string]any)["key"])
		require.Equal(t, []string{"legacy-a", "legacy-b"}[ordinal], tags[0].(map[string]any)["value"].(map[string]any)["str"].(map[string]any)["value"])
	}
	groups := T12LegacyFixtureGroups()
	first, second := T12LegacyFixtureSchema(groups[0]), T12LegacyFixtureSchema(groups[1])
	require.Equal(t, first.GetFields()[0].GetFieldType(), second.GetFields()[0].GetFieldType())
	require.NotEqual(t, first.GetFields()[1].GetFieldType(), second.GetFields()[1].GetFieldType())
	require.NotEqual(t, first.GetTagFamilies()[0].GetTags()[1].GetType(), second.GetTagFamilies()[0].GetTags()[1].GetType())
}

func TestT12LegacyContractRequiresCompatibilityWitness(t *testing.T) {
	for _, mode := range []string{"readiness", "scenarios", "bindings", "text"} {
		t.Run(mode, func(t *testing.T) {
			suite, err := Generate()
			require.NoError(t, err)
			switch mode {
			case "readiness":
				suite.LegacyCompatibilityReadiness = nil
			case "scenarios":
				suite.LegacyCompatibilityScenarios = nil
			case "bindings":
				suite.LegacyCompatibilityScenarios[0].Cases = suite.LegacyCompatibilityScenarios[0].Cases[:2]
			case "text":
				suite.LegacyCompatibilityScenarios[0].Cases[1].Query = "changed"
			}
			require.Error(t, suite.Validate())
		})
	}
}
