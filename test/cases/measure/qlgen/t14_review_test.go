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

func TestT14ReviewQueriesCompileWithContextualIdentifiers(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	base := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	schemas := map[bydbql.MeasureSource]*databasev1.Measure{}
	for _, name := range []string{T14ContainsFixtureName, T14IndexedFixtureName} {
		schema := T14FixtureSchema(name)
		schemas[bydbql.MeasureSource{Group: schema.Metadata.Group, Name: name}] = schema
	}
	for _, scenario := range suite.HavingReviewScenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			var template *bydbql.MeasureTemplate
			for ordinal, c := range scenario.Cases {
				rendered, renderErr := Render(c.Query, base)
				require.NoError(t, renderErr)
				query, params, extractErr := helpers.ExtractQL(rendered)
				require.NoError(t, extractErr)
				if ordinal == 0 {
					statement, prepareErr := bydbql.Prepare(query)
					require.NoError(t, prepareErr)
					template, err = bydbql.NewTransformer(nil).CompileMeasureWithSchemas(context.Background(), statement, schemas)
					require.NoError(t, err)
				}
				// The public compiler receives schemas without current index bindings.
				// Indexed-array rejection is therefore asserted by the real RPC adapter.
				execution, bindErr := template.BindParams(params, base)
				require.NoError(t, bindErr)
				if scenario.Kind == "indexed-array-rejection" {
					require.NotNil(t, execution.Plan.Filter)
				}
			}
		})
	}
}

func TestT14ReviewOracleAndIndexFixture(t *testing.T) {
	scenarios, err := t14IdentifierScenarios()
	require.NoError(t, err)
	for _, scenario := range scenarios {
		wantCounts := []int{2, 1, 0}
		switch scenario.Name {
		case "t14_contextual_not_contains":
			wantCounts = []int{1, 2, 3}
		case "t14_contextual_typed_contains":
			wantCounts = []int{1, 1, 0}
		case "t14_contextual_typed_not_contains":
			wantCounts = []int{1, 1, 2}
		}
		for ordinal, c := range scenario.Cases {
			require.Len(t, t03ExpectedPoints(t, c), wantCounts[ordinal], c.Name)
		}
	}
	fixtures, err := t14ReviewReadiness()
	require.NoError(t, err)
	for _, fixture := range fixtures {
		for _, c := range fixture.Cases {
			require.NotContains(t, c.Query, "labels")
			require.NotContains(t, c.Query, "nums")
		}
	}
	writes := T14FixtureWrites(T14IndexedFixtureName, time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC))
	require.Equal(t, []string{"red", "blue"}, writes[0].DataPoint.TagFamilies[0].Tags[3].GetStrArray().GetValue())
	require.Equal(t, []int64{1, 2}, writes[0].DataPoint.TagFamilies[0].Tags[4].GetIntArray().GetValue())
	rules := T14ReviewIndexRules(T14IndexedFixtureName)
	require.Equal(t, []string{"labels"}, rules[0].Tags)
	require.Equal(t, []string{"nums"}, rules[1].Tags)
}

func TestT14ReviewContractRejectsMissingCompatibilityEvidence(t *testing.T) {
	for _, mode := range []string{"fixture", "scenario", "binding", "text", "code", "message", "dispatch"} {
		t.Run(mode, func(t *testing.T) {
			suite, err := Generate()
			require.NoError(t, err)
			switch mode {
			case "fixture":
				suite.HavingReadiness[4].Cases = nil
			case "scenario":
				suite.HavingReviewScenarios = suite.HavingReviewScenarios[1:]
			case "binding":
				suite.HavingReviewScenarios[0].Cases = suite.HavingReviewScenarios[0].Cases[:2]
			case "text":
				suite.HavingReviewScenarios[0].Cases[1].Query = strings.Replace(suite.HavingReviewScenarios[0].Cases[1].Query, "SELECT", "SELECT rid,", 1)
			case "code":
				suite.HavingReviewScenarios[5].Cases[0].WantCode = 0
			case "message":
				suite.HavingReviewScenarios[5].Cases[0].WantMessage = ""
			case "dispatch":
				suite.HavingReviewScenarios[5].Cases[0].NoScan = false
			}
			require.Error(t, suite.Validate())
		})
	}
}
