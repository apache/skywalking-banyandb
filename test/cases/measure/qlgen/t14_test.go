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
	"google.golang.org/grpc/codes"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

func TestT14PublicGroupedHavingCompile(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT host, SUM(v) AS total FROM replicated_group.ql_t14 WHERE time >= ? AND time < ? GROUP BY host HAVING SUM(v::FIELD) > 3")
	require.NoError(t, err)
	schema := FixtureSchema()
	schema.Metadata.Name = "ql_t14"
	template, err := bydbql.NewTransformer(nil).CompileMeasureWithSchemas(context.Background(), statement,
		map[bydbql.MeasureSource]*databasev1.Measure{{Group: FixtureGroup, Name: "ql_t14"}: schema})
	require.NoError(t, err)
	base := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	_, err = template.BindParams([]*modelv1.TagValue{
		tagValue(base.Add(-time.Millisecond).Format(time.RFC3339Nano)), tagValue(base.Add(10 * time.Second).Format(time.RFC3339Nano)),
	}, base)
	require.NoError(t, err)
}

func TestT14GeneratedQueriesCompileAndRebind(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	base := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	schemas := map[bydbql.MeasureSource]*databasev1.Measure{}
	for _, name := range []string{T14FixtureName, T14GroupsFixtureName} {
		schemas[bydbql.MeasureSource{Group: FixtureGroup, Name: name}] = T14FixtureSchema(name)
	}
	for _, scenario := range suite.HavingScenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			var template *bydbql.MeasureTemplate
			var statement *bydbql.PreparedStatement
			for ordinal, c := range scenario.Cases {
				rendered, renderErr := Render(c.Query, base)
				require.NoError(t, renderErr)
				query, params, extractErr := helpers.ExtractQL(rendered)
				require.NoError(t, extractErr)
				if ordinal == 0 {
					var prepareErr error
					statement, prepareErr = bydbql.Prepare(query)
					require.NoError(t, prepareErr)
					template, err = bydbql.NewTransformer(nil).CompileMeasureWithSchemas(context.Background(), statement, schemas)
					require.NoError(t, err)
				}
				_, preparedBindErr := statement.Bind(params)
				if c.PreparedRejected {
					require.Error(t, preparedBindErr)
				} else {
					require.NoError(t, preparedBindErr)
				}
				execution, bindErr := template.BindParams(params, base)
				if c.WantCode != codes.OK && c.NoScan {
					require.Error(t, bindErr)
				} else {
					require.NoError(t, bindErr)
					if c.Filter != nil {
						require.NotNil(t, execution.Plan.Filter)
					}
					if scenario.Branches > 0 && !c.NoScan {
						require.NotNil(t, execution.Plan.NativeAggregate, "every native cohort includes a source/entity bound proof")
					}
				}
			}
		})
	}
}

func TestT14GeneratedRejectionsFailBeforeDispatch(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	base := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	for _, c := range suite.HavingRejections {
		t.Run(c.Name, func(t *testing.T) {
			rendered, renderErr := Render(c.Query, base)
			require.NoError(t, renderErr)
			query, params, extractErr := helpers.ExtractQL(rendered)
			require.NoError(t, extractErr)
			statement, prepareErr := bydbql.Prepare(query)
			if prepareErr != nil {
				return
			}
			template, compileErr := bydbql.NewTransformer(nil).CompileMeasureWithSchemas(context.Background(), statement,
				map[bydbql.MeasureSource]*databasev1.Measure{{Group: FixtureGroup, Name: T14FixtureName}: T14FixtureSchema(T14FixtureName)})
			if compileErr != nil {
				return
			}
			_, bindErr := template.BindParams(params, base)
			require.Error(t, bindErr, "unsupported HAVING must reject before native execution")
		})
	}
}

func TestT14OracleKeepsPublishedHavingAndMembershipResults(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	found := map[string]bool{}
	for _, scenario := range suite.HavingScenarios {
		c := scenario.Cases[0]
		switch scenario.Name {
		case "t14_having_F", "t14_native_having_F":
			require.Equal(t, uint64(4), c.Having.InputGroups)
			require.Equal(t, uint64(2), c.Having.TrueGroups)
			require.Equal(t, uint64(1), c.Having.UnknownGroups)
			want := `{"dataPoints":[
    {"tagFamilies":[{"name":"default","tags":[{"key":"host","value":{"str":{"value":"b"}}}]}],"fields":[{"name":"total","value":{"int":{"value":"4"}}}]},
    {"tagFamilies":[{"name":"default","tags":[{"key":"host","value":{"str":{"value":"c"}}}]}],"fields":[{"name":"total","value":{"int":{"value":"6"}}}]}]}`
			if scenario.Name == "t14_native_having_F" {
				want = `{"dataPoints":[
    {"tagFamilies":[{"name":"default","tags":[{"key":"host","value":{"str":{"value":"b"}}}]}],"fields":[{"name":"total","value":{"float":{"value":4}}}]},
    {"tagFamilies":[{"name":"default","tags":[{"key":"host","value":{"str":{"value":"c"}}}]}],"fields":[{"name":"total","value":{"float":{"value":6}}}]}]}`
			}
			require.JSONEq(t, want, string(c.WantJSON))
			found[scenario.Name] = true
		case "t14_hidden_aggregate":
			points := t03ExpectedPoints(t, c)
			require.Len(t, points, 2)
			for _, point := range points {
				require.NotContains(t, point, "fields")
			}
			found[scenario.Name] = true
		case "t14_having_null":
			points := t03ExpectedPoints(t, c)
			require.Len(t, points, 1)
			require.JSONEq(t, `{"dataPoints":[{"tagFamilies":[{"name":"default","tags":[{"key":"host","value":{"str":{"value":"n"}}}]}],
 "fields":[{"name":"total","value":{"null":0}}]}]}`, string(c.WantJSON))
			found[scenario.Name] = true
		case "t14_contains_labels_single_F":
			require.JSONEq(t, `{"dataPoints":[{"timestamp":"{{time:1000}}",
 "tagFamilies":[{"name":"default","tags":[{"key":"rid","value":{"int":{"value":"2"}}}]}]}]}`, string(c.WantJSON))
			found[scenario.Name] = true
		case "t14_not_contains_labels_single_F":
			points := t03ExpectedPoints(t, c)
			require.Len(t, points, 2)
			found[scenario.Name] = true
		case "t14_complete_groups":
			require.Len(t, t03ExpectedPoints(t, c), 129)
			found[scenario.Name] = true
		case "t14_having_before_offset":
			require.Empty(t, t03ExpectedPoints(t, c))
			require.Equal(t, uint64(129), c.Having.InputGroups)
			require.Equal(t, uint64(1), c.Having.TrueGroups)
			found[scenario.Name] = true
		}
	}
	require.Len(t, found, 8)
}

func TestT14LegacyWitnessesKeepNativeRequestRouting(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	base := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	for _, scenario := range suite.HavingLegacyScenarios {
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
					template, err = bydbql.NewTransformer(nil).CompileMeasureWithSchemas(context.Background(), statement,
						map[bydbql.MeasureSource]*databasev1.Measure{{Group: FixtureGroup, Name: T14LegacyFixtureName}: T14FixtureSchema(T14LegacyFixtureName)})
					require.NoError(t, err)
				}
				execution, bindErr := template.BindParams(params, base)
				require.NoError(t, bindErr)
				require.Nil(t, execution.Plan, "legacy compatibility witnesses exercise the current native measure path")
			}
		})
	}
}

func TestT14GeneratedContractRejectsMissingHavingEvidence(t *testing.T) {
	for _, mode := range []string{"readiness", "scenario", "legacy", "binding", "text", "truth", "feature"} {
		t.Run(mode, func(t *testing.T) {
			suite, err := Generate()
			require.NoError(t, err)
			switch mode {
			case "readiness":
				suite.HavingReadiness = nil
			case "scenario":
				suite.HavingScenarios = nil
			case "legacy":
				suite.HavingLegacyScenarios = nil
			case "binding":
				suite.HavingScenarios[0].Cases = suite.HavingScenarios[0].Cases[:2]
			case "text":
				suite.HavingScenarios[0].Cases[1].Query = strings.Replace(suite.HavingScenarios[0].Cases[1].Query, "SELECT", "SELECT changed,", 1)
			case "truth":
				suite.HavingScenarios[0].Cases[0].Having.TrueGroups++
			case "feature":
				for index, feature := range suite.Features {
					if feature.ID == "t14.hidden_aggregate" {
						suite.Features = append(suite.Features[:index], suite.Features[index+1:]...)
						break
					}
				}
			}
			require.Error(t, suite.Validate())
		})
	}
}

func TestT14OracleRetainsMeasureScalarMembershipByValue(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	found := 0
	for _, scenario := range suite.HavingLegacyScenarios {
		switch scenario.Name {
		case "t14_legacy_having_host_single", "t14_legacy_having_zone_single",
			"t14_legacy_contains_host_single", "t14_legacy_contains_zone_single":
			require.Len(t, t03ExpectedPoints(t, scenario.Cases[0]), 2, "equal scalar terms match by value")
			require.Empty(t, t03ExpectedPoints(t, scenario.Cases[1]), "one scalar cannot contain two distinct terms")
			require.Empty(t, t03ExpectedPoints(t, scenario.Cases[2]), "NULL requires no matching terms")
			found++
		case "t14_legacy_having_host_list", "t14_legacy_having_zone_list",
			"t14_legacy_contains_host_list", "t14_legacy_contains_zone_list":
			require.Len(t, t03ExpectedPoints(t, scenario.Cases[2]), 2, "duplicate terms do not change value membership")
			found++
		}
	}
	require.Equal(t, 8, found)
}
