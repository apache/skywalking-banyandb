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
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

func TestT12GeneratedCallsCompileAndRebindSameText(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	base := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	for _, scenario := range suite.FilterScenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			var template *bydbql.MeasureTemplate
			var first string
			for _, c := range scenario.Cases {
				rendered, renderErr := Render(c.Query, base)
				require.NoError(t, renderErr)
				query, params, extractErr := helpers.ExtractQL(rendered)
				require.NoError(t, extractErr)
				if first == "" {
					first = query
					statement, prepareErr := bydbql.Prepare(query)
					require.NoError(t, prepareErr)
					var compileErr error
					template, compileErr = bydbql.NewTransformer(nil).CompileMeasureWithSchemas(context.Background(), statement,
						map[bydbql.MeasureSource]*databasev1.Measure{{Group: FixtureGroup, Name: T12FixtureName}: T12FixtureSchema()})
					require.NoError(t, compileErr)
				}
				require.Equal(t, first, query)
				_, bindErr := template.BindParams(params, base)
				require.NoError(t, bindErr, "value errors belong to active execution rows, not cached binding")
			}
		})
	}
}

func TestT12OracleMatchesPublishedFilterExamples(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	found := make(map[string]bool)
	for _, scenario := range suite.FilterScenarios {
		if len(scenario.Cases[0].WantJSON) == 0 {
			continue
		}
		points := t03ExpectedPoints(t, scenario.Cases[0])
		switch scenario.Name {
		case "t12_field_filter_F", "t12_float_filter", "t12_tail_hidden_limit_offset":
			want := []string{"2", "3", "5"}
			if scenario.Name == "t12_float_filter" {
				want = []string{"2", "3", "5", "6"}
			}
			if scenario.Name == "t12_tail_hidden_limit_offset" {
				want = []string{"1202", "1203", "1204"}
			}
			require.Len(t, points, len(want))
			for ordinal, point := range points {
				tags := point["tagFamilies"].([]any)[0].(map[string]any)["tags"].([]any)
				require.Equal(t, want[ordinal], tags[0].(map[string]any)["value"].(map[string]any)["int"].(map[string]any)["value"])
				require.NotContains(t, point, "fields", "predicate-only fields stay hidden")
			}
			found[scenario.Name] = true
		case "t12_filter_before_aggregate", "t12_empty_aggregate", "t12_all_null_aggregate":
			require.Len(t, points, 1)
			fields := points[0]["fields"].([]any)
			if scenario.Name == "t12_filter_before_aggregate" {
				require.JSONEq(t, `{"int":{"value":"12"}}`, mustJSONT12(t, fields[0].(map[string]any)["value"]))
				require.JSONEq(t, `{"int":{"value":"3"}}`, mustJSONT12(t, fields[1].(map[string]any)["value"]))
			} else {
				require.JSONEq(t, `{"null":0}`, mustJSONT12(t, fields[0].(map[string]any)["value"]))
				require.JSONEq(t, `{"int":{"value":"0"}}`, mustJSONT12(t, fields[1].(map[string]any)["value"]))
			}
			require.Equal(t, scenario.Cases[0].Filter.TrueRows, scenario.Cases[0].AggregateInputs)
			found[scenario.Name] = true
		case "t12_tail_omitted_limit":
			require.Len(t, points, 256)
			require.Equal(t, uint64(256), scenario.Cases[0].Filter.InputRows)
			found[scenario.Name] = true
		}
	}
	require.Len(t, found, 7)
}

func TestT12ConstantPredicateTruthCounts(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	found := make(map[string]bool)
	for _, scenario := range suite.FilterScenarios {
		switch scenario.Name {
		case "t12_constant_null", "t12_constant_typed_null", "t12_constant_true", "t12_constant_false":
			c := scenario.Cases[0]
			require.Equal(t, uint64(6), c.Filter.InputRows)
			points := t03ExpectedPoints(t, c)
			switch scenario.Name {
			case "t12_constant_true":
				require.Len(t, points, 6)
				require.Equal(t, uint64(6), c.Filter.TrueRows)
			case "t12_constant_false":
				require.Empty(t, points)
				require.Equal(t, uint64(6), c.Filter.FalseRows)
			default:
				require.Empty(t, points)
				require.Equal(t, uint64(6), c.Filter.UnknownRows)
			}
			found[scenario.Name] = true
		}
	}
	require.Len(t, found, 4)
}

func mustJSONT12(t *testing.T, value any) string {
	t.Helper()
	content, err := json.Marshal(value)
	require.NoError(t, err)
	return string(content)
}

func TestT12IndependentOracleHonorsMasksMembershipAndNaN(t *testing.T) {
	invalid := t10Binary("=", t10Binary("/", t10Literal(int64(1)), t10Literal(int64(0))), t10Literal(int64(0)))
	for _, definition := range []struct {
		op         string
		left, want bool
	}{{"AND", false, false}, {"OR", true, true}} {
		value, err := evalT10(t10Binary(definition.op, t10Literal(definition.left), invalid), t10Rows()[0], nil)
		require.NoError(t, err)
		require.Equal(t, definition.want, value)
	}
	require.Equal(t, false, inT10([]any{nil, []int64{}}))
	require.Equal(t, true, inT10([]any{int64(2), []int64{1, 2}}))
	require.Equal(t, 1, compareT10(math.NaN(), math.Inf(1)))
	require.Equal(t, 0, compareT10(math.NaN(), math.NaN()))
	require.Equal(t, -1, compareT10(math.Inf(1), math.NaN()))
	require.Equal(t, -1, compareT10(t10Timestamp{Millis: 0}, t10Timestamp{Millis: 1000}))
}

func TestT12ContractRejectsMissingCoverageAndTruthRows(t *testing.T) {
	for _, mode := range []string{"readiness", "scenarios", "bindings", "text", "truth", "aggregate", "feature"} {
		t.Run(mode, func(t *testing.T) {
			suite, err := Generate()
			require.NoError(t, err)
			switch mode {
			case "readiness":
				suite.FilterReadiness = nil
			case "scenarios":
				suite.FilterScenarios = nil
			case "bindings":
				suite.FilterScenarios[0].Cases = suite.FilterScenarios[0].Cases[:2]
			case "text":
				suite.FilterScenarios[0].Cases[1].Query = strings.Replace(suite.FilterScenarios[0].Cases[1].Query, "SELECT", "SELECT changed,", 1)
			case "truth":
				suite.FilterScenarios[0].Cases[0].Filter.TrueRows++
			case "aggregate":
				for ordinal := range suite.FilterScenarios {
					if suite.FilterScenarios[ordinal].Cases[0].Filter.Aggregate {
						suite.FilterScenarios[ordinal].Cases[0].AggregateInputs++
						break
					}
				}
			case "feature":
				for ordinal, feature := range suite.Features {
					if feature.ID == "t12.masked_boolean" {
						suite.Features = append(suite.Features[:ordinal], suite.Features[ordinal+1:]...)
						break
					}
				}
			}
			require.Error(t, suite.Validate())
		})
	}
}
