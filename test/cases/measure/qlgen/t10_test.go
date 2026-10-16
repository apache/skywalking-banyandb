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

func TestT10OracleMatchesPublishedFExamples(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	for _, definition := range []struct {
		name string
		want []any
	}{
		{"t10_arithmetic_F", []any{int64(2), int64(4), int64(6), nil, int64(8), nil}},
		{"t10_masked_case", []any{int64(0), int64(4), int64(2), nil, int64(1), nil}},
		{"t10_coalesce_F", []any{int64(0), int64(2), int64(4), int64(-1), int64(6), int64(-1)}},
	} {
		t.Run(definition.name, func(t *testing.T) {
			for _, scenario := range suite.ExpressionScenarios {
				if scenario.Name != definition.name {
					continue
				}
				points := t03ExpectedPoints(t, scenario.Cases[0])
				require.Len(t, points, len(definition.want))
				for ordinal, want := range definition.want {
					fields := points[ordinal]["fields"].([]any)
					wanted, wantErr := json.Marshal(jsonValue(want))
					require.NoError(t, wantErr)
					actual, actualErr := json.Marshal(fields[0].(map[string]any)["value"])
					require.NoError(t, actualErr)
					require.JSONEq(t, string(wanted), string(actual))
				}
				return
			}
			t.Fatal("missing generated F example")
		})
	}
}

func TestT10IndependentOracleMasksAndChecksNumericBoundaries(t *testing.T) {
	row := t10Rows()[0]
	invalid := t10Binary("/", t10Literal(int64(1)), t10Literal(int64(0)))
	for _, expression := range []*t10Expr{
		t10Call("CASE", t10Literal(true), t10Literal(int64(7)), invalid),
		t10Call("COALESCE", t10Literal(int64(7)), invalid),
	} {
		value, err := evalT10(expression, row, nil)
		require.NoError(t, err)
		require.Equal(t, int64(7), value)
	}
	value, err := arithmeticT10("/", int64(-5), int64(2))
	require.NoError(t, err)
	require.Equal(t, int64(-2), value)
	_, err = arithmeticT10("+", int64(math.MaxInt64), int64(1))
	require.ErrorIs(t, err, errT10Value)
	_, err = arithmeticT10("/", int64(math.MinInt64), int64(-1))
	require.ErrorIs(t, err, errT10Value)
	for _, number := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), 9223372036854775808.0} {
		_, err = castT10(number, "BIGINT")
		require.ErrorIs(t, err, errT10Value)
	}
	value, err = castT10(-5.75, "BIGINT")
	require.NoError(t, err)
	require.Equal(t, int64(-5), value)
	require.Equal(t, false, inT10([]any{nil}))
	require.Nil(t, inT10([]any{int64(2), int64(1), nil}))
	require.Equal(t, true, inT10([]any{int64(2), int64(2), nil}))
	require.Equal(t, false, logicalT10("AND", nil, false))
	require.Equal(t, true, logicalT10("OR", nil, true))
	require.Nil(t, logicalT10("AND", nil, true))
}

func TestT10GeneratedCallsCompileAndRebindTheSameText(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	base := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	for _, scenario := range suite.ExpressionScenarios {
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
						map[bydbql.MeasureSource]*databasev1.Measure{{Group: FixtureGroup, Name: T10FixtureName}: T10FixtureSchema()})
					require.NoError(t, compileErr)
				}
				require.Equal(t, first, query)
				_, bindErr := template.BindParams(params, base)
				// Value conversion is execution work: an invalid bound string still
				// binds successfully and can be skipped by a conditional mask.
				require.NoError(t, bindErr)
			}
		})
	}
}

func TestT10ContractRejectsMissingWitnessesAndChangedText(t *testing.T) {
	for _, mode := range []string{"readiness", "scenarios", "bindings", "text", "feature"} {
		t.Run(mode, func(t *testing.T) {
			suite, err := Generate()
			require.NoError(t, err)
			switch mode {
			case "readiness":
				suite.ExpressionReadiness = nil
			case "scenarios":
				suite.ExpressionScenarios = nil
			case "bindings":
				suite.ExpressionScenarios[0].Cases = suite.ExpressionScenarios[0].Cases[:2]
			case "text":
				suite.ExpressionScenarios[0].Cases[1].Query = strings.Replace(suite.ExpressionScenarios[0].Cases[1].Query, "SELECT", "SELECT changed,", 1)
			case "feature":
				for ordinal := range suite.Features {
					if suite.Features[ordinal].ID == "t10.unreachable_invalid" {
						suite.Features = append(suite.Features[:ordinal], suite.Features[ordinal+1:]...)
						break
					}
				}
			}
			require.Error(t, suite.Validate())
		})
	}
}
