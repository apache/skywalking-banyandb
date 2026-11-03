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

package qlgen_test

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func TestT17GeneratedPublishedSortAndPage(t *testing.T) {
	suite, err := qlgen.Generate()
	require.NoError(t, err)
	encoded, err := json.Marshal(suite)
	require.NoError(t, err)
	var generated struct {
		Scenarios []struct {
			Name  string       `json:"name"`
			Cases []qlgen.Case `json:"cases"`
		} `json:"sorting_scenarios"`
	}
	require.NoError(t, json.Unmarshal(encoded, &generated))
	require.NotEmpty(t, generated.Scenarios, "T17 must generate independently ordered responses")
	found := map[string]bool{}
	for _, scenario := range generated.Scenarios {
		want := []string(nil)
		switch scenario.Name {
		case "t17_sort_F":
			want = []string{"5", "3", "2", "1", "4", "6"}
		case "t17_topk_F":
			want = []string{"3", "2"}
		default:
			continue
		}
		require.Len(t, scenario.Cases, 3)
		require.True(t, scenario.Cases[0].Ordered)
		var response struct {
			DataPoints []struct {
				TagFamilies []struct {
					Tags []struct {
						Key   string `json:"key"`
						Value struct {
							Int struct {
								Value string `json:"value"`
							} `json:"int"`
						} `json:"value"`
					} `json:"tags"`
				} `json:"tagFamilies"`
			} `json:"dataPoints"`
		}
		require.NoError(t, json.Unmarshal(scenario.Cases[0].WantJSON, &response))
		var got []string
		for _, point := range response.DataPoints {
			for _, family := range point.TagFamilies {
				for _, tag := range family.Tags {
					if tag.Key == "rid" {
						got = append(got, tag.Value.Int.Value)
					}
				}
			}
		}
		require.Equal(t, want, got, scenario.Name)
		found[scenario.Name] = true
	}
	require.Len(t, found, 2)
}

func TestT17GeneratedParentComposition(t *testing.T) {
	suite, err := qlgen.Generate()
	require.NoError(t, err)
	for _, scenario := range suite.SortingScenarios {
		if scenario.Name != "t17_parent_filter_having_page" {
			continue
		}
		require.JSONEq(t, `{"dataPoints":[{
            "tagFamilies":[{"name":"default","tags":[{"key":"node","value":{"str":{"value":"b"}}}]}],
            "fields":[{"name":"total","value":{"int":{"value":"4"}}}]}]}`, string(scenario.Cases[0].WantJSON))
		require.JSONEq(t, `{}`, string(scenario.Cases[1].WantJSON))
		require.Contains(t, scenario.Cases[0].Query, "WHERE time >= ? AND time < ? AND (v::FIELD > CAST(? AS DOUBLE))")
		require.Contains(t, scenario.Cases[0].Query, "GROUP BY host HAVING (SUM(v::FIELD) > 3) ORDER BY total DESC, node ASC LIMIT ? OFFSET ?")
		return
	}
	t.Fatal("parent WHERE/GROUP BY/HAVING/ORDER BY/LIMIT/OFFSET composition missing")
}

func TestT17GeneratedFiniteOrderingDomain(t *testing.T) {
	suite, err := qlgen.Generate()
	require.NoError(t, err)
	required := map[string]bool{}
	for _, name := range []string{
		"ascending_default", "descending_default", "ascending_nulls_first", "single_key", "ties",
		"alias_precedence", "hidden_sort", "expression_params", "qualified_refs", "bare_refs",
		"group_sort", "hidden_group_sort", "case_avg_ordinal", "distinct_multikey_topk", "bucket_sort", "hidden_bucket_sort",
		"distinct_bucket_sort", "empty", "limit_zero", "wide_exact_integer", "wide_string", "wide_float", "wide_payload_topk", "large_offset",
		"bulk_topk_complete",
	} {
		required["t17_"+name] = true
	}
	for _, scenario := range suite.SortingScenarios {
		delete(required, scenario.Name)
		if scenario.Name == "t17_wide_float" {
			require.Contains(t, string(scenario.Cases[0].WantJSON), `"NaN"`)
		}
	}
	require.Empty(t, required, "ordering capability families are independent generation obligations")
}

func TestT17GeneratedResources(t *testing.T) {
	suite, err := qlgen.Generate()
	require.NoError(t, err)
	encoded, err := json.Marshal(suite)
	require.NoError(t, err)
	var generated struct {
		Resources []struct {
			Control *qlgen.Case `json:"control"`
			Case    qlgen.Case  `json:"case"`
			Cancel  bool        `json:"cancel"`
		} `json:"sorting_resources"`
	}
	require.NoError(t, json.Unmarshal(encoded, &generated))
	require.Len(t, generated.Resources, 4, "full sort and bounded top-k each need budget and cancellation evidence")
	for _, resource := range generated.Resources {
		if !resource.Cancel {
			require.NotNil(t, resource.Control, "budget pressure must retain a successful compact control at the same budget")
		}
	}
}

func TestT17GeneratedExactOrderingAndTransport(t *testing.T) {
	suite, err := qlgen.Generate()
	require.NoError(t, err)
	for _, scenario := range suite.SortingScenarios {
		var want []int64
		switch scenario.Name {
		case "t17_wide_exact_integer":
			want = []int64{8, 5, 6, 2, 1, 3, 7, 4}
		case "t17_wide_string":
			want = []int64{3, 7, 2, 8, 1, 6, 5, 4}
		case "t17_wide_float":
			want = []int64{5, 7, 8, 1, 6, 2, 4, 3}
		case "t17_ascending_default":
			want = []int64{1, 2, 3, 5, 4, 6}
		case "t17_descending_default":
			want = []int64{4, 6, 5, 3, 2, 1}
		case "t17_ascending_nulls_first":
			want = []int64{6, 4, 1, 2, 3, 5}
		default:
			continue
		}
		rendered, renderErr := qlgen.Render(string(scenario.Cases[0].WantJSON), time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC))
		require.NoError(t, renderErr)
		var response measurev1.QueryResponse
		require.NoError(t, protojson.Unmarshal([]byte(rendered), &response), "NaN/Inf expectations must be actual valid protobuf transport JSON")
		var got []int64
		for _, point := range response.DataPoints {
			for _, family := range point.TagFamilies {
				for _, tag := range family.Tags {
					if tag.Key == "rid" {
						got = append(got, tag.Value.GetInt().Value)
					}
				}
			}
		}
		require.Equal(t, want, got, scenario.Name)
	}
	writes := qlgen.T17FixtureWrites(qlgen.T17WideFixtureName, time.Now().Truncate(time.Millisecond))
	require.Len(t, writes, 8)
	require.Equal(t, int64(9007199254740993), writes[0].DataPoint.Fields[0].GetInt().Value)
	require.True(t, math.IsNaN(writes[1].DataPoint.Fields[1].GetFloat().Value))
	require.True(t, math.IsNaN(writes[3].DataPoint.Fields[1].GetFloat().Value))
	require.Greater(t, len(writes[0].DataPoint.TagFamilies[0].Tags[1].GetStr().Value), 16000)
}

func TestT17LoadRejectsMissingOrderingEvidence(t *testing.T) {
	for _, mode := range []string{"scenario", "bindings", "statement", "work", "input", "readiness", "rejection", "resource"} {
		t.Run(mode, func(t *testing.T) {
			suite, err := qlgen.Generate()
			require.NoError(t, err)
			switch mode {
			case "scenario":
				suite.SortingScenarios = suite.SortingScenarios[1:]
			case "bindings":
				suite.SortingScenarios[0].Cases = suite.SortingScenarios[0].Cases[:2]
			case "statement":
				suite.SortingScenarios[0].Cases[1].Query = strings.Replace(suite.SortingScenarios[0].Cases[1].Query, "SELECT", "SELECT changed,", 1)
			case "work":
				suite.SortingScenarios[0].Cases[0].Sorting.KeyEvaluations++
			case "input":
				suite.SortingScenarios[0].Cases[0].Sorting.InputRows++
			case "readiness":
				suite.SortingReadiness[0].Cases = nil
			case "rejection":
				suite.SortingRejections[0].WantCode = codes.OK
			case "resource":
				suite.SortingResources[0].Case.Sorting.InputRows--
			}
			encoded, err := json.Marshal(suite)
			require.NoError(t, err)
			_, err = qlgen.Load(encoded)
			require.Error(t, err)
		})
	}
}

func TestT17GeneratedStatementsPrepareCompileAndBind(t *testing.T) {
	suite, err := qlgen.Generate()
	require.NoError(t, err)
	schemas := map[bydbql.MeasureSource]*databasev1.Measure{}
	var cases []qlgen.Case
	for _, fixture := range suite.SortingReadiness {
		schemas[bydbql.MeasureSource{Group: qlgen.FixtureGroup, Name: fixture.Name}] = qlgen.T17FixtureSchema(fixture.Name)
		cases = append(cases, fixture.Cases...)
	}
	for _, scenario := range suite.SortingScenarios {
		cases = append(cases, scenario.Cases...)
	}
	for _, resource := range suite.SortingResources {
		cases = append(cases, resource.Case)
		if resource.Control != nil {
			cases = append(cases, *resource.Control)
		}
	}
	base := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	transformer := bydbql.NewTransformerWithSource(nil, nil)
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			rendered, renderErr := qlgen.Render(c.Query, base)
			require.NoError(t, renderErr)
			text, params, extractErr := helpers.ExtractQL(rendered)
			require.NoError(t, extractErr)
			statement, prepareErr := bydbql.Prepare(text)
			require.NoError(t, prepareErr)
			template, compileErr := transformer.CompileMeasureWithSchemas(t.Context(), statement, schemas)
			require.NoError(t, compileErr)
			require.NotNil(t, template)
			bound, bindErr := statement.Bind(params)
			require.NoError(t, bindErr)
			_, bindErr = template.Bind(bound, base)
			require.NoError(t, bindErr)
		})
	}
}

func TestT17GeneratedHiddenWhereAcrossStages(t *testing.T) {
	suite, err := qlgen.Generate()
	require.NoError(t, err)
	found := map[string]bool{}
	for _, scenario := range suite.SortingScenarios {
		if !strings.HasPrefix(scenario.Name, "t17_hidden_where_") {
			continue
		}
		require.Len(t, scenario.Cases, 3)
		require.Contains(t, scenario.Cases[0].Query, "(f::FIELD > CAST(? AS DOUBLE))")
		require.JSONEq(t, `{}`, string(scenario.Cases[2].WantJSON))
		require.Zero(t, scenario.Cases[0].Sorting.KeyKernelRows, "visible aliases read completed projections")
		switch scenario.Name {
		case "t17_hidden_where_raw", "t17_hidden_where_computed":
			rendered, renderErr := qlgen.Render(string(scenario.Cases[0].WantJSON), time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC))
			require.NoError(t, renderErr)
			var response measurev1.QueryResponse
			require.NoError(t, protojson.Unmarshal([]byte(rendered), &response))
			var rids []int64
			for _, point := range response.DataPoints {
				for _, family := range point.TagFamilies {
					for _, tag := range family.Tags {
						if tag.Key == "rid" {
							rids = append(rids, tag.Value.GetInt().Value)
						}
					}
				}
			}
			want := []int64{3, 5, 6}
			if scenario.Name == "t17_hidden_where_computed" {
				want = []int64{6, 5, 3}
				require.Contains(t, scenario.Cases[0].Query, "CAST(time AS TIMESTAMP) AS observed")
				require.Contains(t, scenario.Cases[0].Query, "ORDER BY observed::TAG DESC NULLS LAST, rid ASC")
			}
			require.Equal(t, want, rids)
		case "t17_hidden_where_grouped":
			require.JSONEq(t, `{"dataPoints":[{"tagFamilies":[{"name":"default","tags":[{"key":"node","value":{"str":{"value":"c"}}}]}],
                "fields":[{"name":"total","value":{"int":{"value":"6"}}}]}]}`, string(scenario.Cases[0].WantJSON))
			require.Contains(t, string(scenario.Cases[1].WantJSON), `"null":0`)
		case "t17_hidden_where_distinct":
			require.JSONEq(t, `{"dataPoints":[{"tagFamilies":[{"name":"default","tags":[{"key":"node","value":{"str":{"value":"b"}}}]}]}]}`,
				string(scenario.Cases[0].WantJSON))
			require.Contains(t, string(scenario.Cases[1].WantJSON), `"value":"c"`)
		}
		found[scenario.Name] = true
	}
	require.Len(t, found, 4, "sort preserves hidden WHERE-only inputs through each completion stage")
}
