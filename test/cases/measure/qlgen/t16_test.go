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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func TestT16GeneratedPublishedComputedGrouping(t *testing.T) {
	suite, err := qlgen.Generate()
	require.NoError(t, err)
	encoded, err := json.Marshal(suite)
	require.NoError(t, err)
	var generated struct {
		Scenarios []struct {
			Name  string       `json:"name"`
			Cases []qlgen.Case `json:"cases"`
		} `json:"grouping_scenarios"`
	}
	require.NoError(t, json.Unmarshal(encoded, &generated))
	require.NotEmpty(t, generated.Scenarios, "T16 must be generated from independent fixture rows")
	seen := map[string]bool{}
	for _, scenario := range generated.Scenarios {
		if scenario.Name != "t16_computed_group_F" && scenario.Name != "t16_ordinal_equivalence" {
			continue
		}
		require.Len(t, scenario.Cases, 3)
		require.JSONEq(t, `{"dataPoints":[
 {"fields":[{"name":"band","value":{"str":{"value":"low"}}},{"name":"total","value":{"int":{"value":"2"}}}]},
 {"fields":[{"name":"band","value":{"str":{"value":"high"}}},{"name":"total","value":{"int":{"value":"10"}}}]}]}`,
			string(scenario.Cases[0].WantJSON))
		seen[scenario.Name] = true
	}
	require.Len(t, seen, 2, "explicit and ordinal grouping retain the published F result")
}

func TestT16GeneratedDomainAndWork(t *testing.T) {
	suite, err := qlgen.Generate()
	require.NoError(t, err)
	required := map[string]bool{}
	for _, name := range []string{
		"arithmetic_explicit", "arithmetic_ordinal", "cast_explicit", "cast_ordinal", "multiple_computed", "multiple_ordinals",
		"repeated_keys", "structural_parentheses", "group_only_case", "null_key_cast", "constant_null", "param_case", "param_arithmetic",
		"where_having_distinct", "hidden_keys", "star_ordinals", "star_following_expression", "bulk_selected_multi_key",
		"bucket_sum_F", "bucket_ordinal", "bucket_default", "bucket_unaligned_range", "bucket_group_only", "bucket_computed", "bucket_native_merge",
		"bucket_raw_time",
	} {
		required["t16_"+name] = true
	}
	for _, scenario := range suite.GroupingScenarios {
		delete(required, scenario.Name)
		require.Len(t, scenario.Cases, 3)
		text := strings.Split(scenario.Cases[0].Query, "\n#!params:")[0]
		for _, c := range scenario.Cases {
			require.Equal(t, text, strings.Split(c.Query, "\n#!params:")[0], "bindings retain one statement")
			require.Equal(t, c.Grouping.InputRows*c.Grouping.KeyExpressions, c.Grouping.KeyEvaluations)
		}
		if scenario.Name == "t16_repeated_keys" {
			require.Equal(t, uint64(1), scenario.Cases[0].Grouping.KeyExpressions)
			require.Equal(t, uint64(6), scenario.Cases[0].Grouping.KeyEvaluations)
		}
		if scenario.Name == "t16_bulk_selected_multi_key" {
			require.Equal(t, uint64(1151), scenario.Cases[0].Grouping.InputRows)
			require.Equal(t, uint64(2302), scenario.Cases[0].Grouping.KeyEvaluations)
		}
		if scenario.Name == "t16_star_ordinals" {
			require.Contains(t, text, "SELECT *, COUNT(v::FIELD) AS n")
			require.Contains(t, text, "GROUP BY 1, 2")
		}
		if scenario.Name == "t16_bucket_native_merge" {
			require.Equal(t, uint64(5), scenario.Branches)
			require.Equal(t, uint64(3), scenario.Cases[0].AggregateGroups, "one entity occupies three distinct native buckets")
		}
	}
	require.Empty(t, required, "bounded T16 capability witnesses are generation obligations")
}

func TestT16GeneratedMixedTimeBucketIdentity(t *testing.T) {
	suite, err := qlgen.Generate()
	require.NoError(t, err)
	for _, scenario := range suite.GroupingScenarios {
		if scenario.Name != "t16_bucket_raw_time" {
			continue
		}
		require.Zero(t, scenario.Branches, "native bucketing cannot retain the unmodified source timestamp")
		require.Equal(t, uint64(6), scenario.Cases[0].AggregateGroups)
		require.Equal(t, uint64(6), scenario.Cases[0].Grouping.KeyEvaluations)
		require.Contains(t, scenario.Cases[0].Query, "GROUP BY time, TIME_BUCKET('2s')")
		require.JSONEq(t, `{"dataPoints":[
 {"timestamp":"{{time:0}}","tagFamilies":[{"name":"__ql","tags":[
  {"key":"original","value":{"timestamp":"{{time:0}}"}},{"key":"bucket","value":{"timestamp":"{{time:0}}"}}]}],
  "fields":[{"name":"total","value":{"int":{"value":"0"}}}]},
 {"timestamp":"{{time:0}}","tagFamilies":[{"name":"__ql","tags":[
  {"key":"original","value":{"timestamp":"{{time:1000}}"}},{"key":"bucket","value":{"timestamp":"{{time:0}}"}}]}],
  "fields":[{"name":"total","value":{"int":{"value":"2"}}}]},
 {"timestamp":"{{time:2000}}","tagFamilies":[{"name":"__ql","tags":[
  {"key":"original","value":{"timestamp":"{{time:2000}}"}},{"key":"bucket","value":{"timestamp":"{{time:2000}}"}}]}],
  "fields":[{"name":"total","value":{"int":{"value":"4"}}}]},
 {"timestamp":"{{time:2000}}","tagFamilies":[{"name":"__ql","tags":[
  {"key":"original","value":{"timestamp":"{{time:3000}}"}},{"key":"bucket","value":{"timestamp":"{{time:2000}}"}}]}],
  "fields":[{"name":"total","value":{"null":0}}]},
 {"timestamp":"{{time:4000}}","tagFamilies":[{"name":"__ql","tags":[
  {"key":"original","value":{"timestamp":"{{time:4000}}"}},{"key":"bucket","value":{"timestamp":"{{time:4000}}"}}]}],
  "fields":[{"name":"total","value":{"int":{"value":"6"}}}]},
 {"timestamp":"{{time:4000}}","tagFamilies":[{"name":"__ql","tags":[
  {"key":"original","value":{"timestamp":"{{time:5000}}"}},{"key":"bucket","value":{"timestamp":"{{time:4000}}"}}]}],
  "fields":[{"name":"total","value":{"null":0}}]}]}`, string(scenario.Cases[0].WantJSON))
		return
	}
	t.Fatal("generated mixed raw time and bucket witness missing")
}

func TestT16GeneratedStructuralKeyIdentity(t *testing.T) {
	suite, err := qlgen.Generate()
	require.NoError(t, err)
	witnesses := 0
	for _, scenario := range suite.GroupingScenarios {
		switch scenario.Name {
		case "t16_independent_parameter_keys":
			for _, c := range scenario.Cases {
				require.Equal(t, uint64(2), c.Grouping.KeyExpressions, "different placeholder positions remain independent even with equal bindings")
				require.Equal(t, uint64(12), c.Grouping.KeyEvaluations)
			}
			require.Equal(t, uint64(4), scenario.Cases[0].AggregateGroups)
			require.Equal(t, uint64(2), scenario.Cases[2].AggregateGroups)
			witnesses++
		case "t16_repeated_parameter_key", "t16_equivalent_key_spellings":
			for _, c := range scenario.Cases {
				require.Equal(t, uint64(1), c.Grouping.KeyExpressions, "ordinals and source spelling do not duplicate a structural key")
				require.Equal(t, uint64(6), c.Grouping.KeyEvaluations)
			}
			witnesses++
		}
	}
	require.Equal(t, 3, witnesses, "finite key identity requires independent parameter and equivalent spelling witnesses")
}

func TestT16GeneratedReviewBucketCompositions(t *testing.T) {
	suite, err := qlgen.Generate()
	require.NoError(t, err)
	positive := map[string]bool{}
	for _, name := range []string{
		"bucket_distinct_visible", "bucket_distinct_having", "bucket_hidden_distinct", "bucket_hidden_distinct_having",
		"bucket_default_match", "bucket_duration_match", "bucket_equivalent_keys", "bucket_alias_containers",
		"raw_time_bucket_having", "mixed_time_bucket_having", "hidden_raw_time_bucket_having",
	} {
		positive["t16_"+name] = true
	}
	for _, scenario := range suite.GroupingScenarios {
		delete(positive, scenario.Name)
		var response struct {
			DataPoints []map[string]any `json:"dataPoints"`
		}
		require.NoError(t, json.Unmarshal(scenario.Cases[0].WantJSON, &response))
		switch scenario.Name {
		case "t16_bucket_distinct_visible", "t16_bucket_distinct_having":
			var timestamps []string
			for _, point := range response.DataPoints {
				timestamps = append(timestamps, point["timestamp"].(string))
			}
			if scenario.Name == "t16_bucket_distinct_visible" {
				require.Equal(t, []string{"{{time:0}}", "{{time:2000}}", "{{time:4000}}"}, timestamps)
			} else {
				require.Equal(t, []string{"{{time:2000}}", "{{time:4000}}"}, timestamps)
			}
		case "t16_bucket_hidden_distinct", "t16_bucket_hidden_distinct_having":
			require.JSONEq(t, `{"dataPoints":[{"fields":[{"name":"n","value":{"int":{"value":"2"}}}]}]}`,
				string(scenario.Cases[0].WantJSON), "three hidden buckets have one equal visible COUNT tuple and no timestamp")
			require.Equal(t, uint64(1), scenario.Cases[0].DistinctGroups)
		case "t16_bucket_default_match", "t16_bucket_duration_match":
			require.Len(t, response.DataPoints, 2)
			require.Equal(t, uint64(1), scenario.Cases[0].Grouping.KeyExpressions)
			require.Equal(t, uint64(2), scenario.Cases[0].Having.TrueGroups)
		case "t16_bucket_equivalent_keys":
			require.Len(t, response.DataPoints, 3)
			require.Equal(t, uint64(1), scenario.Cases[0].Grouping.KeyExpressions, "default, 2s and 2000ms resolve to one key")
		case "t16_bucket_alias_containers":
			require.Len(t, response.DataPoints, 3)
			require.Contains(t, scenario.Cases[0].Query, "AS total, SUM(v::FIELD) AS total")
			require.Contains(t, scenario.Cases[0].Query, "HAVING (SUM(v::FIELD) > 0)")
		case "t16_raw_time_bucket_having":
			require.Len(t, response.DataPoints, 4)
			require.Equal(t, uint64(4), scenario.Cases[0].Having.TrueGroups)
			require.Zero(t, scenario.Branches)
		case "t16_mixed_time_bucket_having":
			require.Len(t, response.DataPoints, 3)
			require.Equal(t, "{{time:2000}}", response.DataPoints[0]["timestamp"])
			require.Contains(t, string(scenario.Cases[0].WantJSON), `"original","value":{"timestamp":"{{time:3000}}"}`)
			require.Equal(t, uint64(3), scenario.Cases[0].Having.TrueGroups)
			require.Zero(t, scenario.Branches)
		case "t16_hidden_raw_time_bucket_having":
			require.JSONEq(t, `{"dataPoints":[{"fields":[{"name":"total","value":{"null":0}}]},
 {"fields":[{"name":"total","value":{"int":{"value":"6"}}}]},{"fields":[{"name":"total","value":{"null":0}}]}]}`,
				string(scenario.Cases[0].WantJSON), "the grouped raw time remains hidden after HAVING")
			require.Equal(t, uint64(3), scenario.Cases[0].Having.TrueGroups)
			require.Zero(t, scenario.Branches)
		}
	}
	require.Empty(t, positive, "bucket composition witnesses must be generated")
	negative := map[string]bool{}
	for _, name := range []string{
		"having_bucket_mismatch", "having_bucket_mismatch_nested", "having_bucket_absent", "having_bucket_ungrouped",
		"where_bucket_grouped", "where_bucket_raw", "raw_bucket_projection", "bucket_duplicate_tag_alias", "bucket_duplicate_field_alias",
	} {
		negative["t16_reject_"+name] = true
	}
	for _, c := range suite.GroupingRejections {
		delete(negative, c.Name)
		require.True(t, c.NoScan)
		require.Equal(t, codes.InvalidArgument, c.WantCode)
	}
	require.Empty(t, negative, "invalid bucket scope and container collisions must reject before scanning")
}

func TestT16GeneratedBucketAndNullWitnesses(t *testing.T) {
	suite, err := qlgen.Generate()
	require.NoError(t, err)
	for _, scenario := range suite.GroupingScenarios {
		switch scenario.Name {
		case "t16_bucket_sum_F", "t16_bucket_ordinal", "t16_bucket_unaligned_range":
			require.JSONEq(t, `{"dataPoints":[
 {"timestamp":"{{time:0}}","tagFamilies":[{"name":"__ql","tags":[{"key":"bucket","value":{"timestamp":"{{time:0}}"}}]}],
  "fields":[{"name":"total","value":{"int":{"value":"2"}}}]},
 {"timestamp":"{{time:2000}}","tagFamilies":[{"name":"__ql","tags":[{"key":"bucket","value":{"timestamp":"{{time:2000}}"}}]}],
  "fields":[{"name":"total","value":{"int":{"value":"4"}}}]},
 {"timestamp":"{{time:4000}}","tagFamilies":[{"name":"__ql","tags":[{"key":"bucket","value":{"timestamp":"{{time:4000}}"}}]}],
  "fields":[{"name":"total","value":{"int":{"value":"6"}}}]}]}`, string(scenario.Cases[0].WantJSON))
		case "t16_constant_null":
			require.JSONEq(t, `{"dataPoints":[{"fields":[{"name":"key","value":{"null":0}},
 {"name":"total","value":{"int":{"value":"12"}}}]}]}`, string(scenario.Cases[0].WantJSON))
		case "t16_null_key_cast":
			require.JSONEq(t, `{"dataPoints":[
 {"fields":[{"name":"key","value":{"str":{"value":"x"}}},{"name":"total","value":{"int":{"value":"2"}}}]},
 {"fields":[{"name":"key","value":{"str":{"value":"y"}}},{"name":"total","value":{"int":{"value":"4"}}}]},
 {"fields":[{"name":"key","value":{"null":0}},{"name":"total","value":{"int":{"value":"6"}}}]},
 {"fields":[{"name":"key","value":{"str":{"value":""}}},{"name":"total","value":{"null":0}}]}]}`,
				string(scenario.Cases[0].WantJSON))
		}
	}
}

func TestT16GeneratedFixtureWritesPinRowsAndVersions(t *testing.T) {
	base := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	writes := qlgen.T16FixtureWrites(qlgen.T16FixtureName, base)
	require.Len(t, writes, 12)
	for ordinal, value := range []any{int64(0), int64(2), int64(4), nil, int64(6), nil} {
		require.Equal(t, base.Add(time.Duration(ordinal)*time.Second), writes[ordinal+6].DataPoint.Timestamp.AsTime())
		require.Equal(t, writes[ordinal].DataPoint.Timestamp, writes[ordinal+6].DataPoint.Timestamp)
		require.Equal(t, writes[ordinal].DataPoint.TagFamilies, writes[ordinal+6].DataPoint.TagFamilies)
		require.Equal(t, int64(-999), writes[ordinal].DataPoint.Fields[0].GetInt().Value)
		if value == nil {
			require.NotNil(t, writes[ordinal+6].DataPoint.Fields[0].GetValue())
			require.Nil(t, writes[ordinal+6].DataPoint.Fields[0].GetInt())
		} else {
			require.Equal(t, value, writes[ordinal+6].DataPoint.Fields[0].GetInt().Value)
		}
	}
	star := qlgen.T16FixtureSchema(qlgen.T16StarFixtureName)
	require.Len(t, star.TagFamilies[0].Tags, 1)
	require.Equal(t, "host", star.TagFamilies[0].Tags[0].Name)
	require.Len(t, star.Fields, 1)
	require.Equal(t, "v", star.Fields[0].Name)
	require.Len(t, qlgen.T16FixtureWrites(qlgen.T16BulkFixtureName, base), 2306)
}

func TestT16LoadRejectsLostGroupingEvidence(t *testing.T) {
	for _, mode := range []string{"scenario", "bindings", "statement", "work", "input", "readiness", "rejections", "native_branches"} {
		t.Run(mode, func(t *testing.T) {
			suite, err := qlgen.Generate()
			require.NoError(t, err)
			switch mode {
			case "scenario":
				suite.GroupingScenarios = suite.GroupingScenarios[1:]
			case "bindings":
				suite.GroupingScenarios[0].Cases = suite.GroupingScenarios[0].Cases[:2]
			case "statement":
				suite.GroupingScenarios[0].Cases[1].Query = strings.Replace(suite.GroupingScenarios[0].Cases[1].Query, "SELECT", "SELECT changed,", 1)
			case "work":
				suite.GroupingScenarios[0].Cases[0].Grouping.KeyEvaluations++
			case "input":
				suite.GroupingScenarios[0].Cases[0].Grouping.InputRows++
			case "readiness":
				suite.GroupingReadiness[0].Cases = nil
			case "rejections":
				suite.GroupingRejections[0].WantCode = codes.OK
			case "native_branches":
				suite.GroupingScenarios[len(suite.GroupingScenarios)-1].Branches--
			}
			encoded, err := json.Marshal(suite)
			require.NoError(t, err)
			_, err = qlgen.Load(encoded)
			require.Error(t, err)
		})
	}
}
