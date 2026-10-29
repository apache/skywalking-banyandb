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
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

func TestPatternOracle(t *testing.T) {
	for _, tc := range []struct {
		name, value, pattern, escape string
		fold, want                   bool
	}{
		{"whole value", "Amber-Bird", "Amber", "\\", false, false},
		{"case sensitive", "Amber-Bird", "%amber%", "\\", false, false},
		{"case fold", "Amber-Bird", "%amber%", "\\", true, true},
		{"cross separator", "Amber-Bird", "%er-B%", "\\", false, true},
		{"literal underscore", "a_b", "a!_b", "!", false, true},
		{"escaped rejects wildcard", "a-b", "a!_b", "!", false, false},
		{"unicode scalar", "a中b", "a_b", "\\", false, true},
		{"newline scalar", "a\nb", "a_b", "\\", false, true},
		{"empty pattern", "", "", "\\", false, true},
		{"empty percent", "", "%", "\\", false, true},
		{"empty underscore", "", "_", "\\", false, false},
		{"literal star", "a*b", "a*b", "\\", false, true},
		{"literal question", "a?b", "a?b", "\\", false, true},
		{"disabled escape", `a\b`, `a\b`, "", false, true},
		{"simple fold cycle", "ς", "Σ", "\\", true, true},
		{"no multi rune fold", "ss", "ß", "\\", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := patternMatch(tc.value, tc.pattern, tc.escape, tc.fold)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	_, err := patternMatch("x", `x\`, "\\", false)
	require.ErrorContains(t, err, "trailing escape")
	for _, tc := range []struct {
		pattern        any
		fold, residual bool
		want           int64
	}{
		{"%amber%", false, false, 1},
		{"%amber%", true, false, 3},
		{"a_b", false, false, 7},
		{"", false, false, 1},
		{nil, true, false, 0},
		{"%", true, false, 14},
		{"%", true, true, 10},
		{"Σ", true, false, 2},
	} {
		got, countErr := patternCount(tc.pattern, "\\", tc.fold, tc.residual)
		require.NoError(t, countErr)
		require.Equal(t, tc.want, got)
	}
}

func TestPatternScenarioCoverage(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	require.Len(t, suite.PatternScenarios, 34)
	require.Len(t, suite.PatternReadiness, 7)
	for _, scenario := range suite.PatternScenarios {
		if strings.HasPrefix(scenario.Name, "patterns_reject_ambiguous_") {
			require.Len(t, scenario.Cases, 3)
			for _, c := range scenario.Cases {
				require.Equal(t, codes.InvalidArgument, c.WantCode)
				require.Nil(t, c.WantJSON)
			}
		}
	}
	require.NoError(t, validatePatterns(suite))
	suite.PatternScenarios = suite.PatternScenarios[1:]
	require.ErrorContains(t, validatePatterns(suite), "coverage missing")
}

func TestPatternPostgresReference(t *testing.T) {
	data, err := os.ReadFile("testdata/pattern-postgres.json")
	require.NoError(t, err)
	var cases []struct {
		Value, Pattern *string
		Like, ILike    *bool
		Name, Escape   string
	}
	require.NoError(t, json.Unmarshal(data, &cases))
	require.Len(t, cases, 20)
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Value == nil || tc.Pattern == nil {
				require.Nil(t, tc.Like)
				require.Nil(t, tc.ILike)
				return
			}
			for _, fold := range []bool{false, true} {
				expected := tc.Like
				if fold {
					expected = tc.ILike
				}
				require.NotNil(t, expected)
				actual, matchErr := patternMatch(*tc.Value, *tc.Pattern, tc.Escape, fold)
				require.NoError(t, matchErr)
				require.Equal(t, *expected, actual)
			}
		})
	}
}

func TestPatternAmbiguousIndexFixtures(t *testing.T) {
	rules := PatternIndexRules(PatternFixtureName)
	for _, name := range []string{"keyword_high_first", "keyword_low_first", "standard_first", "keyword_before_standard", "multitag_first", "keyword_before_multitag"} {
		var covering []*databasev1.IndexRule
		for _, rule := range rules {
			if slices.Contains(rule.Tags, name) {
				covering = append(covering, rule)
			}
		}
		require.Len(t, covering, 2, name)
		require.NotEqual(t, covering[0].Metadata.Id, covering[1].Metadata.Id)
		if name == "keyword_high_first" {
			require.Greater(t, covering[0].Metadata.Id, covering[1].Metadata.Id)
		}
		if name == "keyword_low_first" {
			require.Less(t, covering[0].Metadata.Id, covering[1].Metadata.Id)
		}
	}
}
