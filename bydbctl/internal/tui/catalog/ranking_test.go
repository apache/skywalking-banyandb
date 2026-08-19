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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	"fmt"
	"slices"
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

func TestTokensSplitsOnEveryNonAlphanumericByte(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  []string
	}{
		{name: "underscores", value: "service_endpoint_latency", want: []string{"service", "endpoint", "latency"}},
		{name: "mixed case", value: "ServiceCPU_99", want: []string{"servicecpu", "99"}},
		{name: "punctuation and spacing", value: "  slowest.endpoint, please  ", want: []string{"slowest", "endpoint", "please"}},
		{name: "duplicates collapse", value: "cpu cpu_cpu", want: []string{"cpu"}},
		{name: "CJK separates runs", value: "延迟latency指标", want: []string{"latency"}},
		{name: "empty", value: "", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := tokens(test.value); !slices.Equal(got, test.want) {
				t.Fatalf("tokens(%q) = %#v, want %#v", test.value, got, test.want)
			}
		})
	}
}

func TestRankPrefersTheGoalResource(t *testing.T) {
	entries := []session.CatalogEntry{
		{Group: "log", Type: session.ResourceTypeStream, Name: "access_log"},
		{Group: "metrics", Type: session.ResourceTypeMeasure, Name: "service_cpm"},
		{Group: "metrics", Type: session.ResourceTypeMeasure, Name: "service_endpoint_latency"},
	}
	ranked := Rank("show the slowest endpoint latency", entries, 3)
	if len(ranked) == 0 {
		t.Fatal("Rank returned no candidates")
	}
	if ranked[0].Name != "service_endpoint_latency" {
		t.Fatalf("Rank ranked %q first, want service_endpoint_latency", ranked[0].Name)
	}
}

func TestRankScoresTypeKeywordsPerResourceType(t *testing.T) {
	entries := []session.CatalogEntry{
		{Group: "records", Type: session.ResourceTypeMeasure, Name: "sw_records"},
		{Group: "records", Type: session.ResourceTypeTrace, Name: "sw_records"},
	}
	ranked := Rank("find the slowest trace span", entries, 2)
	if len(ranked) == 0 {
		t.Fatal("Rank returned no candidates")
	}
	if ranked[0].Type != session.ResourceTypeTrace {
		t.Fatalf("Rank ranked type %q first, want TRACE", ranked[0].Type)
	}
}

func TestRankSkipsInternalGroupsAndHonorsLimit(t *testing.T) {
	entries := []session.CatalogEntry{
		{Group: "_internal", Type: session.ResourceTypeMeasure, Name: "hidden_latency"},
		{Group: "metrics", Type: session.ResourceTypeMeasure, Name: "endpoint_latency"},
		{Group: "metrics", Type: session.ResourceTypeMeasure, Name: "service_latency"},
	}
	ranked := Rank("latency", entries, 1)
	if len(ranked) != 1 {
		t.Fatalf("Rank returned %d candidates, want 1", len(ranked))
	}
	if ranked[0].Group == "_internal" {
		t.Fatal("Rank returned an entry from an internal group")
	}
}

func TestRankIsDeterministicForTiedScores(t *testing.T) {
	entries := make([]session.CatalogEntry, 0, 6)
	for idx := 5; idx >= 0; idx-- {
		entries = append(entries, session.CatalogEntry{
			Group: "metrics",
			Type:  session.ResourceTypeMeasure,
			Name:  fmt.Sprintf("unrelated_%d", idx),
		})
	}
	first := Rank("no matching keywords here", entries, 6)
	second := Rank("no matching keywords here", entries, 6)
	if !slices.Equal(first, second) {
		t.Fatalf("Rank is not deterministic: %#v vs %#v", first, second)
	}
	for idx := 1; idx < len(first); idx++ {
		if first[idx-1].Name > first[idx].Name {
			t.Fatalf("tied candidates are not name-ordered: %#v", first)
		}
	}
}

func TestMatchGoalRejectsAmbiguousCandidates(t *testing.T) {
	catalog := session.SchemaCatalog{Entries: []session.CatalogEntry{
		{Group: "metrics", Type: session.ResourceTypeMeasure, Name: "endpoint_latency"},
		{Group: "metrics", Type: session.ResourceTypeMeasure, Name: "endpoint_latency_hour"},
	}}
	match := MatchGoal("endpoint latency", catalog, "", "", nil)
	if match.Matched {
		t.Fatalf("MatchGoal matched %q despite a near tie", match.Name)
	}
	if !match.Ambiguous {
		t.Fatal("MatchGoal did not report the near tie as ambiguous")
	}
}

func TestMatchGoalIgnoresLowConfidenceGoals(t *testing.T) {
	catalog := session.SchemaCatalog{Entries: []session.CatalogEntry{
		{Group: "metrics", Type: session.ResourceTypeMeasure, Name: "service_cpm"},
	}}
	if match := MatchGoal("hello", catalog, "", "", nil); match.Matched {
		t.Fatalf("MatchGoal matched %q for an unrelated goal", match.Name)
	}
}
