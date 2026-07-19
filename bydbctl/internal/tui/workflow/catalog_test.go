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
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package workflow

import (
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

func TestCatalogRankingGoalPrefersTurnHint(t *testing.T) {
	got := CatalogRankingGoal("top 10 slow payment endpoints", "帮我查询cpu指标")
	if !strings.Contains(got, "cpu") || !strings.Contains(got, "payment") {
		t.Fatalf("unexpected ranking goal: %q", got)
	}
	if !strings.HasPrefix(got, "帮我查询cpu指标") {
		t.Fatalf("expected turn hint first, got %q", got)
	}
}

func TestRankCatalogCandidatesCPUGoal(t *testing.T) {
	catalog := []session.CatalogEntry{
		{Group: "sw_metricsMinute", Type: session.ResourceTypeMeasure, Name: "mq_endpoint_consume_latency_minute"},
		{Group: "sw_metricsMinute", Type: session.ResourceTypeMeasure, Name: "meter_vm_cpu_average_used_minute"},
	}
	ranked := RankCatalogCandidates("帮我查询cpu指标", catalog, 2)
	if len(ranked) == 0 || ranked[0].Name != "meter_vm_cpu_average_used_minute" {
		t.Fatalf("expected cpu resource first, got %+v", ranked)
	}
}

func TestCollapseCJKSpacing(t *testing.T) {
	input := "你说 得 对 ， 我 目前 受 限于 工具 的限制"
	got := collapseCJKSpacing(input)
	want := "你说得对，我目前受限于工具的限制"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestCollapseProviderTextSpacingRepairsIdentifiers(t *testing.T) {
	input := "你说 得 对 ， 我 目前 受 限于 工具 的限制"
	got := NormalizeAgentDisplayText(input)
	if !strings.Contains(got, "你说得对") || strings.Contains(got, "你 说") {
		t.Fatalf("expected cjk spacing repair, got %q", got)
	}
}

func TestFindExplicitResourceMentionDoesNotChangeGranularity(t *testing.T) {
	catalog := []session.CatalogEntry{
		{Group: "sw_metricsHour", Type: session.ResourceTypeMeasure, Name: "meter_instance_host_cpu_used_rate_hour"},
		{Group: "sw_metricsHour", Type: session.ResourceTypeMeasure, Name: "meter_vm_cpu_average_used_hour"},
	}
	got := FindExplicitResourceMention("查询 meter_vm_cpu_average_used_minute 最近30分钟", catalog)
	if got != nil {
		t.Fatalf("expected unmatched minute resource, got %+v", got)
	}
}

func TestFindExplicitResourceMentionRejectsUnrelatedIdentifier(t *testing.T) {
	catalog := []session.CatalogEntry{
		{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_cpm_minute"},
		{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_latency_minute"},
	}
	if got := FindExplicitResourceMention("query completely_unrelated_metric_name", catalog); got != nil {
		t.Fatalf("expected unrelated identifier to remain unmatched, got %+v", got)
	}
}

func TestMatchResourceFromGoalRejectsAmbiguousCandidates(t *testing.T) {
	catalog := session.SchemaCatalog{
		Entries: []session.CatalogEntry{
			{Group: "metrics_a", Type: session.ResourceTypeMeasure, Name: "service_cpu_minute"},
			{Group: "metrics_b", Type: session.ResourceTypeMeasure, Name: "service_cpu_minute"},
		},
	}
	match := matchResourceFromGoal("query service_cpu_minute", catalog, session.ResourceTypeMeasure, "", nil)
	if match.Matched || !match.Ambiguous {
		t.Fatalf("expected ambiguous match, got %+v", match)
	}
}

func TestNormalizePlainAgentTextRepairsEnglishFragments(t *testing.T) {
	input := "Let me start by discovering the sche mas. The auto -matched resource is meter_v m_c pu_a verage_used_min ute, but you don 't see it."
	got := normalizePlainAgentText(input)
	for _, unexpected := range []string{"sche mas", "auto -matched", "auto -", "don 't", "meter_v m_c", "mestart"} {
		if strings.Contains(got, unexpected) {
			t.Fatalf("expected %q to be repaired in %q", unexpected, got)
		}
	}
	if !strings.Contains(got, "schemas") || !strings.Contains(got, "don't") {
		t.Fatalf("expected repaired prose, got %q", got)
	}
}

func TestRankCatalogCandidatesPaymentEndpoints(t *testing.T) {
	catalog := []session.CatalogEntry{
		{Group: "default", Type: session.ResourceTypeMeasure, Name: "service_cpm"},
		{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_endpoint_latency"},
		{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_endpoint_cpm"},
	}
	ranked := RankCatalogCandidates("top 10 slow payment endpoints in last 30 minutes", catalog, 3)
	if len(ranked) == 0 {
		t.Fatal("expected ranked catalog candidates")
	}
	if ranked[0].Name != "service_endpoint_latency" {
		t.Fatalf("unexpected top candidate: %+v", ranked[0])
	}
}

func TestMatchResourceFromGoalEndpointLatency(t *testing.T) {
	catalog := session.SchemaCatalog{
		Entries: []session.CatalogEntry{
			{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_cpm"},
			{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_endpoint_latency"},
			{Group: "default", Type: session.ResourceTypeStream, Name: "access_log"},
		},
	}
	match := matchResourceFromGoal(
		"top 10 slow payment endpoints in last 30 minutes",
		catalog,
		session.ResourceTypeMeasure,
		"",
		nil,
	)
	if !match.Matched {
		t.Fatal("expected catalog match")
	}
	if match.Name != "service_endpoint_latency" {
		t.Fatalf("unexpected resource name: %s", match.Name)
	}
	if match.Group != "sw_metrics" {
		t.Fatalf("unexpected group: %s", match.Group)
	}
}

func TestResolveSessionSlotsGoalOnly(t *testing.T) {
	catalog := session.SchemaCatalog{
		Entries: []session.CatalogEntry{
			{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_endpoint_latency"},
		},
	}
	resolved := ResolveSessionSlots(StartOptions{
		Goal:         "top 10 slow payment endpoints in last 30 minutes",
		ResourceType: session.ResourceTypeMeasure,
	}, catalog)
	if !resolved.AutoMatched {
		t.Fatal("expected auto match")
	}
	if resolved.ResourceName != "service_endpoint_latency" {
		t.Fatalf("unexpected resource name: %s", resolved.ResourceName)
	}
	if resolved.Groups[0] != "sw_metrics" {
		t.Fatalf("unexpected group: %s", resolved.Groups[0])
	}
	if resolved.SlotsPinned {
		t.Fatal("expected unpinned slots")
	}
}

func TestResolveSessionSlotsPinnedOverridesCatalog(t *testing.T) {
	catalog := session.SchemaCatalog{
		Entries: []session.CatalogEntry{
			{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_endpoint_latency"},
		},
	}
	resolved := ResolveSessionSlots(StartOptions{
		Goal:           "slow endpoints",
		ResourceType:   session.ResourceTypeMeasure,
		ResourceName:   "service_cpm",
		Groups:         []string{"production"},
		NameProvided:   true,
		GroupsProvided: true,
		TypeProvided:   true,
	}, catalog)
	if resolved.AutoMatched {
		t.Fatal("expected pinned slots to skip auto match")
	}
	if resolved.ResourceName != "service_cpm" {
		t.Fatalf("unexpected resource name: %s", resolved.ResourceName)
	}
	if resolved.Groups[0] != "production" {
		t.Fatalf("unexpected group: %s", resolved.Groups[0])
	}
	if !resolved.SlotsPinned {
		t.Fatal("expected pinned slots")
	}
}

func TestResolveSessionSlotsUsesExactNameWithoutGuessingType(t *testing.T) {
	catalog := session.SchemaCatalog{Entries: []session.CatalogEntry{{
		Group: "production",
		Type:  session.ResourceTypeStream,
		Name:  "access_log",
	}}}
	resolved := ResolveSessionSlots(StartOptions{
		Goal:         "inspect this resource",
		ResourceName: "access_log",
		NameProvided: true,
	}, catalog)
	if !resolved.AutoMatched || resolved.ResourceType != session.ResourceTypeStream {
		t.Fatalf("expected exact catalog identity, got %+v", resolved)
	}
	if len(resolved.Groups) != 1 || resolved.Groups[0] != "production" {
		t.Fatalf("expected exact resource group, got %+v", resolved.Groups)
	}
}

func TestResolveSessionSlotsDoesNotGuessAmbiguousGroup(t *testing.T) {
	catalog := session.SchemaCatalog{Entries: []session.CatalogEntry{
		{Group: "production", Type: session.ResourceTypeStream, Name: "access_log"},
		{Group: "staging", Type: session.ResourceTypeStream, Name: "access_log"},
	}}
	resolved := ResolveSessionSlots(StartOptions{
		Goal:         "inspect this resource",
		ResourceName: "access_log",
		NameProvided: true,
	}, catalog)
	if resolved.AutoMatched || len(resolved.Groups) != 0 {
		t.Fatalf("expected ambiguous group to remain unresolved, got %+v", resolved)
	}
}
