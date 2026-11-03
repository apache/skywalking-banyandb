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
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
)

func generateT08(b *suiteBuilder) ([]AggregateFixture, []NativeAggregateScenario, error) {
	readiness, err := t08ReadinessCases()
	if err != nil {
		return nil, nil, err
	}
	b.features["t08.fixture"] = true
	var scenarios []NativeAggregateScenario
	add := func(name, kind string, branches uint64, q t08Query, features ...string) {
		if b.err != nil {
			return
		}
		if q.Fixture == "" {
			q.Fixture = T08FixtureName
		}
		windows := [][2]int64{{-1, 8000}, {1000, 6000}, {200000, 201000}}
		if q.Fixture == T08GroupsFixtureName {
			windows = [][2]int64{{-1, 100000}, {1000, 4000}, {200000, 201000}}
		}
		if q.End != 0 {
			windows = [][2]int64{{q.Begin, q.End}, {q.Begin, q.End}, {200000, 201000}}
		}
		scenario := NativeAggregateScenario{Name: "t08_" + name, Kind: kind, Branches: branches}
		for ordinal, window := range windows {
			q.Begin, q.End = window[0], window[1]
			c, buildErr := buildT08Case(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), q, features...)
			if buildErr != nil {
				b.err = buildErr
				return
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		for _, feature := range features {
			b.features[feature] = true
		}
		scenarios = append(scenarios, scenario)
	}
	shared := []t07Column{
		{Function: "SUM", Target: "f", Alias: "s"},
		{Function: "COUNT", Target: "f", Alias: "c"},
		{Function: "AVG", Target: "f", Alias: "a"},
		{Function: "AVG", Target: "f", Alias: "copy"},
		{Function: "MEAN", Target: "f", Alias: "mean"},
	}
	grouped := func(columns []t07Column) t08Query {
		return t08Query{
			Columns: append([]t07Column{{Target: "host"}, {Target: "region"}}, columns...),
			GroupBy: []string{"host", "region"}, Hosts: []string{"a", "b", "c"}, Regions: []string{"east", "west"},
		}
	}
	for _, function := range []string{"SUM", "COUNT", "AVG", "MEAN"} {
		branches := uint64(2)
		if function == "COUNT" {
			branches = 1
		}
		add("float_"+strings.ToLower(function), "query", branches,
			grouped([]t07Column{{Function: function, Target: "f", Alias: "result"}}),
			"t08.native_float", "t08.null_empty", "t08.tuple_keys", "t08.rebind")
	}
	add("shared_float_leaves", "query", 2, grouped(shared), "t08.shared_leaves", "t08.null_empty", "t08.tuple_keys", "t08.rebind")
	integer := []t07Column{
		{Function: "MIN", Target: "v", Alias: "lo"},
		{Function: "MAX", Target: "v", Alias: "hi"},
		{Function: "COUNT", Target: "v", Alias: "c"},
	}
	add("integer_extrema", "query", 3, grouped(integer), "t08.exact_integer", "t08.null_empty", "t08.tuple_keys")
	add("integer_count", "query", 1, grouped([]t07Column{{Function: "COUNT", Target: "v", Alias: "c"}}), "t08.exact_integer")
	scalar := t08Query{Columns: shared, Hosts: []string{"a"}, Regions: []string{"east"}}
	add("scalar_single_entity", "query", 2, scalar, "t08.scalar_entity", "t08.shared_leaves", "t08.null_empty", "t08.rebind")
	scalar.Hosts, scalar.Regions = []string{"c"}, []string{"west"}
	add("scalar_all_null", "query", 2, scalar, "t08.scalar_entity", "t08.null_empty")
	q := grouped(shared)
	q.Columns = append([]t07Column{{Target: "host", Alias: "node"}}, shared...)
	q.GroupBy, q.Regions = []string{"host"}, []string{"east"}
	add("bound_hidden_entity", "query", 2, q, "t08.scalar_entity", "t08.shared_leaves")
	q = grouped(shared)
	q.Columns, q.GroupBy = shared, []string{"host", "region"}
	add("hidden_tuple_keys", "query", 2, q, "t08.tuple_keys", "t08.shared_leaves")
	q = grouped(shared)
	q.Columns[0].Target, q.Columns[1].Target = "default.host", "default.region"
	q.GroupBy = []string{"default.host", "default.region"}
	add("qualified_tuple", "query", 2, q, "t08.tuple_keys")
	q = grouped(shared)
	q.Fixture, q.Hosts = T08GroupsFixtureName, nil
	for host := range 65 {
		q.Hosts = append(q.Hosts, fmt.Sprintf("host-%03d", host))
	}
	add("complete_tuple_groups", "query", 2, q, "t08.complete_groups", "t08.dedup", "t08.tuple_keys", "t08.null_empty")
	q = t08Query{Columns: shared}
	add("fallback_unpruned_scalar", "fallback", 0, q, "t08.fallback", "t08.weighted_average")
	q = grouped(shared)
	q.Columns = append([]t07Column{{Target: "host"}}, shared...)
	q.GroupBy = []string{"host"}
	add("fallback_incomplete_entity", "fallback", 0, q, "t08.fallback", "t08.weighted_average")
	q = grouped(shared)
	q.Columns = append([]t07Column{{Target: "host"}, {Target: "region"}, {Target: "key"}}, shared...)
	q.GroupBy = []string{"host", "region", "key"}
	add("fallback_nullable_key", "fallback", 0, q, "t08.fallback", "t08.null_empty")
	q.Key = new(string)
	add("fallback_bound_empty_key", "fallback", 0, q, "t08.fallback", "t08.null_empty")
	for _, function := range []string{"MIN", "MAX"} {
		add("fallback_float_"+strings.ToLower(function), "fallback", 0,
			grouped([]t07Column{{Function: function, Target: "f", Alias: "result"}}), "t08.fallback")
	}
	for _, function := range []string{"SUM", "AVG"} {
		q = grouped([]t07Column{{Function: function, Target: "v", Alias: "result"}})
		q.Hosts = []string{"a", "b", "c"}
		add("fallback_integer_"+strings.ToLower(function), "fallback", 0, q, "t08.fallback", "t08.exact_integer")
	}
	q = grouped(shared)
	q.Where = &T03Time{Op: "OR", Children: []*T03Time{
		t03Compare("=", 0), t03Compare("=", 2000), t03Compare("=", 4000), t03Compare("=", 200000), t03Compare("=", 200500),
	}}
	add("fallback_residual_time", "fallback", 0, q, "t08.fallback")
	q = grouped([]t07Column{{Function: "COUNT", Target: "key", Alias: "c"}})
	add("fallback_tag_count", "fallback", 0, q, "t08.fallback", "t08.null_empty")
	q = grouped(shared)
	q.Hosts, q.Regions = nil, nil
	add("fallback_unproven_keys", "fallback", 0, q, "t08.fallback")
	q = grouped(shared)
	q.Limit = uint32Pointer(0)
	add("limit_zero", "query", 0, q, "t08.no_scan")
	for _, kind := range []string{"group-cap", "branch-error", "cancellation", "authorization", "child-revision"} {
		features := []string{"t08.lifecycle"}
		if kind == "child-revision" {
			features = append(features, "t08.child_revision")
		}
		add(strings.ReplaceAll(kind, "-", "_"), kind, 2, grouped(shared), features...)
	}
	if b.err != nil {
		return nil, nil, b.err
	}
	// Preserve the independent native SUM oracle and replace its null result
	// with the explicit COALESCE fallback after native finalization.
	for _, original := range scenarios {
		if original.Name != "t08_float_sum" {
			continue
		}
		scenario := NativeAggregateScenario{Name: "qg2_native_float_coalesce", Kind: original.Kind, Branches: original.Branches}
		for _, originalCase := range original.Cases {
			c := originalCase
			c.Name = strings.Replace(c.Name, original.Name, scenario.Name, 1)
			c.Query = strings.Replace(c.Query, "SUM(f) AS result", "COALESCE(SUM(f), 0.0) AS result", 1)
			c.WantJSON = []byte(strings.ReplaceAll(string(c.WantJSON), `{"null":0}`, `{"float":{"value":0}}`))
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
		break
	}
	return readiness, scenarios, nil
}

func validateNativeAggregateScenarios(readiness []AggregateFixture, scenarios []NativeAggregateScenario) error {
	if len(readiness) != 2 || readiness[0].Name != T08FixtureName || readiness[1].Name != T08GroupsFixtureName {
		return fmt.Errorf("generated T08 suite lacks its independent fixture visibility witnesses")
	}
	for _, fixture := range readiness {
		if len(fixture.Cases) != (len(t08Rows(fixture.Name))+15)/16+1 {
			return fmt.Errorf("generated T08 fixture %s lacks complete visibility witnesses", fixture.Name)
		}
	}
	if len(scenarios) == 0 {
		return fmt.Errorf("generated T08 suite lacks native aggregation scenarios")
	}
	for _, scenario := range scenarios {
		if len(scenario.Cases) != 3 {
			return fmt.Errorf("generated T08 scenario %s lacks three changed bindings", scenario.Name)
		}
		switch scenario.Kind {
		case "query", "fallback", "group-cap", "branch-error", "cancellation", "authorization", "child-revision":
		default:
			return fmt.Errorf("generated T08 scenario %s has unknown kind %q", scenario.Name, scenario.Kind)
		}
		text := strings.Split(scenario.Cases[0].Query, "\n#!params:")[0]
		for _, c := range scenario.Cases {
			if c.Ticket != "T08" || c.WantCode != codes.OK || text != strings.Split(c.Query, "\n#!params:")[0] {
				return fmt.Errorf("generated T08 scenario %s must preserve query text while rebinding", scenario.Name)
			}
		}
	}
	return nil
}

func generateT08Rejections(b *suiteBuilder) []Case {
	query, err := renderT08(t08Query{
		Fixture: T08FixtureName, Columns: []t07Column{{Function: "SUM", Target: "v", Alias: "s"}},
		Hosts: []string{"overflow"}, Regions: []string{"east"}, Begin: 8000, End: 10000,
	})
	if err != nil {
		b.err = err
		return nil
	}
	b.features["t08.integer_overflow"] = false
	return []Case{{
		Name: "t08_integer_sum_overflow", Ticket: "T08", Query: query, WantCode: codes.Unknown,
		Features: []string{"t08.integer_overflow"},
	}}
}
