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

func generateT07(b *suiteBuilder) ([]AggregateFixture, []AggregateScenario, error) {
	readiness, err := t07ReadinessCases()
	if err != nil {
		return nil, nil, err
	}
	b.features["t07.fixture"] = true
	var scenarios []AggregateScenario
	add := func(name string, q t07Query, features ...string) {
		if b.err != nil {
			return
		}
		q.Fixture = T07FixtureName
		windows := [][2]int64{{-1, 6000}, {3000, 4000}, {7000, 8000}}
		if strings.Contains(name, "complete") {
			q.Fixture = T07GroupsFixtureName
			windows = [][2]int64{{-1, 103000}, {0, 100}, {200000, 201000}}
		}
		scenario := AggregateScenario{Name: "t07_" + name}
		for ordinal, window := range windows {
			q.Begin, q.End = window[0], window[1]
			c, buildErr := buildT07Case(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), q, features...)
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
	for _, function := range []string{"COUNT", "SUM", "MIN", "MAX", "AVG", "MEAN"} {
		for _, target := range []string{"v", "f"} {
			for _, grouped := range []bool{false, true} {
				q := t07Query{Columns: []t07Column{{Function: function, Target: target, Alias: "result"}}}
				if grouped {
					q.Columns = append([]t07Column{{Target: "host", Alias: "node"}}, q.Columns...)
					q.GroupBy = []string{"host"}
				}
				add(fmt.Sprintf("%s_%s_grouped_%t", strings.ToLower(function), target, grouped), q,
					"t07.aggregate."+function, "t07.null_empty", "t07.rebind")
			}
		}
	}
	for _, target := range []string{"v", "f"} {
		q := t07Query{Columns: []t07Column{
			{Function: "COUNT", Target: target, Alias: "c"},
			{Function: "SUM", Target: target, Alias: "s"},
			{Function: "MIN", Target: target, Alias: "lo"},
			{Function: "MAX", Target: target, Alias: "hi"},
			{Function: "AVG", Target: target, Alias: "a"},
		}}
		add("multiple_"+target, q, "catalog.B3", "t07.multiple", "t07.null_empty", "t07.shared_state")
	}
	shared := []t07Column{
		{Function: "SUM", Target: "v", Alias: "total"},
		{Function: "COUNT", Target: "v", Alias: "counted"},
		{Function: "AVG", Target: "v", Alias: "average"},
		{Function: "AVG", Target: "v", Alias: "copy"},
		{Function: "MEAN", Target: "v", Alias: "mean"},
		{Function: "AVG", Target: "f", Alias: "floating"},
	}
	add("shared_averages", t07Query{Columns: shared}, "catalog.B3", "t07.shared_state")
	for _, key := range []string{"zone", "v", "f"} {
		add("nullable_key_"+key, t07Query{
			Columns: []t07Column{
				{Target: key, Alias: "key"},
				{Function: "COUNT", Target: "rid", Alias: "c"},
				{Function: "SUM", Target: "v", Alias: "s"},
				{Function: "AVG", Target: "f", Alias: "a"},
			},
			GroupBy: []string{key},
		}, "t07.typed_keys", "t07.null_empty")
	}
	add("tuple_keys", t07Query{
		Columns: []t07Column{{Target: "host"}, {Target: "zone"}, {Function: "COUNT", Target: "f", Alias: "c"}},
		GroupBy: []string{"host", "zone"},
	}, "t07.typed_keys")
	add("hidden_group_key", t07Query{Columns: []t07Column{{Function: "COUNT", Target: "v", Alias: "c"}}, GroupBy: []string{"zone"}},
		"t07.typed_keys", "t07.hidden_slots")
	distinctColumns := []t07Column{
		{Function: "COUNT", Target: "zone", Alias: "unique_zones", Distinct: true},
		{Function: "COUNT", Target: "host", Alias: "unique_hosts", Distinct: true},
		{Function: "SUM", Target: "v", Alias: "total"},
	}
	add("count_distinct_scalar", t07Query{Columns: distinctColumns}, "count_distinct.scalar", "count_distinct.null_empty", "count_distinct.multiple")
	add("count_distinct_grouped", t07Query{
		Columns: append([]t07Column{{Target: "host", Alias: "node"}, {Target: "zone"}}, distinctColumns...), GroupBy: []string{"host", "zone"},
	}, "count_distinct.grouped", "count_distinct.multiple", "count_distinct.multikey")
	add("count_tags", t07Query{Columns: []t07Column{
		{Function: "COUNT", Target: "host", Alias: "hosts"},
		{Function: "COUNT", Target: "zone", Alias: "zones"},
		{Function: "COUNT", Target: "rid", Alias: "rows"},
	}}, "t07.count_tags")
	add("native_float_count", t07Query{
		Native: true, Legacy: true,
		Columns: []t07Column{{Function: "COUNT", Target: "f", Alias: "c"}},
	}, "t07.local_activation")
	add("native_multi_default_names", t07Query{
		Native: true, Legacy: true,
		Columns: []t07Column{{Function: "COUNT", Target: "f"}, {Function: "SUM", Target: "v"}},
	}, "t07.local_activation", "t07.default_names")
	add("qualified_columns", t07Query{Columns: []t07Column{
		{Target: "default.host", Alias: "node"},
		{Function: "COUNT", Target: "default.zone", Alias: "c"},
		{Function: "AVG", Target: "v", Alias: "a"},
	}, GroupBy: []string{"default.host"}}, "t07.qualified_columns")
	add("time_or", t07Query{
		Columns: shared,
		Where:   &T03Time{Op: "OR", Children: []*T03Time{t03Compare("=", 0), t03Compare("=", 5000)}},
	}, "t07.residual_time")
	add("complete_groups", t07Query{
		Columns: []t07Column{
			{Target: "zone", Alias: "key"},
			{Function: "COUNT", Target: "v", Alias: "c"},
			{Function: "AVG", Target: "f", Alias: "a"},
		}, GroupBy: []string{"zone"},
	}, "t07.complete_input", "t07.dedup")
	add("complete_scalar", t07Query{Columns: shared}, "t07.complete_input", "t07.dedup")
	add("scalar_limit_zero", t07Query{Columns: shared, Limit: uint32Pointer(0)}, "t07.limit")
	add("scalar_offset", t07Query{Columns: shared, Offset: 1}, "t07.limit")
	if b.err != nil {
		return nil, nil, b.err
	}
	return readiness, scenarios, nil
}

func generateT07Rejections(b *suiteBuilder) []Case {
	start := len(b.cases)
	prefix := "SELECT "
	source := " FROM " + FixtureGroup + "." + T07FixtureName
	for index, shape := range []struct{ projection, suffix string }{
		{"SUM(DISTINCT v) AS c", ""},
		{"host, SUM(v) AS s", ""},
		{"zone, SUM(v) AS s", " GROUP BY host"},
		{"SUM(v) AS same, AVG(f) AS same", ""},
		{"SUM(zone) AS s", ""},
		{"COUNT(absent) AS c", ""},
		{"SUM(v) AS s", " GROUP BY TIME_BUCKET('0s')"},
	} {
		b.reject(fmt.Sprintf("t07_invalid_shape_%d", index), prefix+shape.projection+source+shape.suffix, "t07.invalid_shape", codes.InvalidArgument)
		b.cases[len(b.cases)-1].Ticket = "T07"
		b.cases[len(b.cases)-1].NoScan = true
	}
	cases := b.cases[start:]
	b.cases = b.cases[:start]
	return cases
}

func validateAggregateScenarios(readiness []AggregateFixture, scenarios []AggregateScenario) error {
	if len(readiness) != 2 || readiness[0].Name != T07FixtureName || readiness[1].Name != T07GroupsFixtureName {
		return fmt.Errorf("generated T07 suite lacks its independent fixture visibility witnesses")
	}
	for _, fixture := range readiness {
		if len(fixture.Cases) != (len(t07Rows(fixture.Name))+127)/128 {
			return fmt.Errorf("generated T07 fixture %s lacks complete visibility witnesses", fixture.Name)
		}
	}
	if len(scenarios) == 0 {
		return fmt.Errorf("generated T07 suite lacks aggregation scenarios")
	}
	names := make(map[string]bool)
	for _, scenario := range scenarios {
		if scenario.Name == "" || names[scenario.Name] || len(scenario.Cases) != 3 {
			return fmt.Errorf("invalid generated T07 scenario %q", scenario.Name)
		}
		names[scenario.Name] = true
		text := strings.Split(scenario.Cases[0].Query, "\n#!params:")[0]
		for _, c := range scenario.Cases {
			if c.Ticket != "T07" || c.WantCode != codes.OK || text != strings.Split(c.Query, "\n#!params:")[0] {
				return fmt.Errorf("generated T07 scenario %s must preserve query text while rebinding", scenario.Name)
			}
		}
	}
	return nil
}
