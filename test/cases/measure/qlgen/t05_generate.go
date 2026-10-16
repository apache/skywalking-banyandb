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
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
)

// ReuseScenario is a generated process contract around real QL RPCs. Kind
// chooses a finite lifecycle action; every response still has an independent oracle.
type ReuseScenario struct {
	Name     string   `json:"name"`
	Kind     string   `json:"kind"`
	Cases    []Case   `json:"cases"`
	Features []string `json:"features"`
	Callers  int      `json:"callers,omitempty"`
}

func generateT05() ([]ReuseScenario, error) {
	var scenarios []ReuseScenario
	for _, shape := range []string{"legacy", "typed", "array", "clock", "limit", "null"} {
		scenario := ReuseScenario{Name: "t05_rebind_" + shape, Kind: "rebind", Features: []string{"t05.reuse." + shape}}
		for ordinal := range 3 {
			c, err := t05RebindCase(shape, ordinal)
			if err != nil {
				return nil, err
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
	}
	for _, kind := range []string{
		"cold-burst", "first-waiter-cancel", "all-waiters-cancel", "evict-in-flight",
		"schema-replace-delete-recreate", "multi-group-dependencies", "permissions-and-fallback",
		"same-revision-watch", "lower-revision-watch", "schema-change-during-compile",
		"literal-bypass", "oversized-fallback",
	} {
		scenario := ReuseScenario{Name: "t05_" + strings.ReplaceAll(kind, "-", "_"), Kind: kind, Features: []string{"t05.lifecycle." + kind}}
		for ordinal := range 3 {
			c, err := t05RebindCase("typed", ordinal)
			if err != nil {
				return nil, err
			}
			c.Name = fmt.Sprintf("%s_call_%d", scenario.Name, ordinal)
			scenario.Cases = append(scenario.Cases, c)
		}
		if kind == "first-waiter-cancel" {
			scenario.Cases[0].WantCode = codes.Canceled
		}
		if kind == "all-waiters-cancel" {
			recovery := scenario.Cases[2]
			recovery.Name = "t05_all_cancel_recovery"
			for index := range scenario.Cases {
				scenario.Cases[index].WantCode = codes.Canceled
			}
			scenario.Cases = append(scenario.Cases, recovery)
		}
		if kind == "schema-replace-delete-recreate" {
			missing := scenario.Cases[1]
			missing.Name = "t05_schema_deleted"
			missing.WantCode = codes.Internal
			scenario.Cases = append(scenario.Cases[:2], missing, scenario.Cases[2])
		}
		if kind == "permissions-and-fallback" {
			scenario.Cases[1].WantCode = codes.PermissionDenied
			scenario.Cases[1].NoScan = true
			fallback := scenario.Cases[0]
			fallback.Name = "t05_permissions_uncached_fallback"
			scenario.Cases = append(scenario.Cases, fallback)
		}
		if kind == "multi-group-dependencies" {
			for index := range scenario.Cases {
				scenario.Cases[index].Query = strings.ReplaceAll(scenario.Cases[index].Query, "FROM "+FixtureGroup+"."+T05FixtureName,
					"FROM MEASURE "+T05FixtureName+" IN ("+FixtureGroup+", "+T05OtherFixtureGroup+")")
			}
		}
		if kind == "literal-bypass" {
			for ordinal := range 3 {
				q := T03Query{
					Columns: []T03Column{{Source: "rid", Alias: "literal_row"}}, Shorthand: true,
					Where: t03And(t03Compare(">=", int64(ordinal*2000)), t03Compare("<", int64(ordinal*2000+2000))),
				}
				c, err := BuildT03Case(fmt.Sprintf("t05_literal_%d", ordinal), q)
				if err != nil {
					return nil, err
				}
				c.Ticket = "T05"
				c.Query = strings.ReplaceAll(c.Query, T03FixtureName, T05FixtureName)
				scenario.Cases[ordinal] = c
			}
		}
		if kind == "evict-in-flight" {
			q := T03Query{
				Columns: []T03Column{{Source: "rid", Alias: "different_row"}}, Shorthand: true, Bind: true,
				Where: t03And(t03Compare(">=", 2000), t03Compare("<", 4000)),
			}
			c, err := BuildT03Case("t05_eviction_different_key", q)
			if err != nil {
				return nil, err
			}
			c.Ticket = "T05"
			c.Query = strings.ReplaceAll(c.Query, T03FixtureName, T05FixtureName)
			scenario.Cases[1] = c
		}
		if kind == "cold-burst" {
			scenario.Callers = 32
		}
		scenarios = append(scenarios, scenario)
	}
	return scenarios, nil
}

func t05RebindCase(shape string, ordinal int) (Case, error) {
	name := fmt.Sprintf("t05_%s_bind_%d", shape, ordinal)
	if shape == "legacy" || shape == "array" {
		q := baseQuery()
		q.Columns = []string{"rid", "host", "zone", "v::FIELD", "f::FIELD"}
		q.OrderBy = "TIME"
		q.Bind = true
		if shape == "legacy" {
			q.Where = &Predicate{Op: "=", Column: "host", Values: []any{[]string{"a", "b", "c"}[ordinal]}}
		} else {
			q.Where = &Predicate{Op: "IN", Column: "host", Values: [][]any{{"a"}, {}, {"b", "d"}}[ordinal]}
		}
		c, err := BuildCase(name, q)
		if err != nil {
			return Case{}, err
		}
		if shape == "array" {
			values := [][]string{{"a"}, {}, {"b", "d"}}[ordinal]
			if len(values) == 0 {
				c.WantCode = codes.InvalidArgument
				c.NoScan = true
			}
			c.Query, err = t05Params("SELECT rid, host, zone, v::FIELD, f::FIELD FROM MEASURE "+FixtureName+" IN "+FixtureGroup+
				" TIME BETWEEN ? AND ? WHERE host IN (?) ORDER BY TIME ASC", []any{
				jsonValue(timePlaceholder(-1)), jsonValue(timePlaceholder(8000)),
				map[string]any{"strArray": map[string]any{"value": values}},
			})
		}
		c.Ticket = "T05"
		return c, err
	}
	begin := int64(ordinal * 2000)
	q := T03Query{
		Columns: []T03Column{t03RID(), {Source: "host", Alias: "node"}, {Source: "v::FIELD", Alias: "value"}, {Source: "f::FIELD", Alias: "floating"}},
		Where:   t03And(t03Compare(">=", begin), t03Compare("<", begin+2000)), Shorthand: true, Bind: true,
	}
	if shape == "clock" {
		clock := int64((ordinal + 1) * 2000)
		q.ClockMillis = &clock
		q.Where = t03And(&T03Time{Op: ">=", Now: true, Nanos: -2_000_000_000}, &T03Time{Op: "<", Now: true}, t03Compare(">=", 0))
	}
	if shape == "limit" {
		q.Where = t03Window()
		q.Limit = uint32Pointer([]uint32{3, 0, 5}[ordinal])
	}
	c, err := BuildT03Case(name, q)
	if err != nil {
		return Case{}, err
	}
	c.Ticket = "T05"
	c.Query = strings.ReplaceAll(c.Query, T03FixtureName, T05FixtureName)
	if shape == "null" && ordinal == 1 {
		c.Query, err = t05Params("SELECT rid, host AS node, v::FIELD AS value, f::FIELD AS floating FROM "+FixtureGroup+"."+T05FixtureName+
			" WHERE (time >= ? AND time < ?) ORDER BY TIME ASC", []any{map[string]any{"null": 0}, jsonValue(timePlaceholder(4000))})
		c.WantJSON = nil
		c.WantCode = codes.InvalidArgument
		c.NoScan = true
	}
	return c, err
}

func t05Params(query string, params []any) (string, error) {
	content, err := json.Marshal(params)
	if err != nil {
		return "", fmt.Errorf("encode T05 parameters: %w", err)
	}
	return query + "\n#!params: " + string(content), nil
}

func validateReuseScenarios(scenarios []ReuseScenario) error {
	required := map[string]bool{}
	for _, kind := range []string{
		"rebind", "cold-burst", "first-waiter-cancel", "all-waiters-cancel", "evict-in-flight",
		"schema-replace-delete-recreate", "multi-group-dependencies", "permissions-and-fallback",
		"same-revision-watch", "lower-revision-watch", "schema-change-during-compile",
		"literal-bypass", "oversized-fallback",
	} {
		required[kind] = false
	}
	names := make(map[string]bool)
	rebinds := map[string]bool{"legacy": false, "typed": false, "array": false, "clock": false, "limit": false, "null": false}
	for _, scenario := range scenarios {
		if _, exists := required[scenario.Kind]; !exists || scenario.Name == "" || names[scenario.Name] || len(scenario.Cases) < 3 {
			return fmt.Errorf("invalid generated T05 scenario %q", scenario.Name)
		}
		names[scenario.Name] = true
		if scenario.Kind == "rebind" {
			shape, found := strings.CutPrefix(scenario.Name, "t05_rebind_")
			if _, known := rebinds[shape]; !found || !known {
				return fmt.Errorf("unknown generated T05 rebind shape %q", scenario.Name)
			}
			rebinds[shape] = true
		}
		required[scenario.Kind] = true
		if scenario.Kind == "cold-burst" && scenario.Callers != 32 {
			return fmt.Errorf("T05 cold burst requires 32 callers")
		}
		for _, c := range scenario.Cases {
			if c.Ticket != "T05" || c.Name == "" || c.Query == "" || c.WantCode == codes.OK && !json.Valid(c.WantJSON) {
				return fmt.Errorf("invalid generated T05 call %q", c.Name)
			}
		}
	}
	for shape, seen := range rebinds {
		if !seen {
			return fmt.Errorf("generated T05 suite lacks %s rebind", shape)
		}
	}
	for kind, seen := range required {
		if !seen {
			return fmt.Errorf("generated T05 suite lacks %s", kind)
		}
	}
	return nil
}

func t05ReadinessCase() (Case, error) {
	c, err := BuildT03Case("t05_scan_all", T03Query{Columns: []T03Column{{Source: "rid", Alias: "row_id"}}, Shorthand: true})
	if err != nil {
		return Case{}, err
	}
	c.Ticket = "T05"
	c.Query = strings.ReplaceAll(c.Query, T03FixtureName, T05FixtureName)
	return c, nil
}

func validateReuseReadiness(c Case) error {
	if c.Name != "t05_scan_all" || c.Ticket != "T05" || c.Query == "" || c.WantCode != codes.OK || !json.Valid(c.WantJSON) {
		return fmt.Errorf("generated T05 suite lacks its independent fixture visibility witness")
	}
	return nil
}
