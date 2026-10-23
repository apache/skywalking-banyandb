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

func generateT14(b *suiteBuilder) ([]AggregateFixture, []HavingScenario, []Case, error) {
	readiness, err := t14ReadinessCases()
	if err != nil {
		return nil, nil, nil, err
	}
	b.features["t14.fixture"] = true
	var scenarios []HavingScenario
	add := func(name string, q t14Query, branches uint64, features ...string) {
		if b.err != nil {
			return
		}
		if q.Fixture == "" {
			q.Fixture = T14FixtureName
		}
		windows := [][2]int64{{-1, 8000}, {1000, 6000}, {200000, 201000}}
		if q.Fixture == T14GroupsFixtureName {
			windows = [][2]int64{{-1, 30000}, {-1, 30000}, {200000, 201000}}
		}
		scenario := HavingScenario{Name: "t14_" + name, Kind: "query", Branches: branches}
		for ordinal, window := range windows {
			q.Begin, q.End = window[0], window[1]
			c, buildErr := buildT14Case(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), q, append(features, "t14.rebind")...)
			if buildErr != nil {
				b.err = buildErr
				return
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		for _, feature := range append(features, "t14.rebind") {
			b.features[feature] = true
		}
		scenarios = append(scenarios, scenario)
	}
	host := t07Column{Target: "host"}
	sum := t07Column{Function: "SUM", Target: "v", Alias: "total"}
	floatSum := t07Column{Function: "SUM", Target: "f", Alias: "total"}
	sumV, sumF := t14Aggregate("SUM", "v"), t14Aggregate("SUM", "f")
	greater := func(expression *t10Expr, value int64) *t10Expr { return t10Binary(">", expression, t10Literal(value)) }
	base := t14Query{Columns: []t07Column{host, sum}, GroupBy: []string{"host"}, Having: t10Binary(">", sumV, t10Parameter(0)), Params: []any{int64(3)}}
	add("having_F", base, 0, "catalog.B4", "t14.grouped", "t14.local", "t14.three_valued")
	postfix := *sumV
	postfix.Text = "SUM(v)::FIELD"
	postfixQuery := base
	postfixQuery.Having = t10Binary(">", &postfix, t10Parameter(0))
	add("postfix_aggregate", postfixQuery, 0, "t14.aggregate_syntax", "t14.local")
	castQuery := base
	castQuery.Having = t10Binary(">", t10Cast(sumV, "DOUBLE"), t10Parameter(0))
	add("cast_aggregate", castQuery, 0, "t14.aggregate_syntax", "t14.expression", "t14.local")
	groupOnly := base
	groupOnly.Columns = []t07Column{{Target: "host", Alias: "source_host"}}
	groupOnly.Having = t10Binary("!=", t10Source("host"), t10Literal("a"))
	groupOnly.Params = nil
	add("group_only", groupOnly, 0, "t14.group_only", "t14.grouped", "t14.local")
	hidden := base
	hidden.Columns = []t07Column{host}
	add("hidden_aggregate", hidden, 0, "t14.hidden_aggregate", "t14.local")
	repeated := base
	repeated.Columns = []t07Column{host, sum, {Function: "SUM", Target: "v", Alias: "copy"}, {Function: "COUNT", Target: "v", Alias: "count"}}
	repeated.Having = t10Binary("AND", base.Having, greater(t14Aggregate("COUNT", "v"), 0))
	add("repeated_aggregate", repeated, 0, "t14.repeated_aggregate", "t14.local")
	native := base
	native.Hosts = []string{"a", "b", "c", "n"}
	native.Columns = []t07Column{host, floatSum}
	native.Having = greater(sumF, 3)
	native.Params = nil
	add("native_having_F", native, 2, "t14.native", "t14.grouped")
	hiddenNative := native
	hiddenNative.Columns = []t07Column{host}
	add("native_hidden_aggregate", hiddenNative, 2, "t14.native", "t14.hidden_aggregate")
	repeatedNative := native
	repeatedNative.Columns = []t07Column{
		host, floatSum,
		{Function: "SUM", Target: "f", Alias: "copy"},
		{Function: "COUNT", Target: "f", Alias: "count"},
		{Function: "AVG", Target: "f", Alias: "average"},
		{Function: "MEAN", Target: "f", Alias: "mean"},
	}
	repeatedNative.Having = t10Binary("AND", greater(sumF, 3), greater(t14Aggregate("COUNT", "f"), 0))
	add("native_repeated_aggregate", repeatedNative, 2, "t14.native", "t14.repeated_aggregate")
	for _, definition := range []struct {
		predicate *t10Expr
		name      string
	}{
		{name: "having_null", predicate: t10Unary("IS NULL", sumV)},
		{name: "having_null_constant", predicate: t10Literal(nil)},
		{name: "having_true_constant", predicate: t10Literal(true)},
		{name: "having_false_constant", predicate: t10Literal(false)},
		{name: "having_coalesce", predicate: greater(t10Call("COALESCE", sumV, t10Literal(int64(4))), 3)},
		{name: "having_group_reference", predicate: t10Binary("AND", t10Binary("!=", t10Source("host"), t10Literal("a")), greater(sumV, 3))},
		{name: "having_masked_case", predicate: t10Call("CASE", t10Literal(true), greater(sumV, 3),
			t10Binary("=", t10Binary("/", t10Literal(int64(1)), t10Literal(int64(0))), t10Literal(int64(0))), t10Literal(true), t10Literal(false))},
		{name: "having_masked_or", predicate: t10Binary("OR", t10Binary("=", t14Aggregate("COUNT", "v"), t10Literal(int64(0))),
			greater(t10Binary("/", t10Literal(int64(8)), sumV), 1))},
	} {
		query := base
		query.Having = definition.predicate
		query.Params = nil
		add(definition.name, query, 0, "t14.three_valued", "t14.expression", "t14.local")
	}
	hiddenGroup := base
	hiddenGroup.Columns = []t07Column{sum}
	hiddenGroup.Having = t10Binary("AND", t10Binary("!=", t10Source("host"), t10Literal("a")), greater(sumV, 3))
	hiddenGroup.Params = nil
	add("hidden_group", hiddenGroup, 0, "t14.hidden_group", "t14.grouped", "t14.local")
	scalar := base
	scalar.Columns = []t07Column{sum}
	scalar.GroupBy = nil
	add("scalar_having", scalar, 0, "t14.scalar", "t14.local")
	scalarNull := scalar
	scalarNull.Having = t10Unary("IS NULL", sumV)
	scalarNull.Params = nil
	add("scalar_empty_null", scalarNull, 0, "t14.scalar", "t14.empty", "t14.three_valued")
	scalarCount := scalar
	scalarCount.Columns = []t07Column{{Function: "COUNT", Target: "v", Alias: "count"}}
	scalarCount.Having = t10Binary("=", t14Aggregate("COUNT", "v"), t10Literal(int64(0)))
	scalarCount.Params = nil
	add("scalar_empty_count", scalarCount, 0, "t14.scalar", "t14.empty")
	localFilter := base
	localFilter.Where = greater(t10Source("v"), 0)
	add("where_before_having", localFilter, 0, "t14.raw_filter", "t14.local")
	complete := native
	complete.Fixture = T14GroupsFixtureName
	complete.Hosts = nil
	for group := range 129 {
		complete.Hosts = append(complete.Hosts, fmt.Sprintf("group-%03d", group))
	}
	complete.Having = greater(sumF, -1)
	add("complete_groups", complete, 2, "t14.complete_groups", "t14.native")
	final := complete
	final.Having = t10Binary("=", t10Source("host"), t10Literal("group-128"))
	final.Limit = uint32Pointer(1)
	add("having_before_limit", final, 2, "t14.final_pagination", "t14.complete_groups", "t14.native")
	final.Offset = 1
	add("having_before_offset", final, 2, "t14.final_pagination", "t14.complete_groups", "t14.native")
	zero := base
	zero.Limit = uint32Pointer(0)
	add("zero_limit", zero, 0, "t14.no_scan", "t14.final_pagination")
	membershipScenarios, membershipErr := generateT14Membership(b)
	if membershipErr != nil {
		return nil, nil, nil, membershipErr
	}
	scenarios = append(scenarios, membershipScenarios...)
	rejections, rejectErr := generateT14Rejections(b)
	if rejectErr != nil {
		return nil, nil, nil, rejectErr
	}
	if b.err != nil {
		return nil, nil, nil, b.err
	}
	return readiness, scenarios, rejections, nil
}

func generateT14Rejections(b *suiteBuilder) ([]Case, error) {
	var cases []Case
	for _, definition := range []struct{ name, having string }{
		{"alias_reference", "total > 3"},
		{"ungrouped_field", "v::FIELD > 3"},
		{"ungrouped_tag", "zone = 'x'"},
		{"unknown_source", "missing = 1"},
		{"nested_aggregate", "SUM(SUM(v)) > 3"},
		{"aggregate_argument", "SUM(v + 1) > 3"},
		{"non_boolean", "SUM(v)"},
		{"unknown_function", "ABS(SUM(v)) > 3"},
	} {
		q := t14Query{
			Fixture: T14FixtureName, Columns: []t07Column{{Target: "host"}, {Function: "SUM", Target: "v", Alias: "total"}}, GroupBy: []string{"host"},
			Having: &t10Expr{Text: definition.having}, Begin: -1, End: 8000,
		}
		query, err := renderT14(q)
		if err != nil {
			return nil, err
		}
		cases = append(cases, Case{
			Name: "t14_reject_" + definition.name, Ticket: "T14", Query: query, WantCode: codes.InvalidArgument,
			NoScan: true, Features: []string{"t14.invalid_reference"},
		})
	}
	b.features["t14.invalid_reference"] = false
	return cases, nil
}

func validateHavingScenarios(readiness []AggregateFixture, scenarios []HavingScenario) error {
	fixtureNames := []string{T14FixtureName, T14GroupsFixtureName, T14LegacyFixtureName, T14ContainsFixtureName, T14IndexedFixtureName}
	if len(readiness) != len(fixtureNames) || len(scenarios) == 0 {
		return fmt.Errorf("generated T14 lacks its independent fixtures or scenarios")
	}
	for ordinal, name := range fixtureNames {
		if readiness[ordinal].Name != name || len(readiness[ordinal].Cases) == 0 {
			return fmt.Errorf("generated T14 lacks independent fixture %s", name)
		}
	}
	for _, scenario := range scenarios {
		if len(scenario.Cases) != 3 {
			return fmt.Errorf("generated T14 scenario %s lacks three bindings", scenario.Name)
		}
		if scenario.Kind != "query" && scenario.Kind != "membership" {
			return fmt.Errorf("unknown T14 scenario kind %s", scenario.Kind)
		}
		first, _, _ := strings.Cut(scenario.Cases[0].Query, "\n#!params:")
		for _, c := range scenario.Cases {
			text, _, _ := strings.Cut(c.Query, "\n#!params:")
			if text != first {
				return fmt.Errorf("T14 scenario %s changes SQL text", scenario.Name)
			}
			if scenario.Kind == "query" && c.Having == nil {
				return fmt.Errorf("T14 query %s lacks group truth expectations", c.Name)
			}
			if c.Having != nil && c.Having.InputGroups != c.Having.TrueGroups+c.Having.FalseGroups+c.Having.UnknownGroups {
				return fmt.Errorf("T14 query %s loses a completed group truth value", c.Name)
			}
		}
	}
	return nil
}

func generateT14Membership(b *suiteBuilder) ([]HavingScenario, error) {
	var scenarios []HavingScenario
	add := func(name string, q t14Query, bindings [][]any, features ...string) error {
		scenario := HavingScenario{Name: "t14_" + name, Kind: "membership"}
		for ordinal, values := range bindings {
			membership := *q.Membership
			membership.Values = values
			q.Membership = &membership
			q.Begin, q.End = -1, 8000
			if strings.HasPrefix(membership.Form, "literal") {
				window := [][2]int64{{-1, 8000}, {1000, 6000}, {200000, 201000}}[ordinal]
				q.Begin, q.End = window[0], window[1]
			}
			c, err := buildT14Case(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), q, append(features, "t14.rebind")...)
			if err != nil {
				return err
			}
			scenario.Cases = append(scenario.Cases, c)
			for _, feature := range c.Features {
				b.features[feature] = c.WantCode == codes.OK
			}
		}
		scenarios = append(scenarios, scenario)
		return nil
	}
	for _, column := range []string{"labels", "nums", "host", "rid"} {
		var first, second any = "red", "blue"
		var one, two any = []string{"red"}, []string{"red", "blue"}
		if column == "host" {
			first, second = "a", "b"
			one, two = []string{"a"}, []string{"a", "b"}
		}
		if column == "nums" || column == "rid" {
			first, second = int64(1), int64(2)
			one, two = []int64{1}, []int64{1, 2}
		}
		for _, operator := range []string{"HAVING", "NOT HAVING", "CONTAINS", "NOT CONTAINS"} {
			prefix := featureName(operator) + "_" + column
			for _, form := range []string{"single", "list"} {
				q := t14Query{Fixture: T14FixtureName, Columns: []t07Column{{Target: "rid"}}, Membership: &t14Membership{Column: column, Operator: operator, Form: form}}
				features := []string{"t14.membership_spelling", "t14.membership_binding", "t14.membership_form", "t14.membership_null"}
				if column == "host" || column == "rid" {
					features = append(features, "t14.membership_scalar_source")
				} else {
					features = append(features, "t14.membership_array_source")
				}
				bindings := [][]any{{first}, {two}, {nil}}
				if form == "list" {
					bindings = [][]any{{first}, {two}, {one}}
				}
				if err := add(prefix+"_"+form, q, bindings, features...); err != nil {
					return nil, err
				}
			}
			if column == "labels" || column == "nums" {
				for _, literalForm := range []string{"literal-single", "literal"} {
					values := []any{first}
					if literalForm == "literal" {
						values = append(values, second)
					}
					literal := t14Query{
						Fixture: T14FixtureName, Columns: []t07Column{{Target: "rid"}},
						Membership: &t14Membership{Column: column, Operator: operator, Form: literalForm},
					}
					if err := add(prefix+"_"+literalForm, literal, [][]any{values, values, values}, "t14.membership_literal", "t14.membership_spelling"); err != nil {
						return nil, err
					}
				}
				if column == "labels" {
					single := t14Query{
						Fixture: T14FixtureName, Columns: []t07Column{{Target: "rid"}},
						Membership: &t14Membership{Column: column, Operator: operator, Form: "single"},
						Where:      t10Binary(">", t10Source("v"), t10Literal(int64(0))),
					}
					if err := add(prefix+"_single_F", single, [][]any{{first}, {second}, {nil}}, "t14.membership_F", "t14.membership_spelling", "t14.raw_filter"); err != nil {
						return nil, err
					}
				}
				q := t14Query{Fixture: T14FixtureName, Columns: []t07Column{{Target: "rid"}}, Membership: &t14Membership{Column: column, Operator: operator, Form: "list"}}
				var empty any = []string{}
				if column == "nums" {
					empty = []int64{}
				}
				coercionValues := [][]any{{int64(1)}, {"red"}, {int64(2)}}
				if column == "nums" {
					coercionValues = [][]any{{"2"}, {"invalid"}, {int64(1)}}
				}
				if err := add(prefix+"_coercion", q, coercionValues, "t14.membership_coercion", "t14.membership_spelling"); err != nil {
					return nil, err
				}
				if err := add(prefix+"_empty_recovery", q, [][]any{{empty}, {one}, {two}},
					"t14.membership_binding", "t14.membership_recovery", "t14.membership_spelling"); err != nil {
					return nil, err
				}
				if err := add(prefix+"_null_recovery", q, [][]any{{nil}, {one}, {two}}, "t14.membership_binding", "t14.membership_null", "t14.membership_spelling"); err != nil {
					return nil, err
				}
				// The spelling and a parenthesized literal list keep positional HAVING
				// distinct from the post-aggregate clause in the same statement.
				grouped := t14Query{
					Fixture: T14FixtureName, Columns: []t07Column{{Target: "host"}, {Function: "SUM", Target: "v", Alias: "total"}}, GroupBy: []string{"host"},
					Membership: &t14Membership{Column: column, Operator: operator, Form: "list"}, Having: t10Binary(">", t14Aggregate("SUM", "v"), t10Literal(int64(3))),
				}
				if err := add(prefix+"_and_having", grouped, [][]any{{second}, {one}, {two}}, "t14.membership_having", "t14.membership_spelling", "t14.local"); err != nil {
					return nil, err
				}
				grouped.Where = t10Binary(">", t10Source("v"), t10Literal(int64(0)))
				if err := add(prefix+"_where_and_having", grouped, [][]any{{second}, {one}, {two}},
					"t14.membership_having", "t14.membership_spelling", "t14.raw_filter", "t14.local"); err != nil {
					return nil, err
				}
			}
		}
	}
	return scenarios, nil
}

func generateT14Legacy() ([]HavingScenario, error) {
	var scenarios []HavingScenario
	for _, column := range []string{"host", "zone"} {
		var first, all, duplicate any = "red", []string{"red", "blue"}, []string{"red", "red"}
		if column == "zone" {
			first, all, duplicate = int64(1), []int64{1, 2}, []int64{1, 1}
		}
		if column == "host" {
			first, all, duplicate = "a", []string{"a", "b"}, []string{"a", "a"}
		}
		for _, operator := range []string{"HAVING", "NOT HAVING", "CONTAINS", "NOT CONTAINS"} {
			for _, form := range []string{"single", "list"} {
				scenario := HavingScenario{Name: "t14_legacy_" + featureName(operator) + "_" + column + "_" + form, Kind: "legacy-membership"}
				values := []any{first, all, duplicate}
				if form == "single" {
					values[2] = nil
				}
				for ordinal, value := range values {
					q := t14Query{
						Legacy: true, Fixture: T14LegacyFixtureName, Columns: []t07Column{{Target: "rid"}}, Begin: -1, End: 8000,
						Membership: &t14Membership{Column: column, Operator: operator, Form: form, Values: []any{value}},
					}
					c, err := buildT14Case(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), q, "t14.legacy_membership", "t14.membership_duplicates")
					if err != nil {
						return nil, err
					}
					scenario.Cases = append(scenario.Cases, c)
				}
				scenarios = append(scenarios, scenario)
			}
		}
	}
	return scenarios, nil
}
