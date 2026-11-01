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

func generateT12(b *suiteBuilder) ([]Case, []FilterScenario, []Case, error) {
	readiness, err := t12ReadinessCase()
	if err != nil {
		return nil, nil, nil, err
	}
	b.features["t12.fixture"] = true
	var scenarios []FilterScenario
	add := func(name string, q t12Query, features ...string) {
		if b.err != nil {
			return
		}
		windows := [][2]int64{{-1, 6000}, {1000, 6000}, {200000, 201000}}
		if q.Begin == 10000 {
			windows = [][2]int64{{10000, 13000}, {10400, 13000}, {200000, 201000}}
		}
		scenario := FilterScenario{Name: "t12_" + name, Kind: "query"}
		for ordinal, window := range windows {
			q.Begin, q.End = window[0], window[1]
			c, buildErr := buildT12Case(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), q, append(features, "t12.rebind")...)
			if buildErr != nil {
				b.err = buildErr
				return
			}
			if c.WantCode != codes.OK {
				b.err = fmt.Errorf("successful filter scenario %s generated %s", name, c.WantCode)
				return
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		for _, feature := range append(features, "t12.rebind") {
			b.features[feature] = true
		}
		scenarios = append(scenarios, scenario)
	}
	v, f := t10Source("v"), t10Source("f")
	integer := func(value int64) *t10Expr { return t10Literal(value) }
	compare := func(op string, value int64) *t10Expr { return t10Binary(op, v, integer(value)) }
	rid := []t10Column{{Expr: t10Source("rid")}}
	aggregate := []t07Column{{Function: "SUM", Target: "v", Alias: "total"}, {Function: "COUNT", Target: "v", Alias: "count"}}
	parameterPredicate := t10Binary(">", v, t10Cast(t10Parameter(0), "DOUBLE"))
	for _, definition := range []struct {
		expression *t10Expr
		name       string
	}{
		{name: "constant_null", expression: t10Literal(nil)},
		{name: "constant_typed_null", expression: t10Cast(t10Literal(nil), "BOOLEAN")},
		{name: "constant_true", expression: t10Literal(true)},
		{name: "constant_false", expression: t10Literal(false)},
	} {
		add(definition.name, t12Query{Columns: rid, Predicate: definition.expression, Legacy: true}, "t12.constant_predicate", "t12.three_valued")
	}
	add("field_filter_F", t12Query{Columns: rid, Predicate: parameterPredicate, Params: []any{"1"}},
		"catalog.B8", "t12.field_filter", "t12.hidden_input", "t12.parameter_cast")
	add("filter_before_aggregate", t12Query{Aggregates: aggregate, Predicate: parameterPredicate, Params: []any{"1"}}, "t12.local_aggregate", "t12.hidden_input")
	add("float_filter", t12Query{Columns: rid, Predicate: t10Binary(">", f, t10Literal(0.5))}, "t12.float_filter")
	add("boolean_three_valued", t12Query{Columns: rid, Predicate: t10Binary("AND", t10Unary("NOT", compare("=", 0)),
		t10Binary("OR", t10Binary("<", f, t10Literal(2.0)), t10Unary("IS NULL", f)))}, "t12.three_valued", "t12.boolean_logic")
	add("null_is_true", t12Query{Columns: rid, Predicate: t10Unary("IS NULL", v)}, "t12.three_valued", "t12.null_predicate")
	add("unknown_drops", t12Query{Columns: rid, Predicate: t10Binary("AND", compare(">", 0), t10Cast(t10Literal(nil), "BOOLEAN"))}, "t12.three_valued")
	add("numeric_boolean_cast", t12Query{Columns: rid, Predicate: t10Cast(v, "BOOLEAN")}, "t12.boolean_logic", "t12.parameter_cast")
	arithmetic := t10Binary("/", t10Binary("+", t10Binary("*", t10Unary("negate", v), integer(-2)), t10Unary("positive", integer(2))), integer(2))
	add("arithmetic_predicate", t12Query{Columns: rid, Predicate: t10Binary(">=", t10Binary("%", t10Binary("-", arithmetic, integer(1)), integer(3)), integer(1))},
		"t12.arithmetic")
	invalid := t10Binary("/", integer(1), integer(0))
	add("masked_case", t12Query{Columns: rid, Predicate: t10Call("CASE", compare("=", 0), t10Literal(false),
		t10Binary(">", t10Binary("/", integer(8), v), integer(1)))}, "t12.masked_case")
	add("masked_and", t12Query{Columns: rid, Predicate: t10Binary("AND", compare("!=", 0), t10Binary(">", t10Binary("/", integer(8), v), integer(1)))},
		"t12.boolean_logic", "t12.masked_boolean")
	add("masked_or", t12Query{Columns: rid, Predicate: t10Binary("OR", compare("=", 0), t10Binary(">", t10Binary("/", integer(8), v), integer(1)))},
		"t12.boolean_logic", "t12.masked_boolean")
	add("masked_invalid_condition", t12Query{Columns: rid, Predicate: t10Call("CASE", t10Literal(true), compare(">", 0),
		t10Binary("=", invalid, integer(0)), t10Literal(true), t10Literal(false))}, "t12.masked_case")
	add("simple_case_predicate", t12Query{Columns: rid, Predicate: t10Call("SIMPLE CASE", v, integer(0), t10Literal(false), integer(2), t10Literal(true), compare(">", 4))},
		"t12.masked_case")
	add("coalesced_unknown", t12Query{Columns: rid, Predicate: t10Call("COALESCE", compare(">", 0), t10Literal(true))}, "t12.coalesce", "t12.three_valued")
	add("membership_null", t12Query{Columns: rid, Predicate: t10Call("IN", v, integer(0), integer(4), t10Cast(t10Literal(nil), "BIGINT"))},
		"t12.membership", "t12.three_valued")
	add("not_membership_null", t12Query{Columns: rid, Predicate: t10Call("NOT IN", v, integer(0), t10Cast(t10Literal(nil), "BIGINT"))}, "t12.membership", "t12.three_valued")
	add("empty_membership", t12Query{Columns: rid, Predicate: t10Call("NOT IN", v)}, "t12.membership", "t12.empty_membership", "t12.three_valued")
	add("time_field_or", t12Query{Columns: rid, Predicate: t10Binary("OR", t10Binary("<", t10Source("time"), t12TimeLiteral(1000)), compare(">", 4))}, "t12.time_field_or")
	add("legacy_time_intersection", t12Query{Columns: rid, Legacy: true, Predicate: t10Binary("OR",
		t10Binary("<", t10Source("time"), t12TimeLiteral(2000)), compare(">", 4))},
		"t12.legacy_time", "t12.time_field_or")
	add("native_tag_conjunction", t12Query{Columns: rid, NativeRIDs: []int64{2, 3, 5}, Predicate: compare(">", 3)}, "t12.native_tag_conjunction")
	add("tag_field_or", t12Query{Columns: rid, Predicate: t10Binary("OR", t10Binary("=", t10Source("rid"), integer(1)), compare(">", 4))}, "t12.tag_field_or")
	add("filter_before_projection", t12Query{Columns: []t10Column{t10Field("ratio", t10Binary("/", integer(8), v))}, Predicate: compare(">", 0)}, "t12.projection_order")
	add("filter_before_group", t12Query{Aggregates: append([]t07Column{{Target: "host"}}, aggregate...), GroupBy: []string{"host"}, Predicate: compare(">", 0)},
		"t12.local_aggregate", "t12.group_order")
	add("empty_aggregate", t12Query{Aggregates: aggregate, Predicate: compare(">", 100)}, "t12.local_aggregate", "t12.empty_aggregate")
	add("all_null_aggregate", t12Query{Aggregates: aggregate, Predicate: t10Unary("IS NULL", v)}, "t12.local_aggregate", "t12.empty_aggregate")
	add("tail_omitted_limit", t12Query{Columns: rid, Begin: 10000, Predicate: compare(">=", 0)}, "t12.complete_input", "t12.omitted_limit")
	add("tail_aggregate_limit", t12Query{Aggregates: aggregate, Begin: 10000, Limit: uint32Pointer(1), Predicate: compare(">=", 0)},
		"t12.complete_input", "t12.local_aggregate", "t12.aggregate_before_limit")
	add("tail_hidden_limit_offset", t12Query{Columns: rid, Begin: 10000, Limit: uint32Pointer(3), Offset: 2, Predicate: compare(">=", 200)},
		"t12.complete_input", "t12.hidden_input", "t12.final_limit")
	add("zero_limit", t12Query{Columns: rid, Limit: uint32Pointer(0), Predicate: compare(">", 0)}, "t12.no_scan", "t12.final_limit")
	for _, definition := range []struct {
		name, op string
		values   []any
	}{
		{"membership_parameter", "IN", []any{[]int64{0, 4}, []int64{}, []int64{2, 6}}},
		{"not_membership_parameter", "NOT IN", []any{[]int64{}, []int64{2, 4}, []int64{6}}},
		{"parameter_recovery", ">", []any{"invalid", "1", "3"}},
		{"nan_order", ">", []any{"NaN", "+Inf", "-Inf"}},
	} {
		scenario := FilterScenario{Name: "t12_" + definition.name, Kind: "query"}
		for ordinal, value := range definition.values {
			predicate := t10Binary(definition.op, v, t10Cast(t10Parameter(0), "DOUBLE"))
			features := []string{"t12.parameter_cast", "t12.rebind"}
			if definition.op == "IN" || definition.op == "NOT IN" {
				predicate = t10Call(definition.op, v, t10Parameter(0))
				features = append(features, "t12.membership", "t12.empty_membership")
			}
			if definition.name == "nan_order" {
				predicate = t10Binary(">", t10Cast(t10Parameter(0), "DOUBLE"), f)
				features = append(features, "t12.nan_policy")
			}
			if definition.name == "parameter_recovery" && ordinal == 0 {
				features = []string{"t12.invalid_value"}
			}
			c, buildErr := buildT12Case(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal),
				t12Query{Columns: rid, Predicate: predicate, Params: []any{value}, Begin: -1, End: 6000}, features...)
			if buildErr != nil {
				return nil, nil, nil, buildErr
			}
			for _, feature := range features {
				b.features[feature] = c.WantCode == codes.OK
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
	}
	// SELECT and WHERE parameter positions surround the two source-time bindings.
	add("select_and_filter_parameters", t12Query{
		Columns:   []t10Column{t10Field("shifted", t10Binary("+", v, t10Cast(t10Parameter(0), "BIGINT")))},
		Predicate: t10Binary(">", v, t10Cast(t10Parameter(1), "DOUBLE")), Params: []any{"7", "1"}, SelectParams: 1,
	}, "t12.parameter_order", "t12.projection_order")
	for _, kind := range []string{"low-budget", "cancellation"} {
		scenario := FilterScenario{Name: "t12_" + kind, Kind: kind}
		for ordinal, window := range [][2]int64{{-1, 6000}, {1000, 6000}, {200000, 201000}} {
			c, buildErr := buildT12Case(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal),
				t12Query{Columns: rid, Predicate: compare(">", 0), Begin: window[0], End: window[1]}, "t12.lifecycle", "t12.rebind")
			if buildErr != nil {
				return nil, nil, nil, buildErr
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
	}
	b.features["t12.lifecycle"] = true
	rejections, rejectErr := generateT12Rejections(b, v)
	return []Case{readiness}, scenarios, rejections, rejectErr
}

func generateT12Rejections(b *suiteBuilder, v *t10Expr) ([]Case, error) {
	var cases []Case
	for _, definition := range []struct {
		predicate *t10Expr
		name      string
	}{
		{name: "zero_division", predicate: t10Binary(">", t10Binary("/", t10Literal(int64(8)), v), t10Literal(int64(1)))},
		{name: "invalid_cast", predicate: t10Binary(">", v, t10Cast(t10Literal("invalid"), "DOUBLE"))},
	} {
		c, err := buildT12Case("t12_reject_"+definition.name,
			t12Query{Columns: []t10Column{{Expr: t10Source("rid")}}, Predicate: definition.predicate, Begin: -1, End: 6000}, "t12.invalid_value")
		if err != nil {
			return nil, err
		}
		if c.WantCode != codes.InvalidArgument {
			return nil, fmt.Errorf("filter error %s lacks scalar rejection witness", definition.name)
		}
		cases = append(cases, c)
	}
	for _, definition := range []struct {
		predicate *t10Expr
		name      string
	}{
		{name: "non_boolean", predicate: v},
		{name: "implicit_string", predicate: t10Binary(">", v, t10Literal("1"))},
		{name: "array_comparison", predicate: t10Binary("=", t10Source("labels"), t10Source("labels"))},
		{name: "unknown_field", predicate: &t10Expr{Op: "source", Value: "absent", Text: "absent::FIELD > 0"}},
		{name: "aggregate_in_where", predicate: &t10Expr{Op: "source", Text: "SUM(v) > 0"}},
	} {
		query, err := renderT12(t12Query{Columns: []t10Column{{Expr: t10Source("rid")}}, Predicate: definition.predicate, Begin: -1, End: 6000})
		if err != nil {
			return nil, err
		}
		cases = append(cases, Case{
			Name: "t12_reject_" + definition.name, Ticket: "T12",
			Query: query, WantCode: codes.InvalidArgument, Features: []string{"t12.invalid_type"}, NoScan: true,
		})
	}
	b.features["t12.invalid_value"], b.features["t12.invalid_type"] = false, false
	return cases, nil
}

func validateFilterScenarios(readiness []Case, scenarios []FilterScenario) error {
	if len(readiness) != 1 || readiness[0].Name != "t12_ready" || len(scenarios) == 0 {
		return fmt.Errorf("generated T12 suite lacks fixture visibility or field filter scenarios")
	}
	for _, scenario := range scenarios {
		if len(scenario.Cases) != 3 {
			return fmt.Errorf("generated T12 scenario %s lacks three bindings", scenario.Name)
		}
		if scenario.Kind != "query" && scenario.Kind != "low-budget" && scenario.Kind != "cancellation" {
			return fmt.Errorf("unknown generated filter scenario kind %s", scenario.Kind)
		}
		first, _, _ := strings.Cut(scenario.Cases[0].Query, "\n#!params:")
		for _, c := range scenario.Cases {
			if c.Filter == nil {
				return fmt.Errorf("generated T12 case %s lacks filter expectations", c.Name)
			}
			text, _, _ := strings.Cut(c.Query, "\n#!params:")
			if text != first {
				return fmt.Errorf("generated T12 scenario %s changes SQL text", scenario.Name)
			}
			if c.WantCode == codes.OK && c.Filter.InputRows != c.Filter.TrueRows+c.Filter.FalseRows+c.Filter.UnknownRows {
				return fmt.Errorf("generated filter %s loses a truth value", c.Name)
			}
			if c.Filter.Aggregate && c.AggregateInputs != c.Filter.TrueRows {
				return fmt.Errorf("generated filter %s aggregates rows outside TRUE selection", c.Name)
			}
		}
	}
	return nil
}
