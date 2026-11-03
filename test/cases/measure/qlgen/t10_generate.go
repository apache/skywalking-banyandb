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
	"math"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
)

func generateT10(b *suiteBuilder) ([]Case, []ExpressionScenario, []Case, error) {
	readiness, err := t10ReadinessCase()
	if err != nil {
		return nil, nil, nil, err
	}
	b.features["t10.fixture"] = true
	var scenarios []ExpressionScenario
	add := func(name string, query t10Query, features ...string) {
		if b.err != nil {
			return
		}
		scenario := ExpressionScenario{Name: "t10_" + name}
		for ordinal, window := range [][2]int64{{-1, 6000}, {1000, 6000}, {200000, 201000}} {
			query.Begin, query.End = window[0], window[1]
			c, buildErr := buildT10Case(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), query, append(features, "t10.rebind")...)
			if buildErr != nil {
				b.err = buildErr
				return
			}
			if c.WantCode != codes.OK {
				b.err = fmt.Errorf("success expression scenario %s produced %s", name, c.WantCode)
				return
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		for _, feature := range append(features, "t10.rebind") {
			b.features[feature] = true
		}
		scenarios = append(scenarios, scenario)
	}
	v, f := t10Source("v"), t10Source("f")
	i := func(value int64) *t10Expr { return t10Literal(value) }
	nullInt := t10Cast(t10Literal(nil), "BIGINT")
	cmp := func(op string, value int64) *t10Expr { return t10Binary(op, v, i(value)) }
	projectBool := func(name string, expression *t10Expr) t10Column {
		return t10Field(name, t10Cast(expression, "BIGINT"))
	}
	precedence := t10Binary("+", v, t10Binary("*", i(2), i(3)))
	precedence.Text = "v::FIELD + 2 * 3"
	add("arithmetic_F", t10Query{Columns: []t10Column{
		t10Field("shifted", t10Binary("+", v, i(2))), t10Field("difference", t10Binary("-", v, i(2))),
		t10Field("product", t10Binary("*", v, i(3))), t10Field("quotient", t10Binary("/", v, i(2))),
		t10Field("remainder", t10Binary("%", v, i(3))), t10Field("negative", t10Unary("negate", v)),
		t10Field("positive", t10Unary("positive", v)),
		t10Field("precedence", precedence),
	}}, "t10.arithmetic", "t10.numeric_null", "catalog.B7")
	add("integer_division", t10Query{Columns: []t10Column{
		t10Field("negative", t10Binary("/", t10Unary("negate", i(5)), i(2))),
		t10Field("negative_divisor", t10Binary("/", i(5), t10Unary("negate", i(2)))),
		t10Field("negative_remainder", t10Binary("%", t10Unary("negate", i(5)), i(2))),
	}}, "t10.arithmetic")
	exponent := &t10Expr{Op: "literal", Value: float64(0.125), Text: "1.25e-1"}
	add("floating_literals", t10Query{Columns: []t10Column{
		t10Field("decimal", t10Binary("+", v, t10Literal(0.5))),
		t10Field("exponent", t10Binary("+", f, exponent)),
		t10Field("mixed", t10Binary("/", v, t10Literal(2.0))),
	}}, "t10.float_literals", "t10.mixed_numeric", "t10.numeric_null")
	add("casts", t10Query{Columns: []t10Column{
		t10Field("integer", t10Cast(f, "INT")), t10Field("float", t10Cast(v, "FLOAT")),
		t10Field("truncated", t10Cast(t10Literal(-5.75), "BIGINT")),
		t10Field("number_text", t10Cast(v, "STRING")),
		t10Field("annotated", &t10Expr{Op: "annotate", Type: "DOUBLE", Args: []*t10Expr{v}}),
	}}, "t10.casts", "t10.numeric_null")
	add("comparison", t10Query{Columns: []t10Column{
		projectBool("eq", cmp("=", 2)), projectBool("ne", cmp("!=", 2)), projectBool("lt", cmp("<", 2)),
		projectBool("le", cmp("<=", 2)), projectBool("gt", cmp(">", 2)), projectBool("ge", cmp(">=", 2)),
		t10Field("boolean_text", t10Cast(cmp("=", 2), "STRING")),
	}}, "t10.comparisons", "t10.three_valued")
	add("three_valued_logic", t10Query{Columns: []t10Column{
		projectBool("and", t10Binary("AND", cmp(">", 0), t10Binary("<", f, t10Literal(2.0)))),
		projectBool("or", t10Binary("OR", cmp("=", 0), t10Binary(">", f, t10Literal(3.0)))),
		projectBool("not", t10Unary("NOT", cmp("=", 0))),
		projectBool("unknown_and_false", t10Binary("AND", cmp("=", 0), t10Literal(false))),
		projectBool("unknown_or_true", t10Binary("OR", cmp("=", 0), t10Literal(true))),
		projectBool("is_null", t10Unary("IS NULL", v)), projectBool("not_null", t10Unary("IS NOT NULL", v)),
	}}, "t10.three_valued", "t10.boolean_logic")
	add("membership", t10Query{Columns: []t10Column{
		projectBool("member", t10Call("IN", v, i(0), i(4))),
		projectBool("null_member", t10Call("IN", v, i(0), nullInt)),
		projectBool("not_member", t10Call("NOT IN", v, i(0), nullInt)),
		projectBool("empty", t10Call("IN", v)), projectBool("not_empty", t10Call("NOT IN", v)),
	}}, "t10.membership", "t10.three_valued")
	masked := t10Call("CASE", cmp("=", 0), i(0), t10Binary("/", i(8), v))
	add("masked_case", t10Query{Columns: []t10Column{t10Field("ratio", masked)}}, "t10.masked_case", "t10.numeric_null")
	add("simple_case", t10Query{Columns: []t10Column{
		t10Field("ratio", t10Call("SIMPLE CASE", v, i(0), i(0), t10Binary("/", i(8), v))),
		t10Field("nullable", t10Call("SIMPLE CASE", v, nullInt, i(99), i(-1))),
	}}, "t10.simple_case", "t10.masked_case")
	add("nested_and_implicit_else", t10Query{Columns: []t10Column{
		t10Field("nested", t10Call("CASE", t10Unary("IS NULL", v), i(-1), masked)),
		t10Field("implicit", t10Call("CASE", cmp("=", 2), i(4))),
		t10Field("simple_implicit", t10Call("SIMPLE CASE", v, i(2), i(4))),
	}}, "t10.masked_case", "t10.simple_case")
	invalid := t10Cast(t10Literal("invalid"), "BIGINT")
	add("unreachable_invalid_arm", t10Query{Columns: []t10Column{
		t10Field("first", t10Call("CASE", t10Literal(true), v, invalid)),
		t10Field("later_condition", t10Call("CASE", t10Literal(true), i(7),
			t10Binary("=", t10Binary("/", i(1), i(0)), i(0)), i(8), invalid)),
		t10Field("coalesced", t10Call("COALESCE", i(7), invalid)),
	}}, "t10.masked_case", "t10.masked_coalesce", "t10.unreachable_invalid")
	add("coalesce_F", t10Query{Columns: []t10Column{
		t10Field("filled", t10Call("COALESCE", v, i(-1))),
		t10Field("typed", t10Call("COALESCE", nullInt, v, i(-1))),
		t10Field("late_invalid", t10Call("COALESCE", v, i(-1), invalid)),
	}}, "t10.masked_coalesce", "t10.typed_null")
	add("conditional_numeric_promotion", t10Query{Columns: []t10Column{
		t10Field("chosen", t10Call("CASE", cmp(">=", 2), v, f)),
		t10Field("filled", t10Call("COALESCE", v, t10Literal(-1.5))),
	}}, "t10.mixed_numeric", "t10.masked_case", "t10.masked_coalesce")
	add("typed_null", t10Query{Columns: []t10Column{
		t10Field("integer_null", nullInt), t10Field("double_null", t10Cast(t10Literal(nil), "DOUBLE")),
		t10Field("string_null", t10Cast(t10Literal(nil), "STRING")),
		t10Field("timestamp_null", t10Cast(t10Literal(nil), "TIMESTAMP")),
		t10Field("null_division", t10Binary("/", nullInt, i(0))),
		t10Field("condition_null", t10Cast(t10Call("CASE", t10Cast(t10Literal(nil), "BOOLEAN"),
			t10Literal(true), t10Cast(t10Literal(nil), "BOOLEAN")), "STRING")),
	}}, "t10.typed_null", "t10.three_valued")
	add("strings", t10Query{Columns: []t10Column{
		t10Field("filled", t10Call("COALESCE", t10Source("zone"), t10Literal("missing"))),
		t10Field("chosen", t10Call("CASE", cmp("=", 0), t10Source("host"), t10Literal("other"))),
	}}, "t10.string_output", "t10.masked_coalesce")
	add("arrays", t10Query{Columns: []t10Column{
		t10Field("labels_copy", t10Call("CASE", t10Unary("IS NULL", v), t10Source("labels"), t10Source("labels"))),
		t10Field("nums_copy", t10Call("COALESCE", t10Source("nums"), t10Source("nums"))),
	}}, "t10.array_output", "t10.masked_case", "t10.masked_coalesce")
	add("timestamp", t10Query{Columns: []t10Column{
		t10Field("clock", t10Cast(t10Source("time"), "TIMESTAMP")),
		t10Field("nanos", t10Cast(t10Source("time"), "BIGINT")),
		t10Field("utc", t10Cast(t10Literal("2026-10-16T08:00:00.123456789+08:00"), "TIMESTAMP")),
		t10Field("epoch", t10Cast(i(1700000000123456789), "TIMESTAMP")),
		t10Field("clock_text", t10Cast(t10Source("time"), "STRING")),
	}}, "t10.timestamp_cast")
	add("lexer_compat", t10Query{Columns: []t10Column{
		{Expr: t10Source("metric-value")},
		{Expr: t10Source("rate_total")},
		t10Field("shifted", t10Binary("-", v, i(2))),
	}}, "t10.lexer_compat")
	add("default_output_name", t10Query{Columns: []t10Column{{Expr: t10Binary("+", v, i(2))}}}, "t10.default_names")
	add("residual_selection", t10Query{
		Where:   &T03Time{Op: "OR", Children: []*T03Time{t03Compare("=", 1000), t03Compare("=", 4000)}},
		Columns: []t10Column{t10Field("ratio", t10Binary("/", i(8), v))},
	}, "t10.selection_mask")
	add("pagination", t10Query{
		Limit: uint32Pointer(2), Offset: 1,
		Columns: []t10Column{t10Field("shifted", t10Binary("+", v, i(2)))},
	}, "t10.pagination")
	add("limit_zero", t10Query{
		Limit:   uint32Pointer(0),
		Columns: []t10Column{t10Field("unreachable", t10Binary("/", i(1), i(0)))},
	}, "t10.no_scan")
	if b.err != nil {
		return nil, nil, nil, b.err
	}
	parameterScenarios, parameterErr := generateT10Parameters(b)
	if parameterErr != nil {
		return nil, nil, nil, parameterErr
	}
	scenarios = append(scenarios, parameterScenarios...)
	rejections, rejectionErr := generateT10Rejections(b, v)
	return []Case{readiness}, scenarios, rejections, rejectionErr
}

func generateT10Parameters(b *suiteBuilder) ([]ExpressionScenario, error) {
	var scenarios []ExpressionScenario
	for _, definition := range []struct {
		column t10Column
		name   string
		values []any
	}{
		{
			t10Field("shifted", t10Binary("+", t10Source("v"), t10Cast(t10Parameter(0), "DOUBLE"))),
			"cast_parameter",
			[]any{"1.5", "-2.25", "0.125"},
		},
		{
			t10Field("flag", t10Cast(t10Cast(t10Parameter(0), "BOOLEAN"), "STRING")),
			"boolean_parameter",
			[]any{"true", "false", "true"},
		},
		{
			t10Field("parsed", t10Cast(t10Parameter(0), "DOUBLE")),
			"invalid_parameter_recovery",
			[]any{"invalid", "1.5", "-2.25"},
		},
	} {
		scenario := ExpressionScenario{Name: "t10_" + definition.name}
		for ordinal, value := range definition.values {
			q := t10Query{Columns: []t10Column{definition.column}, Params: []any{value}, Begin: -1, End: 6000}
			features := []string{"t10.parameter_cast", "t10.rebind"}
			if definition.name == "invalid_parameter_recovery" && ordinal == 0 {
				features = []string{"t10.invalid_value"}
			}
			c, err := buildT10Case(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), q, features...)
			if err != nil {
				return nil, err
			}
			for _, feature := range features {
				b.features[feature] = c.WantCode == codes.OK
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		scenarios = append(scenarios, scenario)
	}
	return scenarios, nil
}

func generateT10Rejections(b *suiteBuilder, v *t10Expr) ([]Case, error) {
	var cases []Case
	i := func(value int64) *t10Expr { return t10Literal(value) }
	minimum := t10Binary("-", t10Unary("negate", i(math.MaxInt64)), i(1))
	for name, expression := range map[string]*t10Expr{
		"integer_overflow":    t10Binary("+", i(math.MaxInt64), i(1)),
		"subtract_overflow":   t10Binary("-", minimum, i(1)),
		"multiply_overflow":   t10Binary("*", i(math.MaxInt64), i(2)),
		"divide_overflow":     t10Binary("/", minimum, t10Unary("negate", i(1))),
		"negate_overflow":     t10Unary("negate", minimum),
		"zero_division":       t10Binary("/", i(8), v),
		"float_zero_division": t10Binary("/", t10Literal(8.0), v),
		"zero_remainder":      t10Binary("%", i(8), v),
		"invalid_integer":     t10Cast(t10Literal("1.5"), "BIGINT"),
		"invalid_float":       t10Cast(t10Literal("invalid"), "DOUBLE"),
		"invalid_boolean":     t10Cast(t10Literal("invalid"), "BOOLEAN"),
		"integer_range":       t10Cast(t10Literal("9223372036854775808"), "BIGINT"),
		"double_range":        t10Cast(t10Literal(9223372036854775808.0), "BIGINT"),
		"nan_to_integer":      t10Cast(t10Cast(t10Literal("NaN"), "DOUBLE"), "BIGINT"),
		"infinity_to_integer": t10Cast(t10Cast(t10Literal("+Inf"), "DOUBLE"), "BIGINT"),
	} {
		c, err := buildT10Case("t10_reject_"+name, t10Query{Columns: []t10Column{t10Field("bad", expression)}, Begin: -1, End: 6000},
			"t10.invalid_value")
		if err != nil {
			return nil, err
		}
		if c.WantCode != codes.InvalidArgument {
			return nil, fmt.Errorf("numeric rejection %s has no independent error witness", name)
		}
		cases = append(cases, c)
	}
	// Stable sorting is required because these definitions are intentionally keyed.
	sortT10Cases(cases)
	for name, expression := range map[string]string{
		"boolean_output":   "v::FIELD = 0",
		"untyped_null":     "NULL",
		"nonboolean_when":  "CASE WHEN v::FIELD THEN 1 ELSE 0 END",
		"mixed_case_types": "CASE WHEN v::FIELD = 0 THEN 'text' ELSE 1 END",
		"implicit_string":  "v::FIELD + '1'",
		"unsupported_cast": "CAST(v::FIELD AS UNKNOWN_TYPE)",
	} {
		cases = append(cases, Case{
			Name: "t10_reject_" + name, Ticket: "T10", WantCode: codes.InvalidArgument,
			Query: "SELECT " + expression + " AS bad FROM " + FixtureGroup + "." + T10FixtureName +
				" WHERE time >= '" + timePlaceholder(-1) + "' AND time < '" + timePlaceholder(6000) + "'",
			Features: []string{"t10.invalid_type"}, NoScan: true,
		})
	}
	sortT10Cases(cases)
	b.features["t10.invalid_value"], b.features["t10.invalid_type"] = false, false
	return cases, nil
}

func validateExpressionScenarios(readiness []Case, scenarios []ExpressionScenario) error {
	if len(readiness) != 1 || readiness[0].Name != "t10_ready" || len(scenarios) == 0 {
		return fmt.Errorf("generated T10 suite lacks its fixture visibility or expression scenarios")
	}
	for _, scenario := range scenarios {
		if len(scenario.Cases) != 3 {
			return fmt.Errorf("generated T10 scenario %s lacks three independent bindings", scenario.Name)
		}
		var text string
		for _, c := range scenario.Cases {
			query, _, _ := strings.Cut(c.Query, "\n#!params:")
			if text == "" {
				text = query
			}
			if text != query {
				return fmt.Errorf("generated T10 scenario %s changes its expression query text", scenario.Name)
			}
		}
	}
	return nil
}

func sortT10Cases(cases []Case) {
	slices.SortFunc(cases, func(left, right Case) int { return strings.Compare(left.Name, right.Name) })
}
