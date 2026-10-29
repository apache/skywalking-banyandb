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

func generateT15(b *suiteBuilder) ([]Case, []DistinctScenario, []Case, error) {
	var readiness []Case
	rows := t15OracleRows()
	for start := 0; start < len(rows); start += 64 {
		end := min(start+64, len(rows))
		var points []any
		for _, row := range rows[start:end] {
			points = append(points, projectRow(row, []string{"rid", "host", "zone", "v", "f"}, true))
		}
		query := "SELECT rid, host, zone, v::FIELD, f::FIELD FROM " + FixtureGroup + "." + T15FixtureName +
			" WHERE time >= '" + timePlaceholder(rows[start].Millis) + "' AND time < '" +
			timePlaceholder(rows[end-1].Millis+1) + "' ORDER BY TIME ASC"
		c, e := t07ExpectedCase(fmt.Sprintf("t15_ready_%d", start), query, points, []string{"t15.fixture"})
		if e != nil {
			return nil, nil, nil, e
		}
		c.Ticket = "T15"
		c.Ordered = true
		readiness = append(readiness, c)
	}
	b.features["t15.fixture"] = true
	var scenarios []DistinctScenario
	add := func(name string, q t15Query, features ...string) {
		scenario := DistinctScenario{Name: "t15_" + name}
		for ordinal, window := range [][2]int64{{-1, 300000}, {0, 800}, {400000, 401000}} {
			q.Begin, q.End = window[0], window[1]
			if len(q.Params) > 0 {
				q.Params = []any{int64(ordinal + 1)}
			}
			c, e := buildT15Case(fmt.Sprintf("%s_binding_%d", scenario.Name, ordinal), q, append(features, "t15.rebind")...)
			if e != nil {
				b.err = e
				return
			}
			scenario.Cases = append(scenario.Cases, c)
		}
		for _, feature := range append(features, "t15.rebind") {
			b.features[feature] = true
		}
		scenarios = append(scenarios, scenario)
	}
	for _, source := range []string{"zone", "host", "v", "f", "time"} {
		add("scalar_"+source, t15Query{Columns: []t10Column{t10Field("value", t10Source(source))}}, "t15.scalar", "t15.scalar."+source, "catalog.B6")
	}
	// Cast keeps this fixture selection in the typed residual kernel; it does not
	// claim support for native range predicates on an unindexed entity tag.
	zoneF := t15Query{
		Columns: []t10Column{t10Field("zone", t10Source("zone"))},
		Where: t10Binary("AND",
			t10Binary(">=", t10Cast(t10Source("rid"), "INT"), t10Literal(int64(5))),
			t10Binary("<=", t10Cast(t10Source("rid"), "INT"), t10Literal(int64(8)))),
	}
	add("zone_F", zoneF, "t15.null_empty")
	tuple := t15Query{Columns: []t10Column{t10Field("host", t10Source("host")), t10Field("zone", t10Source("zone"))}}
	add("tuple", tuple, "t15.tuple", "t15.complete_input")
	add("hidden_group", t15Query{Columns: []t10Column{t10Field("zone", t10Source("zone"))}, GroupBy: []string{"host", "zone"}}, "t15.hidden_group")
	add("cast", t15Query{Columns: []t10Column{t10Field("value", t10Cast(t10Source("v"), "STRING"))}}, "t15.expression")
	caseExpression := t10Call("CASE", t10Binary(">", t10Source("v"), t10Literal(int64(2))), t10Literal("high"), t10Literal("low"))
	add("case", t15Query{Columns: []t10Column{t10Field("value", caseExpression)}}, "t15.expression")
	filtered := tuple
	filtered.Where = t10Binary(">=", t10Source("v"), t10Parameter(0))
	filtered.Params = []any{int64(1)}
	add("where", filtered, "t15.where")
	limited := tuple
	limited.Order = ""
	limit := uint32(200)
	limited.Limit = &limit
	limited.Offset = 0
	add("before_limit", limited, "t15.pagination")
	one := uint32(1)
	constant := t15Query{Columns: []t10Column{t10Field("value", t10Literal("retained"))}, Limit: &one}
	add("limit_constant", constant, "t15.limit_early")
	constant.Offset = 1
	add("offset_past_constant", constant, "t15.offset_complete")
	filteredLimit := t15Query{
		Columns: []t10Column{t10Field("value", t10Cast(t10Source("v"), "STRING"))},
		Where:   t10Binary("=", t10Source("v"), t10Parameter(0)), Params: []any{int64(1)}, Limit: &one,
	}
	add("limit_filtered", filteredLimit, "t15.limit_early", "t15.where")
	timestampPage := t15Query{Columns: []t10Column{t10Field("stamp", t10Source("time"))}, Order: "TIME ASC", Offset: 1}
	pageSize := uint32(3)
	timestampPage.Limit = &pageSize
	add("timestamp_page", timestampPage, "t15.pagination")
	zero := uint32(0)
	limited.Limit = &zero
	add("limit_zero", limited, "t15.no_scan")
	aggregate := t15Query{
		Columns: []t10Column{t10Field("total", t14Aggregate("SUM", "v"))},
		GroupBy: []string{"host"},
		Having:  t10Binary(">=", t14Aggregate("COUNT", "v"), t10Literal(int64(1))),
	}
	add("having_hidden_count", aggregate, "t15.having", "t15.aggregate_argument_qualifier")
	var rejections []Case
	for _, source := range []string{"labels", "nums"} {
		q := t15Query{Columns: []t10Column{t10Field("value", t10Source(source))}, Begin: -1, End: 300000}
		query, e := renderT15(q)
		if e != nil {
			return nil, nil, nil, e
		}
		rejections = append(rejections, Case{
			Name: "t15_reject_" + source, Ticket: "T15", Query: query,
			WantCode: codes.InvalidArgument, NoScan: true,
			Features: []string{"t15.unsupported_keys"},
		})
	}
	q := tuple
	q.Order = "v DESC"
	q.Begin, q.End = -1, 300000
	query, e := renderT15(q)
	if e != nil {
		return nil, nil, nil, e
	}
	rejections = append(rejections, Case{
		Name: "t15_reject_hidden_order", Ticket: "T15", Query: query,
		WantCode: codes.InvalidArgument, NoScan: true,
		Features: []string{"t15.hidden_order"},
	})
	b.features["t15.unsupported_keys"], b.features["t15.hidden_order"] = false, false
	return readiness, scenarios, rejections, b.err
}

func validateDistinctScenarios(s Suite) error {
	if len(s.DistinctReadiness) == 0 || len(s.DistinctScenarios) == 0 || len(s.DistinctRejections) != 3 {
		return fmt.Errorf("T15 coverage missing")
	}
	required := map[string]bool{}
	for _, name := range []string{
		"scalar_zone", "scalar_host", "scalar_v", "scalar_f", "scalar_time", "zone_F", "tuple",
		"cast", "case", "where", "before_limit", "limit_zero", "having_hidden_count", "timestamp_page", "hidden_group",
		"limit_constant", "limit_filtered", "offset_past_constant",
	} {
		required["t15_"+name] = true
	}
	for _, scenario := range s.DistinctScenarios {
		if !required[scenario.Name] {
			return fmt.Errorf("unexpected T15 scenario %s", scenario.Name)
		}
		delete(required, scenario.Name)
		if len(scenario.Cases) != 3 {
			return fmt.Errorf("T15 bindings missing")
		}
		query := strings.Split(scenario.Cases[0].Query, "\n#!params:")[0]
		for _, c := range scenario.Cases {
			if !strings.HasPrefix(c.Query, "SELECT DISTINCT ") || strings.Split(c.Query, "\n#!params:")[0] != query {
				return fmt.Errorf("T15 statement changed")
			}
		}
	}
	if len(required) > 0 {
		return fmt.Errorf("T15 bounded capability witnesses missing: %v", required)
	}
	return nil
}
